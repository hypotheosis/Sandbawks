//! Kubernetes leader election via `coordination.k8s.io/v1` Leases (issue #436).
//!
//! The controller Deployment can run with `replicas > 1` for HA without
//! double-reconciling or double-sweeping snapshots: every replica races for a
//! single Lease, and only its holder runs the reconcile loops and the snapshot
//! retention sweeper. Standby replicas idle in an acquire loop and take over
//! within one Lease TTL if the leader is evicted or its node fails.
//!
//! The module is split so the only non-trivial logic — the expiry/holder
//! decision — is a pure function ([`decide`]) that takes an observed Lease spec
//! plus the wall clock and returns a [`LeaseDecision`], with no `Api<Lease>`
//! dependency. [`try_acquire_or_renew`] is the thin I/O shell that turns that
//! decision into a single kube API call. The pure core is unit-tested at the
//! bottom of this file; the `sandbawks-controller` binary owns only the acquire
//! loop and the background renewal task that drive these primitives.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
};
use kube::{api::PostParams, Api};

/// Default Lease TTL (seconds) when neither the env var nor an existing Lease
/// records one. Matches upstream kube-controller-manager's default.
///
/// Used in two places that MUST agree (issue #436 review): the
/// [`LeaderElectionConfig::from_env`] fallback for a missing
/// `SANDBAWKS_LEADER_LEASE_DURATION_SECS`, and the [`decide`] expiry math for a
/// Lease whose `lease_duration_seconds` field is unset. Defining it once keeps
/// a fresh Lease and a malformed one from being judged against different TTLs.
pub const DEFAULT_LEASE_DURATION_SECS: i32 = 15;

/// Default per-operation deadline (seconds) for one acquire/renew call.
pub const DEFAULT_RENEW_DEADLINE_SECS: u64 = 10;

/// Default interval (seconds) between acquire/renew attempts.
pub const DEFAULT_RETRY_PERIOD_SECS: u64 = 2;

/// Configuration for Kubernetes leader election.
///
/// All values are read from environment variables (the Helm chart surfaces
/// `controller.leaderElection.*` as those variables). When
/// `SANDBAWKS_LEADER_ELECTION_ENABLED` is absent or `false` the feature is
/// disabled and [`from_env`](Self::from_env) returns `Ok(None)`.
///
/// Fields are private; the binary reads them through the accessors below so the
/// timing invariant (`0 < retry_period < renew_deadline < lease_duration`) can
/// only be established via [`from_env`](Self::from_env), which calls
/// [`validate`](Self::validate) before constructing the struct.
#[derive(Clone, Debug)]
pub struct LeaderElectionConfig {
    /// Name of the Lease object this controller competes for.
    lease_name: String,
    /// Unique identity for this replica — the Pod name when running in-cluster.
    holder_identity: String,
    /// How long (seconds) a Lease is valid without a renewal heartbeat.
    lease_duration_secs: i32,
    /// Upper bound on how long to wait for a single acquire/renew operation,
    /// and the total since-last-success budget the renewal task spends before
    /// exiting.
    renew_deadline_secs: u64,
    /// How often to attempt acquire or renew.
    retry_period: Duration,
}

impl LeaderElectionConfig {
    /// Read leader election config from environment variables.
    ///
    /// # Returns
    ///
    /// `Ok(None)` when leader election is not enabled (the default
    /// single-replica path). `Ok(Some(config))` when
    /// `SANDBAWKS_LEADER_ELECTION_ENABLED` is `true` or `1` and the parsed
    /// timing values pass [`validate`](Self::validate).
    ///
    /// # Errors
    ///
    /// Returns an error when the timing values violate the
    /// `0 < retry_period < renew_deadline < lease_duration` invariant, so a
    /// misconfigured chart fails fast at startup rather than silently running a
    /// Lease that can never be renewed in time.
    pub fn from_env() -> Result<Option<Self>> {
        let enabled = std::env::var("SANDBAWKS_LEADER_ELECTION_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        if !enabled {
            return Ok(None);
        }
        let lease_name = std::env::var("SANDBAWKS_LEADER_LEASE_NAME")
            .unwrap_or_else(|_| "sandbawks-controller-leader".into());
        // `POD_NAME` is injected by the Helm chart via the downward API and is
        // globally unique. Off-cluster (local dev) there is no POD_NAME, so
        // fall back to `controller-<hostname>-<pid>`: the hostname keeps the
        // identity distinct across machines (PID alone collides between hosts,
        // and can even repeat on one host after PID reuse).
        let holder_identity = std::env::var("POD_NAME")
            .unwrap_or_else(|_| format!("controller-{}-{}", local_hostname(), std::process::id()));
        let lease_duration_secs = parse_env_or(
            "SANDBAWKS_LEADER_LEASE_DURATION_SECS",
            DEFAULT_LEASE_DURATION_SECS,
        );
        let renew_deadline_secs = parse_env_or(
            "SANDBAWKS_LEADER_RENEW_DEADLINE_SECS",
            DEFAULT_RENEW_DEADLINE_SECS,
        );
        let retry_period_secs = parse_env_or(
            "SANDBAWKS_LEADER_RETRY_PERIOD_SECS",
            DEFAULT_RETRY_PERIOD_SECS,
        );

        Self::validate(lease_duration_secs, renew_deadline_secs, retry_period_secs)?;

        Ok(Some(Self {
            lease_name,
            holder_identity,
            lease_duration_secs,
            renew_deadline_secs,
            retry_period: Duration::from_secs(retry_period_secs),
        }))
    }

    /// Validate the leader-election timing invariant and fail fast on a
    /// misconfiguration that would otherwise silently break renewal.
    ///
    /// Enforces `0 < retry_period < renew_deadline < lease_duration`:
    /// - a non-positive `lease_duration` produces a Lease that is always
    ///   expired, letting standbys steal it continuously;
    /// - `retry_period >= renew_deadline` means the leader can miss its renewal
    ///   window between successive attempts;
    /// - `renew_deadline >= lease_duration` means the Lease can expire at the
    ///   apiserver before the leader has stopped trying to renew it.
    ///
    /// Extracted from [`from_env`](Self::from_env) so it can be unit-tested
    /// without environment-variable manipulation.
    ///
    /// # Errors
    ///
    /// Returns an error describing the first violated constraint.
    pub fn validate(
        lease_duration_secs: i32,
        renew_deadline_secs: u64,
        retry_period_secs: u64,
    ) -> Result<()> {
        if lease_duration_secs <= 0 {
            bail!("leader election: leaseDurationSeconds must be > 0, got {lease_duration_secs}");
        }
        if retry_period_secs >= renew_deadline_secs {
            bail!(
                "leader election: retryPeriodSeconds ({retry_period_secs}) must be < \
                 renewDeadlineSeconds ({renew_deadline_secs})"
            );
        }
        // `lease_duration_secs > 0` is established above, so the `as u64` cast
        // is lossless here.
        if renew_deadline_secs >= lease_duration_secs as u64 {
            bail!(
                "leader election: renewDeadlineSeconds ({renew_deadline_secs}) must be < \
                 leaseDurationSeconds ({lease_duration_secs})"
            );
        }
        Ok(())
    }

    /// Name of the Lease object this controller competes for.
    pub fn lease_name(&self) -> &str {
        &self.lease_name
    }

    /// This replica's unique holder identity (the Pod name in-cluster).
    pub fn holder_identity(&self) -> &str {
        &self.holder_identity
    }

    /// Upper bound on a single acquire/renew call — also the total
    /// since-last-success budget the renewal task spends before exiting.
    pub fn renew_deadline(&self) -> Duration {
        Duration::from_secs(self.renew_deadline_secs)
    }

    /// Interval between acquire/renew attempts.
    pub fn retry_period(&self) -> Duration {
        self.retry_period
    }
}

/// Parse an environment variable into `T`, falling back to `default` when the
/// variable is unset or unparseable.
///
/// Generic over any `FromStr` type so the i32/u64 timing values share one
/// helper (Rust monomorphizes a copy per `T` — no runtime dispatch).
fn parse_env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Best-effort local hostname for the off-cluster holder-identity fallback.
///
/// Reads `/etc/hostname` (Linux) rather than taking a `hostname`-crate
/// dependency; this path is only reached in dev where `POD_NAME` is unset, so
/// the cost of a missing file (returning `"unknown-host"`) never affects an
/// in-cluster run, where the downward-API Pod name always wins.
fn local_hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown-host".to_string())
}

/// The action [`decide`] recommends for the observed Lease state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseDecision {
    /// No Lease object exists — CREATE a fresh one (transitions = 0).
    Create,
    /// We already hold the Lease — renew it (PUT, preserving its `acquireTime`
    /// and transition counter). Returned whenever we are the recorded holder,
    /// even if the Lease has expired: reclaiming our own Lease is not a
    /// leadership transition.
    Renew,
    /// Another identity holds the Lease but it has expired (or carries no
    /// `renewTime`) — PUT a takeover: new `acquireTime` epoch, transitions + 1.
    Takeover,
    /// Another identity holds a still-valid Lease — do nothing and stand by.
    Standby,
}

/// Decide what to do about a Lease, given the current time, the observed Lease
/// spec (if any), and this replica's identity. Pure: no I/O, no clock read —
/// the caller passes `now` so this is fully deterministic and unit-testable.
///
/// Expiry is judged by comparing `now` against the Lease's recorded `renewTime`
/// (written by whichever replica last held it, likely on another node) plus the
/// Lease duration. **This depends on loose clock agreement between nodes:**
/// there is deliberately no skew buffer, so a replica whose clock runs fast can
/// preempt a leader whose renewals are on time, and one running slow waits
/// longer than the nominal TTL before taking over. This is the same simple
/// model upstream kube-controller-manager uses. A buffer is intentionally NOT
/// added here: biasing toward *not* stealing on close calls trades faster
/// split-brain resolution for slower takeover, and the fail-fast renewal task
/// already bounds how long a deposed leader keeps working. Do not "tighten"
/// this comparison without accounting for cross-node clock skew.
pub fn decide(
    now: DateTime<Utc>,
    observed: Option<&LeaseSpec>,
    self_identity: &str,
) -> LeaseDecision {
    let Some(spec) = observed else {
        return LeaseDecision::Create;
    };
    let holder = spec.holder_identity.as_deref().unwrap_or("");
    if holder == self_identity {
        // We are the recorded holder — renew regardless of expiry. Reclaiming
        // our own Lease preserves the acquisition epoch and is not a transition.
        return LeaseDecision::Renew;
    }
    let duration_secs = spec
        .lease_duration_seconds
        .unwrap_or(DEFAULT_LEASE_DURATION_SECS) as i64;
    let expired = spec
        .renew_time
        .as_ref()
        .map(|rt| now >= rt.0 + chrono::Duration::seconds(duration_secs))
        // No renewTime recorded → treat as expired so a malformed Lease can't
        // permanently block a fresh cluster.
        .unwrap_or(true);
    if expired {
        LeaseDecision::Takeover
    } else {
        LeaseDecision::Standby
    }
}

/// Result of one [`try_acquire_or_renew`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// This replica holds the Lease after the call (created it, renewed it, or
    /// took over an expired one).
    Held,
    /// Another replica holds a valid Lease ([`decide`] returned
    /// [`LeaseDecision::Standby`]). For the renewal task this is a *confirmed*
    /// loss of leadership.
    Standby,
    /// Optimistic-concurrency conflict: the CREATE or PUT returned HTTP 409
    /// because the Lease changed between our GET and our write. This is
    /// ambiguous — it fires both when another replica legitimately took the
    /// Lease and on a benign third-party write (`kubectl edit`, an admission
    /// webhook stamping an annotation). Callers should re-read before treating
    /// it as a definitive loss.
    Conflict,
}

/// Build a `Lease` object from the given fields.
fn make_lease(
    config: &LeaderElectionConfig,
    acquire_time: DateTime<Utc>,
    transitions: i32,
    renew_time: DateTime<Utc>,
    resource_version: Option<String>,
) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(config.lease_name.clone()),
            resource_version,
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(config.holder_identity.clone()),
            lease_duration_seconds: Some(config.lease_duration_secs),
            acquire_time: Some(MicroTime(acquire_time)),
            renew_time: Some(MicroTime(renew_time)),
            lease_transitions: Some(transitions),
            // k8s 1.31 added preferred_holder + strategy for coordinated leader
            // election; we use the default single-holder (non-coordinated)
            // model so both stay unset.
            preferred_holder: None,
            strategy: None,
        }),
    }
}

/// Attempt to acquire or renew the Lease in one GET-then-write round trip.
///
/// Reads the current Lease, asks [`decide`] what to do, and performs the single
/// corresponding kube API call. Uses optimistic concurrency (`resourceVersion`)
/// on the PUT/CREATE: if two replicas race on an expired (or absent) Lease,
/// only the first write reaches the apiserver and the loser gets
/// [`LeaseOutcome::Conflict`].
///
/// # Errors
///
/// Returns an error only for transient/unexpected API failures (network, 5xx).
/// A 409 is mapped to [`LeaseOutcome::Conflict`], not an error, so callers can
/// distinguish "the Lease moved under me" from "the apiserver is unreachable".
pub async fn try_acquire_or_renew(
    api: &Api<Lease>,
    config: &LeaderElectionConfig,
) -> Result<LeaseOutcome> {
    let now = Utc::now();
    let existing = api.get_opt(&config.lease_name).await.context("GET lease")?;
    let decision = decide(
        now,
        existing.as_ref().and_then(|l| l.spec.as_ref()),
        &config.holder_identity,
    );

    match decision {
        // Another replica holds a valid Lease — nothing to write.
        LeaseDecision::Standby => Ok(LeaseOutcome::Standby),
        LeaseDecision::Create => {
            // Lease does not exist — race to create it. The loser's POST gets
            // HTTP 409 (AlreadyExists), surfaced as a Conflict.
            let lease = make_lease(config, now, 0, now, None);
            match api.create(&PostParams::default(), &lease).await {
                Ok(_) => Ok(LeaseOutcome::Held),
                Err(kube::Error::Api(ref e)) if e.code == 409 => Ok(LeaseOutcome::Conflict),
                Err(e) => Err(e).context("POST lease"),
            }
        }
        LeaseDecision::Renew | LeaseDecision::Takeover => {
            // `decide` only returns Create when the Lease is absent, so both
            // remaining variants imply `existing` is `Some`.
            let existing = existing.expect("Renew/Takeover imply an existing Lease");
            let spec = existing.spec.as_ref();
            let prev_transitions = spec.and_then(|s| s.lease_transitions).unwrap_or(0);
            let (acquire_time, transitions) = if decision == LeaseDecision::Takeover {
                // Taking an expired Lease from another holder: start a new
                // acquisition epoch and bump the transition counter.
                (now, prev_transitions + 1)
            } else {
                // Renewing our own Lease: preserve the original acquireTime and
                // the transition counter.
                let acquire_time = spec
                    .and_then(|s| s.acquire_time.as_ref())
                    .map(|mt| mt.0)
                    .unwrap_or(now);
                (acquire_time, prev_transitions)
            };

            // PUT with the current resourceVersion for optimistic locking. HTTP
            // 409 means the Lease changed between our GET and this PUT.
            let rv = existing.metadata.resource_version.clone();
            let new_lease = make_lease(config, acquire_time, transitions, now, rv);
            match api
                .replace(&config.lease_name, &PostParams::default(), &new_lease)
                .await
            {
                Ok(_) => Ok(LeaseOutcome::Held),
                Err(kube::Error::Api(ref e)) if e.code == 409 => Ok(LeaseOutcome::Conflict),
                Err(e) => Err(e).context("PUT lease"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// UTC timestamp at `secs` past the epoch — a fixed clock for the pure
    /// `decide` tests.
    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0)
            .single()
            .expect("valid timestamp")
    }

    /// Build a `LeaseSpec` with a single `renewTime`/`acquireTime` stamp.
    fn spec(holder: &str, renew_at: Option<DateTime<Utc>>, duration: Option<i32>) -> LeaseSpec {
        LeaseSpec {
            holder_identity: Some(holder.to_string()),
            lease_duration_seconds: duration,
            acquire_time: renew_at.map(MicroTime),
            renew_time: renew_at.map(MicroTime),
            lease_transitions: Some(0),
            preferred_holder: None,
            strategy: None,
        }
    }

    #[test]
    fn decide_no_lease_creates() {
        assert_eq!(decide(ts(100), None, "me"), LeaseDecision::Create);
    }

    #[test]
    fn decide_we_hold_valid_renews() {
        let s = spec("me", Some(ts(100)), Some(15));
        assert_eq!(decide(ts(105), Some(&s), "me"), LeaseDecision::Renew);
    }

    #[test]
    fn decide_we_hold_expired_still_renews() {
        // Our own Lease past its TTL: we reclaim it, which is not a transition.
        let s = spec("me", Some(ts(100)), Some(15));
        assert_eq!(decide(ts(200), Some(&s), "me"), LeaseDecision::Renew);
    }

    #[test]
    fn decide_other_holds_valid_stands_by() {
        let s = spec("other", Some(ts(100)), Some(15));
        assert_eq!(decide(ts(105), Some(&s), "me"), LeaseDecision::Standby);
    }

    #[test]
    fn decide_other_holds_expired_takes_over() {
        let s = spec("other", Some(ts(100)), Some(15));
        assert_eq!(decide(ts(116), Some(&s), "me"), LeaseDecision::Takeover);
    }

    #[test]
    fn decide_exact_expiry_boundary_takes_over() {
        // now == renewTime + duration → `>=` treats it as expired.
        let s = spec("other", Some(ts(100)), Some(15));
        assert_eq!(decide(ts(115), Some(&s), "me"), LeaseDecision::Takeover);
    }

    #[test]
    fn decide_just_before_expiry_stands_by() {
        let s = spec("other", Some(ts(100)), Some(15));
        assert_eq!(decide(ts(114), Some(&s), "me"), LeaseDecision::Standby);
    }

    #[test]
    fn decide_missing_renew_time_takes_over() {
        // No renewTime → treated as expired, so another holder is preemptible.
        let s = spec("other", None, Some(15));
        assert_eq!(decide(ts(100), Some(&s), "me"), LeaseDecision::Takeover);
    }

    #[test]
    fn decide_missing_duration_uses_default() {
        // No lease_duration_seconds → DEFAULT_LEASE_DURATION_SECS (15), so
        // expiry lands at 100 + 15 = 115.
        let s = spec("other", Some(ts(100)), None);
        assert_eq!(decide(ts(114), Some(&s), "me"), LeaseDecision::Standby);
        assert_eq!(decide(ts(115), Some(&s), "me"), LeaseDecision::Takeover);
    }

    #[test]
    fn decide_missing_holder_takes_over_when_expired() {
        // Empty holder never equals our identity; expiry then drives takeover.
        let mut s = spec("", Some(ts(100)), Some(15));
        s.holder_identity = None;
        assert_eq!(decide(ts(200), Some(&s), "me"), LeaseDecision::Takeover);
    }

    #[test]
    fn validate_accepts_default_ordering() {
        assert!(LeaderElectionConfig::validate(15, 10, 2).is_ok());
    }

    #[test]
    fn validate_rejects_nonpositive_duration() {
        assert!(LeaderElectionConfig::validate(0, 10, 2).is_err());
        assert!(LeaderElectionConfig::validate(-1, 10, 2).is_err());
    }

    #[test]
    fn validate_rejects_retry_ge_renew() {
        assert!(LeaderElectionConfig::validate(15, 10, 10).is_err());
        assert!(LeaderElectionConfig::validate(15, 10, 20).is_err());
    }

    #[test]
    fn validate_rejects_renew_ge_lease() {
        assert!(LeaderElectionConfig::validate(15, 15, 2).is_err());
        assert!(LeaderElectionConfig::validate(10, 15, 2).is_err());
    }
}
