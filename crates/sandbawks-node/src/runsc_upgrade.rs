//! Privileged, restart-bearing in-place runsc fleet upgrade (#550, Capability C
//! of the #545 epic).
//!
//! ## Why this is separate from the restart-free reconciler (#546)
//!
//! [`crate::runsc_reconcile`] converges the host `runsc` *binary* with a plain
//! `hostPath` write: containerd execs `runsc` fresh per sandbox, so a new binary
//! is picked up with no k3s restart and no `nsenter`. That is sufficient when
//! only the binary version drifts.
//!
//! Some convergence scenarios need more: re-running the full
//! `scripts/install-runsc-k3s.sh`, which **rewrites the containerd CRI handler
//! tables** (`config.toml.tmpl` + `runsc.toml`) and **restarts k3s/containerd** —
//! handler tables are only re-read on a k3s (re)start. That restart path is
//! host-root and it *kills running sandboxes on the node*, so it is deliberately
//! excluded from #546's restart-free DaemonSet step. This module is that
//! remaining, genuinely privileged slice.
//!
//! ## The three non-negotiable safety rails
//!
//! 1. **One node at a time.** The installer restarts k3s, which kills every
//!    sandbox on the node, so a *simultaneous* fleet-wide restart would take the
//!    whole sandbox fleet down at once. A single cluster-wide
//!    `coordination.k8s.io/v1` **Lease** ([`UpgradeCoordinator`]) serializes the
//!    upgrade: a node must hold the lease before it may run the installer, and at
//!    most one node holds it at a time. The lease has a generous TTL so that a
//!    node which bricks mid-upgrade only stalls the rollout for that TTL, never
//!    forever.
//! 2. **Drained-node only.** A node upgrades only when it hosts no live sandboxes
//!    (the [`crate::runsc_reconcile::LiveSandboxCounter`] gate, reused verbatim):
//!    restarting k3s under a live sandbox destroys it, and swapping runsc makes
//!    its current-version snapshot unrestorable here mid-rollout (this composes
//!    with the #545 Capability B restore-placement gate, which keeps restores off
//!    not-yet-upgraded nodes).
//! 3. **Survives the restart it triggers.** The installer restarts k3s/containerd,
//!    so it is launched **detached from the Pod** — `nsenter` into PID 1's
//!    namespaces, then `systemd-run` a transient unit owned by the host's systemd
//!    ([`NsenterUpgradeRunner`]) — and keeps running under host systemd across the
//!    containerd bounce regardless of what happens to the Pod.
//!
//!    Note (#558): on k3s the DaemonSet Pod is **not** killed by a
//!    `systemctl restart k3s-agent` — the embedded containerd + shims keep the
//!    container running and the kubelet just reconnects. So there is no "new Pod"
//!    to release the lease on convergence; release falls to the *surviving* Pod's
//!    loop, whose post-restart apiserver call can wedge and pin the lease until its
//!    TTL. To keep the rollout from crawling one TTL per node, the lease is
//!    therefore takeable by **any** node once the current holder's
//!    `sandbawks.dev/runsc-version` label shows it has converged (see
//!    [`decide_lock`]'s `holder_converged`) — the holder's own
//!    `AlreadyConverged → release` is kept as the fast path when it works, with
//!    convergence-takeover and finally the TTL as the backstops.
//!
//!    **Concurrency bound of the takeover.** The label is stamped by the #546
//!    reconciler from the host runsc *binary* version, and `install-runsc-k3s.sh`
//!    installs the binary *before* its k3s restarts (it then does up to three
//!    `restart_k3s` passes, each polling `crictl` for up to 30 s). So a #546 tick
//!    can flip the label while the holder is still bouncing k3s, and a taker can
//!    then start its own install concurrently — the takeover does **not** guarantee
//!    zero overlap. The overlap is bounded by the holder's residual restart
//!    sequence (tens of seconds) and to **two** nodes, never the fleet-wide
//!    simultaneous restart this rail actually prevents. It is safe because both the
//!    holder and the taker pass rail 2's drained-node gate before acquiring, so
//!    neither has a live sandbox a concurrent restart could disrupt. Eliminating
//!    the overlap entirely would need a "holder fully done" signal stronger than
//!    the binary-version label (deferred — see #558 options 2-4).
//!
//! ## Opt-in
//!
//! Off by default (`node.runscAutoUpgrade=false`); acts only on a version change.
//! It is mutually exclusive with #546's restart-free `runscAutoReconcile` (the
//! chart fails to render if both are set) — if you only need a binary bump, use
//! the restart-free path; reach for this only when the handler tables must be
//! rewritten.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use sandbawks_types::RUNSC_VERSION_LABEL;

use crate::runsc_reconcile::{release_token, LiveSandboxCounter, RunscVersionReader};
use crate::runsc_upgrade_metrics::{record_upgrade_decision, record_upgrade_error};

/// Default interval between [`run_runsc_upgrader`] ticks.
///
/// Like the #546 reconciler, runsc drift is a slow, host-`apt`-timer-driven
/// event, so a 5-minute cadence is ample. The interval also bounds how quickly a
/// node notices it now holds the fleet lease and may proceed once the previous
/// node finished — short enough that a fleet rollout isn't glacial, long enough
/// that the apiserver isn't polled needlessly.
pub const DEFAULT_UPGRADE_INTERVAL: Duration = Duration::from_secs(300);

/// Default name of the single cluster-wide upgrade-serialization Lease.
pub const DEFAULT_UPGRADE_LEASE_NAME: &str = "sandbawks-runsc-upgrade";

/// Default upgrade-lease TTL (seconds).
///
/// This is the **maximum time the whole rollout stalls if a node bricks
/// mid-upgrade** (it acquired the lease, triggered the restart, and never came
/// back to release it). It must comfortably exceed one installer run — which does
/// a couple of k3s restarts plus a `crictl info` readiness poll — so a node that
/// is merely *slow* is never preempted while its installer is still working.
/// 10 minutes is generous for a sub-minute installer while keeping a dead node's
/// drag on the rollout bounded.
pub const DEFAULT_UPGRADE_LEASE_DURATION_SECS: i32 = 600;

/// Resolved configuration for [`run_runsc_upgrader`]. The host-exec mechanics
/// live on [`NsenterUpgradeRunner`]; this struct carries only the decision inputs
/// so the tick logic stays testable without any host or apiserver.
#[derive(Debug, Clone)]
pub struct RunscUpgradeConfig {
    /// This node's name — the lease holder identity and the log/metric subject.
    pub node_name: String,
    /// In-container path to the **host** runsc binary (the `/host/usr/local/bin`
    /// mount shared with the #546 reconciler). Read to decide whether the host is
    /// already at the pinned version; never written by this path.
    pub host_runsc_path: PathBuf,
    /// Pinned runsc version from `node.runscVersion` (e.g. `20260525.0`); the
    /// single source of truth per #545. The same value the #546 reconciler reads.
    pub desired_version: String,
    /// Opt-in gate (`node.runscAutoUpgrade`, default off). When false the tick
    /// only ever reports/cleans up; it never runs the installer.
    pub auto_upgrade: bool,
    /// Gap between ticks; see [`DEFAULT_UPGRADE_INTERVAL`].
    pub interval: Duration,
    /// Name of the cluster-wide serialization Lease.
    pub lease_name: String,
    /// TTL stamped on the Lease; see [`DEFAULT_UPGRADE_LEASE_DURATION_SECS`].
    pub lease_duration_secs: i32,
}

/// Strip a leading `release-` from a version token, yielding the bare version the
/// installer's `--runsc-version` flag expects (it re-derives `release-<ver>`
/// itself). Idempotent on an already-bare value.
fn bare_version(version: &str) -> &str {
    version.strip_prefix("release-").unwrap_or(version)
}

// ---------------------------------------------------------------------------
// Decision logic (pure, no I/O)
// ---------------------------------------------------------------------------

/// The outcome of one [`upgrade_runsc_once`] tick.
///
/// A closed enum so the policy is exhaustively compiler-checked and the reason is
/// surfaced verbatim in logs + the `runsc_upgrade_total{decision}` metric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeDecision {
    /// `auto_upgrade` is off; the privileged path is never taken.
    Disabled,
    /// The host already runs the pinned version; nothing to do. If this node
    /// still held the lease (its own upgrade just completed and the Pod
    /// restarted), the tick released it so the next node may proceed.
    AlreadyConverged,
    /// The node hosts live sandboxes; the restart would destroy them, so the
    /// upgrade is deferred until the node drains.
    LiveSandboxes(usize),
    /// Another node holds the upgrade lease — stand by so the rollout stays
    /// strictly one-node-at-a-time.
    Contended,
    /// This node already holds the lease and its installer is still in flight
    /// (host not yet at the pinned version): do **not** relaunch it.
    InProgress,
    /// All rails passed and the lease was acquired: the detached host installer
    /// was launched.
    Launched,
}

impl UpgradeDecision {
    /// Stable, lowercase `decision` metric label.
    pub fn decision_label(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::AlreadyConverged => "already_converged",
            Self::LiveSandboxes(_) => "live_sandboxes",
            Self::Contended => "contended",
            Self::InProgress => "in_progress",
            Self::Launched => "launched",
        }
    }
}

/// What the lease state implies for this node, given the observed Lease spec, the
/// current time, this node's identity, the configured TTL, and whether the current
/// holder has already converged. Pure so the serialization policy is unit-testable
/// without an apiserver.
///
/// Mirrors the controller's leader-election `decide`, specialized to a mutex:
/// - no Lease, or one held by **another** node that has **expired** → acquire it
///   (a takeover bumps the transition counter so an operator can see churn);
/// - one held by **another** node that has already **converged** (#558) → acquire
///   it without waiting out the TTL — see [`decide_lock`]'s `holder_converged`;
/// - our **own** lease, expired (our prior attempt stalled past the TTL) →
///   re-acquire and relaunch — a failed installer auto-retries after the TTL
///   rather than wedging the node forever;
/// - our **own** lease, still valid → hold (installer in flight; do not relaunch);
/// - **another** node's still-valid, not-yet-converged lease → stand by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockAction {
    /// Write the lease to this node. `transitions` is the new transition counter.
    Acquire { transitions: i32 },
    /// We already hold a valid lease — no write, installer is in flight.
    Hold,
    /// Another node holds a valid lease — no write, stand by.
    Standby,
}

/// Decide the lock action for an observed Lease. See [`LockAction`].
///
/// `holder_converged` is whether the **current holder** (when it is another node)
/// has already reached the desired runsc version — observed from that node's
/// `sandbawks.dev/runsc-version` label. It exists because of #558: in k3s the
/// DaemonSet Pod **survives** the k3s-agent restart the installer triggers (the
/// embedded containerd keeps the container running), so the original "the *new*
/// Pod releases the lease" path never fires, and a holder whose post-restart
/// release wedges would otherwise pin the lease until its TTL — stalling the whole
/// rollout one TTL per node. Allowing any node to take over a holder that has
/// *already converged* (its label flipped to desired ⇒ its install is done) makes
/// the rollout advance at the label-update cadence instead, independent of whether
/// the disrupted holder ever releases. Pass `false` when this node is the holder,
/// when there is no holder, or when the holder's convergence is unknown (e.g. its
/// Node object is gone) — those cases fall back to the TTL/expiry path.
pub fn decide_lock(
    now: DateTime<Utc>,
    observed: Option<&LeaseSpec>,
    self_identity: &str,
    duration_secs: i32,
    holder_converged: bool,
) -> LockAction {
    let Some(spec) = observed else {
        return LockAction::Acquire { transitions: 0 };
    };
    let prev_transitions = spec.lease_transitions.unwrap_or(0);
    let holder = spec.holder_identity.as_deref().unwrap_or("");
    let ttl = spec.lease_duration_seconds.unwrap_or(duration_secs) as i64;
    // No renewTime recorded → treat as expired so a malformed lease can never
    // permanently block the rollout.
    let expired = spec
        .renew_time
        .as_ref()
        .map(|rt| now >= rt.0 + chrono::Duration::seconds(ttl))
        .unwrap_or(true);

    if holder == self_identity {
        if expired {
            // Reclaiming our own (stalled) lease is not a leadership transition.
            LockAction::Acquire {
                transitions: prev_transitions,
            }
        } else {
            LockAction::Hold
        }
    } else if expired || holder_converged {
        // Another node holds it, but it has either expired or already finished its
        // upgrade (#558) — take over and proceed; either is a leadership transition.
        LockAction::Acquire {
            transitions: prev_transitions + 1,
        }
    } else {
        LockAction::Standby
    }
}

/// Result of one [`UpgradeCoordinator::acquire`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockState {
    /// This node now holds the lease (freshly created/taken, or our own expired
    /// lease reclaimed) and should launch the installer.
    Acquired,
    /// This node already held a valid lease — its installer is still in flight.
    AlreadyHeld,
    /// Another node holds a valid lease — stand by.
    Contended,
}

/// Fleet-wide one-at-a-time serialization for the privileged upgrade.
///
/// Abstracted behind a trait so [`upgrade_runsc_once`] is testable with an
/// in-memory fake; production is [`KubeLeaseUpgradeCoordinator`].
#[async_trait]
pub trait UpgradeCoordinator: Send + Sync {
    /// Try to acquire the upgrade lease for this node. See [`LockState`].
    async fn acquire(&self) -> anyhow::Result<LockState>;
    /// Release the lease **iff this node holds it** (idempotent, best-effort) so
    /// the next node need not wait out the TTL. A no-op when another node (or
    /// nobody) holds it.
    async fn release(&self) -> anyhow::Result<()>;
}

/// Launches the host installer, detached so it survives this Pod's death.
///
/// Abstracted so the tick is testable without a host; production is
/// [`NsenterUpgradeRunner`].
///
/// ## Binary-source asymmetry vs the #546 restart-free path
///
/// Unlike [`crate::runsc_reconcile`], which atomically installs the
/// **image-bundled** runsc + shim (so the host-side and checkpoint-side runsc are
/// guaranteed the same build — the gVisor C/R version-match lockstep), this path
/// runs `install-runsc-k3s.sh`, which **re-downloads** runsc and the shim from the
/// network (`storage.googleapis.com` / `https://github.com/hypotheosis/sandbawks/releases`). Two consequences a
/// future reader must not assume away:
/// - the install requires host **network egress** to succeed mid-rollout; and
/// - runsc is version-pinned (`--runsc-version <desired>`, deterministic), but the
///   shim defaults to `--shim-version latest`, which **can drift between nodes**
///   during a rollout.
///
/// The bundled-binary lockstep the #546 docs emphasize therefore does **not** hold
/// for this install path; pin the shim version in the installer invocation if a
/// rollout needs a fixed shim across the fleet.
#[async_trait]
pub trait HostUpgradeRunner: Send + Sync {
    /// Launch `install-runsc-k3s.sh --runsc-version <bare_version>` on the host,
    /// detached from this Pod (it triggers the k3s restart that kills the Pod).
    /// Returns once the installer has been *started* on the host; the install
    /// itself proceeds in the background under the host's init system. See the
    /// trait docs for why this re-downloads runsc rather than using the bundled
    /// binary the #546 path installs.
    async fn launch(&self, bare_version: &str) -> anyhow::Result<()>;
}

/// Run one upgrade tick. See the module docs for the full safety model.
///
/// # Errors
///
/// Propagates the host-version read, live-sandbox count, lease acquire/release,
/// and installer-launch failures; each bumps `runsc_upgrade_errors_total{step}`
/// before propagating so the failure mode is alertable from the metric.
#[tracing::instrument(
    skip_all,
    fields(node = %cfg.node_name, auto_upgrade = cfg.auto_upgrade)
)]
pub async fn upgrade_runsc_once(
    cfg: &RunscUpgradeConfig,
    reader: &dyn RunscVersionReader,
    counter: &dyn LiveSandboxCounter,
    coordinator: &dyn UpgradeCoordinator,
    runner: &dyn HostUpgradeRunner,
) -> anyhow::Result<UpgradeDecision> {
    let desired = release_token(&cfg.desired_version);
    let host = reader
        .read_release(&cfg.host_runsc_path)
        .await
        .inspect_err(|_| record_upgrade_error("version_read"))?;

    // Steady state: host already at the pinned version. Release the lease if we
    // still hold it — the common case is our own upgrade having completed and our
    // Pod restarted, so handing off promptly keeps the rollout moving without the
    // next node waiting out the TTL.
    if host.as_deref() == Some(desired.as_str()) {
        coordinator
            .release()
            .await
            .inspect_err(|_| record_upgrade_error("lease_release"))?;
        return Ok(finish(UpgradeDecision::AlreadyConverged));
    }

    if !cfg.auto_upgrade {
        return Ok(finish(UpgradeDecision::Disabled));
    }

    // Drained-node gate: a restart under a live sandbox destroys it.
    let live = counter
        .count_live()
        .await
        .inspect_err(|_| record_upgrade_error("count_live"))?;
    if live > 0 {
        return Ok(finish(UpgradeDecision::LiveSandboxes(live)));
    }

    // One-node-at-a-time gate.
    let decision = match coordinator
        .acquire()
        .await
        .inspect_err(|_| record_upgrade_error("lease_acquire"))?
    {
        LockState::Contended => UpgradeDecision::Contended,
        LockState::AlreadyHeld => UpgradeDecision::InProgress,
        LockState::Acquired => {
            runner
                .launch(bare_version(&cfg.desired_version))
                .await
                .inspect_err(|_| record_upgrade_error("launch"))?;
            UpgradeDecision::Launched
        }
    };
    Ok(finish(decision))
}

/// Record the decision metric and return the decision (one call site per return).
fn finish(decision: UpgradeDecision) -> UpgradeDecision {
    record_upgrade_decision(decision.decision_label());
    decision
}

/// Periodically run the privileged upgrade for the life of the process.
///
/// `tokio::spawn`ed and supervised alongside the node's other long-lived loops;
/// it never returns under normal operation. A failing tick is logged and retried
/// next interval rather than crashing the DaemonSet — a transient apiserver blip
/// must not take down pause/resume.
///
/// # Panics
///
/// `cfg.interval` must be non-zero (`tokio::time::interval(Duration::ZERO)`
/// panics); the binary's CLI rejects a zero interval at parse time.
pub async fn run_runsc_upgrader(
    cfg: RunscUpgradeConfig,
    reader: Arc<dyn RunscVersionReader>,
    counter: Arc<dyn LiveSandboxCounter>,
    coordinator: Arc<dyn UpgradeCoordinator>,
    runner: Arc<dyn HostUpgradeRunner>,
) {
    tracing::info!(
        node = %cfg.node_name,
        desired_version = %cfg.desired_version,
        auto_upgrade = cfg.auto_upgrade,
        lease = %cfg.lease_name,
        lease_ttl_secs = cfg.lease_duration_secs,
        interval_secs = cfg.interval.as_secs(),
        "privileged runsc fleet-upgrade reconciler started (#550)"
    );
    let mut ticker = tokio::time::interval(cfg.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match upgrade_runsc_once(
            &cfg,
            reader.as_ref(),
            counter.as_ref(),
            coordinator.as_ref(),
            runner.as_ref(),
        )
        .await
        {
            Ok(decision) => log_decision(&cfg, &decision),
            Err(err) => tracing::warn!(
                node = %cfg.node_name,
                error = %err,
                "runsc fleet-upgrade tick failed; retrying next interval (#550)"
            ),
        }
    }
}

/// Per-tick logging tuned so a steady-state node is near-silent and the
/// actionable events (a launched upgrade, a deferral, a contended rollout) stand
/// out.
fn log_decision(cfg: &RunscUpgradeConfig, decision: &UpgradeDecision) {
    match decision {
        UpgradeDecision::Launched => tracing::warn!(
            node = %cfg.node_name, version = %cfg.desired_version,
            "launched detached host runsc installer (k3s restart-bearing); this \
             node's sandboxes are drained and it holds the fleet upgrade lease (#550)"
        ),
        UpgradeDecision::LiveSandboxes(n) => tracing::info!(
            node = %cfg.node_name, live_sandboxes = n,
            "deferring privileged runsc upgrade: node has live sandboxes a k3s \
             restart would destroy (#550)"
        ),
        UpgradeDecision::Contended => tracing::info!(
            node = %cfg.node_name, lease = %cfg.lease_name,
            "deferring privileged runsc upgrade: another node holds the fleet \
             upgrade lease (one node at a time) (#550)"
        ),
        UpgradeDecision::InProgress => tracing::info!(
            node = %cfg.node_name,
            "privileged runsc upgrade in flight on this node; awaiting host \
             convergence (#550)"
        ),
        // Disabled / AlreadyConverged are steady-state; nothing to log.
        UpgradeDecision::Disabled | UpgradeDecision::AlreadyConverged => {}
    }
}

// ---------------------------------------------------------------------------
// Production UpgradeCoordinator — coordination.k8s.io/v1 Lease
// ---------------------------------------------------------------------------

/// Serializes the fleet upgrade via a single namespaced
/// `coordination.k8s.io/v1` Lease, holder identity = node name.
pub struct KubeLeaseUpgradeCoordinator {
    leases: Api<Lease>,
    /// Cluster-scoped Node reader, used to check whether the current lease holder
    /// has already converged (its `sandbawks.dev/runsc-version` label) so a
    /// converged-but-not-released holder can be taken over (#558).
    nodes: Api<Node>,
    lease_name: String,
    node_name: String,
    duration_secs: i32,
    /// The desired `release-<ver>` token a holder's Node label must equal to count
    /// as converged. Stored in canonical form so the comparison can't drift from
    /// what the #546 reconciler stamps.
    desired_release: String,
}

impl KubeLeaseUpgradeCoordinator {
    /// Build a coordinator over the given namespace (where the node's Pod and the
    /// SnapshotJob CRs already live, so the Lease is co-located with them).
    /// `desired_version` is the raw `node.runscVersion` (bare or full token);
    /// it is canonicalised to the `release-<ver>` form the holder-convergence check
    /// compares against.
    pub fn new(
        client: Client,
        namespace: &str,
        lease_name: impl Into<String>,
        node_name: impl Into<String>,
        duration_secs: i32,
        desired_version: impl Into<String>,
    ) -> Self {
        Self {
            leases: Api::namespaced(client.clone(), namespace),
            nodes: Api::all(client),
            lease_name: lease_name.into(),
            node_name: node_name.into(),
            duration_secs,
            desired_release: release_token(&desired_version.into()),
        }
    }

    /// Whether the lease `holder` node has already reached the desired runsc
    /// version, read from its `sandbawks.dev/runsc-version` label (#558). A holder
    /// whose label equals the desired release has finished its install, so any node
    /// may take the lease over without waiting out the TTL. Returns `false` when the
    /// holder Node is gone or its label is absent/!=desired — those fall back to the
    /// TTL/expiry path (a churned-out holder is reclaimed on expiry, not here).
    async fn holder_converged(&self, holder: &str) -> anyhow::Result<bool> {
        let Some(node) = self.nodes.get_opt(holder).await? else {
            return Ok(false);
        };
        let label = node
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(RUNSC_VERSION_LABEL));
        Ok(label.map(String::as_str) == Some(self.desired_release.as_str()))
    }

    /// Build the Lease object this node would write to claim the lock.
    fn make_lease(
        &self,
        now: DateTime<Utc>,
        transitions: i32,
        resource_version: Option<String>,
    ) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(self.lease_name.clone()),
                resource_version,
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(self.node_name.clone()),
                lease_duration_seconds: Some(self.duration_secs),
                acquire_time: Some(MicroTime(now)),
                renew_time: Some(MicroTime(now)),
                lease_transitions: Some(transitions),
                preferred_holder: None,
                strategy: None,
            }),
        }
    }
}

#[async_trait]
impl UpgradeCoordinator for KubeLeaseUpgradeCoordinator {
    async fn acquire(&self) -> anyhow::Result<LockState> {
        let now = Utc::now();
        let existing = self.leases.get_opt(&self.lease_name).await?;
        // When another node holds the lease, check whether it has already converged
        // (#558) so we can take over a wedged-but-done holder rather than wait out
        // its TTL. Only one extra GET, and only in the contended case.
        let holder = existing
            .as_ref()
            .and_then(|l| l.spec.as_ref())
            .and_then(|s| s.holder_identity.as_deref());
        let holder_converged = match holder {
            Some(h) if h != self.node_name => self.holder_converged(h).await?,
            _ => false,
        };
        let action = decide_lock(
            now,
            existing.as_ref().and_then(|l| l.spec.as_ref()),
            &self.node_name,
            self.duration_secs,
            holder_converged,
        );
        match action {
            LockAction::Standby => Ok(LockState::Contended),
            LockAction::Hold => Ok(LockState::AlreadyHeld),
            LockAction::Acquire { transitions } => {
                // Optimistic concurrency: a 409 means another node wrote the
                // Lease between our GET and our write — treat it as Contended
                // rather than an error so two racing nodes can't both proceed.
                let result = match existing {
                    None => {
                        let lease = self.make_lease(now, transitions, None);
                        self.leases.create(&PostParams::default(), &lease).await
                    }
                    Some(existing) => {
                        let rv = existing.metadata.resource_version.clone();
                        let lease = self.make_lease(now, transitions, rv);
                        self.leases
                            .replace(&self.lease_name, &PostParams::default(), &lease)
                            .await
                    }
                };
                match result {
                    Ok(_) => Ok(LockState::Acquired),
                    Err(kube::Error::Api(ref e)) if e.code == 409 => Ok(LockState::Contended),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    async fn release(&self) -> anyhow::Result<()> {
        let Some(existing) = self.leases.get_opt(&self.lease_name).await? else {
            return Ok(());
        };
        let holder = existing
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_deref());
        if holder != Some(self.node_name.as_str()) {
            // We don't hold it — never delete another node's lock.
            return Ok(());
        }
        match self
            .leases
            .delete(&self.lease_name, &DeleteParams::default())
            .await
        {
            Ok(_) => Ok(()),
            // Already gone (a racing TTL takeover deleted/replaced it): fine.
            Err(kube::Error::Api(ref e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// Production HostUpgradeRunner — nsenter + systemd-run
// ---------------------------------------------------------------------------

/// In-container paths the [`NsenterUpgradeRunner`] needs to stage + invoke the
/// host installer. Grouped so the runner constructor stays within a sane argument
/// count.
#[derive(Debug, Clone)]
pub struct HostInstallerPaths {
    /// In-container path to the installer script bundled in the node image
    /// (`scripts/install-runsc-k3s.sh`, baked at build time). The runner copies
    /// this onto the host before invoking it.
    pub bundled_installer: PathBuf,
    /// In-container path the host installer directory is mounted at; the runner
    /// writes the staged script here so it lands on the host filesystem.
    pub staging_dir: PathBuf,
    /// Host-namespace path the staged script resolves to (the host source of the
    /// `staging_dir` mount). What `systemd-run` actually executes after `nsenter`.
    pub host_exec_path: PathBuf,
}

/// Launches the host installer detached from this Pod via `nsenter` → PID 1
/// namespaces → `systemd-run`, so the install survives the Pod being killed by
/// the k3s restart it triggers (see the module-level safety rails).
pub struct NsenterUpgradeRunner {
    /// `nsenter` binary inside the container image (the node Dockerfile bundles
    /// one, e.g. via busybox). Configurable so an image that ships a real
    /// `util-linux` `nsenter` elsewhere can point at it.
    nsenter: PathBuf,
    paths: HostInstallerPaths,
    /// Transient systemd unit name for the host install (`--unit`). A fixed name
    /// keeps repeated/retried runs from piling up stale units; `--collect`
    /// garbage-collects a failed one before the next launch reuses the name.
    systemd_unit: String,
}

impl NsenterUpgradeRunner {
    /// Build the runner from the resolved in-container paths + unit name.
    pub fn new(
        nsenter: PathBuf,
        paths: HostInstallerPaths,
        systemd_unit: impl Into<String>,
    ) -> Self {
        Self {
            nsenter,
            paths,
            systemd_unit: systemd_unit.into(),
        }
    }
}

/// The file name component of the host exec path — the staged script must land
/// under `staging_dir` with the same name the host path resolves to.
fn installer_file_name(host_exec_path: &Path) -> std::io::Result<&std::ffi::OsStr> {
    host_exec_path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "host installer path {} has no file name",
                host_exec_path.display()
            ),
        )
    })
}

/// Copy the bundled installer onto the host (via the staging mount), mode 0755,
/// so the detached `systemd-run` can execute it from the host filesystem. Blocking
/// fs work; the caller runs it on a blocking thread. Free function (not a method)
/// so the owned [`HostInstallerPaths`] can move into the `spawn_blocking` closure
/// without borrowing `&self` across the await boundary.
fn stage_installer(paths: &HostInstallerPaths) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dst = paths
        .staging_dir
        .join(installer_file_name(&paths.host_exec_path)?);
    let bytes = std::fs::read(&paths.bundled_installer)?;
    std::fs::write(&dst, &bytes)?;
    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o755))?;
    Ok(())
}

#[async_trait]
impl HostUpgradeRunner for NsenterUpgradeRunner {
    async fn launch(&self, bare_version: &str) -> anyhow::Result<()> {
        // 1. Stage the bundled installer onto the host filesystem.
        let paths = self.paths.clone();
        tokio::task::spawn_blocking(move || stage_installer(&paths))
            .await
            .map_err(|join| anyhow::anyhow!("installer staging task panicked: {join}"))??;

        // 2. nsenter into PID 1's mount/uts/ipc/net/pid namespaces, then
        //    systemd-run a transient unit owned by the host's systemd — fully
        //    detached from this Pod's cgroup + process tree, so the k3s restart
        //    the installer triggers cannot kill the install along with the Pod.
        //    `--collect` GCs the unit afterward (and a prior failed run of the
        //    same name) so reruns don't accumulate stale units.
        let host_exec = self.paths.host_exec_path.to_string_lossy().into_owned();
        let output = tokio::process::Command::new(&self.nsenter)
            .args(["-t", "1", "-m", "-u", "-i", "-n", "-p", "--"])
            .arg("systemd-run")
            .arg("--collect")
            .arg(format!("--unit={}", self.systemd_unit))
            .arg(&host_exec)
            .arg("--runsc-version")
            .arg(bare_version)
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("spawning nsenter/systemd-run: {e}"))?;

        if !output.status.success() {
            anyhow::bail!(
                "host installer launch failed (status {}): stdout={:?} stderr={:?}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        tracing::info!(
            unit = %self.systemd_unit,
            host_exec = %host_exec,
            version = %bare_version,
            "started detached host runsc installer under host systemd (#550)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use chrono::TimeZone;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0)
            .single()
            .expect("valid timestamp")
    }

    fn lease_spec(holder: &str, renew_at: Option<DateTime<Utc>>, ttl: Option<i32>) -> LeaseSpec {
        LeaseSpec {
            holder_identity: Some(holder.to_owned()),
            lease_duration_seconds: ttl,
            acquire_time: renew_at.map(MicroTime),
            renew_time: renew_at.map(MicroTime),
            lease_transitions: Some(0),
            preferred_holder: None,
            strategy: None,
        }
    }

    #[test]
    fn bare_version_strips_release_prefix_idempotently() {
        assert_eq!(bare_version("20260525.0"), "20260525.0");
        assert_eq!(bare_version("release-20260525.0"), "20260525.0");
    }

    #[test]
    fn decide_lock_acquires_when_absent() {
        assert_eq!(
            decide_lock(ts(100), None, "node-a", 600, false),
            LockAction::Acquire { transitions: 0 }
        );
    }

    #[test]
    fn decide_lock_holds_our_valid_lease() {
        let s = lease_spec("node-a", Some(ts(100)), Some(600));
        assert_eq!(
            decide_lock(ts(200), Some(&s), "node-a", 600, false),
            LockAction::Hold
        );
    }

    #[test]
    fn decide_lock_reacquires_our_expired_lease_without_transition() {
        // Our own stalled lease past the TTL: relaunch, not a transition.
        let mut s = lease_spec("node-a", Some(ts(100)), Some(600));
        s.lease_transitions = Some(3);
        assert_eq!(
            decide_lock(ts(800), Some(&s), "node-a", 600, false),
            LockAction::Acquire { transitions: 3 }
        );
    }

    #[test]
    fn decide_lock_stands_by_for_another_nodes_valid_unconverged_lease() {
        let s = lease_spec("node-b", Some(ts(100)), Some(600));
        assert_eq!(
            decide_lock(ts(200), Some(&s), "node-a", 600, false),
            LockAction::Standby
        );
    }

    #[test]
    fn decide_lock_takes_over_another_nodes_expired_lease_and_bumps() {
        let mut s = lease_spec("node-b", Some(ts(100)), Some(600));
        s.lease_transitions = Some(2);
        assert_eq!(
            decide_lock(ts(701), Some(&s), "node-a", 600, false),
            LockAction::Acquire { transitions: 3 }
        );
    }

    #[test]
    fn decide_lock_takes_over_a_converged_holder_before_its_ttl_expires() {
        // #558: holder's lease is still VALID (not expired), but it has already
        // converged (holder_converged=true) → take over now rather than wait the
        // TTL; a takeover bumps the transition counter.
        let mut s = lease_spec("node-b", Some(ts(100)), Some(600));
        s.lease_transitions = Some(2);
        assert_eq!(
            decide_lock(ts(200), Some(&s), "node-a", 600, true),
            LockAction::Acquire { transitions: 3 }
        );
    }

    #[test]
    fn decide_lock_self_branch_ignores_converged_flag() {
        // holder_converged is only meaningful for *another* node; for our own valid
        // lease the self branch wins regardless of the flag (the coordinator passes
        // false for self, but lock the precedence so it can't regress).
        let s = lease_spec("node-a", Some(ts(100)), Some(600));
        assert_eq!(
            decide_lock(ts(200), Some(&s), "node-a", 600, true),
            LockAction::Hold
        );
    }

    #[test]
    fn decide_lock_missing_renew_time_is_expired() {
        let s = lease_spec("node-b", None, Some(600));
        assert_eq!(
            decide_lock(ts(100), Some(&s), "node-a", 600, false),
            LockAction::Acquire { transitions: 1 }
        );
    }

    #[test]
    fn decide_lock_missing_ttl_uses_configured_default() {
        let s = lease_spec("node-b", Some(ts(100)), None);
        // Configured TTL 600 → expiry at 700.
        assert_eq!(
            decide_lock(ts(699), Some(&s), "node-a", 600, false),
            LockAction::Standby
        );
        assert_eq!(
            decide_lock(ts(700), Some(&s), "node-a", 600, false),
            LockAction::Acquire { transitions: 1 }
        );
    }

    // --- Tick orchestration, with fakes ------------------------------------

    struct FakeReader(Option<String>);
    #[async_trait]
    impl RunscVersionReader for FakeReader {
        async fn read_release(&self, _path: &Path) -> anyhow::Result<Option<String>> {
            Ok(self.0.clone())
        }
    }

    struct FakeCounter(usize);
    #[async_trait]
    impl LiveSandboxCounter for FakeCounter {
        async fn count_live(&self) -> anyhow::Result<usize> {
            Ok(self.0)
        }
    }

    #[derive(Default)]
    struct FakeCoordinator {
        acquire_state: Option<LockState>,
        acquired: Mutex<bool>,
        released: Mutex<bool>,
    }
    #[async_trait]
    impl UpgradeCoordinator for FakeCoordinator {
        async fn acquire(&self) -> anyhow::Result<LockState> {
            *self.acquired.lock().unwrap() = true;
            Ok(self.acquire_state.expect("acquire not expected this tick"))
        }
        async fn release(&self) -> anyhow::Result<()> {
            *self.released.lock().unwrap() = true;
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeRunner {
        launched: Mutex<Option<String>>,
    }
    #[async_trait]
    impl HostUpgradeRunner for FakeRunner {
        async fn launch(&self, bare_version: &str) -> anyhow::Result<()> {
            *self.launched.lock().unwrap() = Some(bare_version.to_owned());
            Ok(())
        }
    }

    fn cfg(auto_upgrade: bool) -> RunscUpgradeConfig {
        RunscUpgradeConfig {
            node_name: "node-a".to_owned(),
            host_runsc_path: PathBuf::from("/host/usr/local/bin/runsc"),
            desired_version: "20260525.0".to_owned(),
            auto_upgrade,
            interval: Duration::from_secs(300),
            lease_name: DEFAULT_UPGRADE_LEASE_NAME.to_owned(),
            lease_duration_secs: DEFAULT_UPGRADE_LEASE_DURATION_SECS,
        }
    }

    #[tokio::test]
    async fn tick_already_converged_releases_lease_and_skips() {
        let reader = FakeReader(Some("release-20260525.0".to_owned()));
        let counter = FakeCounter(0);
        let coord = FakeCoordinator::default();
        let runner = FakeRunner::default();
        let decision = upgrade_runsc_once(&cfg(true), &reader, &counter, &coord, &runner)
            .await
            .unwrap();
        assert_eq!(decision, UpgradeDecision::AlreadyConverged);
        assert!(*coord.released.lock().unwrap(), "must release the lease");
        assert!(!*coord.acquired.lock().unwrap(), "must not try to acquire");
        assert!(runner.launched.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_disabled_does_not_acquire_or_launch() {
        let reader = FakeReader(Some("release-OLD".to_owned()));
        let coord = FakeCoordinator::default();
        let runner = FakeRunner::default();
        let decision = upgrade_runsc_once(&cfg(false), &reader, &FakeCounter(0), &coord, &runner)
            .await
            .unwrap();
        assert_eq!(decision, UpgradeDecision::Disabled);
        assert!(!*coord.acquired.lock().unwrap());
        assert!(runner.launched.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_defers_when_live_sandboxes_present() {
        let reader = FakeReader(Some("release-OLD".to_owned()));
        let coord = FakeCoordinator::default();
        let runner = FakeRunner::default();
        let decision = upgrade_runsc_once(&cfg(true), &reader, &FakeCounter(2), &coord, &runner)
            .await
            .unwrap();
        assert_eq!(decision, UpgradeDecision::LiveSandboxes(2));
        assert!(
            !*coord.acquired.lock().unwrap(),
            "drained gate precedes lease"
        );
        assert!(runner.launched.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_contended_does_not_launch() {
        let reader = FakeReader(Some("release-OLD".to_owned()));
        let coord = FakeCoordinator {
            acquire_state: Some(LockState::Contended),
            ..Default::default()
        };
        let runner = FakeRunner::default();
        let decision = upgrade_runsc_once(&cfg(true), &reader, &FakeCounter(0), &coord, &runner)
            .await
            .unwrap();
        assert_eq!(decision, UpgradeDecision::Contended);
        assert!(runner.launched.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_in_progress_when_already_held_does_not_relaunch() {
        let reader = FakeReader(Some("release-OLD".to_owned()));
        let coord = FakeCoordinator {
            acquire_state: Some(LockState::AlreadyHeld),
            ..Default::default()
        };
        let runner = FakeRunner::default();
        let decision = upgrade_runsc_once(&cfg(true), &reader, &FakeCounter(0), &coord, &runner)
            .await
            .unwrap();
        assert_eq!(decision, UpgradeDecision::InProgress);
        assert!(
            runner.launched.lock().unwrap().is_none(),
            "an in-flight install must not be relaunched"
        );
    }

    #[tokio::test]
    async fn tick_launches_when_drained_and_lease_acquired() {
        let reader = FakeReader(Some("release-OLD".to_owned()));
        let coord = FakeCoordinator {
            acquire_state: Some(LockState::Acquired),
            ..Default::default()
        };
        let runner = FakeRunner::default();
        let decision = upgrade_runsc_once(&cfg(true), &reader, &FakeCounter(0), &coord, &runner)
            .await
            .unwrap();
        assert_eq!(decision, UpgradeDecision::Launched);
        // The installer is handed the BARE version, not the release- token.
        assert_eq!(
            runner.launched.lock().unwrap().as_deref(),
            Some("20260525.0")
        );
    }

    #[tokio::test]
    async fn tick_absent_host_runsc_is_treated_as_needing_upgrade() {
        // A node with no host runsc at all (fresh/wiped) is not at the pinned
        // version, so a drained node with the lease proceeds to install.
        let reader = FakeReader(None);
        let coord = FakeCoordinator {
            acquire_state: Some(LockState::Acquired),
            ..Default::default()
        };
        let runner = FakeRunner::default();
        let decision = upgrade_runsc_once(&cfg(true), &reader, &FakeCounter(0), &coord, &runner)
            .await
            .unwrap();
        assert_eq!(decision, UpgradeDecision::Launched);
    }

    #[test]
    fn installer_file_name_extracts_basename() {
        let p = PathBuf::from("/opt/sandbawks/install-runsc-k3s.sh");
        assert_eq!(
            installer_file_name(&p).unwrap(),
            std::ffi::OsStr::new("install-runsc-k3s.sh")
        );
    }

    #[test]
    fn stage_installer_copies_bundled_script_executable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let bundled = dir.path().join("bundled.sh");
        std::fs::write(&bundled, b"#!/bin/sh\necho hi\n").expect("write bundled");
        let staging = dir.path().join("host-staging");
        std::fs::create_dir(&staging).expect("mkdir staging");

        let paths = HostInstallerPaths {
            bundled_installer: bundled,
            staging_dir: staging.clone(),
            host_exec_path: PathBuf::from("/opt/sandbawks/install-runsc-k3s.sh"),
        };
        stage_installer(&paths).expect("stage");

        let out = staging.join("install-runsc-k3s.sh");
        assert_eq!(
            std::fs::read_to_string(&out).expect("read staged"),
            "#!/bin/sh\necho hi\n"
        );
        let mode = std::fs::metadata(&out).expect("meta").permissions().mode();
        assert_eq!(mode & 0o777, 0o755);
    }
}
