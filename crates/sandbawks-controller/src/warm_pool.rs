//! `SandboxWarmPool` reconciler.
//!
//! Keeps a labelled fleet of `Running`, ready-to-claim `Sandbox`es between
//! `spec.minReady` and `spec.maxReady` so the gateway can hand one off
//! instantly (issue #332). Members are held `Running` — *not* paused — so
//! a claim is a control-plane reassignment (re-owner/relabel the live
//! member and return its `envd` address) with no pod creation or restore
//! on the claim path. All warm-specific logic lives here; the `Sandbox`
//! state machine ([`crate::state_machine`]) is untouched — the pool just
//! boots members and leaves them `Running`.
//!
//! The module is layered the same way the Sandbox reconcile loop is, so
//! the decision logic is unit-testable without a cluster:
//!
//! - [`PoolMembers::bucket`] / [`status_for`] / [`plan_pool`] /
//!   [`warm_sandbox`] — pure functions over plain data.
//! - [`WarmPoolStore`] — trait over the `kube::Api` calls the reconcile
//!   makes, so [`reconcile_pool`] runs against an in-memory
//!   [`FakeWarmPoolStore`] in tests.
//! - [`reconcile_pool`] — glue: observe members, plan create/trim, then
//!   write `status`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{DeleteParams, ListParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Resource, ResourceExt};
use sandbawks_types::warm_pool::{TEMPLATE_RV_ANNOTATION, WARM_POOL_LABEL};
use sandbawks_types::{
    NetworkPolicy, ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxTemplate,
    SandboxWarmPool, SandboxWarmPoolStatus,
};

use crate::reconcile::ControllerError;
use crate::store::StoreError;

/// Template-derived fields a warm `Sandbox` must carry so it behaves like
/// a cold-booted one.
///
/// `runtime_class_name` is load-bearing: the gateway's cold-boot path
/// resolves the template's `defaultRuntimeClass` (e.g. `gvisor`) onto
/// every `Sandbox`, and the warm path must do the same. A warm member is
/// held `Running` (never auto-paused in the pool), but the requester that
/// claims it can later pause/resume it, and that `runsc checkpoint` only
/// works if the Pod already runs under the template's runtime class.
/// Without it the member would run under the cluster's *default* runtime
/// (runc) and the requester's first pause would land it in `Failed`.
///
/// The template's `defaultTimeoutSeconds` is deliberately *not* carried
/// here: a warm member is minted with the "disabled" timeout sentinel
/// (`timeout_seconds: 0`) so it never auto-pauses while unclaimed (#380).
/// The configured idle timeout is stamped only when a requester claims the
/// member (see `apply_overrides` in `sandbawks-api`), anchored at claim
/// time rather than at the member's mint time.
///
/// `resources` carries the template's `defaultResources` (issue #395) so a
/// warm member's Pod is sized from the template rather than a hardcoded
/// `1` CPU / `1Gi` placeholder. `None` only when the template lookup failed
/// (the lenient miss path), in which case [`warm_sandbox`] falls back to
/// that placeholder so the member still carries a valid resource budget.
///
/// `template_resource_version` carries the template's live `resourceVersion`
/// (issue #397) so [`warm_sandbox`] can stamp [`TEMPLATE_RV_ANNOTATION`] on
/// each member and [`reconcile_pool`] can evict members minted against a
/// superseded template. `None` on a template-lookup miss — in which case the
/// reconcile leaves existing members untouched rather than evicting the whole
/// pool against an unknown version (the same lenient-miss posture as
/// `resources`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WarmSandboxDefaults {
    /// Resolved `defaultRuntimeClass` (`None` when the template sets none).
    pub runtime_class_name: Option<String>,
    /// Resolved `defaultResources` (`None` only on a template-lookup miss).
    pub resources: Option<ResourceRequests>,
    /// Template `resourceVersion` at resolve time (`None` on a lookup miss).
    pub template_resource_version: Option<String>,
    /// Resolved template-level egress baseline (issue #537), `None` when the
    /// template sets none. Stamped onto every minted member's
    /// `spec.networkPolicy` so warm-pool sandboxes inherit the template's
    /// workload egress just like cold-start sandboxes do — the warm-pool half
    /// of closing the "no surface wires `egressAllow` through" gap.
    pub network_policy: Option<NetworkPolicy>,
}

impl WarmSandboxDefaults {
    /// Extract the warm-relevant defaults from a resolved template.
    fn from_template(template: &SandboxTemplate) -> Self {
        let class = template.spec.default_runtime_class.trim();
        Self {
            runtime_class_name: (!class.is_empty()).then(|| class.to_owned()),
            resources: Some(template.spec.default_resources.clone()),
            template_resource_version: template.meta().resource_version.clone(),
            network_policy: template.spec.network_policy.clone(),
        }
    }
}

/// Hardcoded CPU / memory budget used only when a template lookup fails and
/// [`WarmSandboxDefaults::resources`] is therefore `None`. Mirrors the
/// gateway's cold-boot placeholder (`body_to_sandbox`) so a member minted
/// against a momentarily-missing template still carries a valid, schema-shaped
/// resource budget rather than wedging the pool.
fn placeholder_resources() -> ResourceRequests {
    ResourceRequests {
        cpu: "1".parse().expect("hardcoded cpu quantity is valid"),
        memory: "1Gi".parse().expect("hardcoded memory quantity is valid"),
        ephemeral_storage: None,
    }
}

/// How often a pool requeues itself absent a watch event. The warm-pool
/// `Controller` (Plan 08 A5) also watches its member `Sandbox`es, so a
/// member reaching `Running` requeues the pool immediately; this interval
/// is just the steady-state safety net (mirrors the Sandbox reconcile's
/// 30s heartbeat, shortened because top-up latency is user-visible).
const WARM_POOL_REQUEUE: Duration = Duration::from_secs(10);

/// Number of consecutive mint-failure reconciles that trip the circuit breaker
/// (issue #615).
///
/// A "mint failure" is a reconcile pass that reaps one or more
/// `Failed`/`Terminating` members — a member the pool minted that then folded
/// to a terminal phase. Counting *passes* rather than absolute member counts
/// (and resetting the moment a pass reaps none) means a single transient
/// pod-create blip cannot trip the breaker: it self-heals back to `minReady`
/// on the very next clean pass, preserving the #457 reap behaviour. Only a
/// *sustained* failure — every fresh member hitting the same permanent
/// admission-class error, the #615 incident shape — accumulates to the
/// threshold. Five mirrors the example in the issue (`5 consecutive mint
/// failures`).
const BREAKER_TRIP_THRESHOLD: u32 = 5;

/// Base backoff window for the breaker's capped exponential backoff: the
/// `n`-th trip suppresses minting for `min(BREAKER_BACKOFF_BASE * 2^n,
/// BREAKER_BACKOFF_CAP)` (issue #615).
///
/// Mirrors the resume/pause recovery backoff shape in `reconcile.rs`
/// (`RESUME_BACKOFF_BASE`), but anchored to the warm-pool's own cadence: the
/// example note in the issue is "pausing member top-up for 60s", so the base
/// is sized so the first non-trivial window lands near that order rather than
/// the 5s the Sandbox-level retry uses.
const BREAKER_BACKOFF_BASE: Duration = Duration::from_secs(30);

/// Ceiling for the breaker backoff so a long-stuck pool can't schedule its next
/// probe absurdly far out (issue #615). Mirrors `RESUME_BACKOFF_CAP`.
const BREAKER_BACKOFF_CAP: Duration = Duration::from_secs(900);

/// Capped exponential backoff for the breaker's `count`-th trip:
/// `min(BREAKER_BACKOFF_BASE * 2^count, BREAKER_BACKOFF_CAP)` (issue #615).
///
/// `count` is the number of trips so far in the current open episode, so the
/// window grows each time a probe re-trips. The `checked_shl`/`checked_mul`
/// fallbacks and the `min(..)` clamp guard against overflow defensively, the
/// same way [`crate::reconcile`]'s `resume_backoff` does.
fn breaker_backoff(count: u32) -> Duration {
    let factor = 1u32.checked_shl(count).unwrap_or(u32::MAX);
    BREAKER_BACKOFF_BASE
        .checked_mul(factor)
        .unwrap_or(BREAKER_BACKOFF_CAP)
        .min(BREAKER_BACKOFF_CAP)
}

/// Snapshot of a pool's observed members, bucketed once per reconcile so
/// the top-up/trim logic operates on a single consistent view.
#[derive(Debug, Default)]
pub struct PoolMembers {
    /// Members in `Running` — these are the ones actually ready for a
    /// claim (a claim reassigns a live member, so readiness *is* `Running`).
    pub ready: Vec<Sandbox>,
    /// Members still in flight toward `Running` (`Creating`/`Resuming`).
    /// Counted against the ceiling so the pool doesn't over-provision while
    /// sandboxes are still booting.
    pub pending: Vec<Sandbox>,
    /// Legacy `Paused`/`Pausing` members — only possible on a rolling
    /// deploy from the pre-#332 controller that auto-paused warm members
    /// (the new controller never pauses them). Reclaimed (deleted) each
    /// reconcile so they don't sit unclaimable while suppressing top-up;
    /// the pool then re-creates fresh `Running` members. In steady state
    /// this set is always empty.
    pub stale: Vec<Sandbox>,
    /// `Running` members whose stamped [`TEMPLATE_RV_ANNOTATION`] no longer
    /// matches the template's current `resourceVersion` — i.e. the template
    /// was edited after they were minted (issue #397). Evicted each reconcile
    /// so the requester never claims a sandbox built from a superseded spec;
    /// top-up then re-mints fresh members from the updated template. Members
    /// with no annotation (minted before #397) land here too. Empty whenever
    /// the current version is unknown (template-lookup miss) — see
    /// [`Self::bucket`].
    pub version_stale: Vec<Sandbox>,
    /// Members in a terminal `Failed`/`Terminating` phase (issue #457).
    /// Reaped (deleted) each reconcile: they keep the pool's
    /// `ownerReference`, but owner-GC only cascades when the *pool* itself is
    /// deleted, so a live pool never collects them. Without an explicit
    /// delete a transient pod-create failure (e.g. an admission-webhook
    /// outage) accretes `Failed` CRs without bound, since they count toward
    /// neither `ready` nor `pending` and so never suppress top-up. Deleting
    /// them caps the standing `Failed` population; top-up re-mints the freed
    /// slots on a later reconcile.
    pub failed: Vec<Sandbox>,
}

impl PoolMembers {
    /// Bucket a freshly-listed set of pool members by readiness.
    ///
    /// `Failed` / `Terminating` members go to [`Self::failed`] for reaping
    /// (issue #457). A failed warm sandbox is neither ready nor usefully
    /// in-flight, so it counts toward neither `status.ready` nor the ceiling
    /// — but it is *not* free: it keeps the pool's `ownerReference`, and
    /// owner-GC only cascades when the *pool* is deleted, so a live pool
    /// never collects it. The reconcile therefore deletes them explicitly;
    /// otherwise a transient pod-create failure accretes `Failed` CRs without
    /// bound (the pre-#457 bug).
    ///
    /// `Paused` / `Pausing` members go to [`Self::stale`] for reclamation:
    /// in the post-#332 design a warm member is never paused, so one in
    /// either phase is a leftover from a rolling deploy off the old
    /// auto-pausing controller. Crucially it is kept *out* of `pending`,
    /// which would otherwise count it toward the ceiling and wedge the pool
    /// below `min_ready` forever.
    ///
    /// `current_rv` is the template's live `resourceVersion` (issue #397).
    /// When it is `Some`, a `Running` member whose stamped
    /// [`TEMPLATE_RV_ANNOTATION`] differs — or is absent (minted before #397)
    /// — is routed to [`Self::version_stale`] instead of [`Self::ready`], so
    /// the eviction path re-mints it from the updated template. When it is
    /// `None` (the template lookup missed) the version is unknown, so every
    /// `Running` member stays `ready`: evicting against an unknown version
    /// would churn the whole pool on a transient miss, the same lenient
    /// posture the resource/runtime-class defaults take.
    pub fn bucket(all: Vec<Sandbox>, current_rv: Option<&str>) -> Self {
        let mut members = PoolMembers::default();
        for s in all {
            match s.status.as_ref().map(|st| st.phase) {
                Some(SandboxPhase::Running) => {
                    if Self::is_version_stale(&s, current_rv) {
                        members.version_stale.push(s);
                    } else {
                        members.ready.push(s);
                    }
                }
                Some(SandboxPhase::Paused) | Some(SandboxPhase::Pausing) => members.stale.push(s),
                Some(SandboxPhase::Failed) | Some(SandboxPhase::Terminating) => {
                    members.failed.push(s)
                }
                _ => members.pending.push(s),
            }
        }
        members
    }

    /// Whether a `Running` member's stamped template `resourceVersion` no
    /// longer matches `current_rv` (issue #397).
    ///
    /// `false` whenever `current_rv` is `None` (version unknown — never evict
    /// against a template-lookup miss). Otherwise a member is stale when its
    /// [`TEMPLATE_RV_ANNOTATION`] is absent (minted before the feature) or
    /// differs from `current_rv`.
    fn is_version_stale(member: &Sandbox, current_rv: Option<&str>) -> bool {
        let Some(current) = current_rv else {
            return false;
        };
        member
            .annotations()
            .get(TEMPLATE_RV_ANNOTATION)
            .map(String::as_str)
            != Some(current)
    }

    /// Total members observed this reconcile across every bucket — the true
    /// count of warm `Sandbox` CRs the pool currently owns. `max_ready`
    /// bounds only `ready`, so this is what `status.total` surfaces to make
    /// runaway CR growth visible (issue #457).
    pub fn total(&self) -> usize {
        self.ready.len()
            + self.pending.len()
            + self.stale.len()
            + self.version_stale.len()
            + self.failed.len()
    }
}

/// Compute the `status` the reconciler should write for the observed
/// members: `ready` is the `Running` count, `members` their names, plus the
/// `failed` and `total` counts so a pod-create failure storm is visible in
/// `status` rather than hidden behind a healthy-looking `ready` (issue #457).
///
/// The breaker fields (`consecutive_mint_failures`, `degraded`, …) are left at
/// their defaults here and overlaid by [`reconcile_pool`] from the
/// [`BreakerDecision`] — `status_for` describes only the observed-member view
/// so it stays a pure function of [`PoolMembers`] (issue #615).
pub fn status_for(members: &PoolMembers) -> SandboxWarmPoolStatus {
    SandboxWarmPoolStatus {
        ready: members.ready.len() as u32,
        failed: members.failed.len() as u32,
        total: members.total() as u32,
        members: members.ready.iter().map(ResourceExt::name_any).collect(),
        consecutive_mint_failures: 0,
        degraded: false,
        degraded_reason: None,
        backoff_until: None,
        backoff_count: 0,
    }
}

/// Extract the dominant failure message across a set of reaped
/// `Failed`/`Terminating` members for the breaker's trip Event (issue #615).
///
/// Reads each member's terminal `Failed` condition message (set by the Sandbox
/// reconcile's `push_failure_condition`), falling back to the stable
/// `status.failureReason` label, and picks the most frequent — the permanent
/// admission-class error every fresh member hits identically (the #615 incident
/// shape). `None` when no reaped member carries either signal.
pub fn dominant_failure_message(failed: &[Sandbox]) -> Option<String> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for s in failed {
        let Some(st) = s.status.as_ref() else {
            continue;
        };
        let msg = st
            .conditions
            .iter()
            .rev()
            .find(|c| c.kind == "Failed")
            .and_then(|c| c.message.clone())
            .or_else(|| st.failure_reason.clone());
        if let Some(msg) = msg {
            *counts.entry(msg).or_default() += 1;
        }
    }
    // Most frequent wins; `BTreeMap` iteration gives a deterministic tiebreak
    // (lexicographically smallest message) so the chosen reason is stable.
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .map(|(msg, _)| msg)
}

/// Create/delete deltas for a pool given its `spec` bounds + members.
///
/// Pure — returns the names to delete and the count to create so it is
/// exhaustively unit-testable without a cluster.
#[derive(Debug, PartialEq, Eq)]
pub struct PoolPlan {
    /// Number of fresh warm sandboxes to create this round.
    pub create: u32,
    /// Names of `Running` excess to trim, oldest-by-name first.
    pub delete: Vec<String>,
}

/// Decide the create/trim deltas for a pool.
///
/// Creates up to `min_ready`, counting in-flight members so a burst of
/// reconciles while sandboxes boot doesn't over-provision, and clamps the
/// create count to `max_ready` so a `min > max` misconfiguration can't
/// run away. Trims only *`Running`* excess above `max_ready` (never an
/// in-flight boot), oldest-first — by `creationTimestamp`, name-tiebroken
/// — so the longest-running members go first and the policy is
/// deterministic regardless of how the apiserver assigns `generateName`
/// suffixes.
pub fn plan_pool(min_ready: u32, max_ready: u32, members: &PoolMembers) -> PoolPlan {
    let warm = members.ready.len() as u32;
    let in_flight = members.pending.len() as u32;
    let total = warm + in_flight;
    // `saturating_sub` keeps the arithmetic safe when already at/over the
    // floor; the `.min(max - total)` clamp stops a `min_ready > max_ready`
    // misconfig from creating past the ceiling.
    let create = min_ready
        .saturating_sub(total)
        .min(max_ready.saturating_sub(total));
    let mut delete = Vec::new();
    if warm > max_ready {
        // Sort by (creationTimestamp, name): oldest first, name as a stable
        // tiebreak for the 1-second `creationTimestamp` granularity (and for
        // fixtures that set no timestamp). `creation_timestamp()` returns a
        // `Time(DateTime<Utc>)`; `None` sorts first (treated as oldest).
        let mut ordered: Vec<&Sandbox> = members.ready.iter().collect();
        ordered.sort_by(|a, b| {
            let key = |s: &Sandbox| (s.creation_timestamp().map(|t| t.0), s.name_any());
            key(a).cmp(&key(b))
        });
        delete = ordered
            .into_iter()
            .take((warm - max_ready) as usize)
            .map(ResourceExt::name_any)
            .collect();
    }
    PoolPlan { create, delete }
}

/// The mint-failure circuit breaker's decision for one reconcile pass
/// (issue #615).
///
/// Pure output of [`decide_breaker`]: the breaker fields to persist into
/// `status`, the create count the reconcile is actually allowed to mint this
/// pass (capped to `0` while tripped, `1` for a recovery probe, or the planned
/// count when closed), and at most one [`PoolEvent`] marking a trip or a close
/// so the transition is visible in `kubectl describe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakerDecision {
    /// Consecutive mint-failure pass count to persist (`status`).
    pub consecutive_mint_failures: u32,
    /// Whether the breaker is open (suppressing top-up) after this pass.
    pub degraded: bool,
    /// Human-readable cause when `degraded`, else `None`.
    pub degraded_reason: Option<String>,
    /// RFC 3339 instant the next probe mint is allowed, when `degraded`.
    pub backoff_until: Option<String>,
    /// Trip exponent driving the backoff window (`status`).
    pub backoff_count: u32,
    /// Create count the reconcile may actually mint this pass, after the
    /// breaker has gated [`PoolPlan::create`].
    pub allowed_create: u32,
    /// A trip (`WarmPoolBackoff`, Warning) or close (`WarmPoolRecovered`,
    /// Normal) Event to publish, or `None` when the breaker state is unchanged.
    pub event: Option<PoolEvent>,
}

/// Observed failure signal for one reconcile pass, fed to [`decide_breaker`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MintFailureObservation {
    /// `Failed`/`Terminating` members reaped this pass (#457). Non-zero means
    /// this pass is a mint-failure pass.
    pub reaped: usize,
    /// The dominant failure message across the reaped members — the
    /// `status.failureReason`/condition message the pool can read off a folded
    /// member — woven into the trip Event note so the backoff is diagnosable.
    /// `None` when no reaped member carried one.
    pub dominant_message: Option<String>,
}

/// Decide the breaker state for this reconcile pass (issue #615).
///
/// Pure and clock-injected (`now`) so the trip/probe/recover logic is
/// exhaustively unit-testable without a cluster, mirroring the
/// `resume_recovery_trigger`/`pause_recovery_trigger` shape in `reconcile.rs`.
///
/// State machine, given the prior breaker fields read off `status` and this
/// pass's [`MintFailureObservation`]:
///
/// - **Closed, failure observed.** Increment `consecutive_mint_failures`. If it
///   reaches [`BREAKER_TRIP_THRESHOLD`], **trip**: set `degraded`, stamp
///   `backoff_until = now + breaker_backoff(0)`, suppress minting
///   (`allowed_create = 0`), and emit a `WarmPoolBackoff` Warning carrying the
///   dominant failure message and the window. Otherwise stay closed and mint as
///   planned (so a transient single-mint blip self-heals — no regression).
/// - **Closed, no failure.** Reset `consecutive_mint_failures` to `0` and mint
///   as planned.
/// - **Open, still inside the window** (`now < backoff_until`). Mint zero; keep
///   the breaker open; no Event.
/// - **Open, window elapsed.** Allow a single **probe** mint (`allowed_create =
///   min(planned, 1)`):
///   - If this pass *also* reaped failures, the probe (from a prior pass)
///     failed → **re-trip**: bump `backoff_count`, extend the window, refresh
///     the reason, re-emit `WarmPoolBackoff`, and suppress this pass's mint.
///   - Otherwise treat the elapsed window as a recovery opportunity: emit the
///     probe. (The breaker only formally **closes** — `WarmPoolRecovered`,
///     Normal — once a subsequent pass observes no failures with the pool back
///     at/over `minReady`; see the `recovered` arm below.)
///
/// `planned_create` is [`PoolPlan::create`] (already clamped to the pool's
/// band). `below_floor` is whether the pool is still under `minReady` after the
/// observed members — used to decide whether a clean pass means the breaker can
/// fully close (pool healthy) versus merely probe again.
pub fn decide_breaker(
    status: Option<&SandboxWarmPoolStatus>,
    obs: &MintFailureObservation,
    planned_create: u32,
    below_floor: bool,
    now: DateTime<Utc>,
) -> BreakerDecision {
    let prior_failures = status.map_or(0, |s| s.consecutive_mint_failures);
    let prior_degraded = status.is_some_and(|s| s.degraded);
    let prior_backoff_count = status.map_or(0, |s| s.backoff_count);
    let backoff_until = status.and_then(|s| {
        s.backoff_until
            .as_deref()
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.with_timezone(&Utc))
    });
    let failure = obs.reaped > 0;

    // Closed breaker.
    if !prior_degraded {
        if !failure {
            // No failure this pass: reset the counter, mint as planned.
            return BreakerDecision {
                consecutive_mint_failures: 0,
                degraded: false,
                degraded_reason: None,
                backoff_until: None,
                backoff_count: 0,
                allowed_create: planned_create,
                event: None,
            };
        }
        let count = prior_failures + 1;
        if count >= BREAKER_TRIP_THRESHOLD {
            // Trip.
            let window = breaker_backoff(0);
            return trip(count, 0, obs, now, window);
        }
        // Below threshold: keep accumulating but still self-heal this pass.
        return BreakerDecision {
            consecutive_mint_failures: count,
            degraded: false,
            degraded_reason: None,
            backoff_until: None,
            backoff_count: 0,
            allowed_create: planned_create,
            event: None,
        };
    }

    // Open breaker. Are we still inside the suppression window?
    let window_elapsed = backoff_until.is_none_or(|until| now >= until);
    if !window_elapsed {
        // Hold: mint zero, keep state, no Event.
        return BreakerDecision {
            consecutive_mint_failures: prior_failures,
            degraded: true,
            degraded_reason: status.and_then(|s| s.degraded_reason.clone()),
            backoff_until: backoff_until.map(|d| d.to_rfc3339()),
            backoff_count: prior_backoff_count,
            allowed_create: 0,
            event: None,
        };
    }

    // Window elapsed.
    if failure {
        // A prior probe re-failed (this pass reaped). Re-trip with an extended
        // window. `backoff_count` grows so the window climbs the curve.
        let next_count = prior_backoff_count.saturating_add(1);
        let window = breaker_backoff(next_count);
        return trip(prior_failures + 1, next_count, obs, now, window);
    }

    if below_floor {
        // No failures observed but still under the floor: emit a single probe
        // mint and keep the breaker open (the probe's outcome decides the next
        // pass). Refresh the window so a probe that produces a member which
        // fails *later* still re-trips on a bounded cadence.
        let window = breaker_backoff(prior_backoff_count);
        let next_until = now
            .checked_add_signed(
                chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::seconds(0)),
            )
            .map(|d| d.to_rfc3339());
        return BreakerDecision {
            consecutive_mint_failures: prior_failures,
            degraded: true,
            degraded_reason: status.and_then(|s| s.degraded_reason.clone()),
            backoff_until: next_until,
            backoff_count: prior_backoff_count,
            allowed_create: planned_create.min(1),
            event: None,
        };
    }

    // Recovered: window elapsed, no failures, and the pool is at/over its floor
    // (the underlying config was fixed and a probe already refilled it). Close
    // the breaker and resume normal top-up.
    BreakerDecision {
        consecutive_mint_failures: 0,
        degraded: false,
        degraded_reason: None,
        backoff_until: None,
        backoff_count: 0,
        allowed_create: planned_create,
        event: Some(PoolEvent::normal(
            "WarmPoolRecovered",
            "mint-failure breaker closed; resuming normal member top-up".to_owned(),
        )),
    }
}

/// Build a `trip`/`re-trip` [`BreakerDecision`]: open the breaker, stamp the
/// window, suppress minting, and emit a `WarmPoolBackoff` Warning Event whose
/// note carries the dominant failure message and the backoff duration
/// (issue #615).
fn trip(
    consecutive: u32,
    backoff_count: u32,
    obs: &MintFailureObservation,
    now: DateTime<Utc>,
    window: Duration,
) -> BreakerDecision {
    let window_secs = window.as_secs();
    let backoff_until = now
        .checked_add_signed(
            chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::seconds(0)),
        )
        .map(|d| d.to_rfc3339());
    let reason = obs
        .dominant_message
        .clone()
        .unwrap_or_else(|| "permanent mint failure".to_owned());
    let note = format!(
        "pausing member top-up for {window_secs}s after {consecutive} consecutive mint \
         failures: {reason}"
    );
    BreakerDecision {
        consecutive_mint_failures: consecutive,
        degraded: true,
        degraded_reason: Some(reason),
        backoff_until,
        backoff_count,
        allowed_create: 0,
        event: Some(PoolEvent::warning("WarmPoolBackoff", note)),
    }
}

/// Max length of a `generateName` prefix the apiserver keeps intact.
///
/// The apiserver's name generator reserves 5 of a 63-char budget for the
/// random suffix it appends, and silently truncates any `generateName`
/// longer than `63 - 5 = 58` chars before generating. We cap our prefix at
/// the same ceiling so the truncation is predictable and the trailing `-`
/// survives (an apiserver-side cut could otherwise lop it off, yielding
/// `sbx-foosuffix` instead of `sbx-foo-suffix`).
const MAX_GENERATE_NAME_PREFIX: usize = 58;

/// Build the `sbx-<pool>-` `generateName` prefix for a warm member.
///
/// Folds `pool_name` into the prefix so an apiserver-assigned member name
/// (`sbx-<pool>-<suffix>`) self-identifies its owning pool in
/// `kubectl get sandboxes`, logs, and traces (#495). The pool segment is
/// truncated so the whole prefix stays within [`MAX_GENERATE_NAME_PREFIX`];
/// any hyphen exposed at the truncation boundary is trimmed so the name
/// never reads `sbx-foo--<suffix>` and the prefix always ends in a single
/// clean `-`.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(warm_generate_name("python-pool"), "sbx-python-pool-");
/// ```
fn warm_generate_name(pool_name: &str) -> String {
    // `sbx-` head + trailing `-` frame the pool segment; the rest is budget.
    const FRAME: usize = "sbx-".len() + "-".len();
    let budget = MAX_GENERATE_NAME_PREFIX - FRAME;
    // Truncate on a UTF-8 char boundary — K8s names are ASCII in practice,
    // but slicing a multibyte string by byte offset would panic. `nth(budget)`
    // yields the byte index just past the `budget`-th char, or `None` when the
    // name is already short enough.
    let pool_segment = match pool_name.char_indices().nth(budget) {
        Some((idx, _)) => pool_name[..idx].trim_end_matches('-'),
        None => pool_name,
    };
    format!("sbx-{pool_segment}-")
}

/// Build a fresh warm `Sandbox` owned by `pool`.
///
/// The sandbox carries [`WARM_POOL_LABEL`]`=<pool>` so the reconciler and
/// the claim path can find it, and an `ownerReference` to the pool so
/// deleting the pool garbage-collects its whole fleet. The name is left
/// to the apiserver via `metadata.generateName` (`sbx-<pool>-`, built by
/// [`warm_generate_name`]) — folding the pool name into the prefix lets an
/// assigned member name self-identify its owning pool at a glance (#495)
/// while still sidestepping client-side id collisions and keeping this
/// constructor a pure function (no clock). The [`WARM_POOL_LABEL`] remains
/// the source of truth for claim/trim matching; the prefix is cosmetic. `defaults` carries the template-resolved
/// `runtimeClassName` (so the warm Pod runs under the same runtime —
/// e.g. gVisor — the cold-boot path would have used, which a later
/// requester pause/resume depends on) and the template's `defaultResources`
/// (issue #395) so the member's Pod is sized from the template instead of a
/// hardcoded placeholder. A template-lookup miss leaves `defaults.resources`
/// `None`, in which case the cold-boot placeholder budget is used.
pub fn warm_sandbox(
    pool: &SandboxWarmPool,
    template_ref: &str,
    defaults: &WarmSandboxDefaults,
) -> Sandbox {
    let pool_name = pool.name_any();
    // Fold the pool name into the `generateName` prefix (#495). Built before
    // `pool_name` is moved into the `OwnerReference` below.
    let generate_name = warm_generate_name(&pool_name);
    let mut labels = BTreeMap::new();
    labels.insert(WARM_POOL_LABEL.to_owned(), pool_name.clone());

    // Stamp the template's `resourceVersion` (issue #397) so a later reconcile
    // can tell whether this member was minted against the current template or
    // a superseded one. `None` on a template-lookup miss — leave the member
    // unannotated rather than stamp a placeholder; an unannotated member reads
    // as stale only once the reconcile actually knows the current version
    // (see [`PoolMembers::is_version_stale`]).
    let annotations = defaults.template_resource_version.as_ref().map(|rv| {
        let mut a = BTreeMap::new();
        a.insert(TEMPLATE_RV_ANNOTATION.to_owned(), rv.clone());
        a
    });

    // Owner-reference back to the pool so `kubectl delete sbxwp <p>`
    // cascades to its warm sandboxes. `uid` is required on the wire; an
    // unset uid (only in unit fixtures) yields an empty string, which is
    // harmless because the fixture never round-trips through an apiserver.
    let owner = OwnerReference {
        api_version: SandboxWarmPool::api_version(&()).into_owned(),
        kind: SandboxWarmPool::kind(&()).into_owned(),
        name: pool_name,
        uid: pool.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    };

    let spec = SandboxSpec {
        template_ref: template_ref.to_owned(),
        // Load-bearing: a warm sandbox must run under the template's
        // runtime class (gVisor) or the auto-pause `runsc checkpoint`
        // fails — see [`WarmSandboxDefaults`].
        runtime_class_name: defaults.runtime_class_name.clone(),
        // Size from the template's `defaultResources` (#395); the hardcoded
        // placeholder is only reached when the template lookup missed.
        resources: defaults
            .resources
            .clone()
            .unwrap_or_else(placeholder_resources),
        // `0` is the "idle timeout disabled" sentinel (see
        // `reconcile::timeout_deadline`). Warm members are held `Running`
        // until claimed — minting them with the template's
        // `defaultTimeoutSeconds` would anchor an idle-timeout deadline at
        // *mint* time and auto-pause the whole pool a timeout after boot,
        // which is exactly the cold-start latency the pool exists to avoid
        // (#380). The real timeout is stamped at claim time, anchored to the
        // claim instant, by `apply_overrides` in `sandbawks-api`.
        timeout_seconds: 0,
        // `auto_pause: true` only matters *after* a claim: it is the default
        // idle-pause behaviour the claimed sandbox carries unless the
        // requester overrides it. Until then the disabled timeout above means
        // the reconciler never drives a warm member to `Paused`.
        auto_pause: true,
        desired_phase: None,
        env: BTreeMap::new(),
        metadata: BTreeMap::new(),
        // Inherit the template's workload egress baseline (issue #537). Before
        // this, warm members were minted with `None` and the *only* way to
        // widen a pool-backed sandbox's egress was hand-editing the claimed CR
        // — unusable for the auto-mint/auto-claim flow the pool exists for.
        network_policy: defaults.network_policy.clone(),
        snapshot_retention: None,
        pvc_retention: None,
        tolerations: None,
        node_selector: None,
        security_context: None,
        persistence_mode: None,
        volume_claim_templates: None,
        // A warm-pool member is always cold-booted fresh; clone-from-snapshot
        // (issue #416) is a per-create choice, never a pool default.
        restore_from: None,
    };

    Sandbox {
        metadata: ObjectMeta {
            generate_name: Some(generate_name),
            labels: Some(labels),
            annotations,
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec,
        status: None,
    }
}

/// A Kubernetes `Event` the warm-pool reconcile wants published against the
/// pool object (issue #591).
///
/// Mirrors the Sandbox reconcile's `emit_event` shape (`reconcile.rs`): a
/// `Normal`/`Warning` type, a stable machine `reason` (also reused as the
/// event `action`), and a human-readable `note`. The `reason` is `'static`
/// so `kubectl describe` shows a deduplicated, greppable set of reasons
/// rather than per-pass churn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolEvent {
    /// `Normal` for routine top-up/trim activity, `Warning` for reaps and
    /// template-lookup misses.
    pub warning: bool,
    /// Stable machine reason (e.g. `ReapedFailed`), reused as the event
    /// `action`. `'static` so the set stays small and deduplicated.
    pub reason: &'static str,
    /// Human-readable detail shown in `kubectl describe`.
    pub note: String,
}

impl PoolEvent {
    /// A `Normal` event with the given stable `reason` and detail `note`.
    fn normal(reason: &'static str, note: String) -> Self {
        Self {
            warning: false,
            reason,
            note,
        }
    }

    /// A `Warning` event with the given stable `reason` and detail `note`.
    fn warning(reason: &'static str, note: String) -> Self {
        Self {
            warning: true,
            reason,
            note,
        }
    }
}

/// The `kube::Api` calls [`reconcile_pool`] makes, abstracted so the loop
/// is unit-testable against [`FakeWarmPoolStore`]. Production wires
/// [`KubeWarmPoolStore`].
#[async_trait]
pub trait WarmPoolStore: Send + Sync {
    /// List the `Sandbox`es labelled for `pool_name` in `namespace`.
    async fn list_members(
        &self,
        namespace: &str,
        pool_name: &str,
    ) -> Result<Vec<Sandbox>, StoreError>;

    /// Create one warm `Sandbox` (carrying its `generateName`).
    async fn create_sandbox(&self, namespace: &str, sandbox: Sandbox) -> Result<(), StoreError>;

    /// Delete a warm `Sandbox` by name (background propagation).
    async fn delete_sandbox(&self, namespace: &str, name: &str) -> Result<(), StoreError>;

    /// Patch the pool's `status` subresource.
    async fn patch_pool_status(
        &self,
        namespace: &str,
        pool_name: &str,
        status: SandboxWarmPoolStatus,
    ) -> Result<(), StoreError>;

    /// Resolve the template-derived defaults a warm `Sandbox` must carry
    /// (`runtimeClassName`, timeout). A missing template resolves to
    /// [`WarmSandboxDefaults::default`] — the reconciler proceeds rather
    /// than wedging the pool, mirroring the gateway's lenient cold-boot
    /// template lookup.
    async fn template_defaults(
        &self,
        namespace: &str,
        template_ref: &str,
    ) -> Result<WarmSandboxDefaults, StoreError>;

    /// Publish a Kubernetes `Event` against the pool object (issue #591),
    /// best-effort.
    ///
    /// Modelled on the Sandbox reconcile's `emit_event` contract: this never
    /// returns an error — publish failures (a transient apiserver hiccup, an
    /// RBAC gap) are logged at `warn` and discarded so they cannot abort or
    /// requeue-error the reconcile. The `&()` return type is the structural
    /// proof of that. The default impl is a no-op so [`FakeWarmPoolStore`]
    /// (and any other test seam) gets it for free; [`KubeWarmPoolStore`]
    /// overrides it to actually publish via kube-runtime's `Recorder`.
    async fn record_event(&self, _pool: &SandboxWarmPool, _event: PoolEvent) {}
}

/// Reconcile one `SandboxWarmPool`: top-up toward `minReady`, trim above
/// `maxReady`, then write `status`. Members are held `Running` for an
/// instant claim — there is no auto-pause edge.
///
/// # Errors
///
/// Returns [`ControllerError::Invariant`] if the pool has no namespace
/// (cluster-scoped objects can't own namespaced sandboxes), or
/// [`ControllerError::Store`] on any backing `kube` failure.
// `otel.kind = "consumer"` + `messaging.*` render this as a "Task" in the APM
// (the controller consumes SandboxWarmPool watch events, not HTTP). See the
// note on `reconcile` in `reconcile.rs` for the rationale.
// `err` emits an OTel `exception` event on this Task span on `Err`, so warm-pool
// reconcile faults surface in the APM's Issues view.
#[tracing::instrument(
    skip_all,
    name = "reconcile:controller::warm_pool",
    err,
    fields(
        pool = %pool.name_any(),
        otel.kind = "consumer",
        messaging.system = "kubernetes",
        messaging.operation = "process",
        messaging.destination.name = "SandboxWarmPool",
    )
)]
pub async fn reconcile_pool(
    pool: &SandboxWarmPool,
    store: &dyn WarmPoolStore,
) -> Result<Action, ControllerError> {
    // Continue the trace recorded on the pool (by the api gateway's
    // create/update, or absent for an operator-applied pool) so warm-pool
    // upkeep joins that trace rather than rooting a fresh one (OTel Phase 5).
    sandbawks_observability::continue_from_annotations(pool.metadata.annotations.as_ref());
    let namespace = pool
        .namespace()
        .ok_or(ControllerError::Invariant("warm pool missing namespace"))?;
    let pool_name = pool.name_any();
    // A watch-observed object always carries an apiserver-stamped UID; we
    // need it for the warm sandboxes' ownerReference (without it kube GC
    // won't cascade and deleting the pool would orphan its fleet). Fail
    // the reconcile loudly rather than create orphan-prone sandboxes.
    if pool.uid().is_none() {
        return Err(ControllerError::Invariant("warm pool missing uid"));
    }

    // Resolve the template defaults once, up front and unconditionally
    // (issue #397). The template's current `resourceVersion` is needed to
    // bucket members into `version_stale`, so this can no longer be deferred
    // to the `plan.create > 0` branch; the same resolved `defaults` is reused
    // to mint any fresh members below, so this is one extra read at most.
    let defaults = store
        .template_defaults(&namespace, &pool.spec.template_ref)
        .await?;
    let current_rv = defaults.template_resource_version.as_deref();
    let members = PoolMembers::bucket(
        store.list_members(&namespace, &pool_name).await?,
        current_rv,
    );
    let plan = plan_pool(pool.spec.min_ready, pool.spec.max_ready, &members);

    // Circuit breaker (issue #615): decide whether a sustained mint-failure
    // storm should suppress this pass's top-up. The decision reads the breaker
    // state persisted on `pool.status` from prior passes (the warm-pool
    // reconcile is otherwise stateless), folds in this pass's reaped-member
    // signal, and returns the create count we're actually allowed to mint plus
    // any trip/recover Event. `Utc::now()` is read here (once) and the rest of
    // the breaker logic is the pure, clock-injected `decide_breaker`, mirroring
    // how `reconcile.rs` anchors its recovery backoffs.
    let now = Utc::now();
    let breaker = decide_breaker(
        pool.status.as_ref(),
        &MintFailureObservation {
            reaped: members.failed.len(),
            dominant_message: dominant_failure_message(&members.failed),
        },
        plan.create,
        (members.ready.len() as u32) < pool.spec.min_ready,
        now,
    );

    // Reconcile observed garbage *before* topping up. Every delete below is
    // cleanup of members the apiserver already shows us — ceiling trims,
    // legacy paused leftovers, version-stale evictions (#397), and terminal
    // `Failed`/`Terminating` reaps (#457). Running them ahead of the create
    // loop matters because that loop `?`-returns on the first create error:
    // if the active outage also rejects `Sandbox` *CR* creates (a webhook on
    // `Sandbox`, or apiserver pressure from the leak itself), topping up first
    // would bail before reaping and starve the leak fix. None of these deletes
    // changes `plan` (computed above from the observed buckets, all of which
    // already exclude the garbage from `total`), so the create count below is
    // identical regardless of ordering; the freed slots are filled by this
    // pass's top-up and the fresh members reach `Running` on the reconcile the
    // member watch refires.

    // Trim Running excess above the ceiling.
    for name in &plan.delete {
        store.delete_sandbox(&namespace, name).await?;
    }
    // Reclaim legacy `Paused`/`Pausing` members left by a rolling deploy off
    // the old auto-pausing controller (see `PoolMembers::stale`). Deleting
    // them frees the slot so top-up re-creates a fresh `Running` member,
    // letting the pool self-heal back to `min_ready` instead of wedging.
    for member in &members.stale {
        store.delete_sandbox(&namespace, &member.name_any()).await?;
    }
    // Evict members minted against a superseded template (issue #397): the
    // template was edited (new image / runtime class / resources) and these
    // members still carry the old `resourceVersion`. Deleting them frees the
    // slot so top-up re-mints fresh members from the updated template, so a
    // requester never claims a sandbox built from the stale spec.
    for member in &members.version_stale {
        store.delete_sandbox(&namespace, &member.name_any()).await?;
    }
    // Reap terminal `Failed`/`Terminating` members (issue #457). They keep the
    // pool's `ownerReference`, but owner-GC only cascades when the *pool* is
    // deleted, so a live pool never collects them — and because they count
    // toward neither `ready` nor `pending`, top-up keeps minting replacements,
    // so a transient pod-create failure (e.g. an admission-webhook outage)
    // would otherwise accrete `Failed` CRs without bound. Deleting every
    // observed one each pass caps the standing `Failed` population.
    for member in &members.failed {
        store.delete_sandbox(&namespace, &member.name_any()).await?;
    }

    // Emit Kubernetes Events for the work this pass actually did (issue #591),
    // so `kubectl describe sandboxwarmpool <name>` carries a trail explaining
    // why members churn — the reported gap was the silent `Failed` reap. Each
    // emit is aggregated (one Event per category, with a count) and gated on
    // a non-empty list, so an idle heartbeat reconcile emits nothing and a
    // busy one emits at most a handful of deduplicated reasons rather than one
    // Event per member. Publishing is best-effort (see `record_event`): a
    // failure is logged at `warn` and never aborts the reconcile.
    if !members.failed.is_empty() {
        // `Warning` — the reported symptom. A pool stuck minting members that
        // land in `Failed` now leaves a visible reap trail.
        store
            .record_event(
                pool,
                PoolEvent::warning(
                    "ReapedFailed",
                    format!(
                        "Reaped {} terminal (Failed/Terminating) member(s)",
                        members.failed.len()
                    ),
                ),
            )
            .await;
    }
    if !plan.delete.is_empty() {
        store
            .record_event(
                pool,
                PoolEvent::normal(
                    "TrimmedExcess",
                    format!(
                        "Trimmed {} Running member(s) above maxReady={}",
                        plan.delete.len(),
                        pool.spec.max_ready
                    ),
                ),
            )
            .await;
    }
    if !members.stale.is_empty() {
        store
            .record_event(
                pool,
                PoolEvent::normal(
                    "ReclaimedStale",
                    format!(
                        "Reclaimed {} legacy Paused/Pausing member(s)",
                        members.stale.len()
                    ),
                ),
            )
            .await;
    }
    if !members.version_stale.is_empty() {
        store
            .record_event(
                pool,
                PoolEvent::normal(
                    "EvictedVersionStale",
                    format!(
                        "Evicted {} member(s) minted against a superseded template",
                        members.version_stale.len()
                    ),
                ),
            )
            .await;
    }

    // Top-up toward the floor, after the deletes above have drained observed
    // garbage. Each create is an independent warm sandbox (unique server-side
    // name). The template defaults resolved above carry the runtimeClassName
    // (so the warm Pods run under the same runtime — gVisor — the cold-boot
    // path would have picked, which a later requester pause/resume depends on)
    // and the template's `resourceVersion` (stamped onto each member for #397).
    //
    // The breaker (issue #615) gates the planned count: while tripped it caps
    // mints to zero (or one probe once the window elapses), so a permanent
    // mint-failure storm is bounded to ~one mint per growing backoff window
    // instead of one per reconcile cadence.
    let create = breaker.allowed_create;
    if create > 0 {
        for _ in 0..create {
            let mut sandbox = warm_sandbox(pool, &pool.spec.template_ref, &defaults);
            // Stamp this pool reconcile's trace context onto the warm member
            // so the member's own Sandbox reconcile continues the trace (OTel
            // Phase 5). A claim later refreshes it to the requester's trace
            // (see `warm_claim::apply_overrides`).
            let mut trace_annotations = BTreeMap::new();
            sandbawks_observability::inject_into_annotations(&mut trace_annotations);
            if !trace_annotations.is_empty() {
                sandbox
                    .metadata
                    .annotations
                    .get_or_insert_with(BTreeMap::new)
                    .append(&mut trace_annotations);
            }
            store.create_sandbox(&namespace, sandbox).await?;
        }
        // `Normal` top-up Event (issue #591): makes the floor-fill visible in
        // `kubectl describe`, so an operator can see the pool re-minting the
        // slots a reap/trim/evict freed. Gated on `plan.create > 0` (this
        // branch), so an idle pool emits nothing.
        store
            .record_event(
                pool,
                PoolEvent::normal(
                    "ToppedUp",
                    format!(
                        "Created {} warm member(s) toward minReady={}",
                        create, pool.spec.min_ready
                    ),
                ),
            )
            .await;
    }

    // Publish the breaker's trip/recover Event (issue #615) so the backoff is
    // diagnosable from `kubectl describe sandboxwarmpool` rather than only
    // inferable from a flat `failed` count: `WarmPoolBackoff` (Warning) on a
    // trip/re-trip carries the dominant failure message and the window;
    // `WarmPoolRecovered` (Normal) marks the close. Best-effort, like every
    // other pool Event.
    if let Some(event) = breaker.event.clone() {
        store.record_event(pool, event).await;
    }

    // Warn when the template lookup missed (issue #591): the lenient path
    // (`template_defaults` returns `WarmSandboxDefaults::default()`) leaves
    // `resources` `None` — a resolved template always carries `Some` defaults.
    // Surfacing it as a `Warning` Event explains why members minted this pass
    // lack a `runtimeClassName` (and so will fail to pause once claimed). Gated
    // on `create > 0` so we only warn when the miss actually affected a
    // mint — a steady-state pool referencing a missing template that needs no
    // top-up (or a tripped breaker minting nothing) stays quiet.
    if create > 0 && defaults.resources.is_none() {
        store
            .record_event(
                pool,
                PoolEvent::warning(
                    "TemplateNotFound",
                    format!(
                        "Template {:?} not found; warm members minted this pass lack a \
                         runtimeClassName and will fail to pause once claimed",
                        pool.spec.template_ref
                    ),
                ),
            )
            .await;
    }
    // Status reflects the *observed* members (pre-create); the next
    // reconcile, fired by the watch on the new members, folds them in. The
    // breaker fields are overlaid from this pass's decision so the next
    // reconcile reads the persisted state back (issue #615): `degraded` makes a
    // pool stuck below `minReady` because the breaker is open observable rather
    // than mistaken for healthy.
    let mut status = status_for(&members);
    status.consecutive_mint_failures = breaker.consecutive_mint_failures;
    status.degraded = breaker.degraded;
    status.degraded_reason = breaker.degraded_reason.clone();
    status.backoff_until = breaker.backoff_until.clone();
    status.backoff_count = breaker.backoff_count;
    store
        .patch_pool_status(&namespace, &pool_name, status)
        .await?;

    // Mirror the same observed-vs-target counts into OTel instruments (#288):
    // the ready/min/max gauges plot the pool against its band and the
    // create/trim counters expose the thrash loop an alert watches for.
    // The values are exactly the structured log fields below, so the two stay
    // in lock-step.
    crate::warm_pool_metrics::record_reconcile(crate::warm_pool_metrics::PoolObservation {
        pool: &pool_name,
        ready: members.ready.len() as u32,
        failed: members.failed.len() as u32,
        min_ready: pool.spec.min_ready,
        max_ready: pool.spec.max_ready,
        // The *actual* mints this pass — `breaker.allowed_create`, not the
        // planned count — so a tripped breaker reads as zero creates (issue
        // #615) rather than overstating top-up the breaker suppressed.
        created: create,
        // Every delete this pass, not just ceiling trims: the legacy-paused
        // reclaim, version-stale eviction (#397), and Failed reap (#457) are
        // all `delete_sandbox` calls. Counting only `plan.delete` here would
        // make a pod-create-failure storm read as a one-sided `created_total`
        // runaway and mis-fire the documented thrash alert (both counters
        // climbing together), when deletes are in fact occurring at the same
        // rate as the failed re-mints.
        deleted: (plan.delete.len()
            + members.stale.len()
            + members.version_stale.len()
            + members.failed.len()) as u32,
    });

    // One structured line per reconcile so a pool stuck below its floor is
    // visible at the default `debug` level (the bin's `for_each` only logs
    // the opaque object ref). Carries the observed-vs-target counts and the
    // action plan; a Prometheus gauge/counter pair belongs with the
    // project's planned metrics work, not bolted on here with no recorder.
    tracing::debug!(
        pool = %pool_name,
        namespace = %namespace,
        ready = members.ready.len(),
        pending = members.pending.len(),
        min_ready = pool.spec.min_ready,
        max_ready = pool.spec.max_ready,
        planned_create = plan.create,
        create,
        degraded = breaker.degraded,
        backoff_count = breaker.backoff_count,
        trim = plan.delete.len(),
        reclaim = members.stale.len(),
        evict = members.version_stale.len(),
        reap = members.failed.len(),
        total = members.total(),
        "warm-pool reconciled"
    );

    Ok(Action::requeue(WARM_POOL_REQUEUE))
}

/// `kube::Client`-backed [`WarmPoolStore`] for production.
///
/// Builds namespaced `Api`s on demand (cheap — an `Api` just holds an
/// `Arc`'d client), mirroring [`crate::store::KubeSandboxStore`] /
/// `KubeSandboxOps`.
pub struct KubeWarmPoolStore {
    client: kube::Client,
}

impl KubeWarmPoolStore {
    /// Build a store over the given client.
    pub fn new(client: kube::Client) -> Self {
        Self { client }
    }

    fn sandboxes(&self, namespace: &str) -> kube::Api<Sandbox> {
        kube::Api::namespaced(self.client.clone(), namespace)
    }

    fn pools(&self, namespace: &str) -> kube::Api<SandboxWarmPool> {
        kube::Api::namespaced(self.client.clone(), namespace)
    }
}

#[async_trait]
impl WarmPoolStore for KubeWarmPoolStore {
    // No `err` on the per-reconcile read/write store ops below
    // (`list_members`, `patch_status`, `template_defaults`): the warm-pool
    // reconcile requeues every `WARM_POOL_REQUEUE`, so a sustained apiserver
    // flake would emit a steady stream of Issues that add nothing over the
    // parent `reconcile:controller::warm_pool` span (which carries `err` and
    // already reports the failed reconcile). `create_member` / `delete_member`
    // keep `err` — they fire only on an actual scaling delta, not every pass,
    // and a failed member create/delete is a real, actionable event.
    #[tracing::instrument(
        skip_all,
        name = "pool:controller::list_members",
        fields(pool = %pool_name, namespace = %namespace)
    )]
    async fn list_members(
        &self,
        namespace: &str,
        pool_name: &str,
    ) -> Result<Vec<Sandbox>, StoreError> {
        let lp = ListParams::default().labels(&format!("{WARM_POOL_LABEL}={pool_name}"));
        Ok(self.sandboxes(namespace).list(&lp).await?.items)
    }

    #[tracing::instrument(
        skip_all,
        name = "pool:controller::create_member",
        err,
        fields(namespace = %namespace)
    )]
    async fn create_sandbox(&self, namespace: &str, sandbox: Sandbox) -> Result<(), StoreError> {
        self.sandboxes(namespace)
            .create(&PostParams::default(), &sandbox)
            .await?;
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        name = "pool:controller::delete_member",
        err,
        fields(sandbox = %name, namespace = %namespace)
    )]
    async fn delete_sandbox(&self, namespace: &str, name: &str) -> Result<(), StoreError> {
        // Background propagation: the controller observes the delete and
        // reaps the backing Pod; the reconcile only needs the CR gone.
        self.sandboxes(namespace)
            .delete(name, &DeleteParams::background())
            .await?;
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        name = "pool:controller::patch_status",
        fields(pool = %pool_name, namespace = %namespace, ready = status.ready)
    )]
    async fn patch_pool_status(
        &self,
        namespace: &str,
        pool_name: &str,
        status: SandboxWarmPoolStatus,
    ) -> Result<(), StoreError> {
        let patch = serde_json::json!({ "status": status });
        self.pools(namespace)
            .patch_status(pool_name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        name = "pool:controller::template_defaults",
        fields(template = %template_ref, namespace = %namespace)
    )]
    async fn template_defaults(
        &self,
        namespace: &str,
        template_ref: &str,
    ) -> Result<WarmSandboxDefaults, StoreError> {
        let templates: kube::Api<SandboxTemplate> =
            kube::Api::namespaced(self.client.clone(), namespace);
        match templates.get_opt(template_ref).await? {
            Some(template) => Ok(WarmSandboxDefaults::from_template(&template)),
            None => {
                // Pool references a template that doesn't exist (yet). Warm
                // sandboxes created now will run under the default runtime
                // and fail to pause, but wedging the reconcile is worse —
                // warn and proceed, matching the gateway's cold-boot path.
                tracing::warn!(
                    template = %template_ref,
                    namespace = %namespace,
                    "warm-pool template not found; warm sandboxes will lack a runtimeClassName"
                );
                Ok(WarmSandboxDefaults::default())
            }
        }
    }

    /// Publish a pool `Event` via kube-runtime's `Recorder` (issue #591).
    ///
    /// Mirrors the Sandbox reconcile's `emit_event` helper
    /// (`reconcile.rs`): build a `Recorder` with `controller =
    /// "sandbawks-controller"`, the pool's `object_ref(&())` as the involved
    /// object, and publish best-effort — a publish failure is logged at
    /// `warn` and swallowed (the `-> ()` return type guarantees it never
    /// reaches the reconcile loop, matching the documented "publish errors
    /// are discarded" contract). The `events.k8s.io/v1` `create` grant the
    /// Recorder needs is already provisioned cluster-wide (issue #529,
    /// `deploy/helm/sandbawks/templates/rbac.yaml`), which covers pool Events.
    async fn record_event(&self, pool: &SandboxWarmPool, event: PoolEvent) {
        let reporter = Reporter {
            controller: "sandbawks-controller".into(),
            instance: std::env::var("POD_NAME").ok(),
        };
        let recorder = Recorder::new(self.client.clone(), reporter, pool.object_ref(&()));
        let type_ = if event.warning {
            EventType::Warning
        } else {
            EventType::Normal
        };
        if let Err(e) = recorder
            .publish(Event {
                type_,
                reason: event.reason.into(),
                note: Some(event.note),
                action: event.reason.into(),
                secondary: None,
            })
            .await
        {
            tracing::warn!(
                pool = %pool.name_any(),
                reason = %event.reason,
                error = %e,
                "failed to publish warm-pool event (best-effort; reconcile continues)"
            );
        }
    }
}

/// In-memory [`WarmPoolStore`] for tests.
///
/// Records created/deleted/paused operations so reconcile tests can
/// assert on the side effects without a cluster. Cloning is cheap
/// (`Arc<Mutex<_>>`-backed), matching the other fakes in
/// [`crate::store`].
#[derive(Default, Clone)]
pub struct FakeWarmPoolStore {
    members: Arc<Mutex<Vec<Sandbox>>>,
    /// `generateName` prefixes of sandboxes created this run.
    pub created: Arc<Mutex<Vec<Sandbox>>>,
    /// Names passed to `delete_sandbox`.
    pub deleted: Arc<Mutex<Vec<String>>>,
    /// The last `status` written for the pool.
    pub last_status: Arc<Mutex<Option<SandboxWarmPoolStatus>>>,
    /// Defaults `template_defaults` hands back (what a warm sandbox
    /// inherits). Defaults to empty; set via [`Self::with_defaults`].
    pub defaults: Arc<Mutex<WarmSandboxDefaults>>,
    /// [`PoolEvent`]s passed to `record_event`, in order (issue #591). Lets a
    /// reconcile test assert which Events would have been published without a
    /// real `Recorder` — preserving the test seam the trait's default no-op
    /// impl would otherwise make invisible.
    pub events: Arc<Mutex<Vec<PoolEvent>>>,
}

impl FakeWarmPoolStore {
    /// Construct an empty fake store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a member the reconcile will observe.
    pub fn insert_member(&self, sandbox: Sandbox) {
        self.members.lock().expect("poisoned").push(sandbox);
    }

    /// Set the template defaults `template_defaults` returns.
    pub fn with_defaults(self, defaults: WarmSandboxDefaults) -> Self {
        *self.defaults.lock().expect("poisoned") = defaults;
        self
    }
}

#[async_trait]
impl WarmPoolStore for FakeWarmPoolStore {
    async fn list_members(
        &self,
        _namespace: &str,
        pool_name: &str,
    ) -> Result<Vec<Sandbox>, StoreError> {
        // Mirror the label selector the real store applies so a test that
        // seeds a foreign-pool member doesn't see it leak in.
        Ok(self
            .members
            .lock()
            .expect("poisoned")
            .iter()
            .filter(|s| s.labels().get(WARM_POOL_LABEL).map(String::as_str) == Some(pool_name))
            .cloned()
            .collect())
    }

    async fn create_sandbox(&self, _namespace: &str, sandbox: Sandbox) -> Result<(), StoreError> {
        self.created.lock().expect("poisoned").push(sandbox);
        Ok(())
    }

    async fn delete_sandbox(&self, _namespace: &str, name: &str) -> Result<(), StoreError> {
        self.deleted.lock().expect("poisoned").push(name.to_owned());
        Ok(())
    }

    async fn patch_pool_status(
        &self,
        _namespace: &str,
        _pool_name: &str,
        status: SandboxWarmPoolStatus,
    ) -> Result<(), StoreError> {
        *self.last_status.lock().expect("poisoned") = Some(status);
        Ok(())
    }

    async fn template_defaults(
        &self,
        _namespace: &str,
        _template_ref: &str,
    ) -> Result<WarmSandboxDefaults, StoreError> {
        Ok(self.defaults.lock().expect("poisoned").clone())
    }

    async fn record_event(&self, _pool: &SandboxWarmPool, event: PoolEvent) {
        self.events.lock().expect("poisoned").push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sandbawks_types::{SandboxStatus, SandboxTemplateSpec, SandboxWarmPoolSpec};

    /// A namespaced, UID-stamped pool — both are required by [`reconcile_pool`].
    fn pool(name: &str, min: u32, max: u32) -> SandboxWarmPool {
        let mut p = SandboxWarmPool::new(
            name,
            SandboxWarmPoolSpec {
                template_ref: "py".into(),
                min_ready: min,
                max_ready: max,
            },
        );
        p.metadata.namespace = Some("team-acme".into());
        p.metadata.uid = Some("pool-uid-591".into());
        p
    }

    /// A warm-pool member in `phase`, carrying the pool label.
    fn member(name: &str, pool_name: &str, phase: SandboxPhase) -> Sandbox {
        let mut s = Sandbox::new(
            name,
            SandboxSpec {
                template_ref: "py".into(),
                runtime_class_name: None,
                resources: placeholder_resources(),
                timeout_seconds: 0,
                auto_pause: true,
                desired_phase: None,
                env: BTreeMap::new(),
                metadata: BTreeMap::new(),
                network_policy: None,
                snapshot_retention: None,
                pvc_retention: None,
                tolerations: None,
                node_selector: None,
                security_context: None,
                persistence_mode: None,
                volume_claim_templates: None,
                restore_from: None,
            },
        );
        let mut labels = BTreeMap::new();
        labels.insert(WARM_POOL_LABEL.to_owned(), pool_name.to_owned());
        s.metadata.labels = Some(labels);
        s.status = Some(SandboxStatus {
            phase,
            ..SandboxStatus::default()
        });
        s
    }

    /// Template defaults a *resolved* template hands back — `resources` is
    /// `Some`, distinguishing it from the lenient template-miss path whose
    /// `WarmSandboxDefaults::default()` leaves `resources` `None`.
    fn resolved_defaults() -> WarmSandboxDefaults {
        WarmSandboxDefaults {
            runtime_class_name: Some("gvisor".into()),
            resources: Some(placeholder_resources()),
            template_resource_version: None,
            network_policy: None,
        }
    }

    /// Issue #591: a `Failed` member is reaped *and* a `Warning` Event is
    /// published describing the reap — the reported `kubectl describe` gap.
    #[tokio::test]
    async fn reap_emits_warning_event() {
        let store = FakeWarmPoolStore::new().with_defaults(resolved_defaults());
        store.insert_member(member("sbx-p-0", "p", SandboxPhase::Running));
        store.insert_member(member("sbx-f-0", "p", SandboxPhase::Failed));
        store.insert_member(member("sbx-f-1", "p", SandboxPhase::Terminating));

        reconcile_pool(&pool("p", 1, 4), &store)
            .await
            .expect("reconcile ok");

        let events = store.events.lock().expect("poisoned").clone();
        let reap = events
            .iter()
            .find(|e| e.reason == "ReapedFailed")
            .expect("a ReapedFailed event is published");
        assert!(reap.warning, "reap is a Warning");
        assert!(
            reap.note.contains('2'),
            "note carries the reap count: {}",
            reap.note
        );
    }

    /// An idle (no-work) heartbeat reconcile — the pool is already at its
    /// floor with no garbage — publishes no Events at all (acceptance:
    /// "no Events on idle heartbeat reconciles").
    #[tokio::test]
    async fn idle_reconcile_emits_no_events() {
        let store = FakeWarmPoolStore::new().with_defaults(resolved_defaults());
        store.insert_member(member("sbx-p-0", "p", SandboxPhase::Running));
        store.insert_member(member("sbx-p-1", "p", SandboxPhase::Running));

        reconcile_pool(&pool("p", 2, 4), &store)
            .await
            .expect("reconcile ok");

        assert!(
            store.events.lock().expect("poisoned").is_empty(),
            "idle reconcile emits no Events"
        );
    }

    /// Top-up creates emit a `Normal` `ToppedUp` Event; a trim above the
    /// ceiling emits a `Normal` `TrimmedExcess` Event.
    #[tokio::test]
    async fn topup_and_trim_emit_normal_events() {
        // Top-up: floor 2, one Running → create 1.
        let topup = FakeWarmPoolStore::new().with_defaults(resolved_defaults());
        topup.insert_member(member("sbx-p-0", "p", SandboxPhase::Running));
        reconcile_pool(&pool("p", 2, 4), &topup)
            .await
            .expect("top-up ok");
        let events = topup.events.lock().expect("poisoned").clone();
        let topped = events
            .iter()
            .find(|e| e.reason == "ToppedUp")
            .expect("a ToppedUp event is published");
        assert!(!topped.warning, "top-up is Normal");

        // Trim: ceiling 1, three Running → trim 2.
        let trim = FakeWarmPoolStore::new().with_defaults(resolved_defaults());
        for i in 0..3 {
            trim.insert_member(member(&format!("sbx-t-{i}"), "p", SandboxPhase::Running));
        }
        reconcile_pool(&pool("p", 0, 1), &trim)
            .await
            .expect("trim ok");
        let events = trim.events.lock().expect("poisoned").clone();
        let trimmed = events
            .iter()
            .find(|e| e.reason == "TrimmedExcess")
            .expect("a TrimmedExcess event is published");
        assert!(!trimmed.warning, "trim is Normal");
    }

    /// A template-lookup miss (`defaults.resources == None`) that forces a
    /// top-up emits a `Warning` `TemplateNotFound` Event alongside the
    /// `ToppedUp` one.
    #[tokio::test]
    async fn template_miss_on_topup_emits_warning() {
        // Default `FakeWarmPoolStore` returns `WarmSandboxDefaults::default()`
        // (resources None) — the lenient miss path.
        let store = FakeWarmPoolStore::new();
        reconcile_pool(&pool("p", 1, 4), &store)
            .await
            .expect("reconcile ok");

        let events = store.events.lock().expect("poisoned").clone();
        let miss = events
            .iter()
            .find(|e| e.reason == "TemplateNotFound")
            .expect("a TemplateNotFound event is published");
        assert!(miss.warning, "template miss is a Warning");
        assert!(
            events.iter().any(|e| e.reason == "ToppedUp"),
            "the top-up that hit the miss still emits ToppedUp"
        );
    }

    /// Build a `SandboxTemplate` carrying an optional egress baseline.
    fn template_with_egress(network_policy: Option<NetworkPolicy>) -> SandboxTemplate {
        SandboxTemplate::new(
            "tpl",
            SandboxTemplateSpec {
                image: "ghcr.io/hypotheosis/test-workload:latest".into(),
                default_runtime_class: "gvisor".into(),
                default_resources: ResourceRequests {
                    cpu: "1".parse().expect("valid cpu quantity"),
                    memory: "1Gi".parse().expect("valid memory quantity"),
                    ephemeral_storage: None,
                },
                envd_image: None,
                default_timeout_seconds: 60,
                network_policy,
                tolerations: None,
                node_selector: None,
                security_context: None,
                persistence_mode: None,
                volume_claim_templates: None,
                volume_claim_templates_policy: None,
            },
        )
    }

    /// Issue #537: `from_template` lifts the template's egress baseline into the
    /// resolved defaults, so the warm-pool mint path (`warm_sandbox`) can stamp
    /// it onto every member. This is the controller-side half of the propagation
    /// the gateway also performs on cold-start.
    #[test]
    fn from_template_extracts_egress_baseline() {
        let np = NetworkPolicy {
            egress_allow: vec!["git.example.com".into(), "10.0.0.0/8".into()],
        };
        let defaults = WarmSandboxDefaults::from_template(&template_with_egress(Some(np.clone())));
        assert_eq!(defaults.network_policy, Some(np));
    }

    /// A template with no egress config resolves to `None` — minted members stay
    /// at the default-deny baseline rather than carrying an empty policy.
    #[test]
    fn from_template_no_egress_resolves_to_none() {
        let defaults = WarmSandboxDefaults::from_template(&template_with_egress(None));
        assert!(defaults.network_policy.is_none());
    }

    // ---- Circuit breaker (issue #615) ----------------------------------

    use chrono::TimeZone;

    /// A `Failed` member carrying a terminal `Failed` condition whose message is
    /// `msg` — the permanent admission-class error the breaker keys its trip
    /// note off of.
    fn failed_member_with_msg(name: &str, pool_name: &str, msg: &str) -> Sandbox {
        let mut s = member(name, pool_name, SandboxPhase::Failed);
        let st = s.status.as_mut().expect("member seeds a status");
        st.failure_reason = Some("action_failed".into());
        st.conditions.push(sandbawks_types::SandboxCondition {
            kind: "Failed".into(),
            status: "True".into(),
            reason: Some("ActionFailure".into()),
            message: Some(msg.to_owned()),
            last_transition_time: None,
        });
        s
    }

    /// Prior breaker state on a pool's `status` so a reconcile-level test can
    /// drive the open-breaker arms (which read `pool.status`).
    fn breaker_status(
        consecutive: u32,
        degraded: bool,
        backoff_until: Option<String>,
        backoff_count: u32,
    ) -> SandboxWarmPoolStatus {
        SandboxWarmPoolStatus {
            ready: 0,
            failed: 0,
            total: 0,
            members: vec![],
            consecutive_mint_failures: consecutive,
            degraded,
            degraded_reason: degraded.then(|| "boom".to_owned()),
            backoff_until,
            backoff_count,
        }
    }

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 19, 12, 0, 0).unwrap()
    }

    /// `dominant_failure_message` picks the most frequent reaped-member message
    /// — the permanent error every fresh member hits identically.
    #[test]
    fn dominant_message_picks_the_majority_failure() {
        let failed = vec![
            failed_member_with_msg("a", "p", "snapshot unsupported on Kata"),
            failed_member_with_msg("b", "p", "snapshot unsupported on Kata"),
            failed_member_with_msg("c", "p", "some transient blip"),
        ];
        assert_eq!(
            dominant_failure_message(&failed).as_deref(),
            Some("snapshot unsupported on Kata")
        );
    }

    /// No reaped member carrying a message ⇒ `None` (the trip note then falls
    /// back to a generic reason).
    #[test]
    fn dominant_message_none_when_no_messages() {
        let failed = vec![member("a", "p", SandboxPhase::Failed)];
        assert!(dominant_failure_message(&failed).is_none());
    }

    /// A single mint-failure pass does **not** trip the breaker — it keeps
    /// minting as planned, preserving the #457 self-heal-to-`minReady` behavior
    /// (acceptance: transient single-mint failures do not trip).
    #[test]
    fn single_failure_does_not_trip() {
        let obs = MintFailureObservation {
            reaped: 1,
            dominant_message: Some("blip".into()),
        };
        let d = decide_breaker(None, &obs, 1, true, t0());
        assert!(!d.degraded, "one failure must not trip the breaker");
        assert_eq!(d.consecutive_mint_failures, 1);
        assert_eq!(d.allowed_create, 1, "still mints the planned member");
        assert!(d.event.is_none());
    }

    /// Reaching the threshold trips the breaker: it goes degraded, mints zero,
    /// stamps a backoff window, and emits a `WarmPoolBackoff` Warning carrying
    /// the dominant message and the window (acceptance: trip after threshold,
    /// trip Event with reason + duration).
    #[test]
    fn trips_at_threshold_and_emits_warning() {
        // Prior status already at threshold-1 consecutive failures.
        let prior = breaker_status(BREAKER_TRIP_THRESHOLD - 1, false, None, 0);
        let obs = MintFailureObservation {
            reaped: 2,
            dominant_message: Some("snapshot unsupported on Kata".into()),
        };
        let d = decide_breaker(Some(&prior), &obs, 3, true, t0());

        assert!(d.degraded, "breaker trips at the threshold");
        assert_eq!(d.consecutive_mint_failures, BREAKER_TRIP_THRESHOLD);
        assert_eq!(d.allowed_create, 0, "zero creates while tripped");
        assert_eq!(d.backoff_count, 0, "first trip is exponent 0");
        let ev = d.event.expect("a trip Event is emitted");
        assert!(ev.warning, "trip is a Warning");
        assert_eq!(ev.reason, "WarmPoolBackoff");
        assert!(
            ev.note.contains("snapshot unsupported on Kata"),
            "note carries the dominant failure message: {}",
            ev.note
        );
        assert!(
            ev.note
                .contains(&format!("{}s", BREAKER_BACKOFF_BASE.as_secs())),
            "note carries the backoff duration: {}",
            ev.note
        );
        // The window is anchored now + base.
        let until = DateTime::parse_from_rfc3339(d.backoff_until.as_deref().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(until, t0() + breaker_backoff(0));
    }

    /// While tripped and still inside the window, the breaker mints zero and
    /// emits no Event (acceptance: zero-create while tripped).
    #[test]
    fn zero_create_while_inside_window() {
        let until = (t0() + chrono::Duration::seconds(60)).to_rfc3339();
        let prior = breaker_status(BREAKER_TRIP_THRESHOLD, true, Some(until), 0);
        // No failures this pass, but still inside the window.
        let obs = MintFailureObservation::default();
        let d = decide_breaker(Some(&prior), &obs, 3, true, t0());
        assert!(d.degraded, "stays tripped inside the window");
        assert_eq!(d.allowed_create, 0, "mints zero inside the window");
        assert!(d.event.is_none(), "no Event while merely holding");
    }

    /// Once the window elapses with no failures while still below the floor, the
    /// breaker allows a single probe mint and stays open (acceptance: probe
    /// bounds churn to ~one mint per window).
    #[test]
    fn probe_mint_after_window_elapses() {
        let past = (t0() - chrono::Duration::seconds(1)).to_rfc3339();
        let prior = breaker_status(BREAKER_TRIP_THRESHOLD, true, Some(past), 0);
        let obs = MintFailureObservation::default();
        let d = decide_breaker(Some(&prior), &obs, 3, true, t0());
        assert!(d.degraded, "stays open during the probe");
        assert_eq!(d.allowed_create, 1, "exactly one probe mint");
        assert!(d.event.is_none());
    }

    /// A probe that re-fails (this pass reaps again after the window) re-trips
    /// with an **extended** window — `backoff_count` grows so the window climbs
    /// the curve (acceptance: probe-then-re-trip with extended window).
    #[test]
    fn probe_failure_re_trips_with_longer_window() {
        let past = (t0() - chrono::Duration::seconds(1)).to_rfc3339();
        let prior = breaker_status(BREAKER_TRIP_THRESHOLD, true, Some(past), 0);
        let obs = MintFailureObservation {
            reaped: 1,
            dominant_message: Some("still broken".into()),
        };
        let d = decide_breaker(Some(&prior), &obs, 3, true, t0());
        assert!(d.degraded);
        assert_eq!(d.allowed_create, 0, "re-trip suppresses minting again");
        assert_eq!(d.backoff_count, 1, "backoff exponent grew");
        let until = DateTime::parse_from_rfc3339(d.backoff_until.as_deref().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            until,
            t0() + breaker_backoff(1),
            "window extended to the next exponent"
        );
        assert!(
            breaker_backoff(1) > breaker_backoff(0),
            "the extended window is strictly longer"
        );
        let ev = d.event.expect("re-trip re-emits a Warning");
        assert_eq!(ev.reason, "WarmPoolBackoff");
    }

    /// A probe that succeeds (window elapsed, no failures, pool back at/over the
    /// floor) **closes** the breaker, resumes top-up, and emits a
    /// `WarmPoolRecovered` Normal Event (acceptance: recover closes the breaker;
    /// recover Event).
    #[test]
    fn recover_closes_breaker_and_emits_normal() {
        let past = (t0() - chrono::Duration::seconds(1)).to_rfc3339();
        let prior = breaker_status(BREAKER_TRIP_THRESHOLD, true, Some(past), 2);
        let obs = MintFailureObservation::default();
        // below_floor = false ⇒ the pool refilled; the breaker can fully close.
        let d = decide_breaker(Some(&prior), &obs, 0, false, t0());
        assert!(!d.degraded, "breaker closes on recovery");
        assert_eq!(d.consecutive_mint_failures, 0);
        assert_eq!(d.backoff_count, 0);
        assert!(d.backoff_until.is_none());
        assert!(d.degraded_reason.is_none());
        let ev = d.event.expect("recovery emits an Event");
        assert!(!ev.warning, "recovery is Normal");
        assert_eq!(ev.reason, "WarmPoolRecovered");
    }

    /// End-to-end through `reconcile_pool`: a pool already at threshold-1 whose
    /// members all fail this pass trips the breaker — the reconcile mints **zero**
    /// (no `create_sandbox` calls), publishes a `WarmPoolBackoff` Warning, and
    /// writes a `degraded` status.
    #[tokio::test]
    async fn reconcile_trips_breaker_emits_event_and_suppresses_mint() {
        let store = FakeWarmPoolStore::new().with_defaults(resolved_defaults());
        // 5 failed members this pass — enough to clear the threshold from a
        // fresh (no prior status) start in a single pass.
        for i in 0..BREAKER_TRIP_THRESHOLD {
            store.insert_member(failed_member_with_msg(
                &format!("sbx-f-{i}"),
                "p",
                "persistenceMode 'snapshot' is unsupported on Kata",
            ));
        }
        let mut p = pool("p", 3, 5);
        // Seed prior consecutive failures so this pass crosses the threshold.
        p.status = Some(breaker_status(BREAKER_TRIP_THRESHOLD - 1, false, None, 0));

        reconcile_pool(&p, &store).await.expect("reconcile ok");

        // No mints while tripping.
        assert!(
            store.created.lock().expect("poisoned").is_empty(),
            "breaker suppresses all mints on the tripping pass"
        );
        // Trip Event published with the dominant message.
        let events = store.events.lock().expect("poisoned").clone();
        let backoff = events
            .iter()
            .find(|e| e.reason == "WarmPoolBackoff")
            .expect("a WarmPoolBackoff event is published");
        assert!(backoff.warning);
        assert!(
            backoff.note.contains("unsupported on Kata"),
            "{}",
            backoff.note
        );
        // Status records the degraded state.
        let status = store
            .last_status
            .lock()
            .expect("poisoned")
            .clone()
            .expect("status written");
        assert!(status.degraded, "status surfaces the degraded breaker");
        assert!(status.degraded_reason.is_some());
        assert!(status.backoff_until.is_some());
    }

    /// End-to-end recovery: a tripped pool (prior `degraded` status, window
    /// elapsed) that is healthy this pass (no failed members, at floor) closes
    /// the breaker, emits `WarmPoolRecovered`, and resumes top-up.
    #[tokio::test]
    async fn reconcile_recovers_and_emits_recovered_event() {
        let store = FakeWarmPoolStore::new().with_defaults(resolved_defaults());
        // Pool at floor (2 Running, minReady 2) — no failures this pass.
        store.insert_member(member("sbx-p-0", "p", SandboxPhase::Running));
        store.insert_member(member("sbx-p-1", "p", SandboxPhase::Running));

        let mut p = pool("p", 2, 4);
        let past = (Utc::now() - chrono::Duration::seconds(5)).to_rfc3339();
        p.status = Some(breaker_status(BREAKER_TRIP_THRESHOLD, true, Some(past), 1));

        reconcile_pool(&p, &store).await.expect("reconcile ok");

        let events = store.events.lock().expect("poisoned").clone();
        let recovered = events
            .iter()
            .find(|e| e.reason == "WarmPoolRecovered")
            .expect("a WarmPoolRecovered event is published");
        assert!(!recovered.warning, "recovery is Normal");
        let status = store
            .last_status
            .lock()
            .expect("poisoned")
            .clone()
            .expect("status written");
        assert!(!status.degraded, "breaker closed in status");
        assert!(status.backoff_until.is_none());
    }
}
