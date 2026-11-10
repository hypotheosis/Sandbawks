//! Reconcile loop entrypoint.
//!
//! Honors spec §10's two key invariants:
//!
//! 1. **Status is durable before side effects.** We patch
//!    `status.phase` first, then call into the adapter. A crash
//!    between the two leaves the Sandbox in the intermediate phase
//!    (`Pausing`, `Resuming`), and the next reconcile recovers.
//! 2. **Idempotent.** Re-reading the same Sandbox and re-reconciling
//!    must not duplicate adapter calls. The state machine returning
//!    `None` for in-flight phases (e.g. `(Pausing, PauseRequested)`)
//!    is how we get that.
//!
//! The trigger is derived from the Sandbox itself — annotations,
//! deletion timestamp, status.phase. Plan 04 will replace direct
//! annotation reads with reads of `spec.desiredPhase` once that field
//! lands.
//!
//! Plan 05 T4 layers a second trigger source on top: when the Sandbox
//! has an in-flight `SnapshotJob` (stamped in
//! `status.snapshot_job_ref`), the loop polls the job's `status` and
//! folds `phase=Complete | Failed` into the corresponding
//! [`Trigger::SnapshotValidated`] / [`Trigger::StageComplete`] /
//! [`Trigger::Failure`] event. The state machine itself is unchanged —
//! only the trigger-derivation step and the `dispatch()` arms that
//! used to invoke `RuntimeAdapter::pause`/`resume` directly.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use kube::{
    api::ObjectMeta,
    runtime::events::{Event, EventType, Recorder, Reporter},
    Resource,
};
use sandbawks_types::{
    merge_volume_claim_templates, PersistenceMode, PodRef, PodTerminationCause, RestorePlacement,
    RestoreSource, RuntimeAdapter, Sandbox, SandboxCondition, SandboxPhase, SandboxStatus,
    SnapshotJob, SnapshotJobMode, SnapshotJobPhase, SnapshotJobSpec, VctSuppression,
};
use thiserror::Error;

use crate::state_machine::{transition, Action, FailureReason, Trigger};
use crate::store::{SandboxStore, SandboxTemplateStore, SnapshotJobStore, StoreError};

/// Annotation key on the Sandbox carrying the user's desired phase.
///
/// Plan 02 reads this directly. Plan 04 will replace the annotation
/// with a typed `spec.desiredPhase` field once the API gateway lands.
pub const ANN_DESIRED_PHASE: &str = "sandbawks.dev/desired-phase";

/// Annotation marking that the backing Pod has reached Ready=true.
///
/// Plan 03 wires this from a real pod watcher; Plan 02 tests set it.
pub const ANN_POD_READY: &str = "sandbawks.dev/pod-ready";

/// Default cap on how many times the bounded `Resuming` recovery loop recreates
/// the restore Pod before giving up and folding the Sandbox to `Failed` with
/// [`FailureReason::ResumeExhausted`] (epic #484, issue #489). Overridable at
/// runtime via [`RESUME_MAX_RESTARTS_ENV`]; read once through
/// [`resume_max_restarts`].
///
/// Turning the old unbounded `resumingforever` hang into a finite retry budget
/// is the entire point of the recovery loop: a restore that can never reach
/// `Running` (no node capacity, a wedged shim restore, a repeatedly-failing
/// `Stage`) eventually surfaces a clear terminal failure instead of spinning.
const DEFAULT_RESUME_MAX_RESTARTS: u32 = 5;

/// Default for how long a restore Pod may sit `Pending` (unscheduled) before the
/// recovery loop treats it as lost — no node had capacity to place it — and
/// recreates it unpinned so the scheduler re-attempts placement. Overridable via
/// [`RESUME_SCHEDULE_DEADLINE_SECS_ENV`]; read once through
/// [`resume_schedule_deadline`].
const DEFAULT_RESUME_SCHEDULE_DEADLINE: Duration = Duration::from_secs(120);

/// Default for how long a *scheduled* restore Pod may stay not-Ready before the
/// recovery loop treats the restore as wedged and recreates the Pod. Overridable
/// via [`RESUME_READY_DEADLINE_SECS_ENV`]; read once through
/// [`resume_ready_deadline`].
const DEFAULT_RESUME_READY_DEADLINE: Duration = Duration::from_secs(120);

/// Env var overriding [`DEFAULT_RESUME_MAX_RESTARTS`] (a bare integer count).
/// Surfaced through Helm as `controller.restore.maxRestarts` (issue #490).
const RESUME_MAX_RESTARTS_ENV: &str = "SANDBAWKS_RESUME_MAX_RESTARTS";

/// Env var overriding [`DEFAULT_RESUME_SCHEDULE_DEADLINE`], in whole seconds.
/// Surfaced through Helm as `controller.restore.scheduleDeadlineSeconds`.
const RESUME_SCHEDULE_DEADLINE_SECS_ENV: &str = "SANDBAWKS_RESUME_SCHEDULE_DEADLINE_SECS";

/// Env var overriding [`DEFAULT_RESUME_READY_DEADLINE`], in whole seconds.
/// Surfaced through Helm as `controller.restore.readyDeadlineSeconds`. The kind
/// smoke tunes this down so the bounded-recovery path trips inside the resume
/// wait budget (issue #490).
const RESUME_READY_DEADLINE_SECS_ENV: &str = "SANDBAWKS_RESUME_READY_DEADLINE_SECS";

/// Resolve a `u32` tunable from `lookup(key)`, falling back to `default` when the
/// variable is unset, blank, or unparseable. Pure (the env lookup is injected) so
/// the parse/fallback behavior is unit-testable without touching process env.
fn u32_from_env(lookup: impl Fn(&str) -> Option<String>, key: &str, default: u32) -> u32 {
    lookup(key)
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

/// Resolve a seconds-valued [`Duration`] tunable from `lookup(key)`, falling back
/// to `default` when the variable is unset, blank, or unparseable. Pure for the
/// same reason as [`u32_from_env`].
fn duration_secs_from_env(
    lookup: impl Fn(&str) -> Option<String>,
    key: &str,
    default: Duration,
) -> Duration {
    lookup(key)
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(default)
}

/// Resolve a non-blank string tunable from `lookup(key)`, falling back to
/// `default` when the variable is unset or blank/whitespace-only. The non-blank
/// value is returned verbatim (paths are used as-is). Pure for the same reason as
/// [`u32_from_env`], so the override and blank-fallback branches are testable
/// without touching process env.
fn string_from_env(lookup: impl Fn(&str) -> Option<String>, key: &str, default: &str) -> String {
    lookup(key)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// The effective resume restart budget — [`RESUME_MAX_RESTARTS_ENV`] or
/// [`DEFAULT_RESUME_MAX_RESTARTS`]. Read once and cached: the controller process
/// reads its config at startup, so an `OnceLock` avoids re-parsing env on every
/// reconcile while keeping the value a single source of truth.
fn resume_max_restarts() -> u32 {
    static VALUE: OnceLock<u32> = OnceLock::new();
    *VALUE.get_or_init(|| {
        u32_from_env(
            |k| std::env::var(k).ok(),
            RESUME_MAX_RESTARTS_ENV,
            DEFAULT_RESUME_MAX_RESTARTS,
        )
    })
}

/// The effective schedule deadline — [`RESUME_SCHEDULE_DEADLINE_SECS_ENV`] or
/// [`DEFAULT_RESUME_SCHEDULE_DEADLINE`]. Cached like [`resume_max_restarts`].
fn resume_schedule_deadline() -> Duration {
    static VALUE: OnceLock<Duration> = OnceLock::new();
    *VALUE.get_or_init(|| {
        duration_secs_from_env(
            |k| std::env::var(k).ok(),
            RESUME_SCHEDULE_DEADLINE_SECS_ENV,
            DEFAULT_RESUME_SCHEDULE_DEADLINE,
        )
    })
}

/// The effective readiness deadline — [`RESUME_READY_DEADLINE_SECS_ENV`] or
/// [`DEFAULT_RESUME_READY_DEADLINE`]. Cached like [`resume_max_restarts`].
fn resume_ready_deadline() -> Duration {
    static VALUE: OnceLock<Duration> = OnceLock::new();
    *VALUE.get_or_init(|| {
        duration_secs_from_env(
            |k| std::env::var(k).ok(),
            RESUME_READY_DEADLINE_SECS_ENV,
            DEFAULT_RESUME_READY_DEADLINE,
        )
    })
}

/// Default cap on tier-1 pause-checkpoint retries before the Sandbox folds to
/// `Failed` with [`FailureReason::PauseCheckpointFailed`] (issue #510). A
/// single transient `/__state` timeout should not be terminal, so the pause
/// checkpoint is re-dispatched a small bounded number of times — mirroring the
/// `Resuming` recovery bound. Overridable via [`PAUSE_MAX_RETRIES_ENV`]; read
/// once through [`pause_max_retries`].
const DEFAULT_PAUSE_MAX_RETRIES: u32 = 3;

/// Default grace window an orphaned `Failed` Pod (recoverable pause-checkpoint
/// class) may stay envd-unreachable before the tier-3 GC deletes it to reclaim
/// the node slot (issue #510). Measured from `status.failed_at`. Never applies
/// while envd is still serving — a healthy Pod is recovered to `Running`
/// (tier 2), not GC'd. Overridable via [`FAILED_GC_GRACE_SECS_ENV`]; read once
/// through [`failed_gc_grace`].
const DEFAULT_FAILED_GC_GRACE: Duration = Duration::from_secs(300);

/// Env var overriding [`DEFAULT_PAUSE_MAX_RETRIES`] (a bare integer count).
/// Surfaced through Helm as `controller.pauseRecovery.maxRetries`.
const PAUSE_MAX_RETRIES_ENV: &str = "SANDBAWKS_PAUSE_MAX_RETRIES";

/// Env var overriding [`DEFAULT_FAILED_GC_GRACE`], in whole seconds. Surfaced
/// through Helm as `controller.pauseRecovery.gcGraceSeconds`.
const FAILED_GC_GRACE_SECS_ENV: &str = "SANDBAWKS_FAILED_GC_GRACE_SECS";

/// The effective tier-1 pause retry budget — [`PAUSE_MAX_RETRIES_ENV`] or
/// [`DEFAULT_PAUSE_MAX_RETRIES`]. Cached like [`resume_max_restarts`].
fn pause_max_retries() -> u32 {
    static VALUE: OnceLock<u32> = OnceLock::new();
    *VALUE.get_or_init(|| {
        u32_from_env(
            |k| std::env::var(k).ok(),
            PAUSE_MAX_RETRIES_ENV,
            DEFAULT_PAUSE_MAX_RETRIES,
        )
    })
}

/// The effective tier-3 GC grace window — [`FAILED_GC_GRACE_SECS_ENV`] or
/// [`DEFAULT_FAILED_GC_GRACE`]. Cached like [`resume_schedule_deadline`].
fn failed_gc_grace() -> Duration {
    static VALUE: OnceLock<Duration> = OnceLock::new();
    *VALUE.get_or_init(|| {
        duration_secs_from_env(
            |k| std::env::var(k).ok(),
            FAILED_GC_GRACE_SECS_ENV,
            DEFAULT_FAILED_GC_GRACE,
        )
    })
}

/// Base delay for the recovery loops' capped exponential backoff: the `n`-th
/// retry waits `min(RESUME_BACKOFF_BASE * 2^n, RESUME_BACKOFF_CAP)`, measured
/// from the relevant last-attempt anchor. Shared by the `Resuming` recovery
/// loop (anchored at `status.resume_last_attempt`) and the tier-1
/// pause-checkpoint retry loop (anchored at `status.pause_last_attempt`, issue
/// #510). Without it an immediately-dying restore Pod or a fast-failing
/// checkpoint would burn the whole retry budget in one reconcile burst.
const RESUME_BACKOFF_BASE: Duration = Duration::from_secs(5);

/// Ceiling for the recovery backoff so a high retry count can't schedule an
/// unreasonably distant retry. Shared by both recovery loops.
const RESUME_BACKOFF_CAP: Duration = Duration::from_secs(300);

/// Controller-wide policy for garbage-collecting paused `filesystem`-mode
/// sandboxes whose retained PVCs have outlived their retention window (epic
/// #603 Phase 5 / issue #608).
///
/// A snapshot-mode pause parks state as an S3 object swept by
/// [`crate::RetentionConfig`]; a `filesystem`-mode pause keeps a live PVC that
/// bills until reclaimed. This config is the analog for that PVC-backed state:
/// when a paused filesystem sandbox has sat past its window, the reconcile loop
/// deletes the Sandbox CR, cascading its PVCs via their ownerReferences.
///
/// Disabled by default ([`Self::default`]) so a context that forgets to set it
/// — or a binary whose env vars are unset — never auto-deletes a sandbox, the
/// same fail-safe stance as [`crate::RetentionConfig`].
#[derive(Debug, Clone)]
pub struct PvcRetentionConfig {
    /// Master switch. `false` ⇒ the reconcile loop never collects a paused
    /// filesystem sandbox on retention grounds, whatever its `spec.pvcRetention`.
    pub enabled: bool,
    /// Window applied when a Sandbox does not set `spec.pvcRetention`. The
    /// `filesystem`-mode analog of [`crate::RetentionConfig::default_duration`].
    pub default_duration: Duration,
}

impl Default for PvcRetentionConfig {
    fn default() -> Self {
        // 7 days matches the snapshot retention default; `enabled = false` keeps
        // a `Default::default()` (tests, or a binary that forgot the env var)
        // from ever auto-deleting a sandbox.
        Self {
            enabled: false,
            default_duration: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

/// Dependencies the reconcile loop needs.
///
/// Construct once at startup (binary) or per-test (unit tests) and pass
/// in via `Arc<ReconcileContext>` so kube-rs's `Controller` can clone it
/// across requeues cheaply.
pub struct ReconcileContext {
    /// Read/write Sandboxes. Real impl is [`crate::KubeSandboxStore`];
    /// tests use [`crate::FakeSandboxStore`].
    pub store: Arc<dyn SandboxStore>,
    /// Pause/resume backend. Plan 02 wires `FakeRuntimeAdapter`; Plan
    /// 05 swaps in `GvisorAdapter`.
    pub adapter: Arc<dyn RuntimeAdapter>,
    /// `SnapshotJob` CR backend. Real impl is
    /// [`crate::KubeSnapshotJobStore`]; tests use
    /// [`crate::FakeSnapshotJobStore`].
    pub snapshot_jobs: Arc<dyn SnapshotJobStore>,
    /// `SandboxTemplate` CR backend. Resolved on `Action::CreatePod` /
    /// `Action::CreateRestorePod` to pick the Pod's workload + envd
    /// images. Real impl is [`crate::KubeSandboxTemplateStore`]; tests
    /// use [`crate::FakeSandboxTemplateStore`].
    pub templates: Arc<dyn SandboxTemplateStore>,
    /// Kubernetes client for emitting Events on Sandbox objects.
    ///
    /// `None` in unit tests that have no live apiserver — event emission is
    /// silently skipped. Production binary always sets `Some(client)`.
    pub client: Option<kube::Client>,
    /// PVC-retention GC policy for paused `filesystem`-mode sandboxes (epic
    /// #603 Phase 5 / issue #608). [`PvcRetentionConfig::default`] (disabled)
    /// for any context that does not opt in.
    pub pvc_retention: PvcRetentionConfig,
    /// Sink for authoritative per-team usage events emitted on each closed
    /// `Running` interval (epic #480 Phase 2, issue #631). Defaults to
    /// [`crate::usage::NoopUsageSink`] when usage metering is disabled, so the
    /// reconcile loop always has a sink to call; the production binary swaps in the
    /// object-store-backed sink when configured.
    pub usage_sink: Arc<dyn crate::usage::UsageEventSink>,
    /// Durable spill queue for usage events that fail live emission after the
    /// bounded retry (#636). Defaults to [`crate::usage_spill::NoopUsageSpillQueue`]
    /// (spilling disabled → such events are dropped, as before); the production
    /// binary swaps in the PVC-backed queue when a spill directory is configured, so
    /// a sustained object-store outage parks intervals for replay instead of losing
    /// them.
    pub usage_spill: Arc<dyn crate::usage_spill::UsageSpillQueue>,
}

/// Errors raised from the reconcile loop.
#[derive(Debug, Error)]
pub enum ControllerError {
    /// Underlying [`SandboxStore`] failure (kube transport, not-found,
    /// JSON encoding).
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// The reconcile loop saw an inconsistent Sandbox (e.g. attempted
    /// to resume but `status.snapshotRef` was None, or a Sandbox with
    /// no `metadata.name`). Bug if reached at runtime; tests assert
    /// against it explicitly.
    #[error("invariant violation: {0}")]
    Invariant(&'static str),
}

/// What the pre-dispatch SnapshotJob probe yields.
///
/// `Complete` and `Failed` produce a [`Trigger`] that overrides the
/// derived-from-spec trigger; the caller must also clear
/// `status.snapshot_job_ref` and (for Pause completions) fold the
/// returned `snapshot_ref` + `snapshot_sha256` into `status`.
/// `InFlight` short-circuits the reconcile — no transition this round.
enum SnapshotJobObservation {
    Complete {
        trigger: Trigger,
        /// `s3://…` URL the per-node worker wrote back into
        /// `SnapshotJob.status.manifest.snapshot_ref`. `None` for Resume
        /// completions (the resume consumed the snapshot — there's no
        /// new ref to record).
        snapshot_ref: Option<String>,
        /// Aggregate sha256 from the completing job's manifest. `None`
        /// for Resume completions and for Pause completions where the
        /// adapter reported an empty aggregate (legacy / v1-compat
        /// shape). Used to bind the URL to a verifiable fingerprint
        /// on the next Resume dispatch — issue #171 hardening.
        snapshot_sha256: Option<String>,
        /// envd-binary sha256 from the completing job's manifest
        /// (`envd_sha256`). `None` for Resume completions and for Pause
        /// completions whose manifest recorded no envd digest (v1-compat
        /// shape). Committed forward into `SnapshotJobSpec.expected_envd_sha256`
        /// on the next Resume/Stage dispatch so the node can cross-check
        /// the re-fetched manifest's envd digest — issue #278 hardening.
        snapshot_envd_sha256: Option<String>,
        /// Host runsc release the completing job's manifest recorded the
        /// checkpoint was taken at (`runsc_version`). `None` for Resume
        /// completions and for Pause completions whose manifest recorded no
        /// version (v1-compat shape, or a node that couldn't read its runsc
        /// version). Folded into `Sandbox.status.snapshotRuntimeVersion` and
        /// used to gate the next restore Pod's nodeAffinity — #545 Capability B.
        snapshot_runsc_version: Option<String>,
        /// For `Stage` completions, the local filesystem path the node
        /// worker materialized the snapshot at
        /// ([`sandbawks_types::SnapshotJobStatus::local_path`]). The
        /// caller copies it into `Sandbox.status.restore_staging_path`
        /// so `Action::CreateRestorePod` can thread it into the restore
        /// Pod's annotation. `None` for Pause/Resume completions.
        local_path: Option<String>,
    },
    Failed {
        trigger: Trigger,
    },
    InFlight,
}

/// Reconcile a single Sandbox.
///
/// This is the entrypoint kube-rs `Controller` will call (wrapped in
/// the appropriate `Action`/`Error` envelope by the binary in Task 9).
/// Tests call it directly against a [`crate::FakeSandboxStore`] plus
/// [`crate::FakeRuntimeAdapter`].
///
/// # Errors
///
/// Returns [`ControllerError::Store`] for transport / not-found errors
/// from the underlying store, or [`ControllerError::Invariant`] when
/// the observed Sandbox violates an internal invariant. Adapter-side
/// failures (`create_pod` returning `Err`) are *not* surfaced here —
/// they are folded into the [`SandboxPhase::Failed`] phase via a
/// second status patch, since the reconcile *itself* succeeded in
/// recording the failure.
// `otel.kind = "consumer"` + `messaging.*` shape this reconcile as a "Task"
// in the APM: a controller consumes Sandbox watch events rather than serving
// HTTP. Without it the span is `INTERNAL` and the APM (which only renders
// endpoint/task lanes) never surfaces it. Child `#[instrument]` spans nest in
// the task's waterfall. `messaging.destination.name` is the CRD kind
// (low-cardinality); the instance name stays in `sandbox`.
// `err` makes the macro emit an OTel `exception` event on this Task span when
// the reconcile returns `Err`, surfacing genuine reconcile faults in the APM's
// Issues view. (Sandbox-level failures folded into the `Failed` phase return
// `Ok`, so only infrastructure errors trip this — which is what we want.)
#[tracing::instrument(
    skip(ctx),
    name = "reconcile:controller::sandbox",
    err,
    fields(
        sandbox = %name,
        namespace = %namespace,
        otel.kind = "consumer",
        messaging.system = "kubernetes",
        messaging.operation = "process",
        messaging.destination.name = "Sandbox",
    )
)]
pub async fn reconcile(
    namespace: &str,
    name: &str,
    ctx: Arc<ReconcileContext>,
) -> Result<(), ControllerError> {
    let Some(mut sandbox) = ctx.store.get(namespace, name).await? else {
        // The Sandbox was deleted between this reconcile being enqueued and
        // running — routine for retired/claimed warm-pool members. There is
        // nothing to reconcile, so return a benign no-op rather than letting
        // a NotFound propagate as an error (which would emit a spurious OTel
        // exception per sandbox name — #385).
        return Ok(());
    };
    // Continue the trace of whoever wrote this Sandbox (the api gateway's
    // create/resume, or a parent reconcile that stamped the annotation) so the
    // control-plane hop shares one `trace_id` end-to-end (OTel Phase 5). When
    // the annotation is absent — e.g. an operator-edited CR — this is a no-op
    // and the reconcile root span stays a fresh trace root.
    sandbawks_observability::continue_from_annotations(sandbox.metadata.annotations.as_ref());
    let current_phase = current_phase(&sandbox);

    // Active pod-readiness probe — fills the gap left by the (still
    // unimplemented) Pod watcher described in Plan 04 T4. When the
    // Sandbox is `Creating` with a `pod_ref` already stamped but no
    // `envd_address` yet, ask the adapter directly. A `Some(addr)`
    // result patches `status.envd_address`, which then lets the
    // `derive_trigger` call below promote the phase via `PodReady`.
    //
    // Plan 07 T9: the same probe is reused for the `Resuming` phase
    // after `Action::CreateRestorePod` materializes the restore Pod.
    // The probe re-fills pod_uid/envd_address/assigned_node so the
    // following `derive_trigger` call yields `PodReady` and the
    // `(Resuming, PodReady)` arm promotes the born-restored Pod to
    // `Running`.
    //
    // Issue #247: at `Resuming` the probe is *authoritative* — it
    // re-validates even when `envd_address` is already recorded. Unlike
    // cold start (where `envd_address` is only ever written by a live
    // probe), a resuming Sandbox can carry a STALE pre-pause address:
    // `Action::CreateRestorePod` deletes the pre-pause Pod and scrubs the
    // in-memory status before creating the restore Pod, but a controller
    // crash before the post-dispatch patch leaves the durable
    // `envd_address` naming the now-deleted Pod. Trusting it would let
    // `derive_trigger` emit `PodReady` and promote the Sandbox to
    // `Running` on a dead Pod. Because the restore Pod reuses the
    // Sandbox's name, `pod_ref` keeps naming the right Pod across the
    // crash, so re-probing it — and suppressing the unconfirmed carry-over
    // when no Ready Pod answers — makes the restored Pod's real readiness,
    // not the carry-over, decide the promotion. Recovery is then automatic:
    // once the restore Pod reports Ready the next probe writes the fresh
    // address and promotes; if it was never (re)created the Sandbox stays
    // safely stuck at `Resuming` rather than going `Running` on a ghost.
    if matches!(
        current_phase,
        SandboxPhase::Creating | SandboxPhase::Resuming
    ) {
        let has_addr = sandbox
            .status
            .as_ref()
            .and_then(|s| s.envd_address.as_deref())
            .is_some();
        let pod_ref = sandbox
            .status
            .as_ref()
            .and_then(|s| s.pod_ref.as_deref())
            .and_then(parse_pod_ref);
        // While a Stage job is in flight (`snapshot_job_ref` set) the
        // SnapshotJob observer below short-circuits the reconcile, so the
        // readiness edge can't promote yet — skip the probe to leave the
        // staging-phase status untouched. Once the ref clears, the probe
        // runs every pass at `Resuming` (re-validating any recorded
        // address); at `Creating` a recorded address is already
        // authoritative, so keep the original `!has_addr` short-circuit.
        let staging_in_flight = sandbox
            .status
            .as_ref()
            .and_then(|s| s.snapshot_job_ref.as_deref())
            .is_some();
        let should_probe = match current_phase {
            SandboxPhase::Resuming => !staging_in_flight,
            _ => !has_addr,
        };
        if should_probe {
            if let Some(pr) = pod_ref {
                match ctx.adapter.pod_ready(&pr).await {
                    Ok(Some(readiness)) => {
                        // #560 resume serve-readiness gate. Kubelet `Ready`
                        // means the `:49983` TCP listener *accepts* — not that
                        // envd *answers*. An envd `runsc restore`d with a live
                        // external gRPC connection comes back bound-but-wedged:
                        // it accepts connections (kubelet readiness passes) yet
                        // serves nothing on either port. On the resume path,
                        // require a real `GET /__state` 200 before recording the
                        // envd_address that gates the `Resuming → Running`
                        // promotion in `derive_trigger`; if envd is not serving,
                        // withhold the address so the bounded resume readiness
                        // deadline recreates the restore Pod (`RestorePodLost`)
                        // instead of declaring a dead sandbox `Running`. The
                        // cold-start (`Creating`) path keeps trusting kubelet
                        // readiness — there is no checkpoint to wedge it.
                        //
                        // #606: a `filesystem`-mode resume is also `Resuming` but
                        // cold-boots a fresh Pod with no `runsc restore`, so it
                        // *cannot* checkpoint-wedge. It still flows through this
                        // gate, which is the right behavior for a cold boot too —
                        // withhold `Running` until envd actually answers
                        // `/__state`, rather than promote over a still-starting
                        // envd. (A filesystem cold boot has no `resume_last_attempt`
                        // anchor and so no readiness deadline; a persistently
                        // non-serving cold-boot Pod stays `Resuming` until envd
                        // serves, matching cold-start `Creating` semantics.)
                        let serving = if current_phase == SandboxPhase::Resuming {
                            match ctx.adapter.envd_serving(&readiness.envd_address).await {
                                Ok(serving) => serving,
                                Err(e) => {
                                    tracing::debug!(
                                        sandbox = %name,
                                        error = %e,
                                        "resume serve-readiness probe failed; treating as \
                                         not-serving for this pass"
                                    );
                                    false
                                }
                            }
                        } else {
                            true
                        };
                        if serving {
                            let mut probe_status = sandbox.status.clone().unwrap_or_default();
                            probe_status.envd_address = Some(readiness.envd_address);
                            // `.or(...)` so a re-probe never blanks out a
                            // previously-recorded node/uid; the scheduler
                            // doesn't reschedule a Ready Pod, but if the
                            // adapter's surfaced value flips to None on a
                            // transient apiserver hiccup we keep the value
                            // we already have. Both fields are load-bearing
                            // for SnapshotJob dispatch.
                            probe_status.assigned_node =
                                readiness.node_name.or(probe_status.assigned_node);
                            probe_status.pod_uid = readiness.pod_uid.or(probe_status.pod_uid);
                            ctx.store
                                .patch_status(namespace, name, probe_status)
                                .await?;
                            // Re-read so `derive_trigger` sees the new
                            // envd_address; without this it'd run on the
                            // stale view and miss the `PodReady` edge. A
                            // mid-pass disappearance is the same benign
                            // deleted-while-queued race as the entry fetch
                            // (#385) — nothing left to reconcile.
                            let Some(updated) = ctx.store.get(namespace, name).await? else {
                                return Ok(());
                            };
                            sandbox = updated;
                        } else {
                            // Kubelet-Ready but envd not yet serving `/__state`
                            // — the #560 wedge, or an envd still mid-restore.
                            // Suppress any recorded address in-memory for this
                            // pass so `derive_trigger` cannot promote to
                            // `Running`; no durable write (the next requeue
                            // re-probes, and `resuming_trigger`'s readiness
                            // deadline recreates a persistently-wedged restore).
                            //
                            // Recovery from a *persistent* wedge is implicitly
                            // anchored on `status.resume_last_attempt`: that
                            // deadline only fires once the anchor is stamped (by
                            // `Action::CreateRestorePod`), since
                            // `past_deadline(None, …)` is false. The anchor is
                            // always set on the resume path before a restore Pod
                            // can become Ready, so this withhold can never idle
                            // forever in practice; the dependency is called out
                            // here so a future refactor keeps the anchor.
                            let stale_addr =
                                sandbox.status.as_ref().and_then(|s| s.envd_address.clone());
                            if let Some(status) = sandbox.status.as_mut() {
                                status.envd_address = None;
                            }
                            tracing::info!(
                                sandbox = %name,
                                envd_address = %readiness.envd_address,
                                suppressed_stale = ?stale_addr,
                                "restore Pod is kubelet-Ready but envd is not serving /__state \
                                 yet; withholding Running promotion (resume serve-readiness \
                                 gate, #560)"
                            );
                        }
                    }
                    // Issue #247 recovery: the Pod that `pod_ref` names is
                    // not Ready — gone, still booting, or unreachable
                    // (`Ok(None)` or a transient `Err`) — yet a stale
                    // `envd_address` is recorded. It can only be the
                    // pre-pause carry-over (cold start never records one
                    // before a live probe), and an unconfirmed address must
                    // not gate a promotion: `derive_trigger` would otherwise
                    // read it as readiness and flip the Sandbox to `Running`
                    // on a deleted Pod. Suppress it for *this pass only*, in
                    // memory — deliberately no `patch_status`. `pod_ref` is
                    // kept (untouched in durable status) so the next probe
                    // re-targets the same-named restore Pod and promotes once
                    // its shim restore reports Ready.
                    //
                    // Why in-memory, not a durable clear: a durable wipe on
                    // every non-answer is a write storm under apiserver
                    // flakiness (a PATCH per requeue per Resuming Sandbox),
                    // and it's unnecessary — the next requeue re-probes from
                    // scratch, so suppression is self-healing. `Err` is
                    // treated like `Ok(None)` rather than falling through to
                    // a promotion because the failure can be *partial* (the
                    // Pod GET fails while the Sandbox PATCH would still
                    // succeed); promoting on an address we could not
                    // re-validate is exactly the bug this fix closes. This is
                    // the deliberate counterpart to the `Ok(Some)` arm above,
                    // which trusts a positive Ready answer enough to keep its
                    // partial fields via `.or(...)`: a real answer is
                    // authoritative, a non-answer is not.
                    Ok(None) | Err(_) if has_addr => {
                        // Capture before the in-memory scrub so the log names
                        // exactly what was suppressed and what we keep probing
                        // — this is the one line that fires on a real #247
                        // crash recovery in prod.
                        let stale_addr =
                            sandbox.status.as_ref().and_then(|s| s.envd_address.clone());
                        let kept_pod_ref = sandbox.status.as_ref().and_then(|s| s.pod_ref.clone());
                        if let Some(status) = sandbox.status.as_mut() {
                            status.envd_address = None;
                            status.pod_uid = None;
                        }
                        tracing::warn!(
                            sandbox = %name,
                            stale_envd_address = ?stale_addr,
                            pod_ref = ?kept_pod_ref,
                            "resume readiness re-probe found no Ready Pod; suppressed stale \
                             envd_address for this pass to avoid promoting Running on a deleted Pod"
                        );
                    }
                    Ok(None) => tracing::debug!(
                        sandbox = %name,
                        "pod not yet ready; idling until next requeue"
                    ),
                    Err(e) => tracing::debug!(
                        sandbox = %name,
                        error = %e,
                        "pod_ready probe failed; will retry on requeue"
                    ),
                }

                // Project the backing Pod's phase for the gateway's
                // "still starting / not ready yet" notice (issue #625). The
                // gateway holds no pod-read RBAC (#442), so it relies on this
                // controller-written `status.podStatus` to tell a transient
                // "retry shortly" apart from a stuck `ImagePullBackOff` /
                // `CrashLoopBackOff`. Once the readiness probe above records an
                // `envd_address` the sandbox is serving and the projection is
                // cleared (the gateway routes on the address and never consults
                // it). Patched only on a real change so an in-flight or stuck
                // Pod does not generate a status write per requeue.
                let now_serving = sandbox
                    .status
                    .as_ref()
                    .and_then(|s| s.envd_address.as_deref())
                    .is_some();
                let desired_pod_status = if now_serving {
                    None
                } else {
                    match ctx.adapter.pod_status_summary(&pr).await {
                        Ok(summary) => summary,
                        // Transient GET failure: leave the last projection in
                        // place rather than blanking it on an apiserver hiccup.
                        Err(e) => {
                            tracing::debug!(
                                sandbox = %name,
                                error = %e,
                                "pod_status_summary probe failed; keeping prior projection"
                            );
                            sandbox.status.as_ref().and_then(|s| s.pod_status.clone())
                        }
                    }
                };
                let current_pod_status = sandbox.status.as_ref().and_then(|s| s.pod_status.clone());
                if desired_pod_status != current_pod_status {
                    let mut next = sandbox.status.clone().unwrap_or_default();
                    next.pod_status = desired_pod_status.clone();
                    ctx.store.patch_status(namespace, name, next).await?;
                    // Keep the in-memory view consistent for the rest of this
                    // pass; `derive_trigger` below does not key on `pod_status`,
                    // so no re-read is needed.
                    if let Some(status) = sandbox.status.as_mut() {
                        status.pod_status = desired_pod_status;
                    }
                }
            }
        }
    }

    // Issue #542: keep the enforced egress policy in step with
    // `spec.networkPolicy` for a Running sandbox. The per-sandbox
    // `NetworkPolicy` is materialized once at `create_pod` (create-or-409,
    // never reconciled) — fine while egress was set-once, but a warm-pool
    // *claim* patches `spec.networkPolicy` to union a per-create `egressAllow`
    // over the member's template baseline *without* going through `create_pod`,
    // so the live policy would otherwise stay at the baseline and silently drop
    // the override. Re-applying it here propagates the union to enforcement so a
    // claimed member's egress matches a cold-booted one.
    //
    // Gated on `spec.networkPolicy.is_some()`: a sandbox with no egress policy
    // already carries the default-deny baseline from `create_pod`, and egress is
    // only ever *widened* (set-once baseline, claim-time union), never removed —
    // so a `None` spec needs no sync. The adapter no-ops when the live policy
    // already matches, so this is at most one GET on a quiescent reconcile.
    // Best-effort: a transient sync failure is logged and retried on the next
    // requeue rather than aborting the lifecycle reconcile below.
    if current_phase == SandboxPhase::Running && sandbox.spec.network_policy.is_some() {
        if let Err(e) = ctx.adapter.ensure_egress_policy(&sandbox).await {
            tracing::warn!(
                sandbox = %name,
                error = %e,
                "failed to reconcile egress NetworkPolicy to spec; \
                 will retry on requeue (#542)"
            );
        }
    }

    // One wall-clock read per reconcile, threaded into every time-driven probe
    // (idle timeout, resume deadlines, resume backoff) so they all measure
    // against the same instant.
    let now = Utc::now();

    // PVC-retention GC (epic #603 Phase 5 / issue #608): a paused
    // `filesystem`-mode sandbox keeps its retained PVC(s) billing until
    // something reclaims them. Once it has sat past its retention window, delete
    // the Sandbox CR — the namespace garbage collector cascades the PVCs via
    // their controller ownerReferences. Gated on `phase == Paused` so a resumed
    // sandbox carrying a stale `Suspended` condition is never collected, and on
    // the opt-in `pvc_retention.enabled` switch so the default deployment is
    // unaffected. Checked before the SnapshotJob probe and trigger derivation:
    // collection is terminal, so there is no point observing jobs or computing a
    // transition for a Sandbox about to be deleted.
    if current_phase == SandboxPhase::Paused
        && pvc_retention_gc_due(&sandbox, now, &ctx.pvc_retention)
    {
        tracing::info!(
            sandbox = %name,
            "PVC-retention window elapsed for paused filesystem sandbox; deleting Sandbox \
             (cascading retained PVCs)"
        );
        ctx.store.delete(namespace, name).await?;
        // Count the reclamation so operators can alert on / trend the rate of
        // this destructive auto-deletion in aggregate, not just per-object via
        // the Event below (PR #613 review).
        crate::pvc_retention_metrics::record_reclaimed();
        if let Some(client) = &ctx.client {
            emit_event(
                client.clone(),
                &sandbox,
                EventType::Normal,
                "PvcRetentionReclaimed",
                "paused filesystem sandbox exceeded its PVC retention window; \
                 Sandbox deleted and retained PVC(s) cascade-collected"
                    .to_owned(),
            )
            .await;
        }
        return Ok(());
    }

    // Pre-dispatch SnapshotJob probe. When a job is in flight, its
    // status drives the next transition; we ignore the spec-derived
    // trigger so a user changing `desiredPhase` mid-pause doesn't
    // race with the worker.
    let job_obs = observe_snapshot_job(&ctx, namespace, &sandbox, now).await?;
    let trigger = match &job_obs {
        Some(SnapshotJobObservation::InFlight) => {
            tracing::debug!(
                sandbox = %name,
                phase = ?current_phase,
                "snapshot job in flight; idling until status update"
            );
            return Ok(());
        }
        Some(SnapshotJobObservation::Complete { trigger, .. })
        | Some(SnapshotJobObservation::Failed { trigger }) => Some(trigger.clone()),
        None => match derive_trigger(&sandbox, current_phase, now) {
            Some(t) => Some(t),
            // No spec/desired-phase trigger applies. At `Running`, two
            // time-driven probes can still fire, in priority order:
            //
            //   1. Liveness (`running_liveness_trigger`): a sandbox Pod runs
            //      envd with `restartPolicy: Never`, so if envd exits on its
            //      own (its snapshot-and-exit path) or the Pod is deleted, the
            //      Pod goes terminal and nothing else observes it — the
            //      Sandbox would linger at `Running` forever. Fold that into
            //      `Failure` so it transitions to `Failed`. A *dead* Pod wins
            //      over the idle-timeout below: pausing would try to snapshot a
            //      corpse, so `Failed` is the honest terminal state.
            //   2. Idle timeout (`timeout_trigger`, issues #327 / #481):
            //      once the live Pod has gone `spec.timeoutSeconds` with no
            //      client activity (measured from the gateway-stamped
            //      `status.lastActivityTimestamp`, or `creationTimestamp`
            //      until the first stamp lands), emit `Trigger::Timeout` so
            //      the `(Running, Timeout)` edges auto-pause (or
            //      auto-terminate) it. This is what makes
            //      `spec.timeoutSeconds` / `spec.autoPause` more than dead
            //      config.
            None if current_phase == SandboxPhase::Running => {
                match running_liveness_trigger(&ctx, &sandbox, name).await {
                    Some(t) => Some(t),
                    None => timeout_trigger(&sandbox, now),
                }
            }
            // Issue #337: a `Creating` Sandbox whose `pod_ref` is stamped
            // but whose Pod has vanished (node churn / Pod GC) or died gets
            // no trigger from `derive_trigger` and would idle at `Creating`
            // forever — the readiness probe above returns `Ok(None)` for
            // both "Pod gone" and "Pod still booting", so it can't recover
            // on its own. Confirm against the adapter and fold a
            // gone/terminal Pod into `Failure` so the Sandbox transitions to
            // `Failed` (and a warm-pool owner re-creates a replacement
            // rather than the pool wedging below `minReady`). See
            // `creating_liveness_trigger`.
            None if current_phase == SandboxPhase::Creating => {
                creating_liveness_trigger(&ctx, &sandbox, name).await
            }
            // Epic #484 (scheduler-placed restore): a `Resuming` Sandbox
            // whose restore Pod is not yet Ready gets no spec-derived trigger
            // — `derive_trigger` only emits `PodReady` here, and the Pod can't
            // be Ready before its snapshot is staged. `resuming_trigger` folds
            // the node-discovery step (`NodeAssigned`) and the bounded recovery
            // loop (`RestorePodLost` / `Failure { ResumeExhausted }`) for a
            // lost or wedged restore Pod into one probe — the recovery half is
            // what turns the `resumingforever` hang into bounded recovery.
            // #606: a `filesystem`-mode resume cold-boots a fresh Pod with no
            // snapshot, so it is told apart from the snapshot restore flow by
            // the *absence* of `status.snapshotRef`: a snapshot resume always
            // carries the ref its pause committed, while a filesystem pause
            // never sets one. The snapshot path keeps the scheduler-placed
            // restore probe (node-discovery + bounded recovery); the filesystem
            // cold boot uses its own liveness probe that, like a cold start,
            // folds a vanished Pod to `Failed`, AND \u2014 unlike a cold start \u2014
            // bounds the serve-readiness withhold with a readiness deadline
            // (#620 defense-in-depth: a persistently non-serving restore Pod
            // must never hang `Resuming` without bound again).
            None if current_phase == SandboxPhase::Resuming => {
                let has_snapshot = sandbox
                    .status
                    .as_ref()
                    .and_then(|s| s.snapshot_ref.as_deref())
                    .is_some();
                if has_snapshot {
                    resuming_trigger(&ctx, &sandbox, name, now).await
                } else {
                    filesystem_resuming_trigger(&ctx, &sandbox, name, now).await
                }
            }
            // #606: a `filesystem`-mode suspend has no `SnapshotJob`, so once the
            // `Pausing` phase is committed its completion is driven by this probe
            // rather than a job observation (the job observer short-circuits this
            // branch whenever a `snapshot_job_ref` is in flight). The probe
            // resolves the effective mode — the one reconcile site that must tell
            // a filesystem suspend apart from a snapshot pause that crashed before
            // creating its job — and folds the Pod-teardown progress into
            // `PodReleased` (Pod gone) or `PauseRequested` (re-drive the delete).
            None if current_phase == SandboxPhase::Pausing => {
                filesystem_pausing_trigger(&ctx, namespace, &sandbox, name).await
            }
            // Issue #510: `Failed` is terminal for genuine failures, but a
            // fail-closed pause checkpoint leaves the workload Pod running. The
            // recovery probe re-examines *only* the recoverable pause-checkpoint
            // class (it gates on `FailureReason::is_recoverable`) and yields
            // `FailedPodHealthy` (recover to Running) or `FailedPodOrphaned`
            // (GC the orphan) — turning the permanent-orphan sink into a
            // bounded retry → recover → GC path.
            None if current_phase == SandboxPhase::Failed => {
                failed_recovery_trigger(&ctx, &sandbox, name, now).await
            }
            None => None,
        },
    };

    let Some(trigger) = trigger else {
        tracing::debug!(
            sandbox = %name,
            phase = ?current_phase,
            "no trigger; idle"
        );
        return Ok(());
    };

    // Resolve the effective persistence mode for the pause/resume edges that
    // branch on it (epic #603 / #606). The transient `Pausing` / `Resuming`
    // phases route through their own probes above and emit only mode-agnostic
    // triggers here (`PodReleased`, `PodReady`, …), so the only triggers that
    // actually consult the mode are the user/idle pause and the resume request;
    // every other trigger ignores the argument, so the `Snapshot` default is
    // correct and issues no template GET on a steady-state reconcile.
    let persistence_mode = if matches!(
        trigger,
        Trigger::PauseRequested
            | Trigger::ResumeRequested
            | Trigger::RestoreRequested
            | Trigger::Timeout { auto_pause: true }
    ) {
        resolve_effective_persistence_mode(&ctx, namespace, &sandbox).await
    } else {
        PersistenceMode::default()
    };

    let Some((next_phase, action)) = transition(current_phase, trigger.clone(), persistence_mode)
    else {
        tracing::debug!(
            sandbox = %name,
            phase = ?current_phase,
            trigger = ?trigger,
            "no-op transition; idle"
        );
        return Ok(());
    };

    // INVARIANT #1 (spec §10): patch status to the next phase BEFORE
    // dispatching any side effect. A crash between the patch and the
    // adapter call leaves the Sandbox in the intermediate phase
    // (`Pausing`, `Resuming`, ...); the next reconcile recovers from
    // there.
    let mut new_status = sandbox.status.clone().unwrap_or_default();
    new_status.phase = next_phase;

    // Epic #480 Phase 2 (issue #631): the edge that closes a `Running` interval
    // (pause, terminate, fail) is the authoritative billing event. Emit it from the
    // OLD status — whose `last_transition_time` still holds the `Running`-entry
    // instant — BEFORE advancing that timestamp below, so the interval is bounded
    // by [entered Running, now]. Done before the invariant-#1 status patch so a
    // crash between emit and patch re-drives the same edge and re-emits the SAME
    // idempotency key (the sink overwrites, never double-bills); once the patch
    // lands the phase is no longer `Running` and the edge cannot re-fire.
    if current_phase == SandboxPhase::Running && next_phase != SandboxPhase::Running {
        emit_running_usage(&ctx, namespace, &sandbox, now).await;
    }
    // Stamp the phase-entry timestamp so the *next* `Running` close edge can bound
    // its interval, and as the conventional Kubernetes transition breadcrumb. Only
    // on an actual phase change, so steady-state status patches don't bump it.
    if next_phase != current_phase {
        new_status.last_transition_time = Some(now.to_rfc3339());
    }

    // Apply observed-job effects to the status before the first patch
    // so the cleared `snapshot_job_ref` (and any new `snapshot_ref` +
    // `snapshot_sha256`) is durable even if `dispatch()` fails.
    match &job_obs {
        Some(SnapshotJobObservation::Complete {
            snapshot_ref,
            snapshot_sha256,
            snapshot_envd_sha256,
            snapshot_runsc_version,
            local_path,
            ..
        }) => {
            new_status.snapshot_job_ref = None;
            if let Some(sr) = snapshot_ref {
                new_status.snapshot_ref = Some(sr.clone());
            }
            if let Some(sh) = snapshot_sha256 {
                new_status.snapshot_sha256 = Some(sh.clone());
            }
            if let Some(esh) = snapshot_envd_sha256 {
                new_status.snapshot_envd_sha256 = Some(esh.clone());
            }
            if let Some(rv) = snapshot_runsc_version {
                new_status.snapshot_runsc_version = Some(rv.clone());
            }
            // Plan 07 T9: a completing `Stage` job hands back the local
            // path it materialized the snapshot at. Commit it durably
            // before `Action::CreateRestorePod` consumes it so a crash
            // between this patch and pod creation recovers from the
            // recorded path rather than re-staging.
            if let Some(lp) = local_path {
                new_status.restore_staging_path = Some(lp.clone());
            }
        }
        Some(SnapshotJobObservation::Failed { .. }) => {
            new_status.snapshot_job_ref = None;
        }
        _ => {}
    }

    // Issue #407: an eviction-recovery transition stamps a user-visible
    // `StateRestored` / `StateLost` condition recording whether filesystem
    // continuity was preserved, so API consumers can observe what happened to
    // an evicted sandbox. The phase routing (`Resuming` restore vs `Creating`
    // fresh) is the state machine's job; the condition is the human-readable
    // breadcrumb derived from the same trigger the loop already holds. Stamped
    // before the invariant-#1 patch so it is durable even if `dispatch` fails.
    if let Trigger::EvictedRecover {
        has_snapshot,
        message,
    } = &trigger
    {
        push_eviction_recovery_condition(&mut new_status, *has_snapshot, message, now);
    }

    ctx.store
        .patch_status(namespace, name, new_status.clone())
        .await?;
    tracing::info!(
        sandbox = %name,
        from = ?current_phase,
        to = ?next_phase,
        "phase transition committed"
    );

    // Emit a Kubernetes Event so the transition is visible in
    // `kubectl describe sandbox <name>`. Best-effort — swallowed on
    // publish error so the reconcile loop is never blocked by events RBAC
    // or a transient apiserver hiccup.
    if let Some(client) = &ctx.client {
        let (event_type, reason, note) =
            phase_transition_event(current_phase, next_phase, &trigger);
        emit_event(client.clone(), &sandbox, event_type, reason, note).await;
    }

    // Run the action. On success, patch the post-dispatch status
    // (which may include snapshot_ref or a further phase advance).
    // On failure, run a second transition into Failed and patch that.
    //
    // Capture the action's stable label *before* `dispatch` consumes the
    // `Action`, then time the whole call and record the dispatch metric
    // (issue #373) — the metric counterpart of the `dispatch:controller::action`
    // span. Recorded regardless of outcome (failure is tracked separately as a
    // phase fold, #339), so the histogram count tracks the action counter.
    let action_label = action.label();
    let dispatch_start = std::time::Instant::now();
    let dispatch_result = dispatch(&ctx, namespace, &sandbox, action, &mut new_status, now).await;
    crate::dispatch_metrics::record_dispatch(action_label, dispatch_start.elapsed());
    match dispatch_result {
        Ok(()) => {
            ctx.store.patch_status(namespace, name, new_status).await?;
        }
        Err(action_err) => {
            tracing::warn!(
                sandbox = %name,
                error = %action_err,
                "action failed; transitioning to Failed"
            );
            let err_msg = action_err.to_string();
            if let Some((fail_phase, fail_action)) = transition(
                next_phase,
                Trigger::Failure {
                    reason: FailureReason::ActionFailure,
                    message: err_msg.clone(),
                },
                // The `Failure` fold is mode-agnostic; reuse the already-resolved
                // mode rather than re-deriving it.
                persistence_mode,
            ) {
                let mut final_status = new_status;
                final_status.phase = fail_phase;
                if let Action::RecordFailure {
                    reason: fail_reason,
                    message: fail_msg,
                } = fail_action
                {
                    push_failure_condition(&mut final_status, fail_reason, fail_msg, now);
                }
                ctx.store
                    .patch_status(namespace, name, final_status)
                    .await?;
                if let Some(client) = &ctx.client {
                    emit_event(
                        client.clone(),
                        &sandbox,
                        EventType::Warning,
                        "Failed",
                        format!("action_failure: {err_msg}"),
                    )
                    .await;
                }
            }
        }
    }

    Ok(())
}

/// Current phase as observed from `status`, defaulting to `Creating`
/// for a freshly-created Sandbox whose controller has not yet written
/// any status.
fn current_phase(sandbox: &Sandbox) -> SandboxPhase {
    sandbox
        .status
        .as_ref()
        .map_or(SandboxPhase::Creating, |s| s.phase)
}

/// Look up the Sandbox's in-flight `SnapshotJob`, if any, and classify
/// its terminal-or-not status.
///
/// Returns `Ok(None)` when no `snapshot_job_ref` is stamped (regular
/// spec-derived reconcile path), or when the stamped job is gone from
/// the cluster (treated as no observation — the caller falls back to
/// the spec-derived trigger so a stuck Sandbox can be force-recovered
/// by clearing the ref).
// No `err`: this runs on every reconcile for any Sandbox carrying a
// `snapshot_job_ref`, and a transient apiserver error is tolerated by design
// (the next requeue retries). Marking the span errored would surface routine
// hiccups as Issues; genuine failures still propagate to the parent reconcile
// span's `err`.
#[tracing::instrument(
    skip_all,
    name = "observe:controller::snapshot_job",
    fields(sandbox = %sandbox.metadata.name.as_deref().unwrap_or(""))
)]
async fn observe_snapshot_job(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
    now: DateTime<Utc>,
) -> Result<Option<SnapshotJobObservation>, ControllerError> {
    let job_name = match sandbox
        .status
        .as_ref()
        .and_then(|s| s.snapshot_job_ref.as_deref())
    {
        Some(n) => n,
        None => return Ok(None),
    };
    let job = match ctx.snapshot_jobs.get(namespace, job_name).await? {
        Some(j) => j,
        None => {
            tracing::warn!(
                sandbox = %sandbox.metadata.name.as_deref().unwrap_or(""),
                job = %job_name,
                "snapshot_job_ref points at missing job; falling back to spec-derived trigger"
            );
            return Ok(None);
        }
    };
    let status = job.status.clone().unwrap_or_default();
    let obs = match status.phase {
        SnapshotJobPhase::Complete => {
            let trigger = match job.spec.mode {
                SnapshotJobMode::Pause => Trigger::SnapshotValidated,
                // Plan 07 T9: a `Stage` completion advances the resume
                // workflow to `Action::CreateRestorePod`. `Resume` is no
                // longer dispatched by the controller (the container is
                // born restored via the shim), but it's grouped here for
                // exhaustiveness — were one observed, treating it like a
                // Stage completion is the safe fallback.
                SnapshotJobMode::Stage | SnapshotJobMode::Resume => Trigger::StageComplete,
            };
            // Only Pause completions carry a snapshot_ref + sha256 worth
            // folding into the Sandbox; `Stage`/`Resume` completions have
            // either consumed theirs or report a local path instead.
            let (snapshot_ref, snapshot_sha256, snapshot_envd_sha256, snapshot_runsc_version) =
                match job.spec.mode {
                    SnapshotJobMode::Pause => {
                        let manifest = status.manifest.as_ref();
                        let snapshot_ref = manifest
                            .map(|m| m.snapshot_ref.clone())
                            .filter(|s| !s.is_empty());
                        let snapshot_sha256 =
                            manifest.map(|m| m.sha256.clone()).filter(|s| !s.is_empty());
                        let snapshot_envd_sha256 = manifest
                            .map(|m| m.envd_sha256.clone())
                            .filter(|s| !s.is_empty());
                        let snapshot_runsc_version = manifest
                            .map(|m| m.runsc_version.clone())
                            .filter(|s| !s.is_empty());
                        (
                            snapshot_ref,
                            snapshot_sha256,
                            snapshot_envd_sha256,
                            snapshot_runsc_version,
                        )
                    }
                    SnapshotJobMode::Stage | SnapshotJobMode::Resume => (None, None, None, None),
                };
            // Only `Stage` completions report a local staging path; the
            // node worker leaves it unset for Pause/Resume.
            let local_path = match job.spec.mode {
                SnapshotJobMode::Stage => status.local_path.clone().filter(|s| !s.is_empty()),
                SnapshotJobMode::Pause | SnapshotJobMode::Resume => None,
            };
            SnapshotJobObservation::Complete {
                trigger,
                snapshot_ref,
                snapshot_sha256,
                snapshot_envd_sha256,
                snapshot_runsc_version,
                local_path,
            }
        }
        SnapshotJobPhase::Failed => {
            // Epic #484 (issue #489): a failed *Stage* job while the Sandbox is
            // `Resuming` is recoverable — the restore Pod already exists and the
            // snapshot can be re-staged onto a freshly-placed Pod — so route it
            // through the bounded recovery budget instead of folding straight to
            // `Failed`. Every other failure (a Pause/Resume job, or a Stage
            // failure observed outside `Resuming`) stays terminal. Either way
            // the surrounding `Failed` observation clears `snapshot_job_ref`, so
            // the next pass recreates from a clean slate rather than re-observing
            // the dead job.
            let recoverable_stage = job.spec.mode == SnapshotJobMode::Stage
                && current_phase(sandbox) == SandboxPhase::Resuming;
            // Issue #510 tier 1: a failed *Pause* job while `Pausing` is the
            // recoverable pause-checkpoint class — the workload Pod is still
            // running, so route the failure through the bounded retry budget
            // (re-dispatch the checkpoint) instead of folding straight to a
            // permanent terminal `Failed`. On exhaustion the recovery fn yields
            // `Failure { PauseCheckpointFailed }`, a *recoverable* fold the
            // `Failed`-phase probe later recovers or GCs.
            let recoverable_pause = job.spec.mode == SnapshotJobMode::Pause
                && current_phase(sandbox) == SandboxPhase::Pausing;
            if recoverable_stage {
                let sandbox_status = sandbox.status.clone().unwrap_or_default();
                let name = sandbox.metadata.name.as_deref().unwrap_or("");
                match resume_recovery_trigger(&sandbox_status, name, now, "resume Stage job failed")
                {
                    Some(trigger) => SnapshotJobObservation::Failed { trigger },
                    // Still inside the backoff window: idle without clearing the
                    // ref so the next requeue re-evaluates the same failed job
                    // once the backoff elapses.
                    None => SnapshotJobObservation::InFlight,
                }
            } else if recoverable_pause {
                let sandbox_status = sandbox.status.clone().unwrap_or_default();
                let name = sandbox.metadata.name.as_deref().unwrap_or("");
                let fail_msg = status
                    .message
                    .as_deref()
                    .unwrap_or("pause checkpoint failed without message");
                match pause_recovery_trigger(&sandbox_status, name, now, fail_msg) {
                    Some(trigger) => SnapshotJobObservation::Failed { trigger },
                    // Within the backoff window: idle without clearing the ref
                    // so the next requeue re-evaluates once the backoff elapses.
                    None => SnapshotJobObservation::InFlight,
                }
            } else {
                SnapshotJobObservation::Failed {
                    trigger: Trigger::Failure {
                        reason: FailureReason::SnapshotJobFailed,
                        message: status
                            .message
                            .unwrap_or_else(|| "snapshot job failed without message".into()),
                    },
                }
            }
        }
        SnapshotJobPhase::Pending
        | SnapshotJobPhase::Checkpointing
        | SnapshotJobPhase::Uploading
        | SnapshotJobPhase::Validating
        | SnapshotJobPhase::Restoring => SnapshotJobObservation::InFlight,
    };
    Ok(Some(obs))
}

/// Derive an atomic [`Trigger`] from the observed Sandbox.
///
/// Priority order matches spec §5:
///
/// 1. `metadata.deletionTimestamp` set ⇒ `KillRequested` (deletion
///    overrides every other intent).
/// 2. `status.phase == Creating`: yield `PodReady` once the Pod has
///    surfaced an `envd_address` (or the legacy `pod-ready` annotation
///    is true); yield `PodCreate` otherwise.
/// 3. Otherwise dispatch on `(phase, desired-phase)`:
///    - `(Running, Paused)`  ⇒ `PauseRequested`
///    - `(Paused,  Running)` ⇒ `ResumeRequested`, **unless** resuming now would
///      immediately re-trip the idle-timeout auto-pause
///      ([`resume_would_immediately_idle_pause`]) — see issue #654.
///
///    `desired-phase` is read from typed `spec.desired_phase` first,
///    then falls back to the legacy `sandbawks.dev/desired-phase`
///    annotation (removed in Plan 05).
/// 4. Anything else ⇒ `None` (idle).
///
/// `now` is injected (rather than read here) so the idle-deadline arithmetic
/// the resume gate shares with [`timeout_trigger`] stays clock-injected and
/// unit-testable.
fn derive_trigger(sandbox: &Sandbox, current: SandboxPhase, now: DateTime<Utc>) -> Option<Trigger> {
    if sandbox.metadata.deletion_timestamp.is_some() {
        return Some(Trigger::KillRequested);
    }

    let annotations = sandbox.metadata.annotations.as_ref();
    let desired_from_field = sandbox.spec.desired_phase;
    let desired_from_annotation =
        annotations
            .and_then(|a| a.get(ANN_DESIRED_PHASE))
            .and_then(|v| match v.as_str() {
                "Running" => Some(SandboxPhase::Running),
                "Paused" => Some(SandboxPhase::Paused),
                _ => None,
            });
    let desired = desired_from_field.or(desired_from_annotation);

    let pod_ready = sandbox
        .status
        .as_ref()
        .and_then(|s| s.envd_address.as_deref())
        .is_some()
        || annotations
            .and_then(|a| a.get(ANN_POD_READY))
            .map(String::as_str)
            == Some("true");

    // A Pod has already been created when `status.pod_ref` is set.
    // Gating `Trigger::PodCreate` on the *absence* of this prevents the
    // reconcile loop from firing a second `create_pod` while the first
    // one is still in flight — which would surface as a 409
    // AlreadyExists from the apiserver and bounce the Sandbox into
    // `Failed`.
    let has_pod_ref = sandbox
        .status
        .as_ref()
        .and_then(|s| s.pod_ref.as_deref())
        .is_some();

    // Plan 07 T9: the `Resuming` phase has two sub-steps that share a
    // single phase. The dispatch order is:
    //   1. `Action::StageSnapshot` (dispatch a `Stage` SnapshotJob), then
    //   2. `Action::CreateRestorePod` (create the restore Pod once the
    //      `Stage` job completes).
    // The pre-trigger SnapshotJob observer in `reconcile()` short-
    // circuits as long as `snapshot_job_ref` is set, so the
    // `(Resuming, PodReady)` arm here only fires *after* the restore Pod
    // exists and is Ready (job ref already cleared) — promoting the
    // born-restored container straight to `Running`.
    let has_snapshot_job_ref = sandbox
        .status
        .as_ref()
        .and_then(|s| s.snapshot_job_ref.as_deref())
        .is_some();

    // Clone-from-snapshot (issue #416): a `Creating` sandbox carrying
    // `spec.restore_from` stands up from another sandbox's saved state instead
    // of cold-booting, so it gets `RestoreRequested` where a normal sandbox gets
    // `PodCreate`. Gated on the same "no Pod yet" condition as `PodCreate` so it
    // fires exactly once at the start of the lifecycle; once the restore Pod is
    // created (`pod_ref` set) the `Resuming` probes drive the rest.
    let wants_restore = sandbox.spec.restore_from.is_some();

    match (current, desired, pod_ready, has_pod_ref) {
        (SandboxPhase::Creating, _, true, _) => Some(Trigger::PodReady),
        (SandboxPhase::Creating, _, false, false) if wants_restore => {
            Some(Trigger::RestoreRequested)
        }
        (SandboxPhase::Creating, _, false, false) => Some(Trigger::PodCreate),
        // Pod exists but not yet Ready — idle here; the pre-trigger
        // `pod_ready` probe in `reconcile()` is what advances us.
        (SandboxPhase::Creating, _, false, true) => None,
        (SandboxPhase::Running, Some(SandboxPhase::Paused), _, _) => Some(Trigger::PauseRequested),
        // Issue #654: an idle `autoPause` sandbox keeps `desiredPhase=Running`
        // through its idle-timeout pause, so this edge would re-resume it the
        // instant it lands in `Paused` — and the freshly-resumed sandbox is
        // *still* past its idle deadline (its `lastActivityTimestamp` was never
        // refreshed), so the `(Running, Timeout)` edge re-pauses it at once. The
        // two intents ping-pong forever (the #563 resume-side idle-clock re-arm
        // only widened the period to one full idle window; it never removed the
        // contradiction). Withhold the resume while resuming would *immediately*
        // re-trip the auto-pause: a genuine wake-up comes through `/connect`,
        // which stamps `lastActivityTimestamp = now` *before* flipping
        // `desiredPhase=Running` (api `connect_sandbox`), pushing the deadline a
        // full window into the future so this gate passes and the resume fires.
        // A still-idle sandbox simply stays parked (`Suspended`, PVC retained)
        // until a real client connects.
        (SandboxPhase::Paused, Some(SandboxPhase::Running), _, _)
            if !resume_would_immediately_idle_pause(sandbox, now) =>
        {
            Some(Trigger::ResumeRequested)
        }
        // Withheld idle resume (issue #654): fall through to `None` (idle) so the
        // Sandbox stays parked until real activity refreshes the baseline.
        (SandboxPhase::Resuming, _, true, _) if !has_snapshot_job_ref => Some(Trigger::PodReady),
        _ => None,
    }
}

/// Liveness probe for a `Running` Sandbox: yield [`Trigger::Failure`] when
/// the backing Pod has terminated, else `None`.
///
/// This is the `Running`-phase counterpart to the `Creating`/`Resuming`
/// readiness probe in [`reconcile`]. A sandbox Pod runs envd with
/// `restartPolicy: Never`; when envd exits (e.g. its graceful
/// snapshot-and-exit) the Pod settles at `Succeeded` and nothing else
/// observes it, so without this the Sandbox stays falsely `Running` over a
/// dead Pod. The `(Running, Failure)` edge in [`crate::state_machine`]
/// folds the returned trigger into [`SandboxPhase::Failed`].
///
/// Returns `None` (no transition) when the Pod is still live, when no
/// `pod_ref` is recorded, or when the probe errors transiently — the last
/// is logged and retried on the next requeue rather than mistaken for a
/// terminated Pod.
#[tracing::instrument(skip_all, name = "probe:controller::running_liveness", fields(sandbox = %name))]
async fn running_liveness_trigger(
    ctx: &ReconcileContext,
    sandbox: &Sandbox,
    name: &str,
) -> Option<Trigger> {
    let pod_ref = sandbox
        .status
        .as_ref()
        .and_then(|s| s.pod_ref.as_deref())
        .and_then(parse_pod_ref)?;
    match ctx.adapter.pod_terminated(&pod_ref).await {
        Ok(Some(cause)) => Some(running_termination_to_trigger(sandbox, cause, name)),
        Ok(None) => None,
        Err(e) => {
            tracing::debug!(
                sandbox = %name,
                error = %e,
                "running liveness probe failed; will retry on requeue"
            );
            None
        }
    }
}

/// Map a terminated `Running` Pod's cause to the recovery-or-fail trigger
/// (issue #407).
///
/// Resource-pressure terminations — a kubelet eviction or an OOMKill — are
/// *recoverable*: the sandbox's filesystem state must not be silently
/// discarded, so they yield [`Trigger::EvictedRecover`], which the state
/// machine routes to a snapshot restore (`status.snapshotRef` set) or a fresh
/// re-provision. Every other termination — envd's own clean snapshot-and-exit,
/// or the Pod being deleted out from under us
/// ([`PodTerminationCause::Other`]) — is a genuine failure that folds to
/// terminal `Failed` exactly as before.
///
/// The eviction / OOM detail text is built once via
/// [`termination_cause_to_failure`] (the issue #418 message) so the
/// user-visible `StateRestored` / `StateLost` condition still names the cause
/// and resource budget; on the `EvictedRecover` path the paired
/// [`FailureReason`] is intentionally dropped (the Sandbox is not failing).
///
/// ## Flapping is bounded asymmetrically
///
/// The two recovery paths cap a repeatedly-evicted Sandbox differently:
///
/// - **Snapshot-less** (`Running → Creating`): single-shot. If the fresh
///   replacement is evicted again *before* it reaches `Running`,
///   [`creating_liveness_trigger`] folds it to terminal `Failed`
///   ([`FailureReason::PodEvicted`]).
/// - **Snapshot-backed** (`Running → Resuming`): *not* outer-bounded. The
///   `Resuming` restore loop itself is bounded (`RESUME_MAX_RESTARTS` →
///   [`FailureReason::ResumeExhausted`]), but its `resume_restart_count` resets
///   to `0` on every fresh `Running → Resuming` recovery, so a Sandbox that
///   flaps `Running ↔ evicted` — reaching `Running` between evictions — can
///   re-enter restore indefinitely without ever folding to `Failed`.
///
/// This asymmetry is deliberate, not an oversight: every restore is
/// independently recoverable and leaves the Sandbox *alive* (strictly better
/// than the pre-#407 evict-to-`Failed` behavior), and an outer flap cap would
/// require a persisted cross-`Running` eviction counter — a new `status` field,
/// i.e. the CRD change this design avoids. A node under sustained pressure is
/// the scheduler's problem to drain (the restore Pod is unpinned, so it
/// re-places elsewhere); a flap that thrashes here surfaces as a rising
/// `controller.sandbox.recovery_total{outcome="eviction_restored"}` rate. If
/// that proves operationally noisy, the follow-up is a bounded eviction-flap
/// budget gated on a new status field — out of scope for #407.
fn running_termination_to_trigger(
    sandbox: &Sandbox,
    cause: PodTerminationCause,
    name: &str,
) -> Trigger {
    match cause {
        PodTerminationCause::Evicted { .. } | PodTerminationCause::OomKilled { .. } => {
            let has_snapshot = sandbox
                .status
                .as_ref()
                .and_then(|s| s.snapshot_ref.as_deref())
                .is_some();
            let (_, message) = termination_cause_to_failure(cause, "Running");
            tracing::warn!(
                sandbox = %name,
                has_snapshot,
                recovery.message = %message,
                "backing Pod of a Running Sandbox was evicted/OOMKilled; recovering \
                 (restore from snapshot when available, else re-provision fresh)"
            );
            Trigger::EvictedRecover {
                has_snapshot,
                message,
            }
        }
        PodTerminationCause::Other => {
            let (reason, message) =
                termination_cause_to_failure(PodTerminationCause::Other, "Running");
            tracing::warn!(
                sandbox = %name,
                failure.reason = reason.label(),
                failure.message = %message,
                "backing Pod terminated while Running; transitioning Sandbox to Failed"
            );
            Trigger::Failure { reason, message }
        }
    }
}

/// Liveness probe for a `Creating` Sandbox: yield [`Trigger::Failure`]
/// when the backing Pod named by `pod_ref` has vanished or gone terminal
/// before it ever became Ready, else `None`.
///
/// This closes the wedge in issue #337. The reconcile loop's readiness
/// probe consults [`RuntimeAdapter::pod_ready`], which returns `Ok(None)`
/// for *both* "Pod gone" and "Pod still booting" — so a `Creating` Sandbox
/// whose Pod was deleted out from under it (ephemeral-node churn, Pod GC,
/// a failed schedule that was reaped) would idle at `Creating` forever:
/// `derive_trigger` yields nothing for `(Creating, pod_ref set, not
/// ready)`, and nothing else fails it. For a warm-pool member that is
/// fatal — the stuck member keeps counting toward `minReady`, suppressing
/// top-up, so the whole pool wedges at `ready=0`.
///
/// [`RuntimeAdapter::pod_terminated`] *can* tell the two apart (a 404 or a
/// `Succeeded`/`Failed` phase is terminal; a `Pending`/`Running` Pod is
/// not), so reuse it here exactly as the `Running` phase does. The
/// `(Creating, Failure)` edge in [`crate::state_machine`] folds the
/// returned trigger into [`SandboxPhase::Failed`]; `bucket` then drops the
/// failed member and the warm-pool reconciler re-creates a replacement.
///
/// Returns `None` (no transition) when the Pod is still live, when no
/// `pod_ref` is recorded (nothing to probe — `derive_trigger` will emit
/// `PodCreate` instead), or when the probe errors transiently — the last
/// is logged and retried on the next requeue rather than mistaken for a
/// vanished Pod.
#[tracing::instrument(skip_all, name = "probe:controller::creating_liveness", fields(sandbox = %name))]
async fn creating_liveness_trigger(
    ctx: &ReconcileContext,
    sandbox: &Sandbox,
    name: &str,
) -> Option<Trigger> {
    let pod_ref = sandbox
        .status
        .as_ref()
        .and_then(|s| s.pod_ref.as_deref())
        .and_then(parse_pod_ref)?;
    match ctx.adapter.pod_terminated(&pod_ref).await {
        Ok(Some(cause)) => {
            // Eviction and OOM causes surface the resource-detail message and
            // use `PodEvicted` so the metric tracks resource-budget failures
            // separately from generic node-churn. `Other` (deleted Pod, envd
            // crash) stays `PodVanishedCreating`.
            let (reason, message) = match cause {
                PodTerminationCause::Other => (
                    FailureReason::PodVanishedCreating,
                    "backing Pod vanished or went terminal while Creating (node churn or Pod GC)"
                        .to_string(),
                ),
                other => termination_cause_to_failure(other, "Creating"),
            };
            tracing::warn!(
                sandbox = %name,
                failure.reason = reason.label(),
                failure.message = %message,
                "backing Pod vanished or went terminal while Creating; transitioning Sandbox to Failed"
            );
            Some(Trigger::Failure { reason, message })
        }
        Ok(None) => None,
        Err(e) => {
            tracing::debug!(
                sandbox = %name,
                error = %e,
                "creating liveness probe failed; will retry on requeue"
            );
            None
        }
    }
}

/// Liveness/recovery probe for a `filesystem`-mode resume's `Resuming` phase
/// (epic #603 / #606, hardened for issue #620).
///
/// A filesystem resume cold-boots a fresh Pod with no `runsc restore` and no
/// snapshot to stage, so it has no node-discovery step and cannot
/// checkpoint-wedge the way a snapshot restore can. Like a cold start it must
/// fold a *vanished* Pod to `Failed` ([`creating_liveness_trigger`]). But unlike
/// a cold start it has *also* passed through the #560 serve-readiness gate in
/// the readiness-probe section above, which withholds the `Resuming → Running`
/// promotion until envd answers the serve probe.
///
/// #620: that withhold previously had **no deadline** on this path \u2014
/// `creating_liveness_trigger` only recovers a Pod that is gone/terminal, never
/// one that is `1/1 Running` but persistently fails the serve probe. A
/// misconfigured probe (the #620 root cause: a 3s probe against a 33s
/// `/__state`) therefore hung the Sandbox in `Resuming` forever. The primary fix
/// repoints the gate at the fast `/__version` endpoint; this is the
/// defense-in-depth backstop, so a *future* probe misconfiguration can never
/// reproduce an unbounded hang.
///
/// The `resume_last_attempt` anchor is stamped on the resume edge
/// (`Action::CreateRestorePod`, line ~3168) for filesystem resumes just as for
/// snapshot resumes, so the bounded readiness deadline measures from a real
/// instant here. In priority order:
///
/// 1. **vanished Pod** \u2014 [`creating_liveness_trigger`] folds a gone/terminal
///    restore Pod to `Failed`.
/// 2. **readiness deadline** \u2014 a restore Pod still not promoted past
///    [`resume_ready_deadline`] (kubelet-Ready but envd never served, or any
///    other persistent withhold) folds to bounded recovery via
///    [`resume_recovery_trigger`] (`Failure { ResumeExhausted }` once the retry
///    budget is spent), exactly as the snapshot path's wedge recovery does.
/// 3. `None` (idle) \u2014 the Pod is live and within its readiness deadline.
///
/// Clock-injected (`now`) so the deadline arithmetic shares the reconcile's
/// single wall-clock read and stays unit-testable.
#[tracing::instrument(skip_all, name = "probe:controller::filesystem_resuming", fields(sandbox = %name))]
async fn filesystem_resuming_trigger(
    ctx: &ReconcileContext,
    sandbox: &Sandbox,
    name: &str,
    now: DateTime<Utc>,
) -> Option<Trigger> {
    // (1) A gone/terminal restore Pod is lost regardless of any deadline.
    if let Some(trigger) = creating_liveness_trigger(ctx, sandbox, name).await {
        return Some(trigger);
    }

    // (2) The Pod is live but not yet promoted. Bound the withhold: a Pod that
    // sits past the readiness deadline without reaching Running is treated as a
    // wedged restore and recovered, never left to hang `Resuming` forever.
    let status = sandbox.status.as_ref()?;
    let elapsed = status
        .resume_last_attempt
        .as_deref()
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| now.signed_duration_since(dt.with_timezone(&Utc)));
    if past_deadline(elapsed, resume_ready_deadline()) {
        return resume_recovery_trigger(
            status,
            name,
            now,
            "filesystem restore Pod stayed not-Running past the readiness deadline \
             (envd never passed the serve-readiness gate)",
        );
    }
    None
}

/// Completion probe for a `filesystem`-mode suspend's `Pausing` phase (epic
/// #603 Phase 3, issue #606).
///
/// Reached only when no `SnapshotJob` is in flight (the job observer
/// short-circuits otherwise) and `derive_trigger` found no edge — i.e. the
/// suspend's [`Action::SuspendPod`] delete is either done or still in progress.
/// It resolves the effective [`PersistenceMode`] because this is the one
/// reconcile site that must distinguish a filesystem suspend from a
/// `snapshot`-mode pause that crashed before creating its job; a `snapshot`
/// Sandbox here is left to idle (its crash recovery is out of #606's scope) so
/// a snapshot Sandbox's Pod is never deleted by mistake. For a `filesystem`
/// suspend it yields:
///
/// - [`Trigger::PodReleased`] once `status.podRef` is cleared (the delete
///   completed) — advance to `Paused`.
/// - [`Trigger::PauseRequested`] while a `podRef` survives — a crash left the
///   Pod alive mid-suspend, so re-drive the idempotent delete; it converges in
///   one pass once the Pod is gone.
#[tracing::instrument(skip_all, name = "probe:controller::filesystem_pausing", fields(sandbox = %name))]
async fn filesystem_pausing_trigger(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
    name: &str,
) -> Option<Trigger> {
    if resolve_effective_persistence_mode(ctx, namespace, sandbox).await
        != PersistenceMode::Filesystem
    {
        return None;
    }
    let has_pod = sandbox
        .status
        .as_ref()
        .and_then(|s| s.pod_ref.as_deref())
        .is_some();
    if has_pod {
        tracing::info!(
            sandbox = %name,
            "filesystem suspend: Pod still present (crash mid-suspend); re-driving delete"
        );
        Some(Trigger::PauseRequested)
    } else {
        Some(Trigger::PodReleased)
    }
}

/// Resolve a Sandbox's effective [`PersistenceMode`] (epic #603 / #606 / #616).
///
/// An explicit `spec.persistenceMode` wins with no I/O; otherwise the
/// `SandboxTemplate` is consulted — the same template→sandbox inheritance
/// [`resolve_pod_inputs`] applies to the runtime class and the PVC requests. The
/// template's explicit mode wins next, then the VCT inference (issue #616):
/// when neither side sets the mode but the effective `volumeClaimTemplates` is
/// non-empty the mode resolves to `filesystem`, exactly as `resolve_pod_inputs`
/// stamps it at the pod edge — so the pause/resume decision here agrees with the
/// mode the Pod was actually materialized under rather than diverging back to
/// the bare `snapshot` default. A missing template or a transient read error
/// falls back to [`PersistenceMode::default`] (`Snapshot`): the
/// behavior-preserving default for a sandbox whose template we cannot read.
///
/// The effective VCT set mirrors `resolve_pod_inputs`: an *absent* Sandbox list
/// inherits the template's wholesale; an *explicit* list is merged under the
/// template's `volumeClaimTemplatesPolicy`. Inference only consults it when both
/// modes are unset, so the merge runs only on that branch.
///
/// Called only at the pause/resume decision points and during a filesystem
/// suspend's transient `Pausing` phase, so it adds no template GET to a
/// steady-state reconcile.
async fn resolve_effective_persistence_mode(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
) -> PersistenceMode {
    if let Some(mode) = sandbox.spec.persistence_mode {
        return mode;
    }
    match ctx
        .templates
        .get(namespace, sandbox.spec.template_ref.as_str())
        .await
    {
        Ok(Some(template)) => {
            // Compute the effective VCT set the same way `resolve_pod_inputs`
            // does so the inference sees what the Pod was provisioned with.
            let has_volume_claims = match sandbox.spec.volume_claim_templates.as_deref() {
                None => template
                    .spec
                    .volume_claim_templates
                    .as_deref()
                    .is_some_and(|vcts| !vcts.is_empty()),
                Some(tenant) => {
                    let outcome = merge_volume_claim_templates(
                        template
                            .spec
                            .volume_claim_templates
                            .as_deref()
                            .unwrap_or_default(),
                        tenant,
                        template
                            .spec
                            .volume_claim_templates_policy
                            .unwrap_or_default(),
                    );
                    !outcome.effective.is_empty()
                }
            };
            sandbawks_types::resolve_effective_persistence_mode(
                sandbox.spec.persistence_mode,
                template.spec.persistence_mode,
                has_volume_claims,
            )
        }
        Ok(None) => PersistenceMode::default(),
        Err(e) => {
            tracing::debug!(
                sandbox = %sandbox.metadata.name.as_deref().unwrap_or(""),
                error = %e,
                "persistence-mode template read failed; defaulting to Snapshot"
            );
            PersistenceMode::default()
        }
    }
}

/// Combined node-discovery + liveness/recovery probe for a `Resuming` Sandbox
/// whose restore Pod exists but is not yet Ready (epic #484, issue #489).
///
/// This is the `Resuming`-phase counterpart to [`creating_liveness_trigger`],
/// extended with the scheduler-placed restore's node-discovery step. Resume now
/// creates the restore Pod *before* staging the snapshot (create-then-stage), so
/// the controller has to (a) learn which node the scheduler placed the Pod on in
/// order to stage onto *that* node, and (b) notice when the Pod is lost or
/// wedged and recover. In priority order it yields:
///
/// 1. **recovery** — when the restore Pod went terminal/404, sat `Pending` past
///    [`resume_schedule_deadline`] (no node had capacity), or stayed
///    scheduled-but-not-Ready past [`resume_ready_deadline`] (wedged restore).
///    [`resume_recovery_trigger`] folds that into `RestorePodLost` (recreate
///    unpinned) — or `Failure { ResumeExhausted }` once the retry budget is
///    spent, which is the edge that turns the old `resumingforever` hang into a
///    bounded, terminal failure.
/// 2. [`Trigger::NodeAssigned`] — the scheduler has bound the Pod and no node is
///    recorded yet, so dispatch the node-local `Stage` job onto that node. Once
///    a node is recorded the staging step owns the flow, so this fires only in
///    the narrow create→stage window.
/// 3. `None` (idle) — the Pod is healthy and progressing within its deadlines,
///    no `pod_ref` is recorded, or a probe errored transiently (logged and
///    retried on the next requeue rather than mistaken for a loss).
///
/// Only reached when no `snapshot_job_ref` is in flight (the SnapshotJob
/// observer short-circuits otherwise) and `derive_trigger` found no `PodReady`
/// edge — i.e. the restore Pod is genuinely not Ready this pass.
///
/// `now` is injected (rather than read here) so the deadline/backoff arithmetic
/// shares the reconcile's single wall-clock read.
#[tracing::instrument(skip_all, name = "probe:controller::resuming", fields(sandbox = %name))]
async fn resuming_trigger(
    ctx: &ReconcileContext,
    sandbox: &Sandbox,
    name: &str,
    now: DateTime<Utc>,
) -> Option<Trigger> {
    let status = sandbox.status.as_ref()?;
    let pod_ref = status.pod_ref.as_deref().and_then(parse_pod_ref)?;

    // (1a) A terminal/404 restore Pod is lost regardless of any deadline —
    // recover immediately (subject to the backoff/budget in the recovery fn).
    match ctx.adapter.pod_terminated(&pod_ref).await {
        Ok(Some(_cause)) => {
            return resume_recovery_trigger(status, name, now, "restore Pod went terminal or 404");
        }
        Ok(None) => {}
        Err(e) => {
            tracing::debug!(
                sandbox = %name,
                error = %e,
                "resume liveness probe (pod_terminated) failed; will retry on requeue"
            );
            return None;
        }
    }

    // Time the current restore Pod has been alive — the anchor for both the
    // schedule and readiness deadlines is the last (re)creation attempt. Absent
    // (legacy CR / a crash before the first stamp) ⇒ unmeasurable this pass, so
    // only the terminal check above can fire.
    let elapsed = status
        .resume_last_attempt
        .as_deref()
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| now.signed_duration_since(dt.with_timezone(&Utc)));

    match ctx.adapter.pod_scheduled_node(&pod_ref).await {
        // Scheduled. Node-discovery owns dispatch while no node is recorded;
        // once it is (staging dispatched/done), a Pod still not-Ready past the
        // readiness deadline is a wedged restore → recover.
        Ok(Some(node)) => {
            if status.assigned_node.is_none() {
                tracing::info!(
                    sandbox = %name,
                    node = %node,
                    "restore Pod scheduled; dispatching node-local Stage job"
                );
                return Some(Trigger::NodeAssigned);
            }
            if past_deadline(elapsed, resume_ready_deadline()) {
                return resume_recovery_trigger(
                    status,
                    name,
                    now,
                    "restore Pod stayed scheduled-but-not-Ready past the readiness deadline",
                );
            }
            None
        }
        // Not scheduled yet. `Pending` past the schedule deadline means no node
        // had capacity → recover (recreate unpinned so the scheduler re-places
        // it, likely elsewhere).
        Ok(None) => {
            if past_deadline(elapsed, resume_schedule_deadline()) {
                return resume_recovery_trigger(
                    status,
                    name,
                    now,
                    "restore Pod stayed Pending past the schedule deadline",
                );
            }
            tracing::debug!(
                sandbox = %name,
                "restore Pod not yet scheduled to a node; idling until next requeue"
            );
            None
        }
        Err(e) => {
            tracing::debug!(
                sandbox = %name,
                error = %e,
                "pod_scheduled_node probe failed; will retry on requeue"
            );
            None
        }
    }
}

/// Whether `elapsed` (time since the last restore-Pod attempt) has reached
/// `deadline`. A `None` elapsed — no anchor stamped — is treated as "not yet":
/// the deadline is unmeasurable, so we decline to fire on it. A negative
/// `elapsed` (anchor in the future, e.g. clock skew) likewise does not fire.
fn past_deadline(elapsed: Option<chrono::Duration>, deadline: Duration) -> bool {
    elapsed
        .and_then(|e| e.to_std().ok())
        .is_some_and(|e| e >= deadline)
}

/// Capped exponential backoff for the `count`-th retry:
/// `min(RESUME_BACKOFF_BASE * 2^count, RESUME_BACKOFF_CAP)`.
///
/// `count` is bounded by the relevant retry budget ([`resume_max_restarts`] /
/// [`pause_max_retries`]) before this is reached, so the shift can't
/// realistically overflow; the `min(..)` clamp and the `checked_mul` fallback
/// guard it defensively regardless. Shared by the `Resuming` recovery loop and
/// the tier-1 pause-checkpoint retry loop (issue #510).
fn resume_backoff(count: u32) -> Duration {
    let factor = 1u32.checked_shl(count).unwrap_or(u32::MAX);
    RESUME_BACKOFF_BASE
        .checked_mul(factor)
        .unwrap_or(RESUME_BACKOFF_CAP)
        .min(RESUME_BACKOFF_CAP)
}

/// Decide the recovery trigger for a restore Pod observed lost mid-`Resuming`,
/// under the bounded retry budget (epic #484, issue #489).
///
/// Returns:
///
/// - `Some(Trigger::Failure { ResumeExhausted })` once `resume_restart_count`
///   has reached [`resume_max_restarts`] — the `(Resuming, Failure)` edge folds
///   the Sandbox to `Failed`, ending the `resumingforever` hang with a clear
///   terminal cause instead of retrying forever.
/// - `None` while still inside the capped-exponential backoff window anchored at
///   `status.resume_last_attempt` — the loop idles until a later requeue so a
///   fast-dying restore Pod can't burn the whole budget in one burst.
/// - `Some(Trigger::RestorePodLost)` otherwise — recreate the Pod unpinned.
///
/// Pure and clock-injected (`now`) so the budget/backoff logic is unit-testable
/// without a real wall clock. `lost_reason` is woven into the log/condition so
/// an operator can tell *why* the restore was abandoned.
fn resume_recovery_trigger(
    status: &SandboxStatus,
    name: &str,
    now: DateTime<Utc>,
    lost_reason: &str,
) -> Option<Trigger> {
    let count = status.resume_restart_count;
    if count >= resume_max_restarts() {
        tracing::warn!(
            sandbox = %name,
            resume_restart_count = count,
            lost_reason,
            "restore Pod lost and recreated the maximum number of times without reaching \
             Running; folding Sandbox to Failed (ResumeExhausted)"
        );
        return Some(Trigger::Failure {
            reason: FailureReason::ResumeExhausted,
            message: format!(
                "restore Pod lost and recreated {count} times without reaching Running \
                 (most recent loss: {lost_reason}); giving up"
            ),
        });
    }
    // Capped exponential backoff anchored at the last (re)creation attempt.
    // No anchor (first attempt not yet stamped) ⇒ no wait this pass.
    if let Some(anchor) = status
        .resume_last_attempt
        .as_deref()
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&Utc))
    {
        let backoff = chrono::Duration::from_std(resume_backoff(count))
            .unwrap_or_else(|_| chrono::Duration::seconds(0));
        if let Some(ready_at) = anchor.checked_add_signed(backoff) {
            if now < ready_at {
                tracing::debug!(
                    sandbox = %name,
                    resume_restart_count = count,
                    "within resume recovery backoff window; idling until next requeue"
                );
                return None;
            }
        }
    }
    tracing::info!(
        sandbox = %name,
        resume_restart_count = count,
        lost_reason,
        "restore Pod lost; recreating unpinned under bounded recovery"
    );
    Some(Trigger::RestorePodLost)
}

/// Decide the recovery trigger for a pause checkpoint observed failed while the
/// Sandbox is `Pausing`, under the bounded tier-1 retry budget (issue #510).
///
/// This is the pause-side analogue of [`resume_recovery_trigger`]. Returns:
///
/// - `Some(Trigger::Failure { PauseCheckpointFailed })` once
///   `pause_restart_count` has reached [`pause_max_retries`] — the
///   `(Pausing, Failure)` edge folds the Sandbox to `Failed`. Crucially the
///   fold carries the *recoverable* reason, so the `Failed`-phase probe
///   ([`failed_recovery_trigger`]) can still recover the workload to `Running`
///   or GC the orphan rather than leaving it stuck forever.
/// - `None` while still inside the capped-exponential backoff window anchored
///   at `status.pause_last_attempt` — idle until a later requeue so a
///   fast-failing checkpoint can't burn the whole budget in one burst. The
///   caller keeps `snapshot_job_ref` set in this case so the next pass
///   re-evaluates the same failed job once the backoff elapses.
/// - `Some(Trigger::PauseRetry)` otherwise — re-dispatch the pause checkpoint.
///
/// Pure and clock-injected (`now`) so the budget/backoff logic is
/// unit-testable. `fail_msg` is the node worker's failure message, woven into
/// the terminal condition so an operator sees *why* the checkpoint was
/// abandoned (e.g. `checkpoint failed: envd /__state timed out after 10s`).
fn pause_recovery_trigger(
    status: &SandboxStatus,
    name: &str,
    now: DateTime<Utc>,
    fail_msg: &str,
) -> Option<Trigger> {
    let count = status.pause_restart_count;
    if count >= pause_max_retries() {
        tracing::warn!(
            sandbox = %name,
            pause_restart_count = count,
            fail_msg,
            "pause checkpoint failed the maximum number of times; folding Sandbox to Failed \
             (PauseCheckpointFailed, recoverable — the workload Pod is left running)"
        );
        return Some(Trigger::Failure {
            reason: FailureReason::PauseCheckpointFailed,
            message: format!(
                "pause checkpoint failed {count} times without success \
                 (most recent: {fail_msg}){loc}; leaving the workload Pod running",
                loc = pause_failure_location(status),
            ),
        });
    }
    // Capped exponential backoff anchored at the last (re)dispatch attempt.
    // No anchor (first attempt not yet stamped) ⇒ no wait this pass.
    if let Some(anchor) = status
        .pause_last_attempt
        .as_deref()
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&Utc))
    {
        let backoff = chrono::Duration::from_std(resume_backoff(count))
            .unwrap_or_else(|_| chrono::Duration::seconds(0));
        if let Some(ready_at) = anchor.checked_add_signed(backoff) {
            if now < ready_at {
                tracing::debug!(
                    sandbox = %name,
                    pause_restart_count = count,
                    "within pause-checkpoint retry backoff window; idling until next requeue"
                );
                return None;
            }
        }
    }
    tracing::info!(
        sandbox = %name,
        pause_restart_count = count,
        fail_msg,
        "pause checkpoint failed; re-dispatching under bounded tier-1 retry"
    );
    Some(Trigger::PauseRetry)
}

/// Build a human-readable "where" suffix for a pause-failure CR condition,
/// drawn from status fields the controller already holds.
///
/// The node's `HttpEnvdProbe` deliberately strips the pod IP from its own
/// error message so Traceway groups every pause-checkpoint timeout into one
/// exception class instead of one-per-sandbox (#434). That keeps the grouped
/// telemetry useful but leaves the Sandbox CR condition — what an operator
/// reads with `kubectl describe sandbox` — unable to say *which* host was
/// unreachable (#528). This helper closes that gap on the controller side:
/// `assigned_node` and `envd_address` are independent `status` fields, so
/// weaving them into the condition message diagnoses the failure in the
/// Kubernetes object **without** touching the node's Traceway-grouped error.
///
/// `envd_address` is the controller-tracked `host:port` of the sandbox's envd
/// (the node probes the same host); it is the answer to "which host was
/// unreachable". Returns an empty string when neither field is known, so the
/// condition degrades gracefully to its un-located form.
fn pause_failure_location(status: &SandboxStatus) -> String {
    match (
        status.assigned_node.as_deref(),
        status.envd_address.as_deref(),
    ) {
        (Some(node), Some(envd)) => format!("; sandbox envd was at {envd} on node {node}"),
        (None, Some(envd)) => format!("; sandbox envd was at {envd}"),
        (Some(node), None) => format!("; sandbox was on node {node}"),
        (None, None) => String::new(),
    }
}

/// Combined recover/GC probe for a `Failed` Sandbox (issue #510, tiers 2+3).
///
/// `Failed` is a terminal sink for every genuine failure, but a fail-closed
/// pause checkpoint deliberately leaves the workload Pod running — so a Sandbox
/// folded with the recoverable [`FailureReason::PauseCheckpointFailed`] class
/// has a live (or recently-live) Pod the controller must reconcile rather than
/// orphan forever. This probe is the only emitter of the two scoped `Failed`
/// exit edges, and it gates strictly on [`FailureReason::is_recoverable`] so
/// create/terminate failures stay terminal.
///
/// In priority order it yields:
///
/// 1. `None` — the fold was not the recoverable class, or no `pod_ref` is
///    recorded (nothing to reconcile; the GC path already cleared it). The
///    Sandbox stays `Failed`.
/// 2. [`Trigger::FailedPodOrphaned`] — the Pod is already terminal/gone (GC to
///    clear the dangling status and reclaim any slot), **or** the Pod's
///    container has stayed un-`Ready` past [`failed_gc_grace`] measured from
///    `status.failed_at` (the orphan is unrecoverable; delete it to reclaim the
///    node slot).
/// 3. [`Trigger::FailedPodHealthy`] — the Pod's container is `Ready` (the
///    kubelet readiness condition via `pod_ready` → `Some`) **and** no explicit
///    pause is still desired: treat the live Pod as authoritative and recover
///    to `Running`. The pause simply did not happen.
///
/// # Caveats
///
/// - `pod_ready` reports only the Kubernetes **container `Ready` condition**
///   (kubelet readiness), *not* envd's pause-time `/__state` health — that
///   probe lives in `sandbawks-node`, not the controller. So a Pod whose
///   `/__state` is wedged (the originating incident — container `Ready` while
///   `/__state` timed out, contributing trigger #356) is still recovered to
///   `Running` here. The re-armed idle clock keeps that from tight-looping; a
///   genuine envd-liveness gate is out of scope.
/// - Recovery is **suppressed while an explicit `spec.desiredPhase=Paused` (or
///   the legacy annotation) is still pending**: recovering to `Running` would
///   immediately re-trip `(Running, Paused) => PauseRequested` and oscillate
///   `Running → Pausing → Failed → recover → …` (idle re-arming only defends
///   the `Timeout` trigger). Leaving such a Sandbox `Failed` is honest — the
///   user-requested pause genuinely could not checkpoint — and stable; the GC
///   path still reclaims it if its Pod later dies.
///
/// A transient probe error is logged and yields `None` (retry on the next
/// requeue) rather than being mistaken for a dead Pod.
/// Whether the Sandbox's *desired* phase resolves to `Paused` — typed
/// `spec.desired_phase` first, then the legacy
/// [`ANN_DESIRED_PHASE`] annotation, matching [`derive_trigger`]'s resolution.
///
/// Used by [`failed_recovery_trigger`] to suppress tier-2 recovery while an
/// explicit user pause is still pending (issue #510): recovering to `Running`
/// would just re-trip `(Running, Paused) => PauseRequested` and oscillate.
fn desired_phase_is_paused(sandbox: &Sandbox) -> bool {
    let from_field = sandbox.spec.desired_phase;
    let from_annotation = sandbox
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_DESIRED_PHASE))
        .and_then(|v| match v.as_str() {
            "Running" => Some(SandboxPhase::Running),
            "Paused" => Some(SandboxPhase::Paused),
            _ => None,
        });
    matches!(from_field.or(from_annotation), Some(SandboxPhase::Paused))
}

#[tracing::instrument(skip_all, name = "probe:controller::failed_recovery", fields(sandbox = %name))]
async fn failed_recovery_trigger(
    ctx: &ReconcileContext,
    sandbox: &Sandbox,
    name: &str,
    now: DateTime<Utc>,
) -> Option<Trigger> {
    let status = sandbox.status.as_ref()?;
    // Gate: only the recoverable pause-checkpoint failure class is re-examined.
    // Everything else stays a terminal sink.
    let recoverable = status
        .failure_reason
        .as_deref()
        .and_then(FailureReason::from_label)
        .is_some_and(FailureReason::is_recoverable);
    if !recoverable {
        return None;
    }
    let pod_ref = status.pod_ref.as_deref().and_then(parse_pod_ref)?;

    // (1) A terminal/404 Pod is an unrecoverable orphan regardless of the grace
    // window — GC it to clear the dangling reference and reclaim any slot.
    match ctx.adapter.pod_terminated(&pod_ref).await {
        Ok(Some(_cause)) => {
            tracing::info!(
                sandbox = %name,
                "Failed Sandbox's pause-checkpoint Pod is terminal/gone; GC'ing the orphan"
            );
            return Some(Trigger::FailedPodOrphaned);
        }
        Ok(None) => {}
        Err(e) => {
            tracing::debug!(
                sandbox = %name,
                error = %e,
                "failed-recovery probe (pod_terminated) failed; will retry on requeue"
            );
            return None;
        }
    }

    // (2) Pod is alive. If its container is Ready, the workload is intact →
    // recover — unless the user still explicitly wants it Paused, in which case
    // recovering would just re-enter Pausing and oscillate (see the docstring).
    match ctx.adapter.pod_ready(&pod_ref).await {
        Ok(Some(_readiness)) => {
            if desired_phase_is_paused(sandbox) {
                tracing::debug!(
                    sandbox = %name,
                    "Failed Sandbox's container is Ready but an explicit pause is still \
                     desired; leaving it Failed rather than recovering into a pause→fail loop"
                );
                return None;
            }
            tracing::info!(
                sandbox = %name,
                "Failed Sandbox's workload Pod container is Ready; recovering to Running \
                 (the pause checkpoint never happened)"
            );
            Some(Trigger::FailedPodHealthy)
        }
        // Pod alive but its container is not Ready. GC only once it has stayed
        // un-Ready past the grace window; until then idle so a slow-to-recover
        // Pod gets a chance to come back and be recovered instead.
        Ok(None) => {
            let stuck_for = status
                .failed_at
                .as_deref()
                .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                .map(|dt| now.signed_duration_since(dt.with_timezone(&Utc)));
            if past_deadline(stuck_for, failed_gc_grace()) {
                tracing::warn!(
                    sandbox = %name,
                    "Failed Sandbox's Pod stayed envd-unreachable past the GC grace window; \
                     GC'ing the orphan to reclaim the node slot"
                );
                Some(Trigger::FailedPodOrphaned)
            } else {
                tracing::debug!(
                    sandbox = %name,
                    "Failed Sandbox's envd not yet serving; within GC grace window, idling"
                );
                None
            }
        }
        Err(e) => {
            tracing::debug!(
                sandbox = %name,
                error = %e,
                "failed-recovery probe (pod_ready) failed; will retry on requeue"
            );
            None
        }
    }
}

/// Map a [`PodTerminationCause`] to a `(FailureReason, message)` pair for
/// use in [`Trigger::Failure`].
///
/// `phase` is the Sandbox phase the Pod was in when it terminated
/// (`"Running"` or `"Creating"`), used to make the message contextual.
///
/// This is the single place that translates Pod-level eviction/OOMKill
/// signals into user-facing error text (issue #418). Keeping it here — rather
/// than inside each liveness-trigger function — means the mapping is tested
/// independently and consistent across both callers.
fn termination_cause_to_failure(
    cause: PodTerminationCause,
    phase: &str,
) -> (FailureReason, String) {
    match cause {
        PodTerminationCause::Evicted { message } => (
            FailureReason::PodEvicted,
            format!("sandbox was evicted while {phase}: {message}"),
        ),
        PodTerminationCause::OomKilled {
            memory_limit: Some(limit),
        } => (
            FailureReason::PodEvicted,
            format!("sandbox exceeded its {limit} memory budget while {phase} (OOMKilled)"),
        ),
        PodTerminationCause::OomKilled { memory_limit: None } => (
            FailureReason::PodEvicted,
            format!("sandbox was OOMKilled while {phase} (no memory limit recorded in Pod spec)"),
        ),
        PodTerminationCause::Other => (
            FailureReason::PodTerminatedRunning,
            format!("backing Pod terminated while {phase} (envd exited or Pod deleted)"),
        ),
    }
}

/// The instant the **idle clock** is measured from for a `Running`
/// Sandbox: the last observed client activity
/// ([`SandboxStatus::last_activity_timestamp`], stamped by the API
/// gateway on every proxied envd request), or — when no activity has been
/// stamped yet — the Sandbox's `creationTimestamp`.
///
/// Falling back to `creationTimestamp` keeps a freshly-created or
/// freshly-claimed-but-untouched sandbox aging out on schedule (no
/// regression for abandoned claims), while a sandbox under a steady
/// stream of requests has its baseline continually pushed forward so it
/// never auto-pauses mid-use (issue #481).
///
/// Returns `None` only when neither source is available — a CR the
/// apiserver has not yet stamped with a `creationTimestamp` and which
/// also carries no activity stamp — leaving the deadline undefined, so we
/// decline to fire rather than guess.
fn activity_baseline(sandbox: &Sandbox) -> Option<DateTime<Utc>> {
    // A stamped activity timestamp wins; a malformed value (should never
    // happen — the gateway writes `Utc::now().to_rfc3339()`) is ignored so
    // it degrades to the creation fallback rather than disabling the
    // timeout outright.
    let stamped = sandbox
        .status
        .as_ref()
        .and_then(|status| status.last_activity_timestamp.as_deref())
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&Utc));
    stamped.or_else(|| sandbox.metadata.creation_timestamp.as_ref().map(|t| t.0))
}

/// The wall-clock instant a `Running` Sandbox is due to auto-pause or
/// auto-terminate under the **idle-timeout** model (issue #481):
/// [`activity_baseline`] `+ spec.timeoutSeconds`.
///
/// `spec.timeoutSeconds` is re-interpreted as an *idle window* — the span
/// of inactivity tolerated before the sandbox is paused (`autoPause`) or
/// terminated — rather than an absolute lifetime from creation. Any real
/// request to the sandbox refreshes the baseline (via the gateway's
/// activity stamp), pushing this deadline forward, so a continuously-used
/// sandbox never expires. Explicit `set_timeout` still grows the window.
///
/// Returns `None` when the timeout is disabled or uncomputable:
///
/// - `timeout_seconds == 0` is the **disabled sentinel**. Warm-pool
///   members are minted with `timeout_seconds: 0` and only get a real
///   value stamped when a user claims them, so treating `0` as "expire
///   now" would insta-pause the whole warm pool. `0` therefore means "no
///   idle timeout."
/// - No [`activity_baseline`] (a CR with neither an activity stamp nor a
///   `creationTimestamp` yet), or an arithmetic overflow on the addition —
///   both leave the deadline undefined, so we decline to fire rather than
///   guess.
fn timeout_deadline(sandbox: &Sandbox) -> Option<DateTime<Utc>> {
    let timeout_seconds = sandbox.spec.timeout_seconds;
    if timeout_seconds == 0 {
        return None;
    }
    let baseline = activity_baseline(sandbox)?;
    // `i64::from(u32)` is lossless and `checked_add_signed` guards the
    // (astronomically unlikely) far-future overflow rather than panicking.
    baseline.checked_add_signed(chrono::Duration::seconds(i64::from(timeout_seconds)))
}

/// Whether resuming this `Paused` Sandbox *now* would immediately re-trip its
/// idle-timeout auto-pause — the thrash condition behind issue #654.
///
/// True only when **both** hold:
///
/// - `spec.autoPause` is set. Without it the `(Running, Timeout)` edge
///   terminates rather than pauses, so an idle sandbox never lands back in
///   `Paused` to thrash; gating on it guarantees a non-`autoPause` resume is
///   never withheld.
/// - The idle deadline ([`timeout_deadline`]: `lastActivityTimestamp +
///   spec.timeoutSeconds`) is already in the past at `now`. A genuine `/connect`
///   stamps `lastActivityTimestamp = now` *before* requesting the resume, so its
///   deadline is a full window in the future and this returns `false`. A
///   `timeout_seconds == 0` (disabled) sandbox has no deadline
///   ([`timeout_deadline`] is `None`) and so is never withheld either.
///
/// When this is `true` the resume is withheld and the Sandbox stays parked
/// (`Suspended`, PVC retained) until real client activity refreshes the baseline.
fn resume_would_immediately_idle_pause(sandbox: &Sandbox, now: DateTime<Utc>) -> bool {
    sandbox.spec.auto_pause && timeout_deadline(sandbox).is_some_and(|deadline| now >= deadline)
}

/// Idle-timeout probe for a `Running` Sandbox: yield
/// [`Trigger::Timeout`] once `now` reaches the
/// [`timeout_deadline`], else `None` (issue #327).
///
/// Pure and clock-injected (`now` is a parameter) so the deadline logic is
/// unit-testable without a real wall clock. The `(Running, Timeout)` edges
/// in [`crate::state_machine`] fold the returned trigger into `Pausing`
/// (when `spec.autoPause`) or `Terminating` (otherwise) — mirroring
/// `auto_pause` straight off the spec.
fn timeout_trigger(sandbox: &Sandbox, now: DateTime<Utc>) -> Option<Trigger> {
    let deadline = timeout_deadline(sandbox)?;
    (now >= deadline).then_some(Trigger::Timeout {
        auto_pause: sandbox.spec.auto_pause,
    })
}

/// Requeue delay for a `Running` Sandbox, tightened so the idle-timeout
/// (issue #327) fires *near* its deadline instead of up to a full
/// `heartbeat` late.
///
/// The `Running` heartbeat is otherwise a flat 30s (it drives the liveness
/// probe). Relying on it alone to catch the timeout would let a sandbox
/// overrun its `endAt` by almost the whole interval. Instead, when a
/// deadline is in the future we requeue at the time remaining (capped at
/// `heartbeat` so a far-off deadline keeps the normal cadence, floored at
/// 1s so a near deadline can't spin into a hot loop). A deadline already in
/// the past requeues at the 1s floor so the very next pass fires the
/// timeout. With the timeout disabled or uncomputable
/// ([`timeout_deadline`] is `None`) the cadence is the unchanged
/// `heartbeat`.
///
/// Clock-injected (`now` is a parameter) for the same testability reason as
/// [`timeout_trigger`].
pub fn running_requeue_interval(
    sandbox: &Sandbox,
    now: DateTime<Utc>,
    heartbeat: Duration,
) -> Duration {
    const FLOOR: Duration = Duration::from_secs(1);
    match timeout_deadline(sandbox) {
        // `to_std` fails for a negative (past) deadline; both that and a
        // sub-second remainder collapse to the 1s floor so we re-probe
        // promptly without busy-looping.
        Some(deadline) => deadline
            .signed_duration_since(now)
            .to_std()
            .map_or(FLOOR, |remaining| remaining.clamp(FLOOR, heartbeat)),
        None => heartbeat,
    }
}

/// The instant a `filesystem`-mode sandbox finished suspending — read from the
/// `Suspended=True` status condition's transition time — or `None` if this is
/// not a paused filesystem sandbox (epic #603 Phase 5 / issue #608).
///
/// The `Suspended=True` condition is stamped only by the filesystem-suspend
/// finalize path (`Action::FinalizeSuspend`), never by a snapshot-mode pause.
/// Its presence therefore simultaneously (a) confirms a filesystem sandbox
/// whose retained PVC(s) are billing and (b) anchors the retention clock at the
/// pause moment. `upsert_condition` advances the transition time only on a
/// status *change*, so the anchor stays put while the sandbox remains Paused; a
/// later resume-then-re-pause restamps it, resetting the window.
///
/// Callers MUST still gate on `phase == Paused`: a resumed sandbox can carry a
/// stale `Suspended=True` condition until its next suspend overwrites it, and
/// only a *paused* sandbox is a GC candidate.
fn filesystem_suspended_at(sandbox: &Sandbox) -> Option<DateTime<Utc>> {
    sandbox
        .status
        .as_ref()?
        .conditions
        .iter()
        .find(|c| c.kind == "Suspended" && c.status == "True")
        .and_then(|c| c.last_transition_time.as_deref())
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// Resolve the effective PVC-retention window for one paused filesystem sandbox
/// (epic #603 Phase 5 / issue #608), or `None` if it should not be collected.
///
/// Returns `None` — meaning "never GC this sandbox" — when retention is disabled
/// controller-wide, or when an explicit `spec.pvcRetention` fails to parse. The
/// parse-error case fails *safe*: a typo'd duration logs a warning and leaves
/// the sandbox alone rather than risking deletion under a guessed window.
///
/// Resolution order (the `filesystem`-mode analog of
/// [`crate::effective_retention`]): explicit `spec.pvcRetention` →
/// controller-wide `cfg.default_duration`. The per-namespace annotation layer
/// the snapshot sweeper has is deferred here: the reconcile loop does not fetch
/// the Namespace, and adding a per-paused-reconcile GET to read it is not worth
/// it for an opt-in GC.
fn resolve_pvc_retention(sandbox: &Sandbox, cfg: &PvcRetentionConfig) -> Option<Duration> {
    if !cfg.enabled {
        return None;
    }
    match sandbox.spec.pvc_retention.as_deref() {
        Some(raw) => match sandbawks_types::parse_iso8601_duration(raw) {
            Ok(d) => Some(d),
            Err(e) => {
                tracing::warn!(
                    sandbox = ?sandbox.metadata.name,
                    raw,
                    error = %e,
                    "invalid spec.pvcRetention; skipping PVC-retention GC for this sandbox"
                );
                None
            }
        },
        None => Some(cfg.default_duration),
    }
}

/// Whether a paused filesystem sandbox is past its PVC-retention window and due
/// for garbage collection right now (epic #603 Phase 5 / issue #608).
///
/// Combines [`filesystem_suspended_at`] (the anchor + filesystem marker) with
/// [`resolve_pvc_retention`] (the window). `false` whenever retention is
/// disabled, the sandbox is not a paused-filesystem one, or the window has not
/// yet elapsed. The caller still owns the `phase == Paused` gate.
fn pvc_retention_gc_due(sandbox: &Sandbox, now: DateTime<Utc>, cfg: &PvcRetentionConfig) -> bool {
    let (Some(suspended_at), Some(window)) = (
        filesystem_suspended_at(sandbox),
        resolve_pvc_retention(sandbox, cfg),
    ) else {
        return false;
    };
    match suspended_at.checked_add_signed(
        chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::days(365 * 1000)),
    ) {
        Some(deadline) => now >= deadline,
        // Overflow (astronomically large window): treat as "never expires".
        None => false,
    }
}

/// Requeue cadence for a `Paused` sandbox so the PVC-retention GC fires near its
/// deadline rather than a heartbeat late (epic #603 Phase 5 / issue #608).
///
/// Mirrors [`running_requeue_interval`]: when a paused filesystem sandbox has a
/// resolvable retention deadline in the future, requeue at `min(remaining,
/// heartbeat)` floored at 1s so the GC pass lands promptly; otherwise (retention
/// disabled, snapshot-mode pause, or no deadline) fall back to the heartbeat.
pub fn paused_requeue_interval(
    sandbox: &Sandbox,
    now: DateTime<Utc>,
    cfg: &PvcRetentionConfig,
    heartbeat: Duration,
) -> Duration {
    const FLOOR: Duration = Duration::from_secs(1);
    let (Some(suspended_at), Some(window)) = (
        filesystem_suspended_at(sandbox),
        resolve_pvc_retention(sandbox, cfg),
    ) else {
        return heartbeat;
    };
    let Ok(window) = chrono::Duration::from_std(window) else {
        return heartbeat;
    };
    match suspended_at.checked_add_signed(window) {
        Some(deadline) => deadline
            .signed_duration_since(now)
            .to_std()
            .map_or(FLOOR, |remaining| remaining.clamp(FLOOR, heartbeat)),
        None => heartbeat,
    }
}

/// Adapter-call boxed error — only used internally inside `dispatch`
/// where we need to unify `RuntimeError`, `&'static str`, and future
/// failure sources behind a single sum.
type DispatchError = Box<dyn std::error::Error + Send + Sync>;

/// Parse the `"{namespace}/{name}"` form the controller writes to
/// [`SandboxStatus::pod_ref`] back into a typed [`PodRef`].
///
/// Returns `None` if the input is missing the `/` separator — callers
/// (`Action::Noop`, `Action::Terminate`) treat that as "no Pod to act
/// on" and silently skip the adapter call.
fn parse_pod_ref(s: &str) -> Option<PodRef> {
    let (namespace, name) = s.split_once('/')?;
    Some(PodRef {
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    })
}

/// Construct the `SnapshotJob` CR the controller dispatches for a
/// pause or resume cycle.
///
/// Name is `<sandbox>-<mode>-<epoch-millis>` so multiple in-flight jobs
/// for the same Sandbox don't collide. `epoch-millis` collisions across
/// reconciles are tolerated as idempotent re-creates by the underlying
/// store impls (see [`crate::store::KubeSnapshotJobStore::create`]).
#[tracing::instrument(
    skip_all,
    name = "build:controller::snapshot_job",
    fields(sandbox = %sandbox_name, mode = ?mode)
)]
fn build_snapshot_job(
    sandbox: &Sandbox,
    sandbox_name: &str,
    new_status: &SandboxStatus,
    mode: SnapshotJobMode,
) -> SnapshotJob {
    let mode_slug = match mode {
        SnapshotJobMode::Pause => "pause",
        SnapshotJobMode::Resume => "resume",
        SnapshotJobMode::Stage => "stage",
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let job_name = format!("{sandbox_name}-{mode_slug}-{now_ms}");

    // The `snapshot_ref` for stage/resume jobs comes from
    // `Sandbox.status.snapshot_ref` (where the prior Pause cycle stored
    // the manifest URL); Pause jobs omit it entirely. The companion
    // `expected_sha256` / `expected_envd_sha256` fields carry the
    // controller's committed fingerprints forward from the same Pause
    // completion (issue #171 for the aggregate digest, issue #278 for
    // the envd digest — the node DaemonSet uses both to cross-check the
    // re-fetched manifest before any restore work).
    let (snapshot_ref, expected_sha256, expected_envd_sha256) = match mode {
        SnapshotJobMode::Pause => (None, None, None),
        // The controller builds Pause/Stage jobs today (Plan 07 T9);
        // `Resume` is grouped with Stage for exhaustiveness because, like
        // Stage, it needs the upstream snapshot URL (and committed
        // fingerprints) to know what to materialize.
        SnapshotJobMode::Stage | SnapshotJobMode::Resume => (
            new_status.snapshot_ref.clone(),
            new_status.snapshot_sha256.clone(),
            new_status.snapshot_envd_sha256.clone(),
        ),
    };

    // Scheduler-placed restore (epic #484): a `Stage` job materializes the
    // snapshot at the *deterministic* per-sandbox path the restore Pod's
    // `restore-image-path` annotation was already stamped with at create
    // time — so the node lays the image out exactly where the shim will
    // look. Pause/Resume jobs don't target a fixed path.
    let target_path = match mode {
        SnapshotJobMode::Stage => Some(restore_staging_path(sandbox_name)),
        SnapshotJobMode::Pause | SnapshotJobMode::Resume => None,
    };

    // Carry the reconcile's trace context onto the job so the per-node
    // worker's reconcile (sandbawks-node) continues this trace across the
    // controller→node hop the SnapshotJob queue otherwise breaks (OTel Phase
    // 5, OBSERVABILITY.md §6.3). `inject_into_annotations` is a no-op outside
    // a span, so a job built with tracing disabled carries no annotation.
    let mut annotations = BTreeMap::new();
    sandbawks_observability::inject_into_annotations(&mut annotations);

    SnapshotJob {
        metadata: ObjectMeta {
            name: Some(job_name),
            namespace: sandbox.metadata.namespace.clone(),
            annotations: (!annotations.is_empty()).then_some(annotations),
            ..Default::default()
        },
        spec: SnapshotJobSpec {
            sandbox_name: sandbox_name.to_owned(),
            node_name: new_status.assigned_node.clone().unwrap_or_default(),
            mode,
            container_id: new_status.pod_uid.clone().unwrap_or_default(),
            snapshot_ref,
            expected_sha256,
            expected_envd_sha256,
            target_path,
        },
        status: None,
    }
}

/// Default root under which restore snapshots are staged on each node's local
/// disk, mirroring the `sandbawks-node` DaemonSet's `hostPathStagingCache`
/// (`deploy/helm/sandbawks/values.yaml`). Overridable via
/// [`RESTORE_STAGING_ROOT_ENV`]; read once through [`restore_staging_root`].
const DEFAULT_RESTORE_STAGING_ROOT: &str = "/var/lib/sandbawks/staging";

/// Env var overriding [`DEFAULT_RESTORE_STAGING_ROOT`]. Helm sources this from
/// `node.hostPathStagingCache` so the controller's recorded host path tracks the
/// node's staging hostPath (issue #490).
///
/// Relocatable as of issue #512: the same `node.hostPathStagingCache` value
/// drives this env, the node DaemonSet `staging-cache` mountPath/hostPath, and
/// the node binary's `SANDBAWKS_STAGING_ROOT` (all via the `sandbawks.stagingRoot`
/// chart helper), so the three agree by construction and an operator relocates
/// the root by overriding that one value. Must be an absolute path — the host
/// shim reads the recorded `io.sandbawks/restore-image-path` annotation verbatim
/// off the node filesystem; enforced render-side by the helper and at startup by
/// [`ensure_restore_staging_root_absolute`].
const RESTORE_STAGING_ROOT_ENV: &str = "SANDBAWKS_RESTORE_STAGING_ROOT";

/// The effective staging root — [`RESTORE_STAGING_ROOT_ENV`] (when set to a
/// non-blank value) or [`DEFAULT_RESTORE_STAGING_ROOT`]. Read once and cached:
/// the value is fixed for the controller's lifetime, and the cached `String`
/// lives for `'static` so callers can borrow it without re-allocating.
fn restore_staging_root() -> &'static str {
    static VALUE: OnceLock<String> = OnceLock::new();
    VALUE
        .get_or_init(|| {
            string_from_env(
                |k| std::env::var(k).ok(),
                RESTORE_STAGING_ROOT_ENV,
                DEFAULT_RESTORE_STAGING_ROOT,
            )
        })
        .as_str()
}

/// Validate a staging-root value is an absolute path. Factored out as a pure
/// predicate over `&str` (rather than reading the env) so it is unit-testable
/// without touching the process-global [`restore_staging_root`] cache.
fn ensure_staging_root_absolute(root: &str) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        std::path::Path::new(root).is_absolute(),
        "{RESTORE_STAGING_ROOT_ENV} must be an absolute path (got {root:?}): the restore \
         shim reads the recorded io.sandbawks/restore-image-path annotation verbatim off \
         the node filesystem, so a relative root records an unresolvable path"
    );
    Ok(())
}

/// Fail fast at controller startup if [`RESTORE_STAGING_ROOT_ENV`] resolves to a
/// relative path, mirroring the node binary's startup assertion (issue #512,
/// PR #513 review). Returns the validated root for the caller to log.
///
/// Helm feeds this env and the node's `SANDBAWKS_STAGING_ROOT` from the same
/// guarded `sandbawks.stagingRoot` helper, so the deployed path is already safe;
/// this is defense-in-depth for a direct/standalone controller invocation that
/// bypasses Helm and would otherwise silently record a relative annotation.
pub fn ensure_restore_staging_root_absolute() -> Result<&'static str, anyhow::Error> {
    let root = restore_staging_root();
    ensure_staging_root_absolute(root)?;
    Ok(root)
}

/// Deterministic, per-sandbox local path the restore snapshot is staged at
/// (`<stagingRoot>/<sandbox-id>/restore`).
///
/// Scheduler-placed restore (epic #484) creates the restore Pod *before*
/// the snapshot is staged, so the Pod's `io.sandbawks/restore-image-path`
/// annotation and the later `Stage` job's `spec.target_path` must agree on
/// a path computable from the sandbox name alone — they're both derived
/// here. Made a free function (not a closure) so the unit test can pin the
/// exact layout the node DaemonSet is expected to stage into.
fn restore_staging_path(sandbox_name: &str) -> String {
    format!("{}/{sandbox_name}/restore", restore_staging_root())
}

/// Resolve the inputs [`RuntimeAdapter::create_pod`] needs from a
/// Sandbox's `SandboxTemplate`: the workload container image (`spec.image`
/// — the customer's rootfs) and the Sandbox with its effective runtime
/// class filled in.
///
/// envd is not part of the image: the `sandbawks-node` DaemonSet stages
/// the static envd binary onto the node and the Pod mounts it read-only
/// (see [`crate::PodAdapter`]). A missing template is a hard error
/// rather than a silent fall back to a default image, so a Sandbox
/// pointing at a nonexistent template surfaces a clear failure instead
/// of cold-booting some default rootfs that lacks the template's tooling.
///
/// # Runtime class resolution (issue #289)
///
/// `SandboxTemplate.spec.defaultRuntimeClass` is folded into the Sandbox's
/// `spec.runtimeClassName` here so **every** path that materializes a Pod
/// — gateway, warm-pool, or a hand-applied `kubectl apply` CR — inherits
/// the template's runtime class (e.g. `gvisor`). Resolving it at this edge
/// rather than only in the gateway closes the footgun where a Sandbox that
/// omits `runtimeClassName` boots under the cluster default (runc), which
/// silently breaks pause/resume because runsc has no state for the
/// container. An explicit `runtimeClassName` on the CR always wins; the
/// template default only fills an empty slot.
///
/// Returns a [`Cow`] so the common case (explicit class set, or the
/// template default empty) borrows the Sandbox without cloning — only an
/// actual override allocates an owned copy.
#[tracing::instrument(
    skip_all,
    name = "resolve:controller::pod_inputs",
    err,
    fields(
        sandbox = %sandbox.metadata.name.as_deref().unwrap_or(""),
        template = %sandbox.spec.template_ref,
    )
)]
async fn resolve_pod_inputs<'s>(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &'s Sandbox,
) -> Result<(String, Cow<'s, Sandbox>, Vec<VctSuppression>), DispatchError> {
    let template_ref = sandbox.spec.template_ref.as_str();
    let template = ctx
        .templates
        .get(namespace, template_ref)
        .await?
        .ok_or_else(|| format!("SandboxTemplate '{template_ref}' not found"))?;
    let mut spec = template.spec;
    // Own the trimmed runtime class up front: the PVC-policy step below takes a
    // `&mut spec` (`Option::take`), so a live `&spec.default_runtime_class`
    // borrow would conflict. Cloning one short string is cheaper than threading
    // the borrow.
    let default_class = spec.default_runtime_class.trim().to_owned();

    // Three template defaults can fill *empty* slots on the Sandbox before a
    // Pod is born: the runtime class (issue #289) and the scheduling hints
    // `tolerations` / `nodeSelector` (issue #592). Each only fills an empty
    // slot — an explicit value on the CR always wins, the same override
    // precedence across all three. Resolving the scheduling hints at this
    // single edge (rather than at every mint path) means cold-start *and*
    // warm-pool members inherit them, since both materialize their Pod through
    // here, mirroring the runtime-class rationale below.
    //
    // Note the `is_none()` test keys off *absence*, not emptiness: an explicit
    // `Some(vec![])` / `Some({})` on the CR reads as "set" and so suppresses
    // the template default. That is deliberate — it lets a user opt a single
    // sandbox out of the template's inherited tolerations/nodeSelector by
    // setting an empty collection, distinct from an *absent* field which
    // inherits. (`None` ⇒ inherit; `Some(empty)` ⇒ explicitly none.)
    let fill_class = sandbox.spec.runtime_class_name.is_none() && !default_class.is_empty();
    let fill_tolerations = sandbox.spec.tolerations.is_none() && spec.tolerations.is_some();
    let fill_node_selector = sandbox.spec.node_selector.is_none() && spec.node_selector.is_some();
    // Pod securityContext inherits on the same empty-slot rule (issue #617):
    // `None` on the CR inherits the template's; an explicit value wins.
    let fill_security_context =
        sandbox.spec.security_context.is_none() && spec.security_context.is_some();
    // PVC requests resolve in two steps (epic #603 Phase 5 / issue #608):
    //   - An *absent* tenant list (`None`) inherits the template's wholesale,
    //     the same empty-slot rule as the scheduling hints. There is no tenant
    //     input to govern, so no policy check and no suppressions.
    //   - An *explicit* tenant list (`Some`, even empty) is reconciled against
    //     the template's under the template's `volumeClaimTemplatesPolicy`
    //     (default `Disallowed`). `merge_volume_claim_templates` is the single
    //     choke point — cold-start, warm-pool, and hand-applied CRs all pass
    //     through here — returning the effective set plus any claims the policy
    //     suppressed for the caller to surface as a status condition.
    let vct_policy = spec.volume_claim_templates_policy.unwrap_or_default();
    let (resolved_vcts, vct_suppressions) = match sandbox.spec.volume_claim_templates.as_deref() {
        None => (spec.volume_claim_templates.take(), Vec::new()),
        Some(tenant) => {
            let outcome = merge_volume_claim_templates(
                spec.volume_claim_templates.as_deref().unwrap_or_default(),
                tenant,
                vct_policy,
            );
            (Some(outcome.effective), outcome.suppressed)
        }
    };
    // Stamp the resolved PVC set onto the Sandbox whenever either side carried
    // claims: inheriting a template list, or merging an explicit tenant list
    // (which `Disallowed` may have reduced to the template baseline). When
    // neither side has claims, `resolved_vcts` is `None` and there is nothing
    // to apply — the borrow path below stays untouched.
    let set_vcts = resolved_vcts.is_some();

    // Persistence selector resolves at this single edge (epic #603 / issue #616).
    // `resolve_effective_persistence_mode` centralizes the rule shared with the
    // gateway's `admit_persistence_mode` so the two cannot drift: an explicit
    // Sandbox mode wins, else an explicit template mode, else `filesystem` when
    // the *effective* VCT set is non-empty (declared storage is itself the
    // signal to opt into filesystem persistence — VCTs are meaningless under
    // `snapshot`), else the `snapshot` default. `has_volume_claims` keys off the
    // already-merged `resolved_vcts`, not the raw template list, so the
    // inference sees exactly what the adapter will provision.
    let has_volume_claims = resolved_vcts
        .as_deref()
        .is_some_and(|vcts| !vcts.is_empty());
    let effective_persistence = sandbawks_types::resolve_effective_persistence_mode(
        sandbox.spec.persistence_mode,
        spec.persistence_mode,
        has_volume_claims,
    );
    // Stamp the resolved mode whenever it differs from the CR's current slot —
    // i.e. the CR was unset and inherited the template/inference, or carried a
    // value the resolution leaves untouched (in which case this is a no-op).
    // Comparing against `Some(effective_persistence)` keeps an explicit CR value
    // from being needlessly re-stamped while ensuring an inferred mode reaches
    // both the #607 guard below and the adapter.
    let fill_persistence = sandbox.spec.persistence_mode != Some(effective_persistence);

    let effective = if fill_class
        || fill_tolerations
        || fill_node_selector
        || fill_security_context
        || fill_persistence
        || set_vcts
    {
        // At least one template default applies — clone the Sandbox once and
        // stamp every resolved field so the adapter materializes the Pod with
        // them. Log the runtime class: the controller is the new place this
        // resolution happens for non-gateway Sandboxes, so an operator
        // debugging "why is this Pod under runsc when I never asked for it"
        // gets a signal in the reconcile trace.
        let mut owned = sandbox.clone();
        if fill_class {
            tracing::debug!(
                runtime_class = %default_class,
                template = %template_ref,
                "applied template defaultRuntimeClass to Sandbox",
            );
            owned.spec.runtime_class_name = Some(default_class.to_owned());
        }
        // Move (not clone) the template's scheduling hints onto the Sandbox:
        // the owned `template.spec` is dropped at function end (only `image`
        // is read out below), so there is no aliasing and no extra allocation.
        if fill_tolerations {
            owned.spec.tolerations = spec.tolerations;
        }
        if fill_node_selector {
            owned.spec.node_selector = spec.node_selector;
        }
        if fill_security_context {
            owned.spec.security_context = spec.security_context;
        }
        // `persistence_mode` is `Copy`. Stamp the resolved effective mode
        // (explicit-wins, else template, else VCT inference) so the adapter and
        // the #607 guard below both see it. Log at debug when an *unset* CR mode
        // resolved to `filesystem` purely from the declared `volumeClaimTemplates`
        // (issue #616), so the implicit choice is traceable alongside the
        // runtime-class resolution above.
        if fill_persistence {
            if sandbox.spec.persistence_mode.is_none()
                && spec.persistence_mode.is_none()
                && effective_persistence == PersistenceMode::Filesystem
            {
                tracing::debug!(
                    template = %template_ref,
                    "inferred persistenceMode 'filesystem' from non-empty volumeClaimTemplates",
                );
            }
            owned.spec.persistence_mode = Some(effective_persistence);
        }
        if set_vcts {
            owned.spec.volume_claim_templates = resolved_vcts;
        }
        Cow::Owned(owned)
    } else {
        // Explicit values win, or the template sets none: nothing to
        // resolve, so borrow the Sandbox as-is.
        Cow::Borrowed(sandbox)
    };

    // Guardrail (epic #603 Phase 4 / issue #607): reject `snapshot`-mode on a
    // Kata runtime class here, the single edge where *both* the persistence
    // mode and the runtime class are fully resolved (template defaults filled).
    // Every pod-materializing path — gateway cold-start, warm-pool mint, and a
    // hand-applied `kubectl apply` CR — flows through here, so this catches the
    // direct-CR case the gateway's admission check (which only sees API
    // requests) cannot. Kata's memory checkpoint can't be driven by the
    // containerd shim, so failing now surfaces a clear Sandbox-status error
    // rather than fail-closing deep in the node's pause path. `None` mode
    // resolves to the `snapshot` default *only when no VCTs are declared*; a
    // Kata template that declares `volumeClaimTemplates` but omits the mode now
    // resolves to `filesystem` (issue #616) and passes this guard, while one
    // that declares neither still resolves to `snapshot` and is rejected until
    // it opts into `filesystem`.
    effective_persistence
        .validate_for_runtime_class(effective.spec.runtime_class_name.as_deref())?;

    Ok((spec.image, effective, vct_suppressions))
}

/// Stamp the `VolumeClaimTemplatesPolicy` status condition recording that the
/// template's policy suppressed one or more of the Sandbox's explicit PVC
/// claims (epic #603 Phase 5 / issue #608).
///
/// A no-op when nothing was suppressed: the common case (no tenant claims, or a
/// fully-permitted set) leaves the status untouched rather than stamping a
/// reassuring-but-noisy "all good" condition on every Sandbox. When claims
/// *were* dropped, the condition is the only signal a tenant gets that their
/// `volumeClaimTemplates` did not all take effect — surfaced rather than
/// failed, so the sandbox still runs on the template baseline. Upserted (not
/// pushed) so re-resolving on a later reconcile/resume does not stack
/// duplicates.
fn stamp_vct_policy_condition(
    new_status: &mut SandboxStatus,
    suppressions: &[VctSuppression],
    now: DateTime<Utc>,
) {
    if suppressions.is_empty() {
        return;
    }
    // List "<name> (<reason-label>)" for each dropped claim so the operator can
    // see both what was lost and why without cross-referencing the policy.
    let detail = suppressions
        .iter()
        .map(|s| format!("{} ({})", s.name, s.reason.label()))
        .collect::<Vec<_>>()
        .join(", ");
    upsert_condition(
        new_status,
        "VolumeClaimTemplatesPolicy",
        "False",
        "ClaimsSuppressed",
        &format!(
            "{} tenant volumeClaimTemplate(s) dropped by the template's policy: {detail}",
            suppressions.len()
        ),
        now,
    );
}

/// Map a state-machine transition to the Kubernetes Event fields.
///
/// All `Trigger` variants are matched explicitly so new variants added
/// to the state machine produce a compile error here rather than silently
/// falling through to a generic message.
fn phase_transition_event(
    from: SandboxPhase,
    _to: SandboxPhase,
    trigger: &Trigger,
) -> (EventType, &'static str, String) {
    match trigger {
        Trigger::PodCreate => (
            EventType::Normal,
            "PodCreated",
            "Pod submitted for scheduling".into(),
        ),
        Trigger::PodReady => match from {
            SandboxPhase::Resuming => (
                EventType::Normal,
                "ResumeSucceeded",
                "Sandbox Running after restore".into(),
            ),
            SandboxPhase::Creating
            | SandboxPhase::Running
            | SandboxPhase::Pausing
            | SandboxPhase::Paused
            | SandboxPhase::Terminating
            | SandboxPhase::Failed => (
                EventType::Normal,
                "PodReady",
                "Pod reached Ready; sandbox is Running".into(),
            ),
        },
        Trigger::PauseRequested => (
            EventType::Normal,
            "PauseRequested",
            "Pause requested by user".into(),
        ),
        Trigger::ResumeRequested => (
            EventType::Normal,
            "ResumeRequested",
            "Resume requested — restore Pod being scheduled".into(),
        ),
        Trigger::RestoreRequested => (
            EventType::Normal,
            "RestoreFromRequested",
            "Clone-from-snapshot requested — restoring from source sandbox".into(),
        ),
        Trigger::KillRequested => (
            EventType::Normal,
            "TerminateRequested",
            "Termination requested".into(),
        ),
        Trigger::Timeout { auto_pause: true } => (
            EventType::Normal,
            "AutoPause",
            "Idle timeout reached; auto-pausing sandbox".into(),
        ),
        Trigger::Timeout { auto_pause: false } => (
            EventType::Normal,
            "Timeout",
            "Timeout reached; terminating sandbox".into(),
        ),
        Trigger::SnapshotValidated => (
            EventType::Normal,
            "Paused",
            "Snapshot validated and committed; sandbox Paused".into(),
        ),
        Trigger::NodeAssigned => (
            EventType::Normal,
            "NodeAssigned",
            "Restore Pod scheduled; staging snapshot on target node".into(),
        ),
        Trigger::StageComplete => (
            EventType::Normal,
            "SnapshotStaged",
            "Snapshot staged on node; restore Pod unblocked".into(),
        ),
        Trigger::RestorePodLost => (
            EventType::Warning,
            "RestorePodLost",
            "Restore Pod was lost; recreating under bounded recovery".into(),
        ),
        Trigger::PauseRetry => (
            EventType::Warning,
            "PauseRetry",
            "Checkpoint attempt failed; retrying under bounded recovery".into(),
        ),
        Trigger::PodReleased => (
            EventType::Normal,
            "Suspended",
            "Filesystem suspend complete; Pod released, PVC(s) retained".into(),
        ),
        Trigger::FailedPodHealthy => (
            EventType::Normal,
            "RecoverySucceeded",
            "Workload Pod is healthy; sandbox recovered to Running".into(),
        ),
        Trigger::FailedPodOrphaned => (
            EventType::Warning,
            "OrphanedPodGC",
            "Workload Pod is unrecoverable; reclaiming orphan node slot".into(),
        ),
        Trigger::EvictedRecover {
            has_snapshot: true,
            message,
        } => (
            EventType::Warning,
            "EvictionRecovery",
            format!("Pod evicted — restoring from snapshot ({message})"),
        ),
        Trigger::EvictedRecover {
            has_snapshot: false,
            message,
        } => (
            EventType::Warning,
            "EvictionRecovery",
            format!("Pod evicted with no snapshot — re-provisioning ({message})"),
        ),
        Trigger::Failure { reason, message } => (
            EventType::Warning,
            "Failed",
            format!("{}: {message}", reason.label()),
        ),
    }
}

/// Publish a Kubernetes Event on a Sandbox object.
///
/// Returns `()` unconditionally — publish errors are logged at `WARN` and
/// discarded. The `-> ()` return type is the structural proof that a publish
/// failure cannot propagate to the caller; the reconcile loop is never
/// interrupted by a transient API hiccup or an RBAC gap.
async fn emit_event(
    client: kube::Client,
    sandbox: &Sandbox,
    event_type: EventType,
    reason: &'static str,
    note: String,
) {
    let reporter = Reporter {
        controller: "sandbawks-controller".into(),
        instance: std::env::var("POD_NAME").ok(),
    };
    let recorder = Recorder::new(client, reporter, sandbox.object_ref(&()));
    if let Err(e) = recorder
        .publish(Event {
            type_: event_type,
            reason: reason.into(),
            note: Some(note),
            action: reason.into(),
            secondary: None,
        })
        .await
    {
        tracing::warn!(
            sandbox = %sandbox.metadata.name.as_deref().unwrap_or(""),
            reason = %reason,
            error = %e,
            "failed to publish sandbox event (best-effort; reconcile continues)"
        );
    }
}

/// Record a `Failed` fold: emit the OTel counter, persist the structured
/// failure reason + timestamp, and append the condition.
///
/// Called from two sites — the `Action::RecordFailure` dispatch arm and
/// the dispatch-error recovery block — to keep the invariant "every recorded
/// failure both increments the counter and appends a `Failed` condition" in
/// one place.
///
/// Issue #510: the bounded `reason` label is also persisted to
/// `status.failure_reason` (so the `Failed`-phase recovery probe can tell the
/// recoverable pause-checkpoint class apart from genuinely terminal failures),
/// and `now` is stamped into `status.failed_at` (the anchor the tier-3 GC grace
/// window measures from).
/// Set a status condition keyed by `kind`, replacing any existing entry of the
/// same kind rather than appending a duplicate (epic #603 / #606).
///
/// The `Failed` / eviction-recovery pushers append unconditionally because a
/// fold is one-shot, but the filesystem suspend's `Suspended` condition can
/// re-fire across the crash-recovery re-drive. Upserting keeps a single
/// authoritative `Suspended` row whose `status` flips `False` (suspending) →
/// `True` (suspended) rather than accreting duplicates on every pass.
///
/// `last_transition_time` advances only when `status` actually changes, per the
/// Kubernetes condition convention — so the re-drive (`SuspendPod` re-running
/// with `status="False"` each crash-recovery pass) refreshes the reason/message
/// without bumping the transition timestamp on every no-op pass.
fn upsert_condition(
    status: &mut SandboxStatus,
    kind: &str,
    cond_status: &str,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) {
    match status.conditions.iter_mut().find(|c| c.kind == kind) {
        Some(existing) => {
            if existing.status != cond_status {
                existing.last_transition_time = Some(now.to_rfc3339());
            }
            existing.status = cond_status.to_owned();
            existing.reason = Some(reason.to_owned());
            existing.message = Some(message.to_owned());
        }
        None => status.conditions.push(SandboxCondition {
            kind: kind.to_owned(),
            status: cond_status.to_owned(),
            reason: Some(reason.to_owned()),
            message: Some(message.to_owned()),
            last_transition_time: Some(now.to_rfc3339()),
        }),
    }
}

fn push_failure_condition(
    status: &mut SandboxStatus,
    reason: FailureReason,
    message: String,
    now: DateTime<Utc>,
) {
    crate::sandbox_failure_metrics::record_failure(reason);
    status.failure_reason = Some(reason.label().to_owned());
    status.failed_at = Some(now.to_rfc3339());
    status.conditions.push(SandboxCondition {
        kind: "Failed".into(),
        status: "True".into(),
        reason: Some("ActionFailure".into()),
        message: Some(message),
        last_transition_time: None,
    });
}

/// Stamp the `StateRestored` / `StateLost` condition for an eviction-recovery
/// transition and record the recovery metric (issue #407).
///
/// `has_snapshot` selects which: a snapshot-backed eviction recovers by
/// restoring (`StateRestored`, filesystem continuity preserved); a
/// snapshot-less one re-provisions clean (`StateLost`, in-flight state
/// discarded). `detail` is the kubelet's resource-detail text (the issue #418
/// eviction / OOM message), appended in parentheses so the user can see *why*
/// the Pod was evicted alongside *what happened* to their state.
///
/// The condition follows the conventional Kubernetes shape (`status: "True"`,
/// a stable `reason` code, a human-readable `message`); it is additive — a
/// later recovery pushes another condition rather than mutating this one — so
/// the CR's condition history reads as a timeline, matching how
/// [`push_failure_condition`] and the tier-3 `OrphanReclaimed` condition behave.
fn push_eviction_recovery_condition(
    status: &mut SandboxStatus,
    has_snapshot: bool,
    detail: &str,
    now: DateTime<Utc>,
) {
    let (kind, reason, summary, outcome) = if has_snapshot {
        (
            "StateRestored",
            "EvictedRestoredFromSnapshot",
            "backing Pod was evicted; restoring filesystem state from the last snapshot",
            crate::sandbox_recovery_metrics::RecoveryOutcome::EvictionRestored,
        )
    } else {
        (
            "StateLost",
            "EvictedNoSnapshot",
            "backing Pod was evicted with no snapshot to restore from; provisioning a fresh \
             sandbox — in-flight filesystem state was lost",
            crate::sandbox_recovery_metrics::RecoveryOutcome::EvictionStateLost,
        )
    };
    crate::sandbox_recovery_metrics::record_recovery(outcome);
    status.conditions.push(SandboxCondition {
        kind: kind.into(),
        status: "True".into(),
        reason: Some(reason.into()),
        message: Some(format!("{summary} ({detail})")),
        last_transition_time: Some(now.to_rfc3339()),
    });
}

/// Resolve a sandbox's workload image and *effective* runtime class for the
/// clone-from-snapshot compatibility gates (issue #416).
///
/// Mirrors how [`resolve_pod_inputs`] fills the runtime class: an explicit
/// `spec.runtimeClassName` wins, else the template's `defaultRuntimeClass`
/// (blank → cluster default, represented as `None`). Returning both in one
/// template GET keeps the gate to a single lookup per sandbox.
async fn resolve_image_and_runtime_class(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
) -> Result<(String, Option<String>), DispatchError> {
    let template_ref = sandbox.spec.template_ref.as_str();
    let template = ctx
        .templates
        .get(namespace, template_ref)
        .await?
        .ok_or_else(|| format!("SandboxTemplate '{template_ref}' not found"))?;
    let runtime_class = sandbox.spec.runtime_class_name.clone().or_else(|| {
        let default = template.spec.default_runtime_class.trim();
        (!default.is_empty()).then(|| default.to_owned())
    });
    Ok((template.spec.image, runtime_class))
}

/// Validate a clone's runtime class — and, for snapshot mode, its template
/// image — against the restore source (issue #416).
///
/// A clone must run on the same runtime as its source (a gVisor snapshot is not
/// restorable under Kata, and a cloned PVC must mount under a compatible
/// runtime). For a snapshot (memory-image) clone the workload image must match
/// too: a `runsc restore` reconnects the workload's code-page mappings, so a
/// different image would restore corrupt — `check_image` is `true` for the
/// snapshot path and `false` for the filesystem path (a fresh cold boot over
/// cloned data tolerates an image change). Any mismatch returns `Err`, which the
/// dispatch caller folds to `Failed` with this message via the normal
/// action-error path.
async fn gate_restore_compatibility(
    ctx: &ReconcileContext,
    namespace: &str,
    new_sandbox: &Sandbox,
    source: &Sandbox,
    source_name: &str,
    check_image: bool,
) -> Result<(), DispatchError> {
    let (new_image, new_rc) = resolve_image_and_runtime_class(ctx, namespace, new_sandbox).await?;
    // v1 is same-namespace only, but read the source's own namespace defensively
    // so the template GET resolves correctly if that ever changes.
    let source_ns = source.metadata.namespace.as_deref().unwrap_or(namespace);
    let (src_image, src_rc) = resolve_image_and_runtime_class(ctx, source_ns, source).await?;

    if new_rc != src_rc {
        return Err(format!(
            "restoreFrom source '{source_name}' runtime class {src_rc:?} does not match this \
             sandbox's runtime class {new_rc:?}; a clone requires the same runtime class"
        )
        .into());
    }
    if check_image && new_image != src_image {
        return Err(format!(
            "restoreFrom source '{source_name}' template image '{src_image}' does not match this \
             sandbox's template image '{new_image}'; a memory-image restore requires the same image"
        )
        .into());
    }
    Ok(())
}

/// Snapshot-mode clone-from-snapshot resolution (issue #416): resolve the
/// source's snapshot, gate compatibility, and stamp the resolved digests onto
/// `new_status` so the existing scheduler-placed restore flow stages that
/// snapshot onto this new sandbox's restore Pod.
///
/// A no-op unless this is a clone-on-create whose snapshot is not yet resolved
/// (`current_phase == Creating`, `spec.restore_from` set, `status.snapshotRef`
/// unset) — so the clone's *own* later resumes (which carry their own snapshot
/// ref), normal resumes, and cold starts are all untouched. Returning `Err`
/// (source missing / no snapshot / gate mismatch / unsupported `s3://` form)
/// folds the sandbox to `Failed` with an actionable condition.
async fn apply_snapshot_restore_source(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
    new_status: &mut SandboxStatus,
) -> Result<(), DispatchError> {
    if current_phase(sandbox) != SandboxPhase::Creating {
        return Ok(());
    }
    let Some(restore_from) = sandbox.spec.restore_from.as_deref() else {
        return Ok(());
    };
    // Already resolved on a prior pass, or the clone has since earned its own
    // snapshot — never re-resolve from the source.
    if new_status.snapshot_ref.is_some() {
        return Ok(());
    }

    let source_name = match RestoreSource::parse(restore_from) {
        RestoreSource::SandboxId(id) => id,
        RestoreSource::SnapshotUrl(url) => {
            return Err(format!(
                "restoreFrom '{url}': raw s3:// snapshot URLs are not yet supported — pass the \
                 source sandbox ID instead (its Sandbox CR still carries the snapshot reference)"
            )
            .into());
        }
    };

    let source = ctx
        .store
        .get(namespace, source_name)
        .await?
        .ok_or_else(|| {
            format!(
                "restoreFrom source sandbox '{source_name}' not found in namespace '{namespace}'"
            )
        })?;
    let src_status = source.status.as_ref();
    let snapshot_ref = src_status
        .and_then(|s| s.snapshot_ref.clone())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            format!(
                "restoreFrom source sandbox '{source_name}' has no snapshot recorded \
                 (it was never paused in snapshot mode)"
            )
        })?;

    gate_restore_compatibility(ctx, namespace, sandbox, &source, source_name, true).await?;

    new_status.snapshot_ref = Some(snapshot_ref);
    new_status.snapshot_sha256 = src_status.and_then(|s| s.snapshot_sha256.clone());
    new_status.snapshot_envd_sha256 = src_status.and_then(|s| s.snapshot_envd_sha256.clone());
    new_status.snapshot_runsc_version = src_status.and_then(|s| s.snapshot_runsc_version.clone());
    tracing::info!(
        sandbox = %sandbox.metadata.name.as_deref().unwrap_or("?"),
        source = %source_name,
        snapshot_ref = %new_status.snapshot_ref.as_deref().unwrap_or(""),
        "clone-from-snapshot: resolved source snapshot; staging onto the new restore Pod"
    );
    Ok(())
}

/// Filesystem-mode clone-from-snapshot validation (issue #416): gate the source
/// before the new sandbox cold-boots over CSI-cloned PVCs.
///
/// The actual clone is mechanical — [`crate::pod_adapter::PodAdapter::pvc_for`]
/// stamps each new PVC's `dataSource` to the source's correspondingly-named
/// retained PVC. This validates the precondition: the source must exist and be
/// `Suspended` (filesystem-paused) so its PVC is quiescent and consistent to
/// clone, and its runtime class must match. A no-op unless this is a
/// clone-on-create (`current_phase == Creating`, `spec.restore_from` set); the
/// clone's own later filesystem resumes (where the PVC already exists and the
/// `409`-idempotent create skips the `dataSource`) are untouched. Returning
/// `Err` folds the sandbox to `Failed`.
async fn validate_filesystem_restore_source(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
) -> Result<(), DispatchError> {
    if current_phase(sandbox) != SandboxPhase::Creating {
        return Ok(());
    }
    let Some(restore_from) = sandbox.spec.restore_from.as_deref() else {
        return Ok(());
    };
    let source_name = match RestoreSource::parse(restore_from) {
        RestoreSource::SandboxId(id) => id,
        RestoreSource::SnapshotUrl(url) => {
            return Err(format!(
                "restoreFrom '{url}': a filesystem-mode sandbox clones the source's retained \
                 PVC(s), so it requires the source sandbox ID, not an s3:// snapshot URL"
            )
            .into());
        }
    };
    let source = ctx
        .store
        .get(namespace, source_name)
        .await?
        .ok_or_else(|| {
            format!(
                "restoreFrom source sandbox '{source_name}' not found in namespace '{namespace}'"
            )
        })?;
    if filesystem_suspended_at(&source).is_none() {
        return Err(format!(
            "restoreFrom source sandbox '{source_name}' is not Suspended; a filesystem clone \
             requires the source to be filesystem-paused so its PVC(s) are quiescent to clone"
        )
        .into());
    }
    gate_restore_compatibility(ctx, namespace, sandbox, &source, source_name, false).await?;
    Ok(())
}

/// Default number of attempts to persist a usage event before failing open
/// (issue #636). Overridable via `SANDBAWKS_USAGE_EMIT_MAX_ATTEMPTS` for a store
/// that needs a wider window. Three closes the common short-blip case while keeping
/// the worst-case added pause/terminate latency small (see [`USAGE_EMIT_BACKOFF`]).
const USAGE_EMIT_MAX_ATTEMPTS: u32 = 3;

/// Base back-off between usage-emit retries; doubles per attempt, capped at
/// [`USAGE_EMIT_BACKOFF_CAP`]. With the default 3 attempts the worst case adds
/// ~`100ms + 200ms` before failing open — negligible against the pause path's
/// SnapshotJob round-trip, and far cheaper than wedging the transition.
const USAGE_EMIT_BACKOFF: Duration = Duration::from_millis(100);

/// Upper bound on a single usage-emit back-off, so a large override of
/// [`USAGE_EMIT_MAX_ATTEMPTS`] can't blow the per-attempt wait up unboundedly.
const USAGE_EMIT_BACKOFF_CAP: Duration = Duration::from_secs(2);

/// Back-off before the `attempt`-th usage-emit retry: [`USAGE_EMIT_BACKOFF`]
/// doubled per attempt, capped at [`USAGE_EMIT_BACKOFF_CAP`].
///
/// `checked_shl` keeps a large `SANDBAWKS_USAGE_EMIT_MAX_ATTEMPTS` override (whose
/// shift amount is otherwise unbounded) from overflowing — panicking in debug or
/// silently wrapping in release — falling back to `u32::MAX` (the cap then bounds
/// it). Same defensive shape as [`resume_backoff`].
fn usage_emit_backoff(attempt: u32) -> Duration {
    let factor = 1u32
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u32::MAX);
    USAGE_EMIT_BACKOFF
        .saturating_mul(factor)
        .min(USAGE_EMIT_BACKOFF_CAP)
}

/// Emit the authoritative usage event for a just-closed `Running` interval
/// (epic #480 Phase 2, issue #631; retry hardening #636).
///
/// Reads the interval start from `status.last_transition_time` (the `Running`-entry
/// instant stamped by the previous transition), attributes the interval to the
/// owning team (namespace) and `TeamAPIKey` (the
/// [`sandbawks_types::API_KEY_NAME_ANNOTATION`]), and weights it by the sandbox's
/// CPU/RAM request.
///
/// **Bounded retry, then fail-open (#636).** The emit is retried up to
/// [`USAGE_EMIT_MAX_ATTEMPTS`] times with exponential back-off to ride out a
/// transient object-store blip; if every attempt fails the error is logged, the
/// `dropped` outcome is metered ([`crate::usage_metrics`]), and the lifecycle
/// transition proceeds anyway — never wedging pause/terminate on a billing-store
/// outage, mirroring the best-effort Kubernetes Event emission. Retrying is safe
/// because the event is idempotent on `(uid, interval_start)`: a re-emit (here or
/// via a crash re-drive) overwrites the same object and never double-bills.
async fn emit_running_usage(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
    now: DateTime<Utc>,
) {
    let interval_start = sandbox
        .status
        .as_ref()
        .and_then(|s| s.last_transition_time.as_deref())
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|d| d.with_timezone(&Utc));
    let key_name = sandbox
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(sandbawks_types::API_KEY_NAME_ANNOTATION))
        .map(String::as_str)
        .unwrap_or("");
    let sandbox_id = sandbox.metadata.name.as_deref().unwrap_or("");
    let uid = sandbox.metadata.uid.as_deref().unwrap_or("");
    let Some(event) = crate::usage::build_usage_event(
        namespace,
        key_name,
        sandbox_id,
        uid,
        sandbox.spec.resources.cpu.as_str(),
        sandbox.spec.resources.memory.as_str(),
        interval_start,
        now,
    ) else {
        // No bounded interval (e.g. a sandbox that entered `Running` before this
        // controller began stamping `last_transition_time`) — nothing to bill.
        return;
    };
    // Bounded retry before failing open (#636): give a transient object-store blip
    // a few chances rather than dropping the interval on the first error, but never
    // block the transition indefinitely on a sustained outage. Re-emitting is safe —
    // the event's object key is deterministic in `(uid, interval_start)`, so a retry
    // overwrites rather than double-bills.
    let max_attempts = u32_from_env(
        |k| std::env::var(k).ok(),
        "SANDBAWKS_USAGE_EMIT_MAX_ATTEMPTS",
        USAGE_EMIT_MAX_ATTEMPTS,
    )
    .max(1);
    for attempt in 1..=max_attempts {
        match ctx.usage_sink.emit(&event).await {
            Ok(()) => {
                crate::usage_metrics::record_usage_emit(
                    crate::usage_metrics::UsageEmitOutcome::Emitted,
                );
                if attempt > 1 {
                    tracing::info!(
                        sandbox = %sandbox_id,
                        team = %namespace,
                        attempt,
                        "usage event emitted after retry"
                    );
                }
                return;
            }
            Err(e) if attempt == max_attempts => {
                // Every live attempt failed. Before giving up, try to park the event
                // on the durable spill queue (#636) so the drainer can replay it once
                // the store recovers — turning a sustained outage into latency, not
                // lost billing. Either way the lifecycle transition proceeds
                // (fail-open); the spill is just where the interval waits.
                match ctx.usage_spill.spill(&event).await {
                    Ok(()) => {
                        crate::usage_metrics::record_usage_emit(
                            crate::usage_metrics::UsageEmitOutcome::Spilled,
                        );
                        tracing::warn!(
                            sandbox = %sandbox_id,
                            team = %namespace,
                            attempts = max_attempts,
                            error = %e,
                            "usage event emit failed after retries; spilled for replay (lifecycle transition continues)"
                        );
                    }
                    Err(spill_err) => {
                        // No spill queue configured, or the spill write itself failed:
                        // the interval is genuinely lost. `dropped` is the alertable
                        // signal.
                        crate::usage_metrics::record_usage_emit(
                            crate::usage_metrics::UsageEmitOutcome::Dropped,
                        );
                        tracing::error!(
                            sandbox = %sandbox_id,
                            team = %namespace,
                            attempts = max_attempts,
                            error = %e,
                            spill_error = %spill_err,
                            "usage event emit failed after retries and could not be spilled; interval dropped (lifecycle transition continues)"
                        );
                    }
                }
            }
            Err(e) => {
                // Capped exponential back-off before the next try (overflow-safe; see
                // `usage_emit_backoff`).
                let backoff = usage_emit_backoff(attempt);
                tracing::warn!(
                    sandbox = %sandbox_id,
                    team = %namespace,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "usage event emit failed; retrying"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

/// Run the [`Action`] returned by the state machine.
///
/// Side-effects (adapter calls + SnapshotJob CR creation) live here,
/// behind the status-patch barrier in [`reconcile`]. On success,
/// mutates `new_status` to reflect any post-dispatch state (e.g.
/// `snapshot_job_ref` after creating a job, or `pod_ref` after
/// materializing a Pod).
#[tracing::instrument(
    skip_all,
    name = "dispatch:controller::action",
    err,
    fields(
        sandbox = %sandbox.metadata.name.as_deref().unwrap_or(""),
        action = action.label(),
    )
)]
async fn dispatch(
    ctx: &ReconcileContext,
    namespace: &str,
    sandbox: &Sandbox,
    action: Action,
    new_status: &mut SandboxStatus,
    now: DateTime<Utc>,
) -> Result<(), DispatchError> {
    let name = sandbox
        .metadata
        .name
        .as_deref()
        .ok_or("sandbox missing metadata.name")?;
    match action {
        Action::Noop => {
            // For the `(Creating, PodReady)` edge, the adapter's
            // `pod_ready` call yields the `envd_address` the gateway
            // needs. The `(Pausing → Paused)` Noop is also reachable
            // post-T4 (driven by the SnapshotJob observation), but that
            // transition doesn't need an adapter probe — the Sandbox
            // already has its `pod_ref` / `envd_address` from the pre-pause
            // Running phase. (The `(Resuming → Running)` edge is no longer a
            // Noop — it re-arms the idle clock via `Action::ResumeToRunning`,
            // issue #563.) Gate on current phase so we only probe in the
            // `Creating` case.
            if current_phase(sandbox) == SandboxPhase::Creating {
                if let Some(pr) = new_status.pod_ref.as_deref().and_then(parse_pod_ref) {
                    if let Some(readiness) = ctx.adapter.pod_ready(&pr).await? {
                        new_status.envd_address = Some(readiness.envd_address);
                        // Same `.or(...)` pattern as the pre-dispatch
                        // probe in `reconcile()` — never blank out a
                        // previously-recorded value on a transient
                        // re-probe.
                        new_status.assigned_node =
                            readiness.node_name.or(new_status.assigned_node.clone());
                        new_status.pod_uid = readiness.pod_uid.or(new_status.pod_uid.clone());
                    }
                }
            }
            Ok(())
        }
        Action::CreatePod => {
            // Filesystem-mode clone-from-snapshot (issue #416): when this fresh
            // `Creating` sandbox carries `spec.restore_from`, validate the source
            // is a quiescent (Suspended) filesystem-paused sibling with a
            // matching runtime before we provision. The PVC `dataSource` clone
            // itself is stamped by `PodAdapter::pvc_for`; a failed gate here folds
            // to `Failed` before any Pod/PVC is created. No-op for cold starts,
            // eviction reprovisions, and normal filesystem resumes.
            validate_filesystem_restore_source(ctx, namespace, sandbox).await?;
            // The cold-start `(Creating, PodCreate)` edge only fires when no
            // `pod_ref` is recorded (`derive_trigger` gates it on
            // `!has_pod_ref`), so this delete-if-present is a no-op there. The
            // snapshot-less eviction-recovery edge `(Running,
            // EvictedRecover{no snapshot}) → Creating` *does* arrive carrying
            // the evicted Pod's `pod_ref` (issue #407): an evicted Pod lingers
            // as a terminal object, and `create_pod` treats 409 AlreadyExists
            // as an idempotent no-op — so without deleting it first,
            // `create_pod` would hand back the corpse and the fresh,
            // clean-state Pod would never be born, hanging the Sandbox in
            // `Creating`. Delete the stale Pod (`delete_pod` waits for the
            // apiserver to 404 it) and scrub its identity before provisioning.
            if let Some(pr) = new_status.pod_ref.as_deref().and_then(parse_pod_ref) {
                tracing::info!(
                    sandbox = %name,
                    stale_pod = %pr.name,
                    "deleting evicted Pod before provisioning a fresh clean-state replacement"
                );
                ctx.adapter.delete_pod(&pr).await?;
                new_status.pod_ref = None;
                new_status.pod_uid = None;
                new_status.envd_address = None;
                new_status.assigned_node = None;
            }
            let (image, pod_sandbox, vct_suppressions) =
                resolve_pod_inputs(ctx, namespace, sandbox).await?;
            stamp_vct_policy_condition(new_status, &vct_suppressions, now);
            let pod_ref = ctx
                .adapter
                .create_pod(pod_sandbox.as_ref(), &image, None)
                .await?;
            new_status.pod_ref = Some(format!("{}/{}", pod_ref.namespace, pod_ref.name));
            // pod_uid is stamped by the Pod watcher (Task 5) once the
            // kube API server has assigned a UID.
            Ok(())
        }
        Action::StageSnapshot => {
            // Epic #484 resume step 2 (scheduler-placed): the restore Pod
            // already exists and the scheduler has bound it to a node; the
            // `NodeAssigned` trigger that drove us here was emitted by
            // `resuming_trigger` once `pod_scheduled_node`
            // returned `Some`. Re-read the bound node and record it so the
            // node-local `Stage` job lands on exactly the node the Pod will
            // restore on — the snapshot is staged at the deterministic
            // `target_path`, where the restore shim is already blocking the
            // container's `runsc restore` on the `.staged` marker.
            //
            // We need the upstream snapshot URL to stage from — guard
            // against a Sandbox that reached `Resuming` without one.
            if new_status.snapshot_ref.is_none() {
                return Err("stage requested but status.snapshotRef is None".into());
            }
            let pod_ref = new_status
                .pod_ref
                .as_deref()
                .and_then(parse_pod_ref)
                .ok_or("stage requested but status.podRef names no restore Pod")?;
            // `resuming_trigger` already confirmed `spec.nodeName`
            // one step earlier, so this re-read normally returns `Some`. But
            // `Ok(None)` here (a momentary loss of `spec.nodeName`, or an
            // observer that briefly can't see the binding) must NOT fold the
            // Sandbox to `Failed`: the phase is already committed to
            // `Resuming` by the invariant-#1 patch, so an `ok_or(..)?` would
            // turn a transient, recoverable read into a terminal fold. Treat
            // it as "not yet — requeue": leave `assigned_node` /
            // `snapshot_job_ref` unset so the next pass's node-discovery
            // re-fires and re-dispatches. (A genuine transport error still
            // propagates via `?`, matching every other dispatch.)
            let Some(node) = ctx.adapter.pod_scheduled_node(&pod_ref).await? else {
                tracing::debug!(
                    sandbox = %name,
                    "stage requested but the restore Pod's node is not visible this pass; \
                     idling for node-discovery to retry on the next requeue"
                );
                return Ok(());
            };
            new_status.assigned_node = Some(node);
            let job = build_snapshot_job(sandbox, name, new_status, SnapshotJobMode::Stage);
            let job_name = job
                .metadata
                .name
                .clone()
                .ok_or("built snapshot job missing metadata.name")?;
            ctx.snapshot_jobs.create(namespace, job).await?;
            new_status.snapshot_job_ref = Some(job_name);
            // Phase stays `Resuming` — the `Stage` job's Complete status
            // (folded into `Trigger::StageComplete`) drives the next step.
            Ok(())
        }
        Action::CreateRestorePod => {
            // Epic #484 resume step 1 (scheduler-placed): materialize the
            // restore Pod **unpinned** so the scheduler does memory-aware
            // placement, with its `io.sandbawks/restore-image-path`
            // annotation stamped to the *deterministic* per-sandbox staging
            // path. The snapshot does not exist yet — it is staged later,
            // onto whichever node the scheduler picks (see
            // `Action::StageSnapshot`). The restore shim blocks the sandbox
            // container's `runsc restore` until the snapshot's `.staged` marker
            // appears at that path, so creating the Pod first is safe.
            //
            // This edge fires both at resume start (`Paused →
            // ResumeRequested`) and on `RestorePodLost` recovery; in both
            // cases the path is a pure function of the sandbox name, so no
            // prior `status.restoreStagingPath` is required.
            //
            // Snapshot-mode clone-from-snapshot (issue #416): it *also* fires
            // here from `(Creating, RestoreRequested)` for a brand-new sandbox
            // with `spec.restore_from`. Resolve + gate the source snapshot and
            // stamp its digests onto `new_status` first, so the later
            // `StageSnapshot` step finds a `snapshotRef` to stage. A no-op for
            // every ordinary resume/recovery (which already carry their own
            // snapshot ref); a failed gate folds to `Failed` before any Pod is
            // created.
            apply_snapshot_restore_source(ctx, namespace, sandbox, new_status).await?;
            let staging_path = restore_staging_path(name);

            // Bounded-recovery accounting (epic #484, issue #489). The
            // invariant-#1 patch already advanced `new_status.phase` to
            // `Resuming`, so we read the *pre-transition* phase off `sandbox`
            // to tell the two callers apart even though both drive this edge:
            //   - a fresh resume (`Paused → ResumeRequested`) resets the budget;
            //   - a recovery recreate (`Resuming → RestorePodLost`) spends one
            //     retry, incrementing the count the recovery loop bounds against
            //     `resume_max_restarts()`.
            // Either way we stamp `resume_last_attempt` to *now*: it is the
            // anchor the next pass's deadlines and backoff measure from, so it
            // must move forward on every (re)creation. `now` is the reconcile's
            // single clock read, threaded through so the stamp shares the same
            // instant the recovery probe measured against (one read per pass).
            if current_phase(sandbox) == SandboxPhase::Resuming {
                new_status.resume_restart_count = new_status.resume_restart_count.saturating_add(1);
            } else {
                new_status.resume_restart_count = 0;
            }
            new_status.resume_last_attempt = Some(now.to_rfc3339());
            // Issue #196: the pre-pause Pod (resume start) — or a wedged
            // restore Pod (recovery) — survives in a non-running state.
            // `create_pod` treats 409 AlreadyExists as an idempotent no-op,
            // so it would silently return that dead Pod and the fresh
            // restore Pod would never be born — hanging the Sandbox in
            // `Resuming`. Delete whatever Pod `pod_ref` names first
            // (`delete_pod` waits for the apiserver to 404 it) so
            // `create_pod` materializes a genuinely fresh Pod.
            if let Some(pr) = new_status.pod_ref.as_deref().and_then(parse_pod_ref) {
                tracing::info!(
                    sandbox = %name,
                    stale_pod = %pr.name,
                    "deleting prior Pod before creating the scheduler-placed restore Pod"
                );
                ctx.adapter.delete_pod(&pr).await?;
            }
            // Scrub the prior Pod's identity AND `assigned_node`: the
            // restore Pod is recreated unpinned, so the scheduler re-picks
            // a node (likely a different one on recovery). `assigned_node`
            // is re-discovered from the new Pod's `spec.nodeName` by
            // `resuming_trigger` on a later pass; clearing it here
            // is what lets the staging step target the *new* node rather
            // than re-pinning the old, possibly-starved one (epic #484).
            new_status.pod_ref = None;
            new_status.pod_uid = None;
            new_status.envd_address = None;
            new_status.assigned_node = None;
            // A fresh restore Pod also invalidates any path staged for the
            // previous attempt — re-staging targets the same deterministic
            // path on the new node, so drop the stale record.
            new_status.restore_staging_path = None;

            let placement = RestorePlacement { staging_path };
            let (image, pod_sandbox, vct_suppressions) =
                resolve_pod_inputs(ctx, namespace, sandbox).await?;
            stamp_vct_policy_condition(new_status, &vct_suppressions, now);
            let pod_ref = ctx
                .adapter
                .create_pod(pod_sandbox.as_ref(), &image, Some(&placement))
                .await?;
            new_status.pod_ref = Some(format!("{}/{}", pod_ref.namespace, pod_ref.name));
            Ok(())
        }
        Action::ResumeToRunning => {
            // Issue #563: a clean resume reaching `Running`. Re-arm the idle
            // clock to `now` so the idle-timeout deadline is measured from
            // *this* resume, not the stale pre-pause `lastActivityTimestamp`.
            // A long-lived claimed member (`desiredPhase=Running` +
            // `autoPause`) that aged past its deadline before the pause would
            // otherwise be "idle" the instant it lands in `Running`, so the
            // idle edge re-pauses it, `desiredPhase=Running` re-resumes it, and
            // the two intents thrash — churning restore Pods until a pause
            // checkpoint races the teardown and folds the Sandbox to `Failed`.
            // Stamping a fresh activity time gives the resumed Sandbox a full
            // idle window, breaking the oscillation at its source. (The gateway
            // keeps pushing this forward on real requests, so a genuinely-used
            // sandbox still never auto-pauses.) No side effect on the live
            // Pod — it is already container-Ready from the restore. Mirrors the
            // re-arm `Action::RecoverToRunning` performs on the tier-2 edge.
            new_status.last_activity_timestamp = Some(now.to_rfc3339());
            tracing::info!(
                sandbox = %name,
                "resumed Sandbox to Running; idle clock re-armed"
            );
            Ok(())
        }
        Action::Pause => {
            // Plan 05 T4: instead of calling adapter.pause directly,
            // create a SnapshotJob CR. The per-node sandbawks-node
            // worker (Plan 05 T9) reconciles the job and writes
            // `status.phase=Complete | Failed`; the next pass through
            // `reconcile()` picks that up via `observe_snapshot_job`
            // and folds the manifest's snapshot_ref into
            // `Sandbox.status.snapshot_ref`.
            // Bounded tier-1 retry accounting (issue #510). The invariant-#1
            // patch already advanced `new_status.phase` to `Pausing`, so we
            // read the *pre-transition* phase off `sandbox` to tell the two
            // callers of `Action::Pause` apart:
            //   - a fresh pause (`Running → PauseRequested`/`Timeout`) resets
            //     the budget;
            //   - a retry (`Pausing → PauseRetry`) spends one attempt,
            //     incrementing the count `pause_recovery_trigger` bounds against
            //     `pause_max_retries()`.
            // Either way stamp `pause_last_attempt` to `now`: it is the anchor
            // the next failure's backoff measures from, so it must move forward
            // on every (re)dispatch.
            if current_phase(sandbox) == SandboxPhase::Pausing {
                new_status.pause_restart_count = new_status.pause_restart_count.saturating_add(1);
            } else {
                new_status.pause_restart_count = 0;
            }
            new_status.pause_last_attempt = Some(now.to_rfc3339());
            let job = build_snapshot_job(sandbox, name, new_status, SnapshotJobMode::Pause);
            let job_name = job
                .metadata
                .name
                .clone()
                .ok_or("built snapshot job missing metadata.name")?;
            ctx.snapshot_jobs.create(namespace, job).await?;
            new_status.snapshot_job_ref = Some(job_name);
            // Phase stays `Pausing` — only the SnapshotJob's Complete
            // status advances us to `Paused`.
            Ok(())
        }
        Action::SuspendPod => {
            // #606 filesystem suspend: tear the Pod down but keep the
            // Sandbox-owned PVC(s). Those PVCs are owned by the Sandbox CR (not
            // the Pod — see `PodAdapter::pvc_for`), so deleting the Pod does not
            // cascade-delete them; the workspace survives to resume. `delete_pod`
            // is foreground + poll-to-404 and treats an already-gone Pod as
            // success, so this is idempotent across the crash-recovery re-drive.
            // No SnapshotJob and no `snapshot_ref`: a filesystem pause captures
            // nothing to S3 — its only durable state is the retained PVC(s).
            if let Some(pr) = new_status.pod_ref.as_deref().and_then(parse_pod_ref) {
                tracing::info!(
                    sandbox = %name,
                    pod = %pr.name,
                    "filesystem suspend: deleting Pod, retaining PVC(s)"
                );
                ctx.adapter.delete_pod(&pr).await?;
            }
            // Scrub the Pod identity so the next pass's completion probe sees the
            // Pod released, and so a stale `envd_address` can never route the
            // gateway to a torn-down Pod. The retained PVC(s) are untouched.
            new_status.pod_ref = None;
            new_status.pod_uid = None;
            new_status.envd_address = None;
            new_status.assigned_node = None;
            upsert_condition(
                new_status,
                "Suspended",
                "False",
                "Suspending",
                "filesystem-mode pause: deleting Pod and retaining PVC(s)",
                now,
            );
            // Phase stays `Pausing` — `filesystem_pausing_trigger` folds
            // `PodReleased` once the Pod is gone, advancing to `Paused`.
            Ok(())
        }
        Action::FinalizeSuspend => {
            // #606 filesystem suspend complete. `Action::SuspendPod` already
            // released the Pod; re-clear defensively (idempotent) so the `Paused`
            // state is unambiguous even if a crash interleaved the suspend, and
            // leave every `snapshot_*` field empty — a filesystem pause has no
            // snapshot to resume from. The retained PVC(s) carry state forward to
            // the next cold boot.
            new_status.pod_ref = None;
            new_status.pod_uid = None;
            new_status.envd_address = None;
            new_status.assigned_node = None;
            upsert_condition(
                new_status,
                "Suspended",
                "True",
                "PodReleased",
                "filesystem-mode pause complete: Pod deleted, PVC(s) retained",
                now,
            );
            tracing::info!(
                sandbox = %name,
                "filesystem suspend complete; Sandbox Paused with state on retained PVC(s)"
            );
            Ok(())
        }
        Action::Terminate => {
            // If the Pod was materialized (Plan 04 T4+) ask the adapter
            // to tear it down. The Sandbox CR itself is GC'd via
            // owner-ref or finalizer in a later plan; here we just
            // leave the phase at Terminating after the delete.
            if let Some(pr) = new_status.pod_ref.as_deref().and_then(parse_pod_ref) {
                ctx.adapter.delete_pod(&pr).await?;
            }
            Ok(())
        }
        Action::RecordFailure { reason, message } => {
            push_failure_condition(new_status, reason, message, now);
            Ok(())
        }
        Action::RecoverToRunning => {
            // Issue #510 tier 2: the workload Pod never stopped serving — the
            // pause checkpoint simply failed — so there is no adapter side
            // effect here. Clear the failure breadcrumbs and reset the tier-1
            // retry budget so a future pause starts fresh, and re-arm the idle
            // clock to `now`: the recovered Sandbox must get a full idle window
            // before the next auto-pause attempt, otherwise a stale
            // `last_activity_timestamp` would immediately re-trip the idle
            // timeout and tight-loop pause→fail→recover.
            new_status.failure_reason = None;
            new_status.failed_at = None;
            new_status.pause_restart_count = 0;
            new_status.pause_last_attempt = None;
            new_status.last_activity_timestamp = Some(now.to_rfc3339());
            tracing::info!(
                sandbox = %name,
                "recovered Failed Sandbox to Running; idle clock re-armed"
            );
            crate::sandbox_recovery_metrics::record_recovery(
                crate::sandbox_recovery_metrics::RecoveryOutcome::RecoveredToRunning,
            );
            Ok(())
        }
        Action::GcOrphanedPod => {
            // Issue #510 tier 3: the orphaned Pod is unrecoverable; delete it to
            // reclaim the node slot. Stay `Failed` — only the Pod is reclaimed,
            // not the Sandbox record. Clearing the Pod-identity fields is what
            // makes the Sandbox genuinely terminal afterward: with no `pod_ref`,
            // `failed_recovery_trigger` short-circuits to `None` and never
            // re-fires for this Sandbox.
            if let Some(pr) = new_status.pod_ref.as_deref().and_then(parse_pod_ref) {
                tracing::warn!(
                    sandbox = %name,
                    orphan_pod = %pr.name,
                    "GC'ing orphaned Pod of a Failed (pause-checkpoint) Sandbox to reclaim its node slot"
                );
                ctx.adapter.delete_pod(&pr).await?;
            }
            new_status.pod_ref = None;
            new_status.pod_uid = None;
            new_status.envd_address = None;
            new_status.assigned_node = None;
            new_status.conditions.push(SandboxCondition {
                kind: "OrphanReclaimed".into(),
                status: "True".into(),
                reason: Some("PauseCheckpointOrphanGC".into()),
                message: Some(
                    "orphaned workload Pod deleted after an unrecoverable pause-checkpoint \
                     failure; node resources reclaimed"
                        .into(),
                ),
                last_transition_time: Some(now.to_rfc3339()),
            });
            crate::sandbox_recovery_metrics::record_recovery(
                crate::sandbox_recovery_metrics::RecoveryOutcome::OrphanGc,
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{DateTime, TimeZone, Utc};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use proptest::prelude::*;
    use sandbawks_types::{
        ResourceRequests, Sandbox, SandboxCondition, SandboxPhase, SandboxSpec, SandboxStatus,
    };

    use super::{
        derive_trigger, duration_secs_from_env, emit_running_usage, ensure_staging_root_absolute,
        filesystem_suspended_at, past_deadline, pause_failure_location, pause_recovery_trigger,
        paused_requeue_interval, phase_transition_event, pvc_retention_gc_due,
        resolve_pvc_retention, restore_staging_path, resume_backoff, resume_recovery_trigger,
        resume_would_immediately_idle_pause, running_requeue_interval, string_from_env,
        timeout_trigger, u32_from_env, usage_emit_backoff, PvcRetentionConfig, ReconcileContext,
        DEFAULT_PAUSE_MAX_RETRIES, DEFAULT_RESTORE_STAGING_ROOT, DEFAULT_RESUME_MAX_RESTARTS,
        DEFAULT_RESUME_READY_DEADLINE, RESUME_BACKOFF_BASE, RESUME_BACKOFF_CAP, USAGE_EMIT_BACKOFF,
        USAGE_EMIT_BACKOFF_CAP, USAGE_EMIT_MAX_ATTEMPTS,
    };
    use crate::state_machine::{FailureReason, Trigger};
    use crate::usage_spill::UsageSpillQueue;
    use kube::runtime::events::EventType;

    /// Stamp `status.lastActivityTimestamp` so the idle baseline (issue
    /// #481) is the given instant rather than `creationTimestamp`.
    fn with_activity(mut sb: Sandbox, last_activity: DateTime<Utc>) -> Sandbox {
        sb.status = Some(SandboxStatus {
            last_activity_timestamp: Some(last_activity.to_rfc3339()),
            ..Default::default()
        });
        sb
    }

    /// A fixed, deterministic "creation" instant for the clock-injected
    /// tests — any value works since `timeout_trigger`/`running_requeue_interval`
    /// only ever see `now` relative to it.
    fn created_at() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("valid fixed timestamp")
    }

    /// Build a `Sandbox` carrying exactly the fields the timeout logic
    /// reads: `spec.timeout_seconds`, `spec.auto_pause`, and
    /// `metadata.creationTimestamp`. The rest are filled with valid
    /// placeholders.
    fn sandbox_with(
        timeout_seconds: u32,
        auto_pause: bool,
        created: Option<DateTime<Utc>>,
    ) -> Sandbox {
        let mut sb = Sandbox::new(
            "sbx-test",
            SandboxSpec {
                template_ref: "tpl".into(),
                runtime_class_name: None,
                resources: ResourceRequests {
                    cpu: "1".parse().expect("valid cpu"),
                    memory: "1Gi".parse().expect("valid memory"),
                    ephemeral_storage: None,
                },
                timeout_seconds,
                auto_pause,
                desired_phase: None,
                env: Default::default(),
                metadata: Default::default(),
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
        sb.metadata.creation_timestamp = created.map(Time);
        sb
    }

    /// A `Paused` sandbox with `desiredPhase=Running` — the shape that reaches
    /// the `(Paused, Running)` resume edge (issue #654). `auto_pause`,
    /// `timeout_seconds`, and the idle baseline (`lastActivityTimestamp`) are
    /// exactly the inputs the resume gate consults.
    fn paused_wants_running(
        timeout_seconds: u32,
        auto_pause: bool,
        last_activity: DateTime<Utc>,
    ) -> Sandbox {
        let mut sb = sandbox_with(timeout_seconds, auto_pause, Some(created_at()));
        sb.spec.desired_phase = Some(SandboxPhase::Running);
        sb.status = Some(SandboxStatus {
            phase: SandboxPhase::Paused,
            last_activity_timestamp: Some(last_activity.to_rfc3339()),
            ..Default::default()
        });
        sb
    }

    #[test]
    fn resume_withheld_when_idle_deadline_already_past() {
        // An `autoPause` sandbox idle past its 300s window: resuming now would
        // immediately re-trip the idle timeout, so the resume edge is withheld
        // and the sandbox stays parked rather than thrashing (issue #654).
        let last_activity = created_at();
        let sb = paused_wants_running(300, true, last_activity);
        let now = last_activity + chrono::Duration::seconds(301);
        assert!(resume_would_immediately_idle_pause(&sb, now));
        assert_eq!(derive_trigger(&sb, SandboxPhase::Paused, now), None);
    }

    #[test]
    fn resume_fires_when_activity_is_fresh() {
        // A real `/connect` stamps `lastActivityTimestamp = now` *before*
        // flipping `desiredPhase=Running`, so the deadline is a full window in
        // the future and the resume proceeds.
        let last_activity = created_at();
        let sb = paused_wants_running(300, true, last_activity);
        let now = last_activity + chrono::Duration::seconds(5);
        assert!(!resume_would_immediately_idle_pause(&sb, now));
        assert_eq!(
            derive_trigger(&sb, SandboxPhase::Paused, now),
            Some(Trigger::ResumeRequested)
        );
    }

    #[test]
    fn resume_not_withheld_without_auto_pause() {
        // Without `autoPause` the idle edge terminates rather than pauses, so
        // there is no resume↔pause thrash to guard against; a user-paused
        // sandbox must still resume even with a stale idle baseline.
        let last_activity = created_at();
        let sb = paused_wants_running(300, false, last_activity);
        let now = last_activity + chrono::Duration::seconds(301);
        assert!(!resume_would_immediately_idle_pause(&sb, now));
        assert_eq!(
            derive_trigger(&sb, SandboxPhase::Paused, now),
            Some(Trigger::ResumeRequested)
        );
    }

    #[test]
    fn resume_not_withheld_when_timeout_disabled() {
        // `timeout_seconds == 0` disables the idle timeout (warm-pool sentinel):
        // no deadline exists, nothing can re-trip, so the resume always fires.
        let last_activity = created_at();
        let sb = paused_wants_running(0, true, last_activity);
        let now = last_activity + chrono::Duration::seconds(10_000);
        assert!(!resume_would_immediately_idle_pause(&sb, now));
        assert_eq!(
            derive_trigger(&sb, SandboxPhase::Paused, now),
            Some(Trigger::ResumeRequested)
        );
    }

    /// Build a `Paused` sandbox carrying a `Suspended=True` condition (the
    /// filesystem-suspend marker + retention anchor) stamped at `suspended_at`,
    /// optionally with an explicit `spec.pvcRetention`. The shape a paused
    /// filesystem sandbox has after `Action::FinalizeSuspend` (issue #608).
    fn paused_filesystem_sandbox(
        suspended_at: DateTime<Utc>,
        pvc_retention: Option<&str>,
    ) -> Sandbox {
        let mut sb = sandbox_with(0, false, Some(created_at()));
        sb.spec.pvc_retention = pvc_retention.map(str::to_owned);
        sb.status = Some(SandboxStatus {
            phase: SandboxPhase::Paused,
            conditions: vec![SandboxCondition {
                kind: "Suspended".into(),
                status: "True".into(),
                reason: Some("PodReleased".into()),
                message: Some("filesystem-mode pause complete".into()),
                last_transition_time: Some(suspended_at.to_rfc3339()),
            }],
            ..Default::default()
        });
        sb
    }

    /// Retention enabled with a 7-day default window.
    fn pvc_retention_enabled() -> PvcRetentionConfig {
        PvcRetentionConfig {
            enabled: true,
            default_duration: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }

    #[test]
    fn filesystem_suspended_at_reads_the_suspended_condition() {
        let anchor = created_at();
        let sb = paused_filesystem_sandbox(anchor, None);
        assert_eq!(filesystem_suspended_at(&sb), Some(anchor));

        // A snapshot-mode paused sandbox has no `Suspended` condition, so the
        // filesystem marker is absent — it must never be a GC candidate here.
        let snapshot_paused = sandbox_with(0, false, Some(created_at()));
        assert_eq!(filesystem_suspended_at(&snapshot_paused), None);
    }

    /// Build a `ReconcileContext` with the given usage sink and spill queue —
    /// enough to drive [`emit_running_usage`] in isolation.
    fn usage_ctx_full(
        sink: Arc<dyn crate::usage::UsageEventSink>,
        spill: Arc<dyn crate::usage_spill::UsageSpillQueue>,
    ) -> ReconcileContext {
        ReconcileContext {
            store: Arc::new(crate::FakeSandboxStore::new()),
            adapter: Arc::new(crate::FakeRuntimeAdapter::new("fake")),
            snapshot_jobs: Arc::new(crate::FakeSnapshotJobStore::new()),
            templates: Arc::new(crate::FakeSandboxTemplateStore::strict()),
            client: None,
            pvc_retention: PvcRetentionConfig::default(),
            usage_sink: sink,
            usage_spill: spill,
        }
    }

    /// Build a `ReconcileContext` with the given sink and spilling disabled.
    fn usage_ctx_dyn(sink: Arc<dyn crate::usage::UsageEventSink>) -> ReconcileContext {
        usage_ctx_full(sink, Arc::new(crate::usage_spill::NoopUsageSpillQueue))
    }

    /// Convenience wrapper for the common recording-sink case.
    fn usage_ctx(sink: Arc<crate::usage::RecordingUsageSink>) -> ReconcileContext {
        usage_ctx_dyn(sink)
    }

    /// Test [`crate::usage::UsageEventSink`] that fails its first `fail_until` emit
    /// attempts, then succeeds — drives the bounded-retry path (#636). Counts
    /// attempts and records the events that finally land.
    struct CountingUsageSink {
        fail_until: usize,
        attempts: std::sync::atomic::AtomicUsize,
        recorded: std::sync::Mutex<Vec<crate::usage::UsageEvent>>,
    }

    impl CountingUsageSink {
        fn new(fail_until: usize) -> Self {
            Self {
                fail_until,
                attempts: std::sync::atomic::AtomicUsize::new(0),
                recorded: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn attempts(&self) -> usize {
            self.attempts.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn recorded(&self) -> usize {
            self.recorded
                .lock()
                .expect("CountingUsageSink poisoned")
                .len()
        }
    }

    #[async_trait::async_trait]
    impl crate::usage::UsageEventSink for CountingUsageSink {
        async fn emit(
            &self,
            event: &crate::usage::UsageEvent,
        ) -> Result<(), crate::usage::UsageSinkError> {
            let n = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            if n <= self.fail_until {
                Err(crate::usage::UsageSinkError::Store(format!(
                    "simulated failure {n}"
                )))
            } else {
                self.recorded
                    .lock()
                    .expect("CountingUsageSink poisoned")
                    .push(event.clone());
                Ok(())
            }
        }
    }

    /// A `Running` sandbox with the durable interval anchor, uid, and default
    /// resources — the minimum for `emit_running_usage` to build a billable event.
    fn running_billable_sandbox(start: DateTime<Utc>) -> Sandbox {
        let mut sb = sandbox_with(0, false, Some(start));
        sb.metadata.uid = Some("uid-r".into());
        sb.status = Some(SandboxStatus {
            phase: SandboxPhase::Running,
            last_transition_time: Some(start.to_rfc3339()),
            ..Default::default()
        });
        sb
    }

    #[tokio::test]
    async fn emit_running_usage_records_a_weighted_attributed_event() {
        use std::collections::BTreeMap;

        let sink = Arc::new(crate::usage::RecordingUsageSink::new());
        let ctx = usage_ctx(Arc::clone(&sink));

        let start = created_at();
        let mut sb = sandbox_with(0, false, Some(start));
        sb.metadata.uid = Some("uid-1".into());
        sb.spec.resources = ResourceRequests {
            cpu: "2".parse().expect("valid cpu"),
            memory: "2Gi".parse().expect("valid memory"),
            ephemeral_storage: None,
        };
        sb.metadata.annotations = Some(BTreeMap::from([(
            sandbawks_types::API_KEY_NAME_ANNOTATION.to_owned(),
            "key-1".to_owned(),
        )]));
        // Entered `Running` at `start` — the interval anchor the close edge reads.
        sb.status = Some(SandboxStatus {
            phase: SandboxPhase::Running,
            last_transition_time: Some(start.to_rfc3339()),
            ..Default::default()
        });

        let now = start + chrono::Duration::seconds(60);
        emit_running_usage(&ctx, "team-acme", &sb, now).await;

        let events = sink.events();
        assert_eq!(events.len(), 1, "exactly one usage event on the close edge");
        let ev = &events[0];
        assert_eq!(ev.team, "team-acme");
        assert_eq!(ev.key_name, "key-1");
        assert_eq!(ev.sandbox_uid, "uid-1");
        assert_eq!(ev.running_seconds, 60.0);
        // weight = max(2 cores, 2Gi/4) = 2 → 120 weighted seconds.
        assert_eq!(ev.weighted_seconds, 120.0);
        assert_eq!(
            ev.idempotency_key(),
            format!("uid-1:{}", start.to_rfc3339())
        );
    }

    #[tokio::test]
    async fn emit_running_usage_skips_when_no_running_anchor() {
        // A sandbox that entered `Running` before the controller began stamping
        // `last_transition_time` has no bounded interval — nothing to bill, and we
        // must not guess a start.
        let sink = Arc::new(crate::usage::RecordingUsageSink::new());
        let ctx = usage_ctx(Arc::clone(&sink));

        let mut sb = sandbox_with(0, false, Some(created_at()));
        sb.metadata.uid = Some("uid-2".into());
        sb.status = Some(SandboxStatus {
            phase: SandboxPhase::Running,
            last_transition_time: None,
            ..Default::default()
        });

        emit_running_usage(
            &ctx,
            "team-acme",
            &sb,
            created_at() + chrono::Duration::seconds(30),
        )
        .await;
        assert!(
            sink.events().is_empty(),
            "no event without a Running-entry anchor"
        );
    }

    #[test]
    fn usage_emit_backoff_grows_then_caps_without_overflow() {
        // Doubles per attempt up to the cap.
        assert_eq!(usage_emit_backoff(1), USAGE_EMIT_BACKOFF);
        assert_eq!(usage_emit_backoff(2), USAGE_EMIT_BACKOFF * 2);
        // A large `SANDBAWKS_USAGE_EMIT_MAX_ATTEMPTS` override drives the shift amount
        // past 31; the old `1u32 << (attempt - 1)` overflowed there (panic in debug).
        // `checked_shl` clamps to the cap instead of panicking/wrapping.
        assert_eq!(usage_emit_backoff(40), USAGE_EMIT_BACKOFF_CAP);
        assert_eq!(usage_emit_backoff(u32::MAX), USAGE_EMIT_BACKOFF_CAP);
    }

    #[tokio::test]
    async fn emit_running_usage_retries_then_succeeds() {
        // The sink fails once, then succeeds — within the default attempt budget.
        // The interval must still land (a transient object-store blip is ridden
        // out, not dropped). Issue #636.
        let sink = Arc::new(CountingUsageSink::new(1));
        let ctx = usage_ctx_dyn(sink.clone());
        let start = created_at();
        let sb = running_billable_sandbox(start);

        emit_running_usage(
            &ctx,
            "team-acme",
            &sb,
            start + chrono::Duration::seconds(60),
        )
        .await;

        assert_eq!(sink.attempts(), 2, "retried once past the initial failure");
        assert_eq!(sink.recorded(), 1, "the interval landed after the retry");
    }

    #[tokio::test]
    async fn emit_running_usage_drops_when_retries_exhausted_and_no_spill() {
        // The sink always fails and no spill queue is configured (Noop) — emit must
        // exhaust exactly the attempt budget, then RETURN (fail-open) having dropped
        // the interval, never wedging the transition. Issue #636.
        let sink = Arc::new(CountingUsageSink::new(usize::MAX));
        let ctx = usage_ctx_dyn(sink.clone());
        let start = created_at();
        let sb = running_billable_sandbox(start);

        emit_running_usage(
            &ctx,
            "team-acme",
            &sb,
            start + chrono::Duration::seconds(60),
        )
        .await;

        assert_eq!(
            sink.attempts(),
            USAGE_EMIT_MAX_ATTEMPTS as usize,
            "exhausted exactly the bounded attempt budget"
        );
        assert_eq!(
            sink.recorded(),
            0,
            "nothing persisted on a sustained outage with no spill queue"
        );
        // Reaching this line is the assertion that matters: the await returned
        // rather than blocking, so the lifecycle transition is never wedged.
    }

    #[tokio::test]
    async fn emit_running_usage_spills_when_retries_exhausted() {
        // The sink always fails BUT a spill queue is configured — the interval must
        // be parked for replay (not dropped), and the transition still proceeds.
        // Issue #636 (durable spill+replay).
        let sink = Arc::new(CountingUsageSink::new(usize::MAX));
        let spill = Arc::new(crate::usage_spill::RecordingUsageSpillQueue::new());
        let ctx = usage_ctx_full(sink.clone(), spill.clone());
        let start = created_at();
        let sb = running_billable_sandbox(start);

        emit_running_usage(
            &ctx,
            "team-acme",
            &sb,
            start + chrono::Duration::seconds(60),
        )
        .await;

        assert_eq!(
            sink.attempts(),
            USAGE_EMIT_MAX_ATTEMPTS as usize,
            "still exhausts the live attempt budget before spilling"
        );
        assert_eq!(
            spill.len(),
            1,
            "the interval is parked on the spill queue, not dropped"
        );

        // When the store recovers, draining the queue replays the parked interval.
        let good = crate::usage::RecordingUsageSink::new();
        let report = spill.drain(&good).await.expect("drain");
        assert_eq!(report.replayed, 1);
        assert_eq!(good.events().len(), 1, "the parked interval is recovered");
        assert!(spill.is_empty(), "replayed interval leaves the queue");
    }

    #[test]
    fn pvc_retention_gc_fires_only_past_the_window() {
        let anchor = created_at();
        let cfg = pvc_retention_enabled();
        let sb = paused_filesystem_sandbox(anchor, None);

        // One second before the 7-day deadline: not yet due.
        let just_before = anchor + chrono::Duration::days(7) - chrono::Duration::seconds(1);
        assert!(!pvc_retention_gc_due(&sb, just_before, &cfg));

        // At/after the deadline: due.
        let at_deadline = anchor + chrono::Duration::days(7);
        assert!(pvc_retention_gc_due(&sb, at_deadline, &cfg));
        assert!(pvc_retention_gc_due(
            &sb,
            at_deadline + chrono::Duration::hours(1),
            &cfg
        ));
    }

    #[test]
    fn pvc_retention_gc_honors_explicit_spec_window() {
        let anchor = created_at();
        let cfg = pvc_retention_enabled();
        // Explicit 1h window overrides the 7-day default.
        let sb = paused_filesystem_sandbox(anchor, Some("PT1H"));
        assert!(!pvc_retention_gc_due(
            &sb,
            anchor + chrono::Duration::minutes(59),
            &cfg
        ));
        assert!(pvc_retention_gc_due(
            &sb,
            anchor + chrono::Duration::hours(1),
            &cfg
        ));
    }

    #[test]
    fn pvc_retention_gc_never_fires_when_disabled_or_not_filesystem() {
        let anchor = created_at();
        let way_past = anchor + chrono::Duration::days(3650);

        // Disabled controller-wide: never collect, however old.
        let disabled = PvcRetentionConfig::default();
        let sb = paused_filesystem_sandbox(anchor, None);
        assert!(!disabled.enabled);
        assert!(!pvc_retention_gc_due(&sb, way_past, &disabled));

        // Snapshot-mode pause (no `Suspended` condition): not a PVC GC candidate.
        let snapshot_paused = sandbox_with(0, false, Some(created_at()));
        assert!(!pvc_retention_gc_due(
            &snapshot_paused,
            way_past,
            &pvc_retention_enabled()
        ));
    }

    #[test]
    fn pvc_retention_gc_fails_safe_on_unparseable_window() {
        let anchor = created_at();
        // `7d` is not a valid ISO-8601 duration (missing `P`): resolution
        // returns None, so the sandbox is left alone rather than deleted under
        // a guessed window.
        let sb = paused_filesystem_sandbox(anchor, Some("7d"));
        let cfg = pvc_retention_enabled();
        assert_eq!(resolve_pvc_retention(&sb, &cfg), None);
        assert!(!pvc_retention_gc_due(
            &sb,
            anchor + chrono::Duration::days(3650),
            &cfg
        ));
    }

    #[test]
    fn paused_requeue_tightens_toward_the_retention_deadline() {
        let anchor = created_at();
        let cfg = pvc_retention_enabled();
        let heartbeat = Duration::from_secs(30);
        let sb = paused_filesystem_sandbox(anchor, Some("PT60S"));

        // 10s before the 60s deadline → requeue in ~10s, under the heartbeat.
        let near = anchor + chrono::Duration::seconds(50);
        assert_eq!(
            paused_requeue_interval(&sb, near, &cfg, heartbeat),
            Duration::from_secs(10)
        );

        // A snapshot-mode paused sandbox (no deadline) falls back to heartbeat.
        let snapshot_paused = sandbox_with(0, false, Some(created_at()));
        assert_eq!(
            paused_requeue_interval(&snapshot_paused, near, &cfg, heartbeat),
            heartbeat
        );
    }

    #[test]
    fn restore_staging_path_is_deterministic_per_sandbox() {
        // Epic #484: the restore Pod's `restore-image-path` annotation and
        // the `Stage` job's `target_path` are derived from this *same*
        // function, so it must depend only on the sandbox name and match
        // the layout the node DaemonSet stages into
        // (`<stagingRoot>/<sandbox-id>/restore`).
        assert_eq!(
            restore_staging_path("sbx-1"),
            "/var/lib/sandbawks/staging/sbx-1/restore"
        );
        assert_eq!(
            restore_staging_path("sbx-1"),
            restore_staging_path("sbx-1"),
            "the path must be a pure function of the sandbox name"
        );
        assert_ne!(
            restore_staging_path("sbx-1"),
            restore_staging_path("sbx-2"),
            "distinct sandboxes must stage at distinct paths"
        );
    }

    #[test]
    fn u32_from_env_parses_overrides_and_falls_back() {
        // Issue #490: the Helm-surfaced resume knobs parse a bare integer, and
        // an unset / blank / garbage value must fall back to the compile-time
        // default rather than panicking or zeroing the retry budget.
        let lookup = |pairs: &'static [(&str, &str)]| {
            move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| (*v).to_owned())
            }
        };
        assert_eq!(
            u32_from_env(lookup(&[("K", "9")]), "K", DEFAULT_RESUME_MAX_RESTARTS),
            9
        );
        // Surrounding whitespace (a YAML-quoted value) is tolerated.
        assert_eq!(u32_from_env(lookup(&[("K", " 3 ")]), "K", 5), 3);
        // Unset, blank, and non-numeric all fall back to the default.
        assert_eq!(u32_from_env(lookup(&[]), "K", 5), 5);
        assert_eq!(u32_from_env(lookup(&[("K", "")]), "K", 5), 5);
        assert_eq!(u32_from_env(lookup(&[("K", "nope")]), "K", 5), 5);
    }

    #[test]
    fn duration_secs_from_env_parses_overrides_and_falls_back() {
        // Same contract as `u32_from_env`, but the value is whole seconds coerced
        // into a `Duration` — this is how the kind smoke tunes the readiness
        // deadline down to trip the bounded-recovery path inside its wait budget.
        let lookup = |pairs: &'static [(&str, &str)]| {
            move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| (*v).to_owned())
            }
        };
        assert_eq!(
            duration_secs_from_env(lookup(&[("K", "30")]), "K", DEFAULT_RESUME_READY_DEADLINE),
            Duration::from_secs(30)
        );
        assert_eq!(
            duration_secs_from_env(lookup(&[]), "K", DEFAULT_RESUME_READY_DEADLINE),
            DEFAULT_RESUME_READY_DEADLINE
        );
        assert_eq!(
            duration_secs_from_env(lookup(&[("K", "  ")]), "K", Duration::from_secs(7)),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn string_from_env_parses_overrides_and_falls_back() {
        // Backs `restore_staging_root` (issue #490): a non-blank override wins
        // verbatim; unset or blank/whitespace-only falls back to the default —
        // the override and blank branches the inlined version shipped untested.
        let lookup = |pairs: &'static [(&str, &str)]| {
            move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| (*v).to_owned())
            }
        };
        // Override returned verbatim (a path is used as-is, not trimmed).
        assert_eq!(
            string_from_env(
                lookup(&[("K", "/custom/staging")]),
                "K",
                DEFAULT_RESTORE_STAGING_ROOT
            ),
            "/custom/staging"
        );
        // Unset and blank/whitespace-only fall back to the default.
        assert_eq!(
            string_from_env(lookup(&[]), "K", DEFAULT_RESTORE_STAGING_ROOT),
            DEFAULT_RESTORE_STAGING_ROOT
        );
        assert_eq!(
            string_from_env(lookup(&[("K", "   ")]), "K", DEFAULT_RESTORE_STAGING_ROOT),
            DEFAULT_RESTORE_STAGING_ROOT
        );
    }

    /// Backs the controller-side fail-fast guard (issue #512, PR #513 review):
    /// an absolute root passes; a relative or empty root is rejected so a
    /// standalone controller cannot record an unresolvable
    /// `io.sandbawks/restore-image-path` annotation. Mirrors the node binary's
    /// startup assertion. Tested on the pure predicate so it never touches the
    /// process-global `restore_staging_root` cache.
    #[test]
    fn ensure_staging_root_absolute_rejects_relative_paths() {
        // The compile-time default and any other absolute path pass.
        ensure_staging_root_absolute(DEFAULT_RESTORE_STAGING_ROOT).expect("absolute default ok");
        ensure_staging_root_absolute("/mnt/fast/staging").expect("absolute override ok");
        // Relative and empty roots are rejected.
        assert!(ensure_staging_root_absolute("relative/staging").is_err());
        assert!(ensure_staging_root_absolute("").is_err());
    }

    #[test]
    fn timeout_fires_with_auto_pause_once_elapsed() {
        let created = created_at();
        let sb = sandbox_with(60, true, Some(created));
        // Exactly at the deadline counts as elapsed (`>=`).
        let at_deadline = created + chrono::Duration::seconds(60);
        assert_eq!(
            timeout_trigger(&sb, at_deadline),
            Some(Trigger::Timeout { auto_pause: true })
        );
    }

    #[test]
    fn timeout_fires_without_auto_pause_once_elapsed() {
        let created = created_at();
        let sb = sandbox_with(60, false, Some(created));
        let past_deadline = created + chrono::Duration::seconds(61);
        assert_eq!(
            timeout_trigger(&sb, past_deadline),
            Some(Trigger::Timeout { auto_pause: false })
        );
    }

    #[test]
    fn timeout_does_not_fire_before_deadline() {
        let created = created_at();
        let sb = sandbox_with(60, true, Some(created));
        let before_deadline = created + chrono::Duration::seconds(59);
        assert_eq!(timeout_trigger(&sb, before_deadline), None);
    }

    #[test]
    fn zero_timeout_is_the_disabled_sentinel() {
        // Warm-pool members carry `timeout_seconds: 0`; it must mean "no
        // idle timeout", never "expired the instant it was created".
        let created = created_at();
        let sb = sandbox_with(0, true, Some(created));
        let far_future = created + chrono::Duration::days(365);
        assert_eq!(timeout_trigger(&sb, far_future), None);
    }

    #[test]
    fn missing_creation_timestamp_never_fires() {
        // A CR the apiserver has not yet stamped — and with no activity
        // stamp either — has no deadline to honor.
        let sb = sandbox_with(60, true, None);
        assert_eq!(timeout_trigger(&sb, created_at()), None);
    }

    #[test]
    fn recent_activity_keeps_past_creation_deadline_alive() {
        // Issue #481: the window is measured from `lastActivityTimestamp`,
        // not creation. A sandbox created long ago whose last activity is
        // recent must not fire, even well past `created + timeout`.
        let created = created_at();
        let now = created + chrono::Duration::hours(1);
        // Activity 10s ago; a 60s idle window → not yet idle.
        let sb = with_activity(
            sandbox_with(60, true, Some(created)),
            now - chrono::Duration::seconds(10),
        );
        assert_eq!(timeout_trigger(&sb, now), None);
    }

    #[test]
    fn stale_activity_fires_the_idle_timeout() {
        // The mirror case: last activity older than the window fires, with
        // the trigger mirroring `auto_pause`.
        let created = created_at();
        let now = created + chrono::Duration::hours(1);
        // Activity 120s ago; a 60s idle window → idle, must fire.
        let sb = with_activity(
            sandbox_with(60, true, Some(created)),
            now - chrono::Duration::seconds(120),
        );
        assert_eq!(
            timeout_trigger(&sb, now),
            Some(Trigger::Timeout { auto_pause: true })
        );
    }

    #[test]
    fn malformed_activity_falls_back_to_creation_baseline() {
        // A garbage `lastActivityTimestamp` degrades to the creation
        // baseline rather than disabling the timeout. Created at `created`
        // with a 60s window → fires at `created + 60`.
        let created = created_at();
        let mut sb = sandbox_with(60, true, Some(created));
        sb.status = Some(SandboxStatus {
            last_activity_timestamp: Some("not-a-timestamp".to_owned()),
            ..Default::default()
        });
        assert_eq!(
            timeout_trigger(&sb, created + chrono::Duration::seconds(60)),
            Some(Trigger::Timeout { auto_pause: true })
        );
    }

    #[test]
    fn requeue_caps_at_heartbeat_for_a_far_deadline() {
        let created = created_at();
        let sb = sandbox_with(3600, true, Some(created));
        let heartbeat = Duration::from_secs(30);
        // Just created → ~3600s remaining, capped back to the heartbeat.
        assert_eq!(running_requeue_interval(&sb, created, heartbeat), heartbeat);
    }

    #[test]
    fn requeue_tracks_a_near_deadline() {
        let created = created_at();
        let sb = sandbox_with(60, true, Some(created));
        let heartbeat = Duration::from_secs(30);
        // 5s before the deadline → requeue in 5s, not a full heartbeat.
        let now = created + chrono::Duration::seconds(55);
        assert_eq!(
            running_requeue_interval(&sb, now, heartbeat),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn requeue_floors_at_one_second_past_deadline() {
        let created = created_at();
        let sb = sandbox_with(60, true, Some(created));
        let heartbeat = Duration::from_secs(30);
        // Already past the deadline → re-probe on the next pass (1s floor),
        // never a zero/negative or busy-loop interval.
        let now = created + chrono::Duration::seconds(120);
        assert_eq!(
            running_requeue_interval(&sb, now, heartbeat),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn requeue_uses_heartbeat_when_timeout_disabled() {
        let created = created_at();
        let sb = sandbox_with(0, true, Some(created));
        let heartbeat = Duration::from_secs(30);
        assert_eq!(running_requeue_interval(&sb, created, heartbeat), heartbeat);
    }

    proptest! {
        /// `timeout_trigger` fires exactly when `now` has reached the
        /// `created + timeout` deadline (for any enabled timeout), and the
        /// emitted trigger mirrors `spec.auto_pause`.
        #[test]
        fn timeout_fires_iff_deadline_reached(
            timeout in 1u32..100_000,
            auto_pause in any::<bool>(),
            offset in -200_000i64..200_000,
        ) {
            let created = created_at();
            let sb = sandbox_with(timeout, auto_pause, Some(created));
            // now = deadline + offset, so the trigger should fire iff offset >= 0.
            let now = created
                + chrono::Duration::seconds(i64::from(timeout))
                + chrono::Duration::seconds(offset);
            let got = timeout_trigger(&sb, now);
            if offset >= 0 {
                prop_assert_eq!(got, Some(Trigger::Timeout { auto_pause }));
            } else {
                prop_assert_eq!(got, None);
            }
        }

        /// The requeue interval is always within `[1s, heartbeat]` for an
        /// enabled timeout — never zero, never longer than the heartbeat.
        #[test]
        fn requeue_interval_stays_within_bounds(
            timeout in 1u32..100_000,
            offset in -200_000i64..200_000,
        ) {
            let created = created_at();
            let sb = sandbox_with(timeout, true, Some(created));
            let heartbeat = Duration::from_secs(30);
            let now = created + chrono::Duration::seconds(offset);
            let interval = running_requeue_interval(&sb, now, heartbeat);
            prop_assert!(interval >= Duration::from_secs(1));
            prop_assert!(interval <= heartbeat);
        }
    }

    // ── Bounded `Resuming` recovery (epic #484, issue #489) ──────────────

    /// Build a `SandboxStatus` carrying exactly the fields the recovery loop
    /// reads: the restart count and the (optional) last-attempt anchor.
    fn resume_status(count: u32, last_attempt: Option<DateTime<Utc>>) -> SandboxStatus {
        SandboxStatus {
            resume_restart_count: count,
            resume_last_attempt: last_attempt.map(|t| t.to_rfc3339()),
            ..Default::default()
        }
    }

    #[test]
    fn resume_backoff_is_capped_exponential() {
        // Doubles per restart from the base...
        assert_eq!(resume_backoff(0), RESUME_BACKOFF_BASE);
        assert_eq!(resume_backoff(1), RESUME_BACKOFF_BASE * 2);
        assert_eq!(resume_backoff(2), RESUME_BACKOFF_BASE * 4);
        // ...and never exceeds the cap, even for an absurd count that would
        // overflow the naive `1 << count` shift.
        assert!(resume_backoff(3) <= RESUME_BACKOFF_CAP);
        assert_eq!(resume_backoff(1_000), RESUME_BACKOFF_CAP);
    }

    #[test]
    fn past_deadline_handles_missing_and_negative_elapsed() {
        let deadline = DEFAULT_RESUME_READY_DEADLINE;
        // No anchor stamped ⇒ unmeasurable ⇒ never fires.
        assert!(!past_deadline(None, deadline));
        // Anchor in the future (clock skew) ⇒ negative elapsed ⇒ never fires.
        assert!(!past_deadline(
            Some(chrono::Duration::seconds(-5)),
            deadline
        ));
        // Below the deadline ⇒ not yet.
        assert!(!past_deadline(
            Some(chrono::Duration::seconds(119)),
            deadline
        ));
        // At/over the deadline ⇒ fires.
        assert!(past_deadline(
            Some(chrono::Duration::seconds(120)),
            deadline
        ));
        assert!(past_deadline(
            Some(chrono::Duration::seconds(121)),
            deadline
        ));
    }

    #[test]
    fn recovery_recreates_when_budget_remains_and_backoff_elapsed() {
        let now = created_at();
        // One prior restart, last attempt well past the backoff window.
        let status = resume_status(1, Some(now - chrono::Duration::hours(1)));
        assert_eq!(
            resume_recovery_trigger(&status, "sbx-1", now, "pod terminal"),
            Some(Trigger::RestorePodLost),
        );
    }

    #[test]
    fn recovery_with_no_anchor_recreates_immediately() {
        // First loss before any attempt was stamped: no backoff to honor.
        let now = created_at();
        let status = resume_status(0, None);
        assert_eq!(
            resume_recovery_trigger(&status, "sbx-1", now, "pod terminal"),
            Some(Trigger::RestorePodLost),
        );
    }

    #[test]
    fn recovery_idles_within_backoff_window() {
        let now = created_at();
        // count=1 ⇒ backoff = base*2; last attempt only base ago ⇒ still inside
        // the window, so the loop must idle rather than recreate.
        let status = resume_status(1, Some(now - RESUME_BACKOFF_BASE));
        assert_eq!(
            resume_recovery_trigger(&status, "sbx-1", now, "pod terminal"),
            None,
        );
    }

    #[test]
    fn recovery_exhausts_the_budget_into_resume_exhausted() {
        let now = created_at();
        // The count has reached the cap: no further recreate — fold to Failed.
        let status = resume_status(
            DEFAULT_RESUME_MAX_RESTARTS,
            Some(now - chrono::Duration::hours(1)),
        );
        match resume_recovery_trigger(&status, "sbx-1", now, "pod Pending past deadline") {
            Some(Trigger::Failure { reason, message }) => {
                assert_eq!(reason, FailureReason::ResumeExhausted);
                assert!(
                    message.contains("Pending past deadline"),
                    "exhaustion message should carry the most recent loss reason, got {message:?}"
                );
            }
            other => panic!("expected ResumeExhausted Failure, got {other:?}"),
        }
    }

    #[test]
    fn exhaustion_check_precedes_the_backoff_gate() {
        // Even with a fresh anchor (inside any backoff window), an exhausted
        // budget folds to Failed rather than idling forever.
        let now = created_at();
        let status = resume_status(DEFAULT_RESUME_MAX_RESTARTS, Some(now));
        assert!(matches!(
            resume_recovery_trigger(&status, "sbx-1", now, "wedged"),
            Some(Trigger::Failure {
                reason: FailureReason::ResumeExhausted,
                ..
            })
        ));
    }

    // ── Filesystem-resume bounded withhold (issue #620 defense-in-depth) ──

    /// Build a `ReconcileContext` whose adapter reports a still-live restore
    /// Pod (`with_pod_terminated(false)`), the steady state for the #620
    /// scenario: the Pod is `1/1 Running` but envd never passed the
    /// serve-readiness gate, so the Sandbox is withheld in `Resuming`.
    fn fs_resume_ctx() -> super::ReconcileContext {
        use std::sync::Arc;

        use super::{PvcRetentionConfig, ReconcileContext};
        use crate::{
            FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore, FakeSnapshotJobStore,
        };

        ReconcileContext {
            store: Arc::new(FakeSandboxStore::new()),
            adapter: Arc::new(FakeRuntimeAdapter::new("fake").with_pod_terminated(false)),
            snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
            templates: Arc::new(FakeSandboxTemplateStore::strict()),
            client: None,
            pvc_retention: PvcRetentionConfig::default(),
            usage_sink: Arc::new(crate::usage::NoopUsageSink),
            usage_spill: Arc::new(crate::usage_spill::NoopUsageSpillQueue),
        }
    }

    /// A `Resuming` filesystem sandbox (no `snapshotRef`) with a live restore
    /// Pod whose `resume_last_attempt` anchor is `age` in the past.
    fn fs_resuming_sandbox(now: DateTime<Utc>, age: Duration) -> Sandbox {
        let age = chrono::Duration::from_std(age).expect("test age fits in chrono::Duration");
        let mut sb = sandbox_with(60, false, Some(created_at()));
        sb.status = Some(SandboxStatus {
            phase: SandboxPhase::Resuming,
            pod_ref: Some("ns/sbx-test-restore".into()),
            resume_last_attempt: Some((now - age).to_rfc3339()),
            // No snapshot_ref ⇒ this is the filesystem-resume cold boot path.
            ..Default::default()
        });
        sb
    }

    /// #620: a filesystem restore Pod that is alive but still not promoted past
    /// the readiness deadline must fold to bounded recovery — never hang
    /// `Resuming` forever as it did before the deadline was added to this path.
    #[tokio::test]
    async fn filesystem_resume_past_deadline_recovers() {
        let now = created_at();
        let ctx = fs_resume_ctx();
        // Anchor well past the readiness deadline; budget still has room.
        let sb = fs_resuming_sandbox(now, DEFAULT_RESUME_READY_DEADLINE + Duration::from_secs(30));
        assert_eq!(
            super::filesystem_resuming_trigger(&ctx, &sb, "sbx-test", now).await,
            Some(Trigger::RestorePodLost),
            "a live-but-unpromoted filesystem restore Pod past the readiness \
             deadline must recover, not hang"
        );
    }

    /// #620: inside the readiness deadline, a still-booting filesystem restore
    /// Pod is left to idle (no premature recovery).
    #[tokio::test]
    async fn filesystem_resume_within_deadline_idles() {
        let now = created_at();
        let ctx = fs_resume_ctx();
        // Anchor only a few seconds ago — well inside the readiness deadline.
        let sb = fs_resuming_sandbox(now, Duration::from_secs(5));
        assert_eq!(
            super::filesystem_resuming_trigger(&ctx, &sb, "sbx-test", now).await,
            None,
            "a freshly-booted filesystem restore Pod within its readiness \
             deadline must idle, not recover"
        );
    }

    /// #620: a vanished filesystem restore Pod still folds straight to `Failed`
    /// (the pre-existing cold-start behavior), independent of any deadline.
    #[tokio::test]
    async fn filesystem_resume_vanished_pod_fails() {
        use crate::FakeRuntimeAdapter;
        let now = created_at();
        let mut ctx = fs_resume_ctx();
        // Swap in an adapter that reports the Pod gone/terminal.
        ctx.adapter =
            std::sync::Arc::new(FakeRuntimeAdapter::new("fake").with_pod_terminated(true));
        // Anchor recent: this fires on the vanished-Pod check, not the deadline.
        let sb = fs_resuming_sandbox(now, Duration::from_secs(5));
        assert!(
            matches!(
                super::filesystem_resuming_trigger(&ctx, &sb, "sbx-test", now).await,
                Some(Trigger::Failure { .. })
            ),
            "a vanished filesystem restore Pod must fold to Failed"
        );
    }

    // ── Bounded pause-checkpoint retry (issue #510, tier 1) ──────────────

    /// Build a `SandboxStatus` carrying exactly the fields the pause retry loop
    /// reads: the retry count and the (optional) last-attempt anchor.
    fn pause_status(count: u32, last_attempt: Option<DateTime<Utc>>) -> SandboxStatus {
        SandboxStatus {
            pause_restart_count: count,
            pause_last_attempt: last_attempt.map(|t| t.to_rfc3339()),
            ..Default::default()
        }
    }

    #[test]
    fn pause_retry_redispatches_when_budget_remains_and_backoff_elapsed() {
        let now = created_at();
        // One prior retry, last attempt well past the backoff window.
        let status = pause_status(1, Some(now - chrono::Duration::hours(1)));
        assert_eq!(
            pause_recovery_trigger(&status, "sbx-1", now, "envd /__state timed out"),
            Some(Trigger::PauseRetry),
        );
    }

    #[test]
    fn pause_retry_with_no_anchor_redispatches_immediately() {
        // First failure before any attempt was stamped: no backoff to honor.
        let now = created_at();
        let status = pause_status(0, None);
        assert_eq!(
            pause_recovery_trigger(&status, "sbx-1", now, "checkpoint failed"),
            Some(Trigger::PauseRetry),
        );
    }

    #[test]
    fn pause_retry_idles_within_backoff_window() {
        let now = created_at();
        // count=1 ⇒ backoff = base*2; last attempt only base ago ⇒ still inside
        // the window, so the loop idles (caller keeps the job ref) rather than
        // re-dispatching.
        let status = pause_status(1, Some(now - RESUME_BACKOFF_BASE));
        assert_eq!(
            pause_recovery_trigger(&status, "sbx-1", now, "checkpoint failed"),
            None,
        );
    }

    #[test]
    fn pause_retry_exhausts_into_recoverable_failure() {
        let now = created_at();
        // The count has reached the cap: no further retry — fold to Failed, but
        // with the *recoverable* reason so the Failed-phase probe can recover/GC.
        let status = pause_status(
            DEFAULT_PAUSE_MAX_RETRIES,
            Some(now - chrono::Duration::hours(1)),
        );
        match pause_recovery_trigger(&status, "sbx-1", now, "envd /__state timed out after 10s") {
            Some(Trigger::Failure { reason, message }) => {
                assert_eq!(reason, FailureReason::PauseCheckpointFailed);
                assert!(
                    reason.is_recoverable(),
                    "exhausted pause-checkpoint fold must stay recoverable"
                );
                assert!(
                    message.contains("/__state timed out"),
                    "exhaustion message should carry the most recent failure, got {message:?}"
                );
            }
            other => panic!("expected PauseCheckpointFailed Failure, got {other:?}"),
        }
    }

    #[test]
    fn pause_exhaustion_message_locates_the_unreachable_host() {
        // #528: the node strips the pod IP from its Traceway-grouped error, so
        // the CR condition must surface *where* the probe failed from status
        // fields the controller already holds. An operator reading
        // `kubectl describe sandbox` should see the envd host and the node.
        let now = created_at();
        let mut status = pause_status(
            DEFAULT_PAUSE_MAX_RETRIES,
            Some(now - chrono::Duration::hours(1)),
        );
        status.envd_address = Some("10.42.3.17:49983".to_owned());
        status.assigned_node = Some("worker-02".to_owned());
        match pause_recovery_trigger(&status, "sbx-1", now, "connect: Host is unreachable") {
            Some(Trigger::Failure { message, .. }) => {
                assert!(
                    message.contains("10.42.3.17:49983"),
                    "condition must name the unreachable envd host, got {message:?}"
                );
                assert!(
                    message.contains("worker-02"),
                    "condition must name the sandbox's node, got {message:?}"
                );
                assert!(
                    message.contains("leaving the workload Pod running"),
                    "location must not displace the trailing clause, got {message:?}"
                );
            }
            other => panic!("expected PauseCheckpointFailed Failure, got {other:?}"),
        }
    }

    #[test]
    fn pause_failure_location_degrades_without_status_fields() {
        // No node / envd recorded yet: the suffix is empty rather than dangling
        // a half-formed "on node" clause, so the condition reads cleanly.
        assert_eq!(pause_failure_location(&SandboxStatus::default()), "");
        let with_node = SandboxStatus {
            assigned_node: Some("worker-02".to_owned()),
            ..Default::default()
        };
        assert_eq!(
            pause_failure_location(&with_node),
            "; sandbox was on node worker-02"
        );
        // envd-only: covers the fourth match arm — the host is the answer to
        // "which host was unreachable" even before the node is recorded.
        let with_envd = SandboxStatus {
            envd_address: Some("10.42.3.17:49983".to_owned()),
            ..Default::default()
        };
        assert_eq!(
            pause_failure_location(&with_envd),
            "; sandbox envd was at 10.42.3.17:49983"
        );
    }

    #[test]
    fn pause_exhaustion_check_precedes_the_backoff_gate() {
        // Even with a fresh anchor (inside any backoff window), an exhausted
        // budget folds to Failed rather than idling forever.
        let now = created_at();
        let status = pause_status(DEFAULT_PAUSE_MAX_RETRIES, Some(now));
        assert!(matches!(
            pause_recovery_trigger(&status, "sbx-1", now, "wedged"),
            Some(Trigger::Failure {
                reason: FailureReason::PauseCheckpointFailed,
                ..
            })
        ));
    }

    // ── phase_transition_event mapping ──────────────────────────────────────

    /// Spot-check the three most operator-visible reason codes to guard against
    /// regressions in event text and type. The exhaustive `match trigger` in
    /// `phase_transition_event` ensures every `Trigger` variant is handled;
    /// these tests prove the *right* fields are produced for the key paths.
    #[test]
    fn pod_ready_from_creating_yields_pod_ready_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Creating,
            SandboxPhase::Running,
            &Trigger::PodReady,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "PodReady");
    }

    #[test]
    fn pod_ready_from_resuming_yields_resume_succeeded_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Resuming,
            SandboxPhase::Running,
            &Trigger::PodReady,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "ResumeSucceeded");
    }

    #[test]
    fn failure_trigger_yields_warning_event_with_reason_and_message() {
        let (ty, reason, note) = phase_transition_event(
            SandboxPhase::Running,
            SandboxPhase::Failed,
            &Trigger::Failure {
                reason: FailureReason::ActionFailure,
                message: "adapter timed out".into(),
            },
        );
        assert_eq!(ty, EventType::Warning);
        assert_eq!(reason, "Failed");
        assert!(
            note.contains("action_failure"),
            "note must include the failure reason label; got: {note}"
        );
        assert!(
            note.contains("adapter timed out"),
            "note must include the failure message; got: {note}"
        );
    }

    #[test]
    fn auto_pause_timeout_yields_normal_auto_pause_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Running,
            SandboxPhase::Pausing,
            &Trigger::Timeout { auto_pause: true },
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "AutoPause");
    }

    #[test]
    fn eviction_with_snapshot_yields_warning_with_detail() {
        let msg = "node pressure: memory";
        let (ty, reason, note) = phase_transition_event(
            SandboxPhase::Running,
            SandboxPhase::Resuming,
            &Trigger::EvictedRecover {
                has_snapshot: true,
                message: msg.into(),
            },
        );
        assert_eq!(ty, EventType::Warning);
        assert_eq!(reason, "EvictionRecovery");
        assert!(
            note.contains(msg),
            "note must include eviction detail; got: {note}"
        );
    }

    #[test]
    fn pod_create_yields_normal_pod_created_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Creating,
            SandboxPhase::Creating,
            &Trigger::PodCreate,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "PodCreated");
    }

    #[test]
    fn pause_requested_yields_normal_pause_requested_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Running,
            SandboxPhase::Pausing,
            &Trigger::PauseRequested,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "PauseRequested");
    }

    #[test]
    fn resume_requested_yields_normal_resume_requested_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Paused,
            SandboxPhase::Resuming,
            &Trigger::ResumeRequested,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "ResumeRequested");
    }

    #[test]
    fn kill_requested_yields_normal_terminate_requested_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Running,
            SandboxPhase::Terminating,
            &Trigger::KillRequested,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "TerminateRequested");
    }

    #[test]
    fn timeout_no_auto_pause_yields_normal_timeout_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Running,
            SandboxPhase::Terminating,
            &Trigger::Timeout { auto_pause: false },
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "Timeout");
    }

    #[test]
    fn snapshot_validated_yields_normal_paused_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Pausing,
            SandboxPhase::Paused,
            &Trigger::SnapshotValidated,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "Paused");
    }

    #[test]
    fn node_assigned_yields_normal_node_assigned_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Resuming,
            SandboxPhase::Resuming,
            &Trigger::NodeAssigned,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "NodeAssigned");
    }

    #[test]
    fn stage_complete_yields_normal_snapshot_staged_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Resuming,
            SandboxPhase::Resuming,
            &Trigger::StageComplete,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "SnapshotStaged");
    }

    #[test]
    fn restore_pod_lost_yields_warning_restore_pod_lost_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Resuming,
            SandboxPhase::Resuming,
            &Trigger::RestorePodLost,
        );
        assert_eq!(ty, EventType::Warning);
        assert_eq!(reason, "RestorePodLost");
    }

    #[test]
    fn pause_retry_yields_warning_pause_retry_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Pausing,
            SandboxPhase::Pausing,
            &Trigger::PauseRetry,
        );
        assert_eq!(ty, EventType::Warning);
        assert_eq!(reason, "PauseRetry");
    }

    #[test]
    fn failed_pod_healthy_yields_normal_recovery_succeeded_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Failed,
            SandboxPhase::Running,
            &Trigger::FailedPodHealthy,
        );
        assert_eq!(ty, EventType::Normal);
        assert_eq!(reason, "RecoverySucceeded");
    }

    #[test]
    fn failed_pod_orphaned_yields_warning_orphaned_pod_gc_reason() {
        let (ty, reason, _) = phase_transition_event(
            SandboxPhase::Failed,
            SandboxPhase::Terminating,
            &Trigger::FailedPodOrphaned,
        );
        assert_eq!(ty, EventType::Warning);
        assert_eq!(reason, "OrphanedPodGC");
    }

    #[test]
    fn eviction_without_snapshot_yields_warning_with_reprovisioning_note() {
        let msg = "node pressure: disk";
        let (ty, reason, note) = phase_transition_event(
            SandboxPhase::Running,
            SandboxPhase::Creating,
            &Trigger::EvictedRecover {
                has_snapshot: false,
                message: msg.into(),
            },
        );
        assert_eq!(ty, EventType::Warning);
        assert_eq!(reason, "EvictionRecovery");
        assert!(
            note.contains("re-provisioning"),
            "note must mention re-provisioning; got: {note}"
        );
        assert!(
            note.contains(msg),
            "note must include eviction detail; got: {note}"
        );
    }

    /// Epic #603 Phase 4 / issue #607: `resolve_pod_inputs` is the single edge
    /// where both the persistence mode and the runtime class are fully resolved
    /// (template defaults filled), so it rejects the `snapshot`-on-Kata pairing
    /// — including a Kata template that omits `persistenceMode` (which resolves
    /// to the `snapshot` default) — while passing `filesystem`-on-Kata through.
    /// This is the defense-in-depth guard that catches a hand-applied CR the
    /// gateway's admission check never sees.
    #[tokio::test]
    async fn resolve_pod_inputs_rejects_snapshot_on_kata() {
        use std::sync::Arc;

        use sandbawks_types::{PersistenceMode, SandboxTemplate, SandboxTemplateSpec};

        use super::{resolve_pod_inputs, PvcRetentionConfig, ReconcileContext};
        use crate::{
            FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore, FakeSnapshotJobStore,
        };

        // A Kata-runtime-class template, parameterized on its persistence mode.
        fn kata_template(name: &str, mode: Option<PersistenceMode>) -> SandboxTemplate {
            SandboxTemplate::new(
                name,
                SandboxTemplateSpec {
                    image: "ghcr.io/hypotheosis/test-workload:latest".into(),
                    default_runtime_class: "kata-vm-isolation".into(),
                    default_resources: ResourceRequests {
                        cpu: "1".parse().expect("valid cpu"),
                        memory: "1Gi".parse().expect("valid memory"),
                        ephemeral_storage: None,
                    },
                    envd_image: None,
                    default_timeout_seconds: 60,
                    network_policy: None,
                    tolerations: None,
                    node_selector: None,
                    security_context: None,
                    persistence_mode: mode,
                    volume_claim_templates: None,
                    volume_claim_templates_policy: None,
                },
            )
        }

        let templates = Arc::new(FakeSandboxTemplateStore::strict());
        // Reject: Kata class + persistenceMode unset → resolves to `snapshot`.
        templates.insert(kata_template("kata-default", None));
        // Accept: Kata class + explicit `filesystem`.
        templates.insert(kata_template("kata-fs", Some(PersistenceMode::Filesystem)));

        let ctx = ReconcileContext {
            store: Arc::new(FakeSandboxStore::new()),
            adapter: Arc::new(FakeRuntimeAdapter::new("fake")),
            snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
            templates,
            client: None,
            pvc_retention: PvcRetentionConfig::default(),
            usage_sink: Arc::new(crate::usage::NoopUsageSink),
            usage_spill: Arc::new(crate::usage_spill::NoopUsageSpillQueue),
        };

        let sandbox_for = |template_ref: &str| {
            let mut sb = sandbox_with(60, false, None);
            sb.spec.template_ref = template_ref.into();
            sb
        };

        // Default `snapshot` on a Kata class → rejected with an actionable
        // message naming the offending class and the `filesystem` fix.
        // `resolve_pod_inputs` returns a `Cow` borrowing its input, so the
        // sandbox is bound to a local that outlives the resolved value.
        let snapshot_sandbox = sandbox_for("kata-default");
        let err = resolve_pod_inputs(&ctx, "ns", &snapshot_sandbox)
            .await
            .expect_err("snapshot-on-Kata must be rejected at resolve");
        let msg = err.to_string();
        assert!(
            msg.contains("filesystem") && msg.contains("kata-vm-isolation"),
            "error must name the runtime class and the fix; got: {msg}"
        );

        // Explicit `filesystem` on the same Kata class → resolves cleanly with
        // both the runtime class and the mode stamped onto the effective spec.
        let fs_sandbox = sandbox_for("kata-fs");
        let (_image, resolved, _suppressed) = resolve_pod_inputs(&ctx, "ns", &fs_sandbox)
            .await
            .expect("filesystem-on-Kata must resolve");
        assert_eq!(
            resolved.spec.persistence_mode,
            Some(PersistenceMode::Filesystem)
        );
        assert_eq!(
            resolved.spec.runtime_class_name.as_deref(),
            Some("kata-vm-isolation")
        );
    }

    /// Issue #616: a Kata template that declares `volumeClaimTemplates` but
    /// leaves `persistenceMode` unset resolves to `filesystem` at
    /// `resolve_pod_inputs` (the inference fires because the effective VCT set is
    /// non-empty) and so passes the #607 guard — declaring storage is itself the
    /// signal to opt into filesystem persistence. A sibling template with the
    /// same unset mode but *no* VCTs still resolves to `snapshot` and is rejected
    /// on Kata, proving the default case is unchanged.
    #[tokio::test]
    async fn resolve_pod_inputs_infers_filesystem_from_vcts() {
        use std::sync::Arc;

        use sandbawks_types::{
            PersistenceMode, SandboxTemplate, SandboxTemplateSpec, VolumeClaimSpec,
            VolumeClaimTemplate, VolumeClaimTemplateMetadata,
        };

        use super::{resolve_pod_inputs, PvcRetentionConfig, ReconcileContext};
        use crate::{
            FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore, FakeSnapshotJobStore,
        };

        fn vct(name: &str) -> VolumeClaimTemplate {
            VolumeClaimTemplate {
                metadata: VolumeClaimTemplateMetadata {
                    name: name.into(),
                    ..Default::default()
                },
                spec: VolumeClaimSpec {
                    storage: "1Gi".parse().expect("valid quantity"),
                    access_modes: Vec::new(),
                    storage_class_name: None,
                    volume_mode: None,
                },
            }
        }

        // A Kata template with `persistenceMode` unset, parameterized on whether
        // it declares `volumeClaimTemplates`.
        fn kata_template(name: &str, vcts: Option<Vec<VolumeClaimTemplate>>) -> SandboxTemplate {
            SandboxTemplate::new(
                name,
                SandboxTemplateSpec {
                    image: "ghcr.io/hypotheosis/test-workload:latest".into(),
                    default_runtime_class: "kata-vm-isolation".into(),
                    default_resources: ResourceRequests {
                        cpu: "1".parse().expect("valid cpu"),
                        memory: "1Gi".parse().expect("valid memory"),
                        ephemeral_storage: None,
                    },
                    envd_image: None,
                    default_timeout_seconds: 60,
                    network_policy: None,
                    tolerations: None,
                    node_selector: None,
                    security_context: None,
                    persistence_mode: None,
                    volume_claim_templates: vcts,
                    volume_claim_templates_policy: None,
                },
            )
        }

        let templates = Arc::new(FakeSandboxTemplateStore::strict());
        // VCTs declared, mode unset → infer `filesystem` → passes the #607 guard.
        templates.insert(kata_template("kata-vcts", Some(vec![vct("workspace")])));
        // No VCTs, mode unset → `snapshot` default → rejected on Kata.
        templates.insert(kata_template("kata-novcts", None));

        let ctx = ReconcileContext {
            store: Arc::new(FakeSandboxStore::new()),
            adapter: Arc::new(FakeRuntimeAdapter::new("fake")),
            snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
            templates,
            client: None,
            pvc_retention: PvcRetentionConfig::default(),
            usage_sink: Arc::new(crate::usage::NoopUsageSink),
            usage_spill: Arc::new(crate::usage_spill::NoopUsageSpillQueue),
        };

        let sandbox_for = |template_ref: &str| {
            let mut sb = sandbox_with(60, false, None);
            sb.spec.template_ref = template_ref.into();
            sb
        };

        // VCTs ⇒ inferred `filesystem`, stamped onto the effective spec, and the
        // Kata runtime class resolves cleanly past the guard.
        let vct_sandbox = sandbox_for("kata-vcts");
        let (_image, resolved, _suppressed) = resolve_pod_inputs(&ctx, "ns", &vct_sandbox)
            .await
            .expect("VCT-declaring Kata template must infer filesystem and resolve");
        assert_eq!(
            resolved.spec.persistence_mode,
            Some(PersistenceMode::Filesystem),
            "unset mode + non-empty VCTs must infer filesystem",
        );
        assert_eq!(
            resolved.spec.runtime_class_name.as_deref(),
            Some("kata-vm-isolation")
        );

        // No VCTs ⇒ unchanged `snapshot` default ⇒ still rejected on Kata.
        let novct_sandbox = sandbox_for("kata-novcts");
        let err = resolve_pod_inputs(&ctx, "ns", &novct_sandbox)
            .await
            .expect_err("no-VCT Kata template must still reject the snapshot default");
        assert!(
            err.to_string().contains("filesystem"),
            "rejection must point at the filesystem fix; got: {err}"
        );
    }

    /// Epic #603 Phase 5 / issue #608: `resolve_pod_inputs` enforces the
    /// template's `volumeClaimTemplatesPolicy` at the single pod-creation edge.
    /// Under the default `Disallowed`, a tenant's explicit `volumeClaimTemplates`
    /// are dropped to the template baseline and reported as suppressions (for the
    /// status condition) rather than failing the resolve.
    #[tokio::test]
    async fn resolve_pod_inputs_enforces_vct_policy() {
        use std::sync::Arc;

        use sandbawks_types::{
            SandboxTemplate, SandboxTemplateSpec, VolumeClaimSpec, VolumeClaimTemplate,
            VolumeClaimTemplateMetadata, VolumeClaimTemplatesPolicy,
        };

        use super::{resolve_pod_inputs, PvcRetentionConfig, ReconcileContext};
        use crate::{
            FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore, FakeSnapshotJobStore,
        };

        fn vct(name: &str) -> VolumeClaimTemplate {
            VolumeClaimTemplate {
                metadata: VolumeClaimTemplateMetadata {
                    name: name.into(),
                    ..Default::default()
                },
                spec: VolumeClaimSpec {
                    storage: "1Gi".parse().expect("valid quantity"),
                    access_modes: Vec::new(),
                    storage_class_name: None,
                    volume_mode: None,
                },
            }
        }

        fn template(name: &str, policy: Option<VolumeClaimTemplatesPolicy>) -> SandboxTemplate {
            SandboxTemplate::new(
                name,
                SandboxTemplateSpec {
                    image: "ghcr.io/hypotheosis/test-workload:latest".into(),
                    default_runtime_class: "gvisor".into(),
                    default_resources: ResourceRequests {
                        cpu: "1".parse().expect("valid cpu"),
                        memory: "1Gi".parse().expect("valid memory"),
                        ephemeral_storage: None,
                    },
                    envd_image: None,
                    default_timeout_seconds: 60,
                    network_policy: None,
                    tolerations: None,
                    node_selector: None,
                    security_context: None,
                    persistence_mode: None,
                    volume_claim_templates: Some(vec![vct("workspace")]),
                    volume_claim_templates_policy: policy,
                },
            )
        }

        let templates = Arc::new(FakeSandboxTemplateStore::strict());
        templates.insert(template("tpl-disallowed", None)); // None ⇒ Disallowed
        templates.insert(template(
            "tpl-allowed",
            Some(VolumeClaimTemplatesPolicy::Allowed),
        ));

        let ctx = ReconcileContext {
            store: Arc::new(FakeSandboxStore::new()),
            adapter: Arc::new(FakeRuntimeAdapter::new("fake")),
            snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
            templates,
            client: None,
            pvc_retention: PvcRetentionConfig::default(),
            usage_sink: Arc::new(crate::usage::NoopUsageSink),
            usage_spill: Arc::new(crate::usage_spill::NoopUsageSpillQueue),
        };

        let sandbox_for = |template_ref: &str| {
            let mut sb = sandbox_with(60, false, None);
            sb.spec.template_ref = template_ref.into();
            // Tenant tries to bring its own `cache` claim.
            sb.spec.volume_claim_templates = Some(vec![vct("cache")]);
            sb
        };

        // Default `Disallowed`: tenant `cache` dropped, effective = template's
        // `workspace` only, and the drop is reported as a suppression.
        let sb = sandbox_for("tpl-disallowed");
        let (_image, resolved, suppressed) = resolve_pod_inputs(&ctx, "ns", &sb)
            .await
            .expect("resolve must succeed (policy suppresses, never fails)");
        let names: Vec<&str> = resolved
            .spec
            .volume_claim_templates
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|v| v.metadata.name.as_str())
            .collect();
        assert_eq!(
            names,
            ["workspace"],
            "Disallowed keeps only template claims"
        );
        assert_eq!(suppressed.len(), 1);
        assert_eq!(suppressed[0].name, "cache");

        // `Allowed`: tenant `cache` is a new name, so it is admitted alongside
        // the template's `workspace`, and nothing is suppressed.
        let sb = sandbox_for("tpl-allowed");
        let (_image, resolved, suppressed) = resolve_pod_inputs(&ctx, "ns", &sb)
            .await
            .expect("resolve must succeed");
        let names: Vec<&str> = resolved
            .spec
            .volume_claim_templates
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|v| v.metadata.name.as_str())
            .collect();
        assert_eq!(
            names,
            ["workspace", "cache"],
            "Allowed admits new-named adds"
        );
        assert!(suppressed.is_empty());
    }

    /// Issue #617: `resolve_pod_inputs` fills an *empty* `securityContext` slot
    /// from the template (same empty-slot rule as tolerations/nodeSelector), and
    /// an explicit value on the Sandbox wins over the template's.
    #[tokio::test]
    async fn resolve_pod_inputs_inherits_security_context() {
        use std::sync::Arc;

        use sandbawks_types::{PodSecurityContext, SandboxTemplate, SandboxTemplateSpec};

        use super::{resolve_pod_inputs, PvcRetentionConfig, ReconcileContext};
        use crate::{
            FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore, FakeSnapshotJobStore,
        };

        fn template_with_fs_group(name: &str, fs_group: i64) -> SandboxTemplate {
            SandboxTemplate::new(
                name,
                SandboxTemplateSpec {
                    image: "ghcr.io/hypotheosis/test-workload:latest".into(),
                    default_runtime_class: "gvisor".into(),
                    default_resources: ResourceRequests {
                        cpu: "1".parse().expect("valid cpu"),
                        memory: "1Gi".parse().expect("valid memory"),
                        ephemeral_storage: None,
                    },
                    envd_image: None,
                    default_timeout_seconds: 60,
                    network_policy: None,
                    tolerations: None,
                    node_selector: None,
                    security_context: Some(PodSecurityContext {
                        fs_group: Some(fs_group),
                        ..Default::default()
                    }),
                    persistence_mode: None,
                    volume_claim_templates: None,
                    volume_claim_templates_policy: None,
                },
            )
        }

        let templates = Arc::new(FakeSandboxTemplateStore::strict());
        templates.insert(template_with_fs_group("tpl-sc", 1000));

        let ctx = ReconcileContext {
            store: Arc::new(FakeSandboxStore::new()),
            adapter: Arc::new(FakeRuntimeAdapter::new("fake")),
            snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
            templates,
            client: None,
            pvc_retention: PvcRetentionConfig::default(),
            usage_sink: Arc::new(crate::usage::NoopUsageSink),
            usage_spill: Arc::new(crate::usage_spill::NoopUsageSpillQueue),
        };

        // Empty slot → inherits the template's fsGroup=1000.
        let mut sb = sandbox_with(60, false, None);
        sb.spec.template_ref = "tpl-sc".into();
        let (_image, resolved, _suppressed) = resolve_pod_inputs(&ctx, "ns", &sb)
            .await
            .expect("resolve must succeed");
        assert_eq!(
            resolved
                .spec
                .security_context
                .as_ref()
                .and_then(|s| s.fs_group),
            Some(1000),
            "an absent securityContext inherits the template's"
        );

        // Explicit Sandbox value wins over the template's.
        let mut sb = sandbox_with(60, false, None);
        sb.spec.template_ref = "tpl-sc".into();
        sb.spec.security_context = Some(PodSecurityContext {
            fs_group: Some(2000),
            ..Default::default()
        });
        let (_image, resolved, _suppressed) = resolve_pod_inputs(&ctx, "ns", &sb)
            .await
            .expect("resolve must succeed");
        assert_eq!(
            resolved
                .spec
                .security_context
                .as_ref()
                .and_then(|s| s.fs_group),
            Some(2000),
            "an explicit securityContext wins over the template default"
        );
    }
}
