//! Per-node `SnapshotJob` reconciler.
//!
//! `sandbawks-node` runs as a DaemonSet on every cluster node. Each Pod
//! watches the cluster's `SnapshotJob` CRs filtered to its own
//! `spec.nodeName` (via the kube field-selector
//! `spec.nodeName=$NODE_NAME`) and dispatches each job to the local
//! [`RuntimeAdapter`] (`GvisorAdapter` in production): `pause`
//! (checkpoint + upload), `resume` (download + restore), or
//! `stage_snapshot` (download + materialize onto local disk for the
//! containerd-shim restore path, Plan 07).
//!
//! ## Lifecycle
//!
//! For each observed `SnapshotJob`, the reconcile function:
//!
//! 1. Reads `status.phase`. Terminal phases (`Complete` / `Failed`) are
//!    no-ops so requeues after the worker has finished don't re-run the
//!    underlying side-effects.
//! 2. Patches `status.phase` to the appropriate in-flight phase
//!    (`Checkpointing` for `Pause`, `Restoring` for `Resume`) so an
//!    observer can see what the worker is doing — and so a crash mid
//!    flight leaves a non-terminal marker for the next pass to retry.
//! 3. Calls `adapter.pause(...)` / `adapter.resume(...)`.
//! 4. **Pause only (issue #235):** on a successful checkpoint+upload,
//!    patches `Validating` and calls `adapter.validate_restorable(...)`
//!    to prove the just-uploaded bundle restorable *before* reporting
//!    `Complete`. The controller advances `Pausing → Paused` (making the
//!    source Pod reclaimable) only on `Complete`, so a snapshot that
//!    fails this gate lands the job `Failed` with the source still
//!    recoverable rather than as unrecoverable data loss at resume time.
//! 5. Patches `status.phase` again with the terminal result — `Complete`
//!    plus the produced `manifest` on success, or `Failed` with the
//!    error stringified into `message` on failure.
//!
//! ## Idempotency
//!
//! Two floors protect the adapter from re-runs that would corrupt the
//! sandbox or its snapshot:
//!
//! 1. **Terminal-phase**: once a job has reached `Complete` or
//!    `Failed`, no further work happens.
//! 2. **In-flight + recent `started_at`**: a job we've already begun
//!    (phase ∈ {`Pending`, `Checkpointing`, `Uploading`, `Validating`,
//!    `Restoring`} AND `started_at` within [`STUCK_JOB_RECOVERY_AGE`])
//!    is also a no-op. This catches the issue #194 race where the kube-rs
//!    Controller re-fires reconcile with a stale cached object before
//!    the terminal-phase watch event has landed — without this floor,
//!    the re-fire re-ran `pause()` against a container the first run
//!    had already checkpointed (and thus terminated), producing a
//!    Failed terminal phase that overwrote the prior Complete.
//!
//! Recovery from a genuinely crashed worker: if the in-flight phase
//! has `started_at` older than [`STUCK_JOB_RECOVERY_AGE`], we presume
//! the previous worker pod went away mid-pause and re-dispatch. The
//! threshold is sized large enough that no legitimate pause/resume
//! should ever hit it; see the constant's docs.
//!
//! ## Library-first
//!
//! The reconcile function is exposed as [`reconcile`] (testable directly
//! against an in-memory job + fake adapter) with [`run`] booting the
//! kube-rs `Controller` on top of it for production. Tests in
//! `tests/reconcile.rs` exercise the dispatch + idempotency + failure
//! paths without standing up a real apiserver.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use kube::{
    api::{Patch, PatchParams},
    runtime::{controller::Action, watcher, Controller},
    Api, Client,
};
use sandbawks_types::{
    runtime::{RuntimeAdapter, RuntimeError, SnapshotManifest},
    snapshot::{SnapshotSink, SnapshotSource},
    SnapshotJob, SnapshotJobMode, SnapshotJobPhase, SnapshotJobStatus,
};
use thiserror::Error;

/// In-flight SnapshotJobs older than this are presumed crashed (worker
/// pod OOMed mid-checkpoint, kubelet killed it, etc.) and re-dispatched
/// on the next reconcile pass. Sized to be larger than any legitimate
/// pause/resume on the slowest realistic workload:
///
/// - spec §13 ceiling is 8 s pause / 2 s resume p95 for a ~1 GiB
///   sandbox (`crates/sandbawks-types/src/runtime.rs`).
/// - A 1 GiB workload's `runsc checkpoint` + tar+zstd of the
///   `*.img` files + S3 upload bounds at ~30 s on a slow CI runner.
/// - 5 minutes leaves ~10x headroom while still catching a genuinely
///   wedged job within one bench-iteration's 60 s wait_for_phase budget
///   (the bench itself will time the iteration out first; this floor
///   gates the NEXT reconcile pass that fires after).
///
/// Picked deliberately conservative — a false re-dispatch costs us
/// another pause attempt against a (probably-dead) container, which is
/// harmless but wasteful; a missed recovery wedges the SnapshotJob
/// forever, which is much worse.
const STUCK_JOB_RECOVERY_AGE: Duration = Duration::from_secs(5 * 60);

/// Failures raised by the node-side reconcile loop.
#[derive(Debug, Error)]
pub enum NodeReconcileError {
    /// The observed `SnapshotJob` had no `metadata.name`. Treated as a
    /// hard invariant — the apiserver never returns nameless objects in
    /// a watch stream, but the type system can't enforce that.
    #[error("SnapshotJob is missing metadata.name")]
    MissingName,
    /// The observed `SnapshotJob` had no `metadata.namespace`. Same
    /// invariant rationale as [`Self::MissingName`].
    #[error("SnapshotJob is missing metadata.namespace")]
    MissingNamespace,
    /// A `Resume` or `Stage` job was dispatched but its
    /// `spec.snapshotRef` is `None`. The controller must populate this
    /// from `Sandbox.status.snapshotRef`; absence here is a controller
    /// bug, not a transient error, so we surface it as the job's
    /// `Failed`-phase `message`.
    #[error("Resume/Stage SnapshotJob {0} has no spec.snapshotRef")]
    MissingSnapshotRef(String),
    /// Status patch (server-side apply on the `status` subresource)
    /// failed against the kube apiserver. Treated as transient — the
    /// kube-rs `Controller` will requeue with backoff.
    #[error("kube transport: {0}")]
    Kube(#[from] kube::Error),
    /// Serializing the status patch payload to JSON failed. In practice
    /// this is unreachable because every field is `Serialize`, but
    /// `patch_status` writes JSON through `serde_json::json!` which
    /// returns `Result`.
    #[error("serialize status patch: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Write-side of `SnapshotJob` status — factored to a trait so unit
/// tests can drive the reconcile function without an apiserver. The
/// real implementation is [`KubeSnapshotJobStatusStore`]; tests pass
/// in-memory doubles that record the sequence of status patches.
#[async_trait]
pub trait SnapshotJobStatusStore: Send + Sync {
    /// Server-side-apply patch on the `status` subresource of the named
    /// `SnapshotJob`. Implementations must persist `new_status`
    /// atomically — partial writes leave the job in an inconsistent
    /// phase.
    ///
    /// # Errors
    ///
    /// Returns [`NodeReconcileError::Kube`] for transient apiserver
    /// failures (network, 409 conflict, etc.). The `Controller`'s
    /// `error_policy` is responsible for backing off and retrying.
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SnapshotJobStatus,
    ) -> Result<(), NodeReconcileError>;
}

/// Production [`SnapshotJobStatusStore`] backed by a `kube::Client`.
///
/// Each `patch_status` call constructs a namespaced
/// `kube::Api<SnapshotJob>` rather than holding one — the
/// `kube::Client` itself is cheap-to-clone (`Arc`-wrapped internally)
/// and reusing one client across multiple namespaces (should the
/// DaemonSet ever scope to more than `default`) stays painless.
#[derive(Clone)]
pub struct KubeSnapshotJobStatusStore {
    client: Client,
}

impl KubeSnapshotJobStatusStore {
    /// Wrap an existing `kube::Client`.
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SnapshotJobStatusStore for KubeSnapshotJobStatusStore {
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SnapshotJobStatus,
    ) -> Result<(), NodeReconcileError> {
        let api: Api<SnapshotJob> = Api::namespaced(self.client.clone(), namespace);
        // The apiserver expects the patch body to be a top-level
        // object with a single `status` key — mirroring the controller-
        // side `KubeSandboxStore::patch_status` payload.
        let patch = serde_json::json!({ "status": new_status });
        api.patch_status(
            name,
            &PatchParams::apply("sandbawks-node"),
            &Patch::Merge(&patch),
        )
        .await?;
        Ok(())
    }
}

/// Dependencies threaded into [`reconcile`] (and [`run`] above it).
///
/// Cheap to `Arc::clone` — every field is itself behind an `Arc` (or is
/// a small string) so the kube-rs `Controller` cloning the context
/// across each requeue is free.
pub struct ReconcileContext {
    /// `Node.metadata.name` this Pod was scheduled onto. Set from the
    /// `NODE_NAME` env var (downward API). The watch is filtered to
    /// `spec.nodeName=$NODE_NAME` so the reconciler never sees jobs
    /// targeted at other nodes.
    pub node_name: String,
    /// Runtime backend that performs the actual checkpoint / restore.
    /// Production wires [`crate::gvisor::GvisorAdapter`].
    pub adapter: Arc<dyn RuntimeAdapter>,
    /// Where checkpoint bytes are written. Production is
    /// [`crate::s3::S3SnapshotSink`].
    pub sink: Arc<dyn SnapshotSink>,
    /// Where restore bytes are read from. Production is
    /// [`crate::s3::S3SnapshotSource`].
    pub source: Arc<dyn SnapshotSource>,
    /// Status-patch surface; production is
    /// [`KubeSnapshotJobStatusStore`].
    pub status_store: Arc<dyn SnapshotJobStatusStore>,
    /// Capability gate set when [`RuntimeError::RunscRootUnresolved`] fires
    /// for a real container (#379). The [`crate::node_label`] reconciler
    /// reads this on every tick and withdraws the `sandbawks.dev/envd-staged`
    /// label while it is true, so the warm pool stops scheduling new pause
    /// jobs on this node. The flag is sticky for the life of the process —
    /// it is only cleared by a DaemonSet restart (operator corrects the mount
    /// and rolls the Pod).
    ///
    /// Shared (via `Arc`) with [`crate::node_label::run_envd_staged_reconciler`];
    /// the reconciler sets it on failure, the label reconciler reads it on
    /// each tick.
    pub runsc_root_failed: Arc<AtomicBool>,
}

/// Spin up the `SnapshotJob` controller and run it until the kube watch
/// stream terminates (or `shutdown_on_signal` fires on SIGINT/SIGTERM).
///
/// `client` is consumed to build the namespaced `Api<SnapshotJob>` the
/// controller watches; the same client can be reused by the
/// [`SnapshotJobStatusStore`] inside `ctx` (production wires one
/// `Client` into both places).
///
/// # Errors
///
/// Returns `anyhow::Error` if the controller stream itself fails fatally
/// (rare — most errors are routed through `error_policy` and retried).
pub async fn run(
    client: Client,
    namespace: String,
    ctx: Arc<ReconcileContext>,
) -> anyhow::Result<()> {
    let api: Api<SnapshotJob> = Api::all(client);
    // Field-selector keeps the watch budget bounded: with N nodes and M jobs
    // cluster-wide, each node only streams its own jobs (spec.nodeName) across
    // ALL namespaces instead of all M. The watch is cluster-wide (#582) so a
    // single node daemonset serves every namespace; `namespace` below is only
    // the node's install namespace, used for telemetry, not a watch scope.
    let cfg = watcher::Config::default().fields(&format!("spec.nodeName={}", ctx.node_name));

    tracing::info!(
        node = %ctx.node_name,
        install_namespace = %namespace,
        "sandbawks-node SnapshotJob controller starting (cluster-wide watch)"
    );
    // Publish the node's install namespace as the `snapshotjob.watch_info`
    // gauge (#335). Since #582 the watch is cluster-wide, so this is a stable
    // `node → install-namespace` identity signal, not a watch scope — the #333
    // "wrong watched namespace" failure mode no longer applies.
    crate::snapshotjob_metrics::record_watch_namespace(&ctx.node_name, &namespace);

    Controller::new(api, cfg)
        .shutdown_on_signal()
        .run(reconcile_one, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "reconciled SnapshotJob"),
                // `event queue error` here is kube-rs's *transient watcher
                // reconnect*, not a dropped SnapshotJob (#363): a broken watch
                // stream (e.g. an apiserver connectivity blip) surfaces once,
                // then `Controller::run` re-lists and re-establishes the watch,
                // so no pause is lost. Already WARN (correctly below the ERROR
                // the node_label loop reserves for sustained failure), so it
                // does not get the same escalation gating — a single line per
                // reconnect is not spammy the way a per-tick ERROR is.
                Err(e) => tracing::warn!(error = %e, "reconcile SnapshotJob error"),
            }
        })
        .await;
    Ok(())
}

/// kube-rs `Controller` adapter — unwraps the `Arc<SnapshotJob>`,
/// delegates to [`reconcile`], and converts the unit success into an
/// `Action::requeue(30s)` heartbeat. The watch stream is what drives
/// the controller in steady state; the heartbeat is a safety net for
/// missed events.
async fn reconcile_one(
    job: Arc<SnapshotJob>,
    ctx: Arc<ReconcileContext>,
) -> Result<Action, NodeReconcileError> {
    reconcile(&job, &ctx).await?;
    Ok(Action::requeue(Duration::from_secs(30)))
}

/// kube-rs `Controller` error policy. Requeue after a short backoff —
/// transient kube transport failures are common during a cluster
/// upgrade and recover quickly.
fn error_policy(
    _job: Arc<SnapshotJob>,
    _err: &NodeReconcileError,
    _ctx: Arc<ReconcileContext>,
) -> Action {
    Action::requeue(Duration::from_secs(5))
}

/// Single-pass reconcile for one `SnapshotJob`.
///
/// Public so the unit tests can drive the state machine directly
/// against a [`crate::reconcile::SnapshotJobStatusStore`] fake without
/// standing up a `kube::runtime::Controller`. Production callers
/// invoke this through [`run`].
///
/// # Errors
///
/// Returns [`NodeReconcileError::Kube`] if a status patch fails — the
/// caller is expected to retry. `Resume` jobs with no `snapshotRef`
/// surface as [`NodeReconcileError::MissingSnapshotRef`] *after* the
/// `Failed`-phase status has been patched, so the controller observes
/// the failure even though the reconcile loop also propagates the
/// error upward.
// NOTE: this function is deliberately **not** `#[tracing::instrument]`'d. The
// idempotency floors below (terminal / in-flight-and-fresh) do no snapshot work
// and open no child spans; an eager root span here would render each as a
// childless "Task" in the APM, and because every lingering terminal/in-flight
// SnapshotJob re-reconciles on each 30s requeue, that floods the Task list with
// empty Tasks (issue #375). The `reconcile:node::snapshot_job` Task span is
// instead created in [`reconcile_active`], reached only when work is dispatched.
pub async fn reconcile(
    job: &SnapshotJob,
    ctx: &ReconcileContext,
) -> Result<(), NodeReconcileError> {
    let name = job
        .metadata
        .name
        .as_deref()
        .ok_or(NodeReconcileError::MissingName)?;
    let namespace = job
        .metadata
        .namespace
        .as_deref()
        .ok_or(NodeReconcileError::MissingNamespace)?;
    let prior_phase = job.status.as_ref().map(|s| s.phase).unwrap_or_default();
    let prior_started_at = job.status.as_ref().and_then(|s| s.started_at.as_deref());

    // Idempotency floors run *before* the Task span is created (see the note on
    // `reconcile` above): a job that is terminal, or in-flight and fresh, does
    // no snapshot work, so it must not open a `reconcile:node::snapshot_job`
    // Task at all (issue #375).
    if !should_dispatch(prior_phase, prior_started_at, Utc::now(), name, namespace) {
        return Ok(());
    }

    // Preserve the original `started_at` across retries so the controller can
    // see a stable "first picked up" timestamp even if the worker crashes
    // mid-pause and a sibling pod picks the job up.
    let started_at = job
        .status
        .as_ref()
        .and_then(|s| s.started_at.clone())
        .unwrap_or_else(|| Utc::now().to_rfc3339());

    reconcile_active(job, ctx, name, namespace, started_at).await
}

/// Decide whether this reconcile pass should dispatch snapshot work, or
/// short-circuit at one of the two idempotency floors.
///
/// Returns `false` (skip) for a job already in a terminal phase, or in-flight
/// and younger than [`STUCK_JOB_RECOVERY_AGE`]; `true` (dispatch) for a fresh
/// job, or one whose in-flight marker predates the recovery age (implying a
/// crashed worker — re-dispatch as crash recovery). `now` is injected so the
/// age comparison is deterministic under test.
///
/// Span-free and called *before* the `reconcile:node::snapshot_job` Task span
/// is created, so a skip emits no Task (issue #375). The only side effects are
/// the diagnostic logs, so the decision itself stays unit-testable.
fn should_dispatch(
    prior_phase: SnapshotJobPhase,
    prior_started_at: Option<&str>,
    now: DateTime<Utc>,
    name: &str,
    namespace: &str,
) -> bool {
    // Terminal floor: a job already in a terminal phase never gets
    // re-dispatched. Pre-T4 controllers might re-emit watch events for a job
    // after it lands at `Complete`; we ignore them rather than running `runsc
    // checkpoint` twice on the same container.
    if matches!(
        prior_phase,
        SnapshotJobPhase::Complete | SnapshotJobPhase::Failed
    ) {
        tracing::debug!(
            name = %name,
            namespace = %namespace,
            phase = ?prior_phase,
            "skipping reconcile of terminal SnapshotJob"
        );
        return false;
    }

    // Issue #194 (exposed in iteration 1 once issue #193's *.img glob fix
    // widened the pause window from ms to seconds): the kube-rs Controller can
    // re-fire reconcile with a stale cached object whose status still shows the
    // in-flight `Checkpointing` / `Uploading` / `Restoring` phase, before the
    // watch event for the terminal patch we just wrote has landed in the
    // reflector cache. Short-circuiting only on terminal phases let the
    // re-fired reconcile fall through and re-run pause() — which then timed out
    // on the /__state probe because the first pause()'s `runsc checkpoint` had
    // already terminated envd, and the second pause()'s Failed patch overwrote
    // the first one's Complete (issue #194's diagnostic signature).
    //
    // Fix: also short-circuit on in-flight phases. The presence of a
    // `started_at` is the canonical "we've already begun" signal — we stamp it
    // on the Checkpointing/Restoring pre-flight patch and never clear it.
    //
    // Crash recovery: if `started_at` is older than [`STUCK_JOB_RECOVERY_AGE`]
    // we presume the original worker crashed (e.g. DaemonSet pod OOMed
    // mid-checkpoint) and re-dispatch. Sized large enough that no legitimate
    // pause/resume should ever hit it on the slowest realistic workload — see
    // the constant's docs.
    if matches!(
        prior_phase,
        SnapshotJobPhase::Pending
            | SnapshotJobPhase::Checkpointing
            | SnapshotJobPhase::Uploading
            | SnapshotJobPhase::Validating
            | SnapshotJobPhase::Restoring
    ) {
        if let Some(started_at_str) = prior_started_at {
            match DateTime::parse_from_rfc3339(started_at_str) {
                Ok(started_at_ts) => {
                    let age = now.signed_duration_since(started_at_ts);
                    // Skip while the in-flight marker is younger than
                    // STUCK_JOB_RECOVERY_AGE: a fresh, still-running pass owns
                    // the job. Only an age *past* the recovery age is a
                    // presumed-crashed worker we re-dispatch for (below).
                    //
                    // A negative age — the marker is in the future, e.g. this
                    // worker's clock trails the one that stamped it — is also
                    // "younger than recovery", so it's treated as fresh and
                    // skipped rather than re-running `pause()` against a
                    // container a live pass already owns (which under clock skew
                    // would double-run the checkpoint, the issue #194 failure).
                    // This self-heals: as wall-clock advances `age` climbs and
                    // eventually crosses the threshold, so even a pathological
                    // far-future stamp re-dispatches rather than wedging forever.
                    let recovery = chrono::Duration::from_std(STUCK_JOB_RECOVERY_AGE)
                        .expect("STUCK_JOB_RECOVERY_AGE fits in chrono::Duration");
                    if age < recovery {
                        tracing::debug!(
                            name = %name,
                            namespace = %namespace,
                            phase = ?prior_phase,
                            age_secs = age.num_seconds(),
                            "skipping in-flight SnapshotJob (younger than stuck-recovery age)"
                        );
                        return false;
                    }
                    tracing::warn!(
                        name = %name,
                        namespace = %namespace,
                        phase = ?prior_phase,
                        age_secs = age.num_seconds(),
                        "in-flight SnapshotJob exceeded stuck-recovery age; presumed crashed, re-dispatching"
                    );
                }
                Err(e) => {
                    // Malformed timestamp shouldn't happen — we always emit RFC
                    // 3339 via `Utc::now().to_rfc3339()` — but if it does, log +
                    // re-dispatch rather than wedge silently forever.
                    tracing::warn!(
                        name = %name,
                        namespace = %namespace,
                        started_at = %started_at_str,
                        error = %e,
                        "malformed started_at on in-flight SnapshotJob; re-dispatching"
                    );
                }
            }
        }
        // No `started_at` stamped but phase is in-flight: shouldn't happen via
        // our reconciler (we always patch them together), but a
        // manually-applied SnapshotJob with a partial status would land here.
        // Treat as fresh and run.
    }

    true
}

/// Dispatch path for a `SnapshotJob` that cleared the idempotency floors.
///
/// This is where the `reconcile:node::snapshot_job` Task span is created — only
/// reached when there is real snapshot work to do, so no-op reconciles never
/// emit an (empty) Task (issue #375).
///
/// `otel.kind = "consumer"` + `messaging.*` render this as a "Task" in the APM
/// (the node consumes SnapshotJob watch events, not HTTP). See the note on
/// `reconcile` in `sandbawks-controller/src/reconcile.rs` for the rationale.
/// `err` emits an OTel `exception` event on this Task span on `Err`, so
/// SnapshotJob reconcile faults surface in the APM's Issues view.
#[tracing::instrument(
    skip_all,
    name = "reconcile:node::snapshot_job",
    err,
    fields(
        job = name,
        otel.kind = "consumer",
        messaging.system = "kubernetes",
        messaging.operation = "process",
        messaging.destination.name = "SnapshotJob",
    )
)]
async fn reconcile_active(
    job: &SnapshotJob,
    ctx: &ReconcileContext,
    name: &str,
    namespace: &str,
    started_at: String,
) -> Result<(), NodeReconcileError> {
    // Continue the controller's trace, carried across the SnapshotJob queue in
    // the job's annotation (stamped by `build_snapshot_job`), so the pause /
    // resume work this node performs joins the api→controller trace and, in
    // turn, parents the downstream envd spans the gVisor probe injects (OTel
    // Phase 5). Runs under the active Task span; absent the annotation it leaves
    // the span a fresh trace root.
    sandbawks_observability::continue_from_annotations(job.metadata.annotations.as_ref());

    // Time + count the actual dispatch (#300). Record the claim *before*
    // dispatch (#335): this is the node committing to act on the job, so
    // `claims_total` reflects work picked up even if the dispatch then hangs —
    // the signal the cross-node completions counter and the boot-only namespace
    // log both failed to give in #333.
    let mode = job.spec.mode;
    crate::snapshotjob_metrics::record_claim(&ctx.node_name, mode);
    let dispatch_start = Instant::now();
    let result = dispatch(job, ctx, name, namespace, started_at).await;
    crate::snapshotjob_metrics::record_execution(mode, result.is_ok(), dispatch_start.elapsed());
    result
}

/// Dispatch the snapshot operation named by `job.spec.mode`, returning the
/// result of its terminal status patch.
///
/// Split out of [`reconcile`] so the metric wrapper there times exactly the
/// dispatch and nothing else: the reconcile short-circuits (terminal / in-flight
/// idempotency floors) return *before* this function is called, and each of the
/// three arms here `return`s its terminal-patch result, so the outcome is well
/// defined for every path the dispatch can take.
async fn dispatch(
    job: &SnapshotJob,
    ctx: &ReconcileContext,
    name: &str,
    namespace: &str,
    started_at: String,
) -> Result<(), NodeReconcileError> {
    // Scope the snapshot store to this sandbox's own keyspace
    // (`<base>/<sandbox-id>/...`) so concurrent sandboxes never share the
    // mutable `manifest.json` / `parts/<name>.json` keys. Pause and resume
    // both derive the same prefix from `job.spec.sandbox_name`, so the
    // `snapshotRef` a pause records and the keys a later resume reads always
    // agree — without parsing the URL. Without this, two sandboxes paused
    // against one bucket clobber each other's bundles and a resume fails with
    // a part/manifest sha256 mismatch (cross-sandbox corruption).
    let sink = ctx.sink.scoped(&job.spec.sandbox_name);
    let source = ctx.source.scoped(&job.spec.sandbox_name);
    match job.spec.mode {
        SnapshotJobMode::Pause => {
            // Pre-flight: signal the in-flight phase so an observer
            // (controller, kubectl, oncall) can distinguish "queued" from
            // "actively checkpointing". The middle phase (`Uploading`) is
            // not stamped here at T9 — the adapter does the upload
            // internally; T10's `--progress` flag will revisit.
            ctx.status_store
                .patch_status(
                    namespace,
                    name,
                    SnapshotJobStatus {
                        phase: SnapshotJobPhase::Checkpointing,
                        manifest: None,
                        message: None,
                        started_at: Some(started_at.clone()),
                        completed_at: None,
                        local_path: None,
                    },
                )
                .await?;

            // Checkpoint + upload. On success we hold the manifest but do
            // NOT report `Complete` yet — issue #235 gates the terminal
            // phase on a restorability proof so the controller can't reach
            // `Paused` (and thus reclaim the source) off an unrestorable
            // snapshot.
            let manifest = match ctx
                .adapter
                .pause(&job.spec.container_id, sink.as_ref())
                .await
            {
                Ok(manifest) => manifest,
                Err(e) => {
                    // `RunscRootUnresolved` is a structural terminal condition:
                    // no runsc root can be found for this container, so every
                    // retry would re-fail (#379). Set the capability gate so
                    // the label reconciler withdraws `envd-staged` on its next
                    // tick, stopping the warm pool from scheduling further
                    // pause jobs on this node until the DaemonSet is restarted
                    // with a corrected mount. A fresh node that has never hosted
                    // a container is unaffected — the error only fires once a
                    // real container is present, so boot-time behavior is
                    // unchanged (the #366 review concern).
                    if matches!(e, RuntimeError::RunscRootUnresolved { .. }) {
                        ctx.runsc_root_failed.store(true, Ordering::Relaxed);
                        tracing::error!(
                            container_id = %job.spec.container_id,
                            node = %ctx.node_name,
                            error = %e,
                            "RunscRootUnresolved for real container; set capability \
                             gate — envd-staged label will be withdrawn on next \
                             reconciler tick (#379)"
                        );
                    }
                    // Checkpoint/upload failed — the source is still alive
                    // (nothing was reclaimed) and no validation is owed.
                    return ctx
                        .status_store
                        .patch_status(
                            namespace,
                            name,
                            SnapshotJobStatus {
                                phase: SnapshotJobPhase::Failed,
                                manifest: None,
                                message: Some(e.to_string()),
                                started_at: Some(started_at),
                                completed_at: Some(Utc::now().to_rfc3339()),
                                local_path: None,
                            },
                        )
                        .await;
                }
            };

            // Fail-closed gate (issue #235): re-fetch the just-uploaded
            // bundle and prove it restorable before the controller is
            // allowed to advance `Pausing → Paused` and reclaim the
            // source. A validation failure here lands the job `Failed`
            // with the source intact (recoverable) rather than committing
            // a snapshot that only reveals itself unrestorable at resume
            // time, after the source is gone.
            ctx.status_store
                .patch_status(
                    namespace,
                    name,
                    SnapshotJobStatus {
                        phase: SnapshotJobPhase::Validating,
                        manifest: None,
                        message: None,
                        started_at: Some(started_at.clone()),
                        completed_at: None,
                        local_path: None,
                    },
                )
                .await?;

            let (terminal, message, manifest) = match ctx
                .adapter
                .validate_restorable(&manifest, source.as_ref())
                .await
            {
                Ok(()) => (SnapshotJobPhase::Complete, None, Some(manifest)),
                // Drop the manifest on a failed validation: a `Failed`
                // job must not carry a snapshot_ref the controller could
                // fold into `Sandbox.status` and later resume from. Log the
                // reason: it is otherwise only visible in the SnapshotJob's
                // `status.message`, which not every diagnostic dump shows.
                Err(e) => {
                    tracing::warn!(
                        name = %name,
                        namespace = %namespace,
                        error = %e,
                        "snapshot restorability validation failed; pause fails closed (source recoverable)"
                    );
                    (SnapshotJobPhase::Failed, Some(e.to_string()), None)
                }
            };
            ctx.status_store
                .patch_status(
                    namespace,
                    name,
                    SnapshotJobStatus {
                        phase: terminal,
                        manifest,
                        message,
                        started_at: Some(started_at),
                        completed_at: Some(Utc::now().to_rfc3339()),
                        local_path: None,
                    },
                )
                .await
        }
        SnapshotJobMode::Resume => {
            // `Resume` requires an upstream snapshot URL. The controller
            // populates this from `Sandbox.status.snapshotRef`; an empty
            // value here means the controller dispatched a job for a
            // Sandbox that was never paused. Surface it as Failed and
            // also propagate the error so kube-rs can log it.
            if job.spec.snapshot_ref.is_none() {
                let err = NodeReconcileError::MissingSnapshotRef(name.to_owned());
                ctx.status_store
                    .patch_status(
                        namespace,
                        name,
                        SnapshotJobStatus {
                            phase: SnapshotJobPhase::Failed,
                            manifest: None,
                            message: Some(err.to_string()),
                            started_at: Some(started_at),
                            completed_at: Some(Utc::now().to_rfc3339()),
                            local_path: None,
                        },
                    )
                    .await?;
                return Err(err);
            }

            ctx.status_store
                .patch_status(
                    namespace,
                    name,
                    SnapshotJobStatus {
                        phase: SnapshotJobPhase::Restoring,
                        manifest: None,
                        message: None,
                        started_at: Some(started_at.clone()),
                        completed_at: None,
                        local_path: None,
                    },
                )
                .await?;

            // T8's `GvisorAdapter::resume` re-fetches the authoritative
            // manifest from `source` and cross-checks its aggregate
            // sha256 against the `manifest.sha256` we pass here.
            // Issue #171 threaded the controller's committed
            // fingerprint through `SnapshotJobSpec::expected_sha256`
            // (folded in at Pause completion from the worker's
            // manifest), so we fill it in here. When the controller
            // didn't commit a value (legacy callers, or a Pause
            // completion that reported an empty aggregate), we leave
            // it empty and the adapter falls back to per-part sha256
            // verification only.
            let placeholder = SnapshotManifest {
                sandbox_id: job.spec.sandbox_name.clone(),
                runtime_class: ctx.adapter.runtime_class().to_owned(),
                sha256: job.spec.expected_sha256.clone().unwrap_or_default(),
                created_at: String::new(),
                parts: Vec::new(),
                snapshot_ref: job.spec.snapshot_ref.clone().unwrap_or_default(),
                // Issue #278: the controller commits the envd digest it
                // observed at pause through `expected_envd_sha256`, so the
                // adapter can cross-check the re-fetched manifest's
                // `envd_sha256` against it before `verify_envd_digest`
                // compares it to the local binary (mirrors the aggregate
                // `sha256` / issue #171 commit cross-check). Empty when the
                // controller didn't commit one (legacy callers, v1
                // manifests) — the adapter then skips the envd cross-check
                // and the local-binary determinism check still runs.
                envd_sha256: job.spec.expected_envd_sha256.clone().unwrap_or_default(),
                // The restore/stage adapter re-fetches the authoritative
                // manifest and cross-checks aggregate + per-part sha256; the
                // runsc version is not part of that cross-check (the #545
                // placement gate already kept this Pod off a mismatched node),
                // so the placeholder leaves it empty.
                runsc_version: String::new(),
                // The adapter re-fetches the authoritative manifest and runs
                // `verify_envd_digest` against *that* manifest's `envd_captured`
                // (epic #466), not this placeholder's — so the restore-time
                // host-binary-skip decision reads the real stored value. The
                // placeholder leaves it `false`.
                envd_captured: false,
                // Issue #416: `resume` / `stage_snapshot` re-fetch the
                // authoritative manifest and read *its* template fields for the
                // clone gate; the placeholder leaves them empty.
                template_ref: String::new(),
                template_image: String::new(),
            };
            let outcome = ctx
                .adapter
                .resume(&placeholder, &job.spec.container_id, source.as_ref())
                .await;
            let (terminal, message) = match outcome {
                Ok(()) => (SnapshotJobPhase::Complete, None),
                Err(e) => (SnapshotJobPhase::Failed, Some(e.to_string())),
            };
            ctx.status_store
                .patch_status(
                    namespace,
                    name,
                    SnapshotJobStatus {
                        phase: terminal,
                        manifest: None,
                        message,
                        started_at: Some(started_at),
                        completed_at: Some(Utc::now().to_rfc3339()),
                        local_path: None,
                    },
                )
                .await
        }
        SnapshotJobMode::Stage => {
            // `Stage` materializes the snapshot bundle onto local disk
            // for the containerd-shim restore path (Plan 07): no
            // container is restored here. Like `Resume`, it needs an
            // upstream snapshot URL — its absence is a controller bug, so
            // surface it as `Failed` and propagate (mirrors `Resume`).
            if job.spec.snapshot_ref.is_none() {
                let err = NodeReconcileError::MissingSnapshotRef(name.to_owned());
                ctx.status_store
                    .patch_status(
                        namespace,
                        name,
                        SnapshotJobStatus {
                            phase: SnapshotJobPhase::Failed,
                            manifest: None,
                            message: Some(err.to_string()),
                            started_at: Some(started_at),
                            completed_at: Some(Utc::now().to_rfc3339()),
                            local_path: None,
                        },
                    )
                    .await?;
                return Err(err);
            }

            // Pre-flight: `Restoring` is the closest in-flight phase
            // (staging is the download+materialize half of a restore),
            // and stamping `started_at` arms the same stuck-job
            // idempotency floor the Pause/Resume paths rely on.
            ctx.status_store
                .patch_status(
                    namespace,
                    name,
                    SnapshotJobStatus {
                        phase: SnapshotJobPhase::Restoring,
                        manifest: None,
                        message: None,
                        started_at: Some(started_at.clone()),
                        completed_at: None,
                        local_path: None,
                    },
                )
                .await?;

            // `stage_snapshot` re-fetches the authoritative manifest from
            // `source` and verifies every part; the placeholder threads
            // the controller-committed fingerprint (issue #171) for the
            // aggregate cross-check, exactly as the `Resume` arm does.
            let placeholder = SnapshotManifest {
                sandbox_id: job.spec.sandbox_name.clone(),
                runtime_class: ctx.adapter.runtime_class().to_owned(),
                sha256: job.spec.expected_sha256.clone().unwrap_or_default(),
                created_at: String::new(),
                parts: Vec::new(),
                snapshot_ref: job.spec.snapshot_ref.clone().unwrap_or_default(),
                // Issue #278: thread the controller-committed envd digest
                // for the envd-dimension cross-check, exactly as the
                // `Resume` arm does. Empty when uncommitted.
                envd_sha256: job.spec.expected_envd_sha256.clone().unwrap_or_default(),
                // The restore/stage adapter re-fetches the authoritative
                // manifest and cross-checks aggregate + per-part sha256; the
                // runsc version is not part of that cross-check (the #545
                // placement gate already kept this Pod off a mismatched node),
                // so the placeholder leaves it empty.
                runsc_version: String::new(),
                // `stage_snapshot` re-fetches the authoritative manifest and
                // gates on *its* `envd_captured` (epic #466); the placeholder
                // value is unused, so it stays `false`.
                envd_captured: false,
                // Issue #416: `resume` / `stage_snapshot` re-fetch the
                // authoritative manifest and read *its* template fields for the
                // clone gate; the placeholder leaves them empty.
                template_ref: String::new(),
                template_image: String::new(),
            };
            // Epic #484/#487: when the controller pinned a deterministic
            // `target_path`, the adapter stages there and drops the
            // `.staged` marker the restore Pod's stage-gate polls; when it's
            // `None` (legacy), the adapter picks a path and reports it back.
            let outcome = ctx
                .adapter
                .stage_snapshot(
                    &job.spec.sandbox_name,
                    &placeholder,
                    source.as_ref(),
                    job.spec.target_path.as_deref(),
                )
                .await;
            let (terminal, message, local_path) = match outcome {
                Ok(path) => (SnapshotJobPhase::Complete, None, Some(path)),
                Err(e) => (SnapshotJobPhase::Failed, Some(e.to_string()), None),
            };
            ctx.status_store
                .patch_status(
                    namespace,
                    name,
                    SnapshotJobStatus {
                        phase: terminal,
                        manifest: None,
                        message,
                        started_at: Some(started_at),
                        completed_at: Some(Utc::now().to_rfc3339()),
                        local_path,
                    },
                )
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An RFC 3339 timestamp `offset_secs` *before* `now`.
    fn before(now: DateTime<Utc>, offset_secs: i64) -> String {
        (now - chrono::Duration::seconds(offset_secs)).to_rfc3339()
    }

    #[test]
    fn terminal_phases_skip() {
        // A terminal job never re-dispatches, regardless of started_at.
        let now = Utc::now();
        assert!(!should_dispatch(
            SnapshotJobPhase::Complete,
            Some(&before(now, 1)),
            now,
            "j",
            "ns"
        ));
        assert!(!should_dispatch(
            SnapshotJobPhase::Failed,
            None,
            now,
            "j",
            "ns"
        ));
    }

    #[test]
    fn fresh_pending_dispatches() {
        // A brand-new job: phase defaults to Pending with no started_at → run.
        let now = Utc::now();
        assert!(should_dispatch(
            SnapshotJobPhase::Pending,
            None,
            now,
            "j",
            "ns"
        ));
    }

    #[test]
    fn in_flight_recent_skips() {
        // In-flight + started moments ago → within recovery age → skip.
        let now = Utc::now();
        assert!(!should_dispatch(
            SnapshotJobPhase::Checkpointing,
            Some(&before(now, 5)),
            now,
            "j",
            "ns"
        ));
    }

    #[test]
    fn in_flight_stale_redispatches() {
        // In-flight + started past the recovery age → presumed-crashed → run.
        let now = Utc::now();
        let stale = STUCK_JOB_RECOVERY_AGE.as_secs() as i64 + 60;
        assert!(should_dispatch(
            SnapshotJobPhase::Uploading,
            Some(&before(now, stale)),
            now,
            "j",
            "ns"
        ));
    }

    #[test]
    fn in_flight_future_started_at_is_treated_as_fresh() {
        // A negative age (marker in the future — this worker's clock trails the
        // stamping one) is younger than the recovery age, so it's treated as
        // "fresh, still in flight" and skipped rather than re-dispatching into a
        // double-run under clock skew.
        let now = Utc::now();
        let future = (now + chrono::Duration::seconds(30)).to_rfc3339();
        assert!(!should_dispatch(
            SnapshotJobPhase::Restoring,
            Some(&future),
            now,
            "j",
            "ns"
        ));
    }

    #[test]
    fn in_flight_malformed_started_at_redispatches() {
        // A malformed marker can't prove ownership, so we re-dispatch rather
        // than wedge the job forever.
        let now = Utc::now();
        assert!(should_dispatch(
            SnapshotJobPhase::Validating,
            Some("not-a-timestamp"),
            now,
            "j",
            "ns"
        ));
    }
}
