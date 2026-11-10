//! Pure state machine for the `Sandbox` lifecycle.
//!
//! This module is intentionally free of any cluster-client or async-
//! runtime dependency — only the leaf-of-the-graph
//! [`sandbawks_types::SandboxPhase`] is imported. That isolation is
//! what lets the state machine be exhaustively unit- and property-
//! tested cheaply; the reconcile loop layered on top of it does the
//! cluster I/O.
//!
//! Transition diagram in spec §5. Every legal `(phase, trigger)` pair
//! maps to exactly one `(next_phase, action)`. Illegal pairs return
//! `None`, which the reconcile loop interprets as "no change."

use sandbawks_types::{PersistenceMode, SandboxPhase};

/// Low-cardinality reason label for a Sandbox→`Failed` fold.
///
/// Modelling the vocabulary as an enum (rather than passing a bare `&str`) keeps
/// the label set closed and the mapping exhaustively checked by the compiler —
/// the same pattern as [`crate::warm_claim_metrics::ClaimOutcome`] in the api.
///
/// The enum is part of the state machine's public type vocabulary so it can
/// travel inside [`Trigger::Failure`] / [`Action::RecordFailure`] without the
/// metrics module leaking into the pure state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// The backing Pod vanished or went terminal while the Sandbox was `Creating`
    /// (node churn, Pod GC). Added in issue #338.
    PodVanishedCreating,
    /// The backing Pod terminated while the Sandbox was `Running`
    /// (envd exited or Pod deleted).
    PodTerminatedRunning,
    /// The backing Pod was evicted by the kubelet (ephemeral-storage exhaustion,
    /// node-pressure eviction, or OOMKill). The failure message carries the
    /// kubelet-written reason so the user can act on the resource budget
    /// directly (issue #418).
    PodEvicted,
    /// A `SnapshotJob` (pause / stage / resume) reached `phase=Failed`.
    SnapshotJobFailed,
    /// A reconcile action (`create_pod`, `pause`, `terminate`, etc.) returned
    /// an error; the Sandbox is folded to `Failed` as the recovery path.
    ActionFailure,
    /// The bounded `Resuming` recovery loop gave up: the restore Pod was lost
    /// and recreated `RESUME_MAX_RESTARTS` times without ever reaching
    /// `Running` (epic #484). Folding to `Failed` with this reason — rather
    /// than retrying forever — is exactly the `resumingforever` hang the epic
    /// closes. The recovery loop that emits this lands in a later epic phase;
    /// the reason is part of the foundation vocabulary so the state machine
    /// and metrics agree on the label ahead of that wiring.
    ResumeExhausted,
    /// A pause checkpoint failed (the pause `SnapshotJob` reached
    /// `phase=Failed`, or `Action::Pause` errored) while the Sandbox was
    /// `Pausing`, and the bounded tier-1 retry loop exhausted its budget
    /// without a clean checkpoint (issue #510).
    ///
    /// Unlike every other reason this one is **recoverable**, not terminal:
    /// the controller deliberately leaves the workload Pod running on a failed
    /// snapshot (so a live workload isn't destroyed), so a Sandbox folded with
    /// this reason is re-examined by the `Failed`-phase recovery probe —
    /// recovered to `Running` if its Pod + envd are still healthy, or its
    /// orphaned Pod GC'd after a grace window if not. Gating the new, scoped
    /// edges out of `Failed` on this reason is what keeps `Failed` terminal for
    /// genuine create/terminate failures while still reclaiming the
    /// pause-checkpoint orphans.
    PauseCheckpointFailed,
}

impl FailureReason {
    /// Stable, lowercase `reason` label used as the OTel attribute value.
    pub fn label(self) -> &'static str {
        match self {
            Self::PodVanishedCreating => "pod_vanished_creating",
            Self::PodTerminatedRunning => "pod_terminated_running",
            Self::PodEvicted => "pod_evicted",
            Self::SnapshotJobFailed => "snapshot_job_failed",
            Self::ActionFailure => "action_failure",
            Self::ResumeExhausted => "resume_exhausted",
            Self::PauseCheckpointFailed => "pause_checkpoint_failed",
        }
    }

    /// Parse a [`FailureReason`] back from its [`Self::label`] string, the
    /// inverse of [`Self::label`]. Returns `None` for an unrecognized label
    /// (e.g. a label written by a newer controller). Used by the reconcile
    /// loop to read the persisted `status.failureReason` back into the typed
    /// vocabulary when deciding whether a `Failed` Sandbox is recoverable
    /// (issue #510).
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "pod_vanished_creating" => Some(Self::PodVanishedCreating),
            "pod_terminated_running" => Some(Self::PodTerminatedRunning),
            "pod_evicted" => Some(Self::PodEvicted),
            "snapshot_job_failed" => Some(Self::SnapshotJobFailed),
            "action_failure" => Some(Self::ActionFailure),
            "resume_exhausted" => Some(Self::ResumeExhausted),
            "pause_checkpoint_failed" => Some(Self::PauseCheckpointFailed),
            _ => None,
        }
    }

    /// Whether a Sandbox folded to `Failed` with this reason is *recoverable* —
    /// i.e. the `Failed`-phase probe may retry/recover/GC it rather than
    /// treating the fold as a permanent terminal sink (issue #510).
    ///
    /// Only [`Self::PauseCheckpointFailed`] is recoverable: the controller
    /// fail-closes a failed pause by leaving the workload Pod running, so the
    /// live Pod is authoritative and the Sandbox can be brought back. Every
    /// other reason describes a genuinely dead/absent Pod (create/terminate
    /// failures, a lost restore Pod), which stays terminal.
    pub fn is_recoverable(self) -> bool {
        matches!(self, Self::PauseCheckpointFailed)
    }
}

/// Inputs to the state machine.
///
/// Triggers are derived by the reconcile loop from the observed
/// Sandbox spec/status/annotations + adapter call results. They are
/// the *atomic* events the state machine reacts to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// The Sandbox is in `Creating` and the controller has not yet
    /// materialized a Pod. Drives [`Action::CreatePod`].
    PodCreate,
    /// The backing pod reached `Ready=true`. (Plan 03 wires pod
    /// readiness via a watcher; Plan 02 simulates it in tests by
    /// setting an annotation the reconcile loop reads.)
    PodReady,
    /// User wants the sandbox paused (`spec.desiredPhase=Paused`
    /// annotation, or `POST /sandboxes/{id}/pause`).
    PauseRequested,
    /// User wants the sandbox resumed.
    ResumeRequested,
    /// A freshly-created sandbox carries [`sandbawks_types::SandboxSpec::restore_from`]
    /// (clone-from-snapshot, issue #416): instead of cold-booting, stand it up
    /// from another sandbox's saved state. Emitted by the reconcile loop only
    /// while the sandbox is `Creating` with no Pod yet — it replaces the
    /// [`Self::PodCreate`] this sandbox would otherwise get.
    ///
    /// Routes through the same `(… , resume_action(mode))` selection as a normal
    /// resume so the restore reuses the existing scheduler-placed restore flow
    /// (`snapshot` mode → [`Action::CreateRestorePod`]) or the PVC-clone cold
    /// boot (`filesystem` mode → [`Action::CreatePod`], with the clone's
    /// `dataSource` injected at PVC-provision time). The resolve+gate+stamp of
    /// the source artifact happens in the dispatch site of that action, so a
    /// failed compatibility gate folds the sandbox to `Failed` exactly like any
    /// other action error.
    RestoreRequested,
    /// User wants the sandbox killed (deletion timestamp set, or
    /// `POST /sandboxes/{id}/kill`).
    KillRequested,
    /// The sandbox's `spec.timeoutSeconds` elapsed.
    Timeout {
        /// Mirrors `spec.autoPause`. True -> pause; false -> terminate.
        auto_pause: bool,
    },
    /// The Pause `SnapshotJob` reached `Complete` — which, since issue
    /// #235, means the node has not merely uploaded the snapshot but
    /// *validated* it restorable (a throwaway `runsc restore` + envd
    /// health probe via `RuntimeAdapter::validate_restorable`). Gating
    /// `Pausing → Paused` on this — rather than on bare "uploaded" —
    /// is what keeps the source recoverable: the controller only reaches
    /// `Paused` (and thus lets the source Pod be reclaimed) once the
    /// snapshot is proven restorable.
    SnapshotValidated,
    /// The scheduler has bound the (unpinned) restore Pod to a node — the
    /// reconcile loop observed `pod.spec.nodeName` via
    /// [`sandbawks_types::RuntimeAdapter::pod_scheduled_node`] and recorded it
    /// in `Sandbox.status.assignedNode`. Drives [`Action::StageSnapshot`] to
    /// dispatch the node-local `Stage` job onto exactly that node (epic
    /// #484, scheduler-placed restore). This replaces the old
    /// stage-then-create ordering where staging ran *before* the Pod existed.
    NodeAssigned,
    /// A `Stage` `SnapshotJob` reached `Complete`: the snapshot's parts
    /// are materialized at the deterministic staging path on the node the
    /// restore Pod was scheduled onto, and the reconcile loop has copied
    /// the path into `Sandbox.status.restoreStagingPath`. Under
    /// scheduler-placed restore the restore Pod already exists (its
    /// `stage-gate` init container is waiting on the snapshot's `.staged`
    /// marker), so this drives no further action — [`Action::Noop`] — and
    /// the Pod proceeds to restore on its own (epic #484).
    StageComplete,
    /// The restore Pod was lost mid-`Resuming` — it went terminal/404, sat
    /// `Pending` past the schedule deadline (no-capacity), stayed
    /// scheduled-but-not-Ready past the readiness deadline, or its `Stage`
    /// job `Failed`. Drives [`Action::CreateRestorePod`] to recreate it
    /// **unpinned** so the scheduler re-places it — likely on a different
    /// node — under a bounded retry budget (epic #484). This is the edge
    /// that turns the old `resumingforever` hang into bounded recovery.
    RestorePodLost,
    /// A pause checkpoint failed while `Pausing` and the bounded tier-1 retry
    /// budget still has room (issue #510). Drives [`Action::Pause`] to
    /// re-dispatch the checkpoint, keeping the phase at `Pausing`. The
    /// reconcile loop emits this from its pause-recovery probe under a capped
    /// backoff; once the budget is spent it emits [`Self::Failure`] with
    /// [`FailureReason::PauseCheckpointFailed`] instead. This is the edge that
    /// stops a single transient `/__state` timeout from being terminal.
    PauseRetry,
    /// The backing Pod of a `filesystem`-mode suspend has been torn down — its
    /// `pod_ref` is cleared (the [`Action::SuspendPod`] delete completed) —
    /// so the suspend is finished: state now lives only on the retained,
    /// Sandbox-owned PVC(s) (epic #603 Phase 3, issue #606). Drives
    /// [`Action::FinalizeSuspend`] to settle the Sandbox at `Paused` with no
    /// snapshot. Emitted only by the reconcile loop's `filesystem`-mode
    /// `Pausing` completion probe; the `snapshot`-mode pause reaches `Paused`
    /// via [`Self::SnapshotValidated`] instead, so the two persistence modes
    /// never share a completion edge.
    PodReleased,
    /// A `Failed` Sandbox whose fold was the recoverable pause-checkpoint class
    /// has been observed with its workload Pod's container still `Ready` (the
    /// kubelet readiness condition) and no explicit pause still desired (issue
    /// #510, tier 2). Drives [`Action::RecoverToRunning`]: the pause simply did
    /// not happen, so the live Pod is treated as authoritative and the Sandbox
    /// re-enters the normal idle-timeout lifecycle. Emitted only by the
    /// `Failed`-phase recovery probe, which gates on
    /// [`FailureReason::is_recoverable`] so genuine terminal failures never
    /// reach this edge. (Container `Ready` is not an envd `/__state` liveness
    /// guarantee — see the probe's docstring.)
    FailedPodHealthy,
    /// A `Failed` Sandbox whose fold was the recoverable pause-checkpoint class
    /// has an *unrecoverable* Pod — envd unreachable past the grace window, or
    /// the Pod already terminal/gone (issue #510, tier 3). Drives
    /// [`Action::GcOrphanedPod`] to delete the orphaned Pod and reclaim its
    /// node slot, leaving the Sandbox `Failed` with a clear condition. Like
    /// [`Self::FailedPodHealthy`], emitted only by the gated `Failed`-phase
    /// recovery probe.
    FailedPodOrphaned,
    /// The backing Pod of a `Running` Sandbox was terminated by *resource
    /// pressure* — a kubelet eviction (node-pressure / ephemeral-storage) or an
    /// OOMKill (issue #407). Unlike envd's own clean exit or a deleted Pod
    /// (which stay [`Self::Failure`] and fold to terminal `Failed`), this is a
    /// *recoverable* event: silently discarding the sandbox's filesystem state
    /// is the undefined pre-#407 behavior this edge replaces. The controller
    /// instead re-uses the existing restore / provision machinery —
    ///
    ///   - `has_snapshot == true`  → [`Action::CreateRestorePod`] (`Resuming`):
    ///     a snapshot exists (`status.snapshotRef` set, e.g. the Sandbox was
    ///     paused-then-resumed), so re-enter the scheduler-placed restore flow
    ///     (epic #484) from the last snapshot, preserving continuity.
    ///   - `has_snapshot == false` → [`Action::CreatePod`] (`Creating`): no
    ///     snapshot was ever taken, so provision a fresh, clean-state Pod.
    ///
    /// `message` carries the kubelet's resource-detail text (the eviction / OOM
    /// budget, issue #418) so the reconcile loop can record it on the
    /// user-visible `StateRestored` / `StateLost` condition. The state machine
    /// itself ignores `message` — it is data the dispatch site reads, exactly
    /// as it ignores [`Self::Failure`]'s `message`.
    ///
    /// Emitted only by the `Running`-phase liveness probe.
    EvictedRecover {
        /// Whether a snapshot exists to restore from (`status.snapshotRef`),
        /// selecting the restore-vs-reprovision edge.
        has_snapshot: bool,
        /// Kubelet resource-detail text, surfaced on the recovery condition.
        message: String,
    },
    /// Any unrecoverable error during pod creation, pause, or resume.
    ///
    /// `reason` carries the bounded [`FailureReason`] label so the metrics module
    /// can emit a low-cardinality counter without inspecting the free-form `message`.
    Failure {
        /// Bounded reason label for `controller.sandbox.failure_total`.
        reason: FailureReason,
        /// Human-readable description recorded in `status.conditions[].message`.
        message: String,
    },
}

/// What the reconcile loop should do as a result of entering the new
/// phase. The `Action` is associated with the *destination* phase, not
/// the trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// No side effect — the destination phase is observable on its own
    /// (e.g. `Running`, `Paused`).
    Noop,
    /// Call `RuntimeAdapter::create_pod` for this sandbox; phase stays
    /// `Creating` until [`Trigger::PodReady`] fires.
    CreatePod,
    /// Call `RuntimeAdapter::pause` for this sandbox.
    Pause,
    /// `filesystem`-mode suspend (epic #603 Phase 3, issue #606): delete the
    /// backing Pod while **retaining** the Sandbox-owned PVC(s), then scrub the
    /// Pod-identity status fields. This is the runtime-agnostic pause — no
    /// `SnapshotJob`, no S3 snapshot, no node-adapter involvement — so it is the
    /// only pause mechanism available on Kata. Phase stays `Pausing` (the
    /// "suspending / Pod draining" state) until the delete completes and the
    /// `filesystem` completion probe folds [`Trigger::PodReleased`]. Idempotent:
    /// a Pod already gone is success, so a crash mid-suspend re-runs cleanly.
    SuspendPod,
    /// Settle a `filesystem`-mode suspend at `Paused` (the "suspended / Pod
    /// gone" state) once [`Action::SuspendPod`] has released the Pod (epic #603
    /// Phase 3, issue #606). Defensively clears any residual Pod identity and
    /// leaves every `snapshot_*` status field empty — a `filesystem`-paused
    /// Sandbox has no snapshot to resume from; resume cold-boots a fresh Pod
    /// re-mounting the retained PVC(s).
    FinalizeSuspend,
    /// Plan 07 T9 resume step 1: dispatch a `Stage` `SnapshotJob` pinned
    /// to the staging node. The per-node worker downloads + materializes
    /// the snapshot's parts onto local disk and reports the path back in
    /// `SnapshotJob.status.localPath`. Phase stays `Resuming` until the
    /// job reaches `Complete` and the observer folds it into
    /// [`Trigger::StageComplete`].
    StageSnapshot,
    /// Plan 07 T9 resume step 2: materialize the restore Pod via
    /// `RuntimeAdapter::create_pod`, threading
    /// `status.restoreStagingPath` so the `containerd-shim` performs a
    /// start-time `runsc restore` from the staged image. No restore-mode
    /// env — the container is born restored. Phase stays `Resuming`
    /// until [`Trigger::PodReady`] fires for the restored Pod.
    CreateRestorePod,
    /// Promote a freshly-restored Sandbox from `Resuming` to `Running`
    /// (`(Resuming, PodReady)`) and **re-arm the idle clock** by stamping
    /// `status.lastActivityTimestamp` to *now* (issue #563).
    ///
    /// Without the re-arm, the idle-timeout deadline is still measured from
    /// the pre-pause [`activity_baseline`](crate::reconcile) — for a
    /// long-lived claimed member (`desiredPhase=Running` + `autoPause`) that
    /// aged past its deadline before the pause, that baseline is already
    /// expired, so the sandbox is "idle" the instant it lands in `Running`.
    /// The idle edge then re-pauses it, `desiredPhase=Running` re-resumes it,
    /// and the two intents thrash every few seconds — churning restore Pods
    /// until a pause checkpoint races the container teardown and folds the
    /// Sandbox to `Failed`. Stamping a fresh activity time here gives every
    /// resumed Sandbox a full idle window before the next auto-pause, breaking
    /// the oscillation at its source. This mirrors the idle-clock re-arm
    /// [`Action::RecoverToRunning`] already performs on the tier-2 recovery
    /// edge; there are no failure breadcrumbs to clear on a clean resume.
    ResumeToRunning,
    /// Tear down the pod and (optionally) clean up the snapshot.
    Terminate,
    /// Recover a `Failed` (recoverable pause-checkpoint) Sandbox back to
    /// `Running` (issue #510, tier 2): clear the failure breadcrumbs, reset the
    /// pause-retry budget, and re-arm the idle clock so the freshly-recovered
    /// Sandbox gets a full idle window before the next auto-pause attempt
    /// (rather than immediately re-pausing against a stale activity timestamp
    /// and tight-looping). The live workload Pod is left untouched — its
    /// container never went un-`Ready`.
    RecoverToRunning,
    /// Delete the orphaned workload Pod of a `Failed` (recoverable
    /// pause-checkpoint) Sandbox to reclaim its node slot (issue #510, tier 3),
    /// clear the Pod-identity status fields, and record a clear terminal
    /// condition. The Sandbox stays `Failed`; with no `pod_ref` left, the
    /// recovery probe no longer re-fires, so the Sandbox is now genuinely
    /// terminal.
    GcOrphanedPod,
    /// Record an unrecoverable failure in `status.conditions`.
    ///
    /// Carries the same `reason` the triggering [`Trigger::Failure`] named so the
    /// dispatch site can emit `controller.sandbox.failure_total{reason}` without
    /// re-inspecting the free-form `message`.
    RecordFailure {
        /// Bounded reason label forwarded to the failure counter.
        reason: FailureReason,
        /// Human-readable description recorded in `status.conditions[].message`.
        message: String,
    },
}

impl Action {
    /// Stable, low-cardinality label for this action — the variant name with
    /// no embedded payload.
    ///
    /// Used as a tracing span field so the reconcile dispatch span shows
    /// *which* action ran without leaking the `RecordFailure` message (a
    /// high-cardinality, operator-supplied string) into trace attributes.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::CreatePod => "create_pod",
            Self::Pause => "pause",
            Self::SuspendPod => "suspend_pod",
            Self::FinalizeSuspend => "finalize_suspend",
            Self::StageSnapshot => "stage_snapshot",
            Self::CreateRestorePod => "create_restore_pod",
            Self::ResumeToRunning => "resume_to_running",
            Self::Terminate => "terminate",
            Self::RecoverToRunning => "recover_to_running",
            Self::GcOrphanedPod => "gc_orphaned_pod",
            Self::RecordFailure { .. } => "record_failure",
        }
    }
}

/// Pause action for a sandbox's resolved [`PersistenceMode`] (epic #603 / #606).
///
/// `snapshot` (the default) keeps the existing memory+filesystem checkpoint via
/// a `SnapshotJob` ([`Action::Pause`]); `filesystem` tears the Pod down and
/// retains the PVC(s) ([`Action::SuspendPod`]). Factored out because both the
/// user-requested pause (`PauseRequested`) and the idle-timeout auto-pause
/// (`Timeout { auto_pause: true }`) edges select the same way.
fn pause_action(mode: PersistenceMode) -> Action {
    match mode {
        PersistenceMode::Snapshot => Action::Pause,
        PersistenceMode::Filesystem => Action::SuspendPod,
    }
}

/// Resume action for a sandbox's resolved [`PersistenceMode`] (epic #603 / #606).
///
/// `snapshot` re-enters the scheduler-placed restore flow
/// ([`Action::CreateRestorePod`]); `filesystem` cold-boots a fresh Pod
/// re-mounting the retained PVC(s) ([`Action::CreatePod`]) — there is no
/// snapshot to restore, so the standard pod-creation edge applies.
fn resume_action(mode: PersistenceMode) -> Action {
    match mode {
        PersistenceMode::Snapshot => Action::CreateRestorePod,
        PersistenceMode::Filesystem => Action::CreatePod,
    }
}

/// Transition the state machine.
///
/// Returns `Some((next_phase, action))` if `(current, trigger)` is a
/// legal pair from spec §5; `None` otherwise. The caller (reconcile)
/// treats `None` as "this trigger does not apply to the current phase,
/// no change required."
///
/// `mode` is the sandbox's *resolved* [`PersistenceMode`] (epic #603 / #606).
/// It selects between the snapshot and filesystem pause/resume actions on the
/// `Running`/`Paused` edges and gates the filesystem suspend re-drive on
/// `Pausing`; every other `(phase, trigger)` pair is mode-agnostic and the
/// argument is ignored, so passing the [`PersistenceMode::default`]
/// (`Snapshot`) for those is always correct.
pub fn transition(
    current: SandboxPhase,
    trigger: Trigger,
    mode: PersistenceMode,
) -> Option<(SandboxPhase, Action)> {
    use PersistenceMode::Filesystem;
    use SandboxPhase::*;
    use Trigger::*;

    match (current, trigger) {
        // Creating ────────────────────────────────────────────────
        (Creating, PodCreate) => Some((Creating, Action::CreatePod)),
        // Clone-from-snapshot (issue #416): a new sandbox with `restore_from`
        // set enters the restore path straight from `Creating`, reusing the
        // same per-mode action selection as a `Paused` resume. `snapshot` mode
        // creates the (born-restored) restore Pod and stages the source's
        // snapshot onto it via the existing `Resuming` sub-steps; `filesystem`
        // mode cold-boots a fresh Pod over PVCs CSI-cloned from the source. The
        // source resolve + compatibility gate runs in the action's dispatch
        // site, so a mismatch folds to `Failed` via the normal action-error path.
        (Creating, RestoreRequested) => Some((Resuming, resume_action(mode))),
        (Creating, PodReady) => Some((Running, Action::Noop)),
        (Creating, Failure { reason, message }) => {
            Some((Failed, Action::RecordFailure { reason, message }))
        }

        // Running ─────────────────────────────────────────────────
        //
        // Pause (user-requested or idle auto-pause) branches on the resolved
        // persistence mode (epic #603 / #606): `snapshot` checkpoints via a
        // `SnapshotJob` (`Action::Pause`), `filesystem` deletes the Pod and
        // retains the PVC(s) (`Action::SuspendPod`). Both land in `Pausing`.
        (Running, PauseRequested) => Some((Pausing, pause_action(mode))),
        (Running, KillRequested) => Some((Terminating, Action::Terminate)),
        (Running, Timeout { auto_pause: true }) => Some((Pausing, pause_action(mode))),
        (Running, Timeout { auto_pause: false }) => Some((Terminating, Action::Terminate)),
        // Issue #407: a kubelet eviction / OOMKill of the backing Pod is a
        // recoverable resource-pressure event, not a terminal failure. Restore
        // from the last snapshot when one exists, else provision fresh — both
        // re-using the existing `Resuming` / `Creating` machinery (and its
        // bounded-recovery loops) rather than a bespoke path. The user-visible
        // `StateRestored` / `StateLost` condition is stamped by the reconcile
        // loop from the carried `message`; the state machine only routes phase.
        (
            Running,
            EvictedRecover {
                has_snapshot: true, ..
            },
        ) => Some((Resuming, Action::CreateRestorePod)),
        (
            Running,
            EvictedRecover {
                has_snapshot: false,
                ..
            },
        ) => Some((Creating, Action::CreatePod)),
        (Running, Failure { reason, message }) => {
            Some((Failed, Action::RecordFailure { reason, message }))
        }

        // Pausing ─────────────────────────────────────────────────
        // Gated on `SnapshotValidated` (issue #235): the source is only
        // reclaimable once the snapshot is proven restorable, not merely
        // uploaded.
        (Pausing, SnapshotValidated) => Some((Paused, Action::Noop)),
        // Issue #510 tier 1: a failed pause checkpoint with retry budget
        // remaining re-dispatches the checkpoint instead of folding to
        // `Failed`, so a single transient `/__state` timeout is not terminal.
        (Pausing, PauseRetry) => Some((Pausing, Action::Pause)),
        // #606 filesystem suspend re-drive: a crash between the `Pausing` phase
        // patch and the `Action::SuspendPod` delete completing can leave the Pod
        // alive with the pause still requested. The completion probe re-emits
        // `PauseRequested` while the Pod survives, re-running the (idempotent)
        // delete so the suspend always converges. Gated on `Filesystem` so a
        // snapshot-mode pause never deletes its Pod out from under the
        // SnapshotJob; snapshot mode has no `(Pausing, PauseRequested)` edge.
        (Pausing, PauseRequested) if mode == Filesystem => Some((Pausing, Action::SuspendPod)),
        // #606 filesystem suspend complete: the Pod is gone and the PVC(s) are
        // retained, so settle at `Paused` with no snapshot (`FinalizeSuspend`).
        (Pausing, PodReleased) => Some((Paused, Action::FinalizeSuspend)),
        (Pausing, Failure { reason, message }) => {
            Some((Failed, Action::RecordFailure { reason, message }))
        }

        // Paused ──────────────────────────────────────────────────
        //
        // Epic #484 (scheduler-placed restore) inverts the resume
        // ordering from stage-then-create to **create-then-stage**.
        // `ResumeRequested` now creates the restore Pod *first*, unpinned,
        // so the scheduler does memory-aware placement; the snapshot is
        // staged later, onto whichever node the scheduler picked.
        //
        // #606: resume branches on the resolved persistence mode. `filesystem`
        // has no snapshot to restore, so it cold-boots a fresh Pod
        // (`Action::CreatePod`) that re-mounts the retained PVC(s); the
        // `Resuming → Running` promotion (`PodReady`) is then shared with the
        // snapshot path below.
        (Paused, ResumeRequested) => Some((Resuming, resume_action(mode))),
        (Paused, KillRequested) => Some((Terminating, Action::Terminate)),

        // Resuming ────────────────────────────────────────────────
        //
        // The `Resuming` phase spans several sub-steps that share one
        // externally-observable phase (epic #484):
        //   1. `NodeAssigned` — the scheduler bound the restore Pod to a
        //      node; dispatch the node-local `Stage` job there
        //      (`StageSnapshot`). Internal sub-state "scheduled".
        //   2. `StageComplete` — the snapshot is materialized at the
        //      deterministic path; the already-running Pod's `stage-gate`
        //      init container sees the marker and the container restores on
        //      its own, so there's nothing for the controller to do
        //      (`Noop`). Internal sub-state "restoring".
        //   3. `RestorePodLost` — the Pod was lost mid-resume; recreate it
        //      unpinned (`CreateRestorePod`) so the scheduler re-places it.
        //      Bounded by the recovery loop (later epic phase).
        //   4. `PodReady` — the born-restored container is Ready; flip
        //      straight to `Running` (with no further SnapshotJob) and
        //      re-arm the idle clock so a member that aged past its
        //      idle-timeout deadline before the pause isn't re-paused the
        //      instant it lands in `Running` (`Action::ResumeToRunning`,
        //      issue #563).
        (Resuming, NodeAssigned) => Some((Resuming, Action::StageSnapshot)),
        (Resuming, StageComplete) => Some((Resuming, Action::Noop)),
        (Resuming, RestorePodLost) => Some((Resuming, Action::CreateRestorePod)),
        (Resuming, PodReady) => Some((Running, Action::ResumeToRunning)),
        (Resuming, Failure { reason, message }) => {
            Some((Failed, Action::RecordFailure { reason, message }))
        }

        // Failed ──────────────────────────────────────────────────
        //
        // `Failed` is a terminal sink for every genuine failure (create,
        // terminate, lost-restore, …) — `(Failed, _) => None` below. Issue
        // #510 carves out exactly two *scoped* edges for the recoverable
        // pause-checkpoint failure class, both emitted only by the reconcile
        // loop's `Failed`-phase recovery probe, which gates on
        // [`FailureReason::is_recoverable`]:
        //   - `FailedPodHealthy` — the workload Pod is still alive and envd is
        //     container Ready, so the live Pod is authoritative; recover to
        //     `Running` and re-enter the normal idle-timeout lifecycle (tier 2).
        //   - `FailedPodOrphaned` — the Pod is unrecoverable (envd unreachable
        //     past the grace window, or already terminal); GC the orphaned Pod
        //     to reclaim its node slot, staying `Failed` (tier 3).
        // The probe never emits these for a non-recoverable fold, so terminal
        // semantics are preserved for everything else by the catch-all.
        (Failed, FailedPodHealthy) => Some((Running, Action::RecoverToRunning)),
        (Failed, FailedPodOrphaned) => Some((Failed, Action::GcOrphanedPod)),

        // Terminal sinks ──────────────────────────────────────────
        (Failed, _) => None,
        (Terminating, _) => None,

        // Anything else — no transition.
        _ => None,
    }
}
