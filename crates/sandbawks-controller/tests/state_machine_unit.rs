//! Per-edge unit tests for the pure state machine.
//!
//! Each test covers one legal `(phase, trigger, mode) -> (next_phase, action)`
//! edge from spec §5, plus the two absorbing sinks and the idempotency
//! case `(Pausing, PauseRequested) -> None`. Property-based coverage
//! (totality, determinism, sink confinement) lands in T5 under
//! `tests/state_machine_properties.rs`.
//!
//! The `mode` argument (epic #603 / #606) selects the snapshot vs filesystem
//! pause/resume actions. `SNAP` (the pre-#606 default) drives the snapshot
//! edges that must stay byte-identical; `FS` drives the filesystem suspend.

use sandbawks_controller::{transition, Action, FailureReason, Trigger};
use sandbawks_types::{PersistenceMode, SandboxPhase};

/// Snapshot mode — the default; every pre-#606 edge resolves with this.
const SNAP: PersistenceMode = PersistenceMode::Snapshot;
/// Filesystem mode — the runtime-agnostic delete-Pod / cold-boot suspend.
const FS: PersistenceMode = PersistenceMode::Filesystem;

#[test]
fn creating_pod_create_triggers_create_pod_action() {
    let r = transition(SandboxPhase::Creating, Trigger::PodCreate, SNAP);
    assert_eq!(r, Some((SandboxPhase::Creating, Action::CreatePod)));
}

#[test]
fn creating_restore_requested_snapshot_creates_restore_pod() {
    // Clone-from-snapshot (issue #416): a snapshot-mode sandbox with
    // `restore_from` enters the scheduler-placed restore flow from `Creating`,
    // reusing the same `CreateRestorePod` action as a `Paused` resume.
    let (phase, action) =
        transition(SandboxPhase::Creating, Trigger::RestoreRequested, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::CreateRestorePod);
}

#[test]
fn creating_restore_requested_filesystem_creates_pod() {
    // Filesystem-mode clone cold-boots a fresh Pod over CSI-cloned PVCs, so the
    // restore-on-create edge resolves to `CreatePod` (the PVC `dataSource` clone
    // is injected at provision time, not here).
    let (phase, action) =
        transition(SandboxPhase::Creating, Trigger::RestoreRequested, FS).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::CreatePod);
}

#[test]
fn creating_pod_ready_to_running() {
    let (phase, action) = transition(SandboxPhase::Creating, Trigger::PodReady, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Running);
    assert_eq!(action, Action::Noop);
}

#[test]
fn running_pause_requested_to_pausing_pause_action() {
    let (phase, action) = transition(SandboxPhase::Running, Trigger::PauseRequested, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::Pause);
}

#[test]
fn running_timeout_autopause_true_pauses() {
    let (phase, action) = transition(
        SandboxPhase::Running,
        Trigger::Timeout { auto_pause: true },
        SNAP,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::Pause);
}

#[test]
fn running_timeout_autopause_false_terminates() {
    let (phase, action) = transition(
        SandboxPhase::Running,
        Trigger::Timeout { auto_pause: false },
        SNAP,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Terminating);
    assert_eq!(action, Action::Terminate);
}

#[test]
fn running_kill_requested_terminates() {
    let (phase, action) = transition(SandboxPhase::Running, Trigger::KillRequested, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Terminating);
    assert_eq!(action, Action::Terminate);
}

#[test]
fn running_evicted_with_snapshot_restores() {
    // Issue #407: a kubelet eviction / OOMKill of a Running sandbox that has a
    // snapshot recovers by restoring from it — re-entering the `Resuming`
    // scheduler-placed restore flow rather than folding to terminal `Failed`.
    let (phase, action) = transition(
        SandboxPhase::Running,
        Trigger::EvictedRecover {
            has_snapshot: true,
            message: "evicted: ephemeral-storage limit 256Mi".into(),
        },
        SNAP,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::CreateRestorePod);
}

#[test]
fn running_evicted_without_snapshot_reprovisions_fresh() {
    // Issue #407: the same eviction with no snapshot provisions a fresh,
    // clean-state Pod (`Creating` / `CreatePod`); in-flight state is lost but
    // the sandbox stays usable rather than dead-ending at `Failed`.
    let (phase, action) = transition(
        SandboxPhase::Running,
        Trigger::EvictedRecover {
            has_snapshot: false,
            message: "OOMKilled (512Mi)".into(),
        },
        SNAP,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Creating);
    assert_eq!(action, Action::CreatePod);
}

#[test]
fn pausing_snapshot_validated_to_paused() {
    let (phase, action) =
        transition(SandboxPhase::Pausing, Trigger::SnapshotValidated, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Paused);
    assert_eq!(action, Action::Noop);
}

#[test]
fn pausing_failure_to_failed() {
    let (phase, action) = transition(
        SandboxPhase::Pausing,
        Trigger::Failure {
            reason: FailureReason::SnapshotJobFailed,
            message: "oom".into(),
        },
        SNAP,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Failed);
    assert_eq!(
        action,
        Action::RecordFailure {
            reason: FailureReason::SnapshotJobFailed,
            message: "oom".into(),
        }
    );
}

#[test]
fn pausing_pause_retry_redispatches_pause() {
    // Issue #510 tier 1: a failed pause checkpoint with retry budget remaining
    // re-dispatches the checkpoint (`Action::Pause`) while the phase stays
    // `Pausing`, so a single transient `/__state` timeout is not terminal.
    let (phase, action) = transition(SandboxPhase::Pausing, Trigger::PauseRetry, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::Pause);
}

#[test]
fn failed_pod_healthy_recovers_to_running() {
    // Issue #510 tier 2: a recoverable-pause-checkpoint `Failed` Sandbox whose
    // workload Pod is alive recovers to `Running`.
    let (phase, action) =
        transition(SandboxPhase::Failed, Trigger::FailedPodHealthy, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Running);
    assert_eq!(action, Action::RecoverToRunning);
}

#[test]
fn failed_pod_orphaned_gcs_and_stays_failed() {
    // Issue #510 tier 3: an unrecoverable orphan's Pod is GC'd while the
    // Sandbox stays `Failed`.
    let (phase, action) =
        transition(SandboxPhase::Failed, Trigger::FailedPodOrphaned, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Failed);
    assert_eq!(action, Action::GcOrphanedPod);
}

#[test]
fn paused_resume_requested_to_resuming_create_restore_pod() {
    // Epic #484 (scheduler-placed restore): `(Paused, ResumeRequested)`
    // in snapshot mode creates the restore Pod *first*, unpinned, so the
    // scheduler places it — staging happens later, onto the scheduled node.
    let (phase, action) = transition(SandboxPhase::Paused, Trigger::ResumeRequested, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::CreateRestorePod);
}

#[test]
fn resuming_node_assigned_to_stage_snapshot() {
    // Epic #484: once the scheduler binds the restore Pod to a node, the
    // controller learns the node and dispatches the node-local `Stage`
    // job there — `StageSnapshot`, phase stays `Resuming`.
    let (phase, action) = transition(SandboxPhase::Resuming, Trigger::NodeAssigned, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::StageSnapshot);
}

#[test]
fn resuming_stage_complete_to_noop() {
    // Epic #484: with the restore Pod already created and its stage-gate
    // init container waiting on the snapshot marker, a completing `Stage`
    // job needs no controller action — the container restores on its own.
    let (phase, action) = transition(SandboxPhase::Resuming, Trigger::StageComplete, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::Noop);
}

#[test]
fn resuming_restore_pod_lost_recreates_restore_pod() {
    // Epic #484: a lost restore Pod is recreated unpinned so the scheduler
    // re-places it (bounded by the recovery loop in a later epic phase).
    let (phase, action) =
        transition(SandboxPhase::Resuming, Trigger::RestorePodLost, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::CreateRestorePod);
}

#[test]
fn paused_kill_requested_terminates() {
    let (phase, action) = transition(SandboxPhase::Paused, Trigger::KillRequested, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Terminating);
    assert_eq!(action, Action::Terminate);
}

#[test]
fn resuming_pod_ready_to_running() {
    // Plan 07 T9: the restored Pod reaching Ready promotes straight to
    // `Running` with no further SnapshotJob — the container is born
    // restored by the shim's start-time `runsc restore`. Issue #563: the
    // edge now re-arms the idle clock (`ResumeToRunning`) rather than being a
    // bare `Noop`, so a member that aged past its idle-timeout deadline before
    // the pause isn't re-paused the instant it lands in `Running`.
    let (phase, action) = transition(SandboxPhase::Resuming, Trigger::PodReady, SNAP).unwrap();
    assert_eq!(phase, SandboxPhase::Running);
    assert_eq!(action, Action::ResumeToRunning);
}

#[test]
fn resuming_failure_to_failed() {
    let (phase, action) = transition(
        SandboxPhase::Resuming,
        Trigger::Failure {
            reason: FailureReason::SnapshotJobFailed,
            message: "io".into(),
        },
        SNAP,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Failed);
    assert_eq!(
        action,
        Action::RecordFailure {
            reason: FailureReason::SnapshotJobFailed,
            message: "io".into(),
        }
    );
}

#[test]
fn creating_failure_to_failed() {
    let (phase, action) = transition(
        SandboxPhase::Creating,
        Trigger::Failure {
            reason: FailureReason::PodVanishedCreating,
            message: "imgpull".into(),
        },
        SNAP,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Failed);
    assert_eq!(
        action,
        Action::RecordFailure {
            reason: FailureReason::PodVanishedCreating,
            message: "imgpull".into(),
        }
    );
}

#[test]
fn failed_is_absorbing() {
    assert!(transition(SandboxPhase::Failed, Trigger::PodReady, SNAP).is_none());
    assert!(transition(SandboxPhase::Failed, Trigger::PauseRequested, SNAP).is_none());
    assert!(transition(SandboxPhase::Failed, Trigger::ResumeRequested, SNAP).is_none());
    assert!(transition(SandboxPhase::Failed, Trigger::KillRequested, SNAP).is_none());
    assert!(transition(
        SandboxPhase::Failed,
        Trigger::Failure {
            reason: FailureReason::ActionFailure,
            message: "x".into(),
        },
        SNAP,
    )
    .is_none());
    // Issue #510: PauseRetry only applies to `Pausing`, never to `Failed`.
    assert!(transition(SandboxPhase::Failed, Trigger::PauseRetry, SNAP).is_none());
}

#[test]
fn failure_reason_recoverable_is_scoped_to_pause_checkpoint() {
    // Issue #510: only the pause-checkpoint class is recoverable; every other
    // fold stays a terminal sink. This is the gate the reconcile loop's
    // `Failed`-phase probe keys on.
    assert!(FailureReason::PauseCheckpointFailed.is_recoverable());
    for reason in [
        FailureReason::PodVanishedCreating,
        FailureReason::PodTerminatedRunning,
        FailureReason::PodEvicted,
        FailureReason::SnapshotJobFailed,
        FailureReason::ActionFailure,
        FailureReason::ResumeExhausted,
    ] {
        assert!(!reason.is_recoverable(), "{reason:?} must stay terminal");
    }
}

#[test]
fn failure_reason_label_round_trips_through_from_label() {
    // Issue #510: `from_label` is the inverse of `label`, so a reason
    // persisted to `status.failureReason` reads back into the typed vocabulary.
    for reason in [
        FailureReason::PodVanishedCreating,
        FailureReason::PodTerminatedRunning,
        FailureReason::PodEvicted,
        FailureReason::SnapshotJobFailed,
        FailureReason::ActionFailure,
        FailureReason::ResumeExhausted,
        FailureReason::PauseCheckpointFailed,
    ] {
        assert_eq!(FailureReason::from_label(reason.label()), Some(reason));
    }
    assert_eq!(FailureReason::from_label("nonsense_label"), None);
}

#[test]
fn terminating_is_absorbing() {
    assert!(transition(SandboxPhase::Terminating, Trigger::PauseRequested, SNAP).is_none());
    assert!(transition(SandboxPhase::Terminating, Trigger::ResumeRequested, SNAP).is_none());
    assert!(transition(SandboxPhase::Terminating, Trigger::PodReady, SNAP).is_none());
}

#[test]
fn pause_requested_in_pausing_is_idempotent_noop_in_snapshot_mode() {
    // Requesting pause while already pausing returns None in snapshot mode —
    // the caller (reconcile) interprets None as "no-op, the in-flight
    // SnapshotJob will complete on its own." (Filesystem mode re-purposes this
    // edge to re-drive the Pod delete — see `filesystem_*` tests below.)
    assert!(transition(SandboxPhase::Pausing, Trigger::PauseRequested, SNAP).is_none());
}

// ── #606 filesystem suspend mode ─────────────────────────────────────────────

#[test]
fn filesystem_pause_requested_deletes_pod() {
    // `filesystem` mode pauses by deleting the Pod (retaining PVCs) — no
    // SnapshotJob — rather than checkpointing. Both land in `Pausing`.
    let (phase, action) = transition(SandboxPhase::Running, Trigger::PauseRequested, FS).unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::SuspendPod);
}

#[test]
fn filesystem_idle_autopause_deletes_pod() {
    // The idle auto-pause edge selects the same filesystem suspend action.
    let (phase, action) = transition(
        SandboxPhase::Running,
        Trigger::Timeout { auto_pause: true },
        FS,
    )
    .unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::SuspendPod);
}

#[test]
fn filesystem_pod_released_completes_suspend_to_paused() {
    // The filesystem completion probe folds `PodReleased` once the Pod is gone;
    // the Sandbox settles at `Paused` via `FinalizeSuspend`. Only the
    // filesystem probe emits `PodReleased`, so the edge holds for any mode.
    for mode in [SNAP, FS] {
        let (phase, action) =
            transition(SandboxPhase::Pausing, Trigger::PodReleased, mode).unwrap();
        assert_eq!(phase, SandboxPhase::Paused);
        assert_eq!(action, Action::FinalizeSuspend);
    }
}

#[test]
fn filesystem_suspend_redrive_reruns_delete() {
    // A crash mid-suspend leaves the Pod alive with the pause still requested;
    // the probe re-emits `PauseRequested`, which in `filesystem` mode re-runs
    // the idempotent delete (`SuspendPod`). Snapshot mode has no such edge
    // (asserted in `pause_requested_in_pausing_is_idempotent_noop_in_snapshot_mode`).
    let (phase, action) = transition(SandboxPhase::Pausing, Trigger::PauseRequested, FS).unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::SuspendPod);
}

#[test]
fn filesystem_resume_cold_boots_fresh_pod() {
    // `filesystem` resume has no snapshot to restore, so it cold-boots a fresh
    // Pod (`Action::CreatePod`) that re-mounts the retained PVC(s) — distinct
    // from snapshot's `CreateRestorePod`.
    let (phase, action) = transition(SandboxPhase::Paused, Trigger::ResumeRequested, FS).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::CreatePod);
}

#[test]
fn filesystem_resume_to_running_is_shared_with_snapshot() {
    // The `Resuming → Running` promotion (`PodReady` → `ResumeToRunning`) is
    // mode-agnostic: a born-ready Pod re-arms the idle clock identically whether
    // it was cold-booted (filesystem) or restored (snapshot).
    let (phase, action) = transition(SandboxPhase::Resuming, Trigger::PodReady, FS).unwrap();
    assert_eq!(phase, SandboxPhase::Running);
    assert_eq!(action, Action::ResumeToRunning);
}
