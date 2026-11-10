//! Unit tests for the node-side `SnapshotJob` reconciler (Plan 05 T9 / issue #145).
//!
//! These tests drive [`sandbawks_node::reconcile::reconcile`] directly
//! against in-memory test doubles: a recording [`FakeRuntimeAdapter`]
//! that captures `pause`/`resume` calls, a [`FakeSnapshotJobStatusStore`]
//! that records the sequence of status patches the reconciler emits,
//! and the existing [`FakeSnapshotStore`] from `sandbawks-types`.
//!
//! Each test asserts the contract the issue calls out:
//!
//! 1. **Dispatch on mode** — a `Pause` job invokes `adapter.pause`;
//!    a `Resume` job invokes `adapter.resume`. Cross-firing is the
//!    one bug that would silently corrupt sandboxes, so each direction
//!    has its own test.
//! 2. **Idempotency on retry** — a job already at `phase=Complete`
//!    short-circuits without re-calling the adapter. A re-emitted watch
//!    event must not re-run `runsc checkpoint` on the same container.
//! 3. **Failure path** — an adapter error lands the job at
//!    `phase=Failed` with a non-empty `message`. The controller folds
//!    this back into `Sandbox.status.conditions`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sandbawks_node::reconcile::{reconcile, NodeReconcileError};
use sandbawks_types::runtime::RuntimeError;
use sandbawks_types::{SnapshotJobMode, SnapshotJobPhase, SnapshotJobStatus};

mod common;
use common::*;

#[tokio::test]
async fn pause_dispatches_and_writes_complete_with_manifest() {
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job("sbx-abc-pause-1", SnapshotJobMode::Pause, None, None);

    reconcile(&job, &ctx).await.expect("reconcile pause");

    // Adapter saw exactly one pause call, no resume, and the issue #235
    // validation gate fired once before Complete.
    assert_eq!(adapter.pause_calls(), vec![CONTAINER_ID.to_owned()]);
    assert!(adapter.resume_calls().is_empty());
    assert_eq!(
        adapter.validate_calls().len(),
        1,
        "validate_restorable must run exactly once on the pause happy path"
    );

    // Three patches: in-flight `Checkpointing`, the `Validating` gate
    // (issue #235), then terminal `Complete` carrying the manifest.
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 3, "expected 3 status patches");
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Checkpointing);
    assert_eq!(recorded[0].name, "sbx-abc-pause-1");
    assert_eq!(recorded[0].namespace, NAMESPACE);
    assert!(recorded[0].status.started_at.is_some());
    assert!(recorded[0].status.completed_at.is_none());

    // The `Validating` patch is in-flight: no manifest, no completed_at.
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Validating);
    assert!(recorded[1].status.manifest.is_none());
    assert!(recorded[1].status.completed_at.is_none());

    assert_eq!(recorded[2].status.phase, SnapshotJobPhase::Complete);
    assert!(recorded[2].status.message.is_none());
    let manifest = recorded[2]
        .status
        .manifest
        .as_ref()
        .expect("Complete patch must carry manifest");
    assert_eq!(manifest.sandbox_id, CONTAINER_ID);
    assert!(
        !manifest.parts.is_empty(),
        "manifest carries at least one part"
    );
    assert!(recorded[2].status.completed_at.is_some());
}

/// Issue #235 fail-closed teardown: when the restorability validation
/// fails, the Pause job lands `Failed` (with the validation error and
/// **no** manifest), the controller therefore never observes a
/// `Complete` to fold into `Sandbox.status.snapshotRef`, and so the
/// state machine never advances `Pausing → Paused` — the source Pod is
/// never reclaimed and the workload stays recoverable.
#[tokio::test]
async fn pause_validation_failure_lands_at_failed_without_manifest() {
    let adapter = Arc::new(FakeRuntimeAdapter::new().with_validate_outcome(Err(
        RuntimeError::RestoreFailed("throwaway restore: runsc exit 1".into()),
    )));
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job("sbx-abc-pause-235", SnapshotJobMode::Pause, None, None);

    reconcile(&job, &ctx)
        .await
        .expect("reconcile returns Ok even when validation fails");

    // pause() ran (checkpoint+upload happened), then validation ran and
    // failed.
    assert_eq!(adapter.pause_calls(), vec![CONTAINER_ID.to_owned()]);
    assert_eq!(adapter.validate_calls().len(), 1);

    // Three patches: Checkpointing → Validating → Failed.
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 3, "expected 3 status patches");
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Checkpointing);
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Validating);
    assert_eq!(recorded[2].status.phase, SnapshotJobPhase::Failed);

    // Fail-closed invariant: the terminal patch carries NO manifest, so
    // the controller has nothing to fold into Sandbox.status and can
    // never reach Paused off this snapshot.
    assert!(
        recorded[2].status.manifest.is_none(),
        "a validation failure must not commit a manifest the controller could resume from"
    );
    let msg = recorded[2]
        .status
        .message
        .as_deref()
        .expect("Failed phase must carry a message");
    assert!(
        msg.contains("throwaway restore"),
        "message should propagate the validation error: got {msg}"
    );
    assert!(recorded[2].status.completed_at.is_some());
}

#[tokio::test]
async fn resume_dispatches_to_adapter_resume() {
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-abc-resume-1",
        SnapshotJobMode::Resume,
        Some("s3://fake/manifest.json".into()),
        None,
    );

    reconcile(&job, &ctx).await.expect("reconcile resume");

    // Resume side fired; pause side untouched.
    assert!(
        adapter.pause_calls().is_empty(),
        "pause must not fire for Resume"
    );
    assert_eq!(adapter.resume_calls(), vec![CONTAINER_ID.to_owned()]);

    // In-flight `Restoring`, then terminal `Complete` (no manifest on the
    // Resume terminal — the snapshot was consumed, not produced).
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Restoring);
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Complete);
    assert!(
        recorded[1].status.manifest.is_none(),
        "Resume completion does not produce a manifest"
    );
}

#[tokio::test]
async fn terminal_phase_is_idempotent() {
    // A job that already reached `Complete` must be a no-op — neither the
    // adapter nor the status store should see another call. This is what
    // protects the cluster from a re-fired watch event running
    // `runsc checkpoint` twice on the same container.
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-abc-pause-1",
        SnapshotJobMode::Pause,
        None,
        Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            manifest: Some(default_manifest(SANDBOX_NAME)),
            message: None,
            started_at: Some("2026-05-25T12:00:00+00:00".into()),
            completed_at: Some("2026-05-25T12:01:00+00:00".into()),
            local_path: None,
        }),
    );

    reconcile(&job, &ctx).await.expect("terminal job is no-op");

    assert!(
        adapter.pause_calls().is_empty(),
        "terminal job must not re-dispatch"
    );
    assert!(adapter.resume_calls().is_empty());
    assert!(
        status_store.recorded().is_empty(),
        "terminal job must not re-patch status"
    );
}

#[tokio::test]
async fn pause_failure_lands_at_phase_failed_with_message() {
    let adapter = Arc::new(
        FakeRuntimeAdapter::new()
            .with_pause_outcome(Err(RuntimeError::CheckpointFailed("runsc exit 1".into()))),
    );
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job("sbx-abc-pause-1", SnapshotJobMode::Pause, None, None);

    reconcile(&job, &ctx)
        .await
        .expect("reconcile returns Ok even on adapter failure");

    assert_eq!(adapter.pause_calls(), vec![CONTAINER_ID.to_owned()]);
    // A checkpoint/upload failure short-circuits before the validation
    // gate — there is no uploaded snapshot to validate (issue #235).
    assert!(
        adapter.validate_calls().is_empty(),
        "validation must not run when checkpoint/upload itself failed"
    );

    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Checkpointing);
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Failed);
    let msg = recorded[1]
        .status
        .message
        .as_deref()
        .expect("Failed phase must carry a message");
    assert!(!msg.is_empty(), "message must be non-empty");
    assert!(
        msg.contains("runsc exit 1"),
        "message should propagate the adapter error: got {msg}"
    );
    assert!(
        recorded[1].status.manifest.is_none(),
        "no manifest on failure"
    );
}

#[tokio::test]
async fn resume_failure_lands_at_phase_failed_with_message() {
    let adapter = Arc::new(FakeRuntimeAdapter::new().with_resume_outcome(Err(
        RuntimeError::RestoreFailed("runsc restore exit 1".into()),
    )));
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-abc-resume-1",
        SnapshotJobMode::Resume,
        Some("s3://fake/manifest.json".into()),
        None,
    );

    reconcile(&job, &ctx)
        .await
        .expect("reconcile returns Ok even on adapter failure");

    assert_eq!(adapter.resume_calls(), vec![CONTAINER_ID.to_owned()]);
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Restoring);
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Failed);
    let msg = recorded[1]
        .status
        .message
        .as_deref()
        .expect("Failed phase must carry a message");
    assert!(
        msg.contains("runsc restore exit 1"),
        "message should propagate the adapter error: got {msg}"
    );
}

#[tokio::test]
async fn resume_without_snapshot_ref_fails_loudly() {
    // The controller is the source of truth for `spec.snapshotRef`; a
    // Resume with no ref is a controller bug, not a transient error.
    // The reconciler still patches status=Failed so the controller can
    // observe + surface the problem in `Sandbox.status.conditions`.
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job("sbx-abc-resume-bad", SnapshotJobMode::Resume, None, None);

    let err = reconcile(&job, &ctx)
        .await
        .expect_err("missing snapshot ref must propagate");
    assert!(
        matches!(err, NodeReconcileError::MissingSnapshotRef(_)),
        "expected MissingSnapshotRef, got {err:?}"
    );

    // Adapter never called; status patched to Failed exactly once.
    assert!(adapter.resume_calls().is_empty());
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Failed);
    assert!(recorded[0]
        .status
        .message
        .as_deref()
        .is_some_and(|m| !m.is_empty()));
}

// ---------------------------------------------------------------------------
// Issue #194 — in-flight phase idempotency. The kube-rs Controller can
// re-fire reconcile with a stale cached object whose status still shows
// `Checkpointing` after we've already patched the terminal phase. Pre-fix,
// that re-fire fell through the idempotency check and re-ran pause(), which
// then timed out on /__state because the first pause()'s `runsc checkpoint`
// had killed envd. These tests pin the new behaviour: in-flight + recent
// started_at → skip; in-flight + stale started_at → re-dispatch as crash
// recovery.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn in_flight_phase_with_recent_started_at_is_idempotent() {
    // The exact #194 reproducer: SnapshotJob is `Checkpointing` (we
    // already patched it in a prior reconcile pass) and `started_at` is
    // recent — a stale watch event must NOT re-dispatch pause().
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    // RFC 3339 "now"-ish — well inside the 5 minute stuck-recovery age.
    let recent = chrono::Utc::now().to_rfc3339();
    let job = make_job(
        "sbx-abc-pause-194",
        SnapshotJobMode::Pause,
        None,
        Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Checkpointing,
            manifest: None,
            message: None,
            started_at: Some(recent),
            completed_at: None,
            local_path: None,
        }),
    );

    reconcile(&job, &ctx)
        .await
        .expect("in-flight recent SnapshotJob is no-op");

    assert!(
        adapter.pause_calls().is_empty(),
        "in-flight recent SnapshotJob must not re-dispatch pause()"
    );
    assert!(
        status_store.recorded().is_empty(),
        "in-flight recent SnapshotJob must not re-patch status"
    );
}

#[tokio::test]
async fn in_flight_phase_with_stale_started_at_recovers() {
    // Worker crashed (or kubelet killed it) mid-pause; SnapshotJob is
    // stuck in `Checkpointing` with a `started_at` older than the
    // recovery threshold. The reconciler must re-dispatch so the work
    // doesn't wedge forever.
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    // 6 minutes ago — past the 5 minute stuck-recovery age.
    let stale = (chrono::Utc::now() - chrono::Duration::minutes(6)).to_rfc3339();
    let job = make_job(
        "sbx-abc-pause-stuck",
        SnapshotJobMode::Pause,
        None,
        Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Checkpointing,
            manifest: None,
            message: None,
            started_at: Some(stale),
            completed_at: None,
            local_path: None,
        }),
    );

    reconcile(&job, &ctx)
        .await
        .expect("stuck SnapshotJob re-dispatch must succeed against the fake");

    assert_eq!(
        adapter.pause_calls(),
        vec![CONTAINER_ID.to_owned()],
        "stuck SnapshotJob must re-dispatch pause()"
    );
    // Recovery emits the normal pause patch sequence
    // (Checkpointing → Validating → Complete) just like a fresh reconcile
    // (issue #235 adds the Validating gate between checkpoint and Complete).
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 3);
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Checkpointing);
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Validating);
    assert_eq!(recorded[2].status.phase, SnapshotJobPhase::Complete);
}

#[tokio::test]
async fn in_flight_resume_phase_is_idempotent_too() {
    // Resume side has the same race as pause: `Restoring` is the
    // in-flight phase the reconciler patches before calling
    // adapter.resume(). Same #194 cache-staleness re-fire would re-run
    // restore on a container that's already running.
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    let recent = chrono::Utc::now().to_rfc3339();
    let job = make_job(
        "sbx-abc-resume-194",
        SnapshotJobMode::Resume,
        Some("s3://fake/manifest.json".into()),
        Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Restoring,
            manifest: None,
            message: None,
            started_at: Some(recent),
            completed_at: None,
            local_path: None,
        }),
    );

    reconcile(&job, &ctx)
        .await
        .expect("in-flight resume is no-op");

    assert!(
        adapter.resume_calls().is_empty(),
        "in-flight recent SnapshotJob must not re-dispatch resume()"
    );
    assert!(status_store.recorded().is_empty());
}

#[tokio::test]
async fn in_flight_phase_without_started_at_falls_through() {
    // Defensive: a SnapshotJob with phase=Checkpointing but no
    // started_at shouldn't happen in our protocol (we patch them
    // together), but a manually-applied fixture or a partial write
    // could land us here. Treat as fresh and dispatch — better than
    // silently wedging.
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, _status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-abc-pause-no-started",
        SnapshotJobMode::Pause,
        None,
        Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Checkpointing,
            manifest: None,
            message: None,
            started_at: None,
            completed_at: None,
            local_path: None,
        }),
    );

    reconcile(&job, &ctx)
        .await
        .expect("malformed in-flight SnapshotJob falls through to dispatch");

    assert_eq!(
        adapter.pause_calls(),
        vec![CONTAINER_ID.to_owned()],
        "SnapshotJob with no started_at must be treated as fresh"
    );
}

// ---------------------------------------------------------------------------
// Stage dispatch (Plan 07 T8 / issue #212).
//
// A `Stage` job materializes the snapshot bundle onto local disk via
// `adapter.stage_snapshot` and records the returned path in
// `status.localPath` on `Complete`. Like `Resume` it requires a
// `snapshot_ref`, and an adapter error lands the job at `Failed`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stage_dispatches_to_adapter_and_writes_local_path() {
    let staged_path = "/var/lib/sandbawks/staging/sbx-abc/job-xyz";
    let adapter =
        Arc::new(FakeRuntimeAdapter::new().with_stage_outcome(Ok(staged_path.to_owned())));
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-abc-stage-1",
        SnapshotJobMode::Stage,
        Some("s3://fake/manifest.json".into()),
        None,
    );

    reconcile(&job, &ctx).await.expect("reconcile stage");

    // Stage side fired with the sandbox name; pause/resume untouched.
    assert!(
        adapter.pause_calls().is_empty(),
        "pause must not fire for Stage"
    );
    assert!(
        adapter.resume_calls().is_empty(),
        "resume must not fire for Stage"
    );
    assert_eq!(adapter.stage_calls(), vec![SANDBOX_NAME.to_owned()]);
    // Legacy caller (no pinned path): the adapter is told to pick its own.
    assert_eq!(adapter.stage_target_paths(), vec![None]);

    // In-flight `Restoring`, then terminal `Complete` carrying the
    // staged local path (and no manifest — Stage produces a path, not a
    // snapshot).
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 2, "expected 2 status patches");
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Restoring);
    assert!(recorded[0].status.started_at.is_some());
    assert!(recorded[0].status.local_path.is_none());

    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Complete);
    assert!(recorded[1].status.message.is_none());
    assert!(
        recorded[1].status.manifest.is_none(),
        "Stage completion does not produce a manifest"
    );
    assert_eq!(
        recorded[1].status.local_path.as_deref(),
        Some(staged_path),
        "Complete patch must carry the staged local path"
    );
    assert!(recorded[1].status.completed_at.is_some());
}

#[tokio::test]
async fn stage_without_snapshot_ref_fails_loudly() {
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    // No snapshot_ref: a Stage job for a never-paused sandbox is a
    // controller bug, surfaced as Failed + propagated.
    let job = make_job("sbx-abc-stage-bad", SnapshotJobMode::Stage, None, None);

    let err = reconcile(&job, &ctx)
        .await
        .expect_err("stage without snapshotRef must propagate an error");
    assert!(matches!(err, NodeReconcileError::MissingSnapshotRef(_)));

    // Adapter never ran.
    assert!(
        adapter.stage_calls().is_empty(),
        "stage must not run without a ref"
    );

    // Exactly one patch: terminal Failed with a message.
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 1, "expected a single Failed patch");
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Failed);
    assert!(recorded[0].status.local_path.is_none());
    let msg = recorded[0]
        .status
        .message
        .as_deref()
        .expect("Failed phase must carry a message");
    assert!(
        msg.contains("snapshotRef"),
        "message should name the missing ref: got {msg}"
    );
}

#[tokio::test]
async fn stage_failure_lands_at_phase_failed_with_message() {
    let adapter = Arc::new(
        FakeRuntimeAdapter::new()
            .with_stage_outcome(Err(RuntimeError::SnapshotDownload("part fetch 503".into()))),
    );
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-abc-stage-2",
        SnapshotJobMode::Stage,
        Some("s3://fake/manifest.json".into()),
        None,
    );

    reconcile(&job, &ctx)
        .await
        .expect("reconcile returns Ok even on adapter failure");

    assert_eq!(adapter.stage_calls(), vec![SANDBOX_NAME.to_owned()]);

    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Restoring);
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Failed);
    assert!(
        recorded[1].status.local_path.is_none(),
        "Failed Stage must not carry a local path"
    );
    let msg = recorded[1]
        .status
        .message
        .as_deref()
        .expect("Failed phase must carry a message");
    assert!(
        msg.contains("part fetch 503"),
        "message should propagate the adapter error: got {msg}"
    );
}

/// Epic #484 / issue #487: a `Stage` job carrying `spec.target_path`
/// threads that deterministic path through to `adapter.stage_snapshot`,
/// so the node materializes the bundle where the controller already
/// stamped the restore Pod's `restore-image-path` annotation. A legacy
/// `Stage` (no `target_path`) passes `None`, preserving the old behavior.
#[tokio::test]
async fn stage_threads_target_path_to_adapter() {
    let target = "/var/lib/sandbawks/staging/sbx-abc/restore";
    let adapter = Arc::new(FakeRuntimeAdapter::new().with_stage_outcome(Ok(target.to_owned())));
    let (ctx, status_store) = make_ctx(adapter.clone());
    let mut job = make_job(
        "sbx-abc-stage-pinned",
        SnapshotJobMode::Stage,
        Some("s3://fake/manifest.json".into()),
        None,
    );
    job.spec.target_path = Some(target.to_owned());

    reconcile(&job, &ctx).await.expect("reconcile pinned stage");

    // The deterministic path reached the adapter verbatim.
    assert_eq!(
        adapter.stage_target_paths(),
        vec![Some(target.to_owned())],
        "Stage must forward spec.target_path to the adapter"
    );

    // And the returned (pinned) path is recorded on Complete.
    let recorded = status_store.recorded();
    assert_eq!(
        recorded
            .last()
            .expect("a terminal patch")
            .status
            .local_path
            .as_deref(),
        Some(target),
        "Complete patch must carry the pinned staged path"
    );
}

/// `RunscRootUnresolved` fires for a real container: the capability gate must
/// be set and the SnapshotJob must land `Failed` with the typed error in its
/// message (#379).
///
/// This is the core contract of the issue: one failure, one `Failed` phase,
/// and the gate set so the label reconciler withdraws `envd-staged` on its
/// next tick — stopping the warm pool from scheduling further doomed pause
/// jobs on this node.
#[tokio::test]
async fn runsc_root_unresolved_sets_capability_gate_and_fails_job() {
    let adapter = Arc::new(FakeRuntimeAdapter::new().with_pause_outcome(Err(
        RuntimeError::RunscRootUnresolved {
            container_id: "deadbeef".into(),
            candidates: vec![PathBuf::from("/run/containerd/runsc/k8s.io")],
            search_dirs: vec![],
        },
    )));
    let runsc_root_failed = Arc::new(AtomicBool::new(false));
    let (ctx, status_store) = make_ctx_with_flag(adapter.clone(), Arc::clone(&runsc_root_failed));
    let job = make_job("sbx-abc-pause-379", SnapshotJobMode::Pause, None, None);

    reconcile(&job, &ctx)
        .await
        .expect("reconcile returns Ok even when RunscRootUnresolved fires");

    // Gate must be set — the label reconciler checks this on its next tick.
    assert!(
        runsc_root_failed.load(Ordering::Relaxed),
        "capability gate must be set after RunscRootUnresolved"
    );

    // Job must be Failed with the typed error message, not retried.
    let recorded = status_store.recorded();
    assert_eq!(recorded.len(), 2, "expect Checkpointing + Failed patches");
    assert_eq!(recorded[0].status.phase, SnapshotJobPhase::Checkpointing);
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Failed);
    let msg = recorded[1]
        .status
        .message
        .as_deref()
        .expect("Failed phase must carry a message");
    assert!(
        msg.contains("no runsc --root resolved"),
        "message must name the RunscRootUnresolved condition: got {msg}"
    );
    // No manifest: the SnapshotJob carries no snapshot_ref the controller
    // could use to advance the Sandbox to Paused.
    assert!(recorded[1].status.manifest.is_none());
}

/// A non-RunscRootUnresolved pause failure must NOT set the capability gate.
///
/// The gate is a structural signal: it fires only when no runsc root can
/// ever be found for a real container, not on transient runtime failures.
/// A `CheckpointFailed` error (e.g. `runsc` exit 1) is retriable; the gate
/// staying false ensures the node keeps advertising and the warm pool can
/// reschedule the pause (#379 — fresh-node no-regression companion).
#[tokio::test]
async fn non_runsc_root_failure_does_not_set_capability_gate() {
    let adapter = Arc::new(
        FakeRuntimeAdapter::new()
            .with_pause_outcome(Err(RuntimeError::CheckpointFailed("runsc exit 1".into()))),
    );
    let runsc_root_failed = Arc::new(AtomicBool::new(false));
    let (ctx, status_store) = make_ctx_with_flag(adapter.clone(), Arc::clone(&runsc_root_failed));
    let job = make_job("sbx-abc-pause-gate", SnapshotJobMode::Pause, None, None);

    reconcile(&job, &ctx)
        .await
        .expect("reconcile returns Ok on CheckpointFailed");

    // Gate must stay false for a retriable failure.
    assert!(
        !runsc_root_failed.load(Ordering::Relaxed),
        "capability gate must NOT be set for a non-RunscRootUnresolved error"
    );

    let recorded = status_store.recorded();
    assert_eq!(recorded[1].status.phase, SnapshotJobPhase::Failed);
}
