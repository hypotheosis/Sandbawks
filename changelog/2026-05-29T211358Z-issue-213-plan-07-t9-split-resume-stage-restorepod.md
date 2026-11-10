# Plan 07 T9 — controller: split `Resume` into `StageSnapshot` + `CreateRestorePod`

- **Issue:** #213 (epic #204, Plan 07 Task 9)
- **Date:** 2026-05-29T21:13:58Z
- **Area:** `crates/sandbawks-controller/` (state machine, reconcile loop,
  `PodAdapter`) + `crates/sandbawks-types/` (`SandboxStatus`,
  `RuntimeAdapter::create_pod`)

## Summary

Replaces the issue #171 fresh-pod resume dance
(`RecreatePodForResume` → readiness probe → `Resume` SnapshotJob →
`ResumeReady`) with the Plan 07 shim-restore workflow: the container is
*born restored* by the `containerd-shim` performing a start-time
`runsc restore`, so resume no longer dispatches a live-restore
`SnapshotJob` against an already-running idle envd.

The `Resuming` phase now has two sub-steps:

1. `Action::StageSnapshot` — dispatch a `mode: Stage` `SnapshotJob`
   pinned to the staging node. The per-node worker
   (`GvisorAdapter::stage_snapshot`, Plan 07 T8) downloads + materializes
   the snapshot's parts onto local disk and reports the path back in
   `SnapshotJob.status.localPath`.
2. `Action::CreateRestorePod` — once the `Stage` job completes, the
   reconcile loop copies `localPath` into the new
   `Sandbox.status.restoreStagingPath`, clears the job ref, and
   `create_pod` materializes the restore Pod threading that path. The
   restored Pod reaching `Ready` promotes straight to `Running`.

## State machine (`state_machine.rs`)

- **Removed** `Action::RecreatePodForResume`, `Action::Resume`, and
  `Trigger::ResumeReady`.
- **Added** `Action::StageSnapshot`, `Action::CreateRestorePod`, and
  `Trigger::StageComplete`.
- Transitions:
  - `(Paused, ResumeRequested) → (Resuming, StageSnapshot)`
  - `(Resuming, StageComplete) → (Resuming, CreateRestorePod)`
  - `(Resuming, PodReady) → (Running, Noop)` *(was a self-loop to
    `Resuming` + `Resume`)*

## Reconcile loop (`reconcile.rs`)

- `observe_snapshot_job`: a completing `Stage` job now folds into
  `Trigger::StageComplete` and surfaces its `local_path`. The new
  `SnapshotJobObservation::Complete.local_path` is committed durably to
  `status.restoreStagingPath` *before* `CreateRestorePod` runs, so a
  crash between the patch and Pod creation recovers from the recorded
  path rather than re-staging. `SnapshotJobMode::Resume` is grouped with
  `Stage` for match exhaustiveness (the controller no longer dispatches
  Resume jobs).
- `dispatch`:
  - `StageSnapshot` guards on `status.snapshotRef` (the URL to stage
    from), creates the `Stage` job (pinned to `assigned_node`), stamps
    `snapshot_job_ref`.
  - `CreateRestorePod` guards on `status.restoreStagingPath`, scrubs the
    stale pre-pause Pod identity (`pod_ref`/`pod_uid`/`envd_address`),
    keeps `assigned_node` (it names the staging node the restore Pod is
    pinned to), and calls `create_pod(..., Some(&RestorePlacement))`.
  - `CreatePod` now passes `None` (cold start).

## Pod adapter (`pod_adapter.rs`)

- `RuntimeAdapter::create_pod`'s `restore_mode: bool` parameter is
  replaced by `restore: Option<&RestorePlacement>` (new type in
  `sandbawks-types`). For a restore Pod, `pod_spec_for`:
  - defaults `spec.runtimeClassName` to `sandbawks-runsc` unless the
    Sandbox pins its own,
  - pins `spec.nodeName` to the staging node,
  - adds the `io.sandbawks/restore-image-path` annotation,
  - injects **no** `SANDBAWKS_RESTORE_MODE` env and mounts **no**
    hostPath/volume — the container is born restored.
- The legacy `SANDBAWKS_RESTORE_MODE` injection/filtering and its
  `RESTORE_MODE_ENV` const are removed.

## Types (`sandbawks-types`)

- `SandboxStatus.restore_staging_path: Option<String>` (camelCase
  `restoreStagingPath`), `skip_serializing_if = Option::is_none`.
- New `RestorePlacement { staging_path, node_name }`, re-exported from
  the crate root.
- CRD `deploy/crds/sandboxes.sandbawks.io.yaml` regenerated via
  `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`; only the
  Sandbox CRD changed (the new `restoreStagingPath` status field).

## Tests

- `state_machine_unit.rs`: new edges for `StageSnapshot`,
  `CreateRestorePod`, and `(Resuming, PodReady) → Running`.
- `state_machine_properties.rs`: `any_trigger` swaps `ResumeReady` →
  `StageComplete`; the `no_self_loops` exception list now allows
  `(Resuming, StageComplete) → Resuming`.
- `reconcile.rs`:
  `paused_with_desired_running_stages_then_creates_restore_pod` walks
  Stage dispatch → worker Complete (with `local_path`) → CreateRestorePod
  → PodReady → Running, asserting the staging-path fold-in, the scrubbed
  Pod identity, and that no second (Resume) job is created.
- `lifecycle.rs`: the resume leg (steps 3a/3b/3c) rewritten for the
  Stage → CreateRestorePod → PodReady flow.
- `pod_adapter.rs`: the two `SANDBAWKS_RESTORE_MODE` tests replaced by
  restore-placement tests (runtime-class default + override, node pin,
  annotation, no restore-mode env; cold-start has none of these).

## Validation

- `just fmt` ✅
- `just clippy` (`cargo clippy --workspace --all-targets -- -D warnings`) ✅
- `cargo clippy -p sandbawks-types -p sandbawks-controller --all-targets --all-features -- -D warnings` ✅
- `just test` ✅ (253 tests passed, exit 0)
- CRD regenerated and verified to diff only by the new status field.

## Notes

- `CreateRestorePod` does not delete a prior Pod: in the Plan 07
  architecture the pause path checkpoints and tears the Pod down, so by
  `Paused` there is no live Pod — the restore Pod is created fresh on the
  staging node.
- The `runsc restore` itself (shim side) and the staged-image read are
  out of scope for T9; this task is the controller orchestration plus the
  `PodAdapter` Pod shape.
