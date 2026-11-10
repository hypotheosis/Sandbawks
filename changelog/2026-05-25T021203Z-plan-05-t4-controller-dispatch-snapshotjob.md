# Plan 05 T4 — controller `dispatch()` refactor via `SnapshotJob` CRD

**Issue:** #140
**Branch:** `feat-issue-140-dispatch-snapshotjob`

## What changed

The controller's `dispatch()` no longer calls `RuntimeAdapter::pause`
or `RuntimeAdapter::resume` directly. It now creates a `SnapshotJob`
CR per pause/resume cycle and polls the job's status on subsequent
reconciles. The per-node `sandbawks-node` DaemonSet worker (Plan 05
T9) will reconcile those jobs and execute the underlying `runsc
checkpoint` / `runsc restore` work.

### Concretely

- **`crates/sandbawks-controller/src/store.rs`** — added `SnapshotJobStore`
  trait + `KubeSnapshotJobStore` and `FakeSnapshotJobStore` impls. Mirrors
  the existing `SandboxStore` shape; the fake exposes `peek`, `list`,
  and `set_status` for the test driver.
- **`crates/sandbawks-controller/src/reconcile.rs`** —
  - `ReconcileContext` gains a `snapshot_jobs: Arc<dyn SnapshotJobStore>`
    field.
  - New `observe_snapshot_job` helper runs before `derive_trigger`.
    When `Sandbox.status.snapshot_job_ref` is set, it looks up the
    job and returns one of:
    - `InFlight` (`Pending` / `Checkpointing` / `Uploading` / `Restoring`)
      → reconcile returns early; kube-rs requeues.
    - `Complete` → yields `Trigger::SnapshotUploaded` (Pause) or
      `Trigger::ResumeReady` (Resume); the pause variant carries the
      manifest's `snapshot_ref` so the reconcile loop can fold it
      into `Sandbox.status.snapshot_ref`.
    - `Failed` → yields `Trigger::Failure(message)` so the state
      machine moves the Sandbox to `Failed`.
    In every terminal case the reconcile loop clears
    `status.snapshot_job_ref`.
  - `Action::Pause` / `Action::Resume` arms now construct a `SnapshotJob`
    CR named `<sandbox>-<mode>-<epoch-millis>` and stamp
    `Sandbox.status.snapshot_job_ref`. They do *not* advance the
    Sandbox's phase — that's the SnapshotJob observer's job.
  - The `Action::Noop` arm gates its `pod_ready` probe on
    `current_phase(sandbox) == Creating`. The pause/resume completions
    now reach `Action::Noop` via SnapshotJob observation, and we don't
    want to mis-fire a Pod probe in those cases.
  - The legacy `INLINE_MANIFEST_SCHEME` constant and the inline
    manifest reconstruction are gone — superseded by the real S3
    URLs the worker will write.
- **`crates/sandbawks-controller/src/fake_adapter.rs`** — `pause` and
  `resume` overrides removed; `FakeRuntimeAdapter` now only stands in
  for Pod-lifecycle calls (`create_pod`, `delete_pod`, `pod_ready`).
  `FakeAdapterCall` enum + `calls()` method dropped (no test exercises
  them post-T4).
- **`crates/sandbawks-controller/src/bin/sandbawks-controller.rs`** —
  wires `KubeSnapshotJobStore` into `ReconcileContext` and adds a
  `.watches::<SnapshotJob, _>(...)` clause so updates to job `status`
  requeue the parent Sandbox.
- **`crates/sandbawks-controller/tests/reconcile.rs`** — four tests
  rewritten plus a new `snapshot_job_complete_advances_pausing_to_paused_and_folds_snapshot_ref`
  test that exercises the new fold-in step.
- **`crates/sandbawks-controller/tests/lifecycle.rs`** — full
  Creating → Running → Pausing → Paused → Resuming → Running →
  Terminating walk now driven by mutating `SnapshotJob.status` between
  reconciles (simulating the per-node worker).
- **`crates/sandbawks-controller/tests/fake_adapter.rs`** — pause/resume
  behavior tests removed (`FakeAdapterCall::Pause/::Resume` no longer
  exist). The two remaining tests assert `runtime_class` and
  `Arc<dyn RuntimeAdapter>` object safety.

## Validation

- `just fmt-check` — green.
- `cargo clippy --workspace --all-targets -- -D warnings` — green.
- `cargo test --workspace --all-targets` — every test suite green;
  `tests/state_machine_unit.rs` and `tests/state_machine_properties.rs`
  are **unchanged** (pure state machine unaffected).
- `cargo run -p sandbawks-controller -- --help` — binary loads, CLI
  unchanged from the user's perspective.

## Out of scope (handled by later T-tasks per the plan)

- T5 — `PodAdapter` honoring `Sandbox.spec.runtimeClassName`.
- T9 — `sandbawks-node` consuming the SnapshotJob CRs the controller
  now writes.
- T12 — RBAC granting the controller `snapshotjobs` CRUD permissions.
