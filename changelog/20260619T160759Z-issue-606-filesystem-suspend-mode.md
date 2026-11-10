# Filesystem suspend mode — delete-Pod pause / cold-boot resume (epic #603 Phase 3)

**Date:** 2026-06-19T16:07:59Z
**Issue:** #606
**Branch:** `sandbawks/sandbawks-606`

## Why

Epic #603 introduced a runtime-agnostic `filesystem` persistence mode so Kata —
whose VMM-level snapshot the containerd shim cannot drive (see #594) — can be
paused at all. Phase 1 (#609) added the `PersistenceMode` enum and
`volumeClaimTemplates` to the CRD; Phase 2 (#610) provisions and mounts the
Sandbox-owned PVCs. This phase wires the **behavior**: when a sandbox's resolved
`persistenceMode == filesystem`, pause tears the Pod down (retaining the PVCs)
and resume cold-boots a fresh Pod re-mounting the same PVCs — no `SnapshotJob`,
no S3 snapshot, no node-adapter involvement. `snapshot` mode is unchanged.

## What changed

### State machine (`crates/sandbawks-controller/src/state_machine.rs`)

- `transition` now takes a resolved `PersistenceMode`. It is consulted only on
  the pause/resume edges; every other `(phase, trigger)` pair ignores it, so
  passing the `Snapshot` default is always correct for the mode-agnostic edges.
- New `Action::SuspendPod` (filesystem pause: delete Pod, retain PVCs, scrub Pod
  identity) and `Action::FinalizeSuspend` (settle at `Paused`, no snapshot).
- New `Trigger::PodReleased` (the filesystem suspend's Pod teardown completed).
- New/changed edges (all gated on the resolved mode via two small helpers,
  `pause_action` / `resume_action`):
  - `(Running, PauseRequested | Timeout{auto_pause})` → `Pausing` with
    `SuspendPod` in filesystem mode, `Pause` (SnapshotJob) in snapshot mode.
  - `(Pausing, PauseRequested)` **in filesystem mode only** → `Pausing` /
    `SuspendPod` — the crash-recovery re-drive of the idempotent delete. Snapshot
    mode keeps its `(Pausing, PauseRequested) -> None` idempotent no-op.
  - `(Pausing, PodReleased)` → `Paused` / `FinalizeSuspend` (mode-agnostic; only
    the filesystem completion probe ever emits `PodReleased`).
  - `(Paused, ResumeRequested)` → `Resuming` with `CreatePod` (cold boot) in
    filesystem mode, `CreateRestorePod` (scheduler-placed restore) in snapshot
    mode. The `(Resuming, PodReady)` → `Running` promotion is shared by both.

### Reconcile loop (`crates/sandbawks-controller/src/reconcile.rs`)

- `resolve_effective_persistence_mode` — reads `spec.persistenceMode`, falling
  back to the `SandboxTemplate` default, defaulting to `Snapshot` (same
  template→sandbox inheritance `resolve_pod_inputs` applies to runtime class and
  PVCs). Called only at the pause/resume decision points and during the
  transient `Pausing` phase, so steady-state reconciles issue no extra template
  GET.
- `filesystem_pausing_trigger` — the `Pausing` completion probe. Reached only
  when no `SnapshotJob` is in flight; resolves the mode (the one site that must
  tell a filesystem suspend apart from a snapshot pause that crashed before
  creating its job), then folds `PodReleased` (Pod gone → `Paused`) or
  `PauseRequested` (Pod still alive → re-drive the delete).
- The `Resuming` probe now branches on `status.snapshotRef`: a snapshot resume
  always carries the ref its pause committed, so its *absence* selects the
  filesystem cold-boot path (treated like a cold start via
  `creating_liveness_trigger`) over the scheduler-placed restore flow — no extra
  template GET needed to make the distinction.
- `Action::SuspendPod` / `Action::FinalizeSuspend` dispatch arms: `delete_pod`
  (foreground + poll-to-404, idempotent) keeps the Sandbox-owned PVCs (they are
  owned by the Sandbox CR, not the Pod, so no cascade delete), scrubs Pod
  identity, and records a single `Suspended` condition that flips `False`
  (Suspending / draining) → `True` (suspended / Pod gone).
- `upsert_condition` helper — replaces a condition by `kind` rather than
  appending, so the re-drivable `Suspended` row never accretes duplicates.

## Validation

- `just fmt` ✅
- `cargo clippy --all-targets -- -D warnings` ✅
- `just test` ✅ (full workspace)
- New unit tests (`tests/state_machine_unit.rs`): filesystem pause / idle
  auto-pause delete the Pod (`SuspendPod`), `PodReleased` completes to `Paused`,
  the filesystem suspend re-drive is mode-gated, resume cold-boots (`CreatePod`),
  and the `Resuming → Running` promotion is shared. Snapshot-mode edges retested
  unchanged as the regression guard.
- Property tests (`tests/state_machine_properties.rs`) generalized over both
  modes; totality / determinism / sink confinement hold for either, and the new
  filesystem suspend self-loop is accounted for in `no_self_loops`.
- New end-to-end reconcile test (`tests/lifecycle.rs`
  `filesystem_suspend_resume_round_trip`): Running → pause (Pod deleted, PVCs
  retained, no `snapshotRef`, no SnapshotJob, `Suspended=False`) → `Paused`
  (`Suspended=True`) → resume (fresh Pod) → `Running`, asserting **no SnapshotJob
  is created at any point**.

## Not included / out of scope

- No CRD change: this phase adds behavior over the Phase 1 spec fields and the
  existing `status.conditions`; no status schema field was added, so no CRD
  regen / `kubectl apply -f deploy/crds` is required.
- Kata enablement, `snapshot`-on-Kata admission rejection, E2B per-mode docs, and
  the warm-pool VCT cold-boot fallthrough are #603 Phase 4 (#607).
- PVC retention/GC policy is #603 Phase 5 (#608).
- The cross-runtime E2E (write a file → pause → resume → file present on a real
  cluster) is the headline acceptance demo; the PVC mount/retention mechanics it
  exercises are unit-covered by the `PodAdapter` tests from Phase 2.
