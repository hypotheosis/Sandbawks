# Scheduler-placed restore — foundation + happy-path migration

- **Date:** 2026-06-12T14:14:18Z
- **Issue:** #485 (foundation of epic #484)
- **Crates:** `sandbawks-types`, `sandbawks-controller`

## Summary

Foundation for the scheduler-placed restore + Resuming-recovery epic (#484),
plus the controller-side migration needed to keep the workspace green. The
restore Pod is now **scheduler-placed** instead of pinned to the pre-pause node:
resume inverts from *stage-then-create* to **create-then-stage**, so the
scheduler does memory-aware placement and the snapshot is staged onto whichever
node it lands on.

Issue #485 as filed was Phases 1+2 only (pure types + state machine). But the
state machine is the single source of truth for the reconcile *and* lifecycle
integration tests, so reordering the `Resuming` arms can't be done without
breaking those tests — and the breaking changes (`RestorePlacement::node_name`
removal, the reorder) force the consuming reconcile/adapter code to change too.
Per an explicit scoping decision, this PR therefore carries the migration far
enough to keep `just test` green: the type changes, the state machine, the
controller-side of the adapter (unpinned Pod + `pod_scheduled_node`), and the
**happy-path** reconcile flow (node-discovery dispatch). The bounded-recovery
loop and the genuinely separate artifacts are deferred (see below).

## What landed

### `sandbawks-types` (epic Phase 1)

- `SandboxStatus` gains `resume_restart_count: u32` (default 0) and
  `resume_last_attempt: Option<String>` (RFC 3339, matching the sibling
  timestamp convention). Declarative for now — consumed by the recovery loop in
  a later phase.
- `RestorePlacement` loses `node_name`; it keeps only `staging_path`, now
  documented as the *deterministic* per-sandbox path and explicitly **unpinned**.
- `SnapshotJobSpec` gains `target_path: Option<String>` — the deterministic
  `<stagingRoot>/<sandbox-id>/restore` a resume `Stage` job materializes into.
- New `RuntimeAdapter::pod_scheduled_node(&PodRef) -> Result<Option<String>>`
  (default `Ok(None)`): reads `spec.nodeName` at bind time so the controller can
  discover where the scheduler placed the unpinned restore Pod.
- Regenerated CRDs (`deploy/crds/sandboxes.*`, `deploy/crds/snapshotjobs.*`).

### `sandbawks-controller` state machine (epic Phase 2)

- New `Trigger::NodeAssigned` and `Trigger::RestorePodLost`.
- New `FailureReason::ResumeExhausted` (`label()` ⇒ `"resume_exhausted"`),
  including its doc-table row and the exhaustive label/record tests.
- Reordered `Resuming` arms:
  - `(Paused, ResumeRequested) → CreateRestorePod`
  - `(Resuming, NodeAssigned) → StageSnapshot`
  - `(Resuming, StageComplete) → Noop`
  - `(Resuming, RestorePodLost) → CreateRestorePod`
  - `(Resuming, PodReady) → Running` and `(Resuming, Failure) → Failed` kept.
- Unit tests rewritten for the changed arms + added for the new ones; property
  tests extended (new triggers in the strategy, new `Resuming` self-loop
  exceptions).

### `sandbawks-controller` reconcile + adapter (controller side of Phases 5/6)

- `pod_adapter.rs`: the restore Pod is built **unpinned** (no `spec.nodeName`);
  the envd-staged node affinity is now load-bearing on that path. `PodAdapter`
  implements `pod_scheduled_node` by reading `spec.nodeName`.
- `reconcile.rs`:
  - `Action::CreateRestorePod` no longer requires `status.restoreStagingPath`;
    it stamps the restore Pod's annotation with the deterministic
    `restore_staging_path(name)`, deletes the prior Pod (#196), and clears the
    pre-pause identity **including `assigned_node`** so the scheduler re-picks.
  - New `resuming_schedule_trigger` node-discovery probe: once
    `pod_scheduled_node` returns a node, it folds `NodeAssigned`, and
    `Action::StageSnapshot` records the node and dispatches the node-local
    `Stage` job with `target_path`.
- `FakeRuntimeAdapter` gains `with_scheduled_node` and an interior-mutable
  readiness flag (`with_pod_ready` / `set_pod_ready`) so multi-pass resume tests
  can model the stage-gate that holds the restore Pod un-Ready until staged.
- The reconcile + lifecycle resume integration tests were rewritten to the new
  create-then-stage ordering.

## Deferred to later epic phases (explicitly not in this PR)

- **Bounded recovery loop** (epic Phase 6): `resuming_liveness_trigger` emitting
  `RestorePodLost`, the `resume_restart_count` / `resume_last_attempt` backoff,
  and the `ResumeExhausted` fold. `RestorePodLost` ships here as a tested but
  **unwired** state-machine arm.
- **`sandbawks-stage-gate` image + CI** (Phase 3) and the restore Pod's
  no-creds init-gate container (Phase 5 Pod-shape detail).
- **Node-side staging** honoring `target_path` + the atomic `.staged` marker
  (Phase 4).
- **Helm wiring** for the staging root / deadlines / `RESUME_MAX_RESTARTS`
  (Phase 7).

Because Phases 3/4 are absent, the new flow is green under the fake adapter but
not yet cluster-functional end-to-end; that lands as the later phases merge.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — all workspace tests pass (0 failures).

## Deploy note

CRDs changed (`deploy/crds/`). Helm does **not** manage CRDs, so
`kubectl apply -f deploy/crds` is required out-of-band before the new
`resumeRestartCount` / `resumeLastAttempt` / `targetPath` fields are accepted by
the apiserver. No `deploy/helm/` changes in this PR, so no chart version bump.
