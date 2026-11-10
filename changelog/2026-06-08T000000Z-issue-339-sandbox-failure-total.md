# controller.sandbox.failure_total counter (issue #339)

**Date:** 2026-06-08  
**Issue:** #339  
**PR:** TBD

## Summary

Adds a dedicated `controller.sandbox.failure_total{reason}` OTel counter that
increments on every Sandbox→`Failed` fold in the reconcile loop. Before this
change, Sandbox failure folds were visible only via the generic
`phase transition committed` info log and an uptick in
`controller.reconcile.outcome_total{outcome=success}` — neither was alertable
on the failure itself. A node-churn storm that wedged warm-pool members in
`Creating` (issue #337/#338) would silently manifest as `success` reconciles.

## Changes

### `crates/sandbawks-controller/src/state_machine.rs`

- Added `FailureReason` enum: `PodVanishedCreating`, `PodTerminatedRunning`,
  `SnapshotJobFailed`, `ActionFailure`. Each variant carries a stable
  `label()` method used as the OTel attribute value.
- `Trigger::Failure(String)` → `Trigger::Failure { reason: FailureReason, message: String }`:
  the reason is assigned at the trigger-generation site (where the controller
  knows *why* a failure is being folded), not inferred from the free-form message.
- `Action::RecordFailure(String)` → `Action::RecordFailure { reason: FailureReason, message: String }`:
  the state machine passes the reason through so the dispatch site can emit the
  counter without inspecting the message string.

### `crates/sandbawks-controller/src/sandbox_failure_metrics.rs` (new)

OTel metrics module following the `OnceLock`-singleton pattern used by
`warm_pool_metrics` and `dispatch_metrics`:

- Meter scope: `sandbawks-controller.sandbox`
- Instrument: `controller.sandbox.failure_total` (Counter&lt;u64&gt;, attribute `reason`)
- `record_failure(reason: FailureReason)` — single public function, called from
  the two dispatch sites in `reconcile.rs`.
- Unit tests: label coverage + noop-meter smoke test.

### `crates/sandbawks-controller/src/reconcile.rs`

Updated all `Trigger::Failure` construction sites with their respective reasons:

| Site | `reason` |
|---|---|
| `running_liveness_trigger` | `PodTerminatedRunning` |
| `creating_liveness_trigger` | `PodVanishedCreating` |
| `observe_snapshot_job` (Failed phase) | `SnapshotJobFailed` |
| dispatch-error arm | `ActionFailure` |

Metric is emitted from:

1. `dispatch()` → `Action::RecordFailure { reason, .. }` arm.
2. The dispatch-error recovery block (`Err(action_err)` arm) where the
   `transition(next_phase, Trigger::Failure { ActionFailure, .. })` result is
   applied inline.

### `crates/sandbawks-controller/tests/sandbox_failure_metrics.rs` (new)

Dedicated integration-test binary (mirrors `tests/warm_pool_metrics.rs`):
installs a live `SdkMeterProvider` with `InMemoryMetricExporter` before the
first reconcile, drives a `Running` Sandbox whose Pod has terminated
(`pod_terminated_running` fold), flushes the provider, and asserts
`controller.sandbox.failure_total` emits under the `sandbawks-controller.sandbox`
scope on a real recorder.

### `OBSERVABILITY.md`

Added §8.8 documenting the new instrument, its `reason` vocabulary, and why the
dedicated counter is necessary (the reconcile-outcome counter conflates healthy
reconciles with failure folds).

## Acceptance criteria (from issue)

- [x] Single counter increments on every Sandbox→`Failed` fold, labelled by
  a bounded `reason`.
- [x] Emits on a real recorder (dedicated integration-test binary,
  `tests/sandbox_failure_metrics.rs`, per the `OnceLock` cache pattern).
- [x] `OBSERVABILITY.md` updated with the instrument + reason vocabulary (§8.8).
