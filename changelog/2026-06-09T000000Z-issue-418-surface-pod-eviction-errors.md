# Issue #418 — Surface pod eviction (ephemeral-storage/OOM) as a clear sandbox error

**Date:** 2026-06-09  
**Branch:** `feat-issue-418-surface-pod-eviction-errors`

## Problem

When a sandbox Pod was killed by the kubelet's eviction manager mid-request, the only
signal the caller received was an opaque h2 transport error:

```
SandboxException('Code.internal: envd: status: Unknown, message: "h2 protocol error:
error reading a body from connection", details: [], metadata: MetadataMap { ... }')
```

The real cause — `reason: Evicted`, `message: "Pod ephemeral local storage usage exceeds
the total limit of containers 256Mi."` — lived in the Pod status and was invisible to
both the controller and the caller. This was hit in practice after #411 started enforcing
the template's `ephemeralStorage: 256Mi` limit.

## Solution

The controller now inspects Pod status when it detects termination and records an
actionable message in `Sandbox.status.conditions[].message`. Three cases are handled in
priority order:

| Signal | `FailureReason` label | Condition message |
|---|---|---|
| `pod.status.reason == "Evicted"` | `pod_evicted` | `sandbox was evicted while Running: <kubelet message>` |
| `containerStatuses[].state.terminated.reason == "OOMKilled"` | `pod_evicted` | `sandbox exceeded its <limit> memory budget while Running (OOMKilled)` |
| Any other terminal state | `pod_terminated_running` | `backing Pod terminated while Running (envd exited or Pod deleted)` |

## Changes

### `crates/sandbawks-types/src/runtime.rs`
- Added `PodTerminationCause` enum (`Evicted { message }`, `OomKilled { memory_limit }`,
  `Other`) re-exported from the crate root.
- Changed `RuntimeAdapter::pod_terminated` signature from `Result<bool, RuntimeError>` to
  `Result<Option<PodTerminationCause>, RuntimeError>` — `None` = still running,
  `Some(cause)` = terminated with actionable detail.

### `crates/sandbawks-controller/src/pod_adapter.rs`
- Replaced `pod_is_terminal(pod) -> bool` with `pod_termination_cause(pod) ->
  Option<PodTerminationCause>` which extracts eviction/OOMKill details.
- Updated `PodAdapter::pod_terminated` to return the richer type.
- Added unit tests covering all three termination causes plus priority ordering.

### `crates/sandbawks-controller/src/state_machine.rs`
- Added `FailureReason::PodEvicted` variant with label `"pod_evicted"`.

### `crates/sandbawks-controller/src/sandbox_failure_metrics.rs`
- Added `pod_evicted` to the reason table doc comment.
- Extended `reason_label_covers_every_variant` and `recording_against_the_noop_meter_does_not_panic`
  tests to cover the new variant.

### `crates/sandbawks-controller/src/reconcile.rs`
- Added `termination_cause_to_failure` helper that maps `PodTerminationCause` to
  `(FailureReason, String)` — the single place the kubelet signal is translated to
  user-facing text.
- Updated `running_liveness_trigger` and `creating_liveness_trigger` to call the helper
  and embed the cause in the `Trigger::Failure` message.

### `crates/sandbawks-controller/src/fake_adapter.rs`
- Updated `FakeRuntimeAdapter` to store `Option<PodTerminationCause>` internally.
- Kept `with_pod_terminated(bool)` for existing tests (maps `true` → `Other`).
- Added `with_pod_evicted(message)` and `with_pod_oom_killed(memory_limit)` builders.

### `crates/sandbawks-controller/tests/reconcile.rs`
- Updated `CreatingPodAdapter::pod_terminated` to return the new type.
- Added `running_with_evicted_pod_surfaces_eviction_message` test.
- Added `running_with_oomkilled_pod_surfaces_memory_limit` test.
