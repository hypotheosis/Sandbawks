# Recover Failed-but-Pod-still-Running sandboxes (tiered retry → recover → GC)

**Issue:** #510 — `feat(controller): recover Failed-but-Pod-still-Running sandboxes`
**Area:** controller
**Date:** 2026-06-13

## Problem

When an auto-pause **checkpoint fails**, the controller fail-closes the Sandbox
to terminal `Failed` but deliberately leaves the workload Pod running (so a live
workload isn't destroyed on a failed snapshot). `Failed` was a terminal sink
with **no follow-up reconciliation** — no retry, no recovery, no GC — so the Pod
ran forever holding a Guaranteed node slot while the Sandbox read `Failed`.
Observed in production: `sbx-warm-4v5hp` stuck 38h+ (`phase: Failed`,
`checkpoint failed: envd /__state timed out after 10s`) with its envd container
still `ready: true`.

The fail-closed *intent* is correct. The gap was the **absence of any recovery
loop** after fail-close. This change adds a bounded, tiered recovery path.

## What changed

A tiered **retry → recover → GC** path, scoped strictly to the recoverable
pause-checkpoint failure class so genuine create/terminate failures stay
terminal.

### Tier 1 — bounded checkpoint retry (in `Pausing`)

- New `Trigger::PauseRetry` and edge `(Pausing, PauseRetry) → (Pausing, Action::Pause)`.
- `pause_recovery_trigger` (the pause-side analogue of `resume_recovery_trigger`)
  re-dispatches a failed pause checkpoint under a capped exponential backoff up
  to `SANDBAWKS_PAUSE_MAX_RETRIES` (default 3). A single transient `/__state`
  timeout is no longer terminal.
- `observe_snapshot_job` routes a failed *Pause* job observed while `Pausing`
  through this budget (mirroring the existing recoverable-Stage path).
- On exhaustion it folds to `Failed` with the new, **recoverable**
  `FailureReason::PauseCheckpointFailed`.

### Tier 2 — recover `Failed → Running`

- New `Trigger::FailedPodHealthy`, `Action::RecoverToRunning`, and edge
  `(Failed, FailedPodHealthy) → (Running, Action::RecoverToRunning)`.
- A new `Failed`-phase probe, `failed_recovery_trigger`, gates on
  `FailureReason::is_recoverable` (only `PauseCheckpointFailed`). If the
  workload Pod's container is `Ready` (`pod_ready` → `Some`) **and no explicit
  `spec.desiredPhase=Paused` is still pending**, the live Pod is treated as
  authoritative — the pause simply did not happen — so the Sandbox recovers to
  `Running`. `RecoverToRunning` clears the failure breadcrumbs, resets the
  tier-1 budget, and **re-arms the idle clock** (`last_activity_timestamp = now`)
  so a stale activity timestamp can't immediately re-trip the idle timeout into
  a tight pause→fail→recover loop.
  - Note: `pod_ready` reflects only the kubelet container `Ready` condition, not
    envd's pause-time `/__state` health (that probe lives in `sandbawks-node`),
    so a Pod wedged only at `/__state` (the originating incident; contributing
    trigger #356) is still recovered — the idle re-arm keeps it from
    tight-looping.
  - Recovery is suppressed while an explicit pause is still desired: recovering
    would re-trip `(Running, Paused) => PauseRequested` and oscillate. Such a
    Sandbox stays `Failed` (honest + stable; the GC path still reclaims it if
    its Pod later dies).

### Tier 3 — grace-period GC (last resort)

- New `Trigger::FailedPodOrphaned`, `Action::GcOrphanedPod`, and edge
  `(Failed, FailedPodOrphaned) → (Failed, Action::GcOrphanedPod)`.
- If the Pod is already terminal/gone, or envd has been unreachable past
  `SANDBAWKS_FAILED_GC_GRACE_SECS` (default 300s, measured from
  `status.failed_at`), the orphaned Pod is deleted to reclaim the node slot. The
  Sandbox stays `Failed`; clearing `pod_ref` makes the probe stop re-firing, so
  it is now genuinely terminal, with an `OrphanReclaimed` condition.

### Supporting changes

- **`SandboxStatus`** gains `pause_restart_count`, `pause_last_attempt`,
  `failure_reason` (the bounded reason label, so the probe can identify the
  recoverable class), and `failed_at` (the GC grace anchor). CRDs regenerated.
- **`FailureReason`** gains `PauseCheckpointFailed` + label, a `from_label`
  parser (inverse of `label`), and `is_recoverable`.
- **`push_failure_condition`** now persists `failure_reason` + `failed_at` at
  every `Failed` fold.
- **Metric:** new `controller.sandbox.recovery_total{outcome}` counter
  (`recovered_to_running` / `orphan_gc`) for alertability; the existing
  `controller.sandbox.failure_total{reason="pause_checkpoint_failed"}` covers
  the fold.
- **Config knobs** surfaced via Helm `controller.pauseRecovery.{maxRetries,
  gcGraceSeconds}` (env `SANDBAWKS_PAUSE_MAX_RETRIES` /
  `SANDBAWKS_FAILED_GC_GRACE_SECS`); Chart version bumped to 0.1.7.

## Terminal semantics preserved

`(Failed, _) => None` remains the catch-all. The two new `Failed` exit edges are
emitted *only* by `failed_recovery_trigger`, which returns `None` for any
non-recoverable fold and for a Sandbox with no `pod_ref`. Create-time and
terminate failures therefore remain terminal.

## Related

- Contributing trigger (`/__state` 10s timeout from the OTel-collector-unreachable
  tokio wedge) is separately tracked in #356; this fixes the recovery gap
  regardless.
- #407 covers the opposite direction (Pod lost while Sandbox `Running`).
- Pre-existing orphans (`sbx-warm-4v5hp`, `sbx-309925f75bf2`) predate the
  `failure_reason` field, so they are not auto-recovered — delete them manually
  once verified (per the issue's acceptance note).
