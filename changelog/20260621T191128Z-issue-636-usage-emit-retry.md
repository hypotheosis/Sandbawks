# Usage metering: bounded retry + observable drop for event emission

**Date:** 2026-06-21T19:11:28Z
**Issue:** #636 (follow-up to epic #480 / Phase 2 #631)
**Crate:** `sandbawks-controller`

## Summary

Hardens the authoritative usage-event emission against a transient object-store
blip. Previously `emit_running_usage` (the Phase 2 close-edge emitter) was
fail-open on the *first* error: a single failed PUT dropped the interval silently,
and a sustained outage dropped every interval that closed during it with no signal.
This adds a **bounded retry with exponential back-off** before failing open, and a
**metered `emitted` / `dropped` outcome** so a genuine drop is observable and
alertable — without ever wedging the pause/terminate transition.

## Changes

- **New `usage_metrics.rs`** — meter `sandbawks-controller.usage`, counter
  `controller.usage.events_total` with a closed `outcome` ∈ {`emitted`, `dropped`}
  label (mirrors `dispatch_metrics.rs` + the warm-claim outcome-enum idiom).
  `record_usage_emit(outcome)`. `lib.rs` registers the module.
- **`reconcile.rs::emit_running_usage`** — replaced the single best-effort emit with
  a retry loop: up to `USAGE_EMIT_MAX_ATTEMPTS` (default 3, overridable via
  `SANDBAWKS_USAGE_EMIT_MAX_ATTEMPTS`) attempts with exponential back-off
  (`USAGE_EMIT_BACKOFF` 100ms, doubling, capped at `USAGE_EMIT_BACKOFF_CAP` 2s). On
  success it meters `emitted` (and logs a recovery line if it took a retry); on
  exhaustion it meters `dropped`, logs at `error`, and returns so the lifecycle
  transition proceeds.

## Design notes

- **Fail-open preserved.** The retry is bounded; a sustained outage still fails open
  (worst-case ~300ms added before giving up with the default 3 attempts —
  negligible against the pause path's SnapshotJob round-trip). Pause/terminate never
  depends on object-store availability.
- **Retry is safe.** The event's object key is deterministic in
  `(sandbox_uid, interval_start)`, so a re-emit (here, or via a crash re-drive)
  overwrites the same object and never double-bills.
- **Observability.** `rate(controller.usage.events_total{outcome="dropped"}) > 0`
  now alerts that the ledger is losing intervals; the drop is no longer silent.

## Validation

- `just fmt` — formatted.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — workspace green, including:
  - `usage_metrics`: outcome labels + no-op-meter safety.
  - `reconcile`: `emit_running_usage_retries_then_succeeds` (sink fails once, the
    interval still lands) and `emit_running_usage_fails_open_after_exhausting_retries`
    (sink always fails → exhausts exactly the budget, persists nothing, and returns
    rather than blocking).

## Out of scope (still open follow-ups)

- A durable local spill + replay queue for intervals dropped during a *sustained*
  outage (this PR rides out transient blips; a multi-minute outage still drops).
  Could layer onto the `dropped` signal added here.
- Mid-run flushing of long-lived open intervals (#637).
