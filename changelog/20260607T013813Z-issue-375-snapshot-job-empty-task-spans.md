# Stop node SnapshotJob no-op reconciles emitting empty Task spans (issue #375)

**Date:** 2026-06-07
**Issue:** #375 — fix(observability): node SnapshotJob no-op reconciles flood the
APM with empty Task spans.

## Summary

The APM Task list was flooded with `reconcile:node::snapshot_job` Tasks that had
**no child spans**, interspersed with occasional real ones. Root cause: the node
`reconcile()` carried `#[tracing::instrument(name = "reconcile:node::snapshot_job",
otel.kind = "consumer", …)]`, which creates and exports the Task root span
**eagerly at function entry** — before the two idempotency-floor short-circuits
run (terminal phase; in-flight within `STUCK_JOB_RECOVERY_AGE`). Those skips do
no work and open no child spans, so each emitted a childless Task. Because
`reconcile_one` requeues every job every 30s (plus watch re-fires), every
lingering terminal/in-flight SnapshotJob re-emitted an empty Task every 30s
until retention GC'd it.

## Fix

Create the Task span only when work is actually dispatched:

- Removed `#[tracing::instrument]` from `reconcile()`.
- Extracted the idempotency floors into a span-free, unit-testable
  `should_dispatch(prior_phase, prior_started_at, now, name, namespace) -> bool`
  predicate (`now` injected for determinism).
- On a dispatch decision, `reconcile()` calls a new `#[tracing::instrument]`'d
  `reconcile_active()` that holds the `reconcile:node::snapshot_job` Task span,
  runs `continue_from_annotations` (trace continuity, must be under the active
  span), records the dispatch metrics (`snapshotjob_metrics`), and calls
  `dispatch()`. No change to idempotency or metrics behavior.

### Secondary fix — negative-age / clock-skew double-run

Extracting the predicate surfaced that the in-flight guard
`age >= 0 && age < RECOVERY` let a **negative age** (a `started_at` in the
future — e.g. this worker's clock trails the one that stamped it) fall through to
**re-dispatch**, contradicting the long-standing comment claiming it was treated
as "still fresh." Under clock skew this could re-run `pause()` against a
container a live pass already owns (the issue #194 double-run). The guard now
treats a negative age as "younger than recovery → skip" (the comment's original
intent). It still self-heals: as wall-clock advances, `age` climbs and
eventually crosses the threshold, so even a pathological far-future stamp
re-dispatches rather than wedging forever.

## Tests

- `tests/reconcile.rs`: a minimal span-recording `tracing::Subscriber` plus three
  integration tests — terminal and in-flight-fresh reconciles open **no**
  `reconcile:node::snapshot_job` span; a fresh dispatching reconcile opens
  exactly one. The two skip tests fail against the pre-fix code and pass after.
- `src/reconcile.rs`: six `should_dispatch` unit tests covering the decision
  matrix (terminal, fresh-pending, in-flight recent/stale, future/negative-age,
  malformed `started_at`).

## Validation

- `just fmt` ✅
- `cargo clippy --all-targets -- -D warnings` ✅
- `just test` ✅ (existing idempotency integration tests stay green)
