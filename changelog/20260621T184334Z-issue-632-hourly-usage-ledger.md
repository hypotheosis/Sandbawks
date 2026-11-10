# Per-API-key usage metering — Phase 3: hourly idempotent NDJSON usage ledger

**Date:** 2026-06-21T18:43:34Z
**Issue:** #632 (epic #480, Phase 3)
**Crates:** `sandbawks-controller`; Helm chart `deploy/helm/sandbawks`

## Summary

Turns Phase 2's authoritative usage **events** into a durable, queryable,
invoice-safe **ledger**. A periodic controller rollup reads the raw events and
writes idempotent per-`(team, key_name, hour)` `UsageRecord`s as NDJSON to the same
object store, bucketed hourly. Billing reads from this ledger — not the raw events,
and (per the epic trust-model decision) never from OTel.

Per the operator answers in #480 (comment #16264): **object-store NDJSON** ledger,
**hourly** rollups, **per-`TeamAPIKey`** records, derived **only** from Phase 2's
authoritative events.

## Changes

- **New `usage_ledger.rs`** — `UsageRecord` (team, key_name, period start/end,
  weighted + raw running-seconds, distinct-sandbox and source-interval counts), the
  pure `rollup_events`, the `UsageEventSource` (read) and `UsageLedgerSink` (write)
  traits, `run_usage_rollup_pass`, in-memory fakes, and unit tests.
- **`s3_usage.rs`** — added `S3UsageSource` (LIST+GET+parse) and `S3UsageLedger`
  (PUT record), and refactored the Phase 2 `S3UsageSink` to share a `build_bucket`
  helper. All three reuse the existing `rust-s3` + `SANDBAWKS_S3_*` plumbing.
- **`bin/sandbawks-controller.rs`** — `build_usage_rollup_runner()` + a ticker
  (mirroring the retention sweeper), env-gated by `SANDBAWKS_USAGE_ROLLUP_ENABLED`
  (default OFF), `SANDBAWKS_USAGE_ROLLUP_INTERVAL` (default `PT1H`),
  `SANDBAWKS_USAGE_PREFIX` (default `usage`), `SANDBAWKS_USAGE_LEDGER_PREFIX`
  (default `ledger`). Joined into the same `tokio::select!`/abort lifetime.
- **Helm** — new `usage.{metering,rollup,eventPrefix,ledgerPrefix}` values; the
  controller deployment now surfaces the Phase 2 sink env *and* the Phase 3 rollup
  env, and **hoists** the shared snapshot-store S3 connection + credential `envFrom`
  out of the retention-only block so it is emitted whenever retention **or** usage is
  enabled (the chart already flagged this hoist as required). The snapshot-store
  Secret renders when usage needs it too. `Chart.yaml` version `0.1.25 → 0.1.26`.

## Design notes

- **Hourly bucketing across boundaries.** Each `Running` interval is split into
  per-hour slices *proportionally*; each slice's weighted seconds land in the hour it
  ran. Slice factors sum to 1 over a fully-covered interval, so the ledger
  **reconciles**: Σ records' `weighted_running_seconds` = Σ source events'
  `weighted_seconds` over closed hours (unit-tested).
- **Closed hours only.** The in-progress hour is skipped until it closes.
- **Idempotency.** `rollup_events` is pure/deterministic; each record is written at a
  deterministic `(team, key_name, hour)` object key, so re-runs and overlapping ticks
  overwrite rather than duplicate — no double-bill (unit-tested via a re-run pass).
- **Resilience.** Per-record write failures are tallied and the pass continues; the
  next tick re-derives and overwrites. A malformed event object is skipped, not
  fatal.

## Scope note — creates / concurrency

The epic's `UsageRecord` wishlist named `sandbox_creates` and `peak_concurrent`.
Phase 2 (merged) emits only running-interval close events, so those dimensions are
**not derivable** from the authoritative stream (and deriving them from OTel is
forbidden by the trust model). The ledger therefore carries the dimensions the
events support — weighted/raw running-seconds, distinct sandboxes, source-interval
count. Adding creates/concurrency requires a Phase 2 amendment to additionally emit
create/active events; tracked as a follow-up.

## Validation

- `just fmt` — formatted.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — workspace green; 8 new `usage_ledger` unit tests (single-hour roll,
  boundary split, in-progress-hour skip, team/key aggregation + distinct counts,
  reconciliation, determinism, builder round-trip, idempotent re-run pass).
- `helm lint` passes; `helm template` with `usage.metering.enabled=true
  usage.rollup.enabled=true node.enabled=false` emits the usage env + the
  snapshot-store Secret + credential `envFrom`.

## Out of scope (epic non-goals / follow-ups)

- Azure usage source/ledger (S3 only today, mirroring the Phase 2 sink).
- `sandbox_creates` / `peak_concurrent` (needs Phase 2 create/active events).
- Raw-event retention sweep (raw events are retained for replay/backfill; the
  existing retention sweeper can be pointed at the usage prefix in a follow-up).
- Invoicing, pricing, payment, quota enforcement, customer UI.
