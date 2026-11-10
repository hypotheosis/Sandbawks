# Usage metering: bill long-lived open intervals + provisional/final hours

**Date:** 2026-06-21T21:22:10Z
**Issue:** #637 (follow-up to epic #480 / Phase 2/3 #631/#632)
**Crate:** `sandbawks-controller`

## Summary

Before this change a `UsageEvent` was only written when a `Running` interval
**closed**, so a still-running sandbox contributed nothing to the ledger until it
paused (and a never-closing sandbox was never billed). This makes the Phase 3 rollup
additionally account each still-`Running` sandbox's **open interval up to `now`**, so
long-lived sandboxes accrue billable usage every pass — and adds a `provisional`
flag so billing knows which hours are safe to invoice.

Chosen approach (of the three the issue listed): **rollup reads live open
intervals**, which also subsumes the watermark/finality direction. No CRD field, no
extra event-object writes, idempotent by construction.

## Changes

- **`usage_ledger.rs`**
  - `UsageRecord` gains `provisional: bool` — `true` for the in-progress wall-clock
    hour (still accumulating, not yet invoiceable), `false` once the hour has elapsed.
  - New `OpenInterval { team, key_name, sandbox_uid, cpu/mem, running_since }`.
  - `rollup_events(events, open, now)` now also accounts each open interval as a
    synthetic event ending at `now` (reusing `build_usage_event`'s weighting), emits
    the current hour (marked provisional) instead of skipping it, and marks past
    hours final.
  - New `OpenIntervalSource` trait + `NoopOpenIntervalSource` + `FakeOpenIntervalSource`;
    `run_usage_rollup_pass(source, open_source, ledger, now)` lists open intervals
    (failure is non-fatal — it degrades to closed-events-only for that pass) and
    `RollupReport` gains `open_intervals`.
- **`store.rs`** — `KubeOpenIntervalSource`: lists Sandboxes cluster-wide and projects
  each `Running` one (with a `lastTransitionTime` anchor) into an `OpenInterval`.
  Reuses the controller's existing Sandbox list/watch RBAC — no new permissions.
- **`bin`** — the rollup ticker builds `KubeOpenIntervalSource` from the kube client
  and passes it to each pass.

## Why this is correct (idempotency / finality)

The key property: a fully-elapsed hour's slice has the **same value** whether computed
from the open interval `[start, now]` or the eventual closed event `[start, close]`
(both fully cover the hour). So:

- **Past hours are final** even while the sandbox runs — safe to invoice, and a
  never-closing sandbox is therefore billed for its elapsed hours.
- **No double-bill across the open→close transition**: the rollup recomputes and
  overwrites by `(team, key_name, hour)`; when the sandbox closes, the authoritative
  event reproduces the same past-hour values (unit-tested), and the records overwrite.
- **The current hour is provisional** — recomputed each pass as it accumulates.

## Acceptance (#637)

- ✅ Long-running sandboxes accrue billable usage at most one rollup-interval behind
  real time (the current hour is accounted up to `now` each pass), not only at close.
- ✅ A never-closing sandbox is billed for its elapsed running time (tested).
- ✅ No double-billing across open accounting + the eventual close (tested).
- ✅ Clear finality semantics via the `provisional` flag.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — workspace green. New `usage_ledger` tests: provisional current hour
  vs final past hours; open-interval accounted up to `now`; past hours unchanged by
  the eventual close (no double-bill); never-closing sandbox bills elapsed hours;
  `run_usage_rollup_pass` accounts open intervals. Existing rollup/reconciliation
  tests updated for the new signature + flag.

## Out of scope

- Pruning the raw-event / ledger prefixes (events kept for replay/backfill).
- `sandbox_creates` / `peak_concurrent` (needs Phase 2 create/active events).
- Invoicing / pricing.
