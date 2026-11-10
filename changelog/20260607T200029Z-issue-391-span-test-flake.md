# Issue #391 — De-flake `dispatching_reconcile_emits_the_task_span`

**Date:** 2026-06-07T20:00:29Z
**Issue:** #391 — flaky CI: `dispatching_reconcile_emits_the_task_span` panics with `opened: []`
**Type:** test-only fix (`kind/test`, `area/node`)

## Background

The Rust CI job failed intermittently on the node-crate test
`dispatching_reconcile_emits_the_task_span`
(`crates/sandbawks-node/tests/reconcile.rs`) with:

```
a dispatching SnapshotJob must open the Task span; opened: []
```

The test (added for #375) pins that a real dispatch opens exactly one
`reconcile:node::snapshot_job` Task span. It installs a recording
`tracing::Subscriber` via the **thread-local** `tracing::subscriber::with_default`
and asserts the span was created.

## Root cause

`tracing`'s **callsite-interest cache is process-global**, but the recorder is
installed thread-locally. The span under test is created by
`#[tracing::instrument(name = "reconcile:node::snapshot_job", …)]` on
`reconcile_active` (`crates/sandbawks-node/src/reconcile.rs`). On first hit, that
callsite's `Interest` is cached globally and gates whether `new_span` is ever
called.

The other 16 `#[tokio::test]`s in the same binary call `reconcile(...)` directly
under the global `NoSubscriber` (no recorder). When one of them registers the
`reconcile_active` callsite **first**, its interest caches as
`Interest::never()`. Because `with_default` is thread-local, installing the
recorder does **not** invalidate that global cache — the instrument macro
short-circuits on the cached `never`, never calls `new_span` on the recorder, and
the recorder sees nothing (`opened: []`). Whether the dispatching test or a
`NoSubscriber` `reconcile()` touches the callsite first is decided by parallel
test scheduling, hence the flake.

Reproduced locally on the pre-fix binary: **22/80 (~27%)** of full-binary runs
failed; **0/60** when only the three span-emission tests ran in the process.
A `tracing::callsite::rebuild_interest_cache()` inside the `with_default` closure
did **not** help (still 23/80) — a global rebuild only re-queries
globally-registered dispatchers, and a thread-local `with_default` subscriber is
not one of them.

## Change

Process-isolate the span-emission tests so no `NoSubscriber`-driven
`reconcile()` can register the `reconcile_active` callsite before the recorder
does. Each `tests/*.rs` file compiles to its own binary / process with its own
interest cache, which is equivalent to the proven-clean isolated run. **No
production code changed** — the runtime instrumentation is correct as-is (#375
behaviour is untouched).

- **`crates/sandbawks-node/tests/common/mod.rs`** (new): shared test doubles
  extracted from `reconcile.rs` — constants, `PatchRecord`,
  `FakeSnapshotJobStatusStore`, `FakeRuntimeAdapter`, `default_manifest`,
  `make_job`, `make_ctx`. Carries a module-level `#![allow(dead_code)]` because
  each integration-test binary uses a different subset of the helpers (the
  standard `tests/common` idiom under `-D warnings`).
- **`crates/sandbawks-node/tests/reconcile_span_emission.rs`** (new): the three
  Task-span tests (`terminal_*`, `in_flight_recent_*`, `dispatching_*`) plus the
  `SpanNameRecorder` / `reconcile_under_recorder` harness, in their own binary
  via `mod common;`.
- **`crates/sandbawks-node/tests/reconcile.rs`**: dropped the now-shared helpers
  and the span section; switched to `mod common; use common::*;`. The 14
  behavioural `#[tokio::test]`s are unchanged.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — green.
- Looped the new `reconcile_span_emission` binary **100×** at full parallelism:
  **0 failures** (was ~27% as part of the combined binary).
- Looped the trimmed `reconcile` binary **100×**: **0 failures**.
