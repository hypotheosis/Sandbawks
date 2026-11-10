# fix: stop claiming/advertising snapshot-capability when no runsc root resolves (#379)

**Date:** 2026-06-07  
**Branch:** fix-issue-379-runsc-root-unresolved-fail-fast  
**Issues:** #379 (Part B of #377 / Gap #2 of #366)

## Problem

`resolve_runsc_root` previously fell back silently to the configured primary root
when no explicit candidate or search-dir discovery matched the target container.
This produced an opaque `CheckpointFailed` ("FetchSpec failed: loading container:
file does not exist") on every subsequent `runsc checkpoint`, which the node
reconciler treated as retriable.  The warm pool therefore kept scheduling new
pause jobs on the misconfigured node indefinitely — one persistent misconfiguration
turned into an infinite per-sandbox fail loop that wedged the pool.

## Changes

### `crates/sandbawks-types/src/runtime.rs`

Added `RuntimeError::RunscRootUnresolved` — a typed, terminal variant that carries
the container ID, the list of explicit candidates checked, and the search dirs
searched.  Downstream callers can pattern-match on it to distinguish a structural
misconfiguration from a retriable runtime failure.

### `crates/sandbawks-node/src/gvisor.rs`

Changed `resolve_runsc_root` return type from `PathBuf` to
`Result<PathBuf, RuntimeError>`.  Tier 3 (the old warn-and-fallback) now returns
`Err(RuntimeError::RunscRootUnresolved { .. })` instead.  Both `pause` and `resume`
propagate this with `?`.

Updated unit tests:
- `resolve_runsc_root_falls_back_to_primary_when_no_candidate_matches` renamed to
  `resolve_runsc_root_returns_err_when_no_candidate_matches` — now asserts `Err`.
- `discovery_ignores_containerd_task_bundle_false_positive` — now asserts `Err`
  when no runsc tree exists.

### `crates/sandbawks-node/src/envd_advertised_metrics.rs`

Added `ReconcileResult::RunscRootFailed` with label `"runsc_root_failed"` for OTel
counter instrumentation.

### `crates/sandbawks-node/src/node_label.rs`

- Added `AdvertiseOutcome::WithdrawnRunscRoot` variant.
- `reconcile_envd_staged` gains a 4th parameter `runsc_root_failed: &AtomicBool`.
  When the flag is set it issues a null merge-patch (label removal) and returns
  `WithdrawnRunscRoot` — before checking binary presence.
- `run_envd_staged_reconciler` gains a 5th parameter `Arc<AtomicBool>` and
  threads it through to each tick's `reconcile_envd_staged` call.

### `crates/sandbawks-node/src/reconcile.rs`

- `ReconcileContext` gains `runsc_root_failed: Arc<AtomicBool>`.
- In the Pause error handler, `RunscRootUnresolved` triggers
  `ctx.runsc_root_failed.store(true, Ordering::Relaxed)` and a `tracing::error!`
  log before the job is patched `Failed`.

### `crates/sandbawks-node/src/bin/sandbawks-node.rs`

- Creates `Arc<AtomicBool>` at startup (false).
- Passes `Arc::clone` to both `run_envd_staged_reconciler` and `ReconcileContext`.

## Fresh-node safety (#366 review concern)

The flag is set only when a real container's `pause()` returns
`RunscRootUnresolved` — not at boot, not when no containers have landed yet.
A freshly admitted node with a staged binary but no containers keeps advertising
`sandbawks.dev/envd-staged=true` and accepts sandboxes normally.

## Tests added

### `crates/sandbawks-node/tests/reconcile.rs`

- `runsc_root_unresolved_sets_capability_gate_and_fails_job` — gate is set + job
  lands `Failed` with `"no runsc --root resolved"` message.
- `non_runsc_root_failure_does_not_set_capability_gate` — `CheckpointFailed` does
  NOT set gate (retriable errors are unaffected).

### `crates/sandbawks-node/tests/node_label.rs`

- `reconcile_withdraws_label_when_runsc_root_failed_is_set` — with binary present
  but gate set, the reconciler withdraws the label and returns `WithdrawnRunscRoot`.
- `reconcile_advertises_when_gate_clear_and_binary_present` — fresh-node
  no-regression: gate clear + binary present → `Advertised`.

### `crates/sandbawks-node/tests/gvisor.rs`

Added `seed_runsc_root` helper and updated all integration tests that exercise
`pause`/`resume` end-to-end to seed the container state directory in a temp dir
and pass `.with_runsc_root(...)` to the adapter, satisfying the stricter
`resolve_runsc_root` check.

### `crates/sandbawks-node/tests/common/mod.rs`

`make_ctx` now delegates to `make_ctx_with_flag`; the latter accepts a
caller-supplied `Arc<AtomicBool>` so tests can inspect or pre-seed the gate.

## Validation

- `just fmt` — pass
- `cargo clippy --package sandbawks-node --package sandbawks-types --all-targets -- -D warnings` — pass
- `cargo test --package sandbawks-node --package sandbawks-types --all-targets` — pass (170 tests)
- `just test` — fails only on the pre-existing `envd` proto build failure
  (`protoc` missing `google/protobuf/timestamp.proto` in this environment);
  unrelated to this change and present on `main` before this branch.
