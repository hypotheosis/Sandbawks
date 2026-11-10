# Remove the deprecated `SANDBAWKS_RESTORE_MODE` shim

**Date:** 2026-06-04T20:40:30Z
**Issue:** #263 (epic #260, Plan 08 Workstream C)
**Branch:** `chore-issue-263-remove-restore-mode`

## Summary

Plan 07 T10 deprecated the `SANDBAWKS_RESTORE_MODE` env-var shim — envd's
"bind no listeners, hold PID 1 idle until SIGTERM" branch — and promised
removal in Plan 08. Since Plan 07 T9 the controller no longer injects the
env var: the shim-restore workflow makes the container *born restored*, so
the idle-hold branch was dead code in-tree. This change removes it.

envd now always binds its gRPC/HTTP listeners and loads its boot seed —
there is no longer a startup branch that skips them.

## Changes

### `crates/envd/src/bin/envd.rs`
- Deleted the restore-mode idle-hold branch (the
  `if std::env::var(RESTORE_MODE_ENV)...` block that emitted the
  deprecation warning, idled on a signal, and returned early).
- Dropped the now-unused `use envd::restore::{...}` import.
  `wait_for_signal` and `SCHEMA_VERSION` are still used elsewhere, so
  their imports stay.

### `crates/envd/src/restore.rs` + `crates/envd/src/lib.rs`
- Deleted `restore.rs` (the `RESTORE_MODE_ENV` /
  `RESTORE_MODE_DEPRECATION_WARNING` constants).
- Dropped `pub mod restore;` and its entry in the crate-level module map
  doc comment.

### `crates/envd/tests/binary_smoke.rs`
- Deleted `envd_restore_mode_skips_listeners_and_state_file` and the
  `use envd::restore::RESTORE_MODE_DEPRECATION_WARNING;` import. The
  shared test helpers (`wait_for_accept`, `pick_free_port`) remain in use
  by the other smoke tests.

### `deploy/kind/fixtures/pause-resume-smoke.sh`
- `PAUSE_RESUME_SMOKE_FULL` is back to a binary `{0,1}` instead of the
  Plan 07 T11 tri-state `{0,1,legacy}`. The `legacy)` resume branch
  (env-var patch + deprecation-warning poll) and the `legacy` arms of the
  mode/runtime-class documentation are gone; the `case` validator now
  rejects anything but `0` or `1`.

### `crates/sandbawks-node/src/gvisor.rs`
- Removed a stale parenthetical in the in-process `runsc restore` fallback
  comment that named the now-removed env-var path. The fallback itself is
  unchanged.

## Kept intentionally

- `crates/sandbawks-controller/tests/pod_adapter.rs` still asserts the env
  var is **absent** from both restore and cold-start Pods. Those remain
  valid regression guards and are the only live `RESTORE_MODE` references
  left in `crates/ deploy/ ci/ shim/`.

## Validation
- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all binaries pass.
- `bash -n deploy/kind/fixtures/pause-resume-smoke.sh` — syntax OK.
- `grep -rn "RESTORE_MODE" crates/ deploy/ ci/ shim/` — only the
  pod_adapter absence assertions remain (no live code/fixtures).

Not exercised here: the full `PAUSE_RESUME_SMOKE_FULL=1` in-cluster
round-trip needs a kind + gVisor cluster and is out of reach of the
unit/integration suite.
