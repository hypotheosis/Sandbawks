# Plan 07 T10 — deprecate `SANDBAWKS_RESTORE_MODE` envd shim

- **Issue:** #214 (epic #204, Plan 07 Task 10)
- **Date:** 2026-05-29T23:50:18Z
- **Area:** `crates/envd/` (binary entrypoint, subprocess smoke test)

## Summary

The `SANDBAWKS_RESTORE_MODE` env-var shim — envd's "bind no listeners,
hold PID 1 idle until SIGTERM" branch — is superseded by the shim-restore
workflow landed across Plan 07 (T5–T9), where the `containerd-shim`
performs a start-time `runsc restore` so the container is *born restored*
rather than restored over a sleeping idle envd. The env-var branch is now
dead weight kept only for backward compatibility.

This change marks it deprecated without altering behavior: on entry to the
idle hold, envd now logs a `WARN` announcing the Plan 08 removal target,
then proceeds exactly as before (no listener bind, no state-file write,
clean exit on SIGTERM/SIGINT).

## Removal target

**Plan 08 removes the idle-hold branch outright**, tracked against the
fresh-pod resume design in #171. Once removed, setting
`SANDBAWKS_RESTORE_MODE` will have no effect — the controller no longer
injects it, and the shim owns restore end-to-end.

## Changes

### `crates/envd/src/restore.rs` (new lib module)

- Houses `RESTORE_MODE_ENV` and `RESTORE_MODE_DEPRECATION_WARNING` in the
  library crate so the `binary_smoke` integration test imports the exact
  warning string the binary emits — an integration test crate cannot see
  a `src/bin/envd.rs`-local `const`, so keeping the constant binary-local
  would have forced the test to re-inline a parallel literal that could
  drift. The constants moved here verbatim from the binary.

### `crates/envd/src/bin/envd.rs`

- On entry to the restore-mode branch, emit
  `tracing::warn!("{}", RESTORE_MODE_DEPRECATION_WARNING)` before the
  existing idle-hold `info` log. No control-flow change. (The constant is
  passed as a format *argument*, not as the format string, so a future
  edit that adds `{`/`}` to the message can't silently become a format
  directive.)
- Imports the two constants from `envd::restore`.
- **`init_tracing`:** routed the `fmt` layer to **stderr** via
  `.with_writer(std::io::stderr)`. The subscriber's default writer is
  stdout; the issue's acceptance criterion ("assert the warning appears
  in stderr") plus the Unix convention (diagnostics on stderr, program
  output on stdout) make stderr the correct sink.

  **Operator-visible side-effect:** this moves *every* envd log line to
  stderr, not just the new warning. envd previously logged to stdout, so
  a stream-aware scraper (filtering stdout vs. stderr) will see a
  one-release reshuffle. The sibling binaries (`sandbawks-node`,
  `sandbawks-controller`, `sandbawks-api`) still use the default stdout
  writer, so envd is deliberately the outlier — aligning them onto stderr
  is left as a follow-up rather than widened into this deprecation-scoped
  change. No in-tree code consumes envd's diagnostic stdout.

### `crates/envd/tests/binary_smoke.rs`

- `envd_restore_mode_skips_listeners_and_state_file`: bumped the child's
  `--log-level` from `error` to `warn` so the deprecation warning is
  emitted, and added a post-exit assertion that the captured **stderr**
  contains `envd::restore::RESTORE_MODE_DEPRECATION_WARNING` (imported, not
  re-inlined). The child has exited by the read, so the stderr pipe is at
  EOF and `read_to_string` returns without blocking.

## Verification

- `cargo test -p envd --all-features` — green.
- `just fmt`, `cargo clippy --all-targets -- -D warnings` — clean.

## Note on local build environment

`sccache` (`RUSTC_WRAPPER=sccache`) served a stale cached `build.rs`
artifact with an old `env!("CARGO_MANIFEST_DIR")` baked in
(`<previous checkout>` vs. the current
`<repo>`), breaking `protoc` codegen. Forcing a
fresh build-script compile with sccache bypassed resolved it. This is a
local-checkout caching artifact, not a repository change.
