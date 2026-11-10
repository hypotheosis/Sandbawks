# OTel observability Phase 2 — `init_telemetry` rollout to controller, node, envd

**Date:** 2026-06-03
**Issue:** #296 (parent epic #294)
**Branch:** `feat-issue-296-otel-phase2-rollout`

## Summary

Migrated the three remaining binaries off their bespoke `init_tracing`
subscribers and onto the shared `sandbawks_observability::init_telemetry`
bootstrap landed in Phase 1 (#303). All four services now install the same
logs + metrics + traces pipeline over OTLP, each stamped with its own
`service.name`, and each holds the returned `TelemetryGuard` for the whole of
`main` so the batch exporters flush on shutdown.

This phase only establishes the provider + subscriber per binary. These are
reconcile loops / agents, not request servers, so root spans come from
`#[instrument]` (Phase 4), not a `TraceLayer`.

## Changes

### `sandbawks-controller`
- Replaced `init_tracing()` with
  `init_telemetry("sandbawks-controller", env!("CARGO_PKG_VERSION"))`, bound to
  `_telemetry` for the lifetime of `main`.
- Deleted the hand-rolled `init_tracing` fn and the now-unused
  `tracing_subscriber` import; dropped the `tracing-subscriber` dependency from
  `Cargo.toml` and added `sandbawks-observability`.

### `sandbawks-node`
- Same swap, service name `sandbawks-node`. Deleted `init_tracing`, dropped the
  unused `tracing-subscriber` dep, added `sandbawks-observability`.

### `envd`
- Replaced `init_tracing(&cli.log_level)` with
  `init_telemetry("envd", env!("CARGO_PKG_VERSION"))`.
- **`--log-level` funnel:** `init_telemetry` sources its `EnvFilter` from
  `RUST_LOG`, and the `--log-level` flag already defaults from `RUST_LOG`
  (precedence: flag > `RUST_LOG` > `info`). The resolved value is now validated
  with `EnvFilter::try_new` (preserving the original fail-fast on an invalid
  filter) and then exported back into `RUST_LOG` so the flag and the env var
  agree and the documented precedence is preserved.
- **Runtime ordering:** the OTLP batch exporters spawn onto the Tokio runtime,
  so — unlike the `#[tokio::main]` siblings — envd's hand-built runtime must
  exist *before* telemetry is initialised. Init and the guard now live and drop
  *inside* `runtime.block_on(...)`, so the guard flushes with a live runtime in
  scope. The explicit-runtime structure (kept for the exit-code/error-printing
  reason documented on `main`) is retained.
- Kept `tracing-subscriber` (still used for `EnvFilter::try_new` validation);
  trimmed its import to just `EnvFilter` and added `sandbawks-observability`.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all workspace tests pass, 0 failures.

### Artifact exercise (acceptance criteria)

Criterion 1 — *boots with `OTEL_SDK_DISABLED=true`, still logs to stderr, no
exporter constructed* — verified for all three binaries:

- `sandbawks-controller`: emits the disabled-path startup line
  `service_name="sandbawks-controller" ... endpoint="disabled"` on stderr, then
  fails (as expected, off-cluster) at `Client::try_default`.
- `sandbawks-node --placeholder`: emits `service_name="sandbawks-node" ...
  endpoint="disabled"`, then enters placeholder mode.
- `envd self-install <dest>`: emits `service_name="envd" ... endpoint="disabled"`,
  copies the binary, exits 0.

Additional envd checks:
- `--log-level warn` suppresses the INFO startup line; `--log-level info` shows
  it — proving the flag drives the `EnvFilter`.
- `--log-level 'envd=notalevel'` fails fast (exit 1, "invalid log filter"),
  preserving the original rejecting behaviour.
- **Active path** (SDK enabled, `OTEL_EXPORTER_OTLP_ENDPOINT` set, no collector
  running): envd builds the full tracer/meter/logger provider stack, emits
  `OpenTelemetry initialised service_name=envd protocol=http/protobuf`, and the
  guard flushes/shuts down cleanly on exit (exit 0) — confirming the
  init-and-flush-inside-`block_on` restructuring is sound.

Criterion 2 — *spans/logs actually arriving at a collector stamped with the
right `service.name`* — the provider construction and resource stamping are
proven above, but end-to-end arrival at a live OTLP collector was not exercised
(no collector available in this environment).

## Review round 1 (PR #310 feedback)

- **envd subcommands skip OTel init.** `cli.command` is now handled *before*
  the Tokio runtime and `init_telemetry` are built, so a one-shot
  `envd self-install` no longer spins up three OTLP batch exporters just to do
  a synchronous file copy. Failure on that path reports via `eprintln!` +
  `process::exit(1)` (no subscriber is installed there). The server path is
  unchanged: init + guard still live and drop inside `block_on`.
- **Sibling ordering aligned.** `sandbawks-controller` and `sandbawks-node` now
  parse their CLI *before* `init_telemetry`, matching `sandbawks-api`, so
  `--help` / `--version` / arg-parse errors short-circuit before any exporter
  is built. Both also gained the `sandbawks-api` ordering note about
  `OTEL_EXPORTER_OTLP_TIMEOUT` needing to cover `Client::try_default` startup.
- **Restore-mode comment made explicit** that `init_telemetry` has already run
  by the time envd reaches the restore-mode idle hold, so an SDK-enabled
  restore Pod intentionally opens/flushes an OTLP connection around the hold.
- **`std::env::set_var`** left as-is: safe on edition 2021 (single-threaded,
  pre-runtime). At the eventual 2024 bump it needs an `unsafe` block, or — the
  cleaner path — `init_telemetry` could grow an optional filter-override
  parameter to skip the `RUST_LOG` round-trip; deferred as out-of-scope for the
  shared crate here.
- **Criterion 2 follow-up filed** to track end-to-end collector arrival
  (kind-smoke job asserting `service.name` on a received span) before Phase 7.

## Out of scope (later phases)

Trace-correlated log bridge (Phase 3), cross-service propagation (Phases 4–5),
metrics instrumentation (Phase 6), deployment config (Phase 7).
