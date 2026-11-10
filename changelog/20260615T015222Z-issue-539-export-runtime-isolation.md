# Issue #539 — Telemetry export can no longer starve the serving runtime

**Date:** 2026-06-15T01:52:22Z
**Branch:** `fix-issue-539-export-runtime-isolation`
**Area:** `area/envd`, `area/observability` · **Kind:** fix · **Prio:** p2 · `runtime/gvisor`

## Problem

A blocked, slow, or black-holed OTLP collector could starve envd's tokio
runtime, taking down the serving listeners — gRPC `:49983` and the HTTP
`/__state` / `/__quiesce` / `/__rearm` control plane on `:49984` — even though
those have nothing to do with telemetry. Each prior fix (#350, #355, #356,
#533, #534, #536) addressed a specific *trigger* while the underlying coupling
remained: **telemetry export shared fate with request serving.**

The mechanism: the three OTLP pipelines were built with
`opentelemetry_sdk::runtime::Tokio`, whose `spawn` is a bare `tokio::spawn`
onto *whatever runtime is current when the processor is built* — i.e. the
serving runtime. So every batch-processor / metric-reader background task, and
every blocking `getaddrinfo` the OTLP HTTP client runs on the shared
`spawn_blocking` pool (the #356-implicated path), competed for the serving
runtime's worker and blocking threads. A retry-storm against a dead collector
could wedge both listeners.

## Fix — runtime isolation (issue approach option 1, the keystone)

Telemetry export now runs on its **own dedicated tokio runtime**, structurally
incapable of consuming the serving runtime's threads.

- **New `ExportRuntime`** (`crates/sandbawks-observability/src/export_runtime.rs`):
  a small `multi_thread` runtime — 1 worker, blocking pool capped at 2,
  threads named `sandbawks-otel-export` — with its own I/O and time drivers.
- **`build_providers_on`** (`init.rs`) constructs all three providers inside
  `ExportRuntime`'s `Handle::enter()` guard, so the async-runtime processors'
  background tasks (and the blocking DNS those tasks trigger) spawn on the
  dedicated runtime, never the serving one. This is the single seam that
  decouples export from serving; it covers both first init and every re-arm.
- **Lifecycle:** the runtime is shared (`Arc`) between the `TelemetryGuard`
  (keeps it alive across the guard's flush-on-drop) and the re-arm
  `ProviderFactory` (rebuilds resumed providers onto the *same* isolated
  runtime). `Drop` tears it down with `Runtime::shutdown_background()` rather
  than the default blocking drop, which would panic when the guard drops inside
  the serving runtime's async context (envd's `runtime.block_on`, the
  api/controller `#[tokio::main]`).

Because the export task and its DNS resolution are confined to this runtime,
the fix also delivers the issue's resolver-isolation goal (option 3) for free,
and the bounded blocking pool reinforces the circuit-breaking goal (option 2).

## What changed

- `crates/sandbawks-observability/src/export_runtime.rs` — **new** module.
- `crates/sandbawks-observability/src/init.rs` — build the export runtime;
  route both the initial build and `make_provider_factory` through
  `build_providers_on(handle, …)`.
- `crates/sandbawks-observability/src/guard.rs` — `TelemetryGuard` holds the
  `Arc<ExportRuntime>` so it outlives the flush-on-drop.
- `crates/sandbawks-observability/Cargo.toml` — `tokio` promoted from dev- to
  normal dependency (the runtime is now built at runtime).

No public API change: `init_telemetry` still returns `(TelemetryGuard,
RearmHandle)`; envd/api/controller are untouched.

## Tests

New `export_runtime` unit tests:

- `export_runs_on_the_dedicated_runtime` — a provider built inside the export
  runtime exports on a `sandbawks-otel-export` thread, never on the serving
  runtime that emitted the span (the isolation contract).
- `black_holed_export_does_not_starve_the_serving_runtime` — an exporter that
  hard-blocks its OS thread (black-holed collector) does **not** stop a tick
  task on a single-worker serving runtime; a test-controlled gate keeps it fast
  and deterministic. This is the #539 regression guard.
- `dropping_inside_a_runtime_does_not_panic` — proves `shutdown_background`
  makes the guard's drop-inside-`block_on` panic-free.

## Manual validation (against a black-holed collector — accepts TCP, never replies)

- `just fmt`, `just clippy` (`-D warnings`), `just test` — all green (one
  unrelated `sandbawks-node` ETXTBSY flake passed on retry).
- Booted real `envd` pointed at the black hole:
  - `GET /__state` answered immediately (serving unaffected).
  - `POST /__quiesce` → `200` in 3.0s (the bounded `max_export_timeout`, not a
    hang); `POST /__rearm` → `200` in 1ms; `/__state` still `200` afterward.
  - SIGTERM → clean exit code 0, no panics, no re-arm failures logged.

This confirms the issue's `pause → resume → pause` acceptance criterion against
an unreachable collector: telemetry is lost gracefully, serving and the
checkpoint/restore control plane stay intact.
