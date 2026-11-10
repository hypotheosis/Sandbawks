# Issue #400 — envd OTel re-arm: reload layers + `RearmHandle`

**Date:** 2026-06-08T01:07:49Z
**Issue:** #400 — envd OTel re-arm: reload layers + RearmHandle in `sandbawks-observability` (parent: #356)
**Type:** fix (`kind/fix`, `area/envd`, `area/observability`, `runtime/gvisor`)

## Background

A gVisor (`runsc`) checkpoint/restore brings envd back as a **memory image**:
the same process, mid-flight, with its OpenTelemetry SDK frozen and then thawed
against a world that has moved on — the collector connection is dead and the
background OTLP exporter threads are gone (issue #356). The process cannot
re-run `init_telemetry`: the global `tracing` subscriber and the OTel globals
are one-shot per process.

This issue is the **keystone** of the #356 re-arm fix: make
`sandbawks-observability` capable of hot-swapping its OTel providers in a live
process. No envd/node resume wiring lands here (that is #356 Phase 2/3) — only
the API and its tests.

## What changed

### `init.rs` — reloadable subscriber

- The OTel span layer (`tracing_opentelemetry::layer()`) and the trace-correlated
  log bridge are now installed behind a single
  `tracing_subscriber::reload::Layer`. Its payload is a
  `Vec<Box<dyn Layer<BaseSubscriber> + Send + Sync>>` (`ReloadLayers`): an empty
  `Vec` is the no-op (quiesced) state, a two-element `Vec` is armed. Boxing keeps
  the heterogeneous pair one nameable type so `reload::Handle::reload` can swap
  it.
- `BaseSubscriber = Layered<EnvFilter, Registry>` is spelled out so the reload
  handle has a single stable subscriber type. The `fmt` layer and the registry
  `EnvFilter` stay **static**, added around the reload layer, never swapped.
- `build_reload_layers()` constructs the span layer + log bridge from a concrete
  tracer/logger provider pair — called once at init and again on every rearm.
- `build_reloadable_subscriber()` assembles the stack and returns it **without**
  installing, so a binary can `try_init` (global) and a test can `set_default`
  (thread-scoped), side-stepping the one-global-subscriber constraint.
- `init_telemetry` now returns `(TelemetryGuard, RearmHandle)`.

### `rearm.rs` (new) — `RearmHandle`

- `quiesce(timeout)`: swaps both reload layers to no-op, points the global meter
  at `NoopMeterProvider`, then `take`s the live providers and shuts all three
  down on **detached threads** collected against a hard deadline — a hung
  exporter is abandoned, never joined, so the resume path never blocks on a dead
  collector. Idempotent and panic-free.
- `rearm()`: rebuilds the three providers from the **current** environment,
  re-registers the global meter/tracer, swaps the live span + log layers to the
  fresh tracer/logger, and records the new providers. On any error it leaves the
  handle quiesced (no-op layers) rather than half-armed; never panics.
- The `OTEL_SDK_DISABLED` boot short-circuit yields a `RearmHandle` whose methods
  are no-ops.

### `guard.rs` — shared providers cell

- `TelemetryGuard` and `RearmHandle` now share a single
  `Arc<Mutex<Option<Providers>>>`. Whoever tears the providers down `take`s them
  out, so guard-drop and `quiesce` never double-shut, and after a successful
  `rearm` a later guard drop flushes the **new** providers.

### Call sites

- `init_telemetry`'s five callers (`sandbawks-api`, `sandbawks-controller`,
  `sandbawks-node`, `envd`, and the node example) now destructure
  `(_telemetry, _rearm)`. Only envd will eventually drive the handle (#356
  Phase 2); the others hold it inert.

## Design notes

- **Per-layer filters stay outside the reload.** The OTel transport-quieting
  filter (`h2`/`hyper`/`tonic`/… at `warn`) is applied to the reload layer as a
  static `Filtered` wrapper, not baked into the swapped payload. A per-layer
  `Filtered` registers its `FilterId` during the one-shot `Layer::on_layer`
  pass, which a post-init `reload` never re-runs; a freshly-built `Filtered`
  swapped into the payload would filter against an unregistered id. Keeping the
  filter static sidesteps this entirely.
  - **Behaviour nuance:** previously this transport filter scoped *only* the span
    layer; the log bridge was gated solely by the global `EnvFilter`. It now also
    quiets transport-target **log records** from OTLP export. This is a strict
    noise reduction (those records are pure transport chatter) and consistent
    with the span-side intent.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` (`cargo test --workspace --all-targets`) — all pass.
- `cargo test -p sandbawks-observability --doc` — doc examples updated and pass.

New unit tests in `rearm.rs` (thread-scoped subscriber + in-memory span
exporters, so they never touch the process-global default):

- `rearm_routes_spans_to_a_fresh_exporter` — a span before rearm lands in the
  initial exporter; after rearm the next span lands in a freshly-minted exporter,
  proving the live layer swap.
- `quiesce_then_rearm_restores_export_and_is_idempotent` — no export while
  quiesced, a second `quiesce` is a no-op, and `rearm` restores export.
- `shutdown_with_deadline_abandons_a_hung_provider` — a provider whose shutdown
  parks forever is abandoned near the 200 ms deadline rather than blocking.
- `disabled_handle_is_a_noop` — the `OTEL_SDK_DISABLED` handle's methods are safe
  no-ops.

## Out of scope (tracked under #356)

- envd HTTP endpoints to drive quiesce/rearm on resume (Phase 2).
- The gRPC-metrics instrument rebuild (Phase 3).
