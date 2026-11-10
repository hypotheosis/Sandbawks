# OTel observability Phase 1: bootstrap crate + workspace OTel pins (#295)

**Date:** 2026-06-03T12:20:34Z
**Issue:** #295 — OTel observability: Phase 1 — bootstrap crate + workspace OTel pins
**Parent:** #294 — Implement OpenTelemetry observability (logs, metrics, traces) over OTLP
**Branch:** `feat-issue-295-otel-bootstrap-crate`

## Scope

Create the shared bootstrap crate that owns all OpenTelemetry SDK wiring, add
the pinned OTel dependency set to the workspace, and prove the pipeline by
wiring **only** `sandbawks-api` to it. The other three binaries are Phase 2.

## What changed

### New crate `crates/sandbawks-observability`

Modeled on the reference `wnw-observability` (see `OBSERVABILITY.md` /
`TRACING.md` at the repo root), adapted to the post-0.28 OpenTelemetry Rust API.

- **`src/init.rs`** — `init_telemetry(service_name, version) -> Result<TelemetryGuard>`:
  - short-circuits on `OTEL_SDK_DISABLED` (installs an fmt-only stderr
    subscriber, builds no exporter, returns a disabled guard);
  - resolves the wire protocol from `OTEL_EXPORTER_OTLP_PROTOCOL`
    (`http/protobuf` default, `grpc`; anything else is a hard error);
  - `build_resource` with env-wins merge + `unknown_service` placeholder strip
    (`OBSERVABILITY.md` §10), re-expressed against the 0.32 `Resource::builder`
    detector API;
  - `build_sampler` from `OTEL_TRACES_SAMPLER` / `_ARG` (six strategies,
    unknown → spec default `parentbased_always_on`, ratio clamped to `[0,1]`);
  - builds tracer / meter / logger providers, each with its own OTLP exporter
    for the chosen protocol;
  - registers the global tracer + meter providers and the W3C
    `TraceContextPropagator`;
  - installs the layered subscriber: `EnvFilter` + `fmt` (stderr) +
    `tracing_opentelemetry::layer()` **the bridge** + the
    `opentelemetry-appender-tracing` log bridge, with a per-layer noise filter
    that quiets the exporter's own transport targets;
  - emits a one-shot startup line dumping the resolved service identity,
    protocol, endpoint, and raw `OTEL_RESOURCE_ATTRIBUTES`.
- **`src/guard.rs`** — RAII `TelemetryGuard`; `Drop` shuts down all three
  providers, idempotent via `Option::take`, with a `disabled()` variant.
- **`src/lib.rs`** — re-exports `init_telemetry` + `TelemetryGuard`.

### Workspace `Cargo.toml`

Added the OTel stack to `[workspace.dependencies]`, pinned to the **latest
mutually-compatible** line: `opentelemetry*` 0.32 with its paired
`tracing-opentelemetry` 0.33. New crate added to `members`.

### `sandbawks-api`

`main` now calls
`sandbawks_observability::init_telemetry("sandbawks-api", env!("CARGO_PKG_VERSION"))`,
binding the guard to a **named** variable held for the whole of `main`,
replacing the old local `init_tracing`. Dropped the now-unused
`tracing-subscriber` dependency from the crate.

## Two non-obvious decisions (worth flagging for Phase 2)

1. **Async-runtime processors are load-bearing.** As of the 0.28 SDK refactor
   the *default* batch/periodic processors export from a dedicated OS thread
   via `futures_executor::block_on`, which **cannot drive** the async OTLP HTTP
   (`reqwest-client`) or gRPC (`grpc-tonic`) clients — upstream's own docs say
   to use the async-runtime processor for those. So the crate uses the
   Tokio-driven `*_with_async_runtime` processor modules, gated behind three
   `experimental_*` SDK features. Without them the bridge compiles and logs
   appear but **zero spans reach the collector** — the exact silent no-op
   `TRACING.md` #1 warns about. (The issue's original `rt-tokio,trace,metrics,
   logs` feature list predates this refactor.)

2. **OTLP transports are plaintext-only for now (no rustls TLS feature).**
   Enabling otlp's `reqwest-rustls` drags reqwest's default `aws-lc-rs` rustls
   provider into the *same* `rustls 0.23` that `kube` drives with `ring`,
   leaving two `CryptoProvider`s enabled and making auto-selection impossible —
   `kube`'s `Client::try_default()` then panics at startup. Phase 1 only needs
   to reach a *local* collector over plaintext OTLP, so TLS transport (and
   aligning a single rustls provider across `kube` + otlp) is deferred to the
   deployment phases (#294 Phases 4–7). This matches the issue's specified
   otlp feature set exactly (`http-proto, grpc-tonic, reqwest-client`).

## Validation

- `just fmt` / `cargo fmt --all -- --check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace --all-targets` — all pass, including 11 new unit
  tests (resource merge precedence: env wins + `unknown_service` stripped;
  sampler parsing for every strategy + unknown → default; ratio default /
  clamp / warn; protocol resolution; guard idempotency).
- **Live OTLP end-to-end** against `otel/opentelemetry-collector-contrib`
  (`:4318` HTTP): the collector received 2 spans
  (`handler:smoke_root` / `handler:smoke_child`) stamped
  `service.name=sandbawks-api`, plus 3 correlated OTLP log records — confirming
  the `tracing` → OTel bridge actually emits, not just compiles. (Validation
  harness was a throwaway example, not committed.) The real `sandbawks-api`
  binary was also run and printed the expected `OpenTelemetry initialised
  service_name=sandbawks-api protocol=http/protobuf` startup line.

## Out of scope (later phases)

- The other three binaries (Phase 2).
- Trace-correlated log bridge — logs ship here without span-id correlation
  yet (Phase 3).
- Cross-service propagation helper, HTTP/job metrics middleware, TLS transport,
  and deployment config (Phases 4–7).
