# OTel observability Phase 4 — direct-hop trace propagation (api↔envd, node→envd)

**Date:** 2026-06-03
**Issue:** #298 (parent epic #294)
**Branch:** `feat-issue-298-trace-propagation`

## Summary

Stitched a single W3C `trace_id` across the two direct service hops that carry
request traffic — **api → envd** (gRPC) and **node → envd** (HTTP) — so an APM
shows api/node and envd spans under one trace instead of three disconnected
roots. Phase 2 (#296) made every binary *emit* spans; this phase makes those
spans *join up* across process boundaries.

The mechanism is the standard OpenTelemetry context-propagation pair, built on
the `TraceContextPropagator` that `init_telemetry` already registers globally:
a **client injects** the active span's context into outbound headers/metadata
as a `traceparent`, and the **server extracts** it and adopts it as the parent
of its per-request span. Every injection is a no-op outside an active span, so
un-instrumented call paths stay valid (they just start a fresh trace).

## Changes

### `sandbawks-observability` — new `propagation` module

Carrier-agnostic helpers over the globally installed propagator, re-exported
from the crate root:

- `inject_current(&mut dyn Injector)` — serialise the active span's context
  (client side).
- `extract(&dyn Extractor) -> Context` — parse an inbound `traceparent`.
- `continue_span(&Span, &dyn Extractor)` / `continue_current(&dyn Extractor)` —
  adopt the extracted context as a span's parent (server side). `set_parent`
  failures (no OTel layer / span already started) degrade to a trace root and
  are logged at `debug`.
- Carrier adapters: `HttpHeaderInjector` / `HttpHeaderExtractor` over
  `http::HeaderMap` (axum + tonic), and `MapCarrier` (a plain string map) for
  code that hand-builds a request and can't offer a `HeaderMap`.
- Re-exports `opentelemetry::propagation::{Injector, Extractor}` so a call site
  with an exotic carrier (tonic `MetadataMap`) can adapt it without taking a
  direct `opentelemetry` dependency.
- Added a direct `http = "1"` dependency (the carrier type; already in the tree
  transitively via tonic/axum, so the `HeaderMap` type unifies workspace-wide).
- Unit tests: `MapCarrier` get/set/keys, W3C `traceparent` parse into a remote
  context, empty-carrier → empty context, and a `HeaderMap` ↔ `MapCarrier`
  interop round-trip.

### `sandbawks-api` — outbound gRPC injection + inbound extraction

- **`envd_client.rs`:** added `TraceparentInjector`, a tonic `Interceptor` that
  injects the active context into every outbound RPC's metadata (via a small
  `MetadataInjector` adapter implementing `Injector` over `MetadataMap`). The
  pool now hands out clients built with `with_interceptor`; new public type
  aliases `TracedChannel` / `EnvdFilesystemClient` / `EnvdProcessClient` name
  the intercepted client types. `exec.rs`'s two explicit `ProcessClient<Channel>`
  annotations were updated to `EnvdProcessClient` (and its now-unused
  `ProcessClient` / `Channel` imports dropped).
- **`middleware.rs`:** replaced `DefaultMakeSpan` with `ExtractingMakeSpan`,
  which builds the per-request span *and* continues an inbound `traceparent`
  (previously dropped). Because that span is current for the whole request, the
  envd-bound interceptor injects *this* context downstream — so an inbound trace
  flows through the api and on into envd.

### `envd` — server-side gRPC + HTTP extraction

- **`trace.rs` (new):** `GrpcTraceLayer`, a `tower::Layer` installed once on the
  tonic server (`Server::builder().layer(...)`). For each inbound RPC it parents
  a transport span to the caller's `traceparent` and runs the rest of the stack
  inside it, so every handler span (and there are ~19) joins the api's trace
  without a per-handler line. Added `tower` + `http = "1"` deps.
- **`/__state` route:** the handler now takes the inbound `HeaderMap` and calls
  `continue_current`, so the node's pause-probe trace continues into envd's HTTP
  span.

### `sandbawks-node` — outbound HTTP injection

- **`gvisor.rs` (`HttpEnvdProbe`):** the `/__state` probe hand-builds its HTTP
  request over a raw `TcpStream` (it needs `SO_LINGER(0)`, which `reqwest`/
  `hyper` don't expose), so it injects the active context into a `MapCarrier`
  and splices the emitted header(s) into the request. `fetch_state` is now
  `#[instrument]`ed so there is a named span whose context gets propagated.

### Span conventions (`#[tracing::instrument(skip_all, name = "handler:<mod>::<fn>")]`)

- All 19 envd gRPC service-trait handlers (`filesystem::*`, `process::*`).
- All 35 api axum route handlers (`sandboxes`, `events`, `files`, `exec`,
  `envd_proxy`).

`skip_all` keeps PII / auth tokens / large bodies out of span attributes;
stable, low-cardinality names keep the trace tree readable.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean across the workspace.
- `just test` — all pass, including the 4 new `propagation` unit tests.

The end-to-end acceptance criteria (one `trace_id` spanning api + envd in the
collector; a node `/__state` probe continuing into envd) require a live cluster
plus an OTLP collector and are **not** exercised by the workspace gates — see
the PR's Manual Test Plan. The unit tests cover the propagation primitives
(inject/extract/carrier round-trips) deterministically; the cross-process wiring
is verified by construction (matched inject/extract pairs on each hop).

## Out of scope (per the issue)

- Control-plane propagation across the Kubernetes API (api→controller→node) —
  Phase 5.
- Metrics — Phase 6.
