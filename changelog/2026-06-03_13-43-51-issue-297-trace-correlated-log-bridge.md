# Feat: trace-correlated OTLP log bridge (OTel Phase 3)

**Date:** 2026-06-03 13:43:51
**Branch:** `feat-issue-297-trace-correlated-log-bridge`
**Issue:** #297 (parent #294)

## Problem

OTLP log records shipped with **no `trace_id` / `span_id`**, so the APM's
"logs for this trace" view could never correlate a log line to the span it was
emitted from (`OBSERVABILITY.md` §7.1).

The cause is a seam between two upstream crates:

- The OTel SDK stamps each log record's trace context from
  `opentelemetry::Context::current()` at emit time (`SdkLogger::emit`: it only
  fills `trace_context` when `cx.has_active_span()`).
- `tracing-opentelemetry` deliberately keeps its span data in the `tracing`
  span's extensions and **never** publishes it as the current OTel context.

So the upstream `opentelemetry_appender_tracing::OpenTelemetryTracingBridge`,
reading an empty current context, emitted uncorrelated records — even though the
same events *looked* correlated in stderr (the `fmt` layer reads the `tracing`
span tree, not the OTel context).

## Fix

New module `crates/sandbawks-observability/src/log_bridge.rs` exposing
`TraceContextLogBridge<P, L>`, a subscriber `Layer` that wraps the upstream
bridge. Its `on_event`:

1. Looks up the span the event was emitted in (`ctx.event_span(event)`).
2. Recovers that span's OTel `Context` via the public
   `tracing_opentelemetry::get_otel_context(span_id, dispatch)` helper.
3. `attach()`es it as the current OTel context for the duration of the
   synchronous inner `on_event` call — the returned guard is bound to a named
   local (`_attach_guard`) and drops before the function returns, so the attach
   never crosses an `.await`.
4. Delegates to the wrapped `OpenTelemetryTracingBridge`, which now reads the
   populated current context and stamps the record.

`init_telemetry` (`init.rs`) installs it as the log layer in place of the bare
upstream bridge, fed by the same Phase 1 `LoggerProvider`.

### Why `get_otel_context` instead of reading `OtelData` directly

`OBSERVABILITY.md` §7.2 sketched reading `trace_id` / `span_id` out of the
span's `OtelData` extension. In `tracing-opentelemetry` 0.33 the `OtelData`
type is **private**, so that approach no longer compiles. `get_otel_context`
is the public API the crate provides for exactly this purpose; it builds the
span's OTel data if needed and returns the full `Context` (preserving baggage
too), which we attach directly.

### Requirement: a globally installed subscriber

`get_otel_context` needs the `tracing` `Dispatch` backing the active
subscriber, fetched with `tracing::dispatcher::get_default`. That call reaches a
real dispatcher only on its *global* fast path; a thread-scoped subscriber
(`with_default`) is unreachable from inside event dispatch because the
dispatcher's reentrancy guard returns a no-op. `init_telemetry` always installs
the subscriber as the global default (`try_init`), so this holds in every
binary. Under a scoped subscriber the lookup yields `None` and records ship
uncorrelated rather than wrong — a safe degradation, documented in the module.

The bridge also skips attaching when the recovered context carries no active
span at all (e.g. an event whose span isn't tracked by an OTel layer). Note this
is *not* a sampling check — `has_active_span()` tests for a populated span slot,
not the `sampled` flag, so unsampled-but-recording spans still correlate; it
mirrors exactly the predicate `SdkLogger::emit` uses to stamp the record.

## Tests

Unit test `event_in_span_is_stamped_with_span_trace_ids` (in `log_bridge.rs`):

- Builds an in-memory `LoggerProvider` (simple processor — synchronous emit, no
  runtime needed) and an always-on tracer.
- Installs the registry subscriber **globally** (the bridge needs the global
  dispatcher), emits an `info!` event from inside an instrumented span.
- Asserts an emitted record's `trace_id` / `span_id` match the span's own,
  captured via `span.context()`.

Pulled in `opentelemetry_sdk`'s `testing` feature as a dev-dependency to gate
`InMemoryLogExporter`.

## Validation

- `just fmt` — clean (only this change's files touched).
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace green; the new correlation test passes.

Manual collector verification (a log line from inside a handler span showing the
same `trace_id` as the span) is left to a live cluster — it needs a running OTLP
collector, which is out of reach here; the unit test exercises the exact
stamping path end-to-end in-process.

## Files

- `crates/sandbawks-observability/src/log_bridge.rs` — new module + test.
- `crates/sandbawks-observability/src/lib.rs` — `mod log_bridge` + re-export.
- `crates/sandbawks-observability/src/init.rs` — install `TraceContextLogBridge`
  in `install_subscriber`; drop the now-unused upstream-bridge import; refresh
  the correlation comment.
- `crates/sandbawks-observability/Cargo.toml` — `testing` dev-feature.

## Out of scope

Cross-service propagation (Phase 4) — correlation here is within a single
process's spans.
