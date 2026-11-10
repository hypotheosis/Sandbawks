# How We Send a Trace (minimal OpenTelemetry tracing for a Rust web API)

This is a distilled, copy-pasteable version of this project's tracing setup,
stripped down to **a single binary that serves a web API** (no frontend, no
WASM, no job worker). It exists because getting OTel tracing to *actually
emit functional spans* in Rust is easy to get subtly wrong --- the code
compiles, logs appear, but **no traces ever reach the collector**. The pattern
below is the one that works in this repo; follow it exactly and traces show up.

If you need the full multi-service / browser / metrics / logs version, see
[`OBSERVABILITY.md`](./OBSERVABILITY.md). This doc is traces-only on purpose.

---

## The one idea that makes tracing work

In Rust, you instrument your code with the **`tracing`** crate (`#[instrument]`,
spans, `info!`). OpenTelemetry is a **separate** system. They do not talk to
each other unless you install a bridge. That bridge is the single
`tracing_opentelemetry::layer()` line below.

```text
  your code                bridge layer              SDK                wire
  ---------                ------------              ---                ----
  #[tracing::instrument] ->  tracing_opentelemetry ->  TracerProvider ->  OTLP
  tracing::info_span!        ::layer()                 + BatchProcessor   exporter -> collector
                             .with_tracer(tracer)       + Resource
```

**If the bridge layer is missing, you get logs but zero traces.** This is the
#1 reason an OTel setup "looks done" but produces nothing. Everything else in
this doc is plumbing around that one line.

---

## Step 1: dependencies

Pin the OTel crates to one minor-release line. They are tightly coupled and a
version skew between `opentelemetry*` and `tracing-opentelemetry` is the #2
reason tracing silently doesn't work (the layer compiles against mismatched
types and quietly does nothing, or fails to build).

```toml
[dependencies]
tracing               = "0.1"
tracing-subscriber    = { version = "0.3", features = ["env-filter", "fmt"] }

# Keep these four in lockstep. tracing-opentelemetry 0.28 is the PAIRED version
# of the opentelemetry 0.27 line --- they are not the same number on purpose.
opentelemetry         = "0.27"
opentelemetry_sdk     = { version = "0.27", features = ["rt-tokio", "trace"] }
opentelemetry-otlp    = { version = "0.27", features = ["trace", "http-proto", "reqwest-client"] }
tracing-opentelemetry = "0.28"

anyhow = "1"   # only for the Result/context in init below; drop if you don't use it
```

This block lists only the OTel-specific crates. The examples below also assume
the usual web-API stack you almost certainly already have:
`tokio` (with `macros`, `rt-multi-thread`), `axum`,
`tower-http = { version = "0.6", features = ["trace"] }` (for `TraceLayer`),
and `reqwest` (only for the optional outbound-call section).

`rt-tokio` is mandatory: the batch span processor offloads export onto the
Tokio runtime. Without it the batch processor has no executor and spans never
flush.

---

## Step 2: the init function

Drop this in `src/telemetry.rs`. It is the repo's `init_telemetry`, reduced to
traces-over-OTLP/HTTP.

```rust
//! Minimal OpenTelemetry tracing bootstrap: `tracing` spans -> OTLP -> collector.

use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _; // brings `.tracer()` into scope
use opentelemetry::KeyValue;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Holds the provider so spans flush on shutdown. MUST be kept alive for the
/// whole process --- see the warning in `main` below.
pub struct TracingGuard(TracerProvider);

impl Drop for TracingGuard {
    fn drop(&mut self) {
        // Flush any spans still sitting in the batch processor before exit.
        // Without this, a short-lived process exports nothing.
        let _ = self.0.shutdown();
    }
}

/// Initialise tracing. `service_name` becomes the `service.name` resource
/// attribute --- this is how the trace shows up labelled in the collector.
///
/// Reads `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://localhost:4318`) and
/// `RUST_LOG` from the environment. Returns a guard you must hold.
pub fn init_tracing(service_name: &'static str) -> Result<TracingGuard> {
    // 1. OTLP/HTTP span exporter. Endpoint comes from
    //    OTEL_EXPORTER_OTLP_ENDPOINT; the SDK appends `/v1/traces`.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .context("build OTLP span exporter")?;

    // 2. Provider: batch processor (async flush on Tokio) + a resource that
    //    names the service. The resource is what makes a trace identifiable.
    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            service_name,
        )]))
        .build();

    // 3. Register the provider globally and install the W3C trace-context
    //    propagator. The propagator only matters once you call other
    //    services, but registering it now is free and avoids a gotcha later.
    opentelemetry::global::set_tracer_provider(provider.clone());
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // 4. *** THE BRIDGE *** --- convert `tracing` spans into OTel spans.
    //    Without this layer, nothing your code does with `tracing` ever
    //    becomes a trace. `provider.tracer(name)` (note: the provider, NOT
    //    `global::tracer`, which would be a no-op before the registry is set)
    //    is the tracer the layer feeds.
    let tracer = provider.tracer(service_name);
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer()) // human-readable stderr logs
        .with(otel_layer)                        // tracing spans -> OTel spans
        .init();

    Ok(TracingGuard(provider))
}
```

---

## Step 3: wire it into `main` and your handlers

```rust
use axum::{routing::get, Router, extract::Path, response::IntoResponse};

mod telemetry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Hold the guard for the ENTIRE process. See the warning below.
    let _telemetry = telemetry::init_tracing("my-api")?;

    let app = Router::new()
        // Path-param syntax is axum-version-specific: `{id}` on axum 0.8+,
        // `:id` on axum 0.7. Match whatever your repo already uses.
        .route("/widgets/{id}", get(get_widget))
        // Opens a root span for every inbound request and (if present) adopts
        // the caller's `traceparent` header so traces span service boundaries.
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// `#[instrument]` opens a span for the duration of the handler. `skip_all`
// keeps handler arguments out of the span (avoids leaking PII / large bodies).
// The `name` is the stable span name you'll see in the collector.
#[tracing::instrument(skip_all, name = "handler:get_widget")]
async fn get_widget(Path(id): Path<u64>) -> impl IntoResponse {
    // An *event* (not a span) attached to the current span. Shows up as a
    // log line AND, because of the bridge, as a span event on the trace.
    tracing::info!(widget_id = id, "fetching widget");
    format!("widget {id}")
}
```

That is the whole thing. A request to `/widgets/42` now produces a trace with a
root `http_request` span (from `TraceLayer`) and a child `handler:get_widget`
span carrying the `fetching widget` event.

---

## Why this works here when it often doesn't

Claude (and humans) frequently produce OTel code that compiles and logs but
emits no traces. Every item below is a real failure mode this pattern avoids.
When porting, check each one.

1. **No bridge layer.** The exporter and `TracerProvider` get built, but
   `tracing_opentelemetry::layer()` is never added to the subscriber. Result:
   `#[instrument]` spans go nowhere. **Fix:** step 2.4. This is the most common
   miss by far.

2. **Version skew.** `opentelemetry 0.27` with the wrong `tracing-opentelemetry`
   (e.g. `0.27` or `0.30`) either won't compile or links a layer that does
   nothing. **Fix:** the paired pins in step 1 (`0.27` SDK / `0.28` bridge).

3. **The guard is dropped early.** `let _ = init_tracing(...)` drops the guard
   *immediately* (Rust special-cases the `_` binding), so `shutdown()` runs
   before any request is served and the batch processor is torn down. Same
   problem if you don't return/hold a guard at all: short-lived programs exit
   before the batch interval fires and export nothing. **Fix:** bind to a
   *named* variable (`let _telemetry = ...`) that lives to the end of `main`.

4. **Using `global::tracer()` before the provider is set.** Calling
   `opentelemetry::global::tracer("x")` returns a **no-op** tracer if no
   provider is registered yet. **Fix:** feed the layer `provider.tracer(...)`
   from the concrete provider you just built (step 2.4), not the global.

5. **Missing `rt-tokio` / not on a Tokio runtime.** The batch exporter needs an
   async executor. Without the `rt-tokio` feature, or if `init_tracing` runs
   before `#[tokio::main]` starts the runtime, spans never flush. **Fix:** the
   feature in step 1; call `init_tracing` *inside* `main`, after the runtime
   exists.

6. **Creating events but no spans.** `tracing::info!("...")` is an *event*, not
   a span; events alone do not make a trace. You need at least one span open
   (`#[instrument]` or `TraceLayer`) for there to be a trace to attach to.
   **Fix:** instrument handlers + add `TraceLayer` (step 3).

7. **Endpoint/exporter errors are swallowed.** The batch processor logs export
   failures and moves on --- a wrong `OTEL_EXPORTER_OTLP_ENDPOINT` produces
   *silence*, not a crash. **Fix:** set the endpoint (next section) and verify
   with the collector's debug exporter.

---

## Configuration

The exporter reads standard OTel env vars --- use the spec names so any
vendor's example drops in unchanged:

| Variable | Example | Purpose |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Collector base URL; SDK appends `/v1/traces`. **Required** for export. |
| `OTEL_EXPORTER_OTLP_HEADERS` | `x-api-key=secret` | Auth headers for a hosted collector (comma-separated `k=v`). |
| `RUST_LOG` | `info` | Standard `tracing` verbosity filter. |
| `OTEL_SERVICE_NAME` | `my-api` | Overrides the compile-time `service.name` if you want it set at deploy time. (Optional --- the `service_name` argument is the fallback.) |

No collector handy? Run one locally and point the endpoint at it:

```bash
docker run --rm -p 4318:4318 -p 4317:4317 \
  otel/opentelemetry-collector-contrib
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

The contrib image's default config logs received spans to its own stdout, so
you can confirm traces are arriving.

---

## Verifying it actually works

1. Start your collector (or point at the real one).
2. `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 cargo run`
3. Hit an endpoint: `curl localhost:3000/widgets/42`
4. Within a few seconds (batch flush interval) the span batch appears in the
   collector. Confirm the `service.name` matches and the trace has both the
   `http_request` and `handler:get_widget` spans under one `trace_id`.

If nothing arrives, walk the seven failure modes above in order --- in practice
it is almost always #1 (no bridge layer) or #3 (guard dropped).

---

## Calling another service (optional)

If your API calls a second traced service over HTTP and you want one trace to
span both, inject the current context into the outbound request so the callee's
`TraceLayer` continues the trace instead of starting a fresh one. Add
`opentelemetry-http = "0.27"`, then:

```rust
use opentelemetry_http::HeaderInjector;
use reqwest::header::HeaderMap;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Inject `traceparent` / `tracestate` headers from the active span.
pub fn inject_context(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let context = tracing::Span::current().context();
    let mut headers = HeaderMap::new();
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&context, &mut HeaderInjector(&mut headers));
    });
    builder.headers(headers)
}

// usage: let resp = inject_context(client.get(url)).send().await?;
```

This relies on the `TraceContextPropagator` registered in `init_tracing` step
2.3. Without wrapping the call, the downstream service starts a disconnected
trace --- another common "traces don't correlate" gotcha.

---

## GenAI tool-execution spans (envd)

`envd` terminates the E2B-compatible gRPC protocol inside each sandbox. The
commands an external agent drives through that protocol are emitted as
OpenTelemetry **GenAI** `execute_tool` spans (issue #426), nested under the
per-RPC transport span so they share the caller's `trace_id` and land on the
sandbox's resource (`service.instance.id`).

Two surfaces are instrumented, in `crates/envd/src/gen_ai.rs`:

- **`process.Start`** --- one span per spawned child (`crate::process`). The
  span opens at spawn and closes when the child exits, so its duration tracks
  the command's lifetime.
- **`filesystem.{WriteAll,ReadAll,Move,Remove,MakeDir}`** --- one span per file
  operation (`crate::filesystem`), via `gen_ai::instrument_tool`.

Attributes follow the GenAI tool-execution convention:

| Attribute | Value |
|---|---|
| `gen_ai.operation.name` | `execute_tool` |
| `gen_ai.tool.name` | command basename (`bash`, `python`) or `fs.*` op |
| `gen_ai.tool.type` | `extension` (sandbox-side execution) |
| `gen_ai.tool.call.id` | child pid (process) or a monotonic id (filesystem) |
| `process.exit_code` / `error.type` | exit outcome, recorded at close |

The OTel span *name* is `execute_tool {tool_name}`, set via the
`tracing-opentelemetry` `otel.name` field (a `tracing` span's static name cannot
interpolate).

### Content capture is gated and redacted

Command lines, arguments, and paths can carry secrets/PII, so the
`gen_ai.tool.call.arguments` attribute is recorded **only** when
`OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT` is truthy (`true` / `1`),
and even then the value is first run through `gen_ai::redact_command` /
`gen_ai::redact_path`, which mask `--token=...` / `KEY=VALUE` secret pairs, the
value after a known secret flag, provider tokens with well-known prefixes
(`ghp_`, `AKIA`, `xox?-`, ...), JWTs, and generic high-entropy blobs --- including
tokens embedded inside a shell-form `bash -c "..."` argument. Environment-variable
*values* are never recorded under any setting. The var defaults off, so the
always-on attributes above carry no command content. The redactor is a denylist
of *shapes*, not a guarantee --- widen `gen_ai::looks_like_secret_value` before
recommending the env var be enabled in production.

Operators enable it via the Helm value `observability.captureGenaiContent=true`
(default `false`). The chart sets the env var on the controller; the controller
forwards it into every sandbox's envd via `FORWARDED_OTEL_VARS`
(`crates/sandbawks-controller/src/pod_adapter.rs`) --- the same two-hop path the
other `OTEL_*` vars take. Because envd caches the value on first read, it takes
effect for *newly-created* sandboxes after the controller is redeployed.

---

## Per-sandbox resource attributes (issue #435)

Every sandbox Pod carries a set of OTel resource attributes injected by the
controller at pod-build time, so envd spans and the inner Claude Code agent spans
(when the trace beta is enabled) are traceable back to the specific sandbox CR.

### What is injected

The controller calls `per_sandbox_observability_env` in `pod_adapter.rs` and stamps
two env vars on every sandbox Pod:

**`OTEL_RESOURCE_ATTRIBUTES`** — the cluster-level base (forwarded from the
controller's own `OTEL_RESOURCE_ATTRIBUTES`) merged with per-sandbox identity:

| Attribute key | Value | Convention |
|---|---|---|
| `k8s.pod.name` | sandbox Pod name | OTel k8s semconv |
| `k8s.namespace.name` | Pod namespace | OTel k8s semconv |
| `sandbox.template` | `spec.templateRef` | Sandbawks custom |
| `sandbox.uid` | Sandbox CR `metadata.uid` | Sandbawks custom |

Per-sandbox attrs are appended *after* the cluster-level base, so they win on
key collision (OTel spec: last definition takes precedence in
`OTEL_RESOURCE_ATTRIBUTES`).

**`OTEL_SERVICE_NAME`** — always set to `sandbawks-envd`, regardless of any
`OTEL_SERVICE_NAME` on the controller process (that value names the
*controller*, never envd, and is not forwarded — honoring it would leave envd
under the SDK `unknown_service` placeholder). Both envd and the inner Claude
Code agent (which inherits envd's process environment) land under this service
name in Traceway. A customer can still override it per-sandbox via `spec.env`.

### Merge and precedence

For `OTEL_RESOURCE_ATTRIBUTES`, the final value is `<base>,<per-sandbox identity>`,
lowest-to-highest precedence:

1. **Base** — the customer's `spec.env` pin of `OTEL_RESOURCE_ATTRIBUTES` if
   present, otherwise the controller process's cluster-level value (shared
   operator attrs, e.g. `cluster.region`, read at pod-build time — no
   downward-API wiring needed). A customer pin replaces the controller base.
2. **Per-sandbox identity** (`k8s.pod.name`, `k8s.namespace.name`,
   `sandbox.template`, `sandbox.uid`) is *always* appended last, so it wins on
   key collision (OTel last-wins). A customer can override an individual
   identity key but **cannot null out** a sandbox's identity (issue #435).

`OTEL_SERVICE_NAME` is always set to `sandbawks-envd`; a customer can still pin a
different value in `spec.env` (that one *does* win outright), but the controller's
own `OTEL_SERVICE_NAME` is never consulted.

### Verification

Resource attrs propagate to all signals. The cheapest verification path is the
Claude logs already arriving in Traceway: once a sandbox is created after this
change, every log record from that sandbox will carry
`k8s.pod.name`, `k8s.namespace.name`, `sandbox.template`, and `sandbox.uid` as
resource attributes — visible in the Traceway log detail pane. Trace verification
requires the trace beta (`CLAUDE_CODE_ENABLE_TELEMETRY=1 +
CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1 + OTEL_TRACES_EXPORTER=otlp`) to be active.

### Security posture

Consistent with issues #427 and #295: no credentials are injected. The resource
attribute values (pod name, namespace, template name, CR uid) are identity
metadata only. `OTEL_EXPORTER_OTLP_HEADERS` continues to be excluded from
forwarding.

---

## Extending to logs and metrics

This doc is traces-only. To also ship `tracing::info!`/`error!` as correlated
OTel **log records** and emit **metrics**, see `OBSERVABILITY.md` sections 7
(the trace-correlated log bridge) and 8 (the `OnceLock`-cached metric
instruments). Those are additive: the same provider/subscriber pattern, with a
logger provider + log-bridge layer and a meter provider alongside the tracer.
