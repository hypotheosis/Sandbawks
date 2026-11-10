# Observability Strategy: Emitting Logs, Metrics, and Traces over OpenTelemetry

This document explains, in replication-grade detail, how this repository
captures **logs**, **metrics**, and **traces** and ships them over OTLP to a
local APM (Traceway) or any OTel-compatible vendor. It is written so the same
strategy can be lifted into another Rust repository with minimal translation.

The short version: every signal is produced through the `tracing` family of
crates, bridged into the OpenTelemetry SDK by a single shared bootstrap crate
(`wnw-observability`), and exported over OTLP. Browser code (compiled to WASM)
uses a separate, dependency-light crate (`wnw-observability-wasm`) that hand-
rolls OTLP protobuf and POSTs it through a backend proxy. Nothing is
JavaScript; nothing is bespoke per service beyond a service name.

---

## 1. Architecture at a glance

```text
                                browser (wasm32)
                                    |
                                    |  POST /api/telemetry/v1/{logs,metrics,traces}
                                    |    (backend proxy forwards OTLP over reqwest)
                                    v
            +-----------+      +----+----+      +--------+
            | wnw-      |      | wnw-    |      | wnw-   |
            | frontend  +----->| backend +----->| job-   |
            | (SSR)     |  fn  |         | apa- | worker |
            +-----------+      +---------+ lis  +--------+
                  \                |                  /
                   \               | OTLP/HTTP or gRPC
                    \              v                 /
                     \      +-----+-------+         /
                      +---->| OTel        |<-------+
                            | Collector / |
                            | Traceway    |
                            +-------------+
```

Key strategic decisions baked into this shape:

1. **One bootstrap crate, three binaries.** `wnw-observability` owns all SDK
   initialization. Each binary calls one function with its own service name.
2. **`tracing` is the only instrumentation API the application code touches.**
   `tracing::info!`, `#[tracing::instrument]`, and span macros are all the
   application ever sees. The OTel SDK is wired in behind them.
3. **All three signals share one endpoint and one resource.** Traces, metrics,
   and logs go to the same OTLP endpoint, stamped with the same resource
   attributes, so an APM can correlate them.
4. **Protocol is a runtime choice.** A single binary supports both OTLP/HTTP
   and OTLP/gRPC, selected by an environment variable, not a recompile.
5. **Trace context propagates end-to-end** via W3C `traceparent` headers across
   HTTP boundaries, so one user action yields one `trace_id` spanning SSR ->
   backend -> job worker (and, where wired, the browser).

---

## 2. Crate layout and dependency pins

Two crates implement the strategy:

| Crate | Target | Responsibility |
|---|---|---|
| `wnw-observability` | native (server binaries) | SDK init, exporters, the `tracing` <-> OTel bridge, context injection helper, RAII shutdown guard. |
| `wnw-observability-wasm` | `wasm32-unknown-unknown` (browser) | Buffer + encode OTLP protobuf in the browser, flush to the backend proxy. No `tokio`, no full OTel SDK (incompatible with wasm). |

### 2.1 Workspace dependency pins

The entire OTel stack is pinned to one minor-release line in
`[workspace.dependencies]` (root `Cargo.toml`). OTel crates are tightly coupled
across minor versions, so pinning them together in the workspace prevents a
sub-crate from dragging in an incompatible `0.x` and breaking the build:

```toml
# Cargo.toml (workspace root)
tracing            = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }

# OpenTelemetry --- pinned to the 0.27.x line, with tracing-opentelemetry 0.28.x
# (its paired version). Keep these in lockstep.
opentelemetry                  = "0.27"
opentelemetry_sdk              = { version = "0.27", features = ["rt-tokio", "trace", "metrics", "logs"] }
opentelemetry-otlp             = { version = "0.27", features = ["trace", "metrics", "logs", "http-proto", "grpc-tonic", "reqwest-client"] }
opentelemetry-appender-tracing = "0.27"
opentelemetry-http             = "0.27"
opentelemetry-stdout           = { version = "0.27", features = ["trace"] }
tracing-opentelemetry          = "0.28"
```

What each crate does and why it is needed:

- **`tracing`** --- the instrumentation facade the application calls (`info!`,
  `instrument`, `span!`).
- **`tracing-subscriber`** --- the registry that hosts the layered pipeline;
  `env-filter` reads `RUST_LOG`, `fmt` prints to stderr.
- **`opentelemetry`** --- the API types (`KeyValue`, `Context`, propagators,
  global provider accessors).
- **`opentelemetry_sdk`** --- the concrete providers and processors.
  `rt-tokio` plugs batch/periodic export into the Tokio runtime; `trace`,
  `metrics`, `logs` enable all three signal pipelines.
- **`opentelemetry-otlp`** --- the OTLP exporters. Enabling **both**
  `http-proto` and `grpc-tonic` is what makes the runtime protocol switch
  possible. `reqwest-client` is the HTTP transport.
- **`opentelemetry-appender-tracing`** --- the upstream bridge that turns
  `tracing::Event`s into OTel log records (wrapped by a custom layer; see
  [section 7](#7-logs)).
- **`opentelemetry-http`** --- supplies `HeaderInjector` for context
  propagation.
- **`opentelemetry-stdout`** --- optional debug exporter that pretty-prints
  spans to stdout alongside OTLP.
- **`tracing-opentelemetry`** --- the bridge layer that turns `tracing` spans
  into OTel spans. This is the keystone of the whole design.

### 2.2 Browser (WASM) dependencies

The browser crate cannot use `tokio` or most of the OTel SDK on
`wasm32-unknown-unknown`, so it is deliberately minimal:

```toml
# wnw-observability-wasm/Cargo.toml (essentials)
tracing            = { workspace = true }
tracing-subscriber = { version = "0.3", features = ["registry", "std"] }  # no tokio
prost              = "..."   # hand-encode OTLP protobuf
getrandom          = { version = "...", features = ["js"] }  # trace/span IDs in the browser
wasm-bindgen       = "..."
web-sys            = "..."   # DOM, PerformanceObserver, fetch
```

The browser does its own OTLP encoding with `prost` (see
`wnw-observability-wasm/src/proto.rs`) rather than pulling the native SDK.

---

## 3. The single initialization entry point

Everything server-side flows through one function:
`wnw_observability::init_telemetry(service_name, version)` in
`wnw-observability/src/init.rs`. A binary calls it once, near the top of
`main`, and holds the returned guard for the process lifetime:

```rust
// wnw-backend/src/main.rs
let _telemetry = wnw_observability::init_telemetry(
    "wnw-backend",
    env!("CARGO_PKG_VERSION"),
).context("failed to initialise OpenTelemetry SDK")?;
// ... run the app ...
// `_telemetry` drops at the end of main, flushing all three signal pipelines.
```

`init_telemetry` performs these steps in order:

1. **Short-circuit if disabled.** If `OTEL_SDK_DISABLED=true`, install only the
   `fmt` + `EnvFilter` subscriber (so `cargo test` and local runs still see
   stderr logs) and return a *disabled* guard. No exporter is ever constructed,
   so no collector connection is attempted. This is the default in local
   Docker Compose.
2. **Resolve the protocol** from `OTEL_EXPORTER_OTLP_PROTOCOL`
   (`http/protobuf` default, or `grpc`; anything else is a hard error).
3. **Build the resource** (`service.name`, `service.version`, plus env
   attributes; see [section 8](#8-resource-attributes--service-identity)).
4. **Build the sampler** from `OTEL_TRACES_SAMPLER` / `_ARG`.
5. **Build the three providers** --- tracer, meter, logger --- each with its
   own OTLP exporter for the chosen protocol.
6. **Register globals** --- the tracer and meter providers, plus the W3C
   `TraceContextPropagator`, so any code reaching for
   `opentelemetry::global::meter(...)` or the global propagator sees the
   configured stack.
7. **Install the `tracing` subscriber** that bridges the program into OTel
   (see [section 4](#4-the-tracing--otel-bridge-the-keystone)).
8. **Emit a one-shot startup line** dumping the resolved resource attributes
   and the raw `OTEL_RESOURCE_ATTRIBUTES` value, so an operator can diagnose a
   missing-attribute problem from a single log line.
9. **Return a `TelemetryGuard`** holding the three providers.

### 3.1 RAII shutdown guard

`init_telemetry` returns a `TelemetryGuard` (`wnw-observability/src/guard.rs`).
Its `Drop` impl calls `shutdown()` on all three providers, flushing any
batched-but-unsent spans/logs and the final metric collection. Holding the
guard until the end of `main` ensures shutdown runs **after** the Tokio runtime
work is done. The guard is idempotent (uses `Option::take`) and has a
`disabled()` variant carrying no providers.

> Replication note: this is the single most common footgun. If you drop the
> guard early (or never bind it to a name --- `let _ =` drops *immediately*),
> the batch processors never flush and short-lived programs export nothing.
> Bind it to a named variable (`let _telemetry = ...`) that lives as long as
> the program.

---

## 4. The `tracing` -> OTel bridge (the keystone)

The application never calls the OTel API to create spans. It uses `tracing`,
and a layered `tracing_subscriber` registry routes each `tracing` primitive to
the right destination. `init_telemetry` builds this registry in
`install_subscriber`:

```rust
// wnw-observability/src/init.rs (install_subscriber, abridged)
let env_filter = EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| EnvFilter::new("info"));

let tracer = tracer_provider.tracer(service_name.to_string());
let otel_layer = tracing_opentelemetry::layer()
    .with_tracer(tracer)
    .with_filter(build_otel_layer_filter());   // per-layer noise suppression

let log_bridge = TraceContextLogBridge::new(logger_provider);  // see section 7

tracing_subscriber::registry()
    .with(env_filter)                          // RUST_LOG gates everything
    .with(tracing_subscriber::fmt::layer())    // human-readable stderr
    .with(otel_layer)                          // tracing spans  -> OTel spans
    .with(log_bridge)                          // tracing events -> OTel logs
    .try_init();
```

The four layers, and what each one does:

| Layer | Source | Destination |
|---|---|---|
| `EnvFilter` | `RUST_LOG` | Gates verbosity for all layers below it. |
| `fmt::layer()` | spans + events | Pretty stderr output for humans / container logs. |
| `tracing_opentelemetry::layer()` | spans | OTel spans via the `TracerProvider`. |
| `TraceContextLogBridge` | events (`info!`, `error!`, ...) | OTel log records via the `LoggerProvider`. |

`try_init` (not `init`) is used so test binaries that re-enter
`init_telemetry` across a test matrix don't panic on the second global-
subscriber install; a failure is logged and ignored.

### 4.1 Suppressing third-party span noise

The OTel layer gets its **own** per-layer filter (`build_otel_layer_filter`)
that starts from `RUST_LOG` and then drops a few noisy third-party span
targets (e.g. `tower_sessions=warn`, `tower_sessions_sqlx_store=warn`). This
keeps the APM's endpoint trace trees focused on application work **without**
reducing what a developer sees in their terminal --- the registry-level
`EnvFilter` that gates `fmt` is untouched, so `RUST_LOG=debug` still shows
everything locally.

---

## 5. Exporter configuration

All three providers are built the same way: pick the transport from the
protocol enum, optionally apply a timeout, wrap in the appropriate processor.

```rust
// Tracer provider, HTTP/protobuf arm (init.rs, abridged)
let mut otlp = opentelemetry_otlp::SpanExporter::builder().with_http();
if let Some(timeout) = exporter_timeout() {     // OTEL_EXPORTER_OTLP_TIMEOUT (ms)
    otlp = otlp.with_timeout(timeout);
}
let exporter = otlp.build()?;
builder.with_batch_exporter(exporter, runtime::Tokio)   // batched, async
```

Processor choices per signal:

- **Traces** --- **batch** processor (`with_batch_exporter(.., runtime::Tokio)`).
  Spans accumulate and flush asynchronously.
- **Metrics** --- **periodic reader** (`PeriodicReader::builder(.., runtime::Tokio)`).
  The SDK collects and pushes on an interval.
- **Logs** --- **batch** processor, same as traces.

The protocol switch is a `match` on the `OtlpProtocol` enum. The two arms
(`with_http()` vs `with_tonic()`) are intentionally near-duplicated rather than
hidden behind a trait object, because the runtime protocol choice is a
first-class feature and the explicit branch keeps it visible.

### 5.1 Where the data goes (Traceway / the collector)

The exporter reads the standard OTel environment variables --- the bootstrap
crate deliberately reads **only spec-defined names** so a vendor's example
snippets drop in unchanged:

| Variable | Role |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Base URL of the collector / Traceway. The SDK appends `/v1/traces`, `/v1/metrics`, `/v1/logs`. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` (default) or `grpc`. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Comma-separated `key=value` auth headers added to every request (e.g. a vendor bearer token). |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | Per-export timeout in milliseconds. |

Locally, `OTEL_EXPORTER_OTLP_ENDPOINT` points at the OTel Collector container
(`http://otel-collector:4318`); in production it points at Traceway or a
vendor's intake directly.

### 5.2 Optional stdout debug exporter

Setting `OTEL_DEBUG_STDOUT=true` attaches a **second**, *simple* (synchronous)
span processor backed by `opentelemetry_stdout::SpanExporter` alongside the
OTLP one. Spans are pretty-printed to stdout the instant they end, letting an
operator compare the on-the-wire span shape against what the APM is (or isn't)
showing. The OTLP side keeps its batch processor, so wire throughput is
unaffected.

---

## 6. Traces

### 6.1 Root spans per request

Inbound HTTP requests get a root span from `tower_http::trace::TraceLayer`,
installed in both `wnw-backend` and the frontend SSR. The `TraceLayer`
extracts any incoming `traceparent` / `tracestate` headers so the span is
parented to the upstream caller when one exists.

### 6.2 Handler / server-function child spans

Every HTTP handler and every server function carries
`#[tracing::instrument]`, producing a named child span under the request span:

```rust
// wnw-backend/src/api/turn.rs
#[tracing::instrument(skip_all, name = "handler:turn::post_advance")]
async fn post_advance(/* ... */) { /* ... */ }
```

```rust
// wnw-frontend/src/server_fns/market.rs
#[tracing::instrument(skip_all, name = "handler:server_fns::market::buy")]
async fn buy(/* ... */) { /* ... */ }
```

Conventions worth copying:

- **`skip_all`** prevents handler arguments (which may carry PII or large
  payloads) from being recorded as span attributes.
- **Stable `name = "handler:<module>::<fn>"`** gives the APM a clean,
  low-cardinality span name that survives refactors of the function signature.

The `tracing-opentelemetry` layer turns each of these `tracing` spans into an
OTel span (stamping `code.filepath` / `code.lineno`), and span events become
OTel span events. Parent/child linkage is automatic via the layer's
thread-local context tracking.

### 6.3 Cross-service trace propagation (W3C `traceparent`)

`init_telemetry` registers `TraceContextPropagator` (W3C Trace Context) as the
global propagator. Outbound HTTP calls inject the active context with a one-
line helper exported from the bootstrap crate:

```rust
// wnw-observability/src/init.rs
pub fn inject_context(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let context = tracing::Span::current().context();  // OTel ctx of the active tracing span
    let mut headers = HeaderMap::new();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(&mut headers));
    });
    builder.headers(headers)
}
```

Every server-to-server `reqwest` call (SSR -> backend, backend -> job worker)
is wrapped in `inject_context`, so the downstream `TraceLayer` extracts the
`traceparent` and continues the same trace. Outside any span (or before
`init_telemetry`), the context is empty and the helper is a no-op.

> Cross-boundary gaps to be aware of when replicating: a new outbound call that
> *isn't* wrapped in `inject_context` starts a fresh, disconnected trace
> downstream. For background-job queues (Apalis here), the producer must place
> the trace context into job metadata for the consumer to continue the trace;
> until that is wired, the consumer opens a root span. In sandbawks the
> control-plane "queue" is the Kubernetes apiserver: §6.4 closes this gap by
> carrying the context in CR annotations.

### 6.4 Control-plane propagation via CR annotations (OTel Phase 5)

Header propagation (§6.3) only reaches hops that are a single request/response.
The api -> controller -> node legs are not: the api *writes a CR* and returns,
a reconcile loop *observes* it some time later, and the controller hands work
to a per-node worker through a `SnapshotJob` CR. There is no live connection to
carry a `traceparent` header, so the context rides in the resource itself, under
the `sandbawks.dev/traceparent` annotation.

`sandbawks-observability::propagation` provides the CR analogue of the header
helpers, built on the same global `TraceContextPropagator`:

- **Producers** call `inject_into_annotations(&mut BTreeMap<String, String>)`
  just before writing a CR — it serialises the active span's context into the
  `sandbawks.dev/traceparent` annotation (plus `sandbawks.dev/tracestate` when
  present). A no-op outside any span, so a CR written with tracing disabled
  carries no annotation.
- **Consumers** call `continue_from_annotations(meta.annotations.as_ref())` on
  the first line of a reconcile (under its `#[tracing::instrument]` root span),
  adopting the stamped `traceparent` as the span's parent. An absent annotation
  (e.g. an operator-edited CR) leaves the reconcile a fresh trace root.

Wired at:

| Producer (inject)                                   | Consumer (continue)                          |
| --------------------------------------------------- | -------------------------------------------- |
| api `create_sandbox` (cold create)                  | controller `reconcile:controller::sandbox`   |
| api warm-claim resume patch                          | controller `reconcile:controller::sandbox`   |
| controller warm-pool member create                  | controller `reconcile:controller::warm_pool` |
| controller `build_snapshot_job` (SnapshotJob write) | node `reconcile:node::snapshot_job`          |

The node reconcile span then parents the downstream envd spans via the gVisor
`/__state` probe's header injection (§6.3), so a sandbox created through the api
yields one `trace_id` end-to-end: api handler -> controller reconcile -> node
reconcile -> envd.

---

## 7. Logs

Logs are ordinary `tracing` events (`tracing::info!`, `warn!`, `error!`,
`debug!`). They reach two destinations: the `fmt` layer (stderr) and the OTel
`LoggerProvider` (OTLP log records). The bridge to OTLP is the subtle part.

### 7.1 Why a custom log bridge

The upstream `opentelemetry_appender_tracing::OpenTelemetryTracingBridge`
forwards every `tracing::Event` to the `LoggerProvider` correctly, but it reads
`trace_id` / `span_id` from `opentelemetry::Context::current()` at emit time.
The problem: `tracing-opentelemetry` deliberately does **not** attach its spans
as the current OTel context --- the OTel span data lives in the `tracing`
span's extensions, not in the OTel thread-local. The result is that logs emitted
from inside a span ship with **no trace IDs**, and the APM's "logs for this
trace" view cannot correlate them.

### 7.2 The fix: `TraceContextLogBridge`

`wnw-observability/src/log_bridge.rs` wraps the upstream bridge. For each event
it looks up the active `tracing` span, pulls the OTel `trace_id` / `span_id`
out of the span's `OtelData` extension, builds a `SpanContext`, and attaches it
as the current OTel context **only for the duration of the synchronous inner
`on_event` call**:

```rust
// wnw-observability/src/log_bridge.rs (abridged)
fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
    let _attach_guard = current_span_context(&ctx).map(|span_context| {
        OtelContext::current()
            .with_remote_span_context(span_context)
            .attach()                       // TLS attach, bounded to this call
    });
    self.inner.on_event(event, ctx);        // upstream bridge now sees the IDs
    // guard drops here, before returning --- never crosses an .await
}
```

`current_span_context` reads the IDs from the span extensions that
`tracing-opentelemetry` populated in `on_new_span`:

```rust
let otel_data = span_ref.extensions().get::<OtelData>()?;
let trace_id  = otel_data.builder.trace_id?;
let span_id   = otel_data.builder.span_id?;
SpanContext::new(trace_id, span_id, TraceFlags::SAMPLED, false, TraceState::default())
```

Because the attach is bounded to a synchronous call and the guard drops before
the function returns, it never crosses an `.await` boundary --- the standard,
safe escape hatch for thread-local OTel context inside an async runtime. Each
emitted log record then carries the parent span's `trace_id` / `span_id`, plus
`code.target` / `code.filepath` / `code.lineno` and the severity level.

---

## 8. Metrics

Metrics are emitted directly through the OTel metrics API, reading the global
meter provider that `init_telemetry` registered (Phase 2). Each binary owns one
or more metric sources, each a small per-crate module: the four Phase-6 sources
(§§8.1--8.4, issue #300) plus the warm-pool claim/reconciler sources (§§8.6--8.7,
issue #288) the api and controller added once the pipeline existed.

Four design rules are shared by all four sources:

- **Instruments are cached in a `OnceLock`** singleton so the SDK's
  `meter.<kind>(...).build()` (which allocates and does a registry lookup) runs
  once, not per emission:

  ```rust
  fn metrics() -> &'static ServerMetrics {
      static METRICS: OnceLock<ServerMetrics> = OnceLock::new();
      METRICS.get_or_init(ServerMetrics::new)
  }
  ```

- **Label cardinality is bounded.** Every attribute is drawn from a small,
  statically-known set (matched routes, gRPC method paths, CRD kinds, job
  modes, success/error) — never a raw URI, id, or free-form string.
- **The instrumentation scope is named after the emitting library**
  (`sandbawks-<crate>.<area>`), not the service, per OTel guidance.
- **When the SDK is disabled, `global::meter(...)` returns a no-op meter** whose
  instrument operations are zero-cost, so the instrumentation stays installed
  unconditionally — there is no enabled/disabled branch on any hot path.

The intended **service-level indicators** built on these series are
`http.server.request.duration` (api latency) and the error-ratio of
`controller.reconcile.outcome_total{outcome="error"}` /
`snapshotjob.executions_total{status="error"}` against their totals. Wiring
those into Traceway SLOs/alerts is Phase 7 (out of scope here).

### 8.1 api — HTTP server metrics (axum middleware)

`sandbawks-api/src/http_metrics.rs` is an `axum::middleware::from_fn` handler
emitting the three OTel-semconv HTTP server instruments under the meter
`sandbawks-api.http.server`:

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `http.server.request.duration` | `Histogram<f64>` | s | method, route, status code |
| `http.server.active_requests` | `UpDownCounter<i64>` | --- | method, route |
| `http.server.request.count` | `Counter<u64>` | --- | method, route, status code |

The `http.route` label is axum's `MatchedPath` (the registered pattern, e.g.
`/sandboxes/:id`), not the raw URI; an unmatched path falls back to an
`unmatched:<path>` marker so a dashboard can *see* cardinality risk. The
in-flight gauge deliberately carries fewer attributes (no status code) to stay
comprehensible. Installed as the outermost `Router::layer`, which applies it
per matched route — so `MatchedPath` is populated when it runs. The middleware
records `+1`/`-1` on the gauge around `next.run(req)`, then the duration and
count once the response status is known.

### 8.2 controller — reconcile metrics

`sandbawks-controller/src/reconcile_metrics.rs` instruments both kube-rs control
loops (the Sandbox loop and the warm-pool loop), under the meter
`sandbawks-controller.reconcile`:

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `controller.reconcile.duration` | `Histogram<f64>` | s | kind, outcome |
| `controller.reconcile.outcome_total` | `Counter<u64>` | --- | kind, outcome |

`kind` is the CRD kind (`Sandbox` | `SandboxWarmPool`). `outcome` is `success`
(reconcile returned `Ok`), `error` (returned `Err`), or `requeue` (the kube-rs
error policy fired and scheduled a backoff). `requeue` is counter-only — the
error policy is an instant hook with no meaningful latency, so it is not fed
into the duration histogram.

### 8.3 node — SnapshotJob metrics

`sandbawks-node/src/snapshotjob_metrics.rs` instruments the node's `SnapshotJob`
execution path (pause / resume / stage), under the meter
`sandbawks-node.snapshotjob`:

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `snapshotjob.duration` | `Histogram<f64>` | s | mode, status |
| `snapshotjob.executions_total` | `Counter<u64>` | --- | mode, status |
| `snapshotjob.claims_total` | `Counter<u64>` | --- | node, mode |
| `snapshotjob.watch_info` | `Gauge<u64>` | --- | node, namespace |

`status` is `success` | `error`; `mode` is `pause` | `resume` | `stage`. Only
the actual dispatch is recorded — reconcile passes that short-circuit on the
terminal/in-flight idempotency floors do no work and are not counted, so one
logical pause/resume/stage yields exactly one point per instrument. The `stage`
rows are the envd-staged restore path (#286); `status=error` over `pause`
subsumes #277's skipped-digest failures.

`claims_total` and `watch_info` (#335) close the blind spot from #333, where a
node silently watched the wrong namespace, claimed *zero* jobs for ~11h, and
nothing surfaced it. `claims_total` is recorded the moment the reconciler
commits to a job — *before* dispatch — and is labelled by `node`, so a node
draining no work (or one stuck mid-pause) is visible per-node rather than only
in the cross-node completions counter. One consequence for a dashboard: the
stuck-job recovery path (a re-dispatch after `started_at` exceeds the
stuck-recovery age) records a *second* claim for the same logical job — a new
node legitimately committing to abandoned work — so `sum(claims_total) >
sum(executions_total)` is expected there and is not a bug to chase. `watch_info` is the info-gauge pattern
(constant `1`, data in the labels, like `kube_pod_info`), recorded once at
controller boot: `snapshotjob_watch_info{node="..."}` names the namespace each
node's watch is scoped to, which before #335 lived only in a boot-time log line.

#### Alert: pause/resume work queued but no node draining it (#335)

The #333 failure mode is "there is snapshot work to do, but the cluster-wide
claim rate is flat at 0." Expressed in PromQL over the metrics above plus the
warm-pool gauges (§8.7), gated so it stays quiet during legitimate idle periods:

```promql
# No SnapshotJob has been claimed cluster-wide for 15m...
# `or vector(0)` synthesizes a 0 when the series is ABSENT — without it the
# alert silently no-ops in exactly the #333 scenario (a node that boots
# watching the wrong namespace has never written the series, so `rate(...)`,
# `sum(...)`, and `... == 0` are all empty, never `0`).
(sum(rate(snapshotjob_claims_total[15m])) or vector(0)) == 0
  # ...while a warm pool sits below its floor, i.e. there IS resume/top-up work
  # that a node should be draining (the exact #333 wedge).
  and on() (min(warm_pool_ready - warm_pool_min_ready) < 0)
```

The `or vector(0)` is load-bearing: the failure being alerted on is "claims
went to / stayed at zero," and the cold-start case is "claims *never happened*,"
which PromQL represents as no-data rather than `0`. Without the fallback the
alert is blind during the first window after a deploy — precisely when a
misconfigured node is silent. (Pairing it with a standalone
`absent(snapshotjob_claims_total)` alert is an equivalent belt-and-braces.) The
gate's `min(warm_pool_ready - warm_pool_min_ready)` relies on every warm pool
exporting *both* gauges under matching `pool` labels — which `warm_pool_metrics.rs`
guarantees (all three pool gauges are re-recorded together each pass) — so the
implicit vector match holds; the gate silently no-ops only if a pool ever drops
one of the two series.

Once it fires, `snapshotjob_watch_info` names the namespace each node is
actually watching — a row pointing at the wrong namespace is the #333 signature.
This reading assumes one `watch_info` row per node: the gauge is recorded exactly
once per process at boot from the immutable env-derived namespace, so a node
shows a single `(node, namespace)` row. (If a node ever emitted the gauge twice
with different namespaces in one process, both rows would linger until restart —
`record_watch_namespace` documents this single-call contract.)
Per §7, wiring this expression into Traceway SLOs/alert routing is the
downstream vendor's job; the repo's contribution is the queryable series and the
canonical expression. The warm-pool-floor gate covers #333's warm-pool wedge; a
broader gate keyed on a `Sandbox.status.phase=Pausing|Resuming` count would also
catch user-sandbox pause/resume starvation, but no such cluster-wide phase-count
series is emitted today (the controller reconciles one Sandbox at a time).

To confirm these two instruments serialize over OTLP without a full cluster,
`just otel-emit-smoke` stands up a throwaway collector in Docker and drives the
real SDK through them via the `emit_snapshotjob_metrics` example, asserting both
series arrive with the expected `node` / `mode` / `namespace` attributes — the
local complement to the cluster-bound `just otel-arrival-smoke` (#311).

### 8.4 envd — gRPC server metrics

`envd/src/grpc_metrics.rs` is a `tower::Layer` (sibling of the trace layer in
`trace.rs`) installed once on the tonic server, recording per-method instruments
under the meter `envd.grpc.server`:

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `rpc.server.duration` | `Histogram<f64>` | s | rpc.system, rpc.service, rpc.method |
| `rpc.server.request.count` | `Counter<u64>` | --- | rpc.system, rpc.service, rpc.method |

Attributes follow the OTel RPC semantic conventions: the gRPC path
`/package.Service/Method` is split into `rpc.service` (`package.Service`) and
`rpc.method` (`Method`), with a constant `rpc.system="grpc"`. All three are
statically bounded by the proto. Duration is time-to-response (handler setup for
server-streaming RPCs, not the stream lifetime) — the conventional gRPC
server-side latency signal. Both instruments are recorded by a drop guard, so a
cancelled RPC is still counted. One layer covers every RPC on both the
Filesystem and Process services.

### 8.5 Browser web-vitals metrics

Not applicable to sandbawks (no browser/WASM surface). [Section 9](#9-browser-wasm-telemetry)
is inherited reference material from the strategy's origin repository.

### 8.6 api --- warm-pool claim metrics

`sandbawks-api/src/warm_claim_metrics.rs` instruments the warm-pool fast path
(`warm_claim::claim_for`) --- the gateway's headline warm-vs-cold SLI (#288) ---
under the meter `sandbawks-api.warm_claim`:

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `warm.claims_total` | `Counter<u64>` | --- | pool, outcome |
| `warm.claim.duration` | `Histogram<f64>` | s | pool |

`outcome` has five values. Four are **terminal** --- exactly one per
`claim_for` call, so they partition every claim attempt: `hit` (a warm member
was claimed and resumed), `miss_no_pool` (no pool covers the template;
`pool="none"`), `miss_drained` (a pool existed but every candidate was claimed
by a concurrent request first --- cold-boot fallback), and `error` (a backing
`kube` call failed and the gateway propagated a 5xx). The fifth, `race_lost`, is
**orthogonal**: emitted once per individual lost compare-and-swap inside the
retry loop, it is a contention signal (a sustained rate flags the create↔delete
thrash §8.7's counters also expose). The warm hit rate is:

```text
sum(warm_claims_total{outcome="hit"})
  / sum(warm_claims_total{outcome=~"hit|miss_no_pool|miss_drained"})
```

The denominator pins three of the four terminal outcomes: it excludes both
`race_lost` (orthogonal) and `error`, because an errored claim served neither a
warm member nor a cold-boot fallthrough --- it is already counted as a failure
in `http.server.request.count` (§8.1), so folding it into the warm-vs-cold ratio
would understate the hit rate for an outage unrelated to pool sizing. `error`
still completes the partition and is independently alertable.

`warm.claim.duration` times `claim_for` entry to the resume patch's return ---
the sub-second handoff the feature promises --- and is recorded **only on a
`hit`** (the miss paths perform no resume), so its count equals
`warm_claims_total{outcome="hit"}` and it carries no `outcome` attribute.

### 8.7 controller --- warm-pool reconciler metrics

`sandbawks-controller/src/warm_pool_metrics.rs` instruments the
`SandboxWarmPool` reconciler (`warm_pool::reconcile_pool`), recorded once per
pass and labelled by `pool`, under the meter `sandbawks-controller.warm_pool`:

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `warm_pool.ready` | `Gauge<u64>` | --- | pool |
| `warm_pool.min_ready` | `Gauge<u64>` | --- | pool |
| `warm_pool.max_ready` | `Gauge<u64>` | --- | pool |
| `warm_pool.created_total` | `Counter<u64>` | --- | pool |
| `warm_pool.deleted_total` | `Counter<u64>` | --- | pool |
| `warm_pool.pause_requested_total` | `Counter<u64>` | --- | pool |

The three gauges plot `ready` against its configured band without scraping CR
status: a pool sitting below `min_ready` means the next claim cold-boots. The
counters expose the thrash the issue warns about --- when `min_ready ==
max_ready` and a member flickers, a buggy plan oscillates create↔delete, visible
as `rate(warm_pool_created_total) > 0 and rate(warm_pool_deleted_total) > 0` on
the same pool. `pause_requested_total` tracks the auto-pause edge that drives
freshly-booted members to `Paused`; a high pause rate without `ready` rising
points at members failing to pause (e.g. a missing `runtimeClassName`). The
values are exactly the fields the per-reconcile `info` log already carries, so
the metric and the log stay in lock-step.

### 8.8 controller --- sandbox failure counter

`sandbawks-controller/src/sandbox_failure_metrics.rs` records a dedicated,
alertable counter for every Sandbox→`Failed` fold, under the meter
`sandbawks-controller.sandbox`:

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `controller.sandbox.failure_total` | `Counter<u64>` | --- | reason |

#### The `reason` label

`reason` is drawn from a closed enum (`state_machine::FailureReason`) — the
same compiler-checked, bounded-cardinality pattern as §8.6's `outcome` label —
so the label set is exhaustive and the counter can always be alerted on:

| `reason` | When |
|---|---|
| `pod_vanished_creating` | Backing Pod vanished or went terminal before the Sandbox became `Running` (node churn, Pod GC — issue #338). |
| `pod_terminated_running` | Backing Pod terminated while the Sandbox was `Running` (envd exited or Pod deleted). |
| `snapshot_job_failed` | A `SnapshotJob` (pause / stage / resume) reached `phase=Failed`. |
| `action_failure` | A reconcile action (`create_pod`, `pause`, `terminate`, etc.) returned an error; the Sandbox is folded to `Failed` as the recovery path. |

#### Why this counter is necessary

The reconcile outcome counter (§8.2) increments on **`success`** when a
Sandbox folds to `Failed` — because the reconcile itself succeeded in recording
the failure. The only prior signal was the `phase transition committed` info log
at `from=<old> to=Failed`. Neither is reliably alertable:
`controller.reconcile.outcome_total{outcome=success}` conflates healthy
reconciles with failure folds; the log lacks a stable, low-cardinality
structured field.

`controller.sandbox.failure_total{reason="pod_vanished_creating"}` is the
dedicated alertable for the node-churn storm that motivated issue #338: warm-pool
members wedging in `Creating` when their Pods are GC'd now show up as a rising
rate rather than a quiet `success` tick.

---

## 9. Browser (WASM) telemetry

Browser code compiles to `wasm32-unknown-unknown`, where the native OTel SDK
and `tokio` are unavailable. `wnw-observability-wasm` therefore reimplements a
minimal OTLP pipeline.

### 9.1 Initialization and sampling

`init_browser_telemetry(service_version)` is called once from the frontend's
hydrate path. It reads two attributes that the SSR shell injects on the
`<body>` element:

- `data-otel-sample-rate` --- a head-based ratio sampler (default `1.0`),
  driven by `OTEL_BROWSER_SAMPLE_RATE` at SSR render time.
- `data-otel-traceparent` --- the SSR request's `traceparent`, used as the
  parent so browser spans link to the server-side trace for the same page load.

It installs a `tracing-subscriber` registry (with a WASM-specific OTLP logs
layer), a `PerformanceObserver` for web vitals, a flush interval, and a panic
hook.

### 9.2 Buffer, encode, flush

All three signals are buffered in bounded FIFO queues (logs, metrics, spans),
encoded to OTLP protobuf with `prost` (`wnw-observability-wasm/src/proto.rs`),
and drained on a `setInterval` flush loop that POSTs to the backend proxy:

```rust
// wnw-observability-wasm/src/lib.rs (flush targets)
let _ = post("/api/telemetry/v1/logs", body).await;
let _ = post("/api/telemetry/v1/metrics", body).await;
let _ = post("/api/telemetry/v1/traces", body).await;
```

The browser resource is stamped with `service.name=wnw-frontend-wasm`,
`service.version`, `telemetry.sdk.name=wnw-observability-wasm`,
`telemetry.sdk.language=rust-wasm`, and (when available) `browser.user_agent`
and `browser.url`. The panic hook emits an ERROR log record, records an
exception event on the active span, sets span status to ERROR, and flushes
before unwinding.

> The browser never holds OTLP credentials and never talks to the collector
> directly --- it always goes through the backend proxy below.

### 9.3 The backend telemetry proxy

`wnw-backend/src/api/telemetry.rs` exposes
`POST /api/telemetry/v1/{logs,metrics,traces}`. It:

- **Rate-limits per session** (keyed by signed session id, falling back to a
  forwarded-IP `anon:` bucket) with a fixed-window counter, returning `429`
  when exceeded; rejects bodies over a size cap with `413`.
- **Forwards to the upstream collector** using server-side credentials from
  `OTEL_BROWSER_EXPORTER_OTLP_ENDPOINT` / `OTEL_BROWSER_EXPORTER_OTLP_HEADERS`
  (each falling back to the standard `OTEL_EXPORTER_OTLP_*` pair). This keeps
  the browser tier routable to a separate collector with separate auth.
- **Injects the active SSR-side trace context** onto the forwarded request via
  `wnw_observability::inject_context`, so the collector can link the browser
  signal to the server span.

---

## 10. Resource attributes & service identity

Every exported signal (server and browser) carries a resource. The server-side
resource is built in `build_resource`:

```rust
let fallback = Resource::new(vec![
    KeyValue::new("service.name", service_name.to_string()),     // compile-time fallback
    KeyValue::new("service.version", version.to_string()),       // env!("CARGO_PKG_VERSION")
]);
let env = filter_unknown_service_placeholder(Resource::default());
fallback.merge(&env)   // env wins over the compile-time fallback
```

Two subtleties that are easy to get wrong and worth replicating exactly:

1. **Merge direction.** `Resource::merge` lets the *argument* win. To let an
   operator's `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` override the
   binary's compile-time default, the env resource must be the argument:
   `fallback.merge(&env)`. The reverse silently clobbers env-set names.
2. **The `unknown_service` placeholder.** The SDK's
   `SdkProvidedResourceDetector` injects `service.name="unknown_service"` when
   no real value is set. Because env wins the merge, that placeholder would
   overwrite the compile-time fallback every time. `build_resource` strips
   *only* that one placeholder pair before merging, so an explicit env-set
   `service.name` still wins but the placeholder never does.

Service names in this repo: `wnw-backend`, `wnw-job-worker`,
`wnw-frontend-ssr`, `wnw-frontend-wasm`. Per-binary defaults come from the
`init_telemetry` argument; `OTEL_SERVICE_NAME` overrides them in deployment.

---

## 11. Sampling

`build_sampler` reads `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG`,
defaulting to `parentbased_always_on` (the OTel spec default). Supported
strategies: `always_on`, `always_off`, `traceidratio`,
`parentbased_always_on`, `parentbased_always_off`,
`parentbased_traceidratio` (the `*_traceidratio` variants take a `0.0..=1.0`
ratio in `_ARG`). An unrecognized name logs a warning and falls back to the
default rather than failing startup. Browser sampling is separate and head-
based, via `OTEL_BROWSER_SAMPLE_RATE`.

---

## 12. Configuration reference

### Shared (every server service)

| Variable | Default | Purpose |
|---|---|---|
| `OTEL_SDK_DISABLED` | `false` prod / `true` local compose | Short-circuits init to fmt-only; no exporter is built. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Collector/Traceway base URL; SDK appends `/v1/{traces,metrics,logs}`. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | `http/protobuf` or `grpc`. |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | Comma-separated `key=value` auth headers. Secret in prod. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | SDK default (~10s) | Per-export timeout, milliseconds. |
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=local,service.namespace=wnw` (compose) | Extra `key=value` resource tags. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | Sampling strategy. |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0` | Ratio for `*_traceidratio`. |
| `OTEL_DEBUG_STDOUT` | unset | `true` adds a stdout span exporter alongside OTLP. |
| `OTEL_SERVICE_NAME` | per-binary | Overrides the compile-time `service.name`. |

### Browser-only

| Variable | Default | Purpose |
|---|---|---|
| `OTEL_BROWSER_EXPORTER_OTLP_ENDPOINT` | falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` | Where the proxy forwards browser OTLP. |
| `OTEL_BROWSER_EXPORTER_OTLP_HEADERS` | falls back to `OTEL_EXPORTER_OTLP_HEADERS` | Auth for the browser-tier collector. |
| `OTEL_BROWSER_SAMPLE_RATE` | `1.0` | Head-based browser sampling ratio. |

### Local validation

A Docker Compose overlay (`docker-compose.otel.yml`) adds an
`otel/opentelemetry-collector-contrib` container that accepts OTLP on `:4317`
(gRPC) and `:4318` (HTTP) and dumps every signal to both stdout and a file
under `target/otel/`:

```bash
mkdir -p target/otel
docker compose -f docker-compose.yml -f docker-compose.otel.yml up
tail -f target/otel/wnw-otel.log
```

The end-to-end correctness check is that a single user action produces spans
from `wnw-frontend-wasm`, `wnw-frontend-ssr`, `wnw-backend`, and
`wnw-job-worker` that **all share one `trace_id`**.

### Production wiring

Per-environment kustomize overlays carry the config in two ConfigMaps and one
Secret: `shared.env` (endpoint, protocol, resource attrs, sampler),
`shared.secret.env` (the vendor bearer token in `OTEL_EXPORTER_OTLP_HEADERS`),
and per-service `*.env` files setting `OTEL_SERVICE_NAME`. In production the
local collector is usually bypassed and `OTEL_EXPORTER_OTLP_ENDPOINT` points at
Traceway / the vendor intake directly.

---

## 13. Replication checklist for a new repository

To reproduce this strategy elsewhere:

1. **Add the pinned dependency set** from [section 2.1](#21-workspace-dependency-pins)
   to your workspace `Cargo.toml`. Keep the OTel crates on one minor line and
   `tracing-opentelemetry` on its paired line.
2. **Copy the bootstrap crate** (`wnw-observability`): `init.rs` (init +
   exporters + sampler + resource + `inject_context`), `log_bridge.rs`
   (trace-correlated log bridge), and `guard.rs` (RAII shutdown). These are
   self-contained and not application-specific.
3. **Call `init_telemetry("<your-service>", env!("CARGO_PKG_VERSION"))` once**
   at the top of each binary's `main`, binding the guard to a named variable
   that lives for the whole process.
4. **Instrument inbound requests** with `tower_http::trace::TraceLayer` and
   annotate handlers with `#[tracing::instrument(skip_all, name = "...")]`.
5. **Wrap every outbound `reqwest` call** in `inject_context` so traces span
   service boundaries.
6. **Emit metrics** through the global meter with `OnceLock`-cached
   instruments, bounding label cardinality (matched route, not raw URI).
7. **Log with `tracing` macros** --- they automatically become both stderr
   lines and trace-correlated OTLP log records.
8. **Set the standard `OTEL_*` env vars** to point at your collector/Traceway,
   and keep `OTEL_SDK_DISABLED=true` as the local default.
9. **For a browser tier**, port `wnw-observability-wasm` plus the backend
   `/api/telemetry` proxy; keep credentials server-side.

---

## 14. Source map

| Concern | File |
|---|---|
| SDK init, exporters, sampler, resource, `inject_context` | `wnw-observability/src/init.rs` |
| Trace-correlated log bridge | `wnw-observability/src/log_bridge.rs` |
| RAII shutdown guard | `wnw-observability/src/guard.rs` |
| HTTP server metrics middleware | `wnw-backend/src/http_metrics.rs` |
| Job-worker metrics | `wnw-job-worker/src/job_metrics.rs` |
| Browser OTLP pipeline | `wnw-observability-wasm/src/lib.rs`, `.../proto.rs`, `.../tracer.rs` |
| Browser telemetry proxy | `wnw-backend/src/api/telemetry.rs` |
| Instrumented handlers / server fns | `wnw-backend/src/api/*.rs`, `wnw-frontend/src/server_fns/*.rs` |
| Operator-facing env/vendor docs | `docs/observability.md` |
| Local collector overlay | `docker-compose.otel.yml`, `otel-collector/config.yaml` |
| Production config | `kustomize/{prod,preview}/*.env` |
```
