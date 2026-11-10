# Observability (operator guide)

sandbawks emits **logs, metrics, and traces** over OTLP (OpenTelemetry). This
guide is for operators running the chart: how to turn export on, where the
data goes, and how to validate end to end. For the *design* rationale see
[`OBSERVABILITY.md`](../OBSERVABILITY.md) (full strategy) and
[`TRACING.md`](../TRACING.md) (the traces-only distillation) at the repo root.

## What is instrumented

Four services emit telemetry, and a single request correlates across all of
them under one `trace_id`:

| Service | Workload | `service.name` |
|---|---|---|
| API gateway | `Deployment/sandbawks-api` | `sandbawks-api` |
| Controller | `Deployment/sandbawks-controller` | `sandbawks-controller` |
| Node agent | `DaemonSet/sandbawks-node` | `sandbawks-node` |
| In-sandbox agent | `envd` (inside each sandbox Pod) | `sandbawks-envd` |

The api, controller, and node binaries stamp their own `service.name` and
`service.version` at compile time. `envd`'s compile-time fallback is bare
`envd`, but the controller injects `OTEL_SERVICE_NAME=sandbawks-envd` on every
sandbox Pod (issue #435), so its telemetry — and the inner agent's — lands
under `sandbawks-envd`. Every binary derives a stable `service.instance.id`
from the injected `POD_NAME`.
You configure the *transport* (endpoint, protocol, auth, sampling); the
service identity is automatic.

The api, controller, and node workloads are wired by the chart's
`observability` values block (below). `envd` runs inside customer sandbox Pods
and is configured by the controller, not these values.

## Enabling and disabling

Export is **disabled by default**. With it off, the chart sets
`OTEL_SDK_DISABLED=true` on every workload, so each binary short-circuits to
plain stderr logging and never attempts an export — no collector required, no
warning floods about an unreachable `localhost:4318`.

To enable, point the workloads at a **plaintext** OTLP target:

```bash
helm upgrade --install sandbawks deploy/helm/sandbawks \
  --set observability.enabled=true \
  --set observability.endpoint=http://otel-collector.observability.svc:4318
```

> **Plaintext only (Phase 1).** The binaries are built without an OTLP TLS
> feature (enabling reqwest's rustls collides with kube's `ring`
> CryptoProvider and panics at startup). So `observability.endpoint` MUST be a
> plaintext `http://` (or non-TLS `grpc://`) target — an in-cluster collector
> that terminates TLS and forwards to your vendor, or a vendor's in-cluster
> plaintext intake. A TLS Traceway host is not reachable directly yet.

## Values reference

All keys live under `observability:` in
[`values.yaml`](../deploy/helm/sandbawks/values.yaml). They map onto the
standard `OTEL_*` env vars (so any vendor's example drops in unchanged), via
the shared `sandbawks.observabilityEnv` helper in `templates/_helpers.tpl`.

| Value | Env var | Default | Purpose |
|---|---|---|---|
| `observability.enabled` | (gates the block) | `false` | Master switch. `false` sets `OTEL_SDK_DISABLED=true` everywhere. |
| `observability.endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` | `""` | Collector/intake base URL. **Required** when enabled. For `http/protobuf` the SDK appends `/v1/{traces,metrics,logs}`; for `grpc` it uses the endpoint as-is. |
| `observability.protocol` | `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | `http/protobuf` or `grpc`. |
| `observability.sampler` | `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | Sampling strategy. |
| `observability.samplerArg` | `OTEL_TRACES_SAMPLER_ARG` | `"1.0"` | Ratio for the `*_traceidratio` strategies. |
| `observability.resourceAttributes` | `OTEL_RESOURCE_ATTRIBUTES` | `""` | Extra `key=value,key=value` resource tags (e.g. `deployment.environment=prod`). |
| `observability.timeoutMs` | `OTEL_EXPORTER_OTLP_TIMEOUT` | `""` (SDK ~10s) | Per-export timeout, milliseconds. Keep generous — the first export races kube-client startup. |
| `observability.existingHeadersSecret` | `OTEL_EXPORTER_OTLP_HEADERS` (via Secret) | `""` | Name of a Secret with an `otlp-headers` key. **Preferred** for auth tokens. |
| `observability.headers` | `OTEL_EXPORTER_OTLP_HEADERS` (inline) | `""` | Inline `key=value` headers. Lands in the rendered manifest and Helm history — not for real secrets. |
| `observability.collector.enabled` | (gates the collector) | `false` | Deploy a chart-managed in-namespace Vector hop (#496) and default every workload to it. |
| `observability.collector.exporter.endpoint` | (collector `otlphttp`) | `""` | Downstream APM base URL the collector forwards to (e.g. `http://traceway.traceway.svc.cluster.local/api/otel`). **Required** when the collector is enabled. |
| `observability.collector.exporter.existingTokenSecret` | (collector `TRACEWAY_OTLP_TOKEN`) | `""` | Name of a Secret with a `token` key. **Preferred** — the token never enters values.yaml. |
| `observability.collector.exporter.token` | (collector `TRACEWAY_OTLP_TOKEN`) | `""` | Inline bearer token. Dev only; lands in the release. Required when the collector is enabled and no `existingTokenSecret` is set. |
| `observability.collector.prometheus.enabled` | (Vector `prometheus_exporter` sink) | `false` | Re-expose received metrics on a Prometheus scrape endpoint (#496). See [Prometheus scrape endpoint](#prometheus-scrape-endpoint-496). |
| `observability.collector.prometheus.port` | (scrape `/metrics` port) | `9090` | Listen port for the scrape endpoint; exposed on the Pod, Service, and NetworkPolicy ingress. |

### Pointing at Traceway

The recommended topology keeps the vendor token off the application — and is
the **only** topology that works for `envd`, which the controller deliberately
never hands the credential (so pointing it straight at Traceway's authenticated
intake yields a permanent `401`, surfaced as `HTTP export failed: network
error`):

```
sandbawks-{api,controller,node}, envd
        │  plaintext OTLP
        ▼
  in-namespace Vector hop  ──(bearer token)──▶  Traceway
```

> The in-namespace hop runs **Vector** (`timberio/vector`) as of #496; it
> replaced the OpenTelemetry Collector contrib distro. The receiver ports
> (:4317/:4318), the token Secret contract, and the credential-isolation
> property are unchanged, so the values below and the wiring are identical. The
> chart-managed resources keep their `sandbawks-otel-collector` names this pass
> (a rename is a follow-up chore). Vector can additionally re-expose the received
> metrics on a Prometheus scrape endpoint — see
> [Prometheus scrape endpoint](#prometheus-scrape-endpoint-496) below.

**Recommended: let the chart run the collector (issue #421).** Set
`observability.collector.enabled=true`, leave `observability.endpoint` blank
(every workload — including each sandbox's envd — defaults to
`http://sandbawks-otel-collector.<namespace>.svc:4318`), and give the collector
the downstream APM endpoint plus token. One collector per release namespace, so
separate installs can target separate APMs. Supply the token as a Secret with a
`token` key, never inline:

```bash
kubectl create secret generic traceway-otlp \
  --from-literal=token='<token>'
helm upgrade --install sandbawks deploy/helm/sandbawks \
  --set observability.enabled=true \
  --set observability.collector.enabled=true \
  --set observability.collector.exporter.endpoint=http://traceway.traceway.svc.cluster.local/api/otel \
  --set observability.collector.exporter.existingTokenSecret=traceway-otlp
```

> **Rollout ordering:** the collector (and its token Secret) must be up before
> the workloads are repointed at it, or they'll briefly fail to export. A single
> `helm upgrade` renders both, but on a cluster with slow image pulls the app
> Pods may restart first — harmless (exports retry), but expect a few
> export-retry warnings until the collector is ready.

**Escape hatch: export straight to a plaintext Traceway intake.** Leave the
collector disabled, set `observability.endpoint` to the intake, and attach the
token yourself via `existingHeadersSecret` (a Secret with an `otlp-headers`
key). Note this does **not** cover envd — only the api/controller/node
workloads get the header — so sandbox telemetry still won't arrive. Use the
collector for full coverage.

### Prometheus scrape endpoint (#496)

Metrics flow to Traceway over OTLP push by default. When you also run a
Prometheus, enable the Vector `prometheus_exporter` sink so Prometheus can
**pull** the same metric series the hop receives:

```bash
helm upgrade --install sandbawks deploy/helm/sandbawks \
  --set observability.enabled=true \
  --set observability.collector.enabled=true \
  --set observability.collector.exporter.endpoint=http://traceway.traceway.svc.cluster.local/api/otel \
  --set observability.collector.exporter.existingTokenSecret=traceway-otlp \
  --set observability.collector.prometheus.enabled=true
```

This adds:

- a `prometheus` port (`:9090` by default — `observability.collector.prometheus.port`)
  on the `sandbawks-otel-collector` Service, serving the standard `/metrics` path;
- `prometheus.io/scrape` / `prometheus.io/port` / `prometheus.io/path` annotations
  on the hop Pod, for annotation-based scrape discovery;
- a NetworkPolicy ingress rule opening that port (when
  `observability.collector.networkPolicy.enabled`) — left cluster-open so a
  Prometheus in another namespace can reach it; tighten with a
  `namespaceSelector` for your monitoring namespace if scrapers are co-located.

The exposed series carry the same tenant identity that feeds the OTLP push
(re-stamped from the collector Pod's namespace), so the values match. Note two
Prometheus-side quirks:

- **Resource attributes appear as `resource_`-prefixed labels.** The Vector
  `opentelemetry` source flattens OTLP resource attributes onto metric tags with
  a `resource.` prefix, so the tenant stamp originates as
  `resource.service.namespace` / `resource.sandbawks.tenant`. Prometheus naming
  rules forbid `.` in metric and label names, so the hop sanitizes every metric
  and label name to underscores (`.`->`_`) before the `prometheus_exporter` —
  the tenant stamp therefore lands as the labels `resource_service_namespace`
  and `resource_sandbawks_tenant` on every scraper, UTF-8-enabled or classic.
  (The OTLP push to Traceway keeps the dotted attribute names, which are valid
  in OTLP.)
- **Series expire between scrapes.** `prometheus_exporter` only exposes series it
  has seen within its flush window (~60s by default). A metric that arrives only
  intermittently over OTLP can disappear from `/metrics` between emissions, so a
  scrape is not guaranteed to show every series the OTLP push forwarded. Scrape
  at or below the emission cadence for sparse metrics.

**prometheus-operator users:** point a `ServiceMonitor` at the `prometheus`
Service port instead of relying on the Pod annotations:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: sandbawks-telemetry-hop
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: sandbawks
      app.kubernetes.io/component: otel-collector
  endpoints:
    - port: prometheus
      path: /metrics
```

Standing up Prometheus itself is out of scope for the chart — this only exposes
the scrape surface.

## Local validation (the single-`trace_id` check)

A kind fixture stands up a throwaway collector that dumps every received
signal to its own stdout — enough to confirm correlation without a full APM.

1. Bring up the kind cluster (gVisor mode, so pause/resume exercises every
   service):

   ```bash
   SANDBAWKS_KIND_GVISOR=1 just cluster-up
   ```

2. Stand up the collector and install the chart pointing at it:

   ```bash
   just otel-collector-up
   helm upgrade --install sandbawks deploy/helm/sandbawks \
     --set observability.enabled=true \
     --set observability.endpoint=http://otel-collector.observability.svc:4318
   ```

3. Drive a workload and watch the spans arrive:

   ```bash
   just otel-collector-logs        # follow the debug exporter (one terminal)
   just smoke                      # or: just pause-resume-smoke
   ```

   The collector prints each span with its `Trace ID`, `service.name`, and
   resource attributes. **The end-to-end check:** a single sandbox action
   produces spans from `sandbawks-api`, `sandbawks-controller`,
   `sandbawks-node`, and `sandbawks-envd` that all carry **one `trace_id`**, alongside
   the corresponding metrics and trace-correlated log records.

   The narrower **arrival** check — at least one received signal carries the
   expected `service.name` for each of the four binaries — is automated:

   ```bash
   just otel-arrival-smoke
   ```

   It applies the collector fixture itself and polls the debug-exporter
   stdout until every expected `service.name` has landed (or fails listing the
   ones it did see). CI runs it as the `OTel arrival smoke (issue #311)` step
   of the `kind-smoke` job, after the pause/resume smoke has driven every
   service, so a regression that stops telemetry reaching the collector turns
   the build red. Narrow the assertion to a lighter workload with
   `OTEL_SMOKE_EXPECTED_SERVICES="sandbawks-api sandbawks-controller"`.

4. Tear the collector down when finished:

   ```bash
   just otel-collector-down
   ```

The fixture manifest is
[`deploy/kind/fixtures/otel-collector.yaml`](../deploy/kind/fixtures/otel-collector.yaml).
It is a dev aid only — single replica, no persistence, and a debug exporter
that prints payloads in the clear.

### Cross-checking via Traceway

When exporting to a real Traceway instance, you can confirm the same
correlation through the Traceway MCP tools instead of collector stdout:
`list_endpoint_traces` to find the trace, `get_endpoint_trace` to inspect the
span tree across the four services, and `query_metrics` for the emitted
metrics.

## When no traces arrive

Export failures are **silent** — the OTLP batch processor logs and moves on, a
wrong endpoint produces silence rather than a crash. `TRACING.md` enumerates
the [seven failure modes](../TRACING.md#why-this-works-here-when-it-often-doesnt)
that produce "compiles and logs but emits no traces"; walk them in order. For
deployment-side issues specifically, check, in order:

1. `observability.enabled=true` actually took effect — `kubectl exec` into a
   workload and confirm `OTEL_EXPORTER_OTLP_ENDPOINT` is set and
   `OTEL_SDK_DISABLED` is **not**.
2. The endpoint is reachable and plaintext — `kubectl run` a debug pod and
   `curl -v <endpoint>/v1/traces`.
3. The collector is actually receiving — `just otel-collector-logs` shows the
   receiver startup lines and, once traffic flows, the signal dump.

## Instrumenting new code (developer guide)

This section is for contributors *adding* spans, not operators. The goal is a
**complete trace waterfall**: every function that does real work on an
operation's path opens a span, so a Task/Endpoint trace shows the full nested
call tree of what actually happened (issue #371).

### The span hierarchy

Each request/operation has exactly one **root** span (created by #370) and a
tree of **sub-operation** spans beneath it:

| Layer | Root span | Where |
|---|---|---|
| API request | `http.request` (`otel.kind=server`, HTTP semconv) | `sandbawks-api` `middleware.rs` `ExtractingMakeSpan` |
| Controller reconcile | `reconcile:controller::{sandbox,warm_pool}` (`otel.kind=consumer`) | `reconcile.rs`, `warm_pool.rs` |
| Node SnapshotJob | `reconcile:node::snapshot_job` (`otel.kind=consumer`) | node `reconcile.rs` |
| envd RPC | `handler:{process,filesystem}::*`, `handler:envd::state` | `envd` `process.rs`, `filesystem.rs`, `bin/envd.rs` |

Everything below a root is a sub-operation span you add as you write code.

### Metrics that mirror operational spans (#373)

Many of the operational spans above also have a **metric** counterpart — a span
per op is cheap, but a metric series per op is not, so this is a *curated* set
(the ops whose rate/latency an operator actually watches), not a 1:1 mirror of
every span. Each lives in a per-concern `*_metrics.rs` module following the same
pattern as `reconcile_metrics.rs` (cached `OnceLock` instruments off
`global::meter(...)`, free `record_*` functions, no-op-meter-safe under
`OTEL_SDK_DISABLED`). The labels are the same low-cardinality `&'static str`
sets the spans chose.

| Instrument | Type | Unit | Attributes | Mirrors span | Module |
|---|---|---|---|---|---|
| `controller.dispatch.action_total` | `Counter<u64>` | --- | action | `dispatch:controller::action` | controller `dispatch_metrics.rs` |
| `controller.dispatch.duration` | `Histogram<f64>` | s | action | `dispatch:controller::action` | controller `dispatch_metrics.rs` |
| `controller.pod_op.duration` | `Histogram<f64>` | s | op | `pod:controller::{create,delete,ready,terminated}` | controller `pod_op_metrics.rs` |
| `controller.pod_op.outcome_total` | `Counter<u64>` | --- | op, outcome | `pod:controller::{create,delete,ready,terminated}` | controller `pod_op_metrics.rs` |
| `node.snapshot.phase.duration` | `Histogram<f64>` | s | phase | pause/resume/`fetch_verify_and_lay_out` `PhaseTimer` | node `snapshot_phase_metrics.rs` |
| `node.validate.outcome_total` | `Counter<u64>` | --- | level, outcome | `validate:node::restorable` | node `validate_metrics.rs` |
| `api.kube_op.duration` | `Histogram<f64>` | s | resource, op | `kube:api::*` (Sandbox ops + warm-claim) | api `kube_op_metrics.rs` |
| `api.kube_op.outcome_total` | `Counter<u64>` | --- | resource, op, outcome | `kube:api::*` (Sandbox ops + warm-claim) | api `kube_op_metrics.rs` |

These are **distinct layers** from the request-level metrics that already exist
(`http.server.*`, `warm.claims_total`): `api.kube_op.*` counts individual
apiserver round-trips, while `http.server.*` counts inbound requests and
`warm.claims_total` counts whole claim orchestrations — one inbound request can
make several kube calls, so they do not double-count. Operations already metered
elsewhere (SnapshotJob duration/claims, envd digest/advertised, S3 transfer
counters, reconcile/warm-pool) are deliberately **not** re-metered here.

When you add a metric for a new op, prefer extending the relevant
`*_metrics.rs` and reuse the span's existing `label()`/op string so the metric
and span share one low-cardinality vocabulary.

### The recipe

Wrap an operational function with `#[tracing::instrument]`:

```rust
#[tracing::instrument(
    skip_all,                              // never auto-capture args (payloads!)
    name = "pod:controller::create",       // <layer>:<module>::<op>
    err,                                   // fallible? surface failures as Issues
    fields(sandbox = %sandbox.name_any(), restore = restore.is_some()),
)]
async fn create_pod(&self, sandbox: &Sandbox, /* … */) -> Result<PodRef, RuntimeError> { … }
```

- **`skip_all` is mandatory.** The default `#[instrument]` records *every*
  argument via `Debug` — that leaks request payloads, file contents, and
  high-cardinality structs into trace attributes. Always `skip_all`, then add
  back only the low-cardinality fields you want via `fields(...)`.
- **Naming: `<layer>:<module>::<op>`.** Match the established style —
  `reconcile:controller::sandbox`, `probe:node::fetch_state`,
  `handler:files::upload`, `pod:controller::create`, `pause:node::snapshot`,
  `kube:api::sandbox_get`. The `<layer>` is a verb or role (reconcile, probe,
  handler, pod, store, pool, upload, download, resolve, verify, dispatch); the
  `<module>` is the crate's short name (api, controller, node).
- **`err` on fallible operational functions** so a failure records an exception
  event on the span and renders as an **Issue** in the APM (#370). Mind retry
  noise: a function polled in a tight requeue loop under a sustained outage can
  emit a storm of error events — if it proves chatty, drop `err` and log
  terminally instead, or gate the level.
- **Levels vs. the export filter.** INFO spans export by default. The OTel
  export layer (`build_otel_layer_filter`, `crates/sandbawks-observability/src/init.rs`)
  starts from `RUST_LOG`/`info` and only quiets noisy transport libs. Use
  `level = "debug"` on fine-grained spans you don't want in production traces.

### Field cardinality — the cardinal rule

Span fields must be **low-cardinality**. High-cardinality or sensitive values
blow up backend storage, slow queries, and can leak data.

| Record (low-cardinality) | Never record (high-cardinality / sensitive) |
|---|---|
| object names (sandbox, pool, job, pod), namespaces | full request/response payloads, file contents |
| phases, modes, enum *labels* (`action.label()`) | per-file paths inside a loop; per-chunk identifiers |
| booleans (`restore`, `found`, `terminated`) | UUIDs/resource versions as free-form fields |
| sizes & counts (`bytes`, `chunk_count`) | the `RecordFailure(msg)` string / operator text |
| addresses already bounded per-sandbox (`pod_ip`) | secrets, tokens, env values, PII |

For enums, add a `label()`/`as_str()` returning `&'static str` rather than
`Debug`-rendering a variant that carries a payload (see `Action::label` in
`state_machine.rs`).

### Loops and bulk transfers — span the whole, not the parts

**Never open a span per chunk/file/row in a tight loop.** The node S3 transfer
moves thousands of 1-MiB chunks at GiB scale; a span per chunk would multiply
export volume by ~1000× and add real latency. Instead span the *whole transfer*
(`upload:node::part`, `stream_download:node::part`) and record `chunk_count` /
`bytes` as fields. Keep the existing `PhaseTimer` breakdowns as a single
structured field/event on the enclosing span, not as nested spans.

### Cross-service trace continuity

Traces stay connected across process boundaries through context injection — a
new span must be **current** when the injection happens:

- **api → envd**: the gRPC interceptor (`TraceparentInjector`, `envd_client.rs`,
  #298) injects the *current* span into outbound request metadata; envd's
  inbound `GrpcTraceLayer` (`envd/src/trace.rs`) extracts it. So any span that
  is current when the RPC fires propagates automatically — no extra wiring.
- **controller → node**: `build_snapshot_job` calls
  `inject_into_annotations`, stamping the current reconcile context onto the
  `SnapshotJob`; the node reconcile continues from that annotation. The inject
  is a no-op outside a span, so it is always safe to call.

When you add a span around code that crosses one of these seams, confirm it is
current at the injection point (it will be, if the injecting call is inside the
instrumented function's body).

### envd and checkpoint/restore (#356)

envd's request handlers and `/__state` are already instrumented. Per-request
OTel work is non-blocking, but the background OTLP exporter can wedge across a
`runsc` memory restore until #356 re-arms the SDK — so envd spans emitted
*immediately after a resume* may not export. This is a known limitation, not a
reason to skip instrumenting new envd code.

### Instrumentation checklist

Before merging code that adds or changes an operational function:

- [ ] Every function that does real work on the operation's path opens a span.
- [ ] `#[tracing::instrument(skip_all, …)]` — `skip_all` is present.
- [ ] Span name follows `<layer>:<module>::<op>`.
- [ ] Fields are low-cardinality only; no payloads, per-loop paths, or secrets.
- [ ] `err` on fallible operational functions (unless it proves retry-chatty).
- [ ] No span-per-chunk/row in any tight loop; bulk transfers spanned whole.
- [ ] Cross-service spans are current at the inject point (api→envd, controller→node).
- [ ] Validate the waterfall: open the Task/Endpoint in the APM and confirm the
      expected nested spans appear end-to-end (see *Local validation* above).
