# Replace OTel Collector with Vector + Prometheus scrape endpoint (Issue #496)

**Date:** 2026-06-12T10:45:00Z
**Branch:** `feat-issue-496-vector-otlp-prometheus`
**Scope:** Issue #496 **Phase 1 + Phase 2** (of 4). Phases 3–4 deferred.

## Summary

Swaps the in-namespace telemetry hop from the OpenTelemetry Collector contrib
distro (`otel/opentelemetry-collector-contrib:0.123.0`) to **Vector**
(`timberio/vector:0.56.0-debian`), and adds a **Prometheus scrape endpoint** so
metrics that flow to Traceway over OTLP push can also be pulled by a Prometheus
deployment.

The three load-bearing properties of the hop are preserved verbatim:

1. **OTLP receiver ports unchanged** (gRPC `:4317` / HTTP `:4318`) — no client
   wiring changes; the `sandbawks.otelCollector*` helpers and Service names stay
   the same.
2. **Credential isolation** — the Traceway bearer token lives only on the hop
   Pod (mounted from the same Secret into `TRACEWAY_OTLP_TOKEN`), never in the
   app/sandbox processes.
3. **Tenant trust boundary** — `service.namespace` / `sandbawks.tenant` are
   re-stamped from the hop Pod's own downward-API namespace
   (`${SANDBAWKS_NAMESPACE}`) via a Vector `remap` (VRL) transform, the
   equivalent of the old `resource` processor. A customer-controlled `envd`
   cannot spoof another tenant.

## Changes

All under `deploy/helm/sandbawks/`:

### Phase 1 — Vector OTLP passthrough parity

- **`templates/otel-collector-configmap.yaml`** — rewritten from an OTel
  Collector pipeline to a Vector config (`vector.yaml`):
  - `sources.otlp` (`opentelemetry`): OTLP in on `[::]:4317` / `[::]:4318`.
  - `transforms.stamp_{logs,traces,metrics}` (`remap`/VRL): re-stamp tenant
    identity. Logs/traces carry resource attributes under `.resources`; metric
    events carry them as `.tags`, so the stamp is split per signal.
  - `sinks.traceway_{traces,metrics,logs}` (`opentelemetry`): OTLP/HTTP to
    Traceway with `auth.strategy: bearer`, `encoding.codec: otlp`. One sink per
    signal because OTLP/HTTP uses distinct `/v1/{signal}` paths and Vector's
    sink does not auto-append them.
  - `api.enabled: true` on `[::]:8686` to serve the `/health` probe.
  - A **fail-fast guard**: enabling `splitPipelines` or `k8sattributes` (the
    issue #424 OTel-specific scrubbing knobs, not yet ported) aborts the render
    with a Phase-3 pointer rather than silently rendering a single-pipeline
    Vector.
- **`templates/otel-collector-deployment.yaml`** — image → Vector, args
  `--config=/etc/vector/vector.yaml`, health probe → `GET /health` on `:8686`,
  added a writable `data_dir` emptyDir at `/vector-data-dir`.

### Phase 2 — Prometheus scrape endpoint

- **`templates/otel-collector-configmap.yaml`** — `sinks.prometheus`
  (`prometheus_exporter`) on `[::]:9090`, fed from the same tenant-stamped
  metric stream, gated on `observability.collector.prometheus.enabled`.
- **`templates/otel-collector-deployment.yaml`** — `prometheus` container port +
  `prometheus.io/{scrape,port,path}` Pod annotations.
- **`templates/otel-collector-service.yaml`** — `prometheus` Service port.
- **`templates/otel-collector-networkpolicy.yaml`** — health port → `:8686`;
  added a cluster-open ingress rule for the scrape port.
- **`values.yaml`** — repointed `collector.image` to Vector; added
  `collector.prometheus.{enabled,port}`; refreshed the narrative comments.
- **`docs/observability.md`** — Vector swap note, a "Prometheus scrape endpoint"
  section (annotation + `ServiceMonitor` paths), and values-table rows.

### Chart

- **`Chart.yaml`** — `version` 0.1.4 → 0.1.5 (chart-content change).

## Deferred (not in this PR)

- **Phase 3 (#424 parity):** split sandbox/internal pipelines, the sandbox
  `service.name` prefix, k8sattributes-equivalent enrichment, and per-pipeline
  NetworkPolicy rules. Guarded with a fail-fast message until ported.
- **Phase 4:** the kind dev fixture (`deploy/kind/fixtures/otel-collector.yaml`),
  the `just otel-collector-*` / `otel-arrival-smoke` recipes, and the
  kind-smoke workflow still reference the OTel debug-exporter collector.
- The `sandbawks-otel-collector` → `vector` resource/helper **rename** (a churny
  follow-up chore; names kept this pass to minimize blast radius).

## Validation

- `helm template` renders cleanly for: collector off (no resources), collector
  on / prometheus off, collector on / prometheus on (sink, ports, Service port,
  Pod annotations, NetworkPolicy rule all present).
- The Phase-3 guard aborts the render when `splitPipelines` or `k8sattributes`
  is enabled.
- `helm lint` passes.
- **`vector validate`** against the rendered config in the pinned image
  (`timberio/vector:0.56.0-debian`): VRL compiles, topology and all sink types
  validate (`√ Validated`).
- **Runtime boot** of the pinned image with the rendered config: Vector starts,
  healthchecks pass, OTLP source binds, `:8686/health` → `200`,
  `:9090/metrics` → `200`.

**Not validated locally** (needs a cluster + a live Traceway): the end-to-end
arrival check that all four `service.name`s reach Traceway through Vector
(issue #496 Phase 1 acceptance) and a `:9090/metrics` scrape carrying real
series. The Vector metrics OTLP source path is documented as experimental
upstream; the `.tags` stamping path for metric events should be confirmed
against live metric traffic when a cluster is available.
