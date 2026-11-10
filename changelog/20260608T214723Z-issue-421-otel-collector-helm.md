# Issue #421: per-namespace OTel Collector in the Helm chart so envd telemetry reaches Traceway

**Date:** 2026-06-08T21:47:23Z
**Issue:** #421
**PR:** feat-issue-421-otel-collector
**Related:** #294/#295 (observability wiring), #369 (cluster-wide token fix), #311 (otel-arrival-smoke)

## Summary

Sandbox `envd` pods never reported telemetry: they logged `PeriodicReader.ExportFailed`
/ `BatchSpanProcessor.Flush.ExportError` ("HTTP export failed: network error")
forever. Root cause (confirmed on `sbx-warm-lggx4`) is **auth, not connectivity** —
`POST /api/otel/v1/metrics` returns `401` without a bearer token and `200` with one.
The controller deliberately strips `OTEL_EXPORTER_OTLP_HEADERS` from customer-controlled
sandbox Pods (`pod_adapter.rs` `FORWARDED_OTEL_VARS`, test
`never_forwards_otlp_headers_credential`), so envd has no credential — yet the deployment
points it straight at Traceway's authenticated intake.

This change ships the topology the chart already recommends: a chart-managed
`otel/opentelemetry-collector-contrib` Deployment + Service, **one per release
namespace**, that receives plaintext OTLP from every workload (api/controller/node AND
each sandbox's envd) and forwards to the downstream APM with a bearer token held only on
the collector. Repointing the workloads' endpoint at the collector propagates to envd
automatically via the controller's existing env-forwarding — **no Rust change**.

## Changes

### New Helm templates (`deploy/helm/sandbawks/templates/`)

All gated on `observability.enabled && observability.collector.enabled`; stable name
`sandbawks-otel-collector`; mirror existing `sandbawks.labels`/`selectorLabels` and the
`existingSecret` secret convention.

- **`otel-collector-configmap.yaml`** — pipeline: `otlp` receivers (gRPC `[::]:4317` /
  HTTP `[::]:4318`, IPv6/IPv4 dual-stack for the IPv6-primary prod cluster) →
  `[memory_limiter, resource, batch]` → `otlphttp` exporter (`endpoint` = APM base URL,
  `Authorization: "Bearer ${env:TRACEWAY_OTLP_TOKEN}"`, `retry_on_failure` +
  `sending_queue`) across traces/metrics/logs. The `resource` processor upserts
  `service.namespace`/`sandbawks.tenant` from `${env:SANDBAWKS_NAMESPACE}` (downward API)
  to re-establish the tenant trust boundary against customer-controlled envd; a
  `health_check` extension on `[::]:13133` backs the Deployment's readiness/liveness
  probes. Token is env-interpolated, never in the ConfigMap.
- **`otel-collector-secret.yaml`** — chart-managed Secret holding the raw token under key
  `token`; rendered only when the collector is enabled and no `existingTokenSecret` is
  pinned. `required` errors if the token is missing.
- **`otel-collector-deployment.yaml`** — configurable replicas, pinned contrib image via
  the dockerio proxy, `TRACEWAY_OTLP_TOKEN` from the Secret via `secretKeyRef`,
  `SANDBAWKS_NAMESPACE` via the downward API, readiness/liveness probes against the
  `health_check` port, resource limits, and `checksum/config` + (inline-token-only)
  `checksum/secret` pod annotations so config or token edits roll the Pod.

> **Review round (PR #422):** added the `resource` tenant-stamping processor,
> `health_check` extension + probes, `checksum/secret` for inline-token rotation, an
> explicit `type: ClusterIP`, and namespace-reachability docs in response to review.
> The deeper trust-boundary work (split sandbox/internal pipelines, `service.name`
> allowlist, `k8sattributes` + dedicated SA/RBAC, NetworkPolicy) is deferred to a
> follow-up issue.
- **`otel-collector-service.yaml`** — ClusterIP exposing :4317/:4318.

### `deploy/helm/sandbawks/templates/_helpers.tpl`

- New helpers: `sandbawks.otelCollectorName`, `sandbawks.otelCollectorEndpoint`
  (`http://sandbawks-otel-collector.<namespace>.svc:4318`), and
  `sandbawks.otelCollectorTokenSecretName` (existing-vs-managed, like
  `sandbawks.signingSecretName`).
- `sandbawks.observabilityEnv`: when `collector.enabled` and `observability.endpoint` is
  blank, `OTEL_EXPORTER_OTLP_ENDPOINT` defaults to the collector endpoint. This single
  change redirects api/controller/node AND (via `FORWARDED_OTEL_VARS`) envd.

### `deploy/helm/sandbawks/values.yaml`

- New `observability.collector` block: `enabled` (default `false`), `image`, `replicas`,
  `logLevel`, `resources`, and `exporter: { endpoint, existingTokenSecret, token }`, with
  documentation that `headers`/`existingHeadersSecret` must stay blank when the collector
  is on.

### `docs/observability.md`

- Values-reference table extended with the four `observability.collector.*` keys.
- "Pointing at Traceway" rewritten to lead with the chart-shipped collector (the only
  topology that covers envd), with a rollout-ordering note and the direct-export escape
  hatch demoted.

### Explicitly unchanged

- `crates/sandbawks-controller/src/pod_adapter.rs` — its credential-stripping design is
  what makes the plaintext-collector topology correct.
- `deploy/kind/fixtures/otel-collector.yaml` — stays the dev debug-exporter aid.

## Validation

- `helm lint deploy/helm/sandbawks` — passed.
- `helm template` with the collector enabled — renders cleanly; token appears only in the
  Secret and the collector Pod env (`secretKeyRef`), never in the ConfigMap; workloads'
  `OTEL_EXPORTER_OTLP_ENDPOINT` resolves to the collector Service.
- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — passed (no Rust
  changed; run as the workspace gate).
- End-to-end cluster verification (envd export 401 → 200 via the collector, Traceway
  arrival for `service.name=envd`) is in the PR's manual test plan; not run in this
  changeset.
