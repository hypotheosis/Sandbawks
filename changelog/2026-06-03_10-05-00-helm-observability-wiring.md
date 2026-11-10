# Helm chart: OTLP/observability wiring for the api gateway

**Date:** 2026-06-03 10:05:00
**Branch:** `feat-issue-295-otel-bootstrap-crate`
**Issue:** #294/#295 (OTel observability)

PR #303 wired the OTel bootstrap crate into the `sandbawks-api` binary but the
Helm chart had no way to configure it. This adds the deployment-side plumbing
so an operator can point the gateway at an OTLP target (a collector, or an
in-cluster plaintext Traceway intake) via Helm values.

## Changes

- **`values.yaml`** — new `observability` section (`enabled` default `false`):
  `endpoint`, `protocol`, `sampler` / `samplerArg`, `resourceAttributes`,
  `timeoutMs`, and OTLP auth headers via either `existingHeadersSecret`
  (a Secret with an `otlp-headers` key) or an inline `headers` string.
- **`templates/api-deployment.yaml`** — when `observability.enabled`, render the
  spec-standard `OTEL_*` env vars and `POD_NAME` (downward API → stable
  `service.instance.id`). When disabled, render `OTEL_SDK_DISABLED=true` so the
  binary short-circuits to fmt-only logging instead of silently retrying the
  default `localhost:4318` and flooding warnings.

## Constraints baked into the docs

- **Plaintext-only OTLP (Phase 1).** The binary ships without an OTLP TLS
  feature (reqwest-rustls collides with kube's `ring` CryptoProvider and panics
  `Client::try_default()`). `endpoint` must therefore be a plaintext target.
  The recommended topology is app → in-cluster OTel Collector (plaintext) →
  Traceway (TLS + token on the collector). A TLS Traceway ingress host is not
  reachable directly until #294 Phases 4-7.
- `observability.endpoint` is `required` when `enabled=true` (helm fails fast).

## Validation

- `helm lint` ✅ (only the pre-existing "icon recommended" info).
- `helm template` rendered for both paths: disabled → `OTEL_SDK_DISABLED=true`;
  enabled → full `OTEL_*` block + `POD_NAME` downward-API ref.
- `required` guard confirmed: `enabled=true` with empty `endpoint` fails the
  render with a clear message.

## Example

```bash
helm upgrade --install sandbawks deploy/helm/sandbawks \
  --namespace sandbawks --create-namespace \
  --set observability.enabled=true \
  --set observability.endpoint=http://otel-collector.observability.svc:4318 \
  --set observability.resourceAttributes='deployment.environment=prod\,service.namespace=sandbawks'
```
