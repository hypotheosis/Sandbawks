# API Ingress struts (disabled by default)

**Date:** 2026-06-02T01:00:00Z

## Summary

Added the scaffolding for exposing `sandbawks-api` externally via a
Kubernetes `Ingress`. The chart now ships a templated
`templates/api-ingress.yaml` plus an `api.ingress` values block, both
**disabled by default**. No behavior changes for existing installs: with
`api.ingress.enabled=false` (the default) nothing renders, so the api
stays in-cluster behind its `ClusterIP` Service.

## Motivation

Before this change the chart had no way to reach the api gateway from
outside the cluster — the only template touching exposure was
`api-service.yaml`, which renders a `ClusterIP` Service. Operators who
front the api with an Ingress controller had to hand-author the Ingress
out-of-band. This adds the standard, opt-in knobs.

Kept off by default on purpose: most clusters route ingress differently
(nginx/Traefik controller, Gateway API, service mesh), and externally
exposing an authenticated gateway is an explicit operator decision, not a
surprising default of `helm install`. LoadBalancer service-type was
explicitly out of scope for this pass.

## Changes

| File | Change |
| --- | --- |
| `deploy/helm/sandbawks/templates/api-ingress.yaml` | New. Renders `networking.k8s.io/v1` `Ingress` named `sandbawks-api` when both `api.enabled` and `api.ingress.enabled` are true. Passes `className`, `annotations`, and `tls` through, and routes each `hosts[].paths[]` to the `sandbawks-api` Service on `api.service.port`. |
| `deploy/helm/sandbawks/values.yaml` | New `api.ingress` block: `enabled: false`, `className: ""`, `annotations: {}`, an example `hosts` entry (`sandbawks.example.com`, path `/`, `Prefix`), and `tls: []`. |

The `Ingress` reuses the shared `sandbawks.labels` helper and the
existing `app.kubernetes.io/component: api` Service selector indirectly
(it targets the Service by name, not by selector). The render is gated on
`and .Values.api.enabled .Values.api.ingress.enabled` so it stays
suppressed whenever the api Deployment/Service is toggled off.

## Verification

- `helm lint deploy/helm/sandbawks` — 0 failed.
- Default render (`helm template`) emits **0** `kind: Ingress` objects.
- `--set api.ingress.enabled=true --set api.ingress.className=nginx`
  renders a valid single-host Ingress backed by `sandbawks-api:8080`.
- `--set api.enabled=false --set api.ingress.enabled=true` emits **0**
  Ingress objects (the `api.enabled` guard holds).

## Out of scope (intentionally deferred)

- `LoadBalancer` service-type path — not requested.
- TLS / cert-manager issuer wiring beyond a pass-through `tls: []` block.
- Gateway API (`HTTPRoute`) variant.
