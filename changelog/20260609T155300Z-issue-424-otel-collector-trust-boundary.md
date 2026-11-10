# OTel Collector Trust-Boundary Hardening (Issue #424)

**Date:** 2026-06-09T15:53:00Z
**Branch:** `feat-issue-424-otel-collector-trust-boundary`
**PR:** follow-up to PR #422 (issue #421)

## Summary

Fully locks down the in-namespace OTel Collector's trust boundary so that
customer-controlled sandbox `envd` cannot pollute internal service dashboards
or SLOs. PR #422 landed a partial mitigation (the `resource` processor that
pins `service.namespace`/`sandbawks.tenant` from the collector's own env).
This change delivers the three structural pieces deferred from that review.

## Changes

### 1. Split sandbox/internal pipelines (`observability.collector.splitPipelines`)

New flag `observability.collector.splitPipelines.enabled` (default `false`).
When enabled:

- **Sandbox pipeline** (gRPC `:4317` / HTTP `:4318`): receives from `envd`.
  Applies a forced `service.name` prefix (configurable via
  `splitPipelines.sandboxServiceNamePrefix`, default `"sandbox/"`) using the
  OTTL `transform/sandbox_service_name` processor. Prevents a sandbox from
  spoofing `service.name=sandbawks-api` and corrupting internal dashboards.
- **Internal pipeline** (gRPC `:4319` / HTTP `:4320`): receives from api,
  controller, and node. Namespace-stamps only; no scrubbing overhead.

The `sandbawks.otelCollectorEndpoint` Helm helper switches to port `4320` when
split pipelines are active; a new `sandbawks.otelCollectorSandboxEndpoint`
helper always resolves to port `4318`. The `sandbawks.observabilityEnv` helper
continues to emit the correct endpoint for internal workloads without changes.

#### Controller wiring

The controller Deployment gains a `SANDBAWKS_OTEL_SANDBOX_ENDPOINT` env var
(only when `splitPipelines.enabled`) pointing at the sandbox receiver (`:4318`).
`pod_adapter::forwarded_observability_env` checks this var and — when present —
emits it as `OTEL_EXPORTER_OTLP_ENDPOINT` for the sandbox Pod instead of
forwarding the controller's own internal endpoint (`:4320`). This keeps envd
routed to the scrubbed pipeline without requiring any change to how the
controller itself exports telemetry.

### 2. `k8sattributes` processor (`observability.collector.k8sattributes`)

New flag `observability.collector.k8sattributes.enabled` (default `false`;
only effective when `splitPipelines.enabled`). When enabled:

- New `otel-collector-rbac.yaml` renders a `ServiceAccount`,
  namespace-scoped `Role` (GET/LIST/WATCH `pods` + `namespaces`), and
  `RoleBinding` for `sandbawks-otel-collector`.
- The collector Deployment gains `spec.serviceAccountName:
  sandbawks-otel-collector` so the processor can reach the Kubernetes API.
- The sandbox pipeline processor chain includes `k8sattributes`, which looks
  up the sending Pod by source IP and stamps `k8s.pod.name`, `k8s.pod.uid`,
  and `k8s.namespace.name` from a trusted source — attributes the sandbox
  cannot forge.

The Role is namespace-scoped (not `ClusterRole`): the collector only needs to
see Pods in its own release namespace.

### 3. NetworkPolicy (`observability.collector.networkPolicy`)

New flag `observability.collector.networkPolicy.enabled` (default `false`).
When enabled, `otel-collector-networkpolicy.yaml` renders a `NetworkPolicy`
targeting the collector Pod(s):

- **Split pipelines on**: `:4317/:4318` admit only sandbox Pods
  (`app.kubernetes.io/name=sandbawks-sandbox`); `:4319/:4320` admit only
  chart-internal Pods (api/controller/node, identified by
  `sandbawks.selectorLabels`).
- **Split pipelines off**: both Pods sets may reach `:4317/:4318`.
- `:13133` (health check) is unrestricted so kubelet probes succeed.

## Affected files

| File | Change |
|------|--------|
| `deploy/helm/sandbawks/values.yaml` | `splitPipelines`, `k8sattributes`, `networkPolicy` flags |
| `deploy/helm/sandbawks/templates/_helpers.tpl` | `otelCollectorEndpoint` port-aware; new `otelCollectorSandboxEndpoint` |
| `deploy/helm/sandbawks/templates/otel-collector-configmap.yaml` | Split receivers, OTTL transform, k8sattributes, split pipelines |
| `deploy/helm/sandbawks/templates/otel-collector-service.yaml` | Internal receiver ports 4319/4320 (conditional) |
| `deploy/helm/sandbawks/templates/otel-collector-deployment.yaml` | serviceAccountName (conditional), internal container ports |
| `deploy/helm/sandbawks/templates/otel-collector-rbac.yaml` | **NEW** — SA + Role + RoleBinding for k8sattributes |
| `deploy/helm/sandbawks/templates/otel-collector-networkpolicy.yaml` | **NEW** — ingress allowlist per pipeline |
| `deploy/helm/sandbawks/templates/controller-deployment.yaml` | `SANDBAWKS_OTEL_SANDBOX_ENDPOINT` env var (conditional) |
| `crates/sandbawks-controller/src/pod_adapter.rs` | `SANDBOX_OTEL_ENDPOINT_OVERRIDE_ENV` logic + 2 new unit tests |

## Validation

- `just fmt` — passed
- `cargo clippy -p sandbawks-controller -- -D warnings` — passed (full
  `--all-targets` blocked by missing `protoc` in the sandbox build env; the
  envd proto build failure is pre-existing and unrelated to these changes)
- `cargo test -p sandbawks-controller` — all tests pass, including two new
  unit tests for the sandbox endpoint override logic

## Backward compatibility

All three features are **disabled by default** (`false`). A cluster running
with no values overrides sees identical runtime behavior to PR #422. Operators
enable incrementally: `splitPipelines` first, then `k8sattributes` (needs RBAC
consent), then `networkPolicy`.
