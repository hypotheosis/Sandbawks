# OTel Phase 7 — deployment config, operator docs, e2e collector fixture

**Issue:** #301 (parent epic #294 — OpenTelemetry observability over OTLP)
**Date:** 2026-06-03T21:05:01Z

## Summary

Closes the OpenTelemetry epic's deployment story. Phases 1–6 instrumented the
four services (api, controller, node, envd) and made them consume the standard
`OTEL_*` env vars; Phase 7 wires those vars through the Helm chart for **all**
workloads, adds a local OTLP collector fixture for end-to-end validation, and
documents the system for operators.

## What changed

### Helm: OTEL_* threaded through every workload (DRY)

Previously only `Deployment/sandbawks-api` carried the observability env
block; `controller-deployment.yaml` and `node-daemonset.yaml` set **no**
`OTEL_*` vars at all. On a default install that left the controller and node
binaries attempting OTLP export to the SDK default `localhost:4318` and
flooding warnings — a latent bug.

- Extracted the env block into a single named template,
  `sandbawks.observabilityEnv`, in
  `deploy/helm/sandbawks/templates/_helpers.tpl`. It emits the full `OTEL_*`
  set + `POD_NAME` when `observability.enabled`, or `OTEL_SDK_DISABLED=true`
  otherwise.
- Replaced the inline block in `api-deployment.yaml` with an `include`, and
  added the same `include` to `controller-deployment.yaml` and
  `node-daemonset.yaml`. One source of truth across three workloads.
- Updated the `observability` doc comment in `values.yaml`: it now describes
  cluster-wide scope (api + controller + node; envd is controller-configured)
  rather than "the api gateway".

No new values keys — the existing `observability.*` block (endpoint, protocol,
sampler, resourceAttributes, timeoutMs, headers/existingHeadersSecret) now
governs all three workloads.

### Local OTLP collector fixture

- `deploy/kind/fixtures/otel-collector.yaml`: an `observability` namespace
  running `otel/opentelemetry-collector-contrib:0.123.0` (pinned, pulled via
  a Docker Hub pull-through proxy). Accepts OTLP on :4317 (gRPC) / :4318
  (HTTP) and dumps every signal to stdout via the `debug` exporter at
  `verbosity: detailed`. The collector's own telemetry log level is left at
  the `info` default on purpose — the debug exporter writes through that same
  logger, so lowering it to `warn` would silence the dump.
- `justfile` recipes `otel-collector-up`, `otel-collector-logs`,
  `otel-collector-down`.

### Operator docs

- `docs/observability.md`: enabling/disabling via the chart, a values→env
  reference table, the Traceway collector-hop topology (token as a Secret,
  never inline), the local validation walkthrough (single-`trace_id` check),
  the Traceway MCP cross-check, and a deployment-side "no traces arrive"
  checklist that points at `TRACING.md`'s seven failure modes.

## Validation

- `helm template` renders cleanly:
  - default (disabled): `OTEL_SDK_DISABLED` on all three workloads, no
    `OTEL_EXPORTER_*`.
  - enabled: `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_RESOURCE_ATTRIBUTES`, and
    `POD_NAME` on all three; no `OTEL_SDK_DISABLED`.
  - enabled without `endpoint` fails fast with the `required` error.
- Collector fixture exercised against the live kind cluster: applied,
  rolled out, and a synthetic OTLP/HTTP trace POST returned HTTP 200 with the
  `debug` exporter printing the span's `Trace ID` and `service.name` to
  stdout.
- Standard gates: `just fmt`, `cargo clippy --all-targets -- -D warnings`,
  `just test`.

## Notes

- The `kustomize/` overlay holds only CR fixtures (SandboxTemplate,
  TeamAPIKey); it does not deploy the workloads, so there are no workload
  `OTEL_*` env vars to thread there — Helm owns that surface.
- No Rust changes: the binaries already consumed the standard `OTEL_*` vars
  (Phases 1–6). Phase 7 is deployment + docs only.
