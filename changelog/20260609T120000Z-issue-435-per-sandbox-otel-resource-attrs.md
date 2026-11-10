# feat(controller): inject per-sandbox OTel resource attributes for envd + inner agent traces

**Date:** 2026-06-09T12:00:00Z  
**Issue:** #435  
**Branch:** feat-issue-435-per-sandbox-otel-resource-attrs

## Summary

The controller now stamps per-sandbox OTel resource attributes on every sandbox
Pod env, so envd spans and the inner Claude Code agent spans (when its trace beta
is enabled) are traceable back to the specific sandbox CR — template, namespace,
uid, and pod name.

## Changes

### `crates/sandbawks-controller/src/pod_adapter.rs`

- **Removed `OTEL_RESOURCE_ATTRIBUTES` from `FORWARDED_OTEL_VARS`** — the var was
  forwarded verbatim (same value for every sandbox). It is now owned by
  `per_sandbox_observability_env`, which reads the cluster-level base and appends
  per-sandbox identity before injecting.

- **Added `OTEL_SERVICE_NAME_ENV`, `OTEL_RESOURCE_ATTRIBUTES_ENV`, `ENVD_SERVICE_NAME`**
  constants.

- **New `per_sandbox_observability_env(sandbox, lookup)` function** — sibling to
  `forwarded_observability_env`. Builds and merges two env vars:
  - `OTEL_RESOURCE_ATTRIBUTES` — cluster-level base + per-sandbox attrs:
    `k8s.pod.name`, `k8s.namespace.name` (OTel k8s semconv) and
    `sandbox.template`, `sandbox.uid` (Sandbawks custom). Per-sandbox attrs
    appended last so they win on key collision (OTel spec: last wins).
  - `OTEL_SERVICE_NAME=sandbawks-envd` — injected when the operator has not
    already pinned it. Makes envd and inner agent spans distinguishable from
    the controller's service in Traceway.

- **Updated `pod_spec_for`** — calls `per_sandbox_observability_env` after
  `forwarded_observability_env`, respecting the customer-keys filter so
  `spec.env` pins still win.

- **8 new unit tests** covering: attrs built from CR, uid omitted when absent,
  merge-not-clobber, last-wins key precedence, service-name default and
  operator-override, and guards that neither `OTEL_RESOURCE_ATTRIBUTES` nor
  `OTEL_SERVICE_NAME` are in `FORWARDED_OTEL_VARS`.

### `TRACING.md`

Added "Per-sandbox resource attributes (issue #435)" section documenting the
attribute table, merge/precedence rules, verification path, and security posture.

## Validation

- `just fmt` ✅
- `cargo clippy -p sandbawks-controller -- -D warnings` ✅
- `cargo test -p sandbawks-controller` ✅ (54 unit tests + integration tests pass)

## Attribute reference

| `OTEL_RESOURCE_ATTRIBUTES` key | Value source | Convention |
|---|---|---|
| `k8s.pod.name` | `metadata.name` | OTel k8s semconv |
| `k8s.namespace.name` | `metadata.namespace` | OTel k8s semconv |
| `sandbox.template` | `spec.templateRef` | Sandbawks custom |
| `sandbox.uid` | `metadata.uid` (omitted if absent) | Sandbawks custom |

## Security

No credentials injected. `OTEL_EXPORTER_OTLP_HEADERS` remains excluded. All
injected values are identity metadata (pod name, namespace, template name, CR
uid). Consistent with #427 / #295.
