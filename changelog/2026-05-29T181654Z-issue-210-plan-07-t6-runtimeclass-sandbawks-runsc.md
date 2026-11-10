# Plan 07 T6 — Helm RuntimeClass: sandbawks-runsc

- **Issue:** #210 (epic #204, Plan 07 Task 6)
- **Date:** 2026-05-29T18:16:54Z
- **Area:** `deploy/helm/sandbawks/` — chart RuntimeClass templates

## Summary

The chart now renders a cluster-scoped `RuntimeClass: sandbawks-runsc`
(handler `sandbawks-runsc`) gated behind `.Values.sandbawksRunsc.enabled`
(default `true`). This is the Kubernetes-facing counterpart to the
`sandbawks-runsc` containerd handler baked into the kind-node image in
Plan 07 T5 (#209): the RuntimeClass is what a Pod's `runtimeClassName`
binds to so the kubelet routes snapshot/restore sandboxes through the
`containerd-shim-sandbawks-v1` shim. It is purely additive to the existing
`gvisor` RuntimeClass from Plan 05.

## Changes

- `deploy/helm/sandbawks/templates/runtimeclass-sandbawks-runsc.yaml` (new)
  - `node.k8s.io/v1` RuntimeClass named `sandbawks-runsc`, `handler:
    sandbawks-runsc`, wrapped in `{{- if .Values.sandbawksRunsc.enabled }}`.
    Mirrors `runtimeclass-gvisor.yaml` exactly (same `sandbawks.labels`
    helper, same guard idiom) so the two stay consistent.
- `deploy/helm/sandbawks/values.yaml`
  - New `sandbawksRunsc.enabled: true` block, placed directly after the
    `gvisor` block to match the issue's "additive to the existing gvisor
    RuntimeClass" framing.

## Verification

- `helm lint deploy/helm/sandbawks` → clean (only the pre-existing
  "icon is recommended" INFO).
- `helm template test deploy/helm/sandbawks --set sandbawksRunsc.enabled=true
  --show-only templates/runtimeclass-sandbawks-runsc.yaml` → renders the
  RuntimeClass with `handler: sandbawks-runsc`.
- `--set sandbawksRunsc.enabled=false` → template produces no output (the
  `--show-only` "could not find template" error is the expected signal that
  the guard suppressed the resource).
- Workspace gate (unaffected by this YAML-only change, run for safety):
  `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` all
  pass.
