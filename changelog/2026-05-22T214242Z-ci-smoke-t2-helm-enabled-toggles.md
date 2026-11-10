# CI smoke T2 — Helm per-component `enabled` toggles

Date: 2026-05-22T21:42:42Z
Issue: #68
Parent epic: #66

## Summary

Added an `enabled` toggle to each of the `api`, `controller`, and `node`
blocks in `deploy/helm/sandbawks/values.yaml` (all default `true`, so the
default install behavior is unchanged) and gated the matching templates
behind those toggles. This lets the upcoming kind-based smoke test (T7)
install the chart with only the controller active — the api and node
crates don't exist yet (api lands in Plan 04, node in Plan 05), so the
smoke test will pass `--set api.enabled=false --set node.enabled=false`.

## Files touched

- `deploy/helm/sandbawks/values.yaml` — three new `enabled: true` keys
  with explanatory comments naming the gating plans.
- `deploy/helm/sandbawks/templates/api-deployment.yaml` — wrapped in
  `{{- if .Values.api.enabled }} … {{- end }}`.
- `deploy/helm/sandbawks/templates/api-service.yaml` — same.
- `deploy/helm/sandbawks/templates/controller-deployment.yaml` — gated on
  `.Values.controller.enabled`.
- `deploy/helm/sandbawks/templates/node-daemonset.yaml` — gated on
  `.Values.node.enabled`.
- `deploy/helm/sandbawks/templates/rbac.yaml` — restructured so each
  per-component `ClusterRole` + `ClusterRoleBinding` pair is gated on its
  component's `enabled` flag. There is no shared role to keep
  unconditional — every rule in this file is component-specific, and a
  binding without a running pod is dead weight at best and a stale
  permission grant at worst.

## Files intentionally left unconditional

- `deploy/helm/sandbawks/templates/serviceaccount.yaml` — per the issue's
  guidance, all three ServiceAccounts (`sandbawks-api`,
  `sandbawks-controller`, `sandbawks-node`) are always created. They're
  cheap, namespaced, and harmless when no pod consumes them; keeping
  them always-on means re-enabling a component later doesn't require
  recreating an SA-bound secret/token.

## Validation

```
$ helm lint deploy/helm/sandbawks
1 chart(s) linted, 0 chart(s) failed

$ helm template t deploy/helm/sandbawks | grep -c 'kind: Deployment'
2

$ helm template t deploy/helm/sandbawks \
    --set api.enabled=false --set node.enabled=false \
  | grep -c 'kind: Deployment'
1

$ helm template t deploy/helm/sandbawks \
    --set api.enabled=false --set node.enabled=false \
  | grep -c 'kind: DaemonSet'
0
```

Rendered kind counts with `api.enabled=false node.enabled=false`:

| Kind                | Count |
|---------------------|-------|
| ClusterRole         | 1     |
| ClusterRoleBinding  | 1     |
| Deployment          | 1     |
| DaemonSet           | 0     |
| Service             | 0     |
| ServiceAccount      | 3     |

Defensive Rust gate (`just fmt`, `just clippy`, `just test`) is green —
no Rust files were touched, but the workspace still builds and all tests
pass.

## Out of scope

- Adding new components or renaming `images.X.repository` keys. This is
  purely a refactor of the existing template surface.
- The smoke test itself (that's T7, gated by this and T3).
