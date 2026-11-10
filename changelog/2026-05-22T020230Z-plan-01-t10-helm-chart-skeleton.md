# 2026-05-22 — Plan 01 Task 10: Helm chart skeleton

Closes #11.

## What landed

`deploy/helm/sandbawks/` — a complete Helm v2 chart that scaffolds every
workload the controller will eventually need. Workload containers run
with `args: ["--placeholder"]` until later plans land the real binaries;
the chart itself lints, templates, and is install-shaped today.

Files created:

- `Chart.yaml` — apiVersion v2, name `sandbawks`, version `0.1.0`.
- `values.yaml` — image refs (api / controller / node), replica counts,
  resource requests/limits, snapshot S3 config (blank endpoint by
  default so a fresh install does not need a real bucket), and an
  `installCrds` toggle.
- `templates/_helpers.tpl` — `sandbawks.labels` and
  `sandbawks.selectorLabels` partials applied to every resource.
- `templates/serviceaccount.yaml` — three ServiceAccounts:
  `sandbawks-controller`, `sandbawks-api`, `sandbawks-node`.
- `templates/rbac.yaml` — three ClusterRoles + three ClusterRoleBindings
  matching spec §9 verb sets: controller gets full CRUD on all four
  sandbawks CRDs plus status subresources for `sandboxes` and
  `sandboxwarmpools`, plus core resources (pods, services, configmaps,
  secrets, events) and `networking.k8s.io/networkpolicies`; api gets
  CRUD on `sandboxes`, `sandboxtemplates`, `teamapikeys`; node gets
  read-only on `sandboxes` plus status patch and read access to pods
  and nodes.
- `templates/controller-deployment.yaml` — single-replica Deployment
  for the controller (leader-elected; can scale for HA later).
- `templates/api-deployment.yaml` — two-replica Deployment with an
  `http` containerPort on 8080.
- `templates/api-service.yaml` — ClusterIP Service fronting the API.
- `templates/node-daemonset.yaml` — privileged, `hostPID: true`
  DaemonSet with hostPath mounts for the containerd socket
  (`/run/containerd/containerd.sock`) and snapshot cache
  (`/var/lib/sandbawks/snapshots`, value-driven via
  `node.hostPathSnapshotCache`).
- `crds/` — copied verbatim from `deploy/crds/` so the CI drift-check
  passes. Helm installs files under `crds/` before any templates, and
  they must be flat YAML (no templating).

## Validation

- `helm lint deploy/helm/sandbawks` — `1 chart(s) linted, 0 chart(s)
  failed`. Only an `[INFO]` about an optional `icon` field, which the
  spec does not require.
- `helm template test deploy/helm/sandbawks` — renders cleanly. Kind
  inventory: 3 ServiceAccount, 3 ClusterRole, 3 ClusterRoleBinding,
  2 Deployment (api + controller), 1 Service, 1 DaemonSet. With
  `--include-crds`, four CustomResourceDefinitions are also emitted.
- `diff -r deploy/crds deploy/helm/sandbawks/crds` — identical, as
  required by the future drift-check job.
- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — full workspace test suite passes (20 tests across
  `crdgen`, `resources`, `sandbox`, `sandbox_template`, `smoke`,
  `team_api_key`, `warm_pool`).

## Why this shape

Keeping workload containers on a `--placeholder` flag rather than
omitting them entirely means CI (and operators) can render the chart
today and catch RBAC/template errors before binaries exist. The
`installCrds: true` value is a toggle, not a templated conditional on
the CRDs themselves — Helm's convention is that `crds/` files are
always installed when present, so the toggle is reserved for a future
`templates/`-rendered alternative if ArgoCD-style external CRD sync
becomes a requirement.

## What's next

Task 11 — workspace-wide green build verification, then Task 12 — CI
drift-check that re-runs `crdgen` and diffs `deploy/crds/` and
`deploy/helm/sandbawks/crds/`.
