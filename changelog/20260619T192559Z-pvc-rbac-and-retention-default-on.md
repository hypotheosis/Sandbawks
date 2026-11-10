# Helm: grant controller PVC RBAC + enable PVC retention by default

_Date: 2026-06-19_

Chart-only follow-up to epic #603 (filesystem-mode persistence). Two changes so
a `persistenceMode: filesystem` deployment works out of the box.

## Why

- **Missing RBAC (bug).** Phase 2 (#610) added controller-side PVC provisioning
  (`PodAdapter::ensure_pvcs` creates one PVC per `volumeClaimTemplates` entry
  before the Pod), but the chart's `sandbawks-controller` ClusterRole was never
  granted `persistentvolumeclaims`. The core-group rule covered only
  `pods/services/configmaps/secrets/events`, so the first filesystem-mode
  sandbox would fail at PVC creation with a `forbidden` error.
- **Retention opt-in.** Operating filesystem mode without retention leaks paused
  PVCs (they live until the Sandbox is deleted). This deployment opts in.

## What changed (chart only)

- `templates/rbac.yaml`: added a `persistentvolumeclaims` rule
  (`get,list,watch,create,update,patch,delete`) to the controller ClusterRole.
  Cluster-scoped to match the cluster-wide watch — PVCs are created in each
  sandbox's own namespace.
- `values.yaml`: `controller.pvcRetention.enabled: false → true`. Paused
  filesystem sandboxes are now auto-collected after `defaultDuration` (`P7D`) or
  their own `spec.pvcRetention`. **This is a destructive, time-driven deletion
  of paused Sandboxes and their PVC data** — flip back to `false`, or raise
  `defaultDuration`, to reclaim manually. Reclamations are counted by
  `controller.pvc_retention.reclaimed_total` and emit a `PvcRetentionReclaimed`
  Event.
- `Chart.yaml`: `version` 0.1.24 → 0.1.25.

## Validation

`helm lint` + `helm template` — the ClusterRole carries the
`persistentvolumeclaims` rule and `SANDBAWKS_PVC_RETENTION_ENABLED=true` renders
by default. No Rust changed.

## Deploy note

The mode itself is selected on the `SandboxTemplate` CR
(`spec.persistenceMode: filesystem` + `spec.volumeClaimTemplates`), not the
chart. Dynamic PVC provisioning needs a working StorageClass (cluster default or
named per claim). Helm does not manage CRDs — `kubectl apply -f deploy/crds`
must already be in place for the #603 fields.
