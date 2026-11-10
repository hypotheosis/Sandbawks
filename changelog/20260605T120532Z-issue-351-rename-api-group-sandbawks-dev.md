# Rename Kubernetes API group `sandbawks.io` → `sandbawks.dev`

Issue #351. The project domain is `sandbawks.dev` (we could not acquire
`sandbawks.io`). Every `sandbawks.io` string in the live tree was a
Kubernetes API-group identifier or a well-known label/annotation prefix
-- never a real domain (no URLs, ingress hosts, emails, or registries
referenced it). All such identifiers are now `sandbawks.dev`.

## What changed

- **CRD API group** `sandbawks.io` → `sandbawks.dev` on all five kinds
  (`Sandbox`, `SandboxTemplate`, `SandboxWarmPool`, `TeamAPIKey`,
  `SnapshotJob`). `apiVersion` is now `sandbawks.dev/v1alpha1`.
- **Generated CRD files** renamed and regenerated:
  `deploy/crds/<plural>.sandbawks.io.yaml` →
  `deploy/crds/<plural>.sandbawks.dev.yaml` (via
  `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`).
- **Label / annotation keys** re-prefixed `sandbawks.io/` →
  `sandbawks.dev/`: `sandbox`, `desired-phase`, `pod-ready`,
  `envd-staged`, `warm-pool`, `claimed-at`, `snapshot-retention`,
  `traceparent`, `tracestate`. Source-of-truth constants updated in
  `sandbawks-types` (`labels.rs`, `warm_pool.rs`), `sandbawks-controller`
  (`reconcile.rs`, `retention.rs`, `pod_adapter.rs`), `sandbawks-node`
  (`gvisor.rs`), and `sandbawks-observability` (`propagation.rs`).
- **Manifests / fixtures** updated: Helm RBAC `apiGroups`, node-daemonset
  and `values.yaml` comments, `deploy/kind/fixtures/*`, `kustomize/*`, and
  the `.forgejo/workflows/kind-smoke.yaml` smoke workflow.
- **Tests** asserting the literals updated in lockstep.
- **Current docs** updated: `docs/getting-started.md`,
  `docs/kind-smoke.md`, `OBSERVABILITY.md`.

## Migration (breaking)

This is a hard rename with no dual-group / alias shim. Custom resources
created under the old `sandbawks.io` group are invisible to controllers
watching `sandbawks.dev`. A cluster already running sandbawks must:

1. Apply the regenerated CRDs (`kubectl apply -f deploy/crds`), and
2. Re-create any `Sandbox` / `SandboxTemplate` / `SandboxWarmPool` /
   `TeamAPIKey` / `SnapshotJob` resources under the new group.

In-flight state on the old group is not preserved. This is acceptable for
current pre-production deployments.

## Out of scope (intentionally unchanged)

Historical records keep `sandbawks.io` as a point-in-time record of what
was true when written: all `changelog/` entries and the
`docs/superpowers/plans/` + `docs/superpowers/specs/` documents.
