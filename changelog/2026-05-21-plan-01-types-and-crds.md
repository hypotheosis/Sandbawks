# 2026-05-21 — Plan 01: Types crate + CRDs + Helm scaffold

## What landed

- Converted root crate to a Cargo workspace.
- New crate `crates/sandbawks-types`: shared CRD types, error types, resource quantities.
- Four CRDs: `Sandbox`, `SandboxTemplate`, `SandboxWarmPool`, `TeamAPIKey` (group `sandbawks.io/v1alpha1`).
- `crdgen` binary emits CRD YAML to `deploy/crds/`.
- Helm chart scaffold at `deploy/helm/sandbawks/` with placeholder workloads, full RBAC, CRD install.
- CI: fmt + clippy + test + doc, CRD-regen drift check, helm lint.

## What's deliberately not here

- Any runtime behavior. No controller reconcile loop, no API handlers, no node DaemonSet logic.
  Plans 02+ land each.

## Next plan

`docs/superpowers/plans/2026-05-21-sandbawks-02-controller-skeleton.md` —
`sandbawks-controller` with the Sandbox state machine driving a fake
`RuntimeAdapter` (no real pause/resume yet).
