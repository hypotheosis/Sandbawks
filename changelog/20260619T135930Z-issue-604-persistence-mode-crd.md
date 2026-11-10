# Issue #604 — Filesystem persistence CRD: `persistenceMode` + `volumeClaimTemplates`

**Date:** 2026-06-19T13:59:30Z
**Branch:** `sandbawks/sandbawks-604`
**Parent epic:** #603 — PVC-backed filesystem persistence + runtime-agnostic
"filesystem" suspend mode.

## Summary

Phase 1 (the CRD foundation) of epic #603. Introduces a `persistenceMode`
selector (`Snapshot` default | `Filesystem`) and a `volumeClaimTemplates` list
on both `SandboxTemplate` and `Sandbox`, with template→sandbox inheritance
resolved at the controller's single pod-creation edge. No controller behavior
changes yet — this is the API surface and inheritance plumbing only. The
default `Snapshot` reproduces the pre-#603 behavior exactly, so existing
templates and CRs are unaffected.

## Changes

- **`crates/sandbawks-types/src/sandbox.rs`**
  - New `PersistenceMode` enum (`#[default] Snapshot | Filesystem`), serialized
    PascalCase to match the phase/state-machine wire convention.
  - New serde/schemars mirror types `VolumeClaimTemplate`,
    `VolumeClaimTemplateMetadata` (`name`/`labels`/`annotations`), and
    `VolumeClaimSpec` (`storage`/`accessModes`/`storageClassName`/`volumeMode`).
    We mirror the relevant subset of
    `k8s_openapi::api::core::v1::PersistentVolumeClaimSpec` rather than embed it,
    for the same reason as the existing `Toleration` / `NetworkPolicy` mirrors:
    the CRD derives `schemars::JsonSchema`, which the `k8s-openapi` types do not
    implement under this workspace's features (`k8s-openapi v0.23`, `v1_31`
    only, no `schemars` feature). `VolumeClaimSpec.storage` reuses the existing
    `ResourceQuantity` newtype so Kubernetes quantity strings round-trip without
    precision loss, and is required (no `Default` — an empty size is
    meaningless).
  - `SandboxSpec` gains optional `persistence_mode: Option<PersistenceMode>` and
    `volume_claim_templates: Option<Vec<VolumeClaimTemplate>>`
    (`#[serde(default, skip_serializing_if = "Option::is_none")]`).
- **`crates/sandbawks-types/src/sandbox_template.rs`**
  - `SandboxTemplateSpec` gains the same two optional fields, following the
    established optional-field pattern (`network_policy` / `tolerations`).
- **`crates/sandbawks-types/src/lib.rs`**
  - Re-exports `PersistenceMode`, `VolumeClaimTemplate`,
    `VolumeClaimTemplateMetadata`, `VolumeClaimSpec`.
- **`crates/sandbawks-controller/src/reconcile.rs`**
  - `resolve_pod_inputs()` extends the empty-slot inheritance to the two new
    fields alongside `tolerations` / `node_selector`: `None` on the CR inherits
    the template default; an explicit value — including an explicit empty
    `volumeClaimTemplates` list — wins (the `is_none()` absence test, not
    emptiness). Both the cold-start (`Action::CreatePod`) and resume
    (`Action::CreateRestorePod`) edges pass through here, so both inherit.
- **`deploy/crds/sandboxes.sandbawks.dev.yaml`,
  `deploy/crds/sandboxtemplates.sandbawks.dev.yaml`**
  - Regenerated via `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`.
    New fields are non-required; nested `volumeClaimTemplates[].spec.storage`
    and `metadata`/`spec` are required.
- **Mechanical:** every `SandboxSpec` / `SandboxTemplateSpec` struct literal
  across the workspace (tests, builders, warm-pool/store paths) gains the two
  new fields at `None`, keeping existing behavior unchanged.

## Tests

- `crates/sandbawks-types/tests/sandbox.rs`: `PersistenceMode` defaults to
  `Snapshot`; serializes PascalCase; the new optional fields are omitted from
  YAML when `None`; full camelCase round-trip with `volumeClaimTemplates`
  populated.
- `crates/sandbawks-types/tests/sandbox_template.rs`: the new fields are
  optional in the generated CRD schema yet present as properties (mirrors
  `scheduling_hints_are_optional_in_crd_schema`).
- `crates/sandbawks-controller/tests/reconcile.rs`: `resolve_pod_inputs` fills
  empty `persistenceMode` / `volumeClaimTemplates` from the template; an
  explicit (even empty) value on the CR suppresses the template default. The
  recording adapter was extended to capture both resolved fields.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all pass (7 new tests added).

## Deploy note

Helm skips CRDs. After deploy, run `kubectl apply -f deploy/crds` or the new
optional fields are silently pruned from the stored schema.

## Out of scope (later phases)

- PVC object creation / Pod mounting — #603 Phase 2 (#605).
- Pause/resume behavior branching on the resolved mode — #603 Phase 3 (#606).
