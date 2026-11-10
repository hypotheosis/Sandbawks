# Issue #592 — Template `tolerations` / `nodeSelector` for sandbox Pod scheduling

**Date:** 2026-06-18T21:51:33Z
**Branch:** `feat-issue-592-template-scheduling-hints`

## Summary

`SandboxTemplate` previously offered no way to influence sandbox Pod
scheduling beyond the runsc-version node affinity the controller builds
internally. Sandbox Pods therefore could not land on tainted nodes (e.g. a
dedicated Kata/Azure node pool), because there was no surface to express
`tolerations`, and no way to pin placement to a labeled pool via
`nodeSelector`. This change adds both as optional template-level fields that
every minted sandbox — cold-start *and* warm-pool — inherits.

## Changes

- **`crates/sandbawks-types/src/sandbox.rs`**
  - New serde/schemars mirror type `Toleration` (mirrors
    `k8s_openapi::api::core::v1::Toleration`; we mirror rather than reuse it
    because the CRD derives `schemars::JsonSchema`, which the `k8s-openapi`
    types do not implement under this workspace's features). Wire shape matches
    upstream exactly, including camelCase `tolerationSeconds`.
  - `SandboxSpec` gains optional `tolerations: Option<Vec<Toleration>>` and
    `node_selector: Option<BTreeMap<String, String>>`
    (`#[serde(default, skip_serializing_if = "Option::is_none")]`), so a
    hand-applied Sandbox CR can set them directly and they stay non-required /
    omitted-when-`None` in the schema.
- **`crates/sandbawks-types/src/sandbox_template.rs`**
  - `SandboxTemplateSpec` gains the same two optional fields, following the
    `networkPolicy` precedent.
- **`crates/sandbawks-types/src/lib.rs`** — re-export `Toleration`.
- **`crates/sandbawks-controller/src/reconcile.rs`** — `resolve_pod_inputs`
  now fills the Sandbox's `tolerations` / `nodeSelector` from the template
  (alongside the existing `defaultRuntimeClass` resolution) whenever the CR
  leaves them unset. An explicit value on the CR always wins. Resolving at this
  single pod-creation edge means cold-start and warm-pool members both inherit
  without per-mint plumbing. The owned clone is taken once if *any* of the three
  defaults applies, and the template's hints are moved (not cloned) onto it.
- **`crates/sandbawks-controller/src/pod_adapter.rs`** — `pod_spec_for`
  translates the CRD-side `Toleration` mirror into the `k8s_openapi` type and
  stamps `tolerations` / `node_selector` onto the `PodSpec`. These *compose*
  with the existing runsc-version node affinity rather than replacing it (out of
  scope per the issue). `None` leaves both Pod fields unset — byte-identical to
  the pre-#592 PodSpec.
- **`deploy/crds/sandboxtemplates.sandbawks.dev.yaml`** and
  **`deploy/crds/sandboxes.sandbawks.dev.yaml`** — regenerated via
  `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`. Both new fields
  are optional (absent from the `required` set).
- All existing `SandboxSpec` / `SandboxTemplateSpec` struct literals across the
  workspace updated with the two new fields (defaulting to `None`).

## Tests added

- `crates/sandbawks-types/tests/sandbox_template.rs`
  - `scheduling_hints_are_optional_in_crd_schema` — fields present in the schema
    but not required.
  - `scheduling_hints_roundtrip_and_omit_when_none` — YAML round-trip under the
    camelCase wire shape (incl. `tolerationSeconds`); omitted when `None`.
- `crates/sandbawks-controller/src/pod_adapter.rs`
  - `pod_carries_template_scheduling_hints` — Pod carries the values when set,
    leaves both unset by default, and the node affinity still composes.
- `crates/sandbawks-controller/tests/reconcile.rs`
  - `create_pod_inherits_template_scheduling_hints` — controller folds template
    hints onto the Pod's Sandbox at `Action::CreatePod`.
  - `create_pod_explicit_scheduling_hints_override_template` — explicit CR
    values win over template hints.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace suite passes.

## Out of scope (unchanged)

- Pod affinity/anti-affinity controls.
- The controller's existing runsc-version node affinity (composed with, not
  replaced).
- Cluster-side provisioning of the tainted node pool.

## Deploy note

The CRD manifests changed. Helm does **not** apply CRDs, so on deploy run
`kubectl apply -f deploy/crds` before/independently of the chart, or the new
optional fields will be silently pruned from applied templates.
