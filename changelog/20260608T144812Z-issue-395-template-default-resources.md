# Sandbox Pods honor `SandboxTemplate.spec.defaultResources` (issue #395)

- **Date:** 2026-06-08
- **Issue:** #395 (warm-pool / cold-boot Pods ignored the template's
  configured CPU / memory)
- **Crates touched:** `sandbawks-controller`, `sandbawks-api`

## Problem

`SandboxTemplate.spec.defaultResources` (CPU / memory / ephemeral-storage)
was **dead config**: it was accepted on the CRD but no code path ever read
it. Both paths that mint a `Sandbox` hardcoded a placeholder budget:

- the gateway cold-boot path (`body_to_sandbox` in
  `sandbawks-api/src/routes/sandboxes.rs`), and
- the warm-pool minting path (`warm_sandbox` in
  `sandbawks-controller/src/warm_pool.rs`)

both stamped `cpu: "1"`, `memory: "1Gi"` onto `Sandbox.spec.resources`.
`PodAdapter::pod_spec_for` faithfully renders `Sandbox.spec.resources` into
the Pod's container requests/limits — so a template configured for `2` CPU /
`2048Mi` still produced a `1` CPU / `1Gi` Pod, exactly the symptom reported:

```
SandboxTemplate.spec.defaultResources: cpu 2, memory 2048Mi
actual Pod:                            cpu 1, memory 1Gi
```

Stale comments in both files claimed "the controller overrides from the
template on the `Action::CreatePod` edge" — but that override was never
implemented.

## Fix

Resolve the template's `defaultResources` onto `Sandbox.spec.resources` at
**mint time**, in the two paths that create sandboxes — mirroring exactly how
each path already resolves the template's `defaultRuntimeClass` (issue #289).
The hardcoded `1` CPU / `1Gi` budget survives only as a fallback for a
template-lookup miss, so a momentarily-missing template never wedges a pool.

- **Gateway (`create_sandbox`)** — inside the existing `Ok(template)` arm
  that already lifts `defaultRuntimeClass`, also set
  `sandbox.spec.resources = template.spec.default_resources`. This sizes the
  cold-boot Pod *and* makes the create-response DTO (`cpuCount` / `memoryMB` /
  `diskSizeMB`) report the real budget. The E2B create body carries no
  resource fields, so there is no caller override to preserve; a template
  miss keeps the `body_to_sandbox` placeholder.
- **Warm pool** — `WarmSandboxDefaults` gains a
  `resources: Option<ResourceRequests>` field, populated by
  `WarmSandboxDefaults::from_template` from `template.spec.default_resources`.
  `warm_sandbox` uses it, falling back to a new `placeholder_resources()`
  helper only when the template lookup missed (`None`). The
  `KubeWarmPoolStore::template_defaults` miss path already returns
  `WarmSandboxDefaults::default()` (now `resources: None`), so the fallback is
  exercised automatically.

`Sandbox.spec.resources` stays a required field, so a hand-applied `Sandbox`
CR must specify its own resources (there is nothing to backstop), and no CRD
schema change is required — `defaultResources` was already on the template
CRD.

## Tests

- `pod_adapter::pod_resources_mirror_sandbox_spec` — proves the downstream
  half: `Sandbox.spec.resources` (including `ephemeral_storage`) maps to the
  Pod container's `requests` and `limits`. Regression guard for the reported
  `2`-CPU-template-yields-`1`-CPU-Pod bug.
- `warm_pool::warm_sandbox_carries_pool_label_and_owner_ref` — extended to
  assert the member is sized from the template's `defaultResources`
  (`2` / `2048Mi`), not the placeholder.
- `warm_pool::warm_sandbox_falls_back_to_placeholder_resources_on_template_miss`
  — new: a `resources: None` default yields the `1` / `1Gi` placeholder.
- `warm_pool::reconcile_stamps_runtime_class_on_created_warm_sandboxes` —
  extended to assert every reconcile-created member carries the resolved
  template resources.

The gateway's template-resolution arm (both runtime-class and resources) runs
only against a real apiserver — the API unit tests' mock kube client returns
`204`, falling into the lenient `Err` arm — so that arm is covered by the kind
integration smoke, consistent with the existing `defaultRuntimeClass`
resolution.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — full workspace suite green
