# Plan 04 T4 — `spec.desiredPhase` + `RuntimeAdapter` pod-mgmt extension

**Date:** 2026-05-24T01:53:55Z
**Branch:** `feat-issue-103-pod-mgmt-extension`
**Issue:** #103
**Plan reference:** `docs/superpowers/plans/2026-05-23-sandbawks-04-api-gateway.md` § Task 4

## Summary

Lands the controller-side plumbing the API gateway needs from Plan 04.
The Sandbox CR grows a typed `spec.desiredPhase` so the gateway can
`kubectl patch` desired state directly (the legacy
`sandbawks.io/desired-phase` annotation reader stays one plan for back-
compat and is removed in Plan 05). `RuntimeAdapter` gains three new
default-implemented methods — `create_pod`, `delete_pod`, `pod_ready`
— so existing test fakes keep compiling unchanged while T5's
`PodAdapter` can override them with real kube calls. The reconcile
loop now drives `Action::CreatePod` on `(Creating, PodCreate)`, calls
`adapter.delete_pod` on `Terminate`, and surfaces `status.envdAddress`
from the adapter on the `Creating → Running` edge.

## Changes

### `crates/sandbawks-types`

- `src/sandbox.rs`:
  - Added `SandboxSpec.desired_phase: Option<SandboxPhase>` with
    `#[serde(skip_serializing_if = "Option::is_none", default)]`. The
    typed field takes precedence over the legacy annotation.
  - Added `SandboxStatus.pod_uid: Option<String>` for the Pod's kube
    `metadata.uid` (populated by T5's Pod watcher).
- `src/runtime.rs`:
  - New `PodRef { name, namespace }` value type (the handle
    `RuntimeAdapter::create_pod` returns and `delete_pod`/`pod_ready`
    consume).
  - `RuntimeAdapter` extended with default-implemented `create_pod`,
    `delete_pod`, `pod_ready`. Defaults return a synthetic `PodRef` and
    `Some("127.0.0.1:49983")` respectively so test fakes keep driving
    the existing state-machine tests without overriding anything.
- `src/lib.rs`: re-export `PodRef` alongside the existing runtime
  symbols.

### `crates/sandbawks-controller`

- `src/state_machine.rs`:
  - New `Trigger::PodCreate` and `Action::CreatePod`.
  - New edge `(Creating, PodCreate) -> (Creating, Action::CreatePod)` —
    intentionally a self-loop on `Creating` because the side effect
    (materializing a Pod) runs while the Sandbox stays in `Creating`
    until `Trigger::PodReady` advances it.
- `src/reconcile.rs`:
  - `derive_trigger` reads `spec.desired_phase` first and only falls
    back to the legacy annotation. Pod-readiness is detected from
    `status.envdAddress` being `Some` *or* the legacy `pod-ready`
    annotation. `(Creating, _, false)` now yields `PodCreate`;
    `(Creating, _, true)` keeps yielding `PodReady`.
  - `dispatch` handles `Action::CreatePod`: reads
    `$SANDBAWKS_DEFAULT_ENVD_IMAGE` (default
    `ghcr.io/hypotheosis/envd:latest`), calls `adapter.create_pod(sandbox,
    &envd_image)`, and stamps `status.podRef = "{namespace}/{name}"`.
    Phase stays `Creating` (the state machine returns `(Creating,
    CreatePod)`).
  - `dispatch`'s `Action::Noop` branch now consults
    `adapter.pod_ready(&pod_ref)` when `status.podRef` is set and
    surfaces the returned address on `status.envdAddress`. Gated on
    `pod_ref` being set, so the existing annotation-driven tests
    (which never set `pod_ref`) are unaffected.
  - `dispatch`'s `Action::Terminate` now calls
    `adapter.delete_pod(&pod_ref)` when `status.podRef` is set, so the
    reconcile loop tears down materialized Pods.
  - New private helper `parse_pod_ref("ns/name") -> Option<PodRef>`.

### Tests

- `crates/sandbawks-controller/tests/state_machine_unit.rs`: added
  `creating_pod_create_triggers_create_pod_action`.
- `crates/sandbawks-controller/tests/state_machine_properties.rs`:
  - Added `Trigger::PodCreate` to the trigger strategy so the
    totality / determinism properties cover the new edge.
  - Relaxed `no_self_loops` to allow the deliberate
    `(Creating, PodCreate) -> Creating` self-loop while still rejecting
    every other self-loop in the graph.
- Backfilled `desired_phase: None` in every `SandboxSpec { ... }` test
  fixture (3 test files + 1 store unit test) so the new required-by-
  initializer-syntax field is set without changing test intent.

### Generated artifacts

- Re-ran `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`.
  Only `deploy/crds/sandboxes.sandbawks.io.yaml` diffs:
  - Added `spec.desiredPhase` (enum-typed, nullable).
  - Added `status.podUid` (nullable string).
  - No fields removed. Other three CRD YAMLs are byte-identical.

## Validation

- `just fmt` ✅
- `cargo clippy --all-targets -- -D warnings` ✅ (clean, no warnings)
- `just test` ✅ (all 16 state-machine unit tests + 6 properties + the
  full reconcile/lifecycle integration suite green)
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` ✅
  (deterministic — second run produces no further diff)

## Follow-ups (out of scope here, tracked in Plan 04)

- T5 ships `PodAdapter` which overrides the three new default-
  implemented methods with real kube `POST /pods` / `DELETE /pods` /
  Pod watcher calls.
- Plan 05 removes the legacy `sandbawks.io/desired-phase` and
  `sandbawks.io/pod-ready` annotation readers.
