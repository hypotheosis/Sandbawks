# Plan 05 T5: PodAdapter honors `Sandbox.spec.runtimeClassName`

**Date:** 2026-05-25
**Forgejo issue:** #141 (parent epic: #136)
**Branch:** `feat-issue-141-pod-adapter-runtime-class`

## Summary

Cut `PodAdapter` over from the Plan 04 "warn and ignore" stance to real
`RuntimeClass`-driven dispatch. The Pod object posted to kube now
carries `spec.runtimeClassName` straight through from
`Sandbox.spec.runtimeClassName`, so once the corresponding
`RuntimeClass` resource (e.g. `gvisor`) is installed in the cluster
(Plan 05 T12), the kubelet picks the matching runtime handler instead
of always falling back to the cluster default.

## Changes

- `crates/sandbawks-controller/src/pod_adapter.rs`
  - Module docstring rewritten — the field is honored, no longer
    intentionally suppressed.
  - `PodAdapter::pod_spec_for` now sets
    `PodSpec.runtime_class_name = sandbox.spec.runtime_class_name.clone()`.
  - Deleted the `tracing::warn!("PodAdapter ignores spec.runtimeClassName")`
    block in `create_pod` — the warning is obsolete now that the field
    flows through.
- `crates/sandbawks-controller/tests/pod_adapter.rs`
  - Removed `create_pod_logs_warning_when_runtime_class_name_set`: it
    asserted the *opposite* of the new contract (that
    `runtimeClassName` must **not** make it onto the posted Pod).
  - Added `pod_runtime_class_set_when_sandbox_specifies_gvisor`:
    builds a Sandbox with `runtime_class_name = Some("gvisor")` and
    asserts the rendered Pod carries it.
  - Added `pod_runtime_class_absent_when_sandbox_unset`: asserts a
    Sandbox without `runtime_class_name` yields a Pod with no
    `runtimeClassName`, preserving the cluster-default behavior so
    existing kind/Plan 04 deployments are unaffected.
  - Tests module docstring updated to reflect the new
    `pod_spec_for`-level assertions.

## Why scope-tight

The issue explicitly out-of-scopes the `RuntimeClass: gvisor` Helm
template (T12), the kind-side gVisor handler installation (T11/T13),
and the integration test that drives the full path (T14). This PR
restricts itself to the adapter logic and unit assertions. The
existing `runtime_class_is_pod` test is left alone — `PodAdapter`
still advertises itself as `"pod"` to the controller's dispatch layer;
the runtime class it *sets* on the Pod is independent.

## Validation

- `just fmt-check` — clean.
- `cargo clippy -p sandbawks-controller --all-targets -- -D warnings` —
  clean.
- `cargo test -p sandbawks-controller --all-features` — all 8 integration
  tests pass, including the two new `pod_runtime_class_*` cases. State
  machine + smoke + property tests all green.
- `just test` — entire workspace test suite green.
- `helm template test deploy/helm/sandbawks` — chart renders without
  error (smoke check from the issue's Verification block).

## Reviewer notes

The deleted "logs warning" test was the canary for the old behavior;
its replacement is two focused `pod_spec_for` unit tests rather than
another `tower-test::mock::pair` HTTP round-trip, because the
assertion no longer depends on the kube wire format — it's a pure
function of the input Sandbox. This keeps the new tests fast and
non-flaky.
