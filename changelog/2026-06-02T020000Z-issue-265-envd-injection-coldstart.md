> **SUPERSEDED** — the per-Pod init-container + `emptyDir` injection model
> described below was abandoned because it broke gVisor resume. The shipped
> design is the **node-staged read-only hostPath** model in
> [`2026-06-02T153000Z-issue-265-node-staged-envd.md`](2026-06-02T153000Z-issue-265-node-staged-envd.md).
> This file is kept only as a record of the design journey; it does **not**
> describe the current code.

# Issue #265 — cold-start honors SandboxTemplate.spec.image via envd injection

**Date:** 2026-06-02T02:00:00Z
**Branch:** `fix-issue-265-coldstart-template-image`
**Issue:** #265

## Summary

Cold-start sandboxes ignored `SandboxTemplate.spec.image` and always ran
the envd image, so a sandbox created from `compound-sandbox` failed with
`git: command not found` (exit 127). The controller's `Action::CreatePod`
resolved the image from a `SANDBAWKS_DEFAULT_ENVD_IMAGE` env fallback and
never fetched the `SandboxTemplate` at all.

This change makes the controller resolve the template and build the Pod
from the **init-container injection** model: an `envd-inject` init
container copies the static `envd` binary out of the template's
`envd_image` into a shared `emptyDir`, and the workload container runs
the template's `image` (the customer's rootfs) with the injected envd as
its entrypoint. envd therefore execs *inside* the customer's filesystem
— so `git` and other tooling resolve, and `sandbox.spec.env` is inherited
by every exec — without forcing customers to bundle envd in their images.

## Changes

### `crates/sandbawks-types`

- `src/runtime.rs`: new `PodImages { workload, envd }`; `RuntimeAdapter::create_pod`
  now takes `&PodImages` instead of `envd_image: &str`. Doc comments updated.
- `src/sandbox_template.rs`: doc comments on `image` (no longer must bundle
  envd) and `envd_image` (injected via init container, not a sidecar).
- `src/lib.rs`: re-export `PodImages`.

### `crates/sandbawks-controller`

- `src/store.rs`: new `SandboxTemplateStore` trait + `KubeSandboxTemplateStore`
  and `FakeSandboxTemplateStore` (lenient by default; `strict()` for the
  missing-template path), mirroring `SnapshotJobStore`.
- `src/reconcile.rs`: `ReconcileContext` gains a `templates` field;
  `resolve_pod_images` fetches the template and errors on a missing one;
  `Action::CreatePod` / `Action::CreateRestorePod` thread `PodImages`.
  Removed the now-unused `SANDBAWKS_DEFAULT_ENVD_IMAGE` env fallback.
- `src/pod_adapter.rs`: `pod_spec_for` builds two `emptyDir` volumes, an
  `envd-inject` init container (`cp` from the envd image), and a workload
  container whose `command` runs `/sandbawks/bin/envd --state-file
  /sandbawks/run/state.json`. Applied to both cold-start and restore.
- `src/lib.rs` + `src/bin/sandbawks-controller.rs`: re-exports + wiring.

### Tests / CRDs

- Updated the exact-JSON pod test to assert the workload image, the
  injected-binary entrypoint, and the init container.
- Added `creating_with_missing_template_transitions_to_failed`.
- Regenerated `deploy/crds` (description-only diff on the template CRD).

## Deferred (follow-ups noted on #265)

- envd `self-install` subcommand so the carrier image can be
  distroless/scratch (this PR uses `cp` from the existing image).
- Splitting the envd carrier image from the python reference template.
- Pinning `envd_image` by digest for restore determinism.
- kind+gVisor pause/resume smoke on the injection model (requires a
  cluster; not runnable in this environment).

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all tests pass.
