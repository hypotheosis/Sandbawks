# Issue #289 — Resolve `defaultRuntimeClass` in the controller's `CreatePod`

**Date:** 2026-06-04T00:01:07Z
**Branch:** `fix-issue-289-controller-runtime-class`

## Problem

`SandboxTemplate.spec.defaultRuntimeClass` was resolved onto a `Sandbox`
only by the **gateway's** `create_sandbox` (`routes/sandboxes.rs`). The
Sandbox controller's `Action::CreatePod` read only
`sandbox.spec.runtimeClassName` and never consulted the template, so any
`Sandbox` born outside the gateway — a `kubectl apply` of a CR that omits
`runtimeClassName`, or (pre-PR #287) the warm-pool reconciler — cold-booted
under the cluster **default** runtime (runc), silently breaking pause/resume
because runsc has no checkpoint state for a runc container.

PR #287 patched this for the warm pool by resolving the template in
`reconcile_pool`, but left the underlying footgun for hand-applied
Sandboxes.

## Fix (reviewer's "option b")

Resolve `defaultRuntimeClass` at the controller's Pod-materialization edge,
so **every** sandbox — gateway, warm-pool, or hand-applied — inherits the
template's runtime class.

- Folded the existing `resolve_workload_image` helper into
  `resolve_pod_inputs`, which returns both the workload image **and** the
  Sandbox with its effective runtime class filled in.
- The template's `defaultRuntimeClass` is stamped onto
  `spec.runtimeClassName` only when the CR left it unset; an explicit
  `runtimeClassName` on the CR always wins.
- Returns `Cow<Sandbox>` so the common path (explicit class set, or template
  default empty) borrows without cloning — only an actual override
  allocates an owned copy.
- Both `Action::CreatePod` and `Action::CreateRestorePod` now route through
  `resolve_pod_inputs`. For the restore path this is behavior-preserving:
  the gVisor default maps to `sandbawks-runsc` exactly as the unset case did.

The gateway's and warm-pool's eager resolution are now redundant but remain
in place — they merely set the explicit value the controller would resolve
anyway, so they are idempotent. Stripping them is left as a separate
cleanup to keep this change scoped and behavior-preserving for observers
that read the stamped CR.

## Tests

Added two reconcile integration tests with a `RuntimeClassRecordingAdapter`
that captures the effective `runtimeClassName` of each `create_pod` call:

- `create_pod_inherits_template_default_runtime_class` — a `Creating`
  Sandbox with no `runtimeClassName` inherits the template's `gvisor`.
- `create_pod_explicit_runtime_class_overrides_template_default` — an
  explicit `kata-fc` on the CR wins over the template's `gvisor`.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — 59 test binaries green, exit 0

## Review round (PR #322)

Two non-blocking observations from review #131, both addressed:

- **Observability gap.** Added a `tracing::debug!` in `resolve_pod_inputs`'
  override branch logging the resolved `runtime_class` and `template`, so an
  operator debugging an unexpected runtime class sees the controller applied
  a template default rather than honoring the CR's `None`.
- **Restore-path coverage.** Added
  `create_restore_pod_inherits_template_default_runtime_class`, which drives
  the resume flow through `Action::CreateRestorePod` with the
  `RuntimeClassRecordingAdapter` and asserts the controller resolves
  `gvisor` onto the restore Pod's Sandbox — pinning the behavior the
  changelog calls "behavior-preserving" so a future change to PodAdapter's
  mapping arm can't silently regress it.
