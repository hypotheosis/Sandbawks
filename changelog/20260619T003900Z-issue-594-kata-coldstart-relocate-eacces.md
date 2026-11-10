# Kata cold-start fix: gate the envd relocate on gVisor (epic #594)

**Date:** 2026-06-19T00:39:00Z
**Issue:** #594 (follow-up to PR #595)
**Branch:** `fix/kata-coldstart-relocate-eacces`

## Symptom

After #595 deployed, Kata sandbox Pods scheduled and started the `envd`
container but never became Ready — `connection refused` on the `:49983`
readiness probe, so the warm pool churned. envd was crashing on startup:

```
Error: relocating envd onto capture surface /sandbawks/run/envd
Caused by:
    0: creating destination directory /sandbawks/run
    1: Permission denied (os error 13)
```

## Root cause

The controller (`PodAdapter::pod_spec_for`) stamped
`SANDBAWKS_ENVD_RELOCATE_TARGET=/sandbawks/run/envd` on **every** cold-start Pod,
unconditionally. That env drives the capture-in-checkpoint relocation (epic
#466), where envd copies itself into the rootfs overlay so `runsc checkpoint`
captures its code pages — a **gVisor-specific** mechanism.

On a Kata sandbox it is:

- **Unnecessary** — Cloud Hypervisor snapshots the whole guest RAM wherever envd
  execs, so there is nothing to relocate for.
- **Fatal** — envd runs as the image's non-root `USER`, and `/sandbawks` (the
  parent of the relocate target, created root-owned by the read-only seed mount
  at `/sandbawks/bin`) is not writable by that user on the Kata rootfs, so
  `mkdir /sandbawks/run` fails `EACCES` and envd exits before binding `:49983`.
  (Under gVisor's `--overlay2=root:self` the same `mkdir` succeeds, which is why
  this never bit the gVisor path.)

## Fix

Gate the relocate env **and** its memory budget on the runtime being gVisor,
reusing the same `None | Some(GVISOR_RUNTIME_CLASS)` predicate the function
already uses for the restore-class mapping:

- gVisor (`runtime_class_name` unset or `"gvisor"`) → relocate env + budget set,
  **behavior unchanged**.
- Any other runtime (`kata-vm-isolation`, `kata-fc`, …) → no relocate env, bare
  memory request. envd execs the staged seed in place and binds normally.

`crates/sandbawks-controller/src/pod_adapter.rs`:
- `uses_gvisor_capture` predicate added once, before the resource block.
- Memory: `memory_with_relocate_budget(...)` only under gVisor; bare quantity
  otherwise.
- Relocate env injection wrapped in `if uses_gvisor_capture { … }`.

## Tests

- New `kata_sandbox_omits_relocate_env_and_budget`: a `kata-vm-isolation` sandbox
  Pod carries no `SANDBAWKS_ENVD_RELOCATE_TARGET` and a bare `1Gi` memory
  request/limit, with the seed mount + entrypoint intact.
- The three existing relocate tests (`minimal_sandbox` has `runtime_class_name:
  None` → still gVisor) are unchanged and green.
- `cargo fmt --check`, `cargo clippy -p sandbawks-controller --all-targets -D
  warnings`, `cargo test -p sandbawks-controller` — all green.

## Notes

- Controller-only change; no chart/CRD change, no `kubectl apply -f deploy/crds`.
- Unblocks Kata **cold start → Running**. Pause/resume on Kata remains the
  Phase C live-validation boundary tracked in #594.
