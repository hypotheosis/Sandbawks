# Scheduler-placed restore: shim-side staging gate + deploy wiring + tests

**Issue:** #490 (epic #484, scheduler-placed restore + `Resuming` recovery)
**Date:** 2026-06-13T01:28:03Z
**Crates / areas:** `shim/` (`containerd-shim-sandbawks-v1`), `sandbawks-controller`,
`deploy/helm/sandbawks`, `deploy/kind`, `.forgejo`

## Summary

This is the phase that makes scheduler-placed restore (epic #484) actually work
and be deployable end-to-end. Bringing the flow up in CI for the first time
surfaced a fundamental incompatibility in the create-then-stage design:

> the restore Pod's gVisor sandbox is restored as a **whole-sandbox** memory
> checkpoint, and runsc reconstitutes *exactly* the checkpoint's container set.
> A `stage-gate` *init container* (the #487/#488 approach for delaying the
> restore until staging finishes) is therefore rejected by restore-spec
> validation — `checkpoint image does not contain spec for container "stage-gate"`
> — because it is not in the checkpoint.

The fix moves the "wait for the node to finish staging" gate **out of the Pod
and into the restore shim**, where it gates the root container's `runsc restore`
without adding any container to the restored sandbox. The Pod-level stage-gate
init container (and its image/Helm wiring) is removed; the `sandbawks-stage-gate`
crate is deleted as now-dead. The controller's `Resuming` recovery deadlines are
also made runtime-configurable so CI can tune them.

## What changed

### Shim — the staging gate now lives here (`shim/`)

`containerd-shim-sandbawks-v1` already dispatches the whole-sandbox memory
`Restore` on the root container's `Start`. That dispatch now first blocks on the
node's staging marker:

- New `waitForStagedImage` (`internal/wrapper/marker.go`): polls
  `<imagePath>/.staged` (the marker the node drops last, #487) until present,
  ctx-cancelled, or a timeout elapses. Timeout comes from
  `SANDBAWKS_STAGE_GATE_TIMEOUT_SECS` in the shim's own process env (it runs as
  the containerd runtime handler, not a Pod), defaulting to 300s.
- `Service.Start` calls it on the restore branch before `next.Restore`, so the
  restore waits for a complete bundle. A timeout returns the error to containerd;
  the controller's bounded `Resuming` loop recovers it as a lost restore Pod.
- `parseRestoreAnnotation` no longer requires the image path to **exist** at
  Create: under create-then-stage the Pod is created before staging, so the dir
  legitimately does not exist yet. The absolute-path defense (anti path-escape)
  stays; readiness is now proven at Start by the marker gate.

### Controller — no more init container; recovery knobs configurable

`crates/sandbawks-controller/src/pod_adapter.rs`:

- The restore Pod no longer carries the `stage-gate` init container, its staging
  volume, or any of the stage-gate constants/helper. Restore and cold-start Pods
  are now topologically identical (just the `envd` container) — which is exactly
  what whole-sandbox restore requires. The restore-image-path annotation the shim
  reads is unchanged.

`crates/sandbawks-controller/src/reconcile.rs`:

- The `Resuming` recovery tunables that were `const`s are read once (via
  `OnceLock`) from env, defaulting to the prior compile-time values:
  - `SANDBAWKS_RESUME_MAX_RESTARTS` (5) — retry budget before `Failed (ResumeExhausted)`.
  - `SANDBAWKS_RESUME_SCHEDULE_DEADLINE_SECS` (120) — Pending-too-long deadline.
  - `SANDBAWKS_RESUME_READY_DEADLINE_SECS` (120) — scheduled-but-not-Ready deadline.
  - `SANDBAWKS_RESTORE_STAGING_ROOT` (`/var/lib/sandbawks/staging`) — staging root
    the controller records in the restore Pod's host-path annotation, sourced from
    `node.hostPathStagingCache` so the two track at the default. (Not freely
    relocatable: the node DaemonSet `mountPath` and the node binary's
    `DEFAULT_STAGING_ROOT` are hard-coded to the default and must stay equal to it,
    so relocating needs a node-side change too — the default is the supported value.)
- Pure, closure-injected `u32_from_env` / `duration_secs_from_env` helpers do the
  parse-with-fallback, unit-tested without touching process env.

### Helm chart

- `values.yaml`: **no** `images.stageGate` (the init container is gone); a
  `controller.restore` block with `scheduleDeadlineSeconds` / `readyDeadlineSeconds`
  / `maxRestarts` (defaults mirror the Rust `DEFAULT_*` consts, kept aligned by
  hand). The restore *staging* timeout is documented as shim-side, not a Helm value.
- `templates/controller-deployment.yaml`: renders the three resume env vars and
  `SANDBAWKS_RESTORE_STAGING_ROOT` (from `node.hostPathStagingCache`). No
  stage-gate image/timeout env.
- `Chart.yaml`: `0.1.5` → `0.1.6` (project rule).

### Kind CI

- `load-images.sh` reverts to the four-image set (no stage-gate image to load).
- `kind-smoke.yaml`: the `build-images` matrix drops the `stage-gate` leg; both
  gVisor helm installs drop the `images.stageGate` `--set`s; the smoke keeps
  `controller.restore.{scheduleDeadlineSeconds,readyDeadlineSeconds}=90` (the
  shim gate has no Helm knob). The shim change lands in CI because the kind-node
  image cache key already includes `shim/**`, so a `shim/**` change rebuilds the
  node image (with the new shim) from source. The stale bench "resume gate"
  comment is rewritten to describe the shim-gated chain.

### Removed

- `crates/sandbawks-stage-gate` (the init-container binary, #486) and its
  workspace member — dead once the gate moved into the shim.

## Tests

- Shim (`marker_test.go`): marker present / appears-later / timeout /
  ctx-cancel for `waitForStagedImage`; env parsing for `stageGateTimeout`; and an
  end-to-end `Start` test proving Restore is **not** dispatched until the marker
  appears. Existing annotation tests updated: the restore image is now a staged
  *directory*, and an as-yet-unstaged path is accepted at Create.
- Controller: `restore_pod_has_no_init_container` (replaces the old
  has-stage-gate test); the env-binary mount test asserts neither shape adds an
  init container. New `tests/restore_config.rs` proves
  `SANDBAWKS_RESUME_MAX_RESTARTS` resizes the recovery budget end-to-end (own
  binary, since the knob is read once via `OnceLock`). Unit tests for the parse
  helpers. The happy path and recovery paths remain covered by `lifecycle.rs` and
  the #489 tests.

## Validation

- `just shim-check` (gofmt/vet/test/build) — green.
- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — green.
- `helm lint`; `helm template` renders the resume env + staging root (defaults
  and `--set` overrides), no stage-gate env. `kind-smoke.yaml` / `values.yaml`
  validated as YAML.

## Out of scope

- Cross-node orphaned-staging-dir GC — #483.
