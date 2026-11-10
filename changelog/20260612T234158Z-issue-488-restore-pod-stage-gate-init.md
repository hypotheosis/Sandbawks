# Issue #488 — Scheduler-placed restore: pod adapter stage-gate init container

**Date:** 2026-06-12T23:41:58Z
**Branch:** `sandbawks/sandbawks-488`
**Parent epic:** #484 (scheduler-placed restore + Resuming recovery), Phase D
**Depends on:** #485 (`RestorePlacement` shrink), #486 (stage-gate init binary),
#487 (node-side `.staged` marker)

## Context

Scheduler-placed restore (epic #484) creates the restore Pod **first** —
unpinned, with its `io.sandbawks/restore-image-path` annotation already
stamped — and only *then* dispatches the `Stage` job once the scheduler binds
the Pod to a node. The `containerd-shim-sandbawks-v1` reads that annotation and
runs `runsc restore` from local disk the instant the main container starts.

The prior phases (#504) already landed the unpinned, credential-less restore
Pod, the deterministic annotation, the `status` scrub in
`Action::CreateRestorePod`, and `RuntimeAdapter::pod_scheduled_node`. But the
**stage-gate init container was never wired**, so the main container raced node
staging and restored from an empty/partial directory — hanging the Sandbox in
`Resuming` (the CI GiB resume bench failure reported on #488 / PR #506).

This change is the consumer of #487's `.staged` marker.

## What changed

### `crates/sandbawks-controller/src/pod_adapter.rs`

- New constants: `STAGE_GATE_CONTAINER_NAME` (`stage-gate`),
  `STAGE_GATE_VOLUME`, the env-var keys `SANDBAWKS_STAGE_GATE_IMAGE` /
  `SANDBAWKS_STAGE_GATE_TIMEOUT_SECS` the Helm chart (#490) sets on the
  controller, and their fallbacks `DEFAULT_STAGE_GATE_IMAGE`
  (`ghcr.io/hypotheosis/sandbawks-stage-gate:latest`) and
  `DEFAULT_STAGE_GATE_TIMEOUT_SECS` (`300`).
- New pure helper `stage_gate_init_container(staging_path, lookup)` returning
  the init `Container` and its `Volume`:
  - Runs `sandbawks-stage-gate --marker <staging_path>/.staged --timeout-secs
    <n>` via `args` (the image's ENTRYPOINT is the binary). The marker
    filename is the shared `sandbawks_types::STAGED_MARKER_FILENAME`.
  - Mounts a **read-only** hostPath scoped to the per-sandbox staging *parent*
    directory (`<stagingRoot>/<sandbox-id>`, the `parent()` of
    `staging_path`), at the identical in-container path so `--marker` resolves
    the same on host and in the container. `DirectoryOrCreate` tolerates the
    dir not yet existing.
  - Image/timeout resolved from the injected `lookup` (pure, unit-testable);
    a non-numeric timeout degrades to the default rather than breaking Pod
    creation.
- `pod_spec_for` now attaches the init container + staging volume **only** on
  the restore path (`restore.is_some()`). The main `envd` container does not
  mount the staging volume — only the node-level shim reads the staged bundle.
  Cold-start Pods get no init container, keeping the two shapes topologically
  identical apart from the gate.
- Updated the `pod_termination_cause` doc comment: the heuristic inspects only
  `status.containerStatuses`, so the init container (whose terminal state lands
  in `status.initContainerStatuses`) does not affect it; a gate *timeout*
  drives `status.phase` to `Failed`, correctly classified as terminal `Other`
  (→ `RestorePodLost` recovery).

## Tests

- `restore_pod_has_stage_gate_init_container` — restore Pod has exactly one
  init container named `stage-gate` running `--marker <staging>/.staged
  --timeout-secs <default>`, mounting a read-only hostPath scoped to the
  staging parent; the main container does not mount it.
- `cold_start_pod_has_no_init_container` — cold-start Pod has no init container
  and no stage-gate volume.
- `stage_gate_init_container_uses_env_image_and_timeout` — env-configured image
  and timeout flow through.
- `stage_gate_init_container_defaults_when_unset` — defaults apply when unset;
  an unparseable timeout falls back to the default.
- Updated the existing `envd_binary_mounted_read_only_from_node_hostpath`
  integration test: its blanket "no init container in either branch" assertion
  predated epic #484; it now asserts no init container on cold-start and
  exactly the stage-gate on restore, while keeping the envd-binary
  read-only-hostPath / no-emptyDir invariants for both branches.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all pass (new unit tests + updated integration test included).

A live end-to-end check (the GiB resume bench reaching `Running`) is **not**
runnable from this change alone: it needs #490 so Helm surfaces the stage-gate
image (and staging root / gate timeout) to the controller in the CI/kind
cluster. Without #490 the controller falls back to the default `:latest` image,
which the kind cluster has not loaded. This PR delivers the adapter mechanism;
#490 wires the configuration that makes the bench green.

## Review follow-up (PR #507)

Two non-blocking hardening points from the PR review, both on the gate
container in `stage_gate_init_container`:

1. **Explicit `resources`.** The gate had no requests/limits, so a cluster
   `LimitRange` with a required-request policy could reject it at admission —
   failing restore-Pod *creation*, not just the gate. Added a tiny explicit
   request **and** limit (`STAGE_GATE_CPU` = `10m`, `STAGE_GATE_MEMORY` =
   `16Mi`) — generous for a process that only `stat`s a path, and predictable
   QoS under both required-request and max-bound `LimitRange`s.
2. **Locked-down `securityContext`.** The gate is the explicitly
   least-capable, untrusted, credential-less container, so it now runs
   non-root (`runAsNonRoot: true` + an explicit `runAsUser`/`runAsGroup` =
   `65532`, required because the `FROM scratch` image has no `USER` and
   `runAsNonRoot` alone would be rejected), with `readOnlyRootFilesystem:
   true`, `allowPrivilegeEscalation: false`, and `capabilities.drop: [ALL]`.
   Verified safe: the node creates the staging dirs world-searchable
   (`create_dir_all`, `0755`) and the `.staged` marker world-readable
   (`0644`), so a non-root gate can still `stat`/read the marker.

`restore_pod_has_stage_gate_init_container` was extended to assert both the
resources and every securityContext field.

## Out of scope

- Helm surfacing the stage-gate image / staging root / gate timeout to the
  controller — #490.
- The reconcile-side node-discovery dispatch + recovery loop — Phase E
  (#485-chain), already partly present from #504.
