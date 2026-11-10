# Issue #605 — Filesystem persistence: controller PVC reconciliation + Pod mount

_2026-06-19 14:32 UTC — #603 Phase 2._

## Summary

Makes `volumeClaimTemplates` real. The controller now materializes one
Sandbox-owned `PersistentVolumeClaim` per declared template and mounts it into
the sandbox Pod, so the declared paths are durable for a running sandbox. PVCs
cascade-delete with the Sandbox. This phase does **not** change pause/resume —
it only provisions and mounts the volumes (the `filesystem` suspend semantics
land in Phase 3 / #606).

Builds on #603 Phase 1 (#609), which added the `persistenceMode` selector and
the `VolumeClaimTemplate` CRD type (`metadata{name,labels,annotations}` +
`VolumeClaimSpec`). No CRD or Helm change here.

## Changes (`crates/sandbawks-controller/src/pod_adapter.rs`)

- **`pvc_for(sandbox, template)` / `pvcs_for(sandbox)`** — builders mirroring
  `network_policy_for`. Each PVC is named `{template}-{sandbox}` (`pvc_name`),
  owner-references the Sandbox CR (`controller: true`,
  `block_owner_deletion: true`) for cascade GC, carries the per-sandbox identity
  label plus any user labels/annotations, and translates the CRD's
  `VolumeClaimSpec` subset (storage request, access modes, storage class, volume
  mode) into the `k8s_openapi` `PersistentVolumeClaimSpec`.
- **`ensure_pvcs`** — called in `create_pod` after the env Secret / egress
  NetworkPolicy and **before** the Pod. Idempotent create-or-409; on 409 it GETs
  the existing PVC and adopts it only when `pvc_owned_by` confirms a controlling
  owner-ref whose uid matches the Sandbox. A foreign PVC sharing the
  deterministic name is **refused** (loud `tracing::error!`, fail-closed) so the
  controller never clobbers or mounts data it does not own. Create-once: no
  resize/reconcile (per the epic's out-of-scope).
- **`pod_spec_for` injection** — appends one PVC-backed `Volume` (named for the
  template, pointing at the `{template}-{sandbox}` claim) and one container
  `VolumeMount` per template. The read-only envd seed volume/mount stays first
  and the root FS stays ephemeral. `None` / empty `volumeClaimTemplates`
  reproduces the pre-#603 PodSpec exactly. Defensive dedupe gives
  volumeClaimTemplate volumes priority over a same-named volume (StatefulSet
  semantics).

### Mount-path convention (design note)

The Phase 1 CRD's `VolumeClaimTemplate` declares the *volume*, not where the
workload mounts it, and a Sandbox exposes no per-container `volumeMounts`
surface (unlike a StatefulSet pod template). Phase 2 resolves the mount path
from an optional `sandbawks.dev/mount-path` annotation on the template metadata,
falling back to `/<template-name>`. This:

- satisfies the epic's headline acceptance (a template named `workspace` mounts
  at `/workspace` with no annotation),
- supports arbitrary paths (`/home/user/data`) via the annotation,
- adds the mount path **without a CRD schema change** (annotations are a
  free-form map already in the Phase 1 type),
- ignores a blank/whitespace override so an empty `mountPath` can never be
  emitted.

## Tests

- Unit (`pod_adapter.rs`): `pvc_for` name + owner-ref + spec translation;
  `pod_spec_for` injects PVC volume + mount at `/workspace`; unchanged PodSpec
  when no templates; `vct_mount_path` annotation override + blank fallback;
  `pvc_owned_by` reuse-own / refuse-foreign / no-uid.
- Integration (`tests/pod_adapter.rs`): `create_pod_provisions_pvc_before_pod`
  drives the mock kube client and pins the ordering NetworkPolicy → PVC → Pod,
  the PVC body (`workspace-sbx-test`, `10Gi`), and the rendered Pod volume +
  `/workspace` mount.

## Gates

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — green (5 new unit tests + 1 integration test)
