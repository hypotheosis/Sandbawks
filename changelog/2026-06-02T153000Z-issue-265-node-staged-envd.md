# Issue #265 — switch envd delivery to node-staged read-only binary (Option 1)

**Date:** 2026-06-02T15:30:00Z
**Branch:** `fix-issue-265-coldstart-template-image`
**Issue:** #265 / PR #266

## Why this supersedes the init-container injection approach

The earlier init-container + emptyDir injection passed cold start and (after
the mount-gate fix) pause, but **broke resume**: the shim restore path
(`runsc restore`) tried to restore the `envd-inject` init container — which
was never part of the checkpoint — and panicked decoding the `medium: Memory`
tmpfs MemoryFile:

```
starting sub-container [cp /usr/local/bin/envd /sandbawks/bin/envd]:
  failed to load kernel: ... Memory file for "envd:/" not found in CtxMemoryFileMap
```

The proven gVisor resume path requires the restore Pod's container/mount
topology to **match the checkpoint** and the workload to run from the
captured overlay rootfs. An init container + external/tmpfs mounts violate
both. Per a maintainer decision (no-bundling is non-negotiable), we move to
**Option 1: node-staged read-only binary**.

## Design

- The `sandbawks-node` DaemonSet gains a `stage-envd` init container that
  `cp`s the static envd binary from the cluster-wide envd image to a hostPath
  (`node.hostPathEnvd`, default `/var/lib/sandbawks/envd`).
- Every sandbox Pod — **cold-start and restore, identically** — mounts that
  hostPath **read-only** at `/sandbawks/bin` and runs `/sandbawks/bin/envd`
  as its entrypoint. No per-Pod init container, no emptyDir, no tmpfs.
- envd writes its `--state-file` to the default `/var/lib/envd/state.json`,
  which it `create_dir_all`s on the container's overlay rootfs (captured in
  the checkpoint) — so no writable mount is needed for state.

Why this works with checkpoint/restore: the Pod topology is identical to the
proven pre-#266 single-container shape **plus one read-only hostPath mount**.
The binary stays out of `pages.img` (host bind), the read-only mount is exempt
from the #237 pause gate, and on restore gVisor reconnects the same
node-provided file. Because all nodes run the same node release, the staged
binary is byte-identical across nodes — so a sandbox can resume on any node
(this also satisfies the cross-node concern behind #270).

## Trade-off (flagged)

envd is now staged **cluster-wide** from the Helm `envd` image, so
`SandboxTemplate.spec.envd_image` is **not consumed** on the cold-start/restore
paths (it stays a required field, reserved for future per-template staging).
Per-template envd versioning is deferred.

## Changes

- `crates/sandbawks-types/src/runtime.rs` + `lib.rs`: reverted `PodImages`;
  `create_pod` takes the workload `image: &str` again.
- `crates/sandbawks-controller/src/pod_adapter.rs`: `pod_spec_for` builds a
  single container running the workload image with the node-staged envd
  mounted read-only at `/sandbawks/bin`; no init container / emptyDir / tmpfs.
- `crates/sandbawks-controller/src/reconcile.rs`: `resolve_workload_image`
  (was `resolve_pod_images`) returns `spec.image`, still erroring on a missing
  template.
- `crates/sandbawks-node/src/gvisor.rs`: reverted the `SANDBAWKS_MANAGED_MOUNT_DESTS`
  classifier exemption — the read-only mount is already exempt and there are no
  writable external mounts to whitelist.
- `deploy/helm/sandbawks/templates/node-daemonset.yaml` + `values.yaml`:
  `stage-envd` init container + `envd-bin` hostPath volume + `node.hostPathEnvd`.
- `crates/sandbawks-types/src/sandbox_template.rs` + `deploy/crds`: doc updates;
  CRDs regenerated.

## Tests

- `create_pod_posts_expected_pod_to_kube`: asserts workload image, entrypoint
  `[/sandbawks/bin/envd]`, and **no init container**.
- `envd_binary_mounted_read_only_from_node_hostpath`: read-only hostPath mount
  at the node-staged dir, no emptyDir/tmpfs, no init container — for **both**
  cold-start and restore branches.
- `creating_with_missing_template_transitions_to_failed`: unchanged behavior.

## Cluster verification still required (#271)

`just pause-resume-smoke` + the resume bench on kind+gVisor must confirm the
full cold-start → pause → resume cycle on the node-staged shape (the read-only
hostPath mount reconnecting on `runsc restore`). Not runnable without a cluster.

## Validation

- `just fmt` / `cargo clippy --all-targets -- -D warnings` / `just test` — all green.
- `helm template` renders the DaemonSet staging + volume cleanly.
