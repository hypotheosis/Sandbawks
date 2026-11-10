# Issue #389 — Resolve the containerd data root by mounting it at `mountPath == hostPath`

**Date:** 2026-06-07T19:04:26Z
**Issue:** #389 — resolve the containerd data root (overlay upperdir) generically
**Closes:** #383 (the underlying k3s pause failure)
**Type:** feature / generalization (`kind/feat`, `area/node`, `area/deploy`, `runtime/gvisor`)

## Background

containerd's snapshotter reports the overlay `upperdir` as a **host-absolute**
path under containerd's data root (e.g.
`/var/lib/rancher/k3s/agent/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/N/fs`
on k3s). The node DaemonSet previously bind-mounted the host data root at a
**fixed** in-container path `/var/lib/containerd`, so on any distro that
relocates the data root (k3s, RKE2, AKS-with-temp-disk) the reported path did
not exist inside the node container and every pause failed at `rootfs_delta`
with `tar+zstd upperdir: No such file or directory` (#383, Traceway exception
`4a0f536ea133b69a`).

The first fix (#388, abandoned) re-rooted the reported path with an
operator-set strip-prefix (`SANDBAWKS_HOST_CONTAINERD_DATA_ROOT`) — the same
"operator-listed candidates" model that #378 was created to *replace* for the
runsc run root.

## Change

Mount the containerd data root at the **same in-container path as its host
value** (`mountPath == hostPath`), instead of a fixed `/var/lib/containerd`.
The snapshotter-reported upperdir then resolves **verbatim** inside the node
container on any distro — no path translation, no env var, no in-container
constant in the binary. This is the same pattern the chart already uses for the
`staging-cache` and `envd-bin` mounts ("mountPath == hostPath so the recorded
path resolves identically").

This is **chart-only** — no Rust change. `ContainerdClient::lookup_upperdir`
already returns the snapshotter path verbatim; making the mount path match the
host path is what makes that path valid inside the container.

- **`deploy/helm/sandbawks/templates/node-daemonset.yaml`**: `containerd-data`
  volumeMount `mountPath` changed from the literal `/var/lib/containerd` to
  `{{ .Values.node.hostPathContainerdData }}`; comments updated.
- **`deploy/helm/sandbawks/values.yaml`**: documented that the data-root mount
  tracks the host path (`mountPath == hostPath`), unlike the run mount which is
  fixed at `/run/containerd`.

## Why this is more general than #388

The operator sets exactly one fact per distro (`hostPathContainerdData`, which
they had to set anyway to mount the right host directory). Nothing about the
data-root path enters the binary, so the same image works on stock containerd,
k3s, RKE2, AKS-temp-disk, and any future relocated layout without a rebuild or a
second knob. (A fully zero-config variant — discovering containerd's `root` at
runtime — remains tracked as level 2 of #389.)

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — green
  (no Rust changed; confirms no regression).
- `helm template` for the stock default and a k3s override: the `containerd-data`
  `mountPath` tracks `node.hostPathContainerdData` and equals the volume's
  `hostPath` in both cases (`/var/lib/containerd` and
  `/var/lib/rancher/k3s/agent/containerd` respectively).

The kind+gVisor `pause-resume-smoke` was not run: kind uses stock containerd
(`hostPathContainerdData=/var/lib/containerd`), so the rendered DaemonSet is
byte-identical to the pre-change one there — it would exercise the no-op case,
not the relocated-data-root path this change fixes. The relocated case is
verified on a real k3s node (Traceway exception `4a0f536ea133b69a` stops
accruing) or via the relocated-data-root kind harness proposed in #389.

## Review follow-ups (PR #390)

- Refreshed the stale module doc at `crates/sandbawks-node/src/gvisor.rs:439-448`
  — it asserted the DaemonSet hostPath-mounts `/var/lib/containerd`, now only
  true for stock containerd; it now describes the host-absolute upperdir and the
  `mountPath == hostPath` mount.
- Added a `helm install`-time guard: `node.hostPathContainerdData` must be a
  non-empty absolute path, or the template `fail`s with a clear message instead
  of emitting a Pod with an invalid mount (mirrors the existing
  `extraHostPathMounts[].mountPath` `required` guard). Verified that a relative
  or empty value now fails at `helm template`.
