# Node DaemonSet: configurable containerd run/data host paths

**Date:** 2026-06-01T18:25:49Z

## Problem

First real-cluster install on a k3s node failed to start the `sandbawks-node`
DaemonSet pod:

```
Warning  FailedMount  kubelet  MountVolume.SetUp failed for volume "containerd-data":
hostPath type check failed: /var/lib/containerd is not a directory
```

The DaemonSet hardcoded the upstream/stock containerd host paths
(`/run/containerd`, `/var/lib/containerd`) on `hostPath` volumes declared
`type: Directory`. k3s embeds its own containerd with relocated roots, so the
kubelet stat-check fails closed before mounting:

| Volume          | Stock containerd      | k3s                                      |
|-----------------|-----------------------|------------------------------------------|
| `containerd-run`  | `/run/containerd`     | `/run/k3s/containerd`                    |
| `containerd-data` | `/var/lib/containerd` | `/var/lib/rancher/k3s/agent/containerd`  |

(RKE2 is the same run path with `/var/lib/rancher/rke2/agent/containerd` data.)

## Why a chart-only fix is sufficient

The node binary computes the containerd socket
(`SANDBAWKS_CONTAINERD_SOCKET`, default `/run/containerd/containerd.sock`),
the runsc state root (`DEFAULT_RUNSC_ROOT = /run/containerd/runsc/k8s.io`,
`gvisor.rs`), and the overlayfs upperdir (`/var/lib/containerd/...`,
`gvisor.rs`) relative to the **in-container mountPaths**. Those mountPaths stay
fixed; only the host-side `hostPath` *sources* differ per distro. Repointing
the sources keeps every binary-computed path resolving correctly (the
overlayfs and runsc subtrees live under those roots and follow along). No Rust
change required.

## Change

- `deploy/helm/sandbawks/values.yaml`: added `node.hostPathContainerdRun`
  (default `/run/containerd`) and `node.hostPathContainerdData`
  (default `/var/lib/containerd`), documented with k3s/RKE2 override examples.
- `deploy/helm/sandbawks/templates/node-daemonset.yaml`: the `containerd-run`
  and `containerd-data` volumes now reference those values instead of literals.

Defaults are unchanged, so existing stock-containerd installs are unaffected.

## k3s install override

```yaml
node:
  hostPathContainerdRun:  /run/k3s/containerd
  hostPathContainerdData: /var/lib/rancher/k3s/agent/containerd
```

Or `--set node.hostPathContainerdRun=/run/k3s/containerd \
        --set node.hostPathContainerdData=/var/lib/rancher/k3s/agent/containerd`.

Confirm the right paths on a node with `ls -l /run/*/containerd/containerd.sock`.

## Verification

`helm template` renders the stock defaults unchanged and applies the k3s
override correctly for both volumes.
