# install-runsc-k3s.sh: fix cluster-wide runc crash-loop from a v2-base / v3-handler CRI key mismatch

## Symptom

After `install-runsc-k3s.sh` ran on the k3s agent fleet, **every ordinary Pod**
(no `runtimeClassName`) on a tainted node failed to start, cluster-wide:

```
Error: failed to create containerd task: failed to create shim task:
OCI runtime create failed: runc create failed: expected cgroupsPath to be of
format "slice:prefix:name" for systemd cgroups, got
"/kubepods/burstable/pod7b2fb93b-.../external-dns" instead
```

~60 Pods across argocd, cert-manager, ingress-nginx, keycloak, kyverno,
external-dns, sandbawks itself, etc. went `RunContainerError` /
`CrashLoopBackOff`. Privileged host-network DaemonSet pods (cilium,
node-exporter, synology-csi) kept running, which is the tell that this was the
*default `runc` runtime* breaking, not the node as a whole.

## Root cause

`runc` was launched with `--systemd-cgroup` (so it demanded the
`slice:prefix:name` form) but received a cgroupfs path — a containerd-internal
inconsistency. kubelet was `systemd` on every node (confirmed via
`/configz`), so the disagreement lived entirely inside the rendered
`/var/lib/rancher/k3s/agent/etc/containerd/config.toml`.

The rendered file mixed **two containerd CRI plugin keys**:

```toml
version = 2
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]          # k3s base (v2 key)
  ...
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true
[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc]      # our handlers (v3 key)
[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.sandbawks-runsc]
```

On containerd 2.0.4 these are two *different* plugins. Because the file is
`version = 2`, containerd migrates `io.containerd.grpc.v1.cri` into its active
CRI plugin (`io.containerd.cri.v1.runtime`) — but the hand-written
`io.containerd.cri.v1.runtime` section interferes with that migration and the
default `runc` runtime's systemd cgroup-path conversion is silently lost.
Result: runc gets `--systemd-cgroup` but an unconverted cgroupfs path → crash.

**Why the keys diverged.** The script detected the CRI plugin key by grepping
the *standalone* `config.toml` for `version = 3`. On k3s 1.32.3 + containerd
2.0.4 the standalone config k3s generates is **v3**
(`io.containerd.cri.v1.runtime`) — but the `{{ template "base" . }}` include
the script relies on renders the base as **v2** (`io.containerd.grpc.v1.cri`).
The two never agree, so the handlers were always written under a *different*
key than the base they sit above.

Evidence: an un-upgraded node's standalone config is `version = 3` with
runtimes under `io.containerd.cri.v1.runtime`; the same node, once it had our
`config.toml.tmpl`, rendered `version = 2` with the base under
`io.containerd.grpc.v1.cri` (complete — all k3s defaults present, just v2
syntax) plus our v3 handler tables bolted on.

## Fix

Resolve the CRI plugin key from the **rendered base**, never from the
standalone config or our own appended blocks:

- New `detect_cri_plugin()` reads which key the base's `runc` runtime table
  uses, defaulting to the v2 key (what `{{ template "base" . }}` emits today)
  and still tracking a future k3s that flips the base to v3.
- `write_tmpl` gained a `base_only` mode. The render flow now does a **base-only
  pass first** (`write_tmpl 0 1` → restart), so `config.toml` is the pure k3s
  base with no appended blocks to self-confirm a wrong key; detection runs
  against that; then the handlers are written under the detected key
  (`write_tmpl 0` → restart, with the existing runsc-fallback pass after).

This makes the whole file coherent (currently all-v2), which containerd 2.0
migrates wholesale — `runc` keeps `SystemdCgroup = true` and the
`runsc` / `sandbawks-runsc` handlers register. Same layout as the proven
`ci/kind-node/containerd-runsc.toml` fragment.

## Verification

- `bash -n` and `shellcheck -S warning` clean.
- Clean-`bash` render test: base-only pass emits zero handler tables; with a
  v2 base on disk, `detect_cri_plugin` returns `io.containerd.grpc.v1.cri`;
  the full render writes `runsc`/`sandbawks-runsc` under that key, with exactly
  one `{{ template "base" . }}` include and no `io.containerd.cri.v1.runtime`
  leak. With a v3 base on disk, detection returns
  `io.containerd.cri.v1.runtime`.
- Live node (`ace34210`): hot-fixed by rewriting the existing tmpl's handler
  keys to `io.containerd.grpc.v1.cri` and restarting k3s-agent — `k3s crictl
  info` then listed `runc`, `runhcs`, `runsc`, and `sandbawks-runsc`, and
  `external-dns` (plus the rest of the node's ordinary Pods) returned to
  `Running`.

## Operator steps

Hot-fix an already-tainted node without re-running the installer (keeps the
sandbawks handlers registered):

```bash
sudo sed -i 's/io\.containerd\.cri\.v1\.runtime/io.containerd.grpc.v1.cri/g' \
  /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl
sudo systemctl restart k3s-agent
```

Or revert to k3s's native config entirely (drops the handlers until the fixed
installer re-runs):

```bash
sudo rm -f /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl
sudo systemctl restart k3s-agent
```

Then roll the fixed `install-runsc-k3s.sh` across the fleet and the
node-provisioning path so freshly-joined nodes come up correct. Pause any
in-flight node rollout first — new nodes were joining pre-broken.

## Files touched

- `scripts/install-runsc-k3s.sh`
