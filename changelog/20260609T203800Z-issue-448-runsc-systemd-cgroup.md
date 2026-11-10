# Enforce gVisor sandbox resource limits: --systemd-cgroup via the runsc shim

Issue: #448 (root cause diagnosed in #446 / PR #449; parent epic #304).

## Problem

No Guaranteed-QoS resource cap (memory, CPU, pids/io) was enforced for a gVisor
sandbox. The kubelet set the caps correctly on the pod's `kubepods.slice` cgroup,
but that cgroup stayed **empty** — the `runsc` sentry and all its processes ran in
a **separate, unlimited `system.slice` cgroup**. The host OOM/CPU controllers
never engaged, and a single sandbox could consume the whole node's CPU and RAM
regardless of its declared limits (see #446 for the on-node evidence).

## Root cause

`scripts/install-runsc-k3s.sh` emits the containerd config with `SystemdCgroup =
true` for the **default runc** runtime, but the `runsc` / `sandbawks-runsc`
runtime blocks had no `options` table. k3s's kubelet uses the systemd cgroup
driver and hands runsc a systemd-form `cgroupsPath`
(`kubepods-pod<UID>.slice:cri-containerd:<id>`). Not told to use systemd, runsc
treated that triple as a literal cgroupfs path and created the sandbox cgroup
under `system.slice` instead of nesting it under the kubelet's limited pod slice.

## Fix (and a correction)

runsc must be run with **`--systemd-cgroup=true`**. Getting that flag set is *not*
done with the runc-style `SystemdCgroup = true` options key — **the gVisor shim
(`io.containerd.runsc.v1`) ignores that key** (an early version of this PR set it
and it did nothing; on-node, runsc was still launched without the flag and the
sandbox stayed in `system.slice`). The gVisor shim instead reads runsc flags from
a `[runsc_config]` section in a TOML file referenced by the shim's `ConfigPath`
option.

So this change:

1. Writes `${k3s_etc}/runsc.toml` containing:
   ```toml
   [runsc_config]
     systemd-cgroup = "true"
   ```
2. Points both the `runsc` and `sandbawks-runsc` handlers at it:
   ```toml
   [plugins."<cri>".containerd.runtimes.runsc.options]
     TypeUrl = "io.containerd.runsc.v1.options"
     ConfigPath = "${k3s_etc}/runsc.toml"
   ```

The containerd CRI plugin then launches runsc with `--systemd-cgroup=true`, so the
sandbox cgroup nests under the kubelet's limited `kubepods.slice` pod cgroup. The
sandbawks shim (`io.containerd.sandbawks.v1`) wraps runsc and accepts the same
options proto + `ConfigPath`. Because the cause is cgroup *placement*, this one
flag restores enforcement for all controllers (memory, cpu, pids, io) at once.

## Validation

**Static**
- `shellcheck scripts/install-runsc-k3s.sh` — clean.
- Rendered the script's `write_runsc_shim_config` + `write_tmpl` in isolation and
  parsed both outputs with `tomllib`: `runsc.toml` has
  `[runsc_config] systemd-cgroup = "true"`, and both handlers carry
  `TypeUrl` + `ConfigPath` pointing at it.
- `just fmt` / `cargo clippy --all-targets -- -D warnings` / `just test` — pass
  (no Rust changed).

**On-node, end-to-end** (node `k8s-libvirt-d26e5c0b…`, cgroup v2 + systemd driver;
applied the corrected config and restarted k3s-agent):

- `runsc-sandbox` is now launched with **`--systemd-cgroup=true`**.
- A 256Mi / 2-core `gvisor` pod's sentry + all processes now live under
  **`/kubepods.slice/…pod<UID>.slice/cri-containerd-<id>.scope`** (the limited
  branch), with `memory.max=268435456` and `cpu.max=200000 100000` on the cgroup
  that actually holds them. The unlimited `system.slice` cgroup is gone.
- **Memory enforcement:** a hog past 256Mi is **OOM-killed at the cap**
  (`OOMKilled`, exit 137) at ~224 MiB, instead of climbing unbounded.
- **CPU enforcement:** three busy loops under a 1-core cap are throttled —
  `cpu.stat`: `nr_throttled 150 / nr_periods 172`, `throttled_usec 46829985`.
- **sandbawks-runsc:** containerd starts cleanly with the added options; a
  `sandbawks-runsc` sandbox also nests under `kubepods.slice` with its limits
  applied and `--systemd-cgroup=true` passed.

## Notes

- The #9580 guest mis-detection (guest sees host RAM) is unrelated and untouched —
  the host cgroup is what enforces now; the guest's view doesn't matter for the
  cap. (Confirmed: the test node's guest still reports host RAM.)
- Editing this script triggers the `shim-publish` workflow to republish the
  bootstrap installer to `https://github.com/hypotheosis/sandbawks/releases`.
