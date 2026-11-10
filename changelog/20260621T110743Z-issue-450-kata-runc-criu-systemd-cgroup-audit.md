# Issue #450 — Audit `kata-fc` / `runc-criu` for the gVisor `SystemdCgroup` cgroup-placement gap

**Date:** 2026-06-21
**Issue:** #450 (`area/node`, `kind/test`, `prio/p1`)
**Derives from:** #446 / PR #449 (method + gVisor baseline); gVisor fix #448 / PR #453 (merged, verified on-node)
**Verdict:** **Neither `runc-criu` nor `kata-fc` shares a sandbawks-fixable instance of the gVisor gap.** The
`SystemdCgroup` hole #448 closed was sandbawks's to fix *only because sandbawks itself generates the
`runsc` containerd runtime block* (`scripts/install-runsc-k3s.sh`). Sandbawks generates **no** containerd
runtime block for either of the other two runtimes, so there is nothing in this repo that can carry the same
misconfiguration. No sibling fix issue is warranted. Details and the one residual (operator-owned, not
sandbawks-owned) verification item per runtime below.

---

## Why the gVisor gap was sandbawks's to own

#446 found that the `runsc` / `sandbawks-runsc` containerd runtime handlers, **as written by
`scripts/install-runsc-k3s.sh`**, did not deliver the `--systemd-cgroup` flag. k3s's kubelet addresses pod
cgroups as systemd slices (`slice:prefix:name`); without the flag runsc treats that as a literal cgroupfs
path and creates the sandbox cgroup under `system.slice` instead of nesting it under the kubelet's limited
`kubepods.slice` pod cgroup — so no memory/CPU/pids cap bound the sandbox. The fix (#448) edits the
runtime-handler block that **sandbawks generates**:

- `scripts/install-runsc-k3s.sh:380-418` — writes `[plugins."<cri>".containerd.runtimes.runsc(.options)]`
  and `...sandbawks-runsc(.options)`, delivering the flag via `ConfigPath = <runsc.toml>` (the runc-style
  `SystemdCgroup = true` key is *ignored* by the gVisor shim; see the in-script comment at lines 386-395).

The audit question for #450 is therefore precise: **does sandbawks generate an analogous containerd runtime
block for `runc-criu` or `kata-fc`, and if so does that block place the workload under the kubelet's limited
pod cgroup?**

---

## `runc-criu` — Not implemented; no block exists; gap is structurally impossible (today)

**Finding: N/A — no sandbawks-owned config, no selectable runtime, no fix issue.**

- **No adapter.** The node crate ships only `gvisor.rs` and `kata.rs`
  (`crates/sandbawks-node/src/`). There is no `RuncCriuAdapter`. The string `runc-criu` survives **only** as a
  planned-future label in doc comments / `changelog/` / `docs/superpowers/plans/...` (Plan 09/10, "deferred
  past v0.1").
- **Not selectable.** The node binary's runtime-adapter switch
  (`crates/sandbawks-node/src/bin/sandbawks-node.rs:659-681`) accepts exactly `gvisor`, `kata`, `kata-fc`;
  any other value (including `runc-criu`) is a hard `unknown SANDBAWKS_RUNTIME_ADAPTER` error. The Helm
  `node.runtime` knob (`deploy/helm/sandbawks/templates/node-daemonset.yaml:201-225`,
  `deploy/helm/sandbawks/values.yaml`) accepts only `"" | gvisor | kata | kata-fc`.
- **No containerd block, anywhere.** `grep -rn SystemdCgroup` across the repo hits exactly one line — the
  gVisor comment in `install-runsc-k3s.sh`. There is no install script, Helm template, `ci/kind-node/*.toml`,
  or RuntimeClass for `runc-criu`. (Live confirmation: the AKS prod cluster reports handlers
  `kata kata-cc runc untrusted` on kata nodes and `runc untrusted` on system nodes — **no `runc-criu`
  handler exists**; RuntimeClasses are `gvisor`, `kata-vm-isolation`, `runc`, `sandbawks-runsc`.)

**Conclusion:** there is no generated block to carry the gap and no way to schedule onto such a runtime, so
the gap cannot exist for `runc-criu` today. **Recommendation (not a fix issue):** whoever lands the
`RuncCriuAdapter` (Plan 09/10) must, in the same change, write the `runc-criu` containerd block with
`SystemdCgroup = true` in its `options` table — `runc-criu` *does* run real `runc`, so unlike gVisor the
runc-style key is the correct delivery mechanism (not a `ConfigPath`). Captured here so the requirement isn't
rediscovered the hard way.

---

## `kata-fc` — Sandbawks generates no Kata containerd config; the cgroup placement is runtime/operator-owned

**Finding: not a sandbawks gap, and live-verified clean — the kubelet's cap binds the kata VMM.** The
containerd/kata config is AKS/operator-owned (not sandbawks-generated), but the observed placement is correct.

- **Sandbawks does not provision Kata.** The `KataAdapter` (`crates/sandbawks-node/src/kata.rs`) is a
  *fail-closed pause/resume adapter only* — its `pause`/`resume` return `RuntimeError::Unsupported` (an
  out-of-band Cloud-Hypervisor snapshot destroys the shim-managed VM; epic #594). It writes no containerd
  config. The controller's `PodAdapter` only **stamps** `runtimeClassName` (`kata` / `kata-fc` /
  `kata-vm-isolation`) and the node DaemonSet **gates scheduling** on the kubelet-reported handler
  (`node-daemonset.yaml:213-221`). The Kata runtime itself — its `containerd-shim-kata-v2` registration and
  the `[plugins...runtimes.kata.options]` block — is installed and owned **externally** (AKS managed Kata
  pool, or an operator), exactly as the cold-start work documented (`changelog/20260618T233115Z-issue-594-*`,
  `changelog/20260619T103615Z-issue-594-node-runtime-single-knob.md`).
- **No `kata-fc` handler actually exists on the cluster.** The reserved `kata-fc` string is only the
  `KataAdapter`'s default `runtime_class` label and a Helm alias that maps to handler `kata`
  (`node-daemonset.yaml:215-217`). Live AKS prod: kata nodes report handlers `kata kata-cc runc untrusted`;
  the RuntimeClass in use is `kata-vm-isolation` → handler `kata`. So even the name in the issue title is, in
  practice, the operator-managed `kata` handler.
- **The cgroup model differs and is governed externally.** On Kata the workload runs inside a microVM; the
  host cap must bind the **VMM** process (`cloud-hypervisor` / `qemu`), not a container init. Whether the VMM
  lands under `kubepods.slice/...pod<UID>.slice` is governed by the *Kata* runtime's `sandbox_cgroup_only`
  setting and the shim — i.e. by the (AKS-managed) Kata `configuration.toml` and containerd handler, **not by
  anything in this repo.** Sandbawks has no install-script equivalent to edit; the gVisor remedy pattern
  (rewrite our own generated block) has no analogue here.

**Live verification (#446 method, run on the test cluster):** a privileged `hostPID` + host-`/sys/fs/cgroup`
inspector Pod on kata node `aks-kata-00000000-vmss000003`, against the running Guaranteed-QoS sandbox
`sbx-compound-pool-8tcmx` (ns `sandbawks`, pod UID `d29a5db4-…`, limits 2Gi / 2 cpu). **Result: no gap — the
cap binds.**

- **Process placement.** The VMM and its helpers — `cloud-hypervisor`, two `virtiofsd`, and the
  `containerd-shim` — all sit in
  `kubepods.slice/kubepods-pod<UID>.slice/cri-containerd-<sandbox-id>.scope`, i.e. nested under the kubelet's
  pod cgroup. **Not** detached in `system.slice` (the gVisor failure mode).
- **Cap binding (cgroup v2, hierarchical).** The pod slice
  `kubepods-podd29a5db4_….slice` has `memory.max=2776629248` (~2.59 GiB = the 2Gi limit + AKS's kata
  VM-overhead headroom) and `cpu.max=200000 100000` (= 2 cores). The leaf
  `cri-containerd-<id>.scope` that actually holds the VMM PIDs has its own `memory.max=max`, but in cgroup v2
  the parent's limit binds every descendant — so the VMM is capped at the pod-level 2.59 GiB / 2 cpu.
- **Mechanism.** The `kata` handler points at `/usr/share/defaults/kata-containers/configuration.toml`, which
  sets `sandbox_cgroup_only=true` and `static_sandbox_resource_mgmt=true` — Kata's own knobs that place the
  whole sandbox (VMM included) into the systemd-form pod cgroup and statically size the guest to the pod spec.
  The cluster-wide systemd cgroup driver is on (`runc` handler has `SystemdCgroup = true`); the `kata` handler
  block carries no explicit `SystemdCgroup` key, but placement is empirically correct regardless because the
  `io.containerd.kata.v2` shim honors the systemd-form `cgroupsPath` via `sandbox_cgroup_only`. This config is
  AKS/operator-owned, not sandbawks-generated — but the audit answer is unambiguous: **the kubelet's cap
  binds the VMM.**
- **`runc-criu` cross-check.** The same node's `crictl info` lists runtimes `runc`, `untrusted`, `kata`,
  `kata-cc` — **no `runc-criu`**, confirming the static finding.

---

## Outcome

- **No sibling fix issue filed** — neither runtime carries a sandbawks-owned instance of the #448 gap, and
  the one live-checkable runtime (`kata`) places its VMM under the kubelet's cap correctly.
- **`runc-criu`:** does not exist (no adapter, not selectable, no handler on-node); requirement to set
  `SystemdCgroup = true` recorded for the future adapter implementer.
- **`kata-fc`/`kata`:** live-verified — VMM nests under `kubepods.slice/...pod<UID>.slice` and the pod-slice
  `memory.max` / `cpu.max` bind it. Config is AKS/operator-managed; sandbawks owns none of it.

### Method notes (reused from #446)

- Static: `grep -rn SystemdCgroup`, runtime-adapter switch + Helm `node.runtime` enumeration, adapter source.
- Live: cluster RuntimeClass + per-node `status.runtimeHandlers` enumeration, running-sandbox listing, and a
  privileged `hostPID` + host-`/sys/fs/cgroup` inspector Pod on a kata node (test cluster, authorized) reading
  the VMM process cgroups, the pod-slice limits, `crictl info`, and the kata `configuration.toml`. Read-only;
  no hog / enforcement test was needed since hierarchical cgroup-v2 caps were directly observable.
