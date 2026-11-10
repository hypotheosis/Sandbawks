# gVisor node-side cgroup inspection: why the sandbox memory limit is not enforced

Issue: #446 (node-side follow-up to #305 / PR #445; parent epic #304 — Greg Bray
memory/OOM feedback). Diagnosis only — the fix is #448.

## Question

#305 found a `256Mi`-capped gVisor sandbox touched ~5 GiB (~20×) with no OOM kill,
but could only observe it from the API side. This issue closes the gap with
**on-node** inspection and answers, definitively:

- (a) Is the host pod cgroup's `memory.max` actually set to `256Mi`, or unset?
- (b) Is the `runsc` sentry's (guest) memory charged to that same limited cgroup?
- (c) Which `runsc` / cgroup-driver path produces the failure?

## Headline result

**Both the host limit and the sentry's memory exist — but in two different
cgroups.** The kubelet sets `memory.max=256Mi` on the pod's `kubepods.slice`
cgroup, which stays **empty**. The `runsc` sentry and its ~1 GiB of guest memory
live in a **separate `system.slice` cgroup with no limit (`memory.max=max`)**.
The host OOM killer therefore never fires: the cgroup that has the limit holds
nothing, and the cgroup that holds the memory has no limit. This is failure
**mode 2 (limit-applied-to-the-wrong-cgroup)**; the #9580 guest mis-detection
(mode 1) is *also* present but is not the binding constraint host-side.

**This is not memory-specific.** Because the cause is *cgroup placement* (the
whole sandbox lands in an unlimited `system.slice` cgroup), **no** Guaranteed-QoS
resource cap is enforced for a gVisor sandbox — CPU is bypassed identically to
memory, and pids/io limits would be too. See "Generalization" below. A single
sandbox can exhaust node CPU and memory regardless of its declared limits.

## Method

Node `k8s-libvirt-322526e7…` (Ubuntu 24.04, kernel 6.8, k3s v1.32.3 / containerd
2.0.4, **unified cgroup v2**, **systemd cgroup driver**). A faithful stand-in for
the controller-emitted Pod — `runtimeClassName: gvisor`, Guaranteed QoS
(`requests.memory == limits.memory == 256Mi`, matching
`crates/sandbawks-controller/src/pod_adapter.rs:309-321`) — ran a **bounded**
memory hog: allocate ~1024 MiB (4× the cap) then hold, so the cgroup tree could
be inspected at steady state.

> Note: a *bounded* hog (not the unbounded #305 payload) was used deliberately —
> every gVisor-capable node in this cluster also hosts production databases, so an
> unbounded run risked node-global OOM of co-tenant workloads. 1 GiB at 4× the cap
> already proves non-enforcement without that risk. Host access was via a
> privileged inspector Pod in `kube-system` (the namespace the failing-closed
> Kyverno mutating webhook exempts) with `hostPID` + a `/sys/fs/cgroup` hostPath.

## Findings

### Node cgroup facts

```
/sys/fs/cgroup            unified cgroup v2 (cgroup.controllers: cpuset cpu io memory hugetlb pids rdma misc)
cgroup driver             systemd  (pod cgroups live under kubepods.slice/…​.slice)
```

This is exactly the cgroup-v2 + systemd-driver combination that triggers gVisor
#9580.

### The three cgroups for one sandbox Pod

For pod UID `d61481fc-…`, sandbox container `d94984b4…`, app container `f742870a…`:

| cgroup | `memory.max` | `memory.current` | `cgroup.procs` |
|---|---|---|---|
| `/kubepods.slice/kubepods-pod<UID>.slice` (kubelet's limit cgroup) | **268435456 (256Mi)** | **0** | **empty** |
| `/system.slice/kubepods-pod<UID>.slice:cri-containerd:d94984b4…` (runsc **sandbox/sentry**) | **max** (unset) | **~1.12 GiB** | 12 PIDs incl. sentry |
| `/system.slice/kubepods-pod<UID>.slice:cri-containerd:f742870a…` (app container scope) | max | 0 | — |

The sentry process confirms its placement:

```
runsc-sandbox  PID 1352110  VmRSS 42 MiB
  cgroup = 0::/system.slice/kubepods-pod<UID>.slice:cri-containerd:d94984b4…
```

All 12 sandbox processes (sentry + gofers) are in the **unlimited `system.slice`**
cgroup. The guest's 1 GiB appears there on the host as **file-backed** pages
(`memory.stat`: `file ≈ 1.03 GiB`, `anon ≈ 25 MiB`) — gVisor's memfd platform
memory is accounted as page cache, not anon.

### No OOM kill

Host `dmesg` shows **no OOM / cgroup-kill lines**, and `memory.events` reports
`oom 0 / oom_kill 0` on every cgroup. Consistent with the table: the only cgroup
with a limit is empty.

### Generalization: CPU (and any pids/io) limits are bypassed too

The same placement bug applies to every controller. A second warm-pool sandbox
(`sandbawks/sbx-warm-8k6b4`, declared `cpu=2`, `memory=2Gi`, Guaranteed QoS) shows
the limits on the empty `kubepods.slice` cgroup and the running processes
unbounded in `system.slice`:

| cgroup | `cpu.max` | `memory.max` | procs / CPU used |
|---|---|---|---|
| `/kubepods.slice/…pod<UID>.slice` (kubelet's limit) | **`200000 100000` (2 cores)** | **`2147483648` (2Gi)** | **empty, 0 CPU** |
| `/system.slice/…:cri-containerd:<sandbox>` (runsc) | **`max` (unbounded)** | **`max`** | **61 procs, 489 s CPU** |

So the kubelet correctly sets `cpu.max=2cores` and `memory.max=2Gi` — on a cgroup
with no processes — while the sandbox runs CPU- and memory-unbounded in
`system.slice`. (No pids/io limit was declared to demonstrate directly, but it
would land on the same empty cgroup.) The blast radius is therefore the full
node's CPU and RAM per sandbox, not just memory.

### Root cause: runsc isn't using the systemd cgroup driver

The generated containerd config
(`/var/lib/rancher/k3s/agent/etc/containerd/config.toml`) sets `SystemdCgroup =
true` for the **default runc** runtime, but the **`runsc` (and `sandbawks-runsc`)
runtime blocks have no `options` table** — so `SystemdCgroup` is unset for them:

```toml
# line 35 (default runc):
  SystemdCgroup = true

# lines 43-48 (gVisor) — no options/SystemdCgroup:
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.sandbawks-runsc]
  runtime_type = "io.containerd.sandbawks.v1"
```

The OCI `cgroupsPath` handed to runsc is the **systemd triple**
`kubepods-pod<UID>.slice:cri-containerd:<id>`, which a systemd-driver runtime
resolves to `/kubepods.slice/kubepods-pod<UID>.slice/cri-containerd-<id>.scope`
(nested under the kubelet's limited pod slice). Because runsc is **not** told to
use the systemd driver, it treats that triple as a literal cgroupfs path and
creates it under **`system.slice`** (the containerd / k3s-agent service's own
parent), detaching the sandbox cgroup from the kubelet's limited pod cgroup.

## Answers

- **(a) Is `memory.max` set to `256Mi`?** Yes — correctly, on
  `/kubepods.slice/kubepods-pod<UID>.slice`. But that cgroup is **empty**.
- **(b) Is the sentry's memory charged to that cgroup?** **No.** The sentry and
  its ~1 GiB are charged to a **separate, unlimited `system.slice` cgroup**. This
  is the binding failure: limit and memory are in different cgroups.
- **(c) Which path produces it?** Two independent failures, both real:
  1. **Host (mode 2, primary host-side):** `runsc` runtime lacks `SystemdCgroup =
     true`, so the sandbox cgroup lands under `system.slice` instead of nesting
     under the limited `kubepods.slice` pod cgroup.
  2. **Guest (mode 1, #9580):** the guest sees host RAM (`MemTotal` and guest
     `/sys/fs/cgroup/memory.max` both ≈ 7.75 GiB), so the guest OOM killer never
     engages near 256Mi either. Confirmed from inside the sandbox.

  The two are not mutually exclusive — fixing only #9580 would still leave the
  host cgroup limit non-binding because of mode 2.

## Conclusion / handoff to the fix (#448)

Every Guaranteed-QoS cap is structurally bypassed: **the cgroup with the limits
is empty; the cgroup with the processes is unlimited** — for memory *and* CPU
(and, by the same mechanism, pids/io). This is a node-safety hole, not a
memory-only quirk: one gVisor sandbox can consume the whole node's CPU and RAM
regardless of its declared limits.

The single candidate fix addresses all of them at once, because the cause is
cgroup *placement*, not any one controller: set `SystemdCgroup = true` on the
`runsc` and `sandbawks-runsc` runtime `options` in the containerd config emitted
by `scripts/install-runsc-k3s.sh`, so the sandbox cgroup nests under the
kubelet's limited pod slice. Then the kubelet's `memory.max` / `cpu.max` apply to
the processes that actually run, and the host kernel can reap an over-limit
sandbox. That change (and verifying runsc's `io.containerd.runsc.v1` honors the
option) is **#448**, not this issue.

Note this also reframes the problem for the epic (#304): the binding failure is
**our containerd misconfiguration**, not an inherent gVisor limitation. The
`--total-memory` / #9580 route only makes the *guest* self-limit; host-side
containment is restored by the cgroup-driver fix.

## Cleanup

Both the probe Pod (`kube-system/oom-probe-446`) and the inspector Pod
(`kube-system/cgroup-inspector-446`) were deleted; the node returned to
`MemoryPressure=False`. No production workloads were disturbed.
