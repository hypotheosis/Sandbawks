# gVisor sandbox memory/OOM: findings synthesis + product decision

Issue: #309 (synthesis capstone of epic #304 — external user feedback on gVisor
memory-limit / OOM behavior). Consolidates #305, #306, #446, #448, folds in #308.

## Origin

A user request came in: under gVisor, "catch" a memory leak inside a
sandbox to save logs/artifacts before teardown, via a userspace `oomd` or by
subdividing the pod's memory limit into smaller pieces *from inside* the sandbox
(the concrete case was a Docker-in-Docker workflow). The folklore was that
"gVisor doesn't implement nested cgroups, so you can't do additional cgroup
things from the inside." This epic set out to turn that into tested fact.

## What we found

### 1. In-sandbox cgroup control is a dead end (#306)

The emulated `/sys/fs/cgroup` a gVisor sandbox sees is **cgroup v1**, reports
**host RAM** (not the pod cap) at the root, does not materialize the container's
own cgroup, accepts child-cgroup limit writes but **never enforces them** (we
allocated 4× a 16 MiB cap with no OOM kill), and exposes **no PSI**. A userspace
`oomd` / `systemd-oomd` cannot run there (both require cgroup v2 + PSI), and the
pod limit cannot be subdivided in any enforced way. The folklore is **confirmed**.

### 2. The host-side limit wasn't enforced either — and that was the real bug (#305 → #446 → #448)

The more important discovery had nothing to do with the in-sandbox path. #305
found a 256Mi-capped sandbox touched ~5 GiB (~20×) with **no OOM kill**. #446's
on-node inspection showed why: the kubelet set `memory.max`/`cpu.max` correctly
on the pod's `kubepods.slice` cgroup, but that cgroup was **empty** — the runsc
sentry and all its processes ran in a **separate, unlimited `system.slice`
cgroup**. The limit and the processes were in different branches of the tree, so
**no Guaranteed-QoS cap (memory *or* CPU, and by the same mechanism pids/io) was
enforced for any gVisor sandbox** — a node-safety hole, not a feature gap.

Root cause: **our containerd misconfiguration, not a gVisor limitation.** The
`runsc`/`sandbawks-runsc` runtime handlers were not configured to use the systemd
cgroup driver, so runsc treated the kubelet's systemd-form `cgroupsPath` as a
literal cgroupfs path and rooted the sandbox under `system.slice`. The
gVisor-#9580 guest mis-detection (guest sees host RAM) is also present but is
irrelevant to enforcement — the host cgroup is what binds.

### 3. The fix, and a correction worth recording (#448)

runsc must run with **`--systemd-cgroup=true`**. The runc-style
`SystemdCgroup = true` options key is a **no-op for the gVisor shim** — an early
attempt set it and verified on-node that runsc was still launched without the
flag. The working mechanism is the shim's `ConfigPath` pointing at a
`runsc.toml` with `[runsc_config] systemd-cgroup = "true"`, applied to both
handlers. With that, verified end-to-end on a node:

- the sandbox nests under `/kubepods.slice/…pod<UID>.slice/cri-containerd-<id>.scope`
  with `memory.max`/`cpu.max` on the cgroup that actually holds the processes;
- a 256Mi hog is **OOM-killed at the cap** (`OOMKilled`, exit 137);
- a 1-core CPU cap **throttles** (`cpu.stat nr_throttled 150/172`).

> Status note: the first merge (PR #452) shipped the no-op `SystemdCgroup` key;
> the verified `ConfigPath` correction re-lands in **PR #453** (#448 reopened
> until it merges). The no-op behaves exactly like pre-fix, so nothing regressed —
> it just isn't fixed on `main` until #453 lands.

### 4. DinD memory topology (folded from #308) — tested

The original Docker-in-Docker case was exercised live (dockerd inside a `gvisor`
sandbox, node `k8s-libvirt-811bfc83…`):

- **dockerd does not boot out-of-the-box.** It initializes storage (vfs) and
  containerd but dies at network setup: `iptables: Failed to initialize nft:
  Protocol not supported` — gVisor's network stack doesn't support the netfilter
  ops Docker uses to create its NAT bridge chain. It boots with
  `--iptables=false --bridge=none` (and `--storage-driver=vfs`; overlay2 won't
  work over the gofer fs). Inner containers then run with `--network=none`.
- **Inner Docker sees the gVisor emulated cgroup v1** (`docker info`: Cgroup
  Version 1, Driver cgroupfs; `/sys/fs/cgroup` is tmpfs).
- **Per-inner-container memory limits are accepted but NOT enforced.** A
  `docker run --memory=64m` container had `memory.limit_in_bytes=67108864`
  written into the guest cgroupfs (so Docker *did* set it), yet a 200 MiB hog ran
  to completion unimpeded — exactly the cosmetic-but-unenforced behavior #306
  found for direct cgroup writes. Docker itself warns the cgroup memory controller
  is limited.
- **The whole DinD sandbox is still capped at the pod limit** once the #448/#453
  fix is applied — a DinD sandbox is one gVisor pod sharing the pod cgroup, which
  is OOM-killed at its limit (proven directly for a plain gVisor pod in the #448
  verification). So a runaway DinD workload is contained to the pod; it just
  can't be subdivided across inner containers.

Net for the requesting user: the blast radius is contained (the DinD sandbox can't exceed
its pod memory/CPU), but "catch a specific inner container's leak from inside"
is not possible — use one sandbox per workload instead.

## Product decision

| Capability | Supported? | Notes |
|---|---|---|
| In-sandbox `oomd` / `systemd-oomd` | **No, and won't be** | no nested cgroups, no PSI in the guest (#306) |
| Subdividing the pod limit *from inside* the sandbox | **No** | guest cgroupfs is cosmetic; unenforced (#306) |
| Host-side enforcement of the pod's memory/CPU/pids cap | **Yes** (once #453 lands) | was a config bug, now fixed (#448); verified OOM-kill + throttle |
| Per-workload limits for multi-workload users | **Via sandbox-level split** | one sandbox per workload (sandbawks enforces it), not carving one pod limit inside DinD |
| Pre-OOM grace + capture logs before teardown | **Not yet — recommended** | the actual feature the requesting user wanted; tracked in #307, now unblocked |

**Headline:** gVisor *can* contain sandbox resources once the cgroup driver is
configured correctly — this was never an inherent gVisor limitation. What is
genuinely impossible is doing it *from inside* the sandbox. Users who want
per-workload isolation should use one sandbox per workload.

## Follow-ups

- **#307 — host-side memory-pressure reaction (recommended build).** Now
  unblocked: with the sandbox correctly nested, the pod cgroup exposes working
  `memory.high` / `memory.events` / PSI. Set `memory.high` below `memory.max` for
  pre-OOM grace and have `sandbawks-node` watch the pressure signal to snapshot
  logs before teardown. This is the deliverable for the requesting user's actual ask.
- **#450 — cross-runtime audit.** `kata-fc` / `runc-criu` likely share the same
  missing-systemd-cgroup-driver gap; audit and fix per #448's mechanism.
- **#453 — land the real #448 fix** (re-closes #448).
- **#455 — pin the runsc version (chore).** #306 noted the unpinned `release`
  channel lets OOM/cgroup behavior drift silently; #306's matrix and #448's fix
  are only valid for a known runsc version.

## Fleet rollout

No dedicated rollout action: existing nodes pick up the corrected installer
naturally as **cluster-autoscaler rolls older nodes** and the warm pool
reprovisions them from the republished `https://github.com/hypotheosis/sandbawks/releases` script. Until a node is
recycled it retains the pre-fix (unenforced) behavior — status quo, no regression.
