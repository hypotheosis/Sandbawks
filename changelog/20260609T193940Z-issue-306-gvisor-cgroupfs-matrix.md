# gVisor in-sandbox cgroupfs support matrix (memory/OOM)

Issue: #306 (parent #304 — Greg Bray feedback on memory-limit / OOM behavior
under gVisor)

## Question

Greg's user wanted to **subdivide the pod memory limit** or run a **userspace
`oomd`** from *inside* a sandbawks gVisor sandbox. This document turns that
folklore into an empirically-tested support matrix for our pinned `runsc`:
what `/sys/fs/cgroup` looks like inside the sandbox, which controllers exist,
and whether a process can create a child cgroup and set/enforce a memory limit.

## runsc version under test

```
runsc version release-20260525.0
spec: 1.2.1
```

Installed from the **unpinned `release` channel** (see
`scripts/install-runsc-k3s.sh:123`). cgroup/OOM behavior can drift silently
across releases; pinning is tracked separately (see *Follow-up* below) — this
issue only records the finding.

## Method

The probes do not depend on Kubernetes — gVisor's sentry presents the same
emulated `/sys/fs/cgroup` regardless of launcher. Each probe ran a busybox
rootfs under an OCI bundle whose spec set `linux.resources.memory.limit =
268435456` (256 MiB, matching our pod-template default) and mounted
`/sys/fs/cgroup` as a `cgroup`-type mount:

```
sudo runsc --ignore-cgroups --network=none --rootless run --bundle <bundle> <id>
```

`--ignore-cgroups` only suppresses host-side cgroup management (this host's
cgroup root is delegated/busy); it does not change the sentry's emulated
in-sandbox cgroupfs. Host RAM on the test node: **134714572800 bytes
(125.46 GiB)** — note this number, it reappears below.

## Findings

### 1. The sandbox exposes emulated cgroup **v1**, not v2

`stat -fc %T /sys/fs/cgroup` reports `tmpfs`, under which gVisor mounts one
`cgroup`-type mount **per controller** (classic v1 layout). There is **no**
unified v2 hierarchy: `cgroup.controllers` and `cgroup.subtree_control` are
absent.

```
none /sys/fs/cgroup/cpu     cgroup rw,cpu
none /sys/fs/cgroup/cpuacct cgroup rw,cpuacct
none /sys/fs/cgroup/cpuset  cgroup rw,cpuset
none /sys/fs/cgroup/devices cgroup rw,devices
none /sys/fs/cgroup/job     cgroup rw,job
none /sys/fs/cgroup/memory  cgroup rw,memory
none /sys/fs/cgroup/pids    cgroup rw,pids
```

### 2. The root memory limit reads as **host RAM**, not the 256 MiB pod cap

```
/sys/fs/cgroup/memory/memory.limit_in_bytes  = 134714572800   (= 125.46 GiB = host RAM)
/sys/fs/cgroup/memory/memory.usage_in_bytes  = 4325376        (~4 MiB, tracks live)
```

The emulated cgroupfs has **no knowledge of the real 256 MiB pod cap**. This is
the same root cause recorded for the OOM-not-enforced finding (#305): the
sentry surfaces host totals, not the container's actual limit.

### 3. The container's own named cgroup is not materialized

`/proc/self/cgroup` places the process at `/<container-id>` for every
controller, but that directory **does not exist** under
`/sys/fs/cgroup/memory/`. Only the controller **root** is navigable. You
therefore cannot `cd` to "my own cgroup" and subdivide *your actual
allocation* — `mkdir /sys/fs/cgroup/memory/<container-id>/sub` fails with
`No such file or directory`.

### 4. Child cgroups can be created and limits *written* — but never *enforced*

At the controller **root** you can `mkdir` a child and write v1 control files:

```
mkdir /sys/fs/cgroup/memory/cage                       -> OK
echo 16777216 > .../cage/memory.limit_in_bytes         -> OK (reads back 16777216)
echo $$       > .../cage/cgroup.procs                  -> OK (process joins cage)
```

Usage accounting even tracks correctly. But the limit is **cosmetic** — the
decisive enforcement test (16 MiB cap, then allocate 64 MiB into the cage):

```
cage memory.limit_in_bytes = 16 MiB
cage usage_in_bytes before = 344064
allocated up to            = 64 MiB        <-- 4x the cap
cage usage_in_bytes after  = 67452928      <-- 64 MiB, accounted
memory.failcnt             = <file absent>
VERDICT: NOT ENFORCED — no OOM kill at 4x the cap.
```

The `v2` files Greg's user would reach for (`memory.max`, `memory.high`) do not
exist; their v1 analogues (`memory.limit_in_bytes`, `memory.soft_limit_in_bytes`)
are writable but unbacked by any reclaim/OOM path.

### 5. No PSI — `oomd` / `systemd-oomd` cannot run

Both modern userspace OOM managers require **cgroup v2 + PSI pressure**:

```
/proc/pressure              -> No such file or directory
/proc/pressure/memory       -> absent
/sys/fs/cgroup/memory.pressure (v2 PSI) -> absent
```

Only the v1 `memory.pressure_level` eventfd node exists, and (per finding 4) it
is not backed by enforcement. There is no PSI signal for an in-sandbox daemon
to act on.

## Support matrix

| Capability                                              | Result | Notes |
|---------------------------------------------------------|--------|-------|
| cgroup version exposed                                  | **v1 only** | per-controller mounts; no v2 unified `cgroup.controllers` |
| controllers present                                     | cpu, cpuacct, cpuset, devices, job, memory, pids | classic v1 set + gVisor `job` |
| root `memory.limit_in_bytes` reflects pod cap           | **No** | reports host RAM (125.46 GiB), not 256 MiB |
| own container cgroup directory navigable                | **No** | `/proc/self/cgroup` path is not materialized as a dir |
| `mkdir` child cgroup (at controller root)               | **Yes** | succeeds, exposes v1 control files |
| write `memory.limit_in_bytes` on child                  | **Yes (cosmetic)** | value persists, usage accounted... |
| child limit **enforced** (OOM kill on breach)           | **No** | allocated 4x the cap, no kill; no `memory.failcnt` |
| `memory.max` / `memory.high` (v2)                       | **Absent** | only v1 names exist |
| PSI (`/proc/pressure`, `memory.pressure`)               | **Absent** | only v1 `memory.pressure_level` node |
| run `oomd` / `systemd-oomd` inside sandbox              | **No** | requires v2 + PSI, neither present |
| subdivide the pod memory limit from inside              | **No** | root limit is host RAM, own cgroup not navigable, sub-limits unenforced |

## Conclusion

The folklore is confirmed: **you cannot meaningfully subdivide a sandbawks pod's
memory limit, nor run a userspace `oomd`, from inside the gVisor sandbox.** The
in-sandbox cgroupfs is a limited v1 emulation that (a) reports host RAM rather
than the pod cap, (b) does not expose the container's own cgroup, (c) accepts
child-cgroup limit writes but never enforces them, and (d) provides no PSI. Any
memory-containment guarantee must come from the **host** cgroup the kubelet/
runsc place the sandbox into — not from anything observable or settable inside
it. This reinforces #305: the sandbox memory cap is a host-side concern only.

## Follow-up

Pinning the `runsc` release (so this matrix does not drift silently) is a
deploy/chore concern, explicitly out of scope here. File a `kind/chore`
follow-up to pin the gVisor apt channel/version in
`scripts/install-runsc-k3s.sh` if this matrix is to be treated as a contract.
