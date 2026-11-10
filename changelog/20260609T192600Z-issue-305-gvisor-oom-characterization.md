# Issue #305 — Sandbox memory/OOM under gVisor: empirical characterization

- **Date:** 2026-06-09T19:26:00Z
- **Issue:** #305 (parent epic #304 — Greg Bray feedback on memory-limit / OOM behavior under gVisor)
- **Labels:** area/node, kind/test, prio/p2, runtime/gvisor
- **Kind:** investigation / empirical characterization (no production code changed)

## TL;DR — the headline finding refutes the issue's hypothesis

The issue predicted that, because `pod_adapter.rs` sets `requests.memory == limits.memory`
(Guaranteed QoS, a hard cap at the pod cgroup), a sandbox that exhausts its memory would be
reaped by the **host kernel OOM killer** at the limit, taking the whole sandbox down with exit
code 137 / `OOMKilled`.

**Observed instead:** a gVisor sandbox capped at `256Mi` allocated and touched **~5 GiB of
anonymous memory (~20× its limit) without ever being OOM-killed.** The container only stopped
when we deleted the pod. The `256Mi` Guaranteed-QoS cap provided **no effective protection**
against memory exhaustion inside a gVisor sandbox on these nodes.

This is the gVisor cgroup-v2 + systemd-driver memory mis-detection bug class
([gVisor #9580](https://github.com/google/gvisor/issues/9580)) manifesting end-to-end on the
sandbawks fleet, with a second, more serious consequence: the host pod-cgroup limit is also not
constraining the `runsc` sentry's host memory consumption.

## Environment (ground truth)

| Property | Value |
| --- | --- |
| Cluster | live k3s, `gfd/sandbawks` |
| Node under test | `k8s-libvirt-b8e83881-fcff-49e9-8aa0-c7aae5db24ab` |
| Node OS / kernel | Ubuntu 24.04.4 LTS / `6.8.0-106-generic` |
| Container runtime | `containerd://2.0.4-k3s2` |
| **Host cgroup mode** | **cgroup v2 (unified) + systemd driver** — the configuration implicated in gVisor #9580 (Ubuntu 24.04 defaults to unified cgroup v2; k3s uses the systemd cgroup driver) |
| RuntimeClass | `gvisor` → handler `runsc` (the handler the controller actually selects; verified against a live sandbox pod) |
| gVisor guest kernel | `4.19.0-gvisor` (from guest `uname -a`) |

### What the real sandbox path produces (verified, not assumed)

Read off a live warm-pool sandbox pod (`sbx-warm-8k6b4`) to confirm the probe faithfully
mirrors `pod_adapter.rs`:

- `status.qosClass: Guaranteed`
- `spec.runtimeClassName: gvisor`
- `resources.requests.memory == resources.limits.memory`

This matches `crates/sandbawks-controller/src/pod_adapter.rs:309-321`, which inserts the same
`Quantity` into both `requests["memory"]` and `limits["memory"]` (and likewise for cpu /
ephemeral-storage). The issue cited lines 208-215; the code has since shifted to 309-321 but the
behavior is unchanged.

## Method

Rather than exec a hog into a shared warm-pool sandbox (disruptive, and the sandbox entrypoint
is `envd`, not a shell), we deployed a **dedicated, self-contained probe pod** that mirrors the
exact Pod shape the controller emits — `runtimeClassName: gvisor`, Guaranteed QoS — but at
`spec.resources.memory = 256Mi` (the value the issue calls out) and with a memory-hog entrypoint
so the container self-exercises the limit. `restartPolicy: Never` preserves the terminal state;
the OOM (had it fired) is scoped to the pod's own cgroup (`memory.max`), not node-global.

The entrypoint first prints the gVisor-detected guest memory (`/proc/meminfo`) and the
guest-visible cgroup limit — the direct cross-check for gVisor #9580 — then linearly allocates
~1 MiB of distinct, page-touched anonymous memory per iteration.

### Reproduce it

```bash
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: oom-probe-256
  namespace: sandbawks
  labels: { app: oom-probe, issue: "305" }
spec:
  runtimeClassName: gvisor          # handler runsc — same as a real sandbox
  restartPolicy: Never              # preserve terminal status; no restart loop
  containers:
    - name: hog
      image: docker.io/library/busybox:1.36   # dockerio proxy, never docker.io
      resources:                    # Guaranteed QoS: requests == limits
        requests: { cpu: "250m", memory: "256Mi" }
        limits:   { cpu: "250m", memory: "256Mi" }
      command: ["sh", "-c"]
      args:
        - |
          echo "=== uname -a ==="; uname -a
          echo "=== /proc/meminfo (gVisor-detected guest MemTotal) ==="; head -5 /proc/meminfo
          echo "=== guest /sys/fs/cgroup fstype ==="; stat -fc %T /sys/fs/cgroup 2>&1 || true
          echo "=== guest cgroup mem limit ==="
          cat /sys/fs/cgroup/memory.max 2>/dev/null \
            || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null \
            || echo "(no cgroup mem file visible in guest)"
          echo "=== begin linear memory hog: ~1 MiB/iter, target = 256Mi pod cap ==="
          awk 'BEGIN{ s="x"; while(length(s)<1048576) s=s s;
                      for(n=1;;n++){ a[n]=s n; if(n%16==0){ print n" MiB anon touched"; fflush() } } }'
YAML

# Watch it blow past the limit instead of dying at it:
kubectl -n sandbawks logs -f oom-probe-256

# IMPORTANT: the hog is unbounded on these nodes (no OOM kill). Stop it before it
# threatens the node — do NOT leave it running:
kubectl -n sandbawks delete pod oom-probe-256 --grace-period=5
```

## Captured artifacts

### 1. Container exit code / status — **NO OOM kill (hypothesis refuted)**

The container never self-terminated. It ran from `19:21:01Z` to `19:24:05Z` (~3 min, throttled
to 250m CPU) and only stopped on our explicit delete. Expected `137` / `OOMKilled` was **not**
observed.

### 2. Pod events (`kubectl get events --field-selector involvedObject.name=oom-probe-256`)

```
TYPE     REASON      MSG
Normal   Scheduled   Successfully assigned sandbawks/oom-probe-256 to k8s-libvirt-b8e83881-...
Normal   Pulling     Pulling image "docker.io/library/busybox:1.36"
Normal   Pulled      Successfully pulled image ... in 2.822s. Image size: 2217006 bytes.
Normal   Created     Created container: hog
Normal   Started     Started container hog
Normal   Killing     Stopping container hog          <-- our `kubectl delete`, NOT an OOM
```

No `OOMKilling`, no `OOMKilled`, no kubelet eviction. The only stop reason is our own deletion.

### 3. gVisor memory detection — guest stdout (the #9580 cross-check)

```
=== uname -a ===
Linux oom-probe-256 4.19.0-gvisor #1 SMP Sun Jan 10 15:06:54 PST 2016 x86_64 GNU/Linux
=== /proc/meminfo (gVisor-detected guest MemTotal) ===
MemTotal:        8130412 kB        # ~7.75 GiB == HOST node RAM, NOT 256Mi
MemFree:         8125508 kB
MemAvailable:    8125508 kB
=== guest /sys/fs/cgroup fstype ===
tmpfs                              # gVisor's synthetic cgroupfs
=== guest cgroup mem limit ===
8325541888                         # ~7.75 GiB == host RAM, NOT 268435456 (256Mi)
=== begin linear memory hog ... ===
16 MiB anon touched
...
5008 MiB anon touched              # last line captured before delete; still climbing
```

**gVisor detected ~7.75 GiB of total memory — the host node's RAM — instead of the `256Mi`
container limit.** Both the guest `/proc/meminfo` `MemTotal` and gVisor's synthetic
`/sys/fs/cgroup/memory.max` reflect host RAM. The guest kernel therefore believes it has plenty
of memory and imposes no guest-side pressure. This is exactly the gVisor #9580 symptom on
cgroup v2 + systemd.

### 4. Host / node side

- Node `MemoryPressure` remained `False` ("KubeletHasSufficientMemory") throughout — kube never
  saw the over-consumption. The scheduler's memory *requests* accounting only ever reflected the
  `256Mi` **request** (node memory-requests dropped exactly 256Mi, 5482Mi→5226Mi, on delete) —
  i.e. **kube is blind to the sentry's real ~5 GiB footprint**, so a single sandbox can silently
  overcommit a node well past what the scheduler thinks is allocated.
- Node-side kernel `dmesg`/`kern.log` OOM lines: **could not be captured via the read-only
  Kubernetes node-log API proxy** (it serves only a `/var/log` directory index here, not file
  contents). Given the k8s events and the 5-GiB-no-kill result, the kernel-log line is
  corroborating rather than load-bearing. Capturing it (and confirming whether the host
  pod-cgroup `memory.max` was even applied to the sandbox cgroup) requires node SSH and is the
  natural hand-off to the sibling cgroupfs-probe sub-issue under #304.

## Interpretation

There are **two** distinct failures, both rooted in the cgroup-v2 + systemd-driver mishandling:

1. **gVisor doesn't read the limit (`--total-memory` = host RAM).** `runsc` fails to resolve the
   container's memory limit from the cgroup path and falls back to total host memory. The guest
   sees ~7.75 GiB, so the *guest* OOM killer never engages near 256Mi. (gVisor #9580.)

2. **The host pod-cgroup limit doesn't constrain the sentry either.** Even with #1, a correctly
   applied host `memory.max=256Mi` on the sandbox cgroup would make the host kernel reap the
   `runsc` sentry at ~256Mi RSS. It didn't (we reached ~5 GiB), which means the limit is
   effectively not being enforced on the sentry's host memory — most likely the same
   cgroup-v2/systemd path that breaks #1 also fails to apply the limit to the runsc-managed
   sandbox cgroup. Pinning down which cgroup holds (or fails to hold) the limit needs node-side
   inspection (sibling cgroupfs-probe sub-issue).

### Why this matters

- The `256Mi` (or any) memory cap on a sandbox is **not a containment boundary** under gVisor on
  these nodes. A runaway or hostile workload inside a sandbox can consume host RAM far beyond its
  declared limit and drive the **node** into global OOM — endangering co-located warm-pool
  sandboxes and system pods — instead of cleanly killing just itself.
- The expectation encoded around `pod_adapter.rs` (Guaranteed QoS ⇒ self-contained OOM) does not
  hold for the gVisor runtime path as currently configured.

## Recommended follow-ups (out of scope for this characterization issue)

1. Node-side cgroup-v2/systemd inspection (sibling sub-issue): confirm whether `runsc` applies
   `memory.max` to the sandbox cgroup at all, and whether sentry memory is charged to the pod
   cgroup. Capture the `dmesg` OOM line under a deliberately small host limit.
2. Track / pin a gVisor build with the #9580 fix, or pass `runsc --total-memory` / the
   appropriate cgroup flags explicitly so the guest sees its real limit.
3. Host/controller-side reaction (already deferred to a separate sub-issue under #304): detect a
   sandbox exceeding its declared memory and reap it, since the in-guest limit is not trustworthy.

## Cleanup

The probe pod was deleted immediately after the finding was established
(`kubectl -n sandbawks delete pod oom-probe-256`); a label sweep (`-l app=oom-probe`) confirms no
residue. The node recovered (`MemoryPressure: False`, requests back to baseline). No production
code, CRDs, or cluster configuration were modified by this investigation.
