# Host-side memory-pressure reaction for gVisor sandboxes (prototype)

Issue: #307 (parent epic #304; unblocked by the #448 enforcement fix).

## Why

In-sandbox cgroup control is a dead end — gVisor's emulated guest cgroupfs is
cosmetic and exposes no PSI (#306) — so a userspace `oomd` cannot run inside the
sandbox. The only place to "catch" a sandbox approaching its memory cap and save
state before the OOM kill is the **host**. That only became viable after #448:
before it, the sandbox ran unbounded in a `system.slice` cgroup while the
kubelet's limit sat on an empty `kubepods.slice` cgroup, so there was nothing
host-side to watch. After #448 the sandbox nests under the kubelet's *limited*
pod cgroup, whose `memory.current` / `memory.max` / `memory.events` actually
track and bind it.

## What

An opt-in watcher in the `sandbawks-node` DaemonSet that, every few seconds:

1. lists the local sandbox Pods (label `sandbawks.dev/sandbox`, Running);
2. reads each one's pod-cgroup `memory.current` / `memory.max` / `memory.events`
   from a read-only host `/sys/fs/cgroup` mount;
3. when a sandbox crosses a high-water fraction of its `memory.max` (default
   `0.9`) — or the kernel has already recorded a `memory.events` `max`-throttle
   or `oom_kill` — emits a metric + a structured `tracing::warn!`, and
4. captures that sandbox's envd `/__state` (the existing `:49984` snapshot
   endpoint) and uploads it to the snapshot S3 bucket under
   `<key-prefix>/memory-pressure/<sandbox>/<rfc3339>.json`, once per pressure
   episode, before the kernel reaps it.

### Design

Mirrors `headroom.rs`: the decision is a pure, unit-tested `PressurePolicy` over
a `CgroupMemoryStats` reading, and the real cgroup reads live behind a
`CgroupMemoryReader` trait (`SysCgroupMemoryReader`) so the logic is testable
with fixtures and no real `/sys/fs/cgroup`. The watcher reuses the node's
existing `HttpEnvdProbe` (envd `/__state`) and the shared S3 `BlobStore` handle
(no second HTTP pool). It is best-effort: a transient kube/envd/S3 error is
logged + counted and the loop continues — it never terminates on a recoverable
fault, matching the node's other long-lived reconcilers.

### New files / wiring

- `crates/sandbawks-node/src/memory_pressure.rs` — `PressurePolicy`,
  `CgroupMemoryStats`, `CgroupMemoryReader` + `SysCgroupMemoryReader`, the
  cgroup-path resolver, and `run_memory_pressure_watcher`, with unit tests.
- `crates/sandbawks-node/src/memory_pressure_metrics.rs` — OTel instruments
  under `sandbawks-node.memory_pressure`: `node_max_fraction` (gauge),
  `events_total`, `captures_total{result}`. Cardinality bounded to `node`
  (× `result`); per-sandbox detail stays in the warning + the artifact key.
- `bin/sandbawks-node.rs` — `memory_pressure_config()` (env parsing/validation),
  a 4th supervised task in the `select!`, and `build_s3_pair` now also returns
  the shared `BlobStore`.
- Helm: an opt-in `node.memoryPressure.{enabled,cgroupRoot,highWaterFraction,
  intervalSeconds}` block. When enabled it mounts the host `/sys/fs/cgroup`
  read-only (mountPath == hostPath) and sets the `SANDBAWKS_MEMORY_PRESSURE_*`
  env. Off by default — no mount, no poll, no behavior change.

## Validation

- `just fmt` / `cargo clippy --all-targets -- -D warnings` / `just test` — pass.
  New unit tests cover the policy bounds, the unlimited-cgroup case, the
  `memory.events` shortcut, `memory.events` parsing, and the fixture-backed
  reader (including `memory.max = max`).
- `helm template` exercised both ways: with `node.memoryPressure.enabled=true`
  it renders the `SANDBAWKS_MEMORY_PRESSURE_*` env, the `cgroup` volumeMount at
  `/sys/fs/cgroup`, and the backing `hostPath` volume; with the default
  (disabled) it renders **none** of them.

### Not run here (needs an image build + deploy)

The end-to-end demo from the issue — deploy the node with the feature on, run
the #305 memory-hog payload in a 256Mi sandbox, and observe the
`memory_pressure.*` metrics + the captured `/__state` artifact in S3 before the
OOM kill — was not run: it requires building and deploying a node image carrying
this code onto a node (and the test fleet's nodes churn under the autoscaler).
Steps are in the PR's Manual Test Plan.

## Scope / deferred

- This is the prototype the issue asked for: detect + signal + capture. It does
  **not** set `memory.high` for pre-OOM grace — writing to kubelet-managed
  cgroups risks the kubelet resetting it, so that "widen the grace window" lever
  is deferred. Pressure is instead detected via the `memory.current/max`
  high-water line and `memory.events`, which fire before the hard kill on
  cgroup v2.
- The captured artifact is envd `/__state` (the sandbox's process/filesystem
  state); envd has no separate logs HTTP route. Container stdout/stderr logs
  (via the kubelet/CRI) are a possible future addition.
