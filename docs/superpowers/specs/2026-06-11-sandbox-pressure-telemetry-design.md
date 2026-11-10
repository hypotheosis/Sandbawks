# Per-Sandbox Resource Pressure Telemetry

- **Date:** 2026-06-11
- **Status:** Approved design, pending implementation
- **Owner crate:** `crates/sandbawks-node`

## Problem

Some sandbox pods "lag" or fail and we have no per-sandbox telemetry to attribute
the failure to CPU, memory, disk, or network pressure. When a pod dies we have
nothing from before the failure, and during a live repro there is no way to watch
a sandbox's resource consumption in real time.

Known failure modes this must catch:

- Memory pressure inside the gVisor sandbox (see #305: gVisor does not enforce the
  pod memory limit as a containment boundary).
- Ephemeral-storage fill leading to kubelet eviction (see #418: 256Mi
  ephemeral-storage eviction surfaced to clients as an opaque h2 protocol error).
  Disk usage can balloon suddenly (e.g. compiling large Rust projects), so fill
  detection must be fresh at the check interval, not at kubelet's ~10s cadence.
- CPU starvation / throttling and IO stalls that slow a sandbox while raw usage
  numbers look unremarkable.

## Goals

1. Watch every sandbox pod's CPU, memory, disk, and IO from the node daemonset on
   a configurable interval (default 1s).
2. When any watched signal crosses a warn threshold, emit a structured warning log
   from `sandbawks-node` at a configurable report rate (default 1/s) naming the
   sandbox and including its full resource readout (CPU, RAM, disk, network rate).
3. Publish the same readings continuously as OTel gauges so pre-failure baselines
   are queryable in Traceway.
4. Zero cost when disabled. The feature is an opt-in toggle (default off) because
   it is a debugging aid, not an always-on subsystem.

## Non-goals

- No enforcement or remediation (no kills, no captures, no evictions). This is
  telemetry only and must be fail-open: a telemetry error never affects a sandbox.
- No changes to envd. The guest cannot see its own limits truthfully under gVisor
  (guest cgroupfs reports host RAM), so the host-side pod cgroup read by the node
  daemonset is the only correct vantage point.
- No changes to the existing memory-pressure/OOM-capture watcher (#307). The new
  watcher is a sibling, not a replacement (Approach B below).
- No kubelet stats-summary dependency (no new RBAC/TLS surface).
- No network *threshold*. Network rate is reported context only.
- No e2e test in v1; unit tests with mocked readers only.

## Approach decision

Three approaches were considered:

- **A. Generalize `memory_pressure.rs`** into one unified watcher. Rejected:
  entangles chatty telemetry with the fail-closed OOM state-capture machinery,
  and forces one interval where the two features want different cadences.
- **B. New sibling module sharing building blocks.** Chosen. A separate watcher
  loop in `crates/sandbawks-node/src/sandbox_pressure.rs` that reuses
  `pod_cgroup_dir`, the pod-discovery pattern, and the mockable reader-trait
  style from `memory_pressure.rs`. Additive, easy to review and revert; the
  existing watcher is untouched. The duplicate cost (two loops listing pods and
  reading `memory.current`) is negligible.
- **C. envd or per-sandbox sidecar.** Rejected: wrong vantage point under gVisor;
  sidecars multiply pod overhead and snapshot complexity.

## Signals

All read per sandbox pod, each check interval, from the host side:

| Signal | Source | Role |
|---|---|---|
| CPU utilization fraction | `cpu.stat` `usage_usec` delta vs `cpu.max` quota | threshold |
| CPU stall | `cpu.pressure` PSI `some avg10` | threshold |
| Memory fraction | `memory.current` / `memory.max` | threshold |
| Memory stall | `memory.pressure` PSI `some avg10` | threshold |
| Disk fill fraction | estimator (below) vs pod ephemeral-storage limit | threshold |
| IO stall | `io.pressure` PSI `some avg10` | threshold |
| Network rx/tx rate | `/proc/<sandbox-pid>/net/dev` eth0 byte deltas | context only |

Thresholds combine usage fractions (early warning before hard limits: OOM,
eviction) with PSI stall percentages (direct measurement of "this sandbox is
lagging"). PSI files live in the pod cgroup directory alongside the memory files
the daemonset already reads; the host `/sys/fs/cgroup` mount already exists.

### Disk fill estimator

Walking the pod's overlayfs upperdir is the only exact fill measurement and is
O(files) — too expensive at 1 Hz for exactly the workloads that balloon (large
Rust builds). Instead:

- Every tick, read `io.stat` `wbytes` (nearly free) and estimate
  `fill = last_walk_bytes + wbytes_since_walk`. The estimate deliberately
  overestimates (overwrites and deleted temp files count as growth), which errs
  toward warning.
- A real upperdir walk runs every `walkInterval` (default 15s) and corrects
  `last_walk_bytes`. The upperdir is located via the containerd data-root
  discovery already built for #390.
- When the estimate first crosses the warn threshold, an immediate walk is
  triggered to confirm before the episode's first warning log carries the exact
  number. A sudden balloon is therefore detected within one tick.

### Network rate

The sandbox PID is resolved through the existing containerd client
(`gvisor.rs`), then `/proc/<pid>/net/dev` is read for the pod's eth0 and diffed
between polls for a true per-interval rate. Under gVisor the veth device
counters in the pod netns still tick (runsc netstack attaches to the veth inside
the netns). Requires host PID namespace visibility (hostPID or a host `/proc`
mount on the daemonset — whichever the chart does not already provide).

## Watcher loop

`crates/sandbawks-node/src/sandbox_pressure.rs`, spawned from `main()` in
`bin/sandbawks-node.rs` only when enabled; supervised alongside the other
background loops. Each tick:

1. List sandbox pods on this node (`sandbawks.dev/sandbox` label +
   `spec.nodeName=$NODE_NAME,status.phase=Running` field selector — same pattern
   as `memory_pressure.rs`).
2. For each pod, resolve the cgroup dir (reuse `pod_cgroup_dir`, which handles
   systemd and cgroupfs layouts) and read stats through small reader traits
   (sibling style to `CgroupMemoryReader`) so unit tests can mock the
   filesystem, containerd, and `/proc`.
3. Update per-pod state keyed by **pod UID**: previous counters (cpu usage,
   io wbytes, net bytes), last walk result and time, and episode state.
4. Evaluate thresholds, drive the hysteresis state machine, emit logs/metrics.
5. Drop state entries for pods that vanished.

### Fail-open edge cases

A telemetry error must never affect a sandbox or kill the loop:

- `cpu.max` is `max` (no quota): skip the CPU fraction signal; PSI still works.
- Pod has no ephemeral-storage limit: report absolute fill bytes in logs/gauges;
  no disk-fill threshold for that pod.
- PID or upperdir resolution fails: skip that signal for that pod, log the
  failure once per pod (not per tick), keep all other signals working.
- Counter regression (container restart): reset that counter's baseline; skip
  rate signals for one tick.
- Any per-pod read error: skip the pod this tick; never terminate the watcher.

## Hysteresis and emission

A per-sandbox pressure **episode** opens when any threshold signal crosses its
warn level.

- While open: `tracing::warn!` at the report interval with structured fields:
  sandbox name, namespace, pod name, the triggering signal(s), CPU fraction +
  PSI, memory current/max/fraction, disk estimate/limit/fraction, IO PSI, and
  network rx/tx rates.
- The episode closes only when **all** signals drop below their clear levels.
  On close, a single `tracing::info!` "pressure cleared" log records episode
  duration and per-signal peak values.
- Clear levels are derived, not separately configured:
  `clear = warn * clearRatio` (default 0.7). This gives clean bracketed
  episodes in the logs with no boundary flapping.

Check interval and report interval are independent: sample at 1s and report at
5s, or both at 1s. `reportInterval` is clamped to `>= checkInterval`.

## Metrics

New `crates/sandbawks-node/src/sandbox_pressure_metrics.rs` following the
existing `OnceLock` instrument-singleton pattern. Meter:
`sandbawks-node.sandbox_pressure`.

| Instrument | Type | Notes |
|---|---|---|
| `sandbox_pressure.cpu.fraction` | Gauge\<f64\> | absent when no quota |
| `sandbox_pressure.cpu.psi` | Gauge\<f64\> | some avg10, percent |
| `sandbox_pressure.memory.fraction` | Gauge\<f64\> | |
| `sandbox_pressure.memory.psi` | Gauge\<f64\> | |
| `sandbox_pressure.io.psi` | Gauge\<f64\> | |
| `sandbox_pressure.disk.fill_fraction` | Gauge\<f64\> | absent when no limit |
| `sandbox_pressure.disk.fill_bytes` | Gauge\<u64\> | estimator value |
| `sandbox_pressure.net.rx_bytes_per_sec` | Gauge\<f64\> | |
| `sandbox_pressure.net.tx_bytes_per_sec` | Gauge\<f64\> | |
| `sandbox_pressure.episodes_total` | Counter\<u64\> | attribute `resource` |

Gauge attributes: `sandbox`, `namespace`, `node`. Cardinality is bounded by
sandboxes-per-node. Recorded every check tick while enabled; the no-op path
applies when `OTEL_SDK_DISABLED` is set, per the existing pattern.

## Configuration

Helm values under `node.sandboxPressure.*` mapped to env vars / clap flags in
`bin/sandbawks-node.rs`, mirroring the `node.memoryPressure.*` block:

| Helm value | Env var | Default |
|---|---|---|
| `enabled` | `SANDBAWKS_SANDBOX_PRESSURE_ENABLED` | `false` |
| `checkIntervalMs` | `SANDBAWKS_SANDBOX_PRESSURE_CHECK_INTERVAL_MS` | `1000` |
| `reportIntervalMs` | `SANDBAWKS_SANDBOX_PRESSURE_REPORT_INTERVAL_MS` | `1000` |
| `walkIntervalSeconds` | `SANDBAWKS_SANDBOX_PRESSURE_WALK_INTERVAL_SECS` | `15` |
| `cpuWarnFraction` | `SANDBAWKS_SANDBOX_PRESSURE_CPU_WARN_FRACTION` | `0.85` |
| `memWarnFraction` | `SANDBAWKS_SANDBOX_PRESSURE_MEM_WARN_FRACTION` | `0.85` |
| `diskWarnFraction` | `SANDBAWKS_SANDBOX_PRESSURE_DISK_WARN_FRACTION` | `0.80` |
| `psiWarnPercent` | `SANDBAWKS_SANDBOX_PRESSURE_PSI_WARN_PERCENT` | `25` |
| `clearRatio` | `SANDBAWKS_SANDBOX_PRESSURE_CLEAR_RATIO` | `0.7` |
| `cgroupRoot` | `SANDBAWKS_SANDBOX_PRESSURE_CGROUP_ROOT` | `/sys/fs/cgroup` |

All flags are validated fail-fast in `main()` (fractions in (0,1], intervals
>= 1, report >= check). The toggle is the Helm value: flipping it rolls the
daemonset. When disabled the loop is never spawned — zero runtime cost.

Per existing convention (no distro hardcoding), any host-layout assumption
(cgroup root, containerd data root, proc root) is configurable with sane
defaults, reusing the discovery mechanisms already shipped for #377/#390.

## Testing

Unit tests only in v1, all against mocked reader traits:

- Hysteresis state machine: open on any signal, stay open, close only when all
  signals clear, episode peak tracking, no flapping at the boundary.
- Disk fill estimator: overestimate property, correction on walk, immediate
  walk on first threshold crossing, counter-reset handling.
- Rate computation: cpu/io/net deltas, counter regression resets baseline.
- Threshold evaluation: missing signals (no quota, no limit) excluded, clamps.
- Parsers: fixture tests for `cpu.stat`, PSI files (`cpu.pressure` format),
  `io.stat`, `/proc/net/dev`.
- Config validation: rejects bad fractions/intervals, report >= check clamp.

## Implementation phases

Sized so each phase is one reviewable PR:

1. **Readers and parsers.** Cgroup stat readers (cpu.stat, PSI, io.stat) as
   sibling traits to `CgroupMemoryReader`; `/proc/net/dev` reader + PID
   resolution via the containerd client; upperdir size walk reusing #390
   data-root discovery. Pure data layer with fixture/unit tests, no behavior
   change.
2. **Watcher loop, hysteresis, and logs.** The `sandbox_pressure` module: poll
   loop, per-pod state, disk-fill estimator, episode state machine, structured
   warn/clear logs. Binary wiring (clap flags, env vars, validation, spawn when
   enabled).
3. **Metrics and deployment.** `sandbox_pressure_metrics.rs` gauges/counter,
   Helm values + daemonset template env plumbing (and hostPID or `/proc` mount
   if not already present), chart docs.
