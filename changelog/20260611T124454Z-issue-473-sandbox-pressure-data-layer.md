# Sandbox pressure telemetry — data layer (readers and parsers)

- **Date:** 2026-06-11T12:44:54Z
- **Issue:** #473 (phase 1 of epic #472)
- **Crate:** `sandbawks-node`

## Summary

First phase of per-sandbox resource-pressure telemetry (epic #472). Adds the pure
data layer — the readers and parsers that turn host sources into numbers — with no
wiring into the running DaemonSet. The watcher loop, hysteresis, logging, and
metrics follow in phases 2 (#474) and 3 (#475).

## What landed

New module `crates/sandbawks-node/src/sandbox_stats.rs`:

- **Stat types:** `CpuStats` (`cpu.stat usage_usec` + `cpu.max` quota), `CpuQuota`
  (with `allowed_cores()`), `PsiSome` (PSI `some avg10`), `IoStats`
  (`io.stat` rbytes/wbytes), `NetCounters` (rx/tx), and the composite
  `CgroupPressureStats`. Memory `current`/`max` is intentionally omitted — the
  existing `CgroupMemoryReader` already provides it, so phase 2 composes the two
  readers rather than re-reading.
- **Pure parsers (fully unit-tested):** `parse_cpu_stat`, `parse_cpu_max` (maps
  `max` → unlimited), `parse_psi_some_avg10`, `parse_io_stat` (sums across
  devices), `parse_proc_net_dev` (tolerates a counter abutting the iface name).
- **`dir_size_bytes`:** iterative (explicit-stack) recursive byte count for the
  overlayfs upperdir, the input to phase 2's disk-fill estimator. Best-effort and
  infallible (returns `u64`): missing root → `0`; an unreadable subdir/entry is
  skipped (logged once) so the result is a lower bound rather than aborting;
  symlinks are never followed and contribute no bytes; deep trees can't overflow
  the stack.
- **Readers behind traits:** `CgroupPressureReader` / `SysCgroupPressureReader`
  (reuses `memory_pressure::pod_cgroup_dir`, promoted to `pub(crate)`) and
  `ProcNetReader` / `SysProcNetReader` (configurable `/proc` root). Both follow
  the established `Ok(Some)` / `Ok(None)` (absent) / `Err` (present-but-unreadable)
  convention and are fail-open: absent optional files (PSI, `io.stat`) degrade to
  `None` / zero rather than erroring.
- **`SandboxPidResolver` trait:** resolves a container ID to its host init PID;
  implemented for `gvisor::ContainerdClient` via a new `Tasks.Get` call
  (`task_init_pid`). The trait lives in `sandbox_stats` so phase 2 stays free of
  containerd types and can use a fake.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- 14 new unit tests cover every parser (including malformed/empty inputs), the
  symlink-safety and missing-root behavior of the directory walk, the cgroup
  reader against a fixture cgroup tree (and the degraded optional-files case), and
  the `/proc/<pid>/net/dev` reader (present + task-exited cases).

## Not in scope (later phases)

- Watcher loop, disk-fill estimator, hysteresis episodes, warn/clear logs (#474).
- OTel gauges + Helm plumbing (#475).
