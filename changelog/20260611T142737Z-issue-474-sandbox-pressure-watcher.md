# Sandbox pressure telemetry — watcher loop, hysteresis, warn logs

- **Date:** 2026-06-11T14:27:37Z
- **Issue:** #474 (phase 2 of epic #472)
- **Crate:** `sandbawks-node`

## Summary

Phase 2 of per-sandbox resource-pressure telemetry: the behavioral core. A new
opt-in watcher loop polls every sandbox pod on this node, derives CPU / memory /
disk / IO pressure from the phase-1 readers (#473), runs a hysteresis episode
state machine, and emits structured `warn`/`info` logs. After this phase the
feature is usable end-to-end via the `SANDBAWKS_SANDBOX_PRESSURE_*` env family;
OTel gauges and Helm plumbing are phase 3 (#475).

## What landed

New module `crates/sandbawks-node/src/sandbox_pressure.rs`:

- **Pure, unit-tested core:**
  - `PressureThresholds` — validated warn fractions (cpu/mem/disk), PSI warn
    percent, and clear ratio; clear levels derived as `warn * clear_ratio` so the
    band can never invert.
  - `PressureSample` + `warned()` / `above_clear()` — which signals are over warn
    vs. still elevated; `None`-valued signals never contribute (fail-open).
  - `EpisodeTracker` — tick-driven hysteresis: opens on any signal crossing warn,
    emits at the report interval for the whole episode (including the band, with
    `over_warn` empty but `elevated` populated), closes only when all signals fall
    below clear, then emits one `info` summary with duration + peaks. No flapping
    at the boundary. The rising edge logs at `warn` (alert-eligible, one per
    episode); sustained lines log at `info` (continuous tailing output) so a long
    episode is one warn plus an info stream, not hundreds of warns (#478 review).
  - `FillEstimator` — `last_walk_bytes + Δwbytes` per tick, corrected by a real
    upperdir walk every `walk_every` ticks, immediately on the first warn
    crossing, and on an io-counter regression; deliberately overestimates.
  - `cpu_utilization`, `delta_rate`, `parse_quantity_bytes` — rate math (regression
    → `None`) and k8s quantity → bytes (binary + decimal suffixes).
- **Async loop** `run_sandbox_pressure_watcher` — pod discovery (same label +
  nodeName field selector as `memory_pressure`), per-pod state keyed by UID
  (cached PID, rate baselines, fill estimator, episode), per-signal fail-open
  skips logged once per pod (re-armed on recovery so recurring faults re-log),
  blocking upperdir walks offloaded via `spawn_blocking`, and `#[instrument]`
  spans on `sample_pod` / `walk_upperdir` / `compute_net_rates` so the watcher's
  I/O is traceable like the rest of the crate (#478 review). The existing
  memory-pressure watcher (#307) is untouched.
- **Data-layer addition (#473 sibling):** `UpperdirResolver` trait in
  `sandbox_stats`, implemented for `gvisor::ContainerdClient` via the existing
  `lookup_upperdir` path.
- **Binary wiring** in `bin/sandbawks-node.rs`: `SANDBAWKS_SANDBOX_PRESSURE_*` env
  family (enabled, check/report/walk intervals, four thresholds, clear ratio,
  cgroup + proc roots), fail-fast validation, and a supervised background task
  spawned only when enabled (parks otherwise — zero cost when off). Mirrors the
  memory-pressure watcher's env-driven config and `select!` supervision.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- 14 new unit tests: threshold validation; warned/above-clear selection; episode
  open → sustain-through-band → clear (no flap) with peak + duration tracking;
  report cadence; cpu utilization & rate math incl. counter regression; quantity
  parsing; the fill estimator (overestimate, walk correction, first-crossing
  walk, counter-reset rebaseline, no-limit case); and the failure-latch re-arm.

## Not in scope (phase 3, #475)

- OTel gauges + `episodes_total` counter.
- Helm `node.sandboxPressure.*` values and daemonset env/hostPID plumbing.
