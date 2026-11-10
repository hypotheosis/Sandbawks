# Sandbox pressure telemetry — OTel gauges + Helm deployment plumbing

- **Date:** 2026-06-11T17:58:24Z
- **Issue:** #475 (phase 3 of epic #472)
- **Crate:** `sandbawks-node` + `deploy/helm/sandbawks`

## Summary

Phase 3 — the final phase of per-sandbox resource-pressure telemetry. Makes the
watcher's readings queryable in Traceway (continuous per-sandbox OTel gauges +
an episode counter) and deployable/toggleable via Helm. After this phase,
flipping `node.sandboxPressure.enabled` rolls the daemonset with the watcher on,
and per-sandbox baselines exist as gauges *before* any failure.

## What landed

### Metrics

New `crates/sandbawks-node/src/sandbox_pressure_metrics.rs` (the `OnceLock`
instrument-singleton pattern of `memory_pressure_metrics.rs`), meter
`sandbawks-node.sandbox_pressure`:

- Gauges recorded every check tick: `cpu.fraction`, `cpu.psi`,
  `memory.fraction`, `memory.psi`, `io.psi`, `disk.fill_fraction`,
  `disk.fill_bytes`, `net.rx_bytes_per_sec`, `net.tx_bytes_per_sec` — attributes
  `sandbox`, `namespace`, `node`. Non-finite values are dropped before export.
- Counter `episodes_total` — incremented on an episode's rising edge, once per
  signal that opened it. Attributes `node`, `resource` only: a counter never
  resets, so a per-sandbox label would grow without bound.

Cardinality note: the gauges carry a `sandbox` attribute, and the shared meter
provider exports with the SDK-default cumulative temporality and no cardinality
cap — so each distinct sandbox keeps a retained series for the process's
lifetime (it is *not* evicted when the sandbox goes away; the ~2000/instrument
SDK limit is the backstop). This is an accepted, bounded cost for an
off-by-default debugging aid meant to be enabled only during an investigation;
per-sandbox attribution is the feature's whole point, so the `node`-only keying
the memory-pressure sibling uses isn't applicable. A delta/cardinality-limited
View is the documented follow-up if long-term high-churn use is ever wanted.
- The watcher's `compute_disk_fill` now returns the byte estimate alongside the
  fraction so `disk.fill_bytes` is recorded even when a pod has no ephemeral
  limit (absolute fill is still meaningful).

Recording is a no-op under `OTEL_SDK_DISABLED` and only runs when the watcher is
enabled, so there is zero cost when off.

### Helm

- `node.sandboxPressure.*` values block (`deploy/helm/sandbawks/values.yaml`):
  `enabled` (false), `cgroupRoot`, `procRoot`, `checkIntervalMs`,
  `reportIntervalMs`, `walkIntervalSeconds`, `cpuWarnFraction`,
  `memWarnFraction`, `diskWarnFraction`, `psiWarnPercent`, `clearRatio` — all
  documented inline.
- Daemonset template renders the `SANDBAWKS_SANDBOX_PRESSURE_*` env family when
  enabled, mirroring the `memoryPressure` block.
- **Shared cgroup mount:** the memory-pressure (#307) and sandbox-pressure (#472)
  watchers read the same read-only host cgroup tree, so they share one `cgroup`
  volume + mount, rendered when *either* is enabled. A template guard fails the
  install if both are enabled with mismatched `cgroupRoot`s (one mount can't
  serve two paths). Per-watcher absolute-path guards added too.
- **No new `/proc` plumbing:** the daemonset already runs `hostPID: true`, so the
  host `/proc` (for per-sandbox network counters) is visible at `/proc` — the
  `procRoot` default. Noted in the values comment.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- 3 new metrics unit tests: bounded attribute-set construction/ordering, the
  non-finite drop filter, and that the record entry points are safe to call
  under the no-op global meter (they run every tick).
- `helm template` verified across four configs: both watchers off (no
  sandbox-pressure env, no cgroup mount), sandbox-only on (env + one shared
  cgroup mount/volume), both on with matching roots (single shared mount), and
  both on with mismatched roots (fails fast with a clear message). Env values
  render with correct defaults and integer/quote handling.

## Epic #472 complete

With #473 (data layer), #474 (watcher), and this phase, the per-sandbox pressure
telemetry epic is done: enable `node.sandboxPressure.enabled`, drive a sandbox
over a threshold, and watch the warn/info logs from `sandbawks-node` plus the
gauges in Traceway.

## Out of scope

- Alerting rules / dashboards on the new metrics (future work, not in the epic).
