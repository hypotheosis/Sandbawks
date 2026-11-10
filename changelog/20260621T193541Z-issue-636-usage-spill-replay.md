# Usage metering: durable spill + replay for dropped events

**Date:** 2026-06-21T19:35:41Z
**Issue:** #636 (follow-up to epic #480 / Phase 2 #631)
**Crates:** `sandbawks-controller`; Helm chart `deploy/helm/sandbawks`

## Summary

Completes the emission-durability story for the authoritative usage signal. The
prior change (same issue) added a bounded retry that rides out a *transient*
object-store blip; this adds the **durable spill + replay** layer for a *sustained*
outage. When a close-edge emission fails after exhausting its retries, the
controller now **parks** the event on a PVC-backed local queue instead of dropping
it, and a background **drainer** replays the queue to the object store once it
recovers — so a multi-minute outage costs latency, not lost billing.

## Changes

### `sandbawks-controller`
- **New `usage_spill.rs`** — `UsageSpillQueue` trait (`spill` + `drain`),
  `FsUsageSpillQueue` (one atomically-written NDJSON file per parked event, named by
  the idempotency key so re-spill overwrites; `drain` replays via the sink and
  removes each on success, leaving failures for the next pass),
  `NoopUsageSpillQueue` (spilling disabled → pre-#636 drop behaviour),
  `RecordingUsageSpillQueue` (tests), and tests with `tempfile`.
- **`usage_metrics.rs`** — `UsageEmitOutcome` gains `Spilled` and `Replayed`, so
  `controller.usage.events_total{outcome=...}` now distinguishes parked-for-replay
  and recovered from genuinely `dropped`.
- **`reconcile.rs`** — `ReconcileContext` gains `usage_spill`; on retry exhaustion
  `emit_running_usage` spills (outcome `spilled`) instead of dropping, and only
  `dropped`s when the spill itself is unavailable. Fail-open preserved throughout.
- **`bin`** — `build_usage_spill_queue()` (Fs when `SANDBAWKS_USAGE_SPILL_DIR` set,
  else Noop) and `build_usage_spill_drainer()` (a ticker on
  `SANDBAWKS_USAGE_SPILL_DRAIN_INTERVAL`, default `PT1M`, spawned only when a real
  sink + spill dir exist). `build_usage_sink` now reports whether the sink is real.

### Helm (`deploy/helm/sandbawks`)
- New `controller-usage-spill-pvc.yaml` PVC, gated on `usage.spill.enabled`.
- Controller deployment mounts it and sets `SANDBAWKS_USAGE_SPILL_DIR` /
  `_DRAIN_INTERVAL`.
- New `usage.spill.*` values (enabled / mountPath / size / storageClassName /
  accessModes / drainInterval).
- A render-time fail-guard: `usage.spill.enabled` with `controller.replicas > 1`
  requires a `ReadWriteMany` access mode (a RWO spill PVC can't be multi-attached to
  standby replicas). `Chart.yaml` `0.1.26 → 0.1.27`.

## Design notes

- **Idempotent end to end.** Spill files and replayed PUTs are both keyed on
  `(sandbox_uid, interval_start)`, so re-spill overwrites and replay overwrites —
  never a double-bill, even across a controller restart mid-drain.
- **Fail-open never regressed.** Spilling is best-effort too: if the spill write
  fails (or no queue is configured) the interval is `dropped` and the transition
  still proceeds.
- **Drainer safety.** It only runs with a real sink (draining to a Noop sink would
  silently delete parked files); a not-yet-created spill dir is treated as empty.
- **HA.** Only the leader writes/drains, so a shared RWX volume is safe for
  multi-replica controllers; the chart enforces RWX for that case.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — controller green: 6 `usage_spill` tests (spill→drain roundtrip,
  idempotent re-spill, leave-on-failure-then-recover, Noop disabled, recording
  queue) + `emit_running_usage_spills_when_retries_exhausted` (always-fail sink +
  spill queue → parked then replayed) alongside the existing retry/drop tests.
  (One pre-existing node `gvisor.rs` test flakes under parallel load — different
  test each run, all 30 pass `--test-threads=1`; untouched by this change.)
- `helm lint` + `helm template` with `usage.spill.enabled=true`: PVC rendered,
  controller mounts it, env wired; the HA fail-guard fires for RWO + replicas>1.

## Out of scope

- Mid-run flushing of long-lived open intervals (#637).
- Pruning the raw-event prefix / spill retention (events kept for replay/backfill).
