# Observability: per-node SnapshotJob claims counter + watched-namespace gauge

**Date:** 2026-06-06T17:37:33Z

## Summary

`sandbawks-node` now emits two new OTel instruments on its `SnapshotJob`
reconciler so that "a node is processing no work" is a queryable metric, not a
post-mortem log grep:

- `snapshotjob.claims_total` (`Counter<u64>`, labels `node`, `mode`) —
  incremented the moment the reconciler commits to dispatching a job.
- `snapshotjob.watch_info` (`Gauge<u64>`, constant `1`, labels `node`,
  `namespace`) — the info-gauge naming the namespace each node's watch is
  scoped to, recorded once at controller boot.

Both live under the existing meter `sandbawks-node.snapshotjob`.

## Motivation

The namespace-mistargeting bug (#333, fixed in #334) left the warm pool wedged
at `READY=0` for ~11h before anyone noticed: the node was silently watching the
wrong namespace, so it claimed **zero** SnapshotJobs, and the only signal that
named the watched namespace was a single boot-time `tracing::info!`
(`reconcile.rs`). There was no per-node "is this node draining work?" metric and
nothing made the watched namespace queryable.

Issue #335 asked for a claims counter plus a rate alert that would have caught
this in minutes instead of hours.

## What was added

- **`snapshotjob.claims_total`** — recorded in `reconcile()` *before* the
  dispatch runs (not after, like the pre-existing `executions_total`). Recording
  at claim-time means a node stuck mid-pause still shows it picked up work, so
  the alert keys on genuine idleness rather than in-flight latency. Labelled by
  `node` (the dimension #333 lacked) and `mode`; deliberately *not* by `status`
  — a claim is a claim whether the work later succeeds or fails.
- **`snapshotjob.watch_info`** — recorded once in `run()` right after the
  boot-time log line, using the `kube_pod_info`-style info-gauge pattern. Makes
  `snapshotjob_watch_info{node="..."}` resolve to the watched namespace for the
  process lifetime, so the #333 mistarget would have been visible the instant
  the claims alert fired.
- **`record_claim` / `record_watch_namespace`** functions in
  `snapshotjob_metrics.rs`, both safe against the no-op meter (no provider
  installed in unit tests), with the `recording_against_the_noop_meter_does_not_panic`
  test extended to cover them.

## Why `claims_total` is not redundant with `executions_total`

`executions_total{mode,status}` is recorded *after* a dispatch returns and is
unlabelled by node — it answers "what is the success/failure mix per mode?"
`claims_total{node,mode}` is recorded *before* dispatch — it answers "is each
node draining work at all?" Different timing, different label set, different
question (mirrors how the controller carries both `outcome` and `requeue`).

## Alerting

The alert is documented as a PromQL expression in OBSERVABILITY.md §8.3, per the
repo convention (cf. the warm-pool thrash alert in §8.7): cluster-wide claim
rate `== 0` for 15m, gated on a warm pool sitting below its floor
(`warm_pool_ready - warm_pool_min_ready < 0`) so it stays quiet during idle
periods. Wiring the expression into Traceway alert routing is the downstream
vendor's job (§7). A broader gate keyed on a `Sandbox.status.phase=Pausing`
count would also catch user-sandbox pause/resume starvation, but no cluster-wide
phase-count series is emitted today (the controller reconciles one Sandbox at a
time) — noted in §8.3 as a possible follow-up rather than silently assumed.

## Local emission smoke

`just otel-emit-smoke` (`scripts/otel-emit-smoke.sh`) stands up a throwaway
`opentelemetry-collector-contrib` in Docker — the same image as the kind
fixture, no cluster needed — runs the new `emit_snapshotjob_metrics` example
through the *real* OTel SDK (not the no-op meter the unit tests exercise), and
asserts both `snapshotjob.claims_total` and `snapshotjob.watch_info` reach the
collector's debug exporter with the expected `node` / `mode` / `namespace`
attributes. This is the local complement to the cluster-bound
`just otel-arrival-smoke` (#311): it proves the emission path without a gVisor
cluster. Verified passing locally against collector image `0.123.0`.

## Files touched

- `crates/sandbawks-node/src/snapshotjob_metrics.rs` — two new instruments,
  `record_claim` / `record_watch_namespace`, doc + tests.
- `crates/sandbawks-node/src/reconcile.rs` — claim recorded at the dispatch
  point in `reconcile()`; watch_info recorded at boot in `run()`.
- `crates/sandbawks-node/examples/emit_snapshotjob_metrics.rs` — local emission
  harness driving the real SDK through the new instruments.
- `scripts/otel-emit-smoke.sh` + `justfile` (`otel-emit-smoke`) — self-contained
  Docker-collector smoke asserting both series arrive over OTLP.
- `OBSERVABILITY.md` — §8.3 updated with the new instruments, the alert, and the
  local emission smoke.
