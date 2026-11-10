# Warm-pool + claim observability metrics (issue #288)

**Date:** 2026-06-03T23:26:07Z
**Issue:** #288 — *Warm-pool + claim observability: Prometheus counters/histogram/gauge*
**Branch:** `feat-issue-288-warm-pool-claim-metrics`

## Context

PR #287 added structured `tracing` logs for the warm-pool reconcile
(`crates/sandbawks-controller/src/warm_pool.rs`) and the claim fast path
(`crates/sandbawks-api/src/warm_claim.rs`), but **deferred the metrics**: at the
time the workspace had no metrics pipeline — no installed recorder, so
`counter!`/`histogram!`/`gauge!` would have been no-ops. Issue #288 tracked
wiring the same values into a real recorder once one landed.

That blocker is now gone. OTel Phases 6–7 (PRs #317–#320) wired an OTLP metrics
path into the workspace and established the instrument convention across four
sources (`http_metrics.rs`, `reconcile_metrics.rs`, `snapshotjob_metrics.rs`,
`envd_digest_metrics.rs`): instruments built once off the global meter and cached
in a `OnceLock`, no-op-safe when the SDK is disabled. This change adds the
warm-pool/claim instruments following that exact convention. The `tracing` fields
PR #287 emits already carry the right labels, so the wiring is mechanical.

## Change

### api — `crates/sandbawks-api/src/warm_claim_metrics.rs` (new)

Meter `sandbawks-api.warm_claim`:

- `warm.claims_total` (`Counter<u64>`) `{pool, outcome}`. `outcome` extends the
  issue's four-value vocabulary with a fifth `error` outcome (added in review,
  #321) so the counter partitions *every* claim attempt: the four terminal
  outcomes `hit` / `miss_no_pool` / `miss_drained` / `error` partition requests;
  `race_lost` is emitted once per individual lost compare-and-swap as an
  orthogonal contention signal. The hit-rate denominator pins
  `outcome=~"hit|miss_no_pool|miss_drained"`, excluding both `race_lost` and
  `error` (an errored claim is already counted as a failure in
  `http.server.request.count`).
- `warm.claim.duration` (`Histogram<f64>`, s) `{pool}` — the claim-to-resume
  handoff, recorded only on a `hit` (the only path that resumes), so its count
  tracks `warm_claims_total{outcome="hit"}`.

`warm_claim::claim_for` records these at each exit: `miss_no_pool` on the
no-pool early return, `error` (via `map_err`) on any propagated backing-store
failure, `race_lost` per lost CAS, `hit` + the handoff histogram after the
resume patch returns, and `miss_drained` on the drained fall-through. A
`std::time::Instant` started at entry times the handoff.

### controller — `crates/sandbawks-controller/src/warm_pool_metrics.rs` (new)

Meter `sandbawks-controller.warm_pool`, all `{pool}`-labelled:

- Gauges `warm_pool.ready` / `warm_pool.min_ready` / `warm_pool.max_ready` —
  plot ready against the configured band without scraping CR status.
- Counters `warm_pool.created_total` / `warm_pool.deleted_total` /
  `warm_pool.pause_requested_total` — expose the create↔delete thrash loop and
  the auto-pause edge.

`warm_pool::reconcile_pool` records one `PoolObservation` per pass at the
existing summary-log site, fed the same observed-vs-target counts as the `info`
log so metric and log stay in lock-step. Counters are only `add`ed when their
delta is non-zero, keeping the steady-state series quiet between real actions.

### Docs

- `OBSERVABILITY.md` §§8.6–8.7 document both sources, the four-outcome semantics,
  the hit-rate PromQL (with the `race_lost` exclusion), and the thrash alert; the
  §8 intro now reflects that api/controller own a second metric source each.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- Unit tests (`#[cfg(test)]` in each new module) prove label mapping and no-op
  safety against the global no-op meter.
- **Real-recorder emission tests** prove the issue's deliverable end-to-end:
  `crates/sandbawks-api/tests/warm_claim_metrics.rs` and
  `crates/sandbawks-controller/tests/warm_pool_metrics.rs` install a live
  `SdkMeterProvider` backed by an `InMemoryMetricExporter`, drive the real
  `claim_for` / `reconcile_pool` paths (hit / miss / race; top-up+pause / trim),
  force-flush, and assert every instrument emits under the expected meter scope.
  Each lives in its own integration-test binary so the production `OnceLock`
  singleton binds its cached instruments to the live provider rather than a
  no-op meter a sibling test might install first.
