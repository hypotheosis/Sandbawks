# Add metrics for the operational spans instrumented in #371 (issue #373)

**Date:** 2026-06-07
**Issue:** #373 — feat(observability): add metrics for the operational spans
instrumented in #371.

## Summary

PR #372 (issue #371) added `#[tracing::instrument]` **spans** to operational
functions across the controller, node, and api crates so traces show the full
call tree. Many of those spans are also good **metric** candidates. This change
closes that gap with a *curated* set of instruments — not a 1:1 mirror of every
span (a span per op is cheap; a metric series per op multiplies cardinality) and
not a re-meter of what already exists (`reconcile_metrics`, `http_metrics`,
`warm_claim_metrics`, `snapshotjob_metrics`, the s3/envd-digest counters, …).

Each new instrument lives in a per-concern `*_metrics.rs` module following the
`reconcile_metrics.rs` template: cached `OnceLock` instruments off
`global::meter(...)`, free `record_*` functions, all safe against the no-op
meter when `OTEL_SDK_DISABLED` is set. The labels reuse the exact
low-cardinality `&'static str` vocabularies the #372 spans already chose.

## New instruments

| Instrument | Type | Unit | Attributes | Mirrors span |
|---|---|---|---|---|
| `controller.dispatch.action_total` | `Counter<u64>` | --- | action | `dispatch:controller::action` |
| `controller.dispatch.duration` | `Histogram<f64>` | s | action | `dispatch:controller::action` |
| `controller.pod_op.duration` | `Histogram<f64>` | s | op | `pod:controller::{create,delete,ready,terminated}` |
| `controller.pod_op.outcome_total` | `Counter<u64>` | --- | op, outcome | `pod:controller::{create,delete,ready,terminated}` |
| `node.snapshot.phase.duration` | `Histogram<f64>` | s | phase | pause/resume/`fetch_verify_and_lay_out` `PhaseTimer` |
| `node.validate.outcome_total` | `Counter<u64>` | --- | level, outcome | `validate:node::restorable` |
| `api.kube_op.duration` | `Histogram<f64>` | s | resource, op | `kube:api::*` (Sandbox ops + warm-claim) |
| `api.kube_op.outcome_total` | `Counter<u64>` | --- | resource, op, outcome | `kube:api::*` (Sandbox ops + warm-claim) |

## Changes by crate

### Controller (`crates/sandbawks-controller/src`)

- **`dispatch_metrics.rs`** (new) — `controller.dispatch.{action_total,duration}`
  keyed on `Action::label()`. Recorded from the `dispatch()` call site in
  `reconcile.rs` (the action label is captured before `dispatch` consumes the
  `Action`, the call is timed, and the metric is recorded regardless of
  outcome — failure folds are #339, not this counter).
- **`pod_op_metrics.rs`** (new) — `controller.pod_op.{duration,outcome_total}`
  for the four Pod-adapter ops. A `timed(op, fut)` wrapper records around each
  `PodAdapter` method body in `pod_adapter.rs`. A not-ready probe
  (`pod_ready → Ok(None)`) records `success` (the call succeeded).

### Node (`crates/sandbawks-node/src`)

- **`snapshot_phase_metrics.rs`** (new) — `node.snapshot.phase.duration`,
  promoting the existing `PhaseTimer` breakdown (already logged as a structured
  `phases="…"` line) into a histogram. A new `PhaseTimer::record_metrics()` is
  called alongside the emit in pause / resume / `fetch_verify_and_lay_out` in
  `gvisor.rs`, so it fires only on fully-successful pipelines (the same
  contract as the log line). Marks are recorded in seconds (ms / 1000).
- **`validate_metrics.rs`** (new) — `node.validate.outcome_total{level,outcome}`.
  A new `ValidateLevel::label()` provides the `level` label; `validate_restorable`
  is wrapped so every exit path (the `Off`/`Head`/`Sha` early returns and the
  `Full` round-trip tail) funnels through one record call.

### API (`crates/sandbawks-api/src`)

- **`kube_op_metrics.rs`** (new) — `api.kube_op.{duration,outcome_total}` keyed
  on `resource` + `op`. A `timed(resource, op, fut)` wrapper records around each
  `KubeSandboxOps` method (`routes/sandboxes.rs`) and each `KubeWarmClaim`
  method (`warm_claim.rs`). This is the **kube-client** layer — one point per
  apiserver round-trip — and is deliberately distinct from the request-level
  `http.server.*` and the claim-orchestration `warm.claims_total`, so it does
  not double-count. A `relabel_claim` that loses its CAS folds 409/404 to
  `Ok(false)` and therefore records `success` here (the lost race is the
  orchestration layer's `warm.claims_total{outcome="race_lost"}`).

### Docs

- `docs/observability.md` gains a "Metrics that mirror operational spans"
  subsection in the developer guide: the catalog table above plus a note on why
  `api.kube_op.*` is a distinct layer from the existing request-level metrics.

## Validation

- `just fmt` ✅
- `cargo clippy --all-targets -- -D warnings` ✅
- `just test` ✅ (per-module `recording_against_the_noop_meter_does_not_panic`
  tests, `timed` pass-through tests, and a `ValidateLevel::label()` mapping test
  added; full workspace suite green)

E2E confirmation against a live OTLP collector / Traceway `query_metrics` is
deferred to a cluster run (no collector wired into the local build).
