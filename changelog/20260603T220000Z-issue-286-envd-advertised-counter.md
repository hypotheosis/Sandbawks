# envd-staged reconciler advertised counter (issue #286)

**Date:** 2026-06-03T22:00:00Z
**Issue:** #286 — *Promote envd-staged reconciler `result=` tracing to a Prometheus counter when a metrics exporter lands*
**Branch:** `feat-issue-286-envd-advertised-counter`

## Context

`run_envd_staged_reconciler` (`crates/sandbawks-node/src/node_label.rs`, #282)
re-asserts / withdraws the `sandbawks.io/envd-staged` node label every 30s as the
staged `envd` binary appears or disappears, so the controller's nodeAffinity gate
only schedules sandboxes onto nodes that can actually run them (#272). #285
proposed a `sandbawks_node_envd_advertised_total{result="ok|missing|patch_failed"}`
counter as the natural observability hook, but the workspace had no metrics
pipeline at the time, so it shipped a per-tick structured `tracing` event carrying
the same `result` field as a placeholder.

That blocker is now gone: OTel Phase 6 (PR #317) wired an OTLP metrics path into
the workspace and established the instrument convention in
`snapshotjob_metrics.rs` / `envd_digest_metrics.rs` (cached instruments off the
global meter, no-op-safe when the SDK is disabled). This change promotes the
placeholder, as the issue anticipated ("the label is already named to match the
proposed metric, so promotion is mechanical").

## Change

New module `crates/sandbawks-node/src/envd_advertised_metrics.rs`, modelled on
`envd_digest_metrics.rs`:

- One instrument `envd_advertised.total` (`Counter<u64>`) under the meter
  `sandbawks-node.envd_advertised`, with one bounded attribute `result`:

  | `result`       | Tick outcome                | Meaning                                       |
  |----------------|-----------------------------|-----------------------------------------------|
  | `ok`           | `AdvertiseOutcome::Advertised` | staged binary present; label re-asserted    |
  | `missing`      | `AdvertiseOutcome::Withdrawn`  | staged binary gone; label withdrawn         |
  | `patch_failed` | reconcile `Err(..)`            | label patch failed (RBAC/apiserver/conflict) |

- A `ReconcileResult` enum closes the label vocabulary and makes the `result`
  mapping exhaustively compiler-checked — the same idiom the two sibling metrics
  modules use.
- `record_envd_staged_tick(result)` increments the counter exactly once per
  call. The instrument is cached in a `OnceLock`; with `OTEL_SDK_DISABLED` or no
  installed provider the global meter is a no-op, so the call stays on the
  reconcile path unconditionally. (Named for the specific tick rather than a
  generic `record_reconcile` to stay unambiguous against the controller's
  existing `reconcile_metrics::record_reconcile` — PR #320 review #129.)

`run_envd_staged_reconciler` now calls `record_envd_staged_tick` in each of its three
match arms, alongside (not in place of) the existing `tracing` event — the two
share the exact `result` vocabulary, so dashboards/alerts written against the log
field carry straight over to the metric.

### Metric naming

The issue proposed `sandbawks_node_envd_advertised_total`, the pre-OTel name.
This crate's OTel instruments drop the `sandbawks_node_` prefix because it is the
instrumentation *scope*, not part of the instrument name (cf.
`snapshotjob.executions_total`, `envd_digest.checks_total`). The collector
exports this instrument as `envd_advertised_total{result=...}` after the OTLP
dotted-name → Prometheus underscore translation, matching the issue's intent.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all groups pass, including the two new unit tests
  (`label_covers_every_result`, `recording_against_the_noop_meter_does_not_panic`).

The counter's *emission* over a live OTLP pipeline requires the node binary
running against the e2e collector fixture (PR #318) and was not exercised locally;
metrics here are pushed via OTLP rather than scraped from a local `/metrics`
endpoint, so the issue's "scrapeable on the node binary's metrics endpoint"
acceptance is satisfied at the collector. The recording logic is covered by the
no-op-meter unit test.

## Follow-ups

The issue also suggests the same treatment for the controller's reconcile loops
"if a shared metrics module emerges". The three node metric modules
(`snapshotjob`, `envd_digest`, `envd_advertised`) now share an identical shape;
extracting a small shared counter helper is a reasonable next step but out of
scope here.
