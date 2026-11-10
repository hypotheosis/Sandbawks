# Issue #413 — Add quiesce/rearm failure counters for alertable OTel-lifecycle SLO

**Date:** 2026-06-13T23:01:23Z
**Branch:** `sandbawks/sandbawks-413`

## Problem

PR #412 wired envd's OTel lifecycle (`/__quiesce` before `runsc checkpoint`,
`/__rearm` after the post-restore bind wait) into the node adapter's pause/resume
paths. Both calls are best-effort: a failure is logged at `warn!` and pause/resume
continues.

The only signal of a quiesce/rearm failure was the DaemonSet log line. The
`envd_quiesce` / `envd_rearm` phase marks feed the `node.snapshot.phase.duration`
histogram (#373), but that histogram ticks on *either* outcome — it measures wall
time, not success. There was no outcome metric an operator could alert on, so an
SRE had to grep DaemonSet logs to discover a fleet dropping spans across
checkpoints (quiesce failing) or losing telemetry on restored sandboxes (rearm
failing).

## Change

Added one low-cardinality OTel counter, incremented on **both** the `Ok` and
`Err` paths of the two call sites in `crates/sandbawks-node/src/gvisor.rs`:

| Instrument | Type | Attributes |
|---|---|---|
| `node.envd.lifecycle_total` | `Counter<u64>` | `endpoint`, `outcome` |

It lives under the meter `sandbawks-node.envd`. The `endpoint` label carries the
endpoint path (`/__quiesce`, `/__rearm`) and distinguishes the two lifecycle
calls; the `outcome` label is `success` / `failure`. Both label sets are tiny and
code-fixed, so the series stays bounded. Deliberately **no** pod IP or sandbox ID
label, per the issue's bounded-cardinality requirement.

This gives the SRE side a quiesce/rearm-failure-*rate* SLO entirely within one
series — `sum(outcome="failure") / sum(...)` per `endpoint` — with no log
indexing and no second series to join for the denominator.

### Design note (review #248 follow-up)

The initial revision shipped two failure-only counters
(`node.envd.quiesce_failures_total` / `node.envd.rearm_failures_total`). Review
feedback flagged that (a) a per-counter `endpoint` label was redundant with the
metric name, and (b) a failure-only counter has no denominator for a *rate*
alert. Both are resolved by collapsing to a single `*_total` with an
`outcome`-as-label shape — mirroring the sibling `envd_digest_metrics` /
`validate_metrics` gates — where `endpoint` now earns its place by distinguishing
the two calls and `outcome` supplies the denominator in-series.

## Files

- `crates/sandbawks-node/src/envd_lifecycle_metrics.rs` (new) — the cached
  instrument singleton, an `EnvdLifecycleCall` enum (closed `endpoint`
  vocabulary), and a generic `record_lifecycle(call, &result)` entry point that
  takes the call's `Result` directly. Mirrors the existing `validate_metrics.rs`
  / `envd_digest_metrics.rs` pattern (OnceLock cache, no-op-meter safe, zero cost
  when `OTEL_SDK_DISABLED`).
- `crates/sandbawks-node/src/lib.rs` — register the new module.
- `crates/sandbawks-node/src/gvisor.rs` — bind the quiesce/rearm `Result`,
  record the outcome, then branch on `Err` for the existing `warn!`, in
  `GvisorAdapter::pause` and `GvisorAdapter::resume`.

## Validation

- `just fmt`
- `cargo clippy --all-targets -- -D warnings`
- `just test`

Unit tests in the new module assert the `endpoint` label for both calls and
record every `(call, outcome)` combination against the no-op meter to prove the
pause/resume paths never panic on the recording call.
