# OTel Phase 6 — metrics across api, controller, node, envd

**Date:** 2026-06-03T20:30:00Z
**Issue:** #300 (parent epic #294)
**Branch:** `feat-issue-300-otel-metrics`

## Summary

Emits OpenTelemetry metrics from every service through the global meter provider
wired by `init_telemetry` in Phase 2. Each binary gains one small per-crate
metrics module with `OnceLock`-cached instruments and bounded label cardinality
(OBSERVABILITY.md §8). When the SDK is disabled the global meter is a no-op, so
all instrumentation stays installed unconditionally with zero hot-path cost.

This unblocks #277, #286, and #288, which were deferred metrics follow-ups
waiting for a metrics exporter to land.

## Changes

### api — HTTP server metrics

`crates/sandbawks-api/src/http_metrics.rs` (new): an `axum::middleware::from_fn`
handler emitting the three OTel-semconv HTTP-server instruments under the meter
`sandbawks-api.http.server`:

- `http.server.request.duration` (`Histogram<f64>`, s) — method, route, status
- `http.server.active_requests` (`UpDownCounter<i64>`) — method, route
- `http.server.request.count` (`Counter<u64>`) — method, route, status

Route label is axum's `MatchedPath` (e.g. `/sandboxes/:id`), with an
`unmatched:<path>` fallback for cardinality visibility. Installed as the
outermost `Router::layer` in `bin/sandbawks-api.rs`, which applies per matched
route so `MatchedPath` is populated when it runs.

### controller — reconcile metrics

`crates/sandbawks-controller/src/reconcile_metrics.rs` (new), under the meter
`sandbawks-controller.reconcile`:

- `controller.reconcile.duration` (`Histogram<f64>`, s) — kind, outcome
- `controller.reconcile.outcome_total` (`Counter<u64>`) — kind, outcome

`kind` ∈ {`Sandbox`, `SandboxWarmPool`}; `outcome` ∈ {`success`, `error`,
`requeue`}. The two reconcile adapters in `bin/sandbawks-controller.rs` time the
reconcile and record success/error; the two error policies record the
counter-only `requeue` (the error policy has no meaningful latency, so it is not
fed into the histogram).

### node — SnapshotJob metrics

`crates/sandbawks-node/src/snapshotjob_metrics.rs` (new), under the meter
`sandbawks-node.snapshotjob`:

- `snapshotjob.duration` (`Histogram<f64>`, s) — mode, status
- `snapshotjob.executions_total` (`Counter<u64>`) — mode, status

`mode` ∈ {`pause`, `resume`, `stage`}; `status` ∈ {`success`, `error`}. In
`reconcile.rs` the `match job.spec.mode { … }` dispatch is extracted into a
private `dispatch` fn, and `reconcile` times exactly that call and records the
metric once on its result. The reconcile short-circuits (terminal / in-flight
idempotency floors) `return` *before* `dispatch` is even called and are
deliberately not counted — one logical pause/resume/stage yields exactly one
point per instrument.

### envd — gRPC server metrics

`crates/envd/src/grpc_metrics.rs` (new): a `tower::Layer` (sibling of
`trace::GrpcTraceLayer`) installed once on the tonic server in `bin/envd.rs`,
under the meter `envd.grpc.server`:

- `rpc.server.duration` (`Histogram<f64>`, s) — rpc.system, rpc.service, rpc.method
- `rpc.server.request.count` (`Counter<u64>`) — rpc.system, rpc.service, rpc.method

Attributes follow the OTel RPC semconv: the gRPC path `/package.Service/Method`
is split into `rpc.service` + `rpc.method` with `rpc.system="grpc"`. Both
instruments are recorded by an `RpcRecorder` drop guard so a cancelled RPC is
still counted. Duration is time-to-response. The inner future is boxed
(recording is driven by a guard inside it) rather than hand-rolling a `Future`
with manual pin projection, which would require `unsafe`; envd's gRPC surface is
a low-frequency control channel, so the per-RPC allocation is acceptable.

### Dependencies & docs

- `opentelemetry = { workspace = true }` added to the four consuming crates
  (API only — the bootstrap crate still owns all SDK wiring).
- New `pub mod` declarations in each crate's `lib.rs`.
- OBSERVABILITY.md §8 rewritten to describe the four sandbawks metric sources
  (it previously quoted the imported `wnw-backend` reference).

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace green; 7 new unit tests:
  - api: `route_label` unmatched-marker fallback; `track_metrics` passes a
    routed request through unchanged (matched-route branch).
  - controller: `outcome_label` Ok/Err mapping; recording against the no-op
    meter does not panic.
  - node: `mode_label` covers every `SnapshotJobMode`; recording does not panic.
  - envd: the layer passes the inner service's response through unchanged.

End-to-end collector validation (HTTP-server instruments with bounded route
labels; `snapshotjob.*` points over a pause/resume cycle) requires a running
OTLP collector + cluster and is not reproducible in this local environment — see
the PR's manual test plan.
