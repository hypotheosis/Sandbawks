# Issue #350 — Fix: move envd's per-RPC metric recording off the request path

**Date:** 2026-06-05T16:25:51Z
**Branch:** `fix-issue-350-resume-readiness-gate-49983` (PR #355)
**Prior entries:** `…150932Z` (gate+diagnostic), `…153542Z` (envd-log evidence),
plus the bisect that confirmed the cause.

## Root cause (confirmed)

The post-resume `:49983` gRPC hang is a **`runsc` memory-restore** hazard in the
OpenTelemetry SDK. envd builds the SDK with the async (`runtime::Tokio`)
processors — `PeriodicReader` (metrics) and `BatchSpanProcessor` (traces),
`crates/sandbawks-observability/src/init.rs:25,29,469,493`. `runsc checkpoint`
freezes those background tasks live; the thaw leaves the **metrics-aggregation
path wedged**, so a *synchronous* `record()` blocks forever.

Every inbound gRPC RPC recorded inline via `GrpcMetricsLayer`, so that wedge
stalled the entire `:49983` surface, while envd's layer-free `:49984` HTTP
listener kept serving — the exact `:49984`-works / `:49983`-hangs signature.

Confirmed empirically in CI: booting the sandbox with `OTEL_SDK_DISABLED=true`
(baked into the checkpoint via the smoke's `PAUSE_RESUME_SMOKE_SANDBOX_OTEL_DISABLED=1`
flag) made the post-resume `Process.List` + `/files/download` serve and the smoke
go green — single variable. The working HTTP path *creates* trace spans and
serves fine, so the tracer-in-request path is healthy; the **only** blocking
point in the gRPC serving path is the metric record.

## The fix

`crates/envd/src/grpc_metrics.rs`: per-RPC metric recording is moved OFF the
request path. The `RpcRecorder` drop guard now enqueues a small `Measurement`
through a bounded `std::sync::mpsc::sync_channel` with a non-blocking `try_send`;
a single dedicated OS thread owns the OTel instruments and performs the actual
`record()`/`add()`.

- **Serving an RPC never blocks on telemetry.** `try_send` returns immediately;
  a full queue *sheds* the sample. So a wedged meter can at worst stall the one
  recorder thread and drop samples — it can never stall a gRPC handler. This
  closes the #350 post-resume hang and is robust to *any* future meter stall.
- **Dedicated OS thread** (not a tokio task) so a blocked `record()` cannot tie
  up a runtime worker.
- **Bounded** (`QUEUE_DEPTH = 1024`) so a stalled recorder can't grow memory
  unboundedly; envd's gRPC surface is a low-frequency control channel.
- **Fail-safe spawn**: if the recorder thread can't spawn, the sender's receiver
  is dropped and every `try_record` sheds cleanly (per-RPC metrics off) instead
  of panicking the first RPC.

This is a correctness fix, not just a mask: "best-effort telemetry must never
block the data plane" is the right invariant. (It does mean post-restore metric
samples are dropped while the SDK is wedged — acceptable here, since gVisor
sandboxes can't reach the ClusterIP collector anyway; a deeper OTel
checkpoint/restore lifecycle fix is follow-up work.)

New unit tests:
- `try_record_sheds_samples_without_blocking_when_recorder_is_wedged` — a
  no-drain bounded channel stands in for a stalled recorder; once full, samples
  are shed and `try_record` returns promptly (a blocking impl would hang).
- `try_record_sheds_when_recorder_is_gone` — a disconnected recorder sheds
  cleanly rather than panicking.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — all pass (incl. the two new grpc_metrics tests +
  `pod_spec_for_gates_readiness_on_envd_grpc_port`).
- End-to-end gVisor pause/resume smoke: must be confirmed in CI (no kind/gVisor
  cluster reachable locally). Expectation: with OTel left ENABLED (default), the
  post-resume `Process.List` + `/files/download` now serve and the smoke is
  green — the inverse of the bisect run.
