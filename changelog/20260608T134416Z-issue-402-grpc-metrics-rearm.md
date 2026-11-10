# envd gRPC metrics recorder: rebuild instruments on OTel re-arm (issue #402)

- **Date:** 2026-06-08
- **Issue:** #402 (part of #356 — envd OTel survives `runsc` checkpoint/restore)
- **Depends on:** #400 (re-arm semantics / `set_meter_provider` ordering), #401
  (`/__quiesce` + `/__rearm` endpoints)
- **Crates touched:** `envd`

## Problem

The background gRPC metrics recorder added in #355 (`crates/envd/src/grpc_metrics.rs`)
builds its OTel instruments (`RpcMetrics`) **once**, at recorder-thread start, off
the global meter (`global::meter()`). In OpenTelemetry 0.32 `global::meter()` binds
**eagerly**: the returned `Meter` (and every instrument built from it) is pinned to
whichever `MeterProvider` was global at call time, and a later
`global::set_meter_provider(new)` is invisible to it.

After a `runsc` checkpoint/restore the node re-arms telemetry via
`RearmHandle::rearm()`, which swaps the whole OTLP provider stack and calls
`set_meter_provider(new)`. The recorder's cached instruments stay bound to the
**old, shut-down** provider, so post-resume per-RPC duration/count metrics are
recorded into a dead pipeline and silently vanish.

## Fix

Make the recorder rebuild its instruments when telemetry re-arms, without
weakening the #350 invariant (the per-RPC path never blocks).

- The recorder channel item changes from `Measurement` to an enum
  `RecorderMsg { Record(Measurement), Rearm }`. Folding the control signal into
  the existing bounded queue keeps the recorder a single `recv()` loop and
  preserves strict ordering: a `Rearm` enqueued after a burst of records rebuilds
  only once those records have drained onto the old provider.
- `run_recorder` now delegates to a small generic `run_recorder_loop(rx, build)`.
  On `Record` it emits as before; on `Rearm` it calls `build()` to rebuild
  `RpcMetrics` against the now-current global meter. Production passes
  `RpcMetrics::new`; the `build` seam exists so the unit test can drive the loop
  with deterministic, provider-pinned instrument sets instead of racing a
  background thread's lazy bind.
- A new `pub fn rearm()` enqueues `RecorderMsg::Rearm` via the same non-blocking
  `try_send`. A wedged or vanished recorder can only shed the signal (logged at
  `warn`), never block the caller — the #350 invariant is intact.
- The `POST /__rearm` handler (`crates/envd/src/bin/envd.rs`) calls
  `grpc_metrics::rearm()` **after** `rearm.rearm()` returns `Ok` — i.e. after the
  internal `set_meter_provider(new)` — so the rebuild reads the fresh provider.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all workspace tests pass.
- New unit test `rearm_rebuilds_instruments_against_the_new_meter_provider`: two
  in-memory `SdkMeterProvider`s stand in for the pre-/post-restore SDK; the test
  proves the pre-rearm record lands on the original provider and the post-rearm
  record lands on the NEW provider, i.e. `Rearm` rebuilt the instruments. The test
  fails if the rebuild is removed.
- A `testing`-feature `opentelemetry_sdk` dev-dependency was added to `envd` for
  the in-memory metric exporter; production `grpc_metrics` still depends only on
  the OTel API.

## Out of scope

End-to-end re-arm validation across a real `runsc` checkpoint/restore with a live
OTLP collector belongs to #356 Phase 4 (node orchestration) and is not exercised
here — there is no standalone runnable artifact in this change beyond the unit
test, which drives the real recorder loop directly.
