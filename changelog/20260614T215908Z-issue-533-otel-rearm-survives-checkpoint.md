# Issue #533 — OTel tokio tasks survive checkpoint + readiness gate on the wrong port

**Date:** 2026-06-14
**Branch:** `fix-issue-533-otel-rearm-survives-checkpoint`
**Labels:** area/envd, area/node, area/observability, kind/fix, prio/p1, runtime/gvisor

## Summary

Pause failed with `checkpoint failed: envd /__state timed out after 10s` on every
sandbox that had been through a pause/resume cycle. The #356 re-arm work was correct
in isolation, but two independent bugs in its implementation compounded to make it
self-defeating on restored pods. Both are fixed here.

## Bug 1 — OTel background Tokio tasks survived `runsc checkpoint`

`RearmHandle::quiesce` tore the stale OTel providers down on detached `std::thread`s
bounded by a 5s deadline. Each `provider.shutdown()` blocks on a *final flush* whose
export is bounded by the SDK's `max_export_timeout` — defaulting to **30s**. Against an
unreachable (or connected-but-silent) collector, that flush blocked far past the 5s
cap, so the shutdown thread was abandoned **with the underlying Tokio export task still
alive**. `runsc checkpoint` then captured that task mid-retry; on restore it resumed and
starved the runtime, so the HTTP accept loop on port 49984 was never scheduled again.

### Fix (`crates/sandbawks-observability`)

- `init.rs`: cap `max_export_timeout` on the trace + log batch processors and the metric
  reader's timeout at a bounded default (`DEFAULT_EXPORT_FLUSH_TIMEOUT = 3s`, honoring a
  *smaller* `OTEL_EXPORTER_OTLP_TIMEOUT`). With the export bound well under the quiesce
  deadline, each `shutdown` *completes* — its background task exits — instead of being
  abandoned. New helper `export_flush_timeout`.
- `rearm.rs`: `quiesce` now also installs a global `NoopTracerProvider` (alongside the
  existing `NoopMeterProvider`) so cached `global::tracer(...)` handles stop recording —
  belt-and-suspenders to the layer swap. Documented why the bounded export timeout, not
  the deadline, is what guarantees no live task survives checkpoint.

### Test

`shutdown_terminates_background_task_with_bounded_export` drives a *real* async-runtime
`BatchSpanProcessor` (200ms export bound) wrapped around an exporter that simulates a
dead collector and flips a flag on `Drop`. It asserts `shutdown_with_deadline` completes
promptly **and** the background task terminates (drops its exporter) — the exact property
that was violated. Plus a pure unit test for `export_flush_timeout`'s cap/override logic.

## Bug 2 — resume readiness gate probed the gRPC port, not HTTP

After `runsc restore`, the adapter waited on `self.envd_port`, whose default was the gRPC
port (49983). The gRPC port completes the TCP handshake and emits HTTP/2 frames before the
starved Tokio accept loop runs — a false-ready signal. The very next step posts `/__rearm`
on the HTTP port (49984), which was still unresponsive, so the re-arm silently timed out
(logged `WARN`, ignored). The sandbox went `Running` with a permanently starved HTTP
server, making every later pause time out at 10s.

### Fix (`crates/sandbawks-node`)

- `gvisor.rs`: change the `envd_port` default from `ENVD_GRPC_PORT` to `ENVD_HTTP_PORT`
  so readiness reflects the HTTP server that actually serves `/__rearm` and `/__state`.
  The resume call site keeps using `self.envd_port`, so `with_envd_port` test overrides
  still work (and this matches the already-written `with_envd_port` doc). Updated the
  field doc and resume-site comment.

### Test

`default_resume_readiness_probe_targets_http_port` and
`with_envd_port_overrides_resume_readiness_probe` lock in the new default and the override
seam.

## Interaction

The two bugs compound: Bug 1 causes the starvation; Bug 2 lets the re-arm that would fix
it fail silently and produces a false-ready (zombie) sandbox. With both fixed, quiesce no
longer strands a live exporter task across the checkpoint, and resume gates on the surface
that must actually be live — surfacing any residual starvation as a recoverable stuck
resume rather than a silent zombie.

## Validation

- `just fmt` — clean
- `just clippy` (`cargo clippy --workspace --all-targets -- -D warnings`) — clean
- `just test` (`cargo test --workspace --all-targets`) — 770 tests passed across 64 suites

## Review follow-up (PR #534)

- **Stale doc:** `ENVD_GRPC_PORT`'s doc comment claimed the resume TCP-bind wait
  targets the gRPC port; corrected to explain the wait deliberately avoids it.
- **Degenerate zero timeout:** `OTEL_EXPORTER_OTLP_TIMEOUT=0` parses as a valid
  zero `Duration` that would make every export time out instantly. Added
  `MIN_EXPORT_FLUSH_TIMEOUT = 1s` and switched `export_flush_timeout` to
  `clamp(MIN, DEFAULT)`; the unit test now covers the floor and a mid-range
  override.
- Declined two test-coverage suggestions as low-value: a `wait_for_tcp_bind`
  test would exercise the helper rather than the bug (the field-default test
  already proves the default + call-site read), and a hanging-`PushMetricExporter`
  test duplicates the trace shutdown-termination test while all three providers
  share the unit-tested `export_flush_timeout` helper.
