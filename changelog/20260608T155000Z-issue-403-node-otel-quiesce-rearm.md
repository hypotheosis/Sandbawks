# Issue #403: envd OTel re-arm — node orchestration of quiesce/re-arm in gvisor.rs

**Date:** 2026-06-08T15:50:00Z
**Issue:** #403
**PR:** fix-issue-403-envd-otel-quiesce-rearm
**Dependencies:** #401 (`/__quiesce` + `/__rearm` endpoints), #402 (gRPC metrics rearm)

## Summary

Wires the node adapter (`GvisorAdapter` in `crates/sandbawks-node/src/gvisor.rs`) to
drive envd's OTel lifecycle around `runsc checkpoint` / `runsc restore`:

- **Pause path:** `POST /__quiesce` after the envd state fetch + TCP drain,
  immediately before `runsc checkpoint`. Flushes in-flight OTel spans so
  telemetry is not split across the checkpoint boundary.
- **Resume path:** `POST /__rearm` after `wait_for_tcp_bind` confirms envd is
  live. Re-arms the OTel SDK so telemetry resumes on the restored process.

Both calls are **best-effort**: failure is logged as a warning and the pause/resume
continues. A wedged OTel provider must never block a checkpoint; degraded telemetry
must never fail a resume.

## Changes

### `crates/sandbawks-node/src/gvisor.rs`

- **`EnvdStateClient` trait**: extended with `quiesce(&self, pod_ip) -> Result<(), RuntimeError>`
  and `rearm(&self, pod_ip) -> Result<(), RuntimeError>`.

- **`HttpEnvdProbe`**: implements both new methods.
  - `quiesce`: POST `/__quiesce` with `SO_LINGER(0)` RST close (same rationale as
    `fetch_state` — any open TCP endpoint in a non-CLOSED state fails `runsc checkpoint`).
    Timeout-bounded by the existing `HttpEnvdProbe::timeout`.
  - `rearm`: POST `/__rearm` with graceful close (restore is already complete).
    Timeout-bounded; error variant remapped to `RestoreFailed` for correct path attribution.

- **`post_endpoint_inner`**: new private async helper that implements an HTTP/1.1 POST
  over a raw `TcpStream` (matching `fetch_state_inner`'s pattern), with a `use_rst`
  flag to select RST vs graceful close. Injects W3C `traceparent` so envd can continue
  the pause/resume trace.

- **`GvisorAdapter::pause`**: inserts best-effort quiesce call after `probe_drain`,
  immediately before `runsc checkpoint`. Failure is logged; a new `envd_quiesce` phase
  is added to `PhaseTimer`.

- **`GvisorAdapter::resume`**: inserts best-effort rearm call after the `wait_for_tcp_bind`
  returns (i.e., envd is confirmed live). Failure is logged; a new `envd_rearm` phase
  is added to `PhaseTimer`. The `bind` return pattern was refactored to use `?` so the
  rearm call can follow the bind.

- **`fake::FakeEnvdState`**: extended with:
  - `quiesce_calls: Mutex<Vec<String>>` and `rearm_calls: Mutex<Vec<String>>` — call
    recorders so tests can assert the methods were called with the expected pod IP.
  - `with_quiesce_fail()` / `with_rearm_fail()` builder methods — failure injection for
    testing the best-effort tolerance.

### `crates/sandbawks-node/tests/gvisor.rs`

Two new integration tests added per the issue's validation requirements:

- **`pause_calls_quiesce_before_checkpoint_and_tolerates_failure`**: runs the full pause
  pipeline twice (quiesce succeeds, quiesce fails). Asserts pause completes both times
  and that `quiesce_calls` records exactly one call with the pod IP.

- **`resume_calls_rearm_after_bind_wait_and_tolerates_failure`**: runs the full resume
  pipeline twice (rearm succeeds, rearm fails). Asserts resume completes both times
  and that `rearm_calls` records exactly one call with the pod IP.

## Validation

- `just fmt` — passed
- `cargo clippy --all-targets -- -D warnings` — passed (0 warnings)
- `cargo test -p sandbawks-node` — 23 integration + 76 unit tests, all passed

## Phase timer additions

The `pause` and `resume` pipeline timing logs now include two new phases:
- `envd_quiesce` — wall-clock ms from end of probe_drain to end of quiesce call
- `envd_rearm` — wall-clock ms from end of envd_ready to end of rearm call
