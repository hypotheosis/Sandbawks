# Non-blocking `connect`/resume + explicit EAGAIN retry signal (issue #652)

**Date:** 2026-06-24T21:00:23Z
**Issue:** #652 — `connect`/resume blocks server-side until envd is ready, so the SDK's `AbortSignal` times out before the #641 not-ready park can help
**Crate:** `sandbawks-api` (`crates/sandbawks-api/src/routes/sandboxes.rs`, `crates/sandbawks-api/src/routes/envd_proxy.rs`)

## Summary

A client (the `zidane` harness, JS E2B SDK) tried to warm up a paused sandbox in
the AKS prod environment and timed out with the opaque DOMException
`"The operation timed out."` (`AbortSignal.timeout()`) — never receiving the
sandbox handle, even though the resume completed server-side.

Root cause: `POST /sandboxes/{id}/connect` blocked in a poll loop for up to
`CONNECT_TIMEOUT = 300s`, returning only once `status.envdAddress` appeared. The
E2B SDKs wrap each request in an `AbortSignal.timeout(requestTimeoutMs)`
(~30s by default), so any resume slower than that window aborted the client with
no handle. A filesystem-mode cold restore that needs a node scale-up plus a
~670 MB image pull is routinely ~90s — structurally past any per-request client
timeout.

The #641 not-ready park / keepalive-resignal could not help: it lives at the
**envd command-execution layer** (`envd_proxy.rs`, `process.Start`) and only
fires once a command stream exists against an already-connected sandbox. The
failure here was one layer earlier — the `connect` call itself — so no command
stream ever existed for the park to act on.

## Field evidence (AKS prod, `sbx-11b846e505a9`, 2026-06-24)

`ResumeRequested` → `FailedScheduling` (Insufficient cpu) → `TriggeredScaleUp`
(kata pool 1→2) → node scale-up wait → image pull **27.9s** → `ResumeSucceeded`,
**~90s total**. The client had already aborted; the orphaned sandbox then
idle-`AutoPause`d, `Suspended`, and was re-resumed (abort→idle-pause→re-resume
thrash).

## Changes

### 1. Non-blocking `connect_sandbox` (`sandboxes.rs`)

- Replaced the 300s block-until-`envdAddress` loop with a short **settle**:
  `RESUME_SETTLE_TIMEOUT = 5s`.
- Extracted the per-iteration decision into a pure, unit-tested helper
  `connect_step(&Sandbox, settle_elapsed) -> ConnectStep`:
  - `Ready` — `status.envdAddress` present → return the fully-ready DTO.
  - `Terminal` — `Terminating` → give-up error (`sandbox_failed`, 412) rather
    than a dead handle.
  - `Wait` — still resuming, inside the settle window → poll again.
  - `ReturnPending` — still resuming, window elapsed → return the handle
    optimistically; the SDK's first per-sandbox RPC drives readiness through the
    envd proxy, where the not-ready park / #641 keepalive-resignal bridge the
    remaining warm-up.
- `Failed` is intentionally **not** terminal here: re-requesting `Running` can
  drive an evicted-sandbox recovery (issue #518), and the envd proxy
  re-classifies terminal-vs-transient on the first RPC with fresher, watch-fed
  data.
- The 5s window also comfortably outlasts the watch-fed `EnvdCache` catching up
  to the `desiredPhase=Running` patch, so the first RPC parks rather than being
  rejected as `sandbox_paused`.

### 2. Explicit EAGAIN retry signal for not-ready unary ops (`envd_proxy.rs`)

- A transient (resuming/starting) sandbox's unary error — filesystem
  read/write and the other unary RPCs — now tags its message with the POSIX
  `EAGAIN` ("resource temporarily unavailable") vocabulary, reinforcing the
  502 / `UpstreamUnavailable` retry rail it already rides. A client (or an LLM
  driving a harness) reads it unambiguously as "retry later".
- The change is confined to `not_ready_error` (the unary path); the shared
  `not_ready_message` and the streaming #641 stderr notice are untouched.
- `sandbox_paused` (a deliberately paused sandbox with no resume in flight)
  stays the **distinct** "resume-required" precondition (403), not the
  retryable signal — so a caller does not retry-loop forever against a sandbox
  that nothing is resuming.

## Tests

- `connect_step_ready_when_address_present`
- `connect_step_waits_within_settle_window`
- `connect_step_returns_pending_after_settle_window`
- `connect_step_terminal_when_terminating`
- `connect_step_failed_is_not_terminal`
- `not_ready_error_transient_signals_eagain` (and existing
  `not_ready_error_maps_distinct_codes` still green)

`cargo test -p sandbawks-api --lib`: 120 passed. `cargo clippy -p sandbawks-api
--all-targets`: clean. `cargo fmt`: clean.

## Follow-ups / not in scope

- The cold-resume latency itself (kata-pool scale-up + 670 MB image pull) is a
  separate cost; keeping node headroom and shrinking/pre-pulling the image would
  cut it, but is orthogonal to this UX fix.
- For a >15s cold resume, the agent's first op should be a leading-`sleep`
  keepalive so #641's Mode B (300s) bridges it; a plain unary op only parks
  `silent_park_max` (15s) before returning the EAGAIN retry signal.
