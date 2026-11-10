# Issue #189 — `GET /sandboxes/:id` and timeout-refresh endpoints (E2B SDK parity)

## Summary

The official `e2b` Python SDK's `Sandbox.get_info()` and
`Sandbox.set_timeout()` hit gateway endpoints that did not exist in
`crates/sandbawks-api`. This change adds them so those SDK methods work
against the gateway unchanged:

- `GET /sandboxes/:id` — single-sandbox info fetch. Backs
  `Sandbox.get_info()`.
- `POST /sandboxes/:id/timeout` — keep-alive refresh. Backs
  `Sandbox.set_timeout(seconds)`.
- `POST /sandboxes/:id/refreshes` — back-compat alias for the same
  operation, kept because the issue named it explicitly.

The three previously-skipped compat tests (`test_get_info`,
`test_set_timeout`, `test_is_running`) move from `test_skipped.py` back
into `test_lifecycle.py` with real bodies.

## Contract verification (against the installed SDK, not the issue text)

The issue described `set_timeout` as `POST /sandboxes/:id/refreshes`
with `{ "duration": N }` and "extends `endAt` by the requested
duration". The installed SDK (`e2b>=2.21`, the actual CI validation
target) disagrees on two points, so the implementation follows the
SDK:

1. **Endpoint.** `Sandbox._cls_set_timeout` posts to
   `POST /sandboxes/:id/timeout` with `{ "timeout": N }`, *not*
   `/refreshes`/`{duration}`. The `refreshes` route still exists in the
   generated client, so both are registered as aliases — `/timeout` is
   the one the SDK actually calls.

2. **Semantics.** The `timeout` field is documented in the generated
   client as "seconds from the current time after which the sandbox
   should expire" — i.e. keep-alive relative to *now*, not an additive
   bump of `endAt`. The handler therefore sets
   `spec.timeoutSeconds = elapsed_since_creation + requested`, so
   `endAt = creationTimestamp + timeoutSeconds ≈ now + requested`.

3. **`is_running`.** The issue assumed `is_running()` is
   `get_info().state == "running"` (→ `GET /sandboxes/:id`). The
   installed SDK instead probes the in-sandbox envd `/health` route
   through the per-sandbox proxy (#191). The test was moved anyway
   (it's part of the lifecycle surface) but its docstring records the
   accurate routing.

`GET /sandboxes/:id` returns the existing `SandboxDTO`: the SDK parses
it via `SandboxDetail.from_dict`, whose required fields
(`sandboxID`/`templateID`/`clientID`/`envdVersion`/`state`/`cpuCount`/
`memoryMB`/`diskSizeMB`/`startedAt`/`endAt`) are exactly the set the
DTO already emits, so no new response shape was needed.

## Changes

- `crates/sandbawks-api/src/routes/sandboxes.rs`
  - `SandboxOps::patch_timeout_seconds` (trait + `KubeSandboxOps`
    merge-patch + `FakeSandboxOps`).
  - `SetTimeoutBody { timeout }` and `RefreshBody { duration }` DTOs.
  - `get_sandbox` handler (gated on `sandboxes:get`).
  - `refresh_sandbox_timeout` shared logic + `set_sandbox_timeout`
    (`/timeout`) and `refresh_sandbox` (`/refreshes`) handlers (gated on
    `sandboxes:update`), both returning `204`.
  - Routes: `GET /sandboxes/:id`, `POST /sandboxes/:id/timeout`,
    `POST /sandboxes/:id/refreshes`.
- `crates/sandbawks-api/tests/sandboxes.rs` — five new route tests
  (GET shape + 404, `/timeout` bump + 204, `/refreshes` alias, timeout
  404).
- `tests/e2b-sdk-compat/tests/test_lifecycle.py` — three real lifecycle
  tests added; module docstring updated.
- `tests/e2b-sdk-compat/tests/test_skipped.py` — three placeholders
  removed.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace green (Rust route tests included).
- `uv run pytest --collect-only` — 28 tests collected, no import
  errors; the three moved tests now appear under `test_lifecycle.py`.

The three live SDK tests require a running gateway
(`SANDBAWKS_KIND_GVISOR=1 just cluster-up` + `just sdk-smoke`); that is
CI-gated against the kind cluster and was not run locally (no gateway on
`localhost:8080` in this environment). The Rust route tests exercise the
same request/response contract against `FakeSandboxOps`.
