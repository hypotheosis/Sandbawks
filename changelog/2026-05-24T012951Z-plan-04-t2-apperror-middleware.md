# Plan 04 T2 — `AppError` + request-id/trace/timeout middleware

**Date:** 2026-05-24T01:29:51Z
**Branch:** `feat-issue-101-apperror-middleware`
**Issue:** #101
**Plan reference:** `docs/superpowers/plans/2026-05-23-sandbawks-04-api-gateway.md` § Task 2

## Summary

Replaced the Task-1 placeholders in `crates/sandbawks-api/src/error.rs`
and `crates/sandbawks-api/src/middleware.rs` with the real implementations
the rest of Plan 04 will build on. Adds an integration test that locks
the E2B-compatible JSON error shape in place so future router work can
trust the contract.

## Changes

### `crates/sandbawks-api/src/error.rs`

- Defined `AppError` enum with six variants: `NotFound`, `BadRequest`,
  `Unauthorized`, `Forbidden`, `UpstreamUnavailable`, `Internal`.
- Each non-internal variant carries a `&'static str` E2B error code and
  a human-readable message; `Internal` boxes `anyhow::Error`.
- Implemented `From<kube::Error>` (404 → `NotFound`, everything else →
  `UpstreamUnavailable` with a `warn!` log) and
  `From<axum::extract::rejection::JsonRejection>` (→ `BadRequest`).
- Implemented `From<tonic::Status>` mapping `NotFound`/`InvalidArgument`/
  `Unavailable` to specific E2B codes; everything else falls into
  `Internal` with a `warn!` log so the cause is recoverable from logs.
- `IntoResponse` for `AppError`: picks the right `StatusCode`, builds the
  JSON envelope `{ code, message, request_id }`, and stamps the
  `X-Request-Id` header with the same value so header and body always
  agree. Internal errors are logged at `error!` before rendering.
- Locally generated `req_<32-hex>` request-id (timestamp-derived) means
  the body field is never missing even on error paths that bypass the
  middleware-installed extension.

### `crates/sandbawks-api/src/middleware.rs`

- `request_id_layer()` — `SetRequestIdLayer<Counter>` with a custom
  `MakeRequestId` impl that stamps `X-Request-Id: req_<32-hex>` on
  every incoming request. Matches the format used by `error.rs`.
- `trace_layer()` — `TraceLayer::new_for_http()` with default span /
  request / response handlers; headers are excluded from the span by
  default to avoid leaking auth headers into logs.
- `timeout_layer(secs: u64)` — `TimeoutLayer::with_status_code(...)` with
  a 408 status code. Uses the non-deprecated `with_status_code` form so
  `cargo clippy -- -D warnings` stays green on `tower-http >= 0.6.7`.

### `crates/sandbawks-api/tests/error.rs` (new)

End-to-end render test that wires a `boom` handler into a router, layers
on `request_id_layer()`, and asserts:

- Status is `404 Not Found`.
- Response carries `X-Request-Id`.
- Body is `application/json`.
- Body JSON matches `{ "code": "sandbox_not_found", "message": "...",
  "request_id": <header value> }`.

### `crates/sandbawks-api/Cargo.toml`

- Added `http-body-util = "0.1"` to `[dev-dependencies]` so the
  integration test can collect the response body.

## Verification

```bash
just fmt
cargo clippy --all-targets -- -D warnings
just test
```

All three pass. The new test `error::renders_e2b_json_shape` exercises
the full IntoResponse path.

## Notes

- `From<tonic::Status>` was added even though no handler imports `tonic`
  yet — Task 9 (envd client pool) will, and the impl keeps the
  variant-to-code mapping in one place.
- The request-id generated inside `IntoResponse::into_response` is
  deliberately *not* threaded from the request extensions: `IntoResponse`
  doesn't get a `Request` reference, and we need the JSON body field to
  match the response header value. Threading the middleware-set id
  through to the body is a follow-up if it becomes load-bearing for
  log correlation across services.
