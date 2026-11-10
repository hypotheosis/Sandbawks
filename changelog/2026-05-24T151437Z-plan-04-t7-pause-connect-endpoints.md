# Plan 04 T7 — pause + connect endpoints

Date: 2026-05-24T15:14:37Z
Issue: gfd/sandbawks#106 (Plan 04 epic #99)

## Scope

Adds the two remaining lifecycle endpoints to `routes::sandboxes`:

| Method | Path                       | Response                                                  |
|--------|----------------------------|-----------------------------------------------------------|
| POST   | `/sandboxes/:id/pause`     | `200 OK` + `{ sandboxID, state: "paused" }`               |
| POST   | `/sandboxes/:id/connect`   | `200 OK` + `{ sandboxID, envdURL: "ws://<addr>/" }` or 502 `envd_not_ready` |

Both handlers are purely declarative. They patch `spec.desiredPhase`
via `SandboxOps::patch_desired_phase` and let the controller drive the
runtime adapter (T4) to do the real work.

## What changed

- `crates/sandbawks-api/src/routes/sandboxes.rs`
  - Reshaped `ConnectResponse` from `{ sandboxID, envdAddress: Option<String> }`
    to `{ sandboxID, envdURL: String }`. The gateway now does the
    `ws://` scheme assembly; SDKs receive a dialable URL.
  - Added `PauseResponse::state: &'static str` (always `"paused"`) so
    SDKs can assert a stable shape without round-tripping the
    `SandboxPhase` enum spelling.
  - Added `pause_sandbox` handler — gated on `sandboxes:pause` scope.
  - Added `connect_sandbox` handler — gated on `sandboxes:connect`,
    polls `status.envd_address` every 500ms for up to 30s
    (`CONNECT_TIMEOUT`, `CONNECT_POLL_INTERVAL` constants documented
    inline), returns `AppError::upstream("envd_not_ready", ...)` on
    deadline expiry. Uses `tokio::time::Instant` so the deadline is
    monotonic and survives wall-clock jumps.
  - Mounted both routes in `router(state)` under the same
    `auth_middleware` layer as the existing CRUD endpoints.

- `crates/sandbawks-api/tests/sandboxes.rs`
  - New `pause_patches_desired_phase` test: creates a sandbox via the
    public POST, asserts the freshly-created CR has
    `spec.desired_phase == None`, hits `/pause`, asserts 200 +
    `{ sandboxID, state: "paused" }`, then re-fetches the in-memory CR
    via `state.sandbox_ops().get(...)` and asserts
    `spec.desired_phase == Some(Paused)`.

## Why the 30s connect timeout

Sized to cover a cold-start image pull (`PodAdapter::create_pod` on a
fresh node) without hanging an SDK on a stuck reconcile. The kind
smoke test in T15 exercises the warm path against a real Pod where
the image is already cached locally.

## Validation

```
just fmt
cargo clippy --all-targets -- -D warnings
just test
cargo test -p sandbawks-api --test sandboxes
```

All green. The integration-test suite for `routes::sandboxes` now
contains four tests:

- `create_sandbox_returns_201_with_e2b_shape`
- `list_sandboxes_returns_array`
- `kill_returns_204`
- `pause_patches_desired_phase`

The connect handler's happy + timeout paths are deferred to the kind
smoke test (T15) — exercising the 30s deadline in a unit test would
require either injecting the clock (out of scope for this task) or a
real 30s sleep (unacceptable for CI).
