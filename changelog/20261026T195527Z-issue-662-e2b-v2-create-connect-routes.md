# Serve the E2B v2 create and connect routes (issue #662)

**Date:** 2026-10-26T19:55:27Z
**Issue:** #662 — `POST /v2/sandboxes/{id}/connect` is missing, so current official SDKs can't connect to or resume a sandbox
**Crates:** `sandbawks-api` (`crates/sandbawks-api/src/routes/sandboxes.rs`), plus the `tests/e2b-sdk-compat` (Python) and `tests/e2b-sdk-compat-js` harness pins

## Summary

Current official E2B SDKs (e2b 2.61.0, Python and JS) moved two lifecycle calls
onto versioned routes the gateway did not serve:

| SDK call | e2b 2.61 route | Gateway before |
|---|---|---|
| `Sandbox.create` | `POST /v2/sandboxes` | only `GET /v2/sandboxes` (list) → 405 |
| `Sandbox.connect` / resume | `POST /v2/sandboxes/{id}/connect` | not routed → 404, which the SDK reports as "Paused sandbox ... not found" |

The issue text assumed v2 create already worked, because the live gateway answered
`POST /v2/sandboxes` with 401 rather than 404. That 401 is the auth layer, which
wraps the whole router and runs before axum's method check. An authenticated request
would have got 405. The in-process router test added here pins the route.

## Changes

- `POST /v2/sandboxes` → `create_sandbox`. The v2 body (`NewSandboxV2`) is a superset
  of v1's. `CreateSandboxBody` does not deny unknown fields, so `autoPause`,
  `autoPauseMemory`, `secure`, etc. are ignored as before.
- `POST /v2/sandboxes/{id}/connect` → `connect_sandbox`. Neither route reads the body
  (`{timeout?, memory?}` in v2, `{timeout}` in v1); behaviour is unchanged from v1.
- `connect_sandbox` now returns `200 OK` when the sandbox was already running and
  `201 Created` when the call resumed it, per E2B's OpenAPI spec. Both the old (2.23)
  and new (2.61) SDK parsers accept either code. In-repo callers (`pause-resume-smoke.sh`
  uses `curl -f`) accept any 2xx.
- The unversioned `/sandboxes` and `/sandboxes/{id}/connect` routes stay for older SDKs.
- Compat harnesses bumped so CI exercises the routes current SDKs use:
  - Python: `e2b>=2.61,<3` (locked 2.61.0). e2b 2.61 requires Python >= 3.11, so
    `requires-python` moves from `>=3.10` to `>=3.11` (`uv sync` provisions it).
  - JS: direct `e2b@^2.61.0` dev-dependency, which floors the copy `@e2b/code-interpreter`
    (`e2b: ^2.24.0`) resolves to. It was 2.26.0.
  - Both suites already contain pause → `Sandbox.connect` → exec tests
    (`test_lifecycle.py::test_pause_resume_exec_roundtrip`, `test_agentic_turn.py`,
    `lifecycle.test.ts`, `agentic_turn.test.ts`). They missed this gap only because
    the pinned SDKs (2.23.1 / 2.26.0) still used the v1 routes.

## Tests

`crates/sandbawks-api/tests/sandboxes.rs`:

- `create_v2_returns_201_with_e2b_shape`: v2-shaped body (extra `autoPause` fields) → 201 + `sandboxID`.
- `connect_v2_resumes_paused_sandbox_with_201`: paused sandbox via v2 → 201, `desiredPhase=Running`.
- `connect_already_running_returns_200_on_v1_and_v2`: running sandbox → 200 on both routes.
- The existing v1 resume tests now assert 201.

## Remaining SDK surface not served (not v2 aliases, out of scope)

From e2b 2.61's `Sandbox` API: `GET /sandboxes/{id}/metrics`, `PUT /sandboxes/{id}/network`,
`POST /sandboxes/{id}/fork`, `POST /sandboxes/{id}/snapshots`, `GET /snapshots`,
`DELETE /templates/{id}`. These are features sandbawks has not implemented, not routes
that moved to a new version.
