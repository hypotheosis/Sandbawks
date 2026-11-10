# PR #202 follow-up — gateway-synthesized `GET /health` + review fixes

Second iteration on PR #202 (issue #189). Splits into two parts: a live
CI failure surfaced by `just sdk-smoke`, and the three review comments
from review #51.

## CI failure: `is_running()` had no endpoint to hit

The live `sdk-smoke` run against the kind cluster passed both of #189's
real deliverables — `test_get_info_returns_detail` (`GET /sandboxes/:id`)
and `test_set_timeout_extends_deadline` (`POST /sandboxes/:id/timeout`)
— but `test_is_running_after_create` failed with
`e2b.exceptions.NotFoundException`.

### Root cause

The installed e2b SDK (>=2.21) implements `Sandbox.is_running()` as a
plain `GET /health` against the in-sandbox envd (mapping `200` → running,
`502` → not running). It does **not** call `GET /sandboxes/:id` as the
issue assumed. Our envd is a gRPC-only `tonic` server with no HTTP
`/health` route, and the gateway's envd proxy only forwards the
Connect-RPC paths (`/filesystem.Filesystem/*`, `/process.Process/*`) and
`/files` — so `GET /health` fell through to a 404.

### Fix

Added `routes::envd_proxy::health`, registered as `GET /health` in the
`unauthenticated_unary_router` (same header-based, no-API-key posture as
the rest of the per-sandbox proxy). The gateway *synthesizes* the probe
upstream E2B serves natively: it resolves the sandbox via the
`E2b-Sandbox-Id` header (reusing `resolve_sandbox`), dials the sandbox's
envd, and issues a cheap read-only `Process.List` RPC.

Status mapping (standard `AppError` envelope; the SDK branches on HTTP
status, not body):

- reachable, serving envd → `200` → `is_running()` `True`
- envd unreachable / no `envdAddress` yet → `AppError::UpstreamUnavailable`
  → `502` → `is_running()` `False`
- missing (`404`) / paused (`403`) / no header (`400`) propagate from
  `resolve_sandbox`; the SDK raises on those.

A successful `Process.List` (rather than just a completed TCP/h2
handshake) is used deliberately so a dead-but-cached channel can't read
as healthy.

Added `crates/sandbawks-api/tests/envd_proxy.rs` with wiring guards for
the 400 (no header) and 404 (missing sandbox) branches — the 200 path
needs a live envd and is covered by the SDK compat suite.

## Review #51 fixes

- **`fd4695e`** — `tracing::debug!` on the creation-timestamp fallback
  branch in `refresh_sandbox_timeout` so a consistently-failing
  `creationTimestamp` lookup is visible to operators.
- **`7f49a6a`** — docstring on `SandboxSpec::timeout_seconds` noting the
  gateway rewrites it to `elapsed_since_creation + requested` on each
  `/timeout` refresh.
- **`6bbfc74`** — widened the unit-test timeout assertion windows
  (`600..=615`, `900..=915`) for CI headroom.
- **`/v2` asymmetry (review point #1)** — verified against the generated
  SDK client: only `Sandbox.list` uses `/v2/sandboxes`; `get_info`,
  `set_timeout`, `connect`, and `pause` are all unversioned. No v2
  aliases needed.

## Validation

- `just fmt` ✅
- `cargo clippy --all-targets -- -D warnings` ✅
- `just test` ✅ (full workspace green; +2 new `envd_proxy` tests)
- `uv run pytest --collect-only` ✅ (28 tests)
- Live `just sdk-smoke` re-run is the CI gate that confirms
  `test_is_running_after_create` now passes against the kind cluster.
