# Treat the sandbox timeout as an idle timeout (reset on activity)

- **Date:** 2026-06-11T21:40:31Z
- **Issue:** #481
- **Crates:** `sandbawks-types`, `sandbawks-controller`, `sandbawks-api` + `deploy/crds`

## Summary

Reinterprets `spec.timeoutSeconds` as an **idle window** instead of an absolute
deadline measured from claim/creation. A `Running` sandbox is now auto-paused
(`autoPause`) or auto-terminated only once it has had **no client activity** for
the window, rather than once `creationTimestamp + timeoutSeconds` elapses
regardless of use. This fixes the live regression where an actively-working
sandbox (`sbx-warm-5cjzr`, pegged at ~100% CPU running commands) was paused out
from under the client exactly at claim + 300s.

Per the issue author's answers to the open questions:

- **Idle-only, no absolute cap.** The only timeout that matters is the idle
  timeout; a non-idle sandbox is never paused.
- **`timeoutSeconds` *is* the idle window.** No new `idleTimeoutSeconds` field —
  the existing field's semantics change.
- **An open stream counts as active.** A long-running / silent
  `commands.run(...)` (or an interactive pty) is not killed during a long wait;
  the streaming keepalive cadence keeps the idle clock fresh while the stream is
  open. This is a deliberate divergence from the issue's original sketch (which
  proposed *excluding* keepalive frames) — the open stream is the activity.
- **Stamp from the gateway**, which already proxies every per-sandbox request.

## What landed

### Types (`crates/sandbawks-types/src/sandbox.rs`)

- New `SandboxStatus.lastActivityTimestamp: Option<String>` (RFC 3339), matching
  the sibling `lastTransitionTime` string convention. CRDs regenerated
  (`deploy/crds/sandboxes.sandbawks.dev.yaml`).

### Controller (`crates/sandbawks-controller/src/reconcile.rs`)

- New `activity_baseline()`: the idle clock's start instant —
  `status.lastActivityTimestamp` when set (parsed via
  `DateTime::parse_from_rfc3339`; a malformed value degrades to the fallback),
  else `metadata.creationTimestamp` so a never-touched sandbox still ages out.
- `timeout_deadline()` now returns `activity_baseline + timeoutSeconds`. The `0`
  disabled sentinel (warm-pool members) is unchanged; `timeout_trigger()` and
  `running_requeue_interval()` are untouched (they compose over the deadline).

### API gateway (`crates/sandbawks-api`)

- New `activity` module: `ActivityStamper` (per-sandbox throttle, default 10s)
  + `AppState::note_activity()`, a throttled, fire-and-forget status patch that
  adds no latency to the proxied call.
- New `SandboxOps::stamp_activity()` — patches the `status` subresource via
  `patch_status` (a plain `.status` merge is dropped when a status subresource
  exists). Wired in `KubeSandboxOps` and `FakeSandboxOps`.
- `routes::envd_proxy`: every per-sandbox work call (filesystem, process, files,
  exec/pty stream open) refreshes the stamp via `resolve_sandbox`. The liveness
  probe (`/health`, backing `Sandbox.is_running`) deliberately does **not**
  stamp — it calls the new non-stamping `resolve_sandbox_inner` — so an
  `is_running` polling loop can't keep an idle sandbox alive forever.
- Streaming `process.Start` / `process.Connect`: `forward_event_stream` re-stamps
  (throttled) on every forwarded frame — real output **and** synthetic keepalive
  — so an open exec/pty stream keeps the sandbox active for its lifetime.

## Tests

- Controller integration (`tests/reconcile.rs`): recent activity keeps a
  past-creation-deadline sandbox `Running`; stale activity drives `Pausing` with
  a Pause `SnapshotJob`.
- Controller unit (`reconcile::tests`): recent activity suppresses the trigger
  past `created + timeout`; stale activity fires; a malformed timestamp falls
  back to the creation baseline.
- API unit (`activity::tests`): throttle suppresses within the window and is
  per-sandbox.

## Out of scope

- Node-side sandbox-pressure watcher (epic #472) — telemetry-only, unrelated.
- Warm-pool mint/claim timeout-growing logic beyond what the idle model needs.
- Per-request fine-grained activity accounting / billing.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all green (incl. the new cases above). CRD drift check passes.
