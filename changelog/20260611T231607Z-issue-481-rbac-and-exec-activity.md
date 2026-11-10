# Idle timeout — gateway RBAC for status writes + exec WS activity

- **Date:** 2026-06-11T23:16:07Z
- **Issue:** #481 (PR #482 review round 3 + CI fix)
- **Crates / paths:** `sandbawks-api`, `deploy/helm/sandbawks`

## Why

Two findings from the `/prrb` re-run (review #217) and CI:

1. **CI regression — `pause/resume smoke :: resume` failed (exit 22).** The
   review-round fix made `/connect` (and `set_timeout` / warm-claim) write
   `status.lastActivityTimestamp` via the `status` subresource, but the
   gateway's ClusterRole granted `sandboxes` only — **not** `sandboxes/status`.
   So `patch_status` got a 403: `/connect` (awaited) returned non-2xx and the
   smoke's `curl -fsS` failed. The fire-and-forget proxy-path stamp
   (`note_activity`) was also silently 403-ing, so idle stamping was in fact
   inert in-cluster. The unit/router tests couldn't catch this (no RBAC).

2. **Material gap — the WS exec route never stamped activity.** The
   team-scoped `GET /sandboxes/:id/exec` WebSocket bridge (`routes::exec`)
   resolves the sandbox independently and never called `note_activity`, so a
   long interactive exec/pty session would idle out from under the client —
   contradicting the PR's "open exec/pty stream counts as active" claim.

## What changed

### RBAC (`deploy/helm/sandbawks/templates/rbac.yaml`)

Added `sandboxes/status` (`get`/`patch`/`update`) to the `sandbawks-api`
ClusterRole so the gateway can stamp `lastActivityTimestamp`. Chart version
bumped `0.1.2` → `0.1.3`.

### Exec WS activity (`routes::exec`)

`exec` now stamps `note_activity` on connect and spawns a heartbeat that
re-stamps every `EXEC_ACTIVITY_HEARTBEAT` (10s) for the life of the socket
(aborted the instant `run_exec` returns). An open-but-silent interactive
session stays active — mirroring the keepalive-driven stamping on the
Connect-RPC `process.Start` path. `note_activity` is throttled, so this is at
most one status patch per interval per open session.

### Doc (`activity::STAMP_THROTTLE`)

Noted that the throttle is per-gateway-pod / in-memory, so the effective
status-write rate scales up to `N×` with `N` replicas (bounded and harmless —
last-write-wins of a monotonic timestamp).

## Left as-is (replied on the PR)

- `connect`'s `stamp_activity` is on the awaited path (a failed patch fails
  `/connect`): intentional — the resume re-anchor must land to avoid an
  insta-repause — and now unblocked by the RBAC fix. The reviewer marked it
  "fine as-is."

## Validation

- `just fmt` · `cargo clippy --all-targets -- -D warnings` · `just test` — all green.
- `helm lint deploy/helm/sandbawks` — passes; `helm template` renders the new
  `sandboxes/status` api rule.
