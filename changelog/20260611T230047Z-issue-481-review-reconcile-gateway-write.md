# Idle timeout — reconcile gateway `timeoutSeconds` writes with the activity anchor

- **Date:** 2026-06-11T23:00:47Z
- **Issue:** #481 (PR #482 review round)
- **Crates:** `sandbawks-api`, `sandbawks-types` + `deploy/crds`

## Why

The PR #482 review flagged that the controller now reads `spec.timeoutSeconds`
as an idle window anchored on `status.lastActivityTimestamp`, but the gateway
still **wrote** it as a grown offset from creation
(`elapsed_since_creation + duration`, built for the old
`creationTimestamp + timeoutSeconds` model). Once any activity stamp landed, the
effective idle window inflated by `elapsed_since_creation` — e.g. a warm member
claimed 10 min after mint with a 60s timeout tolerated ~11 min of idleness
instead of 60s. Plus two secondary items: the activity throttle `DashMap` leaked,
and the `note_activity` retry wording overstated immediacy.

## What changed

### Gateway now writes the raw idle window (no growing)

- **`set_timeout` / `refreshes`** (`routes::sandboxes::refresh_sandbox_timeout`):
  writes the caller's `keep_alive` verbatim to `spec.timeoutSeconds` and stamps
  `status.lastActivityTimestamp = now`, so the deadline lands `keep_alive`
  seconds from now.
- **`connect` / resume** (`connect_sandbox`): no longer re-derives/grows
  `timeoutSeconds`. Resuming is activity, so it stamps
  `lastActivityTimestamp = now` (gated to a real resume, not an already-Running
  reconnect), giving the resumed sandbox a full fresh window — the #358
  insta-repause guard under the new anchor.
- **warm-claim** (`warm_claim::apply_overrides`): writes the raw
  `overrides.timeout_seconds` and stamps `lastActivityTimestamp = claimed_at`
  (via `patch_status`), anchoring the deadline at the claim instant rather than
  the stale mint-time `creationTimestamp` (#380). Removed `grown_timeout_offset`
  and the shared `timeout_offset_from_now`.
- **`endAt`** (`sandbox_to_dto`): rendered as `idle_baseline + timeoutSeconds`
  (lastActivity ‖ creation), mirroring the controller so the SDK-visible expiry
  matches the pause decision.
- `patch_timeout` simplified to `(ns, name, timeout_seconds)`; the
  `TIMEOUT_DURATION_ANNOTATION` is retained as a human-facing mirror of the
  window. `configured_timeout_duration` (the now-dead un-grow reader) removed.
- Doc comments reconciled: `SandboxSpec::timeout_seconds`, the annotation const,
  warm-claim, and the connect path no longer claim
  `creationTimestamp + timeoutSeconds`.

### Secondary

- **Throttle eviction** (`activity::ActivityStamper`): a background sweeper
  (`spawn_eviction`, started in `AppState::from_kube`) drops entries idle longer
  than `EVICTION_INTERVAL` (60s), bounding the map to recently-active sandboxes
  instead of leaking one entry per ephemeral `sbx-<random>` id. The sweep exits
  when the gateway drops its `ActivityStamper`.
- **Wording**: `note_activity` now states the throttle slot advances on
  *attempt*, so a failed patch self-heals on the next due request (≤
  `STAMP_THROTTLE`) rather than "the next request."

## Tests

- Updated: `set_timeout`/`refreshes` assert the raw window (== 600 / 900) plus a
  baseline stamp; `connect_extends_timeout_past_deadline` asserts the re-anchored
  baseline (not an annotation); warm-claim asserts raw `timeoutSeconds == 60` and
  `lastActivityTimestamp == claimed_at`; the deadline helper is baseline-aware.
- Added: `activity::eviction_drops_only_stale_entries`.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all green. CRDs regenerated (idle-window doc).

## Left as-is (with rationale on the PR)

- Per-frame `activity.note()` on the streaming hot path (review nit): the call
  short-circuits on the throttle to a `DashMap` lock + `Instant::now()`; the
  reviewer marked it "fine as-is."
