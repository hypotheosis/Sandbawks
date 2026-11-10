# Issue #358 — Fix: resume must extend `spec.timeoutSeconds` to avoid immediate re-pause

**Date:** 2026-06-05T20:17:23Z
**Branch:** `fix-issue-358-connect-resume-timeout`
**Area:** `area/api`

## Root cause

The controller's timeout deadline is the wall-clock `creationTimestamp +
spec.timeoutSeconds` (issue #327, PR #357). `connect_sandbox`
(`crates/sandbawks-api/src/routes/sandboxes.rs`) patched `spec.desiredPhase =
Running` on resume but never bumped `spec.timeoutSeconds`.

So a sandbox paused for longer than its `timeoutSeconds` window resumed with the
deadline already in the past. The first `Running` reconcile after resume saw
`now >= creationTimestamp + timeoutSeconds` and immediately fired
`Trigger::Timeout`, which — for the SDK-default `autoPause: true` — re-paused the
sandbox right after the user reconnected ("connect, then it instantly pauses
again"); for `autoPause: false` it terminated it.

Before #357 the `(Running, Timeout)` edges were dead code, which masked this
latent interaction on the resume path; #357 made it user-visible.

## Fix

`connect_sandbox` now re-derives the deadline from the **configured lifetime**
*before* flipping `desiredPhase` to `Running`:

```text
new spec.timeoutSeconds = elapsed_since_creation + configured_lifetime
  ⇒ endAt = creationTimestamp + new timeoutSeconds = now + configured_lifetime
```

This mirrors the `creationTimestamp + timeoutSeconds == requested expiry`
invariant the gateway already maintains for `set_timeout` / keepalive
(`refresh_sandbox_timeout`). The patch is applied while the sandbox is still
`Paused` — the controller's `Trigger::Timeout` only fires in `Running` — so
there is no race with the resume reconcile.

### Resolving the open question: avoiding window growth

The issue flagged an open question: re-applying the **current**
`spec.timeoutSeconds` as the lifetime would compound the window on every
pause/resume cycle, because `spec.timeoutSeconds` is an *absolute deadline
encoded as an offset from creation* (the gateway grows it on each refresh), not
a stable duration.

Resolved by persisting the caller-configured **duration** separately, in a
gateway-private annotation:

- **`sandbawks.dev/timeout-duration-seconds`** — the stable lifetime the caller
  asked for. The controller never reads it; it only honors
  `spec.timeoutSeconds`.

Write/read sites (all in `routes/sandboxes.rs`):

| Path | Behavior |
| --- | --- |
| Cold create (`body_to_sandbox`) | stamps the annotation = `body.timeout`. |
| `set_timeout` / `refreshes` (`refresh_sandbox_timeout`) | updates the annotation = `keep_alive` (the new configured lifetime) alongside the recomputed `spec.timeoutSeconds`. |
| Resume (`connect_sandbox`) | reads the annotation (falls back to `spec.timeoutSeconds` when absent), and persists it via the same patch. |

Because resume re-reads the **persisted duration** rather than the grown offset,
repeated pause/resume cycles keep landing the deadline one configured lifetime
into the future — the window no longer compounds.

The `timeoutSeconds == 0` "disabled" sentinel (warm-pool members) is left
untouched: a `0` lifetime skips the timeout patch entirely so resume can't
stamp a deadline that would auto-pause the pool.

#### Fallback caveat

When the annotation is absent (a sandbox created before this change, or a
warm-claimed member), the configured lifetime falls back to the current
`spec.timeoutSeconds`. For a freshly-created or freshly-claimed sandbox that
equals the requested lifetime, so the fallback is correct. The one imperfect
case is a *legacy* sandbox that was already refreshed before its first
post-upgrade resume: its `spec.timeoutSeconds` is then a grown offset, so the
first resume over-extends once. Resume persists the annotation, so the next
cycle reads the stable value — a one-time over-extension confined to the upgrade
window.

## Implementation notes

- `SandboxOps::patch_timeout_seconds` → renamed **`patch_timeout`** and now
  writes `spec.timeoutSeconds` **and** the duration annotation in a single merge
  patch (sole production caller was `refresh_sandbox_timeout`).
- Extracted two small pure helpers, DRYing the elapsed-time math shared by
  `set_timeout` and resume:
  - `configured_timeout_duration(&Sandbox) -> u32` — annotation, else
    `spec.timeoutSeconds`; `0` = disabled.
  - `timeout_offset_from_now(&Sandbox, ns, id, duration) -> u32` — saturating
    `elapsed + duration`, with the existing clock-skew / missing-timestamp debug
    breadcrumb.

## Tests

`crates/sandbawks-api/tests/sandboxes.rs`:

- `connect_extends_timeout_past_deadline` — seeds a sandbox created 2h ago with a
  1h window (deadline an hour in the past) and a pre-set `status.envdAddress`;
  asserts resume returns `200`, flips `desiredPhase` to `Running`, lands the
  deadline ~1h in the future, and persists the duration annotation. A **second**
  resume asserts the window does **not** grow (still ~1h, not ~2h) — the
  regression guard for the open question.
- `connect_leaves_disabled_timeout_untouched` — a `timeout_seconds: 0` member
  resumes with the timeout still disabled and no annotation written.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace suite green (incl. the two new tests).

Closes #358.
