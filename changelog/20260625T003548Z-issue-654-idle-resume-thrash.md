# Withhold the idle resume to break the filesystem-mode pause/resume thrash (issue #654)

**Date:** 2026-06-25T00:35:48Z
**Issue:** #654 — Idle-timeout thrash, filesystem-mode (post-#563): an idle `desiredPhase=Running` + `autoPause` sandbox oscillates Suspend⟷Resume every idle window
**Crate:** `sandbawks-controller` (`crates/sandbawks-controller/src/reconcile.rs`)

## Summary

A long-lived sandbox with `spec.desiredPhase=Running` + `spec.autoPause=true` that
receives **no envd traffic** oscillated forever between Suspended and Running. The
idle timeout suspended it (filesystem mode: Pod deleted, PVC retained), and because
`desiredPhase` was still `Running`, the controller's `(Paused, →Running) ⇒
ResumeRequested` edge re-resumed it immediately — landing it back in `Running` while
*still* past its idle deadline (its `lastActivityTimestamp` was never refreshed), so
the `(Running, Timeout)` edge re-suspended it one window later. Observed in
production on `sbx-e0239613c32f` and `sbx-11b846e505a9` (AKS prod, 2026-06-24),
~5 min/cycle, churning restore Pods and scheduler capacity indefinitely.

This is the filesystem-mode sibling of #563. The #563 resume-side idle-clock re-arm
(`Action::ResumeToRunning`, stamps `lastActivityTimestamp` on `(Resuming, PodReady)`)
only **widened** the oscillation period from ~7 s to one full idle window — it never
removed the contradiction, because the re-arm happens *after* the resume the gate
should have withheld.

## Root cause

`desiredPhase=Running` and `autoPause=true` were treated as if in conflict. Under
e2b semantics they are not: `autoPause` means *pause on idle, resume on the next
client connect*. An idle auto-pause must not be countermanded by the standing
`desiredPhase=Running` — only a genuine client connect should resume an
idle-paused sandbox. The reconcile resume edge couldn't tell "paused because idle"
from "user wants it up now", so it fought every idle pause.

## Fix

Gate the resume edge on whether resuming would *immediately* re-trip the idle
auto-pause. New pure, clock-injected predicate:

```rust
fn resume_would_immediately_idle_pause(sandbox: &Sandbox, now: DateTime<Utc>) -> bool {
    sandbox.spec.auto_pause
        && timeout_deadline(sandbox).is_some_and(|deadline| now >= deadline)
}
```

`derive_trigger` now takes `now` and withholds `ResumeRequested` on
`(Paused, Some(Running))` when that predicate holds; the arm falls through to `None`
(idle), so the sandbox stays parked (`Suspended`, PVC retained) until real activity
refreshes the baseline.

The signal is already established by the API: `connect_sandbox` stamps
`lastActivityTimestamp = now` **before** flipping `desiredPhase=Running`
(`crates/sandbawks-api/src/routes/sandboxes.rs`), pushing the idle deadline a full
window into the future — so a genuine wake-up passes the gate and resumes, while the
no-client thrash (stale baseline) is withheld. The predicate reuses the existing
`timeout_deadline` helper, so the disabled-timeout sentinel (`timeout_seconds == 0`,
warm-pool members) yields no deadline and is never withheld, and a non-`autoPause`
sandbox (idle terminates rather than pauses, so no thrash) is never withheld either.

No CRD, API, or schema change — the fix is entirely within the controller's trigger
derivation.

## Tests

Added four unit tests in `reconcile::tests`:

- `resume_withheld_when_idle_deadline_already_past` — `autoPause` + stale baseline ⇒
  `derive_trigger` yields `None` (parked, no thrash).
- `resume_fires_when_activity_is_fresh` — fresh baseline (the post-`/connect` shape) ⇒
  `ResumeRequested`.
- `resume_not_withheld_without_auto_pause` — `autoPause=false` always resumes.
- `resume_not_withheld_when_timeout_disabled` — `timeout_seconds == 0` always resumes.

## Validation

- `just fmt` — clean.
- `just clippy` (`-D warnings`, all targets, full workspace) — clean.
- `just test` (full workspace) — all green (70 test binaries, 0 failures).

## Operational note

The safe live mitigation is `kubectl patch sandbox <name> -n sandbawks --type=merge
-p '{"spec":{"desiredPhase":"Paused"}}'` (parks it; resumes on next connect). The
#563 `autoPause=false` workaround is **unsafe** on this path: with a non-zero
`timeoutSeconds` the idle edge becomes `(Running, Timeout { auto_pause: false }) =>
Terminating`, which would delete the sandbox on the next idle window.
