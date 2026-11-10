# Issue #563 — Warm-claim idle-timeout thrash: re-arm the idle clock on resume → Running

**Date:** 2026-06-16T20:20:25Z
**Branch:** `sandbawks/sandbawks-563`
**Issue:** #563 (`area/controller`, `kind/fix`, `prio/p1`)

## Context

A claimed warm-pool member with **`spec.desiredPhase=Running`** *and*
**`spec.autoPause=true`** that had aged past its idle-timeout deadline entered a
tight **pause↔resume thrash** (~7 s/cycle): it resumed to `Running`, the
idle-timeout *instantly* re-paused it, `desiredPhase=Running` *instantly*
re-resumed it (deleting + recreating the restore Pod each cycle), and so on. The
rapid container churn then let the node's pause checkpoint race the container
teardown (`tar+zstd upperdir: No such file`, then `connect: Host is
unreachable`); the bounded tier-1 pause retries (#510) exhausted, and the
Sandbox folded to terminal **`Failed`**. Observed in production on
`sbx-compound-pool-5zj7l` / `-kqrxj` (2026-06-16); sibling members *without*
`desiredPhase=Running` stayed healthy.

This is **not** #560 — the resume itself works (the Sandbox reaches `Running`
and envd serves `GET /__state` → 200). It is the warm-claim / idle-timeout
anchoring class (cf. #380, #482).

## Root cause

The idle-timeout deadline is `activity_baseline + spec.timeoutSeconds`, where
`activity_baseline` reads `status.lastActivityTimestamp` (falling back to
`creationTimestamp`). The `(Resuming, PodReady) → Running` edge dispatched
`Action::Noop`, so a resume left the **pre-pause** activity timestamp in place.
For a long-lived claimed member whose baseline was already expired, the deadline
was in the past the instant it landed in `Running` — so the idle edge fired
immediately, and `desiredPhase=Running` re-resumed it, looping. The checkpoint
errors are downstream symptoms of the churn, not an independent node bug.

## What changed

Implements the issue's **fix (1)** — the actual cure — mirroring the idle-clock
re-arm `Action::RecoverToRunning` already performs on the #510 tier-2 edge.

1. **New `Action::ResumeToRunning`** (`sandbawks-controller/src/state_machine.rs`).
   The `(Resuming, PodReady)` edge now returns `Action::ResumeToRunning` instead
   of `Action::Noop`. Added to `Action::label()` as `resume_to_running`.

2. **Dispatch arm** (`sandbawks-controller/src/reconcile.rs`).
   `Action::ResumeToRunning` stamps `status.lastActivityTimestamp = now`, giving
   every resumed Sandbox a full fresh idle window before the next auto-pause.
   No side effect on the live Pod — it is already container-Ready from the
   restore. The gateway keeps pushing the baseline forward on real requests, so a
   genuinely-used sandbox still never auto-pauses; a genuinely-idle one pauses at
   most once per `timeoutSeconds` rather than thrashing every ~7 s.

3. **Comment fix** — the `Action::Noop` dispatch comment no longer claims
   `(Resuming → Running)` is a Noop.

### Tests

- `state_machine_unit::resuming_pod_ready_to_running` — updated to expect
  `Action::ResumeToRunning`.
- `lifecycle::resume_to_running_rearms_idle_clock_no_thrash` (new) — a
  past-deadline `desiredPhase=Running` + `autoPause` member resumed to `Running`
  re-arms `lastActivityTimestamp` off the stale baseline and, on the next
  reconcile, stays `Running` instead of folding to `Pausing` (the thrash entry
  point).

## Scope / deferred

The issue's **fix (2)** (node-side pause hardening — treat a vanished-container
checkpoint, `upperdir` ENOENT / `Host is unreachable`, as a no-op success) is
*defense-in-depth* for "if it ever recurs." Fix (1) removes the churn at its
source, so the checkpoint race no longer occurs, and a `Failed`-with-live-Pod
sandbox already self-recovers via #510 tier-2 (`RecoverToRunning`). Fix (2)
expands the node checkpoint success/failure contract and is left as a separate,
narrower change.

## Validation

- `just fmt` ✅
- `cargo clippy --all-targets -- -D warnings` ✅
- `just test` ✅ (incl. the new + updated tests above)
