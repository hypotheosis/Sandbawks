# Scheduler-placed restore: reconcile node-discovery dispatch + bounded recovery loop

**Issue:** #489 (epic #484, scheduler-placed restore + `Resuming` recovery)
**Date:** 2026-06-13T00:32:23Z
**Crate:** `sandbawks-controller`

## Summary

This is the controller-side phase that actually closes the `resumingforever`
hang. The foundation (issue #485) already added the state-machine edges
(`RestorePodLost → CreateRestorePod`, `Failure { ResumeExhausted } → Failed`),
the `resume_restart_count` / `resume_last_attempt` status fields, and the
`ResumeExhausted` failure reason. This change wires the reconcile loop to
*drive* them: it detects a lost or wedged restore Pod during `Resuming` and
recovers with a bounded, backed-off recreate, folding to `Failed` only once the
retry budget is spent.

Node-discovery dispatch (`NodeAssigned → StageSnapshot`) already shipped with
the happy-path migration; it is folded into the same probe here so a single pass
handles both "schedule the stage" and "recover the loss".

## What changed

`crates/sandbawks-controller/src/reconcile.rs`:

- **Combined `resuming_trigger`** (replaces the node-discovery-only
  `resuming_schedule_trigger`). For a `Resuming` Sandbox whose restore Pod is not
  yet Ready, in priority order it yields:
  1. **recovery** when the restore Pod went terminal/404, sat `Pending` past
     `RESUME_SCHEDULE_DEADLINE` (no node had capacity), or stayed
     scheduled-but-not-Ready past `RESUME_READY_DEADLINE` (wedged restore);
  2. `NodeAssigned` when the scheduler has bound the Pod and no node is recorded
     yet (the create→stage window);
  3. idle otherwise.
- **`resume_recovery_trigger`** — the bounded-budget decision: `ResumeExhausted`
  once `resume_restart_count` reaches `RESUME_MAX_RESTARTS` (default 5), `None`
  while inside the capped-exponential backoff window anchored at
  `resume_last_attempt`, else `RestorePodLost`.
- **Failed `Stage` job during `Resuming` is now recoverable** — `observe_snapshot_job`
  routes it through `resume_recovery_trigger` (→ `RestorePodLost`, clearing the
  job ref) instead of folding straight to `Failed`. Every other snapshot-job
  failure stays terminal.
- **Recovery accounting in `Action::CreateRestorePod`** — a fresh
  `Paused → ResumeRequested` resets the count to 0; a `Resuming → RestorePodLost`
  recovery increments it. Either way `resume_last_attempt` is re-stamped to
  *now*, the anchor the next pass's deadlines and backoff measure from.
- Helpers `past_deadline` and `resume_backoff` (capped exponential), and a single
  per-reconcile `Utc::now()` threaded through every time-driven probe.

The deadlines / max-restarts / backoff are compile-time constants for now; epic
#484 **Phase F** wires them through Helm / controller config.

## Tests

- Unit (`src/reconcile.rs`): `resume_backoff` capped-exponential shape,
  `past_deadline` missing/negative/at-boundary, and `resume_recovery_trigger`
  across recreate / no-anchor / within-backoff / exhausted / exhaustion-beats-backoff.
- Integration (`tests/reconcile.rs`):
  - terminal restore Pod mid-resume → recreate → schedule → stage → ready →
    `Running`;
  - Pod `Pending` past the schedule deadline, repeated → bounded → `Failed`
    (`ResumeExhausted`) with an explanatory condition;
  - failed `Stage` job → recreate in-place (no `Failed` condition);
  - fresh `Paused → Resuming` resets a stale restart count.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — full workspace suite green (controller: 95 lib + 34 reconcile
  integration tests)

## Out of scope (later epic phases)

- Helm / deploy wiring of the new knobs and the cross-cutting e2e test (Phase F).
