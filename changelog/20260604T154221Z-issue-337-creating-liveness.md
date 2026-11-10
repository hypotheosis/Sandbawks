# Creating-phase liveness: fail a Sandbox whose Pod vanished

**Date:** 2026-06-04T15:42:21Z
**Issue:** #337
**Branch:** `fix-issue-337-creating-liveness`

## Summary

A `Sandbox` stuck in `Creating` with a `status.podRef` pointing at a Pod
that no longer exists (node churn / Pod GC / a reaped failed schedule)
previously idled at `Creating` **forever**. The reconcile loop's readiness
probe (`RuntimeAdapter::pod_ready`) returns `Ok(None)` for *both* "Pod
gone" and "Pod still booting", so it could never recover on its own, and
`derive_trigger` yields nothing for `(Creating, pod_ref set, not ready)`.

For a warm-pool member this is fatal: the wedged member keeps counting
toward `minReady` in `plan_pool`, suppressing top-up, so the whole
`SandboxWarmPool` stalls at `ready=0` and fires no new pods (observed
live on `compound-pool`).

This adds a `Creating`-phase liveness check that mirrors the existing
`Running`-phase one: it consults `RuntimeAdapter::pod_terminated` (which
*can* distinguish a 404/terminal Pod from a live one) and folds a
gone/terminal Pod into `Trigger::Failure`, so the Sandbox transitions to
`Failed`.

## Why this fixes the warm-pool wedge

No `warm_pool.rs` change is needed. Once the wedged member flips to
`Failed`, `PoolMembers::bucket` already drops `Failed` members, so they
stop counting toward `total` in `plan_pool` and top-up immediately
re-creates a healthy replacement. The pool self-heals back to `minReady`.

## Changes

### `crates/sandbawks-controller/src/reconcile.rs`
- New `creating_liveness_trigger` (sibling of `running_liveness_trigger`):
  when a `Creating` Sandbox has a `pod_ref` but `pod_terminated` reports
  the Pod gone/terminal, yield `Trigger::Failure`. A still-booting Pod
  (`Ok(false)`) or a transient probe error (`Err`) leaves it `Creating`.
- Wired into the trigger-derivation `match`: when `derive_trigger`
  returns `None` at `Creating`, run the liveness check (parallels the
  existing `Running` arm). The `(Creating, Failure) → Failed` edge already
  exists in `state_machine.rs`, so no state-machine change was required.

### `crates/sandbawks-controller/tests/reconcile.rs`
- `CreatingPodAdapter` (+ `PodLiveness` enum): `pod_ready → Ok(None)`,
  `pod_terminated` configurable (Gone / Booting / ProbeError).
- Three tests: vanished Pod → `Failed` (with a recorded Failed condition);
  booting Pod → stays `Creating`; transient probe error → stays `Creating`.

## Validation
- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all binaries pass (0 failures).

Not exercised here: the full in-cluster warm-pool recovery (member wedges
→ fails → pool tops back up) needs a kind + gVisor cluster; it is not
reproducible from the unit/integration suite. The unit tests cover the
per-Sandbox transition that drives it.

## Scope / follow-ups
- This is the per-Sandbox half of #337 (fix #1 in the issue). It alone
  un-wedges the pool because `bucket` already drops `Failed`.
- The pool-side hardening (fix #2 — bound in-flight staleness / reap
  `Failed` warm members so they don't accumulate as orphans) overlaps the
  `stale`-reclaim work in PR #336 and is intentionally left to fold in
  there or as a follow-up, to avoid conflicting edits to `warm_pool.rs`.
- Resume-phase liveness is out of scope (issue #247 already governs the
  `Resuming` stale-address recovery path).
