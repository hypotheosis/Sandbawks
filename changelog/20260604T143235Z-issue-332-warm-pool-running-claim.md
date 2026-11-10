# Warm pool: hold members Running, make claim a reassignment

**Date:** 2026-06-04T14:32:35Z
**Issue:** #332
**Branch:** `refactor-issue-332-warm-pool-running-claim`

## Summary

Refactored the `SandboxWarmPool` warm pool so members are held **`Running`**
(ready to claim) instead of being eagerly auto-paused after boot. A claim is
now a **control-plane reassignment** of a live member — relabel + apply the
requester's overrides — with **no pod creation, restore, or resume on the
claim path**.

This implements goals 1 and 2 of issue #332. The snapshot/restore tier
(goal 3 — a cost lever that scales the hot buffer down and back-fills from
snapshots) is intentionally **left untouched**; the pause/resume state machine
and `SnapshotJob` machinery are unchanged and still available for requester-
driven pause/resume of a claimed sandbox.

## Why

The previous design pre-paused every member immediately after a bare boot and
resumed it on claim. Because a claim recreated the pod either way, the slow
part (pod schedule + sandbox create) was paid on both cold-start and
warm-resume — and warm-resume *added* an S3 snapshot download plus an eager
gVisor restore (~5.2s p95). Net: warm resume ≈ cold start, often worse, and
never the instant claim a warm pool is supposed to deliver. Holding members
`Running` makes a claim a metadata patch against a live `envd`.

## Key design note

There is **no idle-timeout `Trigger` in production** — a `Running` sandbox only
leaves `Running` when `spec.desiredPhase = Paused` is patched. The old auto-pause
was driven entirely by the warm-pool reconciler patching `desiredPhase`. Removing
that patch makes members stay `Running` with no other machinery required.

## Changes

### `crates/sandbawks-types/src/warm_pool.rs`
- `SandboxWarmPoolStatus`: renamed status field `warmSnapshots` → `members`
  (the entries are now live `Running` members, not warmed snapshots).
- Updated `WARM_POOL_LABEL` and type docs to `Running` semantics.

### `crates/sandbawks-controller/src/warm_pool.rs`
- `PoolMembers.paused` → `ready`; `bucket` puts `Running` in the ready set
  (Failed/Terminating still dropped, everything else → pending/in-flight).
- `status_for` / `plan_pool` count and trim `Running` members.
- Removed `members_to_pause`, the auto-pause loop, and the
  `WarmPoolStore::patch_desired_phase` method (+ Kube/Fake impls).
- `warm_sandbox` keeps `runtimeClassName` (load-bearing for a *later*
  requester pause/resume) and `auto_pause: true` (post-claim default only).

### `crates/sandbawks-controller/src/warm_pool_metrics.rs`
- Dropped the `warm_pool.pause_requested_total` counter and its
  `PoolObservation.pause_requested` field (now five instruments).

### `crates/sandbawks-api/src/warm_claim.rs`
- `WarmClaim::paused_candidates` → `running_candidates` (filters `Running`).
- `apply_overrides_and_resume` → `apply_overrides`: the patch no longer sets
  `spec.desiredPhase = Running` — the member is already live, so a claim is a
  reassignment, not a resume. Overrides (env/metadata/timeout/runtimeClassName)
  and the trace annotation are still applied.

### `crates/sandbawks-api/src/warm_claim_metrics.rs`
- Doc/description wording updated from "claim-to-resume" to "claim handoff".

### CRDs
- Regenerated `deploy/crds/sandboxwarmpools.sandbawks.io.yaml`
  (`status.warmSnapshots` → `status.members`, doc strings).

### Tests
- Updated controller + api warm-pool/warm-claim unit, reconcile, router, and
  real-recorder metrics tests to `Running` readiness and the no-pause path.
  Replaced `reconcile_auto_pauses_running_member` with
  `reconcile_holds_running_member_without_pausing`.

## Validation
- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all binaries pass (0 failures).

Not exercised: full warm-pool fill → claim → handoff is k8s control-plane
behavior that needs a kind + gVisor cluster; it is not reproducible from the
unit/integration suite.

## Operator notes — rolling deploy

The old controller auto-paused warm members, so right after this controller
rolls out a pool may still hold `Paused`/`Pausing` members. These are now
**self-healed**: `PoolMembers::bucket` routes them to a `stale` set (kept out
of the in-flight count so they can't suppress top-up), and `reconcile_pool`
deletes them each pass. The pool then re-creates fresh `Running` members back
to `min_ready`. No manual `kubectl delete sandboxwarmpool` is required. The
per-reconcile `reclaim=<n>` log field reports how many legacy members were
swept; in steady state it is always `0`.

## Out of scope (tracked in #332)
- Snapshot cost tier / build-time template snapshot (goal 3).
- Idle-cost budget / claim-latency SLO sizing decisions.
- Pre-existing cold-start image bug (#265), which should be fixed before
  benchmarking warm-vs-cold.
