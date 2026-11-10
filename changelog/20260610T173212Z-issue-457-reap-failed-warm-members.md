# Issue #457 — Reap `Failed` warm-pool members

**Date:** 2026-06-10T17:32:12Z
**Branch:** `sandbawks/sandbawks-457`
**Type:** fix (controller)

## Problem

During a brief Kyverno outage (`mutate.kyverno.svc-fail`, `failurePolicy=Fail`,
no endpoints for `kyverno-svc`), a single `min=2/max=5` `SandboxWarmPool`
created **1288 `Failed` `sbx-warm-*` Sandbox CRs in ~6 minutes**. The `Failed`
members were never deleted, so a transient admission-webhook failure turned into
unbounded Sandbox-CR growth (etcd / apiserver load), while the pool reported a
healthy-looking `status.ready=0` the whole time.

## Root cause

`PoolMembers::bucket` (`crates/sandbawks-controller/src/warm_pool.rs`) dropped
`Failed`/`Terminating` members entirely, justified by a doc comment claiming
*"the controller's own GC / ownerReference cleanup reaps it."* That claim was
**false**: a `Failed` member keeps the pool's controlling `ownerReference`, and
Kubernetes owner-GC only cascades when the **owner** (the pool) is deleted. A
live pool never collects them. Two compounding effects:

1. **Leak** — nothing ever issued a `delete` for a `Failed`/`Terminating`
   member (unlike the `stale` and `version_stale` buckets, which are reaped).
2. **Unbounded mint** — `plan_pool` computes `total = ready + pending`. Failed
   members are in neither bucket, so `total` stayed 0, `create` stayed at
   `min_ready`, and every reconcile minted fresh replacements that failed and
   piled up. `max_ready` bounds `ready`, never the total CR count.

The condition was also invisible: `SandboxWarmPoolStatus` exposed only `ready`
and `members`.

## Changes

- **`crates/sandbawks-controller/src/warm_pool.rs`**
  - Added a `failed: Vec<Sandbox>` bucket to `PoolMembers`; `bucket()` now routes
    `Failed`/`Terminating` members into it instead of dropping them. Corrected
    the now-inaccurate doc comment.
  - Added `PoolMembers::total()` (sum across all buckets) — the true owned-CR
    count.
  - `reconcile_pool` now reaps `members.failed` via `delete_sandbox`, alongside
    the existing `stale` / `version_stale` reaps. This caps the standing
    `Failed` population; freed slots are re-minted by top-up on a later
    reconcile.
  - `status_for` now reports `failed` and `total`; the structured reconcile log
    line gained `reap` and `total` fields.
  - `record_reconcile` call now passes `failed`.
- **`crates/sandbawks-controller/src/warm_pool_metrics.rs`**
  - New `warm_pool.failed` gauge; `failed` field on `PoolObservation`; recorded
    every pass. Doc table and rationale updated (six instruments).
- **`crates/sandbawks-types/src/warm_pool.rs`**
  - Added `failed: u32` and `total: u32` to `SandboxWarmPoolStatus`, plus
    `Failed` / `Total` print-columns on the CRD.
- **`deploy/crds/sandboxwarmpools.sandbawks.dev.yaml`** — regenerated via
  `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`.
- **Tests** — `bucket_routes_failed_and_terminating_to_failed` (renamed/updated),
  `status_surfaces_failed_and_total_counts`, `reconcile_reaps_failed_members_and_refills`;
  metrics tests assert the new `warm_pool.failed` series.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — all green (62 test binaries OK; new tests pass)
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` — CRD drift check satisfied

## Out of scope

- Mint backoff / circuit-breaking and retry-in-place on `transient:` create
  errors (issue options b/c) — separate hardening; this change stops the
  unbounded leak and makes the condition observable.
- Forensic retention of recent `Failed` members for post-mortem.
