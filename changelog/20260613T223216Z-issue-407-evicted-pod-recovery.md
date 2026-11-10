# Issue #407 — Sandbox pod eviction recovery path

**Date:** 2026-06-13T22:32:16Z
**Branch:** `feat-issue-407-evicted-pod-recovery`
**Area:** controller (`crates/sandbawks-controller`)

## Problem

When a Kubernetes Pod backing a `Running` sandbox was terminated by *resource
pressure* — a kubelet eviction (node-pressure / ephemeral-storage) or an
OOMKill — the controller folded the Sandbox to terminal `Failed`
(`FailureReason::PodEvicted`). The sandbox's filesystem state was silently
discarded with no recovery path: the behavior the issue calls "undefined."

## Design decision: reuse `Resuming` / `Creating`, do not add new phases

The issue's acceptance criteria name new phases (`Evicted` → `Restoring`,
`Evicted` → `Provisioning`). The controller already has those flows under
existing names:

- `Resuming` + `Action::CreateRestorePod` **is** restore-from-snapshot, with the
  bounded scheduler-placed-restore recovery loop (epic #484:
  `NodeAssigned → StageSnapshot → StageComplete → PodReady → Running`, capped by
  `resume_restart_count` / `RESUME_MAX_RESTARTS`).
- `Creating` + `Action::CreatePod` **is** provision-fresh.

Adding `Evicted` / `Restoring` / `Provisioning` enum variants would duplicate
that machinery, force a CRD schema change + regen, and gain nothing. We reuse
the existing phases and express the user-visible signal the issue actually
requires — *was filesystem continuity preserved?* — as `SandboxCondition`s
(`StateRestored` / `StateLost`). Confirmed with the maintainer before
implementing; also confirmed that OOMKill is treated identically to eviction.

A direct benefit: **no CRD change**. `SandboxPhase` is untouched and no new
status fields are introduced (the conditions use the existing free-form
`SandboxCondition`), so no `kubectl apply -f deploy/crds` is required.

## What changed

### State machine (`state_machine.rs`)

- New `Trigger::EvictedRecover { has_snapshot: bool, message: String }`.
- Two new `Running` edges:
  - `(Running, EvictedRecover { has_snapshot: true,  .. }) → (Resuming, CreateRestorePod)`
  - `(Running, EvictedRecover { has_snapshot: false, .. }) → (Creating, CreatePod)`

`Failure` (envd's own clean exit, or a deleted Pod —
`PodTerminationCause::Other`) still folds to terminal `Failed`: only
resource-pressure terminations are recoverable.

### Reconcile loop (`reconcile.rs`)

- `running_termination_to_trigger` (new): maps a terminated `Running` Pod's
  cause to either `EvictedRecover` (Evicted / OomKilled) or the existing
  `Failure` (Other). `has_snapshot` is read from `status.snapshotRef`. The
  eviction / OOM detail text is built via the existing
  `termination_cause_to_failure` (the issue #418 message) and carried on the
  trigger.
- `push_eviction_recovery_condition` (new): stamps the additive `StateRestored`
  (`reason: EvictedRestoredFromSnapshot`) or `StateLost`
  (`reason: EvictedNoSnapshot`) condition — message includes the kubelet detail
  in parentheses — and records the recovery metric. Stamped before the
  invariant-#1 status patch so it is durable even if dispatch fails.
- `Action::CreatePod` now deletes a pre-existing `pod_ref` and scrubs Pod
  identity before provisioning. Cold start never carries a `pod_ref` here
  (`derive_trigger` gates `PodCreate` on `!has_pod_ref`), so this only fires on
  the snapshot-less recovery edge, where the evicted Pod lingers as a terminal
  object that `create_pod` would otherwise idempotently return (409) instead of
  birthing a fresh, clean-state Pod.

The restore edge reuses `Action::CreateRestorePod` verbatim: because the
pre-transition phase is `Running` (not `Resuming`), it resets
`resume_restart_count` to 0, giving the eviction restore a fresh bounded-retry
budget, then deletes the evicted Pod and re-enters the epic-#484 restore flow.

### Metrics (`sandbox_recovery_metrics.rs`)

- `RecoveryOutcome::{EvictionRestored, EvictionStateLost}` added (labels
  `eviction_restored` / `eviction_state_lost`) on the existing
  `controller.sandbox.recovery_total` counter.

## Tests

- `state_machine_unit.rs`: `running_evicted_with_snapshot_restores`,
  `running_evicted_without_snapshot_reprovisions_fresh`.
- `state_machine_properties.rs`: `EvictedRecover` added to the trigger strategy
  (totality / determinism / Failed-absorbing / no-self-loop invariants hold).
- `reconcile.rs` (reconcile-loop integration tests against in-memory fakes):
  - `running_evicted_without_snapshot_reprovisions_fresh_state_lost` — evict a
    Running, snapshot-less Sandbox → `Creating`, `StateLost` condition naming
    the eviction cause, a fresh Pod provisioned, no SnapshotJob.
  - `running_oomkilled_with_snapshot_restores_from_snapshot` — OOMKill a Running
    Sandbox with a snapshot → `Resuming`, `StateRestored` condition naming the
    memory budget, `snapshotRef` preserved, `resume_restart_count == 0`.
- The two prior tests that asserted eviction → `Failed` were updated to the new
  recovery behavior; the issue #418 message-surfacing intent is preserved inside
  the new conditions.

## Notes / scope

- Eviction during `Creating` (a Pod with no user state yet) still folds to
  `Failed` via `creating_liveness_trigger`; recovery is scoped to `Running`,
  where filesystem state lives.
- **Flapping is bounded asymmetrically (deliberate).** The snapshot-less path
  is single-shot: if the fresh replacement is evicted again before becoming
  Ready, `creating_liveness_trigger` folds it to `Failed`. The snapshot-backed
  path is *not* outer-bounded — the `Resuming` restore loop is capped
  (`RESUME_MAX_RESTARTS` → `ResumeExhausted`), but `resume_restart_count` resets
  to 0 on every fresh `Running → Resuming` recovery, so a Sandbox that flaps
  `Running ↔ evicted` (reaching `Running` between evictions) can restore-loop
  indefinitely without folding to `Failed`. This is accepted: each restore is
  recoverable and keeps the Sandbox alive (strictly better than the pre-#407
  evict-to-`Failed`), and an outer flap cap would need a persisted
  cross-`Running` eviction counter — a new status field, i.e. the CRD change
  this design avoids. A sustained-pressure flap is visible as a rising
  `controller.sandbox.recovery_total{outcome="eviction_restored"}` rate; a
  bounded eviction-flap budget is a possible follow-up if it proves noisy.
- Cluster-level e2e (evict a real Pod on kind+gVisor and observe the CR
  condition) is left to the PR's manual test plan — no kind cluster is wired up
  in this workspace. The reconcile integration tests cover both recovery paths
  end-to-end through the controller (state machine + dispatch + status patch).

## Validation

- `just fmt` — clean
- `cargo clippy -p sandbawks-controller --all-targets -- -D warnings` — clean
- `just test` — full workspace, 0 failures
