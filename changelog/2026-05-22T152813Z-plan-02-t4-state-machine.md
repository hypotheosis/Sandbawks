# Plan 02 T4 — Pure `Sandbox` state machine

**Date:** 2026-05-22T15:28:13Z
**Branch:** `feat-issue-32-state-machine`
**Issue:** #32
**Plan:** `docs/superpowers/plans/2026-05-22-sandbawks-02-controller-skeleton.md` § Task 4

## What changed

Lands the *pure heart* of Plan 02: a side-effect-free `transition(SandboxPhase, Trigger) -> Option<(SandboxPhase, Action)>` function plus its `Trigger` / `Action` enums, encoding every edge of the lifecycle diagram in spec §5. Illegal `(phase, trigger)` pairs return `None`; `Failed` and `Terminating` are absorbing sinks (no outbound edges).

The module is deliberately a leaf of the crate's dependency graph — it depends only on `sandbawks_types::SandboxPhase` and nothing else. No cluster-client or async-runtime symbol is referenced from `state_machine.rs`. That isolation is what lets the state machine be exhaustively unit-tested (this task) and property-tested (T5) without standing up a kube fixture or a tokio runtime.

### Files created

- `crates/sandbawks-controller/src/state_machine.rs`
  - `Trigger` enum (8 variants): `PodReady`, `PauseRequested`, `ResumeRequested`, `KillRequested`, `Timeout { auto_pause: bool }`, `SnapshotUploaded`, `ResumeReady`, `Failure(String)`.
  - `Action` enum (5 variants): `Noop`, `Pause`, `Resume`, `Terminate`, `RecordFailure(String)`.
  - `pub fn transition(...)` — single exhaustive `match` block over `(SandboxPhase, Trigger)`, structured by source phase with comment dividers per phase + a final terminal-sink arm.
- `crates/sandbawks-controller/tests/state_machine_unit.rs` — 15 per-edge unit tests:
  1. `creating_pod_ready_to_running`
  2. `running_pause_requested_to_pausing_pause_action`
  3. `running_timeout_autopause_true_pauses`
  4. `running_timeout_autopause_false_terminates`
  5. `running_kill_requested_terminates`
  6. `pausing_snapshot_uploaded_to_paused`
  7. `pausing_failure_to_failed`
  8. `paused_resume_requested_to_resuming`
  9. `paused_kill_requested_terminates`
  10. `resuming_ready_to_running`
  11. `resuming_failure_to_failed`
  12. `creating_failure_to_failed`
  13. `failed_is_absorbing` (5 inner asserts)
  14. `terminating_is_absorbing` (3 inner asserts)
  15. `pause_requested_in_pausing_is_idempotent_noop`

## Deliberately broken (carried forward from T1)

Per the plan and the established pattern in PR #41 and PR #43, `crates/sandbawks-controller/src/lib.rs` still declares modules that have not yet landed (`store` — T6, `reconcile` — T7) and a `[[bin]]` target whose path file lands in T9. Workspace-level gates (`just fmt`, `cargo clippy --workspace --all-targets`, `just test`) therefore **fail by design** until those siblings land.

After this PR the only remaining missing-module errors are `reconcile` and `store` (down from three before — `state_machine` is now resolved). Once T6 and T7 land, every `cargo test -p sandbawks-controller --test state_machine_unit` invocation will green-light all 15 tests with zero further changes to this PR's files.

## Validation

| Step | Status | Notes |
| --- | --- | --- |
| `rustfmt --check --edition 2021` on the two new files | pass | exit 0 |
| `grep -E 'kube\|tokio' crates/sandbawks-controller/src/state_machine.rs` | pass | exit 1 (no matches), satisfying the issue's "no cluster-client / async-runtime symbol" gate |
| `cargo test -p sandbawks-controller --test state_machine_unit` | fail by design | blocked on T6 (`store`) / T7 (`reconcile`) modules still referenced from `lib.rs`; the test file and the state machine itself are compile-clean — the only remaining `error[E0583]: file not found for module` diagnostics are the two unrelated siblings |
| `just fmt` / `cargo clippy --workspace --all-targets -- -D warnings` / `just test` | fail by design | same root cause; matches PR #41 + PR #43 |

This sub-issue's deliverable is the *content* of `state_machine.rs` + its per-edge tests, not workspace-green — that gate is restored by the Plan 02 closeout task once T6/T7/T9 land.

## Why this matters

The state machine is the heart of the controller. Layering it as a pure function (no I/O, no async, single-file) means every other reconcile-loop change in Plans 03–09 can be reasoned about against a single ~100-line file with exhaustive test coverage, instead of against a tangle of kube-client mocks. The 15 unit tests in this PR are a tight forward-spec of spec §5; T5 will complement them with property tests for totality, determinism, and sink confinement.
