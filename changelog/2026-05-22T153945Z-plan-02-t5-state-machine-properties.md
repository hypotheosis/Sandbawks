# Plan 02 T5 — Property tests for state machine (proptest)

**Date:** 2026-05-22T15:39:45Z
**Branch:** `test-issue-33-state-machine-properties`
**Issue:** #33
**Plan:** `docs/superpowers/plans/2026-05-22-sandbawks-02-controller-skeleton.md` § Task 5

## What changed

Adds proptest-driven invariants that complement the 15 per-edge unit tests landed in PR #44 (T4) by sweeping the whole `(SandboxPhase, Trigger)` space. The unit tests pin individual legal edges; the property tests catch any *structural* regression — a panic on an unspecified pair, a non-deterministic match arm, an accidental edge out of an absorbing sink, a self-loop introduced by a bad refactor.

### Files created

- `crates/sandbawks-controller/tests/state_machine_properties.rs` — six `proptest!` blocks, default ~256 cases each (~1.5k cases per `cargo test` run):
  1. `transition_is_total` — `transition(phase, trigger)` never panics for any pair.
  2. `transition_is_deterministic` — calling twice with the same inputs returns the same output.
  3. `failed_is_absorbing` — `transition(Failed, _)` is always `None`.
  4. `terminating_is_absorbing` — `transition(Terminating, _)` is always `None`.
  5. `sink_states_stay_sunk` — two-step composition: if step 1 lands on `Failed` / `Terminating`, step 2 from there is always `None`.
  6. `no_self_loops` — a successful transition never returns the same `SandboxPhase` it started in.

`proptest = "1.5"` was already wired into `[dev-dependencies]` by PR #41 (T1), so this PR ships exactly one file.

### Strategies

- `any_phase()` enumerates all 7 `SandboxPhase` variants via `prop_oneof![Just(...), ...]` — exhaustive, no shrinking needed.
- `any_trigger()` enumerates all 8 `Trigger` variants. `Timeout { auto_pause }` uses `any::<bool>()` so both branches are exercised; `Failure(String)` uses the regex strategy `"[a-z]{1,16}"` so the proptest engine has something meaningful to shrink against on a hypothetical regression.

## Deliberately broken (carried forward from T1)

Per the plan and the established pattern in PRs #41, #43, and #44, `crates/sandbawks-controller/src/lib.rs` still declares modules that have not yet landed (`store` — T6, `reconcile` — T7) and a `[[bin]]` target whose path file lands in T9. Workspace-level gates (`just fmt`, `cargo clippy --workspace --all-targets`, `just test`) and the per-test invocation `cargo test -p sandbawks-controller --test state_machine_properties` therefore **fail by design** until those siblings land — the only blockers are the two unrelated `error[E0583]: file not found for module` diagnostics on `reconcile` / `store` in `lib.rs`. The property-test file itself is compile-clean (verified with `rustfmt --check`).

Workspace-green is restored by the Plan 02 closeout task once T6/T7/T9 land. At that point this PR's six proptests will run with zero further changes to this file.

## Validation

| Step | Status | Notes |
| --- | --- | --- |
| `rustfmt --check --edition 2021 crates/sandbawks-controller/tests/state_machine_properties.rs` | pass | exit 0 |
| Test count matches plan | pass | `grep -c '#\[test\]' crates/sandbawks-controller/tests/state_machine_properties.rs` → 6 |
| `cargo test -p sandbawks-controller --test state_machine_properties` | fail by design | blocked on T6 (`store`) / T7 (`reconcile`); error trace is exactly the two pre-existing `error[E0583]: file not found for module` lines on `lib.rs:20` and `lib.rs:22` |
| `just fmt` / `cargo clippy --workspace --all-targets -- -D warnings` / `just test` | fail by design | same root cause; matches PRs #41 / #43 / #44 |

## Why this matters

Per-edge unit tests prove the spec §5 graph is *encoded*. Property tests prove the encoding is *structurally sound* — there's no `(phase, trigger)` pair that panics, no non-determinism (would matter if the match arms ever started consulting external state), and the absorbing sinks really absorb under arbitrary two-step composition. As Plans 03–05 grow the reconcile loop and adapter wiring, these six properties become a cheap regression net that catches whole categories of bug at `cargo test` time instead of in a kube fixture.
