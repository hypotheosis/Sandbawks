//! Property tests for the Sandbox state machine.
//!
//! These complement the per-edge unit tests in `state_machine_unit.rs`
//! by checking invariants over the *whole* trigger/phase space.

use proptest::prelude::*;
use sandbawks_controller::{transition, FailureReason, Trigger};
use sandbawks_types::{PersistenceMode, SandboxPhase};

/// Both persistence modes (epic #603 / #606). Every invariant below must hold
/// for either mode — totality, determinism, and sink confinement are
/// mode-independent, and the only mode-specific edge (the filesystem suspend
/// self-loop) is handled explicitly in `no_self_loops`.
fn any_mode() -> impl Strategy<Value = PersistenceMode> {
    prop_oneof![
        Just(PersistenceMode::Snapshot),
        Just(PersistenceMode::Filesystem),
    ]
}

fn any_phase() -> impl Strategy<Value = SandboxPhase> {
    prop_oneof![
        Just(SandboxPhase::Creating),
        Just(SandboxPhase::Running),
        Just(SandboxPhase::Pausing),
        Just(SandboxPhase::Paused),
        Just(SandboxPhase::Resuming),
        Just(SandboxPhase::Terminating),
        Just(SandboxPhase::Failed),
    ]
}

fn any_trigger() -> impl Strategy<Value = Trigger> {
    prop_oneof![
        Just(Trigger::PodCreate),
        Just(Trigger::PodReady),
        Just(Trigger::PauseRequested),
        Just(Trigger::ResumeRequested),
        Just(Trigger::RestoreRequested),
        Just(Trigger::KillRequested),
        any::<bool>().prop_map(|auto_pause| Trigger::Timeout { auto_pause }),
        Just(Trigger::SnapshotValidated),
        Just(Trigger::NodeAssigned),
        Just(Trigger::StageComplete),
        Just(Trigger::RestorePodLost),
        Just(Trigger::PauseRetry),
        Just(Trigger::FailedPodHealthy),
        Just(Trigger::FailedPodOrphaned),
        Just(Trigger::PodReleased),
        any::<bool>().prop_map(|has_snapshot| Trigger::EvictedRecover {
            has_snapshot,
            message: "evicted while Running".into(),
        }),
        "[a-z]{1,16}".prop_map(|message| Trigger::Failure {
            reason: FailureReason::ActionFailure,
            message,
        }),
    ]
}

/// The two scoped recoverable edges out of `Failed` (issue #510). Every other
/// `(Failed, _)` pair stays absorbing; these two are the deliberate carve-out
/// the properties below exclude.
fn is_failed_recoverable_trigger(trigger: &Trigger) -> bool {
    matches!(
        trigger,
        Trigger::FailedPodHealthy | Trigger::FailedPodOrphaned
    )
}

proptest! {
    /// transition() never panics for any (phase, trigger, mode) triple.
    #[test]
    fn transition_is_total(phase in any_phase(), trigger in any_trigger(), mode in any_mode()) {
        let _ = transition(phase, trigger, mode);
    }

    /// transition() is deterministic — calling it twice with the same
    /// inputs returns the same output.
    #[test]
    fn transition_is_deterministic(
        phase in any_phase(),
        trigger in any_trigger(),
        mode in any_mode(),
    ) {
        let a = transition(phase, trigger.clone(), mode);
        let b = transition(phase, trigger, mode);
        prop_assert_eq!(a, b);
    }

    /// Failed has no outbound transitions, with the sole exception of the two
    /// scoped recoverable pause-checkpoint edges (issue #510): `FailedPodHealthy`
    /// (→ Running) and `FailedPodOrphaned` (→ Failed, GC self-loop). Every other
    /// trigger leaves it absorbing, in either mode.
    #[test]
    fn failed_is_absorbing_except_recoverable_edges(
        trigger in any_trigger(),
        mode in any_mode(),
    ) {
        let recoverable = is_failed_recoverable_trigger(&trigger);
        let r = transition(SandboxPhase::Failed, trigger, mode);
        if recoverable {
            prop_assert!(r.is_some());
        } else {
            prop_assert!(r.is_none());
        }
    }

    /// Terminating has no outbound transitions, in either mode.
    #[test]
    fn terminating_is_absorbing(trigger in any_trigger(), mode in any_mode()) {
        prop_assert!(transition(SandboxPhase::Terminating, trigger, mode).is_none());
    }

    /// Once a transition produces Terminating, no further trigger can move out
    /// of it. `Failed` stays sunk too, except for the two scoped recoverable
    /// pause-checkpoint edges (issue #510), which are excluded here.
    #[test]
    fn sink_states_stay_sunk(
        phase in any_phase(),
        t1 in any_trigger(),
        t2 in any_trigger(),
        mode in any_mode(),
    ) {
        if let Some((next, _)) = transition(phase, t1, mode) {
            match next {
                SandboxPhase::Terminating => prop_assert!(transition(next, t2, mode).is_none()),
                SandboxPhase::Failed if !is_failed_recoverable_trigger(&t2) => {
                    prop_assert!(transition(next, t2, mode).is_none());
                }
                _ => {}
            }
        }
    }

    /// A successful transition never lands back on the starting phase,
    /// with deliberate exceptions:
    ///
    /// 1. `(Creating, PodCreate) -> Creating` — the Pod-creation side
    ///    effect runs while the Sandbox stays in `Creating` until
    ///    [`Trigger::PodReady`] fires.
    /// 2. The `Resuming` sub-steps (epic #484, scheduler-placed restore).
    ///    `NodeAssigned`, `StageComplete`, and `RestorePodLost` each
    ///    advance the *internal* resume sub-state while the externally-
    ///    observable phase stays `Resuming`; it only advances to `Running`
    ///    once the born-restored Pod reaches Ready (`Trigger::PodReady`).
    /// 3. `(Pausing, PauseRetry) -> Pausing` — the tier-1 retry re-dispatches
    ///    the pause checkpoint while the phase stays `Pausing` (issue #510).
    /// 4. `(Failed, FailedPodOrphaned) -> Failed` — the tier-3 GC deletes the
    ///    orphaned Pod while the Sandbox stays `Failed` (issue #510).
    /// 5. `(Pausing, PauseRequested) -> Pausing` in `filesystem` mode — the
    ///    suspend re-drive re-runs the idempotent Pod delete while the phase
    ///    stays `Pausing` (issue #606). Only reachable in filesystem mode.
    #[test]
    fn no_self_loops(phase in any_phase(), trigger in any_trigger(), mode in any_mode()) {
        if let Some((next, _)) = transition(phase, trigger.clone(), mode) {
            let filesystem_suspend_redrive = mode == PersistenceMode::Filesystem
                && matches!((phase, &trigger), (SandboxPhase::Pausing, Trigger::PauseRequested));
            if filesystem_suspend_redrive
                || matches!(
                    (phase, &trigger),
                    (SandboxPhase::Creating, Trigger::PodCreate)
                        | (SandboxPhase::Resuming, Trigger::NodeAssigned)
                        | (SandboxPhase::Resuming, Trigger::StageComplete)
                        | (SandboxPhase::Resuming, Trigger::RestorePodLost)
                        | (SandboxPhase::Pausing, Trigger::PauseRetry)
                        | (SandboxPhase::Failed, Trigger::FailedPodOrphaned)
                )
            {
                prop_assert_eq!(next, phase);
            } else {
                prop_assert_ne!(next, phase);
            }
        }
    }
}
