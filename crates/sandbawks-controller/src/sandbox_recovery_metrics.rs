//! OTel metrics for Sandbox recovery actions (issues #510 and #407).
//!
//! A fail-closed pause checkpoint folds a Sandbox to terminal `Failed` while
//! deliberately leaving its workload Pod running. The reconcile loop's
//! `Failed`-phase recovery probe then either recovers that Sandbox back to
//! `Running` (the Pod + envd are healthy — the pause simply never happened) or
//! GCs the orphaned Pod once it is provably unrecoverable. Both outcomes are
//! emitted here as a single counter so an operator can alert on — and
//! distinguish — them:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `controller.sandbox.recovery_total` | `Counter<u64>` | --- | outcome |
//!
//! ## The `outcome` label
//!
//! Drawn from the closed [`RecoveryOutcome`] set (the same compiler-checked,
//! bounded-cardinality approach as
//! [`crate::state_machine::FailureReason`] in
//! [`crate::sandbox_failure_metrics`]):
//!
//! | `outcome` | When |
//! |---|---|
//! | `recovered_to_running` | A `Failed` (pause-checkpoint) Sandbox whose Pod container was `Ready` was recovered to `Running` (tier 2). |
//! | `orphan_gc` | A `Failed` (pause-checkpoint) Sandbox's unrecoverable Pod was deleted to reclaim the node slot (tier 3). |
//! | `eviction_restored` | A `Running` Sandbox whose Pod was evicted / OOMKilled was recovered by restoring from its last snapshot (issue #407). |
//! | `eviction_state_lost` | A `Running` Sandbox whose Pod was evicted / OOMKilled had no snapshot, so a fresh clean-state Pod was provisioned and in-flight state was lost (issue #407). |
//!
//! ## Why this counter matters operationally
//!
//! The orphaned-Pod failure mode (issue #510) previously lingered silently for
//! 38h+, holding a Guaranteed node slot with no signal. A rising
//! `rate(controller_sandbox_recovery_total{outcome="orphan_gc"})` now surfaces
//! how often pause-checkpoint orphans are being reclaimed, and a rising
//! `recovered_to_running` rate flags a pause path that keeps failing-then-
//! recovering (e.g. the OTel-wedge contributing trigger, #356) without ever
//! reaching `Paused`.
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in unit tests) the global meter is a no-op, so the
//! call stays on the reconcile path unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.sandbox";

/// Bounded outcome label for `controller.sandbox.recovery_total` (issue #510).
///
/// Modelling the vocabulary as an enum keeps the label set closed and
/// exhaustively checked, mirroring [`crate::state_machine::FailureReason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// A `Failed` (pause-checkpoint) Sandbox whose Pod container was `Ready`
    /// was recovered back to `Running` (tier 2).
    RecoveredToRunning,
    /// A `Failed` (pause-checkpoint) Sandbox's unrecoverable orphaned Pod was
    /// deleted to reclaim the node slot (tier 3).
    OrphanGc,
    /// A `Running` Sandbox whose backing Pod was evicted / OOMKilled was
    /// recovered by restoring from its last snapshot (issue #407):
    /// `Running → Resuming`, preserving filesystem continuity.
    EvictionRestored,
    /// A `Running` Sandbox whose backing Pod was evicted / OOMKilled had no
    /// snapshot to restore from, so a fresh clean-state Pod was provisioned and
    /// the in-flight filesystem state was lost (issue #407): `Running → Creating`.
    EvictionStateLost,
}

impl RecoveryOutcome {
    /// Stable, lowercase label used as the OTel attribute value.
    pub fn label(self) -> &'static str {
        match self {
            Self::RecoveredToRunning => "recovered_to_running",
            Self::OrphanGc => "orphan_gc",
            Self::EvictionRestored => "eviction_restored",
            Self::EvictionStateLost => "eviction_state_lost",
        }
    }
}

/// Cached recovery instrument, built once via [`metrics`].
struct SandboxRecoveryMetrics {
    /// `controller.sandbox.recovery_total` — `Failed`-Sandbox recovery actions
    /// by outcome.
    recovery_total: Counter<u64>,
}

impl SandboxRecoveryMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            recovery_total: meter
                .u64_counter("controller.sandbox.recovery_total")
                .with_description(
                    "Recovery actions on Failed (pause-checkpoint) Sandboxes, by outcome.",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::sandbox_recovery_metrics`]).
fn metrics() -> &'static SandboxRecoveryMetrics {
    static METRICS: OnceLock<SandboxRecoveryMetrics> = OnceLock::new();
    METRICS.get_or_init(SandboxRecoveryMetrics::new)
}

/// Record one Sandbox recovery action, labelled by `outcome`.
///
/// Called from the [`crate::state_machine::Action::RecoverToRunning`] and
/// [`crate::state_machine::Action::GcOrphanedPod`] dispatch arms (issue #510),
/// and from the reconcile loop's eviction-recovery condition stamp (issue #407,
/// the `eviction_restored` / `eviction_state_lost` outcomes).
pub fn record_recovery(outcome: RecoveryOutcome) {
    let attrs = [KeyValue::new("outcome", outcome.label())];
    metrics().recovery_total.add(1, &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_label_covers_every_variant() {
        assert_eq!(
            RecoveryOutcome::RecoveredToRunning.label(),
            "recovered_to_running"
        );
        assert_eq!(RecoveryOutcome::OrphanGc.label(), "orphan_gc");
        assert_eq!(
            RecoveryOutcome::EvictionRestored.label(),
            "eviction_restored"
        );
        assert_eq!(
            RecoveryOutcome::EvictionStateLost.label(),
            "eviction_state_lost"
        );
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; `record_recovery` must still be safe to invoke on the path.
        record_recovery(RecoveryOutcome::RecoveredToRunning);
        record_recovery(RecoveryOutcome::OrphanGc);
    }
}
