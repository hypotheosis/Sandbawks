//! OTel metrics for Sandbox→`Failed` folds (issue #339).
//!
//! A single counter, under the meter `sandbawks-controller.sandbox`, incremented
//! every time the reconcile loop folds a Sandbox into the `Failed` phase:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `controller.sandbox.failure_total` | `Counter<u64>` | --- | reason |
//!
//! ## The `reason` label
//!
//! `reason` is drawn from a closed set defined by
//! [`crate::state_machine::FailureReason`] — the same compiler-checked,
//! bounded-cardinality approach as `warm_claim_metrics::ClaimOutcome` — so label
//! cardinality stays bounded regardless of how many Sandboxes fail:
//!
//! | `reason` | When |
//! |---|---|
//! | `pod_vanished_creating` | Backing Pod vanished/went terminal before the Sandbox became `Running` (node churn, Pod GC — issue #338). |
//! | `pod_terminated_running` | Backing Pod terminated while the Sandbox was `Running` (envd exited or Pod deleted). |
//! | `pod_evicted` | Backing Pod evicted by the kubelet (ephemeral-storage exhaustion, node-pressure, or OOMKill — issue #418). |
//! | `snapshot_job_failed` | A `SnapshotJob` (pause / stage / resume) reached `phase=Failed`. |
//! | `action_failure` | A reconcile action (`create_pod`, `pause`, `terminate`, etc.) returned an error. |
//! | `resume_exhausted` | The bounded `Resuming` recovery loop recreated the restore Pod `RESUME_MAX_RESTARTS` times without reaching `Running` (epic #484). |
//!
//! ## Why this counter matters operationally
//!
//! The existing signals (`phase transition committed` info log,
//! `controller.reconcile.outcome_total{outcome=success}`) do not surface
//! Sandbox failure folds alertably: a reconcile that records a failure
//! returns `Ok`, so the reconcile-outcome counter increments on `success`.
//! This counter is the dedicated, alertable signal the issue calls for.
//! A node-churn storm that wedges warm-pool members in `Creating` is now
//! visible as a rising `rate(controller_sandbox_failure_total{reason="pod_vanished_creating"})`.
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in unit tests) the global meter is a no-op, so the
//! call stays on the reconcile path unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

use crate::state_machine::FailureReason;

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.sandbox";

/// Cached sandbox-failure instrument, built once via [`metrics`].
struct SandboxFailureMetrics {
    /// `controller.sandbox.failure_total` — Sandbox→Failed folds by reason.
    failure_total: Counter<u64>,
}

impl SandboxFailureMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            failure_total: meter
                .u64_counter("controller.sandbox.failure_total")
                .with_description(
                    "Sandboxes folded into the Failed phase, by bounded failure reason.",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::sandbox_failure_metrics`]).
fn metrics() -> &'static SandboxFailureMetrics {
    static METRICS: OnceLock<SandboxFailureMetrics> = OnceLock::new();
    METRICS.get_or_init(SandboxFailureMetrics::new)
}

/// Record one Sandbox→`Failed` fold, labelled by the bounded `reason`.
///
/// Called from [`crate::reconcile`] at every site where the reconcile loop
/// commits a Sandbox to `phase=Failed` — both the direct
/// [`crate::state_machine::Action::RecordFailure`] dispatch and the action
/// dispatch-error recovery path. See the module docs for the full reason
/// vocabulary.
pub fn record_failure(reason: FailureReason) {
    let attrs = [KeyValue::new("reason", reason.label())];
    metrics().failure_total.add(1, &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_label_covers_every_variant() {
        assert_eq!(
            FailureReason::PodVanishedCreating.label(),
            "pod_vanished_creating"
        );
        assert_eq!(
            FailureReason::PodTerminatedRunning.label(),
            "pod_terminated_running"
        );
        assert_eq!(FailureReason::PodEvicted.label(), "pod_evicted");
        assert_eq!(
            FailureReason::SnapshotJobFailed.label(),
            "snapshot_job_failed"
        );
        assert_eq!(FailureReason::ActionFailure.label(), "action_failure");
        assert_eq!(FailureReason::ResumeExhausted.label(), "resume_exhausted");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; `record_failure` must still be safe to invoke on the path.
        record_failure(FailureReason::PodTerminatedRunning);
        record_failure(FailureReason::PodVanishedCreating);
        record_failure(FailureReason::PodEvicted);
        record_failure(FailureReason::SnapshotJobFailed);
        record_failure(FailureReason::ActionFailure);
        record_failure(FailureReason::ResumeExhausted);
    }
}
