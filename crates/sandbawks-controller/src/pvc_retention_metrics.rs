//! OTel metric for PVC-retention garbage collection (epic #603 Phase 5 /
//! issue #608).
//!
//! A paused `filesystem`-mode sandbox keeps a live PVC that bills until
//! reclaimed. When its retention window elapses the reconcile loop deletes the
//! Sandbox CR, cascading the PVC (and its data) via ownerReferences — an
//! irreversible, time-driven deletion of customer state. Unlike the
//! snapshot-retention sweep (which tallies deleted/kept/failed per pass), this
//! GC is a per-Sandbox reconcile action, so a single monotonic counter is the
//! natural shape: each increment is exactly one reclaimed sandbox.
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `controller.pvc_retention.reclaimed_total` | `Counter<u64>` | --- | --- |
//!
//! ## Why this counter matters operationally
//!
//! A destructive auto-deletion that emits only a Kubernetes Event + log line is
//! visible per-object in `kubectl describe` but invisible in aggregate. A spike
//! in `rate(controller_pvc_retention_reclaimed_total)` is a destructive-action
//! signal worth alerting on — e.g. a mis-set `pvcRetention` or a default
//! window that is too aggressive reclaiming far more sandboxes than expected.
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in unit tests) the global meter is a no-op, so the
//! call stays on the reconcile path unconditionally at zero cost — mirroring
//! [`crate::sandbox_recovery_metrics`].

use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::metrics::Counter;

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.pvc_retention";

/// Cached PVC-retention instrument, built once via [`metrics`].
struct PvcRetentionMetrics {
    /// `controller.pvc_retention.reclaimed_total` — paused filesystem sandboxes
    /// deleted (cascading their PVCs) after exceeding their retention window.
    reclaimed_total: Counter<u64>,
}

impl PvcRetentionMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            reclaimed_total: meter
                .u64_counter("controller.pvc_retention.reclaimed_total")
                .with_description(
                    "Paused filesystem sandboxes deleted (cascading their PVCs) after exceeding \
                     their PVC retention window.",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::pvc_retention_metrics`]).
fn metrics() -> &'static PvcRetentionMetrics {
    static METRICS: OnceLock<PvcRetentionMetrics> = OnceLock::new();
    METRICS.get_or_init(PvcRetentionMetrics::new)
}

/// Record one PVC-retention reclamation (one paused filesystem Sandbox deleted).
///
/// Called from the reconcile loop's PVC-retention GC branch, right after the
/// Sandbox delete is issued (issue #608).
pub fn record_reclaimed() {
    metrics().reclaimed_total.add(1, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; `record_reclaimed` must still be safe to invoke on the path.
        record_reclaimed();
        record_reclaimed();
    }
}
