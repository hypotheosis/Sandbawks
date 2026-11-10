//! OTel metrics for the controller's reconcile-action dispatch (issue #373).
//!
//! Two instruments, under the meter `sandbawks-controller.dispatch`, recorded
//! once per [`crate::reconcile`] dispatch and labelled by the state machine's
//! `action` ([`crate::state_machine::Action::label`]):
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `controller.dispatch.action_total` | `Counter<u64>` | --- | action |
//! | `controller.dispatch.duration` | `Histogram<f64>` | s | action |
//!
//! These mirror the `dispatch:controller::action` span added in #372: that span
//! already carries `action = action.label()` — an already-shaped, low-cardinality
//! label (`noop` / `create_pod` / `pause` / `stage_snapshot` /
//! `create_restore_pod` / `terminate` / `record_failure`, the `RecordFailure`
//! payload deliberately elided). The counter answers "which actions does the
//! reconcile loop dispatch, and how often"; the histogram adds the per-action
//! wall-clock so a heavy `pause` (which creates a `SnapshotJob` CR) is
//! distinguishable on a dashboard from a cheap `noop`.
//!
//! This is deliberately **not** a failure counter — a failed action folds the
//! Sandbox into the `Failed` phase, and that signal is tracked separately under
//! #339. The duration here times the whole dispatch call regardless of outcome,
//! so the histogram count equals the action counter for each action.
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` set (or
//! no installed provider, as in unit tests) the global meter is a no-op, so this
//! call stays on the reconcile path unconditionally at zero cost.

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.dispatch";

/// Cached dispatch instruments, built once via [`metrics`].
struct DispatchMetrics {
    /// `controller.dispatch.action_total` — dispatched-action counter.
    action_total: Counter<u64>,
    /// `controller.dispatch.duration` — per-action dispatch wall-clock (s).
    duration: Histogram<f64>,
}

impl DispatchMetrics {
    /// Build both instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            action_total: meter
                .u64_counter("controller.dispatch.action_total")
                .with_description("Reconcile actions dispatched, by action.")
                .build(),
            duration: meter
                .f64_histogram("controller.dispatch.duration")
                .with_unit("s")
                .with_description("Duration of a reconcile action dispatch, by action.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::dispatch_metrics`]).
fn metrics() -> &'static DispatchMetrics {
    static METRICS: OnceLock<DispatchMetrics> = OnceLock::new();
    METRICS.get_or_init(DispatchMetrics::new)
}

/// Record one dispatched reconcile action: bump its counter and record its
/// `duration`, both labelled by the stable `action` label
/// ([`crate::state_machine::Action::label`]).
///
/// `action` is a `&'static str` — the closed set of variant labels — so the
/// `action` series stays bounded no matter how many Sandboxes reconcile.
pub fn record_dispatch(action: &'static str, duration: Duration) {
    let metrics = metrics();
    let attrs = [KeyValue::new("action", action)];
    metrics.action_total.add(1, &attrs);
    metrics.duration.record(duration.as_secs_f64(), &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; `record_dispatch` must still be safe to invoke on the path.
        record_dispatch("pause", Duration::from_millis(42));
        record_dispatch("noop", Duration::from_micros(3));
    }
}
