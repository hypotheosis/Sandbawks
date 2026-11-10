//! OTel reconcile metrics for the controller's kube-rs control loops.
//!
//! Two instruments, both under the meter `sandbawks-controller.reconcile` and
//! labelled by CRD `kind` so the Sandbox loop and the warm-pool loop are
//! distinguishable on one dashboard:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `controller.reconcile.duration` | `Histogram<f64>` | s | kind, outcome |
//! | `controller.reconcile.outcome_total` | `Counter<u64>` | --- | kind, outcome |
//!
//! `outcome` takes three values:
//!
//! - `success` — the reconcile call returned `Ok`.
//! - `error` — the reconcile call returned `Err` (the runtime then consults the
//!   error policy).
//! - `requeue` — the error policy fired and scheduled a backoff retry. This is a
//!   *counter-only* signal ([`record_requeue`]): the error policy is an instant
//!   decision hook with no meaningful duration, so feeding a near-zero point
//!   into the latency histogram would only skew it.
//!
//! `error` and `requeue` therefore fire on the same failure but answer different
//! questions — how many reconciles failed vs. how often the loop backed off.
//!
//! Instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` set the
//! global meter is a no-op, so these calls stay on the path unconditionally at
//! zero cost.

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.reconcile";

/// Cached reconcile instruments, built once via [`metrics`].
struct ReconcileMetrics {
    /// `controller.reconcile.duration` — per-reconcile wall-clock in seconds.
    duration: Histogram<f64>,
    /// `controller.reconcile.outcome_total` — reconcile outcome counter.
    outcome: Counter<u64>,
}

impl ReconcileMetrics {
    /// Build both instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            duration: meter
                .f64_histogram("controller.reconcile.duration")
                .with_unit("s")
                .with_description("Duration of a controller reconcile pass.")
                .build(),
            outcome: meter
                .u64_counter("controller.reconcile.outcome_total")
                .with_description("Controller reconcile passes by outcome.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::reconcile_metrics`]).
fn metrics() -> &'static ReconcileMetrics {
    static METRICS: OnceLock<ReconcileMetrics> = OnceLock::new();
    METRICS.get_or_init(ReconcileMetrics::new)
}

/// `success` / `error` outcome label, derived from a reconcile [`Result`].
///
/// Free function rather than a closure so both `Result<(), E>` (the Sandbox
/// loop) and `Result<Action, E>` (the warm-pool loop) reuse one mapping.
fn outcome_label<T, E>(result: &Result<T, E>) -> &'static str {
    if result.is_ok() {
        "success"
    } else {
        "error"
    }
}

/// Record one completed reconcile pass: its `duration` and its `success`/`error`
/// outcome, both labelled by CRD `kind` (e.g. `"Sandbox"`).
///
/// Generic over the `Ok` payload so the same call site shape works for a
/// reconcile that returns `()` and one that returns a kube-rs `Action`.
pub fn record_reconcile<T, E>(kind: &'static str, result: &Result<T, E>, duration: Duration) {
    let metrics = metrics();
    let attrs = [
        KeyValue::new("kind", kind),
        KeyValue::new("outcome", outcome_label(result)),
    ];
    metrics.duration.record(duration.as_secs_f64(), &attrs);
    metrics.outcome.add(1, &attrs);
}

/// Record an error-policy backoff for `kind`: a counter-only `requeue` outcome.
///
/// Called from the kube-rs error policy, which runs after a failed reconcile to
/// schedule a retry. No duration is recorded — see the module docs.
pub fn record_requeue(kind: &'static str) {
    let attrs = [
        KeyValue::new("kind", kind),
        KeyValue::new("outcome", "requeue"),
    ];
    metrics().outcome.add(1, &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_label_maps_ok_and_err() {
        let ok: Result<(), &str> = Ok(());
        let err: Result<(), &str> = Err("boom");
        assert_eq!(outcome_label(&ok), "success");
        assert_eq!(outcome_label(&err), "error");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; the recording calls must still be safe to invoke on the path.
        let ok: Result<(), &str> = Ok(());
        record_reconcile("Sandbox", &ok, Duration::from_millis(7));
        let err: Result<(), &str> = Err("boom");
        record_reconcile("SandboxWarmPool", &err, Duration::from_millis(3));
        record_requeue("Sandbox");
    }
}
