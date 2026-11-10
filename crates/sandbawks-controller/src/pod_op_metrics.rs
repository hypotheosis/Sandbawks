//! OTel metrics for the controller's Pod-adapter operations (issue #373).
//!
//! Two instruments, under the meter `sandbawks-controller.pod_op`, recorded once
//! per [`crate::pod_adapter::PodAdapter`] call and labelled by the operation
//! name (and, on the counter, its outcome):
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `controller.pod_op.duration` | `Histogram<f64>` | s | op |
//! | `controller.pod_op.outcome_total` | `Counter<u64>` | --- | op, outcome |
//!
//! These mirror the `pod:controller::{create,delete,ready,terminated}` spans
//! added in #372. `op` is one of the four stable adapter operations — `create`,
//! `delete`, `ready`, `terminated` — so the label set is closed. `outcome` is
//! `success` / `error`, derived from the call's [`Result`]: a transient apiserver
//! error (the adapter's `RuntimeError::Transient`) records `error` even though
//! the reconcile loop then retries, which is the point — the counter exposes the
//! per-op failure rate the retry-quiet spans (`pod_ready` / `pod_terminated`
//! deliberately carry no `err`) otherwise hide.
//!
//! A not-ready probe (`pod_ready` returning `Ok(None)`) is a `success`: the call
//! itself succeeded, it just found the Pod not yet Ready.
//!
//! The instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` set (or
//! no installed provider, as in unit tests) the global meter is a no-op, so these
//! calls stay on the reconcile path unconditionally at zero cost.

use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.pod_op";

/// Cached pod-op instruments, built once via [`metrics`].
struct PodOpMetrics {
    /// `controller.pod_op.duration` — per-op wall-clock (s).
    duration: Histogram<f64>,
    /// `controller.pod_op.outcome_total` — per-op outcome counter.
    outcome: Counter<u64>,
}

impl PodOpMetrics {
    /// Build both instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            duration: meter
                .f64_histogram("controller.pod_op.duration")
                .with_unit("s")
                .with_description("Duration of a controller Pod-adapter operation, by op.")
                .build(),
            outcome: meter
                .u64_counter("controller.pod_op.outcome_total")
                .with_description("Controller Pod-adapter operations by op and outcome.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::pod_op_metrics`]).
fn metrics() -> &'static PodOpMetrics {
    static METRICS: OnceLock<PodOpMetrics> = OnceLock::new();
    METRICS.get_or_init(PodOpMetrics::new)
}

/// Record one completed Pod-adapter operation: its `duration` (labelled by `op`)
/// and its `success`/`error` `outcome` (labelled by `op` + `outcome`).
///
/// Generic over the `Ok` payload so the same call shape serves `create`
/// (`PodRef`), `delete` (`()`), `ready` (`Option<PodReadiness>`), and
/// `terminated` (`bool`).
pub fn record_pod_op<T, E>(op: &'static str, result: &Result<T, E>, duration: Duration) {
    let metrics = metrics();
    metrics
        .duration
        .record(duration.as_secs_f64(), &[KeyValue::new("op", op)]);
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics.outcome.add(
        1,
        &[KeyValue::new("op", op), KeyValue::new("outcome", outcome)],
    );
}

/// Time `fut` and [`record_pod_op`] its outcome under `op`, returning the
/// future's result unchanged.
///
/// The ergonomic call wrapper for the adapter methods: wrap the method body in
/// `pod_op_metrics::timed("create", async { … }).await` and the duration spans
/// exactly the awaited work.
pub async fn timed<T, E, F>(op: &'static str, fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let start = Instant::now();
    let result = fut.await;
    record_pod_op(op, &result, start.elapsed());
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; both outcomes must still be safe to record on the path.
        let ok: Result<(), &str> = Ok(());
        record_pod_op("create", &ok, Duration::from_millis(12));
        let err: Result<(), &str> = Err("boom");
        record_pod_op("delete", &err, Duration::from_millis(5));
    }

    #[tokio::test]
    async fn timed_passes_the_result_through() {
        // The wrapper records around the future and returns its value verbatim.
        let out: Result<u8, &str> = timed("ready", async { Ok(7u8) }).await;
        assert_eq!(out, Ok(7));
        let err: Result<u8, &str> = timed("terminated", async { Err("nope") }).await;
        assert_eq!(err, Err("nope"));
    }
}
