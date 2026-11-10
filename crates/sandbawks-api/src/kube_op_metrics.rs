//! OTel metrics for the gateway's kube-client operations (issue #373).
//!
//! Two instruments, under the meter `sandbawks-api.kube_op`, recorded once per
//! [`crate::routes::sandboxes::KubeSandboxOps`] /
//! [`crate::warm_claim::KubeWarmClaim`] call and labelled by the kube `resource`
//! and `op` (and, on the counter, its outcome):
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `api.kube_op.duration` | `Histogram<f64>` | s | resource, op |
//! | `api.kube_op.outcome_total` | `Counter<u64>` | --- | resource, op, outcome |
//!
//! These mirror the `kube:api::*` client spans added in #372. `resource` is the
//! kube kind the call targets (`sandbox` / `warmpool`); `op` is the adapter
//! operation (`create` / `list` / `delete` / `patch_desired_phase` /
//! `patch_timeout` / `get` for the Sandbox ops; `pool_for_template` /
//! `running_candidates` / `relabel_claim` / `apply_overrides` for the warm-claim
//! ops). Both label sets are closed, so the series stay bounded.
//!
//! ## Why a separate layer from the existing api metrics
//!
//! This is the **kube-client** layer — one point per apiserver round-trip — and
//! is deliberately distinct from:
//!
//! - `http.server.*` ([`crate::http_metrics`]) — the *inbound request* layer
//!   (one point per HTTP request the gateway serves). A single inbound request
//!   may make several kube calls (e.g. `connect` patches phase then gets), so
//!   these count different things and do not double-count.
//! - `warm.claims_total` / `warm.claim.duration`
//!   ([`crate::warm_claim_metrics`]) — the *claim-orchestration* layer (one
//!   point per `claim_for`, recording the warm-vs-cold SLI). `api.kube_op`
//!   instead times the individual apiserver calls *inside* a claim
//!   (`running_candidates`, `relabel_claim`, …), which the orchestration metric
//!   intentionally does not break out.
//!
//! `outcome` is `success` / `error` from the call's [`Result`]; note a
//! `relabel_claim` that loses its compare-and-swap folds the 409/404 to
//! `Ok(false)` *inside* the method, so it records `success` here (the kube call
//! itself succeeded) — the lost race is the orchestration layer's
//! `warm.claims_total{outcome="race_lost"}` signal, not a kube-op error.
//!
//! The instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` set (or
//! no installed provider, as in unit tests) the global meter is a no-op, so these
//! calls stay on the request path unconditionally at zero cost.

use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-api.kube_op";

/// Cached kube-op instruments, built once via [`metrics`].
struct KubeOpMetrics {
    /// `api.kube_op.duration` — per-call wall-clock (s).
    duration: Histogram<f64>,
    /// `api.kube_op.outcome_total` — per-call outcome counter.
    outcome: Counter<u64>,
}

impl KubeOpMetrics {
    /// Build both instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            duration: meter
                .f64_histogram("api.kube_op.duration")
                .with_unit("s")
                .with_description(
                    "Duration of a gateway kube-client operation, by resource and op.",
                )
                .build(),
            outcome: meter
                .u64_counter("api.kube_op.outcome_total")
                .with_description("Gateway kube-client operations by resource, op, and outcome.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::kube_op_metrics`]).
fn metrics() -> &'static KubeOpMetrics {
    static METRICS: OnceLock<KubeOpMetrics> = OnceLock::new();
    METRICS.get_or_init(KubeOpMetrics::new)
}

/// Record one completed kube-client operation: its `duration` (labelled by
/// `resource` + `op`) and its `success`/`error` `outcome` (labelled by
/// `resource` + `op` + `outcome`).
///
/// Generic over the `Ok` payload so one call shape serves every adapter method
/// regardless of its return type.
pub fn record_kube_op<T, E>(
    resource: &'static str,
    op: &'static str,
    result: &Result<T, E>,
    duration: Duration,
) {
    let metrics = metrics();
    let base = [KeyValue::new("resource", resource), KeyValue::new("op", op)];
    metrics.duration.record(duration.as_secs_f64(), &base);
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics.outcome.add(
        1,
        &[
            KeyValue::new("resource", resource),
            KeyValue::new("op", op),
            KeyValue::new("outcome", outcome),
        ],
    );
}

/// Time `fut` and [`record_kube_op`] its outcome under `resource` + `op`,
/// returning the future's result unchanged.
///
/// The ergonomic call wrapper for the adapter methods: wrap the method body in
/// `kube_op_metrics::timed("sandbox", "create", async { … }).await` and the
/// duration spans exactly the awaited apiserver round-trip.
pub async fn timed<T, E, F>(resource: &'static str, op: &'static str, fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let start = Instant::now();
    let result = fut.await;
    record_kube_op(resource, op, &result, start.elapsed());
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
        record_kube_op("sandbox", "create", &ok, Duration::from_millis(8));
        let err: Result<(), &str> = Err("boom");
        record_kube_op(
            "warmpool",
            "pool_for_template",
            &err,
            Duration::from_millis(3),
        );
    }

    #[tokio::test]
    async fn timed_passes_the_result_through() {
        // The wrapper records around the future and returns its value verbatim.
        let out: Result<u8, &str> = timed("sandbox", "get", async { Ok(9u8) }).await;
        assert_eq!(out, Ok(9));
        let err: Result<u8, &str> = timed("sandbox", "delete", async { Err("nope") }).await;
        assert_eq!(err, Err("nope"));
    }
}
