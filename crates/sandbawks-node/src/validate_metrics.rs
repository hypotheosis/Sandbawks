//! OTel metric for the pause-side restorability validation (issue #373).
//!
//! One instrument, under the meter `sandbawks-node.validate`, recorded once per
//! [`crate::gvisor`] `validate_restorable` call and labelled by the configured
//! validate `level` and the call's `outcome`:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `node.validate.outcome_total` | `Counter<u64>` | --- | level, outcome |
//!
//! This mirrors the `validate:node::restorable` span added in #372. `level` is
//! the `ValidateLevel` the adapter was constructed with (`off` / `head` / `sha`
//! / `full`, from `SANDBAWKS_S3_VALIDATE_LEVEL`); `outcome` is `success` /
//! `error`, derived from the validation [`Result`]. Both label sets are tiny and
//! code-fixed, so the series stays bounded.
//!
//! The counter is the fail-closed gate's health signal: a climbing
//! `node.validate.outcome_total{outcome="error"}` means pauses are failing
//! their post-upload restorability proof (the source is kept recoverable, the
//! SnapshotJob lands `Failed`), which an operator wants to see *before* it shows
//! up as stuck `Pausing` Sandboxes. Breaking it out by `level` lets a dashboard
//! tell a strict `full` re-materialize failure apart from a cheap `head`
//! object-presence miss.
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` set (or
//! no installed provider, as in unit tests) the global meter is a no-op, so this
//! call stays on the pause path unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.validate";

/// Cached validate instrument, built once via [`metrics`].
struct ValidateMetrics {
    /// `node.validate.outcome_total` — per-level validation outcome counter.
    outcome: Counter<u64>,
}

impl ValidateMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            outcome: meter
                .u64_counter("node.validate.outcome_total")
                .with_description("Pause restorability validations by level and outcome.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::validate_metrics`]).
fn metrics() -> &'static ValidateMetrics {
    static METRICS: OnceLock<ValidateMetrics> = OnceLock::new();
    METRICS.get_or_init(ValidateMetrics::new)
}

/// Record one restorability validation outcome, labelled by `level` (the
/// `ValidateLevel` label) and the `success`/`error` derived from `result`.
///
/// Generic over the `Ok` payload so the caller can pass its `Result<(), _>`
/// directly without unwrapping it first.
pub fn record_validate<T, E>(level: &'static str, result: &Result<T, E>) {
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics().outcome.add(
        1,
        &[
            KeyValue::new("level", level),
            KeyValue::new("outcome", outcome),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; both outcomes and a representative level must still be safe to
        // record on the pause path.
        let ok: Result<(), &str> = Ok(());
        record_validate("head", &ok);
        let err: Result<(), &str> = Err("mismatch");
        record_validate("full", &err);
    }
}
