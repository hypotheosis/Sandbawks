//! OTel outcome counter for envd's quiesce/rearm lifecycle calls (issue #413).
//!
//! PR #412 wired envd's OTel lifecycle into the node adapter: `/__quiesce`
//! before `runsc checkpoint` (pause) and `/__rearm` after the post-restore bind
//! wait (resume). Both calls are best-effort — a failure is logged at `warn!`
//! and pause/resume continues. The `envd_quiesce` / `envd_rearm` phase marks
//! feed the `node.snapshot.phase.duration` histogram (#373), but that ticks on
//! *either* outcome (wall time, not success), so there is no outcome signal an
//! operator can alert on.
//!
//! One instrument, under the meter `sandbawks-node.envd`, recorded once per
//! quiesce/rearm call at the two call sites in [`crate::gvisor`]:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `node.envd.lifecycle_total` | `Counter<u64>` | --- | endpoint, outcome |
//!
//! The counter ticks on **both** the `Ok` and `Err` paths, so the `outcome`
//! label (`success` / `failure`) carries the denominator a rate alert needs in
//! the *same* series — the SRE SLO is simply
//! `sum(outcome="failure") / sum(...)` per `endpoint`, no second series to join.
//! A climbing failure rate on `endpoint="/__quiesce"` means spans are being
//! dropped across checkpoints; on `endpoint="/__rearm"` it means restored
//! sandboxes are losing telemetry. This mirrors the `outcome`-as-label shape of
//! the sibling [`crate::envd_digest_metrics`] / [`crate::validate_metrics`]
//! gates rather than a bare failure-only counter.
//!
//! The `endpoint` label distinguishes the two lifecycle calls (folding them into
//! one instrument); both label sets are tiny and code-fixed — endpoint is one of
//! two `&'static` paths, outcome one of two words — so the series stays
//! low-cardinality. Deliberately *no* pod IP or sandbox ID label (per the
//! issue's bounded-cardinality requirement).
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` set (or
//! no installed provider, as in unit tests) the global meter is a no-op, so
//! recording stays on the pause/resume paths unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.envd";

/// Which envd lifecycle call is being recorded, carrying its `endpoint` label.
///
/// Modelling the two calls as an enum (rather than passing an `&str` at each
/// call site) keeps the `endpoint` label vocabulary closed and compiler-checked
/// — a Rust idiom a Python developer might otherwise express as loose string
/// constants. The two variants correspond one-to-one to the best-effort call
/// sites in [`crate::gvisor::GvisorAdapter::pause`] / `resume`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvdLifecycleCall {
    /// `/__quiesce` before `runsc checkpoint` (pause path).
    Quiesce,
    /// `/__rearm` after the post-restore bind wait (resume path).
    Rearm,
}

impl EnvdLifecycleCall {
    /// Stable `endpoint` label: the POSTed endpoint path.
    fn endpoint(self) -> &'static str {
        match self {
            Self::Quiesce => "/__quiesce",
            Self::Rearm => "/__rearm",
        }
    }
}

/// Cached envd-lifecycle instrument, built once via [`metrics`].
struct EnvdLifecycleMetrics {
    /// `node.envd.lifecycle_total` — quiesce/rearm calls by endpoint and outcome.
    lifecycle: Counter<u64>,
}

impl EnvdLifecycleMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            lifecycle: meter
                .u64_counter("node.envd.lifecycle_total")
                .with_description(
                    "Best-effort envd quiesce/rearm lifecycle calls, by endpoint and outcome.",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::envd_lifecycle_metrics`]).
fn metrics() -> &'static EnvdLifecycleMetrics {
    static METRICS: OnceLock<EnvdLifecycleMetrics> = OnceLock::new();
    METRICS.get_or_init(EnvdLifecycleMetrics::new)
}

/// Record one envd lifecycle `call`, labelled by its `endpoint` and the
/// `success`/`failure` derived from `result`.
///
/// Ticks on both outcomes so a failure-*rate* alert has its denominator in the
/// same series. Generic over the `Ok`/`Err` payloads so the caller can pass its
/// `Result<(), _>` directly without unwrapping it first.
pub fn record_lifecycle<T, E>(call: EnvdLifecycleCall, result: &Result<T, E>) {
    let outcome = if result.is_ok() { "success" } else { "failure" };
    metrics().lifecycle.add(
        1,
        &[
            KeyValue::new("endpoint", call.endpoint()),
            KeyValue::new("outcome", outcome),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_label_covers_every_call() {
        assert_eq!(EnvdLifecycleCall::Quiesce.endpoint(), "/__quiesce");
        assert_eq!(EnvdLifecycleCall::Rearm.endpoint(), "/__rearm");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; both calls and both outcomes must still be safe to record on
        // the pause/resume paths.
        let ok: Result<(), &str> = Ok(());
        let err: Result<(), &str> = Err("boom");
        record_lifecycle(EnvdLifecycleCall::Quiesce, &ok);
        record_lifecycle(EnvdLifecycleCall::Quiesce, &err);
        record_lifecycle(EnvdLifecycleCall::Rearm, &ok);
        record_lifecycle(EnvdLifecycleCall::Rearm, &err);
    }
}
