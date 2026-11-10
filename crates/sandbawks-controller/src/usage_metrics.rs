//! OTel metrics for authoritative usage-event emission (epic #480, issue #636).
//!
//! One instrument, under the meter `sandbawks-controller.usage`, recorded once per
//! attempt to persist a closed-`Running`-interval [`crate::usage::UsageEvent`] from
//! [`crate::reconcile`]:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `controller.usage.events_total` | `Counter<u64>` | --- | outcome |
//!
//! ## Why this exists
//!
//! Usage emission is **fail-open** — a sink error never wedges the pause/terminate
//! transition (#631). Issue #636 adds a bounded retry before giving up, but a
//! *sustained* object-store outage still drops the intervals that close during it.
//! Without a counter that drop is silent, which is hostile for an authoritative
//! billing signal. The `dropped` outcome makes it observable and alertable
//! (`rate(controller.usage.events_total{outcome="dropped"}) > 0` ⇒ the ledger is
//! genuinely losing intervals); `emitted` is the healthy baseline; `spilled` /
//! `replayed` track the durable spill-and-replay recovery path (#636) — a steady
//! `spilled` with matching `replayed` means the outage is being ridden out, not
//! lost.
//!
//! The `outcome` label is a small closed vocabulary, so the series stays bounded
//! regardless of how many sandboxes close — no `team`/`sandbox` cardinality here
//! (this is an emission-health signal, not the per-team usage itself, which lives
//! in the durable ledger).
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in unit tests) the global meter is a no-op, so the call
//! stays on the emission path unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.usage";

/// Terminal outcome of a usage-event emission, carrying its stable `outcome` label.
///
/// Modelled as an enum so the label set stays closed and exhaustively checked, the
/// same idiom as `warm_claim_metrics::ClaimOutcome` (in `sandbawks-api`) — plain
/// backticks, not an intra-doc link, since that module is in a different crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageEmitOutcome {
    /// The event was persisted to the durable sink on the live path (possibly after
    /// a retry).
    Emitted,
    /// Live emission failed but the event was parked on the durable spill queue for
    /// later replay (#636) — recoverable, not lost.
    Spilled,
    /// A previously-spilled event was successfully replayed to the sink by the
    /// drainer (#636) — the recovery counterpart of [`Self::Spilled`].
    Replayed,
    /// Every live attempt failed AND the event could not be spilled (no spill queue
    /// configured, or the spill write itself failed); the interval is lost
    /// (fail-open). Alertable.
    Dropped,
}

impl UsageEmitOutcome {
    /// Stable, lowercase `outcome` label.
    fn label(self) -> &'static str {
        match self {
            Self::Emitted => "emitted",
            Self::Spilled => "spilled",
            Self::Replayed => "replayed",
            Self::Dropped => "dropped",
        }
    }
}

/// Cached usage-emission instruments, built once via [`metrics`].
struct UsageMetrics {
    /// `controller.usage.events_total` — emission outcomes (see [`UsageEmitOutcome`]).
    events: Counter<u64>,
}

impl UsageMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            events: meter
                .u64_counter("controller.usage.events_total")
                .with_description(
                    "Authoritative usage-event emissions by outcome (emitted vs dropped).",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::usage_metrics`]).
fn metrics() -> &'static UsageMetrics {
    static METRICS: OnceLock<UsageMetrics> = OnceLock::new();
    METRICS.get_or_init(UsageMetrics::new)
}

/// Record one terminal usage-emission `outcome`. Called once per closed interval
/// (after the bounded retry resolves), so `emitted + dropped` partitions every
/// billable interval the reconcile loop tried to persist.
pub fn record_usage_emit(outcome: UsageEmitOutcome) {
    metrics()
        .events
        .add(1, &[KeyValue::new("outcome", outcome.label())]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_label_covers_every_variant() {
        assert_eq!(UsageEmitOutcome::Emitted.label(), "emitted");
        assert_eq!(UsageEmitOutcome::Spilled.label(), "spilled");
        assert_eq!(UsageEmitOutcome::Replayed.label(), "replayed");
        assert_eq!(UsageEmitOutcome::Dropped.label(), "dropped");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a no-op;
        // the recorder must still be safe to invoke on the emission path.
        record_usage_emit(UsageEmitOutcome::Emitted);
        record_usage_emit(UsageEmitOutcome::Spilled);
        record_usage_emit(UsageEmitOutcome::Replayed);
        record_usage_emit(UsageEmitOutcome::Dropped);
    }
}
