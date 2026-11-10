//! OTel metrics for the node's privileged runsc fleet upgrader (#550).
//!
//! Instruments under the meter `sandbawks-node.runsc_upgrade`, recorded by
//! [`crate::runsc_upgrade`]:
//!
//! | Instrument | Type | Attributes | When |
//! |---|---|---|---|
//! | `runsc_upgrade.total` | `Counter<u64>` | decision | each tick (the [`crate::runsc_upgrade::UpgradeDecision`]) |
//! | `runsc_upgrade.errors_total` | `Counter<u64>` | step | each failing tick step |
//!
//! `runsc_upgrade_total{decision}` makes the rollout observable: a rising
//! `decision=launched` is a node entering its (one-at-a-time) upgrade, `contended`
//! shows nodes waiting their turn, `live_sandboxes` shows nodes deferring until
//! drained, and a steady stream of `disabled`/`already_converged` is the no-op
//! steady state. `runsc_upgrade_errors_total{step}` makes the privileged failure
//! modes (lease contention I/O, the detached installer launch) alertable without
//! scraping logs.
//!
//! Naming mirrors [`crate::runsc_version_metrics`]: the `sandbawks_node_` prefix
//! is the instrumentation scope, not part of the instrument name, so the
//! collector exports these as `runsc_upgrade_total{decision}` and
//! `runsc_upgrade_errors_total{step}` after the OTLP → Prometheus translation.
//!
//! Instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in tests) the global meter is a no-op, so these calls
//! stay on the tick path unconditionally.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.runsc_upgrade";

/// Cached upgrader instruments, built once via [`metrics`].
struct UpgradeMetrics {
    /// `runsc_upgrade.total` — upgrade decisions per tick, by `decision`.
    total: Counter<u64>,
    /// `runsc_upgrade.errors_total` — failing tick steps, by `step`.
    errors: Counter<u64>,
}

impl UpgradeMetrics {
    /// Build the instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            total: meter
                .u64_counter("runsc_upgrade.total")
                .with_description(
                    "Privileged runsc fleet-upgrade decisions per tick, by decision \
                     (disabled | already_converged | live_sandboxes | contended | \
                     in_progress | launched).",
                )
                .build(),
            errors: meter
                .u64_counter("runsc_upgrade.errors_total")
                .with_description(
                    "Failing runsc fleet-upgrade tick steps, by step (version_read | \
                     count_live | lease_acquire | lease_release | launch).",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton.
fn metrics() -> &'static UpgradeMetrics {
    static METRICS: OnceLock<UpgradeMetrics> = OnceLock::new();
    METRICS.get_or_init(UpgradeMetrics::new)
}

/// Record one upgrade decision, labelled by its stable `decision` string
/// (see [`crate::runsc_upgrade::UpgradeDecision::decision_label`]).
pub fn record_upgrade_decision(decision: &'static str) {
    metrics()
        .total
        .add(1, &[KeyValue::new("decision", decision)]);
}

/// Record a failing tick step, labelled by the `step` that failed.
pub fn record_upgrade_error(step: &'static str) {
    metrics().errors.add(1, &[KeyValue::new("step", step)]);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No meter provider is installed in tests, so the global meter is a no-op;
    /// every recorder must still be safe to invoke on every tick.
    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        record_upgrade_decision("launched");
        record_upgrade_decision("contended");
        record_upgrade_error("launch");
    }
}
