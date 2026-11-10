//! OTel metrics for the node's runsc reconciler (#546).
//!
//! Instruments under the meter `sandbawks-node.runsc_version`, recorded by
//! [`crate::runsc_reconcile`]:
//!
//! | Instrument | Type | Unit | Attributes | When |
//! |---|---|---|---|---|
//! | `runsc.version` | `Gauge<u64>` | --- | node, release | each tick that resolves a host release |
//! | `runsc_version.reads_total` | `Counter<u64>` | --- | result | each report step (`present`/`absent`) |
//! | `runsc_converge.total` | `Counter<u64>` | --- | decision | each tick (the [`crate::runsc_reconcile::ConvergeDecision`]) |
//! | `runsc_reconcile.errors_total` | `Counter<u64>` | --- | step | each failing tick step |
//!
//! `runsc.version` is the classic Prometheus *info gauge* (always `1`, the
//! release on a label), so `count by (release) (runsc_version)` makes drift
//! alertable. The `reads_total{result}` counter lets operators tell "host has no
//! runsc" (`result=absent` ticking up) from "reconciler is not running" (the
//! series flat) — a distinction the gauge alone can't make, since an absent host
//! binary emits no gauge sample at all. `converge.total{decision}` and
//! `reconcile.errors_total{step}` make the convergence outcomes and failure modes
//! alertable without scraping logs.
//!
//! Naming: this crate's OTel instruments drop the `sandbawks_node_` prefix — it
//! is the instrumentation scope, not part of the instrument name (cf.
//! [`crate::envd_advertised_metrics`]) — so the collector exports these as
//! `runsc_version{node,release}`, `runsc_version_reads_total{result}`,
//! `runsc_converge_total{decision}`, and `runsc_reconcile_errors_total{step}`
//! after the OTLP dotted-name → Prometheus underscore translation.
//!
//! The instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in tests) the global meter is a no-op, so these calls
//! stay on the reconcile path unconditionally.

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.runsc_version";

/// Cached runsc reconciler instruments, built once via [`metrics`].
struct RunscMetrics {
    /// `runsc.version` — info gauge, always `1`, carrying the release on a label.
    version: Gauge<u64>,
    /// `runsc_version.reads_total` — host-version read outcomes (`present`/`absent`).
    reads: Counter<u64>,
    /// `runsc_converge.total` — convergence decisions per tick, by `decision`.
    converge: Counter<u64>,
    /// `runsc_reconcile.errors_total` — failing tick steps, by `step`.
    errors: Counter<u64>,
}

impl RunscMetrics {
    /// Build the instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            version: meter
                .u64_gauge("runsc.version")
                .with_description(
                    "Host runsc (gVisor) release on this node, as an info gauge \
                     (always 1; the release rides on the `release` label).",
                )
                .build(),
            reads: meter
                .u64_counter("runsc_version.reads_total")
                .with_description(
                    "Host runsc version-read outcomes per tick, by result \
                     (present | absent).",
                )
                .build(),
            converge: meter
                .u64_counter("runsc_converge.total")
                .with_description(
                    "runsc convergence decisions per tick, by decision (disabled | \
                     bundled_mismatch | already_converged | live_sandboxes | converge).",
                )
                .build(),
            errors: meter
                .u64_counter("runsc_reconcile.errors_total")
                .with_description(
                    "Failing runsc reconcile-tick steps, by step (version_read | \
                     label_patch | apt_disarm | count_live | install).",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::runsc_version_metrics`]).
fn metrics() -> &'static RunscMetrics {
    static METRICS: OnceLock<RunscMetrics> = OnceLock::new();
    METRICS.get_or_init(RunscMetrics::new)
}

/// Record this node's host runsc release as an info gauge.
///
/// Called each [`crate::runsc_reconcile`] tick that resolves a host release.
/// `node` is the local node name; `release` is the full `release-<ver>` token
/// parsed from `runsc --version`.
pub fn record_runsc_version(node: &str, release: &str) {
    metrics().version.record(
        1,
        &[
            KeyValue::new("node", node.to_owned()),
            KeyValue::new("release", release.to_owned()),
        ],
    );
}

/// Record a host-version read outcome: `present` when a release was resolved,
/// `absent` when the host binary was missing/unparseable. Lets operators
/// distinguish "no host runsc" from "reconciler not running".
pub fn record_version_read(present: bool) {
    let result = if present { "present" } else { "absent" };
    metrics().reads.add(1, &[KeyValue::new("result", result)]);
}

/// Record one convergence decision, labelled by its stable `decision` string
/// (see [`crate::runsc_reconcile::ConvergeDecision::decision_label`]).
pub fn record_converge_decision(decision: &'static str) {
    metrics()
        .converge
        .add(1, &[KeyValue::new("decision", decision)]);
}

/// Record a failing reconcile-tick step, labelled by the `step` that failed.
pub fn record_reconcile_error(step: &'static str) {
    metrics().errors.add(1, &[KeyValue::new("step", step)]);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No meter provider is installed in tests, so the global meter is a no-op;
    /// every recorder must still be safe to invoke on every tick.
    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        record_runsc_version("node-01", "release-20260525.0");
        record_version_read(true);
        record_version_read(false);
        record_converge_decision("converge");
        record_reconcile_error("install");
    }
}
