//! OTel metrics for the node snapshot pipeline's per-phase timings (issue #373).
//!
//! One instrument, under the meter `sandbawks-node.snapshot`, recorded from the
//! pause / resume / `fetch_verify_and_lay_out` pipelines in
//! [`crate::gvisor`] and labelled by the pipeline `phase`:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `node.snapshot.phase.duration` | `Histogram<f64>` | s | phase |
//!
//! This promotes the existing `PhaseTimer` breakdown — already emitted once per
//! successful pipeline as a single structured `phases="checkpoint=4203 …"` log
//! line — into a histogram so phase latencies aggregate across pauses/resumes
//! without grep+awk over `kubectl logs`. `phase` is the existing `mark` name
//! (`resolve_pod`, `checkpoint`, `compress`, `upload_memory`, `restore`, …): a
//! small, code-fixed set, so the label stays low-cardinality. The `PhaseTimer`
//! records each mark in milliseconds; this module divides by 1000 to record
//! seconds, matching the project's other duration histograms (`with_unit("s")`).
//!
//! Like the log line it mirrors, this is recorded only on a fully-successful
//! pipeline (an early `?` return skips the final emit): the perf breakdown is
//! about complete runs, and a partial set of marks would skew the per-phase
//! distributions.
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` set (or
//! no installed provider, as in unit tests) the global meter is a no-op, so these
//! calls stay on the snapshot path unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.snapshot";

/// Milliseconds-per-second divisor, applied to convert the `PhaseTimer`'s `u64`
/// millisecond marks into the seconds the histogram records.
const MILLIS_PER_SEC: f64 = 1000.0;

/// Cached snapshot-phase instrument, built once via [`metrics`].
struct SnapshotPhaseMetrics {
    /// `node.snapshot.phase.duration` — per-phase wall-clock (s).
    duration: Histogram<f64>,
}

impl SnapshotPhaseMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            duration: meter
                .f64_histogram("node.snapshot.phase.duration")
                .with_unit("s")
                .with_description("Duration of a node snapshot-pipeline phase, by phase.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::snapshot_phase_metrics`]).
fn metrics() -> &'static SnapshotPhaseMetrics {
    static METRICS: OnceLock<SnapshotPhaseMetrics> = OnceLock::new();
    METRICS.get_or_init(SnapshotPhaseMetrics::new)
}

/// Record every `(phase, elapsed_ms)` pair from one completed pipeline into the
/// `node.snapshot.phase.duration` histogram.
///
/// Takes the `PhaseTimer`'s `&[(phase, ms)]` slice directly so the timer stays
/// the single source of the phase breakdown — the same data the structured log
/// line renders. Each millisecond mark is widened to `f64` and divided by 1000
/// to record seconds.
pub fn record_phases(phases: &[(&'static str, u64)]) {
    let metrics = metrics();
    for (phase, elapsed_ms) in phases {
        metrics.duration.record(
            *elapsed_ms as f64 / MILLIS_PER_SEC,
            &[KeyValue::new("phase", *phase)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; recording a phase breakdown (and the empty-slice edge case)
        // must still be safe on the snapshot path.
        record_phases(&[
            ("resolve_pod", 12),
            ("checkpoint", 4203),
            ("compress", 1100),
        ]);
        record_phases(&[]);
    }
}
