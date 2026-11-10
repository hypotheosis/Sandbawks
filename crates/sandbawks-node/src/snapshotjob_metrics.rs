//! OTel metrics for the node's `SnapshotJob` execution path.
//!
//! Four instruments, under the meter `sandbawks-node.snapshotjob`:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `snapshotjob.duration` | `Histogram<f64>` | s | mode, status |
//! | `snapshotjob.executions_total` | `Counter<u64>` | --- | mode, status |
//! | `snapshotjob.claims_total` | `Counter<u64>` | --- | node, mode |
//! | `snapshotjob.watch_info` | `Gauge<u64>` | --- | node, namespace |
//!
//! `status` is `success` | `error` (the issue's required label); `mode` is
//! `pause` | `resume` | `stage`, a bounded second dimension that distinguishes
//! the three job kinds — in particular the `stage` rows are the envd-staged
//! restore path tracked by #286, and the `status=error` rows over `pause`
//! subsume #277's skipped-digest failures.
//!
//! ## `claims_total` vs `executions_total` (#335)
//!
//! `executions_total` is recorded *after* a dispatch returns, carries its
//! `status`, and is unlabelled by node. `claims_total` is recorded the moment
//! the reconciler *commits* to acting on a job — before the work runs — and is
//! labelled by `node` (not `status`). The two answer different questions:
//!
//! - `executions_total` — "what is the success/failure mix per mode?"
//! - `claims_total` — "is each node draining work at all?"
//!
//! The `node` dimension is the gap #333 exposed: a node silently watching the
//! wrong namespace claimed *zero* jobs for ~11h, but a cross-node completions
//! counter alone could not name the culprit. Recording the claim *before*
//! dispatch (not after) means a node stuck mid-pause still shows it picked up
//! work, so the "node is processing no work" alert keys on genuine idleness, not
//! in-flight latency. See OBSERVABILITY.md §8.3 for the alert expression.
//!
//! ## `watch_info` — the watched namespace as a metric (#335)
//!
//! `snapshotjob.watch_info` is the info-gauge pattern (constant value `1`, real
//! data in the labels, like `kube_pod_info`): each node records it once at
//! controller boot with its `node` name and the `namespace` its `SnapshotJob`
//! watch is scoped to. Before #335 the watched namespace lived only in a
//! boot-time `tracing::info!`, so the #333 mistarget was invisible to metrics.
//! With this gauge `snapshotjob_watch_info{node="..."}` makes the namespace each
//! node is actually watching queryable the instant the claims alert fires.
//!
//! Only the *dispatch* of a job is timed and counted, not reconcile passes that
//! short-circuit (terminal/in-flight idempotency floors) without doing work — so
//! one logical pause/resume/stage produces exactly one point per instrument.
//!
//! Instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` the global
//! meter is a no-op, so these calls stay on the path unconditionally.

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::{global, KeyValue};
use sandbawks_types::SnapshotJobMode;

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.snapshotjob";

/// Cached snapshot-job instruments, built once via [`metrics`].
struct SnapshotJobMetrics {
    /// `snapshotjob.duration` — per-dispatch wall-clock in seconds.
    duration: Histogram<f64>,
    /// `snapshotjob.executions_total` — dispatched-job counter.
    executions: Counter<u64>,
    /// `snapshotjob.claims_total` — per-node "node picked up work" counter (#335).
    claims: Counter<u64>,
    /// `snapshotjob.watch_info` — info gauge naming each node's watched
    /// namespace (#335).
    watch_info: Gauge<u64>,
}

impl SnapshotJobMetrics {
    /// Build every instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            duration: meter
                .f64_histogram("snapshotjob.duration")
                .with_unit("s")
                .with_description("Duration of a node SnapshotJob dispatch.")
                .build(),
            executions: meter
                .u64_counter("snapshotjob.executions_total")
                .with_description("Node SnapshotJob dispatches by mode and status.")
                .build(),
            claims: meter
                .u64_counter("snapshotjob.claims_total")
                .with_description("Node SnapshotJob claims (work picked up) by node and mode.")
                .build(),
            watch_info: meter
                .u64_gauge("snapshotjob.watch_info")
                .with_description(
                    "Constant 1; labels name the namespace each node's watch is scoped to.",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::snapshotjob_metrics`]).
fn metrics() -> &'static SnapshotJobMetrics {
    static METRICS: OnceLock<SnapshotJobMetrics> = OnceLock::new();
    METRICS.get_or_init(SnapshotJobMetrics::new)
}

/// Stable, lowercase `mode` label for a [`SnapshotJobMode`].
fn mode_label(mode: SnapshotJobMode) -> &'static str {
    match mode {
        SnapshotJobMode::Pause => "pause",
        SnapshotJobMode::Resume => "resume",
        SnapshotJobMode::Stage => "stage",
    }
}

/// Record one dispatched `SnapshotJob`: its `duration` and its `success`/`error`
/// `status`, labelled by `mode`.
///
/// `succeeded` is the `is_ok()` of the dispatch result — passing the bool (not
/// the borrowed `Result`) keeps this free of the caller's error type.
pub fn record_execution(mode: SnapshotJobMode, succeeded: bool, duration: Duration) {
    let metrics = metrics();
    let attrs = [
        KeyValue::new("mode", mode_label(mode)),
        KeyValue::new("status", if succeeded { "success" } else { "error" }),
    ];
    metrics.duration.record(duration.as_secs_f64(), &attrs);
    metrics.executions.add(1, &attrs);
}

/// Record that `node` has *claimed* a `SnapshotJob` of `mode` — i.e. the
/// reconciler has committed to dispatching it (#335).
///
/// Call this once, before the dispatch runs, so the counter reflects work
/// picked up rather than work completed: a node stuck mid-pause still shows a
/// claim, keeping the "node is processing no work" alert (OBSERVABILITY.md
/// §8.3) keyed on genuine idleness rather than in-flight latency.
pub fn record_claim(node: &str, mode: SnapshotJobMode) {
    let attrs = [
        KeyValue::new("node", node.to_owned()),
        KeyValue::new("mode", mode_label(mode)),
    ];
    metrics().claims.add(1, &attrs);
}

/// Publish `node`'s install `namespace` as the `snapshotjob.watch_info` info
/// gauge (constant value `1`) (#335).
///
/// Since #582 the `SnapshotJob` watch is cluster-wide (`Api::all`), so this is
/// no longer a *watch scope* — the original #333 failure mode (a node watching
/// the wrong namespace) cannot occur anymore. The gauge is retained as a stable
/// `node → install-namespace` identity signal (which chart/release a given node
/// belongs to), queryable for the lifetime of the process.
///
/// Call this **once per process**, at controller boot. The gauge holds its last
/// value per attribute set, so the mapping stays queryable.
///
/// # Single-call contract
///
/// Because OTel retains a gauge point per *distinct* attribute set, calling this
/// twice for the same `node` with *different* `namespace` values leaves **both**
/// `(node, ns_old)` and `(node, ns_new)` exporting `1` until the process
/// restarts. Today that cannot happen: `run()` calls this once with the
/// immutable boot-time install namespace. There is deliberately no internal
/// guard (a `OnceLock` would silently swallow a legitimate re-emit); the
/// contract is the caller's to keep, hence the single call site in `run()`.
pub fn record_watch_namespace(node: &str, namespace: &str) {
    let attrs = [
        KeyValue::new("node", node.to_owned()),
        KeyValue::new("namespace", namespace.to_owned()),
    ];
    metrics().watch_info.record(1, &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_label_covers_every_variant() {
        assert_eq!(mode_label(SnapshotJobMode::Pause), "pause");
        assert_eq!(mode_label(SnapshotJobMode::Resume), "resume");
        assert_eq!(mode_label(SnapshotJobMode::Stage), "stage");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; every recording function must still be safe to invoke.
        record_execution(SnapshotJobMode::Pause, true, Duration::from_millis(11));
        record_execution(SnapshotJobMode::Resume, false, Duration::from_millis(5));
        record_claim("node-a", SnapshotJobMode::Pause);
        record_claim("node-b", SnapshotJobMode::Stage);
        record_watch_namespace("node-a", "sandbawks-system");
    }
}
