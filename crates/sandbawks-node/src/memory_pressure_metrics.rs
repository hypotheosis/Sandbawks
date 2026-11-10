//! OTel metrics for the host-side memory-pressure watcher (#307).
//!
//! Three instruments, under the meter `sandbawks-node.memory_pressure`:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `memory_pressure.node_max_fraction` | `Gauge<f64>` | 1 | node |
//! | `memory_pressure.events_total` | `Counter<u64>` | --- | node, trigger |
//! | `memory_pressure.captures_total` | `Counter<u64>` | --- | node, result |
//!
//! Cardinality is deliberately bounded to `node` (× a small enum): the
//! per-sandbox detail lives in the structured `tracing::warn!` and the S3
//! artifact key, not in a metric label, so a churning sandbox population can't
//! blow up the series count. `node_max_fraction` is the highest
//! `memory.current / memory.max` seen across the node's sandboxes on the last
//! poll — a single gauge an alert can threshold. `events_total` is
//! **edge-triggered** — one increment when a sandbox *enters* a pressure
//! episode, not per poll — so it counts distinct episodes; `trigger` is
//! `high_water` | `events_max` | `oom_kill` (why it fired). `result` on captures
//! is `ok` | `no_pod_ip` | `fetch_error` | `upload_error`.
//!
//! Instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` the global
//! meter is a no-op, so these calls stay on the path unconditionally.

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.memory_pressure";

/// Cached memory-pressure instruments, built once via [`metrics`].
struct MemoryPressureMetrics {
    /// `memory_pressure.node_max_fraction` — highest sandbox usage ratio on the
    /// node at the last poll.
    node_max_fraction: Gauge<f64>,
    /// `memory_pressure.events_total` — times a sandbox crossed the high-water
    /// line (one per pressured sandbox per poll).
    events: Counter<u64>,
    /// `memory_pressure.captures_total` — envd-state capture attempts by result.
    captures: Counter<u64>,
}

impl MemoryPressureMetrics {
    /// Build every instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            node_max_fraction: meter
                .f64_gauge("memory_pressure.node_max_fraction")
                .with_unit("1")
                .with_description(
                    "Highest sandbox memory.current/memory.max on the node at the last poll.",
                )
                .build(),
            events: meter
                .u64_counter("memory_pressure.events_total")
                .with_description(
                    "Distinct sandbox pressure episodes (rising edge), by node and trigger.",
                )
                .build(),
            captures: meter
                .u64_counter("memory_pressure.captures_total")
                .with_description("envd-state capture attempts on pressure, by node and result.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton.
fn metrics() -> &'static MemoryPressureMetrics {
    static METRICS: OnceLock<MemoryPressureMetrics> = OnceLock::new();
    METRICS.get_or_init(MemoryPressureMetrics::new)
}

/// Record the highest sandbox usage fraction observed on this node this poll.
///
/// Non-finite values (NaN/inf) are dropped rather than exported — the caller's
/// `usage_fraction` can't produce them (it checks `max > 0`), but this is a
/// public entry point and OTel would faithfully export a NaN.
pub fn record_node_max_fraction(node: &str, fraction: f64) {
    if !fraction.is_finite() {
        return;
    }
    metrics()
        .node_max_fraction
        .record(fraction, &[KeyValue::new("node", node.to_string())]);
}

/// Record that a sandbox on this node entered a pressure episode (rising edge),
/// labelled by the `trigger` that fired (`high_water` | `events_max` |
/// `oom_kill`).
pub fn record_pressure(node: &str, trigger: &'static str) {
    metrics().events.add(
        1,
        &[
            KeyValue::new("node", node.to_string()),
            KeyValue::new("trigger", trigger),
        ],
    );
}

/// Record a capture attempt's outcome (`ok` | `no_pod_ip` | `fetch_error` |
/// `upload_error`).
pub fn record_capture(node: &str, result: &'static str) {
    metrics().captures.add(
        1,
        &[
            KeyValue::new("node", node.to_string()),
            KeyValue::new("result", result),
        ],
    );
}
