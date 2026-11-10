//! OTel metrics for the per-sandbox resource-pressure watcher (epic #472,
//! phase 3 / issue #475).
//!
//! Continuous per-sandbox gauges plus an episode counter, under the meter
//! `sandbawks-node.sandbox_pressure`:
//!
//! | Instrument | Type | Attributes |
//! |---|---|---|
//! | `sandbox_pressure.cpu.fraction` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.cpu.psi` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.memory.fraction` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.memory.psi` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.io.psi` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.disk.fill_fraction` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.disk.fill_bytes` | `Gauge<u64>` | sandbox, namespace, node |
//! | `sandbox_pressure.net.rx_bytes_per_sec` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.net.tx_bytes_per_sec` | `Gauge<f64>` | sandbox, namespace, node |
//! | `sandbox_pressure.episodes_total` | `Counter<u64>` | node, resource |
//!
//! The gauges give the pre-failure baseline the logs can't (history *before* a
//! pod dies), recorded every check tick while the watcher runs.
//!
//! ## Cardinality (read before enabling on a high-churn node)
//!
//! Every gauge carries a `sandbox` attribute. The shared SDK meter provider
//! (`sandbawks-observability::init`) exports with the SDK-default **cumulative**
//! temporality and no cardinality cap, so each distinct `sandbox` value keeps a
//! retained time series for the meter provider's (i.e. process's) lifetime — a
//! sandbox that has gone away is **not** evicted just because newer readings
//! overwrite its current value. A node that churns through many short-lived
//! sandboxes therefore accumulates series until the process restarts (or until
//! the SDK's default ~2000-per-instrument cardinality limit silently starts
//! dropping points; it never grows without bound or OOMs).
//!
//! This is an accepted, bounded cost: the watcher is **off by default** and is
//! meant to be switched on only for the span of an investigation and back off
//! after — disabling it stops the recording, and a DaemonSet roll resets the
//! series. Per-sandbox attribution is the whole point of the feature, so
//! dropping the `sandbox` label (the way the sibling `memory_pressure_metrics`
//! keys its gauge on `node` only) is not an option here. If these gauges are
//! ever wanted enabled long-term on a high-churn node, add a delta-temporality
//! or explicit cardinality-limited `View` for this meter in the shared provider.
//!
//! The `episodes_total` counter deliberately omits `sandbox`: a counter never
//! resets, so a per-sandbox label would grow without bound — it carries only
//! `node` + `resource` (the signal that opened the episode), mirroring the
//! memory-pressure `events_total{node, trigger}` design.
//!
//! Instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` the global
//! meter is a no-op, so these calls stay on the path unconditionally.

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::{global, KeyValue};

use crate::sandbox_pressure::PressureSample;

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.sandbox_pressure";

/// Cached sandbox-pressure instruments, built once via [`metrics`].
struct SandboxPressureMetrics {
    cpu_fraction: Gauge<f64>,
    cpu_psi: Gauge<f64>,
    memory_fraction: Gauge<f64>,
    memory_psi: Gauge<f64>,
    io_psi: Gauge<f64>,
    disk_fill_fraction: Gauge<f64>,
    disk_fill_bytes: Gauge<u64>,
    net_rx_bps: Gauge<f64>,
    net_tx_bps: Gauge<f64>,
    episodes: Counter<u64>,
}

impl SandboxPressureMetrics {
    /// Build every instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            cpu_fraction: meter
                .f64_gauge("sandbox_pressure.cpu.fraction")
                .with_unit("1")
                .with_description("Sandbox CPU utilization as a fraction of its CPU quota.")
                .build(),
            cpu_psi: meter
                .f64_gauge("sandbox_pressure.cpu.psi")
                .with_unit("%")
                .with_description("Sandbox cpu.pressure some avg10 (percent stalled).")
                .build(),
            memory_fraction: meter
                .f64_gauge("sandbox_pressure.memory.fraction")
                .with_unit("1")
                .with_description("Sandbox memory.current / memory.max.")
                .build(),
            memory_psi: meter
                .f64_gauge("sandbox_pressure.memory.psi")
                .with_unit("%")
                .with_description("Sandbox memory.pressure some avg10 (percent stalled).")
                .build(),
            io_psi: meter
                .f64_gauge("sandbox_pressure.io.psi")
                .with_unit("%")
                .with_description("Sandbox io.pressure some avg10 (percent stalled).")
                .build(),
            disk_fill_fraction: meter
                .f64_gauge("sandbox_pressure.disk.fill_fraction")
                .with_unit("1")
                .with_description("Sandbox disk fill estimate / ephemeral-storage limit.")
                .build(),
            disk_fill_bytes: meter
                .u64_gauge("sandbox_pressure.disk.fill_bytes")
                .with_unit("By")
                .with_description("Sandbox writable-layer fill estimate, bytes.")
                .build(),
            net_rx_bps: meter
                .f64_gauge("sandbox_pressure.net.rx_bytes_per_sec")
                .with_unit("By/s")
                .with_description("Sandbox pod-netns receive rate, bytes/sec.")
                .build(),
            net_tx_bps: meter
                .f64_gauge("sandbox_pressure.net.tx_bytes_per_sec")
                .with_unit("By/s")
                .with_description("Sandbox pod-netns transmit rate, bytes/sec.")
                .build(),
            episodes: meter
                .u64_counter("sandbox_pressure.episodes_total")
                .with_description("Distinct sandbox pressure episodes (rising edge), by resource.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton.
fn metrics() -> &'static SandboxPressureMetrics {
    static METRICS: OnceLock<SandboxPressureMetrics> = OnceLock::new();
    METRICS.get_or_init(SandboxPressureMetrics::new)
}

/// Build the bounded gauge attribute set shared by every per-sandbox gauge.
fn sample_attrs(node: &str, namespace: &str, sandbox: &str) -> [KeyValue; 3] {
    [
        KeyValue::new("sandbox", sandbox.to_string()),
        KeyValue::new("namespace", namespace.to_string()),
        KeyValue::new("node", node.to_string()),
    ]
}

/// Pass a finite `f64` through, dropping NaN/inf so OTel never exports a
/// non-finite point (the watcher's math shouldn't produce them, but these are
/// public entry points and OTel would faithfully export whatever it's given).
fn finite(v: f64) -> Option<f64> {
    v.is_finite().then_some(v)
}

/// Record one tick's gauges for a single sandbox. Each signal is recorded only
/// when present (and finite); `fill_bytes` is always available once the cgroup
/// io stats are read, so it is recorded unconditionally.
pub fn record_sample(
    node: &str,
    namespace: &str,
    sandbox: &str,
    sample: &PressureSample,
    fill_bytes: u64,
) {
    let m = metrics();
    let attrs = sample_attrs(node, namespace, sandbox);
    for (gauge, value) in [
        (&m.cpu_fraction, sample.cpu_fraction),
        (&m.cpu_psi, sample.cpu_psi),
        (&m.memory_fraction, sample.mem_fraction),
        (&m.memory_psi, sample.mem_psi),
        (&m.io_psi, sample.io_psi),
        (&m.disk_fill_fraction, sample.disk_fraction),
        (&m.net_rx_bps, sample.net_rx_bps),
        (&m.net_tx_bps, sample.net_tx_bps),
    ] {
        if let Some(v) = value.and_then(finite) {
            gauge.record(v, &attrs);
        }
    }
    // `disk.fill_bytes` is the one signal always available once cgroup io.stat is
    // read: the estimate exists even when the pod has no ephemeral limit (where
    // `disk.fill_fraction` is absent — the design reports absolute bytes then).
    // So it emits unconditionally and is, deliberately, the highest-cardinality
    // (always-present-per-sandbox) series in the set — see the module-level
    // cardinality note.
    m.disk_fill_bytes.record(fill_bytes, &attrs);
}

/// Record that a sandbox on this node entered a pressure episode (rising edge),
/// attributed to the `resource` signal that opened it. Called once per
/// triggering signal on the opening tick.
pub fn record_episode(node: &str, resource: &'static str) {
    metrics().episodes.add(
        1,
        &[
            KeyValue::new("node", node.to_string()),
            KeyValue::new("resource", resource),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attrs_have_bounded_keys_in_order() {
        let a = sample_attrs("node-1", "sandboxes", "sb-abc");
        let keys: Vec<&str> = a.iter().map(|kv| kv.key.as_str()).collect();
        assert_eq!(keys, ["sandbox", "namespace", "node"]);
        assert_eq!(a[0].value.as_str(), "sb-abc");
        assert_eq!(a[2].value.as_str(), "node-1");
    }

    #[test]
    fn finite_filters_non_finite() {
        assert_eq!(finite(0.85), Some(0.85));
        assert_eq!(finite(0.0), Some(0.0));
        assert_eq!(finite(f64::NAN), None);
        assert_eq!(finite(f64::INFINITY), None);
        assert_eq!(finite(f64::NEG_INFINITY), None);
    }

    #[test]
    fn record_paths_are_callable_under_noop_meter() {
        // With no SDK provider installed the global meter is a no-op; the record
        // entry points must still be safe to call (they run every tick).
        let sample = PressureSample {
            cpu_fraction: Some(0.9),
            mem_fraction: Some(f64::NAN), // dropped by `finite`
            ..Default::default()
        };
        record_sample("node-1", "sandboxes", "sb-abc", &sample, 4096);
        record_episode("node-1", "cpu");
    }
}
