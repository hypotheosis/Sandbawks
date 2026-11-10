//! Local emission smoke for the #335 `SnapshotJob` metrics.
//!
//! Drives the *real* OTel SDK (not the no-op meter the unit tests exercise)
//! through [`snapshotjob_metrics::record_claim`] /
//! [`snapshotjob_metrics::record_watch_namespace`] / [`record_execution`] and
//! flushes on exit, so a local collector's debug exporter prints the new
//! `snapshotjob.claims_total` and `snapshotjob.watch_info` series with their
//! `node` / `mode` / `namespace` attributes — the end-to-end check the
//! workspace `cargo test` can't do against the no-op meter.
//!
//! Run against a local collector listening on the OTLP/HTTP default
//! (`http://localhost:4318`):
//!
//! ```text
//! OTEL_SDK_DISABLED=0 OTEL_SERVICE_NAME=sandbawks-node \
//!   cargo run -p sandbawks-node --example emit_snapshotjob_metrics
//! ```

use std::time::Duration;

use sandbawks_node::snapshotjob_metrics::{record_claim, record_execution, record_watch_namespace};
use sandbawks_types::SnapshotJobMode;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The batch/periodic exporters need the Tokio runtime to exist (hence
    // `#[tokio::main]`); the guard's `Drop` force-flushes every pipeline.
    let (_telemetry, _rearm) =
        sandbawks_observability::init_telemetry("sandbawks-node", env!("CARGO_PKG_VERSION"))?;

    // One claim per mode from a synthetic node, mirroring `reconcile()`'s
    // pre-dispatch `record_claim`, plus the matching completion counter.
    for mode in [
        SnapshotJobMode::Pause,
        SnapshotJobMode::Resume,
        SnapshotJobMode::Stage,
    ] {
        record_claim("emit-smoke-node", mode);
        record_execution(mode, true, Duration::from_millis(42));
    }

    // The boot-time watched-namespace info gauge, mirroring `run()`.
    record_watch_namespace("emit-smoke-node", "sandbawks-system");

    tracing::info!("emitted snapshotjob claim/execution/watch_info points; flushing on exit");

    // Give the periodic metric reader a beat before the guard's flush so the
    // run is robust regardless of the configured export interval.
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(())
}
