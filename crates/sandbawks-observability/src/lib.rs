//! Shared OpenTelemetry bootstrap for the sandbawks binaries.
//!
//! This crate owns all OpenTelemetry SDK wiring so each binary touches it
//! through a single call:
//!
//! ```no_run
//! # fn main() -> anyhow::Result<()> {
//! let (_telemetry, _rearm) =
//!     sandbawks_observability::init_telemetry("sandbawks-api", env!("CARGO_PKG_VERSION"))?;
//! // ... run the app; the guard flushes all three signal pipelines on drop ...
//! # Ok(())
//! # }
//! ```
//!
//! The second element is a [`RearmHandle`] that hot-swaps the whole provider
//! stack in the live process across a gVisor pause/resume (`quiesce` then
//! `rearm`); binaries that never checkpoint can ignore it.
//!
//! Application code never calls the OpenTelemetry API directly — it instruments
//! with the [`tracing`] family (`tracing::info!`, `#[tracing::instrument]`,
//! span macros), and [`init_telemetry`] installs the bridge that turns those
//! into OpenTelemetry spans, metrics, and log records exported over OTLP.
//!
//! See `OBSERVABILITY.md` (full strategy) and `TRACING.md` (the traces-only
//! distillation) at the repository root for the design rationale.
//!
//! # Signals
//!
//! All three signals — traces, metrics, and logs — share one OTLP endpoint and
//! one resource, so an APM can correlate them. The wire protocol
//! (`http/protobuf` or `grpc`) is a runtime choice via
//! `OTEL_EXPORTER_OTLP_PROTOCOL`.
//!
//! # Configuration
//!
//! Configuration is entirely through the standard `OTEL_*` environment
//! variables (`OTEL_SDK_DISABLED`, `OTEL_EXPORTER_OTLP_ENDPOINT`,
//! `OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_SERVICE_NAME`,
//! `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_TRACES_SAMPLER[_ARG]`); see
//! `OBSERVABILITY.md` §12 for the full reference.

mod export_runtime;
mod guard;
mod init;
mod log_bridge;
pub mod propagation;
mod rearm;

pub use guard::TelemetryGuard;
pub use init::init_telemetry;
pub use log_bridge::TraceContextLogBridge;
pub use propagation::{
    continue_current, continue_from_annotations, continue_span, extract, inject_current,
    inject_into_annotations, Extractor, HttpHeaderExtractor, HttpHeaderInjector, Injector,
    MapCarrier, TRACEPARENT_ANNOTATION, TRACESTATE_ANNOTATION,
};
pub use rearm::RearmHandle;
