//! Trace-correlated OTLP log bridge.
//!
//! [`TraceContextLogBridge`] wraps the upstream
//! [`OpenTelemetryTracingBridge`](opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge)
//! so that OTLP log records emitted from inside an instrumented span carry that
//! span's `trace_id` / `span_id`. Without it, the APM's "logs for this trace"
//! view stays empty (`OBSERVABILITY.md` §7).
//!
//! # Why this is needed
//!
//! The upstream bridge forwards every `tracing::Event` to the OTel
//! `LoggerProvider` correctly, but the SDK stamps each record's trace context
//! from `opentelemetry::Context::current()` at emit time
//! (`SdkLogger::emit`). The catch: `tracing-opentelemetry` deliberately keeps
//! its span data in the `tracing` span's extensions and **never** publishes it
//! as the current OTel context. So the upstream bridge, reading an empty
//! current context, ships log records with no trace IDs even though the very
//! same events look correlated in stderr (the `fmt` layer reads the `tracing`
//! span tree, not the OTel context).
//!
//! # The fix
//!
//! For each event this layer looks up the active `tracing` span, recovers its
//! OTel [`Context`](opentelemetry::Context) via
//! [`get_otel_context`](tracing_opentelemetry::get_otel_context), and
//! [`attach`](opentelemetry::Context::attach)es it as the current OTel context
//! **only for the duration of the synchronous inner `on_event` call**. The
//! returned guard drops before the function returns, so the attach never
//! crosses an `.await` boundary — the standard, safe escape hatch for
//! thread-local OTel context inside an async runtime. The wrapped upstream
//! bridge then reads the now-populated current context and stamps the record.
//!
//! # Requirement: a globally installed subscriber
//!
//! Recovering the span's OTel context needs the `tracing` [`Dispatch`] backing
//! the active subscriber, fetched here with
//! [`get_default`](tracing::dispatcher::get_default). That call only reaches a
//! real dispatcher on its *global* fast path; a thread-scoped subscriber
//! (`with_default`) is, by design, unreachable from inside event dispatch (the
//! dispatcher's reentrancy guard returns a no-op). `init_telemetry` always
//! installs the subscriber as the global default (`try_init`), so this holds in
//! every binary. Under a scoped subscriber the lookup yields `None` and records
//! ship uncorrelated rather than wrong — a safe degradation.

use opentelemetry::logs::{Logger, LoggerProvider};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context as OtelContext;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use tracing::{Event, Subscriber};
use tracing_opentelemetry::get_otel_context;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// A `tracing` → OTLP log-record bridge that stamps each record with the active
/// span's `trace_id` / `span_id`.
///
/// Construct it from the same `LoggerProvider` the upstream bridge would take
/// and install it as a subscriber layer in place of
/// `OpenTelemetryTracingBridge`. See the [module docs](self) for the mechanism.
pub struct TraceContextLogBridge<P, L>
where
    P: LoggerProvider<Logger = L> + Send + Sync,
    L: Logger + Send + Sync,
{
    /// The upstream bridge this layer delegates the actual record emission to,
    /// once the active span's OTel context has been attached.
    inner: OpenTelemetryTracingBridge<P, L>,
}

impl<P, L> TraceContextLogBridge<P, L>
where
    P: LoggerProvider<Logger = L> + Send + Sync,
    L: Logger + Send + Sync,
{
    /// Wrap a fresh [`OpenTelemetryTracingBridge`] over `provider`.
    ///
    /// `provider` is the OTel `LoggerProvider` whose `Logger` emits the OTLP log
    /// records; borrowing it (rather than taking ownership) mirrors the upstream
    /// `OpenTelemetryTracingBridge::new` signature.
    pub fn new(provider: &P) -> Self {
        Self {
            inner: OpenTelemetryTracingBridge::new(provider),
        }
    }
}

impl<P, L> std::fmt::Debug for TraceContextLogBridge<P, L>
where
    P: LoggerProvider<Logger = L> + Send + Sync,
    L: Logger + Send + Sync,
{
    /// The wrapped `OpenTelemetryTracingBridge` holds a non-`Debug` `Logger`, so
    /// this prints only the type name — enough for `#[derive(Debug)]` parity with
    /// the rest of the crate's public types without bounding `L: Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceContextLogBridge")
            .finish_non_exhaustive()
    }
}

impl<S, P, L> Layer<S> for TraceContextLogBridge<P, L>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    P: LoggerProvider<Logger = L> + Send + Sync + 'static,
    L: Logger + Send + Sync + 'static,
{
    /// Attach the active span's OTel context, then hand the event to the
    /// upstream bridge.
    ///
    /// `_attach_guard` is bound to a named local (never `let _ = …`, which would
    /// drop it *immediately*) so the attach stays live across the synchronous
    /// `inner.on_event` call and detaches the instant this function returns.
    ///
    /// Only `on_event` is forwarded — deliberately. The wrapped
    /// `OpenTelemetryTracingBridge` (upstream 0.32) itself implements *only*
    /// `on_event`; it performs its enablement check (`logger.event_enabled`)
    /// *inside* `on_event` rather than via `Layer::event_enabled`, precisely so
    /// it suppresses only its own emission. Implementing `event_enabled` here
    /// would let a disabled OTel logger short-circuit the event for *every*
    /// sibling layer (the `fmt`/stderr layer included), since `Layered`
    /// `&&`-combines `event_enabled` across layers. Forwarding just `on_event`
    /// is therefore an exact behavioral match to the bare upstream bridge.
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let _attach_guard = active_span_otel_context(event, &ctx).map(OtelContext::attach);
        self.inner.on_event(event, ctx);
    }
}

/// Recover the OTel [`Context`](OtelContext) of the span the event was emitted
/// in, but only when it actually carries an active span.
///
/// Returns `None` when the event is outside any span, when the span has no
/// recoverable OTel context (e.g. the subscriber is only thread-scoped — see the
/// [module docs](self)), or when the recovered context carries no active span at
/// all (nothing to correlate to).
///
/// [`has_active_span`](opentelemetry::trace::TraceContextExt::has_active_span)
/// tests for a *populated* span slot, not its sampling decision — an unsampled
/// span still carries valid trace/span IDs and is correlated like any other; the
/// SDK's own `SdkLogger::emit` gates record stamping on the same predicate, so
/// this mirrors exactly what gets stamped downstream.
fn active_span_otel_context<S>(event: &Event<'_>, ctx: &Context<'_, S>) -> Option<OtelContext>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let span_id = ctx.event_span(event)?.id();
    // `get_default` reaches the active dispatcher only on its global fast path;
    // `init_telemetry` installs the subscriber globally, so that holds in
    // production. `get_otel_context` builds the span's OTel data if needed and
    // briefly locks its extensions — safe here since `on_event` holds none.
    let otel_cx =
        tracing::dispatcher::get_default(|dispatch| get_otel_context(&span_id, dispatch))?;
    otel_cx.has_active_span().then_some(otel_cx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{TraceContextExt, TracerProvider};
    use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use serial_test::serial;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    /// An event emitted inside an instrumented span produces an OTLP log record
    /// whose `trace_id` / `span_id` match that span's — the whole point of the
    /// bridge (issue #297 / OBSERVABILITY.md §7.2).
    ///
    /// `#[serial]` because this installs the process-global default subscriber;
    /// serializing keeps it from racing any future test that installs one.
    #[test]
    #[serial]
    fn event_in_span_is_stamped_with_span_trace_ids() {
        // Arrange: an in-memory logger (a *simple* processor emits synchronously
        // and needs no async runtime) and an always-on tracer so the span gets a
        // valid, sampled OTel context worth correlating against.
        let exporter = InMemoryLogExporter::default();
        let logger_provider = SdkLoggerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();

        // `LevelFilter::INFO` pins enablement explicitly so the test is hermetic
        // — it doesn't depend on the registry's implicit default and can't be
        // silenced by an ambient `RUST_LOG` or global max-level in CI.
        let subscriber = tracing_subscriber::registry()
            .with(LevelFilter::INFO)
            .with(tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("test")))
            .with(TraceContextLogBridge::new(&logger_provider));

        // The bridge recovers the span context via the *global* dispatcher (see
        // the module docs), so the subscriber must be installed globally — a
        // thread-scoped `with_default` would be unreachable from inside event
        // dispatch. A global default cannot be unset, so this is necessarily
        // one-shot per process: only one test in the crate may install one.
        // `try_init` (not `init`) surfaces a pre-existing global as an error
        // rather than panicking, and `#[serial]` keeps this the sole installer
        // for the run — but if a future test also installs a global subscriber,
        // ordering decides which wins, so keep this the only one.
        subscriber
            .try_init()
            .expect("install global subscriber for the correlation test");

        // Act: capture the span's own OTel IDs (which also forces the span to be
        // built), then emit an event from inside it.
        let span = tracing::info_span!("handler");
        let span_context = {
            let _entered = span.enter();
            let span_context = span.context().span().span_context().clone();
            tracing::info!("inside the span");
            span_context
        };

        // The simple processor exports on emit; flush to be explicit regardless.
        logger_provider
            .force_flush()
            .expect("flush logger provider");

        // Assert: an emitted record carries our span's `trace_id`, and its
        // `span_id` matches too — i.e. the log is correlated to the span.
        let logs = exporter.get_emitted_logs().expect("read emitted logs");
        let trace_context = logs
            .iter()
            .filter_map(|log| log.record.trace_context())
            .find(|tc| tc.trace_id == span_context.trace_id())
            .expect("an emitted log record correlated to the span");

        assert_eq!(trace_context.trace_id, span_context.trace_id());
        assert_eq!(trace_context.span_id, span_context.span_id());
    }
}
