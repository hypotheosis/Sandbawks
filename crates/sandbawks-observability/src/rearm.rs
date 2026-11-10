//! Hot-swapping the OpenTelemetry provider stack in a live process.
//!
//! A gVisor checkpoint/restore brings envd back as a *memory* image: the same
//! process, mid-flight, with its OTel SDK frozen and then thawed against a
//! world that has moved on (the collector connection is dead, the background
//! exporter threads are gone). It cannot re-run [`init_telemetry`] — the global
//! `tracing` subscriber and the OTel globals are one-shot per process. So
//! [`RearmHandle`] re-arms telemetry *in place*:
//!
//! 1. [`RearmHandle::quiesce`] swaps the OTel span layer and log bridge to a
//!    no-op, points the global meter at a [`NoopMeterProvider`], and tears the
//!    stale providers down on detached threads bounded by a hard deadline —
//!    never blocking the resume path on a dead collector.
//! 2. [`RearmHandle::rearm`] rebuilds all three providers from the current
//!    environment, re-registers the global meter/tracer, and swaps the live
//!    layers back to the fresh tracer/logger.
//!
//! The handle is cheap to [`Clone`] and `'static`, so the resume orchestration
//! can stash it wherever it needs to (e.g. an HTTP handler, issue #356
//! Phase 2). When `OTEL_SDK_DISABLED` was set at boot every method is a no-op.
//!
//! [`init_telemetry`]: crate::init_telemetry
//! [`NoopMeterProvider`]: opentelemetry::metrics::noop::NoopMeterProvider

use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use opentelemetry::metrics::noop::NoopMeterProvider;
use opentelemetry::trace::noop::NoopTracerProvider;

use crate::guard::{lock_recover, log_shutdown, Providers, SharedProviders};
use crate::init::{build_reload_layers, ReloadHandle};

/// Factory that reconstructs the three providers from the current environment.
///
/// Boxed behind an [`Arc`] so [`RearmHandle`] stays `Clone + Send + Sync +
/// 'static`. Built by
/// [`make_provider_factory`](crate::init::make_provider_factory); each call
/// re-resolves protocol/endpoint/resource/sampler/timeout, so a resumed process
/// honours whatever configuration its restored environment now carries.
pub(crate) type ProviderFactory = Arc<dyn Fn() -> Result<Providers> + Send + Sync + 'static>;

/// A clonable, `'static` handle that hot-swaps the OpenTelemetry provider stack
/// across a pause/resume. See the [module docs](self) for the lifecycle.
#[derive(Clone)]
pub struct RearmHandle {
    /// `Some` for an active SDK; `None` when `OTEL_SDK_DISABLED` made init
    /// fmt-only, in which case every method is a no-op. Shared via [`Arc`] so
    /// all clones drive the *same* live subscriber and provider cell.
    inner: Arc<Option<Active>>,
}

/// The state an active (non-disabled) [`RearmHandle`] needs to swap providers.
struct Active {
    /// Compile-time service name, threaded back into the rebuilt span layer's
    /// tracer on every rearm (it names the instrumentation scope).
    service_name: &'static str,
    /// Live handle into the subscriber's reloadable OTel layers.
    reload: ReloadHandle,
    /// Rebuilds the three providers from the current environment on rearm.
    factory: ProviderFactory,
    /// The live-providers cell shared with the [`TelemetryGuard`](crate::TelemetryGuard):
    /// `quiesce` `take`s the stale set out to shut it down, `rearm` puts the
    /// fresh set back.
    providers: SharedProviders,
}

impl RearmHandle {
    /// Build an active handle. Called only by
    /// [`init_telemetry`](crate::init_telemetry).
    pub(crate) fn new(
        service_name: &'static str,
        reload: ReloadHandle,
        factory: ProviderFactory,
        providers: SharedProviders,
    ) -> Self {
        Self {
            inner: Arc::new(Some(Active {
                service_name,
                reload,
                factory,
                providers,
            })),
        }
    }

    /// Build a no-op handle for the `OTEL_SDK_DISABLED` path.
    ///
    /// Every method on the returned handle is a no-op: [`quiesce`](Self::quiesce)
    /// does nothing and [`rearm`](Self::rearm) returns `Ok(())`. Also useful to
    /// downstream binaries that want a stand-in handle (e.g. wiring tests for the
    /// envd `/__quiesce` / `/__rearm` routes) without installing a live SDK.
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(None),
        }
    }

    /// Whether this handle is the no-op (disabled-SDK) variant.
    pub fn is_disabled(&self) -> bool {
        self.inner.is_none()
    }

    /// Quiesce telemetry: stop emitting and tear the stale providers down
    /// without blocking on a dead collector.
    ///
    /// Steps, in order:
    /// 1. swap both reloadable OTel layers to a no-op (empty payload), so no
    ///    further `tracing` span or event reaches a stale exporter;
    /// 2. point the global tracer and meter at their no-op providers, so any
    ///    cached `opentelemetry::global::tracer(...)` / `...::meter(...)` handle
    ///    (manual instrumentation, propagation helpers) stops recording —
    ///    belt-and-suspenders to the layer swap above (issue #533, Bug 1);
    /// 3. `take` the live providers and shut all three down on detached
    ///    threads, collecting results only until `timeout` elapses — a hung
    ///    exporter is *abandoned*, never joined.
    ///
    /// Step 3 only *bounds* the resume path; it does not by itself guarantee the
    /// SDK's background Tokio export tasks have stopped. That guarantee comes
    /// from the capped `max_export_timeout` the providers are built with (see
    /// `DEFAULT_EXPORT_FLUSH_TIMEOUT` in [`crate::init`]): with the export bound
    /// well under `timeout`, each provider's `shutdown` completes — rather than
    /// being abandoned — so its background task exits before the caller proceeds
    /// to `runsc checkpoint`, instead of being captured mid-retry against a dead
    /// collector (the original #533 starvation).
    ///
    /// Idempotent and panic-free: a second call (or one on a disabled handle)
    /// finds the providers already taken and simply re-asserts the no-op state.
    pub fn quiesce(&self, timeout: Duration) {
        let Some(active) = self.inner.as_ref() else {
            return; // disabled SDK: nothing to quiesce.
        };

        // 1. No-op the span + log layers. An empty payload `Vec` is the no-op
        //    state; `reload` also rebuilds the callsite-interest cache so the
        //    swap takes effect immediately. A poisoned reload lock is the only
        //    failure mode and is not actionable here — swallow it.
        let _ = active.reload.reload(Vec::new());

        // 2. No-op the global tracer + meter. Cheap and infallible.
        opentelemetry::global::set_tracer_provider(NoopTracerProvider::new());
        opentelemetry::global::set_meter_provider(NoopMeterProvider::new());

        // 3. Tear down the stale providers off the resume path. `take` makes
        //    this idempotent and keeps the shared guard from double-shutting.
        if let Some(providers) = lock_recover(&active.providers).take() {
            shutdown_with_deadline(providers, timeout);
        }
    }

    /// Re-arm telemetry: rebuild the provider stack from the current
    /// environment and swap the live OTel layers onto it.
    ///
    /// Rebuilds the tracer/meter/logger providers, re-registers the global
    /// meter and tracer, then swaps the reloadable span + log layers to the
    /// fresh tracer/logger and records the new providers in the shared cell (so
    /// a later guard drop flushes *these*). On a disabled handle this is a
    /// no-op returning `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns an error if the protocol is unsupported, any OTLP exporter fails
    /// to build, or the reload lock is poisoned. On any error the handle is
    /// left **quiesced** (no-op layers) rather than half-armed — a subsequent
    /// `rearm` can retry from a clean state. Never panics.
    pub fn rearm(&self) -> Result<()> {
        let Some(active) = self.inner.as_ref() else {
            return Ok(()); // disabled SDK: re-arm is a no-op.
        };

        // Rebuild from the *current* env (the factory captures only the static
        // service name/version). Any `?` here returns before we touch globals
        // or the cell, leaving the handle in its quiesced state.
        let providers = (active.factory)()?;

        // Wire the fresh tracer/logger into new span + log layers *before*
        // committing globals, so a build failure cannot leave a torn stack.
        let layers = build_reload_layers(&providers.tracer, &providers.logger, active.service_name);

        // Re-register the globals so cached `global::meter(...)` /
        // `global::tracer(...)` lookups resolve to the live stack again.
        opentelemetry::global::set_tracer_provider(providers.tracer.clone());
        opentelemetry::global::set_meter_provider(providers.meter.clone());

        // Swap the live layers; only fails on a poisoned reload lock.
        active
            .reload
            .reload(layers)
            .map_err(|error| anyhow!("reloading telemetry layers: {error}"))?;

        // Record the new providers so the guard flushes them on drop and a
        // later quiesce tears *these* down.
        *lock_recover(&active.providers) = Some(providers);
        Ok(())
    }
}

impl std::fmt::Debug for RearmHandle {
    /// Prints only the disabled/active discriminator — the inner providers and
    /// reload handle are not `Debug`-worth surfacing, and the factory closure
    /// is opaque.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RearmHandle")
            .field("disabled", &self.is_disabled())
            .finish()
    }
}

/// Shut down all three providers on detached threads, abandoning any that have
/// not finished by `timeout`.
///
/// Each provider's `shutdown` can block on network I/O to a collector that may
/// be unreachable after a restore — exactly the case re-arm must survive. So
/// each runs on its own `std::thread` reporting back over a channel, and the
/// caller collects results only until the shared deadline: a thread still
/// running at the deadline is *detached* (it finishes or dies at process exit),
/// never joined. The function therefore returns in at most `timeout`.
fn shutdown_with_deadline(providers: Providers, timeout: Duration) {
    let Providers {
        tracer,
        meter,
        logger,
    } = providers;
    let (tx, rx) = mpsc::channel();

    // Spawn one shutdown per provider. Each owns its provider and sends a
    // labelled result; `let _ =` on the send ignores a disconnected receiver
    // (the caller stopped collecting after the deadline).
    {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(("tracer", tracer.shutdown()));
        });
    }
    {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(("meter", meter.shutdown()));
        });
    }
    std::thread::spawn(move || {
        let _ = tx.send(("logger", logger.shutdown()));
    });

    // Collect up to three results, but never wait past the deadline.
    let deadline = Instant::now() + timeout;
    for _ in 0..3 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok((provider, result)) => log_shutdown(provider, result),
            // Timed out (or all senders gone): abandon the rest.
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::logs::SdkLoggerProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
    use serial_test::serial;

    use crate::init::{build_reload_layers, build_reloadable_subscriber};

    /// Recorder shared between a test and its [`ProviderFactory`]: every rebuild
    /// pushes its fresh in-memory span exporter here, so the test can assert
    /// *which* exporter a span landed in before vs. after a rearm.
    type ExporterLog = Arc<Mutex<Vec<InMemorySpanExporter>>>;

    /// Build a [`ProviderFactory`] that mints a brand-new in-memory span
    /// exporter on every call (recording each in `log`), wrapped in an
    /// always-on tracer so emitted spans are sampled and captured. The meter
    /// and logger providers are bare (no exporter) — the tests assert on spans.
    fn in_memory_factory(log: ExporterLog) -> ProviderFactory {
        Arc::new(move || {
            let exporter = InMemorySpanExporter::default();
            lock_recover(&log).push(exporter.clone());
            let tracer = SdkTracerProvider::builder()
                .with_simple_exporter(exporter)
                .with_sampler(Sampler::AlwaysOn)
                .build();
            Ok(Providers::new(
                tracer,
                SdkMeterProvider::builder().build(),
                SdkLoggerProvider::builder().build(),
            ))
        })
    }

    /// Emit one closed span named `name` through the current (thread-scoped)
    /// subscriber. Closing the span is what makes the simple processor export
    /// it, so the span is fully formed by the time this returns.
    fn emit_span(name: &'static str) {
        let span = tracing::info_span!("scope", marker = name);
        let _entered = span.enter();
    }

    /// How many spans the `idx`-th minted exporter captured.
    fn captured(log: &ExporterLog, idx: usize) -> usize {
        let exporters = lock_recover(log);
        let spans: Vec<SpanData> = exporters[idx]
            .get_finished_spans()
            .expect("read finished spans");
        spans.len()
    }

    /// Build an active [`RearmHandle`] wired to a thread-scoped subscriber so
    /// the test never touches the process-global default (which another test
    /// may already own). Returns the handle, the exporter log, and the scoped
    /// subscriber guard (kept alive by the caller for the test's duration).
    fn scoped_handle(log: ExporterLog) -> (RearmHandle, tracing::subscriber::DefaultGuard) {
        let factory = in_memory_factory(log);
        // Initial providers + layers, mirroring `init_telemetry`'s assembly.
        let initial = (factory)().expect("build initial providers");
        let layers = build_reload_layers(&initial.tracer, &initial.logger, "test-service");
        let (subscriber, reload) = build_reloadable_subscriber(layers);
        let guard = tracing::subscriber::set_default(subscriber);

        let providers: SharedProviders = Arc::new(Mutex::new(Some(initial)));
        let handle = RearmHandle::new("test-service", reload, factory, providers);
        (handle, guard)
    }

    #[test]
    #[serial]
    fn rearm_routes_spans_to_a_fresh_exporter() {
        // Hermetic filters: force the `info` default so an ambient `RUST_LOG`
        // can't silence the info-level spans these assertions depend on.
        std::env::remove_var("RUST_LOG");

        let log: ExporterLog = Arc::new(Mutex::new(Vec::new()));
        let (handle, _scope) = scoped_handle(log.clone());

        // Initial exporter (index 0) captures the first span.
        emit_span("before");
        assert_eq!(
            captured(&log, 0),
            1,
            "first span lands in the initial exporter"
        );

        // Re-arm: a new provider stack (and a new exporter at index 1) is built
        // and swapped into the live subscriber. Overwriting the providers cell
        // drops the stale `SdkTracerProvider`, whose `Drop` shuts its processor
        // down — so the old exporter is torn down here, not merely bypassed.
        handle.rearm().expect("rearm succeeds");
        assert_eq!(lock_recover(&log).len(), 2, "rearm minted a fresh exporter");

        // The next span must land in the *new* exporter. Exactly one span — the
        // post-rearm one — proving the live span layer now points at the fresh
        // tracer rather than the original (which would have left this at 0).
        emit_span("after");
        assert_eq!(
            captured(&log, 1),
            1,
            "the post-rearm span lands in the fresh exporter, proving the layer swap"
        );
    }

    #[test]
    #[serial]
    fn quiesce_then_rearm_restores_export_and_is_idempotent() {
        std::env::remove_var("RUST_LOG");

        let log: ExporterLog = Arc::new(Mutex::new(Vec::new()));
        let (handle, _scope) = scoped_handle(log.clone());

        // Quiesce drops the span layer to a no-op: spans go nowhere.
        handle.quiesce(Duration::from_secs(5));
        emit_span("while-quiesced");
        assert_eq!(captured(&log, 0), 0, "no span is exported while quiesced");

        // Quiesce again — must be a panic-free no-op (providers already taken).
        handle.quiesce(Duration::from_secs(5));

        // Re-arm restores export to a fresh exporter (index 1).
        handle.rearm().expect("rearm after quiesce succeeds");
        emit_span("after-rearm");
        assert_eq!(
            captured(&log, 1),
            1,
            "export resumes into the fresh exporter after rearm"
        );
    }

    /// A span processor whose shutdown blocks forever — stands in for an
    /// exporter wedged on a dead collector after a restore.
    #[derive(Debug)]
    struct HangingProcessor;

    impl opentelemetry_sdk::trace::SpanProcessor for HangingProcessor {
        fn on_start(
            &self,
            _span: &mut opentelemetry_sdk::trace::Span,
            _cx: &opentelemetry::Context,
        ) {
        }
        fn on_end(&self, _span: SpanData) {}
        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }
        /// Park forever: the detached shutdown thread is expected to be
        /// abandoned at the deadline, then reaped at process exit.
        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
    }

    #[test]
    fn shutdown_with_deadline_abandons_a_hung_provider() {
        // One provider hangs in shutdown; the other two complete instantly.
        let providers = Providers::new(
            SdkTracerProvider::builder()
                .with_span_processor(HangingProcessor)
                .build(),
            SdkMeterProvider::builder().build(),
            SdkLoggerProvider::builder().build(),
        );

        let timeout = Duration::from_millis(200);
        let start = Instant::now();
        shutdown_with_deadline(providers, timeout);
        let elapsed = start.elapsed();

        // It must return near the deadline, not block on the parked thread.
        assert!(
            elapsed < Duration::from_secs(2),
            "shutdown_with_deadline blocked for {elapsed:?}; expected to abandon near {timeout:?}"
        );
    }

    /// A span exporter standing in for a collector that accepts the TCP
    /// connection but never replies (the exact #533 failure mode): every
    /// `export` blocks far longer than the processor's `max_export_timeout`, so
    /// the timeout bound — not the collector — is what ends the call. Records on
    /// `Drop` so a test can observe the owning background task terminating.
    #[derive(Debug)]
    struct DeadCollectorExporter {
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for DeadCollectorExporter {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl opentelemetry_sdk::trace::SpanExporter for DeadCollectorExporter {
        /// Park far past any sane `max_export_timeout`; the batch processor's
        /// bound cancels this future, simulating an unreachable collector.
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(())
        }
    }

    /// The core #533 Bug 1 guarantee: with a bounded `max_export_timeout`, a
    /// provider's `shutdown` *completes* against a dead collector instead of
    /// being abandoned — so the SDK's background Tokio export task exits (rather
    /// than surviving `runsc checkpoint` mid-retry). A multi-thread runtime is
    /// required: `shutdown_with_deadline` blocks the calling thread on the
    /// shutdown ack, so the background task must run on a separate worker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_terminates_background_task_with_bounded_export() {
        use opentelemetry::trace::{Tracer, TracerProvider};
        use opentelemetry_sdk::runtime;
        use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
        use opentelemetry_sdk::trace::BatchConfigBuilder;

        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exporter = DeadCollectorExporter {
            dropped: dropped.clone(),
        };

        // Bound exports at 200ms; push the periodic flush out to 60s so the
        // *shutdown* flush is the one that exercises the export bound.
        let batch_config = BatchConfigBuilder::default()
            .with_max_export_timeout(Duration::from_millis(200))
            .with_scheduled_delay(Duration::from_secs(60))
            .build();
        let processor = BatchSpanProcessor::builder(exporter, runtime::Tokio)
            .with_batch_config(batch_config)
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_sampler(Sampler::AlwaysOn)
            .build();

        // Emit one span so shutdown's final flush has a batch to export — that
        // export hangs on the dead collector, then self-cancels at 200ms.
        tracer_provider.tracer("test").in_span("probe", |_| {});

        let providers = Providers::new(
            tracer_provider,
            SdkMeterProvider::builder().build(),
            SdkLoggerProvider::builder().build(),
        );

        // Generous deadline: the 200ms export bound means shutdown completes
        // well under it, so the background task is joined, not abandoned.
        let start = Instant::now();
        shutdown_with_deadline(providers, Duration::from_secs(5));
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "shutdown blocked for {:?}; the export bound should let it complete promptly",
            start.elapsed()
        );

        // The background task drops the exporter it owns once it exits. Poll
        // briefly for the runtime to run that drop.
        let mut terminated = false;
        for _ in 0..100 {
            if dropped.load(std::sync::atomic::Ordering::SeqCst) {
                terminated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            terminated,
            "background export task must terminate (its exporter dropped) so it \
             cannot survive runsc checkpoint mid-retry"
        );
    }

    #[test]
    fn disabled_handle_is_a_noop() {
        let handle = RearmHandle::disabled();
        assert!(handle.is_disabled());
        // Both methods must be safe no-ops on a disabled handle.
        handle.quiesce(Duration::from_millis(1));
        handle.rearm().expect("disabled rearm is Ok");
        // Cloning a disabled handle keeps it disabled.
        assert!(handle.clone().is_disabled());
    }
}
