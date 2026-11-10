//! A dedicated Tokio runtime that owns *all* OpenTelemetry export work.
//!
//! # Why a second runtime exists
//!
//! envd's serving runtime drives two latency-critical listeners — gRPC on
//! `:49983` and the pause/resume HTTP control plane on `:49984`. The OTLP batch
//! processors and metric reader are normally built with
//! [`opentelemetry_sdk::runtime::Tokio`], whose `spawn` is a bare
//! [`tokio::spawn`] onto *whatever runtime is current when the processor is
//! built* — i.e. the serving runtime. That couples telemetry export to request
//! serving: a blocked, slow, or black-holed collector makes every export task
//! (and the blocking `getaddrinfo` the OTLP HTTP client runs on the shared
//! `spawn_blocking` pool) compete for the serving runtime's worker threads, and
//! a retry-storm against a dead collector can wedge both listeners. This is the
//! coupling behind the repeated post-restore serve hangs (issues #350, #356,
//! #533, #536).
//!
//! [`ExportRuntime`] breaks that coupling structurally. It is a small, separate
//! multi-thread Tokio runtime with its own worker and blocking pools. Building
//! the providers inside its [`Handle::enter`] guard (see
//! [`crate::init::build_providers_on`]) makes every batch-processor/reader
//! background task — and every blocking DNS resolution those tasks trigger —
//! spawn here, never on the serving runtime. A collector that hangs forever can
//! now at most saturate *this* runtime's pools; the serving listeners keep
//! answering. This is the keystone of issue #539: with it, the next
//! egress/collector-outage trigger loses telemetry instead of wedging the
//! sandbox.
//!
//! # Lifecycle
//!
//! Created once by [`crate::init_telemetry`] and shared (via [`Arc`]) between
//! the [`TelemetryGuard`](crate::TelemetryGuard) — which keeps it alive across
//! the guard's flush-on-drop — and the [`ProviderFactory`](crate::rearm) the
//! re-arm path rebuilds providers through, so a resumed process re-spawns its
//! fresh export tasks onto the *same* isolated runtime. The runtime is torn down
//! with [`Runtime::shutdown_background`] in `Drop` rather than the default
//! blocking shutdown: the guard frequently drops *inside* the serving runtime's
//! async context (envd's `runtime.block_on`), where a blocking runtime drop
//! panics. `shutdown_background` returns immediately from any context.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::runtime::{Builder, Handle, Runtime};

/// Name stamped on every thread the export runtime spawns (worker *and*
/// blocking pool). Distinct from the serving runtime's threads so an operator
/// — and the isolation regression test — can attribute export/DNS work to the
/// dedicated runtime in a stack dump or `/proc/<pid>/task` listing.
pub(crate) const EXPORT_THREAD_NAME: &str = "sandbawks-otel-export";

/// Worker-thread count for the export runtime.
///
/// Telemetry export is I/O-bound and low-volume (three batch pipelines on
/// seconds-scale flush intervals), so a single worker comfortably drives it.
/// Keeping the footprint minimal is deliberate: this runtime exists to *contain*
/// export, not to scale it, and a wedged collector should saturate as little as
/// possible.
const EXPORT_WORKER_THREADS: usize = 1;

/// Cap on the export runtime's blocking pool.
///
/// The OTLP HTTP client resolves the collector hostname via the blocking
/// `getaddrinfo` (`tokio::task::spawn_blocking`) — the exact mechanism that
/// starved the *serving* runtime's shared blocking pool in issue #356. Confining
/// it here is half the fix; bounding it is the other half, so a black-holed DNS
/// path cannot spawn an unbounded thread pile even within this runtime. Two
/// threads cover concurrent resolutions across the trace/metric/log exporters
/// without letting a hung resolver balloon.
const EXPORT_MAX_BLOCKING_THREADS: usize = 2;

/// The dedicated export runtime. See the [module docs](self) for the rationale.
pub(crate) struct ExportRuntime {
    /// `Option` only so `Drop` can `take` the runtime and shut it down without
    /// blocking; `Some` for the whole observable lifetime.
    runtime: Option<Runtime>,
    /// Cached clone of the runtime's handle, used to `enter` its context when
    /// building providers. A `Handle` does *not* keep the runtime alive, which
    /// is why the owning `Runtime` is held alongside it.
    handle: Handle,
}

impl ExportRuntime {
    /// Build the dedicated export runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the OS refuses to spawn the runtime's threads.
    pub(crate) fn new() -> Result<Arc<Self>> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(EXPORT_WORKER_THREADS)
            .max_blocking_threads(EXPORT_MAX_BLOCKING_THREADS)
            .thread_name(EXPORT_THREAD_NAME)
            // `enable_all` turns on the I/O driver (sockets to the collector)
            // and the time driver (the batch processors' scheduled-delay timer
            // and `max_export_timeout`).
            .enable_all()
            .build()
            .context("building the dedicated OpenTelemetry export runtime")?;
        let handle = runtime.handle().clone();
        Ok(Arc::new(Self {
            runtime: Some(runtime),
            handle,
        }))
    }

    /// The runtime's handle, for entering its context while building providers.
    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl std::fmt::Debug for ExportRuntime {
    /// Opaque: the inner `Runtime`/`Handle` have no useful `Debug` for an
    /// operator, and this exists only so the owning [`TelemetryGuard`] can keep
    /// its `#[derive(Debug)]`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportRuntime")
            .field("thread_name", &EXPORT_THREAD_NAME)
            .field("live", &self.runtime.is_some())
            .finish()
    }
}

impl Drop for ExportRuntime {
    fn drop(&mut self) {
        // Tear down WITHOUT blocking. A plain `Runtime` drop blocks the current
        // thread until the runtime's tasks stop, which PANICS when the drop
        // happens inside another runtime's async context — exactly where the
        // `TelemetryGuard` drops (envd's `runtime.block_on`, the api/controller
        // `#[tokio::main]`). `shutdown_background` returns immediately and is
        // safe from any context; the providers have already been flushed by the
        // guard's `shutdown()` before this runs, so no export work is lost here.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use opentelemetry::trace::{Tracer, TracerProvider};
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
    use opentelemetry_sdk::trace::Sampler;
    use opentelemetry_sdk::trace::{BatchConfigBuilder, SdkTracerProvider, SpanData, SpanExporter};

    use super::{ExportRuntime, EXPORT_THREAD_NAME};

    /// A span exporter that records the name of the thread its `export` runs on.
    /// Proves *which* runtime drives the batch processor's background task.
    #[derive(Debug, Clone)]
    struct ThreadNameExporter {
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl SpanExporter for ThreadNameExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            let name = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_owned();
            self.seen.lock().expect("seen lock").push(name);
            Ok(())
        }
    }

    /// A one-shot gate the test opens after its assertions, so a blocked export
    /// thread is released deterministically rather than waiting out a fixed
    /// sleep (which would make the provider's flush-on-drop stall the test).
    #[derive(Debug, Default)]
    struct Gate {
        open: Mutex<bool>,
        cv: Condvar,
    }

    impl Gate {
        /// Block the calling OS thread until [`open`](Self::open) is called.
        fn wait(&self) {
            let mut open = self.open.lock().expect("gate lock");
            while !*open {
                open = self.cv.wait(open).expect("gate wait");
            }
        }

        /// Release any waiter; never blocks on the export having started.
        fn open(&self) {
            *self.open.lock().expect("gate lock") = true;
            self.cv.notify_all();
        }
    }

    /// A span exporter that hard-blocks its executing OS thread on every export
    /// until the test opens its [`Gate`], standing in for a black-holed
    /// collector (accepts TCP, never replies) or the blocking-DNS starvation of
    /// issues #356/#533. On a shared runtime this would pin a serving worker; the
    /// test proves that confined to the export runtime it cannot.
    #[derive(Debug)]
    struct BlackHoleExporter {
        gate: Arc<Gate>,
    }

    impl SpanExporter for BlackHoleExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            // Block the OS thread itself (not just the async task) — the worst
            // case the isolation must withstand — until the test releases us.
            self.gate.wait();
            Ok(())
        }
    }

    /// Build a tracer provider whose batch processor is driven by `export_rt`,
    /// by entering the runtime's context while the processor is constructed —
    /// the exact mechanism `init::build_providers_on` uses in production.
    fn provider_on<E: SpanExporter + 'static>(
        export_rt: &ExportRuntime,
        exporter: E,
    ) -> SdkTracerProvider {
        let _enter = export_rt.handle().enter();
        // Tiny scheduled delay so an ended span is picked up and exported almost
        // immediately, rather than waiting out the multi-second default.
        let batch_config = BatchConfigBuilder::default()
            .with_scheduled_delay(Duration::from_millis(10))
            .build();
        let processor = BatchSpanProcessor::builder(exporter, runtime::Tokio)
            .with_batch_config(batch_config)
            .build();
        SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_sampler(Sampler::AlwaysOn)
            .build()
    }

    /// Dropping the export runtime *inside another runtime's async context* must
    /// not panic. This is the envd production path: the `TelemetryGuard` holds
    /// the only remaining `Arc<ExportRuntime>` and drops at the end of
    /// `runtime.block_on(...)`, i.e. on a thread that is inside the serving
    /// runtime. A plain `Runtime` drop blocks there and panics ("Cannot drop a
    /// runtime in a context where blocking is not allowed"); `Drop`'s
    /// `shutdown_background` is what makes this safe.
    #[test]
    fn dropping_inside_a_runtime_does_not_panic() {
        let serving = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build serving runtime");

        serving.block_on(async {
            let export_rt = ExportRuntime::new().expect("build export runtime");
            // Spawn a never-finishing task onto it so the drop has live tasks to
            // abandon — the case a blocking drop would wait on (and panic over).
            export_rt
                .handle()
                .spawn(async { std::future::pending::<()>().await });
            // The drop happens here, inside `block_on`. If it blocked, this test
            // would panic rather than return.
            drop(export_rt);
        });
    }

    /// The isolation contract: a provider built inside the export runtime's
    /// context exports on an `EXPORT_THREAD_NAME` thread, never on the serving
    /// runtime that emitted the span.
    #[test]
    fn export_runs_on_the_dedicated_runtime() {
        let export_rt = ExportRuntime::new().expect("build export runtime");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let provider = provider_on(&export_rt, ThreadNameExporter { seen: seen.clone() });

        // A distinctly-named serving runtime emits the span; the export must NOT
        // land on one of its threads.
        let serving = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("test-serving-runtime")
            .enable_all()
            .build()
            .expect("build serving runtime");
        serving.block_on(async {
            provider.tracer("test").in_span("probe", |_| {});
            // Let the batch processor's scheduled delay fire and the export run.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let names = seen.lock().expect("seen lock");
        assert!(
            !names.is_empty(),
            "the span should have been exported at least once"
        );
        assert!(
            names.iter().all(|n| n.starts_with(EXPORT_THREAD_NAME)),
            "export ran on {names:?}; expected only `{EXPORT_THREAD_NAME}` threads, \
             proving it is confined to the dedicated runtime"
        );
    }

    /// The #539 guarantee: a black-holed exporter that pins its worker thread
    /// forever must NOT starve the serving runtime. Both runtimes have a single
    /// worker, so if export shared the serving runtime the blocked thread would
    /// stall the tick task and the assertion would time out.
    #[test]
    fn black_holed_export_does_not_starve_the_serving_runtime() {
        let export_rt = ExportRuntime::new().expect("build export runtime");
        let gate = Arc::new(Gate::default());
        let provider = provider_on(&export_rt, BlackHoleExporter { gate: gate.clone() });

        let serving = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build serving runtime");

        serving.block_on(async move {
            let tracer = provider.tracer("test");
            // Emit spans to trigger exports that black-hole on the export runtime.
            for _ in 0..16 {
                tracer.in_span("probe", |_| {});
            }

            // Concurrently, prove the serving runtime keeps making progress: a
            // task that ticks on the timer must complete within a generous bound.
            // Were export sharing this single-worker runtime, the blocked export
            // thread would prevent these ticks and the timeout would fire.
            let ticks = Arc::new(AtomicUsize::new(0));
            let counter = ticks.clone();
            let ticker = tokio::spawn(async move {
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });

            tokio::time::timeout(Duration::from_secs(5), ticker)
                .await
                .expect("serving runtime starved: tick task did not finish in time")
                .expect("tick task panicked");
            assert_eq!(
                ticks.load(Ordering::SeqCst),
                5,
                "the serving runtime must keep ticking despite a black-holed exporter"
            );

            // Release the black hole so the provider's flush-on-drop (when this
            // block ends) doesn't wait out a still-blocked export thread.
            gate.open();
        });
    }
}
