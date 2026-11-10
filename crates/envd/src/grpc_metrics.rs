//! Per-method OTel metrics for envd's inbound gRPC surface.
//!
//! [`GrpcMetricsLayer`] is a [`tower::Layer`] installed once on the tonic server
//! (`Server::builder().layer(GrpcMetricsLayer)`), the sibling of
//! [`crate::trace::GrpcTraceLayer`]. For every inbound RPC it records two
//! instruments under the meter `envd.grpc.server`:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `rpc.server.duration` | `Histogram<f64>` | s | rpc.system, rpc.service, rpc.method |
//! | `rpc.server.request.count` | `Counter<u64>` | --- | rpc.system, rpc.service, rpc.method |
//!
//! Attributes follow the OTel RPC semantic conventions: the gRPC path
//! `/package.Service/Method` is split into `rpc.service` (`package.Service`) and
//! `rpc.method` (`Method`), with a constant `rpc.system="grpc"` — rather than
//! cramming the whole path into `rpc.method`, which would not group correctly in
//! an APM's native gRPC views. All three are statically bounded by the proto
//! (~18 methods), so they are safe metric labels. Duration is measured until the
//! inner service resolves the response (time-to-first-response); for
//! server-streaming RPCs that is handler setup, not the stream lifetime — the
//! conventional gRPC server-side latency signal.
//!
//! Both instruments are recorded by an [`RpcRecorder`] drop guard, so a request
//! future cancelled mid-flight (client disconnect, deadline) is still counted
//! with the time the server spent on it, rather than silently vanishing.
//!
//! Doing this as one tower layer (rather than a line in every RPC handler) keeps
//! it DRY and impossible to forget on a new method. With `OTEL_SDK_DISABLED` the
//! global meter is a no-op, so the layer stays installed unconditionally.
//!
//! # Recording never blocks a request (issue #350)
//!
//! The actual `record()`/`add()` calls are deliberately moved OFF the request
//! path onto a single dedicated OS thread, fed through a bounded
//! [`std::sync::mpsc::sync_channel`] with a non-blocking [`try_send`]. The drop
//! guard only enqueues a tiny [`Measurement`]; if the queue is full it sheds the
//! sample rather than blocking.
//!
//! This is load-bearing, not premature optimisation: a `runsc` **memory restore**
//! can thaw the OpenTelemetry metrics-aggregation lock in a wedged state (a
//! background reader frozen mid-collect), so a *synchronous* `record()` on the
//! restored process blocks forever. Before this change every inbound gRPC RPC
//! recorded inline, so that wedge stalled the entire `:49983` surface while
//! envd's layer-free `:49984` HTTP listener kept serving — the exact post-resume
//! hang in #350. Confining the (possibly-blocking) record to one thread means a
//! wedged meter can at worst stall that thread and shed samples; it can never
//! stall a handler. A dedicated OS thread (not a tokio task) is used so a blocked
//! `record()` cannot tie up a runtime worker.
//!
//! # Surviving an OTel re-arm (issue #402)
//!
//! The recorder caches its [`RpcMetrics`] *once* at thread start (built off the
//! global meter `init_telemetry` installed). After a `runsc` checkpoint/restore
//! the node swaps the whole OTLP provider stack via
//! [`RearmHandle::rearm`](sandbawks_observability::RearmHandle::rearm), which
//! calls `global::set_meter_provider(new)`. That swap alone is invisible to the
//! recorder: its cached instruments stay bound to the *old*, shut-down provider,
//! so post-resume RPC metrics silently vanish.
//!
//! [`rearm`] fixes this by enqueuing a [`RecorderMsg::Rearm`] control message
//! that makes the recorder rebuild [`RpcMetrics`] against the now-current global
//! meter. The `/__rearm` handler calls it *after* `set_meter_provider(new)` so
//! the rebuild reads the fresh provider. The signal rides the same bounded queue
//! as samples and uses the same non-blocking [`try_send`], so the #350 invariant
//! is preserved — a wedged recorder can only shed the signal, never block.
//!
//! [`try_send`]: std::sync::mpsc::SyncSender::try_send

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};
use tower::{Layer, Service};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "envd.grpc.server";

/// Cached gRPC-server instruments, built once via [`metrics`].
struct RpcMetrics {
    /// `rpc.server.duration` — per-RPC time-to-response in seconds.
    duration: Histogram<f64>,
    /// `rpc.server.request.count` — per-RPC request counter.
    count: Counter<u64>,
}

impl RpcMetrics {
    /// Build both instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            duration: meter
                .f64_histogram("rpc.server.duration")
                .with_unit("s")
                .with_description("Duration of inbound gRPC server RPCs.")
                .build(),
            count: meter
                .u64_counter("rpc.server.request.count")
                .with_description("Total inbound gRPC server RPCs.")
                .build(),
        }
    }
}

/// A single recorded RPC, handed to the background recorder thread.
struct Measurement {
    /// `rpc.system` / `rpc.service` / `rpc.method` per OTel RPC semconv.
    attrs: [KeyValue; 3],
    /// Time-to-response in seconds, for the duration histogram.
    duration_s: f64,
}

/// A message on the recorder's bounded queue: either an RPC to emit, or a
/// control signal to rebuild the cached instruments after a telemetry re-arm.
///
/// Folding the control signal into the *same* channel as samples (rather than a
/// second side-channel) keeps the recorder a single `recv()` loop and preserves
/// strict ordering: a [`Rearm`] enqueued after a burst of [`Record`]s rebuilds
/// only once those samples have drained onto the old provider, never before.
///
/// [`Record`]: RecorderMsg::Record
/// [`Rearm`]: RecorderMsg::Rearm
enum RecorderMsg {
    /// Emit this measurement on the current instruments.
    Record(Measurement),
    /// Rebuild [`RpcMetrics`] against the now-current global meter provider,
    /// after a `set_meter_provider(new)` swap (issue #402). See [`rearm`].
    Rearm,
}

/// Bounded queue depth before samples are shed. envd's gRPC surface is a
/// low-frequency control channel, so this easily absorbs normal bursts; the
/// cap exists only to bound memory if the recorder thread stalls (the #350
/// meter wedge), at which point excess samples are dropped, never queued
/// unboundedly.
const QUEUE_DEPTH: usize = 1024;

/// Process-wide handle to the background metrics recorder.
///
/// The first call spawns a dedicated OS thread that owns the OTel instruments
/// and performs the actual `record()`/`add()`. See the module docs for why
/// recording is moved off the request path (#350). If the thread fails to
/// spawn, the returned sender's receiver has been dropped, so every
/// [`try_record`] cleanly sheds its sample rather than panicking the first RPC.
fn recorder() -> &'static SyncSender<RecorderMsg> {
    static RECORDER: OnceLock<SyncSender<RecorderMsg>> = OnceLock::new();
    RECORDER.get_or_init(|| {
        let (tx, rx) = sync_channel::<RecorderMsg>(QUEUE_DEPTH);
        if let Err(error) = std::thread::Builder::new()
            .name("envd-grpc-metrics".to_owned())
            .spawn(move || run_recorder(rx))
        {
            // `rx` was moved into the closure that never ran, so it is dropped
            // here and the sender is disconnected — `try_record` then sheds
            // every sample (per-RPC metrics off) instead of blowing up.
            tracing::warn!(%error, "failed to spawn gRPC metrics recorder thread; per-RPC metrics disabled");
        }
        tx
    })
}

/// Drain `rx` until all senders drop, recording each [`Measurement`] on the OTel
/// instruments and rebuilding those instruments on each [`RecorderMsg::Rearm`].
///
/// The instruments are built here, on this thread, so the global meter provider
/// (installed by `init_telemetry` before the first RPC) is already in place. On
/// a re-arm the provider is swapped out from under us (#402); the `Rearm` arm
/// rebuilds [`RpcMetrics`] so subsequent records bind to the live provider
/// rather than the shut-down one captured at thread start.
fn run_recorder(rx: Receiver<RecorderMsg>) {
    // Production builds instruments off the live global meter. The build is a
    // seam (rather than an inline `RpcMetrics::new()`) so tests can drive the
    // loop with deterministic, provider-pinned instrument sets instead of racing
    // a background thread's lazy `global::meter()` bind. See [`run_recorder_loop`].
    run_recorder_loop(rx, RpcMetrics::new)
}

/// The recorder's recv loop, generic over how a fresh instrument set is built.
///
/// Each [`RecorderMsg::Record`] is emitted on the current instruments; each
/// [`RecorderMsg::Rearm`] rebuilds them via `build`, the #402 fix — after a
/// `set_meter_provider(new)` swap the old instruments are bound to a shut-down
/// provider (`global::meter()` binds eagerly), so they must be rebuilt to follow
/// the live provider. The loop ends when every sender drops.
fn run_recorder_loop(rx: Receiver<RecorderMsg>, mut build: impl FnMut() -> RpcMetrics) {
    // `mut` so the `Rearm` arm can swap in a freshly-built instrument set.
    let mut metrics = build();
    while let Ok(msg) = rx.recv() {
        match msg {
            RecorderMsg::Record(m) => {
                // These may block if the OTel SDK is wedged (#350). Isolating
                // them on this thread is the whole point: the block stays here,
                // never on a request.
                metrics.duration.record(m.duration_s, &m.attrs);
                metrics.count.add(1, &m.attrs);
            }
            RecorderMsg::Rearm => {
                // The global meter provider was just swapped (#401's
                // `set_meter_provider`). Rebuild off it so we stop emitting into
                // the old, shut-down provider — the #402 fix.
                metrics = build();
            }
        }
    }
}

/// Enqueue a measurement for the background recorder, or DROP it if the queue
/// is full or the recorder is gone. Returns whether it was queued.
///
/// This is the #350 invariant in one line: [`SyncSender::try_send`] never
/// blocks, so a wedged meter can only ever cost a shed sample — never a stalled
/// gRPC handler.
///
/// [`SyncSender::try_send`]: std::sync::mpsc::SyncSender::try_send
fn try_record(tx: &SyncSender<RecorderMsg>, measurement: Measurement) -> bool {
    tx.try_send(RecorderMsg::Record(measurement)).is_ok()
}

/// Signal the background recorder to rebuild its cached gRPC instruments against
/// the *current* global meter provider, after a telemetry re-arm (issue #402).
///
/// The recorder caches [`RpcMetrics`] once at thread start, so a bare
/// `global::set_meter_provider(new)` leaves it emitting into the old, shut-down
/// provider. The `/__rearm` handler calls this **after** that swap so the
/// rebuild reads the fresh provider.
///
/// Like the per-RPC path, the signal rides the bounded queue via a non-blocking
/// [`try_send`], preserving the #350 invariant: a wedged or vanished recorder
/// can only shed the signal, never block the caller. Returns whether the signal
/// was queued; a shed signal is logged at `warn` since it leaves the recorder on
/// the pre-rearm provider until the next successful re-arm.
///
/// [`try_send`]: std::sync::mpsc::SyncSender::try_send
pub fn rearm() -> bool {
    let queued = recorder().try_send(RecorderMsg::Rearm).is_ok();
    if !queued {
        tracing::warn!(
            "gRPC metrics re-arm signal shed (recorder queue full or gone); \
             per-RPC metrics may keep emitting into the pre-rearm meter provider"
        );
    }
    queued
}

/// Split a gRPC path `/package.Service/Method` into the OTel-semconv
/// (`rpc.service`, `rpc.method`) pair (`package.Service`, `Method`).
///
/// A malformed path (no `/` after the leading one) falls back to the whole
/// trimmed path as the service with an empty method — defensive only; tonic
/// always routes the conventional shape.
fn split_grpc_path(path: &str) -> (&str, &str) {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    match trimmed.rsplit_once('/') {
        Some((service, method)) => (service, method),
        None => (trimmed, ""),
    }
}

/// RAII guard that records the per-RPC duration + count on *every* exit path of
/// the request future — normal completion or a mid-flight drop (cancellation,
/// client disconnect, deadline). Recording as plain statements after the awaited
/// inner future would skip the cancellation path; `Drop` runs there too.
struct RpcRecorder {
    /// `rpc.system` / `rpc.service` / `rpc.method` per OTel RPC semconv. Wrapped
    /// in `Option` so `Drop` can MOVE the array into a [`Measurement`] via
    /// `take()` with no clone.
    attrs: Option<[KeyValue; 3]>,
    /// When the RPC began, for the duration histogram.
    start: Instant,
}

impl RpcRecorder {
    /// Stamp the semconv attributes and start the clock.
    fn new(service: &str, method: &str) -> Self {
        Self {
            attrs: Some([
                KeyValue::new("rpc.system", "grpc"),
                KeyValue::new("rpc.service", service.to_owned()),
                KeyValue::new("rpc.method", method.to_owned()),
            ]),
            start: Instant::now(),
        }
    }
}

impl Drop for RpcRecorder {
    fn drop(&mut self) {
        // Hand the sample to the background recorder, or shed it if the queue is
        // full / the recorder is gone. This NEVER blocks the request (#350) —
        // see the module docs. `take()` runs once (Drop fires once), so the
        // `Option` is always `Some` here on the real path.
        if let Some(attrs) = self.attrs.take() {
            try_record(
                recorder(),
                Measurement {
                    attrs,
                    duration_s: self.start.elapsed().as_secs_f64(),
                },
            );
        }
    }
}

/// A [`tower::Layer`] that records per-method duration + count for every inbound
/// gRPC request. Install it on the tonic server with
/// `Server::builder().layer(GrpcMetricsLayer)`.
#[derive(Clone, Copy, Default, Debug)]
pub struct GrpcMetricsLayer;

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetrics { inner }
    }
}

/// The [`Service`] produced by [`GrpcMetricsLayer`]. Wraps the inner gRPC
/// service, timing each request and recording its method.
#[derive(Clone, Debug)]
pub struct GrpcMetrics<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for GrpcMetrics<S>
where
    // The bounds tonic's `serve` already requires of the layered service: it is
    // cloned per connection and driven on the server task.
    S: Service<http::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    // Boxed because the recording happens *after* the inner future resolves: the
    // alternative — a hand-written `Future` with manual pin projection — would
    // need `unsafe`, which this crate avoids. envd's gRPC surface is a
    // low-frequency control channel, so one small allocation per RPC is fine.
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        // The gRPC path (`/package.Service/Method`) is statically bounded by the
        // proto; split into the semconv `rpc.service` / `rpc.method` pair. Done
        // here (before the move into the future) so the recorder owns just the
        // two short slices, not the whole request.
        let (service, method) = split_grpc_path(request.uri().path());
        let recorder = RpcRecorder::new(service, method);

        // Standard tower readiness dance: the slot reserved by `poll_ready` lives
        // on the *current* `inner`, so swap a fresh clone into `self` and `call`
        // the reserved original (see `trace::GrpcTrace` for the full rationale).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            // `recorder` records duration + count when it drops — whether this
            // block completes or is cancelled mid-await.
            let _recorder = recorder;
            inner.call(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::future::poll_fn;

    /// Minimal inner gRPC-shaped service returning an empty 200 response. Hand
    /// -rolled (rather than `tower::service_fn`) so the test needs no tower
    /// feature beyond the core `Service`/`Layer` traits the layer itself uses.
    #[derive(Clone)]
    struct Echo;

    impl Service<http::Request<()>> for Echo {
        type Response = http::Response<()>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<http::Response<()>, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: http::Request<()>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(())))
        }
    }

    /// Build throwaway semconv attributes for the recorder tests.
    fn test_attrs() -> [KeyValue; 3] {
        [
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", "process.Process"),
            KeyValue::new("rpc.method", "List"),
        ]
    }

    /// The #350 invariant: enqueuing a measurement must NEVER block, even when
    /// the recorder is wedged. A bounded channel with no draining receiver
    /// stands in for a stalled recorder thread (e.g. the OTel metrics-aggregation
    /// lock frozen across a `runsc` memory restore). Once full, further samples
    /// are SHED, and `try_record` returns promptly — if it ever blocked on a
    /// full queue, this test would hang instead of completing.
    #[test]
    fn try_record_sheds_samples_without_blocking_when_recorder_is_wedged() {
        // Hold `_rx` but never drain it: the recorder is "stuck". sync_channel(2)
        // buffers exactly two samples without a receiver taking them.
        let (tx, _rx) = sync_channel::<RecorderMsg>(2);
        let sample = || Measurement {
            attrs: test_attrs(),
            duration_s: 0.0,
        };

        // The first two fill the bounded buffer...
        assert!(try_record(&tx, sample()), "first sample should queue");
        assert!(try_record(&tx, sample()), "second sample should queue");
        // ...and every further sample is shed (dropped), not blocked. Reaching
        // these asserts at all proves `try_record` did not block on the full
        // queue.
        assert!(
            !try_record(&tx, sample()),
            "a full queue must shed the sample, not block"
        );
        assert!(
            !try_record(&tx, sample()),
            "a full queue must keep shedding, never block"
        );
    }

    /// A disconnected recorder (spawn failed, or the thread exited) must also
    /// shed cleanly rather than panic — the fallback path in [`recorder`].
    #[test]
    fn try_record_sheds_when_recorder_is_gone() {
        let (tx, rx) = sync_channel::<RecorderMsg>(2);
        drop(rx); // recorder thread "never spawned" / exited.
        assert!(
            !try_record(
                &tx,
                Measurement {
                    attrs: test_attrs(),
                    duration_s: 0.0,
                }
            ),
            "a disconnected recorder must shed the sample without panicking"
        );
    }

    #[test]
    fn split_grpc_path_separates_service_and_method() {
        assert_eq!(
            split_grpc_path("/process.Process/List"),
            ("process.Process", "List")
        );
        assert_eq!(
            split_grpc_path("/filesystem.Filesystem/Stat"),
            ("filesystem.Filesystem", "Stat")
        );
        // Malformed (no method segment) falls back to whole-path-as-service.
        assert_eq!(split_grpc_path("/healthz"), ("healthz", ""));
    }

    #[tokio::test]
    async fn layer_passes_the_request_through() {
        // With no meter provider installed the global meter is a no-op, so this
        // exercises the recording path without an exporter and proves the layer
        // returns the inner service's response unchanged.
        let mut svc = GrpcMetricsLayer.layer(Echo);
        poll_fn(|cx| svc.poll_ready(cx)).await.expect("ready");
        let request = http::Request::builder()
            .uri("/process.Process/List")
            .body(())
            .expect("building a test request");
        let response = svc.call(request).await.expect("inner call succeeds");
        assert_eq!(response.status(), http::StatusCode::OK);
    }

    /// Count the data points exported for the `rpc.server.request.count` counter
    /// across everything an in-memory exporter has captured. Used to prove which
    /// meter provider a record landed on: the SDK's value fields are crate
    /// -private, but the per-series data-point count is public and discriminating
    /// enough — a provider that received no post-rearm record exports zero.
    fn counter_data_points(exporter: &opentelemetry_sdk::metrics::InMemoryMetricExporter) -> usize {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        exporter
            .get_finished_metrics()
            .expect("metrics are exported")
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .filter(|m| m.name() == "rpc.server.request.count")
            .map(|m| match m.data() {
                // The counter is `u64`; sum aggregation is the default for a
                // monotonic counter.
                AggregatedMetrics::U64(MetricData::Sum(sum)) => sum.data_points().count(),
                _ => 0,
            })
            .sum()
    }

    /// The #402 fix: a `Rearm` message must rebuild the recorder's instruments,
    /// so records that follow the re-arm land on the meter provider installed at
    /// re-arm time — not the one captured before it.
    ///
    /// Two independent in-memory meter providers stand in for the pre- and
    /// post-restore OTel SDK. The instrument sets are *pinned* up front — one off
    /// each provider — and fed to the real [`run_recorder_loop`] via its build
    /// seam. Because `global::meter()` binds eagerly (a later `set_meter_provider`
    /// is invisible to an already-built instrument), this single-threaded drive is
    /// both deterministic (no race against a background thread's bind) and a
    /// faithful stand-in for what `RpcMetrics::new()` builds at each point.
    #[test]
    fn rearm_rebuilds_instruments_against_the_new_meter_provider() {
        use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

        let exporter_before = InMemoryMetricExporter::default();
        let provider_before = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter_before.clone())
            .build();
        let exporter_after = InMemoryMetricExporter::default();
        let provider_after = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter_after.clone())
            .build();

        // Pin one instrument set per provider, mirroring the eager bind: the
        // "before" set is what the recorder caches at start; the "after" set is
        // what a post-swap `RpcMetrics::new()` would build. The build seam hands
        // them out in order — the initial build, then one per `Rearm`.
        global::set_meter_provider(provider_before.clone());
        let before_metrics = RpcMetrics::new();
        global::set_meter_provider(provider_after.clone());
        let after_metrics = RpcMetrics::new();
        let mut builds = vec![before_metrics, after_metrics].into_iter();
        let build = move || {
            builds
                .next()
                .expect("recorder rebuilt instruments more than expected")
        };

        let sample = |duration_s| {
            RecorderMsg::Record(Measurement {
                attrs: test_attrs(),
                duration_s,
            })
        };

        // Pre-load the full transcript and close the channel, then drive the loop
        // synchronously: Record (-> before), Rearm (-> rebuild to after), Record
        // (-> after). No threads, so no race and no double-flush bookkeeping.
        let (tx, rx) = sync_channel::<RecorderMsg>(QUEUE_DEPTH);
        tx.send(sample(0.1)).expect("queue pre-rearm record");
        tx.send(RecorderMsg::Rearm).expect("queue rearm");
        tx.send(sample(0.2)).expect("queue post-rearm record");
        drop(tx);
        run_recorder_loop(rx, build);

        // Flush each provider exactly once and inspect what it captured.
        provider_before
            .force_flush()
            .expect("flush before provider");
        provider_after.force_flush().expect("flush after provider");

        assert_eq!(
            counter_data_points(&exporter_before),
            1,
            "the pre-rearm RPC must record on the original meter provider"
        );
        assert_eq!(
            counter_data_points(&exporter_after),
            1,
            "the post-rearm RPC must record on the NEW meter provider, \
             proving Rearm rebuilt the instruments off the swapped-in provider"
        );
    }
}
