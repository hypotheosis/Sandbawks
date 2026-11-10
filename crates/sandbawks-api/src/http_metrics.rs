//! OTel HTTP-server metrics middleware (OBSERVABILITY.md §8.1).
//!
//! A single [`axum::middleware::from_fn`] handler emits the three OTel-semconv
//! HTTP-server instruments for every inbound request:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `http.server.request.duration` | `Histogram<f64>` | s | method, route, status |
//! | `http.server.active_requests` | `UpDownCounter<i64>` | --- | method, route |
//! | `http.server.request.count` | `Counter<u64>` | --- | method, route, status |
//!
//! Two cardinality-bounding choices, copied from the strategy doc:
//!
//! - **Route is the matched pattern, not the raw URI.** Reading axum's
//!   [`MatchedPath`] (e.g. `/sandboxes/:id`) keeps the `http.route` label set to
//!   the handful of registered routes rather than one series per distinct id. An
//!   unmatched path (no route, e.g. a 404) falls back to an `unmatched:<path>`
//!   marker so a dashboard can *see* cardinality risk instead of silently
//!   absorbing it.
//! - **The in-flight gauge carries fewer attributes** (no status code) — its job
//!   is a comprehensible "requests in flight per route", not a status breakdown.
//!
//! The instruments are cached in a [`OnceLock`] singleton so the SDK's
//! `meter.<kind>(...).build()` (a registry lookup + allocation) runs once, not
//! per request. When `OTEL_SDK_DISABLED` is set, `global::meter(...)` returns a
//! no-op meter whose instrument operations are zero-cost, so this middleware
//! stays installed unconditionally — there is no enabled/disabled branch on the
//! request path.

use std::sync::OnceLock;
use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name: the *library* surface emitting these points, per
/// OTel guidance (not the service name — that is a resource attribute).
const METER_NAME: &str = "sandbawks-api.http.server";

/// Cached HTTP-server instruments, built once via [`metrics`].
struct ServerMetrics {
    /// `http.server.request.duration` — request latency in seconds.
    duration: Histogram<f64>,
    /// `http.server.active_requests` — in-flight request gauge.
    active: UpDownCounter<i64>,
    /// `http.server.request.count` — completed-request counter.
    count: Counter<u64>,
}

impl ServerMetrics {
    /// Build the three instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            duration: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .with_description("Duration of inbound HTTP server requests.")
                .build(),
            active: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_description("In-flight inbound HTTP server requests.")
                .build(),
            count: meter
                .u64_counter("http.server.request.count")
                .with_description("Total inbound HTTP server requests.")
                .build(),
        }
    }
}

/// The process-wide instrument singleton.
///
/// `get_or_init` runs [`ServerMetrics::new`] exactly once on first request; every
/// later call is a cheap pointer read, keeping the per-request cost to recording
/// points rather than rebuilding instruments.
fn metrics() -> &'static ServerMetrics {
    static METRICS: OnceLock<ServerMetrics> = OnceLock::new();
    METRICS.get_or_init(ServerMetrics::new)
}

/// RAII guard that fires the in-flight gauge's `-1` on *every* exit path —
/// normal return, a handler panic that unwinds, or the request future being
/// dropped mid-flight (client disconnect / cancellation).
///
/// Recording the `-1` as a plain statement after `next.run(...).await` would
/// skip it on the panic and cancellation paths, so the gauge would drift upward
/// forever and stop reflecting reality on a long-running process. `Drop` runs on
/// unwind and on cancellation, so the increment/decrement always balance.
struct InFlightGuard {
    /// The shared instruments — held so `Drop` needs no second `metrics()`
    /// lookup.
    metrics: &'static ServerMetrics,
    /// The method + route attributes the matching `+1` was recorded with.
    attrs: [KeyValue; 2],
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.active.add(-1, &self.attrs);
    }
}

/// Resolve the low-cardinality `http.route` label for a request.
///
/// Prefers axum's [`MatchedPath`] (the registered route pattern); falls back to
/// an `unmatched:<path>` marker when no route matched, so unbounded raw paths
/// are flagged rather than silently exploding the label set.
///
/// Generic over the body type and `pub(crate)` so the request-span middleware
/// ([`crate::middleware::ExtractingMakeSpan`]) tags `http.route` with the same
/// value the metrics use — one source of truth for the endpoint identity the
/// APM groups on (#369 follow-up).
pub(crate) fn route_label<B>(request: &axum::http::Request<B>) -> String {
    match request.extensions().get::<MatchedPath>() {
        Some(matched) => matched.as_str().to_owned(),
        None => format!("unmatched:{}", request.uri().path()),
    }
}

/// axum middleware emitting the three HTTP-server instruments around each
/// request.
///
/// Install it as a top-level `Router` layer:
///
/// ```no_run
/// # use axum::Router;
/// # let app: Router = Router::new();
/// let app = app.layer(axum::middleware::from_fn(
///     sandbawks_api::http_metrics::track_metrics,
/// ));
/// ```
///
/// `Router::layer` applies the middleware *per matched route*, so the
/// [`MatchedPath`] extension is already populated when this runs — that is what
/// makes the bounded `http.route` label work.
///
/// The active-requests gauge is incremented before the inner handler and
/// decremented by an [`InFlightGuard`] on every exit path, so it reflects true
/// in-flight concurrency even under panics and cancellations; the duration
/// histogram and request counter are recorded once the response status is known
/// (so a cancelled request — which never produces a status — is not counted,
/// which is correct: it had no outcome).
pub async fn track_metrics(request: Request, next: Next) -> Response {
    let metrics = metrics();
    let method = request.method().as_str().to_owned();
    let route = route_label(&request);

    // The gauge carries only method + route (no status) — see the module docs.
    // Built from clones so `method` / `route` survive for the richer attribute
    // set recorded after the response status is known.
    let active_attrs = [
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
    ];
    metrics.active.add(1, &active_attrs);
    // The matching `-1` fires when `_in_flight` drops — at the end of this
    // function, or early on a panic-unwind / cancellation. See `InFlightGuard`.
    let _in_flight = InFlightGuard {
        metrics,
        attrs: active_attrs,
    };

    let start = Instant::now();
    let response = next.run(request).await;
    let elapsed = start.elapsed().as_secs_f64();

    // Duration + count additionally carry the response status code. `as i64`
    // because OTel attribute integers are i64; an HTTP status always fits.
    let status = response.status().as_u16() as i64;
    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status),
    ];
    metrics.duration.record(elapsed, &attrs);
    metrics.count.add(1, &attrs);

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt; // brings `oneshot` into scope

    #[test]
    fn route_label_falls_back_to_unmatched_marker() {
        // A bare request never routed through a `Router` has no `MatchedPath`
        // extension, so the label is the `unmatched:` marker over the raw path.
        let request = Request::builder()
            .uri("/no/such/route")
            .body(Body::empty())
            .expect("building a test request");
        assert_eq!(route_label(&request), "unmatched:/no/such/route");
    }

    #[tokio::test]
    async fn track_metrics_passes_the_request_through() {
        // End-to-end through a real `Router` so the `MatchedPath` extension is
        // populated when the middleware runs (the matched-route branch). With no
        // meter provider installed the global meter is a no-op, so this exercises
        // the recording path without an exporter — and proves the middleware does
        // not alter the response.
        let app = Router::new()
            .route("/h/:id", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(track_metrics));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/h/42")
                    .body(Body::empty())
                    .expect("building a test request"),
            )
            .await
            .expect("router responds");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
