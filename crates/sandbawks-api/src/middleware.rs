//! Cross-cutting axum middleware: request-id assignment, structured
//! tracing of every request, and a global timeout.
//!
//! These are factory functions rather than `Layer` impls because the
//! tower-http types they wrap aren't `Default`-constructible and we
//! want one canonical site for tuning (header name, span shape,
//! timeout duration).

use std::time::Duration;

use axum::http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use tower_http::{
    classify::{ServerErrorsAsFailures, SharedClassifier},
    request_id::{MakeRequestId, RequestId, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::{DefaultOnRequest, DefaultOnResponse, MakeSpan, OnResponse, TraceLayer},
};
use tracing::Span;

/// Cheap monotonic-ish 32-hex request-id generator.
///
/// We deliberately avoid pulling `uuid` as a dependency: request-ids
/// are log-correlation tokens, not security identifiers, so a
/// timestamp-derived value is sufficient. Matches the format used by
/// `error::generate_request_id` so headers and bodies agree on shape.
#[derive(Clone, Default)]
pub struct Counter;

impl MakeRequestId for Counter {
    fn make_request_id<B>(&mut self, _req: &Request<B>) -> Option<RequestId> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let id = format!("req_{nanos:032x}");
        HeaderValue::from_str(&id).ok().map(RequestId::new)
    }
}

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// `tower-http::SetRequestIdLayer` factory.
///
/// Install this *before* `trace_layer()` in the stack so the span
/// created by `TraceLayer` can pick the id up from request extensions.
pub fn request_id_layer() -> SetRequestIdLayer<Counter> {
    SetRequestIdLayer::new(REQUEST_ID_HEADER, Counter)
}

/// Builds the per-request `tracing` span and, crucially, **continues an
/// inbound W3C trace** rather than starting a fresh one.
///
/// `tower-http`'s `DefaultMakeSpan` tags the span with method / URI but does
/// *not* read `traceparent`, so an upstream caller's trace would be severed at
/// the gateway. This [`MakeSpan`] impl extracts the inbound context and adopts
/// it as the span's parent (issue #298), so a request arriving with a
/// `traceparent` (today: a future ingress; tomorrow: any traced client)
/// continues the same `trace_id` through the api and on into envd — the span is
/// current for the whole request, so the envd-bound gRPC interceptor injects
/// *this* context downstream.
///
/// A request without a `traceparent` (the common case) extracts an empty
/// context, leaving the span a trace root exactly as before.
#[derive(Clone, Copy, Default)]
pub struct ExtractingMakeSpan;

impl<B> MakeSpan<B> for ExtractingMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        // Tag the span with the OTLP HTTP-server *semantic-convention*
        // attributes an APM groups endpoints on — `http.request.method` +
        // `http.route` (+ `url.path`, `http.response.status_code`) — not the
        // ad-hoc `method`/`uri` keys we used before. Those were ingested fine
        // but never rendered as endpoint traces, because the backend keys the
        // endpoints view on the semconv attributes (the metrics already use
        // them via `http_metrics`, which is why metrics showed but traces did
        // not). The span name follows the `"{method} {route}"` HTTP-server
        // convention via `otel.name`, and `otel.kind = "server"` marks the
        // inbound edge (issue #298). `http.route` reads the same
        // [`MatchedPath`] the metrics do; it is populated here because this
        // layer runs per matched route (see `http_metrics::track_metrics`).
        let method = request.method().clone();
        let route = crate::http_metrics::route_label(request);
        let span = tracing::info_span!(
            "http.request",
            otel.kind = "server",
            otel.name = %format!("{method} {route}"),
            http.request.method = %method,
            http.route = %route,
            url.path = %request.uri().path(),
            // Recorded by [`RecordStatus`] once the response status is known.
            http.response.status_code = tracing::field::Empty,
        );
        sandbawks_observability::continue_span(
            &span,
            &sandbawks_observability::HttpHeaderExtractor(request.headers()),
        );
        span
    }
}

/// [`OnResponse`] hook that records the final `http.response.status_code`
/// onto the request span (declared `Empty` by [`ExtractingMakeSpan`]) so the
/// APM can show per-endpoint status, then defers to the default response log.
///
/// The status is only known after the inner service responds, so it cannot be
/// set at span-creation time; this closes that gap without losing
/// [`DefaultOnResponse`]'s latency/status log line.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordStatus;

impl<B> OnResponse<B> for RecordStatus {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
        // OTLP `http.response.status_code` is an integer; OTel attribute ints
        // are i64 and an HTTP status always fits.
        span.record(
            "http.response.status_code",
            response.status().as_u16() as i64,
        );
        DefaultOnResponse::new().on_response(response, latency, span);
    }
}

/// Wraps every request in a `tracing` span carrying the OTLP HTTP-server
/// semantic-convention attributes (so the APM renders it as an endpoint
/// trace), continuing any inbound W3C trace via [`ExtractingMakeSpan`] and
/// recording the response status via [`RecordStatus`].
#[allow(clippy::type_complexity)]
pub fn trace_layer() -> TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    ExtractingMakeSpan,
    DefaultOnRequest,
    RecordStatus,
> {
    TraceLayer::new_for_http()
        .make_span_with(ExtractingMakeSpan)
        .on_request(DefaultOnRequest::new())
        .on_response(RecordStatus)
}

/// Outer per-request HTTP budget (in seconds) for the timed router.
///
/// This is the canonical knob for the "how long may a single non-streaming
/// request take" question. It is exported as a plain `u64` (not just the
/// [`Duration`] below) so the inner per-RPC tonic deadline in
/// [`crate::envd_client`] can derive itself from this number in a `const`
/// context and the invariant *inner per-RPC deadline < outer HTTP budget*
/// holds by construction (issue #353).
///
/// 60s (was 30s): the first unary request after a *resume* races the resumed
/// envd becoming serve-ready on a fresh Pod. A large (GiB-scale) restore can
/// push that window toward 30s, so a file download issued right after
/// `connect` would hit the old ceiling even though the sandbox is healthy.
/// 60s gives restore + serve-readiness headroom while still bounding genuinely
/// stuck unary calls.
pub const HTTP_REQUEST_TIMEOUT_SECS: u64 = 60;

/// [`HTTP_REQUEST_TIMEOUT_SECS`] as a [`Duration`], for call-sites that want
/// the typed value directly (e.g. [`timeout_layer`]).
pub const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(HTTP_REQUEST_TIMEOUT_SECS);

/// Global per-request timeout. Long-running streams (exec WS, file
/// upload/download) are exempt — they nest their own per-route layers
/// that override this outer bound.
///
/// Pass [`HTTP_REQUEST_TIMEOUT`] for the canonical budget; the parameter is
/// kept explicit so tests and any future per-router override can dial a
/// different bound without reaching for a second factory.
pub fn timeout_layer(timeout: Duration) -> TimeoutLayer {
    TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, timeout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt; // brings `oneshot` into scope

    /// A request routed through `trace_layer()` reaches its handler and the
    /// response is returned unaltered — exercising the matched-route path
    /// (so `MatchedPath`/`http.route` is populated in `make_span`) plus the
    /// `RecordStatus` on-response hook, end to end, without panicking.
    #[tokio::test]
    async fn trace_layer_passes_matched_request_through() {
        let app = Router::new()
            .route("/h/:id", get(|| async { "ok" }))
            .layer(trace_layer());

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

    /// `RecordStatus` runs on error responses too: a 500 flows through the
    /// layer unchanged (the status it records is whatever the handler set).
    #[tokio::test]
    async fn trace_layer_preserves_error_status() {
        let app = Router::new()
            .route("/boom", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
            .layer(trace_layer());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/boom")
                    .body(Body::empty())
                    .expect("building a test request"),
            )
            .await
            .expect("router responds");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
