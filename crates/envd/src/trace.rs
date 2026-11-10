//! Server-side gRPC trace-context propagation.
//!
//! envd is the *downstream* end of the api -> envd gRPC hop (issue #298). The
//! api gateway injects a W3C `traceparent` into each request's metadata
//! (`EnvdPool`'s interceptor); without a matching extractor here every RPC
//! would open a *fresh* trace, severing the api span from the work envd does on
//! its behalf.
//!
//! [`GrpcTraceLayer`] is that extractor, installed once on the tonic server
//! (`Server::builder().layer(GrpcTraceLayer)`). For every inbound request it
//! creates a transport-level span whose parent is the caller's context, then
//! runs the rest of the stack — the generated tonic router and the
//! `#[tracing::instrument]` handler span it dispatches to — *inside* that span.
//! Because the handler span is created while the transport span is current, it
//! nests under it and the whole subtree shares the api's `trace_id`.
//!
//! Doing this as one tower layer (rather than a line at the top of all ~18 RPC
//! handlers) keeps the extraction DRY and impossible to forget on a new RPC.
//! The per-handler `#[instrument]` attributes still matter — they give each
//! method a stable, readable span name; this layer only supplies the *parent*.

use std::task::{Context, Poll};

use tower::{Layer, Service};
use tracing::Instrument;

/// A [`tower::Layer`] that parents every inbound gRPC request to the caller's
/// W3C trace context. Install it on the tonic server with
/// `Server::builder().layer(GrpcTraceLayer)`.
#[derive(Clone, Copy, Default, Debug)]
pub struct GrpcTraceLayer;

impl<S> Layer<S> for GrpcTraceLayer {
    type Service = GrpcTrace<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcTrace { inner }
    }
}

/// The [`Service`] produced by [`GrpcTraceLayer`]. Wraps the inner gRPC service,
/// adopting the inbound `traceparent` as the parent of a per-request span.
#[derive(Clone, Debug)]
pub struct GrpcTrace<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for GrpcTrace<S>
where
    // The bounds tonic's `serve` already requires of the layered service: it is
    // cloned per connection and driven on the server task, so `Clone + Send +
    // 'static` with a `Send + 'static` future.
    S: Service<http::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    // The inner future, wrapped so it is polled with the trace span entered for
    // its entire lifetime. Nameable (no `BoxFuture`) so there is zero per-call
    // heap allocation on this hot path.
    type Future = tracing::instrument::Instrumented<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        // The gRPC method path (`/package.Service/Method`) is low-cardinality
        // and bounded — useful as a span *field*. The span *name* stays static
        // (`grpc.request`); the per-method name is supplied by the handler's own
        // `#[instrument]` span, which nests under this one.
        let span = tracing::info_span!(
            "grpc.request",
            otel.kind = "server",
            rpc.path = %request.uri().path(),
        );
        sandbawks_observability::continue_span(
            &span,
            &sandbawks_observability::HttpHeaderExtractor(request.headers()),
        );

        // Standard tower readiness dance: the slot reserved by `poll_ready`
        // lives on the *current* `inner`, so swap a fresh clone into `self`
        // (the one polled to readiness next) and `call` the reserved original.
        // Skipping this risks calling a service that was never `poll_ready`-ed.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        inner.call(request).instrument(span)
    }
}
