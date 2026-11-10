//! Cross-service W3C trace-context propagation helpers.
//!
//! [`init_telemetry`](crate::init_telemetry) registers a
//! [`TraceContextPropagator`](opentelemetry_sdk::propagation::TraceContextPropagator)
//! as the global text-map propagator. This module turns that registration into
//! a small, carrier-agnostic API the binaries use to *stitch one `trace_id`
//! across a service hop* (OBSERVABILITY.md §6.3):
//!
//! - **Clients** call [`inject_current`] just before an outbound request,
//!   serialising the active span's context into the request's headers /
//!   metadata as a W3C `traceparent`.
//! - **Servers** call [`continue_current`] (or [`continue_span`]) at the top of
//!   a handler, adopting the inbound `traceparent` as the handler span's parent
//!   so the downstream span tree joins the caller's trace instead of starting a
//!   fresh, disconnected one.
//!
//! # Carriers
//!
//! The propagator reads and writes through the [`Injector`] / [`Extractor`]
//! traits (re-exported here). This crate ships adapters for the two carrier
//! shapes the sandbawks binaries actually use:
//!
//! - [`HttpHeaderInjector`] / [`HttpHeaderExtractor`] over [`http::HeaderMap`]
//!   — axum and tonic both expose request headers as this exact type.
//! - [`MapCarrier`], a plain string map, for code that builds a request **by
//!   hand** and cannot hand us a `HeaderMap` (the node's raw-TCP `/__state`
//!   probe writes its HTTP request a byte at a time).
//!
//! A carrier the crate does not cover — tonic's `MetadataMap`, say — is a
//! handful of lines to adapt at the call site by implementing [`Injector`] /
//! [`Extractor`]; both traits are re-exported for exactly that.
//!
//! # No-op when there is nothing to propagate
//!
//! [`inject_current`] reads `tracing::Span::current().context()`. Outside any
//! span — or before `init_telemetry` runs — that context is empty and the
//! propagator writes nothing, so an un-instrumented call path degrades to "no
//! `traceparent` header" rather than a malformed one. Symmetrically,
//! [`extract`] of an absent / unparseable header yields an empty context, and
//! setting a span's parent to an empty context simply leaves it a trace root.

use std::collections::{BTreeMap, HashMap};

use opentelemetry::global;
use opentelemetry::Context;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

// Re-exported so a call site with an exotic carrier (e.g. tonic `MetadataMap`)
// can implement the traits without taking a direct `opentelemetry` dependency.
pub use opentelemetry::propagation::{Extractor, Injector};

/// Annotation key carrying the W3C `traceparent` across a Kubernetes
/// custom-resource hop — the api → controller → node leg that HTTP/gRPC
/// header propagation cannot reach because the producer and consumer are
/// decoupled by the apiserver and a reconcile loop (OBSERVABILITY.md §6.3).
///
/// A producer stamps it with [`inject_into_annotations`] when it writes a CR;
/// the consuming reconcile adopts it with [`continue_from_annotations`].
pub const TRACEPARENT_ANNOTATION: &str = "sandbawks.dev/traceparent";

/// Companion key for the W3C `tracestate` (vendor trace state). Stamped only
/// when the active context actually carries one, so the common single-vendor
/// setup writes [`TRACEPARENT_ANNOTATION`] alone.
pub const TRACESTATE_ANNOTATION: &str = "sandbawks.dev/tracestate";

/// Mapping between the W3C header names the global propagator reads/writes and
/// the namespaced annotation keys we persist them under on a CR.
///
/// The propagator only ever probes for these two keys, so an entry not present
/// in a given context is simply absent from both carrier and annotations.
const W3C_ANNOTATION_KEYS: [(&str, &str); 2] = [
    ("traceparent", TRACEPARENT_ANNOTATION),
    ("tracestate", TRACESTATE_ANNOTATION),
];

/// Inject the **active span's** trace context into `carrier` as a W3C
/// `traceparent` (plus `tracestate` when present), via the globally installed
/// propagator.
///
/// Call this immediately before dispatching an outbound request. When invoked
/// outside any span the current context is empty and nothing is written — see
/// the [module docs](self).
pub fn inject_current(carrier: &mut dyn Injector) {
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, carrier);
    });
}

/// Extract a parent trace [`Context`] from `carrier`.
///
/// Returns an empty context (no active span) when the carrier holds no valid
/// `traceparent`, so the result is always safe to hand to [`continue_span`].
pub fn extract(carrier: &dyn Extractor) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(carrier))
}

/// Adopt the `traceparent` in `carrier` as `span`'s parent.
///
/// The inverse of [`inject_current`]: a server uses it so its per-request span
/// continues the caller's trace. Setting the parent to an empty context (no
/// inbound header) is a no-op that leaves `span` a trace root.
pub fn continue_span(span: &Span, carrier: &dyn Extractor) {
    // `set_parent` errors only when the span has no OpenTelemetry layer behind
    // it (e.g. `OTEL_SDK_DISABLED`, so fmt-only) or was already started — both
    // benign here: the span simply stays a trace root. Log at `debug` so the
    // degradation is diagnosable without spamming the steady state.
    if let Err(error) = span.set_parent(extract(carrier)) {
        tracing::debug!(%error, "could not adopt inbound trace context as span parent");
    }
}

/// Adopt the `traceparent` in `carrier` as the **current** span's parent.
///
/// Convenience for the common handler shape where the span is created by
/// `#[tracing::instrument]` and is therefore already current — call this on the
/// first line of the body. Equivalent to
/// `continue_span(&Span::current(), carrier)`.
pub fn continue_current(carrier: &dyn Extractor) {
    continue_span(&Span::current(), carrier);
}

/// Stamp the **active span's** trace context into a CR's annotation map.
///
/// The control-plane analogue of [`inject_current`]: where that serialises the
/// context into an outbound request's headers, this serialises it into the
/// `metadata.annotations` of a `Sandbox` / `SnapshotJob` / `SandboxWarmPool`
/// the producer is about to write, under [`TRACEPARENT_ANNOTATION`] (plus
/// [`TRACESTATE_ANNOTATION`] when the context carries vendor state). A later
/// reconcile that reads the resource can then [`continue_from_annotations`] to
/// join this trace instead of starting a disconnected one.
///
/// A no-op outside any span: the current context is empty, the propagator
/// writes nothing, and `annotations` is left untouched — so an
/// operator-created CR carries no `traceparent` and its reconcile cleanly
/// roots a fresh trace.
pub fn inject_into_annotations(annotations: &mut BTreeMap<String, String>) {
    let mut carrier = MapCarrier::new();
    inject_current(&mut carrier);
    annotations_from_carrier(&carrier, annotations);
}

/// Adopt the `traceparent` recorded in a CR's annotations as the **current**
/// span's parent.
///
/// The read-side inverse of [`inject_into_annotations`]: call it at the top of
/// a reconcile (under a `#[tracing::instrument]` root span) so the reconcile
/// joins the trace of whoever produced the resource. `annotations` is
/// `metadata.annotations` straight off the object — `None` (no annotations at
/// all) and a map lacking [`TRACEPARENT_ANNOTATION`] both leave the span a
/// fresh trace root.
pub fn continue_from_annotations(annotations: Option<&BTreeMap<String, String>>) {
    let Some(annotations) = annotations else {
        return;
    };
    continue_current(&carrier_from_annotations(annotations));
}

/// Copy the W3C keys the propagator wrote into `carrier` out to `annotations`,
/// translating each header name to its namespaced annotation key.
///
/// Pure (no globals) so the header ↔ annotation mapping is unit-testable
/// without standing up a tracer subscriber.
fn annotations_from_carrier(carrier: &MapCarrier, annotations: &mut BTreeMap<String, String>) {
    for (w3c_key, annotation_key) in W3C_ANNOTATION_KEYS {
        if let Some(value) = carrier.get(w3c_key) {
            annotations.insert(annotation_key.to_owned(), value.to_owned());
        }
    }
}

/// Build a W3C carrier from a CR's annotations, translating each namespaced
/// annotation key back to the header name the propagator expects.
///
/// Pure counterpart to [`annotations_from_carrier`]; the resulting carrier is
/// safe to hand to [`extract`] / [`continue_current`].
fn carrier_from_annotations(annotations: &BTreeMap<String, String>) -> MapCarrier {
    let mut carrier = MapCarrier::new();
    for (w3c_key, annotation_key) in W3C_ANNOTATION_KEYS {
        if let Some(value) = annotations.get(annotation_key) {
            carrier.set(w3c_key, value.clone());
        }
    }
    carrier
}

/// [`Injector`] / [`Extractor`] over an [`http::HeaderMap`] borrowed mutably,
/// for serialising context into an outbound request's headers.
///
/// Construct it around the header map of the request being built, e.g.
/// `HttpHeaderInjector(req.headers_mut())`.
pub struct HttpHeaderInjector<'a>(pub &'a mut http::HeaderMap);

impl Injector for HttpHeaderInjector<'_> {
    /// Set `key: value`, silently skipping a pair that is not a valid HTTP
    /// header (the W3C keys `traceparent` / `tracestate` always are, so this
    /// only guards against a future non-conforming propagator).
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            http::header::HeaderName::from_bytes(key.as_bytes()),
            http::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, value);
        }
    }
}

/// [`Extractor`] over an [`http::HeaderMap`] borrowed immutably, for reading an
/// inbound request's `traceparent`.
///
/// Construct it around the inbound headers, e.g.
/// `HttpHeaderExtractor(req.headers())`.
pub struct HttpHeaderExtractor<'a>(pub &'a http::HeaderMap);

impl Extractor for HttpHeaderExtractor<'_> {
    /// A header's value as `&str`, or `None` if absent or non-ASCII.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    /// All header names present, so the propagator can probe for the keys it
    /// understands.
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

/// A plain `String → String` carrier for code that cannot offer a
/// [`http::HeaderMap`].
///
/// The node's `/__state` probe writes a raw HTTP/1.1 request over a bare
/// `TcpStream` (it needs `SO_LINGER(0)`, which `reqwest`/`hyper` don't expose);
/// it injects into a [`MapCarrier`] and then appends each entry to the request
/// it is hand-assembling. Also the convenient carrier for unit tests.
#[derive(Debug, Default, Clone)]
pub struct MapCarrier {
    entries: HashMap<String, String>,
}

impl MapCarrier {
    /// An empty carrier.
    pub fn new() -> Self {
        Self::default()
    }

    /// Iterate the `(name, value)` pairs the propagator wrote, e.g. to splice
    /// them into a hand-built request.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// `true` when the propagator wrote nothing (no active context to inject).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Injector for MapCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.entries.insert(key.to_owned(), value);
    }
}

impl Extractor for MapCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.entries.keys().map(String::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use serial_test::serial;

    /// A spec-example W3C `traceparent` (W3C Trace Context §3.2.1) — sampled
    /// (`-01`), with known trace and span IDs we can assert against.
    const SAMPLE_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const SAMPLE_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const SAMPLE_SPAN_ID: &str = "00f067aa0ba902b7";

    /// Ensure the global propagator is the W3C one. Idempotent and cheap;
    /// `init_telemetry` does the same in production. Guarded by `#[serial]`
    /// callers because it mutates process-global state.
    fn install_w3c_propagator() {
        global::set_text_map_propagator(TraceContextPropagator::new());
    }

    #[test]
    fn map_carrier_round_trips_set_get_keys() {
        // Arrange / Act
        let mut carrier = MapCarrier::new();
        carrier.set("traceparent", SAMPLE_TRACEPARENT.to_owned());

        // Assert: the three `Injector`/`Extractor` surfaces agree.
        assert_eq!(carrier.get("traceparent"), Some(SAMPLE_TRACEPARENT));
        assert_eq!(carrier.get("absent"), None);
        assert_eq!(carrier.keys(), vec!["traceparent"]);
        assert!(!carrier.is_empty());
    }

    #[test]
    #[serial]
    fn extract_parses_w3c_traceparent_into_context() {
        // Arrange: the global W3C propagator and a carrier holding the header.
        install_w3c_propagator();
        let mut carrier = MapCarrier::new();
        carrier.set("traceparent", SAMPLE_TRACEPARENT.to_owned());

        // Act
        let context = extract(&carrier);

        // Assert: the extracted remote span carries the spec's IDs.
        let span_context = context.span().span_context().clone();
        assert!(
            span_context.is_remote(),
            "extracted context is a remote parent"
        );
        assert_eq!(span_context.trace_id().to_string(), SAMPLE_TRACE_ID);
        assert_eq!(span_context.span_id().to_string(), SAMPLE_SPAN_ID);
    }

    #[test]
    #[serial]
    fn extract_of_missing_header_is_empty_context() {
        // Arrange: propagator installed, but the carrier is empty.
        install_w3c_propagator();
        let carrier = MapCarrier::new();

        // Act / Assert: no header => no active span, safe to use as a parent.
        let context = extract(&carrier);
        assert!(!context.span().span_context().is_valid());
    }

    #[test]
    fn annotations_from_carrier_namespaces_w3c_keys() {
        // Arrange: a carrier as `inject_current` would leave it.
        let mut carrier = MapCarrier::new();
        carrier.set("traceparent", SAMPLE_TRACEPARENT.to_owned());
        carrier.set("tracestate", "vendor=abc".to_owned());

        // Act
        let mut annotations = BTreeMap::new();
        annotations_from_carrier(&carrier, &mut annotations);

        // Assert: both W3C headers land under their `sandbawks.dev/` keys.
        assert_eq!(
            annotations.get(TRACEPARENT_ANNOTATION).map(String::as_str),
            Some(SAMPLE_TRACEPARENT)
        );
        assert_eq!(
            annotations.get(TRACESTATE_ANNOTATION).map(String::as_str),
            Some("vendor=abc")
        );
    }

    #[test]
    fn annotations_from_empty_carrier_writes_nothing() {
        // Arrange: an empty carrier models "no active span to inject".
        let carrier = MapCarrier::new();

        // Act: a pre-existing unrelated annotation must survive untouched.
        let mut annotations = BTreeMap::from([("app".to_owned(), "sandbawks".to_owned())]);
        annotations_from_carrier(&carrier, &mut annotations);

        // Assert: no trace keys added, existing entry preserved.
        assert!(!annotations.contains_key(TRACEPARENT_ANNOTATION));
        assert_eq!(
            annotations.get("app").map(String::as_str),
            Some("sandbawks")
        );
    }

    #[test]
    #[serial]
    fn annotations_round_trip_into_a_parseable_context() {
        // Arrange: stamp a known traceparent into annotations...
        install_w3c_propagator();
        let mut carrier = MapCarrier::new();
        carrier.set("traceparent", SAMPLE_TRACEPARENT.to_owned());
        let mut annotations = BTreeMap::new();
        annotations_from_carrier(&carrier, &mut annotations);

        // Act: ...then read them back out the way a reconcile would.
        let context = extract(&carrier_from_annotations(&annotations));

        // Assert: the spec's trace/span IDs survive the annotation round-trip.
        let span_context = context.span().span_context().clone();
        assert!(span_context.is_remote(), "carrier yields a remote parent");
        assert_eq!(span_context.trace_id().to_string(), SAMPLE_TRACE_ID);
        assert_eq!(span_context.span_id().to_string(), SAMPLE_SPAN_ID);
    }

    #[test]
    #[serial]
    fn carrier_from_annotations_without_traceparent_is_empty_context() {
        // Arrange: annotations carrying only unrelated `sandbawks.dev/` keys.
        install_w3c_propagator();
        let annotations =
            BTreeMap::from([("sandbawks.dev/pod-ready".to_owned(), "true".to_owned())]);

        // Act / Assert: no traceparent => empty context => fresh trace root.
        let context = extract(&carrier_from_annotations(&annotations));
        assert!(!context.span().span_context().is_valid());
    }

    #[test]
    #[serial]
    fn http_header_carrier_interops_with_map_carrier() {
        // Arrange: write a traceparent via the http injector...
        install_w3c_propagator();
        let mut headers = http::HeaderMap::new();
        HttpHeaderInjector(&mut headers).set("traceparent", SAMPLE_TRACEPARENT.to_owned());

        // Act: ...and read it back through the http extractor.
        let extractor = HttpHeaderExtractor(&headers);

        // Assert: the value survived the HeaderMap round-trip and parses.
        assert_eq!(extractor.get("traceparent"), Some(SAMPLE_TRACEPARENT));
        let span_context = extract(&extractor).span().span_context().clone();
        assert_eq!(span_context.trace_id().to_string(), SAMPLE_TRACE_ID);
    }
}
