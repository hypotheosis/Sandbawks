# api: emit OTLP HTTP semantic-convention attributes on request spans

**Branch:** `fix-api-traces-http-semconv`

## Problem

After the prod OTLP auth token was corrected, logs and metrics flowed to the
traceway APM but **traces never appeared** — even though traceway accepted the
exports (HTTP 200, no rejection).

Root cause, confirmed empirically against the live traceway by POSTing
hand-crafted OTLP payloads: traceway groups endpoint traces by the OTLP
HTTP semantic-convention attributes `http.request.method` + `http.route`. The
api's per-request span (`ExtractingMakeSpan` in `crates/sandbawks-api/src/middleware.rs`)
tagged the span with **ad-hoc keys** `method` / `uri` instead, so traceway
ingested the spans but could not render them as endpoints. Metrics worked
because `http_metrics` already records the correct semconv keys — the exact
asymmetry observed (metrics ✓, traces ✗).

A/B proof on the live cluster: a span with `http.request.method` + `http.route`
+ `url.path` + `http.response.status_code` and `otel.kind=server` showed up in
the endpoints list; an otherwise-identical span using `method`/`uri` did not.

## Change (`crates/sandbawks-api`)

- **`middleware.rs` `ExtractingMakeSpan`**: emit the OTLP HTTP-server semconv
  attributes on the request span — `http.request.method`, `http.route`
  (resolved from axum `MatchedPath`, same as the metrics), `url.path` — and set
  the span name to the `"{method} {route}"` HTTP-server convention via
  `otel.name`. Kept `otel.kind = "server"` and the inbound W3C trace
  continuation (issue #298). Dropped the old `method`/`uri` fields.
- **`middleware.rs` `RecordStatus`**: a new `OnResponse` hook records
  `http.response.status_code` onto the span (declared `Empty` at creation,
  since the status is only known post-handler), then defers to
  `DefaultOnResponse` so the existing latency/status log line is preserved.
  Wired into `trace_layer()` in place of `DefaultOnResponse`.
- **`http_metrics.rs`**: `route_label` is now generic over the body type and
  `pub(crate)`, so the span and the metrics derive `http.route` from one shared
  helper (DRY) — they cannot drift.

No behavior change for logs/metrics; this only reshapes the api's request span.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- New tests in `middleware.rs`: a request routed through `trace_layer()` reaches
  its handler unaltered (matched-route path, so `http.route`/`MatchedPath` is
  populated, plus the `RecordStatus` hook), and an error (500) response flows
  through unchanged. Existing `http_metrics` `route_label` test still passes
  against the now-generic signature.
- Empirically proven against the live traceway (the A/B above); the field names
  are literals checked at compile time by the `tracing` macro.

## Notes

- Verifying recorded span *attribute values* in a unit test would need a
  capturing `tracing` subscriber (not a current dev-dependency); the end-to-end
  pass-through tests plus the live A/B cover the behavior without adding a dep.
- Follow-up to the telemetry-export work (#369): that issue was the wrong
  Helm-injected OTLP bearer token (fixed by redeploy) + log-spam/Secret
  hardening; this is the distinct traces-not-rendered cause.
