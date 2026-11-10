# Fix: Vector hop dropped all traces/logs (and metrics) at the OTLP sink

**Date:** 2026-06-13T21:19:15Z
**Area:** `deploy/helm/sandbawks/templates/otel-collector-configmap.yaml`
**Relates to:** #496 (Vector telemetry hop), regression from PR #499

## Symptom

After PR #499 swapped the in-namespace telemetry hop from the OTel Collector to
Vector, no telemetry reached Traceway and the Prometheus port showed nothing.
Vector logged, per sink, on every flush:

```
ERROR sink{component_id=traceway_traces}: Trace event does not contain OTLP top-level field (resourceSpans)
ERROR sink{component_id=traceway_logs}:   Log event does not contain OTLP top-level fields (resourceLogs or resourceMetrics)
ERROR sink: component_events_dropped intentional=false reason="Failed serializing frame."
```

## Root cause

A source/sink shape mismatch inside Vector, not a Traceway or auth problem.

- The `opentelemetry` **source** defaults to `use_otlp_decoding: false`, which
  *flattens* incoming OTLP into Vector's native event model: resource attributes
  land under `.resources` (logs/traces) or `resource.`-prefixed `.tags`
  (metrics), and there is **no** top-level `resourceSpans` / `resourceLogs` /
  `resourceMetrics` envelope.
- The `opentelemetry` **sink** with `encoding.codec: otlp` requires exactly that
  envelope. With none present, every event failed to serialize and was dropped
  before export.

PR #499's topology (`source -> remap stamp -> otlp sink`) therefore dropped
100% of traces and logs. The `prometheus_exporter` is a separate sink and was
not implicated by these errors, but the metric path had its own latent bug
(below).

Verified against the pinned image (`vector 0.56.0`, rev `6817c02`) by replaying
real OTLP protobuf from `telemetrygen` and dumping the emitted event shapes in
both decoding modes.

## Fix

Use Vector's intended pairing: the source preserves the raw OTLP envelope so the
otlp-codec sink can re-serialize it losslessly.

1. **Source — per-signal `use_otlp_decoding`:**
   - `logs: true`, `traces: true` -> raw OTLP envelopes forwarded to Traceway
     losslessly.
   - `metrics: false` -> kept as native Vector metric events so the
     `prometheus_exporter` sink can re-expose them.

2. **Tenant stamp rewritten for the new shapes.** The anti-spoofing re-stamp of
   `service.namespace` / `sandbawks.tenant` from the collector Pod's own
   namespace now operates on:
   - logs/traces: the OTLP envelope's `.resource{Logs,Spans}[].resource.attributes[]`
     KeyValue array (drop the customer's entries, append ours);
   - metrics: the native `resource.`-prefixed tags.

3. **Metrics "Both" path (chosen trade-off).** Metrics must feed two consumers
   that need different event shapes — `prometheus_exporter` needs native metric
   events, the otlp-codec Traceway sink needs an OTLP envelope — and a single
   source emits only one shape per signal. So the Traceway metric leg adds a
   `metric_to_log` transform plus a `build_otlp_metrics` remap that hand-assembles
   the OTLP `resourceMetrics` envelope (gauge -> Gauge, counter -> cumulative
   monotonic Sum, aggregated_histogram -> Histogram with the implicit `+inf`
   bucket folded into `bucketCounts`). The native stream still feeds Prometheus
   directly.

### Pre-existing bug fixed in passing

The old `stamp_metrics` wrote `.tags."service.namespace"`, but the source
exposes the metric resource attribute as `.tags."resource.service.namespace"`.
The stamp was therefore adding a new tag while leaving the customer-supplied
`resource.service.namespace` untouched — the **metrics tenant boundary was
open**. The new `stamp_metrics_native` overwrites the correct key.

## Known lossiness (accepted)

The native metric decode discards OTLP temporality/monotonicity, so the
hand-built Traceway envelope re-emits every counter as a **cumulative monotonic
Sum**. Up-down counters (e.g. `http.server.active_requests`) are therefore
mislabeled monotonic on the Traceway side. Start time is unknown post-decode, so
`startTimeUnixNano == timeUnixNano`. The `prometheus_exporter` path is unaffected
and remains the lossless metric surface — prefer it for metric correctness. This
is the cost of serving both metric consumers from one Vector source; tracked as
the trade-off for #496's "Both" metrics decision.

## Verification (local, `vector 0.56.0-debian`)

Two-Vector round-trip: the rendered chart config as sender, a second Vector with
an `opentelemetry` source (full passthrough) as a stand-in Traceway intake.
`telemetrygen` fed traces, logs, and gauge/sum/histogram metrics with a spoofed
`service.namespace="EVIL-TENANT"`.

- `vector validate` passes on the rendered config (all four sinks healthcheck).
- Sender: **0** `does not contain OTLP top-level field` / `Failed serializing`
  errors (was every flush).
- Receiver decoded valid OTLP for all three signals.
- Tenant boundary: **0** `EVIL-TENANT` occurrences received; resource attribute
  re-stamped to the collector namespace on traces, logs, **and** metrics.
- Hand-built metrics round-tripped correctly: Gauge value preserved; Sum
  `aggregationTemporality=CUMULATIVE, isMonotonic=true`; Histogram `count=10`,
  `sum=3940`, `bucketCounts(16) == explicitBounds(15) + 1` (OTLP `+inf`
  invariant holds).
- `:9090/metrics` exposed `gen`, `gen_bucket`, `gen_sum`, `gen_count` with the
  stamped tenant labels.

## Files changed

- `deploy/helm/sandbawks/templates/otel-collector-configmap.yaml` — source
  `use_otlp_decoding`, rewritten stamps, `metrics_to_log` + `build_otlp_metrics`,
  re-wired `traceway_metrics` / `prometheus` sink inputs, header doc.
- `deploy/helm/sandbawks/Chart.yaml` — `version` 0.1.8 -> 0.1.9.
- `docs/observability.md` — corrected the Prometheus label-name note
  (`resource.`-prefixed, dot handling is Prometheus-version dependent).

## Not validated without a cluster

Real arrival to the production Traceway intake and a live Prometheus scrape of
`:9090` — the receiver here is a stand-in Vector OTLP source. The OTLP envelopes
it accepts are byte-compatible with what Traceway expects, but confirm
end-to-end after deploy.
