# Fix: Vector hop emitted Prometheus metric/label names containing `.`

**Date:** 2026-06-17T11:47:35Z
**Area:** `deploy/helm/sandbawks/templates/otel-collector-configmap.yaml`
**Relates to:** #516 (comment) — follow-up to the #496 / PR #515 metrics work;
guard added in PR #570

## Symptom

The Vector hop's `prometheus_exporter` surface (`:9090/metrics`) emitted label
names — and, for any dotted OTLP metric name, metric names — containing `.`:

```
gen{resource.service.namespace="sandbawks-prod",resource.sandbawks.tenant="sandbawks-prod",...} 4
```

Prometheus naming rules require metric names to match
`[a-zA-Z_:][a-zA-Z0-9_:]*` and label names `[a-zA-Z_][a-zA-Z0-9_]*` — `.` is
forbidden. Classic scrapers silently rewrite the dots to `_` on ingest, but the
raw exposition is non-conformant, and UTF-8-enabled scrapers (Prometheus 3.x)
keep the dots, so the label name a query must use diverges by scraper.

## Root cause

The `opentelemetry` source flattens OTLP resource attributes onto native metric
tags with a `resource.` prefix (`resource.service.namespace`, …), and OTLP
metric names are dotted by convention. That native stream
(`stamp_metrics_native`) fed `prometheus_exporter` directly, so the dotted
names reached the exposition verbatim.

## Fix

A new prometheus-only transform `sanitize_prometheus_names` maps `.` -> `_` in
the metric name and every tag key, and the `prometheus` sink now reads from it:

```yaml
sanitize_prometheus_names:
  type: remap
  inputs: ["stamp_metrics_native"]
  source: |
    .name = replace(string!(.name), ".", "_")
    .tags = map_keys(object(.tags) ?? {}) -> |key| { replace(key, ".", "_") }
```

The tenant stamp therefore lands as `resource_service_namespace` /
`resource_sandbawks_tenant` on every scraper. The transform is on the
**prometheus leg only** — the Traceway OTLP leg (`metrics_to_log` ->
`build_otlp_metrics`) still reads `stamp_metrics_native` with the dotted keys,
which are valid OTLP attribute names and must be preserved.

## Regression guard

`scripts/vector-hop-roundtrip-smoke.sh` (the #516 / PR #570 harness) gains
**Assertion 6**: after scraping `:9090/metrics` it strips comments and quoted
label values, then fails if any `.` remains in a metric or label name. Verified:

- Harness passes against the fixed chart (no dotted names in the exposition).
- **Negative control**: pointing the prometheus sink back at
  `stamp_metrics_native` makes Assertion 6 fail on `resource.service.namespace=`.

`map_keys`/`replace` confirmed available in the pinned Vector `0.56.0`.

## Files changed

- `deploy/helm/sandbawks/templates/otel-collector-configmap.yaml` — add
  `sanitize_prometheus_names`, repoint the `prometheus` sink, header/inline docs.
- `deploy/helm/sandbawks/Chart.yaml` — `version` 0.1.16 -> 0.1.17.
- `scripts/vector-hop-roundtrip-smoke.sh` — Assertion 6 (no dotted prom names).
- `docs/observability.md` — note now reflects underscore-sanitized labels on
  every scraper instead of scraper-dependent dot handling.
