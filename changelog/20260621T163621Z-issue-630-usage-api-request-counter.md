# Per-API-key usage metering — Phase 1: API request anti-abuse counter

**Date:** 2026-06-21T16:36:21Z
**Issue:** #630 (epic #480, Phase 1)
**Crate:** `sandbawks-api`

## Summary

Adds the first slice of per-team usage metering: a dedicated OpenTelemetry counter
that records every authenticated API request, attributed to the owning team with a
per-`TeamAPIKey` breakdown. This is an **anti-abuse / rate-limit signal only** — it
is explicitly not the billing source of record (per epic #480's trust-model
decision, OTel is not authoritative for billing; the durable, resource-weighted
billing signal comes from the controller in Phases 2/3, #631/#632).

## Changes

- **New module `crates/sandbawks-api/src/usage_metrics.rs`.** Defines the
  `sandbawks-api.usage` meter (deliberately separate from the cardinality-bounded
  operational `sandbawks-api.http.server` meter in `http_metrics.rs`, so adding
  `team` cardinality never pollutes ops dashboards) and the instrument
  `usage.api.requests.count` (`Counter<u64>`, attributes `{ team, key_name }`).
  Follows the established `OnceLock` singleton + `global::meter(METER_NAME)` pattern
  used by `warm_claim_metrics.rs` / `http_metrics.rs` / `kube_op_metrics.rs`. The
  module doc records the cardinality budget.
- **`record_request(team, key_name)`** public fn, called once per request from
  `auth_middleware` (`auth.rs`) right after the key resolves — the single chokepoint
  every authenticated request flows through with a team identity in scope. This
  mirrors the repo's own pattern of a metrics-module `record_*` fn driven from the
  domain path (`warm_claim_metrics::record_claim` ← `warm_claim.rs`), and avoids
  repeating a metrics layer across all four authenticated routers (sandboxes,
  events, files, exec). `team` is the grouping/billing key; `key_name` is a
  breakdown dimension only.
- **`lib.rs`** registers `pub mod usage_metrics;`.

## Design notes

- **Why record in `auth_middleware`, not a global outer layer.** `TeamContext` is
  inserted into the request extensions by the inner per-router auth layer; a global
  outer layer moves the request into `next.run(...)` before it could read those
  extensions back, so it cannot see the team. Recording at the auth chokepoint is
  both correct and DRY.
- **Counts authenticated requests regardless of handler status.** Recording happens
  before the handler runs, so a team hammering endpoints that later 4xx/5xx is still
  counted — the right denominator for an abuse signal. Unauthenticated routes
  (envd-proxy, `/healthz`, `/readyz`) carry no team and are correctly excluded.

## Validation

- `just fmt` — formatted.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — workspace tests pass, including:
  - `usage_metrics` unit test: recorder is no-op-meter safe.
  - `tests/usage_metrics.rs` integration test: live `InMemoryMetricExporter` proves
    `usage.api.requests.count` emits under scope `sandbawks-api.usage` with the
    expected `{ team, key_name }` values; two authed requests count 2, an
    unauthenticated request adds nothing.

## Out of scope (later phases of #480)

- Authoritative resource-weighted sandbox-seconds from the controller (#631).
- Durable hourly NDJSON usage ledger (#632).
- Rate-limit *enforcement* — this counter is a signal only.
