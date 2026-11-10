# OTel Phase 5 — control-plane trace propagation via CRD annotations

**Date:** 2026-06-03T19:40:52Z
**Issue:** #299 (parent epic #294)
**Branch:** `feat-issue-299-crd-trace-propagation`

## Summary

Extends the single `trace_id` across the Kubernetes control-plane hops
(api → controller → node) that W3C header propagation (Phase 4) cannot reach,
because the producer and consumer of those hops are decoupled by the apiserver
and a reconcile loop rather than a live request/response. The context now rides
in the custom resource itself, under a `sandbawks.io/traceparent` annotation.

Result: a sandbox created through the api yields one `trace_id` end-to-end —
api handler span → controller reconcile span → node reconcile span → downstream
envd spans (the last leg via Phase 4's `/__state` probe header injection).

## Changes

### `sandbawks-observability` (new propagation API)

`crates/sandbawks-observability/src/propagation.rs`:

- `TRACEPARENT_ANNOTATION` (`sandbawks.io/traceparent`) and
  `TRACESTATE_ANNOTATION` (`sandbawks.io/tracestate`) constants.
- `inject_into_annotations(&mut BTreeMap<String, String>)` — producer side;
  serialises the active span's context into the annotation map via the global
  `TraceContextPropagator`. No-op outside any span.
- `continue_from_annotations(Option<&BTreeMap<String, String>>)` — consumer
  side; adopts the stamped `traceparent` as the current span's parent. Absent
  annotation leaves the span a fresh trace root.
- Pure inner helpers `annotations_from_carrier` / `carrier_from_annotations`
  translate between the W3C header names the propagator uses and the namespaced
  annotation keys, so the mapping is unit-testable without a live subscriber.
- Re-exported from the crate root (`lib.rs`).
- Four new unit tests covering the key translation, the empty-carrier no-op, the
  annotation round-trip into a parseable context, and the absent-`traceparent`
  empty-context case.

### Producers (inject)

- `sandbawks-api` `routes::sandboxes::create_sandbox` — stamps the annotation on
  the cold-create `Sandbox` before `create`.
- `sandbawks-api` `warm_claim::apply_overrides_and_resume` — adds the annotation
  to the warm-claim resume merge patch, refreshing whatever traceparent the warm
  member carried from its speculative top-up so the reconcile follows *this*
  claim's trace.
- `sandbawks-controller` `reconcile::build_snapshot_job` — stamps the
  `SnapshotJob` so the per-node worker continues the controller's trace across
  the SnapshotJob "queue".
- `sandbawks-controller` `warm_pool::reconcile_pool` — stamps each warm member
  `Sandbox` at create time.

### Consumers (continue + root span)

Each reconcile entry gained a `#[tracing::instrument]` root span that adopts the
annotation's `traceparent` as its parent:

- `sandbawks-controller` `reconcile::reconcile` → span `reconcile:controller::sandbox`
- `sandbawks-controller` `warm_pool::reconcile_pool` → span `reconcile:controller::warm_pool`
- `sandbawks-node` `reconcile::reconcile` → span `reconcile:node::snapshot_job`

### Docs

- `OBSERVABILITY.md` §6.4 added (CR-annotation propagation), and the §6.3
  job-queue gap note now points to it.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace green, including the 4 new propagation tests.

The issue's end-to-end acceptance criterion (a live trace whose `trace_id` is
shared across api/controller/node/envd spans) is a cluster + OTLP-collector
check that cannot run in this environment; it is covered by the per-hop unit
tests plus the documented wiring above. Graceful degradation — a reconcile of an
annotation-free CR (operator-edited) opening a clean root span — is exercised by
`carrier_from_annotations_without_traceparent_is_empty_context` and the no-op
paths in every helper.

## Design notes

- The context is stamped only when a span is active and tracing is enabled; with
  `OTEL_SDK_DISABLED` (the local default) no annotation is written and reconciles
  cleanly root fresh traces, so `cargo run` against kind needs no collector.
- Parent (not span-link) semantics per the issue: the annotation persists on the
  CR, so steady-state heartbeat reconciles continue the last writer's trace until
  the next api create/update refreshes it.
