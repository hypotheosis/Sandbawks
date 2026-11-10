# controller/node: render reconcile loops as APM "Task" traces

**Branch:** `fix-api-traces-http-semconv` (same PR as the api span fix — this
turns it into a global OTel span-shape cleanup).

## Problem

`sandbawks-controller` and `sandbawks-node` have no HTTP surface — they're kube
reconcilers driven by watch streams. Their `#[tracing::instrument]` reconcile
spans are `SpanKind=INTERNAL` with no semantic-convention attributes, so the
traceway APM ingests them (HTTP 200) but never renders them in any lane.

Empirically probed against the live traceway (hand-crafted OTLP POSTs) to map
what it surfaces:

| span shape | renders as |
|---|---|
| `SERVER` + `http.request.method` + `http.route` | **Endpoint** |
| `CONSUMER` + `messaging.*` | **Task** |
| `INTERNAL`, or `SERVER` without http attrs | nothing |

A reconcile loop is a consumer of watch events, so the **Task** lane
(`CONSUMER` + `messaging.*`) is the semantically-correct home — no HTTP faking.
A `CONSUMER` span renders as a Task even when it is a child of the kube-rs
`reconciling object` wrapper span (the APM labels it "(Non Root)"), so no change
to the OTel layer filter is needed.

## Change

Add `otel.kind = "consumer"` + OTLP `messaging.*` attributes to each top-level
reconcile span:

- `crates/sandbawks-controller/src/reconcile.rs` — `reconcile` (Sandbox),
  `messaging.destination.name = "Sandbox"`.
- `crates/sandbawks-controller/src/warm_pool.rs` — `reconcile_pool`
  (SandboxWarmPool), `messaging.destination.name = "SandboxWarmPool"`.
- `crates/sandbawks-node/src/reconcile.rs` — `reconcile` (SnapshotJob),
  `messaging.destination.name = "SnapshotJob"`.

All use `messaging.system = "kubernetes"`, `messaging.operation = "process"`,
and keep the existing instance field (`sandbox` / `pool` / `job`). Child
`#[instrument]` spans are unchanged and nest in the task waterfall.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- Span-shape requirements proven against the live traceway (the probe table
  above); the attribute names are literals checked at compile time by the
  `tracing` macro.
- Post-deploy: confirm controller/node reconciles appear under traceway's Task
  view (grouped by reconcile name + destination kind), with child spans in the
  waterfall.

## Notes

- Companion to the api change in this PR (HTTP request spans → Endpoint traces).
- `messaging.destination.name` is the CRD *kind* (low-cardinality); the object
  instance stays in the existing field, so Task grouping isn't blown up.
- Exceptions on a reconcile span were not observed in traceway's Issues view in
  probing; surfacing reconcile errors as Issues (if desired) is a follow-up.
