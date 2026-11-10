# Instrument operational functions for full trace waterfalls (issue #371)

**Date:** 2026-06-07
**Issue:** #371 — feat(observability): instrument every operational function so
Task/Endpoint traces show the full call tree.

## Summary

PR #370 gave us trace *roots* (api Endpoint spans, controller/node reconcile
Task spans) but the waterfalls beneath them were nearly empty — almost no
sub-operation was instrumented. This change adds `#[tracing::instrument]` spans
to the operational functions on every operation's path so a single
Task/Endpoint trace now renders the full nested call tree (and timings) of what
actually ran.

All new spans follow the established conventions: `#[tracing::instrument(skip_all, …)]`
with explicit **low-cardinality** fields, the `<layer>:<module>::<op>` naming
style, and `err` on fallible operational functions so failures surface as
Issues (#370). No span is created per chunk/row in tight loops — bulk S3
transfers are spanned as a whole with byte/chunk counts as fields.

## Phase 1 — Controller (`crates/sandbawks-controller/src`)

- `reconcile.rs`: `dispatch:controller::action` (with an `action` label),
  `observe:controller::snapshot_job`, `resolve:controller::pod_inputs`,
  `probe:controller::{running,creating}_liveness`, `build:controller::snapshot_job`.
- `pod_adapter.rs`: `pod:controller::{create,delete,ready,terminated}`.
- `store.rs`: `store:controller::{sandbox_get,sandbox_patch_status,job_get,job_create}`
  (real `Kube*` impls only — the in-memory test fakes are left uninstrumented).
- `warm_pool.rs`: `pool:controller::{list_members,create_member,delete_member,patch_status,template_defaults}`.
- `state_machine.rs`: added `Action::label()` returning a `&'static str` so the
  dispatch span records the action variant without leaking the
  `RecordFailure(msg)` string.

## Phase 2 — Node (`crates/sandbawks-node/src`)

- `gvisor.rs` pause/resume/stage/validate pipeline: `pause:node::snapshot`,
  `resume:node::snapshot`, `stage:node::snapshot`, `validate:node::restorable`,
  `resolve:node::pod`, `inspect:node::container_mounts`,
  `fetch_verify:node::snapshot`, `verify:node::envd_digest`,
  `headroom:node::enforce`, `compress:node::memory_images`,
  `verify:node::checkpoint_postconditions`, `readiness:node::envd_bind`.
  (`probe:node::fetch_state` already existed.) Added `Debug` to `HeadroomGate`
  so it can be a span field.
- `s3.rs` whole-transfer spans (no per-chunk spans):
  `upload:node::part`, `stream_upload:node::part`, `upload:node::manifest`,
  `download:node::part`, `stream_download:node::part`, `download:node::manifest`,
  `verify:node::part_present`. `PhaseTimer` breakdowns are retained as existing
  structured log fields.

## Phase 3 — API (`crates/sandbawks-api/src`)

- `envd_client.rs`: `client:api::envd_connect` (`otel.kind=client`) on the
  cold-cache connect path only (the cache-hit fast path stays span-free so
  steady-state RPCs don't each emit one). Per-RPC propagation already works —
  the `TraceparentInjector` gRPC interceptor injects the *current* handler
  span, and envd's inbound `GrpcTraceLayer` continues it — so the api→envd hop
  is connected without rewriting every call site.
- `routes/sandboxes.rs` (`KubeSandboxOps`): `kube:api::sandbox_{create,list,delete,get,patch_desired_phase,patch_timeout}`.
- `warm_claim.rs` (`KubeWarmClaim`): `kube:api::{pool_for_template,running_candidates,relabel_claim,apply_overrides}`.

## Phase 4 — envd (`crates/envd/src`)

No code change required: every gRPC handler (19 across `process.rs` /
`filesystem.rs`), the HTTP `/__state` handler, and the inbound `GrpcTraceLayer`
trace-context extractor are already instrumented. Verified the 20 `handler:*`
spans and the inbound layer are present. envd OTel SDK survival across
checkpoint/restore remains tracked in #356 (out of scope here): spans emitted
immediately after a resume may not export until the SDK is re-armed.

## Phase 5 — Conventions + guardrails

- `docs/observability.md`: added a developer-facing **"Instrumenting new code"**
  guide (span hierarchy, the `#[instrument]` recipe, cardinality rules,
  loop/bulk-transfer guidance, cross-service continuity, the #356 caveat) and an
  instrumentation **checklist**.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — green.
- No per-chunk spans on the GiB-scale S3 hot path (transfers spanned whole,
  counts/bytes as fields), so trace export volume stays bounded.

## Review feedback (PR #372)

Addressed the review's central point — the new docs warn against `err` on
retry-chatty functions, so the code must follow its own advice:

- Dropped `err` from `pod:controller::{ready,terminated}` and
  `observe:controller::snapshot_job` (their transient errors are swallowed by
  callers and retried next requeue — `err` would surface false Issues).
- Dropped `err` from the per-reconcile warm-pool store reads/write
  (`pool:controller::{list_members,template_defaults,patch_status}`, which run
  every `WARM_POOL_REQUEUE`); kept it on `create_member`/`delete_member`, which
  fire only on a real scaling delta. Genuine failures still surface via the
  parent `reconcile:controller::warm_pool` span's `err`.
- Renamed the envd channel span and moved it off the cache-hit fast path (see
  Phase 3) so it no longer fires per RPC.
- Gave `stream_upload:node::part` / `stream_download:node::part` a `bytes` field
  (recorded post-transfer) for size parity with the buffered transfer spans.

## Out of scope

- envd OTel SDK survival across checkpoint/restore — #356.
- OTLP auth-token-from-Secret + export-failure log-spam hardening — #369.
- Metric counterparts for the new spans (the dispatch `action` label, warm-pool
  reconcile, validate outcomes are good histogram/counter candidates) — a
  natural tracing→metrics follow-up, noted in review.
