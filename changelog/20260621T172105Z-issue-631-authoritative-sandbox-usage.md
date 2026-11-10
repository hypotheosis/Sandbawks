# Per-API-key usage metering — Phase 2: authoritative resource-weighted sandbox-seconds

**Date:** 2026-06-21T17:21:05Z
**Issue:** #631 (epic #480, Phase 2)
**Crates:** `sandbawks-types`, `sandbawks-api`, `sandbawks-controller`

## Summary

Emits the **authoritative billing signal** for the usage-metering epic: per-team,
resource-weighted sandbox running-seconds, sourced from the controller's reconcile
loop (the only vantage point that sees every `running`/`paused`/`terminated`
transition). On each edge that closes a `Running` interval the controller emits a
durable, append-only `UsageEvent` through a pluggable sink.

Per the operator answers in #480 (comment #16264): pricing axis = **resource-weighted
seconds** (CPU/RAM request); per-`TeamAPIKey` breakdown; and — load-bearing —
**OpenTelemetry is not a valid authoritative source for billing**, so the event
stream (not an OTel counter) is the system of record the Phase 3 ledger (#632) rolls
up from.

## Changes

### `sandbawks-types`
- New `API_KEY_NAME_ANNOTATION` (`sandbawks.dev/api-key-name`) in `labels.rs`,
  re-exported from the crate root. Carries the owning `TeamAPIKey` name on a Sandbox
  (the namespace already identifies the team; this carries the key).

### `sandbawks-api` — stamp the owning key onto Sandboxes
- `routes/sandboxes.rs`: `create_sandbox` stamps the annotation via a new
  `stamp_api_key_name` helper (skips an empty key, so keyless/operator-applied
  Sandboxes stay annotation-free).
- `warm_claim.rs`: `ClaimOverrides` gains `api_key_name`; `claim_for` reads it from
  the requester (the create handler stamped it) and `apply_overrides` brands the
  claimed warm member — so a claimed member is attributed to the claiming team's key,
  not left unowned.

### `sandbawks-controller` — emit authoritative usage events
- New `usage.rs`: `UsageEvent` (team, key_name, sandbox id/uid, interval
  start/end, running_seconds, cpu/mem request, weighted_seconds), the pluggable
  `UsageEventSink` trait, `NoopUsageSink` (default when metering is off),
  `RecordingUsageSink` (tests), CPU/RAM quantity parsers, and the resource-weight
  normalization `max(cpu_cores, mem_gib / MEM_GIB_PER_COMPUTE_UNIT)`.
- New `s3_usage.rs`: `S3UsageSink`, an object-store-backed sink reusing the same
  `rust-s3` plumbing and `SANDBAWKS_S3_*` env vars as snapshot retention; writes one
  NDJSON event per closed interval under a deterministic, idempotent key.
- `reconcile.rs`: on the `Running` → non-`Running` edge, `emit_running_usage` builds
  and emits the event (fail-open) **before** the status patch; the loop now stamps
  `status.lastTransitionTime` on phase transitions, which anchors each interval's
  start. `ReconcileContext` gains a `usage_sink`.
- `bin/sandbawks-controller.rs`: `build_usage_sink()` wires the sink from env
  (`SANDBAWKS_USAGE_METERING_ENABLED`, default OFF; `SANDBAWKS_USAGE_PREFIX`,
  default `usage`). Never fails startup — incomplete/unsupported config logs a WARN
  and falls back to Noop.

## Design notes

- **Interval anchoring.** A `Running` interval is `[lastTransitionTime when it
  entered Running, now]`. `lastTransitionTime` existed in the CRD but was never
  written in production; the loop now stamps it on every transition. Sandboxes that
  entered `Running` before this change have no anchor and are skipped (not billed)
  rather than billed from a guessed start.
- **Idempotency.** Events are keyed by `(sandbox_uid, interval_start)`. The close
  edge fires once (the state machine returns `None` while staying in a phase); a
  crash between emit and the status patch re-drives the same edge and re-emits the
  *same* key, so the object-store PUT overwrites rather than double-bills.
- **Fail-open.** A sink error is logged and the lifecycle transition proceeds, so a
  billing-store outage can never wedge pause/terminate — mirroring the best-effort
  Kubernetes Event emission. A sustained outage drops intervals, which the Phase 3
  ledger can detect by reconciling against live state.
- **Authoritative, not OTel.** Any OTel `usage.sandbox.*` counters would be an
  explicitly non-authoritative mirror; the ledger derives only from these events.

## Validation

- `just fmt` — formatted.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — workspace green, including:
  - `usage.rs` unit tests: CPU/RAM parsing, weight normalization, interval bounding,
    idempotency/object key, recording sink.
  - `reconcile.rs`: `emit_running_usage` records a weighted, attributed event and
    skips when there is no Running-entry anchor.
  - `warm_claim.rs`: a claim brands the member with the owning key.
  - `routes/sandboxes.rs`: `stamp_api_key_name` sets / skips the annotation.

## Follow-ups / out of scope

- **Azure usage sink.** Only the S3-compatible backend is wired today; an Azure sink
  (mirroring `AzureRetentionStore`) is a follow-up. Azure clusters fall back to Noop
  with a warning.
- Phase 3 (#632): the durable hourly NDJSON ledger that rolls these events up.
- Pricing, invoicing, quota enforcement — epic non-goals.
