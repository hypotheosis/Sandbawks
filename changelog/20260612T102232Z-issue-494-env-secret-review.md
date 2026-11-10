# Env secret indirection — PR #497 review round

- **Date:** 2026-06-12T10:22:32Z
- **Issue:** #494 (PR #497 review round)
- **Crates:** `sandbawks-controller`

## Why

PR #497 moves customer `spec.env` out of the inline Pod spec and into a
controller-owned `sandbox-<name>-env` Secret referenced via `secretKeyRef`, so
`kubectl describe pod` no longer leaks secrets. The self-review raised two
non-blocking notes:

1. `env_secret_for` wrote **every** `spec.env` key to the Secret's `string_data`,
   including `OTEL_RESOURCE_ATTRIBUTES`. But `pod_spec_for` consumes that key as
   an *inline* merged var (#435 — the customer value is folded as the base of the
   sandbox identity attrs), never via `secretKeyRef`, so the Secret carried an
   unreferenced key. The Secret should hold exactly the keys the Pod consumes.
2. The `create`-then-`409` idempotency in `create_pod` materializes the Secret
   once and never reconciles it. Harmless today (`spec.env` is set-once), but a
   future mutable-env feature would silently persist stale values.

## What changed

- **`env_secret_for`** now builds `string_data` by filtering out
  `OTEL_RESOURCE_ATTRIBUTES` in one pass, and returns `None` when the filtered
  map is empty — so a `spec.env` whose only entry is that inline-consumed key no
  longer produces a dangling Secret. The Secret keys now match exactly the
  `secretKeyRef` entries `pod_spec_for` emits (which already `retain`-drops
  `OTEL_RESOURCE_ATTRIBUTES` before re-adding it inline).
- **`create_pod`** gains a doc note on the `409` branch: create-or-409 means the
  Secret is never reconciled; mutable env would require an update/patch path
  rather than treating `AlreadyExists` as success.

## Tests

- Added `env_secret_excludes_inline_consumed_otel_resource_attributes`: a Secret
  built from `{OTEL_RESOURCE_ATTRIBUTES, PLAIN}` carries only `PLAIN`; a `spec.env`
  holding only `OTEL_RESOURCE_ATTRIBUTES` yields no Secret.

## Validation

- `just fmt` — clean.
- `cargo clippy -p sandbawks-controller --all-targets -- -D warnings` — clean.
- `cargo test -p sandbawks-controller --lib` — green.
