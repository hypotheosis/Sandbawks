# Plan 04 T3 — TeamAPIKey auth middleware + `TeamContext` extractor

**Date:** 2026-05-24T01:39:00Z
**Branch:** `feat-issue-102-team-api-key-auth`
**Issue:** #102
**Plan reference:** `docs/superpowers/plans/2026-05-23-sandbawks-04-api-gateway.md` § Task 3

## Summary

Every API request now authenticates via `X-API-Key: sbk_...`. The
gateway splits off the 8-char prefix, looks up the matching
`TeamAPIKey` CR cluster-wide, constant-time-compares the SHA-256
digest against the stored hash, and injects a `TeamContext` into the
request extensions so downstream handlers learn the caller's
namespace + scopes without re-resolving the key.

The hashing primitives live in `sandbawks-types` so the controller's
later admission code can re-use them; the middleware itself lives in
`sandbawks-api` so it can stay coupled to `AppError` and `axum`.

## Changes

### `crates/sandbawks-types`

- Added workspace deps `hmac`, `sha2`, `subtle`, `base64` to the crate's
  `[dependencies]`. (`hmac` + `base64` are pulled in alongside the
  others for the upcoming signed-URL helper that will land in T11.)
- New `src/auth.rs`:
  - `KeyPrefix(String)` newtype. `KeyPrefix::from_key(raw)` takes
    *characters* (not bytes), capped at 8 — short keys are *not*
    padded, multibyte input doesn't slice on a non-char boundary.
  - `ApiKeyHash(String)` newtype carrying the canonical wire form
    `sha256:<lowercase-hex>`.
  - `hash_key(raw) -> ApiKeyHash` — pure SHA-256 with a manual lowercase
    hex encoder (no extra crate dep just for hex).
  - `verify_key(raw, &ApiKeyHash) -> bool` — constant-time compare via
    `subtle::ConstantTimeEq`. Returns `false` for any malformed stored
    hash (wrong prefix / wrong length / non-hex bytes) so a bad CR
    can't accidentally authenticate anyone.
- Re-exported `pub mod auth;` from `src/lib.rs`.
- `tests/auth.rs` covers: round-trip success, rejects-wrong-key,
  prefix is first 8 chars, short keys aren't padded, and three
  malformed-hash rejection paths. All 5 tests pass.

### `crates/sandbawks-api`

- Replaced the Task-1 placeholder `src/auth.rs` with the real module:
  - `TeamContext { namespace, scopes, key_name }` + `allows(op)` method
    (MVP: `sandboxes:*` matches anything; explicit scope strings match
    by equality).
  - `KeyStore` async trait with one method `resolve(prefix, raw_key)`.
  - `KubeKeyStore` — production impl. Lists `TeamAPIKey` cluster-wide
    with `limit(500)`, client-side filters by `spec.keyPrefix`, calls
    `verify_key` on each candidate. CRs without a namespace surface as
    `AppError::Internal` (impossible for namespaced CRs but guarded
    explicitly so the failure mode is loud, not silent).
  - `FakeKeyStore` — in-memory `Vec<(raw_key, TeamContext)>` for tests.
    No hashing; raw-key equality so test failures are easy to read.
  - `auth_middleware<S: KeyStore + ?Sized>(State<Arc<S>>, HeaderMap,
    Request, Next)` reads `x-api-key`, resolves it, injects
    `TeamContext` into request extensions, then runs the inner stack.
    Missing / wrong key both render the standard E2B `unauthorized`
    envelope from T2.
- `tests/auth.rs` integration test: happy-path (200, body is the
  injected namespace), missing-header (401), wrong-key (401). All 3
  pass.

## Verification

- `cargo test -p sandbawks-types --test auth` — 5 passed.
- `cargo test -p sandbawks-types -p sandbawks-api` — full crate tests
  green.
- `cargo clippy -p sandbawks-types -p sandbawks-api -- -D warnings` —
  clean.
- `just fmt` — clean.

## Notes / Follow-ups

- `kube` field selectors don't index `spec.*` on custom resources, so
  `KubeKeyStore` does a cluster-wide list + client-side filter. For
  MVP scale (hundreds of teams) this is fine. A later plan can land
  an admission webhook that enforces
  `metadata.name == "{keyPrefix}-{rand}"` and use a `metadata.name`-
  prefix field selector for cheaper listing.
- `auth_middleware` is generic over `S: KeyStore + ?Sized` so it works
  with both `Arc<KubeKeyStore>` (concrete) and `Arc<dyn KeyStore>`
  (trait-object) depending on what the binary builds at wire-up time.
- The `key_name` field on `TeamContext` is intended for audit logging
  in a later plan — it lets us identify *which* of a team's keys
  authenticated a request without echoing back the raw key.
