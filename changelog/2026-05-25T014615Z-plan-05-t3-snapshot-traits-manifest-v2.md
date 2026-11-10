# Plan 05 T3 — `SnapshotSink` / `SnapshotSource` traits + `SnapshotManifest` v2 + `FakeSnapshotStore`

Forgejo issue: #139 (part of
Plan 05 epic #136).
Plan: `docs/superpowers/plans/2026-05-24-sandbawks-05-gvisor-node.md` § Task 3.

## Summary

Introduces the storage-agnostic snapshot traits that future per-runtime
checkpoint/restore code will consume, evolves the existing
`SnapshotManifest` from a single-blob v1 shape into a multi-part v2
shape with per-part sha256s, splits `RuntimeError` into snapshot-aware
variants, and ships a `FakeSnapshotStore` test-double gated behind a
new `testing` cargo feature. No production wiring yet — the controller
keeps calling `pause`/`resume` with a no-op store until Plan 05 T4
replaces the direct call sites with `SnapshotJob` CR creation.

## Changes

### `crates/sandbawks-types/src/snapshot.rs` (new)
- `SnapshotPart { name, sha256, size_bytes }` — one chunk of a
  snapshot bundle. `SnapshotPart::from_bytes(name, bytes)` computes
  the part's sha256 in-place from a buffered payload so adapters can
  build the manifest by streaming each chunk through the sink.
- `aggregate_sha256(parts: &[SnapshotPart]) -> String` — sorts parts
  by name, then hashes the concatenation of their hex-encoded sha256s.
  Stable under reordering (so the aggregate is usable as a cache key)
  and returns the empty string for an empty parts list (matches v1
  semantics for "no snapshot recorded yet").
- `#[async_trait] SnapshotSink { put_part, put_manifest }` —
  write-side trait. `put_part` takes an owned `Vec<u8>` for now; Plan
  05 T6's `S3SnapshotSink` adds a streaming overload alongside.
- `#[async_trait] SnapshotSource { get_part, get_manifest }` —
  symmetric read-side trait.
- `NoopSnapshotStore` — zero-sized type implementing both traits as
  no-ops; `put_part` computes the part metadata but discards the
  bytes, `get_*` return `RuntimeError::SnapshotDownload`. Used by
  the controller's reconcile loop until T4.
- `#[cfg(test)] mod tests` — unit tests for `aggregate_sha256`
  reordering stability and the NIST SHA-256 "abc" vector to pin
  hash-function correctness.

### `crates/sandbawks-types/src/runtime.rs`
- `SnapshotManifest` evolved to v2: adds `parts: Vec<SnapshotPart>`
  and `snapshot_ref: String`, both `#[serde(default)]` so a v1 JSON
  document deserializes cleanly into a v2 manifest with empty
  defaults. `sha256` is repurposed as the aggregate over sorted part
  hashes. New `recompute_aggregate(&mut self)` helper for callers
  that build a manifest by pushing parts then sealing the aggregate.
- `RuntimeError` adds four variants: `SnapshotUpload`,
  `SnapshotDownload`, `CheckpointFailed`, `RestoreFailed`. Lets the
  controller's future retry policy distinguish transfer-side from
  runtime-side failures.
- `RuntimeAdapter::pause` signature extended to
  `(&self, pod_uid: &str, sink: &dyn SnapshotSink)`. Provided as a
  trait default that ignores `sink` and returns an empty manifest;
  Plan 05 T7's `GvisorAdapter` overrides this with real checkpointing.
- `RuntimeAdapter::resume` signature extended to
  `(&self, manifest: &SnapshotManifest, pod_uid: &str, source: &dyn SnapshotSource)`
  with the same default-no-op pattern.
- `noop_sink()` / `noop_source()` helpers — return a `'static`
  reference to a `NoopSnapshotStore` so call sites that don't have a
  real store yet (the controller's `Action::Pause` / `Action::Resume`
  arms) stay tidy.

### `crates/sandbawks-types/src/testing.rs` (new, `testing` feature)
- `FakeSnapshotStore` — in-memory snapshot store backed by
  `Arc<Mutex<HashMap<String, Vec<u8>>>>`. Implements both
  `SnapshotSink` and `SnapshotSource` so a single instance can drive
  a pause-then-resume round trip in unit tests. The manifest is
  serialized to JSON and stored under a reserved `::manifest::` key,
  which can't collide with any plausible part name.

### `crates/sandbawks-types/Cargo.toml`
- Adds `[features] testing = []` — gates the `testing` module so
  production controller/api binaries don't pull the helper surface
  into release builds.
- Adds `hegel` to `[dev-dependencies]` for the property-based
  round-trip test.

### `crates/sandbawks-types/src/lib.rs`
- Exposes new modules (`pub mod snapshot;`, `#[cfg(feature = "testing")] pub mod testing;`).
- Re-exports `SnapshotPart`, `SnapshotSink`, `SnapshotSource`,
  `NoopSnapshotStore`, `aggregate_sha256`, `noop_sink`, `noop_source`
  so downstream crates can `use sandbawks_types::SnapshotSink` etc.
  without spelling the submodule path.

### `crates/sandbawks-types/tests/runtime_trait.rs`
- `NullAdapter` updated for the new pause/resume signatures (takes
  `&dyn SnapshotSink` / `&dyn SnapshotSource`).
- Added `snapshot_manifest_v1_compat_deserialize` regression test
  proving a v1 JSON document (no `parts`, no `snapshotRef`)
  deserializes successfully with v2 defaults.

### `crates/sandbawks-types/tests/snapshot_store.rs` (new, `testing` feature)
- Hegel property test `parts_round_trip_byte_identical`. Generates an
  arbitrary `Vec<(name, bytes)>` (deduplicated by name to match the
  last-write-wins semantics of `put_part`), pushes every pair through
  `FakeSnapshotStore::put_part`, asserts that
  - each returned `SnapshotPart` equals a freshly-computed
    `SnapshotPart::from_bytes(name, bytes)` (per-part sha256
    invariant),
  - `get_part(name)` returns the original bytes verbatim (byte
    identity),
  - `aggregate_sha256(parts)` is stable across the operation,
  - and a full `SnapshotManifest` round-trips through
    `put_manifest`/`get_manifest` with structural equality.

### Controller call sites (no behavioural change)
- `crates/sandbawks-controller/src/fake_adapter.rs` — `FakeRuntimeAdapter`
  picks up the new `&dyn SnapshotSink` / `&dyn SnapshotSource`
  parameters but continues to ignore them.
- `crates/sandbawks-controller/src/pod_adapter.rs` — `PodAdapter`
  same.
- `crates/sandbawks-controller/src/reconcile.rs` — `Action::Pause`
  and `Action::Resume` pass `noop_sink()` / `noop_source()`. Plan 05
  T4 replaces this with `SnapshotJob` CR creation, at which point
  these arms stop calling the adapter directly.
- `crates/sandbawks-controller/tests/fake_adapter.rs` — call sites
  updated to pass `noop_sink()` / `noop_source()`.

## Verification

- `cargo test -p sandbawks-types --all-features` — green (3 doc-tests + 9
  integration tests including the Hegel property test).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean
  (the no-feature build still excludes the `testing` module).
- `cargo doc -p sandbawks-types --no-deps` — clean (no broken
  intra-doc links).
- `just test` — full workspace green.

## Out of scope (covered by later tasks)

- `S3SnapshotSink` / `S3SnapshotSource` impls — Plan 05 T6.
- Controller's `dispatch()` creating `SnapshotJob` CRs instead of
  calling `pause`/`resume` directly — Plan 05 T4.
- `GvisorAdapter` calling the new traits — Plan 05 T7 / T8.

## Follow-ups

- The `pause`/`resume` default impls currently fill a placeholder
  `SnapshotManifest` (empty parts, empty sha256, fixed epoch
  `created_at`). Once `FakeRuntimeAdapter` is deleted in T4 we can
  decide whether the default should be `unimplemented!()` instead —
  there will no longer be a non-overriding caller that depends on
  the default returning `Ok`.
