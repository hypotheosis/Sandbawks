# Plan 05 T6 — `S3SnapshotSink` / `S3SnapshotSource`

**Issue:** #142
**Plan:** `docs/superpowers/plans/2026-05-24-sandbawks-05-gvisor-node.md` §
Task 6
**Branch:** `feat-issue-142-s3-snapshot-sink`

## What landed

`crates/sandbawks-node/src/s3.rs` now ships the production
`S3SnapshotSink` and `S3SnapshotSource` implementations of the
`sandbawks_types::snapshot::SnapshotSink` / `SnapshotSource` traits
introduced by Plan 05 T3 (#139). Both are written against an internal
`BlobStore` trait so the same code path drives either a `rust-s3`
`Bucket` (production) or the new `InMemoryS3` test double (a
`HashMap<String, Vec<u8>>` wrapper) without changing the call sites.

### Storage layout under `<key_prefix>/`

```text
manifest.json                   SnapshotManifest (serde JSON)
parts/<part_name>.json          PartIndex — ordered chunk refs + sha256
chunks/<chunk_sha256>           chunk bodies, deduped by content
```

### Transfer characteristics

- **Chunking.** `put_part` splits the input `Vec<u8>` into 4 MiB
  chunks (`CHUNK_SIZE`); each chunk is hashed and uploaded under its
  content-addressed key.
- **Parallelism.** Chunk PUTs are driven by
  `futures::stream::buffer_unordered(8)`. `get_part` uses the same
  pattern for downloads, then re-assembles by sequence index.
- **Idempotent retry.** Every chunk PUT is preceded by a `HEAD` on the
  same key; if the chunk already exists, the PUT is skipped. A pause
  that crashes mid-upload restarts cleanly without garbage-collecting
  partial state.
- **Cross-part dedup.** Two parts that share a 4 MiB region share the
  underlying `chunks/<sha256>` object — useful when consecutive
  snapshots' CRIU memory images overlap heavily.
- **Verification on read.** `get_part` re-hashes each chunk as it
  lands and verifies the assembled aggregate sha256 matches the index.
  Mismatches surface as `RuntimeError::SnapshotDownload`.

### Tests

- **Inline (`s3::tests`)** — six `#[tokio::test]` cases covering
  small parts, multi-chunk parts (4 MiB + 1 boundary), idempotent
  retry as a no-op, empty parts, manifest round-trip, and explicit
  chunk-corruption detection.
- **Integration (`tests/s3.rs`)** — two `hegel` property tests
  (`parts_round_trip_byte_identical`, `idempotent_retry_is_a_noop`)
  bounded to ~8 MiB per case to keep the 256-case run memory-bounded,
  plus an explicit `round_trip_at_32_mib_hard_cap` `#[tokio::test]`
  that exercises the spec's 32 MiB upper bound on a single case.

## Verification

- `just fmt` clean.
- `cargo clippy --all-targets -- -D warnings` clean across the whole
  workspace.
- `just test` — entire workspace green; `sandbawks-node` adds 6
  in-crate `s3` tests and 3 integration tests (incl. the 32 MiB
  case, ~4 s).

## Out of scope (covered by later tasks)

- `GvisorAdapter::pause` calling `S3SnapshotSink` → Plan 05 T7.
- `GvisorAdapter::resume` calling `S3SnapshotSource` → Plan 05 T8.
- Wiring sink/source construction into the `sandbawks-node` binary —
  Plan 05 T9 (reconcile-loop integration).
- MinIO fixture on `kind` → Plan 05 T13.
- Live-MinIO smoke test → Plan 05 T14.
