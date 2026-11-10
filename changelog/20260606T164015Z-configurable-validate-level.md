# Configurable pause-validate level (default head+size) + double-hash note

**Date:** 2026-06-06T16:40:15Z

## Summary

The pause restorability check (#235) is now configurable via
`SANDBAWKS_S3_VALIDATE_LEVEL` / chart `node.snapshotValidateLevel`, and its
**default changes from `full` to `head`** (head+size). With garage given enough
CPU, the GiB bench showed the old `full` re-download+decompress was ~40% of a
pause (~8–9 s) and is pure overhead — re-reading bytes we just wrote.

## Levels (weakest → strongest)

- `off` — skip the post-upload check (trust the write path).
- `head` — **default.** HEAD each object + verify content-length, and
  cross-check the part index vs the manifest (catches the cross-sandbox key
  collision). No bodies transferred, no decompress.
- `sha` — re-download every part, verify per-chunk + per-part sha256; no
  decompress/lay-out. Adds byte-integrity over `head`.
- `full` — re-download, sha-verify, decompress + un-tar to scratch, and assert
  the `*.img` checkpoint postconditions (#232) + the #331 disk-headroom gate.
  The previous behavior.

## ⚠ Safety note

This **relaxes the default fail-closed guarantee**, by deliberate decision:
`head` confirms presence + size but not in-chunk byte corruption or that the
archive decompresses. Deployments that want the stronger guarantee set
`node.snapshotValidateLevel: sha` (or `full`). The cross-sandbox identity guard
(index sha vs manifest) is retained at `head`, so the class of corruption that
motivated the per-sandbox-prefix fix is still caught.

## Implementation

- `sandbawks-types`: `SnapshotSource::verify_part_present(part)` with a
  download-and-verify default (so Noop/Fake work unchanged).
- `sandbawks-node/s3.rs`: `BlobStore::head_size` (Bucket/InMemoryS3/test
  double); `S3SnapshotSource::verify_part_present` overrides it to read the part
  index + HEAD every chunk + check sizes + index/manifest identity. Unit test
  `verify_part_present_head_size`.
- `sandbawks-node/gvisor.rs`: `ValidateLevel` enum + `parse_validate_level` +
  `effective_validate_level` (env, default head); a `validate_level` field on
  `GvisorAdapter` (default from env) with `with_validate_level(&str)` for tests;
  `validate_restorable` branches on it. Existing full-behavior tests pinned to
  `full`; added `validate_restorable_head_level_accepts_intact_bundle`.
- chart: `node.snapshotValidateLevel` → conditional `SANDBAWKS_S3_VALIDATE_LEVEL`.

## Double-SHA on upload — investigated, not changed

rust-s3 0.35 re-SHA-256s every PUT body for the SigV4 `x-amz-content-sha256`
header (`command().sha256()`, unconditional for PUT; `UNSIGNED-PAYLOAD` is
presigned-only), so each uploaded chunk is hashed both for its content-addressed
key and again for signing. It's **not removable via config** — only by forking
rust-s3 or migrating to aws-sdk-s3. Since the bench proved upload is
backend-bound (not node-CPU-bound), this redundant hash (~1 CPU-s/GiB on a
non-saturated node) is off the critical path; documented inline at `BlobStore::put`
and left as-is pending a possible client migration.

## Verification

`cargo clippy -D warnings`, `cargo fmt --check`, full `sandbawks-types` +
`sandbawks-node` suites, and `helm template` (env present at `head`, absent when
unset) all pass.
