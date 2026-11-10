# Stream the validate / resume download path to disk (issue #329)

**Date:** 2026-06-04T11:29:51Z
**Branch:** `fix-issue-329-stream-validate-download`
**Crates touched:** `sandbawks-types`, `sandbawks-node`

## Symptom

The GiB pause bench (`payload_mib=1024`, `include_resume=false`) OOM-killed
the `sandbawks-node` DaemonSet during the pause `Validating` step:

```
Error: status.phase for sbx-… never reached Paused within 300s (last="Pausing")
```

The `SnapshotJob` stranded at `phase: Validating`; the node pod showed
`RESTARTS 1` with `Last State: Reason: OOMKilled`; the Sandbox stayed
`Pausing` until the bench's 300s wait timed out.

## Root cause (regression from #323)

#323 streamed the *upload / pause-bundling* side to flat ~64 MiB RAM and, on
that basis, dropped the bench's `node.resources.limits.memory=4Gi` override
back to the chart-default `1Gi`. It did **not** touch the *download /
validate* path: `GvisorAdapter::validate_restorable` (a storage round-trip,
no live restore) calls `fetch_verify_and_lay_out`, which did

```rust
let bytes = source.get_part(&part.name).await?;   // entire ~1 GiB memory.zst → Vec<u8>
// … untar_zstd_to_dir_blocking(&bytes, &dest)     // + decompression, more RAM
```

`SnapshotSource::get_part` returns `Result<Vec<u8>, RuntimeError>`, so the
whole `memory.zst` part was buffered in RAM and then decompressed — blowing
past the 1Gi node limit → OOM → restart → the orphaned `Validating` job was
skipped for `STUCK_JOB_RECOVERY_AGE` (5 min) → controller never saw
`Complete`.

## Fix

Mirror #293's streaming on the **download** side so node RAM stays flat at
the 1Gi default:

- **`sandbawks-types`** — new `SnapshotSource::get_part_to_file(name, path)`
  trait method (read-side mirror of `SnapshotSink::put_part_file`). It
  streams the part straight to a local file and returns the observed
  `SnapshotPart` (name/sha256/length) so callers can cross-check it against
  the manifest without re-reading the file. The default impl delegates to
  `get_part` + `std::fs::write` — fine for the KB-scale in-memory test
  doubles (`NoopSnapshotStore`, `FakeSnapshotStore`), with the same
  "production impls MUST override" warning `put_part_file` carries.

- **`sandbawks-node` (`s3.rs`)** — `S3SnapshotSource` overrides
  `get_part_to_file` with a streaming implementation: chunks are fetched
  with bounded concurrency via `stream::iter(...).buffered(PARALLELISM)`
  (ordered, unlike `get_part`'s `buffer_unordered`) and written to a
  `BufWriter<File>` by a single blocking task draining a bounded channel.
  Per-chunk sha256/length verification and the aggregate part-digest check
  are preserved. Resident chunk bodies are bounded at
  `(2 * PARALLELISM + 1) * CHUNK_SIZE` ≈ 68 MiB — flat whether the part is
  16 MiB or 4 GiB, exactly mirroring the upload side.

- **`sandbawks-node` (`gvisor.rs`)** — `fetch_verify_and_lay_out` now
  streams `memory.zst` to a staged file via `get_part_to_file`, expands it
  with `untar_zstd_file_to_dir_blocking` (a streaming `zstd` decoder over
  the file rather than over an in-RAM `Vec<u8>`), then removes the staged
  archive so the bundle isn't resident on disk twice. The small parts
  (`rootfs-delta.tar.zst`, `envd-state.json`) keep the buffered `get_part`
  path. The shared sha/size cross-check is factored into
  `verify_observed_part`.

This covers both `validate_restorable` and the `resume`/`stage` fetch path,
since they share `fetch_verify_and_lay_out`. The bench stays at the 1Gi
default — the point of the fix is that the override is no longer needed.

## Tests

- `s3::tests::get_part_to_file_streams_multi_chunk_part_byte_for_byte` — the
  streamed file matches the original payload byte-for-byte and the returned
  `SnapshotPart` matches the buffered `put_part` manifest entry.
- `s3::tests::get_part_to_file_handles_empty_part` — zero-length part →
  empty staged file + empty-input sha256.
- `s3::tests::get_part_to_file_rejects_corrupt_chunk` — a tampered backend
  chunk surfaces as `RuntimeError::SnapshotDownload` rather than writing bad
  bytes to disk.

`just fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo test --workspace --all-targets` all pass. The GiB-scale memory
flatness itself is structurally guaranteed by the bounded channel +
`buffered` look-ahead (same shape as #293's upload path) and is verified
end-to-end by the GiB pause bench on a real cluster, which can't be run
locally.

## Out of scope

The issue's secondary note — `STUCK_JOB_RECOVERY_AGE` (5 min) equalling the
bench's 300s pause budget, so a node restart mid-job can't recover within
the bench window — is a separate concern that becomes moot for this bug once
the OOM (and therefore the restart) is removed. Left for its own change.

## Review round 1 (PR #330)

- **Trait docs**: documented `get_part_to_file`'s partial-file-on-error
  contract — on a mid-stream failure a partially-written file may remain at
  `path`; callers must use a scratch dir they clean up on error.
- **`gvisor.rs`**: noted that the staged file name is intentionally the part
  name (`memory.zst`) and is removed before the shim reads the staging dir.
- **`s3.rs`**: aligned the aggregate sha256 / length mismatch error
  format-strings with `get_part`'s named-arg style.
- **Tests**: added `get_part_to_file_fetches_chunks_concurrently`, the
  download-side mirror of `put_part_file_uploads_chunks_concurrently`, using
  a now-`get`-instrumented `ConcurrencyProbeStore` to guard against a serial
  reader regression.

Disk-headroom on the validate path (the staged `.zst` now lands on disk
during `validate_restorable`'s `HeadroomGate::Skip` window) is a separate
follow-up — not a regression vs. what stage/resume already do.
