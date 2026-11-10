# Stream the snapshot bundle to disk so node DaemonSet RAM is flat at GiB scale

**Issue:** #293
**Date:** 2026-06-04T00:26:02Z

## Problem

The gVisor pause path built the entire `memory.zst` snapshot part in the
node DaemonSet's RAM, so node memory scaled with the size of the sandbox
being checkpointed. At GiB scale this OOM-killed the 1Gi-capped DaemonSet
and the sandbox hung in `Pausing` forever.

Two buffers compounded:

1. `tar_zstd_checkpoint_files` accumulated the whole compressed memory
   image into a `Vec<u8>`. The `pages.img` payload (gVisor's
   overlay-tmpfs upper layer, where a workload's `/tmp` writes land) is
   often incompressible, so the `Vec` ≈ the full payload size.
2. `SnapshotSink::put_part` took the part by value and then re-chunked it
   into per-chunk `Vec`s, roughly doubling peak.

## Fix

Make node DaemonSet pause-path memory **flat** regardless of payload size
by staging the bundle through disk:

- **New streaming sink method.** Added `SnapshotSink::put_part_file(name,
  path)`. The default impl reads the file and delegates to `put_part`
  (fine for the small in-memory test doubles). `S3SnapshotSink` overrides
  it to stream: a `spawn_blocking` reader reads the file in `CHUNK_SIZE`
  (4 MiB) blocks, hashing each chunk and the running whole-part sha256,
  and hands bodies across a **bounded** channel (`PARALLELISM`-deep) to
  the async uploader, which drives HEAD-then-PUT with at most
  `PARALLELISM` in flight. Peak ≈ `2 * PARALLELISM * CHUNK_SIZE` (~64 MiB)
  whether the part is 16 MiB or 4 GiB.
- **Pause stages to disk.** `tar_zstd_checkpoint_files` now writes the
  archive to `<work_dir>/memory.zst` on the `hostPathSnapshotCache`
  volume (already disk-headroom-gated) via a `BufWriter<File>` instead of
  a `Vec`, and returns the path. Pause streams it with `put_part_file`
  and then deletes the staged copy so no GiB duplicate of `pages.img` is
  left behind.
- The buffered `put_part` is unchanged and still used for the small parts
  (`envd-state.json`, the empty gVisor `rootfs-delta.tar.zst`). The
  shared part-index write was extracted into `write_part_index` (DRY).

The wire layout is byte-for-byte identical between the two paths — proven
by a new unit test that drives both against the same content and asserts
the resulting object stores (chunk objects + part index) are equal.

## CI

Removed the bench-only `--set node.resources.limits.memory=4Gi` override
from `.forgejo/workflows/kind-smoke.yaml`. The node DaemonSet now runs the
GiB bench at the chart-default **1Gi** limit, which `helm template`
confirms is the default.

## Restore-path review (acceptance criterion 4)

`stage_snapshot` still buffers each whole part into a `Vec<u8>`
(`gvisor.rs`: `let bytes = source.get_part(&part.name).await?` →
`untar_zstd_to_dir_blocking(&bytes, …)`), so the **resume** side carries
the symmetric OOM at GiB scale. That is out of scope for this pause-side
fix and is tracked separately (resume-at-GiB, #302); the GiB bench runs
pause-only until it lands.

## Review round 1 (PR #323)

Six inline review comments addressed across three commits:

- **`d25e33f`** — Pause now removes the staged `memory.zst` regardless of
  upload outcome (before propagating any error), so a mid-upload failure
  no longer leaks a GiB-scale orphan on the snapshot cache volume.
- **`c3b4e4a`** — `put_part_file`'s uploader loop rewritten with a
  `biased tokio::select!` that polls the in-flight set and the channel
  concurrently; the previous `loop { rx.recv().await }` left
  `FuturesUnordered` unpolled while waiting for the next read, serializing
  uploads behind a disk-bound reader. Added a `ConcurrencyProbeStore`
  (delaying `BlobStore` double) regression test asserting max-in-flight
  ≥ 2. Also added a `tracing::info!` completion record
  (`bytes_compressed`, `chunks`, `elapsed_ms`), corrected the peak-memory
  docstring to `(2 * PARALLELISM + 1) * CHUNK_SIZE` ≈ ~80 MiB, and
  documented that the reader's error intentionally wins over a concurrent
  upload error.
- **`b44fa1d`** — Strengthened the `put_part_file` default-impl doc with a
  `# Warning` that the synchronous `std::fs::read` blocks the executor and
  is safe only for test-scale files; production sinks must override.
  (Chose the doc-warning option over `spawn_blocking` because
  `sandbawks-types` keeps `tokio` a dev-dependency.)

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — all pass, including the three `put_part_file` tests
  (equivalence, empty-file, and the new concurrency regression test)
- `helm template … --set node.enabled=true` — node DaemonSet renders with
  `memory: 1Gi` default
