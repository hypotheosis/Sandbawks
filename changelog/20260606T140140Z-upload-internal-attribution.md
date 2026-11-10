# Upload-internal attribution: process-bound vs transfer-bound

**Date:** 2026-06-06T140140Z

## Summary

`S3SnapshotSink::put_part_file` (the streaming `memory.zst` upload) now emits
process-vs-transfer attribution counters, and the bench PR-comment renders them
as an "Upload internals" table. This answers, from one local-vs-central bench
run, whether our upload code is bound by the network or by our own process
(disk IOPS, sha256, per-PUT overhead, or concurrency starvation) — rather than
inferring it from a throughput coincidence.

## Why

The `upload_memory` phase (~20s, ~48% of a GiB pause) lumped together four very
different costs: reading the file off disk, sha256-ing each chunk, the HEAD, and
the PUT. To know if the *upload code* is inefficient we need to see inside it.

## What was added (`crates/sandbawks-node/src/s3.rs`)

The reader task now splits its wall time into **busy** (`read_hash` = disk read
+ sha256) vs **blocked** (`send_block` = waiting on the bounded channel, i.e.
the uploader is the long pole). The uploader accumulates per-op `head`/`put`
time + counts, tracks `skipped` (HEAD-hit chunks) and `max_inflight` (peak
concurrency actually reached). All via cheap atomics; behavior is unchanged.

The existing `streamed snapshot part to object store` log line gains:
`read_hash_ms, send_block_ms, head_ms, put_ms, heads, puts, skipped,
max_inflight, avg_put_ms`. (Only `memory.zst` uses `put_part_file`, so this
fires exactly for the part that matters.)

## How to read it

- `read_hash` ≈ total → **process/IOPS-bound** (the reader can't feed the
  uploader; disk + sha256 is the cap).
- `send_block` ≈ total → **transfer-bound** (uploader is the long pole; look at
  `avg_put`).
- `avg_put` = per-chunk PUT latency. If it's high on **both** local and central,
  it's per-PUT/client overhead, not the network.
- `max_inflight` < `PARALLELISM` (8) → uploader is **starved by the reader**.

## CI surfacing

- `ci/node-timings-pr-comment.mjs` parses the upload line into an "Upload
  internals" table (per-iteration + medians) alongside the phase tables.
- The bench "Node snapshot phase timings" step's grep now also captures the
  `streamed snapshot part to object store` line.

## Verification

`cargo clippy -D warnings`, `cargo fmt --check`, and the full `sandbawks-node`
test suite pass; `node --check` clean and the comment renders the new table.
