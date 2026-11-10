# Tunable S3 chunk concurrency (`SANDBAWKS_S3_PARALLELISM`)

**Date:** 2026-06-06T14:41:34Z

## Summary

The snapshot transfer's in-flight chunk concurrency is now overridable via
`SANDBAWKS_S3_PARALLELISM` (default unchanged: 8), exposed as the chart value
`node.s3Parallelism`. The bench job sets it to 32 to test whether upload
throughput scales with concurrency.

## Why

The local-vs-central GiB bench proved the snapshot upload is **process /
per-request-latency-bound, not network-bound**: loopback (local garage) was
*slower* than the central cluster (upload ~42 vs ~53 MB/s), download was
identical (~95 MB/s), and the new upload-internal counters showed `avg_put ≈
655 ms for a 4 MiB chunk on loopback` with `read_hash ≈ 3.4 s` (disk+sha not
the bottleneck), `send_block ≈ 20 s` (uploader is the long pole), and
`max_inflight` pegged at 8. When you're latency-per-request bound with
concurrency saturated, throughput scales ~linearly with concurrency until the
backend saturates — so the obvious lever is raising the in-flight cap.

## Change

- `crates/sandbawks-node/src/s3.rs`: added `effective_parallelism()` — reads
  `SANDBAWKS_S3_PARALLELISM` once (`OnceLock`), validates ≥ 1, falls back to the
  `PARALLELISM` const (8). All runtime sites (the upload/download
  `buffer_unordered`/`buffered` factors, the bounded-channel depths, and the
  in-flight admission cap) now use it. The upload log line gains a
  `parallelism` field next to `max_inflight`.
- `deploy/helm/sandbawks`: `node.s3Parallelism` value (default `""` → binary
  default) → conditional `SANDBAWKS_S3_PARALLELISM` env on the node DaemonSet.
- `.forgejo/workflows/kind-smoke.yaml`: the bench's gVisor helm install sets
  `--set node.s3Parallelism=32`.
- `ci/node-timings-pr-comment.mjs`: the Upload-internals table gains an
  `N (cfg)` column so configured concurrency is visible next to the achieved
  `max_inflight` (N == max_inflight ⇒ saturated; max_inflight < N ⇒ the reader
  is starving the uploader).

## Caveat

Peak node RSS during transfer is `~(2N + 1) · CHUNK_SIZE` (≈ 68 MiB at 8, ≈
260 MiB at 32). Keep `node.s3Parallelism` well under the DaemonSet's memory
limit (1Gi); 32 is comfortable.

## What the next bench run tells us

- Upload time drops roughly in proportion to N (≈ 25 s → ~6–7 s at 32) ⇒
  concurrency was the cap; promote a higher default.
- Upload time barely moves and `avg_put` climbs ⇒ garage itself is the
  per-request bottleneck; concurrency won't save us and the lever moves to the
  validate re-download / chunk size / backend.

## Verification

`cargo clippy -D warnings`, `cargo fmt --check`, full `sandbawks-node` tests,
`node --check`, and `helm template` (env present at 32, absent when unset) all
pass.
