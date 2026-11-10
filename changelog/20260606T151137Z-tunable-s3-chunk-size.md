# Tunable S3 chunk size (`SANDBAWKS_S3_CHUNK_MIB`)

**Date:** 2026-06-06T15:11:37Z

## Summary

The snapshot transfer chunk size is now overridable via
`SANDBAWKS_S3_CHUNK_MIB` (default unchanged: 4 MiB), exposed as the chart
value `node.s3ChunkMib`. The bench job sets it to 16 MiB to test whether
fewer/larger objects cut upload time.

## Why (the concurrency experiment ruled itself out)

The N=32 GiB bench (local garage) showed concurrency is **not** the lever:
quadrupling `PARALLELISM` (8→32) left upload flat (~25 s, ~43 MB/s) while
`avg_put` scaled ~4× (655 ms → 2,728 ms). That's classic backend saturation
(Little's Law) — garage effectively serializes writes (~82 ms per 4 MiB chunk:
LMDB + fsync), so more in-flight requests only queue. We were already past the
knee at N=8.

When the per-write cost is fixed-per-object rather than byte-proportional, the
remaining upload lever is **fewer objects** — i.e. a larger chunk size. 257 ×
4 MiB chunks → ~64 × 16 MiB chunks is ¼ the PUT/HEAD count. This run measures
whether that cuts upload ~4× (fixed-overhead-bound) or barely moves it
(byte-bound).

## Change

- `crates/sandbawks-node/src/s3.rs`: added `effective_chunk_size()` — reads
  `SANDBAWKS_S3_CHUNK_MIB` (whole MiB) once via `OnceLock`, validates ≥ 1,
  falls back to the `CHUNK_SIZE` const (4 MiB). The two write-path sites
  (`put_part`'s `bytes.chunks(...)` and `put_part_file`'s read buffer) use it.
  The read path is unchanged — it uses the chunk sizes recorded in each part
  index, so bundles written at one chunk size still restore after the value
  changes.
- `deploy/helm/sandbawks`: `node.s3ChunkMib` value (default `""`) → conditional
  `SANDBAWKS_S3_CHUNK_MIB` env on the node DaemonSet.
- `.forgejo/workflows/kind-smoke.yaml`: the bench install now sets
  `--set node.s3ChunkMib=16` and drops the `node.s3Parallelism=32` override
  (back to the default 8) so chunk size is the only variable vs the prior
  N=8 / 4 MiB local baseline.

## Caveats

- Larger chunks coarsen content-addressed dedup (a chunk only dedups against an
  identical same-size chunk).
- Peak RSS during transfer is `~(2N + 1) · chunk` — at N=8 / 16 MiB ≈ 272 MiB,
  fine under the 1Gi node limit. Combining a high `s3Parallelism` with a large
  `s3ChunkMib` can blow the limit (e.g. 32 × 16 MiB ≈ 1 GiB), so tune them
  together.

## What the next run tells us

- Upload ~25 s → ~6–8 s ⇒ per-PUT cost is fixed-overhead-bound; larger chunks
  are a universal win (consider raising the default).
- Upload barely moves ⇒ byte-bound; chunk size won't help and the lever moves
  to the validate re-download removal.

## Verification

`cargo clippy -D warnings`, `cargo fmt --check`, full `sandbawks-node` tests,
and `helm template` (env present at 16, absent unset) all pass.
