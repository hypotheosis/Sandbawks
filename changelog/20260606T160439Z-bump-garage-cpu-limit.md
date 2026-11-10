# Bump local garage CPU limit (1 → 8) — perf investigation

**Date:** 2026-06-06T16:04:39Z

## Why

Three independent results converged on the local garage's CPU limit being the
pause/resume upload bottleneck:

1. **Concurrency doesn't help.** N=8→32 left upload flat (~25 s, ~43 MB/s) while
   `avg_put` scaled ~4× — saturated resource.
2. **Chunk size doesn't help.** 4→16 MiB left upload flat (~24.6 s) and
   `avg_put` scaled ~4× with the 4× bytes — cost is **byte-proportional**, not
   fixed-per-object.
3. **CPU split.** Per-pod CPU (kubelet summary) over the bench window: our
   `sandbawks-node` is *not* CPU-bound (0.33 of its 2 cores), but garage's
   228 CPU-s is consistent with it pegged at its **1-core limit** during the
   ~235 s of actual transfer.

Byte-proportional + serialized + one core = garage's content-addressed block
hashing pinned to a single CPU at ~43 MB/s, immune to client concurrency and
chunk size. And that 1-core limit was set by **our own test fixture**
(`install-garage.sh`), so the "local garage is slow" A/B was partly measuring a
throttle we imposed (assume-our-fault-first).

## Change

`deploy/kind/fixtures/install-garage.sh`: garage container
`limits.cpu` 1 → 8 (requests 50m → 1; memory 512Mi → 2Gi for headroom). 8 ≥ the
default client `PARALLELISM` (8) so each in-flight PUT can land on its own garage
core; the CI build host has 32 cores. Also reverted the bench's
`node.s3ChunkMib=16` override back to the 4 MiB default so garage CPU is the only
variable vs the original N=8 / 4 MiB / garage-1cpu local baseline.

## Prediction / how to read the next run

- Upload ~25 s → **single-digit seconds** ⇒ garage was CPU-core-throttled (by
  our fixture); the local slowness was largely self-inflicted. Promote a saner
  default garage sizing and re-confirm central.
- Upload **still ~25 s** ⇒ garage's write path serializes on something other
  than CPU (LMDB write lock / single fsync / disk), so more cores can't help —
  pivot to the validate re-download removal and accept the per-write floor.

## Note

This is a kind test-fixture change only (production garage sizing is separate).

## Verification

`bash -n` + `shellcheck` on the fixture; workflow YAML parses; chunk override
removed.
