# Default CI snapshot backend to local garage

**Date:** 2026-06-06T17:01:29Z

## Change

Both the `kind-smoke` and `bench` jobs now default
`KIND_SMOKE_REMOTE_SNAPSHOT_STORE` to `'0'` (in-cluster single-node garage).
Flip to `'1'` to re-test against the central cluster.

## Why

The slow-pause/resume A/B concluded the local slowness was *our own fixture*
starving garage of CPU (a 1-core limit, since raised to 8). With that fixed,
the central multi-node cluster showed no performance advantage, and local is
self-contained (no `SNAPSHOT_BUCKET`/`SCCACHE_*`/`AWS_*` dependency, no shared
persistent bucket). So CI stays on local garage by default; the central path
and its toggle remain available for ad-hoc verification.
