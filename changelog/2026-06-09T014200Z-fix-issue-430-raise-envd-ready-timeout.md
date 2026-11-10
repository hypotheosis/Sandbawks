# fix: raise ENVD_READY_TIMEOUT and CONNECT_TIMEOUT from 30s to 300s

**Date:** 2026-06-09T01:42:00Z
**Issue:** #430
**Branch:** fix-issue-430-raise-envd-ready-timeout

## Summary

Raised the two timeout constants that govern how long the API gateway
waits for a cold sandbox's `status.envdAddress` to be populated, from
30 seconds to 300 seconds. This covers worst-case cold-start image
pulls on unprimed nodes, which can take 60–300 seconds.

## Files Changed

- `crates/sandbawks-api/src/routes/envd_proxy.rs` — `ENVD_READY_TIMEOUT`
  raised from 30s to 300s; doc comment updated.
- `crates/sandbawks-api/src/routes/sandboxes.rs` — `CONNECT_TIMEOUT`
  raised from 30s to 300s; doc comment updated (including the
  round-trip estimate that referenced the old 30s value).
- `crates/sandbawks-api/src/envd_cache.rs` — Four test deadline helpers
  in `#[cfg(test)]` updated from `Duration::from_secs(30)` to
  `Duration::from_secs(300)` to stay consistent with the production
  constants.

## Root Cause

Both constants were hardcoded to 30 seconds, shorter than the typical
cold-start time (60s) and well short of the 5-minute worst case. The
waiting mechanism (`EnvdCache::wait_until_ready`) was correct; only the
constant values were wrong.

## Out of Scope

- Making the timeout configurable via Helm values or environment variable.
- `DRAIN_TIMEOUT` in `crates/envd/src/bin/envd.rs` (unrelated 30s constant).
