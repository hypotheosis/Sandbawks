# Auto-post node phase-timing table to the PR + run bench against local garage

**Date:** 2026-06-06T13:46:46Z

## Summary

1. Every bench run now renders the node-side snapshot pipeline per-phase
   timings into an upserted PR comment (a table per pause / validate / stage,
   with per-iteration rows + medians and effective MB/s).
2. The bench job is temporarily pointed at the **local single-node garage**
   (`KIND_SMOKE_REMOTE_SNAPSHOT_STORE='0'`) to capture local timings for the
   local-vs-central A/B comparison.

## 1. Node phase-timings PR comment

`crates/sandbawks-node` emits one structured `… phase timings (ms)` line per
pause / `PauseValidate` / `Stage`. The bench job already grepped them into the
log via the "Node snapshot phase timings" step, but a reviewer had to read raw
CI logs to see the breakdown. Now:

- `ci/node-timings-pr-comment.mjs` (new, sibling to `bench-pr-comment.mjs`,
  dependency-free Node 20) parses those lines and renders three markdown
  tables — Pause pipeline (checkpoint / compress / upload_memory / ↑rate /
  total), Pause validate (re-download / ↓rate / re-decompress / total), Resume
  stage (download / ↓rate / decompress / total) — each with a median row, plus
  a "full pause = pause()+validate" median and a `store: local|central garage`
  footer. Idempotent via an HTML marker; best-effort (always exits 0).
- The "Node snapshot phase timings" step now `tee`s the grepped lines to
  `node-phase-timings.log`, and a new "Comment node phase timings on PR" step
  (`if: always() && pull_request`) renders + upserts the comment, mirroring the
  existing bench-timings comment.

This complements the bench-timings comment (end-to-end p50/p95): that shows
*how long* pause/resume took; this shows *where the time went*.

## 2. Local-garage bench run

The bench job's `KIND_SMOKE_REMOTE_SNAPSHOT_STORE` is flipped to `'0'` so the
next run uses the in-cluster single-node garage. This gives the local half of
the comparison against the already-captured central numbers (central GiB pause
median ≈ 41.7s; upload ≈ 53 MB/s, download ≈ 99 MB/s — a ~1.9× asymmetry
showing the uplink, not the backend, caps upload). **Revert to `'1'` once the
local numbers are captured.**

## Verification

`node ci/node-timings-pr-comment.mjs` was run against the recorded central
output and produced the expected tables (full-pause median 41.67 s, upload
~53 MB/s, download ~99 MB/s). `node --check` clean; workflow YAML parses.
