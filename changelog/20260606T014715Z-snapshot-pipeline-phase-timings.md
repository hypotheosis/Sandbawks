# Snapshot pipeline: per-phase timing instrumentation

**Date:** 2026-06-06T01:47:15Z

## Summary

The `sandbawks-node` pause / resume pipeline now emits a single structured
per-phase timing line per operation, so the cost of every sub-step (runsc
checkpoint, compress, S3 upload, validate re-download, decompress, runsc
restore, …) is visible from `kubectl logs ds/sandbawks-node` without
guessing from coarse end-to-end numbers. The bench job prints these lines on
every run.

## Motivation

Pointing the bench at the central garage cluster showed pause/resume was no
faster than a local single-node garage (p50 pause 44s / resume 20s for
1 GiB). That ruled out the storage backend as the bottleneck, but the only
instrumentation was the S3 module's per-part `elapsed_ms` — the checkpoint,
compress, validate re-download, decompress, and restore costs had to be
inferred from log timestamps. This adds first-class timing so the next
optimization targets the measured bottleneck instead of a guess.

## What was added

- A small `PhaseTimer` helper in `crates/sandbawks-node/src/gvisor.rs`:
  `start()` / `mark(name)` / `rendered()` / `total_ms()`. Each `mark` records
  the wall-clock ms since the previous mark; the set is emitted as one
  `phases="name=ms name=ms …" total_ms=…` tracing line. Diagnostic only — no
  control-flow impact, one `Instant::now()` per mark.
- **`pause()`** emits `pause pipeline phase timings (ms)` with:
  `resolve_pod, mount_check, envd_fetch, probe_drain, checkpoint,
  verify_postconditions, compress, upload_memory, rootfs_delta, envd_upload,
  envd_digest, manifest_upload` plus `bytes_memory` (to derive MB/s).
- **`fetch_verify_and_lay_out()`** (the shared read path used by
  pause-validate, resume-stage, and the resume fallback) emits
  `snapshot fetch/verify/lay-out phase timings (ms)` with `manifest_fetch,
  preflight, memory_download, memory_decompress, small_parts` and a `caller`
  field (`PauseValidate` / `Stage` / `Resume`) so each line is attributable.
- **`resume()`** (shim-not-installed fallback) emits `resume pipeline phase
  timings (ms)` with `resolve_pod, fetch_verify, restore, envd_ready`.
- A `Node snapshot phase timings` step in the bench job
  (`.forgejo/workflows/kind-smoke.yaml`, `if: always()`) greps and prints the
  timing lines after every bench run — the existing `Dump diagnostics` step
  only fires on failure.

## Reading the output

```
pause pipeline phase timings (ms)  sandbox=sbx-… bytes_memory=1074292943 \
  phases="resolve_pod=2 mount_check=5 envd_fetch=11 probe_drain=250 \
  checkpoint=4203 verify_postconditions=8 compress=1100 upload_memory=19847 \
  rootfs_delta=120 envd_upload=40 envd_digest=15 manifest_upload=30" \
  total_ms=40210
```

`phases=` and `total_ms=` are awk-able to aggregate across the bench's 5
iterations.

## Notes

- Timings are logged on the success path only; a phase that errors `?`-returns
  before the emit (the error itself is already logged). Perf analysis cares
  about successful runs.
- On the shim resume path (what the bench uses) the actual `runsc restore`
  runs in the kubelet container, not the node process — so resume's node-side
  cost shows up under the `Stage` line (`fetch_verify_and_lay_out`), while the
  restore proper is visible in the sandbox pod's events/logs.
