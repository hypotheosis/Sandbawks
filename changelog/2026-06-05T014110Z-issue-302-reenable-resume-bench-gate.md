# Re-enable `BENCH_INCLUDE_RESUME=1` in the GiB-scale CI bench gate

**Issue:** #302 — GiB-scale resume
fails: restored envd exits immediately, Sandbox stuck `Resuming`.

**Depends on:** #325 (merged) — stop
envd self-exiting at 60s.

## What changed

`.forgejo/workflows/kind-smoke.yaml` `bench` job: flipped
`BENCH_INCLUDE_RESUME` from `'0'` to `'1'` and rewrote the explanatory comment
block to match the corrected root cause. No other knobs touched — the resume
budget (`BENCH_RESUME_BUDGET_SECS=300`) and the provisional resume p95 ceiling
(`BENCH_RESUME_P95_MS=180000`) were already in place and DORMANT; this flip
makes them LIVE again.

With `BENCH_INCLUDE_RESUME=1` the bench drives the full
create → upload → pause → kill → `/connect` loop and measures resume wall-clock,
rather than the pause-only create → upload → pause → kill loop that ran while the
gate was off.

## Why now

When the GiB-scale payload landed (#291 / PR #292), resume was gated off at
`'0'` because the restored envd exited 0 (`Completed`) instead of staying up,
leaving the Sandbox stuck `Resuming`. The original hypothesis in #302 was a
pause-time race: a ~49 s `runsc checkpoint` overlapping envd's 30 s
graceful-shutdown drain, so the snapshot captured envd mid-exit.

The real root cause — diagnosed on the #325 bench run — was different. In
`crates/envd/src/bin/envd.rs`, the gRPC serve task was wrapped in
`tokio::time::timeout(DRAIN_TIMEOUT, serve_task)` **without first awaiting a
shutdown signal**. `tokio::time::timeout` starts its clock when polled, so the
30 s cap bounded the *serving lifetime*, not the post-signal drain. With no
signal ever sent, envd logged `server drain timed out after 30s` at boot+30 s
and exited 0 at boot+60 s — no SIGTERM, no `Killing` event, no `shutdown signal
received` line. This matches #302's tell exactly: the restored process jumps
straight to the drain-timeout log with no preceding signal line.

The scale dependence falls out of timer-vs-checkpoint timing: at GiB the slow
checkpoint captures envd already past its spurious boot+30 s drain start, so the
restore replays the exit; at 16 MiB the fast checkpoint finishes before the
boot+30 s / +60 s timers fire, so small payloads appeared to work.

#325's `join_with_drain_cap(task, gate, cap)` gates the drain timeout on the
shutdown broadcast: until a signal fires the gate blocks and both servers serve
unbounded; once it fires, `cap` bounds only the drain (the original intent). A
checkpointed envd is therefore captured healthy and resumes serving regardless
of checkpoint duration.

## Verification

This is a CI-workflow-only change (one env var + a comment block). It is
verified by the `bench` job itself running green with resume measured in the PR
CI, and by the per-run resume p95 published to the PR via
`ci/bench-pr-comment.mjs`. No Rust changed; no local `cargo` step applies.

## Follow-ups / caveats

- A separate node OOM in the bench's `Validating` step at GiB scale is tracked
  in #329; it is an independent
  root cause from #302 and may still need attention for a fully clean GiB bench
  run.
- The provisional resume p95 ceiling (`180000` ms) is intentionally generous
  for this constrained runner; tighten toward the spec §13 2 s target once a
  few GiB runs establish a baseline, or move the tight gate to a dedicated
  heavier bench runner (#291 option (b)).
