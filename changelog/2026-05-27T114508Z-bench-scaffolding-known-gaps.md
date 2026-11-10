# Bench harness scaffolding — known gaps documented

Date: 2026-05-27T11:45:08Z
PR: #192
Issue: #185

## Summary

The PR #192 bench harness lands as **scaffolding**, not a fully-
working perf gate. CI runs the harness end to end against a real
gVisor-backed kind cluster and produces a JSON report, but the
latency numbers it captures are dominated by control-plane overhead
rather than the workload — for reasons that are now product bugs
rather than bench bugs.

Two issues filed alongside the merge document the remaining gaps:

- **#193** — Snapshot
  bundle is essentially empty on gVisor pause regardless of workload.
  After uploading 256 × 1 MiB of incompressible bytes to `/tmp` inside
  the sandbox, every pause produces `memory.zst = 18 B`,
  `rootfs-delta.tar.zst ≈ 270 B`, `envd-state.json = 51 B`. zstd
  cannot compress incompressible bytes — the rootfs-delta runsc /
  containerd actually produced contained none of the workload's
  state. Most likely cause: gVisor mounts an internal tmpfs at
  `/tmp` by default, so writes never reach the containerd overlayfs
  upper layer the rootfs-delta is diffed from. Also worth checking
  whether the node DaemonSet's `memory*` tar glob matches what runsc
  actually drops in its work dir, since memory.zst is also empty.
- **#194** — envd
  exits cleanly (Exit Code 0) mid-pause on the Nth iteration. Three
  consecutive pauses succeed; the fourth's envd terminates ~13 s
  into its lifetime with no shutdown log and no error trace. The
  `/__state` probe then times out because the listener is gone. The
  bench's CI defaults are dialed down to N=3 to stay under this
  threshold for now.

## What was tuned for the scaffolding-state merge

- `BENCH_ITERATIONS=3` (down from 5) — sits under #194's flake
  threshold while still giving enough samples for the type-7
  percentile math.
- `BENCH_PAYLOAD_MIB=16` (down from 256) — the data doesn't end up
  in the snapshot anyway per #193, so the larger payload was just
  burning CI time on uploads. 16 MiB still exercises the gateway →
  envd file-IO path enough to catch regressions.
- Module doc + workflow comments updated to call out the
  scaffolding state and link the follow-up issues.

## What the bench still validates (the value it ships with)

Even with the measurement-quality gaps, the harness is a real
regression detector for:

- The bench's own arithmetic + JSON output (six original + four
  SplitMix64 unit tests, total 10).
- Cluster bring-up: kind + runsc-baked node image + Garage S3
  fixture + helm install with the gVisor wiring + node DaemonSet
  rollout.
- The `api-smoke-template` SandboxTemplate / TeamAPIKey fixture
  shape.
- The full chunked-upload → /files/mint → envd WriteAll path under
  the api gateway's 30 s request timeout and 4 MiB tonic ceiling.
- Controller dispatch of pause → Pausing → SnapshotJob → Complete /
  Failed, with the matching state machine transitions.
- Snapshot sink S3 writes (Garage receives the manifest + part
  blobs even if they're tiny).
- The bench JSON report → workflow artifact upload pipeline.

## What needs to happen to flip from scaffolding to a real gate

Once #193 lands a fix that puts the workload in the snapshot:

1. Raise `BENCH_PAYLOAD_MIB` in `.forgejo/workflows/bench.yaml` —
   16 → 256, or 1024 once the perf budget allows.
2. The latency numbers will become meaningful; spec §13 ceilings
   (`BENCH_PAUSE_P95_MS=8000`, `BENCH_RESUME_P95_MS=2000`) will be
   real gates rather than easy-to-clear thresholds.

Once #194 lands a fix that lets pause cycle indefinitely:

1. Raise `BENCH_ITERATIONS` in `.forgejo/workflows/bench.yaml` —
   3 → 20 (the issue's specified count).
2. p95 becomes meaningful with 20+ samples; 3 samples leans
   heavily on max.

Once #176 lands the runtime-side runsc-restore fix:

1. Flip `BENCH_INCLUDE_RESUME=1` in the workflow.
2. The 2 s resume p95 gate becomes enforceable; the harness already
   has the measurement code path in place.
