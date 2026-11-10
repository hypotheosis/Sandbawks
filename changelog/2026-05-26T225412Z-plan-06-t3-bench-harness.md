# Plan 06 T3 — pause/resume perf bench harness + CI gate

Date: 2026-05-26T22:54:12Z
Issue: #185
Parent: #182 (Plan 06 epic)

## Summary

Stands up a perf bench harness that drives the in-cluster gateway
through a repeating create → upload payload → pause (→ resume) → kill
loop, records p50 / p95 / max per phase, writes a JSON report, and
fails CI when the spec §13 latency gates regress.

The bench ships as a sibling Rust project under `tests/bench/` (not a
workspace member, per the issue body) and runs in CI as its own
Forgejo Actions workflow (`.forgejo/workflows/bench.yaml`) so perf
failures show up under a distinct check name in PR status.

## Files added

- **`tests/bench/Cargo.toml`** — standalone Cargo project (its own
  `[workspace]` block). Dependencies: `tokio` (rt, process, fs),
  `reqwest` (rustls-tls + stream), `serde` / `serde_json`, `anyhow`,
  `chrono`, `futures` (for `stream::iter` → streaming PUT body).
  Release profile mirrors the workspace's `lto = "thin"` + single
  codegen-unit so a local `cargo run --release` is comparable to CI.
- **`tests/bench/src/main.rs`** — the bench. Notable bits:
  - Config from env vars (`SANDBAWKS_API_BASE`, `SANDBAWKS_API_KEY`,
    `BENCH_ITERATIONS`, `BENCH_PAYLOAD_MIB`, `BENCH_INCLUDE_RESUME`,
    `BENCH_PAUSE_P95_MS`, `BENCH_RESUME_P95_MS`,
    `BENCH_RESULTS_PATH`). All knobs have defaults; CI overrides for
    its tighter time budget.
  - `wait_for_readyz` polls the gateway's `/readyz` for 30 s before
    the first iteration — same cadence as the existing api-smoke /
    pause-resume-smoke fixtures, so a slow port-forward doesn't blow
    up the bench.
  - One iteration = create sandbox → wait `status.envdAddress` →
    upload `payload_mib` MiB to `/tmp/payload.bin` via `/files/mint`
    PUT (streaming body; no 1 GiB buffer) → time `POST /pause` until
    `status.phase == Paused` → optionally time `POST /connect` until
    `status.phase == Running` + envd reachable → `POST /kill`.
  - Each iteration uses a fresh sandbox so a regression in one
    doesn't poison the next; a per-iteration cleanup helper kills
    the sandbox even if the iteration errors mid-flight.
  - Status polling shells out to `kubectl get sandbox -o jsonpath`
    (via `tokio::process::Command`) instead of pulling in a kube
    client, keeping the dep footprint small and the polling
    semantics observably identical to the smoke fixtures.
  - Type-7 (R / numpy default) percentile, inline (~15 lines) — no
    stats crate.
  - Unit tests for `percentile`, `PhaseStats::from_samples`, and
    `parse_env_bool` cover the bench's arithmetic without needing a
    live cluster.
- **`.forgejo/workflows/bench.yaml`** — `build-images` matrix
  (controller / api / envd — node is gVisor-only and not needed
  here) followed by a `bench` job that brings up a single-node kind
  cluster, loads the freshly-built `:sha-<sha>` images as `:dev`,
  applies CRDs + the helm chart + the `api-smoke-template`
  SandboxTemplate + TeamAPIKey fixture, backgrounds a
  `kubectl port-forward`, and runs `just bench`. `bench-results.json`
  is uploaded as a workflow artifact regardless of pass/fail so trend
  analysis can be a "fetch the JSON" away.
- **`justfile`** — `bench *EXTRA_ARGS` recipe; `cargo run --release`
  inside `tests/bench/` so the binary characteristics match CI.

## Files touched

- `justfile` — appended the `bench` recipe after the existing
  `sdk-smoke` recipe; recipes above it unchanged.

## Open decisions resolved in this PR

### Workload mechanism: file upload, not a custom startup command

The issue body offered two paths for the "~1 GiB RSS at boot"
workload:

1. Bake a python allocator into the SandboxTemplate's startup command.
2. `exec` after `Running` to fill RSS.

Both have current-codebase obstacles:

- **Path 1 doesn't fit the CRD.** The SandboxTemplate CRD
  (`deploy/crds/sandboxtemplates.sandbawks.io.yaml`) exposes
  `image` + `defaultResources` + `defaultRuntimeClass` + `envdImage`
  + `defaultTimeoutSeconds`, but no `command` / `args`. The
  controller's `pod_adapter` only stamps the single `envd`
  container; the template's `image` field isn't used for a separate
  container today. Adding a "startup command" channel would require
  a CRD + controller change well outside the scope of this task.
- **Path 2 means a WebSocket client.** `POST /sandboxes/:id/exec`
  (`crates/sandbawks-api/src/routes/exec.rs`) is a WebSocket bridge.
  Driving it from Rust would mean pulling in `tokio-tungstenite` —
  acceptable, but the issue body asked the bench's dep footprint to
  stay small ("`reqwest` + `serde_json` + an inline p50/p95 calc"),
  so this would have been a step against that intent for a workload
  that can be approximated otherwise.

The bench instead uploads a configurable payload (default 1 GiB) to
`/tmp/payload.bin` via the existing `/files/mint` PUT route. The
payload lands in the sandbox's rootfs and shows up in the
`rootfs-delta.tar.zst` member of the snapshot bundle — the same S3
chunked sink the "true RSS" path would also traverse. RSS-vs-rootfs
is a real distinction at the runsc-checkpoint level (the memory
image is separate from the rootfs delta), but the **data movement
that the latency gate is meant to govern** runs the same code path
either way. For pause this is essentially correct; for resume it
will become more relevant once forgejo #176 lands (see "Resume gate"
below), at which point the bench can be enhanced to fill RSS with a
real `exec` if the rootfs proxy turns out not to be representative.

### Resume gate: off by default; flip when #176 lands

The existing `deploy/kind/fixtures/pause-resume-smoke.sh` gates the
resume half of the smoke behind `PAUSE_RESUME_SMOKE_FULL=1` because
the in-cluster `runsc restore` path is still broken against
kubelet-running containers (issue #176). Until that lands, a
"green CI bench that always measures resume" is impossible.

The bench mirrors that gate: `BENCH_INCLUDE_RESUME` controls both
the measurement AND the 2 s p95 enforcement. Default off; CI sets
it to `'0'` explicitly. Local runs with the runtime fix in place
can flip it to `1`; the bench workflow flips at the same time #176
merges.

The pause gate (8 s p95) is enforced unconditionally — that's the
half of the spec §13 work that's exercisable today.

### CI iteration count + payload size

Issue #185 defaults: `N=20` iterations on a `~1 GiB` workload. The
local default (`just bench` with no env override) honors both — the
harness's compiled-in defaults are 20 / 1024 MiB.

CI overrides to `BENCH_ITERATIONS=5` and `BENCH_PAYLOAD_MIB=256` so
the workflow fits inside its 30-minute timeout when combined with
the kind cluster bring-up (~3 min) and the per-iteration upload
(~256 MiB through `kubectl port-forward` × 5 ≈ 5–10 min total).
The smaller CI workload still exercises the same code path; the
spec §13 gates were sized against ~1 GiB but 256 MiB regressing
above 8 s p95 would still be a real signal.

## Validation

- `just fmt` — workspace-wide; the bench's `Cargo.toml` standalone
  workspace means `cargo fmt --all` from there formats the bench
  separately. Both ran clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `(cd tests/bench && cargo clippy --all-targets -- -D warnings)` —
  clean.
- `just test` — workspace tests still green.
- `(cd tests/bench && cargo test)` — 6 unit tests pass
  (`percentile_*`, `phase_stats_*`, `parse_env_bool_truthy`).
- Cluster-level validation deliberately deferred to CI; the bench
  needs a live in-cluster gateway and the local dev path is
  documented (`SANDBAWKS_KIND_GVISOR=1 just cluster-up && just bench`).

## Not yet done — explicit non-goals

- Warm-pool cold-start gate (§13 item 4). Depends on
  `SandboxWarmPool` from Plan 07; the bench is structured so a
  future "create sandbox" trait/function can swap in without
  restructuring iterations.
- KataFc / runc-criu perf gates — Plans 08 / 09.
- Historical perf trend tracking / dashboarding (Grafana, etc.) —
  out of MVP scope. The JSON artifact is the source of truth.
- Synthetic-regression verification (set the pause threshold to 0
  ms in a draft PR; confirm CI turns red; revert before merge) — to
  be done after this PR lands and CI has a green baseline to
  contrast against.
