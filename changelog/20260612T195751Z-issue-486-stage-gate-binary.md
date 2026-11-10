# Issue #486 — `sandbawks-stage-gate` binary + distroless image + CI

**Date:** 2026-06-12
**Branch:** `feat-issue-486-stage-gate-binary`
**Parent epic:** #484 (scheduler-placed restore + Resuming recovery), Phase A

## Summary

Adds a new workspace crate, `sandbawks-stage-gate`: a tiny, no-creds init
container for scheduler-placed restore Pods. It polls for a marker file that
the node DaemonSet drops once it has finished staging a sandbox checkpoint,
then exits `0` so the Pod's main container can start, or non-zero on timeout.

It is deliberately the least-capable code in the system — file-poll only, no
S3, no Kubernetes client, no OTel exporter — because it runs inside the
*untrusted* sandbox Pod. Wiring the init container into the restore Pod spec is
out of scope (Phase D); this crate ships independently.

## Changes

### New crate `crates/sandbawks-stage-gate`

- `src/lib.rs` — `wait_for_marker(marker, timeout, poll_interval)`, a
  synchronous deadline poll over `std::fs` + `std::thread::sleep`. Uses a
  monotonic `Instant` deadline and `checked_duration_since` so it never panics
  past the deadline and never sleeps past it (the cadence is clamped to the
  remaining time). Custom `GateError::Timeout` via `thiserror`. Four unit tests
  against `tempfile` temp dirs: marker-already-present, marker-appears-mid-wait,
  never-appears-times-out, and a deadline-vs-cadence timing assertion. Plus one
  doctest on `wait_for_marker`.
- `src/bin/sandbawks-stage-gate.rs` — `clap` derive CLI with `--marker`,
  `--timeout-secs`, and an optional `--poll-interval-ms` (default 250ms, ~4
  `stat(2)`/s). Each flag also reads a `SANDBAWKS_STAGE_GATE_*` env var so the
  DaemonSet can inject via Pod env. Plain stderr `tracing` subscriber
  (`RUST_LOG`-overridable), no OTel. Exit codes: `0` marker appeared, `1`
  timeout, `2` arg-parse (clap default).
- `Dockerfile` — multi-stage cargo-chef build mirroring the envd carrier:
  chef / planner / cacher stages kept byte-identical to the other workspace
  Dockerfiles so BuildKit shares the cooker layer hash. Final image is the
  static musl binary on `FROM scratch` (no userland). Carries the same
  static-link CI guard envd uses (issue #267): a dynamic link would break exec
  inside an arbitrary customer rootfs, so the build fails loudly if the binary
  ever stops being statically linked.

### Wiring

- `Cargo.toml` — registered `crates/sandbawks-stage-gate` as a workspace member.
- All four existing Dockerfiles (controller, api, envd, node) — added the
  `COPY crates/sandbawks-stage-gate/Cargo.toml …` line to their planner stages.
  Required because the workspace now lists the crate, so `cargo chef prepare`'s
  `cargo metadata` would otherwise fail to load the manifest.
- `justfile` — `stage-gate-image-build` / `stage-gate-image-run` recipes,
  matching the `:dev`-tag convention of the other image recipes.
- `.forgejo/workflows/kind-smoke.yaml` — added a fifth `build-images` matrix
  leg (`stage-gate` → `ghcr.io/hypotheosis/sandbawks-stage-gate`) and updated the
  header comment (four → five workspace images).

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — full workspace green; the new crate's 4 unit tests + 1 doctest
  pass.
- Binary exercised directly: `--help` renders; missing marker exits `1` after
  the deadline with a structured ERROR log; present marker exits `0`
  immediately.
- `just stage-gate-image-build` — `docker build` of the new Dockerfile (vanilla
  local build, sccache on the local-disk fallback path); the static-link guard
  passes and the `scratch` image is produced.

## Out of scope

- Init-container wiring into the restore Pod spec — epic #484 Phase D.
