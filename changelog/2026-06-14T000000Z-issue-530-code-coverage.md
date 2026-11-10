# Issue #530: Code coverage CI job

**Date:** 2026-06-14  
**Branch:** `sandbawks/sandbawks-530`  
**Issues:** #530

## Summary

Added a `coverage` CI job to `.forgejo/workflows/ci.yml` and a companion
`ci/coverage-pr-comment.mjs` reporter script that together implement automated
code coverage tracking for the workspace.

## Changes

### `.forgejo/workflows/ci.yml` — new `coverage` job

- Gated on `detect-changes.outputs.rust == 'true'` (same as the existing `rust`
  job) so chart-only or Go-only PRs skip it.
- Runs inside `ghcr.io/hypotheosis/sandbawks-ci-runner:25b6631` on the `docker` runner
  with the dind sidecar, matching the kind-smoke/bench environment.
- Sets `CARGO_INCREMENTAL=0` (required for correct LLVM profdata) and
  `RUSTFLAGS='--cfg tokio_unstable'` (required by `apalis-sql`).
- Uses a `coverage-` prefixed cache key so instrumented objects never pollute
  the `cargo-` cache slot used by the plain `rust` job.
- Downloads the pinned `cargo-llvm-cov v0.6.16` musl binary (~5 s) instead of
  `cargo install`-ing it (~5 min).
- Runs `cargo llvm-cov --no-report --workspace --all-targets` for a single
  instrumented test pass, then calls `cargo llvm-cov report` twice (LCOV +
  JSON) to generate both output formats without a second test run.
- Uploads `lcov.info` as a build artifact (retained for external tooling:
  Codecov, `genhtml`, diff-coverage scripts).
- Posts / updates-in-place a per-PR coverage table comment via
  `ci/coverage-pr-comment.mjs` with an HTML marker for idempotency. The step
  runs `if: always()` so a gate breach in the next step does not retroactively
  suppress the report.
- Enforces ≥80% line coverage as the **final** step, ensuring the comment is
  always posted before any gate failure is raised.

### `ci/coverage-pr-comment.mjs` — new reporter

Parses `cargo llvm-cov report --json` output (`llvm-cov export` format) and
posts or patches a Forgejo PR comment using the same HTML-marker + Gitea REST
API pattern as `junit-pr-comment.mjs` and `bench-pr-comment.mjs`. Displays
lines / functions / regions covered/total/%, emits a `[!CAUTION]` callout when
line coverage is below the 80% gate, and exits 0 on any API hiccup (best-effort
reporter, never a CI gate by itself).

## Key design decisions

| Decision | Rationale |
|---|---|
| `--no-report` + two `report` calls | Single instrumented run avoids running tests twice; profdata in `target/llvm-cov-target/` is reused |
| `coverage-` cache key prefix | Instrumented artifacts embed LLVM profiling stubs incompatible with plain debug/release objects |
| Binary download over `cargo install` | Cold install time ~5 s vs ~5 min; version is pinned so format divergence is explicit |
| Comment before gate | Gate is last so `if: always()` on the comment step guarantees the report posts even when coverage is below the floor |
| `runs-on: docker` + ci-runner container | Matches kind-smoke/bench environment; DinD sidecar available for future testcontainers-backed integration tests |
