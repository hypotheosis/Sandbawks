# Plan 03 T1 — Workspace + crates/envd scaffold

**Date:** 2026-05-23
**Issue:** #54
**Epic:** #53 — Plan 03 envd-rs

## What landed

A new workspace member, `crates/envd`, plus the shared dependency
plumbing later Plan 03 tasks need. The crate intentionally only ships
empty module stubs at this stage — the goal is to fix the layout so
that proto vendoring (T2), codegen wiring (T3), and per-RPC
implementations (T4+) can plug into a stable surface without further
churn at the workspace level.

This mirrors the staged scaffold approach already used for
`sandbawks-controller` in Plan 02 T1 (issue #29).

## Changes

- `Cargo.toml`
  - Added `"crates/envd"` to `[workspace] members`.
  - Added `[workspace.dependencies]` entries that downstream tasks
    will consume:
    - gRPC / proto: `tonic` (transport + tls + codegen),
      `tonic-build`, `prost`, `prost-types`, `tokio-stream`,
      `tokio-util` (io feature), `bytes`.
    - Unix / process: `nix` (fs, process, signal, term features).
    - Test utilities: `tempfile`.
- `crates/envd/Cargo.toml` — declares `[lib]` and `[[bin]] name =
  "envd"`. No `[dependencies]` yet; T2+ will add `tonic` etc. as
  they're actually used.
- `crates/envd/src/lib.rs` — module map + `pub const CRATE_NAME: &str
  = "envd";`. Declares `proto`, `filesystem`, `process`, `pty`, and
  `state` modules.
- `crates/envd/src/{proto,filesystem,process,pty,state}.rs` —
  doc-comment-only stub files so the lib actually compiles. Each
  comment names the task that will fill the module in.
- `crates/envd/src/bin/envd.rs` — stub `fn main()` that prints
  `envd::CRATE_NAME`. Exists so `cargo build -p envd --bin envd`
  succeeds.
- `crates/envd/tests/smoke.rs` — single test asserting
  `envd::CRATE_NAME == "envd"`. Its job is to fail the build if the
  `[lib]` target or workspace-member wiring ever regresses, not to
  test real behaviour.

## Validation

All four acceptance criteria from the issue pass locally:

- `cargo metadata --format-version 1 --no-deps` lists `envd` as a
  workspace member alongside `sandbawks-types` and
  `sandbawks-controller`.
- `cargo test -p envd --test smoke` — 1 passed, 0 failed.
- `cargo build -p envd --bin envd` — clean build.
- `cargo build --workspace` — all three crates build, no warnings.

Workspace gate also clean:

- `just fmt` (rustfmt all-in-place) — no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` (workspace test) — all suites green, including the new
  envd smoke.

## Out of scope (deferred to later Plan 03 tasks)

- Vendoring the E2B-compatible `.proto` files (T2).
- Wiring `tonic-build` into `crates/envd/build.rs` (T3).
- Any actual `FilesystemService` / `ProcessService` / `PtyService`
  implementation (T4+).
- `envd` Dockerfile + image build (later Plan 03).
