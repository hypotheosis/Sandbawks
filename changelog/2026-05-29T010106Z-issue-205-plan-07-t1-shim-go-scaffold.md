# Plan 07 T1 — `shim/` Go subtree scaffold (issue #205)

**Date:** 2026-05-29T01:01:06Z
**Branch:** `feat-issue-205-shim-go-scaffold`
**Issue:** #205 (epic #204)
**Plan:** `docs/superpowers/plans/2026-05-28-sandbawks-07-shim-wrapper-resume.md`, Task 1

## Summary

Introduces the first Go code into this otherwise all-Rust repository: the
`shim/` subtree that will, in later Plan 07 tasks, become
`containerd-shim-sandbawks-v1` — a containerd runtime shim wrapping gVisor's
`CreateWithFSRestore` + `Restore` extension RPCs so kubelet-managed
containers can be born already restored from a checkpoint.

Task 1 is scaffold only: no gVisor dependency, no restore logic. It
establishes the module, a runnable stub binary, the build recipe, and the
CI plumbing so the new language is exercised on every PR that touches it.

## What landed

- **`shim/README.md`** — rationale for one Go binary in a Rust repo
  (gVisor's reusable shim packages are Go; wrapping them is far cheaper than
  a Rust reimplementation), the layout, and how to build. Notes the Rust
  workspace neither builds nor links the subtree.
- **`shim/go.mod`** — module `github.com/hypotheosis/sandbawks/shim`, Go 1.23. No
  dependencies yet (gVisor pin arrives in T2), so no `go.sum`.
- **`shim/cmd/containerd-shim-sandbawks-v1/main.go`** — stub that prints
  `containerd-shim-sandbawks-v1 (scaffold)` and exits 0.
- **`justfile`** — `shim-build` recipe: `go build` into
  `target/shim/containerd-shim-sandbawks-v1` (under the already-gitignored
  shared `target/` tree).
- **`.gitignore`** — `shim/vendor/` (build input, reproducible from
  `go.mod`/`go.sum`; not checked in if we ever `go mod vendor`).
- **`.forgejo/workflows/ci.yml`** — added a `go` path filter
  (`shim/**` + the workflow file) to `detect-changes`, and a new `go` job
  gated on it that runs `go build`, `go vet`, and `go test` against the
  subtree. The Go version is read from `shim/go.mod` so there is one source
  of truth for the toolchain. This is the project-specific addition the
  issue called out: CI had no Go awareness, so without it a broken shim
  would never be caught (the `rust` job never compiles the subtree).

## Why the CI change (beyond the issue's literal file list)

The `rust` job compiles the Cargo workspace, which deliberately excludes
`shim/`. The shim therefore has zero CI coverage until a dedicated job
builds it with the Go toolchain. The `go` job mirrors the `rust` job's
shape — path-filtered, `needs: detect-changes`, fully-qualified `uses:`
URLs — so it skips on Rust-only PRs and reports green-by-skip rather than
stuck-pending. Caching is off until T2 adds dependencies and a `go.sum` to
key the module cache on.

## Validation

- `just shim-build` → binary at `target/shim/containerd-shim-sandbawks-v1`;
  running it prints `containerd-shim-sandbawks-v1 (scaffold)` and exits 0.
- `gofmt -l shim/` → clean; `cd shim && go vet ./...` → clean;
  `cd shim && go test ./...` → no test files, exits 0.
- `cargo fmt --all -- --check` → clean (no Rust changes).
- `cargo clippy --workspace --all-targets -- -D warnings` → clean.
- `cargo test --workspace --all-targets` → green; the workspace is
  unaffected by the new subtree (Task 1 smoke criterion).
- `.forgejo/workflows/ci.yml` parses as valid YAML.

## Deliberately not here

Everything Plan 07 T2+ delivers: the gVisor dependency pin, the
`internal/wrapper` pass-through decorator, annotation-driven dispatch to
`CreateWithFSRestore` / `Restore`, the kind-node image stage, and the
`sandbawks-runsc` RuntimeClass.
