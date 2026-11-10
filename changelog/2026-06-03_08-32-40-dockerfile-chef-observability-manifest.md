# Fix: copy `sandbawks-observability` manifest into Docker `cargo chef` planner

**Date:** 2026-06-03 08:32:40
**Branch:** `feat-issue-295-otel-bootstrap-crate`

## Problem

The `build-images` CI step failed at the `planner` stage of every service
Dockerfile:

```
[planner 7/7] RUN cargo chef prepare --recipe-path recipe.json
Error: Failed to compute recipe
Caused by: Cannot extract Cargo metadata
  `cargo metadata` exited with an error: failed to load manifest for
  workspace member `/src/crates/sandbawks-api`
  Caused by: failed to load manifest for dependency `sandbawks-observability`
  Caused by: failed to read `/src/crates/sandbawks-observability/Cargo.toml`
  Caused by: No such file or directory (os error 2)
```

Phase 1 of the OTel work (#295) added a new workspace member,
`crates/sandbawks-observability`, and made `sandbawks-api` / `sandbawks-node`
depend on it. The `cargo chef` planner stage copies each member's `Cargo.toml`
individually (so the dependency cache layer is invalidated only when a manifest
changes) and then runs `cargo chef prepare`, which shells out to
`cargo metadata`. Because the new crate's manifest was never copied,
`cargo metadata` could not resolve the workspace and the build aborted.

## Fix

Added the missing manifest copy to the planner stage of all four service
Dockerfiles, immediately before `RUN cargo chef prepare`:

```dockerfile
COPY crates/sandbawks-observability/Cargo.toml crates/sandbawks-observability/Cargo.toml
```

Files changed:

- `crates/sandbawks-api/Dockerfile`
- `crates/sandbawks-node/Dockerfile`
- `crates/sandbawks-controller/Dockerfile`
- `crates/envd/Dockerfile`

The later build stages already use `COPY . .` for the real source tree, so they
pick up the new crate automatically; the `cacher` (`cargo chef cook`) stage only
needs the manifests, which `cargo chef` turns into stub sources. No further
changes were required.

## Verification

- `cargo metadata --no-deps` resolves the workspace cleanly.
- No `.dockerignore` rule excludes the new crate.
