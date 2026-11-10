# envd static-link CI guard (issue #267)

- **Date:** 2026-06-02T17:00:00Z
- **Issue:** #267 — *envd: CI guard that envd stays a fully static binary*
- **Sub-issue of:** #265 (init-container injection model)
- **Follow-up to:** PR #266

## Problem

The init-container injection model copies the `envd` binary out of the
envd image and runs it inside an **arbitrary** customer rootfs. This only
works if `envd` is fully statically linked — no dependency on the template
image's libc or dynamic loader. It is today (`crates/envd/Dockerfile`
builds the `x86_64-unknown-linux-musl` target with
`-C target-feature=+crt-static`), but nothing prevented a future
dependency from silently reintroducing a dynamic link, which would break
cold start across **every** template at once.

## Change

Added a static-link assertion to the `builder` stage of
`crates/envd/Dockerfile`, immediately after the `cargo build` step that
produces the binary:

- Installs `file` in the **builder** stage only (not the shared `chef`
  stage), so the `chef` / `planner` / `cacher` stages stay byte-identical
  to the controller / api Dockerfiles and BuildKit keeps sharing the
  cooker layer hash across all three workspace image builds.
- A dedicated `RUN` runs `file -b` on the built binary and fails the build
  unless the output reports `statically linked` (the current non-PIE
  build, since `-C relocation-model=static` disables PIE) or
  `static-pie linked` (tolerated in case the relocation model is relaxed
  later). Both mean no dynamic loader is required. Anything reporting a
  dynamic interpreter exits non-zero.

## Why this location

The guard lives at the exact point the production binary is produced, so
it costs no extra compile and guards the real artifact rather than a
re-built proxy. It is exercised on every build path:

- locally via `just envd-image-build`;
- in CI via the `build-images` matrix in
  `.forgejo/workflows/kind-smoke.yaml` — the `envd` leg builds the
  Dockerfile on every `pull_request` and `push`, **including fork PRs**
  (which build but don't push), so a regression fails the pipeline.

As a dedicated layer keyed on the build output, the assertion re-runs
whenever the binary changes and is served from cache (already verified)
otherwise.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — all workspace tests pass.
- Guard case-matching logic unit-checked against a real dynamic binary
  (rejected) and both static wordings (accepted).
- `just envd-image-build` — full image build succeeds; the guard prints
  `OK: envd is statically linked` on the current build.

## Acceptance criteria (issue #267)

- [x] CI fails if `envd` becomes dynamically linked — the builder-stage
      `RUN` exits non-zero, failing the `build-images` job.
- [x] Passes on the current static build.
