# Issue #130 — CI speedup phase 2: wire sccache (S3-backed) into the workspace Dockerfiles

Date: 2026-05-24

## Summary

Issue #130 — bake an
`RUSTC_WRAPPER=sccache` layer into all three workspace Dockerfiles
(`sandbawks-controller`, `sandbawks-api`, `envd`) so the CI image-build
flow caches per-`rustc`-invocation outputs in S3 on top of the
cargo-chef dependency-layer cache landed in PR #129. Bundled with the
deferred follow-up to PR #132 / issue #131: pin every workflow that
builds these images (`kind-smoke.yaml`, `api-image.yaml`,
`controller-image.yaml`, `envd-image.yaml`) to the new ci-runner short
SHA (`25b6631`) and pass `--build-arg BUILDER_IMAGE=…` so the chef
stage reuses the pre-baked toolchain instead of rebuilding it on every
run.

## Changes

### Workspace Dockerfiles (controller / api / envd)

Mirroring the pattern from `~/repos/gfd/wnw/wnw-backend/Dockerfile`:

- **`chef` stage**: install sccache 0.10.0 with a
  `command -v sccache || (curl … | tar …)` short-circuit so the
  ci-runner image (which already ships the same pinned binary, see
  `ci/runner/Dockerfile`) skips the install and a vanilla
  `docker build .` on a developer workstation falls through to the
  upstream tarball.
- **`chef` ENV**: `RUSTC_WRAPPER=sccache`, `CARGO_INCREMENTAL=0`
  (mandatory — incremental is incompatible with sccache),
  `AWS_EC2_METADATA_DISABLED=true` (avoids a 5 s IMDS timeout on
  non-EC2 builders).
- **`SCCACHE_*` ARGs are NOT promoted to ENV in chef.** sccache
  enables its S3 backend the moment `SCCACHE_ENDPOINT` is present in
  env — even when empty — and then panics with `InvalidUri(Empty)`,
  breaking the developer-workstation no-args build. The
  `SCCACHE_BUCKET` / `SCCACHE_ENDPOINT` / `SCCACHE_REGION` ARGs are
  re-declared in the `cacher` and `builder` stages instead, and gated
  by an `if [ -n "${SCCACHE_BUCKET}" ]; then … else unset
  RUSTC_WRAPPER; fi` guard inside the RUN so the developer path
  bypasses sccache entirely while CI exports the full S3 config plus
  AWS creds.
- **`planner` stage**: `ENV RUSTC_WRAPPER=` to skip sccache for the
  `cargo metadata` round-trip — that throwaway invocation has no
  secrets mounted and would otherwise stall trying to auth to S3.
- **`cacher` + `builder` RUNs**: add
  `--mount=type=secret,id=aws-access-key-id,required=false` and
  `…,id=aws-secret-access-key,required=false`. `required=false` so a
  vanilla `docker build` without `--secret` flags still succeeds. Each
  RUN brackets the cargo invocation with `sccache --zero-stats` /
  `sccache --show-stats` so per-stage hit/miss deltas land in CI logs.

### Workflows

- `kind-smoke.yaml`:
  - Bumped `container:` from `:e574650` to `:25b6631`.
  - Added `build-args:` (BUILDER_IMAGE + SCCACHE_BUCKET / ENDPOINT /
    REGION) and `secrets:` (aws-access-key-id / aws-secret-access-key)
    to all three `docker/build-push-action` calls (controller, api,
    envd).
  - Deleted the inline `apt-get install -y jq openssl` stopgap from
    the api-smoke step — those packages are baked into the new runner
    via `ci/runner/Dockerfile`.
- `api-image.yaml`: pinned from `:latest` to `:25b6631`, added the
  same `build-args:` / `secrets:` block.
- `controller-image.yaml` / `envd-image.yaml`: rebased onto the
  ci-runner (`container:` field, dropped the inline
  `Install docker CLI` step) and added the same `build-args:` /
  `secrets:` block, mirroring `api-image.yaml`.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — full workspace passes (50+ tests across 8 binaries).
- `docker build -f crates/sandbawks-controller/Dockerfile .` — succeeds
  with no SCCACHE_* args; image runs and `--help` exits 0 (18.3 MB).
- `docker build -f crates/sandbawks-api/Dockerfile .` — succeeds with
  no SCCACHE_* args; binary loads under distroless (21.7 MB).
- `docker build -f crates/envd/Dockerfile .` — succeeds with no
  SCCACHE_* args; binary loads under python:3.12-slim (183 MB).

The S3-enabled path (sccache write/read against the private bucket) is
exercised in CI by the first push that lands this branch onto a
feature branch under Forgejo Actions — the `sccache --show-stats` tail
appended to each cacher/builder RUN reports hit/miss against the
bucket without needing a separate step.

## Acceptance criteria status (from issue #130)

- [x] S3 bucket + credentials provisioned (done outside repo).
- [x] All three Dockerfiles wrap cargo via `RUSTC_WRAPPER=sccache`
      with a `command -v sccache || install` short-circuit.
- [x] `kind-smoke.yaml` (and `api-image.yaml`,
      `controller-image.yaml`, `envd-image.yaml`) pass `secrets:` and
      `SCCACHE_*` build-args to every `docker/build-push-action`,
      bundled with the BUILDER_IMAGE / container-pin bump deferred
      from PR #132.
- [ ] **Deferred to post-merge verification:** first three CI runs
      after merge show non-zero sccache hit rates and a
      no-Cargo.lock-change push has CI build time under 3 min total.
- [x] Vanilla `docker build -f crates/sandbawks-controller/Dockerfile .`
      on a developer machine without any `SCCACHE_*` env still
      succeeds (verified locally).
