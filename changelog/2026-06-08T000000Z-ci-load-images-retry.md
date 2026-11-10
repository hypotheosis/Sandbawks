# ci: harden image-load step with retry and shared script (issue #368)

**Date:** 2026-06-08

## Summary

Adds `deploy/kind/fixtures/load-images.sh` — a shared script that pulls
commit-scoped images from Harbor and loads them into a kind cluster with
bounded retry logic. Both the `smoke` and `bench` jobs in
`.forgejo/workflows/kind-smoke.yaml` now call this script instead of
maintaining their own inline loops.

## Problem

The `bench` job's "Pull commit-scoped images and load into kind" step
flaked on PR #367: `kind load` reported `ghcr.io/hypotheosis/envd:dev` "not
present locally" even though `docker pull` had just succeeded. The root
cause was a containerd image-store eventual-consistency window — `docker
pull` returns 0, but the tag is not yet visible to `docker image inspect`
(and therefore not to `kind load`'s internal `docker save`).

A previous fix removed the `:dev` retag dance (which introduced a
cross-job race); the remaining gap was the lack of retry and the
duplicated loops that were drifting out of sync.

## Changes

### New: `deploy/kind/fixtures/load-images.sh`

- Pulls `ghcr.io/hypotheosis/{sandbawks-controller,sandbawks-api,envd,sandbawks-node}:${IMAGE_TAG}`
- After each pull, asserts the tag is visible with `docker image inspect`
  before calling `kind load` — catches the containerd eventual-consistency
  window with a clear message rather than an opaque kind-load error.
- Wraps the pull + inspect + load sequence in a 3-attempt retry with a
  2-second delay — a one-off store race self-heals without failing the
  75-minute bench job.
- Configurable via `MAX_ATTEMPTS` and `RETRY_DELAY` env vars.

### Modified: `.forgejo/workflows/kind-smoke.yaml`

- `smoke` job "Pull commit-scoped images and load into kind" step: inline
  loop replaced with `bash deploy/kind/fixtures/load-images.sh`.
- `bench` job same step: inline loop replaced identically.
- Comments updated to reference the shared script and issue #368.

## Validation

- `just fmt` — no changes (shell/YAML only)
- `cargo clippy` — no warnings
- `just test` — all tests pass
- `bash -n deploy/kind/fixtures/load-images.sh` — syntax OK
