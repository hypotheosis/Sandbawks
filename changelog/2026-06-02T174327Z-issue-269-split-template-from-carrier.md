# 2026-06-02 — Issue #269: split the reference template image from the envd carrier image

Sub-issue of #265 (injection model); follow-up to PR #266 and the
`use compound-sandbox for template` change (commit `2b26842`).

## Context

Historically `crates/envd/Dockerfile` played two roles at once: the
**carrier** of the static `envd` binary *and* the **reference
SandboxTemplate** (its final stage is `python:3.12-slim` with `envd`
baked in). Under the injection model these are distinct:

- `envdImage` — minimal carrier of just the static `envd` binary. The
  node DaemonSet's `stage-envd` init container copies it out onto a
  hostPath; every sandbox Pod execs it as its entrypoint.
- `SandboxTemplate.spec.image` — the customer rootfs, which must **not**
  contain `envd`.

The functional split (pointing the reference `SandboxTemplate.spec.image`
at the envd-free `ghcr.io/hypotheosis/compound-sandbox:latest` while
`spec.envdImage` points at `ghcr.io/hypotheosis/envd:latest`) already landed on
`main` in `kustomize/sandbox-template.yaml`. All smoke/e2e fixtures
(`deploy/kind/fixtures/sandbox-smoke.yaml`, the `api-smoke-template` and
bench fixtures in `.forgejo/workflows/kind-smoke.yaml`) already use an
envd-free `image` (busybox) with a separate `envdImage`.

What remained was the **documentation decoupling**: two live build files
still described the envd image as "the reference template image" — the
exact conflation the issue's "Why" section cites (Dockerfile lines 8-11).

## Changes

- `crates/envd/Dockerfile` — rewrote the header comment to describe the
  image as the *envd carrier*, not a reference SandboxTemplate, and to
  explain the `python:3.12-slim` base as a temporary consequence of the
  `cp`-based `stage-envd` init container needing a userland (slimmed by
  issue #268's `self-install` + distroless/scratch carrier), rather than
  falsely attributing it to the image being a Python sandbox template.
- `justfile` — rewrote the `envd-image-build` recipe comment: the image
  is the carrier that a `SandboxTemplate` points `envdImage` (not
  `image`) at; its `image` is a separate, envd-free rootfs.

No functional / Rust code changes — comment-only.

## Acceptance verification

- **The reference template image does not contain `/usr/local/bin/envd`.**
  Pulled `ghcr.io/hypotheosis/compound-sandbox:latest` and confirmed
  `/usr/local/bin/envd` is absent and a recursive `find / -name envd`
  returns nothing.
- **A sandbox from the reference template still works end to end (envd
  injected, tooling present).** Delivered by the injection model (PR
  #266) and exercised by the kind-smoke CI gate; unchanged here.

## Validation

- `just fmt` — clean (no Rust changes).
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all workspace tests pass, 0 failures.
- `docker build --check -f crates/envd/Dockerfile .` — no warnings.
