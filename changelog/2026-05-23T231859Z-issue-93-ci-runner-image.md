# Issue #93 — bake kind-smoke toolchain into a custom CI runner image

**Date:** 2026-05-23
**Issue:** #93 (area/deploy, kind/chore, prio/p2)
**Status:** Implemented; PR pending. See branch `fix-issue-93-ci-runner-image`.

## Motivation

`.forgejo/workflows/kind-smoke.yaml` curled the docker CLI, kind, kubectl,
and helm at the top of every CI run, hitting four separate hosts
(`download.docker.com`, `github.com`, `dl.k8s.io`, `get.helm.sh`). A run
failed on a transient DNS resolution flake against
`release-assets.githubusercontent.com` (GitHub's release-asset CDN, which
`github.com/.../releases/download/...` redirects to). The workflow's own
comment block had anticipated this:

> If either fails DNS we'll need to switch to apt or bundle the binaries
> into a custom runner image.

This change cashes that comment in.

## What changed

1. **New `ci/runner/Dockerfile`** — `FROM debian:bookworm-slim`, installs
   `bash ca-certificates curl git` via apt, then curls the docker CLI
   (`27.3.1`, static), kind (`v0.24.0`), kubectl (`v1.31.0`), and helm
   (`v3.16.0`) using the same URLs that `kind-smoke.yaml` previously did
   at workflow-start. Tool versions are pinned via `ARG` for one-line
   bumps. Final `RUN` validates every binary so a bad pin fails the
   *image build*, not the next unsuspecting CI run.

   - `docker version` (no flag) tries to round-trip to `dockerd`; not
     available at image-build time. Used `docker --version` instead,
     which prints the client build string directly. Documented inline.

2. **New `.forgejo/workflows/ci-runner-image.yaml`** — mirrors
   `controller-image.yaml` / `envd-image.yaml`. Triggers on
   `workflow_dispatch` (to allow on-demand republish) and on `push` to
   `main` when `ci/runner/**` or the workflow itself changes. Publishes
   `ghcr.io/hypotheosis/sandbawks-ci-runner:<short-sha>` and `:latest`. Reuses
   the existing `HARBOR_USERNAME` / `HARBOR_PASSWORD` org secrets.

3. **`.forgejo/workflows/kind-smoke.yaml` migration** — added a
   job-level `container: image: ghcr.io/hypotheosis/sandbawks-ci-runner:latest`,
   removed the entire 19-line "Install docker CLI, kind, kubectl, helm"
   step plus the four `env:` pins (`KIND_VERSION`, `KUBECTL_VERSION`,
   `HELM_VERSION`, `DOCKER_CLI_VERSION`) — those live in the Dockerfile
   now. Replaced the obsolete DNS-flake comment with a pointer to issue
   #93 and to `ci/runner/Dockerfile`. Everything else (cargo cache,
   kindest/node cache, buildx + GHA cache, kind cluster bring-up, helm
   install, smoke assertions, diagnostics dump) is untouched.

## Trade-off: `:latest` pin

The issue spec asks for a calver pin (e.g. `:2026.05.0`) on the
`kind-smoke.yaml` consumer side. That tag doesn't exist until
`ci-runner-image.yaml` publishes for the first time — i.e. on push to
`main`, after this PR merges. To keep the migration atomic within one PR,
the consumer is pinned to `:latest` for now, with an inline comment
explaining that a follow-up PR should tighten the pin to a specific
published runner tag. This honors the spirit of the issue (kind-smoke
runs inside the pre-baked runner image) while admitting the operational
sequencing reality.

## Local validation

- `docker build -t sandbawks-ci-runner:test -f ci/runner/Dockerfile ci/runner`
  → built successfully; the final validation `RUN` printed:
  ```
  Docker version 27.3.1, build ce12230
  kind v0.24.0 go1.22.6 linux/amd64
  Client Version: v1.31.0
  Kustomize Version: v5.4.2
  v3.16.0+g0d439e1
  ```
- `docker run --rm sandbawks-ci-runner:test sh -c "docker --version && kind version && kubectl version --client && helm version --short && git --version"`
  → reprinted the same four version lines plus `git version 2.39.5`,
  confirming `actions/checkout`'s git dependency is satisfied.
- Resulting image size: **473 MB**. Dominated by the four pinned binaries
  (docker static client + kind + kubectl + helm), which were already
  being downloaded fresh on every kind-smoke run; this just relocates
  them from per-run curl to a registry layer.
- Workspace gate: `just fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and `just test` all pass. (No Rust source was touched, but the gate is
  cheap and confirms nothing else broke.)

## What this PR explicitly does not do

- No migration of `ci.yml`'s `helm` job (it uses `azure/setup-helm@v4`,
  which has not flaked).
- No `protoc` or Rust toolchain in the runner image.
- No scheduled "your runner image is N months old" nag workflow.
- No tightening of `kind-smoke.yaml` to a calver/sha pin — see trade-off
  above; that's a follow-up PR once the registry has a known tag.

## Follow-ups suggested by this work

- **Follow-up PR after first runner publish:** flip `kind-smoke.yaml`'s
  `container.image` from `:latest` to the dated/sha tag of the specific
  runner build that this PR's merge commit produces.
- **Consider folding `ci.yml`'s helm job onto the same runner image** so
  there's a single source of truth for `helm` version. Strictly optional.
