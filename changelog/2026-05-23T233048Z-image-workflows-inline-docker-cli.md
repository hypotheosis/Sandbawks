# Image-build workflows — inline docker CLI install

**Date:** 2026-05-23
**Touches:** `.forgejo/workflows/ci-runner-image.yaml`, `controller-image.yaml`, `envd-image.yaml`
**Follow-up to:** PR #98 / issue #93

## Symptom

`workflow_dispatch`-ing `ci-runner-image.yaml` failed at the
`https://github.com/docker/login-action@v3` step:

```
Logging into the private registry...
::error::Unable to locate executable file: docker. Please verify either
the file path exists or the file can be found within a directory specified
by the PATH environment variable.
::error::exitcode '1': failure
```

## Root cause

The Forgejo runner's default workflow container for `runs-on: docker`
(no explicit `container:` field) does not ship the docker CLI. The
pre-#98 `kind-smoke.yaml` knew this and inline-installed the static
binary; the comment on `DOCKER_CLI_VERSION` spelled it out:

> "the workflow image itself does not ship the docker CLI — kind shells
> out to `docker info`, so we install the static binary."

PR #98 baked docker (+ kind, kubectl, helm) into `ghcr.io/hypotheosis/sandbawks-ci-runner`
so `kind-smoke` no longer needs the inline install — but the image-build
workflows themselves can't use the ci-runner image as their `container:`
(chicken-and-egg on `ci-runner-image.yaml`; same pattern symmetry for the
other two), and they were assuming docker was already present in the
runner's default container. `docker/setup-buildx-action@v3` plus
`docker/login-action@v3` both shell out to `docker`, so the very first
docker-touching step blows up.

`controller-image.yaml` and `envd-image.yaml` had been working only
incidentally; they have the identical bootstrap and are one runner-config
nudge away from breaking the same way. Defensive fix to all three.

## Fix

Inserted an `Install docker CLI` step immediately after `actions/checkout@v4`
in all three workflows. Mirrors the pre-#98 kind-smoke pattern: idempotent
(`command -v docker` guard), curls the same pinned static binary
(`27.3.1`) from `download.docker.com` that `ci/runner/Dockerfile` uses,
drops it at `/usr/local/bin/docker`, and runs `docker --version` to fail
the step loudly if the install didn't land.

`DOCKER_CLI_VERSION` is duplicated between the Dockerfile and the three
workflow files. A future tightening could read it from a single source,
but a single literal in four places is honest and trivially grep-able
(`grep -r DOCKER_CLI_VERSION\|docker-27\.3\.1 .forgejo ci/`).

## Validation

- `cargo fmt`, `cargo clippy`, `cargo test` — N/A (no Rust touched).
- Manual: `workflow_dispatch` of `ci-runner-image.yaml` is the
  reproducer; expect the install step to print `Docker version 27.3.1,
  build ce12230` and the rest of the job to proceed through buildx
  setup, login, metadata, and build-push as designed.

## Out of scope

- Tightening the kind-smoke `:latest` pin (still tracked as the follow-up
  noted in PR #98).
- A scheduled re-publish of the ci-runner image.
- Centralising `DOCKER_CLI_VERSION` across the Dockerfile and workflows.
