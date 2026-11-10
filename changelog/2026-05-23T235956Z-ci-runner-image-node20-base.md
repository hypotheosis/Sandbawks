# ci-runner image — switch base to `node:20-bookworm-slim`

**Date:** 2026-05-23
**Touches:** `ci/runner/Dockerfile`
**Follow-up to:** PR #98 / issue #93

## Symptom

`kind-smoke` (whose job-level `container:` is
`ghcr.io/hypotheosis/sandbawks-ci-runner:latest`) red-x'd at its first step:

```
https://github.com/actions/checkout@v4
OCI runtime exec failed: exec failed: unable to start container process:
exec: "node": executable file not found in $PATH
::error::exitcode '127': command not found
```

## Root cause

`actions/checkout@v4` — and every other action `kind-smoke` consumes —
declares `runs.using: node20` in its `action.yml`. Forgejo Actions
executes JS actions by exec'ing `node` inside the workflow container
with the action's `dist/index.js` as the entrypoint. If `node` isn't in
PATH the action exits 127 before its first user-code line runs.

`ci/runner/Dockerfile` was built on `debian:bookworm-slim` plus an apt
install of `bash ca-certificates curl git` and curl-installed static
binaries for docker / kind / kubectl / helm. None of those ships node,
so `actions/checkout@v4` was guaranteed to fail in any workflow that
sets `container:` to the ci-runner image. We hadn't caught this in PR
#98's validation because the local `docker run` smoke just exercised
the bundled CLIs — it didn't try to run a JS action.

## Fix

Swapped the base image from `debian:bookworm-slim` to
`node:20-bookworm-slim`. The official Node image is literally Debian
bookworm-slim + a pinned node20 runtime — same glibc, same apt, same
package universe — so no other layer needed to change. Updated the
top-of-file comment block to explain the choice (and to name the JS
action consumers so a future maintainer doesn't undo it). Added
`node --version` to the final validation `RUN` so any future base-image
swap that loses node fails the *image build* instead of the next
kind-smoke run.

## Validation

- `docker build -t sandbawks-ci-runner:test -f ci/runner/Dockerfile ci/runner`
  succeeded; final `RUN` printed:
  ```
  Docker version 27.3.1, build ce12230
  kind v0.24.0 go1.22.6 linux/amd64
  Client Version: v1.31.0
  Kustomize Version: v5.4.2
  v3.16.0+g0d439e1
  v20.20.2
  ```
- `docker run --rm sandbawks-ci-runner:test sh -c 'command -v node && node --version && command -v docker && command -v git'`
  reported `/usr/local/bin/node` + `v20.20.2`, `/usr/local/bin/docker`,
  `/usr/bin/git` — all four runtime-PATH-resolvable.

## Out of scope

- Pinning node to a patch version via `ARG NODE_VERSION=...` rather than
  riding the `node:20-bookworm-slim` tag. Deferred until / if upstream
  node 20 ever drops `runs.using: node20` compatibility, which would
  manifest as an action-build failure first.
- Trimming the ~30 MB the node runtime adds to the published image
  (current uncompressed size ~503 MB, up from ~473 MB).
