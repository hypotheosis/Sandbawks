# Plan 05 T10 — `sandbawks-node` Dockerfile + Forgejo Actions matrix entry

## Summary

Ships a distroless multi-stage Dockerfile for the `sandbawks-node` DaemonSet
and wires it into the existing `build-images` matrix in
`.forgejo/workflows/kind-smoke.yaml` so every PR push and `main` merge
produces a `ghcr.io/hypotheosis/sandbawks-node:sha-<sha>` (and on `main`, `:latest`)
tag at Harbor. Closes #146.

## Files touched

- `crates/sandbawks-node/Dockerfile` — new. Mirrors the four-stage
  cargo-chef pipeline (`chef` → `planner` → `cacher` → `builder`) used by
  the controller/api/envd Dockerfiles so BuildKit can share the cooker
  layer hash across all four workspace image builds when the workspace
  manifests are unchanged. Adds a dedicated `runsc` downloader stage that
  pulls the pinned gVisor release tarball, verifies the published
  `runsc.sha512`, and stages the binary for the runtime stage. Final
  runtime image is `gcr.io/distroless/cc-debian12:nonroot` per Plan 05 T10
  — heavier than the controller/api's `static-debian12` because `runsc`
  may shell out to helpers during `checkpoint`/`restore`.
- `.forgejo/workflows/kind-smoke.yaml` — adds `image: node` /
  `repo: ghcr.io/hypotheosis/sandbawks-node` /
  `dockerfile: crates/sandbawks-node/Dockerfile` to the `build-images`
  strategy matrix. The downstream `kind-smoke` job's `Pull commit-scoped
  images and load into kind` step is intentionally **not** extended —
  Plan 05 T13 covers wiring the node image into the smoke cluster.
- `justfile` — adds `node-image-build` and `node-image-run` recipes
  alongside the controller/api/envd equivalents.

## Design notes

### Why a separate `runsc` stage?

Putting the gVisor download in the Rust builder would bust the cargo-chef
cache on every `GVISOR_RELEASE` bump and force a full workspace
re-compile. The dedicated `runsc` stage keys its layer hash on
`GVISOR_RELEASE` alone, so a runsc version bump rebuilds ~50 MB instead
of the entire workspace.

### Why `cc-debian12` over `static-debian12`?

The `sandbawks-node` workspace binary is itself a fully-static
x86_64-unknown-linux-musl ELF (same `RUSTFLAGS` as controller/api), so a
`static-debian12` runtime would suffice for it. But `runsc` —
particularly its `checkpoint`/`restore` paths — historically links
against glibc helpers and may invoke `/proc`-touching auxiliaries that
expect the standard C runtime to be reachable. `cc-debian12` adds
~20 MB of libgcc / libc6 / libssl3 over `static-debian12`; cheap
insurance versus losing pause/resume to a `no such file or directory`
exec fault at runtime. Plan 05 T10 explicitly specifies `cc-debian12`.

### Why pin `GVISOR_RELEASE=20240603.0`?

Matches the pin Plan 05 T11 will use for the `ci/kind-node/Dockerfile`
that bakes `runsc` into the kind node image. Pause and resume **must**
run identical runsc versions or the checkpoint format on disk can
diverge between sender and restorer. Bumps land in lockstep across both
Dockerfiles.

### sha512 verification

gVisor publishes a `runsc.sha512` companion file next to each binary.
The runsc stage downloads both and runs `sha512sum -c` before
`install`ing the binary, so a corrupted or tampered download fails the
build rather than silently producing a broken image.

## Verification

- `cargo fmt --all -- --check` ✅
- `cargo clippy --workspace --all-targets -- -D warnings` ✅
- `cargo test --workspace --all-targets` ✅
- `just node-image-build` ✅
- `just node-image-run` ✅ (binary loads under distroless `cc-debian12`;
  the placeholder `--help` exits 2 by clap convention when no command
  matches but proves the entrypoint is wired correctly)

## Out of scope

- Custom kind node image (`sandbawks-kind-node`) → Plan 05 T11.
- Helm chart switching the DaemonSet to the real binary → Plan 05 T12.
- Loading the image into the smoke cluster during `cluster-up` → Plan 05 T13.
