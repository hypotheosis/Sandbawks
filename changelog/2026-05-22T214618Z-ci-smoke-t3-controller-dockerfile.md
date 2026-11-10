# CI smoke T3 — Controller Dockerfile + image build target

**Issue:** #69
**Epic:** #66 — CI: kind-based deployment smoke test
**Date:** 2026-05-22

## What landed

- `crates/sandbawks-controller/Dockerfile` — multi-stage build:
  - **Stage 1 (builder):** `rust:1.88-bookworm` (see *Deviations* below),
    installs `musl-tools` + `protobuf-compiler`, adds the
    `x86_64-unknown-linux-musl` target, and compiles
    `sandbawks-controller` statically against musl.
  - **Stage 2 (runtime):** `gcr.io/distroless/static-debian12:nonroot`,
    copies the static binary to `/usr/local/bin/sandbawks-controller`,
    runs as the unprivileged `nonroot` user.
- `.dockerignore` at the repo root — excludes `target/`, `.git/`,
  `**/*.snap`, `changelog/`, `docs/`, `deploy/helm/`, `deploy/kind/` so
  the build context stays small and reproducible.
- `justfile` recipes:
  - `controller-image-build` — `docker build -t
    ghcr.io/hypotheosis/sandbawks-controller:dev …`.
  - `controller-image-run` — `docker run --rm … --help` as a cheap
    runnability check.

## Why a musl static binary

The runtime stage is distroless `static`, which has no libc at all. Linking
against glibc would leave the binary unable to start. Building with the
`x86_64-unknown-linux-musl` target produces a self-contained executable.

`ring` (pulled in via `rustls`) needs to know which C compiler to invoke
when cross-compiling — without `CC_x86_64_unknown_linux_musl=musl-gcc`
and `CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc` it falls
back to the host's glibc `cc` and the resulting binary panics on load.
Those two `ENV` lines aren't called out in the issue but they're load-bearing.

## Deviations from the issue spec (and why)

Verifying the image actually builds and runs surfaced two problems with
the exact recipe in the issue body. Both are documented here so review
can sanity-check the choices.

### Bump `rust:1.83-bookworm` → `rust:1.88-bookworm`

The crates.io transitive dependency graph requires `edition2024`
(stabilized in 1.85) and `home@0.5.12` requires rustc ≥ 1.88. Building
on 1.83 fails at dependency resolution with:

```
error: rustc 1.85.1 is not supported by the following package:
  home@0.5.12 requires rustc 1.88
```

1.88 is the minimum that compiles the current workspace.

### Add `RUSTFLAGS=-C target-feature=+crt-static -C relocation-model=static`

Without this, Rust 1.71+ produces a *static-PIE* musl binary that embeds
`/lib/ld-musl-x86_64.so.1` as its ELF interpreter. `distroless/static-debian12`
doesn't ship that loader, so the container immediately exits with:

```
exec /usr/local/bin/sandbawks-controller: no such file or directory
```

`-C relocation-model=static` disables PIE, producing a true position-dependent
fully-static ELF with no `PT_INTERP` entry — which is what distroless
`static` is designed to run. Verified with:

```
$ file sandbawks-controller
ELF 64-bit LSB executable, x86-64, version 1 (SYSV), statically linked, stripped
```

(versus the static-PIE form: `… dynamically linked, interpreter
/lib/ld-musl-x86_64.so.1, … statically linked` from `ldd`.)

## Why this image tag

`ghcr.io/hypotheosis/sandbawks-controller:dev` matches what T4 (publish workflow)
and T5 (`kind load docker-image`) expect. Using a `:dev` tag locally keeps
unpublished builds visibly distinct from CI-built images that would carry
a commit SHA.

## Validation

Repo-level (run before push):

- `just fmt` — no diff.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — green.

Image-level validation (run locally with Docker 29.4.x):

- `just controller-image-build` succeeds — produces
  `ghcr.io/hypotheosis/sandbawks-controller:dev`.
- `docker images ghcr.io/hypotheosis/sandbawks-controller:dev` reports
  **16.7 MB on disk / 3.7 MB content size** — comfortably under the
  25 MiB target.
- `docker run --rm ghcr.io/hypotheosis/sandbawks-controller:dev --help` starts
  the binary under the distroless runtime. The controller doesn't parse
  args today, so `--help` is ignored and the binary attempts to build a
  kube client; outside a cluster (and with no `KUBECONFIG`) it exits 1
  with `Error: building in-cluster kube client`. That's the *binary
  loaded and ran* signal we want; T5 will exercise it for real inside
  kind.

## Out of scope (per the issue)

- CI publishing — T4.
- Multi-arch — deferred.

## Dependencies

- None.
