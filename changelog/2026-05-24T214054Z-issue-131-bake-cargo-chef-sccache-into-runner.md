# Issue #131 — Bake cargo-chef (+ sccache + Rust toolchain) into the sandbawks-ci-runner image

Forgejo issue: #131. Follow-up
to PR #129 / issue
#114 (Plan 04 T15).

## Summary

PR #129 added `cargo-chef` to all three workspace Dockerfiles
(`sandbawks-controller`, `sandbawks-api`, `envd`). Each one does

```dockerfile
RUN cargo install cargo-chef --locked --version 0.1.68
```

inline in its `chef` base stage. On a cold CI cache that's a ~30 s
compile, replicated across all three images for ~90 s of pure
tool-bootstrap on every cold smoke run. This change moves the binary
into the `ghcr.io/hypotheosis/sandbawks-ci-runner` image and wires the
workspace Dockerfiles to consume it as their chef base, so the entire
chef stage becomes near-instant in CI.

Scope was expanded beyond the issue's literal text (steps 1+2) after
auditing how wnw realizes the same speedup. The mechanism needs three
load-bearing pieces:

1. A pre-baked binary in the runner image (step 1 of the issue).
2. A short-circuit in the workspace Dockerfile that uses it
   when present (step 2 of the issue).
3. A way to actually point the chef stage's FROM at that runner image
   — neither the issue nor the original wnw rationale describes this
   without (3), but (3) is the only thing that makes (2) ever fire.

Without (3) the chef stage's `FROM rust:1.88-bookworm` keeps using
the upstream Rust image, which has no pre-baked binaries, so
`command -v cargo-chef` always returns false and the install never
short-circuits. This PR adds (3) via wnw's `ARG BUILDER_IMAGE`
pattern.

For the runner image to actually serve as `BUILDER_IMAGE`, it also
needs a Rust toolchain and the musl cross target — its
`node:20-bookworm-slim` base has neither today. So this PR also
installs rustup + Rust 1.88.0 + the
`x86_64-unknown-linux-musl` target + the apt packages the chef stage
needs (`musl-tools`, `protobuf-compiler`, `libprotobuf-dev`,
`build-essential`, `pkg-config`).

## Changes

### `ci/runner/Dockerfile`

- Adds `CARGO_CHEF_VERSION=0.1.68`, `SCCACHE_VERSION=0.10.0`,
  `RUST_VERSION=1.88.0`, `RUST_TARGET=x86_64-unknown-linux-musl`
  ARGs. The `RUST_VERSION` pin MUST match the workspace
  Dockerfiles' chef-stage default (`FROM rust:1.88-bookworm`) —
  different rustc versions emit different cached artifacts, so a
  divergence would silently bust the cargo-chef cooker cache.
- Expands the apt-install layer with `build-essential`,
  `musl-tools`, `protobuf-compiler`, `libprotobuf-dev`, and
  `pkg-config` so the chef stage's own `apt-get install` becomes a
  no-op when this image is the BUILDER_IMAGE.
- Pulls the prebuilt musl binaries for `cargo-chef` (LukeMathWalker's
  GitHub releases) and `sccache` (mozilla's GitHub releases) into
  `/usr/local/bin`. Tarballs are used rather than `cargo install`
  because the base ships no rustc; doing the install via cargo would
  fail at the rustup-bootstrap step or balloon the runner-image
  build time.
- Adds a rustup install of Rust `${RUST_VERSION}` with `rustfmt` +
  `clippy` components and the `${RUST_TARGET}` target,
  pointed at `/usr/local/{rustup,cargo}` so the binaries land on
  PATH for every user. `chmod -R a+w` so a future `rustup update`
  from a non-root container user still works.
- Extends the bottom validation `RUN` block with `cargo-chef
  --version`, `sccache --version`, `rustc --version`, `cargo
  --version`, and `rustup target list --installed | grep
  ${RUST_TARGET}`. Image build fails if any of these regress.

### `crates/sandbawks-controller/Dockerfile`, `crates/sandbawks-api/Dockerfile`, `crates/envd/Dockerfile`

Two changes per file:

1. `FROM rust:1.88-bookworm AS chef` → `ARG
   BUILDER_IMAGE=rust:1.88-bookworm` + `FROM ${BUILDER_IMAGE} AS
   chef`. The default keeps a vanilla `docker build .` on a
   developer workstation working unchanged; CI overrides it via
   `--build-arg BUILDER_IMAGE=ghcr.io/hypotheosis/sandbawks-ci-runner:<sha>`
   to pick up the pre-baked toolchain + binaries.
2. `RUN cargo install cargo-chef --locked --version 0.1.68` →
   `RUN command -v cargo-chef >/dev/null 2>&1 || cargo install
   cargo-chef --locked --version 0.1.68`. Same wnw pattern. The
   version pin on the fallback matches `CARGO_CHEF_VERSION` in
   the runner Dockerfile.

The controller Dockerfile carries the full rationale comment; the api
and envd Dockerfiles point at it to avoid drift.

### Measured speedup

End-to-end built locally:

- Chef stage with default `BUILDER_IMAGE=rust:1.88-bookworm` (vanilla
  developer build): apt-get install (~22 s cold) + rustup target add
  (~7 s) + cargo install cargo-chef (~14 s with sccache, ~30 s without)
  = ~45 s.
- Chef stage with `BUILDER_IMAGE=sandbawks-ci-runner:<sha>` (CI fast
  path): apt-get reports "already the newest version" (~1.8 s, just
  the apt index update), rustup target reports "up to date" (~0.4 s),
  cargo-chef short-circuit (~0.3 s) = **~3 s total**. Replicated
  across all three workspace images, that's the ~90 s/cold-run saving
  the issue predicted.

## Deferred to a follow-up PR

The issue lists four steps; this PR ships steps 1 and 2 plus the
BUILDER_IMAGE plumbing that activates them. Steps 3 (bump the pinned
ci-runner tag in `.forgejo/workflows/kind-smoke.yaml` AND add
`build-args: BUILDER_IMAGE=...` to its `docker/build-push-action`
calls) and 4 (delete the stopgap `apt-get install -y jq openssl`
block in the api-smoke step) cannot land in the same PR because:

- The runner-image tag is `type=sha,prefix=,format=short` (see
  `.forgejo/workflows/ci-runner-image.yaml`). That short SHA only
  exists *after* the merge commit lands on `main` and
  `ci-runner-image.yaml` publishes the resulting image, so it can't
  be pinned ahead of time. The current `:e574650` pin predates this
  Dockerfile rewrite and would fail as a BUILDER_IMAGE (no Rust
  toolchain).
- The issue's own "Why this is a separate issue" note reinforces this
  — each step gets its own clean revert button.

The follow-up PR will:

1. Repin `kind-smoke.yaml`'s `container.image` from
   `ghcr.io/hypotheosis/sandbawks-ci-runner:e574650` to the new published
   short SHA.
2. Add `build-args: BUILDER_IMAGE=ghcr.io/hypotheosis/sandbawks-ci-runner:<new-sha>`
   to the three `docker/build-push-action` steps that build the
   controller, api, and envd images.
3. Delete the inline `apt-get install -y jq openssl` in the api-smoke
   step that issue #114 added as a stopgap (the new runner image
   already bakes both).
4. Optionally extend the per-image publish workflows
   (`controller-image.yaml`, `api-image.yaml`, `envd-image.yaml`) to
   also pass `build-args: BUILDER_IMAGE=...` — currently only
   `api-image.yaml` runs inside the ci-runner container.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`
  all pass.
- `docker build -f ci/runner/Dockerfile ci/runner` succeeds locally;
  the final validation `RUN` prints `cargo-chef 0.1.68`, `sccache
  0.10.0`, `rustc 1.88.0`, `cargo 1.88.0` and confirms the musl
  target is installed.
- `docker build -f crates/sandbawks-controller/Dockerfile --target
  chef --build-arg BUILDER_IMAGE=sandbawks-ci-runner:issue-131-test .`
  succeeds, and the apt-get + rustup-target + cargo-chef-install
  steps inside the chef stage all collapse to ~3 s as described
  above.
- `docker build -f crates/sandbawks-controller/Dockerfile --target
  chef .` (default BUILDER_IMAGE) also succeeds, falling through to
  the historical `cargo install cargo-chef` path.

## Acceptance criteria status (issue #131)

- [x] `ci/runner/Dockerfile` installs cargo-chef + sccache via GitHub
      release tarballs.
- [ ] `ci-runner-image.yaml` workflow publishes a new tag — happens
      automatically on merge via the `paths: ci/runner/**` trigger.
- [x] All three workspace Dockerfiles use the `command -v cargo-chef
      || cargo install` short-circuit.
- [x] *(beyond literal scope)* All three workspace Dockerfiles carry
      `ARG BUILDER_IMAGE` so CI can swap the chef stage's base to the
      pre-baked runner image. Without this the short-circuit never
      fires.
- [ ] `.forgejo/workflows/kind-smoke.yaml` repinned + inline
      `apt-get install -y jq openssl` removed. Deferred to follow-up
      (chicken-and-egg with the runner-image tag).
- [ ] First post-merge kind-smoke run shows cargo-chef as
      already-present. Verified after follow-up lands.
- [ ] Wall-clock on the controller image's chef stage drops by ~30 s
      vs. baseline. Verified after follow-up lands. Measured locally
      to be a ~40 s drop per image, ~90 s across the three.
