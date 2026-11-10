# Multi-stage build for the sandbawks-controller binary.
#
# Layout (cargo-chef pattern, mirrored across all three workspace
# Dockerfiles — sandbawks-controller, sandbawks-api, envd — so BuildKit
# can share the `chef` and `cacher` layer hashes between them when the
# workspace manifests haven't changed):
#
#   1. `chef`    — pinned `rust:1.88-bookworm` with musl-tools, protoc,
#                  the musl rustup target, and `cargo-chef` itself
#                  pre-installed. ENV (RUSTFLAGS, musl linker) is set
#                  here so every downstream stage inherits identical
#                  build settings — required for cargo-chef's cooked
#                  dependency artifacts to be reusable by the builder
#                  stage.
#   2. `planner` — copy only `Cargo.toml` / `Cargo.lock` (no source)
#                  and emit `recipe.json`. Cheap to rerun; the layer
#                  invalidates only when a manifest changes.
#   3. `cacher`  — `cargo chef cook` against `recipe.json`. This
#                  compiles every workspace dependency once; BuildKit
#                  caches the resulting layer via the workflow's
#                  `cache-from: type=gha` / `cache-to: type=gha,mode=max`
#                  pair. On a warm rebuild (source change only, no
#                  manifest change) this layer is a cache hit — the
#                  ~3-minute dep compile collapses to a tar restore.
#   4. `builder` — copy in the full workspace, restore the cooked
#                  `target/` from `cacher`, and `cargo build` just the
#                  binary this Dockerfile owns. With deps already
#                  compiled this step takes <30s on a warm cache.
#   5. distroless static — final runtime image with the binary only.
#
# `RUN --mount=type=cache` keeps `~/.cargo/registry` + `~/.cargo/git`
# warm across builds independently of the layer cache, so even a cold
# cooker doesn't re-download the crates.io index on every run.
#
# The runtime image is distroless `static-debian12:nonroot`. The musl
# binary is fully-static (non-PIE — see RUSTFLAGS below) so no glibc
# or dynamic loader is needed.

# `BUILDER_IMAGE` defaults to the upstream Rust image, which is what both
# a developer `docker build .` and CI (the `build-images` matrix in
# .github/workflows/kind-smoke.yaml) use. It stays an ARG (issue #131) so a
# pre-baked builder with Rust + the musl target + cargo-chef + sccache +
# musl-tools + protoc can be swapped in to make the chef stage near-instant.
ARG BUILDER_IMAGE=rust:1.88-bookworm
FROM ${BUILDER_IMAGE} AS chef
WORKDIR /src

# musl-tools          provides musl-gcc, the linker `ring`'s build.rs
#                     shells out to during cross-compilation.
# protobuf-compiler   `protoc` itself; required by `tonic-build` via the
#                     transitive envd dep.
# libprotobuf-dev     Google well-known `.proto` files (Timestamp et al.)
#                     bookworm splits these out of the compiler package.
RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

# Pinned to the matching minor; cargo-chef 0.1.x is API-stable but a
# silent floor bump could swing recipe.json output and bust the cache.
# The `command -v` short-circuit (issue #131) skips the ~30 s cold
# `cargo install` when a pre-baked BUILDER_IMAGE already has the binary on
# $PATH; the default image hits the fallback and installs it, so builds
# stay self-contained.
RUN command -v cargo-chef >/dev/null 2>&1 \
    || cargo install cargo-chef --locked --version 0.1.68

# sccache (issue #130). `RUSTC_WRAPPER=sccache` redirects every rustc
# invocation through sccache, which keys cache lookups on
# (source hash, rustc flags, dep hashes) and stores results in an
# S3-compatible object store. Composes additively with cargo-chef
# (issue #114): chef caches the dep layer, sccache caches per-rustc
# results — including workspace re-monomorphisation that chef can't
# help with.
#
# `command -v sccache` short-circuit mirrors the cargo-chef pattern
# above: a pre-baked BUILDER_IMAGE (issue #131) hits the no-op branch;
# the default image installs the pinned binary from upstream.
ARG SCCACHE_VERSION=0.10.0
RUN command -v sccache >/dev/null 2>&1 \
    || (curl -fsSL "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
            | tar -xz --strip-components=1 -C /usr/local/bin \
                "sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache" \
        && chmod +x /usr/local/bin/sccache)

# S3 backend config (SCCACHE_BUCKET / SCCACHE_ENDPOINT / SCCACHE_REGION)
# is intentionally NOT promoted to ENV here. sccache treats the presence
# of `SCCACHE_ENDPOINT` (even when empty) as a signal to enable the S3
# backend and then panics on the empty URI — breaking the
# developer-workstation `docker build .` path. The build-args are
# re-declared as ARGs in the cacher + builder stages instead, where
# a per-RUN guard exports them only when SCCACHE_BUCKET is non-empty.
# When unset, that guard also unsets RUSTC_WRAPPER so sccache is fully
# bypassed and rustc runs directly (local-disk fallback isn't useful
# under BuildKit anyway, since the sccache home is ephemeral per layer).
#
# Build settings inherited by every downstream stage. `cargo chef cook`
# compiles dependencies using *these* settings, so the builder stage
# must use identical settings or the cooked artifacts won't match.
#
# `RUSTC_WRAPPER=sccache` is set here so every downstream `cargo`
# invocation funnels through sccache. `CARGO_INCREMENTAL=0` is
# mandatory: cargo's incremental mode is incompatible with sccache
# (sccache returns NotCacheable for any incremental rustc call), and
# a release build doesn't benefit from incremental anyway.
# `AWS_EC2_METADATA_DISABLED=true` short-circuits the AWS SDK's
# in-VPC IMDS probe; without it sccache stalls for ~5 s per stage on
# a non-EC2 builder waiting for 169.254.169.254 to time out.
#
# `relocation-model=static` disables Rust 1.71+'s default static-PIE
# musl layout, which embeds `/lib/ld-musl-x86_64.so.1` as the ELF
# interpreter — distroless `static-debian12` doesn't ship that loader,
# so the resulting binary fails with `no such file or directory` on
# exec. Disabling PIE drops the PT_INTERP entry; the binary then runs
# cleanly under distroless static.
ENV RUSTC_WRAPPER=sccache \
    CARGO_INCREMENTAL=0 \
    AWS_EC2_METADATA_DISABLED=true \
    CC_x86_64_unknown_linux_musl=musl-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=static"

# ── planner ──
# Only the workspace manifests land here. Source files are intentionally
# excluded so this layer's hash depends solely on Cargo.toml / Cargo.lock
# content; any source-only change leaves it untouched.
FROM chef AS planner
# `cargo chef prepare` runs `cargo metadata`, which shells out to
# `rustc -vV` to discover the toolchain version. With RUSTC_WRAPPER
# inherited from the chef base, that throwaway invocation would go
# through sccache — which then tries to authenticate against S3 for
# its startup `.sccache_check`. The planner stage has no AWS secrets
# mounted (and no reason to compile anything), so unset the wrapper
# here. cargo treats an empty RUSTC_WRAPPER as unset.
ENV RUSTC_WRAPPER=
COPY Cargo.toml Cargo.lock ./
COPY crates/sandbawks-types/Cargo.toml      crates/sandbawks-types/Cargo.toml
COPY crates/sandbawks-controller/Cargo.toml crates/sandbawks-controller/Cargo.toml
COPY crates/envd/Cargo.toml                 crates/envd/Cargo.toml
COPY crates/sandbawks-api/Cargo.toml        crates/sandbawks-api/Cargo.toml
COPY crates/sandbawks-node/Cargo.toml       crates/sandbawks-node/Cargo.toml
COPY crates/sandbawks-observability/Cargo.toml crates/sandbawks-observability/Cargo.toml
RUN cargo chef prepare --recipe-path recipe.json

# ── cacher ──
# `cargo chef cook` compiles every registry dependency listed in
# recipe.json against the musl target. The resulting `target/` is
# what the builder stage restores. The two cache mounts persist the
# crates.io registry + git index across builds so a cold cook still
# avoids re-downloading the world.
#
# AWS creds for sccache S3 land as files at /run/secrets/* via
# BuildKit's `--mount=type=secret`. `required=false` so a vanilla
# `docker build .` without `--secret` flags still succeeds — the
# `cat … 2>/dev/null || true` pair then yields empty strings,
# `SCCACHE_BUCKET` is also empty by default, and sccache falls back
# to its local-disk cache (no S3 auth attempt).
#
# `sccache --zero-stats` resets the daemon before the cargo invocation
# so the `--show-stats` tail surfaces a clean per-stage hit/miss delta
# in CI logs. Both stats calls are guarded with `|| true` so a broken
# sccache (e.g. unreachable S3 endpoint) doesn't red-x the build.
FROM chef AS cacher
# ARGs are stage-local; redeclare so the cacher RUN can substitute the
# build-args passed by CI. Empty defaults keep a vanilla `docker build .`
# on the local fallback path (see the if-guard in the RUN below).
ARG SCCACHE_BUCKET=""
ARG SCCACHE_ENDPOINT=""
ARG SCCACHE_REGION=""
# Garage (the S3-compatible store at the private endpoint) requires
# path-style addressing (bucket in URL path, not Host header) and
# plain HTTP. sccache defaults to virtual-host style + HTTPS for
# anything that doesn't look like AWS, so both must be flipped
# explicitly. Empty defaults keep the developer fallback path on
# whatever sccache picks for the (unused) local-cache mode.
ARG SCCACHE_S3_USE_SSL=""
ARG SCCACHE_S3_ENABLE_VIRTUAL_HOST_STYLE=""
COPY --from=planner /src/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=secret,id=aws-access-key-id,required=false \
    --mount=type=secret,id=aws-secret-access-key,required=false \
    if [ -n "${SCCACHE_BUCKET}" ]; then \
        export SCCACHE_BUCKET="${SCCACHE_BUCKET}" \
            SCCACHE_ENDPOINT="${SCCACHE_ENDPOINT}" \
            SCCACHE_REGION="${SCCACHE_REGION}" \
            SCCACHE_S3_USE_SSL="${SCCACHE_S3_USE_SSL}" \
            SCCACHE_S3_ENABLE_VIRTUAL_HOST_STYLE="${SCCACHE_S3_ENABLE_VIRTUAL_HOST_STYLE}" \
            AWS_ACCESS_KEY_ID="$(cat /run/secrets/aws-access-key-id 2>/dev/null || true)" \
            AWS_SECRET_ACCESS_KEY="$(cat /run/secrets/aws-secret-access-key 2>/dev/null || true)"; \
    else \
        unset RUSTC_WRAPPER; \
    fi \
    && (sccache --zero-stats >/dev/null 2>&1 || true) \
    && cargo chef cook --release \
        --target x86_64-unknown-linux-musl \
        --recipe-path recipe.json \
    && (sccache --show-stats || true)

# ── builder ──
FROM chef AS builder
ARG SCCACHE_BUCKET=""
ARG SCCACHE_ENDPOINT=""
ARG SCCACHE_REGION=""
# Garage (the S3-compatible store at the private endpoint) requires
# path-style addressing (bucket in URL path, not Host header) and
# plain HTTP. sccache defaults to virtual-host style + HTTPS for
# anything that doesn't look like AWS, so both must be flipped
# explicitly. Empty defaults keep the developer fallback path on
# whatever sccache picks for the (unused) local-cache mode.
ARG SCCACHE_S3_USE_SSL=""
ARG SCCACHE_S3_ENABLE_VIRTUAL_HOST_STYLE=""
# Restore the cooked dep artifacts from cacher. Without this copy the
# next `cargo build` would re-compile every dep from scratch and the
# whole cargo-chef dance would be a no-op.
COPY --from=cacher /src/target target
# Git SHA stamped into the binary's OTel telemetry as `vcs.revision` (issue
# #387). CI passes `--build-arg GIT_SHA=<commit>`; a vanilla `docker build .`
# defaults to `unknown` because `.dockerignore` excludes `.git` from the build
# context, so build.rs's git fallback can't resolve a SHA in-image. This ENV
# is builder-stage-only: the distroless final stage doesn't inherit it, so no
# build metadata leaks into the runtime image (a future single-stage refactor
# would need to re-declare or strip it).
ARG GIT_SHA=unknown
ENV SANDBAWKS_GIT_SHA=$GIT_SHA
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=secret,id=aws-access-key-id,required=false \
    --mount=type=secret,id=aws-secret-access-key,required=false \
    if [ -n "${SCCACHE_BUCKET}" ]; then \
        export SCCACHE_BUCKET="${SCCACHE_BUCKET}" \
            SCCACHE_ENDPOINT="${SCCACHE_ENDPOINT}" \
            SCCACHE_REGION="${SCCACHE_REGION}" \
            SCCACHE_S3_USE_SSL="${SCCACHE_S3_USE_SSL}" \
            SCCACHE_S3_ENABLE_VIRTUAL_HOST_STYLE="${SCCACHE_S3_ENABLE_VIRTUAL_HOST_STYLE}" \
            AWS_ACCESS_KEY_ID="$(cat /run/secrets/aws-access-key-id 2>/dev/null || true)" \
            AWS_SECRET_ACCESS_KEY="$(cat /run/secrets/aws-secret-access-key 2>/dev/null || true)"; \
    else \
        unset RUSTC_WRAPPER; \
    fi \
    && (sccache --zero-stats >/dev/null 2>&1 || true) \
    && cargo build --release \
        --target x86_64-unknown-linux-musl \
        -p sandbawks-controller --bin sandbawks-controller \
    && (sccache --show-stats || true)

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder \
        /src/target/x86_64-unknown-linux-musl/release/sandbawks-controller \
        /usr/local/bin/sandbawks-controller
USER nonroot
ENTRYPOINT ["/usr/local/bin/sandbawks-controller"]
