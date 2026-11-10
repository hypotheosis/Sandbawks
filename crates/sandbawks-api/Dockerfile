# Multi-stage build for the sandbawks-api binary.
#
# Layout (cargo-chef pattern — see crates/sandbawks-controller/Dockerfile
# for the full rationale; the chef / planner / cacher stages here are
# byte-identical to that file's so BuildKit can share the cooker layer
# between the two builds when the workspace manifests haven't changed).
# The only divergence is the final builder stage's `-p` flag and the
# binary that gets copied into the distroless runtime image.

# See crates/sandbawks-controller/Dockerfile for the rationale behind
# `ARG BUILDER_IMAGE` (issue #131).
ARG BUILDER_IMAGE=rust:1.88-bookworm
FROM ${BUILDER_IMAGE} AS chef
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

# See crates/sandbawks-controller/Dockerfile for the rationale behind
# the `command -v` short-circuit (issue #131).
RUN command -v cargo-chef >/dev/null 2>&1 \
    || cargo install cargo-chef --locked --version 0.1.68

# See crates/sandbawks-controller/Dockerfile for the rationale behind
# the sccache wrapping + S3 backend ARGs (issue #130). The chef /
# planner / cacher / builder stages here remain byte-identical to
# that file's so BuildKit can share the cooker layer hash when the
# workspace manifests haven't changed.
ARG SCCACHE_VERSION=0.10.0
RUN command -v sccache >/dev/null 2>&1 \
    || (curl -fsSL "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
            | tar -xz --strip-components=1 -C /usr/local/bin \
                "sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache" \
        && chmod +x /usr/local/bin/sccache)

# SCCACHE_* are intentionally NOT promoted to ENV here — see
# crates/sandbawks-controller/Dockerfile for the rationale; the cacher
# + builder stages re-declare them as ARGs and gate sccache via a
# per-RUN guard.
ENV RUSTC_WRAPPER=sccache \
    CARGO_INCREMENTAL=0 \
    AWS_EC2_METADATA_DISABLED=true \
    CC_x86_64_unknown_linux_musl=musl-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=static"

FROM chef AS planner
# See crates/sandbawks-controller/Dockerfile for why the planner
# disables `RUSTC_WRAPPER` (issue #130).
ENV RUSTC_WRAPPER=
COPY Cargo.toml Cargo.lock ./
COPY crates/sandbawks-types/Cargo.toml      crates/sandbawks-types/Cargo.toml
COPY crates/sandbawks-controller/Cargo.toml crates/sandbawks-controller/Cargo.toml
COPY crates/envd/Cargo.toml                 crates/envd/Cargo.toml
COPY crates/sandbawks-api/Cargo.toml        crates/sandbawks-api/Cargo.toml
COPY crates/sandbawks-node/Cargo.toml       crates/sandbawks-node/Cargo.toml
COPY crates/sandbawks-observability/Cargo.toml crates/sandbawks-observability/Cargo.toml
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS cacher
ARG SCCACHE_BUCKET=""
ARG SCCACHE_ENDPOINT=""
ARG SCCACHE_REGION=""
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

FROM chef AS builder
ARG SCCACHE_BUCKET=""
ARG SCCACHE_ENDPOINT=""
ARG SCCACHE_REGION=""
ARG SCCACHE_S3_USE_SSL=""
ARG SCCACHE_S3_ENABLE_VIRTUAL_HOST_STYLE=""
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
        -p sandbawks-api --bin sandbawks-api \
    && (sccache --show-stats || true)

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder \
        /src/target/x86_64-unknown-linux-musl/release/sandbawks-api \
        /usr/local/bin/sandbawks-api
USER nonroot
ENTRYPOINT ["/usr/local/bin/sandbawks-api"]
