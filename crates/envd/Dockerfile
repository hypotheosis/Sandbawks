# Multi-stage build for the `envd` carrier image.
#
# This image is the *carrier* for the in-sandbox `envd` agent — NOT a
# reference SandboxTemplate. Under the injection model (issue #265) the
# node DaemonSet's `stage-envd` init container copies the static `envd`
# binary out of this image onto a hostPath, and every sandbox Pod execs
# it as its entrypoint, so envd is never baked into a customer's template
# rootfs. The reference SandboxTemplate points its `image` at a
# standalone, envd-free rootfs (e.g. `ghcr.io/hypotheosis/compound-sandbox`) and
# its `envdImage` at this carrier (see kustomize/sandbox-template.yaml).
#
# Layout (cargo-chef pattern — see crates/sandbawks-controller/Dockerfile
# for the full rationale; the chef / planner / cacher stages here are
# byte-identical to that file's and to crates/sandbawks-api/Dockerfile so
# BuildKit can share the cooker layer hash across all three builds when
# the workspace manifests haven't changed). The only divergences are the
# `-p` flag in the final builder stage and the runtime image: this carrier
# ships the static `envd` binary on `scratch` (no userland at all). The
# node DaemonSet's `stage-envd` init container stages it with
# `envd self-install <dest>` (issue #268), which copies `/proc/self/exe`
# with no shell — so the carrier needs neither `cp` nor the
# `python:3.12-slim` base that used to provide it.

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
# the sccache wrapping + S3 backend ARGs (issue #130). Kept
# byte-identical to that file's chef stage so BuildKit can share the
# cooker layer hash across all three workspace image builds when the
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
# Rust target triple, single source of truth for the build invocation, the
# static-link guard's binary path, and the final-stage COPY below — so the
# three can never drift out of sync if the target changes (review feedback
# on PR #274). Only the builder + final stages are parameterized: the
# shared chef / planner / cacher stages stay byte-identical to the
# controller / api Dockerfiles (preserving BuildKit's cooker layer
# sharing), so their `rustup target add`, `CC_*` / `CARGO_TARGET_*` env,
# and `cargo chef cook --target` remain musl-pinned. Overriding TARGET
# therefore only makes sense for another musl triple the chef stage
# already provisions (e.g. an aarch64-unknown-linux-musl cross leg also
# adds that target + linker upstream); a non-musl override fails loudly at
# build time rather than silently shipping a dynamic binary.
ARG TARGET=x86_64-unknown-linux-musl
# `file` powers the static-link CI guard at the end of this stage (issue
# #267). Installed in the builder stage rather than the shared chef stage
# so the chef / planner / cacher stages stay byte-identical to the
# controller / api Dockerfiles and BuildKit keeps sharing the cooker layer
# hash across all three builds. Placed before the source COPY so this
# layer is cache-stable (it never depends on the workspace contents).
RUN apt-get update && apt-get install -y --no-install-recommends file \
    && rm -rf /var/lib/apt/lists/*
COPY --from=cacher /src/target target
# Git SHA stamped into the binary's OTel telemetry as `vcs.revision` (issue
# #387). CI passes `--build-arg GIT_SHA=<commit>`; a vanilla `docker build .`
# defaults to `unknown` because `.dockerignore` excludes `.git` from the build
# context, so build.rs's git fallback can't resolve a SHA in-image. This ENV
# is builder-stage-only: the `FROM scratch` final stage doesn't inherit it, so
# no build metadata leaks into the runtime image (a future single-stage
# refactor would need to re-declare or strip it).
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
        --target "${TARGET}" \
        -p envd --bin envd \
    && (sccache --show-stats || true)

# Static-link CI guard (issue #267).
#
# The init-container injection model (issue #265 / PR #266) copies this
# `envd` binary out of the image and runs it inside an *arbitrary*
# customer rootfs. That only works if `envd` has no dependency on the
# template image's libc or dynamic loader — i.e. it must stay fully
# statically linked. The musl target + `-C target-feature=+crt-static`
# above produce a static binary today, but nothing else stops a future
# dependency from silently reintroducing a dynamic link, which would break
# cold start across every template at once.
#
# This assertion runs as part of every envd image build — locally via
# `just envd-image-build` and in CI via the `build-images` matrix in
# .github/workflows/kind-smoke.yaml (the envd leg builds on every PR and
# push, even fork PRs) — so a regression fails the pipeline here rather
# than shipping a binary that won't run in customer rootfs. As a dedicated
# layer keyed on the build output above, it re-runs whenever the binary
# changes and is cached (already-verified) otherwise.
#
# We accept both the classic "statically linked" wording (the current
# non-PIE build, since `-C relocation-model=static` disables PIE) and
# "static-pie linked" (in case the relocation model is relaxed later);
# both mean no dynamic loader is required. Anything reporting a dynamic
# interpreter fails the build.
RUN envd_bin="/src/target/${TARGET}/release/envd"; \
    file_out=$(file -b "$envd_bin"); \
    echo "envd ELF: ${file_out}"; \
    case "$file_out" in \
        *"statically linked"*|*"static-pie linked"*) \
            echo "OK: envd is statically linked (issue #267 guard passed)" ;; \
        *) \
            echo "ERROR: envd is NOT statically linked (issue #267 guard failed):" >&2; \
            echo "  ${file_out}" >&2; \
            echo "The init-container injection model requires a fully static envd;" >&2; \
            echo "a dynamic link breaks cold start in arbitrary customer rootfs." >&2; \
            exit 1 ;; \
    esac

# Minimal carrier: just the static `envd` binary on `scratch` (issue
# #268). envd is fully static (asserted by the guard above, issue #267),
# so it needs no libc, dynamic loader, or userland at runtime — `scratch`
# is the smallest possible base.
#
# This image is only ever run by the node DaemonSet's `stage-envd` init
# container, which execs `envd self-install /sandbawks/envd/envd` (as
# root, to write the root-owned `DirectoryOrCreate` hostPath) and exits.
# So there is deliberately no `USER`, no state directory, and no `EXPOSE`:
# envd's server path — which binds 49983/49984 and writes the
# `--state-file` snapshot under `/var/tmp/envd` (world-writable so a
# non-root workload `USER` can persist it) — runs from the *node-staged*
# binary inside the customer rootfs, never from this image.
FROM scratch
# Re-declared because ARGs do not cross FROM boundaries; the default must
# match the builder stage's so a plain `docker build` needs no --build-arg.
ARG TARGET=x86_64-unknown-linux-musl
COPY --from=builder \
        /src/target/${TARGET}/release/envd \
        /usr/local/bin/envd

ENTRYPOINT ["/usr/local/bin/envd"]
