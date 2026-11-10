# Multi-stage build for the sandbawks-node binary.
#
# Layout (cargo-chef pattern — see crates/sandbawks-controller/Dockerfile
# for the full rationale; the chef / planner / cacher stages here are
# byte-identical to that file's and to crates/sandbawks-api/Dockerfile
# and crates/envd/Dockerfile so BuildKit can share the cooker layer
# hash across all four workspace image builds when the workspace
# manifests haven't changed). The divergences are:
#
#   - the `-p` flag in the final builder stage targets `sandbawks-node`;
#   - a dedicated `runsc` downloader stage pulls the gVisor release tarball
#     and stages `runsc` for the runtime image to consume;
#   - the runtime image is `gcr.io/distroless/cc-debian12:nonroot` rather
#     than the `static-debian12:nonroot` used by controller/api. Plan 05
#     T10 specifies `cc-debian12` so the image has the C runtime + ld
#     loader available to any helper binary `runsc` may invoke during
#     `checkpoint`/`restore` — the workspace binary itself is the same
#     fully-static musl ELF as in the other workspace images.

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
# cooker layer hash across all four workspace image builds when the
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
        -p sandbawks-node --bin sandbawks-node \
    && (sccache --show-stats || true)

# ── runsc downloader ──
# Separate stage so the gVisor release tarball is fetched out-of-band
# from the cargo-chef pipeline; the layer's hash only depends on the
# pinned `GVISOR_RELEASE` ARG, so a workspace source change doesn't bust
# this download. The tarball model would be cleaner if upstream shipped
# one — gVisor publishes per-arch flat binaries instead, so this stage
# does a direct binary download + sha512 verification.
#
# Pinned to a known-good release. Bumps land in lockstep with
# `ci/kind-node/Dockerfile` (Plan 05 T11) so the runsc bundled in the
# sandbawks-node DaemonSet matches the runsc baked into the kind node
# image — pause and resume must run identical runsc versions or
# checkpoint format incompatibilities surface at restore.
#
# Plan 07: bumped 20240603.0 → 20260525.0 to track the gVisor version
# `shim/go.mod` pins. containerd-shim-sandbawks-v1 drives gVisor's
# `CreateWithFSRestore`/`Restore` extension (the `-fs-restore-image-path`
# runsc flag) the 2024 binary lacked; the host runsc must carry the same
# extension so the staged restore and its checkpoint format line up across
# the shim and the DaemonSet.
#
# debian:bookworm-slim is the lightest base that ships curl + sha512sum
# already (the rust:1.88-bookworm chef base also has these, but reusing
# it here would needlessly pin this stage to the workspace toolchain).
FROM docker.io/library/debian:bookworm-slim AS runsc
ARG GVISOR_RELEASE=20260525.0
ARG GVISOR_ARCH=x86_64
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# Both runsc AND its containerd shim are bundled: #546's restart-free
# convergence atomically copies *both* onto the host `/usr/local/bin`, so the
# host-side runsc and the DaemonSet's checkpoint-side runsc are guaranteed the
# same build (the gVisor C/R version-match requirement). The shim must therefore
# travel in the runtime image, not just runsc.
RUN set -eux; \
    base="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE}/${GVISOR_ARCH}"; \
    for bin in runsc containerd-shim-runsc-v1; do \
        curl -fsSL -o "/tmp/${bin}"        "${base}/${bin}"; \
        curl -fsSL -o "/tmp/${bin}.sha512" "${base}/${bin}.sha512"; \
        (cd /tmp && sha512sum -c "${bin}.sha512"); \
        install -m 0755 "/tmp/${bin}" "/usr/local/bin/${bin}"; \
    done; \
    /usr/local/bin/runsc --version

# ── busybox (nsenter) ──
# #550 Capability C: the privileged fleet-upgrader re-runs the host installer
# DETACHED — `nsenter` into PID 1's namespaces, then `systemd-run` a transient
# unit owned by the host's systemd — so the install survives the k3s restart that
# kills this Pod. The distroless `cc` runtime ships no util-linux, so bundle a
# busybox and expose its `nsenter` applet. busybox dispatches by argv[0] basename,
# so a *copy* named `nsenter` IS the nsenter applet — no symlink (which distroless
# can't create at runtime).
#
# MUST be the `:musl` (statically-linked) variant, NOT the default `:glibc` build:
# the glibc busybox is dynamically linked and requires GLIBC_2.38, but the
# distroless cc-debian12 runtime is bookworm (glibc 2.36), so a glibc busybox dies
# at load with `nsenter: libc.so.6: version 'GLIBC_2.38' not found` and the upgrade
# never launches. The musl build is static-PIE (no INTERP, no libc dependency) and
# runs on the runtime regardless of its glibc. The applet's existence is verified
# at build time so a busybox lacking it fails the image rather than the cluster.
FROM docker.io/library/busybox:musl AS busybox
RUN busybox 2>&1 | tr ',' '\n' | grep -qw nsenter \
    && mkdir -p /out \
    && cp /bin/busybox /out/busybox \
    && cp /bin/busybox /out/nsenter

# ── runtime ──
# Distroless cc — see header for the rationale on cc-debian12 over
# static-debian12. `nonroot` (uid/gid 65532) matches the DaemonSet's
# securityContext (Plan 04 chart) so /var/lib/sandbawks/snapshots
# hostPath mounts inherit a writable owner.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder \
        /src/target/x86_64-unknown-linux-musl/release/sandbawks-node \
        /usr/local/bin/sandbawks-node
COPY --from=runsc /usr/local/bin/runsc /usr/local/bin/runsc
# #546: the bundled shim is the convergence companion to runsc; the node binary
# atomically installs both onto the host so the runsc + shim pair never desync.
COPY --from=runsc /usr/local/bin/containerd-shim-runsc-v1 /usr/local/bin/containerd-shim-runsc-v1
# #550: static busybox + an `nsenter`-named copy of it, and the host installer
# script, for the privileged restart-bearing fleet upgrade (Capability C). Inert
# unless `node.runscUpgrade.enabled` arms the upgrader; bundling them keeps the
# host-side install self-contained (no curl-at-runtime, no util-linux on the host).
COPY --from=busybox /out/busybox /usr/local/bin/busybox
COPY --from=busybox /out/nsenter /usr/local/bin/nsenter
COPY scripts/install-runsc-k3s.sh /usr/local/bin/install-runsc-k3s.sh
USER nonroot
ENTRYPOINT ["/usr/local/bin/sandbawks-node"]
