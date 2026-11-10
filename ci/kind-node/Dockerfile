# sandbawks-kind-node: a custom kind node image with gVisor's `runsc`
# pre-baked.
#
# Built and published by .github/workflows/kind-node-image.yaml; will be
# consumed (Plan 05 T13) by `deploy/kind/cluster.yaml` so the smoke
# cluster's nodes ship a working `runsc` binary plus a containerd
# runtime handler that the `gvisor` RuntimeClass (T12) points at.
#
# Why a custom image instead of post-create provisioning:
#
#   - The vanilla `kindest/node:v1.31.0` image has containerd pre-baked
#     but no `runsc` and no `runsc` runtime handler in its containerd
#     config. Adding both via a `kind create` lifecycle hook would mean
#     restarting containerd inside every smoke run, which is slow and
#     racy against the kubelet's bootstrap.
#   - Baking the binary + the conf.d drop-in into the image means
#     containerd starts with the handler already registered; the
#     `RuntimeClass: gvisor` resource we install via Helm (T12) finds a
#     backing handler on day one.
#
# Tool versions are pinned via ARG. The `GVISOR_RELEASE` MUST stay in
# lockstep with the version baked into `crates/sandbawks-node/Dockerfile`
# — the snapshot pause/resume code path in the node DaemonSet shells
# out to the *host's* runsc (via the containerd runtime handler), and
# checkpoint/restore is only defined to work when both ends run the
# same runsc version.

# ---------------------------------------------------------------------------
# Stage 1 (shim-builder): compile `containerd-shim-sandbawks-v1` from the
# in-repo Go subtree (`shim/`).
#
# The builder image is pinned to match `shim/go.mod`'s `go 1.26.3` directive.
# Using an older base (the plan draft said `golang:1.23`) would force Go's
# GOTOOLCHAIN=auto machinery to download the 1.26.3 toolchain mid-build — a
# slow, network-fragile step that the matching base avoids entirely.
#
# CGO_ENABLED=0 produces a fully static binary. This is both necessary and
# correct: the golang:*-alpine image ships no C compiler, so a cgo build of
# the net resolver would fail; and a static binary runs in the glibc-based
# kindest/node final stage (a musl-linked alpine cgo build would not).
ARG GO_BUILDER_VERSION=1.26.3
# Fully-qualified docker.io reference, matching every other base image in this
# repo (see crates/sandbawks-node/Dockerfile's debian/busybox stages).
FROM docker.io/library/golang:${GO_BUILDER_VERSION}-alpine AS shim-builder
WORKDIR /src
# Copy the manifests first so module downloads cache independently of source
# churn — editing shim/*.go won't re-run `go mod download`.
COPY shim/go.mod shim/go.sum ./
RUN go mod download
COPY shim/ ./
# -trimpath strips build-host paths for reproducibility across builders;
# -ldflags='-s -w' drops the symbol table and DWARF, shaving several MB off
# the static binary baked into every kind node. The shim is a shipped
# artifact, not debugged in-image, so the lost in-binary symbols are an
# acceptable trade — any crash is diagnosed from source and logs.
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' \
    -o /out/containerd-shim-sandbawks-v1 \
    ./cmd/containerd-shim-sandbawks-v1

# ---------------------------------------------------------------------------
# Stage 2: the kind node image itself.
# ---------------------------------------------------------------------------
# Fully-qualified docker.io reference, same as the builder stage.
FROM docker.io/kindest/node:v1.31.0

# Keep in lockstep with crates/sandbawks-node/Dockerfile's GVISOR_RELEASE.
# Bumping this ARG without bumping the node Dockerfile (or vice versa)
# will silently break snapshot pause/resume — the host runsc and the
# in-pod runsc must match.
#
# It must ALSO track the gVisor version `shim/go.mod` pins
# (`v0.0.0-20260527191743-…`): containerd-shim-sandbawks-v1 drives gVisor's
# `CreateWithFSRestore` / `Restore` extension, which has runsc create the
# container with a `-fs-restore-image-path` flag. The old `20240603.0` binary
# predates that flag, so restore failed with `flag provided but not defined:
# -fs-restore-image-path`. `20260525.0` is the latest published gVisor release
# at/before the shim's pinned commit (2026-05-27) — it carries the extension
# and is protocol-compatible with the shim. (gVisor cut no 05-27 release;
# releases land ~weekly.)
ARG GVISOR_RELEASE=20260525.0

# gVisor publishes binaries under `x86_64` and `aarch64`. We pin
# explicitly rather than using `dpkg --print-architecture` (which would
# emit `amd64`/`arm64` and 404 against the release path) so the build
# fails loudly on a bad pin instead of silently producing a broken image.
ARG GVISOR_ARCH=x86_64

# Download + verify both `runsc` AND the matching
# `containerd-shim-runsc-v1` shim binary. Both are required: containerd
# resolves the `io.containerd.runsc.v1` runtime_type (set in
# `containerd-runsc.toml`) by exec'ing the shim, which then launches
# `runsc` proper. Without the shim, containerd fails pod sandbox
# creation with `runtime "io.containerd.runsc.v1" binary not installed
# "containerd-shim-runsc-v1": file does not exist`.
#
# sha512 verification mirrors `crates/sandbawks-node/Dockerfile` so a
# corrupted GCS object can't slip into the kind cluster image either.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    base="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE}/${GVISOR_ARCH}"; \
    for bin in runsc containerd-shim-runsc-v1; do \
        curl -fsSL -o "/tmp/${bin}"        "${base}/${bin}"; \
        curl -fsSL -o "/tmp/${bin}.sha512" "${base}/${bin}.sha512"; \
        (cd /tmp && sha512sum -c "${bin}.sha512"); \
        install -m 0755 "/tmp/${bin}" "/usr/local/bin/${bin}"; \
        rm -f "/tmp/${bin}" "/tmp/${bin}.sha512"; \
    done; \
    /usr/local/bin/runsc --version

# Register the runsc runtime handler by appending to kind's primary
# containerd config.
#
# Why append, not a `conf.d/` drop-in: kind's `/etc/containerd/config.toml`
# does not set the `imports = [...]` top-level key, so containerd never
# loads anything under `/etc/containerd/conf.d/`. We verified this
# empirically — dropping a file there leaves `crictl info` showing only
# `runc` and `test-handler` as registered runtimes. Appending the
# fragment directly to the primary config makes containerd pick up
# `runsc` on first boot, which is what the `gvisor` RuntimeClass needs.
#
# The fragment lives in `ci/kind-node/containerd-runsc.toml` (rather
# than being inlined here as a `RUN cat <<EOF`) so that diffs to the
# runtime config are reviewable on their own, independent of any
# unrelated Dockerfile churn.
#
# It is bind-mounted from the build context for the duration of the RUN
# rather than COPY'd in: a COPY would persist the fragment in its own
# image layer (a following `rm` only deletes it from a *later* layer, not
# from the layer graph), whereas a `--mount=type=bind` source never enters
# the image at all.
RUN --mount=type=bind,source=ci/kind-node/containerd-runsc.toml,target=/tmp/containerd-runsc.toml \
    set -eux; \
    cat /tmp/containerd-runsc.toml >> /etc/containerd/config.toml

# Bake the sandbawks custom shim built in stage 1. Containerd resolves the
# `io.containerd.sandbawks.v1` runtime_type (set in
# `containerd-sandbawks-runsc.toml`) to this binary by name convention
# (io.containerd.<name>.<version> -> containerd-shim-<name>-<version>).
COPY --from=shim-builder /out/containerd-shim-sandbawks-v1 \
    /usr/local/bin/containerd-shim-sandbawks-v1

# Register the `sandbawks-runsc` runtime handler, using the exact same
# append-not-conf.d pattern as the `runsc` fragment above — kind's
# containerd config has no `imports = [...]`, so a conf.d/ drop-in would be
# silently ignored. The `sandbawks-runsc` RuntimeClass (Plan 07 T6) points
# at the handler name registered here. Bind-mounted (not COPY'd) for the
# same no-dead-layer reason as the runsc fragment above.
RUN --mount=type=bind,source=ci/kind-node/containerd-sandbawks-runsc.toml,target=/tmp/containerd-sandbawks-runsc.toml \
    set -eux; \
    cat /tmp/containerd-sandbawks-runsc.toml >> /etc/containerd/config.toml
