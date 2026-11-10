# Plan 05 T11 — Custom `sandbawks-kind-node` image (runsc pre-baked) + workflow

## Summary

Layers gVisor's `runsc` binary, its containerd shim, and a containerd CRI
runtime-handler config block onto `kindest/node:v1.31.0`, producing
`ghcr.io/hypotheosis/sandbawks-kind-node:{sha-<sha>,latest}` so the kind smoke
cluster has a backing handler for the `gvisor` RuntimeClass that Plan 05
T12 will install via Helm. Closes #147.

## Files touched

- `ci/kind-node/Dockerfile` — new. Two-step layer atop
  `kindest/node:v1.31.0`: (1) download + sha512-verify `runsc` and
  `containerd-shim-runsc-v1` from the pinned gVisor release path under
  `https://storage.googleapis.com/gvisor/releases/`, install both into
  `/usr/local/bin`; (2) append the runsc runtime-handler stanza to
  `/etc/containerd/config.toml`. `GVISOR_RELEASE` is pinned to the same
  value (`20240603.0`) as `crates/sandbawks-node/Dockerfile` so the host
  runsc in the kind node and the in-pod runsc in the node DaemonSet
  speak the same checkpoint format — pause/resume across versions is
  undefined.
- `ci/kind-node/containerd-runsc.toml` — new. Standalone fragment
  registering the `runsc` handler under
  `[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]`,
  appended onto `/etc/containerd/config.toml` at image build time.
  Lives in its own file so future runtime-config changes are reviewable
  on their own diff.
- `.forgejo/workflows/kind-node-image.yaml` — new. Mirrors
  `ci-runner-image.yaml`: `workflow_dispatch` + `push` on `main` with a
  `ci/kind-node/**` path filter, publishes to Harbor with the
  `sha-<sha>` + `latest` + `YYYY.MM` tag scheme used elsewhere in the
  repo. Build context is the repo root (not `ci/kind-node/`) because
  the Dockerfile `COPY`s `ci/kind-node/containerd-runsc.toml` via its
  path-from-root.
- `justfile` — adds the `kind-node-image-build` recipe, placed next to
  the `KIND_NODE_IMAGE` constant so the local-build path for the future
  `sandbawks-kind-node:dev` image is discoverable from the existing
  cluster-up flow.

## Design notes

### Why append to `/etc/containerd/config.toml` instead of `/etc/containerd/conf.d/runsc.toml`?

The issue body proposed dropping a file into `/etc/containerd/conf.d/`,
which is the canonical containerd config-overlay path. The first build
of this image followed that recipe verbatim and produced an image where
`crictl info` showed only `runc` and `test-handler` as registered
runtime handlers — `runsc` was absent. Cause: the
`kindest/node:v1.31.0` containerd config does **not** set
`imports = [...]` at the top level, so containerd never enumerates
anything under `conf.d/`. The fix that survived empirical verification
is to concatenate the fragment onto the primary config at image-build
time; the fragment file is still preserved as a standalone artifact so
diffs to the runtime config remain easy to review.

### Why download `containerd-shim-runsc-v1` separately?

`runsc` alone is not enough. The containerd `io.containerd.runsc.v1`
runtime type is resolved by `exec`'ing a `containerd-shim-runsc-v1`
shim binary, which in turn launches `runsc` proper. Without the shim,
pod sandbox creation fails with the very explicit error
`runtime "io.containerd.runsc.v1" binary not installed
"containerd-shim-runsc-v1": file does not exist`. The first build that
shipped only `runsc` reproduced this error; adding the shim to the
download loop fixed it.

### Why pin `GVISOR_ARCH=x86_64` instead of using `dpkg --print-architecture`?

The issue body sketched `arch="$(dpkg --print-architecture)"`, which
emits `amd64` on x86_64 Debian. gVisor's release path uses the kernel
arch name (`x86_64`, `aarch64`), so `${base}/amd64/runsc` would 404. An
explicit `ARG GVISOR_ARCH=x86_64` matches the pattern already
established in `crates/sandbawks-node/Dockerfile` and fails the image
build loudly if the pin drifts off gVisor's release layout.

### Image size

The built image lands at ~1.56 GiB, ~30 MiB above vanilla
`kindest/node:v1.31.0` (1.53 GiB). The delta is dominated by the two
gVisor binaries (~25 MiB) plus a transient `apt update` layer for
`curl` + `ca-certificates`. The apt-cache cleanup is in the same `RUN`
step so the lists never land in a published layer.

## Verification

Local:

- `just fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `just test` — all green (Rust workspace untouched, but the gates are
  run as part of the pre-push protocol).
- `just kind-node-image-build` — produces
  `sandbawks-kind-node:dev`, ~1.56 GiB. The build's final step prints
  `runsc version release-20240603.0 / spec: 1.1.0-rc.1`.
- `kind create cluster --image sandbawks-kind-node:dev` — succeeds.
- `docker exec <node-container> crictl info | grep -A2 runsc` — shows
  `"runsc": { "runtimeType": "io.containerd.runsc.v1" }` registered
  alongside `runc` and `test-handler`. This is the issue's stated
  acceptance criterion and it passes.

## Known follow-up for T12/T14

Launching a Pod with `runtimeClassName: gvisor` in this kind cluster
fails with
`cannot run with network enabled in root network namespace` — the
default `runsc --network=host` doesn't work inside a containerized kind
node. The fix is one of:

- a `runsc.toml` (referenced by
  `[...].runtimes.runsc.options.ConfigPath`) setting `network = "sandbox"`,
  or
- a `[...].runtimes.runsc.options.TypeUrl = "io.containerd.runsc.v1.options"`
  block with an inline `Network` field.

This is intentionally **out of scope** here — the issue's "Out of scope"
list explicitly defers end-to-end gVisor pod exercise to T12 (the
`RuntimeClass: gvisor` Helm template) and T14 (integration test).
Calling it out so T12's author has the gotcha in hand and isn't
surprised when the first pod attempt fails.
