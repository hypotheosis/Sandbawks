# Plan 07 T5 — bake containerd-shim-sandbawks-v1 into the kind-node image

- **Issue:** #209 (epic #204, Plan 07 Task 5)
- **Date:** 2026-05-29T12:16:19Z
- **Area:** `ci/kind-node/` — gVisor kind-node image

## Summary

The custom shim `containerd-shim-sandbawks-v1` (built across Plan 07 T1–T4)
now ships pre-baked in the `sandbawks-kind-node` image, and containerd starts
with a `sandbawks-runsc` runtime handler already registered. This is the
runtime-side prerequisite for the `RuntimeClass: sandbawks-runsc` resource
(Plan 07 T6) and for any Pod that opts into `runtimeClassName: sandbawks-runsc`.

A new multi-stage build compiles the in-repo Go subtree (`shim/`) into a static
binary and copies it into the final image alongside `runsc`; a new containerd
config fragment registers the handler using the same append-not-`conf.d/`
pattern the existing `runsc` fragment uses.

## Changes

- `ci/kind-node/Dockerfile`
  - New first stage `shim-builder` (`FROM golang:${GO_BUILDER_VERSION}-alpine`,
    default `1.26.3`) that runs `CGO_ENABLED=0 go build` over `shim/` and emits
    `/out/containerd-shim-sandbawks-v1`. `go.mod`/`go.sum` are copied before the
    source so module downloads cache independently of source churn.
  - Final stage gains a `COPY --from=shim-builder` of the binary into
    `/usr/local/bin/`, plus a `COPY … && cat … >> /etc/containerd/config.toml`
    step for the new fragment — mirroring the existing `runsc` registration.
- `ci/kind-node/containerd-sandbawks-runsc.toml` (new)
  - Registers handler `sandbawks-runsc` with `runtime_type =
    "io.containerd.sandbawks.v1"`.

## Notes / deviations from the issue text

- **Builder Go version.** The issue/plan draft said `golang:1.23-alpine`, but
  `shim/go.mod` requires `go 1.26.3`. A 1.23 base would force Go's
  `GOTOOLCHAIN=auto` to download the 1.26.3 toolchain mid-build (slow, network-
  fragile — and it fails outright in toolchain-restricted sandboxes). The
  builder is pinned to `1.26.3-alpine` to match `go.mod` exactly, exposed as
  `ARG GO_BUILDER_VERSION` for future bumps.
- **`runtime_type` value.** The plan's literal example used
  `runtime_type = "io.containerd.sandbawks-runsc.v1"`. That is a typo: containerd
  resolves `io.containerd.<name>.<version>` to the binary
  `containerd-shim-<name>-<version>`, so that string would look for
  `containerd-shim-sandbawks-runsc-v1`, which does not exist. The shipped binary
  is `containerd-shim-sandbawks-v1` and the shim's `NewShimManager` (in
  `shim/cmd/containerd-shim-sandbawks-v1/main.go`) registers
  `io.containerd.sandbawks.v1`, so the fragment uses that type. The handler is
  still *named* `sandbawks-runsc` (the table key) to match the RuntimeClass —
  only the type tracks the binary. The plan's own prose ("the binary we ship at
  containerd-shim-sandbawks-v1 satisfies that") agrees with this correction.
- **`CGO_ENABLED=0`.** Necessary (the alpine Go image has no C compiler, so a cgo
  build of the net resolver would fail) and correct (a static binary runs in the
  glibc-based `kindest/node` final stage; a musl-linked cgo build would not).

## Verification

- `just kind-node-image-build` → succeeds; shim compiles in-stage.
- Image introspection: `/usr/local/bin/containerd-shim-sandbawks-v1` present
  (static ELF), and `/etc/containerd/config.toml` carries both the `runsc` and
  `sandbawks-runsc` runtime tables.
- `kind create cluster --image sandbawks-kind-node:dev` → ready; `crictl info`
  reports all four handlers:
  - `runc -> io.containerd.runc.v2`
  - `runsc -> io.containerd.runsc.v1`
  - `sandbawks-runsc -> io.containerd.sandbawks.v1`
  - `test-handler -> io.containerd.runc.v2`
  `containerd config dump` confirms the merged config parses the
  `sandbawks-runsc` table. Cluster torn down after the check.
- Workspace gate (unaffected by this change, run for safety): `just fmt`,
  `just clippy`, `just test` all pass.
