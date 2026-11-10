# Plan 07 T2 — pass-through `TaskService` decorator over gVisor v1 (issue #206)

**Date:** 2026-05-29T01:36:11Z
**Branch:** `feat-issue-206-passthrough-taskservice-decorator`
**Issue:** #206 (epic #204)
**Plan:** `docs/superpowers/plans/2026-05-28-sandbawks-07-shim-wrapper-resume.md`, Task 2

## Summary

Wires the `shim/` subtree (scaffolded in T1, #205) to gVisor's runsc shim
harness and installs the sandbawks decorator in front of the runsc task
service. The decorator (`internal/wrapper.Service`) implements every
`task.TaskService` RPC plus gVisor's two extension RPCs
(`CreateWithFSRestore`, `Restore`) and, for now, forwards every call straight
through to the wrapped service. No annotation parsing yet — that arrives in
T3 (Create → `CreateWithFSRestore`) and T4 (Start → `Restore`), which slot
their dispatch logic into this decorator without touching `main`.

## What landed

- **`shim/go.mod`** — added `gvisor.dev/gvisor` and bumped the `go` directive
  to `1.26.3` (see "gVisor version pin" below). `go mod tidy` pulled the
  containerd shim-v2 dependency graph.
- **`shim/go.sum`** — new (first dependencies in the subtree).
- **`shim/internal/wrapper/service.go`** — `Service` wraps
  `extension.TaskServiceExt`. Explicit per-method forwarding (not struct
  embedding) so T3/T4 have one obvious interception point and a future added
  interface method fails the build at the `var _ extension.TaskServiceExt`
  assertion rather than silently inheriting an embedded forward. `NewService`
  matches the `extension.NewExtension` hook signature.
- **`shim/internal/wrapper/service_test.go`** — table test over all 17
  standard RPCs + 2 extensions, each asserting the call reaches a recording
  fake; plus a constructor test that `NewService` wraps `next` and never opts
  out (`nil, nil`).
- **`shim/cmd/containerd-shim-sandbawks-v1/main.go`** — replaced the T1 stub.
  Sets `extension.NewExtension = wrapper.NewService`, blank-imports
  `pkg/shim/v1/plugin` (registers runsc's task-service plugin with
  containerd), and runs containerd's shim-v2 harness with
  `shimv1.NewShimManager("io.containerd.sandbawks.v1")`.

## gVisor version pin — deviation from the plan, recorded

The plan (and epic #204) said to pin gVisor to **`20240603.0`** — the release
baked into `sandbawks-kind-node` (`ci/kind-node/Dockerfile`) — "so the
extension RPC surface matches the runtime on the node." That is not buildable:

- gVisor `20240603.0` (June 2024) predates the `pkg/shim/v1` + `extension`
  restructure. At that release the shim lives at `pkg/shim/` and exposes only
  the containerd shim-v2 `TaskService` surface — there is **no**
  `extension.TaskServiceExt`, `CreateWithFSRestore`, or `Restore`. The entire
  epic's API does not exist there. The "locked in #176" pin rested on a false
  premise about that release's contents.
- The `extension` package the plan describes only exists in recent gVisor.

Per maintainer decision (issue #206 discussion): **bump to latest gVisor**
and build T2 exactly as written. Two practical wrinkles:

1. **`@go` branch, not the release tag.** gVisor's `release-*` tags point at
   the Bazel tree, which omits generated `*_go_proto` packages, so `go build`
   against a release tag fails (`does not contain package ...registers_go_proto`).
   gVisor's go-buildable distribution is the rolling **`go` branch**, which
   commits the generated files. Pinned to `gvisor.dev/gvisor@go` →
   `v0.0.0-20260527191743-a81fd9dd382e` (gVisor ~20260527, i.e. "latest").
2. **`go 1.26.3`.** The `go` branch's own `go.mod` requires `go 1.26.3`, so the
   subtree directive rose from `1.23` to `1.26.3`. `go1.26.3` is a published
   toolchain (the Go module proxy serves it — `go get` downloaded
   `go1.26.3 (linux/amd64)` during the pin), so `setup-go` reading
   `go-version-file: shim/go.mod` is *intended* to install it with no workflow
   change. That path is exercised by the `go` CI job on this PR (the green
   `just shim-check` above ran the workstation toolchain, not `setup-go`), so
   the toolchain-install claim is confirmed by that job rather than asserted
   here — verify it on the first CI run before relying on it.

**Deferred to T5 (kind-node image):** bump `ci/kind-node/Dockerfile`'s
`GVISOR_RELEASE` to a release that matches this shim's gVisor `go`-branch
commit, so the shim's ttrpc/extension surface matches the node's `runsc` at
runtime. T2's gate (`go test ./...`) does not talk to a real node, so the
version alignment is not load-bearing until the image lands.

## Runtime handler name

`NewShimManager("io.containerd.sandbawks.v1")` follows containerd's
binary-resolution convention (`io.containerd.<name>.<version>` →
`containerd-shim-<name>-<version>`), so it maps to the T1-locked binary
`containerd-shim-sandbawks-v1`. The epic's prose mentions a
`io.containerd.sandbawks-runsc.v1` handler, which would imply a
`containerd-shim-sandbawks-runsc-v1` binary — inconsistent with T1. The
`sandbawks-runsc` RuntimeClass → containerd runtime-handler → binary mapping
is finalized in T5/T6; this is noted in `main.go`.

## Validation

- `just shim-check` → gofmt clean, `go vet` clean, `go test ./...` green
  (19 forwarding subtests + constructor test), `go build ./...` clean.
- `just shim-build` → 42 MiB binary at
  `target/shim/containerd-shim-sandbawks-v1` (runsc-linked; the size jump from
  the T1 stub is expected).
- Exercised the binary: a bare invocation reports
  `io.containerd.sandbawks.v1: shim namespace cannot be empty` (confirms the
  real containerd shim-v2 harness runs under our manager name, not the T1
  marker), and `-h` prints the full containerd shim flag set.
- Local `just shim-check` requires a `GOPATH` distinct from `GOROOT`; this
  machine's default `GOPATH=$HOME/go` collides with its `GOROOT`, which is a
  pre-existing environment quirk unrelated to this change (CI's `setup-go`
  configures them separately).

## Deliberately not here

T3/T4 annotation dispatch (`io.sandbawks/restore-image-path` →
`CreateWithFSRestore` / `Restore`), the kind-node image stage and
`GVISOR_RELEASE` bump (T5), the `sandbawks-runsc` RuntimeClass (T6), and any
real-container exercise of the decorator (T11 smoke).
