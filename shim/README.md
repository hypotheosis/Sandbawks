# `shim/` — the one Go binary in a Rust repo

This subtree holds `containerd-shim-sandbawks-v1`, a custom
[containerd](https://containerd.io/) runtime shim that drives gVisor's memory
`Restore` task-service extension RPC at container-start time, so a
kubelet-managed container can be born *already restored* from a checkpoint. It
is the runtime half of sandbawks' pause / resume story (Plan 07).

Under sandbawks' default `--overlay2=root:self`, a workload's rootfs writes
live in the sandbox-internal overlay-tmpfs, which the memory checkpoint
captures in `pages.img`. A plain `Create` followed by a memory `Restore`
therefore reconstructs both memory *and* filesystem — no separate
filesystem-restore step is needed. An earlier design also dispatched
`CreateWithFSRestore`, but that double-restored the overlay-tmpfs and failed
every restore with `containerManager.Restore: EOF`; it was removed in #232.
See `changelog/` for the experiments (#230/#231/#233) behind that decision.

## Why Go, in an otherwise all-Rust repo

The rest of sandbawks is Rust. This binary is deliberately not.

gVisor ships its reusable shim machinery as Go packages under
[`gvisor.dev/gvisor/pkg/shim/v1/...`](https://github.com/google/gvisor/tree/master/pkg/shim/v1).
Those packages already expose the memory `Restore` extension RPC we need;
containerd itself never calls it because CRI has no "restore" concept. The
minimum glue is therefore a thin Go decorator that links `pkg/shim/v1` as a
library and forwards every standard task RPC to it, intercepting only `Create`
(to record a restore annotation's staged image path) and `Start` (to dispatch
the memory `Restore` against that path) when a sandbawks restore annotation is
present.

A Rust reimplementation of gVisor's shim packages would be enormously more
code — and would have to track upstream gVisor's ttrpc surface on every
release — than a small wrapper that imports them directly. Go interop is an
acceptable cost for exactly one binary that talks ttrpc; it is not the start
of a polyglot codebase.

The Rust side of sandbawks does **not** depend on this subtree. The shim is
shipped as a standalone binary baked into the gVisor node image
(`ci/kind-node/`), discovered by containerd via the `sandbawks-runsc` runtime
handler, whose `runtime_type` is `io.containerd.sandbawks.v1` (the binary-name
convention that resolves to `containerd-shim-sandbawks-v1`). The Cargo
workspace neither builds nor links it.

## Layout

```
shim/
  go.mod                                   module github.com/hypotheosis/sandbawks/shim
  cmd/containerd-shim-sandbawks-v1/
    main.go                                shim entry point
  internal/wrapper/
    service.go                             decorator over gVisor's TaskServiceExt
```

`internal/wrapper.Service` decorates gVisor's runsc task service: it is
installed via the `extension.NewExtension` hook (see `main.go`) and forwards
every RPC to the wrapped service. The only interceptions are `Create` (records
a restore annotation's staged image path) and `Start` (dispatches a memory
`Restore` against that path); every other RPC — including the
interface-mandated `CreateWithFSRestore`, which the decorator no longer
dispatches to — is a verbatim pass-through.

## Building

From the repo root:

```sh
just shim-build
```

That runs `go build` and emits the binary to
`target/shim/containerd-shim-sandbawks-v1`. The `shim/` subtree manages its
own dependencies via `go.mod`; it is invisible to `cargo`.

## See also

- Plan 07 — `docs/superpowers/plans/2026-05-28-sandbawks-07-shim-wrapper-resume.md`
- Issue #176 — the
  originating runtime-side resume gap and locked shim design.
