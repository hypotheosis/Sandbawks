# Node-side Kata runtime support (epic #594)

**Date:** 2026-06-18T23:31:15Z
**Issue:** #594
**Branch:** `feat/node-kata-runtime-support`

## Context

A `SandboxWarmPool` on an AKS Kata node pool (`kata-mshv-vm-isolation`) never
came online. PR #593 added sandbox-Pod `tolerations`/`nodeSelector` to
`SandboxTemplate`, which was necessary but not sufficient: the **node side** was
gVisor/`runsc`-specific end to end. Three remaining gaps, fixed here:

- **A.** The `sandbawks-node` DaemonSet had no way to tolerate a tainted runtime
  pool, so it never staged envd nor advertised `sandbawks.dev/envd-staged` on the
  Kata nodes — sandbox Pods pinned there by the RuntimeClass nodeSelector wedged
  in `Pending`.
- **B.** The `envd-staged` staging gate is hardcoded to require the `runsc`
  runtime handler; Kata nodes register `kata`, and the override flag was not
  exposed through the Helm chart.
- **C.** Pause/resume was `runsc checkpoint`/`restore`-only; no Kata adapter
  existed.

## Changes

### Phase A — node DaemonSet scheduling (`deploy/helm`)

- `values.yaml`: added `node.tolerations` (default `[]`) and `node.nodeSelector`
  (default `{}`).
- `node-daemonset.yaml`: renders both into the pod spec via `with` guards.
  Default render is **byte-identical** to before (verified with `helm template`).
- The DaemonSet controller already tolerates the stock node-condition taints but
  NOT custom pool taints, so these knobs are what let the DS land on e.g. an AKS
  Kata pool tainted `kata=true:NoSchedule`.

### Phase B — runtime-aware staging gate (`deploy/helm`)

- `values.yaml`: added `node.runtimeHandler` (default `""`).
- `node-daemonset.yaml`: when set, renders `SANDBAWKS_GVISOR_RUNTIME_HANDLER`
  (the existing, env-settable flag backing the #417 gate). On a Kata pool set
  `node.runtimeHandler: kata` so the gate matches the handler the kubelet
  actually registers. Unset → binary default `runsc`, unchanged.

### Phase C — Kata pause/resume adapter (`crates/sandbawks-node`)

- New `kata.rs` module: `KataAdapter` implements the existing
  `RuntimeAdapter` trait, driving pause/resume through **Cloud Hypervisor**'s
  snapshot/restore API (`vm.pause` → `vm.snapshot` → storage; `vm.restore` →
  `vm.resume`). The `state.json` / `config.json` / `memory-ranges` bundle is
  streamed through the same `SnapshotSink`/`SnapshotSource` abstraction the gVisor
  adapter uses, so the controller state machine and S3/Azure backends are reused.
- External I/O is injected via two traits — `CloudHypervisor` (VMM control plane,
  production impl `HttpCloudHypervisor` over the VM's Unix API socket) and
  `KataVmLocator` (pod→socket mapping, production impl `RunVcKataLocator`) — so the
  pause/resume/validate orchestration is fully unit-tested against fakes.
- Resume and `validate_restorable` verify every part's sha256 against the
  committed manifest and fail closed (`SnapshotDownload` / empty-memory
  `CheckpointFailed`) before touching the VMM.
- Node binary: new `--runtime-adapter` (`SANDBAWKS_RUNTIME_ADAPTER`, default
  `gvisor`) selects `gvisor` | `kata`/`kata-fc`; `--kata-run-root` /
  `--kata-api-socket-name` configure socket discovery.

## Validation boundary (Phase C)

The orchestration is unit-tested, but two **live** seams need a real Kata node
and are NOT validated end to end (tracked in #594):

1. `HttpCloudHypervisor`'s actual Unix-socket round-trip (request serialization
   and response parsing ARE unit-tested; the socket I/O is not).
2. `RunVcKataLocator`'s socket-path assumption and the pod-UID → Kata sandbox-id
   resolution. Stock Kata exposes no snapshot/restore through the containerd
   shim, so arranging a restore-ready Cloud Hypervisor process is the open
   integration question.

With A+B, warm members can reach **Running** on Kata; C makes pause/resume
*possible* but unproven until exercised on a Kata node.

## Tests

- `cargo fmt --check` — clean.
- `cargo clippy -p sandbawks-node --all-targets -- -D warnings` — clean.
- `cargo test -p sandbawks-node` — all green, incl. 8 new `kata::tests`
  (pause/snapshot ordering, pause→validate→resume round-trip, tampered-part
  fail-closed, snapshot-failure→`CheckpointFailed`, request builder, status
  parser, `file://` URL).
- `helm template` — default node DS unchanged; kata-configured render emits the
  toleration, nodeSelector, and `SANDBAWKS_GVISOR_RUNTIME_HANDLER=kata`.

## Deploy notes

- Helm skips CRDs — no CRD change here, so no `kubectl apply -f deploy/crds`
  needed for this PR.
- Chart `version` bumped `0.1.21` → `0.1.22`.
- On a Kata pool, set: `node.tolerations` for the pool taint,
  `node.nodeSelector` for the pool, `node.runtimeHandler: kata`, and run the node
  DaemonSet with `SANDBAWKS_RUNTIME_ADAPTER=kata`.
