# 2026-05-22 — Plan 02: Controller skeleton + state machine

## What landed

- New crate `crates/sandbawks-controller`: state machine, reconcile loop, store abstraction, fake runtime adapter, binary entrypoint.
- `RuntimeAdapter` trait + `SnapshotManifest` + `RuntimeError` added to `sandbawks-types::runtime` (shared contract between controller and the future node crate).
- Pure state machine: `Trigger`, `Action`, `transition()` function covering every edge from spec §5; absorbing `Failed` and `Terminating` sinks.
- Property tests (`proptest`) for totality, determinism, absorbing-state stickiness, no self-loops.
- `SandboxStore` trait: `KubeSandboxStore` for production, `FakeSandboxStore` for tests.
- Reconcile loop honors spec §10's durability-before-side-effects invariant: status patched, then adapter dispatched.
- Full-lifecycle integration test against fakes: Creating → Running → Paused → Running → Terminating.
- Helm chart's controller Deployment now runs the real binary (env-driven config; no more `--placeholder`).

## What's deliberately not here

- Pod materialization — Plan 03 (envd-rs + reference template image) lands the in-sandbox agent; pod creation is part of plan 04 or 05 depending on how E2B API translation shakes out.
- Real `RuntimeAdapter` impls (gVisor, Kata-FC, runc+CRIU) — Plans 05, 08, 09.
- Pod-readiness watcher — Plan 03 wires it; Plan 02 simulates via the `sandbawks.io/pod-ready` annotation in tests.
- Finalizer logic for clean pod / snapshot tear-down — deferred to Plan 05.
- Leader election — single-replica deployment in the Helm chart is sufficient until multi-replica HA becomes a goal.

## Next plan

`docs/superpowers/plans/2026-05-22-sandbawks-03-envd-and-template-image.md` —
in-sandbox `envd-rs` agent (gRPC surface on port 49983) and a reference
template OCI image that downstream pause/resume tests will run against.
