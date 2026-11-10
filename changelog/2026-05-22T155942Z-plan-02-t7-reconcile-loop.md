# Plan 02 T7 — Reconcile loop with status-first durability

**Issue:** #35
**Plan:** `docs/superpowers/plans/2026-05-22-sandbawks-02-controller-skeleton.md` Task 7

## Summary

Lands `crates/sandbawks-controller/src/reconcile.rs` — the glue layer
that ties the pure state machine (T4), the `RuntimeAdapter` trait (T2,
fake impl T3), and the `SandboxStore` abstraction (T6) together into
one `pub async fn reconcile(name, ctx)` that kube-rs `Controller` can
drive. Honors the two invariants from the spec §10:

1. **Status is durable before side effects.** `status.phase` is patched
   to the next phase (e.g. `Pausing`) *before* the adapter is invoked.
   If the controller crashes between the two, the next reconcile reads
   `status.phase=Pausing` and recovers.
2. **Idempotent on retry.** Re-reading the same Sandbox in a steady
   state yields no trigger — the state machine returns `None` for
   `(Pausing, PauseRequested)` and friends, so the second reconcile is
   a no-op and adapter calls are not duplicated.

## What's new

- `crates/sandbawks-controller/src/reconcile.rs` (new) —
  - `ANN_DESIRED_PHASE` / `ANN_POD_READY` constants (Plan 04 replaces
    the annotations with typed spec fields).
  - `ReconcileContext { store: Arc<dyn SandboxStore>, adapter:
    Arc<dyn RuntimeAdapter> }` — the dependency bundle kube-rs's
    `Controller` clones across requeues.
  - `ControllerError` — `thiserror` enum with `Store(#[from] StoreError)`
    and `Invariant(&'static str)`.
  - `reconcile(name, ctx)` — read sandbox → derive trigger → run
    transition → patch status to next phase → dispatch action → patch
    post-dispatch status. Adapter failures fold into `Failed` via a
    second `transition()` + status patch with a `SandboxCondition`.
- `crates/sandbawks-controller/tests/reconcile.rs` (new) — six async
  tests covering every scenario in the issue acceptance criteria.
- `crates/sandbawks-controller/Cargo.toml` — `chrono = "0.4"` added to
  `[dev-dependencies]` for the deletion-timestamp test.
- `crates/sandbawks-controller/src/bin/sandbawks-controller.rs` (stub)
  — minimal placeholder so the `[[bin]]` declaration resolves and the
  workspace gates compile. T9 replaces this stub with the real
  `tokio::main` boot.

## Validation

- `cargo test -p sandbawks-controller --test reconcile` — 6 tests pass.
- `just fmt` — clean, no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` — silent.
- `just test` — 52 tests pass across the workspace; reconcile,
  state_machine_unit, state_machine_properties, fake_adapter, smoke,
  and store::tests are all green.

## What's deliberately not here

- Real pod creation (Plan 03).
- Real adapter dispatch (`GvisorAdapter` is Plan 05; we still wire
  `FakeRuntimeAdapter`).
- Finalizer logic (Plan 05).
- Pod-readiness watcher — tests still set the
  `sandbawks.io/pod-ready` annotation directly (Plan 03 wires the
  real watcher).
- The Plan 02 T9 binary entrypoint — only a stub `fn main()` lands
  here so `cargo test` resolves targets.
