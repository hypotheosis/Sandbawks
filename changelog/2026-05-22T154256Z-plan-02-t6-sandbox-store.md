# Plan 02 T6 — `SandboxStore` trait + Kube/Fake impls

**Date:** 2026-05-22T15:42:56Z
**Branch:** `feat-issue-34-sandbox-store-trait`
**Issue:** #34
**Plan:** `docs/superpowers/plans/2026-05-22-sandbawks-02-controller-skeleton.md` § Task 6

## What changed

Adds `crates/sandbawks-controller/src/store.rs` — the seam that lets the reconcile loop landing in T7 (#35) read Sandboxes and patch their status without binding to a real `kube::Api<Sandbox>` in unit tests.

### Files created

- `crates/sandbawks-controller/src/store.rs` — five public items + an inline `#[cfg(test)]` mod:
  - `StoreError` — `thiserror` enum with `NotFound(String)`, `Kube(#[from] kube::Error)`, `Json(#[from] serde_json::Error)`.
  - `SandboxStore` — `#[async_trait] pub trait : Send + Sync` with `get(name)` and `patch_status(name, new_status)`.
  - `KubeSandboxStore` — production impl wrapping `kube::Api::namespaced(client, namespace)`. `patch_status` uses server-side apply via `PatchParams::apply("sandbawks-controller")` with `Patch::Merge(json!({"status": new_status}))` against the `status` subresource.
  - `FakeSandboxStore` — `Default + Clone` wrapper around `Arc<Mutex<HashMap<String, Sandbox>>>`. Helpers: `insert(sandbox)`, `peek(name) -> Option<Sandbox>`.
  - `#[cfg(test)] mod tests` — three async unit tests against `FakeSandboxStore` (`fake_get_returns_inserted_sandbox`, `fake_get_missing_returns_not_found`, `fake_patch_status_updates_in_place`).

### Design notes

- **Server-side apply on `patch_status`.** Using `Patch::Merge` against the `status` subresource with a stable field manager (`sandbawks-controller`) means re-applying the same patch is a no-op and concurrent writes from a future split-brain controller surface as `kube::Error::Api` 409s instead of silently clobbering each other.
- **Namespacing inside the impl.** `SandboxStore::get` and `patch_status` take only `name` — namespace selection is fixed at `KubeSandboxStore::new(client, namespace)` time. This keeps the reconcile loop in T7 ignorant of namespaces.
- **`peek` on the fake is non-async on purpose.** Tests assert on post-reconcile state without an `.await`; the trait method `get` (which is `async`) is only used by the reconcile loop itself.
- **Test helper uses struct-update syntax.** `SandboxStatus { phase: ..., ..Default::default() }` instead of `let mut s = default(); s.phase = ...;` — pre-empts `clippy::field_reassign_with_default` once T7 unblocks the lib build and `cargo clippy --workspace --all-targets -- -D warnings` starts running over this file.

## Deliberately broken (carried forward from T1)

Per the plan and the established pattern in PRs #41 / #43 / #44 / #45, `crates/sandbawks-controller/src/lib.rs` still declares `pub mod reconcile;` (T7) and a `[[bin]]` whose source file lands in T9. Workspace-level gates (`just fmt`, `cargo clippy --workspace --all-targets`, `just test`) therefore **fail by design** until those siblings land — the only blocker is a single `error[E0583]: file not found for module 'reconcile'` diagnostic on `lib.rs:20`. The `store` module declaration on `lib.rs:22` now resolves.

The store module itself is verified compile- and lint-clean by stubbing `reconcile.rs` and `bin/sandbawks-controller.rs` *locally* (not committed), running `cargo test -p sandbawks-controller --lib store` (3/3 pass) and `cargo clippy -p sandbawks-controller --lib --tests -- -D warnings` (0 warnings), then deleting the stubs before commit.

## Validation

| Step | Status | Notes |
| --- | --- | --- |
| `rustfmt --check --edition 2021 crates/sandbawks-controller/src/store.rs` | pass | exit 0 |
| `cargo test -p sandbawks-controller --lib store` (with local stubs) | pass | 3/3 tests green |
| `cargo clippy -p sandbawks-controller --lib --tests -- -D warnings` (with local stubs) | pass | 0 warnings |
| `just fmt` / `cargo clippy --workspace --all-targets -- -D warnings` / `just test` | fail by design | blocked on T7 (`reconcile`) / T9 (`bin/`); matches PRs #41 / #43 / #44 / #45 |

## Why this matters

The reconcile loop in T7 has two invariants from spec §10:

1. **Status is durable before side effects.** Patch `status.phase = Pausing` *before* calling `adapter.pause()`. If the controller crashes between the patch and the adapter call, the next reconcile sees `status.phase = Pausing` and resumes.
2. **Idempotent on retry.** Re-reading the same Sandbox and running reconcile again must not duplicate side effects.

Both invariants are testable only if we can substitute the kube API server with an in-memory fake under the reconcile loop. `SandboxStore` is that seam; this PR ships it.
