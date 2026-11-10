# Plan 01 T2 — `sandbawks-types` crate skeleton

**Date:** 2026-05-22T01:06:34Z
**Branch:** `feat-issue-3-types-crate-skeleton`
**Issue:** #3
**Plan:** `docs/superpowers/plans/2026-05-21-sandbawks-01-types-and-crds.md` § Task 2

## What changed

Added the `crates/sandbawks-types/` workspace member with module declarations only — the modules themselves (`error`, `resources`, `sandbox`, `sandbox_template`, `team_api_key`, `warm_pool`) are filled in by Tasks 3–8. A smoke test under `tests/smoke.rs` exercises the `CRATE_NAME` public constant.

### Files created

- `crates/sandbawks-types/Cargo.toml` — workspace-pinned package manifest pulling shared deps (`kube`, `k8s-openapi`, `schemars`, `serde`, `serde_json`, `serde_yaml`, `thiserror`). Includes the `crdgen` bin target wired up by Task 9.
- `crates/sandbawks-types/src/lib.rs` — module map plus the `CRATE_NAME` constant and the public re-exports the rest of the workspace will consume.
- `crates/sandbawks-types/tests/smoke.rs` — single-assertion link test, expected to start passing once Task 3 lands `error.rs`.

### Side-effect change

- `justfile` — added at the workspace root. The `forgejo-issue-to-pr` workflow expects `just fmt` / `just test` to exist; previously the repo had none. Recipes are thin wrappers over `cargo`.

## Deliberately broken

Per the plan, compilation is intentionally broken after this task — `lib.rs` references modules that don't exist yet, and the `crdgen` bin target points at a file Task 9 will create. This is the explicit Task 2 acceptance criterion ("Compilation is deliberately broken at this point — modules referenced from `lib.rs` don't exist yet. This is fixed incrementally by T3–T8.").

`cargo metadata --format-version 1 --no-deps` does succeed and lists `sandbawks-types` as a workspace member, which is the actually-testable acceptance criterion.

## Validation

| Step | Status | Notes |
| --- | --- | --- |
| `cargo fmt --all -- --check` | pass | applies to the new files |
| `cargo metadata --format-version 1 --no-deps` | pass | workspace member resolved |
| `cargo clippy` | **fail (expected)** | unresolved modules; landed by T3–T8 |
| `cargo test` | **fail (expected)** | same reason |

## Next steps

Tasks 3–8 from Plan 01 will incrementally land the six modules, after which the smoke test plus the additional per-module tests should all go green. Task 9 wires up the `crdgen` binary that this crate's `Cargo.toml` already declares.
