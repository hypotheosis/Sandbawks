# Plan 02 T1 — `sandbawks-controller` crate scaffold

**Date:** 2026-05-22T14:24:34Z
**Branch:** `feat-issue-29-controller-scaffold`
**Issue:** #29
**Plan:** `docs/superpowers/plans/2026-05-22-sandbawks-02-controller-skeleton.md` § Task 1

## What changed

Added the `crates/sandbawks-controller/` workspace member with module declarations only — the modules themselves (`state_machine`, `fake_adapter`, `store`, `reconcile`) and the `sandbawks-controller` bin target land in Plan 02 Tasks 2–9. A smoke test under `tests/smoke.rs` exercises the `CRATE_NAME` public constant.

The workspace root `Cargo.toml` picks up three new shared dependencies — `async-trait`, `tracing-subscriber`, and `futures` — that the later tasks (and the future `sandbawks-node` crate) will share.

### Files created

- `crates/sandbawks-controller/Cargo.toml` — workspace-pinned manifest. Depends on `sandbawks-types` via path, plus the kube / serde / tracing / tokio / async-trait / futures workspace deps. Declares the `sandbawks-controller` bin target (path filled by Task 9) and a `proptest` dev-dependency (used by Task 5).
- `crates/sandbawks-controller/src/lib.rs` — module map (`fake_adapter`, `reconcile`, `state_machine`, `store`), the `CRATE_NAME` constant, and the public re-exports downstream callers will consume.
- `crates/sandbawks-controller/tests/smoke.rs` — single-assertion link test against `CRATE_NAME`.

### Files modified

- `Cargo.toml` — adds `"crates/sandbawks-controller"` to `members`; adds `async-trait = "0.1.83"`, `tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }`, and `futures = "0.3"` to `[workspace.dependencies]`. The pre-existing comment on `tokio` ("kept for later crates; types crate doesn't use these") is dropped because the new crate actually uses tokio now.

## Deliberately broken

Per the plan and the issue's acceptance criteria, **compilation is intentionally broken at this commit**:

- `lib.rs` declares modules (`fake_adapter`, `reconcile`, `state_machine`, `store`) that land in T3–T7.
- The `[[bin]]` target path `src/bin/sandbawks-controller.rs` is filled by T9.

This mirrors the Plan 01 T2 pattern (`feat(types): skeleton sandbawks-types crate`, PR #16).

The acceptance criterion that is verifiable now: `cargo metadata --format-version 1 --no-deps` lists `sandbawks-controller` as a workspace member.

## Validation

| Step | Status | Notes |
| --- | --- | --- |
| `cargo metadata --format-version 1 --no-deps` | pass | emits `["sandbawks-types", "sandbawks-controller"]` |
| `rustfmt --check` (new files only, `skip_children=true` on `lib.rs`) | pass | exit 0 |
| `just fmt` / `cargo clippy` / `just test` | **fail by design** | unresolved modules + missing bin target; restored green by T2–T9 |

## Next steps

Task 2 lands the `RuntimeAdapter` trait in `sandbawks-types::runtime` (the controller depends on it via the types crate). Tasks 3–7 fill in the controller modules incrementally. Task 8 wires the kube-rs `Controller`. Task 9 adds the binary entrypoint. After T9 the smoke test plus per-task tests should all go green.
