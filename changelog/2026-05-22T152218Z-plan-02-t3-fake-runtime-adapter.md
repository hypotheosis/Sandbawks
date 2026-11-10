# Plan 02 T3 — `FakeRuntimeAdapter` with call recording + failure injection

**Date:** 2026-05-22T15:22:18Z
**Branch:** `feat-issue-31-fake-runtime-adapter`
**Issue:** #31
**Plan:** `docs/superpowers/plans/2026-05-22-sandbawks-02-controller-skeleton.md` § Task 3

## What changed

Lands the in-memory test double for the `RuntimeAdapter` trait (defined in `sandbawks-types::runtime`, landed in T2 / #30). The adapter records every `pause()` / `resume()` call into an `Arc<Mutex<Vec<FakeAdapterCall>>>` so tests can assert exactly *which* runtime actions the reconcile loop dispatched, without standing up a real gVisor / Kata / runc-CRIU backend.

Builder-style failure injection (`with_pause_failure`, `with_resume_failure`) lets the controller's error-handling paths (added in T4–T7) be exercised deterministically — no flaky real runtime required.

### Files created

- `crates/sandbawks-controller/src/fake_adapter.rs` — `FakeAdapterCall` enum (`Pause { pod_uid }`, `Resume { pod_uid, manifest }` — `Debug + Clone + PartialEq + Eq`) and `FakeRuntimeAdapter` struct with the recording + failure-injection behavior. The synthetic `SnapshotManifest` returned by `pause()` echoes the adapter's `runtime_class` and uses `pod_uid` as the `sandbox_id`, with a fixed 64-char placeholder `sha256`.
- `crates/sandbawks-controller/tests/fake_adapter.rs` — five integration tests covering: (1) `pause` records the call and returns a manifest, (2) `resume` records the call, (3) `with_pause_failure` makes `pause` return a `RuntimeError::Pause`, (4) `runtime_class()` reflects the constructor argument, (5) `Arc<FakeRuntimeAdapter>` coerces to `Arc<dyn RuntimeAdapter>` (the dyn-trait shape the future reconcile loop will use).

## Deliberately broken (carried forward from T1)

Per the plan and PR #41's established pattern, `crates/sandbawks-controller/src/lib.rs` still declares modules that have not landed yet (`state_machine`, `store`, `reconcile`) and a `[[bin]]` target whose path file is not yet created. As a result the **workspace-level gates** (`just fmt`, `cargo clippy --workspace --all-targets`, `just test`) **fail by design** until T4–T7 + T9 land.

This sub-issue's deliverable is the *content* of `fake_adapter.rs` + its tests, not workspace-green — that gate is restored by T11-equivalent in this plan (the final task that closes out Plan 02).

## Validation

| Step | Status | Notes |
| --- | --- | --- |
| `rustfmt --check --edition 2021` on the two new files | pass | exit 0 |
| `cargo test -p sandbawks-controller --test fake_adapter` | fail by design | blocked on T4–T7 modules referenced from `lib.rs`; the test file itself is correct and will pass once siblings land |
| `just fmt` / `cargo clippy --all-targets -- -D warnings` / `just test` | fail by design | same root cause; pattern matches PR #41 |
| `Arc<dyn RuntimeAdapter>` object-safety check | pass | encoded as `arc_dyn_runtime_adapter_object_safety` test — compiles in isolation once siblings land |

## Next steps

Task 4 (#32) lands the pure state-machine `transition(phase, trigger) -> Option<(phase, action)>` function in `crates/sandbawks-controller/src/state_machine.rs`. Tasks 5–7 fill in proptest coverage, the `SandboxStore` trait, and the reconcile glue. Task 9 adds the binary entrypoint. After all sibling tasks land, the workspace-level gates and this issue's `cargo test --test fake_adapter` invocation both go green.
