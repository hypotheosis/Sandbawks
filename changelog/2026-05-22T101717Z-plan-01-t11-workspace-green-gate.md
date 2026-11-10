# 2026-05-22 — Plan 01 Task 11: Workspace-wide green build gate

Closes #12.

## What landed

The Plan 01 verification gate runs the full local toolchain to catch
cross-crate issues before opening the CI PR. Running it on `main`
surfaced one race condition in the `crdgen` integration tests that the
per-crate gates had not exercised:

`crates/sandbawks-types/tests/crdgen.rs` previously namespaced its
hermetic temp directory by `std::process::id()`. Both `#[test]`
functions in the file share the same test binary and therefore the
same PID, so they raced on the same `target/tmp/crdgen-<pid>/`
directory. Under parallel execution, `crdgen_emits_four_files` could
read a YAML file mid-write by `crdgen_is_idempotent` and observe an
empty body.

Fix: `hermetic_tempdir` now takes a `label: &str` so each test gets a
unique path (`crdgen-<pid>-<label>/`). The comment on the helper was
corrected from "Per-process" to "Per-test" to match the new contract.

## Validation

Ran the four commands the issue specifies, in order, from `main`:

- `cargo test --workspace` — 20 tests pass (0 crdgen lib + 0 crdgen bin
  + 2 crdgen integration + 6 resources + 4 sandbox + 2 sandbox_template
  + 2 smoke + 2 team_api_key + 2 warm_pool + 0 doc tests).
- `cargo clippy --workspace --all-targets -- -D warnings` — silent.
- `cargo fmt --all -- --check` — silent.
- `cargo doc --workspace --no-deps` — no warnings, two HTML files
  emitted under `target/doc/sandbawks_types/`.

The `~16 tests` figure in the issue body was an estimate; the real
count is 20 because Tasks T6–T9 each added a couple of tests beyond
the original budget.

## What's next

Task 12 — the CI drift-check job that runs `crdgen` and diffs against
the committed `deploy/crds/` tree (issue #13).
