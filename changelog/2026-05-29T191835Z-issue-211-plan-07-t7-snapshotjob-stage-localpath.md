# Plan 07 T7 — SnapshotJobMode::Stage + status.localPath (types)

- **Issue:** #211 (epic #204, Plan 07 Task 7)
- **Date:** 2026-05-29T19:18:35Z
- **Area:** `crates/sandbawks-types/` — `SnapshotJob` CRD types + `RuntimeAdapter` trait

## Summary

Adds the type-level surface for the shim-driven *stage* path: a new
`SnapshotJobMode::Stage` variant, a `SnapshotJobStatus.local_path`
status field, and a `RuntimeAdapter::stage_snapshot` trait method.
`Stage` means "download a snapshot and materialize its parts onto local
disk *without* restoring a container", leaving the resulting path in
`status.localPath` for a downstream consumer — the
`containerd-shim-sandbawks-v1` performing a start-time `runsc restore`
(Plan 07 T2–T6).

This is a types-only task. The controller and node crates are touched
only to the extent needed to keep the workspace compiling (the new
enum variant and status field force every exhaustive `match mode` and
every `SnapshotJobStatus { .. }` literal to be updated). Real
controller-side dispatch and node-side handling of `Stage` jobs land in
later tasks; here they are stubbed fail-closed.

## Changes

- `crates/sandbawks-types/src/snapshot_job.rs`
  - `SnapshotJobMode::Stage` — PascalCase JSON tag `"Stage"`. Deliberately
    **no** `#[serde(rename_all)]`: deployed `Pause`/`Resume` resources
    encode those exact tags and renaming would break round-tripping.
  - `SnapshotJobStatus.local_path: Option<String>`
    (`#[serde(default, skip_serializing_if = "Option::is_none")]`;
    serializes as `localPath` via the struct's existing camelCase rename).
  - New `#[cfg(test)] mod tests`: PascalCase tag serialization for all
    three modes, `Stage` round-trip, and `localPath` omit-when-`None` /
    camelCase round-trip.
- `crates/sandbawks-types/src/runtime.rs`
  - `RuntimeError::Unsupported(Cow<'static, str>)` — new variant naming
    the unimplemented method. `Cow<'static, str>` keeps the common
    static-name case zero-alloc (`Borrowed`) while letting a future
    caller pass an owned runtime-formatted detail without a new variant.
    The default impl returns
    `RuntimeError::Unsupported(Cow::Borrowed("stage_snapshot"))`.
  - `RuntimeAdapter::stage_snapshot(&self, sandbox_id, manifest, source)
    -> Result<String, RuntimeError>` with a default impl returning
    `Unsupported("stage_snapshot")`.
- `crates/sandbawks-types/tests/runtime_trait.rs`
  - Test that the default `stage_snapshot` (inherited by `NullAdapter`)
    rejects with `RuntimeError::Unsupported("stage_snapshot")`.
- `deploy/crds/snapshotjobs.sandbawks.io.yaml` — regenerated: `Stage`
  added to the `mode` enum, `localPath` added to the status schema.
- `crates/sandbawks-controller/src/reconcile.rs` (compile-preservation)
  - `Stage` grouped with `Resume` in `observe_snapshot_job`'s trigger
    and ref-folding matches (exhaustiveness only — the controller never
    dispatches `Stage` today) and given a `"stage"` slug in
    `build_snapshot_job`.
- `crates/sandbawks-node/src/reconcile.rs` (compile-preservation)
  - New `NodeReconcileError::UnsupportedMode(String)`.
  - `Stage` arm in the dispatch match: fail-closed — patch a `Failed`
    phase with an observable message, then propagate the error. The node
    `SnapshotJob` worker does not implement staging; that path is
    shim-driven. Mirrors the existing `MissingSnapshotRef` shape.
  - `local_path: None` added to all status literals.
- Test literals updated with `local_path: None` (node tests spelled out
  every field; controller tests already used `..Default::default()`).

## Verification

- `cargo test -p sandbawks-types --all-features` → green (16 unit + all
  integration/doc tests pass, including the four new tests).
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` → regenerates
  5 files; only `snapshotjobs.sandbawks.io.yaml` changes, and a second run
  is idempotent (no further diff). The diff is exactly the new `Stage`
  enum value + `localPath` field.
- Workspace gate: `just fmt`, `cargo clippy --all-targets -- -D warnings`,
  `just test` all pass (exit 0, no failed suites).

## Notes

- `Stage` handling in the controller/node is intentionally a stub in this
  PR. A node-side handler that actually calls `stage_snapshot` and records
  `status.localPath` is a separate downstream task; until it lands, a
  `Stage` job reaching the node worker fails loudly rather than being
  silently mishandled.
