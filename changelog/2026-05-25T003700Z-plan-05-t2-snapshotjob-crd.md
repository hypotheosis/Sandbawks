# Plan 05 T2 — `SnapshotJob` CRD + `Sandbox.status.snapshotJobRef`

Forgejo issue: #138 (part of
Plan 05 epic #136).
Plan: `docs/superpowers/plans/2026-05-24-sandbawks-05-gvisor-node.md` § Task 2.

## Summary

Introduces a new `SnapshotJob` CRD that the controller will create to
delegate per-node checkpoint/restore work to the upcoming
`sandbawks-node` DaemonSet (T9), plus a `snapshotJobRef` pointer on
`Sandbox.status` so a single in-flight job can be observed and joined
across controller restarts. No reconciler wiring yet — that's T4 on the
controller side and T9 on the node side.

## Changes

### `crates/sandbawks-types/src/snapshot_job.rs` (new)
- `SnapshotJobMode { Pause, Resume }` — direction of the operation.
- `SnapshotJobPhase { Pending, Checkpointing, Uploading, Restoring,
  Complete, Failed }` — observable lifecycle. `Default = Pending` so
  freshly-created CRs without a written status read as `Pending`.
- `SnapshotJobSpec { sandboxName, nodeName, mode, containerId,
  snapshotRef }` — `snapshotRef` is `Option<String>` because it only
  applies to `Resume`.
- `SnapshotJobStatus { phase, manifest, message, startedAt,
  completedAt }` — `manifest` reuses the existing v1 `SnapshotManifest`
  from `runtime.rs`; T3 upgrades it to v2 with `parts`.
- `#[kube(...)]` macro derives the CRD with group `sandbawks.io`,
  version `v1alpha1`, plural `snapshotjobs`, shortname `sbxsj`,
  namespaced, status subresource enabled. Printcolumns: `State`,
  `Mode`, `Node`, `Sandbox`, `Age` (the issue spec adds `Mode` on top
  of the plan doc's four — surfaces direction in `kubectl get sbxsj`).

### `crates/sandbawks-types/src/lib.rs`
- Added `pub mod snapshot_job;` between `sandbox_template` and
  `team_api_key` (alphabetical).
- Re-exported `SnapshotJob`, `SnapshotJobMode`, `SnapshotJobPhase`,
  `SnapshotJobSpec`, `SnapshotJobStatus` so downstream crates can
  `use sandbawks_types::SnapshotJob` without spelling the submodule.

### `crates/sandbawks-types/src/sandbox.rs`
- Added `pub snapshot_job_ref: Option<String>` to `SandboxStatus`
  (camelCase `snapshotJobRef` via the struct's `rename_all`).
  Placed between `snapshot_ref` and `last_transition_time` to keep
  snapshot-related fields adjacent. Skipped on serialize when `None`
  so existing CRs without the field don't bloat in storage.

### `crates/sandbawks-types/src/bin/crdgen.rs`
- Imported `SnapshotJob`.
- Appended `write_crd(out_dir, "snapshotjobs.sandbawks.io.yaml",
  &SnapshotJob::crd())?` after the `teamapikeys` write so the existing
  four-file ordering is preserved.

### `crates/sandbawks-types/tests/crdgen.rs`
- Bumped `EXPECTED_FILES` from `[&str; 4]` to `[&str; 5]` and added
  `"snapshotjobs.sandbawks.io.yaml"`.
- Renamed `crdgen_emits_four_files` → `crdgen_emits_all_files` so the
  test name doesn't lie when T3+ adds more CRDs.

### `deploy/crds/snapshotjobs.sandbawks.io.yaml` (new, generated)
- Regenerated artifact from `cargo run -p sandbawks-types --bin crdgen
  -- deploy/crds`. The CI drift-check job (Plan 01 T12) will fail if
  this drifts from the source `snapshot_job.rs`.

### `deploy/crds/sandboxes.sandbawks.io.yaml` (regenerated)
- Picks up the new `snapshotJobRef` field under `status` — a single
  3-line block, sorted alphabetically between `snapshotRef` and the
  pre-existing fields.

## Verification

- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` →
  5 files emitted, idempotent on second run.
- `cargo test -p sandbawks-types --all-features` → green.
- `cargo clippy -p sandbawks-types -- -D warnings` → clean.
- `just fmt` → no changes.

## Out of scope (covered by later tasks)

- `SnapshotSink`/`SnapshotSource` traits + `SnapshotManifest` v2 → T3.
- Controller `dispatch()` writing `SnapshotJob` CRs and joining on the
  terminal status → T4.
- `sandbawks-node` reconciling `SnapshotJob`s → T9.
- RBAC + Helm chart wiring for the new CRD → T12.
