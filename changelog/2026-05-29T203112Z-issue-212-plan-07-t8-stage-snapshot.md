# Plan 07 T8 — `GvisorAdapter::stage_snapshot` (S3 download + verify + `*.img` unpack)

- **Issue:** #212 (epic #204, Plan 07 Task 8)
- **Date:** 2026-05-29T20:31:12Z
- **Area:** `crates/sandbawks-node/` — `GvisorAdapter` + `SnapshotJob` node reconciler

## Summary

Implements the shim-driven *stage* path: `GvisorAdapter::stage_snapshot`
downloads a snapshot bundle from the `SnapshotSource`, verifies every
part's sha256, and materializes the bundle onto local disk **without**
restoring a container. The returned directory is the `ImagePath` the
`containerd-shim-sandbawks-v1` hands to gVisor's `CreateWithFSRestore`
(Plan 07 T3/T4). The node `SnapshotJob` reconciler now dispatches
`SnapshotJobMode::Stage` to this method and records the staged path in
`status.localPath`.

Per issue #195, `memory.zst` carries gVisor's `*.img` checkpoint files
(`checkpoint.img` / `pages.img` / `pages_meta.img`), so staging unpacks
those into the dir exactly as the existing `resume()` fallback does. The
shared unpack is factored into one private helper so the on-disk layout
has a single definition (DRY) and the two read-side paths can't drift.

## Changes

- `crates/sandbawks-node/src/gvisor.rs`
  - New `DEFAULT_STAGING_ROOT = "/var/lib/sandbawks/staging"` + a
    `staging_root` field on `GvisorAdapter` with a `with_staging_root`
    builder (tests point it at a `tempfile::tempdir()`). Distinct from
    `work_root`: the staging dir is *source-only* input the shim reads
    off the host FS directly, so it gets its own hostPath in deploy.
  - New private `RootfsDelta<'_>` enum (`ApplyToContainer { container_id }`
    vs `WriteToStagingDir`) — the one piece of the layout that differs
    between `resume` (apply the delta to a live containerd snapshot) and
    `stage_snapshot` (no container exists yet, so keep the delta as a
    file in the staging dir for the shim to consume).
  - New private `GvisorAdapter::fetch_verify_and_lay_out` helper: fetches
    the authoritative manifest from `source`, cross-checks the aggregate
    sha256 (issue #171), then streams + verifies each part (sha256 +
    size) and lays it out — `memory.zst` un-zstd+un-tar into the dir,
    `envd-state.json` written verbatim, `rootfs-delta.tar.zst` routed per
    `RootfsDelta`. Shared by `resume` and `stage_snapshot`.
  - `resume()` refactored onto the shared helper; behavior is otherwise
    unchanged. Added a `tracing::warn!` noting it is the
    shim-not-installed fallback (Plan 07 Decision §6).
  - `stage_snapshot()` implemented on `impl RuntimeAdapter for
    GvisorAdapter`: allocates `<staging_root>/<sandbox-id>/<clock-segment>/`,
    calls the shared helper with `WriteToStagingDir`, returns the dir
    path (rejecting a non-UTF-8 path rather than lossily round-tripping).
- `crates/sandbawks-node/src/reconcile.rs`
  - `SnapshotJobMode::Stage` now dispatches to `adapter.stage_snapshot`:
    requires `spec.snapshotRef` (mirrors `Resume`; absence →
    `MissingSnapshotRef`), patches an in-flight `Restoring` phase with
    `started_at` (arming the stuck-job idempotency floor), then patches
    terminal `Complete` + `status.localPath` on success or `Failed` +
    `message` on error.
  - Removed the now-unused `NodeReconcileError::UnsupportedMode` variant
    (the `Stage` arm previously failed closed with it). `MissingSnapshotRef`
    docs/message generalized to cover both `Resume` and `Stage`.
  - Module doc updated to list `stage_snapshot` as a third dispatch
    target.
- `crates/sandbawks-node/tests/gvisor.rs`
  - `stage_snapshot_materializes_bundle_and_returns_path`: seeds a
    manifest + three parts (`memory.zst` carrying all three `*.img`
    files, `rootfs-delta.tar.zst`, `envd-state.json`), runs
    `stage_snapshot`, asserts the returned dir exists under
    `<staging_root>/<sandbox-id>/`, all three `.img` files + envd-state
    unpacked verbatim, the rootfs delta staged as a file (and *not*
    applied to a container), and no pod was resolved.
  - `stage_snapshot_bails_on_part_sha256_mismatch`: tampered part sha →
    `SnapshotDownload` before any file is laid out.
- `crates/sandbawks-node/tests/reconcile.rs`
  - `FakeRuntimeAdapter` extended to record `stage_calls` + a
    configurable `stage_outcome`, implementing `stage_snapshot`.
  - Three new tests: happy-path dispatch (asserts `Restoring` →
    `Complete` + `localPath`), missing-`snapshotRef` fail-loud, and
    adapter-failure → `Failed` + `message`.

## Verification

- `cargo test -p sandbawks-node --all-features` → green (gvisor: 10
  passed incl. 2 new stage tests; reconcile: 13 passed incl. 3 new stage
  tests; s3/smoke unchanged).
- Workspace gate: `just fmt` (fmt --check exit 0),
  `cargo clippy --all-targets --all-features -- -D warnings` (clean),
  `just test` (all suites green).

## Notes

- **Intentional error-class change on the resume path.** Pre-T8, an
  `envd-state.json` write failure during resume surfaced as
  `RuntimeError::RestoreFailed`. Now that `resume` and `stage_snapshot`
  share `fetch_verify_and_lay_out`, that write failure is categorized as
  `RuntimeError::SnapshotDownload` for both paths — it is a download /
  lay-out failure, not a `runsc restore` failure. Log analysis or
  alerting that buckets on the error variant will see resume-time
  filesystem errors move from `RestoreFailed` to `SnapshotDownload`.
- `stage_snapshot` best-effort `remove_dir_all`s the partial staging dir
  on the error path: a failed stage (or a stuck-job retry, each landing
  in a fresh clock-segment subdir) would otherwise leak orphan
  directories. `status.localPath` is set on `Complete` alone, so
  removing the partial dir is safe.
- The staging dir keeps `rootfs-delta.tar.zst` as a file rather than
  applying it (the resume path's containerd Apply needs a live
  container, which doesn't exist at stage time). For gVisor's default
  `--overlay2=root:self` the delta is typically empty anyway — rootfs
  writes live in `pages.img` inside `memory.zst`.
- Production wiring of the `staging_root` env override and the DaemonSet
  hostPath is out of scope here; the adapter defaults to the spec'd
  `/var/lib/sandbawks/staging`. Controller-side `Stage` dispatch is Plan
  07 T9.
