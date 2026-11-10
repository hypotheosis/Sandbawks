# Issue #487 — Scheduler-placed restore: node-side staging to `target_path` + atomic `.staged` marker

**Date:** 2026-06-12T21:11:09Z
**Branch:** `sandbawks/sandbawks-487`
**Parent epic:** #484 (scheduler-placed restore + Resuming recovery)
**Depends on:** #485 (`SnapshotJobSpec.target_path`), #486 (stage-gate init binary)

## Context

Scheduler-placed restore (epic #484) inverts the old resume ordering: the
controller creates the **restore Pod first** — unpinned, with its
`io.sandbawks/restore-image-path` annotation already stamped — and only
*then* dispatches the `Stage` job once the scheduler has bound the Pod to a
node. For that to work, the controller and node must agree on a
**deterministic** staging path computable from the sandbox name alone
(`<stagingRoot>/<sandbox-id>/restore`), and the credential-less stage-gate
init container (#486) needs a signal that the bundle is fully on disk.

Before this change the node's `Stage` path always allocated a *timestamped*
directory and reported the chosen path back via `status.localPath`. That
report-back is incompatible with stamping the Pod's annotation up front, and
there was no completion marker for the gate to poll. The `target_path` spec
field (#485) existed but was unused.

## What changed

### `sandbawks-types`
- Added `pub const STAGED_MARKER_FILENAME: &str = ".staged"`
  (`snapshot_job.rs`), re-exported from the crate root. The marker filename
  is the node↔controller contract for the stage-gate, so it lives in one
  place rather than as duplicated string literals.
- `RuntimeAdapter::stage_snapshot` gained a `target_path: Option<&str>`
  parameter, with the contract documented on the trait: `Some` ⇒ stage at
  the pinned path and drop the marker; `None` ⇒ legacy self-chosen path,
  no marker.

### `sandbawks-node`
- `GvisorAdapter::stage_snapshot` (`gvisor.rs`) now branches on
  `target_path`:
  - **`Some(path)`** — clears any prior attempt's bytes
    (`remove_dir_all`, tolerating `NotFound`), lays the bundle out at the
    pinned path, and writes the `.staged` marker **last**. The controller
    guarantees single-flight per sandbox, so there is no concurrent writer.
  - **`None`** — unchanged timestamped behavior.
- New free function `write_staged_marker` writes the marker atomically:
  bytes go to a sibling `.staged.tmp` and are `rename`d into place
  (intra-directory `rename(2)` is atomic on POSIX), so a poller can never
  observe a half-written marker. It is written only after every part is on
  disk, so its presence proves bundle completeness. A marker-write failure
  surfaces as a stage failure, so the gate keeps blocking rather than
  releasing onto an unmarked bundle.
- Threaded `job.spec.target_path.as_deref()` through the `Stage` reconcile
  arm (`reconcile.rs`).

## Tests

- `stage_snapshot_honors_target_path_and_drops_staged_marker` — stages to a
  pinned path *outside* `staging_root`, asserts every part landed there, a
  pre-seeded stale file was cleared, the `.staged` marker exists with a
  non-empty body, and no `.staged.tmp` leaked.
- `stage_snapshot_legacy_path_writes_no_staged_marker` — the `None` path
  drops no marker.
- `stage_threads_target_path_to_adapter` (reconcile) — `spec.target_path`
  reaches the adapter verbatim and the pinned path is recorded on
  `Complete`.
- Existing `stage_dispatches_to_adapter_and_writes_local_path` now also
  asserts the legacy path forwards `None`.
- Recording `FakeRuntimeAdapter` captures the per-call `target_path`.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — all pass (new node-side tests included)

## Review follow-up (PR #506)

Two hardening points from the PR review, both in `GvisorAdapter::stage_snapshot`:

1. **Containment guard on the CRD-supplied `target_path`.** Before
   `remove_dir_all`-ing (and writing into) the path, the new
   `ensure_under_staging_root` helper rejects anything that isn't a
   descendant of `staging_root`: it refuses any `..` component, then
   canonicalizes the deepest existing ancestor (resolving symlinks) and
   requires it under the canonicalized root. A malformed or hostile
   `SnapshotJob.spec.target_path` now fails closed
   (`RuntimeError::SnapshotDownload`) instead of recursively deleting an
   arbitrary node-mounted tree. New test
   `stage_snapshot_rejects_target_outside_root` proves a path outside the
   root is refused and an unrelated sentinel file survives.
2. **Best-effort cleanup on marker-write failure.** When
   `write_staged_marker` errors, the staging dir is now removed before the
   error propagates, mirroring the `fetch_verify_and_lay_out` failure arm —
   keeping the fail-closed-and-clean pattern consistent.

The existing `stage_snapshot_honors_target_path_and_drops_staged_marker`
test was updated to pin the path under `staging_root`
(`<staging_root>/<sandbox-id>/restore`, the real-world invariant the
controller derives) so it satisfies the new guard.

## Out of scope

- Cross-node orphaned-staging-dir GC — tracked in #483.
- Wiring the stage-gate init container's `--marker` arg into the restore Pod
  spec — controller-side, a later epic-#484 phase. This change only
  establishes the node-side `<target_path>/.staged` contract.
