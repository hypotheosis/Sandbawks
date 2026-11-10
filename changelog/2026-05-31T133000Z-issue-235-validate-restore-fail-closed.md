# Plan 07 hardening: validate a restore before tearing down the source (fail-closed teardown)

- **Issue:** #235 (one of the deferred fail-closed hardening items from #232's
  "Snapshot integrity / fail-closed" section; PR #234 shipped the
  checkpoint-postconditions floor, this is the end-to-end gate). Part of epic
  #204 (Plan 07). PR #239.
- **Date:** 2026-05-31T19:30:00Z
- **Status:** implemented and fully unit/integration-tested; validated
  end-to-end in the gVisor kind pause/resume smoke (both the kind-smoke and the
  resource-tight bench cluster).

## Problem

Pause checkpoints + uploads, then the controller advances `Pausing → Paused`
and the source Pod becomes reclaimable. If the uploaded snapshot turns out to
be unrestorable, that only surfaces at *resume* time — after the source is
gone — i.e. unrecoverable data loss. #234's checkpoint postconditions are the
cheap structural floor (the three `*.img` files exist and are non-empty while
the source is alive); they do not prove the *uploaded* bundle round-trips.

## Fix — gate the pause→reclaim edge on a *validated* snapshot

The Pause `SnapshotJob` now only reports `Complete` after the node has proven
the just-uploaded snapshot restorable. The controller reaches `Paused` (and
thus lets the source be reclaimed) only on `Complete`, so a snapshot that
fails to validate lands the job `Failed` with the source still recoverable.

### Control plane

- **`sandbawks-types` `SnapshotJobPhase`** — new `Validating` variant between
  `Uploading` and `Restoring`, observable so `kubectl get sbxsj` shows the
  gate. Added to every in-flight phase match (node + controller idempotency
  floors, controller `observe_snapshot_job`).
- **`state_machine.rs`** — `Trigger::SnapshotUploaded` → `Trigger::SnapshotValidated`.
  The Pausing→Paused edge now gates on "validated" rather than bare "uploaded",
  making the invariant legible in the pure state machine. `observe_snapshot_job`
  maps a Pause job's `Complete` to `SnapshotValidated` (Complete now *means*
  validated).
- **`RuntimeAdapter::validate_restorable(manifest, source)`** — new trait
  method, default `Ok(())` (test fakes / partial adapters), mirroring the no-op
  `pause`/`resume` defaults.
- **node `reconcile.rs`** — the Pause arm now: `Checkpointing` → `pause()` →
  `Validating` → `validate_restorable()` → `Complete` (with manifest) / `Failed`
  (no manifest). A checkpoint/upload failure short-circuits before the gate; a
  validation failure drops the manifest so the controller can never fold a
  `snapshotRef` it would later resume from, and is `tracing::warn!`-logged.

### Node adapter — a storage round-trip proof

`GvisorAdapter::validate_restorable` re-fetches the **uploaded** snapshot from
`source` into a throwaway scratch dir and proves it materializes back into a
complete, restorable checkpoint bundle — via the shared
`fetch_verify_and_lay_out` + `verify_checkpoint_postconditions` helpers (one
definition each, shared with resume/stage/pause):

1. **Integrity** — manifest re-fetches, aggregate sha256 cross-checks (#171),
   every part's per-chunk + per-part sha256/size verifies, and `memory.zst`
   decompresses + un-tars cleanly. Catches upload corruption, truncation,
   missing parts, undecompressable archives.
2. **Completeness** — the round-tripped `*.img` set satisfies the same
   checkpoint postconditions a live pause enforces (all three present and
   non-empty; #232). Catches a snapshot whose parts verify individually but
   whose memory image set is structurally incomplete.

No second sandbox boot, no dependency on the source container's bundle → the
gate is robust across environments and adds no per-pause sandbox cost.

## Why not a *live* throwaway `runsc restore`?

The issue's "ideal" was a throwaway `runsc restore` of the staged bundle. That
was built and validated against a single-level Docker host (standalone `runsc`
+ a local kind cluster), where it round-tripped green. **CI proved it
inherently racy**, and the R&D is worth recording:

- A gVisor whole-sandbox memory restore must replay the **pod-sandbox (root)**
  container's OCI bundle — restoring the checkpointed *app* sub-container
  standalone fails `cannot load sandbox`.
- But containerd **reaps that bundle directory** (`config.json` + rootfs)
  within seconds of `runsc checkpoint` stopping the sandbox. The separate
  `validate_restorable` step then races the reap: it *won* on kind-smoke (a
  roomy two-node cluster, sandbox on a separate worker) and *lost* on the
  resource-tight single-node **bench** cluster, where validation failed with
  `FetchSpec failed: … config.json: no such file or directory`.
- A fail-closed gate that flakes on reap timing is worse than none — it would
  spuriously fail legitimate pauses under load. The live restore also booted a
  second full sandbox per pause (extra memory + latency that already strained
  the bench's tight node).

Other findings from the live-restore R&D (all moot now, kept for the record):
the restore had to use a fresh scratch `runsc --root` + `--network=none` for
isolation; `runsc restore --detach` daemonizes a sandbox that inherits the
parent's stdout pipe, so `.output()` hangs (must use a stderr file +
`.status()`); and the node DaemonSet needed `mountPropagation: HostToContainer`
for the restored gofer to see the rootfs. The round-trip proof needs none of
this.

## Why the resume edge needs no new gating

At resume time the source is already gone and the durable snapshot lives in S3;
a bad staged snapshot already surfaces as the restore Pod never reaching Ready
→ `Failure` → `Failed`, with the S3 snapshot intact. The new gating is on the
pause→reclaim edge, where a live source is at risk.

## Validation

- **Unit/integration (all green):**
  - node `tests/reconcile.rs` — `pause_validation_failure_lands_at_failed_without_manifest`
    (the issue's first acceptance criterion: validation failure → `Failed`, no
    manifest → controller never reaches `Paused` → source never reclaimed),
    plus the happy-path/recovery patch sequences updated for the `Validating`
    gate.
  - node `tests/gvisor.rs` — `validate_restorable_succeeds_for_intact_uploaded_bundle`,
    `validate_restorable_rejects_corrupt_uploaded_bundle` (per-part sha mismatch
    → `SnapshotDownload`), `validate_restorable_rejects_incomplete_image_set`
    (missing `pages_meta.img` → `CheckpointFailed`).
  - controller state-machine unit/property tests updated for `SnapshotValidated`.
- **gVisor kind smoke (PASSED):** healthy `pause → snapshot → delete pod →
  resume → file round-trip` green **with the validation gate active**, on both
  the two-node kind-smoke cluster and the resource-tight single-node bench
  cluster — the round-trip proof has no reap race and no second-sandbox cost,
  so it passes deterministically where the live restore flaked.

## Files

- `crates/sandbawks-types/src/snapshot_job.rs`, `src/runtime.rs`
- `crates/sandbawks-controller/src/state_machine.rs`, `src/reconcile.rs`, `src/store.rs`
- `crates/sandbawks-node/src/reconcile.rs`, `src/gvisor.rs`
- `deploy/crds/snapshotjobs.sandbawks.io.yaml` (regenerated — `Validating` enum)
- tests: controller `state_machine_unit.rs` / `state_machine_properties.rs`,
  node `tests/reconcile.rs` / `tests/gvisor.rs`
