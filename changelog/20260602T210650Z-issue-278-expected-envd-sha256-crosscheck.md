# Issue #278 — thread `expected_envd_sha256` through SnapshotJob for the pause-side envd cross-check

Date: 2026-06-02T21:06:50Z
Branch: `feat-issue-278-expected-envd-sha256`

Follow-up to #270 / #171.

## Problem

PR #276 (issue #270) records the staged envd binary's sha256 in the
snapshot manifest at pause and re-checks it at restore via
`verify_envd_digest`. But that restore-side check compares the
**re-fetched** manifest's `envd_sha256` against the local binary — it is
**not** cross-checked against a controller-committed value the way the
aggregate manifest sha256 is (`SnapshotJobSpec.expected_sha256`, issue
#171).

A manifest tampered in storage that flips **both** `sha256` and
`envd_sha256` to a drifted binary the attacker controls would therefore
pass the envd dimension of the pause-side commit cross-check: the storage
manifest redefines "the expected envd" on the restore node, and the
local-binary comparison then succeeds against the attacker's substitute.
The aggregate-sha256 commit cross-check (#171) already covers the
manifest-tamper case whenever the controller committed a non-empty value,
but the envd dimension was uncommitted.

## Resolution

Mirrored the #171 aggregate-sha256 commit chain for the envd digest, so
the controller commits the envd sha256 it observed at pause and the node
cross-checks it against the re-fetched manifest **before**
`verify_envd_digest` compares it to the local binary.

### Type changes — `crates/sandbawks-types`

- `SnapshotJobSpec.expected_envd_sha256: Option<String>`
  (`snapshot_job.rs`) — the committed envd digest, sourced from
  `Sandbox.status.snapshot_envd_sha256`. Optional + `skip_serializing_if`
  so legacy `SnapshotJob` resources round-trip unchanged.
- `SandboxStatus.snapshot_envd_sha256: Option<String>` (`sandbox.rs`) —
  carries the pause-observed envd digest forward to the next Resume/Stage
  dispatch, mirroring `snapshot_sha256`.

### Controller — `crates/sandbawks-controller/src/reconcile.rs`

- `SnapshotJobObservation::Complete` gains `snapshot_envd_sha256`,
  populated from the completing Pause job's `manifest.envd_sha256`
  (filtered to non-empty) in `observe_snapshot_job`.
- The main reconcile loop folds it into
  `new_status.snapshot_envd_sha256`.
- `build_snapshot_job` reads it from status into
  `SnapshotJobSpec.expected_envd_sha256` for `Resume`/`Stage` jobs
  (`None` for `Pause`).

### Node — `crates/sandbawks-node/src/reconcile.rs`

- The `Resume` and `Stage` arms thread
  `job.spec.expected_envd_sha256` into the placeholder
  `SnapshotManifest.envd_sha256` (previously hardcoded empty), so the
  adapter receives the committed value.

### Adapter cross-check — `crates/sandbawks-node/src/gvisor.rs`

- `fetch_verify_and_lay_out` adds, immediately after the #171 aggregate
  cross-check and before `verify_envd_digest`:

  ```rust
  if !manifest.envd_sha256.is_empty() && fetched.envd_sha256 != manifest.envd_sha256 {
      return Err(RuntimeError::SnapshotDownload(...));
  }
  ```

  No-op when the controller committed no value (legacy callers / v1
  manifests) — the always-on per-part sha256 gate and the local-binary
  determinism check still run. Fires before any bytes are materialized.

### CRD regeneration — `deploy/crds/`

- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`.
- `snapshotjobs.sandbawks.io.yaml` gains `spec.expectedEnvdSha256`.
- `sandboxes.sandbawks.io.yaml` gains `status.snapshotEnvdSha256`.

## Tests

- `crates/sandbawks-node/tests/gvisor.rs::resume_rejects_storage_envd_sha256_mismatch_when_committed`
  — aggregate sha256 kept matching so the new envd cross-check is the gate
  that fires; asserts `SnapshotDownload` with the storage digest surfaced
  and no rootfs delta applied.
- `crates/sandbawks-types/src/snapshot_job.rs` — `expected_envd_sha256`
  omitted-when-`None` and camelCase round-trip.
- Extended controller tests: the Pause→Paused fold-in now asserts
  `manifest.envd_sha256` → `status.snapshot_envd_sha256`, and the
  stage-dispatch test asserts `status.snapshot_envd_sha256` →
  `spec.expected_envd_sha256`.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — all pass

## Notes

Lower priority than #270 itself: this hardens against deliberate manifest
tampering, whereas #270 addressed the (much more likely) accidental
tag-drift case.
