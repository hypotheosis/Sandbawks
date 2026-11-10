# Issue #270 — pin envd by digest for restore determinism

**Date:** 2026-06-02T19:00:00Z
**Branch:** `fix-issue-270-pin-envd-digest-restore`
**Labels:** `kind/fix`, `area/controller`, `runtime/gvisor`, `prio/p1`

## Problem

Under the node-staging envd delivery model (issue #265 / PR #266), the
workload container execs the *cluster-wide* `envd` binary that the
`sandbawks-node` DaemonSet stages onto every node and mounts read-only.
The binary's executable pages are therefore `mmap`-backed by that on-disk
file. A memory-only `runsc restore` reconnects those mappings to whatever
`envd` binary the **restore** node has staged.

If the cluster's `envd.image` is a mutable tag and gets re-pushed between
a sandbox's pause (`runsc checkpoint`) and its resume (`runsc restore`),
the restore node stages a *different* binary. The restored process's code
pages then mismatch the on-disk file → silent restore corruption.

> Note: the issue was filed against the older *init-container injection*
> model. PR #266 replaced that with node-staging, so there is no longer a
> per-Pod inject init container — but the determinism requirement is
> unchanged, and `SandboxTemplate.spec.envd_image` remains unconsumed on
> the cold-start/restore paths. The thing that must be pinned is the
> cluster-wide Helm `envd.image`/`envd.tag`.

## Fix

Make the resume path carry a **deterministic, manifest-recorded envd
digest**, and fail closed when the restore node's binary has drifted:

1. **`SnapshotManifest.envd_sha256`** (`crates/sandbawks-types`): new
   `#[serde(default)]` field — the lower-case-hex SHA-256 of the staged
   envd binary the sandbox was running. Empty for v1 manifests, so the
   schema change is backward-compatible (deserializes to `""`).

2. **Pause records it** (`GvisorAdapter::pause`): the node worker hashes
   the staged binary at `DEFAULT_ENVD_BIN_PATH`
   (`/var/lib/sandbawks/envd/envd`) and stores the digest in the manifest.
   Best-effort: a worker without the envd mount records an empty digest
   and logs a warning rather than failing the pause.

3. **Restore verifies it** (`GvisorAdapter::verify_envd_digest`, called
   from the shared `fetch_verify_and_lay_out`, so both `stage_snapshot`
   and the in-process `resume` get it): if the authoritative manifest
   carries a digest and it differs from this node's staged binary, fail
   closed with `RuntimeError::RestoreFailed` *before* materializing any
   bytes. Graceful fall-throughs (warn + proceed) when there is no
   recorded digest or the local binary is unreadable — the per-part
   sha256 gate still protects the snapshot bytes.

4. **Helm**: the `node` container now mounts the staged-envd hostPath
   read-only (it previously was mounted only into the `stage-envd` init
   container), so the worker can hash it. `values.yaml` documents that
   `envd.tag` must be pinned to an immutable tag for deterministic
   restore.

5. **CRD regenerated** (`deploy/crds/snapshotjobs.sandbawks.io.yaml`):
   `SnapshotManifest` is embedded in `SnapshotJob.status`.

6. **DRY**: factored the byte-slice hashing into
   `sandbawks_types::snapshot::sha256_hex`, reused by
   `SnapshotPart::from_bytes` and the new envd digest.

## Acceptance mapping

- *"The resume path has a deterministic envd digest (manifest-recorded or
  otherwise threaded)."* — Satisfied: the digest is recorded in the
  manifest at pause and re-checked at restore.
- *"A pause→resume cycle re-injects the same envd binary even if the tag
  is re-pushed in between."* — With a pinned digest every node stages a
  byte-identical binary and resume succeeds. With a mutable tag that
  drifts, the restore **fails closed** (clean, retriable) instead of
  silently corrupting. Carrying a per-snapshot copy of the binary to make
  a drifted resume *succeed* was deliberately left out of scope: it would
  break the "cold-start and restore Pods are topologically identical to
  the checkpoint" invariant the node-staging model relies on.

## Tests

- `sandbawks-types`: `sha256_hex` known-vector + agreement with
  `from_bytes`; manifest serde round-trip carries `envdSha256`; v1 compat
  deserialize defaults it to empty.
- `sandbawks-node` (`tests/gvisor.rs`):
  - `pause_records_node_staged_envd_digest`
  - `pause_tolerates_missing_envd_binary`
  - `stage_fails_closed_on_drifted_envd_binary`
  - `stage_accepts_matching_envd_binary`

## Validation

- `just fmt` — clean
- `cargo clippy --workspace --all-targets -- -D warnings` — clean
- `just test` — all pass
- `helm template … --set node.enabled=true` — renders; `envd-bin`
  read-only mount present on the `node` container
