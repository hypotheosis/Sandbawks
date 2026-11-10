# Plan 07 hardening: envd rootfs fingerprint — hash at pause, re-verify after restore

- **Issue:** #236 (one of the deferred fail-closed hardening items from #232's
  "Snapshot integrity / fail-closed" section). Part of epic #204 (Plan 07).
- **Date:** 2026-05-31T21:57:19Z
- **Status:** implemented and fully unit/property/integration-tested; validated
  end-to-end against the real `envd` binary (capture, matching-resume, and
  fail-closed tamper-resume all exercised locally).

## Problem

Memory-only `runsc restore` (#232) reconstructs the sandbox rootfs alongside
its memory image. Nothing today gives resume a *deterministic oracle* for
rootfs divergence: a spill, a truncation, a silent same-length corruption, or a
missed non-overlay mount would hand back a sandbox whose filesystem is subtly
wrong, with no signal. #235 added the storage round-trip gate (the uploaded
bundle re-fetches + verifies), but that proves the *bytes were stored*, not
that the *restored filesystem* matches what was captured.

## Fix — a content fingerprint that pairs pause with resume

New module `crates/envd/src/fingerprint.rs` (`RootfsFingerprint`):

- **`compute(root)`** walks the writable rootfs deterministically and folds a
  SHA-256 over it. Each entry is hashed into its own 32-byte digest over a
  *length-framed* encoding of `(relative path, st_mode, rdev, content |
  symlink-target | type-tag)`; the per-entry digests are sorted and folded into
  the final hash. Sorting makes the result order-independent (it mirrors the
  `aggregate_sha256` pattern in `sandbawks-types::snapshot`), and the per-entry
  digests are mutually independent so the walk is trivially parallelizable if
  ever hot.
- **`verify(&self, root)`** recomputes and compares, returning
  `FingerprintError::Mismatch { expected, actual }` on divergence or
  `FingerprintError::Io` if the rootfs can't even be read.

### Design decisions (the "decide and document" calls from the issue)

- **Full-content hashing, not content-free.** The issue lists *corruption*
  among the divergences to catch, and only hashing file contents detects a
  same-length byte flip. A path+mode+size variant is cheaper but blind to that.
  Cost is `O(bytes under --root)`, run only at pause and resume boot (never on a
  request hot path) and offloaded to a blocking thread so the async reactor is
  never stalled. For the overlay-tmpfs default the writes are bounded, so the
  cost tracks the workload delta in the common case.
- **Surface = envd's `--root`.** envd runs *inside* the sandbox and sees the
  merged overlay view; it can't address the host-side overlay *upper* dir
  (that's a node/containerd concept). The in-sandbox view is the closest
  surface envd can re-verify directly — which is exactly why #236 prefers the
  envd-side check. Scoping the walk to the upper once envd is handed that path
  is noted as a future refinement.
- **No `runsc`-following of symlinks.** `symlink_metadata` + hashing the link
  *target* means a symlink that points outside the tree (or forms a cycle)
  can't make the walk escape or loop.
- **A missing root fails loudly; a nested dir vanishing mid-walk is benign.**
  `collect_entries` distinguishes the top-level call (`is_root`) so a
  nonexistent restored rootfs surfaces as `Io(NotFound)` rather than papering
  over as the empty-tree digest, while a concurrent removal of an
  already-hashed subdir is skipped.

### Wiring

- **`EnvdState`** (`state.rs`) grows `rootfs_fingerprint: Option<RootfsFingerprint>`
  with `#[serde(default)]`. **Additive** — snapshots written by a pre-#236 envd
  decode the absent field to `None`, so `SCHEMA_VERSION` stays at `1`. Added a
  `with_rootfs_fingerprint` builder so the I/O-heavy compute can run on a
  blocking thread and fold in without re-cloning the registries.
- **Capture (pause)** — `SnapshotHandle` now carries `root`;
  `SnapshotHandle::capture` computes the fingerprint on `spawn_blocking`. A
  compute failure is logged and the fingerprint left absent rather than failing
  the capture (resume fails closed only on a genuine *mismatch*, never on
  absence).
- **Re-verify (resume boot)** — `load_boot_seed` now takes `root` and calls
  `verify_rootfs_fingerprint` before rehydrating the registries. On
  `Mismatch` the boot aborts with a clear error chain; absence logs and skips.
  This is the envd-side oracle #236 calls for — the verdict is surfaced both
  via the boot result and `/__state`.
- **Node mirror** (`sandbawks-node::gvisor::EnvdState`) needs **no change**: it
  is lenient (`#[serde(default)]`, ignores unknown fields) and the node does
  not do the comparison — envd does. The new field is simply ignored there.

## Tests

- **Unit (`fingerprint.rs`, 12 tests):** deterministic + order-independent;
  content change / truncation / add / remove / permission / symlink-target all
  change the digest; empty dir is the fixed SHA-256-of-nothing; `verify`
  accepts a match, returns `Mismatch` on divergence, and `Io(NotFound)` for a
  missing root.
- **State (`tests/state.rs`):** `capture` defaults the field to `None`; a
  `Some` fingerprint survives the JSON round-trip; an older snapshot with no
  `rootfs_fingerprint` key decodes to `None` and still restores.
- **Property (`tests/state_properties.rs`):** the `EnvdState` generator now
  draws an optional fingerprint, so the round-trip + determinism invariants
  cover the new field.
- **End-to-end (manual, real binary):** boot against a tempdir root → `/__state`
  and the flushed `state.json` both carry `{digest, entries:3}`; resume against
  the matching tree logs "rootfs fingerprint re-verified"; resume against a
  same-length tampered file aborts with exit code 1 and the mismatch error.

## Standard gates

- `just fmt` — clean.
- `just clippy` (`cargo clippy --workspace --all-targets -- -D warnings`) — clean.
- `just test` (`cargo test --workspace --all-targets`) — all green.

## Out of scope

Acting on the verdict at the orchestration layer is the validate-before-teardown
gate (#235, shipped in PR #239); this item provides the oracle. Scoping the walk
to the overlay upper (vs. the merged in-sandbox view) is a future refinement.
