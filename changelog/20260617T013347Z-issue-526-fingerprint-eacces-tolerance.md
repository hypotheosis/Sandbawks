# Issue #526 — rootfs fingerprint tolerates unreadable entries

**Date:** 2026-06-17T01:33:47Z
**Branch:** `fix-issue-526-fingerprint-eacces-tolerance`
**Crate:** `envd` (`crates/envd/src/fingerprint.rs`)

## Problem

`RootfsFingerprint::compute` walks `--root` (default `/`) and SHA-256
fingerprints the writable rootfs as a resume oracle (#236). The walk had
**no tolerance for an unreadable entry**: a single `EACCES` on `read_dir`,
`symlink_metadata`, or `File::open` aborted the whole computation and
propagated the `io::Error`.

In production envd runs as the image's non-root `USER` (e.g.
`compound-sandbox` ships `USER dev`) with `--root=/`, where root-owned
`0600` files and `0700` directories are common. The first such file made
`compute` return `Err`; `SnapshotHandle::fingerprint_rootfs` swallows that
by design (logs a WARN, stores `None`), so the snapshot carried **no
fingerprint** and `verify_rootfs_fingerprint` silently skipped
re-verification. The corruption-detection guarantee #236 was built for
degraded to no protection whenever the walk hit an unreadable path.

## Fix (issue Option 1: deterministic tolerance)

The fix is contained entirely in `fingerprint.rs`; the binary's
capture/verify paths are unchanged — once `compute` stops erroring on
`EACCES`, the existing code populates and re-verifies the oracle.

An unreadable entry is now recorded **deterministically** from whatever
metadata is available, under a dedicated domain-separation tag, instead of
aborting the walk:

- **Unreadable file/symlink contents** (`File::open` / `read_link` →
  `EACCES`): hashed as `framed(path) + mode + rdev + TAG_UNREADABLE + size`.
  The `mode` field carries the file-type bits, so an unreadable file cannot
  collide with an unreadable symlink under the single tag. Path, mode,
  rdev, and size still contribute, so a permission/type/size divergence is
  still caught; only the (unreadable) bytes fall outside the oracle.
- **Unreadable directory** (`read_dir` → `EACCES` on a *nested* dir):
  children are skipped (the directory entry itself was already hashed by
  its parent as a normal `TAG_DIR` entry). Mirrors the existing
  `NotFound`-on-nested-dir tolerance; a missing/unreadable **root** stays
  fatal.
- **Un-stat'able entry** (`symlink_metadata` → `EACCES`, e.g. a parent dir
  readable but not searchable): recorded path-only under a distinct
  `TAG_UNREADABLE_NO_META` tag, then skipped (no type ⇒ cannot know whether
  to descend).

The **readable encoding is byte-for-byte unchanged** (`TAG_FILE + size +
contents`), so any snapshot that previously produced a fingerprint (i.e.
had a fully-readable tree) hashes identically — no format-compatibility
risk. Non-`EACCES` I/O errors still propagate.

### Determinism across pause/resume

envd resumes under the same UID against the restored filesystem, so the
same entries are unreadable on both sides and the digest is stable. If an
entry's readability or permissions diverge, its `mode`/tag changes and the
resume **fails closed** exactly as a content divergence would — preserving
the documented fail-closed-on-*mismatch* semantics. The "absence ≠
mismatch" behavior is untouched (it just no longer triggers in the common
production config).

### Behavioral activation risk

By populating the fingerprint where it was previously absent, this converts
"no oracle, verification skipped" into an active oracle that, for an
unreadable file, keys on its `mode` **and** `size`. Unreadability does not
imply immutability: a root-owned, same-filesystem file envd cannot read but
whose size changes between pause and resume (e.g. a log another process
appends to) now produces a hard `FingerprintError::Mismatch` and fails the
resume, where before #526 the aborted walk stored `None` and verification
was skipped. In practice a paused sandbox is frozen and the overlay-tmpfs
default bounds the surface, so this is the intended #236 hardening rather
than a regression — but it is a genuinely new failure mode and is now
called out in the `compute` doc comment.

## Tests

Four new `#[cfg(unix)]` unit tests, gated off DAC-bypass environments
(root / `CAP_DAC_OVERRIDE`) via a `0000`-file probe — mirroring the pattern
in `sandbawks-node::runsc_reconcile` — since root reads everything and the
`EACCES` path would be vacuous:

- `unreadable_file_does_not_abort_walk` — a `0000` file is counted, the
  walk completes, the digest is deterministic across recompute, and
  `verify` round-trips.
- `unreadable_file_becoming_readable_fails_closed` — flipping a file
  readable changes the digest and `verify` returns `Mismatch`.
- `unreadable_directory_skips_children_without_aborting` — a `0000`
  directory is counted but its child is not walked; deterministic.
- `unstattable_entry_under_unsearchable_parent_is_recorded` — a `0600`
  (readable-but-not-searchable) parent makes `symlink_metadata` on its
  child fail with `EACCES`, exercising the `hash_unstattable_entry`
  path-only-marker branch; the child is counted and deterministic.

The unreadable-*symlink* `read_link`→`EACCES` branch is defensive symmetry
that is not reachable via the normal walk — `readlink` and `lstat` have
identical permission requirements on the parent, so any path that reaches
`read_link` already stat'd successfully — and so is not separately tested.

All four are also gated behind the `require_fs_reflects_mutations!()` probe
introduced by #566 (#527): they assert on entry counts and chmod
observation, which a masking overlay (the gVisor `--overlay2` merged view)
does not promptly reflect, so on such a filesystem they skip with a
breadcrumb rather than failing spuriously.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — all suites pass; the new tests genuinely exercised the
  `EACCES` path on the non-root
  runner.

## Acceptance criteria

- [x] Non-root UID with `--root=/` produces a snapshot whose
  `rootfs_fingerprint` is **present** (an unreadable entry no longer aborts
  `compute`).
- [x] An unreadable entry no longer aborts the fingerprint; the result is
  deterministic across a pause/resume cycle.
- [x] Resume re-verification operates on a populated fingerprint in the
  production permission configuration.
- [x] Tests cover the unreadable-entry case, gated off DAC-bypass
  environments.
