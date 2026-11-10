# Issue #527 — envd `RootfsFingerprint` tests fail on overlayfs sandbox

**Date:** 2026-06-16T20:43:43Z
**Branch:** `fix-issue-527-fingerprint-overlayfs-tests`
**Area:** `area/envd`, `kind/test`

## Problem

Nine `RootfsFingerprint` unit tests in `crates/envd/src/fingerprint.rs` fail
when the test tempdirs land on an overlayfs mount (the gVisor `--overlay2`
merged view inside a sandbox, where `/` — and therefore `/tmp` — is `overlay`).
They pass on ext4/tmpfs.

The failures are environmental, not algorithmic. The masking filesystem does
not promptly reflect, within a single process, the mutations the assertions
depend on:

- `read_dir` returns a stale listing, so the walk counts `1` entry instead of
  `3` (`compute_is_deterministic`).
- File content reads return cached bytes, so a same-length rewrite or
  truncation does not move the digest.
- chmod / symlink-retarget likewise go unobserved.

The production `RootfsFingerprint::compute` / walker is correct on a
well-behaved filesystem and is explicitly out of scope to change.

## Fix

Test-only. Added a **behavior probe** (issue approach 1, done by probing
observed behavior rather than sniffing the filesystem type via `statfs` magic
numbers — brittle, and not the actual precondition):

- `probe_fs_reflects_mutations()` builds a throwaway tree on the tempdir
  filesystem and checks the two preconditions every gated assertion shares:
  the walk sees all freshly-created entries (`entries == 3`), and a
  same-length content rewrite moves the digest. Any I/O error conservatively
  reports the filesystem as unusable.
- `fs_reflects_mutations()` caches that probe once per test process via
  `OnceLock` (the masking is a deterministic filesystem property).
- `require_fs_reflects_mutations!()` macro skips the enclosing test (early
  `return`, with a diagnostic line on stderr) when the probe fails.

The macro guards the nine mutation-dependent tests. The four tests that pass
on overlayfs (`order_independent_across_creation_order`,
`empty_dir_is_stable_and_nonempty_digest`, `verify_accepts_matching_tree`,
`verify_surfaces_io_error_for_missing_root`) are left ungated.

This mirrors the behavior-probe convention already used in
`sandbawks-node::runsc_reconcile` (root self-detect "without a uid
dependency") instead of introducing a uid/magic-number dependency.

## Validation

- `cargo test -p envd --lib fingerprint` — green on tmpfs (this host): the
  probe passes and all 13 tests run as before.
- On a masking overlay the nine gated tests skip with a stderr breadcrumb
  instead of failing; the suite stays green.
- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`.

## Out of scope

- Production fingerprint algorithm / walker — unchanged.
- Unrelated test failures.
