# Plan 07 hardening #236 — review round on PR #240 (scope the fingerprint walk)

- **Issue:** #236. **PR:** #240.
- **Date:** 2026-05-31T22:19:13Z
- **Status:** review comments addressed; full `just fmt` / `clippy` / `test`
  gate green, including a new fail-closed resume boot integration test.

## What the review caught

The self-review on PR #240 (quality 68/100) flagged two **blocking correctness
bugs** — both the same root cause: with the production default `--root=/`, the
fingerprint walk hashed surfaces that are *not* stable across a pause/resume
boundary, so the oracle would fail closed against itself on the first real
resume:

1. **State-file self-reference.** `--state-file=/var/lib/envd/state.json` lives
   *inside* `--root=/` and embeds the fingerprint. Hashing it makes each
   capture's fingerprint depend on the previous one — never re-verifiable.
2. **Kernel pseudo-filesystems.** Walking `/` descends into `/proc`, `/sys`,
   `/dev`, `/run`, whose contents churn microsecond-to-microsecond.

Plus nits: a misstated `meta.len()` rationale, an under-documented non-unix
`os_bytes` weakness, a missing doc note that the fingerprint is not part of the
bundle hash, a `capture` builder round-trip, and two test-coverage gaps.

## Fixes

- **`RootfsFingerprint::compute(root, exclude)` / `verify(root, exclude)`** now
  take an `exclude` list and enforce a **single-filesystem (`st_dev`)
  boundary**:
  - The walk skips any entry on a different filesystem than `root` (mount
    points), which excludes pseudo-fs *and* bind-mounted volumes (the latter
    legitimately mutate between pause and resume). Detecting a *missing* mount
    is explicitly left to the mount-set assertion (#237) this oracle pairs with.
  - The `exclude` list drops the `--state-file`. The binary threads
    `--state-file` into both the capture (`SnapshotHandle`) and the resume
    re-verify (`verify_rootfs_fingerprint`) so both walk the identical surface.
- **`SnapshotHandle::capture`** simplified to assign `rootfs_fingerprint`
  directly (no builder round-trip).
- **Doc fixes:** corrected the `meta.len()` comment (content stream already
  encodes length; size is a cheap framing field, not truncation defense);
  documented that the non-unix `os_bytes` path is structurally weaker so local
  divergence tests are blunter than production; added a note on
  `EnvdState.rootfs_fingerprint` that it is internal and *not* an input to
  `aggregate_sha256` / per-part bundle hashes (#235 treats the file as opaque).

## Tests added

- **`fingerprint::tests::excluded_state_file_does_not_affect_fingerprint`** —
  mutating an excluded in-root state file leaves the fingerprint unchanged and
  still verifies; without the exclude the same mutation *is* visible (proving
  the exclude is load-bearing).
- **`binary_smoke::resume_reverifies_rootfs_and_fails_closed_on_divergence`** —
  the end-to-end behaviour the review called load-bearing: capture → clean
  resume reaches Ready → same-length tamper → resume **exits non-zero without
  binding listeners**, and stderr names the fingerprint mismatch.

## Left as-is (with rationale, replied on-thread)

- The `NotFound`-while-*recursing* race branch is inherently nondeterministic to
  trigger in a test; the steady-state removal path is covered, and the branch is
  a two-line guard. Offered a follow-up if desired.

## Known follow-ups (unchanged from the original PR)

- Scoping the walk to the overlay *upper* (vs. the merged in-sandbox view) to
  bound cost on large base images remains a future refinement; the `st_dev`
  boundary makes the merged-view walk *correct*, not yet *cheap*.
