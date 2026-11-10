# Snapshot store: per-sandbox key prefix (fixes cross-sandbox bundle corruption)

**Date:** 2026-06-06T01:11:28Z

## Summary

Snapshots are now written and read under a per-sandbox key prefix
(`<base>/<sandbox-id>/manifest.json`, `<base>/<sandbox-id>/parts/...`,
`<base>/<sandbox-id>/chunks/...`) instead of a single flat `key_prefix`
shared by every sandbox. This fixes a **product correctness bug** in the
`sandbawks-node` pause/resume snapshot path.

## The bug

`S3SnapshotSink`/`S3SnapshotSource` keyed `manifest.json` and
`parts/<name>.json` by **name only**, under one process-wide `key_prefix`
(default `snapshots`). Every sandbox therefore shared the *same* mutable
keys, and every sandbox's `snapshotRef` was the identical
`s3://<bucket>/snapshots/manifest.json`.

With a single active sandbox per bucket (exactly what the ephemeral,
single-node kind garage fixture provides) this is invisible. But against a
shared or persistent bucket, two sandboxes snapshotting in overlapping
windows clobber each other's `manifest.json` / `parts/*.json`. A resume
then reassembles a part whose bytes don't match its manifest hash and
fails:

```
snapshot download failed: sha256 mismatch for part rootfs-delta.tar.zst:
  expected 3709b52d…  got 90da5637…
```

This surfaced the moment the kind bench was pointed at the shared central
garage cluster (PR #362): pause uploaded the 1 GiB bundle fine, but resume
crossed this sandbox's `manifest.json` with another snapshot's part index.
The content-addressed `chunks/<sha>` objects were always safe; only the
name-keyed mutable objects collided.

Affected components: `sandbawks-node` (snapshot read/write) and any
deployment — not just the bench — where more than one sandbox shares a
snapshot bucket.

## The fix

- Added `scoped(sub_prefix)` to the `SnapshotSink` / `SnapshotSource`
  traits (`sandbawks-types`). The S3 impls return a handle whose
  `key_prefix` is `join(base, sub_prefix)` while sharing the underlying
  HTTP client; `manifest_url()` then encodes the per-sandbox path into the
  recorded `snapshotRef`. The in-memory test doubles (`NoopSnapshotStore`,
  `FakeSnapshotStore`) implement it as share-state no-ops.
- The node reconcile `dispatch()` scopes both sink and source by
  `job.spec.sandbox_name` before pause / validate / resume / stage. Pause
  and resume independently derive the *same* `<base>/<sandbox-id>` prefix
  from the sandbox id, so the keys always agree — no `snapshotRef` URL
  parsing required.
- Updated the controller retention sweeper's comments: its delimiter-less
  whole-bucket `LIST` recurses into the per-sandbox subtrees, so age-based
  GC still sees every object with no code change.

## Tradeoffs

- **Cross-sandbox chunk dedup is gone** (chunks now live under each
  sandbox's prefix). Within a sandbox's own pause cycles dedup still
  works. Correctness over dedup.
- **Migration:** bundles written under the old flat layout are not found
  after upgrade. Acceptable for this actively-developed feature; clear any
  stale central bucket.

## Tests

- New `s3::tests::scoped_sandboxes_do_not_clobber_shared_keys`: two
  sandboxes writing the same part name to one backing store read back their
  own bytes, and their `snapshotRef`s differ. Fails under the old flat
  layout; passes with scoping.
- Full `sandbawks-types` / `sandbawks-node` / `sandbawks-controller` test
  suites green; `cargo clippy -D warnings` and `cargo fmt --check` clean.
