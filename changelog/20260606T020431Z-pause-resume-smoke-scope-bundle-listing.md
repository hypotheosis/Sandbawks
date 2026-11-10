# pause-resume-smoke: scope the bundle-presence listing to the sandbox prefix

**Date:** 2026-06-06T02:04:31Z

## Summary

The pause/resume smoke's snapshot-bundle check now lists only the current
sandbox's own key subtree (`<prefix>/<sandbox-id>/`), derived from the
`snapshotRef`, instead of recursively listing the entire bucket. It also
retries the listing a few times to absorb multi-node LIST propagation lag.

## The failure

Against the persistent central garage cluster the smoke failed with:

```
pause-resume-smoke: snapshot bundle missing files: manifest.json
  parts/memory.zst.json parts/rootfs-delta.tar.zst.json parts/envd-state.json.json
```

— yet the dumped `mc` listing clearly showed those exact objects present at
`snapshots/sbx-5b15fa31a68b/...`, and the pause itself (incl. the validate
re-fetch) had already read them back by key. So the bundle existed; the
*assertion* failed to see it.

## Root cause

After per-sandbox prefixing landed (`SnapshotSink::scoped`), the smoke still
did `mc ls --recursive garage/<bucket>/` — the **whole bucket**. On the
shared, persistent central store that bucket accumulates every prior bench +
smoke run's objects (thousands of 4 MiB bench chunks, no GC), so the listing
is ~10k lines. A listing that large is fragile to capture (container-log
truncation via `kubectl logs`, and a multi-node S3 `LIST` is only eventually
consistent), which is how a present bundle reads back as "missing". The
whole-bucket scan was also conceptually obsolete: the `snapshotRef` already
names the exact per-sandbox prefix.

## The fix

`deploy/kind/fixtures/pause-resume-smoke.sh`:

- Derive `SANDBOX_PREFIX` from `snapshotRef`
  (`s3://<bucket>/<prefix>/<sandbox-id>/manifest.json` →
  `<prefix>/<sandbox-id>`) and `mc ls --recursive` only that subtree — ~7
  objects regardless of how much the bucket has accumulated.
- Wrap the list + presence check in a 5-attempt retry (re-running the
  one-shot `mc` pod, 2s between tries) so a brief multi-node LIST lag doesn't
  flake the check; objects are durably readable by key the whole time.
- The large-overlay size diagnostic still works — `MC_OUTPUT` is now scoped
  to this sandbox, so its `memory.zst`/`chunks/` grep is if anything cleaner.

This is smoke-fixture only; no product code changed. `bash -n` + `shellcheck`
clean.

## Note

The central bucket has no GC, so it will keep growing across runs. This fix
makes the smoke independent of that growth, but a periodic prune of the
`snapshots/` prefix (or enabling snapshot retention against it) is worth
doing so the store doesn't accumulate unbounded test bundles.
