# CI: tear down per-run buildx builders to stop dind ENOSPC

Issue: #428

## Symptom

A Forgejo runner doing docker-in-docker (dind) failed image builds with:

```
ResourceExhausted: failed to compute cache key: write
/var/lib/buildkit/runc-overlayfs/snapshots/snapshots/16/fs/usr/local/rustup/...:
no space left on device
```

The runner host showed no disk pressure. The space was held *inside* the dind
daemon, which the host sees only as one opaque volume.

## Root cause

`docker/setup-buildx-action@v3` provisions a dedicated `builder-<uuid>`
BuildKit container (the `docker-container` driver) on every run, each with its
own cache volume. The action's own post-cleanup hook does not fire reliably on
the Forgejo runner — cancelled and hard-killed jobs skip post-steps — so the
builders accumulated. Live inventory at time of diagnosis: 20+ orphaned
`buildx_buildkit_builder-*` containers, the oldest 7 days old, each pinning a
cache volume. A host-level `docker system prune -af` reclaimed 0B because the
garbage lives in the nested daemon, and `docker buildx prune` only touches the
*currently selected* builder, not the 20 orphans.

The kind-cluster leak was already handled by existing `always()` teardown
steps (issue #406); only the buildx side was unmanaged.

## Change

Added an `id: buildx` to every `setup-buildx-action` step and an
`if: always()` "Tear down buildx builder" step to every job that creates one,
across all three workflows:

- `.forgejo/workflows/ci-runner-image.yaml` (`build-and-push`)
- `.forgejo/workflows/kind-node-image.yaml` (`build-and-push`)
- `.forgejo/workflows/kind-smoke.yaml` (`build-images` matrix, `kind-smoke`,
  `bench`)

Each teardown:

1. **Scoped removal** — `docker buildx rm --force "${{ steps.buildx.outputs.name }}"`
   removes *this run's* builder plus its cache volume on every exit, including
   cancellation. Guarded by a non-empty check because the `kind-smoke` and
   `bench` jobs only set up buildx on a kind-node cache miss.
2. **Age-based orphan sweep** — reaps any `buildx_buildkit_*` container older
   than 2h (and its named volumes), backstopping runs hard-killed before any
   step could run. The 2h gate is greater than the longest job timeout
   (`bench`, 75 min), so a live co-resident run's builder is never reaped.

This mirrors the design of the existing co-resident-safe kind-cluster
teardown: per-run scoping for the normal path, an age-gated sweep for the
hard-kill path, and strict avoidance of bulldozing a concurrent run's
resources.

## One-time manual cleanup (operator action, not in CI)

The pre-existing orphans must be cleared once by hand from inside the dind
daemon (the workflow change only prevents *future* accumulation):

```sh
docker buildx ls
docker buildx rm builder-<uuid>      # each stale builder -> removes container + volume
docker container prune -f
docker volume prune -af
```

## Validation

- All three workflow files parse under `yaml.safe_load`.
- 5 `id: buildx` tags and 5 `Tear down buildx builder` steps present
  (1 + 1 + 3).
