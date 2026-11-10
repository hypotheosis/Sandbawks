# kind-smoke: toggle snapshot store between local garage and central cluster

**Date:** 2026-06-06T00:29:01Z

## Summary

The kind-smoke `kind-smoke` (pause/resume) and `bench` jobs can now point
their S3 snapshot backend at the **central garage cluster** instead of a
single-node garage stood up inside the kind cluster. This is the A/B
lever for the slow pause/resume investigation: it lets us compare a local
single-node garage against the real multi-node central cluster and tell
whether the slowness is an artifact of the local fixture.

## Motivation

Pause/resume round-trips are slow in CI. One untested hypothesis is that
the in-cluster single-node garage fixture (`install-garage.sh`) — an
`emptyDir`-backed, 1-replica `Deployment` — is itself the bottleneck,
rather than the snapshot pipeline. Routing the same smoke at the
production-shaped central garage cluster isolates that variable.

## Change

A new env var `KIND_SMOKE_REMOTE_SNAPSHOT_STORE` selects the backend per
job in `.forgejo/workflows/kind-smoke.yaml` (default `'1'` = central):

- **`'1'` (central):** new fixture
  `deploy/kind/fixtures/use-central-snapshot-store.sh` reuses the build
  cache's S3 connection settings (`SCCACHE_ENDPOINT`, `SCCACHE_REGION`,
  `SCCACHE_S3_USE_SSL`) and the `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` credentials, but writes snapshots into a
  **separate** bucket named by the new `SNAPSHOT_BUCKET` Forgejo var so
  build-cache and snapshot objects never collide. It provisions nothing
  in-cluster.
- **`'0'` (local):** unchanged — `install-garage.sh` stands up the
  single-node in-cluster garage.

Both branches write the `sandbawks-snapshot-store` Secret and export
`SNAPSHOT_STORE_ENDPOINT` / `SNAPSHOT_STORE_BUCKET` /
`SNAPSHOT_STORE_REGION` to `$GITHUB_ENV`, so the downstream `helm install`
`--set snapshotStore.*` flags (now including `snapshotStore.region`) and
the pause/resume smoke's `mc` bundle check are backend-agnostic.

## Files touched

- `deploy/kind/fixtures/use-central-snapshot-store.sh` — **new.** Validates
  required env, composes the endpoint scheme from `SCCACHE_S3_USE_SSL`,
  writes the snapshot-store Secret from the AWS creds, emits the resolved
  endpoint/bucket/region to `$GITHUB_ENV`.
- `deploy/kind/fixtures/pause-resume-smoke.sh` — added the
  `SANDBAWKS_SMOKE_S3_ENDPOINT_URL` knob (defaults to the in-cluster
  garage Service URL, so `just cluster-up` is unchanged) and splice the
  resolved creds into the `mc` `MC_HOST_garage` value.
- `.forgejo/workflows/kind-smoke.yaml` — both jobs: added the
  `KIND_SMOKE_REMOTE_SNAPSHOT_STORE` env (default `'1'`), replaced the
  "Install Garage" step with a branching "Provision snapshot store" step,
  drove the helm `snapshotStore.*` flags (incl. new `region`) from
  `$GITHUB_ENV`, and passed `SANDBAWKS_S3_BUCKET` +
  `SANDBAWKS_SMOKE_S3_ENDPOINT_URL` to the pause/resume smoke step.
- `docs/kind-smoke.md` — new "Snapshot store backend" section.

## Required configuration (for the `'1'` default)

Forgejo **vars**: `SNAPSHOT_BUCKET`, `SCCACHE_ENDPOINT`, `SCCACHE_REGION`,
`SCCACHE_S3_USE_SSL`. Forgejo **secrets**: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`. The kind cluster's node DaemonSet pods must have
network egress to the central endpoint. A missing var fails the provision
step fast with an actionable message.

## Known limitations

- The central bucket is not garbage-collected between runs, so the
  bundle-presence `mc ls` check can match objects written by prior runs.
  The per-sandbox byte-for-byte sentinel round-trip after resume remains
  the authoritative integrity gate.
- Local `just cluster-up` is intentionally out of scope — it always uses
  the in-cluster garage fixture.

## Reverting

Set `KIND_SMOKE_REMOTE_SNAPSHOT_STORE: '0'` in either/both job `env`
blocks to restore the in-cluster single-node garage path.
