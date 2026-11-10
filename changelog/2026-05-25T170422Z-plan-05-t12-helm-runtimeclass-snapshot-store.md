# Plan 05 T12 — Helm chart: RuntimeClass + S3 secret + real node binary + RBAC

**Date:** 2026-05-25
**Issue:** #148
**Plan:** `docs/superpowers/plans/2026-05-24-sandbawks-05-gvisor-node.md` § Task 12
**Parent epic:** #136

## Why

Before T12 the Helm chart could not stand up a working gVisor snapshot
path end-to-end:

- No `RuntimeClass: gvisor` resource existed, so any Pod stamped with
  `runtimeClassName: gvisor` by the controller's PodAdapter would stay
  Pending with `RuntimeClassNotFound`.
- The node DaemonSet still ran `--placeholder` and had no S3 credentials
  or `NODE_NAME` downward-API env, so the real `SnapshotJob` reconciler
  (landed in T9) could not be exercised from the chart.
- The chart's RBAC granted neither the node nor the controller any
  permissions on `snapshotjobs`, so the controller could not create the
  CRs and the node could not watch/update them.
- Operators had no in-chart way to bring S3 access/secret keys — they
  had to be created out-of-band, which made fresh `helm install`
  deployments to scratch clusters useless without a manual prep step.

## What changed

### New templates

- `templates/runtimeclass-gvisor.yaml` — cluster-scoped `RuntimeClass`
  named `gvisor` with `handler: runsc`. Gated on `.Values.gvisor.enabled`
  so operators on clusters without runsc can opt out.
- `templates/snapshot-store-secret.yaml` — auto-generated Opaque Secret
  named `sandbawks-snapshot-store` carrying `SANDBAWKS_S3_ACCESS_KEY`
  (32 chars) and `SANDBAWKS_S3_SECRET_KEY` (64 chars). Renders only when
  `.Values.snapshotStore.existingSecret` is empty. Uses Helm's `lookup`
  to preserve credentials across upgrades (rotating them would orphan
  every snapshot in the bucket) and ships with
  `helm.sh/resource-policy: keep` so uninstalls don't blow them away.

### Modified templates

- `templates/_helpers.tpl` — added `sandbawks.snapshotStoreSecretName`
  helper mirroring `sandbawks.signingSecretName`, resolving to either
  the operator-pinned `existingSecret` or the chart-managed default.
- `templates/node-daemonset.yaml`:
  - `args: ["--placeholder"]` → `args: []` (real binary now lives in
    the image, no more placeholder mode).
  - Added `env.NODE_NAME` via `fieldRef: spec.nodeName` downward-API,
    plus `SANDBAWKS_S3_ENDPOINT`, `_BUCKET`, `_REGION` from values.
  - Added `envFrom.secretRef` pointing at the resolved snapshot-store
    Secret name, surfacing `SANDBAWKS_S3_ACCESS_KEY` and
    `SANDBAWKS_S3_SECRET_KEY` into the container env.
- `templates/rbac.yaml`:
  - Extended `sandbawks-controller` ClusterRole with `snapshotjobs`
    (full verbs) and `snapshotjobs/status` (get/update/patch) — the
    controller creates SnapshotJob CRs.
  - Extended `sandbawks-node` ClusterRole with the same `snapshotjobs`
    rules — the node reconciler watches and patches its own CRs.

### values.yaml

- Added `gvisor.enabled: true`.
- Removed `snapshotStore.credentialsSecret: sandbawks-s3-credentials`
  (unreferenced — no template ever consumed it).
- Added `snapshotStore.existingSecret: ""` (empty → auto-generate).

## Verification

```bash
helm lint deploy/helm/sandbawks
# → 0 chart(s) failed (only the existing icon INFO).

helm template test deploy/helm/sandbawks --set gvisor.enabled=true
# → renders RuntimeClass gvisor, sandbawks-snapshot-store Secret,
#   DaemonSet with args: [], NODE_NAME downward-API, envFrom secretRef
#   sandbawks-snapshot-store, both ClusterRoles with snapshotjobs rules.

helm template test deploy/helm/sandbawks --set snapshotStore.existingSecret=my-creds
# → does NOT render sandbawks-snapshot-store Secret; DaemonSet's
#   envFrom secretRef points at "my-creds".
```

Workspace gate (Rust):

```bash
just fmt           # cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings   # clean
just test          # 47 suites, all passed
```

## Out of scope

Per the issue body these belong to later tasks:

- T13 — kind cluster config switching to multi-node + custom node image,
  MinIO fixture creating the bucket + Secret.
- T14 — integration test exercising the full snapshot/resume loop.

## Notes for reviewers

- The snapshot-store Secret uses `lookup` to preserve credentials across
  `helm upgrade` runs. This is the same trick the api signing secret
  could use but doesn't — that one rotates intentionally because
  unsigned tokens have a 24h TTL. Snapshot creds must NOT rotate.
- I chose the existing fixed-name convention (`sandbawks-snapshot-store`)
  over introducing a `sandbawks.fullname` helper as the issue body
  loosely suggested, to stay consistent with the existing
  `sandbawks-api-signing` / `sandbawks-controller` resource naming.
