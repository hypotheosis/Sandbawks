# Clone-from-snapshot: `spec.restoreFrom` (snapshot **and** filesystem modes)

**Date:** 2026-06-22T10:56:45Z
**Issue:** #416
**Branch:** `feat-issue-416-restore-from-snapshot`

## Why

Until now a sandbox could only be restored from **its own** `Paused` state. There
was no supported way to stand up a *brand-new* sandbox whose initial state is the
saved state of a *different* sandbox — both a recovery path (a wedged/`Failed`
sandbox's last-good state still lives in S3 / on a retained PVC) and a general
clone/fork primitive.

This adds an optional `spec.restoreFrom` to the `Sandbox` CR. When set on a
freshly-created sandbox, the controller restores the referenced source's state
instead of cold-booting, then transitions straight to `Running`.

The issue was filed when only gVisor S3 snapshots existed. Epic #603 since added
PVC-backed (`filesystem`) persistence, so this implementation supports **both**
persistence modes.

## What changed

### Source resolution (`RestoreSource::parse`, `sandbawks-types`)

`restoreFrom` accepts two prefix-disambiguated forms:

- a **source sandbox ID** (e.g. `sbx-warm-l5dkn`) — the controller looks up that
  Sandbox CR in the same namespace. Works even if the source is `Failed`, as long
  as the source CR still exists.
- a raw **`s3://…/manifest.json` URL** (snapshot mode, recovery path for a
  *deleted* source) — recognized but **not yet actioned** (see Deferred below).

### Branch on the new sandbox's resolved persistence mode

The controller reuses the existing lifecycle machinery, selected by the **new
sandbox's** resolved `persistenceMode`:

- **`snapshot` mode** — `resolve` the source's `status.snapshotRef` + digests,
  stamp them onto the new sandbox's status, and drive the existing
  scheduler-placed restore flow (`Creating → Resuming → StageSnapshot →
  CreateRestorePod → Running`, epic #484). The `Stage` job reads the stamped
  `snapshotRef`, exactly as a normal resume does.
- **`filesystem` mode** — each PVC the new sandbox provisions is CSI-cloned from
  the source's correspondingly-named retained PVC via the PVC `spec.dataSource`
  (`PodAdapter::pvc_for`), then a fresh Pod cold-boots over the cloned volumes.
  The source must be `Suspended` (filesystem-paused) so its PVC is quiescent.

A new `Trigger::RestoreRequested` + the `(Creating, RestoreRequested) → (Resuming,
resume_action(mode))` state-machine edge routes both modes through one entry
point, mirroring the per-mode `(Paused, ResumeRequested)` resume selection.

### Compatibility gates (hard-fail → `Failed` + condition)

Before restoring, the controller validates the source against the new sandbox and
folds to `Failed` with an actionable condition on mismatch:

- **runtime class** must match (a gVisor snapshot is not restorable under Kata; a
  cloned PVC must mount under a compatible runtime).
- **template image** (snapshot mode only) must match — a memory-image restore
  reconnects the workload's code-page mappings, so a different image would restore
  corrupt. A filesystem clone (fresh cold boot over cloned data) tolerates an
  image change, so the image check is snapshot-only.
- **filesystem source must be `Suspended`** so its PVC is consistent to clone.

### Manifest fields (`SnapshotManifest`)

Added `template_ref` + `template_image` (`#[serde(default)]`, so legacy manifests
deserialize). For the sandbox-ID restore form the controller compares the source
CR's *resolved* template image directly, so these are not yet load-bearing; they
are recorded for the deferred `s3://` recovery form, where the source CR may be
gone and the manifest is the only template record.

### API surface (`POST /sandboxes`)

`CreateSandboxBody` gains an optional `restoreFrom` (also accepted as the
E2B-flavored `sourceSandboxID` alias), mapped verbatim onto `spec.restoreFrom`.

### CRDs

Regenerated: `sandboxes.sandbawks.dev.yaml` (new `restoreFrom`) and
`snapshotjobs.sandbawks.dev.yaml` (the embedded `SnapshotManifest` gained
`templateRef`/`templateImage`). **CRDs are not Helm-managed** — re-apply with
`kubectl apply -f deploy/crds` after deploy.

## Tests

- `RestoreSource::parse` classifier (doctest).
- State machine: `(Creating, RestoreRequested)` → `CreateRestorePod` (snapshot) /
  `CreatePod` (filesystem); added to the property-test trigger generator.
- `PodAdapter::pvc_for`: injects `dataSource` for a sandbox-ID restore; omits it
  for no-restore and for an `s3://` source.
- Reconcile integration (fake stores): snapshot clone resolves the source and
  enters `Resuming` with digests stamped; missing source → `Failed`; source with
  no snapshot → `Failed`; raw `s3://` → `Failed`; filesystem clone from a
  `Suspended` source enters `Resuming`; filesystem clone from a non-`Suspended`
  source → `Failed`.
- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` all pass.

## Known limitation: cloned guest identity (snapshot mode)

A gVisor `snapshot`-mode restore reconstructs the whole Sentry (guest kernel)
from the checkpoint, so a clone inherits the **source's** in-kernel identity:
`gethostname(2)` / `uname -n`, the netstack interface MAC + IP, and any captured
`/etc/machine-id` / boot-id return the *source's* values. The kubelet-managed
furniture binds (`/etc/hostname`, `/etc/hosts`, `/etc/resolv.conf`) are
re-materialized on the new Pod (they are not captured — see
`RUNTIME_MANAGED_BIND_DESTS` in the node gVisor adapter), so a clone has a split
between its kernel hostname (source) and `/etc/hostname` (destination).

This does **not** fail the restore — the resume path already restores into a
fresh Pod with a different IP/MAC, proving gVisor tolerates a restore-time
environment that differs from checkpoint time; the only hard restore gates are
the runsc-version (#545) and mount-set (#237) checks. For a normal same-sandbox
resume the Pod name equals the sandbox name, so the checkpointed hostname matches
the new Pod and the split is invisible. For a *clone* the destination is a
different sandbox, so a workload that keys on hostname / machine-id / MAC
(service registration, clustering, licensing) inherits the source's identity
instead of a unique one. v1 avoids live *collisions* because a clone source is
always quiescent (`Paused` / `Failed` for snapshot, `Suspended` for filesystem),
so two identical identities are never on the wire simultaneously. Reconciling
guest identity on clone is tracked in #643.

## Deferred (follow-ups, not in this PR)

- **Raw `s3://` URL restore form.** Requires threading an S3 manifest-fetch
  client into the reconcile hot path (today the controller's S3 client lives only
  in the retention sweeper). The sandbox-ID form covers the issue's motivating
  recovery case (the `Failed` source CR still exists), so the `s3://` form is
  recognized but folds to `Failed` directing the operator to pass the source
  sandbox ID. Pairs with node-side population of the manifest `templateRef`/
  `templateImage` fields.
- **E2B SDK clone/fork mapping** and the optional `POST /sandboxes/:id/fork`
  alias (open question in the issue; the create-body field is independent).
- **GC ref-counting** between a snapshot clone and its source S3 bundle, and
  **cross-namespace** restore (both explicitly out of scope in the issue).
- **Integration smoke** (`clone-from-snapshot-smoke.sh`) — needs a live cluster.
