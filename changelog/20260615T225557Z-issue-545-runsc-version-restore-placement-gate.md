# Issue #545 — Capability B: gate restore placement on a runsc-version match

**Date:** 2026-06-15T22:55:57Z
**Branch:** `feat-issue-545-runsc-version-restore-gate`
**Issue:** #545 (epic) — Capability B only

## Context

gVisor checkpoint/restore requires the **exact same** `runsc` build on the
pausing and restoring node. A drifted fleet (some nodes on `release-20260608.0`,
others on the pinned `release-20260525.0`) made the scheduler place a restore Pod
onto a mismatched node, where `runsc restore` failed fatally:

```
OCI runtime restore failed: ... runsc version does not match across checkpoint
restore, checkpoint: release-20260608.0  current: release-20260525.0
```

— a `FailedCreatePodSandBox` crash-loop.

Capability A (per-node `sandbawks.dev/runsc-version` label + drift metric) and the
restart-free part of Capability C landed earlier via **#546** (merged in #549).
This change implements **Capability B**: turn the fatal mismatch into a bounded,
diagnosable `Pending` scheduling decision. The privileged form of Capability C
(`nsenter` + k3s restart, cordon/drain rails) remains a separate follow-up, as
the epic and issue comment #14692 already scoped.

## What changed

The runsc version travels the same path the envd digest (#270/#278) already does:
**pause node → snapshot manifest → controller → `Sandbox.status` → restore Pod
`nodeAffinity`.**

1. **`SnapshotManifest.runsc_version`** (`sandbawks-types/src/runtime.rs`) — new
   `#[serde(default)]` field carrying the `release-<ver>` token the checkpoint was
   taken against. Empty for v1 manifests / unreadable runsc.

2. **Capture at pause** (`sandbawks-node/src/gvisor.rs`) — `GvisorAdapter::pause`
   shells `runsc --version` (new `read_runsc_release` helper, reusing
   `runsc_reconcile::parse_runsc_release`) after checkpoint and stamps the
   manifest. Best-effort: an unreadable version records an empty string and the
   restore gate degrades to envd-staged affinity only, never failing the pause.
   The version is read from the **host** runsc binary (new
   `host_runsc_binary` field / `with_host_runsc_binary`, wired in
   `sandbawks-node` to the same `/host/usr/local/bin/runsc` mount Capability A's
   reconciler labels the node from) — **not** the image-bundled `runsc_binary`
   used for `runsc checkpoint`. On a `#546 BundledMismatch` node the two differ;
   recording the bundled version would emit a gate matching no host-derived node
   label and strand every restore `Pending`.

3. **`SandboxStatus.snapshot_runsc_version`** (`sandbawks-types/src/sandbox.rs`) —
   new `Option<String>` status field.

4. **Controller folding** (`sandbawks-controller/src/reconcile.rs`) — on a Pause
   `SnapshotJob` completion, fold `manifest.runsc_version` into
   `status.snapshotRunscVersion`, mirroring `snapshot_envd_sha256`.

5. **Restore-placement gate** (`sandbawks-controller/src/pod_adapter.rs`) —
   `pod_spec_for` appends a second `NodeSelectorRequirement` to the existing #272
   envd-staged term: an `In` match on `RUNSC_VERSION_LABEL` against the recorded
   version. Gated on `restore.is_some()` **and** a non-empty recorded version, so:
   - cold starts stay schedulable fleet-wide (no checkpoint to match), and
   - pre-#545 snapshots (no recorded version) degrade to envd-staged-only rather
     than pinning to an empty label value (which would strand every restore).

## Tests

- `pod_adapter`: `restore_pod_gates_on_recorded_runsc_version`,
  `cold_start_pod_has_no_runsc_version_gate`,
  `restore_pod_without_recorded_version_degrades_to_envd_only` (None + empty).
- `gvisor`: checkpoint stub now answers `--version`; `pause_emits_three_parts_and_manifest`
  asserts the captured `runsc_version`.
- `runtime_trait`/`reconcile` manifests carry a real version to exercise serde +
  folding.

## Operational note

`SandboxStatus` and the SnapshotJob manifest schema changed, so
`deploy/crds/sandboxes.sandbawks.dev.yaml` and
`deploy/crds/snapshotjobs.sandbawks.dev.yaml` were regenerated
(`cargo run -p sandbawks-types --bin crdgen -- deploy/crds`). **Helm does not
deploy CRDs** — run `kubectl apply -f deploy/crds` after merge, otherwise the new
status field is silently pruned and the gate never engages.

## Out of scope (unchanged from the epic)

- Privileged Capability C (`nsenter` host runsc upgrade + k3s restart).
- Cross-version checkpoint migration (gVisor does not support it).
