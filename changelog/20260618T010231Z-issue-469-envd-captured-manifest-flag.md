# Issue #469 — capture-in-checkpoint envd: manifest `envd_captured` flag + pause/restore guard branch (and a #468 correction)

**Date:** 2026-06-18T01:02:31Z
**Area:** types / node / controller
**Parent epic:** #466 (make snapshots survive envd upgrades)
**Depends on:** #467 (envd relocate/re-exec) and #468 (pod relocate surface)

## Summary

Retires the #270 host-binary determinism guard for *capture-mode* snapshots
while keeping it for *legacy* ones, via a new self-describing manifest field,
`envd_captured`. While wiring this up, the pause/resume CI smoke surfaced that
#468's relocate surface was architecturally wrong, so this change also corrects
it.

## The #468 correction (root cause of the smoke failure)

#468 mounted a `medium: Memory` emptyDir at `/sandbawks/run` and asserted it was
"a sentry-internal tmpfs that runsc checkpoint walks into pages.img." That is
false. Under the cluster's `--overlay2=root:self` (confirmed in
`shim/internal/wrapper/service.go`), only the **root** filesystem is the
captured overlay-tmpfs. A kubelet `medium: Memory` emptyDir is presented to the
sandbox as a **gofer-backed `bind`** (the CI error showed exactly this:
`type=bind, source=…/kubernetes.io~empty-dir/sandbawks-envd-run`), so it is:

- **not** captured in `pages.img` (it would re-fault from the host on restore —
  the very upgrade-survival failure the epic exists to fix), and
- a writable non-overlay-backed mount that trips the node's #237 mount-set gate,
  **failing every pause closed**.

**Fix:** the relocate target (`/sandbawks/run/envd`) is now a plain directory in
the *workload rootfs* — no separate volume. Under `root:self` that lives in the
captured overlay-tmpfs, so envd's relocated code pages ride in `pages.img`
(achieving the epic's goal) and there is no extra mount to violate #237. envd
`mkdir -p`s the parent itself, exactly as it already does for its `--state-file`
under `/var/tmp/envd`.

## Changes

`crates/sandbawks-types`:

- `SnapshotManifest::envd_captured: bool` with `#[serde(default)]` (v1/legacy →
  `false`, fail-closed). Surfaces in the `SnapshotJob` CRD status →
  regenerated `deploy/crds/snapshotjobs.sandbawks.dev.yaml`.
- Relocate-contract constants (`ENVD_RUN_DIR`, `ENVD_RELOCATE_TARGET`,
  `ENVD_RELOCATE_TARGET_ENV`) promoted to `runtime` and re-exported as one
  cross-crate source of truth.

`crates/sandbawks-controller/src/pod_adapter.rs` (#468 correction):

- Dropped the `sandbawks-envd-run` `medium: Memory` emptyDir volume + its
  `/sandbawks/run` mount. The only Pod volume/mount is now the read-only envd
  seed. Renamed the memory-budget const/fn to `ENVD_RELOCATE_BUDGET_BYTES` /
  `memory_with_relocate_budget` (the 64 MiB still covers the relocated copy,
  which now lives in the overlay-tmpfs). Updated comments + tests.

`crates/sandbawks-node/src/gvisor.rs`:

- **Pause** stamps `manifest.envd_captured` from the container's OCI
  `process.env`: capture-mode iff `SANDBAWKS_ENVD_RELOCATE_TARGET` is set (new
  `ContainerdHandle::container_relocate_target` + pure `parse_oci_relocate_target`).
  A missing/unparseable spec degrades to `false` (legacy) — the safe direction.
  (Mount-set detection was the first attempt; it's impossible now that there's no
  mount.) The host-staged envd sha is still recorded for diagnostics but is no
  longer load-bearing under relocation.
- **Restore** (`verify_envd_digest`) short-circuits `Ok(())` when
  `envd_captured`, recording a new `skipped` / `envd_captured` metric exit;
  legacy/absent snapshots keep the unchanged fail-closed comparison.

## Tests

- types: v1 deserialize defaults `envd_captured` to `false`; camelCase `true`
  round-trips; S3 manifest round-trip carries `true`.
- controller: cold-start/restore Pods carry the relocate env but **no** emptyDir
  and **no** `/sandbawks/run` mount (only the read-only seed); relocate budget
  still added to memory request+limit.
- node: pause marks capture-mode iff the relocate env is present;
  `parse_oci_relocate_target` reads/absent/malformed cases; capture-mode restore
  succeeds despite a drifted host envd binary (legacy mirror of
  `stage_fails_closed_on_drifted_envd_binary`); metrics exit coverage.

## Out of scope / follow-up

- The live gVisor end-to-end proof that the rootfs-overlay relocate is captured
  (epic phase 4 / next issue) — this change makes the surface *correct in
  principle* and unblocks the smoke; the e2e capture proof is still owed.
- A #278-style storage-integrity commit for `envd_captured` (restore reads the
  re-fetched authoritative manifest; per-part + aggregate sha256 gates apply).
- Migration edge: a legacy snapshot restored onto a relocate-env Pod and
  re-paused is bounded by snapshot drain and protected by per-part sha256.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — green (one pre-existing port/timing flake in the untouched
  `envd` crate, `quiesce_drains_grpc_connections_and_rebinds`, passes in
  isolation and intermittently under load; unrelated to this change)
