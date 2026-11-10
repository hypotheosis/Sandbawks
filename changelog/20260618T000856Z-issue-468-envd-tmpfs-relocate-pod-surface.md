# Issue #468 — capture-in-checkpoint envd: pod_adapter tmpfs + relocate env + topology parity

**Date:** 2026-06-18T00:08:56Z
**Area:** controller / pod_adapter
**Parent epic:** #466 (make snapshots survive envd upgrades)
**Depends on:** #467 (envd-side relocate/re-exec logic, shares the `SANDBAWKS_ENVD_RELOCATE_TARGET` contract)

## Summary

Gives every sandbox Pod the captured-envd tmpfs surface and tells envd to
relocate onto it, so a cold-started sandbox runs envd from a sentry-internal
tmpfs whose pages `runsc checkpoint` walks into `pages.img`. Once #467 lands,
the relocated envd is captured in the snapshot and the snapshot survives a
later host-side envd upgrade (which would otherwise re-fault the hostPath-bound
seed binary's code pages on restore).

## Changes

`crates/sandbawks-controller/src/pod_adapter.rs`:

- **Tmpfs volume.** Added a `medium: Memory` emptyDir (`sandbawks-envd-run`)
  mounted **writable** at `/sandbawks/run` on the envd container, with a
  `sizeLimit` equal to the budgeted bytes. Mounted on **both** cold-start and
  restore Pods so the restore Pod's mount topology matches the checkpoint
  (mirrors the #237 invariant — the relocate step itself does not re-run on a
  restore Pod, but the mount set must still match).
- **Relocate env.** Set `SANDBAWKS_ENVD_RELOCATE_TARGET=/sandbawks/run/envd` on
  the envd container. It is a load-bearing internal contract, **not**
  customer-overridable: a `spec.env` pin of the same key is dropped so the
  controller's value always wins (no duplicate env entry). The container
  entrypoint is unchanged (`command: ["/sandbawks/bin/envd"]` — the staged
  seed binary).
- **Memory budget.** A `medium: Memory` emptyDir's pages count against the
  container memory cgroup, so the Pod's memory request **and** limit now carry
  the customer's request plus a fixed 64 MiB tmpfs budget
  (`ENVD_TMPFS_BUDGET_BYTES`), emitted as a bare byte count. The workload keeps
  its full requested headroom. Helpers `quantity_to_bytes` (IEC/SI suffix
  decode) and `memory_with_tmpfs_budget` (best-effort, falls back to the
  verbatim quantity on an unrecognized unit so it never reshapes a valid
  request into something the API server rejects).
- **Seed mount unchanged.** The existing read-only hostPath seed mount
  (`/sandbawks/bin`) is retained on both shapes. Updated the surrounding
  topology comment (it previously asserted "no MemoryFile-backed volume",
  which #468 deliberately supersedes).

## Tests

- `cold_start_pod_carries_envd_tmpfs_surface` / `restore_pod_carries_envd_tmpfs_surface`
  — both shapes carry the `medium: Memory` tmpfs + `/sandbawks/run` writable
  mount + relocate env; seed mount and entrypoint intact (shared
  `assert_envd_tmpfs_surface` helper holds the two shapes to identical topology).
- `envd_tmpfs_budget_added_to_memory` — request and limit both equal
  `customer + budget`.
- `quantity_to_bytes_decodes_known_units` / `memory_budget_falls_back_on_unparseable_unit`
  — unit decode and verbatim fallback.
- Updated two pre-#468 integration tests that encoded the now-superseded
  "no emptyDir / memory mirrors spec exactly" invariants
  (`envd_binary_mounted_read_only_from_node_hostpath`,
  `pod_resources_mirror_sandbox_spec`).

## Out of scope

- envd's relocate/re-exec behavior (#467).
- The manifest `envd_captured` flag and guard branch (epic phase 3).

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — full workspace green
