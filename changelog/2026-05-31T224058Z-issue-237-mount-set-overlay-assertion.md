# Issue #237 — Mount-set assertion at pause: every writable mount must be overlay-backed

**Date:** 2026-05-31T22:40:58Z
**Branch:** `feat-issue-237-mount-set-overlay-assertion`
**Parent:** #232 (Plan 07 resume: memory-only `runsc restore`) — fail-closed hardening.

## Problem

Memory-only restore reconstructs the rootfs only because, under
`--overlay2=root:self`, the writable overlay upper lives in the sandbox
MemoryFile and therefore rides along in `pages.img`. A writable mount that
is *not* overlay-backed — a `bind`, a gofer-passthrough, or an
out-of-overlay `tmpfs` — is invisible to the memory checkpoint and would be
silently lost on restore. This is exactly the `--overlay2=none` data-loss
failure measured in #231. The property is knowable at pause time from the
container's OCI runtime spec, so it can be asserted before any checkpoint or
upload rather than surfacing as silent corruption at resume.

## Change

`crates/sandbawks-node/src/gvisor.rs`:

- **`SpecMount`** — a minimal projection of one OCI `mounts[]` entry
  (`destination`, `type`, `options`); deserialized straight from the spec
  JSON.
- **`parse_oci_mounts`** — pure parse of the OCI spec `mounts` array from
  the spec JSON bytes. Fail-closed on malformed JSON.
- **`mounts_lost_on_restore`** — pure classifier (mount list → offending
  mounts), mirroring the existing `upperdir_from_mounts` helper. A mount is
  flagged when it is writable (no `ro` option) and its state would *not*
  survive a memory-only restore. State is considered to survive for:
  - `overlay` — the root overlay upper rides in the MemoryFile (`pages.img`);
  - `tmpfs` — gVisor's tmpfs is sentry-internal and MemoryFile-backed (the
    *same* mechanism that makes the root-overlay restore work in #232), so it
    is captured in `pages.img` too;
  - kernel pseudo-filesystems (`proc`/`sysfs`/`cgroup`/`devpts`/`mqueue`/…) —
    stateless, rebuilt by the sentry on restore;
  - the kubelet/CRI rw bind *furniture* (`/etc/hosts`, `/etc/hostname`,
    `/etc/resolv.conf`, `/dev/termination-log`) — re-materialized by the
    kubelet on the fresh restored Pod.

  Everything else writable — a gofer-backed `bind` to a hostPath / PVC /
  default-medium `emptyDir`, or `9p`/`virtiofs`/`nfs` — passes writes through
  to the host, is invisible to the memory checkpoint, and is flagged.
- **`assert_writable_mounts_overlay_backed`** — thin wrapper that turns a
  non-empty offending set into `RuntimeError::CheckpointFailed`, naming every
  offending mount (destination + type), consistent with the
  checkpoint-postcondition pattern from #234.
- **`ContainerdHandle::container_mounts`** — new trait method. Production
  (`ContainerdClient`) sources the mounts from the container's stored OCI
  spec (`Container.spec`, the verbatim `config.json`), so no bundle hostPath
  is required. The `FakeContainerd` double returns a canned set, configured
  via the new `with_spec_mounts` builder.
- **`GvisorAdapter::pause`** — new step 1b: enumerate the container's mounts
  and assert before the checkpoint and before any upload. On failure the
  store stays empty and envd is never probed (fail-fast).

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — full workspace green (0 failures).

New tests:

- Unit (`gvisor::tests`): all-overlay set accepted; writable `bind` rejected
  and named; out-of-overlay `tmpfs` rejected; read-only bind accepted;
  pseudo-filesystems exempt even when writable; multiple offenders all named
  (and the safe mount not named); `parse_oci_mounts` extracts
  destination/type/options, tolerates a missing `mounts` array, and
  fail-closes on malformed JSON.
- Integration (`tests/gvisor.rs`):
  `pause_fails_closed_on_out_of_overlay_writable_mount` — a spec mixing the
  pseudo-fs, a read-only bind, and one writable bind fails the pause before
  upload; the store stays empty, envd is never probed, and a `runsc` guard
  stub confirms `runsc checkpoint` is never invoked.

## Smoke-driven refinement (vs. the issue's literal wording)

The issue framed the escape as "a bind / gofer-passthrough / **out-of-overlay
tmpfs**". A first cut that flagged every writable non-overlay mount failed the
kind smoke at `Pausing → Failed`: every real kubelet/CRI Pod carries a
writable `/dev` tmpfs and rw bind dotfiles, none of which is overlay-backed.

Two corrections, both grounded in gVisor semantics and confirmed against the
canonical `runsc spec` mount set:

1. **tmpfs is captured, not lost.** gVisor's tmpfs is sentry-internal and
   MemoryFile-backed — the identical mechanism that lets the root overlay
   survive a memory-only restore (the whole premise of #232). If tmpfs were
   lost, every existing pause (which always carries `/dev` tmpfs) would
   already be unrestorable, contradicting #232/#233's measured results. So
   tmpfs is treated as captured.
2. **Runtime furniture binds are reconstructed.** The kubelet re-materializes
   `/etc/hosts` & co. on the fresh restored Pod, so their sandbox-side copy
   is not workload state.

The net check is still strict and fail-closed for genuine escapes — a
writable hostPath / PVC / default-medium `emptyDir` is host-backed and flagged
— while no longer false-positiving on the furniture every Pod carries.

## Round 2 — PR #241 review + ground-truth verification

The first refinement still failed the kind smoke. Verified the real mount set
against a live gVisor Pod (`crictl inspect <cid> | jq '.info.runtimeSpec.mounts'`
on a local single-node kind+gVisor cluster). Result: the sole flagged mount was
**`/dev/shm`**, which containerd injects as a writable `bind` (pod-level POSIX
shared memory), *not* a `tmpfs`. Added `/dev/shm` to `RUNTIME_MANAGED_BIND_DESTS`
— it is ephemeral pod-scoped shm a restored Pod re-creates empty, never workload
state. With that, a real Pod's entire mount set passes and only genuine user
volumes (hostPath / PVC / default-medium emptyDir) are flagged.

Also addressed the review comments on the classifier:

- **`SpecMount.source`** now carried and included in the fail-closed error
  (`destination + type + source`) so an operator can see *which* host path an
  offending bind leaked.
- **`ContainerdClient::get_container`** factors the duplicated `Containers.Get`
  shared by `lookup_upperdir` and `container_mounts` (DRY; the two calls still
  occur at different points in the pause path, so this is dedup, not a shared
  round-trip).
- Documented the deliberate `["ro","rw"]` → read-only precedence in
  `mount_is_writable` (safe direction: fail-closed, never fail-open).
- Documented the resolve-pod-before-mount-set precedence in the `pause` step 1b
  comment.

CI diagnostics: the first `kind-smoke` job's failure dump only logged the
controller/api; added `ds/sandbawks-node` logs + SnapshotJobs so a pause-side
failure (which only surfaces in the node DaemonSet log) is diagnosable from CI
output without a local repro.

## Out of scope

Capturing non-overlay mounts (a separate fs-checkpoint path, explicitly
rejected in #231/#232). This item only *detects and fails*; it does not
extend coverage.
