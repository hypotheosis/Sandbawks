# Capture-in-checkpoint envd: upgrade-proof snapshots

**Status:** Approved (design)
**Date:** 2026-06-10
**Author:** Peter Grace (with Claude)
**Relates to:** #265 (node-staging injection), #270 (digest-pin guard, closed), #237 (mounts-lost-on-restore invariant), #232 (memory-only restore), Plan 07 (resume seam)

## Problem

Resuming a paused sandbox fails closed whenever the node-staged `envd`
binary has changed since the snapshot was taken. The triggering incident:
`sbx-warm-v4vpc` was checkpointed against envd sha `6734f033…`; a routine
Helm redeploy to commit `32cd988` re-staged a new envd (`3a63f88…`) on every
node; the resume hit the `#270` guard
([`verify_envd_digest`](../../../crates/sandbawks-node/src/gvisor.rs)) and
went straight to `Failed` with `ActionFailure`.

### Root cause

Under node-staging (#265) the sandbox workload **execs envd mounted
read-only from the host** at `/sandbawks/bin/envd` (hostPath
`/var/lib/sandbawks/envd/envd`). Its executable pages are `MAP_PRIVATE`,
file-backed by that gofer-backed host file. gVisor's memory checkpoint does
**not** save clean file-backed code pages — on `runsc restore` they fault
back in *from whatever file is on disk at restore time*. A different on-disk
envd → the restored process's code pages become a different binary's
instructions → silent memory corruption. The `#270` guard correctly converts
that into a clean failure, but the failure is unavoidable because the node
keeps **exactly one** envd version at a fixed path, atomically overwritten on
every DaemonSet roll. The binary the snapshot needs is simply gone.

### Blast radius

Only an **envd binary change** invalidates a snapshot — the controller, node
agent, and api are never mmap'd into the checkpointed workload, so upgrading
them leaves snapshots restorable. But because every redeploy rebuilds envd
(and a non-reproducible rebuild yields a new content sha even from identical
source unless pinned by digest), in practice **any deploy that bumps the envd
image invalidates every in-flight snapshot.** As envd is actively iterated,
this silently kills a large fraction of paused/warm-pool sandboxes on every
release. That is the disaster this design removes.

`#270` (now closed) only ever addressed the *spurious* flavor — pinning a
floating tag so unrelated redeploys don't churn the sha. It does nothing for
a genuine envd code change.

## Approach

Make the workload exec envd from a **checkpoint-captured filesystem** instead
of the gofer-backed hostPath bind, so envd's code pages live inside the
sandbox's MemoryFile and are saved in `pages.img`. On a memory-only restore
gVisor rehydrates those pages from the snapshot — the host binary is never
consulted, so it can churn freely across deploys without invalidating
anything.

This is grounded in what the node code already asserts about capturable
mounts
([`mount_state_survives_restore`](../../../crates/sandbawks-node/src/gvisor.rs)):

> a `tmpfs` mount "is sentry-internal and MemoryFile-backed — … its contents
> are saved in `pages.img`. A `medium: Memory` emptyDir is a tmpfs and is
> likewise captured; a *default-medium* emptyDir is a host-backed `bind` and
> is caught below."

So a `medium: Memory` emptyDir is exactly the captured surface we need. The
gofer-backed hostPath bind envd currently execs from is exactly the
*non*-captured surface causing the bug.

### Alternatives considered

- **Content-addressed envd store (rejected).** Keep envd out of the
  checkpoint (preserve #237), store each binary content-addressed by sha
  (e.g. `s3://bucket/envd/<sha>`), have the node keep a multi-version cache
  and fetch the snapshot's sha on demand at restore, and mount the matching
  sha per restore pod. Works, but *adds* machinery (binary upload, node
  cache + GC, per-restore mount juggling, a scheduling/availability story)
  where the capture approach *deletes* machinery. Rejected in favor of the
  simpler design.
- **Bake envd into the customer template (rejected outright).** Forces users
  to vendor our binary; a non-starter.

### Why this wins

It removes code rather than adding it: the `#270` `verify_envd_digest`
fail-closed guard becomes meaningless for new snapshots, there is no
content-addressed object store, no node-side multi-version cache, no GC, and
no per-restore mount selection. Users never touch their templates — envd is
still node-injected.

## Design

### 1. Pod shape (controller / `pod_adapter`)

Node injection is **unchanged**: the DaemonSet still stages the host binary
at `/var/lib/sandbawks/envd/envd`; the pod still mounts it read-only at
`/sandbawks/bin/envd`; `stage-envd` and the Helm chart are untouched.

`pod_adapter` adds, to the envd container:

- a `medium: Memory` emptyDir volume mounted at `/sandbawks/run` (the
  captured tmpfs surface);
- env var `SANDBAWKS_ENVD_RELOCATE_TARGET=/sandbawks/run/envd`.

The entrypoint stays `command: ["/sandbawks/bin/envd"]` (envd.rs's serve
path, `cli.command == None`).

Both the tmpfs volume and the hostPath seed mount are present on **both**
cold-start and restore pods, so the restore pod's mount topology matches the
checkpoint (gVisor validates mount config on restore; this mirrors the
existing #237 topology invariant). The relocate step (below) does **not**
re-run on a restore pod — a memory restore replaces the container process
with the checkpointed one rather than re-execing the entrypoint.

### 2. envd self-relocate (the only envd change)

At the top of `main()`'s server path (after the one-shot subcommand check,
`crates/envd/src/bin/envd.rs:368`):

1. If `SANDBAWKS_ENVD_RELOCATE_TARGET` is **unset** → serve in place
   (tests, cold/legacy paths). 
2. Else read `std::env::current_exe()`:
   - if it already equals the target → this is the re-exec'd process; **serve
     normally**;
   - otherwise copy self to the target via the existing
     `install_from(SELF_EXE, target)` (atomic temp-file + `rename`,
     byte-identical, preserves `0755`), then `execve(target, argv, envp)`,
     replacing the process image.

argv/envp are preserved across the `execve`, so every flag carries through.
No shell, no `/usr/bin/cp`, no customer-image dependency — this reuses copy
logic that already exists and is unit-tested
(`install_from_produces_byte_identical_executable_copy`).

After relocation the serving envd's code pages are mmap'd from the tmpfs file
→ resident in MemoryFile → saved by `runsc checkpoint` → rehydrated from
`pages.img` on restore.

**Failure mode (decision):** if `RELOCATE_TARGET` is set but the copy or
`execve` fails, envd exits non-zero so the pod surfaces the error. It does
**not** fall back to serving from the hostPath mount — a silently
non-restorable sandbox is exactly the disaster being fixed.

### 3. Retire the `#270` guard — capture-mode only (decision)

Under capture-mode the host binary *legitimately* differs from the snapshot's
envd, so the existing fail-closed comparison in `verify_envd_digest`
(`crates/sandbawks-node/src/gvisor.rs`) would now **reject exactly the
cross-version restores this design enables.** It must be neutralized for
capture-mode snapshots — but pre-existing (pre-change) snapshots still have
host-backed envd pages and still need the old guard for safety.

Resolution — add a capture marker to the snapshot manifest:

- Extend `SnapshotManifest` with `envd_captured: bool` (serde default
  `false`, so v1/legacy manifests deserialize as legacy).
- **Pause** sets `envd_captured = true` when the sandbox was launched in
  relocate mode (the controller knows it injected the relocate env / tmpfs
  volume; thread that through the SnapshotJob the way other restore metadata
  is threaded). Pause also stops treating the host-binary sha as
  load-bearing — under relocation the host binary may already differ from the
  running (tmpfs) envd, so hashing it at pause was already meaningless.
- **Restore** (`verify_envd_digest`) branches:
  - `envd_captured == true` → skip the host-binary comparison (host is
    irrelevant; the per-part sha256 gate still protects the snapshot bytes);
  - `false`/absent → keep today's fail-closed comparison so legacy snapshots
    drain safely.

`manifest.envd_sha256` may remain as an informational field but is no longer
load-bearing for capture-mode restores.

### 4. Memory accounting (note)

A `medium: Memory` emptyDir is charged to the pod's memory cgroup, and under
gVisor the tmpfs lives in the sandbox MemoryFile. The tmpfs file and its
mmap'd code pages share memory (one resident copy of envd, ~tens of MB), but
the sandbox `resources.memory` budget must account for it so it doesn't eat
workload headroom. Verify the relocated checkpoint size delta against the
existing headroom gate (`enforce_headroom`).

## Components touched

| Component | Change |
|---|---|
| `crates/envd/src/bin/envd.rs` | self-relocate + re-exec hook in `main()`; reuse `install_from`/`SELF_EXE` |
| `crates/sandbawks-controller/src/pod_adapter.rs` | `medium: Memory` tmpfs volume + mount; `SANDBAWKS_ENVD_RELOCATE_TARGET` env; topology parity cold-start/restore |
| `crates/sandbawks-types` (snapshot manifest) | add `envd_captured: bool` (serde default false) |
| `crates/sandbawks-node/src/gvisor.rs` | branch `verify_envd_digest` on `envd_captured`; pause sets the flag, drops host-sha as load-bearing |
| tests | envd relocate unit tests; pod_adapter spec tests; **live gVisor capture/restore acceptance** |
| Helm / DaemonSet | **none** (host staging unchanged) |

## Testing

- **Unit (envd):** relocate decision table — unset target serves in place;
  `current_exe == target` serves; `current_exe != target` copies + re-execs;
  copy/execve failure exits non-zero. Reuse existing `install_from` byte-identity tests.
- **Unit (pod_adapter):** rendered pod spec carries the tmpfs volume + mount +
  env on both cold-start and restore pods; entrypoint unchanged.
- **Unit (manifest):** `envd_captured` serde default `false` for v1 manifests;
  round-trips for `true`.
- **Unit (node):** `verify_envd_digest` skips the host comparison when
  `envd_captured`, keeps fail-closed when legacy.
- **Acceptance (live gVisor — make-or-break):** cold-start a sandbox, let
  envd relocate to tmpfs, `runsc checkpoint`, **delete/replace the host
  binary**, `runsc restore`, assert the sandbox serves and reports the *old*
  envd version. Proves exec'd-from-tmpfs code pages survive and the host
  binary is irrelevant. Runnable on the existing kind+gVisor smoke or
  standalone `sudo runsc`.

## Out of scope / accepted losses

- `sbx-warm-v4vpc` and its `job_id 33998b9f…` snapshot stay lost — its
  checkpoint is bound to an envd binary no longer on any node. Accepted.
- Legacy (pre-change) snapshots remain host-binary-sensitive and are protected
  by the retained legacy guard branch; they age out naturally.

## Rollout

Backward compatible. New snapshots are capture-mode and upgrade-proof;
in-flight legacy snapshots keep the old fail-closed protection until they
drain. No data migration. Land behind the relocate env var so the behavior is
inert until `pod_adapter` sets it.
