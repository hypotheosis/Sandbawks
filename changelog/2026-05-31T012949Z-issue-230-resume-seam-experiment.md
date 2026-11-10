# Plan 07 resume: closing the fs-checkpoint seam — local gVisor experiment + A/B decision

- **Issue:** #230 (epic #204, Plan 07). Builds on the root-cause doc
  `changelog/2026-05-31T011404Z-issue-215-resume-fs-checkpoint-seam.md`.
- **Date:** 2026-05-31T01:29:49Z
- **Status:** experiment complete; **recommendation made**. No production code
  changed in this PR — it delivers the harness, the measurements, and the
  decision so the follow-up fix can be scoped tightly.

## Question this answers

#230 framed the seam as a deliberate fork:

- **Option A** — pause *also* runs `runsc fscheckpoint`; keep the shim's
  `CreateWithFSRestore` / `--fs-restore-image-path` design.
- **Option B** — drop `CreateWithFSRestore`; apply `rootfs-delta.tar.zst` to the
  restore Pod's snapshotter upperdir, shim does a memory-only `Restore`.

The issue explicitly asked to validate the round-trips in the standalone gVisor
harness *before* writing node/controller code. This is that validation.

## Harness

Standalone `runsc` (no kind, no VM — the dev container can't nest netns, but
`runsc` runs directly under `sudo`). Everything matches production pins:

- `runsc` release **`20260525.0`**, x86_64 (the `GVISOR_RELEASE` baked into
  `ci/kind-node/Dockerfile` and `crates/sandbawks-node/Dockerfile`). SHA-512
  verified against the published `runsc.sha512`.
- Overlay mode **`--overlay2=root:self`** — gVisor's default and, per
  `crates/sandbawks-types/src/snapshot.rs` and `gvisor.rs:1316`, what sandbawks
  runs. (This matters — see "Why overlay medium is the crux" below.)
- Flags: `--ignore-cgroups --network=none --platform=systrap
  --overlay2=root:self`. A no-network single-container busybox bundle, so the
  kind netns limitation is irrelevant.

Two harness bugs were fixed mid-investigation and are worth recording because
they are easy to hit again:

1. **`fscheckpoint` and `checkpoint` collide in one image dir.** Both emit
   `pages.img` and `pages_meta.img`; pointing both subcommands at the same
   `--image-path` makes the second fail with `... pages_meta.img: file exists`,
   silently leaving a 0-byte `checkpoint.img` that then fails restore with
   `EOF`. They need **separate** directories.
2. **The container's sandbox-root can't write `/`.** The host rootfs dir is
   owned by the unprivileged dev user (uid 1000); gVisor's gofer surfaces that
   owner, so sandbox-uid-0 has no write access and the sentinel write fails with
   `Permission denied`. `chown -R 0:0 rootfs` fixes it. (In production the
   kubelet/containerd snapshotter owns the rootfs as root, so this is a
   harness-only artifact — but it masked the real result for two runs.)

## Measurements

### Finding 1 — memory-only `runsc restore` fully recovers rootfs writes (decisive)

Under `--overlay2=root:self`:

1. `runsc run` a busybox that writes `/sentinel.txt`, then `runsc exec` a
   **unique marker** (`MARKER-<uuid>`) into `/marker.txt` of the *live*
   container — written by `exec`, **never** by the entrypoint.
2. `runsc checkpoint --image-path MEMCK -leave-running` → `checkpoint.img` +
   `pages.img` + `pages_meta.img` (the exact three files the node's
   `tar_zstd_checkpoint_files` already bundles into `memory.zst`).
3. `runsc create --bundle <clean copy, marker.txt removed>` (no
   `--fs-restore-image-path`) + `runsc restore -image-path MEMCK` (no
   `-fs-restore-image-path`).
4. `runsc exec dstB cat /marker.txt` → **`MARKER-<uuid>` returns.**

Because the marker was `exec`-written (not re-creatable by the restored
process, which resumes mid-`sleep` rather than re-running its entrypoint) and
the destination bundle's rootfs had no `marker.txt`, the only possible source is
`pages.img`. **The workload's rootfs writes are inside the memory checkpoint and
a plain memory `runsc restore` reconstructs them.** This confirms sandbawks'
own documented invariant (`snapshot.rs`: "the default `--overlay2=root:self`
keeps all rootfs writes in the sandbox-internal overlay-tmpfs, which is captured
by `memory.zst`").

### Finding 2 — `fscheckpoint` works, but layering it on a memory restore breaks restore

With clean, separate image dirs:

- `runsc fscheckpoint -image-path FSCK -leave-running` succeeds and **does**
  capture the filesystem: `multitar.img` contains `./sentinel.txt`, and
  `fscheckpoint.json` lists a `tmpfs` resource at `/`. So Option A is
  *implementable* — pause could produce a valid `fscheckpoint.json`.
- But the shim's actual sequence — `runsc create --fs-restore-image-path FSCK`
  **then** `runsc restore -image-path MEMCK -fs-restore-image-path FSCK` — fails
  every time with `urpc method "containerManager.Restore" failed: EOF` (the
  sandbox process dies; the host-side log shows only the urpc EOF).

The reason is structural: under `root:self` the overlay-tmpfs upper is present
in **both** `pages.img` (memory) **and** the `fscheckpoint` `multitar.img`
(filesystem). Restoring the same `/` filesystem from two sources at once is the
conflict. Making Option A actually round-trip would require *also* excluding the
overlay-tmpfs from the memory checkpoint so the two halves stop overlapping —
a far deeper, gVisor-level change than "add an `fscheckpoint` pass to pause."

### Finding 3 — `rootfs-delta.tar.zst` is empty for this model

`snapshot.rs` already documents it ("for gVisor this is typically empty"), and
the experiment is consistent: with `root:self`, session writes live in the
overlay-tmpfs (→ `pages.img`), not in containerd's snapshotter upperdir that the
Diff API reads. So Option B's premise — *apply the `rootfs-delta` to the restore
Pod's upperdir* — rests on a tar that carries nothing for the in-memory upper.

## Why overlay medium is the crux

`runsc`'s `--overlay2={mount}:{medium}` decides **where rootfs writes live**, and
therefore **which checkpoint subcommand captures them**:

| `--overlay2` medium        | rootfs writes land in       | recovered by memory `restore`?            |
|----------------------------|-----------------------------|-------------------------------------------|
| `root:self` (sandbawks)    | sandbox MemoryFile          | **yes** — in `pages.img` (measured)       |
| `all:memory`               | sandbox MemoryFile          | **yes** — in `pages.img` (measured)       |
| `none` (gofer passthrough) | host gofer-backed rootfs fs | **no** — never enters `pages.img` (measured) |

The three rows are all measured in this harness, and they are the whole point:

- With an **overlay** (`root:self` *or* `all:memory`) the upper is in the sentry
  MemoryFile, so `runsc checkpoint` captures it into `pages.img` and the memory
  `restore` reconstructs it on a fresh pod (the marker test, Finding 1).
  `fscheckpoint` is therefore *redundant* here — and for `all:memory` it has
  nothing to grab: it reports **`no checkpointable filesystems`** and emits empty
  files (measured directly).
- With **`--overlay2=none`** the writes bypass the sentry entirely and land on
  the gofer-backed rootfs on disk; `pages.img` never sees them, so a memory-only
  restore into a clean rootfs **loses** them — a marker written under `none` did
  not survive (measured). *This* — a configuration whose rootfs state lives
  outside the MemoryFile, so the memory checkpoint cannot carry it to the restore
  pod — is the shape gVisor's `--fs-restore-image-path` is built to transport.
  Sandbawks does not use it: `root:self` keeps the upper in the MemoryFile, which
  `memory.zst` already moves to the restore pod.

(Corollary worth recording: sandbawks must keep an overlay medium — switching to
`--overlay2=none` would silently break resume, since the rootfs would no longer
ride along in the memory checkpoint.)

## Recommendation — Option B, simplified (remove the fs-restore graft)

**Resume should do a plain memory `runsc restore`. Delete the shim's
`CreateWithFSRestore` / `--fs-restore-image-path` path entirely.**

This is Option B's "drop `CreateWithFSRestore`, memory-only `Restore`" half —
and the experiment shows Option B's *other* half (apply `rootfs-delta` to the
upperdir) is a **no-op** for the `root:self` overlay-tmpfs model and can be
dropped too. Net effect:

- **Pause:** unchanged. `runsc checkpoint` + `memory.zst` already carries the
  rootfs. No `fscheckpoint` pass needed (Option A rejected — redundant *and*
  breaks restore).
- **Shim Create:** stop annotating for `CreateWithFSRestore`; let Create build
  the sandbox normally, then `Restore` from the staged `*.img`. Removes the
  `fscheckpoint.json` dependency that is #230's literal failure.
- **`rootfs-delta.tar.zst`:** keep producing/staging it for now (cheap, and it
  is the only carrier for any *committed lower-layer* writes), but resume need
  not apply it for the overlay-tmpfs case. Revisit once the large/overflowing
  overlay-upper open question (first item under "Open questions" below) is
  settled.

Why not A: it is redundant with `pages.img`, fails to compose with the memory
restore (Finding 2), and would force a deeper change to strip the overlay-tmpfs
from the memory checkpoint. Why not B-as-written: its upperdir-apply step is a
no-op here (Finding 3); doing the memory restore alone is strictly simpler.

## Open questions for the follow-up (don't pretend these are closed)

- **Large/overflowing overlay upper.** `root:self` uses a *file-backed*
  filestore in the rootfs dir; a workload that writes enough to spill the upper
  past memory could put bytes on the gofer file rather than purely in the
  MemoryFile. The marker test used a tiny write. Before declaring memory-only
  restore sufficient, a follow-up must checkpoint/restore a workload that writes
  hundreds of MiB and confirm the data survives — or characterize where it goes.
- **Multi-container pod sandbox + envd.** The harness is single-container,
  no-network. The shim's #229 root-vs-subcontainer restore guard and the real
  envd/TCP-drain path are *not* exercised here. Memory-only restore of a true
  pod sandbox (pause container + envd + workload) must be validated in the kind
  smoke (`PAUSE_RESUME_SMOKE_FULL=1`).
- **gVisor's exact EOF cause** for `restore -fs-restore-image-path` over a
  memory checkpoint was not root-caused at the sandbox level (only the host-side
  urpc EOF is visible). Not needed for the recommendation, but noted so nobody
  re-derives it expecting Option A to "just need the right flags."

## Reproduce

```bash
WORK=/tmp/gvisor-harness; mkdir -p "$WORK/bin"; cd "$WORK"
curl -fsSLO https://storage.googleapis.com/gvisor/releases/release/20260525.0/x86_64/runsc
curl -fsSLO https://storage.googleapis.com/gvisor/releases/release/20260525.0/x86_64/runsc.sha512
chmod +x runsc && mv runsc bin/
sha512sum -c <(sed "s| .*| bin/runsc|" runsc.sha512)   # binary is in bin/ now
# busybox rootfs + `runsc spec`; set process.args to write /sentinel.txt then idle;
# chown -R 0:0 rootfs. Then, as root, with separate image dirs:
F="--ignore-cgroups --network=none --platform=systrap --overlay2=root:self --root $WORK/rt"
sudo bin/runsc $F run -detach --bundle bundle srcA
sudo bin/runsc $F exec srcA sh -c 'echo MARKER > /marker.txt'
sudo bin/runsc $F checkpoint -image-path mem -leave-running srcA
# memory-only restore into a CLEAN bundle copy (marker.txt removed):
sudo bin/runsc $F create --bundle bundle_clean dstB
sudo bin/runsc $F restore -image-path mem -detach dstB
sudo bin/runsc $F exec dstB cat /marker.txt        # -> MARKER  (survives)
```
