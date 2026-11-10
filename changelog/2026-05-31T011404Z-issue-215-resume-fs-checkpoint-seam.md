# Plan 07 resume: pause/resume filesystem-checkpoint seam (root cause)

- **Issue:** #215 (epic #204, Plan 07) — surfaced by PR #229 flipping
  `PAUSE_RESUME_SMOKE_FULL=1`.
- **Date:** 2026-05-31T01:14:04Z
- **Status:** root cause identified by a local gVisor experiment; resolution
  is an epic-level design decision (deliberately **not** fixed in #229).

## TL;DR

The shim restores a sandbox's filesystem with gVisor's **`CreateWithFSRestore`**
(`runsc create --fs-restore-image-path=<dir>`), which **requires a
`fscheckpoint.json`** produced by gVisor's separate **`runsc fscheckpoint`**
subcommand. The node's *pause* never runs `runsc fscheckpoint` — it runs
`runsc checkpoint` (memory/sentry → `*.img`) and captures the filesystem
*separately* as a containerd-snapshotter `rootfs-delta.tar.zst`. So the staged
restore directory is **missing `fscheckpoint.json`**, and resume fails with:

```
OCI runtime create failed: creating container: cannot create sandbox:
failed to open filesystem checkpoint files: opening manifest file
".../fscheckpoint.json": no such file or directory
```

Plan 07 grafted a shim that restores the filesystem via gVisor's native
fs-checkpoint onto a snapshot format (Plan 05/06) that captures the filesystem
as a snapshotter tar delta. **The two halves use different, unreconciled
filesystem mechanisms.** That seam is the architectural root cause beneath the
cascade of surface errors the `FULL=1` smoke exposed.

## How this was determined (local gVisor experiment)

The CI dev host can't run the full kind+gVisor smoke (kernel 6.19 + kind
inside Docker-in-Docker → `cannot run with network enabled in root network
namespace`). But gVisor itself runs fine **standalone** in this container:

- `runsc` release `20260525.0` (matching the bumped Dockerfiles) runs with
  `--rootless --ignore-cgroups --network=none` on the systrap/kvm platforms.
- `runsc create`/`restore` are **rejected in rootless mode** ("Rootless mode
  not supported"), so the restore half needs real root — available here via
  passwordless `sudo` (no VM required after all).

Experiment (as root, `--ignore-cgroups --network=none --platform=systrap
--overlay2=root:self`, no-network busybox bundle so the kind netns issue is
irrelevant):

1. `runsc create A` + `start A` + write a sentinel.
2. `runsc checkpoint --image-path /tmp/ckpt A` → produces **exactly**
   `checkpoint.img` + `pages.img` + `pages_meta.img`, **no `fscheckpoint.json`**.
   (This matches `crates/sandbawks-node/src/gvisor.rs`'s `tar_zstd_checkpoint_files`
   and what `stage_snapshot` unpacks — the node's staged `*.img` layout is a
   faithful `runsc checkpoint` dir.)
3. `runsc create --bundle … --fs-restore-image-path /tmp/ckpt B` →
   `failed to open … fscheckpoint.json: no such file or directory`.
4. `runsc --help` confirms two separate subcommands:
   - `checkpoint` — *checkpoint current state of container* (memory; what pause uses)
   - `fscheckpoint` — *checkpoint container filesystems* (the fs image the shim's
     `--fs-restore-image-path` consumes; **never run by sandbawks**)

### Reproducing

```bash
curl -fsSLO https://storage.googleapis.com/gvisor/releases/release/20260525.0/x86_64/runsc
chmod +x runsc
# standalone, no-network bundle (busybox rootfs + `runsc spec`), then:
sudo ./runsc --ignore-cgroups --network=none --platform=systrap --overlay2=root:self \
     --root /tmp/r checkpoint --image-path /tmp/ck <cid>     # -> only *.img
sudo ./runsc … create --fs-restore-image-path /tmp/ck <new> # -> wants fscheckpoint.json
```

## The seam in the code

- `crates/sandbawks-node/src/gvisor.rs:905` — pause shells out to
  `runsc … checkpoint --image-path <dir> <cid>` (memory only).
- `fetch_verify_and_lay_out` stages `checkpoint.img`/`pages.img`/`pages_meta.img`
  (from `memory.zst`) + `rootfs-delta.tar.zst` + `envd-state.json`.
- `shim/internal/wrapper/service.go:112` — Create dispatches
  `CreateWithFSRestore{ Conf: FSRestoreConfig{ ImagePath: <staged dir> } }`
  → `runsc create --fs-restore-image-path=<staged dir>` → needs
  `fscheckpoint.json`, which is not in the staged dir.

## Resolution options (epic-level; pick deliberately)

**A. Pause also runs `runsc fscheckpoint`.** Add an `fscheckpoint` pass to the
node's pause so the snapshot carries gVisor's filesystem checkpoint
(`fscheckpoint.json` + fs data); stage it beside the `*.img`. Keeps the shim's
"born-restored via `CreateWithFSRestore`" design. Open questions: does
`fscheckpoint` + `checkpoint` + `create --fs-restore-image-path` + `Restore`
round-trip a sentinel? does `fscheckpoint` subsume or coexist with the
`rootfs-delta` tar? Both are answerable in the local gVisor harness above
*before* writing controller/node code.

**B. Resume via the snapshotter delta, memory-only gVisor restore.** Drop
`CreateWithFSRestore`; apply the existing `rootfs-delta.tar.zst` to the restore
Pod's snapshotter upperdir before container start (the pre-shim
`RootfsDelta::ApplyToContainer` mechanism), and have the shim do only the
memory `Restore`. Reuses the pause-captured tar but reworks the shim and
abandons its `CreateWithFSRestore` premise.

## What PR #229 *did* fix (real, independent of the seam)

The `FULL=1` flip drove out a chain of genuine, mostly-verified defects, each
moving the failure strictly forward:

1. **Staging root not hostPath-mounted** — node DaemonSet now mounts
   `/var/lib/sandbawks/staging` (the shim couldn't read the staged image at all).
2. **`Resuming` requeued only every 30s** — given the 3s `Creating` cadence
   (the `pod_ready` probe runs for both); resume `Running` lagged.
3. **Smoke parsed `.envdURL`** from `/connect`, which the SDK DTO doesn't carry.
4. **kind-smoke pulled a stale `:latest` kind-node image** — now builds it from
   branch source when `ci/kind-node/**` or `shim/**` changes (+ buildx).
5. **Baked runsc `20240603.0`** predated `-fs-restore-image-path` — bumped to
   `20260525.0` to match the shim's pinned gVisor.
6. **Shim restored the wrong container** — `--fs-restore-image-path` is only
   valid on the root/sandbox container (`IsRootContainer`), not a sub-container;
   guard now restores the pod sandbox, passes sub-containers through.

These are worth keeping regardless of how the seam (A vs B) is resolved.
`PAUSE_RESUME_SMOKE_FULL=1` cannot go green until the seam is closed.
