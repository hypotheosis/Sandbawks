# PVC-per-Sandbox vs. S3 Snapshotting — Design Spike

- **Status:** Draft (spike / design investigation) — **rev 2** (re-scoped to the
  intended design after review feedback)
- **Date:** 2026-06-03
- **Authors:** Peter Grace
- **Tracking issue:** #312
- **Scope:** Go/no-go on giving each sandbox its own mounted PersistentVolumeClaim.
- **Related:** #260 (Plan 08 warm-pool + lazy restore), #293 (stream bundle to
  disk), #302 (GiB-scale resume races envd drain), #243 (resume memory
  headroom gate), **#231 (`--overlay2=none` restore data-loss)**,
  **#237 (fail-closed writable-mount assertion)**, #232 (memory-only restore
  reconstructs rootfs).

> **Revision note (rev 2).** The first draft of this spike analysed the wrong
> design: it put the *checkpoint bundle* on the PVC and concluded the memory
> checkpoint — the dominant cost — was immovable, so a PVC "can't help."
> The **intended** design is different and better: make the PVC the sandbox's
> **live writable working filesystem**, so file writes never enter the
> in-sandbox overlay-tmpfs and therefore never enter the memory checkpoint.
> That *shrinks the dominant cost* rather than relocating it. This revision is
> re-centred on that design. The latency anchor used in rev 1 (a 48 s pause for
> a 1 GiB payload) was measured on a **resource-constrained build server
> running Garage**, so it is treated here as directional only, not a production
> baseline.

## 1. Executive summary

**Recommendation: CONDITIONAL GO — build a prototype**, gated on three
unknowns that only a prototype can close (§6). This reverses rev 1's blanket
no-go, because rev 1 mis-scoped the design.

The intended design — **PVC as the sandbox's live working filesystem** — attacks
the actual bottleneck. Today, under gVisor's `--overlay2=root:self`, a sandbox's
file writes land in an in-sandbox overlay-tmpfs that is captured by the
**memory checkpoint** (`pages.img`). That is *why* a 1 GiB-of-files workload
produces a ~1 GiB memory image and a slow pause: the files ride inside the
checkpoint. Relocate those writes onto a gofer-backed PVC mount and the bulk
file data **leaves `pages.img`**, shrinking the memory checkpoint toward real
process RSS (tens of MiB for a file-heavy workload). Pause then becomes
"checkpoint a small memory image + detach the PVC"; resume becomes "reattach
the PVC + restore a small memory image."

Crucially, this is **the exact configuration the codebase currently
fail-closes on** (`assert_writable_mounts_overlay_backed`, #237) *because* a
host-backed writable mount's writes are "invisible to the memory checkpoint and
silently lost on restore" — the `--overlay2=none` failure measured in #231. The
PVC is precisely the missing piece that makes that config safe: it gives the
gofer-backed writes a **durable, portable backing store** that survives a
memory-only restore. The design does not fight #231; it resolves #231's root
cause.

What still gates a go (§6): (1) gVisor must correctly checkpoint and restore a
sandbox holding **open file descriptors into a gofer-backed mount**, re-binding
them to the reattached PVC — #231 shows this path is not free; (2) the actual
`pages.img` shrink depends on gVisor's **gofer page-cache** policy and must be
measured; (3) production-representative latency, including **CSI
attach/detach**, must be measured (the 48 s build-server number is not it). The
node/zone-affinity and warm-pool-scale constraints (§7) are real and bound
*where* this design is usable, not *whether* it works.

## 2. How pause/resume works today (grounding)

Accurate baseline; all file-anchored. Unchanged from rev 1 except the added
#237 detail in §2.6.

### 2.1 The snapshot bundle

A snapshot is a small set of named *parts* (`crates/sandbawks-types/src/snapshot.rs`):

- **`memory.zst`** — tar+zstd of the gVisor checkpoint `*.img` files (sentry
  state + `pages.img` MemoryFile pages + page metadata). **Dominant payload.**
- **`rootfs-delta.tar.zst`** — overlayfs upperdir snapshot. **Typically empty
  for gVisor** under `--overlay2=root:self`: the writable rootfs upper lives in
  an in-sandbox overlay-tmpfs captured inside `pages.img`
  (`crates/sandbawks-node/src/gvisor.rs:710,752`).
- **`envd-state.json`** — userland process registry (KB range).

### 2.2 Why the memory checkpoint is the cost — and why that is *movable*

The decisive mechanism (`gvisor.rs:706–757`): a writable mount's state survives
a memory-only restore **only** if it is `overlay` / `tmpfs` / a pseudo-fs / a
runtime-managed furniture bind — i.e. something the sentry MemoryFile captures
into `pages.img`. Under the default `--overlay2=root:self`, **all rootfs writes
are overlay-tmpfs**, so they are in `pages.img`. A 1 GiB-of-files workload
therefore yields a ~1 GiB memory checkpoint.

Rev 1's error was reading this as "the checkpoint is immovable." It is not: the
files are in `pages.img` *because of the overlay config*. Change the config so
writes go to a gofer-backed mount and they leave the checkpoint. That is the
whole idea (§3).

### 2.3 Storage abstraction

Two traits decouple the gVisor adapter from the backend
(`crates/sandbawks-types/src/snapshot.rs:345,384`):

```rust
#[async_trait]
pub trait SnapshotSink: Send + Sync {
    async fn put_part(&self, name: &str, bytes: Vec<u8>) -> Result<SnapshotPart, RuntimeError>;
    async fn put_manifest(&self, manifest: &SnapshotManifest) -> Result<(), RuntimeError>;
    fn manifest_url(&self) -> String;
}

#[async_trait]
pub trait SnapshotSource: Send + Sync {
    async fn get_part(&self, name: &str) -> Result<Vec<u8>, RuntimeError>;
    async fn get_manifest(&self) -> Result<SnapshotManifest, RuntimeError>;
}
```

Production impls: `S3SnapshotSink` / `S3SnapshotSource`
(`crates/sandbawks-node/src/s3.rs`, on the `rust-s3` crate): 4 MiB
content-addressed chunks, 8-way parallel, per-chunk + aggregate sha256,
idempotent upload. The (small, post-relocation) memory checkpoint can keep using
this path unchanged.

### 2.4 Pause → Stage → Resume flow

1. **Pause.** `runsc checkpoint --image-path <work_dir> <cid>` writes `*.img`;
   the adapter tars+zstds them into `memory.zst` and `put_part`s the bundle to
   S3 (`gvisor.rs:1250` checkpoint, ~1286–1350 bundling). The pause path has
   **no per-step timing** around checkpoint / tar+zstd / upload (verified) — so
   the 48 s split is not currently observable internally; see §6.
2. **Stage.** A `Stage` `SnapshotJob` (`snapshot_job.rs:38`) materializes the
   bundle from S3 into a node-local staging dir
   (`DEFAULT_STAGING_ROOT = /var/lib/sandbawks/staging`, `gvisor.rs:174,1523`).
3. **Resume.** Controller reads the staged `local_path` + `node_name`
   (`RestorePlacement`, `runtime.rs`), pins the resume Pod via `spec.nodeName`
   (`crates/sandbawks-controller/src/pod_adapter.rs`), and the shim runs
   `runsc restore --image-path <staging_dir> <cid>` (`gvisor.rs:1455`).

### 2.5 Lazy restore (Plan 08, #260)

The shim sets `RestoreConfig{ Background: true }`
(`shim/internal/wrapper/service.go`): `restore` returns before all pages fault
in; gVisor faults the rest **off the local `--image-path` directory**. Staging
retention (`staging_bundle_needed` + `run_staging_sweeper`, `gvisor.rs`) keeps
the bundle for the sandbox lifetime. This composes with the proposed design:
a smaller memory image means fewer pages to fault.

### 2.6 The guard that blocks the proposed design today

`assert_writable_mounts_overlay_backed` (#237, `gvisor.rs:760–809`, called at
`gvisor.rs:1214`) **fails the pause** if any writable mount is a gofer-backed
bind (hostPath / **PVC** / default-medium emptyDir) / `9p` / `virtiofs` / `nfs`,
because such writes are "invisible to that checkpoint and would be silently lost
on restore (#231's `--overlay2=none` failure)." It runs *before* the checkpoint,
so it fails closed cheaply.

This is the single most important fact for this spike: **the intended design is
currently illegal by construction**, and for a measured reason. The spike's job
is to determine whether a PVC removes that reason (§4) and, if so, to define the
relaxed invariant (§6).

### 2.7 Storage layout and the latency gate

`deploy/helm/sandbawks/templates/node-daemonset.yaml` mounts `snapshot-cache`
and `staging-cache` as `hostPath: DirectoryOrCreate`. **No StorageClass / PVC /
`volumeClaimTemplate` exists anywhere** in `deploy/helm/sandbawks`. The latency
gate (`tests/bench/`, spec §13): **`BENCH_RESUME_P95_MS=2000`** (1 GiB),
`BENCH_PAUSE_P95_MS=8000`. The bench fills payload from a **splitmix64 PRNG so
it is incompressible** (`tests/bench/src/main.rs:83–90`): a 1 GiB payload is a
~1 GiB bundle that tar+zstd does not shrink — which is why pause is expensive
and why getting that 1 GiB *out of the checkpoint* (§3) is the lever.

## 3. The proposed design — PVC as the live working filesystem

Mount a per-sandbox PVC as the sandbox's writable working directory (and, for a
full-rootfs variant, configure runsc so the rootfs writable layer is that mount
rather than `--overlay2=root:self`). Then:

- **Steady state.** All file operations hit the PVC (a gofer-backed host
  passthrough), not the in-sandbox overlay-tmpfs.
- **Pause.** `runsc checkpoint` now serialises only the **process/kernel memory
  state** — the bulk file data is on the PVC, not in `pages.img`. Flush the PVC,
  then **detach** it. The (small) memory image goes to S3 (existing
  `S3SnapshotSink`, now cheap) or onto the PVC itself.
- **Resume.** **Reattach** the PVC on the target node; `runsc restore` rehydrates
  the small memory image and re-binds the gofer to the repopulated mount.

The `--overlay2` mode is a runsc/RuntimeClass-level config the snapshot path
currently assumes throughout (`shim/internal/wrapper/service.go:13,111`;
`gvisor.rs`); relocating writes off it is the core change.

### What the win actually is

Not "skip the S3 round-trip" (rev 1's framing). The win is **shrinking
`pages.img`** by relocating bulk file data out of the overlay-tmpfs. For the
bench's 1 GiB file payload, the checkpoint could drop from ~1 GiB toward
process-RSS scale — a potential order-of-magnitude reduction in the bytes pause
and resume must checkpoint, compress, and move, *regardless of S3 throughput*.
That is the only mechanism in this whole investigation that reduces the
dominant cost rather than re-routing it.

### Clarifying the "just detach/reattach" intuition

Detach/reattach restores the **filesystem**. The **running process memory** must
still be checkpointed, or resumed processes come back cold (warm files, dead
PIDs) — a weaker semantic than today's pause/resume. The good news: post-
relocation that checkpoint is *small*, so preserving full process state stays
cheap. The design is therefore "small memory checkpoint **+** PVC detach," and
the small checkpoint is a feature, not overhead.

## 4. Why this can work where #231 / #237 failed

#231's `--overlay2=none` failure and #237's fail-closed guard both stem from one
root cause: a gofer-backed writable mount's writes are **not in the memory
checkpoint**, and a memory-only restore does not restore the host filesystem, so
the writes are **lost**. The data had nowhere durable to live across pause →
resume.

A per-sandbox PVC supplies exactly that missing durability:

- On pause, dirty pages are flushed to the PVC and the volume is detached — the
  data persists in the volume, not in (absent from) the checkpoint.
- On resume, the **same** PVC is reattached and the gofer re-binds to a
  populated path — the files are present, so they are not "lost on restore."

So the PVC converts the #231 failure mode (host-backed writes with no durable
home) into a supported case (host-backed writes with a portable durable home).
The #237 guard would be **relaxed** to permit a writable mount **iff** it is a
PVC enrolled in the snapshot set and guaranteed reattached on resume — a new,
narrower invariant (§6).

This does **not** make it automatically correct — it removes the *known* reason
it was illegal. The residual risk is gVisor's behaviour on restore with open
FDs into that mount (§6, Risk 1).

## 5. Latency: what changes, and what we cannot yet claim

The honest position post-feedback: **absolute latency must be measured in
production.** The 48 s/1 GiB pause was on a resource-constrained build server
running Garage; it is directional, not a baseline. What the *model* says
qualitatively:

- **Pause** ≈ `T_checkpoint(small) + T_compress(small) + T_upload(small) + T_flush_pvc + T_detach`.
  The first three terms shrink with `pages.img`; `T_detach` is new and
  CSI-dependent.
- **Resume** ≈ `T_attach + T_mount + T_restore_return(small) + T_envd_ready`
  (+ on-PVC file-read latency during execution). `T_attach` is the new fixed
  cost; the download term is gone.

The decisive numbers, none of which we have yet:

1. **Shrink factor of `pages.img`** — workload-dependent and subject to the
   gofer page-cache caveat (§6, Risk 2). Drives every "small" term above.
2. **CSI attach/detach** on the chosen driver — commonly 10–40 s for networked
   block (EBS), sub-second for a pre-attached local PV. This can dominate and
   even *invert* the win for small/short-lived sandboxes.
3. **On-PVC read latency** for the working set during execution (local SSD ≈
   fine; networked block / RWX network-FS ≈ a tail).

Net: the design can plausibly clear the 2 s/1 GiB **resume** gate *if* the PVC is
local-backed or pre-attached and the checkpoint shrinks as hoped; it can *miss*
it badly if CSI attach is on the critical path. Measurement decides.

## 6. What a prototype must prove (the gates)

A minimal prototype: one sandbox, working dir on an RWO PVC, runsc configured so
writes bypass `--overlay2=root:self`, #237 guard relaxed for that mount.

1. **Risk 1 — checkpoint/restore correctness with open FDs (the #231 redux).**
   Prove a process holding open FDs / mmaps into the PVC mount checkpoints and
   restores without data loss or corruption after detach → reattach, including
   on a *different* node. This is the make-or-break gate; #231 says do not assume
   it.
2. **Risk 2 — measured `pages.img` shrink.** Compare checkpoint size for the
   1 GiB bench payload, overlay-tmpfs vs PVC-working-dir. Confirm gofer page
   cache does not silently pull the file bytes back into `pages.img`. (Add the
   missing per-step pause timing — §2.4 — so checkpoint/compress/upload are
   individually visible; this is the cheap instrumentation the issue's
   "measured numbers" mandate needs.)
3. **Risk 3 — production-representative latency.** Measure pause + resume +
   CSI attach/detach on a real driver in a non-constrained environment, across
   small / 1 GiB / multi-GiB, vs the current S3 path. The build-server/Garage
   number does not count.

If go: implementation seam — `PvcSnapshotSink`/`Source` *or* (more likely) a
working-dir mount plumbed by the controller; per-sandbox PVC templating +
lifecycle/GC in `sandbawks-controller`; a relaxed `mount_state_survives_restore`
classification for enrolled PVCs in `gvisor.rs`; `node.snapshotStorageClass`
wiring in `deploy/helm/sandbawks` (no StorageClass exists today).

## 7. Hard constraints that bound usability (unchanged)

These do not block correctness; they bound *where* the design applies.

- **Node/zone affinity.** RWO volumes (EBS) are single-attach and zonal —
  resume can no longer land on *any* node, the property S3 deliberately
  provides. RWX (EFS/NFS/CephFS) avoids it but adds network-FS read latency and
  per-volume cost.
- **Warm-pool scale (#260).** Thousands of pre-paused sandboxes ⇒ thousands of
  PVCs + PVs: CSI attach churn, provisioning limits, orphaned-volume GC, and
  cost that millions of S3 objects do not incur. A PVC-backed warm pool likely
  needs volume pooling/recycling, not one-PVC-per-sandbox.
- **Lifecycle/GC.** Per-sandbox PVC create/delete, reclaim policy, and leak
  cleanup must be designed (today the controller GCs S3 objects + staging dirs).
- **Scheduling.** With a PVC, CSI topology constraints — not `spec.nodeName`
  staging-locality — drive the resume affinity model.

## 8. Relationship to the rev-1 variants

Rev 1's A/B/C are subsumed:

- **A (FS-only PVC, checkpoint still S3):** the "filesystem on PVC" half of the
  proposed design, but without relocating writes *out of the overlay*, so it
  misses the `pages.img` shrink. Strictly weaker than §3.
- **B (checkpoint-bundle on PVC):** re-routes the bundle without shrinking it;
  trades S3 throughput for CSI attach. The proposed design instead shrinks the
  bundle.
- **C (RWX):** still relevant as the *backend choice* that preserves node-agnostic
  resume (§7), at network-FS read-latency cost.

The proposed design (§3) is the one worth prototyping; backend (RWO local / RWO
networked / RWX) is a secondary, measured decision.

## 9. Go/no-go and honesty ledger

- **Decision:** **CONDITIONAL GO to prototype**, gated on Risks 1–3 (§6). Not a
  commitment to ship, and not a commitment to a CSI driver (out of scope).
- **If Risk 1 fails** (gVisor cannot cleanly restore open FDs into a reattached
  gofer mount): **no-go** — the design is unsound, exactly as #231 warned, and
  the durable backing does not save it.
- **If Risk 1 passes but Risk 2 shows little shrink** (page cache pulls file
  bytes back into `pages.img`): **marginal** — revisit gofer cache config or
  fall back to optimizing the S3 path (#293 streaming restore).

| Item | Status |
|---|---|
| Re-scoped to the intended design (PVC = live working FS) | ✅ §3 (rev 2) |
| Identified the blocking guard (#237) and the failure it encodes (#231) | ✅ §2.6, §4 |
| Articulated the real win (shrink `pages.img`, not re-route it) | ✅ §3 |
| Affinity / scale / lifecycle constraints | ✅ §7 |
| **Measured** checkpoint-shrink factor | ❌ not done — Risk 2; prototype |
| **Measured** open-FD checkpoint/restore correctness | ❌ not done — Risk 1; prototype |
| **Production-representative** latency incl. CSI attach | ❌ not done — 48 s was Garage on a constrained build server; Risk 3 |
| Per-step pause timing instrumentation | ❌ not present in `gvisor.rs`; add it (§6.2) |

The qualitative reasoning is grounded in the current code paths (all anchored);
every *quantitative* claim about shrink factor, CSI attach, and end-to-end
latency remains an assumption pending the §6 prototype.
