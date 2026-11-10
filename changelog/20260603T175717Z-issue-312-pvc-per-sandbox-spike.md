# Issue #312 — PVC-per-sandbox storage spike

- **Date:** 2026-06-03T17:57:17Z
- **Branch:** `feat-issue-312-pvc-per-sandbox-spike`
- **Type:** design spike (docs only; no code change)

## What

Added a design-spike doc,
`docs/superpowers/specs/2026-06-03-pvc-per-sandbox-spike.md`, investigating
whether giving each sandbox its own mounted PersistentVolumeClaim would make
pause/resume dramatically faster than the current S3 + node-local-staging
snapshot store.

## Findings

- Grounded the analysis in the current code paths: `SnapshotSink`/
  `SnapshotSource` traits (`crates/sandbawks-types/src/snapshot.rs`), the S3
  impls (`crates/sandbawks-node/src/s3.rs`), the `Stage`/lazy-restore flow and
  `--overlay2=root:self` nuance (`crates/sandbawks-node/src/gvisor.rs`),
  controller `nodeName` pinning, and the hostPath-only Helm node DaemonSet.
- Key reframing: with Plan 08 lazy restore (#260) the resume hot path already
  faults pages off **node-local disk**, so S3 only pays a one-time bulk
  download. A PVC must beat that, not a naive synchronous S3 read.
- Built an analytical latency model for Variant B across small / 1 GiB /
  multi-GiB sandboxes.

## Recommendation

**Rev 1 (superseded): NO-GO** — but rev 1 analysed the wrong design (checkpoint
bundle on the PVC) and wrongly treated the memory checkpoint as immovable.

**Rev 2 (current): CONDITIONAL GO to prototype.** Re-scoped to the intended
design: make the PVC the sandbox's **live writable working filesystem** so file
writes bypass the in-sandbox overlay-tmpfs and therefore leave `pages.img` —
shrinking the dominant cost (the memory checkpoint) rather than re-routing it.
This is the exact config the codebase fail-closes on today
(`assert_writable_mounts_overlay_backed`, #237) because of the #231
`--overlay2=none` data-loss; a per-sandbox PVC is precisely the durable backing
#231 lacked, converting that failure mode into a supported case. Gated on three
prototype-only unknowns: (1) gVisor checkpoint/restore correctness with open FDs
into a reattached gofer mount; (2) measured `pages.img` shrink factor (gofer
page-cache caveat); (3) production-representative latency incl. CSI attach (the
48 s build-server/Garage number is not a baseline). Node-affinity and
warm-pool-scale constraints bound *where* the design applies, not whether it
works.

## Validation

- `just fmt` — no Rust touched; formatting unaffected.
- `cargo clippy --all-targets -- -D warnings` — no Rust touched.
- `just test` — no Rust touched.
- The latency model's CSI attach numbers are **analytical estimates, not
  measured** (committing to a CSI driver is out of scope per the issue); the
  doc records this gap explicitly in its honesty ledger.
