# Issue #331 — Fix: gate validate path on disk headroom for the expansion peak

**Date:** 2026-06-05T21:11:37Z
**Branch:** `fix-issue-331-validate-disk-headroom`
**Area:** `area/node` (gVisor validate/stage/resume path)

## Background

`GvisorAdapter::validate_restorable` is the pause-time restorability proof:
it re-fetches the just-uploaded snapshot into a scratch dir and proves it
materializes back into a complete checkpoint bundle before the worker reports
the pause `Complete`. It called the shared lay-out helper
`fetch_verify_and_lay_out` with `HeadroomGate::Skip` — by design, since #238
scoped the headroom guard to the stage/restore *consumers* of a snapshot, not
the producer.

After #330 (which fixed the #329 validate-path RAM OOM by streaming
`memory.zst` to a staged file instead of buffering it in a `Vec<u8>`), the
helper streams the compressed part to disk and then un-zstd + un-tars it into
the `*.img` files **in the same dir, before deleting the `.zst`**. So during
expansion the compressed part and its uncompressed expansion coexist on disk:
peak transient disk grew by roughly the compressed-part size.

On a node whose snapshot-cache / staging volume is tight relative to a
GiB-scale bundle, the #329 RAM-OOM fix would therefore shift the failure
symptom from OOM to **ENOSPC mid-expansion** — and on the validate path that
ENOSPC happened with no headroom gate at all to turn it into a clean error.

## Fix (issue Direction 1)

Two changes, both in `crates/sandbawks-node/src/gvisor.rs`:

1. **`validate_restorable`: `HeadroomGate::Skip` → `HeadroomGate::Disk`.** A
   too-big bundle on a disk-tight node now fails its `SnapshotJob` cleanly with
   a `RuntimeError::SnapshotDownload` headroom error (validate's existing
   error variant) instead of ENOSPC partway through expansion. Memory is not
   gated: validate expands only to disk and runs no in-process `runsc restore`
   that would fault `pages.img` into RAM (that remains the resume path's
   `DiskAndMemory`).

2. **`enforce_headroom`: disk need is now the transient expansion peak.**
   Changed from `Σ(compressed parts)` to
   `Σ(compressed parts) + uncompressed_memory_bytes` (the `memory.zst`
   part's recorded `uncompressed_bytes`, falling back to its compressed
   `size_bytes` for pre-#243 snapshots — the same figure the memory gate
   already used, now hoisted and shared). This is the real accounting fix:
   the prior figure counted only the compressed wire total and silently
   ignored the on-disk expansion. The bound is deliberately conservative
   (the compressed `memory.zst` and the other parts are never all resident
   simultaneously with the full expansion), but a fail-closed gate should
   over- not under-estimate, and the `HeadroomPolicy` fraction + floor add
   the cushion. `saturating_add` so a bogus/overflowing manifest fails the
   gate rather than wrapping to a tiny `needed`.

Because the accounting fix lives in the shared `enforce_headroom`, the
**stage** (`Disk`) and **resume** (`DiskAndMemory`) paths also stop
undercounting their disk need — they previously shared the same blind spot,
they just already had a gate where validate did not.

### Dead variant removed

With validate moved to `Disk`, `HeadroomGate::Skip` had no remaining callers.
Leaving it would be a dead variant (a `-D warnings` failure) and, more to the
point, #331 decides validate *should* gate — so `Skip`'s only reason to exist
is gone. The variant and its `enforce_headroom` early-return arm were removed.

## Behavior change

This is a deliberate change to pause semantics, as the issue flagged: a pause
can now fail during its own validation round-trip with a disk-headroom error on
the pausing node. That is the intended outcome — a clean, retriable
`SnapshotDownload` failure with the source still recoverable, rather than a
half-expanded ENOSPC at GiB scale.

## Tests

`crates/sandbawks-node/tests/headroom.rs` gains
`validate_fails_closed_on_insufficient_disk_for_expansion_peak`:

- compressed `memory.zst` = 1 GiB, uncompressed `*.img` = 8 GiB, available
  disk = 5 GiB (default 0.8 fraction → 4 GiB cap).
- The compressed-only need (1 GiB) would *pass*; the 9 GiB expansion peak must
  *fail*. The trip proves **both** that validate gates at all (was `Skip`) and
  that the disk figure is the expansion peak, not the wire total. The
  shortfall message is asserted to report the 9 GiB peak so a regression
  reverting either change is loud.
- Fail-closed assertion: no `*.img` survive (the gate trips before any part is
  fetched; validate tears its scratch dir down on every exit path).

The existing stage/resume/uncompressed/ample headroom tests still pass under
the new accounting (verified: their disk margins are far from the gate).

## Out of scope

- **Direction 2** (incremental expansion so compressed + expanded never
  coexist) — the issue itself flags it as hard (tar+zstd streaming unpack reads
  the whole compressed stream); over-scoped for a `prio/p2` note.
- **Direction 3** (document-only) — superseded by actually enforcing the gate.
