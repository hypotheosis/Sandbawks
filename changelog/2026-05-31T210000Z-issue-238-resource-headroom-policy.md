# 2026-05-31 — #238 resource-headroom policy: gate pause/restore on node disk + memory

Fail-closed, per-node resource-headroom guard for the gVisor snapshot
stage/restore paths. One of the deferred hardening items from #232's
"Snapshot integrity / fail-closed" section.

## Why

The committed overlay upper is disk-backed and grows monotonically, and
an in-process `runsc restore` faults the saved `pages.img` working set
into RAM. #233 measured an 8 GiB `pages.img` restoring in 43.6 s — but
only because the host had ~88 GiB free. On a busy node, staging a
multi-GiB bundle or restoring it could hit `ENOSPC` (disk) or the OOM
killer (memory). Previously nothing checked node headroom before the
node worker began materializing a snapshot, so an oversized snapshot
would fail *mid-operation* with an opaque error rather than failing its
`SnapshotJob` cleanly up front.

## What changed

### New pure policy module — `crates/sandbawks-node/src/headroom.rs`

- `HeadroomPolicy { max_fraction, min_free_bytes }` — a pure,
  unit-testable policy. A `needed` byte count "fits" within `available`
  iff `needed <= max_fraction * available` **and**
  `available - needed >= min_free_bytes`. The two checks are
  independent: a tiny snapshot on a nearly-full resource is rejected by
  the floor; a huge snapshot on a roomy resource is rejected by the
  fraction. `check_disk` / `check_memory` return a `Shortfall` that
  names the resource (disk vs memory) and carries the policy params so
  the `Display` message is self-describing.
- `NodeStats` trait + `SysNodeStats` impl — disk via `nix::sys::statvfs`
  (`blocks_available × fragment_size`), memory via a `/proc/meminfo`
  `MemAvailable:` parse (`parse_mem_available_bytes`, a pure function
  with its own unit tests). Behind a trait so the gate is testable with
  a fixed reading.
- Conservative defaults: `DEFAULT_MAX_FRACTION = 0.8` (keep ≥ 20% of the
  resource in reserve) and `DEFAULT_MIN_FREE_BYTES = 1 GiB`.

### Gate wired into the adapter — `crates/sandbawks-node/src/gvisor.rs`

- `GvisorAdapter` gains `node_stats: Arc<dyn NodeStats>` (default
  `SysNodeStats`) and `headroom: HeadroomPolicy` (default conservative),
  with `with_node_stats` / `with_headroom_policy` builders mirroring the
  existing injectable-dependency pattern.
- A private `HeadroomGate { Skip, Disk, DiskAndMemory }` threads through
  the shared `fetch_verify_and_lay_out` helper, which now calls
  `enforce_headroom` immediately after fetching the authoritative
  manifest and **before** writing any part to disk:
  - `stage_snapshot` → `Disk` (bundle lands on disk; this node runs no
    restore) → shortfall surfaces as `RuntimeError::SnapshotDownload`.
  - `resume` → `DiskAndMemory` (in-process `runsc restore` faults
    `pages.img` into RAM) → shortfall surfaces as
    `RuntimeError::RestoreFailed`.
  - `validate_restorable` → `Skip` — the pause-time restorability
    round-trip is itself the fail-closed pause guard (#235) and runs on
    the source node; #238 scopes the guard to the stage/restore
    consumers.
- Disk need = sum of the manifest parts' `size_bytes`; memory need = the
  `memory.zst` part. Both are the *compressed* on-the-wire sizes the
  manifest records — the manifest carries no uncompressed size — so the
  policy's fraction reserve + 1 GiB floor absorb the on-disk
  decompression expansion. (A precise uncompressed-size field would mean
  changing the pause/upload manifest schema, which is out of scope here.)

### Config surface

- `crates/sandbawks-node/src/bin/sandbawks-node.rs`: `--headroom-fraction`
  / `--headroom-min-free-bytes` clap args (with `SANDBAWKS_HEADROOM_FRACTION`
  / `SANDBAWKS_HEADROOM_MIN_FREE_BYTES` env equivalents), validated at
  startup (fraction must be in `(0.0, 1.0]`) and threaded into the
  adapter. Both are logged in the boot line.
- Helm: `node.headroomFraction` (0.8) + `node.headroomMinFreeBytes`
  (1 GiB) in `values.yaml`, rendered into the node DaemonSet env in
  `templates/node-daemonset.yaml`. The min-free-bytes value is rendered
  with `| int64 | quote`: Helm parses YAML numbers as float64, so a bare
  `| quote` emitted `1073741824` as `"1.073741824e+09"`, which the
  binary's `u64` flag parser rejected (caught by the kind-smoke test);
  `int64` forces a plain integer string.
- `crates/sandbawks-node/Cargo.toml`: added `nix = { workspace = true }`
  (the workspace dep already enables the `fs` feature `statvfs` lives
  behind; memory uses std, no extra dep).

## Tests

- `headroom.rs` unit tests: ample headroom passes both dimensions; a
  disk shortfall is named disk; a memory shortfall is named memory; the
  absolute floor rejects a small snapshot on a nearly-full resource; the
  fraction boundary is inclusive; zero-available rejects any nonzero
  need; `parse_mem_available_bytes` extracts kB→bytes and returns `None`
  when the line is absent.
- `crates/sandbawks-node/tests/headroom.rs` integration tests: a stubbed
  low-disk reading fails `stage_snapshot` closed (naming disk, no `*.img`
  staged); a stubbed low-memory reading fails `resume` closed (naming
  memory, `apply_rootfs_delta` never called); ample headroom does not
  trip the gate.

## Out of scope

Scheduling/bin-packing snapshots across nodes by size — this is a
per-node fail-closed guard, not a placement optimizer.
