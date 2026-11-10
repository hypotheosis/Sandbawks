# Headroom guard: gate restore memory on the uncompressed `pages.img` size

**Issue:** #243 (follow-up to #242 / #238 resource-headroom policy)
**Type:** fix (`area/node`, `runtime/gvisor`)

## Problem

The resume-path memory headroom gate (`GvisorAdapter::enforce_headroom`,
`HeadroomGate::DiskAndMemory`) used the **compressed** `memory.zst` part
`size_bytes` as the proxy for the RAM working set an in-process `runsc
restore` faults in. zstd ratios of 3-5x on memory pages are common, so a
~2 GiB `memory.zst` can decompress to an ~8 GiB `pages.img` working set.
The gate therefore substantially **under**estimated real RAM need and
could admit a snapshot that then OOMs on restore. The 0.8 fraction +
1 GiB floor gave only a coarse cushion, not tight admission control.

The disk gate was already correct (the bundle lands on disk compressed,
so disk-need = Σ compressed part sizes) and is unchanged.

## Change

Record the uncompressed memory-image size in the snapshot manifest at
pause time and have the memory gate prefer it.

- **`sandbawks-types`** — `SnapshotPart` gains
  `uncompressed_bytes: Option<u64>` (`#[serde(default,
  skip_serializing_if = "Option::is_none")]`). Absent on v1/v2 manifests
  paused before this change → the gate falls back to the compressed
  `size_bytes`, preserving prior behavior. Added
  `SnapshotPart::with_uncompressed_bytes` (builder) and kept
  `from_bytes` setting the field to `None` (it only sees post-compression
  bytes).
- **`sandbawks-node` pause** — `tar_zstd_checkpoint_files` now also
  returns the summed uncompressed `*.img` byte total (computed via
  `File::metadata` before each `tar` append, which does not move the read
  cursor). `GvisorAdapter::pause` attaches it to the `memory.zst` part
  returned by `put_part`.
- **`sandbawks-node` resume** — `enforce_headroom` computes `needed_mem`
  as `uncompressed_bytes.unwrap_or(size_bytes)` for the `memory.zst`
  part. Disk-need computation is untouched.
- **CRD** — regenerated `deploy/crds/snapshotjobs.sandbawks.io.yaml` so
  the embedded manifest schema carries the new nullable `uncompressedBytes`
  property (otherwise the API server would prune it from `SnapshotJob`
  status). The `crdgen` drift test confirms the committed CRD matches.

## Tests

- `sandbawks-types`: `part_without_uncompressed_field_deserializes_to_none`
  (backward-compat) and `part_uncompressed_field_round_trips_and_is_omitted_when_absent`
  (camelCase wire key + `skip_serializing_if`).
- `sandbawks-node` (`tests/headroom.rs`):
  `resume_memory_gate_uses_uncompressed_size_when_present` — a manifest
  whose `memory.zst` is 1 GiB compressed but 8 GiB uncompressed, against
  5 GiB available memory, **trips** the gate (the compressed proxy would
  have passed), and the shortfall message reports the 8 GiB figure.
- `sandbawks-node` unit test `tar_zstd_checkpoint_files_skips_non_img_files`
  extended to assert the returned uncompressed total sums only the `.img`
  payload sizes (43 bytes), excluding decoy files and tar framing.

`just fmt`, `cargo clippy --all-targets -- -D warnings`, and `just test`
all pass.

## Scope

Out of scope (unchanged): the disk-need computation — compressed sizes
are correct for staging.
