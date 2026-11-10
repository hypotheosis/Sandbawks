# Plan 03 T6 — envd-state.json serialization + hegel property tests

Date: 2026-05-23

Issue: #59
Epic: #53 — Plan 03 envd-rs

## What landed

Replaces the placeholder-only `crates/envd/src/state.rs` with the
pause/resume snapshot layer that lives next to `memory.zst` and
`rootfs-delta.tar.zst` in the snapshot bundle (spec §6.1). envd
serializes its in-memory bookkeeping (process registry, PTY registry,
filesystem-watch registry) to a single JSON file; on resume the binary
loads the file and rehydrates equivalent registry state. The actual
process memory / kernel-fd state is the gVisor adapter's responsibility
in Plan 05 — this module owns only envd's *bookkeeping* about those
resources.

## Files

### New

- `crates/envd/tests/state.rs` — 7 integration tests (the 6 from the
  issue plus a `write_and_read_roundtrip_through_disk` covering the
  temp-file + rename file I/O dance):
  - `empty_state_roundtrip`
  - `state_with_processes_roundtrip`
  - `state_with_ptys_roundtrip`
  - `state_with_watches_roundtrip`
  - `forwards_compat_unknown_field_ignored`
  - `version_mismatch_rejected`
  - `write_and_read_roundtrip_through_disk`
- `crates/envd/tests/state_properties.rs` — 2 `hegel`-driven property
  tests:
  - `arbitrary_state_roundtrips_through_json` — `arbitrary_state →
    serialize → deserialize == arbitrary_state` for any
    structurally-valid `EnvdState`.
  - `serialization_is_deterministic` — same state serialized twice
    yields byte-identical JSON. The determinism invariant is what
    makes the snapshot bundle's content-hash a stable cache key on the
    Plan 05 controller path.

### Modified

- `crates/envd/src/state.rs` — full module, replaces the T1
  placeholder. Public types:
  - `EnvdState { version: u32, processes: Vec<ProcessSnapshot>, ptys:
    Vec<PtySnapshot>, watches: Vec<WatchSnapshot> }`.
  - `ProcessSnapshot`, `PtySnapshot`, `WatchSnapshot` — field-for-field
    matches the schema sketch in the issue.
  - `ProcessRegistry`, `PtyRegistry`, `WatchRegistry` — newtypes
    wrapping `Vec<Snapshot>` so callers (the running services, in T7)
    can `push` snapshots and the resume path can `from_snapshots` /
    `into_inner` without exposing the underlying vec.
  - `RestoreResult { processes, ptys, watches }`.
  - `RestoreError` — three variants: `VersionMismatch { found,
    expected }`, `Io(std::io::Error)`, `Parse(serde_json::Error)`.
    Hand-rolled rather than reaching for `thiserror` for a single
    error site.
  - `pub const SCHEMA_VERSION: u32 = 1`.
  - `EnvdState::capture(&ProcessRegistry, &PtyRegistry,
    &WatchRegistry) -> Self`.
  - `EnvdState::restore(self) -> Result<RestoreResult, RestoreError>`
    — rejects mismatched versions.
  - `EnvdState::to_json` / `from_json` — `serde_json::to_string_pretty`
    + `from_slice`.
  - `EnvdState::write_to` / `read_from` — file I/O with a temp-file
    + `rename` to keep the previous snapshot intact if a write crashes
    mid-flight.
- `crates/envd/Cargo.toml` — adds `serde`, `serde_json` to
  `[dependencies]` and `hegel = { workspace = true }` to
  `[dev-dependencies]`.
- `Cargo.toml` — adds `hegel = { package = "hegeltest", version =
  "0.14" }` to `[workspace.dependencies]`. The crates.io package is
  named `hegeltest` but its proc-macros (`#[hegel::composite]`,
  `#[hegel::test]`) embed `hegel::` in their expansion, so we rename
  the import name on the way in.

## Design notes

### Determinism

The determinism property holds because every collection in the type
tree is a `Vec` (preserves insertion order) or a `BTreeMap` (orders by
key). A `HashMap<String, String>` for `ProcessSnapshot::env` would
silently introduce nondeterminism via its hasher's seed — the docs on
that field call this out explicitly.

### Forward compatibility

Serde's default behavior is to ignore unknown JSON fields on
deserialize, so a future schema version can grow new optional fields
and an older envd will still load the file (it just won't see the new
fields). None of the snapshot structs carry `#[serde(deny_unknown_fields)]`.
The `forwards_compat_unknown_field_ignored` test pins this invariant.

### Backwards compatibility

If a future version's wire shape changes in a backwards-incompatible
way, the producer bumps `SCHEMA_VERSION` and the consumer rejects the
file with `RestoreError::VersionMismatch` rather than silently
mis-parsing.

### Registry decoupling

`ProcessRegistry` is **not** the same thing as `ProcessService.entries`
in `process.rs`. The service's registry owns runtime-only state
(broadcast channels, tokio `JoinHandle`s, the `tokio::process::Child`
itself) that does not survive a pause. The snapshot-shaped registry
here owns only the serializable bookkeeping. The wiring that walks the
service's map to populate this one is left to T7 (the resume binary).

## Validation

| Step | Status |
| --- | --- |
| `just fmt` | clean |
| `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| `cargo test -p envd --test state` | 7 passed |
| `cargo test -p envd --test state_properties` | 2 passed (hegel default ~100 cases each) |
| `just test` | green workspace-wide |

## Out of scope (carried forward)

- Snapshot upload/download to S3 — `sandbawks-node`, Plan 05.
- Compression — file is KB-range; defer until measured otherwise.
- Wiring of the live `ProcessService` / `PtyService` /
  `FilesystemService` registries into `ProcessRegistry` /
  `PtyRegistry` / `WatchRegistry` at pause time — T7's job.
