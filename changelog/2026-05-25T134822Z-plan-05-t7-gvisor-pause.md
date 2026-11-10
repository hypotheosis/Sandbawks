# Plan 05 T7 — `GvisorAdapter::pause`

**Issue:** #143
**Plan:** `docs/superpowers/plans/2026-05-24-sandbawks-05-gvisor-node.md` §
Task 7
**Branch:** `feat-issue-143-gvisor-pause`

## What landed

`crates/sandbawks-node/src/gvisor.rs` now hosts the
`GvisorAdapter::pause` half of the `runsc checkpoint` workflow.
Resume (`pause`'s symmetric counterpart) lands in Plan 05 T8.

### Pause workflow

1. Resolve `pod_uid` → `(container_id, pod_ip, sandbox_id)` via the
   injected `ContainerdHandle` (production wraps `containerd_client::
   Client` + kube API; the live impl ships in T14).
2. Allocate a fresh work-dir
   `<work_root>/<sandbox_id>/<sanitized-rfc3339-ts>/`. RFC 3339
   timestamps carry `:` separators which trip up Windows-mounted
   volumes, so we replace them with `-` before path-joining.
3. Shell out to `runsc checkpoint --image-path <work_dir> <container_id>`
   via `tokio::process::Command`. Non-zero exits surface stderr as
   `RuntimeError::CheckpointFailed`.
4. Tar + zstd every `memory*` file in the work-dir into a single
   buffer; upload as `memory.zst`. `tar` and `zstd` are sync crates,
   wrapped in `tokio::task::spawn_blocking` so concurrent pauses don't
   stall the runtime.
5. Compute rootfs delta via `ContainerdHandle::rootfs_delta`; upload
   as `rootfs-delta.tar.zst`.
6. GET `http://<pod_ip>:49983/__state` via the injected
   `EnvdStateClient` (production uses `reqwest::Client` with rustls);
   upload the response body as `envd-state.json`.
7. Assemble a `SnapshotManifest` v2 with all three parts +
   `recompute_aggregate()`; persist via `SnapshotSink::put_manifest`;
   return the manifest.

### Dependency injection

Hitting a real containerd socket, real envd HTTP endpoint, and real
`runsc` binary inside a unit test is impractical. The module factors
each external system behind a trait, mirroring the `BlobStore` pattern
T6 established for the S3 sink:

| Trait                  | Production impl     | Test fake (in `gvisor::fake`) |
|------------------------|---------------------|-------------------------------|
| `ContainerdHandle`     | `ContainerdClient`  | `FakeContainerd`              |
| `EnvdStateClient`      | `ReqwestEnvdState`  | `FakeEnvdState`               |
| `Clock`                | `SystemClock`       | `FixedClock`                  |
| `runsc` binary path    | `$PATH`-resolved    | `with_runsc_binary(stub.sh)`  |

`ContainerdClient::resolve_pod` and `::rootfs_delta` currently return
`RuntimeError::CheckpointFailed("…Plan 05 T14")`; the live containerd
snapshotter Diff + kube Pod GET wire-up lands in T14 alongside the
gVisor-enabled `kind` cluster. Surfacing the gap loudly keeps a
misconfigured prod deploy from silently producing empty snapshots.

### Tests

- **`crates/sandbawks-node/src/gvisor.rs` (inline `mod tests`)** —
  three `#[test]` cases covering the path sanitizer, the `EnvdState`
  serde default, and `tar_zstd_memory_files_blocking`'s "only tar
  files whose name starts with `memory`" filter.
- **`crates/sandbawks-node/tests/gvisor.rs` (integration)** — two
  `#[tokio::test]` cases:
  - `pause_emits_three_parts_and_manifest` — drives the full
    workflow against `FakeContainerd` + `FakeEnvdState` +
    `FixedClock` + `FakeSnapshotStore` + a shell-script `runsc` stub
    written to a `tempfile::tempdir`. Asserts (a) three parts land
    with names `memory.zst`, `rootfs-delta.tar.zst`,
    `envd-state.json`, (b) per-part sha256s match a fresh hash over
    the stored bytes, (c) the manifest's aggregate sha256 matches
    `aggregate_sha256(&parts)`, (d) `created_at` equals the
    `FixedClock`'s `now`, (e) bytes round-trip verbatim for the
    envd/rootfs parts and the un-tar of `memory.zst` reproduces the
    two known input files.
  - `pause_surfaces_runsc_nonzero_exit_as_checkpoint_failed` —
    points the adapter at a stub that exits non-zero with a stderr
    message and asserts the stderr surfaces in the
    `RuntimeError::CheckpointFailed` variant.

## New dependencies

In `[workspace.dependencies]`:

- `reqwest 0.12` — `default-features=false`, `rustls-tls` + `json` +
  `stream` features. The static-link policy matches `rust-s3` (no
  OpenSSL on distroless).
- `tokio` feature set extended with `process` + `fs` for the runsc
  subprocess + async work-dir creation.

In `crates/sandbawks-node/Cargo.toml`:

- `containerd-client`, `zstd`, `tar`, `reqwest`, `chrono` —
  workspace-pinned; the prior comment ("T7 will add…") becomes "T7
  adds…".
- `dev-dependencies` now opts `sandbawks-types` into its `testing`
  feature so the integration test can construct `FakeSnapshotStore`.

## Verification

- `just fmt` clean.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `just test` — entire workspace green; `sandbawks-node` adds 3
  inline `gvisor` tests and 2 integration tests.
- `cargo run -p sandbawks-node -- --help` still prints the placeholder
  CLI (T9 will replace this entrypoint).

## Out of scope (covered by later tasks)

- Live `containerd_client::Client` snapshotter Diff implementation and
  kube Pod GET lookup → Plan 05 T14.
- `GvisorAdapter::resume` (download + verify + `runsc restore`) →
  Plan 05 T8.
- Reconciler binary that dispatches `SnapshotJob` CRs to `pause`/
  `resume` → Plan 05 T9.
- Helm-chart wiring of the production binary and S3 credentials →
  Plan 05 T12.
- Real `runsc` + MinIO smoke test on `kind` → Plan 05 T14.
