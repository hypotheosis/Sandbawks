# Plan 05 T8 — GvisorAdapter::resume

Issue: #144 (Plan 05 epic
#136)

## Summary

Land the symmetric counterpart of T7's pause path: `GvisorAdapter::resume`
fetches the snapshot manifest from a `SnapshotSource`, verifies every
part's sha256 as bytes land, reapplies the rootfs delta via the
containerd snapshotter, writes envd state and restores the memory
image into a fresh work-dir, shells out to `runsc restore`, and waits
for the restored process to bind its envd TCP port before returning.

## Surface area

- `crates/sandbawks-node/src/gvisor.rs`
  - `ContainerdHandle::apply_rootfs_delta(container_id, bytes)` — new
    trait method gated on `SnapshotSink::put_part("rootfs-delta.tar.zst", …)`
    on the pause side. Production `ContainerdClient` returns
    `RuntimeError::RestoreFailed("not-yet-implemented")` until Plan 05
    T14 wires up the live containerd snapshotter Apply API.
  - `GvisorAdapter::with_envd_port(port)` — builder override so unit
    tests can probe an ephemeral local port instead of fighting for the
    fixed `ENVD_HTTP_PORT` (49983).
  - `impl RuntimeAdapter for GvisorAdapter::resume(...)` — full
    implementation per the issue's six-step plan; cross-checks the
    controller-supplied manifest against the storage manifest via
    aggregate sha256 before any restore work, so a stale or tampered
    `Sandbox.status.snapshotRef` cannot drive a restore against the
    wrong bundle.
  - `untar_zstd_to_dir_blocking(bytes, dest)` — reverse of T7's
    `tar_zstd_memory_files_blocking`; un-zstd + un-tar a `memory.zst`
    payload back into the resume work-dir.
  - `wait_for_tcp_bind(host, port, budget, interval)` — polls
    `tokio::net::TcpStream::connect` until the first successful
    `connect` or `RuntimeError::RestoreFailed` once the 30 s budget
    elapses. Each attempt is wrapped in a per-iteration timeout so an
    OS-level connect-hang cannot blow past the overall budget.
- `crates/sandbawks-node/tests/gvisor.rs`
  - `resume_unpacks_parts_applies_delta_and_waits_for_envd` —
    end-to-end happy path against `FakeSnapshotStore` seeded with the
    three parts + manifest a real pause would have produced, a stubbed
    `runsc restore` shell script that exits 0, and a
    `std::net::TcpListener` bound on an ephemeral port the
    `with_envd_port` override targets. Asserts work-dir layout,
    `apply_rootfs_delta` byte-identity round-trip, and pod_uid
    propagation.
  - `resume_surfaces_runsc_nonzero_exit_as_restore_failed` — non-zero
    exit on the `runsc` stub surfaces as `RestoreFailed` with stderr
    inlined.
  - `resume_bails_on_part_sha256_mismatch` — a tampered manifest
    `sha256` for `memory.zst` short-circuits before any snapshotter
    Apply call.
- `Cargo.toml` — workspace `tokio` features grow `"net"` for
  `TcpStream::connect`.

## Verification

- `just fmt`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `just test`

All three pass on the branch.

## Out of scope (per the issue)

- Reconciler binary dispatching `mode=Resume` SnapshotJobs to
  `resume` → T9.
- Live integration on `kind` (real containerd snapshotter + real
  envd binary) → T14.
