# 2026-05-26 — Issue #174: wire live envd services into `/__state` snapshot

## Scope

Closes the wiring gap inline TODOs have flagged since Plan 03 T6 /
T7. Before this change, envd's `/__state` route and shutdown-time
snapshot writer always emitted an empty body
(`{"version":1,"processes":[],"ptys":[],"watches":[]}`) because the
three snapshot registries were populated only at boot from
`--state-file`; the live `ProcessService` / `PtyService` /
`FilesystemService` never pushed into them.

This change retires the `SnapshotState` indirection and routes
`/__state` and the shutdown writer through new `collect_*_snapshots`
walks on the live services. After it, a `GET /__state` against a
running envd reflects the actual in-process bookkeeping:

- A `Process.Process/Start` (pipe-backed) → next probe's `processes`
  array carries the child with `pid`, `cmd`, `args`, `env`, `cwd`,
  `started_at`.
- A `Process.Process/Start` with `pty: true` → next probe's `ptys`
  array carries the entry with `id`, `pid`, `rows`, `cols`, `cmd`,
  `args`. `Update` resize updates the snapshot's rows/cols too.
- A `Filesystem.Filesystem/CreateWatcher` → next probe's `watches`
  array carries the entry with `path` and `subscriber_id`.
- A `Process.SendSignal SIGKILL` (or `RemoveWatcher`) churns the
  entry off the next probe once the live-service registry clears it.

## Implementation sketch

Pull-model "extract methods" on the services, per the issue's
preferred design. Loose coupling: services don't hold a snapshot
handle; capture sites walk them at probe time.

### `crates/envd/src/process.rs`

- `ProcessEntry` grows `pty_id: Option<u64>`, `pty_rows: AtomicU16`,
  `pty_cols: AtomicU16`, `started_at: i64`. The pre-existing
  `config: ProcessConfig` already carried `cmd / args / envs / cwd`,
  so those don't need duplication; snapshot capture clones them
  out and converts the `HashMap` envs to `BTreeMap` for determinism.
- `ProcessService` becomes `#[derive(Clone)]` (all fields are
  `Arc`-wrapped already + the new `next_pty_id: Arc<AtomicU64>` +
  cheap `Arc<Vec<_>>` seed fields). Cheap clones let the
  `/__state` HTTP handler hold a snapshot-capture handle to the same
  registry the gRPC server is mutating.
- New `ProcessService::with_seed(processes, ptys)` constructor — seed
  arrays are delivered through this rather than a separate
  `SnapshotState` indirection.
- New `collect_process_snapshots()` and `collect_pty_snapshots()`
  async methods. They acquire the registry mutex, build snapshot
  vectors from the live entries (filtering: pipe-backed → processes;
  pty-backed → ptys), drop the lock, append seed entries, and sort
  deterministically.
- `update()` resize now mirrors the kernel-side `TIOCSWINSZ` into
  `pty_rows` / `pty_cols` so the snapshot reports the post-resize
  geometry.

### `crates/envd/src/filesystem.rs`

- `FilesystemService` becomes `#[derive(Clone)]` — wraps
  `next_watcher_id` in `Arc<AtomicU64>` (was a bare `AtomicU64`) and
  gains an `Arc<Vec<WatchSnapshot>>` seed field.
- New `with_seed(root, watches)` constructor.
- `WatcherEntry` grows `path: PathBuf` (the request-side path the
  client registered against) and `subscriber_id: u64` (the numeric
  id whose string form is what `CreateWatcher` returns). Storing
  request-side rather than `resolve()`-canonical so the snapshot is
  portable across resume.
- New `collect_watch_snapshots()` async method. Same shape as the
  process-service collects.

### `crates/envd/src/bin/envd.rs`

- `SnapshotState` retired. Replaced with `SnapshotHandle { process,
  filesystem }` — both are cheap `Clone` handles into the live
  services. Its `capture()` walks all three collects.
- `load_snapshot()` → `load_boot_seed()` returns three plain `Vec<_>`s
  ready to feed `with_seed` constructors. No intermediate
  `RestoreResult` mutex.
- HTTP handler and `write_snapshot()` go through the new handle, so
  the response body and the shutdown file land with the live
  registry contents.

### Tests

- `tests/process.rs`: new
  `collect_snapshots_reports_running_pipe_process` (`Start` →
  assert full metadata in snapshot) and
  `collect_snapshots_returns_seeded_entries`.
- `tests/pty.rs`: new
  `collect_pty_snapshots_reports_allocated_pty` (`Start(pty)` →
  assert in `ptys`, NOT in `processes`; resize updates snapshot).
- `tests/filesystem.rs`: new
  `collect_watch_snapshots_reports_active_watcher` (`CreateWatcher`
  → assert in snapshot, `RemoveWatcher` clears it) and
  `collect_watch_snapshots_returns_seeded_entries`.
- `tests/binary_smoke.rs`: new
  `live_state_reports_started_process` — drive a `Process.Start` via
  the `ProcessClient` gRPC, probe `/__state` over raw TCP, assert
  the spawned pid + cmd + args + non-zero `started_at` land in the
  body. The original parse-only smoke is preserved.
- `tests/lifecycle.rs`: the "bridge across the Plan 05 wiring gap"
  workaround at line 266 is gone. Phase 5 now calls
  `server.process_handle.collect_process_snapshots()` /
  `collect_pty_snapshots()` directly. `BootedServer` carries cheap
  `process_handle` / `filesystem_handle` clones for this purpose.
  Phase 8's assertion that the *restored* registries surface the
  pre-pause pids still passes — `sleep_pid` lands in `processes`,
  `bash_pid` in `ptys`, exactly as the snapshot intended.

## Files touched

| File | Change |
|------|--------|
| `crates/envd/src/process.rs` | Extend `ProcessEntry` with pty/start-time metadata; derive `Clone` on `ProcessService`; add `with_seed` + `collect_process_snapshots` + `collect_pty_snapshots`; resize mirrors into snapshot fields |
| `crates/envd/src/filesystem.rs` | Derive `Clone` on `FilesystemService`; add `with_seed` + `collect_watch_snapshots`; `WatcherEntry` carries `path` + numeric `subscriber_id` |
| `crates/envd/src/bin/envd.rs` | Retire `SnapshotState`; replace with `SnapshotHandle`; `load_boot_seed` feeds services via `with_seed`; HTTP handler + shutdown writer walk live services |
| `crates/envd/tests/process.rs` | New live + seeded `collect_process_snapshots` tests |
| `crates/envd/tests/pty.rs` | New live `collect_pty_snapshots` test + resize-updates-snapshot assertion |
| `crates/envd/tests/filesystem.rs` | New live + seeded `collect_watch_snapshots` tests |
| `crates/envd/tests/binary_smoke.rs` | New `live_state_reports_started_process` end-to-end smoke via real `ProcessClient` + raw-TCP `/__state` probe |
| `crates/envd/tests/lifecycle.rs` | Drop Plan-05 wiring-gap workaround; `BootedServer` carries service handles; phase 5 walks live registries |

## Out of scope

- Resume-side rehydration of the live services from the seeded
  snapshots (i.e. synthesizing ghost OS processes on boot). The seed
  is informational only; the gVisor adapter's `runsc restore` is
  what re-mints runtime-side processes.
- A pause/resume smoke that asserts envd-side state survives
  end-to-end. Gated on (a) the deferred half of #170 staging
  `envd-state.json` into the resumed Pod's filesystem and (b) the
  full resume orchestration past #171. File separately when both
  land.

## Validation

| Gate | Result |
|------|--------|
| `just fmt` (auto-formatted on touch) | ok |
| `cargo clippy --workspace --all-targets -- -D warnings` | ok |
| `just test` | ok — envd's 3 binary-smoke / 7 process / 5 pty / 13 fs / 5 lifecycle suites green, full workspace green |
