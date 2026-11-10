# Plan 03 T3: `FilesystemService` impl

**Issue:** #56 — Plan 03 T3.
**Parent:** #53 — Plan 03 epic.
**Predecessor:** #55 (PR
#86) — landed the vendored
`.proto` schemas + `tonic-build` codegen this task implements against.

## What changed

- **`crates/envd/src/filesystem.rs`** — fully implements the
  `filesystem.Filesystem` gRPC service:
  - **`Stat`** — `tokio::fs::symlink_metadata` → `EntryInfo`. Uses the
    symlink-aware metadata call so `stat("/some/symlink")` reports the
    link itself rather than its target; `entry.symlink_target` is
    populated from `read_link` only for symlinks.
  - **`MakeDir`** — `tokio::fs::create_dir_all`. The vendored
    `MakeDirRequest` has no `recursive` field; `create_dir_all`
    matches the upstream E2B SDK semantics (`files.makeDir` creates
    parent dirs as needed).
  - **`Move`** — `tokio::fs::rename`.
  - **`ListDir`** — `tokio::fs::read_dir` over the resolved path, one
    `EntryInfo` per child. Per-entry metadata reads that race a
    concurrent delete are skipped (the listing must not fail as a
    whole because one child vanished mid-iteration).
  - **`Remove`** — checks `symlink_metadata` then dispatches to
    `remove_file` / `remove_dir`. Non-recursive for directories per
    the issue scope; recursive delete is out of scope for MVP.
  - **`WatchDir` (streaming)** — backed by `notify::recommended_watcher`
    (inotify on Linux). Emits a `WatchDirResponse::Start` event first
    (so clients can synchronize "stream is live" before triggering
    filesystem activity), then forwards `notify::Event` instances
    mapped to the proto's coarser `EventType` enum. The watcher is
    held alive by a tokio task that listens on `tx.closed()`; when the
    client drops the stream, the task wakes and drops the watcher.
  - **`CreateWatcher` / `GetWatcherEvents` / `RemoveWatcher`
    (polling)** — the non-streaming trio shares a `HashMap<String,
    WatcherEntry>` keyed by an opaque id minted from an `AtomicU64`.
    Each watcher's callback appends to a `Vec<FilesystemEvent>`
    behind a `std::sync::Mutex` (sync, because `notify` callbacks run
    on its own thread and cannot `.await`); `GetWatcherEvents` drains
    the buffer with `mem::take`.
- **Path-traversal guard** lives in `FilesystemService::resolve`. It
  strips a leading `/` (so request paths can be sandbox-style absolute
  while the host-side mount differs), then canonicalizes the deepest
  existing ancestor and re-appends any missing trailing components.
  The result is rejected with `Status::permission_denied` unless it
  starts with the canonicalized service root. Tested by
  `path_traversal_rejected` with both `../etc/passwd` and
  `/../../../../etc/passwd`.
- **Per-RPC tests** under `crates/envd/tests/filesystem.rs` drive the
  `Filesystem` trait directly (no transport, no codec) against a
  tempdir root:
  - `list_nonempty_dir_returns_files_and_subdirs`
  - `stat_returns_size_and_mtime`
  - `remove_file_then_stat_returns_not_found`
  - `mkdir_recursive_creates_chain`
  - `move_renames_file`
  - `watch_observes_create_modify_delete` (poll-based trio)
  - `watch_dir_streams_start_then_filesystem_event` (streaming variant)
  - `path_traversal_rejected`
- **Dependencies** added to `crates/envd/Cargo.toml`:
  - `tokio` features `["fs", "io-util", "sync"]`,
  - `tokio-stream`, `futures`, `tracing`, `notify` (runtime),
  - `tempfile`, `tokio` test features (`["macros", "rt-multi-thread",
    "time", "fs", "io-util", "sync"]`), `tokio-stream` (dev).
- **Workspace `Cargo.toml`** gains `notify = "6"` so a future
  `crates/sandbawks-node` can pull from the same pin.

## Scope adjustment (vs. issue #56 body)

The issue body listed `Read(path) -> stream Chunk` and
`Write(stream WriteRequest) -> WriteResponse` in scope, plus a
`read_write_roundtrip` integration test. **These do not exist in the
vendored proto.** Upstream E2B's `filesystem.proto` is metadata-only;
file content moves through HTTP/presigned-URL endpoints minted by the
API gateway. The repo's vendored proto is pinned byte-identical to
upstream (see `proto/NOTICE` and `proto/SOURCE`) and adding RPCs would
silently break the published E2B Python/JS SDKs.

Resolution chosen with the user before implementation:

- This PR implements **only what's in the proto** — the eight metadata
  RPCs above.
- The Read/Write surface is **deferred to Plan 04**, where the
  `sandbawks-api` HTTP gateway will mint short-lived signed URLs that
  stream content into / out of envd's filesystem on the server side,
  per the design spec's §7 "API surface" table.
- The design spec §14 (decomposition note) has been updated to make
  this explicit so that when Plan 04 is decomposed into sub-issues,
  the file-content transfer work is properly captured.

The `read_write_roundtrip` test from the issue scope is dropped for
the same reason; the watch + traversal + metadata tests are kept
verbatim from the issue's checklist.

## Validation

- `just fmt-check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
  - One narrowly-scoped `#[allow(clippy::result_large_err)]` on the
    private `FilesystemService::resolve` helper, because every RPC
    handler in the service returns `Result<_, tonic::Status>` by trait
    contract and routing the helper's error through a `Box` would
    clash with that.
- `just test` — all suites green, including 8 new tests under
  `tests/filesystem.rs`.

## Out of scope (deferred to later Plan 03 tasks)

- `ProcessService` impl (T4) — `Start`, `Connect`, `SendInput`,
  `SendSignal`, `List`, `Update`.
- `PtyService` impl (T5) — via the `pty` re-exports + `nix::pty`.
- `envd-state.json` (re)serialization (T6) — pause/resume safety net
  for watcher subscriptions and in-flight watches.
- `envd` binary wire-up (T7).
- Recursive `ListDir` (`depth > 1`) — proto carries the field; the MVP
  honours `depth = 0/1` and treats larger values as 1.
- Recursive `Remove` for non-empty directories — out of MVP scope.
- File-content transfer (`Read`/`Write` of bytes) — deferred to Plan 04
  per the scope adjustment above.
