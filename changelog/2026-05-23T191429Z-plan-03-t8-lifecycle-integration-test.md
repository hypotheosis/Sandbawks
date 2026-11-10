# Plan 03 T8 — Full-lifecycle integration test for `envd`

**Date:** 2026-05-23T19:14:29Z
**Issue:** #61
**Branch:** `feat-issue-61-lifecycle-test`

## Summary

Lands the end-to-end integration test the Plan 03 spec asks for: one
test that exercises every envd surface against an in-process tonic
server and proves that the pause/resume snapshot survives a
save/restart round-trip.

The test boots a real `tonic::transport::Server` on
`127.0.0.1:0`, drives the Filesystem and Process services through
generated gRPC clients over a real TCP channel, captures bookkeeping
into an `EnvdState`, writes it to a tempfile, drops the server, boots
a *second* server, restores the registries, and asserts that the
pids of the sleep + bash children both survive the round-trip.

## Files changed

- `crates/envd/Cargo.toml` — adds the `net` feature to the dev-deps
  `tokio` and `tokio-stream` declarations so the test can bind a
  tokio `TcpListener` on `127.0.0.1:0` and feed it through
  `TcpListenerStream` into
  `Server::serve_with_incoming_shutdown`. The production binary does
  not need `net` directly because tonic enables it transitively.
- `crates/envd/tests/lifecycle.rs` — new ~360-line test file.

## What the test covers

1. **Boot** — tokio `TcpListener` binds an ephemeral port; tonic serves
   `FilesystemServer` + `ProcessServer` on it via
   `serve_with_incoming_shutdown`. The function returns only after the
   listener is live, so client `connect_lazy()` channels never race
   the bind.
2. **Process flow** — `Start("sleep", &["60"])` → assert pid via
   `StartEvent`; `Connect(pid)` → drain one poll to prove the stream
   opens; `List()` → assert pid is reported.
3. **Filesystem flow** — `MakeDir(/wares)` → `Stat(/wares)` →
   `ListDir(/)`. (See "Scope deviations" below for why this isn't
   `Read`/`Write`.)
4. **PTY flow** — `Process.Start` with the `pty` field set to a 24×80
   PTY running `bash --noprofile --norc -i`; `SendInput::Pty("echo
   $((40+2))\\n")`; expect `42` in the merged tty output within 5 s.
5. **State save** — build registries from the live `Process.List()`
   output, capture an `EnvdState`, write it to a tempfile via
   `spawn_blocking(EnvdState::write_to)`.
6. **Teardown + restart** — SIGKILL the live children, drop both
   clients, send shutdown via the one-shot, await the first serve
   task. Then boot a *second* server (same filesystem root, fresh
   ProcessService).
7. **Verify** — on the second server:
    - Live `Process.List()` is empty (the children died with the first
      server, by design).
    - The filesystem dir `/wares` is still visible (on-disk fs state
      survives independently of the service).
    - `EnvdState::read_from` + `EnvdState::restore` rehydrates the
      ProcessRegistry and PtyRegistry — both contain the original
      pids.

## Scope deviations from the issue text

The issue's "Implementation notes" section describes APIs that don't
match the actual envd surface area. I called these out in module docs
on the test file and adapted as follows:

- **Filesystem `Write`/`Read`** — the upstream `filesystem.proto` is
  metadata-only (`Stat`, `MakeDir`, `Move`, `ListDir`, `Remove`,
  watcher trio). File *content* flows through HTTP presigned URLs
  minted by `sandbawks-api` in Plan 04; extending the proto here
  would diverge from the published E2B SDKs. The substitute
  coverage is `MakeDir(/wares)` → `Stat(/wares)` → `ListDir(/)`,
  which exercises the same `resolve`/canonicalize path that a
  hypothetical Read/Write would.
- **Separate PTY service** — upstream E2B folds PTY into
  `Process.Start` via the optional `pty` field, so there is no
  `PtyServiceClient`. The test drives the PTY surface with the same
  `ProcessClient` it uses for pipe-backed processes.
- **`Process.List()` on the restored server surfacing the original
  pid** — the live `ProcessService` does not auto-populate snapshot
  registries today (Plan 05 wires that). The test bridges the gap by
  populating registries from the live `List()` output before
  `capture`, and asserts pid survival on the **restored registries**
  post-restart rather than on the new live `List()` (which correctly
  starts empty because `kill_on_drop` reaps children when the first
  server's services drop).

## Why a multi-thread runtime

The issue spec calls for `tokio::test(flavor = "multi_thread",
worker_threads = 4)`. Tonic's HTTP/2 server needs at least the
acceptor + worker tasks plus per-RPC reader/writer tasks. A
current-thread runtime would serialize all of those and turn the
PTY echo round-trip into a livelock waiting on `read_pty_until`.

## Validation

- `just fmt` — applied.
- `cargo clippy -p envd --tests -- -D warnings` — silent.
- `cargo test -p envd --test lifecycle` — green on Linux.
- Wall-clock runtime: well under the 30 s ceiling the issue calls
  out (typically ~1–2 s on a warm cache; the outer `timeout` is
  pinned to 25 s as belt-and-suspenders for CI).

## Out of scope (per the issue)

- Pause/resume against a real runtime (Plan 05 — gVisor adapter).
- E2B SDK against envd (Plan 06).
