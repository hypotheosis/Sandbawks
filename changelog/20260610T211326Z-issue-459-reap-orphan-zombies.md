# fix(envd): reap adopted orphan zombies as PID 1 (issue #459)

**Date:** 2026-06-10

## Summary

`envd` runs as PID 1 in every sandbox container, so the kernel reparents every
orphaned grandchild onto it — e.g. the `rustc` / `cc` / `tail` workers a build
tool forks and then abandons when their immediate parent (`cargo`, `just`)
exits. PID 1 must `waitpid` those adopted orphans or they linger in the `Z`
(zombie) state, each holding a process-table slot. A single `cargo build` /
`just check` leaked ~10–15 such entries.

`envd` now runs a single background reaper task that owns the process's only
`waitpid(-1)` loop, draining every available zombie on each `SIGCHLD`.

## Problem

`envd` correctly reaped the processes it *directly* spawns — the per-process
wait task in `spawn_pipe_child` / `spawn_pty_child` awaited
`tokio::process::Child::wait`. But grandchildren reparented to PID 1 were
unknown to tokio: nothing waited on them, so they accumulated as zombies until
`envd` itself exited.

### Why the obvious fix is wrong

The naive fix — a background task that calls `waitpid(-1, WNOHANG)` on each
`SIGCHLD` — **regresses direct-child exit reporting.** tokio reaps the children
it spawns by `waitpid`-ing each specific pid (via a pidfd-readiness wake on
Linux 5.3+, or its internal `SIGCHLD` path otherwise) when you poll
`Child::wait`. Whoever calls `waitpid` first collects the status. A generic
`waitpid(-1)` reaper races tokio and, on the *normal* exit of a direct child
(`just` / `cargo`), wins — draining that child along with the grandchildren.
tokio's subsequent `waitpid(pid)` then returns `ECHILD`, `Child::wait` returns
`Err`, and `build_end_event` emits `exit_code: -1, exited: false,
error: "...ECHILD..."` instead of the real exit code — silently corrupting the
`Process.Start` contract for the very build commands this issue is about. tokio
documents exactly this incompatibility.

## Fix

Make the reaper the **sole** `waitpid` owner and route direct children's exit
statuses back to their wait tasks, so tokio never reaps (and never races).

### `crates/envd/src/process.rs`

- **`ChildReaper`** — a cloneable handle over
  `Arc<Mutex<HashMap<Pid, oneshot::Sender<ExitStatus>>>>` (a plain
  `std::sync::Mutex`; no holder ever `.await`s under it).
  - `drain()` loops `waitpid(-1, WNOHANG)` until no child is ready. A reaped
    pid that is registered is a direct child → its status is routed over the
    channel; an unregistered pid is an adopted orphan → reaped and discarded.
  - `route()` takes the registry lock per reaped pid; `registry()` is held by
    `spawn_*` across `Command::spawn`, so a child that exits before its sender
    is inserted is not misclassified as an orphan (route blocks on the guard
    until registration completes).
  - Reconstructs a `std::process::ExitStatus` from the decoded `WaitStatus`
    via `ExitStatusExt::from_raw`, re-encoding the raw `wait(2)` status word
    (exit code in bits 8..16; or signal in the low 7 bits with `0x80` for a
    core dump) so the existing `build_end_event` is unchanged.
- **`global_reaper()` + `static REAPER: OnceLock<ChildReaper>`** — the reaper is
  a **process-global singleton**. It must be: `waitpid(-1)` reaps *any* child,
  so two reapers in one process would each collect (and, finding it
  unregistered, discard) the other's direct children, hanging their wait tasks.
  On first call it starts the `waitpid` loop on a **dedicated thread** running
  its own current-thread tokio runtime, so the loop is independent of the main
  serving runtime — and, in tests, of each `#[tokio::test]`'s short-lived
  runtime. The loop drains, then awaits `SIGCHLD` (`recv` coalesces signals,
  hence the drain loop rather than one-reap-per-signal).
- **`ProcessService::with_seed`** obtains the handle from `global_reaper()`, so
  the service is self-contained — a `Start` RPC reaps correctly with no caller
  wiring. (This replaced an earlier `spawn_orphan_reaper(sigchld)` method whose
  exit-status routing only worked if the caller remembered to start it, which
  broke every `ProcessService` constructed without it.)
- **`spawn_pipe_child` / `spawn_pty_child`** now:
  - register the child's `oneshot` sender **under the reaper lock, held across
    `Command::spawn`** — closing the spawn/register race as above.
  - in the wait task, **hold the `Child`** (so tokio never orphan-reaps it on
    drop) and `await` the routed status instead of calling `child.wait()`. The
    held `Child` is dropped once the exit is recorded; `kill_on_drop`'s reap is
    then a harmless `ESRCH`/`ECHILD` no-op. If the wait task is cancelled, the
    drop SIGKILLs the child and the reaper collects the resulting zombie.

### `crates/envd/src/bin/envd.rs`

- No reaper wiring needed: the process-global reaper starts lazily when `run()`
  constructs the `ProcessService`. A comment records where it lives.

## Tests

- **Unit (`process.rs`):** `exit_status_from_code` / `exit_status_from_signal`
  round-trips — exit codes, signal terminations, and the core-dump flag survive
  the raw-`wait`-word reconstruction.
- **Integration (`tests/reaper.rs`, its own binary):** makes the test process a
  child subreaper (`PR_SET_CHILD_SUBREAPER`) so an orphaned grandchild reparents
  onto it (standing in for envd-as-PID-1), then asserts both that a direct
  child's exit code (`exit 7`) survives reaper routing and that an orphaned
  grandchild's zombie is collected within a timeout. It lives in a dedicated
  binary because `waitpid(-1)` would otherwise steal the tokio children of the
  sibling `process.rs` / `pty.rs` tests.

## Validation

- `just fmt`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `just test`

## Out of scope

- Rehydrating pre-pause process entries on resume (issue #178).
- Changing how direct children are reaped beyond the routing seam required to
  avoid the `waitpid` race.
