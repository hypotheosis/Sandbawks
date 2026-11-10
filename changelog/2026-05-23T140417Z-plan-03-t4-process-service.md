# Plan 03 T4 — envd Process service implementation

Date: 2026-05-23

Issue: #57
Epic: #53 — Plan 03 envd-rs

## What landed

Replaced the empty `crates/envd/src/process.rs` stub from T1 with a
working `ProcessService` that implements the full upstream E2B
`process` gRPC surface (non-PTY exec):

- `Start` — spawns the child via `tokio::process::Command`, returns a
  server-streaming `StartResponse` whose first message is the
  `StartEvent { pid }` and which then forwards stdout/stderr chunks
  and the terminating `EndEvent`.
- `Connect` — server-streaming subscriber to an already-running (or
  recently-exited) process; race-free against the wait task via the
  subscribe-then-snapshot pattern documented in the module.
- `SendInput` — writes to the child's stdin pipe (only if the client
  opted in at `Start`).
- `SendSignal` — proto `Signal` → `nix::sys::signal::Signal`;
  delivered via `nix::sys::signal::kill` against the registered pid
  only (never an arbitrary host pid).
- `List` — returns running + recently-exited processes (60 s
  retention window).
- `Update` — no-op placeholder per the issue's scope; reserved for
  future cgroup hooks.
- `StreamInput` and `CloseStdin` — implemented for proto-completeness;
  `StreamInput` dispatches into the same stdin-forwarding pipe.

## Design notes

- **Registry**: `Arc<tokio::sync::Mutex<HashMap<u32, Arc<ProcessEntry>>>>`
  keyed by OS pid. Each `ProcessEntry` owns a
  `broadcast::Sender<ProcessEvent>` (cap 256), an
  `Option<mpsc::Sender<Vec<u8>>>` for stdin, and a cached
  `Mutex<Option<ProcessEvent>>` carrying the final `EndEvent`.
- **Fan-out**: `Start` and `Connect` both call `build_event_stream`,
  which subscribes to the broadcast *before* it snapshots the exit
  cache. The wait task's invariant — cache the end event, *then*
  publish on broadcast — makes every interleaving correct: late
  subscribers see the cached end; live subscribers receive it via
  broadcast.
- **Reader tasks**: stdout/stderr are read in 4 KiB byte chunks
  rather than line-by-line so non-UTF8 payloads and partial-line
  writes (e.g., `printf "hi"`) survive. The proto's `bytes` typing
  on the data event makes this the byte-faithful representation; the
  issue text's "line-by-line" hint is honored in spirit but not in
  literal API choice.
- **Drain-before-end**: the wait task awaits the stdout/stderr
  reader handles *before* it publishes the `EndEvent`, so any final
  buffered output reaches subscribers before they see the exit. The
  `concurrent_connect_fanout` test asserts every subscriber observes
  the tail `three\n` chunk.
- **Retention**: a detached `tokio::time::sleep(60 s)` removes the
  entry after exit. `List` returns the entry during this window.
- **Privilege guard on signals**: `SendSignal` requires the pid to
  exist in our registry. Direct `kill(2)` against an arbitrary host
  pid is rejected with `NotFound` — signalling a pid we don't own
  would be a privilege-escalation dressed as a typo.

## Workspace deps touched

`crates/envd/Cargo.toml`:

- `tokio` gains the `process`, `rt`, and `time` features (the lib
  spawns reader/wait tasks and sleeps the retention timer).
- `tokio-stream` gains the `sync` feature for
  `wrappers::ReceiverStream`.
- New direct dep on `nix` (workspace pin) for
  `nix::sys::signal::kill`.

The dev-deps profile already had `macros` / `rt-multi-thread` /
`time` from T3; `process` was added so the integration tests can
spawn `echo`, `cat`, `sleep`, and `true`.

## Tests

Integration tests under `crates/envd/tests/process.rs`. All six pass:

- `echo_hello_world` — happy path; `Stdout("hello\n")` then exit 0.
- `late_connect_observes_exit` — 200 ms after a `sleep 0.1` exits,
  a fresh `Connect` still observes the cached `EndEvent`.
- `stdin_pipe` — `cat` echoes injected stdin; `SIGTERM` produces an
  end event.
- `concurrent_connect_fanout` — three subscribers all see the tail
  of the same stdout stream and the same exit.
- `send_signal_kills` — `SIGKILL` on `sleep 60` produces an end
  event within 3 s.
- `list_includes_running_and_recent_exits` — `List` reports both a
  running `sleep 10` and a `true` that already exited.

## Validation

- `just fmt` ✓ (no diff)
- `cargo clippy --workspace --all-targets -- -D warnings` ✓
- `just test` ✓ — full workspace green, including the six new
  `process.rs` tests.

## Out of scope (deferred)

- **PTY allocation** — sibling `pty.rs` module; Plan 03 T5 (#58).
- **cgroup / resource updates** — `Update` is intentionally a no-op
  per the issue scope.
- **`exit_signal` over the wire** — proto's `EndEvent` has no
  dedicated signal field; signal-terminated processes report
  `exit_code = -1` and `status = "terminated by signal N"`. This
  matches what upstream E2B does on the JSON-over-HTTP boundary
  today.
