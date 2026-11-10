# Plan 04 T10 — Exec WebSocket bridge (WS public, gRPC internal)

**Scope:** issue #109 — implement `GET /sandboxes/:id/exec[?pty=true]`,
a WebSocket endpoint that bridges JSON frames to envd's process gRPC
surface.

## What changed

- Rewrote `crates/sandbawks-api/src/routes/exec.rs` from a placeholder
  to a full WS↔gRPC bridge:
  - `ExecQuery { pty: bool }` query extractor.
  - `ClientFrame` (`Start | Stdin | Resize | Signal`) and `ServerFrame`
    (`Stdout | Stderr | Exit | Error`) tagged on `kind`.
  - Handler `exec(State, Extension<TeamContext>, Path<id>, Query, WS)`
    resolves the sandbox via `state.sandbox_ops().get(...)`, reads
    `status.envd_address` (502 with `envd_not_ready` if missing), and
    upgrades the socket. Gated on the `sandboxes:exec` scope.
  - `run_exec` splits the socket, spawns a single writer task that
    serializes every server frame, and runs `drive` for the bridge
    handshake.
  - `drive` reads the first WS frame (must be `Start`), calls
    `ProcessClient::start(...)`, consumes the first `StartEvent` for
    the pid, then forks an inbound pump (WS → `SendInput`/
    `SendSignal`/`Update`) and an outbound pump
    (`StartResponse` stream → WS frames).
  - Either half closing aborts the other and drops the outbound
    channel, which flushes the writer and closes the WS.
- Wired `routes::exec::router(state.clone())` into the binary in
  `crates/sandbawks-api/src/bin/sandbawks-api.rs`.

## Adapter notes

The issue spec sketched `mpsc::channel::<StartRequest>(16)` and
`process.start(ReceiverStream<StartRequest>)` as if `Process.Start`
were bidirectional. The vendored E2B proto defines `Start` as
server-streaming only; bidi input is a separate `StreamInput` RPC,
and per-message control uses the unary `SendInput`, `SendSignal`,
and `Update` RPCs. The implementation follows the actual proto — the
module docs in `exec.rs` explain the deviation.

Signal wire format only supports `SIGTERM` and `SIGKILL` (the only
values in the proto's `Signal` enum). Anything else returns a
`ServerFrame::Error` and keeps the bridge alive.

PTY plumbing is wired on the gRPC side (`Pty` attached to
`StartRequest`, `Update` for resize, `ProcessInput::Pty` for stdin)
but end-to-end PTY validation lands in T15.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — full workspace passes (no new tests required by the
  issue; T15 covers the end-to-end exec round-trip). The new module
  ships eight unit tests covering frame (de)serialization and
  `build_start_request` branches.

## Follow-ups

- T15 will add an integration test that drives the bridge against a
  real envd process running under `tokio::spawn`.
- A future revision can add base64 framing for binary-safe stdio if
  any SDK consumer needs it; today's wire format is UTF-8-lossy.
