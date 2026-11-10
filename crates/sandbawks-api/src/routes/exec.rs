//! `GET /sandboxes/:id/exec[?pty=true]` — WebSocket bridge that turns
//! the public JSON wire protocol into envd's `Process.Start` server-
//! streaming gRPC plus the unary `SendInput` / `SendSignal` / `Update`
//! RPCs.
//!
//! # Wire protocol
//!
//! Client → server frames (`kind` discriminator):
//!
//! ```json
//! {"kind": "start",  "cmd": "python", "args": ["-i"], "envs": {}}
//! {"kind": "stdin",  "data": "print(1)\n"}
//! {"kind": "resize", "cols": 80, "rows": 24}
//! {"kind": "signal", "signal": "SIGTERM"}
//! ```
//!
//! Server → client frames:
//!
//! ```json
//! {"kind": "stdout", "data": "..."}
//! {"kind": "stderr", "data": "..."}
//! {"kind": "exit",   "code": 0}
//! {"kind": "error",  "message": "..."}
//! ```
//!
//! # Adapter shape
//!
//! The issue spec sketched `mpsc::channel::<StartRequest>(16)` and
//! `process.start(ReceiverStream<StartRequest>)` as if `Process.Start`
//! were bidirectional. The vendored E2B proto (`crates/envd/proto/
//! process/process.proto`) actually defines `Start` as **server-
//! streaming only** (one `StartRequest` in, a stream of events out).
//! The bidirectional input channel is a separate `StreamInput` RPC,
//! and per-message control uses the unary `SendInput`, `SendSignal`,
//! and `Update` RPCs.
//!
//! We follow the actual proto:
//!
//! 1. The first WS frame **must** be `Start`. We synthesize a single
//!    [`StartRequest`] and call [`ProcessClient::start`]. The returned
//!    server-stream of [`envd::proto::process::StartResponse`] events
//!    drives outbound WS traffic for the lifetime of the connection.
//! 2. The first event from that stream is always
//!    `ProcessEvent::Start { pid }`. We capture the pid and use it as
//!    the [`ProcessSelector`] for every subsequent unary RPC.
//! 3. A WS-reader task pumps inbound `Stdin` frames into
//!    `SendInput`, `Signal` frames into `SendSignal`, and (when
//!    `pty=true`) `Resize` frames into `Update`. The two halves are
//!    joined so closing either side tears the other one down.
//!
//! PTY plumbing on the gRPC side is wired here, but end-to-end
//! validation against a real PTY-attached envd lands in T15.
//! `Resize` frames are silently ignored when `pty=false`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::Response;
use axum::routing::get;
use axum::{Extension, Router};
use envd::proto::process::{
    process_event::{data_event, Event as ProcessEventKind},
    process_input, process_selector,
    pty::Size as PtySize,
    ProcessConfig, ProcessInput, ProcessSelector, Pty, SendInputRequest, SendSignalRequest, Signal,
    StartRequest, StartResponse, UpdateRequest,
};
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::Streaming;

use crate::{
    auth::{auth_middleware, TeamContext},
    envd_client::{EnvdPool, EnvdProcessClient},
    error::AppError,
    AppState,
};

/// Buffer for the per-connection server→client frame channel. The
/// outbound side serializes through this `mpsc` so the WS-reader task
/// and the gRPC-reader task never race the underlying `WebSocket`
/// sink — only the writer task ever calls `.send()` on the WS.
///
/// 32 frames is comfortably above the burst we see when a process
/// flushes a buffered line plus the trailing `EndEvent`, while keeping
/// memory bounded if the client is slow to drain.
const OUTBOUND_BUFFER: usize = 32;

/// Default PTY size when the client opens with `pty=true` but hasn't
/// sent a `Resize` frame yet. Matches the xterm convention upstream
/// E2B uses for their `Pty.Connect` default.
const DEFAULT_PTY_COLS: u32 = 80;
const DEFAULT_PTY_ROWS: u32 = 24;

/// How often an open exec/pty WebSocket session refreshes the sandbox's
/// idle-timeout activity stamp (issue #481).
///
/// An interactive session that is connected but momentarily silent (the user
/// is reading, a long command is blocked) still counts as active, so a
/// heartbeat keeps the idle clock fresh for the life of the socket — mirroring
/// the keepalive-driven stamping on the Connect-RPC `process.Start` path.
/// `note_activity` is throttled, so this is at most one status patch per
/// interval per open session.
const EXEC_ACTIVITY_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(10);

/// Query string for the WS upgrade. The only knob today is whether
/// the spawned process gets a PTY allocated; everything else is
/// derived from the inbound `Start` frame.
#[derive(Debug, Default, Deserialize)]
pub struct ExecQuery {
    /// When `true`, the upstream `Process.Start` request includes a
    /// `Pty` block and the resulting process runs under a controlling
    /// TTY; `Resize` frames are honored. When `false` (default),
    /// `Resize` frames are silently dropped.
    #[serde(default)]
    pub pty: bool,
}

/// Frames the client sends to the gateway over the WebSocket.
///
/// Tagged with `kind`; `serde` lowercases the variant names so the
/// wire shape matches the protocol documented at the top of this
/// module without extra `rename` attributes.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientFrame {
    /// Initial frame; must be the first frame on the socket. Carries
    /// the process configuration that becomes a single
    /// [`StartRequest`] sent to envd.
    Start {
        /// Executable path or command name (resolved via envd's PATH).
        cmd: String,
        /// Positional argv tail. Empty `Vec` if omitted.
        #[serde(default)]
        args: Vec<String>,
        /// Extra environment overlay applied on top of the sandbox's
        /// base env. Missing → empty map.
        #[serde(default)]
        envs: HashMap<String, String>,
        /// Optional working directory override (`process.config.cwd`
        /// in the proto). `None` → inherit envd's default cwd.
        #[serde(default)]
        cwd: Option<String>,
    },

    /// Stdin payload to forward to the running process. The string is
    /// UTF-8 encoded and converted to bytes for [`SendInputRequest`];
    /// callers needing arbitrary binary input must base64 outside and
    /// inside, which the MVP wire format doesn't yet expose.
    Stdin {
        /// Bytes to push to the child's stdin (or PTY master if
        /// `pty=true`).
        data: String,
    },

    /// PTY resize. Ignored when `pty=false`.
    Resize {
        /// New column count. Maps to `Pty.Size.cols`.
        cols: u32,
        /// New row count. Maps to `Pty.Size.rows`.
        rows: u32,
    },

    /// POSIX signal to deliver to the process. Only `SIGTERM` and
    /// `SIGKILL` are wire-supported today (mirroring the proto's
    /// `Signal` enum); any other name produces a server-side
    /// [`ServerFrame::Error`] frame.
    Signal {
        /// Signal name (`"SIGTERM"`, `"SIGKILL"`).
        signal: String,
    },
}

/// Frames the gateway sends back to the client.
///
/// Mirrors [`ClientFrame`]'s tagging convention so a thin generic
/// client can route by `kind` without per-variant logic.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Stdout chunk. Bytes are lossily decoded to UTF-8 — invalid
    /// sequences become `U+FFFD`. Callers that need byte-exact output
    /// will need a future base64 mode.
    Stdout {
        /// UTF-8 (lossy) view of the bytes envd produced.
        data: String,
    },

    /// Stderr chunk. Same UTF-8-lossy contract as `Stdout`.
    Stderr {
        /// UTF-8 (lossy) view of the bytes envd produced.
        data: String,
    },

    /// Terminal exit code. The corresponding `EndEvent.exit_code` is
    /// a signed 32-bit int because POSIX allows signal-truncation
    /// semantics (`128 + signo`, negative on some platforms).
    Exit {
        /// Process exit code as reported by envd.
        code: i32,
    },

    /// Sent for any condition that breaks the bridge: malformed
    /// inbound frame, unknown signal, lost gRPC stream, etc. The
    /// gateway closes the WebSocket immediately after emitting this.
    Error {
        /// Human-readable description; opaque to the SDK.
        message: String,
    },
}

/// `GET /sandboxes/:id/exec` handler — performs the WS upgrade and
/// hands the upgraded socket to [`run_exec`].
///
/// `WebSocketUpgrade` must be the last extractor in the signature
/// (axum requirement: it consumes the request to take ownership of
/// the upgrade headers). All earlier extractors are exactly the ones
/// the issue spec calls for.
#[tracing::instrument(skip_all, name = "handler:exec::exec")]
async fn exec(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
    Query(query): Query<ExecQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    if !team.allows("sandboxes:exec") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:exec required",
        ));
    }

    // Resolve the sandbox up front so a 404 / 502 surfaces as a normal
    // HTTP error envelope instead of an opaque WS handshake failure.
    let sandbox = state.sandbox_ops().get(&team.namespace, &id).await?;
    let envd_address = sandbox.status.and_then(|s| s.envd_address).ok_or_else(|| {
        AppError::upstream(
            "envd_not_ready",
            format!("sandbox {id} has no envd address yet"),
        )
    })?;

    // Opening an exec/pty session is activity (issue #481), and it stays
    // active for the life of the socket. Stamp now, then keep stamping on a
    // heartbeat (below) so a long-lived interactive session — connected but
    // momentarily silent — is not paused out from under the client. Unlike
    // the Connect-RPC proxy this WS surface has no per-request seam, so the
    // heartbeat is how "open stream counts as active" is honored here.
    state.note_activity(&team.namespace, &id);

    let pool = state.envd_pool.clone();
    let pty = query.pty;
    let activity_state = state.clone();
    let activity_id = id.clone();
    let activity_namespace = team.namespace.clone();
    Ok(ws.on_upgrade(move |socket| async move {
        // Refresh the idle stamp until the session ends. `note_activity` is
        // throttled and fire-and-forget, so an idle tick is cheap; the task is
        // aborted the moment `run_exec` returns (client disconnect / process
        // exit), so it never outlives the socket.
        let heartbeat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(EXEC_ACTIVITY_HEARTBEAT);
            loop {
                tick.tick().await;
                activity_state.note_activity(&activity_namespace, &activity_id);
            }
        });
        run_exec(socket, pool, envd_address, pty).await;
        heartbeat.abort();
    }))
}

/// Top-level WS driver: split the socket, launch the writer task,
/// run [`drive`], and clean up.
///
/// All terminal conditions (`EndEvent`, gRPC error, WS close, decode
/// failure) funnel through the outbound `mpsc` so the WS half always
/// sees a final `Exit` or `Error` frame before the socket is closed.
async fn run_exec(socket: WebSocket, pool: EnvdPool, addr: String, pty: bool) {
    let (mut ws_tx, ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<ServerFrame>(OUTBOUND_BUFFER);

    // Spawn the WS-writer task immediately. Both pumps send every
    // server frame through `out_tx`; the writer is the only place
    // that touches `ws_tx`, which keeps frame ordering deterministic
    // and gives us a single shutdown point.
    let writer = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            let payload = match serde_json::to_string(&frame) {
                Ok(s) => s,
                Err(err) => {
                    tracing::error!(error = %err, "encoding ServerFrame failed");
                    break;
                }
            };
            if ws_tx.send(Message::Text(payload)).await.is_err() {
                break;
            }
        }
        let _ = ws_tx.close().await;
    });

    if let Err(err) = drive(ws_rx, pool, addr, pty, out_tx.clone()).await {
        // `drive` only returns on terminal failure — the success path
        // shuts itself down via `Exit` and a clean channel close.
        let _ = out_tx
            .send(ServerFrame::Error {
                message: err.to_string(),
            })
            .await;
    }

    // Dropping `out_tx` flushes the writer, which then closes the WS.
    drop(out_tx);
    let _ = writer.await;
}

/// Orchestrate the post-upgrade exchange: handshake `Start`, spawn
/// the inbound pump, pump outbound events until the stream ends.
///
/// Returns `Err` only for conditions the writer hasn't already seen
/// (so [`run_exec`] can map them to a final `Error` frame).
async fn drive(
    mut ws_rx: SplitStream<WebSocket>,
    pool: EnvdPool,
    addr: String,
    pty: bool,
    out_tx: mpsc::Sender<ServerFrame>,
) -> Result<(), BridgeError> {
    let start = await_start_frame(&mut ws_rx).await?;
    let mut client = pool
        .process(&addr)
        .await
        .map_err(|err| BridgeError::Upstream(format!("envd connect: {err}")))?;

    let start_request = build_start_request(start, pty);
    let mut stream = client
        .start(tonic::Request::new(start_request))
        .await
        .map_err(|err| BridgeError::Upstream(format!("Process.Start: {err}")))?
        .into_inner();

    let pid = read_pid(&mut stream).await?;

    // Two clones of the channel-backed client: the inbound pump owns
    // one for unary RPCs (`SendInput`, `SendSignal`, `Update`); we
    // keep the original for the streaming response we already opened.
    // `ProcessClient` is `Clone` over a `Clone` channel — both share
    // the pooled h2 connection.
    let input_client = client.clone();
    let inbound = tokio::spawn(pump_inbound(ws_rx, input_client, pid, pty, out_tx.clone()));

    let outbound_result = pump_outbound(&mut stream, &out_tx).await;
    inbound.abort();
    outbound_result
}

/// Terminal error inside the bridge. Maps to a final
/// [`ServerFrame::Error`] before the WS is closed.
#[derive(Debug, thiserror::Error)]
enum BridgeError {
    /// Client sent a non-`Start` frame as the first message, or the
    /// socket closed before any frame arrived.
    #[error("expected start frame: {0}")]
    Handshake(String),

    /// Inbound frame failed to deserialize.
    #[error("decode: {0}")]
    Decode(String),

    /// Anything upstream (envd connect, gRPC call, stream poll).
    #[error("upstream: {0}")]
    Upstream(String),
}

/// Read messages off the WebSocket until we get a JSON `Start` frame.
/// Non-text messages (binary, close) are treated as a protocol
/// violation — the contract is "JSON text frames only".
async fn await_start_frame(ws_rx: &mut SplitStream<WebSocket>) -> Result<ClientFrame, BridgeError> {
    let msg = ws_rx
        .next()
        .await
        .ok_or_else(|| BridgeError::Handshake("socket closed before first frame".into()))?
        .map_err(|err| BridgeError::Handshake(format!("ws error: {err}")))?;
    let text = match msg {
        Message::Text(s) => s,
        Message::Close(_) => {
            return Err(BridgeError::Handshake("client closed before start".into()))
        }
        _ => {
            return Err(BridgeError::Handshake(
                "first frame must be text JSON".into(),
            ))
        }
    };
    let frame: ClientFrame =
        serde_json::from_str(&text).map_err(|err| BridgeError::Decode(err.to_string()))?;
    if !matches!(frame, ClientFrame::Start { .. }) {
        return Err(BridgeError::Handshake(
            "first frame must be a Start frame".into(),
        ));
    }
    Ok(frame)
}

/// Translate a [`ClientFrame::Start`] into the gRPC [`StartRequest`].
///
/// Panics if `frame` is not a `Start` — callers gate on that via
/// [`await_start_frame`].
fn build_start_request(frame: ClientFrame, pty: bool) -> StartRequest {
    let ClientFrame::Start {
        cmd,
        args,
        envs,
        cwd,
    } = frame
    else {
        unreachable!("build_start_request called with non-Start frame");
    };
    StartRequest {
        process: Some(ProcessConfig {
            cmd,
            args,
            envs: envs.into_iter().collect(),
            cwd,
        }),
        // PTY block is only attached when the client asked for one;
        // initial size is xterm-default (24x80) and `Resize` frames
        // update it via the `Update` RPC.
        pty: pty.then_some(Pty {
            size: Some(PtySize {
                cols: DEFAULT_PTY_COLS,
                rows: DEFAULT_PTY_ROWS,
            }),
        }),
        // We don't assign a `tag`; the pid surfaced via the first
        // `StartEvent` is the only handle we need.
        tag: None,
        // `Some(true)` — we want a stdin pipe so `Stdin` frames have
        // somewhere to land. Defaults to `true` in the proto but is
        // declared `optional` for backwards compatibility, so we set
        // it explicitly.
        stdin: Some(true),
    }
}

/// Drain the first event off the `Start` stream and extract the pid.
/// envd always sends `ProcessEvent::Start { pid }` first, before any
/// data — see `crates/envd/src/process.rs`'s `subscribe_*` helpers.
async fn read_pid(stream: &mut Streaming<StartResponse>) -> Result<u32, BridgeError> {
    let event = stream
        .next()
        .await
        .ok_or_else(|| BridgeError::Upstream("Start stream closed before pid".into()))?
        .map_err(|err| BridgeError::Upstream(format!("Start stream: {err}")))?;
    match event.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(start)) => Ok(start.pid),
        Some(ProcessEventKind::End(end)) => Err(BridgeError::Upstream(format!(
            "process exited before pid: code={} status={}",
            end.exit_code, end.status,
        ))),
        _ => Err(BridgeError::Upstream(
            "first Start event was not a StartEvent".into(),
        )),
    }
}

/// Pump WebSocket → envd. Owned by a spawned task so the outbound
/// pump can `.abort()` it on stream end.
///
/// Every loop iteration handles exactly one inbound WS message. Any
/// per-frame failure (decode error, unknown signal, transient gRPC
/// error) is reported as a [`ServerFrame::Error`] but does **not**
/// terminate the bridge — the client may keep streaming valid frames
/// afterwards. Only a closed socket breaks the loop.
async fn pump_inbound(
    mut ws_rx: SplitStream<WebSocket>,
    mut client: EnvdProcessClient,
    pid: u32,
    pty: bool,
    out_tx: mpsc::Sender<ServerFrame>,
) {
    while let Some(msg) = ws_rx.next().await {
        let text = match msg {
            Ok(Message::Text(s)) => s,
            Ok(Message::Close(_)) | Err(_) => break,
            // Ping/Pong are handled by axum's built-in machinery;
            // Binary frames are not part of our wire format and we
            // silently drop them rather than fault the bridge.
            Ok(_) => continue,
        };

        let frame: ClientFrame = match serde_json::from_str(&text) {
            Ok(f) => f,
            Err(err) => {
                let _ = out_tx
                    .send(ServerFrame::Error {
                        message: format!("decode: {err}"),
                    })
                    .await;
                continue;
            }
        };

        if let Err(err) = handle_client_frame(&mut client, pid, pty, frame).await {
            let _ = out_tx.send(ServerFrame::Error { message: err }).await;
        }
    }
}

/// Dispatch a single non-`Start` inbound frame.
///
/// Returns `Err(message)` for conditions worth surfacing to the
/// client (unknown signal, gRPC failure); the caller relays them as
/// an `Error` frame but keeps the bridge alive.
async fn handle_client_frame(
    client: &mut EnvdProcessClient,
    pid: u32,
    pty: bool,
    frame: ClientFrame,
) -> Result<(), String> {
    match frame {
        ClientFrame::Start { .. } => Err("duplicate start frame".into()),
        ClientFrame::Stdin { data } => {
            // For non-PTY processes, stdin bytes go to the child's
            // pipe; for PTY, they go to the master. The proto models
            // these as different `ProcessInput` variants so the
            // service can choose the right destination.
            let input = if pty {
                process_input::Input::Pty(data.into_bytes())
            } else {
                process_input::Input::Stdin(data.into_bytes())
            };
            client
                .send_input(tonic::Request::new(SendInputRequest {
                    process: Some(selector(pid)),
                    input: Some(ProcessInput { input: Some(input) }),
                }))
                .await
                .map(|_| ())
                .map_err(|err| format!("SendInput: {err}"))
        }
        ClientFrame::Resize { cols, rows } => {
            // Silent no-op when the process wasn't started with a
            // PTY — there's no terminal to resize.
            if !pty {
                return Ok(());
            }
            client
                .update(tonic::Request::new(UpdateRequest {
                    process: Some(selector(pid)),
                    pty: Some(Pty {
                        size: Some(PtySize { cols, rows }),
                    }),
                }))
                .await
                .map(|_| ())
                .map_err(|err| format!("Update: {err}"))
        }
        ClientFrame::Signal { signal } => {
            let signo = parse_signal(&signal)?;
            client
                .send_signal(tonic::Request::new(SendSignalRequest {
                    process: Some(selector(pid)),
                    signal: signo as i32,
                }))
                .await
                .map(|_| ())
                .map_err(|err| format!("SendSignal: {err}"))
        }
    }
}

/// Build a pid-based [`ProcessSelector`].
fn selector(pid: u32) -> ProcessSelector {
    ProcessSelector {
        selector: Some(process_selector::Selector::Pid(pid)),
    }
}

/// Map a wire signal name to the proto enum. The proto only exposes
/// `SIGTERM` and `SIGKILL` today — `SIGINT` and friends are
/// intentionally unsupported because envd would have to round-trip
/// them through `Signal::SIGNAL_UNSPECIFIED`, which the upstream
/// service rejects.
fn parse_signal(name: &str) -> Result<Signal, String> {
    match name {
        "SIGTERM" => Ok(Signal::Sigterm),
        "SIGKILL" => Ok(Signal::Sigkill),
        other => Err(format!(
            "unsupported signal {other:?}; only SIGTERM/SIGKILL are wire-supported"
        )),
    }
}

/// Pump envd → WebSocket. Returns `Ok(())` on a clean `EndEvent`,
/// `Err(BridgeError)` for any upstream failure.
async fn pump_outbound(
    stream: &mut Streaming<StartResponse>,
    out_tx: &mpsc::Sender<ServerFrame>,
) -> Result<(), BridgeError> {
    while let Some(item) = stream.next().await {
        let response = item.map_err(|err| BridgeError::Upstream(format!("Start stream: {err}")))?;
        let Some(kind) = response.event.and_then(|e| e.event) else {
            continue;
        };
        match kind {
            // The first StartEvent was already consumed by `read_pid`;
            // any subsequent one is a protocol oddity worth ignoring
            // rather than terminating on.
            ProcessEventKind::Start(_) => continue,
            ProcessEventKind::Data(data) => {
                if let Some(output) = data.output {
                    let frame = match output {
                        data_event::Output::Stdout(bytes) => ServerFrame::Stdout {
                            data: String::from_utf8_lossy(&bytes).into_owned(),
                        },
                        data_event::Output::Stderr(bytes) => ServerFrame::Stderr {
                            data: String::from_utf8_lossy(&bytes).into_owned(),
                        },
                        // PTY output multiplexes stdout/stderr onto a
                        // single byte stream — we surface it as stdout
                        // so the SDK-side ordering is preserved.
                        data_event::Output::Pty(bytes) => ServerFrame::Stdout {
                            data: String::from_utf8_lossy(&bytes).into_owned(),
                        },
                    };
                    if out_tx.send(frame).await.is_err() {
                        break;
                    }
                }
            }
            ProcessEventKind::End(end) => {
                let _ = out_tx
                    .send(ServerFrame::Exit {
                        code: end.exit_code,
                    })
                    .await;
                return Ok(());
            }
            // KeepAlive is internal to the envd→gateway link; we
            // don't relay it to the client. A WS-side keepalive
            // belongs in the websocket layer, not the wire protocol.
            ProcessEventKind::Keepalive(_) => continue,
        }
    }
    Ok(())
}

/// Build the exec router wired with `auth_middleware`. Mirrors the
/// `Arc::clone(&state.key_store)` dance used by every other router so
/// the middleware-state and handler-state types stay independent.
pub fn router(state: AppState) -> Router {
    let key_store = Arc::clone(&state.key_store);
    Router::new()
        .route("/sandboxes/:id/exec", get(exec))
        .layer(axum::middleware::from_fn_with_state(
            key_store,
            auth_middleware,
        ))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_frame_round_trip_start() {
        let raw = r#"{"kind":"start","cmd":"python","args":["-i"],"envs":{}}"#;
        let frame: ClientFrame = serde_json::from_str(raw).expect("decode start");
        match frame {
            ClientFrame::Start {
                cmd,
                args,
                envs,
                cwd,
            } => {
                assert_eq!(cmd, "python");
                assert_eq!(args, vec!["-i".to_owned()]);
                assert!(envs.is_empty());
                assert!(cwd.is_none());
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn client_frame_round_trip_stdin() {
        let raw = r#"{"kind":"stdin","data":"print(1)\n"}"#;
        let frame: ClientFrame = serde_json::from_str(raw).expect("decode stdin");
        match frame {
            ClientFrame::Stdin { data } => assert_eq!(data, "print(1)\n"),
            _ => panic!("expected Stdin"),
        }
    }

    #[test]
    fn client_frame_round_trip_resize() {
        let raw = r#"{"kind":"resize","cols":120,"rows":40}"#;
        let frame: ClientFrame = serde_json::from_str(raw).expect("decode resize");
        match frame {
            ClientFrame::Resize { cols, rows } => {
                assert_eq!(cols, 120);
                assert_eq!(rows, 40);
            }
            _ => panic!("expected Resize"),
        }
    }

    #[test]
    fn client_frame_round_trip_signal() {
        let raw = r#"{"kind":"signal","signal":"SIGTERM"}"#;
        let frame: ClientFrame = serde_json::from_str(raw).expect("decode signal");
        match frame {
            ClientFrame::Signal { signal } => assert_eq!(signal, "SIGTERM"),
            _ => panic!("expected Signal"),
        }
    }

    #[test]
    fn server_frame_serializes_to_expected_shape() {
        let stdout =
            serde_json::to_value(ServerFrame::Stdout { data: "hi".into() }).expect("encode stdout");
        assert_eq!(stdout["kind"], "stdout");
        assert_eq!(stdout["data"], "hi");

        let exit = serde_json::to_value(ServerFrame::Exit { code: 0 }).expect("encode exit");
        assert_eq!(exit["kind"], "exit");
        assert_eq!(exit["code"], 0);

        let err = serde_json::to_value(ServerFrame::Error {
            message: "boom".into(),
        })
        .expect("encode error");
        assert_eq!(err["kind"], "error");
        assert_eq!(err["message"], "boom");
    }

    #[test]
    fn parse_signal_known_and_unknown() {
        assert!(matches!(parse_signal("SIGTERM"), Ok(Signal::Sigterm)));
        assert!(matches!(parse_signal("SIGKILL"), Ok(Signal::Sigkill)));
        assert!(parse_signal("SIGINT").is_err());
    }

    #[test]
    fn build_start_request_attaches_pty_when_requested() {
        let frame = ClientFrame::Start {
            cmd: "bash".into(),
            args: vec![],
            envs: HashMap::new(),
            cwd: None,
        };
        let req = build_start_request(frame, true);
        assert!(req.pty.is_some(), "pty=true must populate the Pty block");
        let size = req.pty.unwrap().size.expect("default size");
        assert_eq!(size.cols, DEFAULT_PTY_COLS);
        assert_eq!(size.rows, DEFAULT_PTY_ROWS);
        assert_eq!(req.stdin, Some(true));
    }

    #[test]
    fn build_start_request_skips_pty_when_disabled() {
        let frame = ClientFrame::Start {
            cmd: "true".into(),
            args: vec![],
            envs: HashMap::new(),
            cwd: None,
        };
        let req = build_start_request(frame, false);
        assert!(req.pty.is_none(), "pty=false must omit the Pty block");
    }

    #[test]
    fn exec_query_defaults_to_no_pty() {
        let q = ExecQuery::default();
        assert!(!q.pty);
    }
}
