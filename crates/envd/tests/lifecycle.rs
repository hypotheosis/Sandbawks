//! Full-lifecycle integration test for envd (Plan 03 T8).
//!
//! One end-to-end scenario that exercises every envd surface against an
//! in-process tonic server and proves the snapshot state survives a
//! save/restart round-trip. Concretely:
//!
//! 1. Boots a tonic server on `127.0.0.1:0` with the Filesystem and
//!    Process services (PTY is folded into Process per the upstream
//!    proto — see [`envd::pty`]'s module docs).
//! 2. Drives each surface through generated gRPC clients over a real
//!    TCP channel.
//! 3. Captures bookkeeping into an [`EnvdState`], writes to a tempfile,
//!    drops the server (graceful shutdown via oneshot).
//! 4. Restarts a fresh server bound to a new port.
//! 5. Loads the snapshot, restores registries, and asserts the
//!    bookkeeping (pids of the sleep + bash children) survived.
//!
//! # Scope deviations from the issue text
//!
//! - **Filesystem read/write** is not part of the envd proto. The
//!   upstream `filesystem.proto` is metadata-only (`Stat`, `MakeDir`,
//!   `Move`, `ListDir`, `Remove`); file *content* flows through HTTP
//!   presigned URLs minted by `sandbawks-api` in Plan 04. The substitute
//!   coverage here is `MakeDir(/wares)` → `Stat(/wares)` → `ListDir(/)`,
//!   which exercises the resolve/canonicalize path the same way a
//!   hypothetical Write/Read would.
//! - **PTY service** does not exist as a separate gRPC service. The PTY
//!   flow drives `ProcessClient` with the `pty` field set on
//!   `StartRequest` (`Update` resizes, `SendInput::Pty(bytes)` writes
//!   to the master fd).
//! - **Live snapshot capture** uses each service's `collect_*_snapshots`
//!   walk-the-live-registry API (issue #174). Phase 5 here keeps a
//!   clone of the booted `ProcessService` / `FilesystemService` so it
//!   can capture directly from the same registries the gRPC server is
//!   serving, no manual `List()` → `ProcessRegistry` bridge.
//!   Post-restart, `Phase 7` asserts the new live `List()` starts
//!   empty (services are fresh), and `Phase 8` asserts the *restored*
//!   registries from disk surface the pre-pause pids.
//!
//! # Why the multi-thread runtime
//!
//! Tonic's HTTP/2 server needs at least two tasks (acceptor + worker)
//! plus the readers/writers each in-flight RPC spawns. A current-thread
//! runtime would serialize all of those and turn the PTY echo round-trip
//! into a livelock against `read_pty_until`. `worker_threads = 4`
//! matches the upper bound the issue spec calls out.

#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::Request;

use envd::filesystem::FilesystemService;
use envd::process::ProcessService;
use envd::proto::filesystem::filesystem_client::FilesystemClient;
use envd::proto::filesystem::filesystem_server::FilesystemServer;
use envd::proto::filesystem::{ListDirRequest, MakeDirRequest, StatRequest};
use envd::proto::process::process_client::ProcessClient;
use envd::proto::process::process_event::data_event::Output as DataOutput;
use envd::proto::process::process_event::Event as ProcessEventKind;
use envd::proto::process::process_input::Input as ProcessInputKind;
use envd::proto::process::process_selector::Selector;
use envd::proto::process::process_server::ProcessServer;
use envd::proto::process::{
    pty::Size as PtySize, ConnectRequest, ListRequest, ProcessConfig, ProcessInput,
    ProcessSelector, Pty, SendInputRequest, SendSignalRequest, Signal, StartRequest, StartResponse,
};
use envd::state::{EnvdState, SCHEMA_VERSION};

/// Per-test wall-clock cap. The internal phase timeouts are tight so a
/// real hang shows up locally as a clear panic; this outer guard exists
/// purely so CI surfaces the timeout as a test failure rather than a
/// runner-level kill (the issue spec calls out a 30 s ceiling).
const TEST_CEILING: Duration = Duration::from_secs(25);

/// Owning handle for a booted in-process server. Holds the shutdown
/// sender, the serve task, and clones of the underlying services so
/// the test can capture state directly from the live registries — no
/// manual `List()` → `ProcessRegistry` bridge.
struct BootedServer {
    addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    serve_handle: JoinHandle<Result<(), tonic::transport::Error>>,
    process_handle: ProcessService,
    filesystem_handle: FilesystemService,
}

/// Boot a fresh tonic server with Filesystem + Process services rooted
/// at `root`. Binds an OS-chosen port and returns once the listener is
/// live — the next `connect_lazy()` channel is guaranteed to reach a
/// listening socket.
async fn boot_server(root: PathBuf) -> BootedServer {
    let filesystem = FilesystemService::new(root).expect("filesystem service");
    let process = ProcessService::new();

    // Clone the cheap-Clone services *before* moving them into tonic so
    // the test can probe their registries directly. Both clones share
    // the same underlying `Arc`s, so snapshots reflect live state.
    let filesystem_handle = filesystem.clone();
    let process_handle = process.clone();

    // Bind synchronously so the returned address is immediately
    // reachable. Going through `serve_with_incoming_shutdown` rather
    // than `serve_with_shutdown(addr, ...)` is what gives us that
    // "listener is up by the time this function returns" guarantee.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    let incoming = TcpListenerStream::new(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve = Server::builder()
        .add_service(FilesystemServer::new(filesystem))
        .add_service(ProcessServer::new(process))
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = shutdown_rx.await;
        });
    let serve_handle = tokio::spawn(serve);

    BootedServer {
        addr,
        shutdown_tx,
        serve_handle,
        process_handle,
        filesystem_handle,
    }
}

/// Build a lazy channel pointing at an in-process server. `connect_lazy`
/// is safe here because `boot_server` returns only after the listener is
/// live — tonic establishes the TCP connection on the first RPC and
/// never observes a "connection refused" race.
fn channel_for(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .expect("valid endpoint URL")
        .connect_lazy()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_lifecycle_roundtrip() {
    // Wrapping the body in `timeout` (rather than annotating the test
    // function with one) lets us also exercise the snapshot-writing
    // `spawn_blocking` path with a sensible upper bound while still
    // producing a clear panic message if any step hangs.
    timeout(TEST_CEILING, run_lifecycle())
        .await
        .expect("lifecycle test exceeded TEST_CEILING");
}

/// Body of the lifecycle test. Split out so the outer
/// [`full_lifecycle_roundtrip`] can wrap it in a single `timeout` for
/// the wall-clock ceiling.
async fn run_lifecycle() {
    let root_dir = TempDir::new().expect("filesystem root tempdir");
    let snapshot_dir = TempDir::new().expect("snapshot tempdir");
    let state_path = snapshot_dir.path().join("envd-state.json");

    // ---- Phase 1: boot the first server and wire up clients --------
    let server = boot_server(root_dir.path().to_path_buf()).await;
    let channel = channel_for(server.addr);
    let mut fs_client = FilesystemClient::new(channel.clone());
    let mut process_client = ProcessClient::new(channel);

    // ---- Phase 2: Process flow (Start `sleep 60`, Connect, List) ---
    let start_stream = process_client
        .start(Request::new(pipe_start_request("sleep", &["60"])))
        .await
        .expect("Process.Start(sleep)")
        .into_inner();
    let mut start_stream = Box::pin(start_stream);
    let sleep_pid = read_start_pid(&mut start_stream).await;
    assert!(sleep_pid > 0, "Start must surface a non-zero pid");

    // `Connect` to the running sleep, drain one poll to prove the
    // stream opens cleanly, then drop it. We don't assert on payload
    // because `sleep 60` is silent — the contract under test here is
    // that `Connect` resolves without error against a running pid.
    let connect_stream = process_client
        .connect(Request::new(ConnectRequest {
            process: pid_selector(sleep_pid),
        }))
        .await
        .expect("Process.Connect")
        .into_inner();
    drop(connect_stream);

    // `List` must include the sleep pid.
    let live_pids = list_pids(&mut process_client).await;
    assert!(
        live_pids.contains(&sleep_pid),
        "Process.List must include pid {sleep_pid}; got {live_pids:?}",
    );

    // ---- Phase 3: Filesystem flow (metadata-only substitute) --------
    fs_client
        .make_dir(Request::new(MakeDirRequest {
            path: "/wares".into(),
        }))
        .await
        .expect("Filesystem.MakeDir");
    let stat = fs_client
        .stat(Request::new(StatRequest {
            path: "/wares".into(),
        }))
        .await
        .expect("Filesystem.Stat")
        .into_inner()
        .entry
        .expect("stat returned a populated EntryInfo");
    assert_eq!(stat.name, "wares", "Stat must echo the created dir name");

    let entries = fs_client
        .list_dir(Request::new(ListDirRequest {
            path: "/".into(),
            depth: 0,
        }))
        .await
        .expect("Filesystem.ListDir")
        .into_inner()
        .entries;
    assert!(
        entries.iter().any(|e| e.name == "wares"),
        "ListDir must include the created entry; got {entries:?}",
    );

    // ---- Phase 4: PTY flow (Process.Start with `pty` set) -----------
    let pty_stream = process_client
        .start(Request::new(pty_start_request(
            "bash",
            &["--noprofile", "--norc", "-i"],
            24,
            80,
        )))
        .await
        .expect("Process.Start(pty bash)")
        .into_inner();
    let mut pty_stream = Box::pin(pty_stream);
    let bash_pid = read_start_pid(&mut pty_stream).await;

    process_client
        .send_input(Request::new(SendInputRequest {
            process: pid_selector(bash_pid),
            input: Some(ProcessInput {
                input: Some(ProcessInputKind::Pty(b"echo $((40+2))\n".to_vec())),
            }),
        }))
        .await
        .expect("Process.SendInput(pty)");

    let pty_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let pty_output = read_pty_until(&mut pty_stream, b"42", pty_deadline).await;
    assert!(
        pty_output.windows(2).any(|w| w == b"42"),
        "PTY must yield '42' within 5 s; got {:?}",
        String::from_utf8_lossy(&pty_output),
    );

    // ---- Phase 5: capture state and write to a tempfile -------------
    // Walk the live registries directly via the service handles (issue
    // #174). Pipe-backed entries land in `processes`; PTY-backed
    // entries land in `ptys`. Watch snapshots are empty here because
    // the test doesn't exercise `CreateWatcher`.
    let state = EnvdState {
        version: SCHEMA_VERSION,
        processes: server.process_handle.collect_process_snapshots().await,
        ptys: server.process_handle.collect_pty_snapshots().await,
        watches: server.filesystem_handle.collect_watch_snapshots().await,
        // This test exercises the registry capture, not the rootfs
        // oracle (#236), so it leaves the fingerprint unset.
        rootfs_fingerprint: None,
    };
    assert!(
        state.processes.iter().any(|s| s.pid == sleep_pid),
        "live capture must include pipe-backed sleep pid {sleep_pid}; got {:?}",
        state.processes,
    );
    assert!(
        state.ptys.iter().any(|s| s.pid == bash_pid),
        "live capture must include PTY-backed bash pid {bash_pid}; got {:?}",
        state.ptys,
    );

    let write_path = state_path.clone();
    let write_state = state.clone();
    tokio::task::spawn_blocking(move || write_state.write_to(&write_path))
        .await
        .expect("snapshot writer joined")
        .expect("snapshot write");

    // ---- Phase 6: tear down + restart -------------------------------
    // Kill both live children explicitly. `kill_on_drop` would handle
    // this when the services drop, but issuing SIGKILL first makes the
    // teardown timing deterministic (no race between the drop and the
    // OS-level reap on slow CI runners).
    let _ = process_client
        .send_signal(Request::new(SendSignalRequest {
            process: pid_selector(sleep_pid),
            signal: Signal::Sigkill as i32,
        }))
        .await;
    let _ = process_client
        .send_signal(Request::new(SendSignalRequest {
            process: pid_selector(bash_pid),
            signal: Signal::Sigkill as i32,
        }))
        .await;

    // Drop client handles so the channel can close before we signal the
    // server to drain. Without this drop, `serve_with_incoming_shutdown`
    // would wait for the open HTTP/2 streams to finish on their own.
    drop(fs_client);
    drop(process_client);

    let _ = server.shutdown_tx.send(());
    // Awaiting the serve task ensures the first server's services
    // (and therefore the registered children) are fully dropped before
    // the second server boots, so the post-restart `List()` assertion
    // below is meaningful. Errors here are not the test's contract —
    // a stray h2 frame post-shutdown is acceptable.
    let _ = timeout(Duration::from_secs(5), server.serve_handle).await;

    // ---- Phase 7: boot a fresh server, verify live List() is empty --
    // Same filesystem root so the previously-created `/wares` survives
    // (filesystem state lives on disk, not in the service); the
    // ProcessService and PtyService start empty by design.
    let server2 = boot_server(root_dir.path().to_path_buf()).await;
    let channel2 = channel_for(server2.addr);
    let mut process_client2 = ProcessClient::new(channel2.clone());
    let mut fs_client2 = FilesystemClient::new(channel2);

    let live_pids_after = list_pids(&mut process_client2).await;
    assert!(
        live_pids_after.is_empty(),
        "fresh server's live List() must start empty; got {live_pids_after:?}",
    );

    // The filesystem dir should still be visible — it lived on disk
    // in `root_dir`, not inside the previous service instance.
    let entries_after = fs_client2
        .list_dir(Request::new(ListDirRequest {
            path: "/".into(),
            depth: 0,
        }))
        .await
        .expect("Filesystem.ListDir on restored server")
        .into_inner()
        .entries;
    assert!(
        entries_after.iter().any(|e| e.name == "wares"),
        "on-disk fs state must survive restart; got {entries_after:?}",
    );

    // ---- Phase 8: restore registries from the snapshot file ---------
    let read_path = state_path.clone();
    let read_state = tokio::task::spawn_blocking(move || EnvdState::read_from(&read_path))
        .await
        .expect("snapshot reader joined")
        .expect("snapshot read");
    let restored = read_state.restore().expect("restore registries");

    let restored_proc_pids: Vec<u32> = restored
        .processes
        .snapshots()
        .iter()
        .map(|s| s.pid)
        .collect();
    assert!(
        restored_proc_pids.contains(&sleep_pid),
        "restored ProcessRegistry must surface sleep pid {sleep_pid}; got {restored_proc_pids:?}",
    );

    let restored_pty_pids: Vec<u32> = restored.ptys.snapshots().iter().map(|s| s.pid).collect();
    assert!(
        restored_pty_pids.contains(&bash_pid),
        "restored PtyRegistry must surface bash pid {bash_pid}; got {restored_pty_pids:?}",
    );

    // ---- Final teardown ---------------------------------------------
    drop(fs_client2);
    drop(process_client2);
    let _ = server2.shutdown_tx.send(());
    let _ = timeout(Duration::from_secs(5), server2.serve_handle).await;
}

// ---- helpers ----------------------------------------------------------

/// Wrap a pid in the `ProcessSelector::Pid` oneof variant.
fn pid_selector(pid: u32) -> Option<ProcessSelector> {
    Some(ProcessSelector {
        selector: Some(Selector::Pid(pid)),
    })
}

/// Build a pipe-backed (non-PTY) `StartRequest`.
fn pipe_start_request(cmd: &str, args: &[&str]) -> StartRequest {
    StartRequest {
        process: Some(ProcessConfig {
            cmd: cmd.to_string(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            envs: HashMap::new(),
            cwd: None,
        }),
        pty: None,
        tag: None,
        // `Some(false)` opts the child into `/dev/null` stdin; the
        // `sleep` child does not read stdin, but the explicit value
        // keeps the wire payload close to what the upstream SDK sends.
        stdin: Some(false),
    }
}

/// Build a PTY-backed `StartRequest` with the requested initial window.
fn pty_start_request(cmd: &str, args: &[&str], rows: u32, cols: u32) -> StartRequest {
    StartRequest {
        process: Some(ProcessConfig {
            cmd: cmd.to_string(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            envs: HashMap::new(),
            cwd: None,
        }),
        pty: Some(Pty {
            size: Some(PtySize { rows, cols }),
        }),
        tag: None,
        // For PTY children the `stdin` flag is moot — input flows
        // through the PTY master, gated server-side on
        // `pty_master.is_some()`.
        stdin: None,
    }
}

/// Pull the first event from a `StartResponse` stream and unwrap the
/// `StartEvent` pid. Panics if the first event isn't `Start` or if it
/// doesn't arrive within 5 s — both are honest test failures.
async fn read_start_pid<S>(stream: &mut S) -> u32
where
    S: futures::Stream<Item = Result<StartResponse, tonic::Status>> + Unpin,
{
    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first event arrives within 5 s")
        .expect("stream had at least one item")
        .expect("stream item was Ok");
    match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    }
}

/// Drain `DataEvent::Pty` bytes from a `StartResponse` stream until
/// `needle` appears or `deadline` passes. Returns the accumulated bytes
/// either way so the caller can emit a helpful diff on failure.
async fn read_pty_until<S>(stream: &mut S, needle: &[u8], deadline: tokio::time::Instant) -> Vec<u8>
where
    S: futures::Stream<Item = Result<StartResponse, tonic::Status>> + Unpin,
{
    let mut buf = Vec::new();
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        let next = match timeout(remaining, stream.next()).await {
            Ok(Some(Ok(item))) => item,
            // Stream end / error / outer timeout — bail and let the
            // caller decide based on what's been accumulated.
            _ => return buf,
        };
        if let Some(ProcessEventKind::Data(d)) = next.event.and_then(|e| e.event) {
            if let Some(DataOutput::Pty(b)) = d.output {
                buf.extend_from_slice(&b);
                if buf.windows(needle.len()).any(|w| w == needle) {
                    return buf;
                }
            }
        }
    }
    buf
}

/// Call `Process.List` and return the pids in iteration order.
async fn list_pids(client: &mut ProcessClient<Channel>) -> Vec<u32> {
    client
        .list(Request::new(ListRequest {}))
        .await
        .expect("Process.List")
        .into_inner()
        .processes
        .iter()
        .map(|p| p.pid)
        .collect()
}
