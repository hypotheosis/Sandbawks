//! Subprocess smoke test for the `envd` binary.
//!
//! Boots the binary as a child process, waits for the gRPC and HTTP
//! ports to accept connections, sends `SIGTERM`, and asserts:
//!
//! 1. The process exits cleanly (status code 0) within 5 s.
//! 2. The pause/resume snapshot file is on disk afterwards.
//! 3. The snapshot file parses back into a valid [`EnvdState`].
//! 4. `GET /__state` returns a 200 with a body that parses as the
//!    live [`EnvdState`] — the pause-time probe contract.
//!
//! Together these checks pin the contract the T7 binary owns: clean
//! shutdown on a real signal and a parseable snapshot after exit. The
//! handler-level behaviour of each gRPC method is covered by the
//! sibling `filesystem.rs` / `process.rs` / `pty.rs` integration
//! tests, which exercise the traits directly.
//!
//! # Why a real subprocess (and not the binary's `run` function)
//!
//! Signal delivery, runtime startup, and the `clap`-parsed argv path
//! only behave the same way when the binary is executed end-to-end.
//! Calling `run(cli)` from inside this test would skip the runtime
//! construction, share a process address space, and need extra
//! signal-handler hacks to avoid killing the test runner — for the
//! handful of behaviours we want to pin, spawning the binary is
//! simpler than mocking out half of `main`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use envd::state::{EnvdState, SCHEMA_VERSION};

/// gRPC client imports kept module-local so they don't pollute the
/// surface of the existing tests that work purely against the snapshot
/// file. Only the live-content smoke (`live_state_reports_started_process`)
/// uses them.
mod grpc_helpers {
    pub use envd::proto::process::process_client::ProcessClient;
    pub use envd::proto::process::process_event::Event as ProcessEventKind;
    pub use envd::proto::process::{ProcessConfig, StartRequest};
}

/// Maximum time we wait for the spawned envd to accept a TCP
/// connection on its `--listen` port. Generous because the first
/// run after a fresh `cargo build` still pays the dynamic-linker
/// startup cost.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum time we wait for the binary to exit after we deliver
/// SIGTERM. The issue spec calls out 5 s as the hard ceiling; honour
/// it here so a regression that hangs the drain shows up as a test
/// failure rather than as a CI-wide timeout.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Find an unused TCP port on loopback by binding `127.0.0.1:0` and
/// immediately dropping the listener. The kernel will not re-issue
/// the same port to another process *instantly*, but in practice the
/// window between drop and the envd `bind` is microseconds — small
/// enough that we don't bother with retries here.
fn pick_free_port() -> u16 {
    let listener =
        TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port for free-port probe");
    let port = listener
        .local_addr()
        .expect("ephemeral listener has no local addr")
        .port();
    drop(listener);
    port
}

/// Poll `addr` until a TCP connect succeeds or `timeout` elapses.
/// Returns `true` on success, `false` on timeout.
fn wait_for_accept(addr: SocketAddr, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Issue a raw `GET /__state HTTP/1.1` request and parse the JSON
/// body back into an [`EnvdState`].
///
/// Implemented over a plain `TcpStream` so the test doesn't grow a
/// dev-dep on an HTTP client. The response body is small (KB range)
/// and unchunked because axum's `Json` extractor sets a
/// `Content-Length` header; reading until EOF after sending
/// `Connection: close` is safe.
fn probe_state_http(addr: SocketAddr) -> std::io::Result<EnvdState> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let request = format!(
        "GET /__state HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    // Split on the CRLFCRLF that terminates the HTTP header block.
    // `windows(4)` keeps the implementation small enough that pulling
    // in `httparse` would be overkill for the single probe site.
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "no header terminator")
        })?;
    let body = &raw[split + 4..];
    let status_line =
        std::str::from_utf8(&raw[..raw.iter().position(|&b| b == b'\r').unwrap_or(0)])
            .unwrap_or("");
    if !status_line.contains(" 200 ") {
        return Err(std::io::Error::other(format!(
            "non-200 status: {status_line}"
        )));
    }
    serde_json::from_slice(body)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("parse: {e}")))
}

/// Spawn the `envd` binary with the standard arg set, stdio piped for
/// diagnostics. Factored out so the resume round-trip test below can
/// boot the binary three times without repeating the `Command` builder.
fn spawn_envd(
    bin: &str,
    listen: &str,
    http_listen: &str,
    state_path: &std::path::Path,
    root_path: &std::path::Path,
) -> std::process::Child {
    Command::new(bin)
        .arg("--listen")
        .arg(listen)
        .arg("--http-listen")
        .arg(http_listen)
        .arg("--state-file")
        .arg(state_path)
        .arg("--root")
        .arg(root_path)
        .arg("--log-level")
        .arg("info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn envd")
}

/// Send SIGTERM to a pid. Implemented via `nix` rather than spawning
/// `kill(1)` so the test is self-contained on the workspace's
/// existing dependency set.
fn sigterm(pid: u32) {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), Signal::SIGTERM).expect("SIGTERM");
}

/// Wait up to `timeout` for `child` to exit, polling
/// `Child::try_wait` rather than blocking on `wait` so we can enforce
/// the hard 5 s ceiling on the drain.
fn wait_exit(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match child.try_wait().expect("try_wait") {
            Some(status) => return Some(status),
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    None
}

#[test]
fn envd_boots_handles_sigterm_and_writes_snapshot() {
    let bin = env!("CARGO_BIN_EXE_envd");
    let tempdir = tempfile::tempdir().expect("tempdir");
    // Use the tempdir as the filesystem service root too — the
    // default `/` would technically work but exposes the host fs to
    // a process we then signal a few hundred milliseconds later, and
    // there's no reason to canonicalize `/` inside a unit test.
    let state_path = tempdir.path().join("state.json");
    let root_path = tempdir.path().join("root");
    std::fs::create_dir_all(&root_path).expect("mkdir root");

    let port = pick_free_port();
    let listen = format!("127.0.0.1:{port}");
    let listen_addr: SocketAddr = listen.parse().expect("parse listen addr");
    let http_port = pick_free_port();
    let http_listen = format!("127.0.0.1:{http_port}");
    let http_listen_addr: SocketAddr = http_listen.parse().expect("parse http listen addr");

    let mut child = Command::new(bin)
        .arg("--listen")
        .arg(&listen)
        .arg("--http-listen")
        .arg(&http_listen)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--root")
        .arg(&root_path)
        // Quiet down envd's own logging so the test output is
        // readable; `error` still surfaces anything genuinely bad.
        .arg("--log-level")
        .arg("error")
        // Inherit stdout/stderr piped into Vec for diagnostics on
        // assertion failure. `Stdio::null` would hide a panic-on-boot
        // and make the timeout look unexplained.
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn envd");

    // RAII guard so a panicking assertion below still cleans up the
    // child rather than leaking a half-running process. Drops kill
    // and reap on exit of the test fn scope, including unwind.
    struct Reaper<'a>(&'a mut std::process::Child);
    impl Drop for Reaper<'_> {
        fn drop(&mut self) {
            // Best-effort: ignore errors because the child may have
            // already exited via the happy path above.
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let ready = wait_for_accept(listen_addr, READY_TIMEOUT);
    assert!(
        ready,
        "envd never accepted a connection on {listen} within {:?}",
        READY_TIMEOUT
    );
    let http_ready = wait_for_accept(http_listen_addr, READY_TIMEOUT);
    assert!(
        http_ready,
        "envd HTTP /__state never accepted a connection on {http_listen} within {:?}",
        READY_TIMEOUT
    );

    // Probe `GET /__state` and parse the response body back into an
    // `EnvdState`. Raw TCP write rather than pulling in `reqwest` as
    // a dev-dep — the request is a fixed string and the response is
    // small enough to read in one go.
    let probed = probe_state_http(http_listen_addr).expect("probe /__state");
    assert_eq!(
        probed.version, SCHEMA_VERSION,
        "/__state body schema version drifted: got {} want {}",
        probed.version, SCHEMA_VERSION,
    );

    let pid = child.id();
    sigterm(pid);

    let exit_status = wait_exit(&mut child, SHUTDOWN_TIMEOUT);
    // Drop the reaper guard now — `wait_exit` already wait()ed
    // successfully if `Some`, so we don't need the guard's cleanup;
    // if it's `None`, we want the guard to fire so the child
    // doesn't leak past the assertion.
    let status = match exit_status {
        Some(s) => s,
        None => {
            let _guard = Reaper(&mut child);
            panic!("envd did not exit within {:?} of SIGTERM", SHUTDOWN_TIMEOUT);
        }
    };

    assert!(
        status.success(),
        "envd exited with non-zero status: {status:?}"
    );

    // Snapshot file must exist…
    assert!(
        state_path.exists(),
        "snapshot file missing after shutdown: {}",
        state_path.display()
    );

    // …and round-trip cleanly back into an `EnvdState`. We don't
    // assert on the contents (the live services are empty on this
    // run, but a future T7-adjacent change might pre-populate them),
    // only that the file is parseable — which is the durable contract
    // a future envd boot relies on.
    let parsed = EnvdState::read_from(&state_path)
        .expect("envd-state.json on disk must parse back into EnvdState");
    assert_eq!(
        parsed.version,
        envd::state::SCHEMA_VERSION,
        "snapshot schema version drifted"
    );
}

/// #236: the rootfs fingerprint oracle, end to end through the real
/// binary — the load-bearing fail-closed behaviour of the whole feature.
///
/// 1. **Capture**: boot envd against a rootfs with a known file, let it
///    flush `state.json` (which now embeds the fingerprint) on SIGTERM.
/// 2. **Clean resume**: boot again against the *unchanged* rootfs and
///    confirm it reaches Ready — i.e. `load_boot_seed` re-verified the
///    fingerprint and proceeded.
/// 3. **Fail-closed resume**: tamper a file to the *same byte length*
///    (the case only full-content hashing catches), boot again, and
///    confirm the process exits non-zero without binding its listeners.
///
/// The state file lives *outside* `--root` here, matching the deployment
/// constraint; the in-root self-reference case is unit-tested separately
/// in `fingerprint::tests::excluded_state_file_does_not_affect_fingerprint`.
#[test]
fn resume_reverifies_rootfs_and_fails_closed_on_divergence() {
    let bin = env!("CARGO_BIN_EXE_envd");
    let tempdir = tempfile::tempdir().expect("tempdir");
    let state_path = tempdir.path().join("state.json");
    let root_path = tempdir.path().join("root");
    std::fs::create_dir_all(&root_path).expect("mkdir root");
    // A known file whose bytes the fingerprint will cover.
    let tracked = root_path.join("workload.txt");
    std::fs::write(&tracked, b"alpha").expect("seed workload file");

    struct Reaper<'a>(&'a mut std::process::Child);
    impl Drop for Reaper<'_> {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    // Boot envd, wait for both listeners, SIGTERM, and assert a clean
    // exit. Returns nothing — panics on any deviation.
    let boot_then_terminate = |label: &str| {
        let port = pick_free_port();
        let listen = format!("127.0.0.1:{port}");
        let listen_addr: SocketAddr = listen.parse().expect("parse listen addr");
        let http_port = pick_free_port();
        let http_listen = format!("127.0.0.1:{http_port}");
        let http_addr: SocketAddr = http_listen.parse().expect("parse http addr");

        let mut child = spawn_envd(bin, &listen, &http_listen, &state_path, &root_path);
        assert!(
            wait_for_accept(listen_addr, READY_TIMEOUT),
            "{label}: envd never accepted gRPC on {listen} within {READY_TIMEOUT:?}",
        );
        assert!(
            wait_for_accept(http_addr, READY_TIMEOUT),
            "{label}: envd never accepted HTTP on {http_listen} within {READY_TIMEOUT:?}",
        );
        let pid = child.id();
        sigterm(pid);
        let status = match wait_exit(&mut child, SHUTDOWN_TIMEOUT) {
            Some(s) => s,
            None => {
                let _guard = Reaper(&mut child);
                panic!("{label}: envd did not exit within {SHUTDOWN_TIMEOUT:?} of SIGTERM");
            }
        };
        assert!(
            status.success(),
            "{label}: envd exited non-zero: {status:?}"
        );
    };

    // 1. Capture — flushes state.json with the fingerprint embedded.
    boot_then_terminate("capture");
    assert!(
        state_path.exists(),
        "capture run must leave a state file at {}",
        state_path.display()
    );

    // 2. Clean resume — unchanged rootfs must re-verify and reach Ready.
    boot_then_terminate("clean-resume");

    // 3. Fail-closed resume — same-length tamper must abort the boot.
    std::fs::write(&tracked, b"ALPHA").expect("tamper workload file (same length)");
    let port = pick_free_port();
    let listen = format!("127.0.0.1:{port}");
    let listen_addr: SocketAddr = listen.parse().expect("parse listen addr");
    let http_port = pick_free_port();
    let http_listen = format!("127.0.0.1:{http_port}");
    let http_addr: SocketAddr = http_listen.parse().expect("parse http addr");

    let mut child = spawn_envd(bin, &listen, &http_listen, &state_path, &root_path);
    // The boot must fail in `load_boot_seed` *before* binding listeners.
    // Give it the full shutdown budget to exit on its own.
    let status = match wait_exit(&mut child, SHUTDOWN_TIMEOUT) {
        Some(s) => s,
        None => {
            let _guard = Reaper(&mut child);
            panic!("fail-closed envd did not exit within {SHUTDOWN_TIMEOUT:?}");
        }
    };
    assert!(
        !status.success(),
        "fail-closed resume must exit non-zero on rootfs divergence; got {status:?}",
    );
    // The listeners must never have come up — the abort precedes the bind.
    assert!(
        !wait_for_accept(listen_addr, Duration::from_millis(100)),
        "fail-closed envd must not bind gRPC on {listen}",
    );
    assert!(
        !wait_for_accept(http_addr, Duration::from_millis(100)),
        "fail-closed envd must not bind HTTP on {http_listen}",
    );

    // The error chain must name the fingerprint mismatch, not some other
    // boot failure — otherwise this test could pass for the wrong reason.
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("child stderr piped")
        .read_to_string(&mut stderr)
        .expect("read envd stderr");
    assert!(
        stderr.contains("rootfs fingerprint"),
        "fail-closed envd stderr must mention the fingerprint mismatch; got:\n{stderr}",
    );
}

/// Issue #174: `GET /__state` against a running envd must reflect the
/// *live* registries. Start a `sleep` via the `Process` gRPC client,
/// then probe `/__state` and assert the response body's `processes`
/// array carries the spawned pid.
///
/// `#[tokio::test]` rather than the sync `#[test]` form because the
/// gRPC client is async. The envd subprocess is still spawned via
/// `std::process::Command` — we just need a tokio runtime for the
/// in-process client side.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_state_reports_started_process() {
    use grpc_helpers::*;
    use tonic::Request;

    let bin = env!("CARGO_BIN_EXE_envd");
    let tempdir = tempfile::tempdir().expect("tempdir");
    let state_path = tempdir.path().join("state.json");
    let root_path = tempdir.path().join("root");
    std::fs::create_dir_all(&root_path).expect("mkdir root");

    let port = pick_free_port();
    let listen = format!("127.0.0.1:{port}");
    let listen_addr: SocketAddr = listen.parse().expect("parse listen addr");
    let http_port = pick_free_port();
    let http_listen = format!("127.0.0.1:{http_port}");
    let http_listen_addr: SocketAddr = http_listen.parse().expect("parse http listen addr");

    let mut child = Command::new(bin)
        .arg("--listen")
        .arg(&listen)
        .arg("--http-listen")
        .arg(&http_listen)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--root")
        .arg(&root_path)
        .arg("--log-level")
        .arg("error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn envd");
    let pid = child.id();

    // RAII cleanup matching the sibling sync test: if any assertion
    // below panics, the child is still SIGKILL'd before we return.
    struct Reaper<'a>(&'a mut std::process::Child);
    impl Drop for Reaper<'_> {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let grpc_ready = wait_for_accept(listen_addr, READY_TIMEOUT);
    assert!(
        grpc_ready,
        "envd never accepted gRPC connection on {listen} within {READY_TIMEOUT:?}",
    );
    let http_ready = wait_for_accept(http_listen_addr, READY_TIMEOUT);
    assert!(
        http_ready,
        "envd never accepted HTTP connection on {http_listen} within {READY_TIMEOUT:?}",
    );

    // Drive a `Process.Start` over gRPC. `sleep 30` is long-lived
    // enough that the probe below catches it before any post-exit
    // retention machinery comes into play.
    let endpoint = format!("http://{listen}");
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect channel");
    let mut client = ProcessClient::new(channel);
    let stream = client
        .start(Request::new(StartRequest {
            process: Some(ProcessConfig {
                cmd: "sleep".to_string(),
                args: vec!["30".to_string()],
                envs: std::collections::HashMap::new(),
                cwd: None,
            }),
            pty: None,
            tag: None,
            stdin: Some(false),
        }))
        .await
        .expect("Process.Start over gRPC")
        .into_inner();
    let mut stream = Box::pin(stream);
    use futures::StreamExt;
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("StartEvent within 5 s")
        .expect("stream item")
        .expect("ok");
    let spawned_pid = match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    };

    // Probe `/__state` and assert the spawned pid is present.
    let probed = tokio::task::spawn_blocking(move || probe_state_http(http_listen_addr))
        .await
        .expect("probe task joined")
        .expect("probe /__state");
    assert_eq!(probed.version, SCHEMA_VERSION);
    assert!(
        probed.processes.iter().any(|s| s.pid == spawned_pid),
        "/__state must include spawned pid {spawned_pid}; got {:?}",
        probed.processes,
    );
    let entry = probed
        .processes
        .iter()
        .find(|s| s.pid == spawned_pid)
        .expect("entry present");
    assert_eq!(entry.cmd, "sleep");
    assert_eq!(entry.args, vec!["30".to_string()]);
    assert!(
        entry.started_at > 0,
        "started_at must be populated (got 0); entry={entry:?}",
    );

    // Clean teardown so the snapshot file lands on disk.
    drop(stream);
    drop(client);
    sigterm(pid);
    let exit_status = wait_exit(&mut child, SHUTDOWN_TIMEOUT);
    let status = match exit_status {
        Some(s) => s,
        None => {
            let _guard = Reaper(&mut child);
            panic!("envd did not exit within {SHUTDOWN_TIMEOUT:?} of SIGTERM");
        }
    };
    assert!(
        status.success(),
        "envd exited with non-zero status: {status:?}",
    );
}

/// `envd self-install <dest>` produces a byte-identical copy of the
/// running binary (issue #268 acceptance criterion).
///
/// This exercises the real `clap`-parsed subcommand end-to-end against
/// the compiled binary — the same code path the node DaemonSet's
/// `stage-envd` init container runs to stage envd onto a node. The
/// sibling unit tests in `src/bin/envd.rs` cover the copy logic in
/// isolation; this one proves the subcommand is wired up and that the
/// installed bytes match the binary that produced them.
#[test]
fn self_install_produces_byte_identical_copy() {
    let bin = env!("CARGO_BIN_EXE_envd");
    let tempdir = tempfile::tempdir().expect("tempdir");
    // Install into a not-yet-existing subdirectory so the parent-dir
    // creation path (matching the DaemonSet's DirectoryOrCreate hostPath)
    // is exercised too.
    let dest = tempdir.path().join("bin").join("envd");

    let status = Command::new(bin)
        .arg("self-install")
        .arg(&dest)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn envd self-install");
    assert!(
        status.success(),
        "envd self-install exited with non-zero status: {status:?}"
    );

    let original = std::fs::read(bin).expect("read original binary");
    let installed = std::fs::read(&dest).expect("read installed binary");
    assert_eq!(
        installed, original,
        "self-install must produce a byte-identical copy of the running binary"
    );
}
