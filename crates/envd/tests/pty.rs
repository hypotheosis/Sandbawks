//! Integration tests for the PTY path through [`envd::process::ProcessService`].
//!
//! PTY semantics are kernel-specific (Linux line discipline, `TIOCSCTTY`
//! / `TIOCSWINSZ` ioctls, slave-close → master-EIO), so the whole file
//! is gated on Linux. macOS and Windows are out of scope for sandbawks
//! per the upstream issue's "Out of scope" note.

#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;
use tonic::Request;

use envd::process::ProcessService;
use envd::proto::process::process_event::data_event::Output as DataOutput;
use envd::proto::process::process_event::Event as ProcessEventKind;
use envd::proto::process::process_input::Input as ProcessInputKind;
use envd::proto::process::process_selector::Selector;
use envd::proto::process::process_server::Process;
use envd::proto::process::{
    pty::Size as PtySize, ProcessConfig, ProcessInput, ProcessSelector, Pty, SendInputRequest,
    SendSignalRequest, Signal, StartRequest, StartResponse, UpdateRequest,
};

/// Build a PTY-backed `StartRequest`.
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
        // For PTY-backed processes the `stdin` flag is moot — input
        // flows through the PTY master, gated by `pty_master.is_some()`
        // server-side.
        stdin: None,
    }
}

fn pid_selector(pid: u32) -> Option<ProcessSelector> {
    Some(ProcessSelector {
        selector: Some(Selector::Pid(pid)),
    })
}

/// Pull the first event from a `StartResponse` stream and unwrap the
/// `StartEvent` pid. The pid is what subsequent `SendInput` /
/// `SendSignal` / `Update` calls key off of.
async fn read_pid<S>(stream: &mut S) -> u32
where
    S: futures::Stream<Item = Result<StartResponse, tonic::Status>> + Unpin,
{
    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first event arrives")
        .expect("stream item")
        .expect("ok");
    match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    }
}

/// Drain PTY output from a `StartResponse` stream until either
/// `needle` appears in the accumulated bytes or `deadline` is reached.
/// Returns the accumulated bytes either way so the test can assert on
/// them and emit a helpful diff on failure.
async fn read_pty_until<S>(stream: &mut S, needle: &[u8], deadline: tokio::time::Instant) -> Vec<u8>
where
    S: futures::Stream<Item = Result<StartResponse, tonic::Status>> + Unpin,
{
    let mut buf = Vec::new();
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        let next = match timeout(remaining, stream.next()).await {
            Ok(Some(Ok(item))) => item,
            // Stream end / error / outer timeout: bail and let the
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

#[tokio::test]
async fn bash_prompt_echoes() {
    let svc = ProcessService::new();
    let stream = svc
        .start(Request::new(pty_start_request(
            "bash",
            &["--noprofile", "--norc", "-i"],
            24,
            80,
        )))
        .await
        .expect("start")
        .into_inner();
    let mut stream = Box::pin(stream);

    let pid = read_pid(&mut stream).await;

    svc.send_input(Request::new(SendInputRequest {
        process: pid_selector(pid),
        input: Some(ProcessInput {
            input: Some(ProcessInputKind::Pty(b"echo $((40 + 2))\n".to_vec())),
        }),
    }))
    .await
    .expect("send_input");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let output = read_pty_until(&mut stream, b"42", deadline).await;
    assert!(
        output.windows(2).any(|w| w == b"42"),
        "should see `42` in PTY output within 5 s; got {:?}",
        String::from_utf8_lossy(&output),
    );

    // Clean up: SIGKILL the shell so the test doesn't depend on
    // `kill_on_drop` for teardown ordering, and drain to completion.
    svc.send_signal(Request::new(SendSignalRequest {
        process: pid_selector(pid),
        signal: Signal::Sigkill as i32,
    }))
    .await
    .expect("send_signal");
    while let Ok(Some(_)) = timeout(Duration::from_secs(3), stream.next()).await {}
}

#[tokio::test]
async fn resize_succeeds() {
    let svc = ProcessService::new();
    // Initial size is intentionally tiny (1x1). The resize *must* land
    // before `stty size` runs, otherwise the test passes only by
    // accident; the `sleep 0.5` gives the test code time to send
    // `Update` before stty queries the kernel.
    let stream = svc
        .start(Request::new(pty_start_request(
            "bash",
            &["-c", "sleep 0.5; stty size"],
            1,
            1,
        )))
        .await
        .expect("start")
        .into_inner();
    let mut stream = Box::pin(stream);
    let pid = read_pid(&mut stream).await;

    svc.update(Request::new(UpdateRequest {
        process: pid_selector(pid),
        pty: Some(Pty {
            size: Some(PtySize { rows: 24, cols: 80 }),
        }),
    }))
    .await
    .expect("update");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let output = read_pty_until(&mut stream, b"24 80", deadline).await;
    assert!(
        output.windows(b"24 80".len()).any(|w| w == b"24 80"),
        "expected `24 80` in stty output; got {:?}",
        String::from_utf8_lossy(&output),
    );
}

#[tokio::test]
async fn kill_with_sigterm_exits() {
    let svc = ProcessService::new();
    let stream = svc
        .start(Request::new(pty_start_request("sleep", &["60"], 24, 80)))
        .await
        .expect("start")
        .into_inner();
    let mut stream = Box::pin(stream);
    let pid = read_pid(&mut stream).await;

    svc.send_signal(Request::new(SendSignalRequest {
        process: pid_selector(pid),
        signal: Signal::Sigterm as i32,
    }))
    .await
    .expect("send_signal");

    let mut saw_end = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        let next = match timeout(remaining, stream.next()).await {
            Ok(Some(Ok(item))) => item,
            _ => break,
        };
        if matches!(
            next.event.and_then(|e| e.event),
            Some(ProcessEventKind::End(_))
        ) {
            saw_end = true;
            break;
        }
    }
    assert!(saw_end, "SIGTERM should produce EndEvent within 3 s");
}

#[tokio::test]
async fn collect_pty_snapshots_reports_allocated_pty() {
    // Issue #174 contract: after a PTY-backed Start, the next snapshot
    // capture must surface the entry in the `ptys` array with its id,
    // pid, last-known size, cmd, and args.
    let svc = ProcessService::new();
    let stream = svc
        .start(Request::new(pty_start_request("sleep", &["30"], 24, 80)))
        .await
        .expect("start")
        .into_inner();
    let mut stream = Box::pin(stream);
    let pid = read_pid(&mut stream).await;

    // Process snapshots must NOT include the PTY-backed entry; it
    // belongs in `ptys`.
    let process_snaps = svc.collect_process_snapshots().await;
    assert!(
        !process_snaps.iter().any(|s| s.pid == pid),
        "PTY-backed pid must not appear in process snapshots; got {process_snaps:?}",
    );

    let pty_snaps = svc.collect_pty_snapshots().await;
    let entry = pty_snaps
        .iter()
        .find(|s| s.pid == pid)
        .unwrap_or_else(|| panic!("PTY snapshot must contain pid {pid}; got {pty_snaps:?}"));
    assert!(entry.id > 0, "pty id must be a fresh-minted positive value");
    assert_eq!(entry.rows, 24);
    assert_eq!(entry.cols, 80);
    assert_eq!(entry.cmd, "sleep");
    assert_eq!(entry.args, vec!["30".to_string()]);

    // A subsequent resize must update the snapshot's rows/cols.
    svc.update(Request::new(UpdateRequest {
        process: pid_selector(pid),
        pty: Some(Pty {
            size: Some(PtySize {
                rows: 40,
                cols: 132,
            }),
        }),
    }))
    .await
    .expect("update");
    let pty_snaps = svc.collect_pty_snapshots().await;
    let entry = pty_snaps
        .iter()
        .find(|s| s.pid == pid)
        .expect("PTY snapshot still present");
    assert_eq!(entry.rows, 40);
    assert_eq!(entry.cols, 132);

    // Clean up.
    svc.send_signal(Request::new(SendSignalRequest {
        process: pid_selector(pid),
        signal: Signal::Sigkill as i32,
    }))
    .await
    .expect("send_signal");
    while let Ok(Some(_)) = timeout(Duration::from_secs(3), stream.next()).await {}
}

#[tokio::test]
async fn multiple_inputs_serialize_in_order() {
    let svc = ProcessService::new();
    // `cat` in cooked-mode PTY echoes each line back: the kernel line
    // discipline buffers per-line, delivers to cat on `\n`, cat writes
    // it to stdout (the slave), and the kernel also echoes the input
    // bytes to the master. So each input line surfaces in the master
    // output stream. Ordering is what matters here.
    let stream = svc
        .start(Request::new(pty_start_request("cat", &[], 24, 80)))
        .await
        .expect("start")
        .into_inner();
    let mut stream = Box::pin(stream);
    let pid = read_pid(&mut stream).await;

    for chunk in [&b"a\n"[..], &b"b\n"[..], &b"c\n"[..]] {
        svc.send_input(Request::new(SendInputRequest {
            process: pid_selector(pid),
            input: Some(ProcessInput {
                input: Some(ProcessInputKind::Pty(chunk.to_vec())),
            }),
        }))
        .await
        .expect("send_input");
    }

    // Read until we've observed all three needle bytes; checking that
    // their *first* occurrences are ordered a<b<c.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut buf = Vec::new();
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        let next = match timeout(remaining, stream.next()).await {
            Ok(Some(Ok(item))) => item,
            _ => break,
        };
        if let Some(ProcessEventKind::Data(d)) = next.event.and_then(|e| e.event) {
            if let Some(DataOutput::Pty(b)) = d.output {
                buf.extend_from_slice(&b);
                if buf.contains(&b'a') && buf.contains(&b'b') && buf.contains(&b'c') {
                    break;
                }
            }
        }
    }

    let pos_a = buf.iter().position(|&b| b == b'a');
    let pos_b = buf.iter().position(|&b| b == b'b');
    let pos_c = buf.iter().position(|&b| b == b'c');
    assert!(
        matches!((pos_a, pos_b, pos_c), (Some(a), Some(b), Some(c)) if a < b && b < c),
        "PTY input ordering should be preserved (a<b<c); got positions a={pos_a:?}, b={pos_b:?}, c={pos_c:?} in {:?}",
        String::from_utf8_lossy(&buf),
    );

    // Send EOF (Ctrl-D, 0x04) to make `cat` exit cleanly, then drain.
    svc.send_input(Request::new(SendInputRequest {
        process: pid_selector(pid),
        input: Some(ProcessInput {
            input: Some(ProcessInputKind::Pty(vec![0x04])),
        }),
    }))
    .await
    .expect("send_input");
    while let Ok(Some(_)) = timeout(Duration::from_secs(3), stream.next()).await {}
}
