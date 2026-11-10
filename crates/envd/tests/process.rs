//! Integration tests for [`envd::process::ProcessService`].
//!
//! These tests drive the `Process` gRPC trait directly (no transport,
//! no codec) just like the sibling `filesystem.rs` integration tests
//! do. That keeps them fast and deterministic — the surface under test
//! is the handler code, not the gRPC machinery.

use std::collections::{BTreeMap, HashMap};
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
    ConnectRequest, ConnectResponse, ListRequest, ProcessConfig, ProcessInput, ProcessSelector,
    SendInputRequest, SendSignalRequest, Signal, StartRequest, StartResponse,
};
use envd::state::ProcessSnapshot;

/// Build a basic `StartRequest`. `stdin = true` opens a pipe; `false`
/// inherits `/dev/null` (matches the proto's optional semantics).
fn start_request(cmd: &str, args: &[&str], stdin: bool) -> StartRequest {
    StartRequest {
        process: Some(ProcessConfig {
            cmd: cmd.to_string(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            envs: HashMap::new(),
            cwd: None,
        }),
        pty: None,
        tag: None,
        stdin: Some(stdin),
    }
}

/// Convenience: wrap a pid in the `ProcessSelector::Pid` oneof variant.
fn pid_selector(pid: u32) -> Option<ProcessSelector> {
    Some(ProcessSelector {
        selector: Some(Selector::Pid(pid)),
    })
}

/// Drain a `StartResponse` stream until the `EndEvent`, returning
/// (pid, accumulated stdout bytes, exit code).
///
/// Stops as soon as the `EndEvent` is observed; data after that is not
/// part of the contract (and shouldn't exist anyway since the reader
/// tasks are awaited before the end event is published).
async fn drain_start<S>(mut stream: S) -> (Option<u32>, Vec<u8>, Vec<u8>, Option<i32>)
where
    S: futures::Stream<Item = Result<StartResponse, tonic::Status>> + Unpin,
{
    let mut pid = None;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit_code = None;
    while let Some(msg) = stream.next().await {
        let resp = msg.expect("stream item");
        let event = resp.event.and_then(|e| e.event);
        match event {
            Some(ProcessEventKind::Start(s)) => pid = Some(s.pid),
            Some(ProcessEventKind::Data(d)) => match d.output {
                Some(DataOutput::Stdout(b)) => stdout.extend_from_slice(&b),
                Some(DataOutput::Stderr(b)) => stderr.extend_from_slice(&b),
                _ => {}
            },
            Some(ProcessEventKind::End(e)) => {
                exit_code = Some(e.exit_code);
                break;
            }
            _ => {}
        }
    }
    (pid, stdout, stderr, exit_code)
}

/// Drain a `ConnectResponse` stream until the `EndEvent`, returning
/// the accumulated stdout bytes and the exit code.
async fn drain_connect<S>(mut stream: S) -> (Vec<u8>, Option<i32>)
where
    S: futures::Stream<Item = Result<ConnectResponse, tonic::Status>> + Unpin,
{
    let mut stdout = Vec::new();
    let mut exit_code = None;
    while let Some(msg) = stream.next().await {
        let resp = msg.expect("stream item");
        let event = resp.event.and_then(|e| e.event);
        match event {
            Some(ProcessEventKind::Data(d)) => {
                if let Some(DataOutput::Stdout(b)) = d.output {
                    stdout.extend_from_slice(&b);
                }
            }
            Some(ProcessEventKind::End(e)) => {
                exit_code = Some(e.exit_code);
                break;
            }
            _ => {}
        }
    }
    (stdout, exit_code)
}

#[tokio::test]
async fn echo_hello_world() {
    let svc = ProcessService::new();
    let stream = svc
        .start(Request::new(start_request("echo", &["hello"], false)))
        .await
        .expect("start")
        .into_inner();

    // 5 s is generous; `echo` exits in microseconds. The timeout is
    // here so a hang shows up as a clear test failure instead of CI
    // killing the runner.
    let (pid, stdout, _stderr, exit_code) = timeout(Duration::from_secs(5), drain_start(stream))
        .await
        .expect("start stream completes");

    assert!(pid.is_some(), "start event should carry a pid");
    assert_eq!(stdout, b"hello\n");
    assert_eq!(exit_code, Some(0));
}

#[tokio::test]
async fn late_connect_observes_exit() {
    let svc = ProcessService::new();

    // Run a process that exits quickly, drain Start to get its pid +
    // confirm the registry has cached the EndEvent.
    let stream = svc
        .start(Request::new(start_request(
            "sh",
            &["-c", "sleep 0.1"],
            false,
        )))
        .await
        .expect("start")
        .into_inner();
    let (pid, _stdout, _stderr, exit_code) = timeout(Duration::from_secs(5), drain_start(stream))
        .await
        .expect("first stream completes");
    let pid = pid.expect("pid");
    assert_eq!(exit_code, Some(0));

    // Wait an extra 200 ms past exit, then Connect — broadcast won't
    // replay history, but the cached `EndEvent` should be replayed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stream = svc
        .connect(Request::new(ConnectRequest {
            process: pid_selector(pid),
        }))
        .await
        .expect("connect")
        .into_inner();
    let (_stdout, exit_code) = timeout(Duration::from_secs(5), drain_connect(stream))
        .await
        .expect("connect stream completes");
    assert_eq!(exit_code, Some(0), "late connect must see cached EndEvent");
}

#[tokio::test]
async fn stdin_pipe() {
    let svc = ProcessService::new();
    let stream = svc
        .start(Request::new(start_request("cat", &[], true)))
        .await
        .expect("start")
        .into_inner();

    // Pull the StartEvent (and only that) so we can grab the pid
    // before the response stream blocks waiting for input.
    let mut stream = Box::pin(stream);
    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first event")
        .expect("stream item")
        .expect("ok");
    let pid = match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    };

    // SendInput → expect to read it back on stdout via the response
    // stream, since `cat` echoes stdin to stdout.
    svc.send_input(Request::new(SendInputRequest {
        process: pid_selector(pid),
        input: Some(ProcessInput {
            input: Some(ProcessInputKind::Stdin(b"hi\n".to_vec())),
        }),
    }))
    .await
    .expect("send_input");

    let mut saw_stdout = false;
    let echo_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < echo_deadline {
        let next = timeout(echo_deadline - tokio::time::Instant::now(), stream.next())
            .await
            .expect("stdout event arrives")
            .expect("stream item")
            .expect("ok");
        if let Some(ProcessEventKind::Data(d)) = next.event.and_then(|e| e.event) {
            if let Some(DataOutput::Stdout(b)) = d.output {
                if b.windows(3).any(|w| w == b"hi\n") {
                    saw_stdout = true;
                    break;
                }
            }
        }
    }
    assert!(saw_stdout, "should observe cat's echoed stdout");

    // Now SIGTERM the cat process and confirm the stream surfaces the
    // EndEvent within a few seconds.
    svc.send_signal(Request::new(SendSignalRequest {
        process: pid_selector(pid),
        signal: Signal::Sigterm as i32,
    }))
    .await
    .expect("send_signal");

    let mut exit_code = None;
    while tokio::time::Instant::now() < echo_deadline {
        let next = timeout(echo_deadline - tokio::time::Instant::now(), stream.next())
            .await
            .expect("end event arrives")
            .expect("stream item")
            .expect("ok");
        if let Some(ProcessEventKind::End(e)) = next.event.and_then(|e| e.event) {
            exit_code = Some(e.exit_code);
            break;
        }
    }
    assert!(exit_code.is_some(), "must observe EndEvent after SIGTERM");
}

#[tokio::test]
async fn concurrent_connect_fanout() {
    let svc = ProcessService::new();

    // Run something whose output is emitted across multiple chunks so
    // each subscriber actually exercises the fan-out, then exits.
    let stream = svc
        .start(Request::new(start_request(
            "sh",
            &["-c", "echo one; echo two; echo three"],
            false,
        )))
        .await
        .expect("start")
        .into_inner();
    let mut start_stream = Box::pin(stream);
    let first = timeout(Duration::from_secs(5), start_stream.next())
        .await
        .expect("first event")
        .expect("stream item")
        .expect("ok");
    let pid = match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    };

    // Three Connect subscribers, all on the same pid.
    let a = svc
        .connect(Request::new(ConnectRequest {
            process: pid_selector(pid),
        }))
        .await
        .expect("connect a")
        .into_inner();
    let b = svc
        .connect(Request::new(ConnectRequest {
            process: pid_selector(pid),
        }))
        .await
        .expect("connect b")
        .into_inner();
    let c = svc
        .connect(Request::new(ConnectRequest {
            process: pid_selector(pid),
        }))
        .await
        .expect("connect c")
        .into_inner();

    // Drive all four streams to completion in parallel. The Start
    // stream needs to be drained so the wait task can finish and
    // publish the EndEvent.
    let (_, (a_stdout, a_code), (b_stdout, b_code), (c_stdout, c_code)) = tokio::join!(
        drain_start(start_stream),
        drain_connect(a),
        drain_connect(b),
        drain_connect(c),
    );

    assert_eq!(a_code, Some(0));
    assert_eq!(b_code, Some(0));
    assert_eq!(c_code, Some(0));
    // Each subscriber should see *some* output. We don't assert exact
    // bytes because broadcast is lossy when a subscriber lags, but the
    // tail (the final `three\n` close to exit) should be present.
    for (name, payload) in [("a", &a_stdout), ("b", &b_stdout), ("c", &c_stdout)] {
        assert!(
            payload.windows(b"three\n".len()).any(|w| w == b"three\n"),
            "subscriber {name} should see the final stdout chunk; got {payload:?}",
        );
    }
}

#[tokio::test]
async fn send_signal_kills() {
    let svc = ProcessService::new();
    let stream = svc
        .start(Request::new(start_request("sleep", &["60"], false)))
        .await
        .expect("start")
        .into_inner();
    let mut stream = Box::pin(stream);
    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first event")
        .expect("stream item")
        .expect("ok");
    let pid = match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    };

    svc.send_signal(Request::new(SendSignalRequest {
        process: pid_selector(pid),
        signal: Signal::Sigkill as i32,
    }))
    .await
    .expect("send_signal");

    let mut saw_end = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        let next = timeout(remaining, stream.next())
            .await
            .expect("event arrives")
            .expect("stream item")
            .expect("ok");
        if matches!(
            next.event.and_then(|e| e.event),
            Some(ProcessEventKind::End(_))
        ) {
            saw_end = true;
            break;
        }
    }
    assert!(saw_end, "SIGKILL should produce EndEvent within 3 s");
}

#[tokio::test]
async fn collect_snapshots_reports_running_pipe_process() {
    // Issue #174 contract: after a `Start`, the next snapshot capture
    // must surface the child with its full start-time metadata, so a
    // pause-time `/__state` probe is non-empty.
    let svc = ProcessService::new();
    let mut envs = HashMap::new();
    envs.insert("KEY_A".to_string(), "value-a".to_string());
    envs.insert("KEY_B".to_string(), "value-b".to_string());
    let cfg = ProcessConfig {
        cmd: "sleep".to_string(),
        args: vec!["30".to_string()],
        envs,
        cwd: Some("/tmp".to_string()),
    };
    let stream = svc
        .start(Request::new(StartRequest {
            process: Some(cfg.clone()),
            pty: None,
            tag: None,
            stdin: Some(false),
        }))
        .await
        .expect("start")
        .into_inner();
    let mut stream = Box::pin(stream);
    // Pull the StartEvent so the pid is observable before we probe.
    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first event")
        .expect("stream item")
        .expect("ok");
    let pid = match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    };

    let snapshots = svc.collect_process_snapshots().await;
    let entry = snapshots
        .iter()
        .find(|s| s.pid == pid)
        .unwrap_or_else(|| panic!("snapshot must contain pid {pid}; got {snapshots:?}"));
    assert_eq!(entry.cmd, "sleep");
    assert_eq!(entry.args, vec!["30".to_string()]);
    assert_eq!(entry.cwd, std::path::PathBuf::from("/tmp"));
    let expected: BTreeMap<String, String> = [
        ("KEY_A".to_string(), "value-a".to_string()),
        ("KEY_B".to_string(), "value-b".to_string()),
    ]
    .into_iter()
    .collect();
    assert_eq!(entry.env, expected);
    // `started_at` is wall-clock seconds; assert it's at least within
    // the last hour (sloppy bound, just guards against the placeholder
    // `0` accidentally landing in production code).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    assert!(
        entry.started_at > now - 3600 && entry.started_at <= now,
        "started_at should be within the last hour; got {} (now={now})",
        entry.started_at
    );

    // Clean up — drain the stream after killing so the wait task can
    // remove the entry within its retention window.
    svc.send_signal(Request::new(SendSignalRequest {
        process: pid_selector(pid),
        signal: Signal::Sigkill as i32,
    }))
    .await
    .expect("send_signal");
    while let Ok(Some(_)) = timeout(Duration::from_secs(3), stream.next()).await {}
}

#[tokio::test]
async fn boot_seed_is_addressable_but_not_in_collected_snapshots() {
    // Issue #178: the boot-time seed must NOT bleed into
    // `collect_process_snapshots` — that's the property that keeps
    // shutdown writes from growing the snapshot across pause/resume
    // cycles. It remains addressable via `boot_seed_processes` for the
    // future rehydration path.
    let seed = ProcessSnapshot {
        pid: 1234,
        cmd: "ghost".to_string(),
        args: vec!["arg1".to_string(), "arg2".to_string()],
        env: BTreeMap::new(),
        cwd: std::path::PathBuf::from("/var/empty"),
        started_at: 1_700_000_000,
    };
    let svc = ProcessService::with_seed(vec![seed.clone()], Vec::new());

    let snapshots = svc.collect_process_snapshots().await;
    assert!(
        snapshots.is_empty(),
        "boot seed must not appear in collect_process_snapshots (issue #178); got {snapshots:?}",
    );

    let seed_view = svc.boot_seed_processes();
    assert_eq!(seed_view.len(), 1);
    assert_eq!(seed_view[0], seed);
}

#[tokio::test]
async fn list_includes_running_and_recent_exits() {
    let svc = ProcessService::new();

    // One long-running and one quick process.
    let long_stream = svc
        .start(Request::new(start_request("sleep", &["10"], false)))
        .await
        .expect("start long")
        .into_inner();
    let mut long_stream = Box::pin(long_stream);
    let first = timeout(Duration::from_secs(5), long_stream.next())
        .await
        .expect("first event")
        .expect("stream item")
        .expect("ok");
    let long_pid = match first.event.and_then(|e| e.event) {
        Some(ProcessEventKind::Start(s)) => s.pid,
        other => panic!("expected StartEvent, got {other:?}"),
    };

    let quick_stream = svc
        .start(Request::new(start_request("true", &[], false)))
        .await
        .expect("start quick")
        .into_inner();
    let (quick_pid, _so, _se, quick_code) =
        timeout(Duration::from_secs(5), drain_start(quick_stream))
            .await
            .expect("quick stream completes");
    let quick_pid = quick_pid.expect("quick pid");
    assert_eq!(quick_code, Some(0));

    let resp = svc
        .list(Request::new(ListRequest {}))
        .await
        .expect("list")
        .into_inner();
    let pids: Vec<u32> = resp.processes.iter().map(|p| p.pid).collect();
    assert!(
        pids.contains(&long_pid),
        "List must include running process {long_pid}; got {pids:?}",
    );
    assert!(
        pids.contains(&quick_pid),
        "List must include recently-exited process {quick_pid}; got {pids:?}",
    );

    // Clean up: kill the long-runner so the test process doesn't sit
    // around waiting for kill_on_drop. Drains the stream so the wait
    // task can finish and release its registry entry.
    svc.send_signal(Request::new(SendSignalRequest {
        process: pid_selector(long_pid),
        signal: Signal::Sigkill as i32,
    }))
    .await
    .expect("send_signal");
    while let Ok(Some(_)) = timeout(Duration::from_secs(3), long_stream.next()).await {}
}
