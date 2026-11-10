//! Integration test for the PID-1 orphan reaper (issue #459).
//!
//! envd runs as PID 1 in the sandbox, so the kernel reparents every orphaned
//! grandchild (e.g. the `rustc`/`cc`/`tail` workers forked by `cargo` /
//! `just`) onto it; PID 1 must `waitpid` them or they accumulate as zombies.
//! The process-global `ChildReaper` (started lazily by `global_reaper` on the
//! first `ProcessService` construction) drives the sole `waitpid(-1)` loop
//! that collects them and routes each *direct* child's status back to its
//! wait task.
//!
//! The reaper is process-global and starts lazily on the first
//! `ProcessService` construction, so this test only has to make the process a
//! child subreaper and then drive a `Start` RPC. It still lives in its own
//! integration binary so the subreaper setting and the `waitpid(-1)` loop
//! cannot perturb the sibling `process.rs` / `pty.rs` tests.

use std::time::Duration;

use futures::StreamExt;
use tokio::time::{sleep, timeout};
use tonic::Request;

use envd::process::ProcessService;
use envd::proto::process::process_event::data_event::Output as DataOutput;
use envd::proto::process::process_event::Event as ProcessEventKind;
use envd::proto::process::process_server::Process;
use envd::proto::process::{ProcessConfig, StartRequest};

/// Build a `StartRequest` for `sh -c <script>` with a stdout pipe (no stdin).
fn sh_request(script: &str) -> StartRequest {
    StartRequest {
        process: Some(ProcessConfig {
            cmd: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            envs: Default::default(),
            cwd: None,
        }),
        pty: None,
        tag: None,
        stdin: Some(false),
    }
}

/// Drive a `Start` stream to its `EndEvent`, returning (stdout, exit_code).
async fn drain_start(svc: &ProcessService, script: &str) -> (Vec<u8>, i32) {
    let mut stream = svc
        .start(Request::new(sh_request(script)))
        .await
        .expect("start rpc")
        .into_inner();

    let mut stdout = Vec::new();
    let mut exit_code = None;
    while let Some(msg) = stream.next().await {
        let event = msg.expect("stream item").event.and_then(|e| e.event);
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
    (stdout, exit_code.expect("end event carried an exit code"))
}

/// Report whether `pid` is still present in `/proc` in the zombie (`Z`)
/// state. A missing `/proc/<pid>` entry means the process was reaped.
///
/// `/proc/<pid>/stat` is `pid (comm) state ...`; `comm` may itself contain
/// spaces or parentheses, so the state char is the first non-space token
/// after the **last** `)`.
fn is_zombie(pid: u32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.trim_start().chars().next())
            .map(|state| state == 'Z')
            .unwrap_or(false),
        // No entry: the process has been reaped (or never existed).
        Err(_) => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reaper_collects_orphans_and_preserves_direct_child_exit_code() {
    // Become a child subreaper so an orphaned grandchild reparents onto this
    // test process instead of real PID 1 — exactly the adoption envd relies
    // on as the sandbox's PID 1. Must happen before any child is spawned.
    nix::sys::prctl::set_child_subreaper(true).expect("set PR_SET_CHILD_SUBREAPER");

    // Constructing the service starts the process-global reaper (if it is not
    // already running), so no explicit reaper wiring is needed here.
    let svc = ProcessService::new();

    // Regression guard: a direct child's exit status must still be reported
    // accurately. The reaper — not `Child::wait` — now collects it and routes
    // it back over the per-child channel, so a non-trivial code proves the
    // routing path, not an accidental zero.
    let (_out, code) = drain_start(&svc, "exit 7").await;
    assert_eq!(
        code, 7,
        "direct child exit code must survive reaper routing"
    );

    // Orphan reaping: an inner shell prints its pid then `exec`s `sleep` (so
    // the printed pid *is* the grandchild's), while the outer shell
    // backgrounds it and exits immediately — orphaning the grandchild onto
    // this subreaper process. The grandchild then exits and becomes a zombie
    // that only our reaper will collect.
    //
    // The outer shell's stdout pipe stays open until the backgrounded
    // grandchild closes it on exit, so `drain_start` returns only *after* the
    // grandchild has exited: at that point it is either already reaped or a
    // zombie awaiting our reaper — never still running.
    let script = r#"sh -c 'echo $$; exec sleep 0.3' & exit 0"#;
    let (stdout, _code) = drain_start(&svc, script).await;
    let grandchild_pid: u32 = String::from_utf8_lossy(&stdout)
        .lines()
        .next()
        .and_then(|l| l.trim().parse().ok())
        .expect("grandchild printed its pid");

    // Poll until the reaper collects the zombie. Without the reaper the
    // grandchild would stay `Z` indefinitely (we are its reaping parent), so
    // this loop would exhaust its budget and the test would fail.
    let reaped = timeout(Duration::from_secs(5), async {
        while is_zombie(grandchild_pid) {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        reaped.is_ok(),
        "grandchild pid {grandchild_pid} left a lingering zombie; reaper did not collect it",
    );
}
