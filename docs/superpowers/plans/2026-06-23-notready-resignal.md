# Not-Ready Park + Re-Signal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unbounded silent not-ready park with (A) a bounded silent park for plain commands and (B) a fully-synthetic, keepalive-protected park for a leading `sleep` that ends in a non-zero "resend"/"still-waiting" signal — never proxying the command.

**Architecture:** All changes are in the streaming process path of `crates/sandbawks-api/src/routes/envd_proxy.rs`. We split the sandbox classification (cache lookup, no wait) from the park, bound the silent park by a configurable knob, and add a synthetic park-then-resignal stream for intercepted sleeps. The e2b SDK requires the first Connect frame to be a real `start`, so the intercepted-sleep call is synthetic end-to-end (`start(pid=0)` → keepalives → notice) and the real command runs on the agent's retry.

**Tech Stack:** Rust, axum, tokio, `tokio::sync::mpsc` streaming bodies, Connect-RPC envelopes; Helm for config wiring.

**Spec:** `docs/superpowers/specs/2026-06-23-sleep-aware-notready-bounded-park-design.md`

---

## File Structure

- `crates/sandbawks-api/src/routes/envd_proxy.rs` — all Rust changes (config readers, classify/park split, synthetic sleep-park producer, `proc_start` wiring) and their unit tests (in the existing `#[cfg(test)] mod tests`).
- `deploy/helm/sandbawks/values.yaml` — two new `api.*` knobs.
- `deploy/helm/sandbawks/templates/api-deployment.yaml` — surface the knobs as env vars.
- `deploy/helm/sandbawks/Chart.yaml` — version bump.
- `changelog/` — dated entry replacing the old #641 one.

Env var names (match the `SANDBAWKS_` convention): `SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS` (default 15), `SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS` (default 300).

---

## Task 1: Configurable park-window knobs

**Files:**
- Modify: `crates/sandbawks-api/src/routes/envd_proxy.rs` (near the existing `ENVD_*` consts, ~line 171; tests in `mod tests`)

- [ ] **Step 1: Write the failing test for the pure parser**

Add to the `#[cfg(test)] mod tests` block (e.g. after the existing cap tests):

```rust
#[test]
fn parse_env_secs_valid_invalid_and_absent() {
    let default = std::time::Duration::from_secs(15);
    // Absent → default.
    assert_eq!(parse_env_secs(None, default), default);
    // Valid integer seconds.
    assert_eq!(
        parse_env_secs(Some("45"), default),
        std::time::Duration::from_secs(45)
    );
    // Zero is allowed (an operator may disable the silent park).
    assert_eq!(parse_env_secs(Some("0"), default), std::time::Duration::ZERO);
    // Garbage / empty / negative → default (parse of u64 fails).
    assert_eq!(parse_env_secs(Some(""), default), default);
    assert_eq!(parse_env_secs(Some("abc"), default), default);
    assert_eq!(parse_env_secs(Some("-5"), default), default);
}

#[test]
fn silent_park_max_defaults_to_15s() {
    // With no env override the Mode A silent-park ceiling is 15s. (Phase-
    // independent now; the old per-phase 15s/300s split is gone in Task 5.)
    assert_eq!(silent_park_max(), std::time::Duration::from_secs(15));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy::tests::parse_env_secs_valid_invalid_and_absent envd_proxy::tests::silent_park_max_defaults`
Expected: FAIL — `cannot find function parse_env_secs` / `silent_park_max`.

- [ ] **Step 3: Implement the pure parser and the two cached readers**

Insert after the `ENVD_RESUME_NOTICE_TIMEOUT` const (~line 185). Remove `ENVD_SLEEP_AWARE_MAX_WAIT` (~line 196) in Task 2; leave it for now so the file still compiles.

```rust
/// Default ceiling on the **silent** not-ready park (Mode A). A plain
/// not-ready command parks at most this long with no keepalive (it cannot
/// emit one — there is no `start` event yet), so it must stay under the
/// smallest expected ingress/LB idle timeout. Overridable via
/// `SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS`.
const SILENT_PARK_MAX_DEFAULT: std::time::Duration = std::time::Duration::from_secs(15);

/// Default ceiling on the **keepalive-protected** park when honoring a leading
/// `sleep <x>` (Mode B). Keepalive frames keep the connection alive, so this
/// may be long; the agent's own `commands.run(timeout=...)` is the real bound.
/// Overridable via `SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS`.
const SLEEP_HONOR_MAX_DEFAULT: std::time::Duration = std::time::Duration::from_secs(300);

/// Parse a whole-seconds duration from an environment value, falling back to
/// `default` on an absent or unparseable value. Pure, so it is unit-testable
/// without touching the process environment.
fn parse_env_secs(raw: Option<&str>, default: std::time::Duration) -> std::time::Duration {
    match raw {
        Some(s) => match s.parse::<u64>() {
            Ok(secs) => std::time::Duration::from_secs(secs),
            Err(_) => default,
        },
        None => default,
    }
}

/// Cached `SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS` (Mode A silent-park ceiling).
fn silent_park_max() -> std::time::Duration {
    static CACHED: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        let raw = std::env::var("SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS").ok();
        let v = parse_env_secs(raw.as_deref(), SILENT_PARK_MAX_DEFAULT);
        if raw.is_some() && v == SILENT_PARK_MAX_DEFAULT && raw.as_deref() != Some("15") {
            tracing::warn!(
                value = ?raw,
                "SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS unparseable; using default"
            );
        }
        v
    })
}

/// Cached `SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS` (Mode B keepalive-park ceiling).
fn sleep_honor_max() -> std::time::Duration {
    static CACHED: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        let raw = std::env::var("SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS").ok();
        parse_env_secs(raw.as_deref(), SLEEP_HONOR_MAX_DEFAULT)
    })
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy::tests::parse_env_secs_valid_invalid_and_absent envd_proxy::tests::silent_park_max_defaults`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-api/src/routes/envd_proxy.rs
git commit -m "feat(api): add configurable not-ready park-window knobs (#641)"
```

---

## Task 2: Add `classify_sandbox`; bound the silent park in `resolve_sandbox_outcome`

This is a refactor validated by the build and the existing test suite. The old
`resolve_sandbox_outcome_with_wait` / `not_ready_cap` / `honored_park_cap` /
`ENVD_SLEEP_AWARE_MAX_WAIT` are **kept** here because `proc_start` still calls
`_with_wait`; they are deleted in Task 5 once `proc_start` stops using them. This
keeps every task's build green.

**Files:**
- Modify: `crates/sandbawks-api/src/routes/envd_proxy.rs` (`resolve_sandbox_outcome` ~245; add `classify_sandbox` near `Resolved` ~215)

- [ ] **Step 1: Add `classify_sandbox` and rewrite `resolve_sandbox_outcome`**

Add the classification type and function (place near `Resolved`, ~215):

```rust
/// Non-blocking classification of the request's sandbox: a cache lookup
/// (seeding once on a miss to preserve the 404), with **no** park. Callers
/// decide whether and how to wait. Splitting this out lets the streaming
/// `proc_start` branch a leading `sleep` to the synthetic keepalive park
/// before any silent wait, while the unary/connect paths still park.
struct Classification {
    sandbox_id: String,
    requested_port: Option<String>,
    readiness: Readiness,
}

async fn classify_sandbox(state: &AppState, headers: &HeaderMap) -> AppResult<Classification> {
    let (sandbox_id, requested_port) = resolve_target(state, headers)?;
    let cached = match state.envd_cache.lookup(&sandbox_id) {
        Some(cached) => Some(cached),
        None => {
            let sandbox = state
                .sandbox_ops()
                .get(&state.default_namespace, &sandbox_id)
                .await?;
            state.envd_cache.seed_if_absent(&sandbox);
            state.envd_cache.lookup(&sandbox_id)
        }
    };
    // An empty cache after a successful `get` (degenerate empty-named CR) is
    // treated as transiently not-ready; the park's `wait_until_ready` handles
    // an absent entry by timing out into a transient notice.
    let readiness = cached.map(|c| c.readiness()).unwrap_or_else(|| {
        Readiness::NotReady(NotReady {
            sandbox_phase: SandboxPhase::default(),
            pod: None,
            failure_reason: None,
            terminal: false,
        })
    });
    Ok(Classification {
        sandbox_id,
        requested_port,
        readiness,
    })
}
```

**Rewrite** `resolve_sandbox_outcome` (currently delegates to `_with_wait`) to use
`classify_sandbox` and the bounded silent park. Leave `resolve_sandbox_outcome_with_wait`,
`not_ready_cap`, `honored_park_cap`, and the `ENVD_*` consts **in place** — `proc_start`
still calls `_with_wait`; they go away in Task 5.

```rust
/// Resolve the request's sandbox, parking the **silent** [`silent_park_max`]
/// window on a transient not-ready state. Used by the unary handlers and the
/// streaming `process.Connect` re-attach (which carry no leading `sleep` to
/// inspect). The streaming `process.Start` path classifies directly so it can
/// route a leading `sleep` to the synthetic keepalive park instead.
async fn resolve_sandbox_outcome(state: &AppState, headers: &HeaderMap) -> AppResult<Resolved> {
    let Classification {
        sandbox_id,
        requested_port,
        readiness,
    } = classify_sandbox(state, headers).await?;
    match readiness {
        Readiness::Ready(addr) => {
            warn_on_port_mismatch(&sandbox_id, &addr, requested_port.as_deref());
            Ok(Resolved::Ready { addr, sandbox_id })
        }
        Readiness::Paused => Err(sandbox_paused_error()),
        Readiness::NotReady(detail) if detail.terminal => {
            Ok(Resolved::NotReady { sandbox_id, detail })
        }
        Readiness::NotReady(_) => {
            let deadline = tokio::time::Instant::now() + silent_park_max();
            park_for_address(state, sandbox_id, requested_port.as_deref(), deadline).await
        }
    }
}
```

Note: `resolve_sandbox_outcome_with_wait` is now only reached from `proc_start`; that
is expected and temporary. It still references `not_ready_cap`/`honored_park_cap`, so
no dead-code warnings yet.

- [ ] **Step 2: Build and run the suite**

Run: `cargo build -p sandbawks-api 2>&1 | tail -20 && cargo test -p sandbawks-api --lib -- envd_proxy::tests`
Expected: clean build; all existing tests still PASS (the old per-phase cap tests
remain valid because `not_ready_cap` is untouched; the new `silent_park_max`/parser
tests pass).

- [ ] **Step 3: Clippy clean**

Run: `cargo clippy -p sandbawks-api --all-targets -- -D warnings 2>&1 | tail -5`
Expected: clippy clean (no dead code — `_with_wait` is still used by `proc_start`).

- [ ] **Step 4: Commit**

```bash
git add crates/sandbawks-api/src/routes/envd_proxy.rs
git commit -m "refactor(api): add classify_sandbox; bound the silent not-ready park (#641)"
```

---

## Task 3: Ready-resend exit code + shared notice frames

**Files:**
- Modify: `crates/sandbawks-api/src/routes/envd_proxy.rs` (`synthetic_notice_response` ~1956; exit-code consts ~1931; tests in `mod tests`)

- [ ] **Step 1: Write the failing test for the shared frame builder**

Add to `mod tests`:

```rust
#[test]
fn synthetic_notice_frames_emit_start_data_end_endstream() {
    // The shared builder produces the canonical 4-frame sequence: a synthetic
    // start (pid 0), one stderr data frame, an end carrying the exit code, and
    // the end-stream envelope.
    let frames = synthetic_notice_frames("NOTICE: still warming up\n", NOTICE_EXIT_TRANSIENT);
    assert_eq!(frames.len(), 4);
    // First frame decodes to a start event with pid 0.
    let body = serde_json::from_slice::<serde_json::Value>(&frames[0][5..]).unwrap();
    assert_eq!(body, serde_json::json!({ "event": { "start": { "pid": 0 } } }));
}

#[test]
fn ready_resend_exit_code_is_distinct() {
    // The "pod is ready, resend" signal must be distinguishable by exit code
    // from the "still waiting" transient notice.
    assert_ne!(NOTICE_EXIT_READY_RESEND, NOTICE_EXIT_TRANSIENT);
    assert_ne!(NOTICE_EXIT_READY_RESEND, NOTICE_EXIT_TERMINAL);
    assert_ne!(NOTICE_EXIT_READY_RESEND, 0);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy::tests::synthetic_notice_frames_emit envd_proxy::tests::ready_resend_exit_code`
Expected: FAIL — `synthetic_notice_frames` / `NOTICE_EXIT_READY_RESEND` not found.

- [ ] **Step 3: Add the exit code and factor out the frame builder**

Add after `NOTICE_EXIT_TERMINAL` (~1938):

```rust
/// Exit code on the synthetic stream when an intercepted `sleep` park ends
/// because the sandbox **became ready** — the leading `sleep` was consumed
/// while waiting, so the caller should resend the real command (issue #641).
///
/// `100` is a deliberate sandbawks sentinel outside the `sysexits.h` band
/// (64–78): there is no standard code for "ready now, resend," and claiming a
/// sysexits meaning would mislead. Distinct from `NOTICE_EXIT_TRANSIENT` (75,
/// "still waiting") so a harness can branch on the exit code alone; the stderr
/// message remains authoritative. The stream is fully synthetic, so there is
/// no collision with a real program's exit code.
const NOTICE_EXIT_READY_RESEND: i32 = 100;
```

Refactor `synthetic_notice_response` (~1956) to delegate to a new shared builder. Replace the `let frames = [ ... ];` array construction with a call, and add the builder:

```rust
/// Build the canonical synthetic 4-frame sequence (`start{pid:0}` →
/// `data{stderr}` → `end{exit_code}` → end-stream) used by every not-ready
/// notice and by the intercepted-`sleep` re-signal (issue #625, #641).
fn synthetic_notice_frames(stderr_line: &str, exit_code: i32) -> [Bytes; 4] {
    [
        process_event_frame(ProcessEventInner {
            start: Some(StartEventDto { pid: 0 }),
            data: None,
            end: None,
            keepalive: None,
        }),
        process_event_frame(ProcessEventInner {
            start: None,
            data: Some(DataEventDto {
                stdout: None,
                stderr: Some(b64(stderr_line.as_bytes())),
                pty: None,
            }),
            end: None,
            keepalive: None,
        }),
        process_event_frame(ProcessEventInner {
            start: None,
            data: None,
            end: Some(EndEventDto {
                exit_code,
                exited: true,
                status: String::new(),
                error: None,
            }),
            keepalive: None,
        }),
        envelope(END_STREAM_FLAG, b"{}"),
    ]
}
```

Then rewrite `synthetic_notice_response`'s body to use it:

```rust
fn synthetic_notice_response(detail: &NotReady) -> Response {
    let lead = if detail.terminal { "ERROR" } else { "NOTICE" };
    let exit_code = if detail.terminal {
        NOTICE_EXIT_TERMINAL
    } else {
        NOTICE_EXIT_TRANSIENT
    };
    let line = format!("{lead}: {}\n", not_ready_message(detail));
    let frames = synthetic_notice_frames(&line, exit_code);
    let body = Body::from_stream(futures::stream::iter(
        frames
            .into_iter()
            .map(Ok::<Bytes, std::convert::Infallible>),
    ));
    ([(header::CONTENT_TYPE, "application/connect+json")], body).into_response()
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy::tests::synthetic_notice_frames_emit envd_proxy::tests::ready_resend_exit_code`
Expected: PASS.

- [ ] **Step 5: Verify the existing synthetic-notice behavior is unchanged**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy::tests`
Expected: all existing tests still PASS (the refactor is behavior-preserving).

- [ ] **Step 6: Commit**

```bash
git add crates/sandbawks-api/src/routes/envd_proxy.rs
git commit -m "refactor(api): factor synthetic_notice_frames; add ready-resend exit code (#641)"
```

---

## Task 4: Synthetic keepalive park-then-resignal producer

**Files:**
- Modify: `crates/sandbawks-api/src/routes/envd_proxy.rs` (new functions near `forward_event_stream` ~1827; tests in `mod tests`)

- [ ] **Step 1: Write the failing test for the producer core**

Add to `mod tests` (the module already uses `#[tokio::test(start_paused = true)]` style; mirror the existing `silent_stream_emits_keepalive_within_interval` test at ~2411):

```rust
#[tokio::test(start_paused = true)]
async fn sleep_park_ready_emits_resend_notice() {
    // A readiness future that resolves Ready immediately → the producer emits
    // the synthetic start, then a ready-resend notice (exit 100), no timeout.
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let ready = async { crate::envd_cache::WaitOutcome::Ready("10.0.0.1:49983".into()) };
    run_sleep_park(tx, ready, PROC_KEEPALIVE_INTERVAL).await;

    // Frame 0: synthetic start(pid 0).
    let f0 = rx.recv().await.unwrap().unwrap();
    let v0 = serde_json::from_slice::<serde_json::Value>(&f0[5..]).unwrap();
    assert_eq!(v0, serde_json::json!({ "event": { "start": { "pid": 0 } } }));
    // Then the ready-resend frames carry exit 100 in the end event.
    let mut saw_ready_exit = false;
    while let Ok(frame) = rx.try_recv() {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&frame[5..]) {
            if v["event"]["end"]["exitCode"] == serde_json::json!(NOTICE_EXIT_READY_RESEND) {
                saw_ready_exit = true;
            }
        }
    }
    assert!(saw_ready_exit, "ready park must end with the resend exit code");
}

#[tokio::test(start_paused = true)]
async fn sleep_park_keeps_alive_then_times_out() {
    // A readiness future that never resolves before the deadline → the producer
    // emits the synthetic start, at least one keepalive across a silent interval,
    // and finally the transient still-waiting notice (exit 75).
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let detail = NotReady {
        sandbox_phase: SandboxPhase::Creating,
        pod: None,
        failure_reason: None,
        terminal: false,
    };
    // Resolve TimedOut only after two keepalive intervals of (virtual) silence.
    let timed_out = async move {
        tokio::time::sleep(PROC_KEEPALIVE_INTERVAL * 2 + std::time::Duration::from_secs(1)).await;
        crate::envd_cache::WaitOutcome::TimedOut(detail)
    };
    run_sleep_park(tx, timed_out, PROC_KEEPALIVE_INTERVAL).await;

    let mut keepalives = 0;
    let mut saw_transient_exit = false;
    while let Some(Ok(frame)) = rx.recv().await {
        if frame == keepalive_envelope() {
            keepalives += 1;
        }
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&frame[5..]) {
            if v["event"]["end"]["exitCode"] == serde_json::json!(NOTICE_EXIT_TRANSIENT) {
                saw_transient_exit = true;
            }
        }
    }
    assert!(keepalives >= 1, "a silent park must emit keepalives");
    assert!(saw_transient_exit, "timeout must end with the transient notice");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy::tests::sleep_park_`
Expected: FAIL — `run_sleep_park` not found.

- [ ] **Step 3: Implement the producer core and the response wrapper**

Add near `forward_event_stream` (~1827):

```rust
/// Drive a synthetic intercepted-`sleep` park to completion on `tx` (issue
/// #641). Sends a synthetic `start(pid = 0)` immediately (satisfying the SDK's
/// first-frame-must-be-`start` handshake), emits a keepalive every
/// `keepalive_every` while `readiness` is pending (keeping the connection alive
/// across a long wait on any ingress), then terminates with a non-zero notice:
///
/// * `Ready` → `NOTICE_EXIT_READY_RESEND` + "resend your command".
/// * `Paused` → terminal "sandbox is paused" notice.
/// * `Terminal` / `TimedOut` → the existing transient/terminal not-ready notice.
///
/// The call is **fully synthetic** — no real process is ever attached — so the
/// `pid = 0` start is harmless. The real command runs on the caller's retry.
///
/// Generic over the readiness future so the timing can be unit-tested with a
/// hand-built future (mirrors [`forward_event_stream`]'s generic stream).
async fn run_sleep_park<F>(
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::convert::Infallible>>,
    readiness: F,
    keepalive_every: std::time::Duration,
) where
    F: std::future::Future<Output = crate::envd_cache::WaitOutcome>,
{
    // Synthetic start first — the SDK reads this as the process start (pid 0).
    let start = process_event_frame(ProcessEventInner {
        start: Some(StartEventDto { pid: 0 }),
        data: None,
        end: None,
        keepalive: None,
    });
    if tx.send(Ok(start)).await.is_err() {
        return;
    }

    let keepalive = keepalive_envelope();
    let mut idle = tokio::time::interval(keepalive_every);
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    idle.tick().await; // consume the immediate first tick

    tokio::pin!(readiness);
    let outcome = loop {
        tokio::select! {
            o = &mut readiness => break o,
            _ = idle.tick() => {
                if tx.send(Ok(keepalive.clone())).await.is_err() {
                    return;
                }
            }
        }
    };

    // Terminate with the matching notice.
    let (line, exit_code) = match outcome {
        crate::envd_cache::WaitOutcome::Ready(_) => (
            "NOTICE: sandbox is now available; the leading `sleep` was consumed \
             while waiting — resend your command (without the leading sleep)\n"
                .to_owned(),
            NOTICE_EXIT_READY_RESEND,
        ),
        crate::envd_cache::WaitOutcome::Paused => (
            "ERROR: sandbox is paused; resume it before issuing per-sandbox RPCs\n"
                .to_owned(),
            NOTICE_EXIT_TERMINAL,
        ),
        crate::envd_cache::WaitOutcome::Terminal(detail) => {
            (format!("ERROR: {}\n", not_ready_message(&detail)), NOTICE_EXIT_TERMINAL)
        }
        crate::envd_cache::WaitOutcome::TimedOut(detail) => {
            (format!("NOTICE: {}\n", not_ready_message(&detail)), NOTICE_EXIT_TRANSIENT)
        }
    };
    for frame in synthetic_notice_frames(&line, exit_code) {
        if tx.send(Ok(frame)).await.is_err() {
            return;
        }
    }
}

/// Build the streaming response for an intercepted leading `sleep` against a
/// transiently not-ready sandbox: park (keepalive-protected) up to
/// `min(sleep, sleep_honor_max())` for readiness, then re-signal. Never proxies
/// the command (issue #641).
// Wired into `proc_start` in Task 5; the allow is removed there.
#[allow(dead_code)]
fn sleep_park_response(
    state: AppState,
    sandbox_id: String,
    sleep: std::time::Duration,
    cap: std::time::Duration,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(8);
    let park = sleep.min(cap);
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + park;
        let readiness = state.envd_cache.wait_until_ready(&sandbox_id, deadline);
        run_sleep_park(tx, readiness, PROC_KEEPALIVE_INTERVAL).await;
    });
    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    ([(header::CONTENT_TYPE, "application/connect+json")], body).into_response()
}
```

- [ ] **Step 4: Run the producer tests**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy::tests::sleep_park_`
Expected: PASS.

- [ ] **Step 5: Clippy + full module tests**

Run: `cargo clippy -p sandbawks-api --all-targets -- -D warnings 2>&1 | tail -5 && cargo test -p sandbawks-api --lib -- envd_proxy::tests`
Expected: clippy clean; all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/sandbawks-api/src/routes/envd_proxy.rs
git commit -m "feat(api): synthetic keepalive sleep-park with ready-resend signal (#641)"
```

---

## Task 5: Wire `proc_start` to the two modes

**Files:**
- Modify: `crates/sandbawks-api/src/routes/envd_proxy.rs` (`proc_start` ~1537-1583)

- [ ] **Step 1: Rewrite `proc_start` to classify, then branch**

Replace the body of `proc_start` (from the `let honored_sleep = ...` line through the resolve `match`) so it classifies once and routes: Ready → proxy; Paused → 403; terminal → notice; transient + leading sleep → `sleep_park_response`; transient + no sleep → silent park → proxy-or-notice. The full function:

```rust
async fn proc_start(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    // Decode the request *before* resolving so the not-ready decision can see a
    // leading `sleep <x>` (issue #641). The body is small and already buffered.
    let raw = read_body_bytes(body).await?;
    let inner = decode_stream_request_envelope(&raw)?;
    let req: StartReq = if inner.is_empty() {
        StartReq::default()
    } else {
        serde_json::from_slice(inner).map_err(|err| {
            AppError::bad_request("invalid_json", format!("decoding stream body: {err}"))
        })?
    };
    let honored_sleep = parse_leading_sleep(&req.process.cmd, &req.process.args);

    // Classify without parking so a leading `sleep` against a transiently
    // not-ready sandbox routes to the synthetic keepalive park, while a plain
    // command keeps the bounded silent park + #625 notice.
    let Classification {
        sandbox_id,
        requested_port,
        readiness,
    } = classify_sandbox(&state, &headers).await?;

    let (addr, sandbox_id) = match readiness {
        Readiness::Ready(addr) => {
            warn_on_port_mismatch(&sandbox_id, &addr, requested_port.as_deref());
            (addr, sandbox_id)
        }
        Readiness::Paused => return Err(sandbox_paused_error().into()),
        Readiness::NotReady(detail) if detail.terminal => {
            return Ok(synthetic_notice_response(&detail));
        }
        Readiness::NotReady(_) => {
            // Transient. A leading `sleep` → honored as a keepalive-protected
            // park that re-signals (never proxied). Otherwise a bounded silent
            // park, then proxy if it readied or emit the transient notice.
            if let Some(sleep) = honored_sleep {
                return Ok(sleep_park_response(
                    state.clone(),
                    sandbox_id,
                    sleep,
                    sleep_honor_max(),
                ));
            }
            let deadline = tokio::time::Instant::now() + silent_park_max();
            match park_for_address(&state, sandbox_id, requested_port.as_deref(), deadline).await? {
                Resolved::Ready { addr, sandbox_id } => (addr, sandbox_id),
                Resolved::NotReady { detail, .. } => {
                    return Ok(synthetic_notice_response(&detail));
                }
            }
        }
    };

    state.note_activity(&sandbox_id);
    let mut client = state.envd_pool.process_streaming(&addr).await?;
    let stream = client.start(req.into_proto()).await?.into_inner();
    Ok(build_event_stream_response(
        stream,
        Some(StreamActivity {
            state: state.clone(),
            sandbox_id,
        }),
    ))
}
```

- [ ] **Step 2: Delete the now-unused park machinery and stale tests**

`proc_start` no longer calls `resolve_sandbox_outcome_with_wait`, so delete the
dead code:

- Delete `resolve_sandbox_outcome_with_wait` (~254-308), `not_ready_cap` (~352),
  `honored_park_cap` (~376), and the `ENVD_SLEEP_AWARE_MAX_WAIT` const (~196).
- Remove the `#[allow(dead_code)]` line added above `sleep_park_response` in Task 4
  (it is now used by `proc_start`).
- Delete the stale unit tests `not_ready_cap_short_for_resume_long_for_cold_start`
  (~2685) and `honored_park_cap_extends_resume_window_bounded` (~2792).
- Check the remaining `ENVD_*` timeout consts:

Run: `grep -n 'ENVD_READY_TIMEOUT\|ENVD_RESUME_NOTICE_TIMEOUT' crates/sandbawks-api/src/routes/envd_proxy.rs`

Delete any of these consts that now have only doc-comment references, and reword
any doc comment whose `[...]` intra-doc link pointed at a deleted const to mention
`silent_park_max` / `sleep_honor_max` instead.

- [ ] **Step 3: Build**

Run: `cargo build -p sandbawks-api 2>&1 | tail -20`
Expected: clean build. (If `sandbox_paused_error().into()` fails, confirm `ConnectErr: From<AppError>`; the existing `?` on `classify_sandbox(...).await?` proves the conversion exists — use `return Err(ConnectErr::from(sandbox_paused_error()))` if the turbofish is needed.)

- [ ] **Step 4: Confirm proc_connect and the unary path are unchanged in shape**

Run: `grep -n 'resolve_sandbox_outcome\|sleep_park_response\|classify_sandbox' crates/sandbawks-api/src/routes/envd_proxy.rs`
Expected: `proc_connect` and the unary handler still call `resolve_sandbox_outcome` (now silent-park-bounded); only `proc_start` calls `classify_sandbox` + `sleep_park_response`.

- [ ] **Step 5: Full test + clippy + fmt**

Run: `cargo test -p sandbawks-api --lib -- envd_proxy && cargo clippy -p sandbawks-api --all-targets -- -D warnings 2>&1 | tail -5 && cargo fmt --check 2>&1 | tail -5`
Expected: tests PASS; clippy clean; fmt clean (run `cargo fmt` if not).

- [ ] **Step 6: Commit**

```bash
git add crates/sandbawks-api/src/routes/envd_proxy.rs
git commit -m "feat(api): route leading sleep on not-ready sandbox to keepalive re-signal (#641)"
```

---

## Task 6: Wire the knobs into Helm

**Files:**
- Modify: `deploy/helm/sandbawks/values.yaml` (the `api:` block)
- Modify: `deploy/helm/sandbawks/templates/api-deployment.yaml` (env list ~44-60)
- Modify: `deploy/helm/sandbawks/Chart.yaml` (version)

- [ ] **Step 1: Add the values**

In `deploy/helm/sandbawks/values.yaml`, inside the `api:` block (e.g. after `sandboxDomain: ""`):

```yaml
  # Max seconds the gateway parks a *plain* not-ready process.Start before
  # emitting the #625 retry notice. Silent (no keepalive — there is no process
  # yet), so keep it under your ingress/LB idle timeout. Surfaced as
  # SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS.
  envdSilentParkMaxSecs: 15
  # Max seconds the gateway honors a leading `sleep <x>` on a not-ready sandbox,
  # parking with keepalives before re-signalling the agent to resend (issue
  # #641). Keepalive-protected, so it may exceed the idle timeout; the client's
  # own commands.run(timeout=...) is the real bound. Surfaced as
  # SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS.
  envdSleepHonorMaxSecs: 300
```

- [ ] **Step 2: Surface them as env vars**

In `deploy/helm/sandbawks/templates/api-deployment.yaml`, add to the `env:` list (e.g. after the `SANDBAWKS_SANDBOX_DOMAIN` block, before `SANDBAWKS_SIGNING_SECRET` at ~55):

```yaml
            - name: SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS
              value: {{ .Values.api.envdSilentParkMaxSecs | quote }}
            - name: SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS
              value: {{ .Values.api.envdSleepHonorMaxSecs | quote }}
```

- [ ] **Step 3: Bump the chart version**

In `deploy/helm/sandbawks/Chart.yaml`, change `version: 0.1.27` to `version: 0.1.28` (leave `appVersion` untouched).

- [ ] **Step 4: Render-check the chart**

Run: `helm template deploy/helm/sandbawks | grep -A1 'SANDBAWKS_ENVD_S'`
Expected: both env vars render with values `"15"` and `"300"`. (If `helm` is unavailable, eyeball the template diff.)

- [ ] **Step 5: Commit**

```bash
git add deploy/helm/sandbawks/values.yaml deploy/helm/sandbawks/templates/api-deployment.yaml deploy/helm/sandbawks/Chart.yaml
git commit -m "chore(helm): wire not-ready park knobs; bump chart to 0.1.28 (#641)"
```

---

## Task 7: Documentation

**Files:**
- Delete: `changelog/20260622T111219Z-issue-641-sleep-aware-not-ready.md`
- Create: `changelog/<UTC timestamp>-issue-641-notready-resignal.md`
- Modify: `crates/sandbawks-api/src/routes/envd_proxy.rs` (module-level doc comment ~80-105)

- [ ] **Step 1: Replace the changelog entry**

```bash
git rm changelog/20260622T111219Z-issue-641-sleep-aware-not-ready.md
```

Create `changelog/$(date -u +%Y%m%dT%H%M%SZ)-issue-641-notready-resignal.md` with:

```markdown
# Not-ready park + re-signal (issue #641)

## What changed

The streaming `process.Start` not-ready path was reworked into two modes:

- **Plain not-ready command** parks **silently** at most
  `SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS` (default 15s, all phases including cold
  start — previously cold start parked 300s silently), then proxies if it
  readied or emits the existing #625 retry notice.
- **A leading `sleep <x>`** on a transiently not-ready sandbox is honored as a
  keepalive-protected park up to `min(<x>, SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS)`
  (default cap 300s). The call is fully synthetic — `start(pid=0)` + keepalives
  — and ends with a **non-zero** signal: "sandbox now available; resend your
  command" (exit 100) on readiness, or the transient "still waiting" notice
  (exit 75) on timeout. The command is **never proxied** from this call; the
  agent resends it verbatim against the now-ready sandbox.

## Why

Both e2b SDKs (js 2.30.5 / py 2.29.5) require the first Connect frame to be a
real `start` (JS throws; Python silently reads pid 0), so a keepalive cannot
precede a real process and a synthetic start cannot splice the real command
without corrupting pid. A single call therefore cannot both wait for an
unrealized sandbox and run the real command. Bounding the silent park keeps a
long quiet wait from being cut by an ingress idle timeout (which a headless
agent reads as a malfunction); the keepalive sleep-park gives positive
liveness, and the non-zero re-signal keeps the agent in a clean retry loop
instead of an opaque timeout.

## Config

- `SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS` (default 15) — Helm `api.envdSilentParkMaxSecs`.
- `SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS` (default 300) — Helm `api.envdSleepHonorMaxSecs`.
- An agent honoring a long `sleep` must set its own `commands.run(timeout=...)`
  to match (the e2b default is 60s).
```

- [ ] **Step 2: Update the module doc comment**

In `envd_proxy.rs`, update the module-level `//!` block (~80-105) that describes the resume park: remove the "cold start parks the full ready timeout" wording, and add a short note recording the **SDK first-frame-must-be-`start` constraint** and that intercepted sleeps run a synthetic keepalive park that re-signals (never proxies), so no one re-attempts keepalive-before-`start` or splice-across-readiness. Keep it to ~4-6 lines.

- [ ] **Step 3: Build docs to confirm no broken intra-doc links**

Run: `cargo doc -p sandbawks-api --no-deps 2>&1 | tail -15`
Expected: no warnings about broken `[...]` links (fix any that pointed at deleted consts).

- [ ] **Step 4: Commit**

```bash
git add changelog/ crates/sandbawks-api/src/routes/envd_proxy.rs
git commit -m "docs: changelog + module notes for not-ready re-signal (#641)"
```

---

## Final verification

- [ ] **Full gate** (mirrors the project's pre-commit checklist):

```bash
cargo fmt --check && \
cargo clippy -p sandbawks-api --all-targets -- -D warnings && \
cargo test -p sandbawks-api
```
Expected: fmt clean, clippy clean, all tests pass.

- [ ] **Confirm the old machinery is gone:**

```bash
grep -nE 'honored_park_cap|not_ready_cap|ENVD_SLEEP_AWARE_MAX_WAIT|resolve_sandbox_outcome_with_wait' crates/sandbawks-api/src/routes/envd_proxy.rs
```
Expected: no matches.
