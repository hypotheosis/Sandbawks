//! Connect-RPC reverse proxy for per-sandbox envd calls (issue #191).
//!
//! # Why this module exists
//!
//! The official E2B Python and JS SDKs (>=2.21 / >=1.x) route every
//! per-sandbox call — `commands.run`, `files.write`, `files.read`,
//! `files.make_dir`, etc. — through the gateway base URL, carrying two
//! request headers:
//!
//! ```text
//! E2b-Sandbox-Id: sbx-<id>
//! E2b-Sandbox-Port: <envd-port>
//! ```
//!
//! In upstream E2B, the gateway is expected to read these headers,
//! resolve the matching sandbox's envd address, and reverse-proxy the
//! request (Connect-RPC JSON over HTTP) to that envd instance. The same
//! header-based resolution also backs the plain-HTTP `GET /health`
//! probe the SDK's `Sandbox.is_running()` issues — see [`health`]. Today
//! only `E2b-Sandbox-Id` is authoritative — with one envd per sandbox
//! the port is fixed in `status.envdAddress`, so `E2b-Sandbox-Port` is
//! read only to warn-log a mismatch (see `resolve_sandbox`). It
//! becomes load-bearing if multi-port routing is ever added.
//!
//! Our envd implementation only speaks native gRPC over HTTP/2 (it is a
//! plain `tonic` server). The two wire protocols are not byte-compatible
//! — Connect-RPC unary calls are unframed JSON bodies while native gRPC
//! requires length-prefixed framing plus HTTP/2 trailers. This module
//! bridges the two: each Connect-RPC method gets a per-method axum
//! handler that decodes the Connect-RPC JSON body, dispatches via the
//! pooled tonic client, and re-encodes the response in the Connect
//! shape.
//!
//! # Surface
//!
//! Filesystem (`POST /filesystem.Filesystem/<Method>`):
//!
//! - `MakeDir`, `Stat`, `Move`, `Remove`, `ListDir` — unary
//!
//! Process (`POST /process.Process/<Method>`):
//!
//! - `List`, `SendSignal`, `SendInput`, `Update`, `CloseStdin` — unary
//! - `Start`, `Connect` — server-streaming (Connect envelope framing)
//!
//! Direct HTTP file content I/O:
//!
//! - `GET /files?path=&username=...` — proxied to envd's `ReadAll`
//! - `POST /files?path=...` — multipart upload proxied to envd's `WriteAll`
//!
//! # Auth and pause gating
//!
//! The official E2B SDKs do *not* attach `X-API-Key` (or any other
//! credential header) to the per-sandbox envd RPC requests they make —
//! `sandbox_headers` on the SDK side carries only `E2b-Sandbox-Id`,
//! `E2b-Sandbox-Port`, and optionally `X-Access-Token`. In upstream
//! E2B, the security boundary is the team-scoped sandbox URL itself
//! (`{port}-{sandbox_id}.{team_domain}` resolves via wildcard DNS to a
//! team-isolated edge).
//!
//! This proxy therefore does *not* run the gateway's `auth_middleware`.
//! Instead every route requires the sandbox's own credential (issue #683),
//! checked in [`authorize`] before the sandbox is even looked up:
//!
//! - `X-Access-Token: <envdAccessToken>` — the token the create / connect /
//!   get answers carry (see [`crate::envd_auth`]); the SDK sends it on every
//!   per-sandbox call, Connect-RPC and `/health` alike.
//! - On `/files` only, a `signature` (+ optional `signature_expiration`) query
//!   parameter instead — the SDK's `download_url` / `upload_url`, so a signed
//!   URL can be handed to a client that has no token.
//!
//! A sandbox id alone is no longer a credential: ids are not secrets (they
//! appear in logs and hostnames and are partly time-ordered).
//!
//! If the resolved sandbox has `spec.desiredPhase == Paused`, the proxy
//! returns 403 with code `sandbox_paused` rather than forwarding the
//! call — gVisor checkpoints have no live envd to talk to (the runsc
//! process is frozen). Plan 04 T4's pause semantics are inherited
//! without any extra controller-side wiring.
//!
//! # Not-ready notice and park modes (issues #625, #641, #649)
//!
//! A tool call can race a sandbox that is assigned but *not yet serving* —
//! a cold start still pulling its image, a resume whose `runsc restore` is
//! in flight, or a backing Pod stuck in `ImagePullBackOff` /
//! `CrashLoopBackOff`. Rather than surface an opaque `503`, the proxy
//! classifies the sandbox from its controller-written `status.phase` +
//! `status.podStatus` projection and turns a not-ready sandbox into an
//! *actionable* signal:
//!
//! - **Unary** calls get a distinct code: `sandbox_not_ready`
//!   (transient — retry) or `sandbox_failed` (terminal — give up), both
//!   distinguishable from `sandbox_paused` and from a generic envd
//!   failure.
//! - **`process.Connect`** (re-attach) parks the silent `silent_park_max`
//!   window, then emits a synthetic NOTICE on stderr and a clean close.
//! - **`process.Start`** (issue #641) has three modes depending on the command
//!   and the projected Pod state:
//!   - *Mode A — plain command, scheduled-and-booting Pod:* parks **silently**
//!     up to `silent_park_max` (default 15 s, **all phases including cold
//!     start** — previously cold start parked 300 s silently), then proxies the
//!     command if the sandbox readied, or emits the transient/terminal NOTICE.
//!   - *Mode B — leading `sleep <x>`:* enters a **fully synthetic** keepalive
//!     park up to `min(<x>, sleep_honor_max())` (default cap 300 s), ending
//!     with a **non-zero** re-signal: exit 100 ("resend your command") on
//!     readiness, or exit 75 ("still waiting") on timeout. The command is
//!     **never proxied** from this call; the agent retries.
//!   - *Mode C — plain command, unscheduled/`Pending` Pod (issue #649):* when
//!     the backing Pod cannot plausibly serve within the silent window — it is
//!     not yet scheduled (`Pending`, no progressing reason) or stuck
//!     `Unschedulable` for capacity — the proxy emits the **transient** NOTICE
//!     (`exit 75`, "retry shortly (Pod is Pending)") **immediately**, with no
//!     silent park, so a client polling readiness with a sub-15 s per-call
//!     timeout actually receives the signal during a cold start instead of
//!     aborting on its own timeout first. This is **transient** (keep retrying),
//!     *not* the terminal fast-fail (`exit 69`, give up): a capacity-`Pending`
//!     Pod may schedule once a node frees up.
//!
//! **Why Mode B re-signals instead of holding the call open:** both e2b SDKs
//! require the first Connect frame to be a real `start` (JS throws on anything
//! else; Python silently reads `pid = 0`). A keepalive therefore cannot precede
//! a real process, and a synthetic `start` cannot splice the real command across
//! readiness without corrupting pid. A single call cannot simultaneously wait for
//! an unrealized sandbox and run the real command — hence the re-signal.

use axum::body::Body;
use axum::extract::{FromRequest, Multipart, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use bytes::Bytes;
use envd::proto::filesystem::{
    EntryInfo, FileType, ListDirRequest, MakeDirRequest, MoveRequest, ReadAllRequest,
    RemoveRequest, StatRequest, WriteAllRequest,
};
use envd::proto::process::{
    process_event, process_input, process_selector, pty::Size as PtySize, CloseStdinRequest,
    ConnectRequest, ListRequest, ProcessConfig, ProcessInput, ProcessSelector, Pty,
    SendInputRequest, SendSignalRequest, Signal, StartRequest, UpdateRequest,
};
use futures::StreamExt;
use sandbawks_types::{PodStatusSummary, SandboxPhase};
use serde::{Deserialize, Serialize};

use crate::{
    envd_auth::{self, FileOperation, FileRequest},
    envd_cache::{NotReady, Readiness, WaitOutcome},
    error::{AppError, AppResult},
    AppState,
};

// ---------- Shared helpers ----------

/// Header name carrying the sandbox id on every per-sandbox proxy call.
const HEADER_SANDBOX_ID: &str = "e2b-sandbox-id";

/// Header name carrying the in-sandbox envd port the SDK intends to
/// reach. With one envd per sandbox today this always matches the port
/// baked into `status.envdAddress`; we read it only to warn-log a
/// mismatch (see [`resolve_sandbox`]). When multi-port routing lands
/// it becomes load-bearing.
const HEADER_SANDBOX_PORT: &str = "e2b-sandbox-port";

/// Header carrying the sandbox's `envdAccessToken` (issue #683). The SDK sets
/// it on every per-sandbox call once the create / connect answer carries one.
const HEADER_ACCESS_TOKEN: &str = "x-access-token";

/// The credential a per-sandbox request must present (issue #683).
#[derive(Debug, Clone, Copy)]
enum Access<'a> {
    /// `X-Access-Token` only: every Connect-RPC route and `/health`.
    Token,
    /// `X-Access-Token`, or a `/files` URL signature over this request, so a
    /// signed download / upload URL works without the header.
    TokenOrSignature(FileRequest<'a>),
}

/// Reject the request unless it carries `sandbox_id`'s credential.
///
/// Runs before the sandbox is looked up, so an unauthenticated caller gets the
/// same `401` whether or not the id exists. Either credential is enough, as in
/// envd: a wrong `X-Access-Token` does not veto a valid `/files` signature.
///
/// # Errors
///
/// `Unauthorized` (`invalid_access_token` / `invalid_signature`) when neither
/// credential is valid.
fn authorize(
    state: &AppState,
    sandbox_id: &str,
    headers: &HeaderMap,
    access: Access<'_>,
) -> AppResult<()> {
    let key = &state.signing_key;
    let token = headers
        .get(HEADER_ACCESS_TOKEN)
        .and_then(|v| v.to_str().ok());
    if token.is_some_and(|t| envd_auth::verify_access_token(key, sandbox_id, t)) {
        return Ok(());
    }
    match access {
        Access::TokenOrSignature(request) if request.signature.is_some() => {
            envd_auth::verify_file_signature(
                key,
                sandbox_id,
                &request,
                chrono::Utc::now().timestamp(),
            )
        }
        Access::Token | Access::TokenOrSignature(_) => Err(AppError::unauthorized(
            "invalid_access_token",
            if token.is_some() {
                "X-Access-Token does not match this sandbox"
            } else {
                "missing X-Access-Token header (use the sandbox's envdAccessToken)"
            },
        )),
    }
}

/// Hard ceiling on a single `/files` upload body, enforced at the
/// HTTP-body layer via [`RequestBodyLimitLayer`].
///
/// `files_post` buffers the whole upload into memory before handing it
/// to envd's `WriteAll` (see that handler's "Buffering and size" note),
/// so an unbounded body is a direct OOM vector on the gateway pod.
/// axum's implicit `DefaultBodyLimit` is only 2 MiB and only covers the
/// `Multipart` extractor — the raw `application/octet-stream` path
/// bypasses it entirely. A single explicit limit on the route covers
/// both. 64 MiB is comfortably above the "config + small data file"
/// sizes the E2B SDK moves while keeping the per-request memory
/// footprint bounded; the chunked-transfer follow-up (Plan 07/09) is
/// where larger uploads get a streaming path instead.
const MAX_UPLOAD_BYTES: usize = 64 * 1024 * 1024;

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
        let v = parse_env_secs(raw.as_deref(), SLEEP_HONOR_MAX_DEFAULT);
        if raw.is_some() && v == SLEEP_HONOR_MAX_DEFAULT && raw.as_deref() != Some("300") {
            tracing::warn!(
                value = ?raw,
                "SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS unparseable; using default"
            );
        }
        v
    })
}

/// Outcome of resolving a per-sandbox request to a routable envd address.
///
/// `Ready` is the common case — proxy to `addr`. `NotReady` carries the
/// [`NotReady`] detail the gateway renders into an actionable notice: a
/// synthetic stderr frame for the streaming process handlers, or a distinct
/// `sandbox_not_ready` / `sandbox_failed` error code for the unary handlers.
enum Resolved {
    /// The sandbox is serving — route to `addr`. `sandbox_id` feeds the
    /// idle-timeout activity stamp on the work paths.
    Ready { addr: String, sandbox_id: String },
    /// The sandbox is not (yet) serving — still starting/restoring, or in a
    /// terminal state that will not recover without intervention.
    NotReady {
        #[allow(dead_code)]
        sandbox_id: String,
        detail: NotReady,
    },
}

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

async fn classify_sandbox(
    state: &AppState,
    headers: &HeaderMap,
    access: Access<'_>,
) -> AppResult<Classification> {
    let (sandbox_id, requested_port) = resolve_target(state, headers)?;
    authorize(state, &sandbox_id, headers, access)?;
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
    // an absent entry by timing out into a transient notice. This now parks the
    // 15s `silent_park_max` budget rather than the prior 300s cold-start budget
    // — deliberate: an empty-named CR is a configuration error, not a real cold
    // start, so a short notice-then-retry is the right behavior.
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

/// Resolve the request's sandbox, parking the **silent** [`silent_park_max`]
/// window on a transient not-ready state. Used by the unary handlers and the
/// streaming `process.Connect` re-attach (which carry no leading `sleep` to
/// inspect). The streaming `process.Start` path classifies directly so it can
/// route a leading `sleep` to the synthetic keepalive park instead.
///
/// Returns:
///
/// - `Err` `BadRequest` (`missing_sandbox_id`) — header/host absent.
/// - `Err` `NotFound` (`sandbox_not_found`) — sandbox doesn't exist.
/// - `Err` `Forbidden` (`sandbox_paused`) — `spec.desiredPhase == Paused`.
/// - `Ok(Resolved::Ready)` — serving; proxy to the address.
/// - `Ok(Resolved::NotReady)` — terminal (immediately) or still transiently
///   not-ready after the silent-park cap; the caller renders the notice.
async fn resolve_sandbox_outcome(
    state: &AppState,
    headers: &HeaderMap,
    access: Access<'_>,
) -> AppResult<Resolved> {
    let Classification {
        sandbox_id,
        requested_port,
        readiness,
    } = classify_sandbox(state, headers, access).await?;
    match readiness {
        Readiness::Ready(addr) => {
            warn_on_port_mismatch(&sandbox_id, &addr, requested_port.as_deref());
            Ok(Resolved::Ready { addr, sandbox_id })
        }
        Readiness::Paused => Err(sandbox_paused_error()),
        Readiness::NotReady(detail) if detail.terminal => {
            Ok(Resolved::NotReady { sandbox_id, detail })
        }
        // Transient, but the backing Pod cannot plausibly serve within the
        // silent window (unscheduled / capacity-`Pending`): surface the notice
        // immediately instead of parking the full `silent_park_max()`, so a
        // short-timeout poller on the unary / `process.Connect` path gets the
        // "retry shortly (Pod is Pending)" signal during a cold start (#649).
        // The streaming `process.Start` path applies the same gate in
        // `proc_start_mode` (the `ImmediateNotice` arm); keep the two aligned —
        // `NotReady::can_serve_within_silent_window` is the single source of
        // truth for the predicate itself.
        Readiness::NotReady(detail) if !detail.can_serve_within_silent_window() => {
            Ok(Resolved::NotReady { sandbox_id, detail })
        }
        Readiness::NotReady(_) => {
            let deadline = tokio::time::Instant::now() + silent_park_max();
            park_for_address(state, sandbox_id, requested_port.as_deref(), deadline).await
        }
    }
}

/// Park on the cache until `sandbox_id` becomes resolvable or `deadline`
/// elapses, mapping the [`WaitOutcome`] into a [`Resolved`].
async fn park_for_address(
    state: &AppState,
    sandbox_id: String,
    requested_port: Option<&str>,
    deadline: tokio::time::Instant,
) -> AppResult<Resolved> {
    match state
        .envd_cache
        .wait_until_ready(&sandbox_id, deadline)
        .await
    {
        WaitOutcome::Ready(addr) => {
            warn_on_port_mismatch(&sandbox_id, &addr, requested_port);
            Ok(Resolved::Ready { addr, sandbox_id })
        }
        WaitOutcome::Paused => Err(sandbox_paused_error()),
        // A mid-wait terminal state and a transient timeout both surface as a
        // notice; `detail.terminal` distinguishes "give up" from "retry".
        WaitOutcome::Terminal(detail) | WaitOutcome::TimedOut(detail) => {
            Ok(Resolved::NotReady { sandbox_id, detail })
        }
    }
}

/// The `sandbox_paused` rejection, shared by every resolve exit.
fn sandbox_paused_error() -> AppError {
    AppError::forbidden(
        "sandbox_paused",
        "sandbox is paused; resume it before issuing per-sandbox RPCs",
    )
}

/// Extract the duration of a leading `sleep <x>` from a `process.Start`
/// command, returning `None` when no unambiguous sleep is present (issue #641).
///
/// Recognized forms (POSIX/GNU `sleep` semantics):
///
/// * a direct `cmd == "sleep"` whose `args` are **all** valid duration
///   tokens (GNU sleep sums multiple operands, e.g. `sleep 1m 30s` → 90s), and
/// * a shell wrapper `cmd == "sh" | "bash"` with `args == ["-c", "<script>"]`
///   whose `<script>` **begins** with `sleep <x>` — the leading duration
///   operand(s) are summed and the remainder of the line is ignored (it runs
///   in envd normally once the sandbox is ready).
///
/// A duration token is a non-negative number with an optional single-letter
/// GNU suffix (`s`econds, `m`inutes, `h`ours, `d`ays); a bare number is
/// seconds. `sleep infinity` / `sleep inf` parse to a non-finite value and are
/// rejected (never honored) — as is anything ambiguous (variables, a `sleep`
/// that is not the first word, an empty operand list). Rejecting maps to the
/// current behavior: emit the not-ready notice immediately.
fn parse_leading_sleep(cmd: &str, args: &[String]) -> Option<std::time::Duration> {
    match cmd {
        "sleep" => sum_sleep_operands(args.iter().map(String::as_str)),
        "sh" | "bash" => {
            // Only the canonical `-c <script>` form; anything else is too
            // ambiguous to inspect safely.
            let [flag, script] = args else { return None };
            if flag != "-c" {
                return None;
            }
            let mut tokens = script.split_whitespace();
            if tokens.next()? != "sleep" {
                return None;
            }
            // Sum the leading duration operands, stopping at the first token
            // that is not a duration (a shell separator, the next command, …).
            // The script must begin with at least one duration operand.
            let mut total = std::time::Duration::ZERO;
            let mut parsed_any = false;
            for token in tokens {
                match parse_duration_token(token) {
                    Some(dur) => {
                        total = total.checked_add(dur)?;
                        parsed_any = true;
                    }
                    None => break,
                }
            }
            parsed_any.then_some(total)
        }
        _ => None,
    }
}

/// Sum a sequence of `sleep` operands, requiring at least one and that **every**
/// operand is a valid duration token (a direct `sleep` exec has no trailing
/// command, so a non-duration operand means the line is malformed → `None`).
fn sum_sleep_operands<'a>(operands: impl Iterator<Item = &'a str>) -> Option<std::time::Duration> {
    let mut total = std::time::Duration::ZERO;
    let mut parsed_any = false;
    for operand in operands {
        total = total.checked_add(parse_duration_token(operand)?)?;
        parsed_any = true;
    }
    parsed_any.then_some(total)
}

/// Parse a single GNU `sleep` duration token (e.g. `30`, `1.5`, `1m`, `2h`)
/// into a [`Duration`](std::time::Duration).
///
/// A bare number is seconds; a trailing `s`/`m`/`h`/`d` scales to
/// seconds/minutes/hours/days. Returns `None` for an empty numeric part, a
/// negative/`NaN`/infinite value (so `infinity`/`inf` are rejected), or an
/// overflowing duration.
fn parse_duration_token(token: &str) -> Option<std::time::Duration> {
    // Suffix chars are single-byte ASCII, so byte-slicing the last char is safe.
    let (number, multiplier) = match token.as_bytes().last()? {
        b's' => (&token[..token.len() - 1], 1.0),
        b'm' => (&token[..token.len() - 1], 60.0),
        b'h' => (&token[..token.len() - 1], 3600.0),
        b'd' => (&token[..token.len() - 1], 86_400.0),
        // No recognized suffix: the whole token is a bare-seconds number.
        _ => (token, 1.0),
    };
    let value: f64 = number.parse().ok()?;
    // `try_from_secs_f64` rejects negative, `NaN`, infinite, and overflowing
    // values — covering `sleep infinity` and `sleep inf` (which parse to a
    // non-finite f64) without a special case.
    std::time::Duration::try_from_secs_f64(value * multiplier).ok()
}

/// Resolve the sandbox and refresh its idle-timeout activity stamp (issue
/// #481), returning `status.envdAddress` for the **unary** proxy handlers.
///
/// A not-ready sandbox maps to a distinct, retry-or-give-up error code (see
/// [`not_ready_error`]) rather than an opaque `503 unavailable` (issue #625).
///
/// The liveness probe (`/health`) deliberately bypasses this and resolves
/// without stamping activity: a status poll is not sandbox *work*, and
/// counting it would let an `is_running` loop keep an idle sandbox alive
/// forever.
async fn resolve_sandbox(
    state: &AppState,
    headers: &HeaderMap,
    access: Access<'_>,
) -> AppResult<String> {
    match resolve_sandbox_outcome(state, headers, access).await? {
        Resolved::Ready { addr, sandbox_id } => {
            state.note_activity(&sandbox_id);
            Ok(addr)
        }
        Resolved::NotReady { detail, .. } => Err(not_ready_error(&detail)),
    }
}

/// Map a [`NotReady`] detail to the unary error code (issue #625):
/// `sandbox_failed` (terminal, `failed_precondition` — give up) or
/// `sandbox_not_ready` (transient, `unavailable` — retry shortly), both
/// distinguishable from `sandbox_paused` and from a generic envd failure.
fn not_ready_error(detail: &NotReady) -> AppError {
    let message = not_ready_message(detail);
    if detail.terminal {
        AppError::failed_precondition("sandbox_failed", message)
    } else {
        // Transient: the sandbox is resuming / still starting, so a retry once
        // it is serving will succeed. Tag the message with the POSIX `EAGAIN`
        // ("resource temporarily unavailable") vocabulary so a client — or an
        // LLM driving a harness — reads it unambiguously as "retry later",
        // reinforcing the 502 / `UpstreamUnavailable` retry rail this already
        // rides (issue #652). The `sandbox_paused` rejection (a deliberately
        // paused sandbox with no resume in flight) stays a distinct
        // "resume-required" precondition, *not* this retryable signal.
        AppError::upstream(
            "sandbox_not_ready",
            format!("{message} [EAGAIN: resource temporarily unavailable]"),
        )
    }
}

/// Human-readable description of a not-ready sandbox, shared by the unary
/// error body and the streaming stderr notice.
fn not_ready_message(detail: &NotReady) -> String {
    let pod = detail.pod.as_ref().map(format_pod_summary);
    if detail.terminal {
        // Lead with the most actionable cause: the stuck Pod, else the
        // sandbox-level failure reason, else the bare phase.
        let cause = pod
            .or_else(|| {
                detail
                    .failure_reason
                    .as_ref()
                    .map(|r| format!("the sandbox failed ({r})"))
            })
            .unwrap_or_else(|| format!("the sandbox is {:?}", detail.sandbox_phase));
        format!("sandbox will not become ready without intervention: {cause}")
    } else {
        let activity = match detail.sandbox_phase {
            SandboxPhase::Resuming | SandboxPhase::Paused => "restoring its state",
            SandboxPhase::Pausing => "completing a pause",
            _ => "starting up",
        };
        match pod {
            Some(pod) => format!("sandbox is still {activity} ({pod}); retry shortly"),
            None => format!("sandbox is still {activity}; retry shortly"),
        }
    }
}

/// Render a projected [`PodStatusSummary`] as a compact human clause, e.g.
/// `Pod is Pending (ImagePullBackOff: back-off pulling image ...)`.
fn format_pod_summary(pod: &PodStatusSummary) -> String {
    match (&pod.reason, &pod.message) {
        (Some(reason), Some(message)) => format!("Pod is {} ({reason}: {message})", pod.phase),
        (Some(reason), None) => format!("Pod is {} ({reason})", pod.phase),
        (None, _) => format!("Pod is {}", pod.phase),
    }
}

/// Warn if the caller's *intended* envd port disagrees with the port in
/// the resolved `status.envdAddress`.
///
/// `requested_port` is whichever the caller supplied — the `{port}`
/// label of the E2B wildcard hostname, or the `E2b-Sandbox-Port` header
/// — resolved once in [`resolve_target`]. We dial `envd_address`
/// verbatim regardless: with one envd per sandbox the requested port is
/// redundant. But a divergent port means the caller *expected* to reach
/// a different endpoint than the one we'll route to, which would
/// otherwise be a silent wrong-endpoint bug. A warn-log surfaces it
/// without failing the request (the port is not authoritative). When
/// per-port routing lands this should become a real lookup instead of a
/// warning.
fn warn_on_port_mismatch(sandbox_id: &str, envd_address: &str, requested_port: Option<&str>) {
    let Some(requested) = requested_port else {
        return;
    };
    // `status.envdAddress` is `host:port`; take the segment after the
    // last ':' so IPv6 literals (which contain ':') still parse.
    let resolved = envd_address.rsplit(':').next().unwrap_or("");
    if requested != resolved {
        tracing::warn!(
            sandbox_id,
            requested_port = requested,
            resolved_port = resolved,
            envd_address,
            "requested envd port disagrees with status.envdAddress port; \
             routing to the resolved address (port is not authoritative)"
        );
    }
}

/// Resolve the `(sandbox_id, requested_port)` pair a request targets.
///
/// Two sources, in precedence order (issue #364):
///
/// 1. **E2B wildcard hostname** — only when `state.sandbox_domain` is
///    configured. The official SDKs encode the target as
///    `{port}-{sandbox_id}.{domain}` in the URL, so behind a wildcard
///    Ingress the `Host` carries everything we need and no
///    `E2b-Sandbox-Id` header is required. See [`parse_sandbox_authority`].
/// 2. **`E2b-Sandbox-Id` header** — the fallback for localhost / kind /
///    port-forward, where there is no hostname to demux on. The
///    `E2b-Sandbox-Port` header supplies the (non-authoritative) port.
///
/// A `Host` that doesn't parse as a sandbox of the configured domain
/// (e.g. `localhost:8080`, or a bare apex) falls through to the header
/// rather than erroring — the two mechanisms coexist on one gateway.
///
/// Returns `BadRequest` (`missing_sandbox_id`) only when *neither*
/// source yields an id, preserving the existing contract.
fn resolve_target(state: &AppState, headers: &HeaderMap) -> AppResult<(String, Option<String>)> {
    // Hostname path: only attempt it when a base domain is configured.
    // `header::HOST` is what an Ingress forwards on the upstream HTTP/1.1
    // request; for direct HTTP/2 the `:authority` pseudo-header is what
    // hyper folds into this same `host` entry.
    if let Some(domain) = state.sandbox_domain.as_deref() {
        if let Some(authority) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) {
            if let Some((id, port)) = parse_sandbox_authority(authority, domain) {
                // The Host wins (documented contract). But if a sidecar or
                // proxy *also* injected a disagreeing `E2b-Sandbox-Id` header,
                // surface it: silently dropping it would otherwise mask a
                // misconfiguration. Same diagnostic spirit as
                // `warn_on_port_mismatch` — the header is already in scope.
                if let Some(header_id) = headers
                    .get(HEADER_SANDBOX_ID)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    if header_id != id {
                        tracing::warn!(
                            host_sandbox_id = %id,
                            header_sandbox_id = header_id,
                            "E2b-Sandbox-Id header disagrees with the \
                             hostname-derived sandbox id; routing to the \
                             hostname (Host wins)"
                        );
                    }
                }
                return Ok((id, Some(port)));
            }
        }
    }

    // Header fallback.
    let sandbox_id = headers
        .get(HEADER_SANDBOX_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::bad_request(
                "missing_sandbox_id",
                "no sandbox id: set the E2b-Sandbox-Id header or address the \
                 gateway as {port}-{sandbox_id}.{domain}",
            )
        })?;
    let requested_port = headers
        .get(HEADER_SANDBOX_PORT)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    Ok((sandbox_id.to_owned(), requested_port))
}

/// Parse an E2B sandbox URL authority of the form
/// `{port}-{sandbox_id}.{domain}` into `(sandbox_id, port)`.
///
/// Returns `None` (signalling "not a sandbox host — fall back to the
/// header") when `authority` is not a subdomain of `domain`, the port
/// segment is absent or non-numeric, or the id segment is empty.
///
/// Splitting on the **first** `-` is deliberate: only the leading
/// `{port}` segment is fixed, so sandbox ids that themselves contain
/// dashes (e.g. `sbx-abc123`) survive intact.
///
/// Matching is normalized to be robust to how `Host` values actually
/// arrive: a trailing `:port` (e.g. `host:8080`) is dropped, a trailing
/// FQDN dot (`...domain.`) is trimmed, and both sides are lowercased —
/// DNS names are case-insensitive (RFC 1035) and intermediaries don't
/// normalize the HTTP `Host`, so a mixed-case authority must still match.
fn parse_sandbox_authority(authority: &str, domain: &str) -> Option<(String, String)> {
    // Defensive: an empty configured domain is degenerate (`strip_suffix("")`
    // always succeeds), so reject it outright rather than parse weirdly. The
    // binary already normalizes an empty `SANDBAWKS_SANDBOX_DOMAIN` to `None`;
    // this keeps the parser correct in isolation regardless of caller.
    if domain.is_empty() {
        return None;
    }
    // Normalize the authority for matching: drop a trailing `:port`, trim a
    // trailing FQDN dot, and lowercase. A bracketed IPv6 literal (`[::1]:443`)
    // splits to `host = "["`, which can't strip-suffix any real DNS domain and
    // so falls through cleanly — the split is safe because that input is
    // *handled*, not because it can't occur. k8s sandbox-id resource names are
    // always lowercase, so lowercasing the extracted id keeps it aligned with
    // the CR name.
    let host = authority
        .split(':')
        .next()
        .unwrap_or(authority)
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let domain = domain.to_ascii_lowercase();
    // Must be a strict subdomain of the configured base domain: strip
    // the suffix and the separating dot. A bare apex (`host == domain`)
    // leaves an empty label after the dot strip and is rejected below.
    let label = host.strip_suffix(domain.as_str())?.strip_suffix('.')?;
    let (port, sandbox_id) = label.split_once('-')?;
    if port.is_empty() || sandbox_id.is_empty() {
        return None;
    }
    // Reject a non-numeric leading segment so hostnames that merely
    // contain a dash (and aren't E2B sandbox URLs) fall through to the
    // header path instead of being mis-parsed.
    if !port.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((sandbox_id.to_owned(), port.to_owned()))
}

/// Pull `application/json` POST body bytes and decode them with serde.
///
/// Connect-RPC unary uses content-type `application/json` (no envelope
/// framing). We accept missing/empty bodies as the default value of `T`
/// because some E2B SDK call sites send an empty object for `Empty`
/// requests like `process.List`.
async fn decode_json_body<T>(body: Body) -> AppResult<T>
where
    T: for<'de> Deserialize<'de> + Default,
{
    let bytes = read_body_bytes(body).await?;
    if bytes.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(&bytes)
        .map_err(|err| AppError::bad_request("invalid_json", format!("decoding body: {err}")))
}

/// Drain a body into a `Vec<u8>`, mapping transport errors to a 500.
async fn read_body_bytes(body: Body) -> AppResult<Vec<u8>> {
    use futures::TryStreamExt;
    body.into_data_stream()
        .map_err(|err| AppError::Internal(anyhow::anyhow!(err)))
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
}

/// Render a JSON value as the standard Connect-RPC unary response.
fn json_ok<T: Serialize>(value: &T) -> Result<Response, ConnectErr> {
    let body = serde_json::to_vec(value).map_err(|err| {
        ConnectErr(AppError::Internal(anyhow::anyhow!(
            "serializing response: {err}"
        )))
    })?;
    Ok(([(header::CONTENT_TYPE, "application/json")], body).into_response())
}

/// Newtype wrapper that renders an [`AppError`] in the Connect-RPC
/// canonical error envelope rather than the sandbawks-standard one.
///
/// The Connect protocol mandates a specific JSON error shape and a
/// closed set of `code` values (see
/// <https://connectrpc.com/docs/protocol/#error-codes>). The SDKs
/// branch on those codes — e.g. the Python SDK's
/// `Filesystem.exists()` parses the response and returns `False` only
/// when it sees `Code.not_found`; any other code surfaces as a raised
/// exception. Returning sandbawks's `{code, message, request_id}`
/// envelope here would make every "file does not exist" surface as a
/// `SandboxException(Code.unknown, ...)` instead of `False`.
///
/// Scoped to the unary handlers in this module (the streaming handlers
/// already emit canonical errors in their end-stream envelope, and the
/// HTTP `/files` route is consumed by the SDK via HTTP-status branching
/// rather than the body shape, so it keeps the sandbawks envelope).
///
/// # request-id asymmetry
///
/// Unlike the sandbawks `AppError` envelope, the Connect canonical body
/// does *not* include the `request_id` field — the Connect error schema
/// has no place for it. Correlation with logs is still possible via the
/// `X-Request-Id` response header, which the outer `request_id_layer`
/// sets on every response regardless of body shape. So the id is in the
/// header but not the body on these routes, the inverse of every other
/// handler in this crate (which emits it in both).
struct ConnectErr(AppError);

impl From<AppError> for ConnectErr {
    fn from(err: AppError) -> Self {
        Self(err)
    }
}

impl From<tonic::Status> for ConnectErr {
    fn from(status: tonic::Status) -> Self {
        Self(AppError::from(status))
    }
}

impl IntoResponse for ConnectErr {
    fn into_response(self) -> Response {
        // Mapping table mirrors issue #200's spec:
        //   `AppError::BadRequest`         → invalid_argument / 400
        //   `AppError::NotFound`           → not_found        / 404
        //   `AppError::Unauthorized`       → unauthenticated  / 401
        //   `AppError::Forbidden`          → permission_denied/ 403
        //   `AppError::UpstreamUnavailable`→ unavailable      / 503
        //   `AppError::FailedPrecondition` → failed_precondition / 412
        //   `AppError::Internal`           → internal         / 500
        let (status, code, message) = match self.0 {
            AppError::BadRequest { message, .. } => {
                (StatusCode::BAD_REQUEST, "invalid_argument", message)
            }
            AppError::NotFound { message, .. } => (StatusCode::NOT_FOUND, "not_found", message),
            AppError::Unauthorized { message, .. } => {
                (StatusCode::UNAUTHORIZED, "unauthenticated", message)
            }
            AppError::Forbidden { message, .. } => {
                (StatusCode::FORBIDDEN, "permission_denied", message)
            }
            AppError::UpstreamUnavailable { message, .. } => {
                (StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
            }
            AppError::FailedPrecondition { message, .. } => (
                StatusCode::PRECONDITION_FAILED,
                "failed_precondition",
                message,
            ),
            AppError::Internal(err) => {
                // Second 5xx chokepoint: the Connect-protocol routes own this
                // `IntoResponse` and intercept `AppError::Internal` before
                // `error.rs` runs, so it needs the same exception-event emission
                // to surface in the APM's Issues view. The MISSING message is
                // load-bearing — tracing-opentelemetry only rewrites a
                // *messageless* error event with an `error` field into an
                // `exception` event. DO NOT add a message string here (see the
                // longer rationale on `AppError::Internal` in `error.rs`).
                tracing::error!(error = ?err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "an internal error occurred".to_owned(),
                )
            }
        };
        (
            status,
            Json(serde_json::json!({ "code": code, "message": message })),
        )
            .into_response()
    }
}

// ---------- DTOs (proto3 canonical JSON shape) ----------

/// Proto enum `FileType` rendered as the SDK-visible string form.
///
/// Proto3-canonical JSON spells these `"FILE_TYPE_*"`, so the wire
/// names keep the `FILE_TYPE_` prefix verbatim per-variant (we'd lose
/// the prefix with `rename_all` — clippy's `enum_variant_names` lint
/// is intentionally suppressed because the prefix is the spec).
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum FileTypeStr {
    #[default]
    #[serde(rename = "FILE_TYPE_UNSPECIFIED")]
    FileTypeUnspecified,
    #[serde(rename = "FILE_TYPE_FILE")]
    FileTypeFile,
    #[serde(rename = "FILE_TYPE_DIRECTORY")]
    FileTypeDirectory,
    #[serde(rename = "FILE_TYPE_SYMLINK")]
    FileTypeSymlink,
}

impl FileTypeStr {
    fn from_proto(value: i32) -> Self {
        match FileType::try_from(value) {
            Ok(FileType::File) => Self::FileTypeFile,
            Ok(FileType::Directory) => Self::FileTypeDirectory,
            Ok(FileType::Symlink) => Self::FileTypeSymlink,
            Ok(FileType::Unspecified) | Err(_) => Self::FileTypeUnspecified,
        }
    }
}

/// Wire shape of `filesystem.EntryInfo` — matches the field names the
/// official Python SDK's `json_format.MessageToJson` produces.
///
/// Serialize-only: we only ever encode this struct on the wire (envd's
/// proto types are the source of truth for any incoming entry). The
/// `skip_serializing_if` attrs keep zero-valued fields out of the JSON,
/// matching proto3-canonical's "default values are omitted" rule.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct EntryInfoDto {
    #[serde(skip_serializing_if = "String::is_empty")]
    name: String,
    #[serde(rename = "type", skip_serializing_if = "is_default_filetype")]
    kind: FileTypeStr,
    #[serde(skip_serializing_if = "String::is_empty")]
    path: String,
    #[serde(skip_serializing_if = "is_default_i64")]
    size: i64,
    #[serde(skip_serializing_if = "is_default_u32")]
    mode: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    permissions: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    owner: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    group: String,
    /// RFC 3339 timestamp string. Proto3-canonical JSON renders
    /// `google.protobuf.Timestamp` this way; the SDK's
    /// `json_format.Parse` accepts the same.
    #[serde(skip_serializing_if = "Option::is_none")]
    modified_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symlink_target: Option<String>,
}

fn is_default_filetype(v: &FileTypeStr) -> bool {
    matches!(v, FileTypeStr::FileTypeUnspecified)
}

fn is_default_i64(v: &i64) -> bool {
    *v == 0
}

fn is_default_u32(v: &u32) -> bool {
    *v == 0
}

impl EntryInfoDto {
    fn from_proto(entry: EntryInfo) -> Self {
        let modified_time = entry.modified_time.and_then(|ts| {
            let nanos = u32::try_from(ts.nanos).ok()?;
            chrono::DateTime::from_timestamp(ts.seconds, nanos).map(|dt| dt.to_rfc3339())
        });
        Self {
            kind: FileTypeStr::from_proto(entry.r#type),
            name: entry.name,
            path: entry.path,
            size: entry.size,
            mode: entry.mode,
            permissions: entry.permissions,
            owner: entry.owner,
            group: entry.group,
            modified_time,
            symlink_target: entry.symlink_target,
        }
    }
}

// ---------- Filesystem method DTOs ----------

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MakeDirReq {
    #[serde(default)]
    path: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MakeDirRes {
    entry: EntryInfoDto,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatReq {
    #[serde(default)]
    path: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatRes {
    entry: EntryInfoDto,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MoveReq {
    #[serde(default)]
    source: String,
    #[serde(default)]
    destination: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MoveRes {
    entry: EntryInfoDto,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoveReq {
    #[serde(default)]
    path: String,
}

#[derive(Debug, Default, Serialize)]
struct EmptyMessage {}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ListDirReq {
    #[serde(default)]
    path: String,
    #[serde(default)]
    depth: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ListDirRes {
    entries: Vec<EntryInfoDto>,
}

// ---------- Filesystem handlers ----------

#[tracing::instrument(skip_all, name = "handler:envd_proxy::fs_make_dir")]
async fn fs_make_dir(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: MakeDirReq = decode_json_body(body).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let entry = client
        .make_dir(MakeDirRequest { path: req.path })
        .await?
        .into_inner()
        .entry
        .ok_or_else(|| AppError::upstream("envd_invalid_response", "MakeDir returned no entry"))?;
    json_ok(&MakeDirRes {
        entry: EntryInfoDto::from_proto(entry),
    })
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::fs_stat")]
async fn fs_stat(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: StatReq = decode_json_body(body).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let entry = client
        .stat(StatRequest { path: req.path })
        .await?
        .into_inner()
        .entry
        .ok_or_else(|| AppError::upstream("envd_invalid_response", "Stat returned no entry"))?;
    json_ok(&StatRes {
        entry: EntryInfoDto::from_proto(entry),
    })
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::fs_move")]
async fn fs_move(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: MoveReq = decode_json_body(body).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let entry = client
        .r#move(MoveRequest {
            source: req.source,
            destination: req.destination,
        })
        .await?
        .into_inner()
        .entry
        .ok_or_else(|| AppError::upstream("envd_invalid_response", "Move returned no entry"))?;
    json_ok(&MoveRes {
        entry: EntryInfoDto::from_proto(entry),
    })
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::fs_remove")]
async fn fs_remove(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: RemoveReq = decode_json_body(body).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    client.remove(RemoveRequest { path: req.path }).await?;
    json_ok(&EmptyMessage {})
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::fs_list_dir")]
async fn fs_list_dir(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: ListDirReq = decode_json_body(body).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let entries = client
        .list_dir(ListDirRequest {
            path: req.path,
            depth: req.depth,
        })
        .await?
        .into_inner()
        .entries
        .into_iter()
        .map(EntryInfoDto::from_proto)
        .collect();
    json_ok(&ListDirRes { entries })
}

// ---------- Process DTOs ----------

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ProcessConfigDto {
    #[serde(default)]
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    envs: std::collections::HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
}

impl ProcessConfigDto {
    fn into_proto(self) -> ProcessConfig {
        ProcessConfig {
            cmd: self.cmd,
            args: self.args,
            envs: self.envs.into_iter().collect(),
            cwd: self.cwd,
        }
    }

    fn from_proto(c: ProcessConfig) -> Self {
        Self {
            cmd: c.cmd,
            args: c.args,
            envs: c.envs.into_iter().collect(),
            cwd: c.cwd,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct PtySizeDto {
    #[serde(default)]
    cols: u32,
    #[serde(default)]
    rows: u32,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct PtyDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    size: Option<PtySizeDto>,
}

impl PtyDto {
    fn into_proto(self) -> Pty {
        Pty {
            size: self.size.map(|s| PtySize {
                cols: s.cols,
                rows: s.rows,
            }),
        }
    }
}

/// `ProcessSelector` is a proto oneof — only one of `pid` or `tag`
/// should be set. Default JSON encoding emits each field that's
/// present; both `pid` and `tag` are skipped when absent.
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ProcessSelectorDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
}

impl ProcessSelectorDto {
    fn into_proto(self) -> ProcessSelector {
        let selector = match (self.pid, self.tag) {
            (Some(pid), _) => Some(process_selector::Selector::Pid(pid)),
            (None, Some(tag)) => Some(process_selector::Selector::Tag(tag)),
            (None, None) => None,
        };
        ProcessSelector { selector }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartReq {
    #[serde(default)]
    process: ProcessConfigDto,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pty: Option<PtyDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stdin: Option<bool>,
}

impl StartReq {
    fn into_proto(self) -> StartRequest {
        StartRequest {
            process: Some(self.process.into_proto()),
            pty: self.pty.map(PtyDto::into_proto),
            tag: self.tag,
            stdin: self.stdin,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessInfoDto {
    config: ProcessConfigDto,
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ListProcessRes {
    processes: Vec<ProcessInfoDto>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListReqDto {}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendSignalReq {
    #[serde(default)]
    process: ProcessSelectorDto,
    /// Proto3-canonical JSON renders enum as the variant name string;
    /// `Parse` also accepts integer codes.
    #[serde(default)]
    signal: SignalDto,
}

/// Proto enum `Signal` rendered as the wire string form. Same
/// proto3-canonical-JSON rationale as [`FileTypeStr`] for the
/// `SIGNAL_` prefix.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
enum SignalDto {
    #[default]
    #[serde(rename = "SIGNAL_UNSPECIFIED")]
    SignalUnspecified,
    #[serde(rename = "SIGNAL_SIGTERM")]
    SignalSigterm,
    #[serde(rename = "SIGNAL_SIGKILL")]
    SignalSigkill,
}

impl SignalDto {
    fn to_proto(self) -> i32 {
        match self {
            Self::SignalUnspecified => Signal::Unspecified as i32,
            Self::SignalSigterm => Signal::Sigterm as i32,
            Self::SignalSigkill => Signal::Sigkill as i32,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendInputReq {
    #[serde(default)]
    process: ProcessSelectorDto,
    #[serde(default)]
    input: ProcessInputDto,
}

/// `ProcessInput` is a proto oneof — proto3-canonical JSON encodes bytes
/// as base64 strings.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessInputDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stdin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pty: Option<String>,
}

fn decode_b64(s: &str) -> AppResult<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|err| AppError::bad_request("invalid_base64", format!("decoding base64: {err}")))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateReqDto {
    #[serde(default)]
    process: ProcessSelectorDto,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pty: Option<PtyDto>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CloseStdinReqDto {
    #[serde(default)]
    process: ProcessSelectorDto,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectReqDto {
    #[serde(default)]
    process: ProcessSelectorDto,
}

// ---------- Process unary handlers ----------

#[tracing::instrument(skip_all, name = "handler:envd_proxy::proc_list")]
async fn proc_list(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let _req: ListReqDto = decode_json_body(body).await?;
    let mut client = state.envd_pool.process(&addr).await?;
    let processes = client
        .list(ListRequest {})
        .await?
        .into_inner()
        .processes
        .into_iter()
        .map(|p| ProcessInfoDto {
            config: p
                .config
                .map(ProcessConfigDto::from_proto)
                .unwrap_or_default(),
            pid: p.pid,
            tag: p.tag,
        })
        .collect();
    json_ok(&ListProcessRes { processes })
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::proc_send_signal")]
async fn proc_send_signal(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: SendSignalReq = decode_json_body(body).await?;
    // Reject `SIGNAL_UNSPECIFIED` (proto enum zero value) here rather
    // than forwarding `signal: 0` to envd. envd would either no-op or
    // return a generic InvalidArgument — neither tells the SDK caller
    // they sent a malformed request. The proto only models
    // `SIGNAL_SIGTERM` (15) and `SIGNAL_SIGKILL` (9); the SDK chooses
    // one of those explicitly, so an unspecified signal is always a
    // caller bug.
    if matches!(req.signal, SignalDto::SignalUnspecified) {
        return Err(AppError::bad_request(
            "invalid_signal",
            "signal must be SIGNAL_SIGTERM or SIGNAL_SIGKILL",
        )
        .into());
    }
    let mut client = state.envd_pool.process(&addr).await?;
    client
        .send_signal(SendSignalRequest {
            process: Some(req.process.into_proto()),
            signal: req.signal.to_proto(),
        })
        .await?;
    json_ok(&EmptyMessage {})
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::proc_send_input")]
async fn proc_send_input(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: SendInputReq = decode_json_body(body).await?;
    let input = match (req.input.stdin, req.input.pty) {
        (Some(s), _) => Some(process_input::Input::Stdin(decode_b64(&s)?)),
        (None, Some(s)) => Some(process_input::Input::Pty(decode_b64(&s)?)),
        (None, None) => None,
    };
    let mut client = state.envd_pool.process(&addr).await?;
    client
        .send_input(SendInputRequest {
            process: Some(req.process.into_proto()),
            input: Some(ProcessInput { input }),
        })
        .await?;
    json_ok(&EmptyMessage {})
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::proc_update")]
async fn proc_update(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: UpdateReqDto = decode_json_body(body).await?;
    let mut client = state.envd_pool.process(&addr).await?;
    client
        .update(UpdateRequest {
            process: Some(req.process.into_proto()),
            pty: req.pty.map(PtyDto::into_proto),
        })
        .await?;
    json_ok(&EmptyMessage {})
}

#[tracing::instrument(skip_all, name = "handler:envd_proxy::proc_close_stdin")]
async fn proc_close_stdin(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, Access::Token).await?;
    let req: CloseStdinReqDto = decode_json_body(body).await?;
    let mut client = state.envd_pool.process(&addr).await?;
    client
        .close_stdin(CloseStdinRequest {
            process: Some(req.process.into_proto()),
        })
        .await?;
    json_ok(&EmptyMessage {})
}

// ---------- Server-streaming (Connect envelope) ----------

/// Connect-RPC envelope flag bits.
///
/// The Connect protocol wraps each streaming message in a 5-byte
/// header: one flag byte plus a big-endian `u32` payload length. The
/// flag bit 0x02 marks the final envelope; that envelope's body is the
/// stream's terminal status — either `{}` for success or
/// `{"error": {"code": "...", "message": "..."}}` for failure.
const END_STREAM_FLAG: u8 = 0x02;

/// Encode one Connect-RPC streaming envelope.
///
/// Panics in debug builds if `payload.len()` exceeds `u32::MAX`. The
/// Connect envelope length is a 4-byte big-endian unsigned, so a single
/// payload above 4 GiB would silently truncate on the wire. envd's
/// per-event chunk sizes (pipe / pty reads) are bounded well below this
/// in practice, but the assert keeps the failure mode loud if that ever
/// changes.
fn envelope(flags: u8, payload: &[u8]) -> Bytes {
    debug_assert!(
        payload.len() <= u32::MAX as usize,
        "Connect-RPC envelope payload exceeds u32 length: {}",
        payload.len(),
    );
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flags);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Bytes::from(out)
}

/// Decode the SDK's request envelope on a server-streaming call.
///
/// The Python `e2b_connect` client puts a single envelope on the wire
/// with the unary request payload inside, then ends the request body.
/// Returns the decoded inner JSON bytes.
///
/// # Trailing bytes are intentionally accepted
///
/// We return exactly `payload[..len]` and **ignore anything after it**.
/// This is deliberate, not an oversight: Connect-RPC streaming clients
/// may append a trailing end-stream envelope (flag `0x02`) on the
/// request body after the message envelope. We only need the first
/// (message) envelope, so trailing bytes are not an error. The
/// `payload.len() < len` check guards the opposite case — a body
/// *shorter* than the declared length, which is a genuine truncation.
/// Do not "tighten" this to reject trailing bytes; that would break
/// clients that send the end-stream marker.
fn decode_stream_request_envelope(bytes: &[u8]) -> AppResult<&[u8]> {
    if bytes.len() < 5 {
        return Err(AppError::bad_request(
            "invalid_envelope",
            "stream request shorter than envelope header",
        ));
    }
    let flags = bytes[0];
    let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    // We expect a single-message request: no compression, no end-stream.
    if flags & 0x01 != 0 {
        return Err(AppError::bad_request(
            "unsupported_compression",
            "compressed Connect-RPC frames are not supported",
        ));
    }
    let payload = &bytes[5..];
    if payload.len() < len {
        return Err(AppError::bad_request(
            "truncated_envelope",
            "stream request body truncated",
        ));
    }
    // `&payload[..len]` — any bytes past `len` (e.g. an appended
    // end-stream envelope) are intentionally dropped; see the
    // "Trailing bytes" note above.
    Ok(&payload[..len])
}

/// Which path [`proc_start`] takes for a classified sandbox, factored out so the
/// mode-selection branch table is unit-testable independently of the streaming
/// handler (issue #641). The ordering matters: a `Ready` sandbox always proxies
/// the command verbatim — *even a leading `sleep`* — so the `has_leading_sleep`
/// check must sit below the `Ready` / terminal arms, never above them. Hoisting
/// it would silently turn every real `sleep N` against a ready sandbox into a
/// no-op re-signal.
#[derive(Debug, PartialEq, Eq)]
enum ProcStartMode {
    /// Sandbox is serving — proxy the command verbatim.
    Proxy {
        addr: String,
        sandbox_id: String,
        requested_port: Option<String>,
    },
    /// `spec.desiredPhase == Paused` — reject with `sandbox_paused`.
    Paused,
    /// Terminal not-ready — emit the terminal notice now (no wait).
    TerminalNotice(NotReady),
    /// Transient not-ready whose backing Pod cannot plausibly serve within the
    /// silent-park window (unscheduled / `Pending` for capacity) — emit the
    /// **transient** NOTICE (`exit 75`, "retry shortly") *immediately*, with no
    /// silent park, so a short-timeout readiness poller actually receives it
    /// during a cold start (issue #649). Distinct from [`Self::TerminalNotice`]:
    /// the caller is told to keep retrying, not to give up.
    ImmediateNotice(NotReady),
    /// Transient not-ready + a leading `sleep` — synthetic keepalive re-signal.
    SleepPark { sandbox_id: String },
    /// Transient not-ready, no leading `sleep` — bounded silent park.
    SilentPark {
        sandbox_id: String,
        requested_port: Option<String>,
    },
}

/// Decide [`proc_start`]'s mode from the (non-blocking) classification and
/// whether the command begins with a parseable `sleep <x>`. Pure, so the branch
/// table is unit-testable; see [`ProcStartMode`] for the ordering invariant.
fn proc_start_mode(
    readiness: Readiness,
    sandbox_id: String,
    requested_port: Option<String>,
    has_leading_sleep: bool,
) -> ProcStartMode {
    match readiness {
        Readiness::Ready(addr) => ProcStartMode::Proxy {
            addr,
            sandbox_id,
            requested_port,
        },
        Readiness::Paused => ProcStartMode::Paused,
        Readiness::NotReady(detail) if detail.terminal => ProcStartMode::TerminalNotice(detail),
        // A leading `sleep` is the SDK's deliberate keepalive-protected park
        // and re-signal (issue #641/#646); honor it even for a `Pending` Pod —
        // the connection is held alive and the agent is told to resend, which
        // is strictly better than an immediate retry bounce.
        Readiness::NotReady(_) if has_leading_sleep => ProcStartMode::SleepPark { sandbox_id },
        // Transient, plain command: if the backing Pod cannot plausibly serve
        // within the silent window (unscheduled / capacity-`Pending`), emit the
        // transient NOTICE *immediately* so a short-timeout readiness poller
        // receives it during a cold start (issue #649). The unary /
        // `process.Connect` path applies the same gate in
        // `resolve_sandbox_outcome`; keep the two aligned —
        // `NotReady::can_serve_within_silent_window` is the single source of
        // truth for the predicate itself ...
        Readiness::NotReady(detail) if !detail.can_serve_within_silent_window() => {
            ProcStartMode::ImmediateNotice(detail)
        }
        // ... otherwise (mid-restore, or a scheduled-and-booting Pod that
        // p95-readies within seconds) keep the bounded silent park so the call
        // proxies transparently once envd comes up.
        Readiness::NotReady(_) => ProcStartMode::SilentPark {
            sandbox_id,
            requested_port,
        },
    }
}

/// Server-streaming `process.Start` handler — translates the gRPC event
/// stream into Connect envelope JSON frames.
///
/// Branches on both the sandbox readiness and the request command (issue #641):
///
/// - **Ready** → proxy directly to envd.
/// - **Paused** → 403 `sandbox_paused`.
/// - **Transient not-ready + leading `sleep <x>`** → keepalive-protected park
///   up to `min(sleep, sleep_honor_max())`, then a re-signal notice. The
///   connection is never proxied — the caller retries with the real command once
///   the sandbox is available.
/// - **Transient not-ready, no leading `sleep`** → bounded silent park up to
///   `silent_park_max()`, then proxy if it readied, or emit the `NOTICE` notice.
/// - **Terminal not-ready** → immediate `ERROR` notice (will not recover).
#[tracing::instrument(skip_all, name = "handler:envd_proxy::proc_start")]
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
    } = classify_sandbox(&state, &headers, Access::Token).await?;

    let (addr, sandbox_id) = match proc_start_mode(
        readiness,
        sandbox_id,
        requested_port,
        honored_sleep.is_some(),
    ) {
        ProcStartMode::Proxy {
            addr,
            sandbox_id,
            requested_port,
        } => {
            warn_on_port_mismatch(&sandbox_id, &addr, requested_port.as_deref());
            (addr, sandbox_id)
        }
        ProcStartMode::Paused => return Err(ConnectErr::from(sandbox_paused_error())),
        ProcStartMode::TerminalNotice(detail) => {
            return Ok(synthetic_notice_response(&detail));
        }
        ProcStartMode::ImmediateNotice(detail) => {
            // Transient not-ready whose Pod is unscheduled / capacity-`Pending`:
            // emit the `NOTICE` (`exit 75`, retry shortly) now rather than after
            // the silent park, so a sub-15s readiness poller sees it (#649).
            return Ok(synthetic_notice_response(&detail));
        }
        ProcStartMode::SleepPark { sandbox_id } => {
            // Transient not-ready + leading `sleep`: honored as a
            // keepalive-protected park that re-signals the agent to resend;
            // the command is never proxied from this call.
            let sleep =
                honored_sleep.expect("SleepPark mode is only chosen when a sleep was parsed");
            return Ok(sleep_park_response(
                state.clone(),
                sandbox_id,
                sleep,
                sleep_honor_max(),
            ));
        }
        ProcStartMode::SilentPark {
            sandbox_id,
            requested_port,
        } => {
            // Bounded silent park — 15s for all phases now; cold-start "SDK
            // transparency" is handled by the sleep Mode B re-signal, not a
            // long silent park. Then proxy if it readied, else notice.
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
    // `process_streaming` — deadline-free channel so a long-running
    // `commands.run(...)` (or its `.wait()`) is not cut off by the
    // unary 30s `grpc-timeout`. See `EnvdPool::streaming_channel`.
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

/// Server-streaming `process.Connect` handler — re-subscribes to an
/// existing process and re-relays its event stream. Used by background
/// `commands.run` handles when `.wait()` is called.
#[tracing::instrument(skip_all, name = "handler:envd_proxy::proc_connect")]
async fn proc_connect(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ConnectErr> {
    // Same idle-timeout treatment as `proc_start` (issue #481): a re-attach
    // to a long-running background command via `.wait()` keeps the sandbox
    // active for the life of the stream. A not-ready sandbox yields the same
    // synthetic stderr NOTICE stream as `proc_start` (issue #625).
    let (addr, sandbox_id) = match resolve_sandbox_outcome(&state, &headers, Access::Token).await? {
        Resolved::Ready { addr, sandbox_id } => (addr, sandbox_id),
        Resolved::NotReady { detail, .. } => return Ok(synthetic_notice_response(&detail)),
    };
    state.note_activity(&sandbox_id);
    let raw = read_body_bytes(body).await?;
    let inner = decode_stream_request_envelope(&raw)?;
    let req: ConnectReqDto = if inner.is_empty() {
        ConnectReqDto::default()
    } else {
        serde_json::from_slice(inner).map_err(|err| {
            AppError::bad_request("invalid_json", format!("decoding stream body: {err}"))
        })?
    };
    // Deadline-free channel — a re-attach via `.wait()` on a
    // background command can block well past the unary 30s deadline.
    let mut client = state.envd_pool.process_streaming(&addr).await?;
    let stream = client
        .connect(ConnectRequest {
            process: Some(req.process.into_proto()),
        })
        .await?
        .into_inner();
    Ok(build_connect_event_stream_response(
        stream,
        Some(StreamActivity {
            state: state.clone(),
            sandbox_id,
        }),
    ))
}

/// JSON shape of a single envd `ProcessEvent` rendered for Connect.
/// Mirrors `json_format.MessageToJson(StartResponse(event=...))`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessEventDto {
    event: ProcessEventInner,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessEventInner {
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<StartEventDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<DataEventDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<EndEventDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keepalive: Option<EmptyMessage>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StartEventDto {
    pid: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DataEventDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pty: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EndEventDto {
    exit_code: i32,
    exited: bool,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn map_process_event(ev: process_event::Event) -> ProcessEventInner {
    let mut inner = ProcessEventInner {
        start: None,
        data: None,
        end: None,
        keepalive: None,
    };
    match ev {
        process_event::Event::Start(s) => {
            inner.start = Some(StartEventDto { pid: s.pid });
        }
        process_event::Event::Data(d) => {
            let mut data = DataEventDto {
                stdout: None,
                stderr: None,
                pty: None,
            };
            if let Some(out) = d.output {
                match out {
                    process_event::data_event::Output::Stdout(b) => data.stdout = Some(b64(&b)),
                    process_event::data_event::Output::Stderr(b) => data.stderr = Some(b64(&b)),
                    process_event::data_event::Output::Pty(b) => data.pty = Some(b64(&b)),
                }
            }
            inner.data = Some(data);
        }
        process_event::Event::End(e) => {
            inner.end = Some(EndEventDto {
                exit_code: e.exit_code,
                exited: e.exited,
                status: e.status,
                error: e.error,
            });
        }
        process_event::Event::Keepalive(_) => {
            inner.keepalive = Some(EmptyMessage {});
        }
    }
    inner
}

/// What [`forward_event_stream`] needs to refresh a streaming sandbox's
/// idle-timeout activity stamp (issue #481) as frames flow.
///
/// An open exec/pty stream is treated as *active*: every frame forwarded
/// to the client — real output **or** a synthetic keepalive — re-stamps
/// `status.lastActivityTimestamp` (throttled), so a long-running or silent
/// `commands.run(...)` is never paused out from under the client mid-wait.
/// The keepalive cadence (`PROC_KEEPALIVE_INTERVAL`, 15s) guarantees the
/// stamp refreshes during silence; real output refreshes it sooner. `None`
/// in unit tests that exercise only the keepalive timing.
struct StreamActivity {
    state: AppState,
    sandbox_id: String,
}

impl StreamActivity {
    /// Refresh the activity stamp for this stream's sandbox (throttled).
    fn note(&self) {
        self.state.note_activity(&self.sandbox_id);
    }
}

/// Wrap a tonic streaming response in a Connect-RPC envelope body.
///
/// Each `StartResponse` becomes one zero-flag envelope; the stream ends
/// with an `END_STREAM_FLAG` envelope carrying `{}` on success or the
/// canonical `{"error": {...}}` body on failure.
fn build_event_stream_response(
    stream: tonic::Streaming<envd::proto::process::StartResponse>,
    activity: Option<StreamActivity>,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(8);
    tokio::spawn(forward_event_stream(stream, tx, activity));
    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    ([(header::CONTENT_TYPE, "application/connect+json")], body).into_response()
}

/// Variant for `process.Connect` responses (the proto type differs).
fn build_connect_event_stream_response(
    stream: tonic::Streaming<envd::proto::process::ConnectResponse>,
    activity: Option<StreamActivity>,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(8);
    tokio::spawn(forward_event_stream(stream, tx, activity));
    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    ([(header::CONTENT_TYPE, "application/connect+json")], body).into_response()
}

/// Common forwarding loop for both `StartResponse` and `ConnectResponse`
/// — both carry an outer `event` field with the same `ProcessEvent`
/// payload. The trait bound erases the wrapper distinction.
trait HasProcessEvent {
    fn into_event(self) -> Option<process_event::Event>;
}

impl HasProcessEvent for envd::proto::process::StartResponse {
    fn into_event(self) -> Option<process_event::Event> {
        self.event.and_then(|e| e.event)
    }
}

impl HasProcessEvent for envd::proto::process::ConnectResponse {
    fn into_event(self) -> Option<process_event::Event> {
        self.event.and_then(|e| e.event)
    }
}

/// How long the command may produce no output before the proxy injects a
/// synthetic `keepalive` `ProcessEvent`. Tuned below the ~60s idle-timeout
/// enforced by k8s Ingress / cloud LBs: a silent long-running command (a
/// build that prints nothing for minutes, `claude --print` thinking, a bare
/// `sleep`) would otherwise have its HTTP/2 stream reset out from under the
/// SDK (`RST_STREAM`, `INTERNAL_ERROR`). Mirrors the SSE keepalive in
/// `events.rs`; the SDK decodes the frame, finds neither `data` nor `end`
/// set, and silently discards it.
const PROC_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// Pre-encode the Connect envelope for a synthetic keepalive `ProcessEvent`.
///
/// The wire shape (`{"event":{"keepalive":{}}}`) is byte-for-byte what envd
/// emits for its own keepalives, so the SDK handles it on the existing path
/// and ignores it. Built through [`ProcessEventDto`] rather than a string
/// literal so it can never drift from the serialized event shape.
fn keepalive_envelope() -> Bytes {
    let dto = ProcessEventDto {
        event: ProcessEventInner {
            start: None,
            data: None,
            end: None,
            keepalive: Some(EmptyMessage {}),
        },
    };
    // Serializing this fixed, field-complete struct cannot fail; `.expect`
    // documents the invariant instead of forwarding an impossible error.
    let payload = serde_json::to_vec(&dto).expect("keepalive DTO always serializes");
    envelope(0, &payload)
}

/// Common forwarding loop for both `StartResponse` and `ConnectResponse`.
///
/// Generic over the source `Stream` (rather than a concrete
/// `tonic::Streaming<T>`) so the idle-keepalive timing can be exercised in
/// unit tests with a hand-built stream. `tonic::Streaming<T>` satisfies the
/// bound, so callers are unchanged.
async fn forward_event_stream<T, S>(
    mut stream: S,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::convert::Infallible>>,
    activity: Option<StreamActivity>,
) where
    T: HasProcessEvent,
    S: futures::Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    let keepalive = keepalive_envelope();
    let mut idle = tokio::time::interval(PROC_KEEPALIVE_INTERVAL);
    // A delayed forwarding loop should emit one catch-up keepalive, not a
    // backlogged burst.
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval`'s first tick is immediate; consume it so the first real
    // keepalive only fires after a full interval of genuine silence.
    idle.tick().await;

    loop {
        tokio::select! {
            // `StreamExt::next` is cancel-safe: when the keepalive branch
            // wins the select, dropping this future doesn't drop an item.
            item = stream.next() => {
                let Some(item) = item else { break };
                match item {
                    Ok(resp) => {
                        let Some(ev) = resp.into_event() else {
                            continue;
                        };
                        let dto = ProcessEventDto {
                            event: map_process_event(ev),
                        };
                        let payload = match serde_json::to_vec(&dto) {
                            Ok(b) => b,
                            Err(err) => {
                                let _ = tx
                                    .send(Ok(encode_end_error(&format!("serialize event: {err}"))))
                                    .await;
                                return;
                            }
                        };
                        if tx.send(Ok(envelope(0, &payload))).await.is_err() {
                            return;
                        }
                        // Real output is genuine activity — refresh the idle
                        // timeout (issue #481) so a long, chatty command never
                        // pauses mid-run. Throttled inside `note_activity`.
                        if let Some(activity) = &activity {
                            activity.note();
                        }
                        // Real output already keeps the stream warm; restart
                        // the idle clock so keepalives only fill true silence.
                        idle.reset();
                    }
                    Err(status) => {
                        let _ = tx
                            .send(Ok(encode_end_error(&format!("envd: {status}"))))
                            .await;
                        return;
                    }
                }
            }
            _ = idle.tick() => {
                if tx.send(Ok(keepalive.clone())).await.is_err() {
                    return;
                }
                // An open-but-silent stream still counts as active (issue
                // #481): the per-keepalive refresh keeps a `sleep`/blocked
                // `commands.run` from being paused out from under the client
                // during a long wait. Throttled inside `note_activity`.
                if let Some(activity) = &activity {
                    activity.note();
                }
            }
        }
    }
    let _ = tx.send(Ok(envelope(END_STREAM_FLAG, b"{}"))).await;
}

fn encode_end_error(msg: &str) -> Bytes {
    #[derive(Serialize)]
    struct EndError {
        error: ErrorBody,
    }
    #[derive(Serialize)]
    struct ErrorBody {
        code: &'static str,
        message: String,
    }
    let payload = serde_json::to_vec(&EndError {
        error: ErrorBody {
            code: "internal",
            message: msg.to_owned(),
        },
    })
    .unwrap_or_else(|_| b"{}".to_vec());
    envelope(END_STREAM_FLAG, &payload)
}

/// Exit code on the synthetic not-ready stream for a **transient** state
/// ("retry shortly"). `75` is `EX_TEMPFAIL` from `sysexits.h` — "temporary
/// failure; the user is invited to retry" — so a harness that keys off the
/// exit code alone (not just the in-band stderr NOTICE) can tell this apart
/// from a genuine command failure. The stream is fully synthetic — no real
/// process ran — so there is no collision with a real program's exit code.
const NOTICE_EXIT_TRANSIENT: i32 = 75;

/// Exit code on the synthetic not-ready stream for a **terminal** state
/// ("give up"). `69` is `EX_UNAVAILABLE` — "a service is unavailable" — so the
/// exit code distinguishes retry (`75`) from give-up (`69`) from a real
/// command failure (any other code), mirroring the `NOTICE` / `ERROR` stderr
/// lead word.
const NOTICE_EXIT_TERMINAL: i32 = 69;

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

/// Build the canonical synthetic 4-frame sequence (`start{pid:0}` →
/// `data{stderr}` → `end{exit_code}` → end-stream) used by every not-ready
/// notice (issue #625). For the intercepted-`sleep` re-signal, which has
/// already sent its own start, use [`synthetic_notice_tail`] instead (#641).
///
/// # Arguments
///
/// * `stderr_line` - Human-readable message written to the synthetic process's
///   stderr channel. Should end with `\n` to match real envd output.
/// * `exit_code` - Exit code carried in the `End` frame. Use one of the
///   `NOTICE_EXIT_*` constants so the caller can branch on exit code alone.
///
/// # Returns
///
/// An array of four `Bytes` values — each a fully-encoded Connect envelope —
/// ready to be streamed to the client.
fn synthetic_notice_frames(stderr_line: &str, exit_code: i32) -> [Bytes; 4] {
    let start = process_event_frame(ProcessEventInner {
        start: Some(StartEventDto { pid: 0 }),
        data: None,
        end: None,
        keepalive: None,
    });
    let [data, end, end_stream] = synthetic_notice_tail(stderr_line, exit_code);
    [start, data, end, end_stream]
}

/// The tail of a synthetic notice — `data{stderr}` → `end{exit_code}` →
/// end-stream — WITHOUT a leading `start`. Used by [`run_sleep_park`], which
/// has already sent its own `start(pid = 0)` before parking, so re-emitting a
/// start here would put a redundant second start frame on the wire.
///
/// # Arguments
///
/// * `stderr_line` - Human-readable message written to the synthetic process's
///   stderr channel. Should end with `\n` to match real envd output.
/// * `exit_code` - Exit code carried in the `End` frame. Use one of the
///   `NOTICE_EXIT_*` constants so the caller can branch on exit code alone.
///
/// # Returns
///
/// An array of three `Bytes` values — each a fully-encoded Connect envelope —
/// ready to be streamed to the client after a leading `start` frame.
fn synthetic_notice_tail(stderr_line: &str, exit_code: i32) -> [Bytes; 3] {
    [
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

/// Build a synthetic Connect server-streaming response that delivers a
/// not-ready notice on the process **stderr** channel and ends cleanly,
/// instead of proxying to an envd that is not serving yet (issue #625).
///
/// The LLM driving a harness sees a human-readable hint *in-band* — e.g.
/// `NOTICE: sandbox is still restoring its state (Pod is Pending); retry
/// shortly` — followed by a non-zero process exit, rather than a hung stream
/// or an opaque transport error. The frame sequence (Start → Data{stderr} →
/// End → end-stream) is exactly what envd emits for a real short-lived
/// process, so the SDK decodes it on its existing path: `commands.run`
/// returns the stderr and the exit code with no special handling.
///
/// `pid: 0` is a synthetic placeholder — no process was actually started; the
/// id only exists so the SDK's start-event handshake is satisfied. The
/// `NOTICE` / `ERROR` lead word mirrors the transient/terminal split so the
/// model can tell "retry shortly" apart from "this will not resolve".
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

/// Encode one `ProcessEvent` as a zero-flag Connect envelope. Serializing the
/// fixed, field-complete DTO cannot fail; `.expect` documents the invariant.
fn process_event_frame(event: ProcessEventInner) -> Bytes {
    let payload = serde_json::to_vec(&ProcessEventDto { event })
        .expect("synthetic ProcessEvent always serializes");
    envelope(0, &payload)
}

// ---------- sleep-park producer (issue #641) ----------

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
///
/// # Arguments
///
/// * `tx` - Channel sender for Connect-framed bytes.
/// * `readiness` - Future that resolves to a [`WaitOutcome`] when the sandbox's
///   readiness state is known (or the deadline expires).
/// * `keepalive_every` - How often to emit a synthetic keepalive frame during
///   the wait, keeping the HTTP connection alive past ingress idle timeouts.
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
    // A delayed loop should emit one catch-up keepalive, not a backlogged burst.
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval`'s first tick is immediate; consume it so the first real
    // keepalive only fires after a full interval of genuine silence.
    idle.tick().await;

    // Pin the readiness future so we can poll it repeatedly in the select loop
    // without moving it — `tokio::pin!` pins it to the stack.
    tokio::pin!(readiness);
    let outcome = loop {
        tokio::select! {
            // Cancel-safe: when the keepalive branch wins, the pinned readiness
            // future is not dropped, so wait_until_ready's progress is preserved.
            o = &mut readiness => break o,
            _ = idle.tick() => {
                if tx.send(Ok(keepalive.clone())).await.is_err() {
                    return;
                }
            }
        }
    };

    // Terminate with the matching notice tail. We already sent the `start(pid =
    // 0)` frame above, so we use `synthetic_notice_tail` (not
    // `synthetic_notice_frames`) to avoid putting a redundant second start frame
    // on the wire (issue #641).
    let (line, exit_code) = match outcome {
        crate::envd_cache::WaitOutcome::Ready(_) => (
            "NOTICE: sandbox is now available; the leading `sleep` was consumed \
             while waiting — resend your command (without the leading sleep)\n"
                .to_owned(),
            NOTICE_EXIT_READY_RESEND,
        ),
        crate::envd_cache::WaitOutcome::Paused => (
            "ERROR: sandbox is paused; resume it before issuing per-sandbox RPCs\n".to_owned(),
            NOTICE_EXIT_TERMINAL,
        ),
        crate::envd_cache::WaitOutcome::Terminal(detail) => (
            format!("ERROR: {}\n", not_ready_message(&detail)),
            NOTICE_EXIT_TERMINAL,
        ),
        crate::envd_cache::WaitOutcome::TimedOut(detail) => (
            format!("NOTICE: {}\n", not_ready_message(&detail)),
            NOTICE_EXIT_TRANSIENT,
        ),
    };
    for frame in synthetic_notice_tail(&line, exit_code) {
        if tx.send(Ok(frame)).await.is_err() {
            return;
        }
    }
}

/// Build the streaming response for an intercepted leading `sleep` against a
/// transiently not-ready sandbox: park (keepalive-protected) up to
/// `min(sleep, cap)` for readiness, then re-signal. Never proxies
/// the command (issue #641).
///
/// Does not stamp idle-timeout activity: the sandbox is not yet serving, and
/// the agent's resend (a fresh `proc_start`) stamps activity once it lands.
///
/// # Arguments
///
/// * `state` - Application state providing the [`EnvdCache`] waiter.
/// * `sandbox_id` - The sandbox to wait on.
/// * `sleep` - How long the client's leading `sleep` asked to wait.
/// * `cap` - Maximum allowed park window (operator-configured ceiling).
///
/// # Returns
///
/// A streaming Connect-RPC response that emits a keepalive every
/// [`PROC_KEEPALIVE_INTERVAL`] while parked, then a terminal notice frame.
fn sleep_park_response(
    state: AppState,
    sandbox_id: String,
    sleep: std::time::Duration,
    cap: std::time::Duration,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(8);
    let park = park_window(sleep, cap);
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + park;
        let readiness = state.envd_cache.wait_until_ready(&sandbox_id, deadline);
        run_sleep_park(tx, readiness, PROC_KEEPALIVE_INTERVAL).await;
    });
    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    ([(header::CONTENT_TYPE, "application/connect+json")], body).into_response()
}

/// How far under the honored sleep the keepalive park ends, so the "still
/// waiting" re-signal reaches the agent *before* a client whose own
/// `commands.run(timeout=...)` equals `<x>` (issue #641 review). Without the
/// shave the park deadline and the client deadline coincide, and the agent
/// might see an opaque client-side timeout instead of the clean notice.
const PARK_TIMEOUT_MARGIN: std::time::Duration = std::time::Duration::from_secs(1);

/// The keepalive-park window: the honored `sleep` clamped to `cap`, then shaved
/// by [`PARK_TIMEOUT_MARGIN`]. `saturating_sub` keeps a tiny or zero sleep at
/// zero (it collapses to an immediate notice) rather than underflowing.
fn park_window(sleep: std::time::Duration, cap: std::time::Duration) -> std::time::Duration {
    sleep.min(cap).saturating_sub(PARK_TIMEOUT_MARGIN)
}

// ---------- /files HTTP routes (direct content I/O) ----------

#[derive(Debug, Deserialize)]
struct FilesQuery {
    path: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    signature: Option<String>,
    #[serde(default)]
    signature_expiration: Option<String>,
}

impl FilesQuery {
    /// The credential this `/files` request may present: `X-Access-Token`,
    /// or a URL signature over `operation` and these query parameters.
    fn access(&self, operation: FileOperation) -> Access<'_> {
        Access::TokenOrSignature(FileRequest {
            path: &self.path,
            operation,
            username: self.username.as_deref(),
            signature: self.signature.as_deref(),
            signature_expiration: self.signature_expiration.as_deref(),
        })
    }
}

/// `GET /files?path=&username=...` — read a file's bytes.
#[tracing::instrument(skip_all, name = "handler:envd_proxy::files_get")]
async fn files_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<FilesQuery>,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, q.access(FileOperation::Read)).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let resp = client
        .read_all(ReadAllRequest { path: q.path })
        .await?
        .into_inner();
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        resp.data,
    )
        .into_response())
}

/// `POST /files?path=...` — write a file. The SDK posts as either a raw
/// `application/octet-stream` body or a multipart form (httpx `files=`);
/// we sniff content-type to handle both.
///
/// # Buffering and size
///
/// Both the multipart and raw paths buffer the entire upload into a
/// `Vec<u8>` before handing it to envd's `WriteAll` RPC. This matches
/// the existing `routes::files::upload` policy (see that module's
/// "Why the body is buffered (and not streamed)" header): envd's
/// `WriteAll` is a single-message RPC, MVP file sizes fit in memory,
/// and a chunked variant is deferred to Plan 07/09 when benchmarks
/// warrant it.
///
/// Because the body is buffered, the route carries an explicit
/// [`MAX_UPLOAD_BYTES`] ceiling via `RequestBodyLimitLayer` (applied in
/// [`unauthenticated_unary_router`]) so a multi-GiB upload is rejected
/// with `413 Payload Too Large` rather than OOM-ing the gateway pod.
/// The limit covers both the multipart and the raw-octet-stream paths.
#[tracing::instrument(skip_all, name = "handler:envd_proxy::files_post")]
async fn files_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<FilesQuery>,
    request: axum::extract::Request,
) -> Result<Response, ConnectErr> {
    let addr = resolve_sandbox(&state, &headers, q.access(FileOperation::Write)).await?;
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let (target_path, data) = if content_type.starts_with("multipart/") {
        let mut multipart = Multipart::from_request(request, &()).await.map_err(|err| {
            AppError::bad_request("invalid_multipart", format!("parsing multipart: {err}"))
        })?;
        // The SDK uploads exactly one file per request via httpx's
        // `files=` kwarg. We take the first field with content; extras
        // (if any) are silently ignored.
        let field = multipart
            .next_field()
            .await
            .map_err(|err| {
                AppError::bad_request("invalid_multipart", format!("reading field: {err}"))
            })?
            .ok_or_else(|| AppError::bad_request("empty_upload", "no file part in multipart"))?;
        // Buffered read — see the function-level "Buffering and size"
        // note above for why and what the failure mode looks like.
        let bytes = field
            .bytes()
            .await
            .map_err(|err| AppError::Internal(anyhow::anyhow!("reading field bytes: {err}")))?
            .to_vec();
        (q.path, bytes)
    } else {
        let body = request.into_body();
        let bytes = read_body_bytes(body).await?;
        (q.path, bytes)
    };

    let mut client = state.envd_pool.filesystem(&addr).await?;
    client
        .write_all(WriteAllRequest {
            path: target_path.clone(),
            data,
        })
        .await?;

    // Response shape: `[{name, type, path}]` per upload entry. This is
    // what the Python SDK's `Filesystem.write` / `write_files` consume
    // via `WriteInfo(**f)` (see
    // `tests/e2b-sdk-compat/.venv/.../e2b/sandbox/filesystem/filesystem.py`'s
    // `class WriteInfo`, which has three required fields `name`,
    // `type`, and `path` — omitting any of them would raise
    // `TypeError` on the SDK side). The JS SDK reads the same trio.
    //
    // We synthesize the metadata locally rather than round-tripping a
    // `Stat` RPC: we just wrote the file, so we know its path, its
    // type is a regular file (directories go through `MakeDir`), and
    // its name is the path's basename. The richer `EntryInfo`
    // subclass (`size`, `mode`, `permissions`, ...) isn't consumed by
    // any `WriteInfo` call site we can see in the SDK; SDK callers
    // that need that detail do an explicit `files.get_info` (which
    // routes through `filesystem.Filesystem/Stat`).
    let name = target_path
        .rsplit('/')
        .next()
        .unwrap_or(target_path.as_str())
        .to_owned();
    json_ok(&serde_json::json!([{
        "name": name,
        "type": "FILE_TYPE_FILE",
        "path": target_path,
    }]))
}

// ---------- Router wiring ----------

/// Build the Connect-RPC + HTTP proxy router for **unary** routes only.
///
/// Routes are mounted at the canonical Connect-RPC paths
/// (`/<service>/<Method>`) so the stock E2B SDK works without
/// configuration beyond pointing `E2B_SANDBOX_URL` at the gateway.
///
/// # No API-key auth — name carries the warning
///
/// The SDK does not send `X-API-Key` on per-sandbox calls (see the
/// module-level docs), so `auth_middleware` is intentionally **not**
/// applied to these routes; each handler checks the per-sandbox
/// `X-Access-Token` / file signature itself (see [`authorize`]). The
/// `unauthenticated_` prefix (no *API-key* auth) is load-bearing: it
/// stops a future caller — in this crate or another — from merging this
/// surface under a router they *assume* is auth-gated and silently
/// re-exposing it. Treat the prefix as the contract; do not
/// rename it back to a neutral `unary_router` without restoring the
/// auth layer first.
///
/// The streaming routes (`process.Process/Start`,
/// `process.Process/Connect`) live in
/// [`unauthenticated_streaming_router`] so the binary can apply the
/// global request-timeout layer to the unary subset without amputating
/// a long-running `commands.run(...).wait()` stream — see the comment
/// in `crates/sandbawks-api/src/bin/sandbawks-api.rs` where both are
/// merged into the top-level app.
/// `GET /health` — per-sandbox envd liveness probe.
///
/// The E2B SDK's `Sandbox.is_running()` issues `GET /health` against the
/// in-sandbox envd (routed here by the `E2b-Sandbox-Id` header) and maps
/// the HTTP status: `200` → running, `502` → not running, anything else
/// → raise. Upstream E2B's envd serves `/health` as a plain HTTP route;
/// ours is gRPC-only, so the gateway *synthesizes* the probe — it
/// resolves the sandbox, dials its envd, and issues a cheap, read-only
/// `Process.List` RPC.
///
/// Status mapping (all via the standard `AppError` envelope, which the
/// SDK consumes by HTTP status, not body shape — same posture as the
/// `/files` route):
///
/// - reachable, serving envd → `200` (`is_running()` → `True`).
/// - envd unreachable, or the sandbox not yet serving (still starting /
///   restoring, or terminal) → `502` (`is_running()` → `False`).
/// - sandbox missing (`404`) / paused (`403`) / header absent (`400`)
///   propagate from [`resolve_sandbox_outcome`]; the SDK raises on those,
///   which matches "this is not a running sandbox you can probe".
#[tracing::instrument(skip_all, name = "handler:envd_proxy::health")]
async fn health(State(state): State<AppState>, headers: HeaderMap) -> AppResult<StatusCode> {
    // Resolve without `note_activity`: a liveness probe must not refresh the
    // idle-timeout activity stamp (issue #481), or an `is_running` polling
    // loop would keep an idle sandbox alive forever. A not-ready sandbox maps
    // to the not-ready error → 502 → `is_running()` is `False` (issue #625).
    let addr = match resolve_sandbox_outcome(&state, &headers, Access::Token).await? {
        Resolved::Ready { addr, .. } => addr,
        Resolved::NotReady { detail, .. } => return Err(not_ready_error(&detail)),
    };
    let mut client = state.envd_pool.process(&addr).await?;
    // `Process.List` is the cheapest read-only RPC envd exposes; a
    // successful round-trip proves the gRPC server is actually serving,
    // not merely that the TCP/h2 handshake completed. A transport
    // failure surfaces as `tonic::Code::Unavailable` → `502`.
    client.list(ListRequest {}).await.map_err(AppError::from)?;
    Ok(StatusCode::OK)
}

pub fn unauthenticated_unary_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/filesystem.Filesystem/MakeDir", post(fs_make_dir))
        .route("/filesystem.Filesystem/Stat", post(fs_stat))
        .route("/filesystem.Filesystem/Move", post(fs_move))
        .route("/filesystem.Filesystem/Remove", post(fs_remove))
        .route("/filesystem.Filesystem/ListDir", post(fs_list_dir))
        .route("/process.Process/List", post(proc_list))
        .route("/process.Process/SendSignal", post(proc_send_signal))
        .route("/process.Process/SendInput", post(proc_send_input))
        .route("/process.Process/Update", post(proc_update))
        .route("/process.Process/CloseStdin", post(proc_close_stdin))
        .route(
            "/files",
            get(files_get)
                .post(files_post)
                // Bound the upload body for *both* the multipart and
                // raw paths with one explicit ceiling — see
                // `MAX_UPLOAD_BYTES`. `route_layer` scopes the limit to
                // `/files` so the small Connect-RPC unary bodies on the
                // other routes keep axum's default handling.
                .route_layer(tower_http::limit::RequestBodyLimitLayer::new(
                    MAX_UPLOAD_BYTES,
                )),
        )
        .with_state(state)
}

/// Build the Connect-RPC proxy router for **server-streaming** routes
/// only.
///
/// Same unauthenticated posture as
/// [`unauthenticated_unary_router`] — the `unauthenticated_` prefix is
/// the contract, not a label. These RPCs are bounded by the underlying
/// envd process lifetime, not by the per-request `timeout_layer`
/// (`HTTP_REQUEST_TIMEOUT`) the gateway applies to its unary surface. The binary merges this
/// router *outside* the timeout layer so a `commands.run(...)`
/// background that runs for minutes (installing dependencies, running a
/// test suite) does not get its Connect stream amputated with a
/// truncated end-stream envelope.
pub fn unauthenticated_streaming_router(state: AppState) -> Router {
    Router::new()
        .route("/process.Process/Start", post(proc_start))
        .route("/process.Process/Connect", post(proc_connect))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_authority_extracts_port_and_id() {
        // Golden path: `{port}-{sandbox_id}.{domain}`.
        let parsed = parse_sandbox_authority("3000-sbx-abc123.e2b.example.com", "e2b.example.com");
        assert_eq!(parsed, Some(("sbx-abc123".to_owned(), "3000".to_owned())));
    }

    #[test]
    fn parse_authority_strips_host_port() {
        // A `Host` value may carry its own `:port` — it must not leak
        // into the domain-suffix match.
        let parsed = parse_sandbox_authority("49983-sbx-x.e2b.example.com:443", "e2b.example.com");
        assert_eq!(parsed, Some(("sbx-x".to_owned(), "49983".to_owned())));
    }

    #[test]
    fn parse_authority_keeps_dashes_in_id() {
        // Only the leading port segment is split off; the id keeps its
        // own dashes.
        let parsed = parse_sandbox_authority("8080-my-multi-dash-id.dom.test", "dom.test");
        assert_eq!(
            parsed,
            Some(("my-multi-dash-id".to_owned(), "8080".to_owned()))
        );
    }

    #[test]
    fn parse_authority_rejects_wrong_suffix() {
        // Not a subdomain of the configured base → fall through to header.
        assert_eq!(
            parse_sandbox_authority("localhost:8080", "e2b.example.com"),
            None
        );
        assert_eq!(
            parse_sandbox_authority("3000-sbx.other.example.com", "e2b.example.com"),
            None
        );
    }

    #[test]
    fn parse_authority_rejects_bare_apex_and_malformed() {
        // Bare apex (no subdomain label).
        assert_eq!(
            parse_sandbox_authority("e2b.example.com", "e2b.example.com"),
            None
        );
        // Non-numeric leading segment — a host that merely contains a dash.
        assert_eq!(
            parse_sandbox_authority("api-gateway.e2b.example.com", "e2b.example.com"),
            None
        );
        // Empty id segment.
        assert_eq!(
            parse_sandbox_authority("3000-.e2b.example.com", "e2b.example.com"),
            None
        );
        // No dash at all.
        assert_eq!(
            parse_sandbox_authority("sbx.e2b.example.com", "e2b.example.com"),
            None
        );
    }

    #[test]
    fn parse_authority_is_case_insensitive() {
        // Mixed-case Host (RFC 1035 — DNS is case-insensitive) still matches;
        // the extracted id is lowercased to align with the k8s CR name.
        let parsed = parse_sandbox_authority("3000-SBX-Abc.E2B.Example.com", "e2b.example.com");
        assert_eq!(parsed, Some(("sbx-abc".to_owned(), "3000".to_owned())));
        // A mixed-case *configured* domain works too.
        let parsed = parse_sandbox_authority("3000-sbx.e2b.example.com", "E2B.Example.COM");
        assert_eq!(parsed, Some(("sbx".to_owned(), "3000".to_owned())));
    }

    #[test]
    fn parse_authority_strips_trailing_fqdn_dot() {
        // FQDN-form trailing dot is legal and occasionally emitted.
        let parsed = parse_sandbox_authority("3000-sbx.e2b.example.com.", "e2b.example.com");
        assert_eq!(parsed, Some(("sbx".to_owned(), "3000".to_owned())));
    }

    #[test]
    fn parse_authority_rejects_empty_domain() {
        // Degenerate empty domain must not parse (the binary also maps an
        // empty env var to `None`, but the parser is correct in isolation).
        assert_eq!(
            parse_sandbox_authority("3000-sbx.e2b.example.com", ""),
            None
        );
    }

    // `tokio::test`: `AppState::for_test` builds a tower `Buffer` whose worker
    // is `tokio::spawn`ed, so a runtime must be in scope even though
    // `resolve_target` itself is synchronous.
    #[tokio::test]
    async fn resolve_target_host_wins_over_disagreeing_header() {
        // Precedence guard (issue #364): when the Host parses, it wins even
        // if a disagreeing `E2b-Sandbox-Id` header is also present. Tested at
        // the resolver directly because the integration path can't distinguish
        // which id was used (both miss → 404) without seeding a sandbox.
        let state = AppState::for_test(
            "team-acme",
            std::sync::Arc::new(crate::auth::FakeKeyStore::default()),
        )
        .with_sandbox_domain(Some(std::sync::Arc::from("e2b.example.com")));

        let mut headers = HeaderMap::new();
        headers.insert(
            header::HOST,
            axum::http::HeaderValue::from_static("3000-sbx-from-host.e2b.example.com"),
        );
        headers.insert(
            HEADER_SANDBOX_ID,
            axum::http::HeaderValue::from_static("sbx-from-header"),
        );

        let (id, port) = resolve_target(&state, &headers).expect("host parses → resolves");
        assert_eq!(id, "sbx-from-host");
        assert_eq!(port.as_deref(), Some("3000"));
    }

    #[test]
    fn envelope_encodes_5_byte_header() {
        let env = envelope(0, b"hi");
        assert_eq!(env.len(), 7);
        assert_eq!(env[0], 0);
        assert_eq!(&env[1..5], &[0, 0, 0, 2]);
        assert_eq!(&env[5..], b"hi");
    }

    #[test]
    fn envelope_marks_end_stream() {
        let env = envelope(END_STREAM_FLAG, b"{}");
        assert_eq!(env[0], 0x02);
    }

    #[test]
    fn keepalive_envelope_is_ignorable_event() {
        let env = keepalive_envelope();
        // Zero flag byte = a normal message frame, not an end-of-stream one.
        assert_eq!(env[0], 0);
        let len = u32::from_be_bytes(env[1..5].try_into().expect("4-byte length")) as usize;
        assert_eq!(len, env.len() - 5);
        let body: serde_json::Value = serde_json::from_slice(&env[5..]).expect("json body");
        // Byte-for-byte an envd keepalive: neither `data` nor `end` is set,
        // so the SDK's `_handle_events` decodes and silently skips it.
        assert_eq!(body, serde_json::json!({ "event": { "keepalive": {} } }));
    }

    #[tokio::test(start_paused = true)]
    async fn silent_stream_emits_keepalive_within_interval() {
        use envd::proto::process::StartResponse;

        // Source stream that never yields output — a command like `sleep 90`
        // that stays silent far longer than the LB idle timeout.
        let (src_tx, src_rx) =
            tokio::sync::mpsc::channel::<Result<StartResponse, tonic::Status>>(1);
        let src = tokio_stream::wrappers::ReceiverStream::new(src_rx);

        let (out_tx, mut out_rx) =
            tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(8);
        // `None` activity: this test exercises only the keepalive timing,
        // not the idle-timeout stamp (issue #481).
        tokio::spawn(forward_event_stream(src, out_tx, None));

        // The paused clock auto-advances while the forwarding task parks on
        // the idle timer, so a keepalive must surface without any real output.
        let frame = tokio::time::timeout(PROC_KEEPALIVE_INTERVAL * 2, out_rx.recv())
            .await
            .expect("keepalive emitted within two idle intervals")
            .expect("output channel stays open")
            .expect("frame is infallible");
        assert_eq!(frame, keepalive_envelope());

        // Hold the source open until after the assertion; dropping it now
        // lets the loop finish cleanly with the end-stream envelope.
        drop(src_tx);
    }

    #[test]
    fn decode_stream_request_envelope_extracts_payload() {
        let payload = br#"{"process":{"cmd":"echo"}}"#;
        let mut framed = vec![0u8];
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(payload);
        let inner = decode_stream_request_envelope(&framed).expect("decode");
        assert_eq!(inner, &payload[..]);
    }

    #[test]
    fn decode_stream_request_envelope_rejects_compression() {
        let payload = b"";
        let mut framed = vec![0x01u8];
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(payload);
        let err = decode_stream_request_envelope(&framed).expect_err("compressed");
        match err {
            AppError::BadRequest { code, .. } => assert_eq!(code, "unsupported_compression"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn decode_stream_request_envelope_accepts_trailing_end_stream() {
        // Message envelope followed by an appended end-stream envelope
        // (flag 0x02, empty body). The decoder must return only the
        // first message payload and ignore the trailing bytes.
        let payload = br#"{"process":{"cmd":"echo"}}"#;
        let mut framed = vec![0u8];
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(payload);
        // Trailing end-stream envelope: flag 0x02, length 0.
        framed.push(0x02);
        framed.extend_from_slice(&0u32.to_be_bytes());
        let inner = decode_stream_request_envelope(&framed).expect("decode");
        assert_eq!(inner, &payload[..]);
    }

    #[test]
    fn decode_stream_request_envelope_rejects_truncated_body() {
        // Declared length 100 but only 2 payload bytes present.
        let mut framed = vec![0u8];
        framed.extend_from_slice(&100u32.to_be_bytes());
        framed.extend_from_slice(b"{}");
        let err = decode_stream_request_envelope(&framed).expect_err("truncated");
        match err {
            AppError::BadRequest { code, .. } => assert_eq!(code, "truncated_envelope"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn file_type_round_trips_through_dto() {
        let dto = FileTypeStr::from_proto(FileType::Directory as i32);
        assert!(matches!(dto, FileTypeStr::FileTypeDirectory));
        let unknown = FileTypeStr::from_proto(999);
        assert!(matches!(unknown, FileTypeStr::FileTypeUnspecified));
    }

    #[test]
    fn signal_dto_maps_to_proto() {
        assert_eq!(SignalDto::SignalSigterm.to_proto(), Signal::Sigterm as i32);
        assert_eq!(SignalDto::SignalSigkill.to_proto(), Signal::Sigkill as i32);
        assert_eq!(
            SignalDto::SignalUnspecified.to_proto(),
            Signal::Unspecified as i32
        );
    }

    // Regression guard for the `proc_send_signal` gateway-side rejection
    // of `SIGNAL_UNSPECIFIED` (review #50, comment #7386). proto3-
    // canonical JSON omits zero-valued fields, so a `SendSignalRequest`
    // with no `signal` decodes to `SignalUnspecified`; the handler turns
    // that into a 400 rather than forwarding `signal: 0` to envd. If
    // upstream ever changes its default-population behaviour these
    // assertions fail loudly instead of silently shipping a wire-compat
    // break.
    #[test]
    fn send_signal_req_omitted_signal_is_unspecified() {
        let req: SendSignalReq =
            serde_json::from_str(r#"{"process":{"pid":42}}"#).expect("decode without signal");
        assert!(
            matches!(req.signal, SignalDto::SignalUnspecified),
            "an omitted signal field must decode to SignalUnspecified so the \
             handler's 400 guard fires"
        );
    }

    #[test]
    fn send_signal_req_explicit_signal_decodes() {
        let term: SendSignalReq =
            serde_json::from_str(r#"{"process":{"pid":1},"signal":"SIGNAL_SIGTERM"}"#)
                .expect("decode SIGTERM");
        assert!(matches!(term.signal, SignalDto::SignalSigterm));

        let kill: SendSignalReq =
            serde_json::from_str(r#"{"process":{"pid":1},"signal":"SIGNAL_SIGKILL"}"#)
                .expect("decode SIGKILL");
        assert!(matches!(kill.signal, SignalDto::SignalSigkill));
    }

    #[test]
    fn process_selector_prefers_pid_when_both_set() {
        let sel = ProcessSelectorDto {
            pid: Some(42),
            tag: Some("ignored".into()),
        };
        let proto = sel.into_proto();
        match proto.selector {
            Some(process_selector::Selector::Pid(pid)) => assert_eq!(pid, 42),
            other => panic!("expected Pid, got {other:?}"),
        }
    }

    #[test]
    fn process_selector_falls_back_to_tag() {
        let sel = ProcessSelectorDto {
            pid: None,
            tag: Some("foo".into()),
        };
        let proto = sel.into_proto();
        match proto.selector {
            Some(process_selector::Selector::Tag(t)) => assert_eq!(t, "foo"),
            other => panic!("expected Tag, got {other:?}"),
        }
    }

    #[test]
    fn entry_info_dto_renders_modified_time_as_rfc3339() {
        let entry = EntryInfo {
            name: "f".into(),
            r#type: 1,
            path: "/f".into(),
            size: 0,
            mode: 0,
            permissions: String::new(),
            owner: String::new(),
            group: String::new(),
            modified_time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            symlink_target: None,
        };
        let dto = EntryInfoDto::from_proto(entry);
        let s = dto.modified_time.expect("rfc3339 string");
        // 1700000000 = 2023-11-14T22:13:20+00:00
        assert!(
            s.starts_with("2023-11-14T22:13:20"),
            "unexpected rfc3339: {s}"
        );
    }

    // ---------- not-ready notice (issue #625) ----------

    fn pod_summary(phase: &str, reason: Option<&str>, message: Option<&str>) -> PodStatusSummary {
        PodStatusSummary {
            phase: phase.to_owned(),
            reason: reason.map(str::to_owned),
            message: message.map(str::to_owned),
        }
    }

    #[test]
    fn not_ready_message_transient_names_phase_and_pod() {
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Resuming,
            pod: Some(pod_summary("Pending", Some("ContainerCreating"), None)),
            failure_reason: None,
            terminal: false,
        };
        let m = not_ready_message(&detail);
        assert!(m.contains("restoring its state"), "{m}");
        assert!(m.contains("Pod is Pending (ContainerCreating)"), "{m}");
        assert!(m.contains("retry shortly"), "{m}");
    }

    #[test]
    fn not_ready_message_terminal_leads_with_pod_cause() {
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Creating,
            pod: Some(pod_summary(
                "Pending",
                Some("ImagePullBackOff"),
                Some("back-off pulling image"),
            )),
            failure_reason: None,
            terminal: true,
        };
        let m = not_ready_message(&detail);
        assert!(
            m.contains("will not become ready without intervention"),
            "{m}"
        );
        assert!(
            m.contains("ImagePullBackOff: back-off pulling image"),
            "{m}"
        );
    }

    #[test]
    fn not_ready_message_terminal_falls_back_to_failure_reason() {
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Failed,
            pod: None,
            failure_reason: Some("resume_exhausted".to_owned()),
            terminal: true,
        };
        let m = not_ready_message(&detail);
        assert!(m.contains("resume_exhausted"), "{m}");
    }

    #[test]
    fn not_ready_error_maps_distinct_codes() {
        let transient = NotReady {
            sandbox_phase: SandboxPhase::Resuming,
            pod: None,
            failure_reason: None,
            terminal: false,
        };
        // Transient → retryable `sandbox_not_ready` (Connect `unavailable`).
        assert!(matches!(
            not_ready_error(&transient),
            AppError::UpstreamUnavailable {
                code: "sandbox_not_ready",
                ..
            }
        ));
        let terminal = NotReady {
            sandbox_phase: SandboxPhase::Failed,
            pod: None,
            failure_reason: Some("resume_exhausted".to_owned()),
            terminal: true,
        };
        // Terminal → give-up `sandbox_failed` (Connect `failed_precondition`).
        assert!(matches!(
            not_ready_error(&terminal),
            AppError::FailedPrecondition {
                code: "sandbox_failed",
                ..
            }
        ));
    }

    /// Issue #652: a transient (resuming) sandbox's unary error carries the
    /// `EAGAIN` "retry later" vocabulary so the client recognizes it as a
    /// retryable condition rather than a fatal one. Terminal errors must not.
    #[test]
    fn not_ready_error_transient_signals_eagain() {
        let transient = NotReady {
            sandbox_phase: SandboxPhase::Resuming,
            pod: None,
            failure_reason: None,
            terminal: false,
        };
        let AppError::UpstreamUnavailable { message, .. } = not_ready_error(&transient) else {
            panic!("transient not-ready must be UpstreamUnavailable");
        };
        assert!(message.contains("EAGAIN"), "{message}");

        let terminal = NotReady {
            sandbox_phase: SandboxPhase::Failed,
            pod: None,
            failure_reason: Some("resume_exhausted".to_owned()),
            terminal: true,
        };
        let AppError::FailedPrecondition { message, .. } = not_ready_error(&terminal) else {
            panic!("terminal not-ready must be FailedPrecondition");
        };
        assert!(
            !message.contains("EAGAIN"),
            "a terminal sandbox is not retryable: {message}"
        );
    }

    // ---- issue #641: sleep-aware not-ready park ----

    /// Helper: parse a direct `sleep` exec from a list of operand strs.
    fn sleep_exec(args: &[&str]) -> Option<std::time::Duration> {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
        parse_leading_sleep("sleep", &owned)
    }

    /// Helper: parse a `sh -c "<script>"` wrapper.
    fn sleep_shell(script: &str) -> Option<std::time::Duration> {
        parse_leading_sleep("sh", &["-c".to_owned(), script.to_owned()])
    }

    fn secs(n: u64) -> std::time::Duration {
        std::time::Duration::from_secs(n)
    }

    #[test]
    fn parse_leading_sleep_direct_exec() {
        // Bare seconds, the canonical case from the acceptance criteria.
        assert_eq!(sleep_exec(&["5"]), Some(secs(5)));
        assert_eq!(sleep_exec(&["0"]), Some(secs(0)));
        // Decimal seconds.
        assert_eq!(
            sleep_exec(&["1.5"]),
            Some(std::time::Duration::from_millis(1500))
        );
        // GNU sums multiple operands.
        assert_eq!(sleep_exec(&["1m", "30s"]), Some(secs(90)));
    }

    #[test]
    fn parse_leading_sleep_unit_suffixes() {
        // Issue #641 comment: suffixes are honored, parsed to seconds.
        assert_eq!(sleep_exec(&["30s"]), Some(secs(30)));
        assert_eq!(sleep_exec(&["2m"]), Some(secs(120)));
        assert_eq!(sleep_exec(&["1h"]), Some(secs(3600)));
        assert_eq!(sleep_exec(&["1d"]), Some(secs(86_400)));
    }

    #[test]
    fn parse_leading_sleep_rejects_infinity_and_garbage() {
        // `sleep infinity` / `sleep inf` must never be honored.
        assert_eq!(sleep_exec(&["infinity"]), None);
        assert_eq!(sleep_exec(&["inf"]), None);
        assert_eq!(sleep_exec(&["nan"]), None);
        // Negative, empty, non-numeric, and bare-suffix operands.
        assert_eq!(sleep_exec(&["-5"]), None);
        assert_eq!(sleep_exec(&["m"]), None);
        assert_eq!(sleep_exec(&["abc"]), None);
        assert_eq!(sleep_exec(&[]), None);
        // A direct exec is malformed if *any* operand is not a duration.
        assert_eq!(sleep_exec(&["5", "foo"]), None);
    }

    #[test]
    fn parse_leading_sleep_shell_wrapper() {
        // Script that begins with `sleep <x>` followed by more commands.
        assert_eq!(sleep_shell("sleep 5"), Some(secs(5)));
        assert_eq!(sleep_shell("sleep 5 && echo done"), Some(secs(5)));
        assert_eq!(sleep_shell("sleep 1m 30s && ls"), Some(secs(90)));
        assert_eq!(sleep_shell("sleep 2h || true"), Some(secs(7200)));
        // A separator glued to the last operand (no space) stops the sum early:
        // conservative under-count, never an over-count. `30s;` is unparseable,
        // so only the clean leading `1m` is honored.
        assert_eq!(sleep_shell("sleep 1m 30s; ls"), Some(secs(60)));
    }

    #[test]
    fn parse_leading_sleep_shell_wrapper_negatives() {
        // `sleep` not at the head of the line → not honored (out of scope).
        assert_eq!(sleep_shell("echo hi && sleep 5"), None);
        // Infinity inside the shell form is still rejected.
        assert_eq!(sleep_shell("sleep infinity"), None);
        // No duration operand after `sleep`.
        assert_eq!(sleep_shell("sleep && echo"), None);
        // Wrong flag / arg shape.
        assert_eq!(parse_leading_sleep("sh", &["sleep 5".to_owned()]), None);
        assert_eq!(
            parse_leading_sleep("bash", &["-lc".to_owned(), "sleep 5".to_owned()]),
            None
        );
    }

    #[test]
    fn parse_leading_sleep_non_sleep_command() {
        // A real workload is never misread as a wait.
        assert_eq!(parse_leading_sleep("python", &["app.py".to_owned()]), None);
        assert_eq!(parse_leading_sleep("ls", &["-la".to_owned()]), None);
    }

    #[tokio::test]
    async fn synthetic_notice_stream_carries_stderr_notice() {
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Resuming,
            pod: Some(pod_summary("Pending", None, None)),
            failure_reason: None,
            terminal: false,
        };
        let resp = synthetic_notice_response(&detail);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/connect+json")
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("collect synthetic stream body");
        let hay = String::from_utf8_lossy(&body);
        // The base64-encoded stderr line is present in a Data frame.
        let expected = b64(format!("NOTICE: {}\n", not_ready_message(&detail)).as_bytes());
        assert!(
            hay.contains(&expected),
            "stderr notice frame missing; body={hay}"
        );
        // Transient → EX_TEMPFAIL exit code so an exit-code-only harness can
        // tell "retry" apart from a real command failure (proto3 camelCase).
        assert!(
            hay.contains(&format!("\"exitCode\":{NOTICE_EXIT_TRANSIENT}")),
            "transient exit code missing; body={hay}"
        );
        // A successful end-stream envelope (`{}`) closes the stream.
        assert!(
            hay.contains("{}"),
            "missing end-stream envelope; body={hay}"
        );
    }

    #[tokio::test]
    async fn synthetic_notice_stream_terminal_uses_distinct_exit_code() {
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Creating,
            pod: Some(pod_summary("Pending", Some("ImagePullBackOff"), None)),
            failure_reason: None,
            terminal: true,
        };
        let resp = synthetic_notice_response(&detail);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("collect synthetic stream body");
        let hay = String::from_utf8_lossy(&body);
        // Terminal leads with ERROR on stderr and EX_UNAVAILABLE as the exit
        // code — distinct from the transient retry code.
        let expected = b64(format!("ERROR: {}\n", not_ready_message(&detail)).as_bytes());
        assert!(
            hay.contains(&expected),
            "terminal stderr frame missing; body={hay}"
        );
        assert!(
            hay.contains(&format!("\"exitCode\":{NOTICE_EXIT_TERMINAL}")),
            "terminal exit code missing; body={hay}"
        );
    }

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
        assert_eq!(
            parse_env_secs(Some("0"), default),
            std::time::Duration::ZERO
        );
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

    #[test]
    fn synthetic_notice_frames_emit_start_data_end_endstream() {
        // The shared builder produces the canonical 4-frame sequence: a synthetic
        // start (pid 0), one stderr data frame, an end carrying the exit code, and
        // the end-stream envelope.
        let frames = synthetic_notice_frames("NOTICE: still warming up\n", NOTICE_EXIT_TRANSIENT);
        assert_eq!(frames.len(), 4);
        // Each frame's JSON body begins after the 5-byte Connect envelope header.
        let json = |frame: &Bytes| {
            serde_json::from_slice::<serde_json::Value>(&frame[5..])
                .expect("notice frame must be valid JSON after the 5-byte envelope header")
        };
        // Frame 0: synthetic start with pid 0.
        assert_eq!(
            json(&frames[0]),
            serde_json::json!({ "event": { "start": { "pid": 0 } } })
        );
        // Frame 1: data on stderr only (the notice goes to stderr, never stdout).
        let data = json(&frames[1]);
        assert_eq!(data["event"]["data"]["stdout"], serde_json::Value::Null);
        assert!(data["event"]["data"]["stderr"].is_string());
        // Frame 2: end carries the requested exit code and is marked exited.
        let end = json(&frames[2]);
        assert_eq!(end["event"]["end"]["exitCode"], NOTICE_EXIT_TRANSIENT);
        assert_eq!(end["event"]["end"]["exited"], true);
        // Frame 3: the end-stream envelope carries an empty body.
        assert_eq!(json(&frames[3]), serde_json::json!({}));
    }

    #[test]
    fn ready_resend_exit_code_is_distinct() {
        // The "pod is ready, resend" signal must be distinguishable by exit code
        // from the "still waiting" transient notice.
        assert_ne!(NOTICE_EXIT_READY_RESEND, NOTICE_EXIT_TRANSIENT);
        assert_ne!(NOTICE_EXIT_READY_RESEND, NOTICE_EXIT_TERMINAL);
        assert_ne!(NOTICE_EXIT_READY_RESEND, 0);
    }

    // ---------- sleep-park producer (issue #641) ----------

    #[tokio::test(start_paused = true)]
    async fn sleep_park_ready_emits_resend_notice() {
        // A readiness future that resolves Ready immediately → the producer emits
        // exactly one synthetic start, then a ready-resend notice (exit 100).
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let ready = async { crate::envd_cache::WaitOutcome::Ready("10.0.0.1:49983".into()) };
        run_sleep_park(tx, ready, PROC_KEEPALIVE_INTERVAL).await;

        let mut frames = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            frames.push(frame.expect("infallible channel item"));
        }
        let json = |frame: &Bytes| {
            serde_json::from_slice::<serde_json::Value>(&frame[5..])
                .expect("frame must be valid JSON after the 5-byte envelope header")
        };
        // Exactly one start frame (no redundant second start).
        let starts = frames
            .iter()
            .filter(|f| json(f)["event"]["start"].is_object())
            .count();
        assert_eq!(starts, 1, "sleep park must emit exactly one start frame");
        // Some frame carries the ready-resend exit code, and the stderr mentions resend.
        let saw_ready_exit = frames.iter().any(|f| {
            json(f)["event"]["end"]["exitCode"] == serde_json::json!(NOTICE_EXIT_READY_RESEND)
        });
        assert!(
            saw_ready_exit,
            "ready park must end with the resend exit code"
        );
        let resend_stderr = frames.iter().any(|f| {
            json(f)["event"]["data"]["stderr"]
                .as_str()
                .and_then(|b64s| base64::engine::general_purpose::STANDARD.decode(b64s).ok())
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .is_some_and(|s| s.contains("resend"))
        });
        assert!(
            resend_stderr,
            "ready notice stderr must tell the agent to resend"
        );
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
            tokio::time::sleep(PROC_KEEPALIVE_INTERVAL * 2 + std::time::Duration::from_secs(1))
                .await;
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
        assert!(
            saw_transient_exit,
            "timeout must end with the transient notice"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_park_zero_collapses_to_immediate_notice() {
        // A degenerate `sleep 0` parks for ~0s: the readiness future resolves
        // TimedOut on its first poll, so the producer emits the synthetic start
        // and the transient notice with NO keepalive in between. Pins the
        // boundary so a future refactor cannot silently start emitting a
        // keepalive (or holding the call) for a zero-length honored sleep.
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Creating,
            pod: None,
            failure_reason: None,
            terminal: false,
        };
        let timed_out = async move { crate::envd_cache::WaitOutcome::TimedOut(detail) };
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
        assert_eq!(keepalives, 0, "a zero-length park must emit no keepalive");
        assert!(
            saw_transient_exit,
            "a zero-length park still ends with the transient notice"
        );
    }

    #[test]
    fn proc_start_mode_ready_proxies_even_with_leading_sleep() {
        // Highest-blast-radius invariant: a Ready sandbox proxies the command
        // verbatim even when it begins with `sleep` — never intercepted. A
        // regression that hoisted the sleep check above this arm would turn
        // every real `sleep N` against a ready sandbox into a no-op re-signal.
        let mode = proc_start_mode(
            Readiness::Ready("10.0.0.1:49983".into()),
            "sb".into(),
            Some("49983".into()),
            true,
        );
        assert!(matches!(mode, ProcStartMode::Proxy { .. }));
    }

    #[test]
    fn proc_start_mode_branch_table() {
        // A transient detail whose Pod is scheduled-and-booting (`Resuming`
        // phase) → still worth the short silent park.
        let transient = || {
            Readiness::NotReady(NotReady {
                sandbox_phase: SandboxPhase::Resuming,
                pod: None,
                failure_reason: None,
                terminal: false,
            })
        };
        let terminal = || {
            Readiness::NotReady(NotReady {
                sandbox_phase: SandboxPhase::Failed,
                pod: None,
                failure_reason: None,
                terminal: true,
            })
        };
        // Paused regardless of a leading sleep.
        assert_eq!(
            proc_start_mode(Readiness::Paused, "s".into(), None, true),
            ProcStartMode::Paused
        );
        // Terminal not-ready → immediate notice, even with a leading sleep
        // (a terminal sandbox never enters the sleep-park).
        assert!(matches!(
            proc_start_mode(terminal(), "s".into(), None, true),
            ProcStartMode::TerminalNotice(_)
        ));
        // Transient not-ready: a leading sleep parks-and-resignals; a plain
        // command takes the bounded silent park.
        assert!(matches!(
            proc_start_mode(transient(), "s".into(), None, true),
            ProcStartMode::SleepPark { .. }
        ));
        assert!(matches!(
            proc_start_mode(transient(), "s".into(), None, false),
            ProcStartMode::SilentPark { .. }
        ));
    }

    #[test]
    fn proc_start_mode_pending_unscheduled_pod_gets_immediate_notice() {
        // A transient not-ready sandbox whose backing Pod is `Pending` with no
        // progressing reason — not yet scheduled / capacity-`Pending` — must
        // emit the transient NOTICE immediately rather than silent-parking, so
        // a sub-15s readiness poller actually receives it (issue #649).
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Creating,
            pod: Some(PodStatusSummary {
                phase: "Pending".to_owned(),
                reason: None,
                message: None,
            }),
            failure_reason: None,
            terminal: false,
        };
        let mode = proc_start_mode(
            Readiness::NotReady(detail),
            "sb".into(),
            None,
            // No leading sleep — the plain-command path.
            false,
        );
        assert!(
            matches!(mode, ProcStartMode::ImmediateNotice(_)),
            "an unscheduled/Pending Pod must take ImmediateNotice, got {mode:?}"
        );
    }

    #[test]
    fn proc_start_mode_pending_unschedulable_pod_gets_immediate_notice() {
        // A capacity-`Pending` Pod (`PodScheduled=False` → `Unschedulable`)
        // likewise gets the immediate transient notice — it is *not* terminal
        // (a node may free up), so the caller is told to keep retrying.
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Creating,
            pod: Some(PodStatusSummary {
                phase: "Pending".to_owned(),
                reason: Some("Unschedulable".to_owned()),
                message: Some("0/3 nodes are available: ...".to_owned()),
            }),
            failure_reason: None,
            terminal: false,
        };
        let mode = proc_start_mode(Readiness::NotReady(detail), "sb".into(), None, false);
        assert!(
            matches!(mode, ProcStartMode::ImmediateNotice(_)),
            "an Unschedulable Pod must take ImmediateNotice, got {mode:?}"
        );
    }

    #[test]
    fn proc_start_mode_scheduled_booting_pod_keeps_silent_park() {
        // A scheduled-and-booting Pod (`ContainerCreating`) p95-readies within
        // the window — keep the bounded silent park so the command proxies
        // transparently and we don't regress transparent fast-start (#649).
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Creating,
            pod: Some(PodStatusSummary {
                phase: "Pending".to_owned(),
                reason: Some("ContainerCreating".to_owned()),
                message: None,
            }),
            failure_reason: None,
            terminal: false,
        };
        let mode = proc_start_mode(Readiness::NotReady(detail), "sb".into(), None, false);
        assert!(
            matches!(mode, ProcStartMode::SilentPark { .. }),
            "a ContainerCreating Pod must keep the silent park, got {mode:?}"
        );
    }

    #[test]
    fn proc_start_mode_resuming_keeps_silent_park_even_with_pending_pod() {
        // Transparent fast-resume guard: a `Resuming` sandbox readies in a few
        // seconds and must keep the short park so a `commands.run` proxies
        // transparently — even though its restore Pod is briefly `Pending`
        // with no reason (#649 must not regress pause/resume).
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Resuming,
            pod: Some(PodStatusSummary {
                phase: "Pending".to_owned(),
                reason: None,
                message: None,
            }),
            failure_reason: None,
            terminal: false,
        };
        let mode = proc_start_mode(Readiness::NotReady(detail), "sb".into(), None, false);
        assert!(
            matches!(mode, ProcStartMode::SilentPark { .. }),
            "a Resuming sandbox must keep the silent park, got {mode:?}"
        );
    }

    #[test]
    fn proc_start_mode_unscheduled_pod_with_leading_sleep_still_sleep_parks() {
        // A leading `sleep` is the SDK's deliberate keepalive park + re-signal
        // (#641/#646); honor it even for an unscheduled Pod — the immediate
        // notice only governs the *plain-command* path (#649).
        let detail = NotReady {
            sandbox_phase: SandboxPhase::Creating,
            pod: Some(PodStatusSummary {
                phase: "Pending".to_owned(),
                reason: None,
                message: None,
            }),
            failure_reason: None,
            terminal: false,
        };
        let mode = proc_start_mode(Readiness::NotReady(detail), "sb".into(), None, true);
        assert!(
            matches!(mode, ProcStartMode::SleepPark { .. }),
            "a leading sleep still routes to SleepPark, got {mode:?}"
        );
    }

    #[test]
    fn park_window_clamps_then_shaves_margin() {
        // Clamp to the cap, then shave the 1s margin so the re-signal beats a
        // client whose own timeout equals its sleep.
        assert_eq!(park_window(secs(30), secs(300)), secs(29));
        assert_eq!(park_window(secs(600), secs(300)), secs(299));
        // Tiny / zero sleeps saturate to zero rather than underflowing.
        assert_eq!(park_window(secs(0), secs(300)), secs(0));
        assert_eq!(
            park_window(std::time::Duration::from_millis(500), secs(300)),
            std::time::Duration::ZERO
        );
    }
}
