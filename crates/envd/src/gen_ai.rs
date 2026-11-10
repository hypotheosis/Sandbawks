//! OpenTelemetry **GenAI** semantic-convention spans for the agentic
//! commands that reach the sandbox over the E2B-compatible gRPC protocol
//! (issue #426).
//!
//! Every command an external agent issues arrives as a `process.Start`
//! RPC (spawn a child) or a `filesystem.*` RPC (write/read/move/remove/
//! make_dir). The transport spans installed by [`crate::trace`] tell us
//! *that* a sandbox was busy; this module enriches each command with a
//! GenAI `execute_tool` span so an APM (Traceway) shows *what agentic
//! action* ran, correlated to the owning sandbox via the resource
//! attributes the controller already injects (`service.instance.id`).
//!
//! # The span shape
//!
//! Each action opens an `execute_tool` span nested under the existing
//! transport span (it is created while the handler span is current, so
//! it inherits the same `trace_id`). The span carries the GenAI
//! tool-execution attributes:
//!
//! - `gen_ai.operation.name = "execute_tool"`
//! - `gen_ai.tool.name` — the command basename (`bash`, `python`) for a
//!   process spawn, or the operation (`fs.write`, `fs.move`, …) for a
//!   filesystem RPC.
//! - `gen_ai.tool.type = "extension"` — sandbox-side execution, not an
//!   in-model function call.
//! - `gen_ai.tool.call.id` — the child pid for a spawn, or a
//!   process-monotonic id for a filesystem op.
//! - the exit outcome (`process.exit_code` / `error.type`), recorded
//!   when the child exits or the handler returns.
//!
//! The OTel span *name* is set dynamically to `execute_tool {tool_name}`
//! via the `tracing-opentelemetry` `otel.name` field, since a `tracing`
//! span's static name cannot interpolate.
//!
//! # Content capture is gated and redacted
//!
//! Command lines, arguments, and environment can carry secrets/PII, so
//! the argument string (`gen_ai.tool.call.arguments`) is captured **only
//! when** the standard `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT`
//! env var opts in, and even then it is run through [`redact_command`] /
//! [`redact_path`] first. Environment-variable *values* are never
//! recorded under any setting.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use tonic::Status;
use tracing::{Instrument, Span};

/// `gen_ai.operation.name` attribute key.
pub const GEN_AI_OPERATION_NAME: &str = "gen_ai.operation.name";
/// `gen_ai.tool.name` attribute key.
pub const GEN_AI_TOOL_NAME: &str = "gen_ai.tool.name";
/// `gen_ai.tool.type` attribute key.
pub const GEN_AI_TOOL_TYPE: &str = "gen_ai.tool.type";
/// `gen_ai.tool.call.id` attribute key.
pub const GEN_AI_TOOL_CALL_ID: &str = "gen_ai.tool.call.id";
/// `gen_ai.tool.call.arguments` attribute key (gated content).
pub const GEN_AI_TOOL_CALL_ARGUMENTS: &str = "gen_ai.tool.call.arguments";
/// `process.exit_code` attribute key (process spawns only).
pub const PROCESS_EXIT_CODE: &str = "process.exit_code";
/// `error.type` attribute key, recorded on a failed action.
pub const ERROR_TYPE: &str = "error.type";

/// `gen_ai.operation.name` value for a tool execution.
const OP_EXECUTE_TOOL: &str = "execute_tool";
/// `gen_ai.tool.type` value: sandbox-side execution, not a model call.
const TOOL_TYPE_EXTENSION: &str = "extension";
/// Replacement token written in place of a redacted secret.
const REDACTED: &str = "***";

/// Environment variable that opts in to capturing command content on the
/// GenAI spans. Follows the OTel GenAI content-capture convention.
const CAPTURE_CONTENT_ENV: &str = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT";

/// Substrings that mark an argument *key* as carrying a secret value.
/// Matched case-insensitively against the dash-stripped key, so both
/// `--api-key` and `DB_PASSWORD` are caught.
const SECRET_KEY_MARKERS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "api_key",
    "api-key",
    "credential",
    "access_key",
    "access-key",
    "private_key",
    "private-key",
    "session",
    "bearer",
];

/// Flags whose *following* argument is the secret value (e.g.
/// `--token abc123`). Deliberately excludes ambiguous short flags like
/// `-p` (which means `--parents` to `mkdir`), to avoid redacting benign
/// arguments.
const SECRET_FLAGS: &[&str] = &[
    "--password",
    "--passwd",
    "--token",
    "--secret",
    "--api-key",
    "--apikey",
    "--credential",
    "--access-key",
    "--private-key",
];

/// Well-known provider-token prefixes. A token starting with one of
/// these (and of plausible length) is masked even when not preceded by a
/// secret flag — these leak constantly inside shell-form `-c "…"` blobs.
/// Case-sensitive: real tokens carry these prefixes verbatim.
const SECRET_VALUE_PREFIXES: &[&str] = &[
    "ghp_",        // GitHub personal access token
    "gho_",        // GitHub OAuth token
    "ghu_",        // GitHub user-to-server token
    "ghs_",        // GitHub server-to-server token
    "ghr_",        // GitHub refresh token
    "github_pat_", // GitHub fine-grained PAT
    "xoxb-",       // Slack bot token
    "xoxp-",       // Slack user token
    "xoxa-",       // Slack app token
    "xoxr-",       // Slack refresh token
    "xoxs-",       // Slack workspace token
    "AKIA",        // AWS long-term access key id
    "ASIA",        // AWS temporary access key id
    "sk-",         // OpenAI-style secret key
    "AIza",        // Google API key
];

/// Process-monotonic source for [`next_call_id`]. Processes key their
/// span on the child pid instead; this covers the filesystem RPCs, which
/// have no natural numeric id.
static CALL_ID: AtomicU64 = AtomicU64::new(1);

/// Mint a fresh `gen_ai.tool.call.id` for a filesystem operation.
///
/// Monotonic and process-wide; the value is opaque and only needs to be
/// unique enough to disambiguate concurrent ops within one trace.
pub fn next_call_id() -> u64 {
    CALL_ID.fetch_add(1, Ordering::Relaxed)
}

/// Whether command content may be recorded on the GenAI spans.
///
/// Reads [`CAPTURE_CONTENT_ENV`] once and caches the result, so the
/// per-RPC hot path is a single atomic load. Defaults to `false`: with
/// the var unset, no argument strings are ever recorded.
///
/// Because the value is cached for the process lifetime, toggling the env
/// var at runtime (e.g. `kubectl set env`) has **no effect until envd
/// restarts** — the cache is read on the first call, which in the spawn
/// path is the first `Start` RPC, not boot.
pub fn capture_content() -> bool {
    static CAPTURE: OnceLock<bool> = OnceLock::new();
    *CAPTURE.get_or_init(|| parse_capture(std::env::var(CAPTURE_CONTENT_ENV).ok().as_deref()))
}

/// Pure parser behind [`capture_content`], split out so the env-var
/// semantics are unit-testable without mutating the process environment.
///
/// Truthy values are `true` / `1` (case-insensitive, trimmed); anything
/// else — including `None` — is `false`.
fn parse_capture(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("true") | Some("1")
    )
}

/// Derive `gen_ai.tool.name` from a command path: the final non-empty
/// path component (`/usr/bin/python3` -> `python3`, `bash` -> `bash`).
pub fn command_tool_name(cmd: &str) -> &str {
    cmd.rsplit('/').find(|seg| !seg.is_empty()).unwrap_or(cmd)
}

/// Build an `execute_tool` span for one agentic action.
///
/// The span is created under the current context, so it nests beneath
/// the in-flight gRPC handler span and shares its `trace_id`. The
/// content field (`gen_ai.tool.call.arguments`) and the exit-outcome
/// fields are declared empty here and filled in later by the caller
/// (gated for content, at exit for the outcome).
///
/// # Arguments
///
/// * `tool_name` - basename of the command, or the `fs.*` operation.
/// * `call_id` - the child pid (process spawn) or [`next_call_id`] (fs).
pub fn tool_span(tool_name: &str, call_id: u64) -> Span {
    // `tracing` span names must be `'static`, so the GenAI-convention
    // dynamic name (`execute_tool {tool}`) is supplied via the
    // `tracing-opentelemetry` `otel.name` field instead. Field names that
    // contain a Rust keyword (`type`) must be quoted.
    let span = tracing::info_span!(
        "execute_tool",
        otel.name = tracing::field::Empty,
        otel.kind = "internal",
        "gen_ai.operation.name" = OP_EXECUTE_TOOL,
        "gen_ai.tool.name" = tool_name,
        "gen_ai.tool.type" = TOOL_TYPE_EXTENSION,
        "gen_ai.tool.call.id" = call_id,
        "gen_ai.tool.call.arguments" = tracing::field::Empty,
        "process.exit_code" = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
    );
    span.record(
        "otel.name",
        format!("{OP_EXECUTE_TOOL} {tool_name}").as_str(),
    );
    span
}

/// Record a process's exit outcome on its `execute_tool` span.
///
/// `process.exit_code` is always set; `error.type` is set only on a
/// non-clean exit so a successful command carries no error attribute.
/// The three failure modes are kept distinct as low-cardinality values:
/// `"wait_failed"` (the `wait` syscall itself errored, no status),
/// `"signaled"` (the child was killed by a signal — no exit code), and
/// `"non_zero_exit"` (the child exited normally with a non-zero code).
/// Conflating the signal case into `"non_zero_exit"` would hide a
/// SIGKILL/SIGTERM behind the same label as an ordinary failing command.
///
/// # Arguments
///
/// * `exit_code` - the child's exit code (`-1` for signal/wait failures).
/// * `exited` - whether the child exited normally (`false` ⇒ signaled).
/// * `errored` - whether the `wait` itself failed (no status available).
pub fn record_process_exit(span: &Span, exit_code: i32, exited: bool, errored: bool) {
    span.record(PROCESS_EXIT_CODE, exit_code as i64);
    let error_type = if errored {
        Some("wait_failed")
    } else if !exited {
        // No exit code ⇒ terminated by a signal, not a non-zero return.
        Some("signaled")
    } else if exit_code != 0 {
        Some("non_zero_exit")
    } else {
        None
    };
    if let Some(kind) = error_type {
        span.record(ERROR_TYPE, kind);
    }
}

/// Run a filesystem operation inside an `execute_tool` span.
///
/// Creates the span, records the (already-redacted, already-gated)
/// `arguments` when present, drives `op` to completion within the span,
/// and maps a returned [`Status`] to a low-cardinality `error.type`.
///
/// # Arguments
///
/// * `tool_name` - the `fs.*` operation name.
/// * `call_id` - id from [`next_call_id`].
/// * `arguments` - redacted target, or `None` when content capture is off.
/// * `op` - the handler body, yielding the gRPC result.
// `tonic::Status` is ~176 B, so any `Result<_, Status>` trips
// `result_large_err`; the gRPC handler trait forces this error type, the
// same allow the filesystem/process handlers already carry.
#[allow(clippy::result_large_err)]
pub async fn instrument_tool<F, T>(
    tool_name: &'static str,
    call_id: u64,
    arguments: Option<&str>,
    op: F,
) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<T, Status>>,
{
    let span = tool_span(tool_name, call_id);
    if let Some(args) = arguments {
        span.record(GEN_AI_TOOL_CALL_ARGUMENTS, args);
    }
    // Clone so the span survives the instrumented future and can carry
    // `error.type` recorded after it resolves; both clones share one
    // span id, and the OTel span is exported when the last drops. The
    // `tracing-opentelemetry` bridge reads field updates by span id, not
    // from a snapshot taken at creation, so this post-`await` `record` is
    // not a no-op — do not "simplify" it away.
    let result = op.instrument(span.clone()).await;
    if let Err(status) = &result {
        span.record(ERROR_TYPE, format!("{:?}", status.code()).as_str());
    }
    result
}

/// Redact a single command-line argument.
///
/// First the argument is checked as one token (a `key=value` secret pair
/// or a standalone secret value). If that doesn't match and the argument
/// is a shell-form compound (`bash -c "...curl -H 'Authorization: Bearer
/// ghp_…'..."`), its whitespace-separated sub-tokens are scanned so a
/// secret embedded *inside* the string is still masked — the common case
/// the per-flag lookahead in [`redact_command`] cannot reach. A compound
/// with no secret is returned unchanged (spacing preserved).
fn redact_arg(arg: &str) -> Cow<'_, str> {
    if let Some(redacted) = redact_token(arg) {
        return Cow::Owned(redacted);
    }
    // `nth(1).is_some()` ⇒ at least two whitespace-separated tokens, i.e.
    // a compound argument worth scanning for embedded secrets.
    if arg.split_whitespace().nth(1).is_some() {
        let mut changed = false;
        let rebuilt = arg
            .split_whitespace()
            .map(|token| match redact_token(token) {
                Some(redacted) => {
                    changed = true;
                    redacted
                }
                None => token.to_owned(),
            })
            .collect::<Vec<_>>()
            .join(" ");
        if changed {
            return Cow::Owned(rebuilt);
        }
    }
    Cow::Borrowed(arg)
}

/// Redact a single token if it looks secret, else `None`.
///
/// Masks a `key=value` pair whose key names a secret (keeping the key)
/// and a standalone value that looks like a secret (JWT, known token
/// prefix, or a high-entropy blob). The unit of redaction shared by the
/// whole-arg and embedded-token paths of [`redact_arg`].
fn redact_token(token: &str) -> Option<String> {
    if let Some((key, value)) = token.split_once('=') {
        if !value.is_empty() && key_is_secret(key) {
            return Some(format!("{key}={REDACTED}"));
        }
    }
    if looks_like_secret_value(token) {
        return Some(REDACTED.to_owned());
    }
    None
}

/// Whether a `key=value` key (with any leading dashes) names a secret.
fn key_is_secret(key: &str) -> bool {
    let key = key.trim_start_matches('-').to_ascii_lowercase();
    SECRET_KEY_MARKERS.iter().any(|marker| key.contains(marker))
}

/// Whether a standalone token looks like a secret value.
///
/// Three shapes are caught: a provider token with a well-known prefix
/// (GitHub `ghp_…`, AWS `AKIA…`, Slack `xox?-…`, …), a JWT, and a generic
/// high-entropy blob. Trailing punctuation (quotes, commas) does not
/// defeat the prefix check, so secrets embedded in shell strings are
/// still caught.
///
/// # Known gap
///
/// This is a denylist of *shapes*, not a guarantee. A bespoke or
/// unprefixed secret that is short, contains separators, or is otherwise
/// low-entropy can still pass through when content capture is on. The
/// gate (`OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT`) defaults
/// off for exactly this reason — widen the heuristics before recommending
/// it be enabled in production.
fn looks_like_secret_value(value: &str) -> bool {
    has_known_token_prefix(value) || is_jwt_shaped(value) || is_high_entropy_blob(value)
}

/// A well-known provider-token prefix on a token of plausible length.
fn has_known_token_prefix(value: &str) -> bool {
    SECRET_VALUE_PREFIXES
        .iter()
        .any(|prefix| value.len() >= prefix.len() + 8 && value.starts_with(prefix))
}

/// The JWT shape: three non-empty base64url segments separated by dots.
fn is_jwt_shaped(value: &str) -> bool {
    if value.len() < 30 {
        return false;
    }
    let segments: Vec<&str> = value.split('.').collect();
    segments.len() == 3
        && segments
            .iter()
            .all(|seg| seg.len() >= 3 && is_base64urlish(seg))
}

/// A long, mixed-case/digit base64url-or-hex blob with no separators —
/// the shape of an opaque API key or hash. Requires both a letter and a
/// digit so a long ordinary word is not masked, and rejects any token
/// containing path/URL separators (`/`, `.`, `:`).
fn is_high_entropy_blob(value: &str) -> bool {
    value.len() >= 40
        && value.bytes().any(|b| b.is_ascii_digit())
        && value.bytes().any(|b| b.is_ascii_alphabetic())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'=')
}

/// Whether every byte is in the base64url alphabet (`[A-Za-z0-9_-]`).
fn is_base64urlish(s: &str) -> bool {
    s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Redact a full command line (`cmd` + `args`) into a display string.
///
/// Per-argument redaction via [`redact_arg`], plus a lookahead that masks
/// the argument *after* a known secret flag (e.g. `--token <secret>`).
/// The leading command path itself is kept verbatim — it is never a
/// secret and identifies the tool.
pub fn redact_command(cmd: &str, args: &[String]) -> String {
    let mut parts: Vec<Cow<'_, str>> = Vec::with_capacity(args.len() + 1);
    parts.push(Cow::Borrowed(cmd));

    let mut redact_next = false;
    for arg in args {
        if redact_next {
            parts.push(Cow::Borrowed(REDACTED));
            redact_next = false;
        } else if SECRET_FLAGS.contains(&arg.as_str()) {
            parts.push(Cow::Borrowed(arg));
            redact_next = true;
        } else {
            parts.push(redact_arg(arg));
        }
    }
    parts.join(" ")
}

/// Redact a filesystem path argument.
///
/// Paths rarely embed secrets, so this only masks a path that itself
/// looks like a bare secret token; the common case returns borrowed and
/// allocation-free.
pub fn redact_path(path: &str) -> Cow<'_, str> {
    if looks_like_secret_value(path) {
        Cow::Borrowed(REDACTED)
    } else {
        Cow::Borrowed(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    #[test]
    fn parse_capture_honors_truthy_values_and_defaults_false() {
        assert!(!parse_capture(None), "unset defaults to off");
        assert!(!parse_capture(Some("")), "empty is off");
        assert!(!parse_capture(Some("false")), "false is off");
        assert!(!parse_capture(Some("0")), "0 is off");
        assert!(parse_capture(Some("true")));
        assert!(parse_capture(Some("TRUE")), "case-insensitive");
        assert!(parse_capture(Some(" 1 ")), "trimmed");
    }

    #[test]
    fn command_tool_name_picks_the_basename() {
        assert_eq!(command_tool_name("/usr/bin/python3"), "python3");
        assert_eq!(command_tool_name("bash"), "bash");
        assert_eq!(command_tool_name("/bin/"), "bin", "trailing slash ignored");
    }

    #[test]
    fn redact_command_leaves_benign_args_intact() {
        assert_eq!(
            redact_command(
                "python",
                &["train.py".into(), "--epochs".into(), "3".into()]
            ),
            "python train.py --epochs 3"
        );
    }

    #[test]
    fn redact_command_masks_secret_key_value_pairs() {
        assert_eq!(
            redact_command("psql", &["--password=hunter2".into()]),
            "psql --password=***"
        );
        assert_eq!(
            redact_command("env", &["API_KEY=abcdef123456".into()]),
            "env API_KEY=***",
            "env-style KEY=VALUE with a secret key is masked"
        );
    }

    #[test]
    fn redact_command_masks_value_after_secret_flag() {
        assert_eq!(
            redact_command("curl", &["--token".into(), "s3cr3t-value".into()]),
            "curl --token ***"
        );
        // An ambiguous short flag is NOT treated as secret: `mkdir -p dir`
        // must keep its directory argument.
        assert_eq!(
            redact_command("mkdir", &["-p".into(), "build".into()]),
            "mkdir -p build"
        );
    }

    #[test]
    fn redact_command_masks_standalone_jwt() {
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N";
        assert_eq!(
            redact_command("app", &[jwt.into()]),
            "app ***",
            "a JWT-shaped argument is masked whole"
        );
    }

    #[test]
    fn redact_command_masks_known_token_prefixes() {
        // GitHub PAT and AWS access-key id, neither preceded by a flag.
        assert_eq!(
            redact_command("gh", &["auth".into(), "ghp_0123456789abcdefABCDEF".into()]),
            "gh auth ***"
        );
        assert_eq!(
            redact_command("aws", &["--profile".into(), "AKIAIOSFODNN7EXAMPLE".into()]),
            "aws --profile ***"
        );
    }

    #[test]
    fn redact_command_masks_high_entropy_blob() {
        // 40+ char mixed letter/digit base64url blob with no separators.
        let blob = "AbC123dEf456GhI789jKl012MnO345pQr678StUv90";
        assert_eq!(redact_command("deploy", &[blob.into()]), "deploy ***");
    }

    #[test]
    fn redact_command_masks_secret_embedded_in_shell_string() {
        // The secret is a *substring* of a single `-c` argument, so the
        // per-flag lookahead can't reach it — the compound scan must.
        let script = "curl -H 'Authorization: Bearer ghp_0123456789abcdefABCDEF'";
        let out = redact_command("bash", &["-c".into(), script.into()]);
        assert!(out.contains("***"), "embedded PAT must be masked: {out}");
        assert!(
            !out.contains("ghp_0123456789abcdefABCDEF"),
            "the raw token must not survive: {out}"
        );
    }

    #[test]
    fn redact_command_keeps_benign_compound_string_intact() {
        // A compound `-c` arg with no secret is returned unchanged.
        assert_eq!(
            redact_command("sh", &["-c".into(), "echo hello world".into()]),
            "sh -c echo hello world"
        );
    }

    #[test]
    fn redact_path_passes_ordinary_paths_through() {
        assert_eq!(redact_path("/home/user/data.csv"), "/home/user/data.csv");
    }

    /// One span captured by [`CaptureSubscriber`]: its `tracing` name and
    /// the field values recorded on it (at creation and via `record`).
    #[derive(Default)]
    struct Captured {
        name: String,
        fields: HashMap<String, String>,
    }

    /// Shared store of captured spans, cloned into the subscriber.
    #[derive(Clone, Default)]
    struct CaptureState(Arc<Mutex<Vec<Captured>>>);

    /// Minimal `tracing::Subscriber` that records every new span's name
    /// and fields, so a test can assert the GenAI attributes `tool_span`
    /// sets — without standing up an OTLP exporter. Implemented directly
    /// (rather than via `tracing-subscriber`) to avoid the `registry`
    /// feature, which the workspace does not enable.
    struct CaptureSubscriber(CaptureState);

    /// Visitor that stringifies each field value into the span's map.
    struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

    impl Visit for FieldVisitor<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl Subscriber for CaptureSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attrs: &Attributes<'_>) -> Id {
            let mut fields = HashMap::new();
            attrs.record(&mut FieldVisitor(&mut fields));
            let mut spans = self.0 .0.lock().expect("capture store poisoned");
            spans.push(Captured {
                name: attrs.metadata().name().to_string(),
                fields,
            });
            // Ids are 1-based (0 is invalid); index = id - 1.
            Id::from_u64(spans.len() as u64)
        }

        fn record(&self, id: &Id, values: &Record<'_>) {
            let idx = (id.into_u64() - 1) as usize;
            let mut spans = self.0 .0.lock().expect("capture store poisoned");
            if let Some(captured) = spans.get_mut(idx) {
                values.record(&mut FieldVisitor(&mut captured.fields));
            }
        }

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
        fn event(&self, _event: &Event<'_>) {}
        fn enter(&self, _span: &Id) {}
        fn exit(&self, _span: &Id) {}
    }

    #[test]
    fn tool_span_sets_genai_attributes() {
        let state = CaptureState::default();
        tracing::subscriber::with_default(CaptureSubscriber(state.clone()), || {
            let span = tool_span("python", 4242);
            span.record(GEN_AI_TOOL_CALL_ARGUMENTS, "python train.py");
        });

        let spans = state.0.lock().expect("capture store poisoned");
        let captured = spans
            .iter()
            .find(|c| c.name == "execute_tool")
            .expect("an execute_tool span was created");

        let field = |key: &str| captured.fields.get(key).map(String::as_str);
        assert_eq!(field(GEN_AI_OPERATION_NAME), Some("execute_tool"));
        assert_eq!(field(GEN_AI_TOOL_NAME), Some("python"));
        assert_eq!(field(GEN_AI_TOOL_TYPE), Some("extension"));
        assert_eq!(field(GEN_AI_TOOL_CALL_ID), Some("4242"));
        assert_eq!(
            field("otel.name"),
            Some("execute_tool python"),
            "the dynamic OTel span name is set via otel.name"
        );
        assert_eq!(
            field(GEN_AI_TOOL_CALL_ARGUMENTS),
            Some("python train.py"),
            "recorded content lands on the declared field"
        );
    }

    #[test]
    fn record_process_exit_sets_error_type_only_on_failure() {
        let state = CaptureState::default();
        tracing::subscriber::with_default(CaptureSubscriber(state.clone()), || {
            let ok = tool_span("true", 1);
            record_process_exit(&ok, 0, true, false);
            let bad = tool_span("false", 2);
            record_process_exit(&bad, 1, true, false);
            // Signal-terminated: no exit code, `exited = false`.
            let killed = tool_span("sleep", 3);
            record_process_exit(&killed, -1, false, false);
            // `wait` syscall itself failed.
            let waiterr = tool_span("ghost", 4);
            record_process_exit(&waiterr, -1, false, true);
        });

        let spans = state.0.lock().expect("capture store poisoned");
        let by_tool = |name: &str| {
            spans
                .iter()
                .find(|c| c.fields.get(GEN_AI_TOOL_NAME).map(String::as_str) == Some(name))
                .unwrap_or_else(|| panic!("span for {name}"))
        };

        let ok = by_tool("true");
        assert_eq!(
            ok.fields.get(PROCESS_EXIT_CODE).map(String::as_str),
            Some("0")
        );
        assert!(
            !ok.fields.contains_key(ERROR_TYPE),
            "a clean exit records no error.type"
        );

        let bad = by_tool("false");
        assert_eq!(
            bad.fields.get(ERROR_TYPE).map(String::as_str),
            Some("non_zero_exit")
        );

        // A signal kill is distinguished from a non-zero exit...
        let killed = by_tool("sleep");
        assert_eq!(
            killed.fields.get(ERROR_TYPE).map(String::as_str),
            Some("signaled")
        );
        // ...and a failed `wait` from both.
        let waiterr = by_tool("ghost");
        assert_eq!(
            waiterr.fields.get(ERROR_TYPE).map(String::as_str),
            Some("wait_failed")
        );
    }
}
