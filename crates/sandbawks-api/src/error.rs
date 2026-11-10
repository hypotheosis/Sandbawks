//! `AppError` — the single error type every handler returns.
//!
//! Renders to the E2B-compatible JSON envelope:
//!
//! ```json
//! { "code": "sandbox_not_found", "message": "...", "request_id": "..." }
//! ```
//!
//! `request_id` is generated locally in `IntoResponse` and mirrored to
//! the `X-Request-Id` response header so the value in the JSON body
//! always matches the header. Internal-error variants log the
//! underlying cause at `error!`/`warn!` before rendering so we never
//! lose a stack trace at the network boundary.

use axum::{
    extract::rejection::JsonRejection,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

/// Result alias used by every handler.
pub type AppResult<T> = Result<T, AppError>;

/// Errors a handler can raise.
///
/// The variant carries enough structured information for `IntoResponse`
/// to pick a stable E2B `code` + HTTP status pair. Internal failures
/// surface as 500 with code `internal_error` and a generic body — the
/// real cause is `tracing`-logged at `error!` so it's recoverable from
/// logs without leaking to the wire.
#[derive(Debug, Error)]
pub enum AppError {
    /// A resource we expected to exist was missing.
    #[error("{code}: {message}")]
    NotFound { code: &'static str, message: String },

    /// The request was malformed (missing field, bad enum value, body
    /// failed to deserialize).
    #[error("{code}: {message}")]
    BadRequest { code: &'static str, message: String },

    /// Authentication failed (missing/invalid `X-API-Key`).
    #[error("{code}: {message}")]
    Unauthorized { code: &'static str, message: String },

    /// Authentication succeeded but the caller's scopes don't permit
    /// the operation.
    #[error("{code}: {message}")]
    Forbidden { code: &'static str, message: String },

    /// A backend operation failed transiently (envd unreachable, kube
    /// transport error). Returned as 502; SDKs are expected to retry.
    #[error("{code}: {message}")]
    UpstreamUnavailable { code: &'static str, message: String },

    /// The target resource is in a state that prevents the operation and
    /// will not recover without intervention — a per-sandbox tool call
    /// against a sandbox whose Pod is stuck (`ImagePullBackOff`,
    /// `CrashLoopBackOff`, ...) or has folded to `Failed` (issue #625).
    /// Distinct from [`Self::UpstreamUnavailable`] (a transient "retry
    /// shortly") so a caller — or the LLM driving a harness — can tell
    /// "give up / fix the sandbox" apart from "wait and retry". Renders as
    /// 412 with the Connect `failed_precondition` code.
    #[error("{code}: {message}")]
    FailedPrecondition { code: &'static str, message: String },

    /// Everything else — bug, unexpected I/O, JSON encoding failure.
    /// Renders as 500 with code `internal_error` and a generic body;
    /// the underlying error is logged at `error!`.
    #[error("internal: {0}")]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    /// Build a `NotFound` with a stable E2B error code.
    pub fn not_found(code: &'static str, msg: impl Into<String>) -> Self {
        Self::NotFound {
            code,
            message: msg.into(),
        }
    }

    /// Build a `BadRequest` with a stable E2B error code.
    pub fn bad_request(code: &'static str, msg: impl Into<String>) -> Self {
        Self::BadRequest {
            code,
            message: msg.into(),
        }
    }

    /// Build an `Unauthorized` with a stable E2B error code.
    pub fn unauthorized(code: &'static str, msg: impl Into<String>) -> Self {
        Self::Unauthorized {
            code,
            message: msg.into(),
        }
    }

    /// Build a `Forbidden` with a stable E2B error code.
    pub fn forbidden(code: &'static str, msg: impl Into<String>) -> Self {
        Self::Forbidden {
            code,
            message: msg.into(),
        }
    }

    /// Build an `UpstreamUnavailable` with a stable E2B error code.
    pub fn upstream(code: &'static str, msg: impl Into<String>) -> Self {
        Self::UpstreamUnavailable {
            code,
            message: msg.into(),
        }
    }

    /// Build a `FailedPrecondition` with a stable E2B error code.
    pub fn failed_precondition(code: &'static str, msg: impl Into<String>) -> Self {
        Self::FailedPrecondition {
            code,
            message: msg.into(),
        }
    }
}

impl From<kube::Error> for AppError {
    fn from(err: kube::Error) -> Self {
        match err {
            kube::Error::Api(ref api) if api.code == 404 => {
                AppError::not_found("not_found", api.message.clone())
            }
            other => {
                tracing::warn!(error = %other, "kube error → upstream_unavailable");
                AppError::upstream("kube_unavailable", other.to_string())
            }
        }
    }
}

impl From<JsonRejection> for AppError {
    fn from(rej: JsonRejection) -> Self {
        AppError::bad_request("invalid_json", rej.body_text())
    }
}

impl From<tonic::Status> for AppError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => {
                AppError::not_found("envd_not_found", status.message().to_owned())
            }
            tonic::Code::InvalidArgument => {
                AppError::bad_request("envd_invalid_argument", status.message().to_owned())
            }
            tonic::Code::Unavailable => {
                AppError::upstream("envd_unavailable", status.message().to_owned())
            }
            _ => {
                tracing::warn!(?status, "tonic error → internal");
                AppError::Internal(anyhow::anyhow!("envd: {status}"))
            }
        }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
    request_id: &'a str,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            AppError::NotFound { code, message } => (StatusCode::NOT_FOUND, *code, message.clone()),
            AppError::BadRequest { code, message } => {
                (StatusCode::BAD_REQUEST, *code, message.clone())
            }
            AppError::Unauthorized { code, message } => {
                (StatusCode::UNAUTHORIZED, *code, message.clone())
            }
            AppError::Forbidden { code, message } => {
                (StatusCode::FORBIDDEN, *code, message.clone())
            }
            AppError::UpstreamUnavailable { code, message } => {
                (StatusCode::BAD_GATEWAY, *code, message.clone())
            }
            AppError::FailedPrecondition { code, message } => {
                (StatusCode::PRECONDITION_FAILED, *code, message.clone())
            }
            AppError::Internal(err) => {
                // Emit an OTel `exception` span event so this 5xx server fault
                // surfaces in the APM's **Issues** view, attached to the current
                // `http.request` (Endpoint) span. tracing-opentelemetry rewrites
                // an error-level event that has NO message and an `error` field
                // into an `exception` event (and sets span status=ERROR); adding
                // a message string would disqualify that mapping and only set
                // status — which is why Issues stayed empty before. The error's
                // Debug becomes `exception.message`, so the detail isn't lost.
                // Only 5xx does this: 4xx client errors are not faults and must
                // not create Issues.
                // DO NOT add a message string here — its absence is what triggers
                // the exception-event mapping (see the block comment above).
                tracing::error!(error = ?err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "an internal error occurred".to_owned(),
                )
            }
        };

        // We always generate a request-id here rather than threading
        // from the middleware-set request extension: `IntoResponse`
        // doesn't see the request, and the body must match the header.
        // The outer `request_id_layer` populates the same header on
        // success paths via `tower_http::request_id::PropagateRequestIdLayer`
        // (added in T2's router wiring later); for error paths we
        // overwrite with a locally-generated id and use it for both
        // header and body so they always agree.
        let request_id = generate_request_id();

        let body = ErrorBody {
            code,
            message: &message,
            request_id: &request_id,
        };

        let mut resp = (status, Json(body)).into_response();
        resp.headers_mut().insert(
            header::HeaderName::from_static("x-request-id"),
            header::HeaderValue::from_str(&request_id).expect("ascii request-id"),
        );
        resp
    }
}

/// Generate a `req_<32-hex>` correlation id.
///
/// Uses the system clock as a cheap, dependency-free entropy source.
/// Collisions don't matter for log correlation — request-ids are
/// non-security identifiers used to stitch logs across services.
fn generate_request_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("req_{nanos:032x}")
}
