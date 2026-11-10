//! HMAC-SHA256 signed URLs for file upload/download tokens.
//!
//! The gateway mints short-lived URLs of the shape
//! `/files/{upload,download}?token=<base64url(payload)>.<base64url(hmac)>`
//! and verifies them when the client `PUT`s/`GET`s. The token alone
//! grants exactly one operation (`PUT` or `GET`) against one
//! `(sandbox_id, path)` pair until `exp` (Unix epoch seconds); after
//! that, the gateway rejects it.
//!
//! `payload` is the JSON-serialised [`SignedUrlPayload`]. `hmac` is
//! `HMAC-SHA256(payload_bytes)` keyed with the secret loaded from
//! [`SigningKey::from_env`] (Helm wires this from a Kubernetes Secret
//! in Plan 04 T13). Signature verification uses constant-time
//! comparison via [`subtle::ConstantTimeEq`] so a key-recovery side
//! channel on tag comparison is closed.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::error::AppError;

/// HMAC-SHA256, the only MAC the token format speaks today. Held as a
/// type alias so the `Mac` trait methods (`update`, `finalize`,
/// `new_from_slice`) stay readable at the call site.
type HmacSha256 = Hmac<Sha256>;

/// Secret bytes used to MAC every signed URL.
///
/// The wrapped `Vec<u8>` is intentionally a newtype rather than a bare
/// `Vec<u8>` so handlers don't accidentally pass the wrong byte string
/// into [`mint`] or [`verify`]. The bytes themselves are not redacted
/// in `Debug`; the type is never logged on the success path, and a
/// debug-printed key in an error path is preferable to silent
/// truncation in incident triage.
#[derive(Clone)]
pub struct SigningKey(pub Vec<u8>);

impl SigningKey {
    /// Load the key from `SANDBAWKS_SIGNING_SECRET`.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment variable is unset. An empty
    /// value is allowed by `Hmac::new_from_slice`; the Helm chart in
    /// Plan 04 T13 guarantees the var is populated with random bytes
    /// before the gateway Pod starts.
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var("SANDBAWKS_SIGNING_SECRET")
            .map_err(|_| anyhow::anyhow!("SANDBAWKS_SIGNING_SECRET unset"))?;
        Ok(Self(raw.into_bytes()))
    }
}

/// The body of a signed token. Serialised as JSON and base64url-encoded
/// in the first half of the token string.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedUrlPayload {
    /// Sandbox the token authorises; the upload/download handler
    /// resolves this through `SandboxOps::get` to find the envd
    /// address. Mints for a non-existent sandbox are not rejected
    /// here — the handler returns `sandbox_not_found` at use time.
    pub sandbox_id: String,

    /// Sandbox-internal absolute path the operation targets. envd's
    /// `resolve()` chroot guard is the final defence against `../`
    /// escapes; the signing layer does not parse the path itself.
    pub path: String,

    /// `"PUT"` or `"GET"`. Upper-cased on mint so the verify side can
    /// compare with `==` instead of a case-insensitive match.
    pub method: String,

    /// Expiry instant, Unix epoch seconds. Compared against
    /// `chrono::Utc::now().timestamp()` on verify; tokens whose `exp`
    /// is in the past are rejected as `invalid_token`.
    pub exp: i64,
}

/// Build a token authorising `payload`.
///
/// The returned string has the shape `<body_b64>.<tag_b64>` using
/// URL-safe base64 with no padding so the token is a single query-string
/// value the SDKs can drop into a URL without further escaping.
pub fn mint(key: &SigningKey, payload: &SignedUrlPayload) -> String {
    // `serde_json::to_vec` on a `SignedUrlPayload` cannot fail —
    // every field is `String`/`i64`, neither of which has a non-UTF8
    // failure mode — so `expect` documents that invariant without
    // turning every mint call into a `Result`.
    let body = serde_json::to_vec(payload).expect("SignedUrlPayload always serialises");
    let mut mac = HmacSha256::new_from_slice(&key.0).expect("HMAC accepts keys of any length");
    mac.update(&body);
    let tag = mac.finalize().into_bytes();
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(&body),
        URL_SAFE_NO_PAD.encode(tag),
    )
}

/// Decode and validate a token minted by [`mint`].
///
/// # Errors
///
/// Returns [`AppError::BadRequest`] (`invalid_token`) for malformed
/// inputs (missing `.`, bad base64, payload not JSON) and
/// [`AppError::Unauthorized`] (`invalid_token`) when the MAC doesn't
/// match or `exp` is in the past. The distinction lets the gateway
/// surface 400 vs 401 to the SDK; both share the same `code` because
/// the SDK only branches on the code, not on which substring failed.
pub fn verify(key: &SigningKey, token: &str) -> Result<SignedUrlPayload, AppError> {
    let (body_b64, tag_b64) = token
        .split_once('.')
        .ok_or_else(|| AppError::bad_request("invalid_token", "expected body.tag"))?;
    let body = URL_SAFE_NO_PAD
        .decode(body_b64)
        .map_err(|_| AppError::bad_request("invalid_token", "bad payload b64"))?;
    let tag = URL_SAFE_NO_PAD
        .decode(tag_b64)
        .map_err(|_| AppError::bad_request("invalid_token", "bad tag b64"))?;

    // Recompute the MAC over the *decoded* body so trailing-garbage
    // attacks on the base64 encoding can't bypass the check.
    let mut mac = HmacSha256::new_from_slice(&key.0).expect("HMAC accepts keys of any length");
    mac.update(&body);
    let expected = mac.finalize().into_bytes();

    // Constant-time comparison: a `==` here would leak the position
    // of the first mismatched byte through timing and let an attacker
    // recover the tag one byte at a time.
    if expected.as_slice().ct_eq(&tag).unwrap_u8() == 0 {
        return Err(AppError::unauthorized(
            "invalid_token",
            "signature mismatch",
        ));
    }

    let payload: SignedUrlPayload = serde_json::from_slice(&body)
        .map_err(|_| AppError::bad_request("invalid_token", "payload not json"))?;
    if payload.exp < chrono::Utc::now().timestamp() {
        return Err(AppError::unauthorized("invalid_token", "expired"));
    }
    Ok(payload)
}
