//! Per-sandbox envd access tokens and E2B v1 file-URL signatures (issue #683).
//!
//! Hosted E2B secures every sandbox: the create / connect / get answer carries
//! an `envdAccessToken`, the SDK sends it as `X-Access-Token` on every envd
//! call, and `/files` URLs handed to third parties carry a `signature` derived
//! from it instead. This module is the gateway's half of that contract.
//!
//! # Token derivation
//!
//! The token is `HMAC-SHA256(signing_key, DOMAIN || sandbox_id)`,
//! base64url-encoded without padding. Deriving it (rather than minting a
//! random value and storing it on the `Sandbox` CR) means:
//!
//! - no CRD field and no apiserver round-trip to verify a request — the proxy
//!   checks the credential *before* looking the sandbox up, so an unauthorised
//!   caller cannot even learn whether an id exists;
//! - the token survives pause/resume, warm-pool claims and gateway restarts;
//! - a clone (new sandbox id) automatically gets a different token.
//!
//! The `DOMAIN` prefix separates these MACs from the signed-URL tokens in
//! [`crate::signing`], which share the same key. Rotating
//! `SANDBAWKS_SIGNING_SECRET` rotates every sandbox's token; clients holding
//! an old one must `connect` again to learn the new value.
//!
//! # File signatures
//!
//! The SDK's `get_signature` (`e2b/sandbox/signature.py`) computes
//! `"v1_" + base64(sha256("{path}:{operation}:{user}:{token}[:{expiration}]"))`
//! with standard base64 and the `=` padding stripped. [`verify_file_signature`]
//! recomputes that digest and checks the optional expiration.

use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::AppError;
use crate::signing::SigningKey;

/// Domain-separation prefix for the access-token MAC. The trailing NUL keeps a
/// sandbox id from ever being read as part of the prefix.
const TOKEN_DOMAIN: &[u8] = b"sandbawks/envd-access-token/v1\0";

/// Prefix of every E2B v1 file signature.
const SIGNATURE_PREFIX: &str = "v1_";

/// Which `/files` operation a signature authorises. The wire spelling is part
/// of the signed string, so it must match the SDK exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOperation {
    /// `GET /files` — download.
    Read,
    /// `POST /files` — upload.
    Write,
}

impl FileOperation {
    /// The SDK's spelling of the operation inside the signed string.
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// The parts of a `/files` request a signature is checked against, borrowed
/// straight from the query string.
#[derive(Debug, Clone, Copy)]
pub struct FileRequest<'a> {
    /// `path` query parameter, exactly as sent.
    pub path: &'a str,
    /// Operation implied by the HTTP method.
    pub operation: FileOperation,
    /// `username` query parameter; absent signs as the empty string.
    pub username: Option<&'a str>,
    /// `signature` query parameter.
    pub signature: Option<&'a str>,
    /// `signature_expiration` query parameter (Unix seconds, as text).
    pub signature_expiration: Option<&'a str>,
}

/// Derive the envd access token for `sandbox_id`.
///
/// # Arguments
///
/// * `key` - The gateway's signing key (`SANDBAWKS_SIGNING_SECRET`).
/// * `sandbox_id` - The sandbox id (`metadata.name` of the `Sandbox` CR).
///
/// # Returns
///
/// A 43-character base64url string, stable for a given key and id.
pub fn envd_access_token(key: &SigningKey, sandbox_id: &str) -> String {
    URL_SAFE_NO_PAD.encode(token_mac(key, sandbox_id))
}

/// Check that `presented` is `sandbox_id`'s access token, in constant time.
///
/// # Arguments
///
/// * `key` - The gateway's signing key.
/// * `sandbox_id` - The sandbox the request targets.
/// * `presented` - The `X-Access-Token` header value.
///
/// # Returns
///
/// `true` only when the token matches.
pub fn verify_access_token(key: &SigningKey, sandbox_id: &str, presented: &str) -> bool {
    let expected = envd_access_token(key, sandbox_id);
    expected.as_bytes().ct_eq(presented.as_bytes()).into()
}

/// Check a `/files` URL signature against `sandbox_id`'s access token.
///
/// # Arguments
///
/// * `key` - The gateway's signing key.
/// * `sandbox_id` - The sandbox the request targets.
/// * `request` - The signed parts of the request.
/// * `now_unix` - Current time in Unix seconds (a parameter so tests can pin it).
///
/// # Errors
///
/// Returns `AppError::Unauthorized` (`invalid_signature`) when the signature is
/// absent, malformed, does not match, or has expired, or when an expiration is
/// sent without a signature or does not parse.
pub fn verify_file_signature(
    key: &SigningKey,
    sandbox_id: &str,
    request: &FileRequest<'_>,
    now_unix: i64,
) -> Result<(), AppError> {
    let invalid = |msg: &str| AppError::unauthorized("invalid_signature", msg.to_owned());
    let signature = request
        .signature
        .ok_or_else(|| invalid("missing X-Access-Token header or signature"))?;
    let expiration = request
        .signature_expiration
        .map(|raw| {
            raw.parse::<i64>()
                .map_err(|_| invalid("signature_expiration is not a Unix timestamp"))
        })
        .transpose()?;

    let token = envd_access_token(key, sandbox_id);
    let expected = expected_signature(request, &token, expiration);
    if expected.as_bytes().ct_eq(signature.as_bytes()).unwrap_u8() == 0 {
        return Err(invalid("signature mismatch"));
    }
    // Checked after the MAC, so the expiration enforced is one the signer chose.
    if expiration.is_some_and(|exp| exp < now_unix) {
        return Err(invalid("signature expired"));
    }
    Ok(())
}

/// Raw HMAC bytes behind [`envd_access_token`].
fn token_mac(key: &SigningKey, sandbox_id: &str) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.0).expect("HMAC accepts keys of any length");
    mac.update(TOKEN_DOMAIN);
    mac.update(sandbox_id.as_bytes());
    mac.finalize().into_bytes().into()
}

/// The signature the SDK would compute for `request` (see the module docs).
fn expected_signature(request: &FileRequest<'_>, token: &str, expiration: Option<i64>) -> String {
    let mut hasher = Sha256::new();
    // Feeding the pieces separately hashes the same bytes as the SDK's
    // f-string without building the intermediate `String`.
    for part in [
        request.path,
        request.operation.as_str(),
        request.username.unwrap_or(""),
    ] {
        hasher.update(part.as_bytes());
        hasher.update(b":");
    }
    hasher.update(token.as_bytes());
    if let Some(exp) = expiration {
        hasher.update(format!(":{exp}").as_bytes());
    }
    format!(
        "{SIGNATURE_PREFIX}{}",
        STANDARD_NO_PAD.encode(hasher.finalize())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SigningKey {
        SigningKey(b"unit-test-key".to_vec())
    }

    fn request<'a>(signature: Option<&'a str>, exp: Option<&'a str>) -> FileRequest<'a> {
        FileRequest {
            path: "/tmp/up.txt",
            operation: FileOperation::Write,
            username: Some("user"),
            signature,
            signature_expiration: exp,
        }
    }

    /// Reference implementation copied from the SDK's `get_signature`, so the
    /// tests pin the wire format independently of [`expected_signature`].
    fn sdk_signature(path: &str, op: &str, user: &str, token: &str, exp: Option<i64>) -> String {
        let raw = match exp {
            None => format!("{path}:{op}:{user}:{token}"),
            Some(exp) => format!("{path}:{op}:{user}:{token}:{exp}"),
        };
        let digest = Sha256::digest(raw.as_bytes());
        format!("v1_{}", STANDARD_NO_PAD.encode(digest))
    }

    #[test]
    fn token_is_stable_and_per_sandbox() {
        let a = envd_access_token(&key(), "sbx-a");
        assert_eq!(a, envd_access_token(&key(), "sbx-a"));
        assert_ne!(a, envd_access_token(&key(), "sbx-b"));
        assert_ne!(
            a,
            envd_access_token(&SigningKey(b"other".to_vec()), "sbx-a")
        );
        assert_eq!(a.len(), 43);
    }

    #[test]
    fn verify_access_token_accepts_only_the_derived_token() {
        let token = envd_access_token(&key(), "sbx-a");
        assert!(verify_access_token(&key(), "sbx-a", &token));
        assert!(!verify_access_token(&key(), "sbx-b", &token));
        assert!(!verify_access_token(&key(), "sbx-a", ""));
    }

    #[test]
    fn signature_matches_sdk_without_expiration() {
        let token = envd_access_token(&key(), "sbx-a");
        let sig = sdk_signature("/tmp/up.txt", "write", "user", &token, None);
        assert!(verify_file_signature(&key(), "sbx-a", &request(Some(&sig), None), 0).is_ok());
    }

    #[test]
    fn signature_matches_sdk_with_expiration_until_it_expires() {
        let token = envd_access_token(&key(), "sbx-a");
        let sig = sdk_signature("/tmp/up.txt", "write", "user", &token, Some(1_000));
        let req = request(Some(&sig), Some("1000"));
        assert!(verify_file_signature(&key(), "sbx-a", &req, 1_000).is_ok());
        assert!(verify_file_signature(&key(), "sbx-a", &req, 1_001).is_err());
    }

    #[test]
    fn absent_username_signs_as_empty_string() {
        let token = envd_access_token(&key(), "sbx-a");
        let sig = sdk_signature("/f", "read", "", &token, None);
        let req = FileRequest {
            path: "/f",
            operation: FileOperation::Read,
            username: None,
            signature: Some(&sig),
            signature_expiration: None,
        };
        assert!(verify_file_signature(&key(), "sbx-a", &req, 0).is_ok());
    }

    #[test]
    fn signature_is_bound_to_sandbox_operation_and_expiration() {
        let token = envd_access_token(&key(), "sbx-a");
        let sig = sdk_signature("/tmp/up.txt", "read", "user", &token, Some(1_000));
        // Signed for `read`, presented for `write`.
        let req = request(Some(&sig), Some("1000"));
        assert!(verify_file_signature(&key(), "sbx-a", &req, 0).is_err());
        // Extending the expiration invalidates the MAC.
        let sig = sdk_signature("/tmp/up.txt", "write", "user", &token, Some(1_000));
        let req = request(Some(&sig), Some("9999"));
        assert!(verify_file_signature(&key(), "sbx-a", &req, 0).is_err());
        // Another sandbox's id.
        let req = request(Some(&sig), Some("1000"));
        assert!(verify_file_signature(&key(), "sbx-b", &req, 0).is_err());
    }

    #[test]
    fn missing_or_malformed_signature_is_rejected() {
        assert!(verify_file_signature(&key(), "sbx-a", &request(None, None), 0).is_err());
        assert!(verify_file_signature(&key(), "sbx-a", &request(None, Some("1")), 0).is_err());
        assert!(
            verify_file_signature(&key(), "sbx-a", &request(Some("v1_x"), Some("soon")), 0)
                .is_err()
        );
    }

    /// Vectors produced by the real SDKs' public `download_url` / `upload_url`
    /// (Python e2b 2.61.0 and JS e2b 2.62.0 gave identical signatures), with
    /// the clock frozen at 1_700_000_000, the key `unit-test-key` and the
    /// sandbox `sbx-a`. The SDK fills in `username=user` itself when the
    /// reported envd version is below 0.4.0 (ours is today, see #675), and
    /// otherwise leaves `username` out and signs it as `""`.
    #[test]
    fn real_sdk_vectors_verify() {
        assert_eq!(
            envd_access_token(&key(), "sbx-a"),
            "dp64teuDWRiZJxUhd9qEy2UkaNySo5-5ujiPR1LNMb8",
        );
        let path = "/tmp/my file \u{fc}.txt";
        // (operation, username query param, signature, signature_expiration)
        let vectors = [
            // envd >= 0.4.0, no user, no expiration: signs `user` as "".
            (
                FileOperation::Read,
                None,
                "v1_EdqosBYTySy5/2gStlSI2iJrWWVPAWObFsDFmeBND1k",
                None,
            ),
            // envd >= 0.4.0, no user, 60 s expiration.
            (
                FileOperation::Read,
                None,
                "v1_JOpsNxfGorpDHNwB/2eDmjc5+cHVACG29snbf3m/zls",
                Some("1700000060"),
            ),
            // envd < 0.4.0, no user: the SDK sends and signs `username=user`.
            (
                FileOperation::Write,
                Some("user"),
                "v1_QEnwnkcCJrpV3xdwgsmBELz2n0vOg7n+q7VOhuS08no",
                Some("1700000060"),
            ),
            // Explicit user.
            (
                FileOperation::Read,
                Some("root"),
                "v1_lW8LMmVhzFfnLEOzUQstDe8OdBqpT6XDJxeYTYi1/Tk",
                None,
            ),
        ];
        for (operation, username, signature, expiration) in vectors {
            let req = FileRequest {
                path,
                operation,
                username,
                signature: Some(signature),
                signature_expiration: expiration,
            };
            assert!(
                verify_file_signature(&key(), "sbx-a", &req, 1_700_000_000).is_ok(),
                "SDK vector rejected: {operation:?} {username:?} {expiration:?}",
            );
        }
    }
}
