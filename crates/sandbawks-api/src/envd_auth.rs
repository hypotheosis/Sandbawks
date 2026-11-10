//! Per-sandbox envd access tokens and E2B v1 file-URL signatures (issue #683).
//!
//! Hosted E2B secures every sandbox: the create / connect / get answer carries
//! an `envdAccessToken`, the SDK sends it as `X-Access-Token` on every envd
//! call, and `/files` URLs handed to third parties carry a `signature` derived
//! from it instead. This module is the gateway's half of that contract.
//!
//! # Token derivation
//!
//! The token is `<namespace>.<mac>`, where `mac` is
//! `HMAC-SHA256(signing_key, DOMAIN || namespace || NUL || sandbox_id)`,
//! base64url-encoded without padding (issue #13). Deriving it (rather than
//! minting a random value and storing it on the `Sandbox` CR) means:
//!
//! - no CRD field and no apiserver round-trip to verify a request — the proxy
//!   checks the credential *before* looking the sandbox up, so an unauthorised
//!   caller cannot even learn whether an id exists;
//! - the token survives pause/resume, warm-pool claims and gateway restarts;
//! - a clone (new sandbox id) automatically gets a different token.
//!
//! Binding the namespace into the MAC is what makes envd traffic safe across
//! tenant namespaces. Sandbox names are only unique *within* a namespace, so a
//! token over the id alone would let a tenant that holds a sandbox named `X`
//! reach another tenant's `X`. Carrying the namespace in clear text in front
//! of the MAC tells the proxy where to look without a lookup: it recomputes
//! the MAC for that `(namespace, id)` pair and only then resolves the sandbox.
//! A namespace is a DNS-1123 label and base64url has no `.`, so the first `.`
//! splits the two parts unambiguously.
//!
//! ## Legacy tokens
//!
//! Before issue #13 the token was `HMAC(signing_key, DOMAIN_V1 || sandbox_id)`
//! with no namespace, and the proxy only served sandboxes in the gateway's
//! default namespace. Clients may still hold such a token, so
//! [`verify_access_token`] accepts it — but only as a credential for the
//! *default* namespace, which is exactly what it could reach before. New
//! tokens are always namespaced.
//!
//! The `DOMAIN` prefixes separate these MACs from the signed-URL tokens in
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

/// Domain-separation prefix for the legacy (namespace-less) access-token MAC.
/// The trailing NUL keeps a sandbox id from ever being read as part of the
/// prefix.
const LEGACY_TOKEN_DOMAIN: &[u8] = b"sandbawks/envd-access-token/v1\0";

/// Domain-separation prefix for the namespaced access-token MAC (issue #13).
const TOKEN_DOMAIN: &[u8] = b"sandbawks/envd-access-token/v2\0";

/// Separator between the namespace and the MAC in a namespaced token.
const TOKEN_SEPARATOR: char = '.';

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

/// Derive the envd access token for the sandbox `sandbox_id` in `namespace`.
///
/// # Arguments
///
/// * `key` - The gateway's signing key (`SANDBAWKS_SIGNING_SECRET`).
/// * `namespace` - The sandbox's namespace (`metadata.namespace`).
/// * `sandbox_id` - The sandbox id (`metadata.name` of the `Sandbox` CR).
///
/// # Returns
///
/// `<namespace>.<43-character base64url MAC>`, stable for a given key,
/// namespace and id.
pub fn envd_access_token(key: &SigningKey, namespace: &str, sandbox_id: &str) -> String {
    let mac = URL_SAFE_NO_PAD.encode(token_mac(key, namespace, sandbox_id));
    let mut token = String::with_capacity(namespace.len() + 1 + mac.len());
    token.push_str(namespace);
    token.push(TOKEN_SEPARATOR);
    token.push_str(&mac);
    token
}

/// Derive the legacy, namespace-less token for `sandbox_id` (see the module
/// docs). Only ever valid for a sandbox in the gateway's default namespace.
///
/// # Arguments
///
/// * `key` - The gateway's signing key.
/// * `sandbox_id` - The sandbox id.
///
/// # Returns
///
/// A 43-character base64url string.
pub fn legacy_envd_access_token(key: &SigningKey, sandbox_id: &str) -> String {
    URL_SAFE_NO_PAD.encode(legacy_token_mac(key, sandbox_id))
}

/// Check `presented` against `sandbox_id`, in constant time, and return the
/// namespace it authorises.
///
/// A namespaced token authorises the namespace it names, if its MAC matches.
/// A legacy token authorises `default_namespace` only.
///
/// # Arguments
///
/// * `key` - The gateway's signing key.
/// * `sandbox_id` - The sandbox the request targets.
/// * `presented` - The `X-Access-Token` header value.
/// * `default_namespace` - The gateway's default namespace, the only one a
///   legacy token is valid for.
///
/// # Returns
///
/// The authorised namespace, or `None` when the token doesn't match.
pub fn verify_access_token<'a>(
    key: &SigningKey,
    sandbox_id: &str,
    presented: &'a str,
    default_namespace: &'a str,
) -> Option<&'a str> {
    match presented.split_once(TOKEN_SEPARATOR) {
        Some((namespace, _)) if !namespace.is_empty() => {
            let expected = envd_access_token(key, namespace, sandbox_id);
            bool::from(expected.as_bytes().ct_eq(presented.as_bytes())).then_some(namespace)
        }
        Some(_) => None,
        None => {
            let expected = legacy_envd_access_token(key, sandbox_id);
            bool::from(expected.as_bytes().ct_eq(presented.as_bytes())).then_some(default_namespace)
        }
    }
}

/// Check a `/files` URL signature against the access token it was made with.
///
/// The SDK signs with the token string, so the caller supplies each candidate
/// token in turn (see `authorize` in the envd proxy).
///
/// # Arguments
///
/// * `token` - The access token the signature is checked against.
/// * `request` - The signed parts of the request.
/// * `now_unix` - Current time in Unix seconds (a parameter so tests can pin it).
///
/// # Errors
///
/// Returns `AppError::Unauthorized` (`invalid_signature`) when the signature is
/// absent, malformed, does not match, or has expired, or when an expiration is
/// sent without a signature or does not parse.
pub fn verify_file_signature(
    token: &str,
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

    let expected = expected_signature(request, token, expiration);
    if expected.as_bytes().ct_eq(signature.as_bytes()).unwrap_u8() == 0 {
        return Err(invalid("signature mismatch"));
    }
    // Checked after the MAC, so the expiration enforced is one the signer chose.
    if expiration.is_some_and(|exp| exp < now_unix) {
        return Err(invalid("signature expired"));
    }
    Ok(())
}

/// Raw HMAC bytes behind [`envd_access_token`]. The NUL between namespace and
/// id keeps `("a", "bc")` and `("ab", "c")` apart.
fn token_mac(key: &SigningKey, namespace: &str, sandbox_id: &str) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.0).expect("HMAC accepts keys of any length");
    mac.update(TOKEN_DOMAIN);
    mac.update(namespace.as_bytes());
    mac.update(b"\0");
    mac.update(sandbox_id.as_bytes());
    mac.finalize().into_bytes().into()
}

/// Raw HMAC bytes behind [`legacy_envd_access_token`].
fn legacy_token_mac(key: &SigningKey, sandbox_id: &str) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.0).expect("HMAC accepts keys of any length");
    mac.update(LEGACY_TOKEN_DOMAIN);
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
    fn token_is_stable_and_per_sandbox_and_namespace() {
        let a = envd_access_token(&key(), "team-a", "sbx-a");
        assert_eq!(a, envd_access_token(&key(), "team-a", "sbx-a"));
        assert_ne!(a, envd_access_token(&key(), "team-a", "sbx-b"));
        assert_ne!(a, envd_access_token(&key(), "team-b", "sbx-a"));
        assert_ne!(
            a,
            envd_access_token(&SigningKey(b"other".to_vec()), "team-a", "sbx-a")
        );
        let (namespace, mac) = a.split_once('.').expect("namespaced token");
        assert_eq!(namespace, "team-a");
        assert_eq!(mac.len(), 43);
    }

    #[test]
    fn namespace_and_id_boundary_is_unambiguous() {
        assert_ne!(
            token_mac(&key(), "ab", "c"),
            token_mac(&key(), "a", "bc"),
            "the NUL separator must keep (ab, c) and (a, bc) apart"
        );
    }

    #[test]
    fn verify_access_token_returns_the_namespace_it_was_issued_for() {
        let token = envd_access_token(&key(), "team-a", "sbx-a");
        assert_eq!(
            verify_access_token(&key(), "sbx-a", &token, "default"),
            Some("team-a")
        );
        assert_eq!(
            verify_access_token(&key(), "sbx-b", &token, "default"),
            None
        );
        assert_eq!(verify_access_token(&key(), "sbx-a", "", "default"), None);
        assert_eq!(verify_access_token(&key(), "sbx-a", ".x", "default"), None);
    }

    #[test]
    fn a_token_cannot_be_relabelled_to_another_namespace() {
        let token = envd_access_token(&key(), "team-a", "sbx-a");
        let mac = token.split_once('.').unwrap().1;
        let forged = format!("team-b.{mac}");
        assert_eq!(
            verify_access_token(&key(), "sbx-a", &forged, "default"),
            None
        );
    }

    #[test]
    fn legacy_token_authorises_only_the_default_namespace() {
        let legacy = legacy_envd_access_token(&key(), "sbx-a");
        assert_eq!(legacy.len(), 43);
        assert!(!legacy.contains('.'));
        assert_eq!(
            verify_access_token(&key(), "sbx-a", &legacy, "sandbawks"),
            Some("sandbawks")
        );
        assert_eq!(
            verify_access_token(&key(), "sbx-b", &legacy, "sandbawks"),
            None
        );
        // A legacy MAC dressed up with a namespace is not a valid v2 token.
        let relabelled = format!("team-b.{legacy}");
        assert_eq!(
            verify_access_token(&key(), "sbx-a", &relabelled, "sandbawks"),
            None
        );
    }

    #[test]
    fn signature_matches_sdk_without_expiration() {
        let token = envd_access_token(&key(), "team-a", "sbx-a");
        let sig = sdk_signature("/tmp/up.txt", "write", "user", &token, None);
        assert!(verify_file_signature(&token, &request(Some(&sig), None), 0).is_ok());
    }

    #[test]
    fn signature_matches_sdk_with_expiration_until_it_expires() {
        let token = envd_access_token(&key(), "team-a", "sbx-a");
        let sig = sdk_signature("/tmp/up.txt", "write", "user", &token, Some(1_000));
        let req = request(Some(&sig), Some("1000"));
        assert!(verify_file_signature(&token, &req, 1_000).is_ok());
        assert!(verify_file_signature(&token, &req, 1_001).is_err());
    }

    #[test]
    fn absent_username_signs_as_empty_string() {
        let token = envd_access_token(&key(), "team-a", "sbx-a");
        let sig = sdk_signature("/f", "read", "", &token, None);
        let req = FileRequest {
            path: "/f",
            operation: FileOperation::Read,
            username: None,
            signature: Some(&sig),
            signature_expiration: None,
        };
        assert!(verify_file_signature(&token, &req, 0).is_ok());
    }

    #[test]
    fn signature_is_bound_to_sandbox_operation_and_expiration() {
        let token = envd_access_token(&key(), "team-a", "sbx-a");
        let sig = sdk_signature("/tmp/up.txt", "read", "user", &token, Some(1_000));
        // Signed for `read`, presented for `write`.
        let req = request(Some(&sig), Some("1000"));
        assert!(verify_file_signature(&token, &req, 0).is_err());
        // Extending the expiration invalidates the MAC.
        let sig = sdk_signature("/tmp/up.txt", "write", "user", &token, Some(1_000));
        let req = request(Some(&sig), Some("9999"));
        assert!(verify_file_signature(&token, &req, 0).is_err());
        // Another sandbox's token.
        let req = request(Some(&sig), Some("1000"));
        let other = envd_access_token(&key(), "team-a", "sbx-b");
        assert!(verify_file_signature(&other, &req, 0).is_err());
        // The same sandbox id in another namespace.
        let other = envd_access_token(&key(), "team-b", "sbx-a");
        assert!(verify_file_signature(&other, &req, 0).is_err());
    }

    #[test]
    fn missing_or_malformed_signature_is_rejected() {
        let token = envd_access_token(&key(), "team-a", "sbx-a");
        assert!(verify_file_signature(&token, &request(None, None), 0).is_err());
        assert!(verify_file_signature(&token, &request(None, Some("1")), 0).is_err());
        assert!(verify_file_signature(&token, &request(Some("v1_x"), Some("soon")), 0).is_err());
    }

    /// Vectors produced by the real SDKs' public `download_url` / `upload_url`
    /// (Python e2b 2.61.0 and JS e2b 2.62.0 gave identical signatures), with
    /// the clock frozen at 1_700_000_000, the key `unit-test-key` and the
    /// sandbox `sbx-a`, signed with the legacy token. The SDK treats the token
    /// as an opaque string, so these also pin the signature format for
    /// namespaced tokens. The SDK fills in `username=user` itself when the
    /// reported envd version is below 0.4.0 (ours is today, see #675), and
    /// otherwise leaves `username` out and signs it as `""`.
    #[test]
    fn real_sdk_vectors_verify() {
        let token = legacy_envd_access_token(&key(), "sbx-a");
        assert_eq!(token, "dp64teuDWRiZJxUhd9qEy2UkaNySo5-5ujiPR1LNMb8");
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
                verify_file_signature(&token, &req, 1_700_000_000).is_ok(),
                "SDK vector rejected: {operation:?} {username:?} {expiration:?}",
            );
        }
    }
}
