//! `mint` + `verify` round-trip and tamper-rejection tests.
//!
//! These don't drive any router; they exercise the signing module
//! directly so a regression in the token shape or HMAC layer surfaces
//! independently of the upload/download handlers.

use sandbawks_api::signing::{mint, verify, SignedUrlPayload, SigningKey};

/// Shared key for the test cases. The bytes don't matter — only
/// that mint and verify use the same value.
fn key() -> SigningKey {
    SigningKey(b"unit-test-key-please-rotate".to_vec())
}

fn fresh_payload() -> SignedUrlPayload {
    SignedUrlPayload {
        sandbox_id: "sbx-abc".into(),
        path: "/home/user/x.txt".into(),
        method: "PUT".into(),
        exp: chrono::Utc::now().timestamp() + 60,
        namespace: Some("team-acme".into()),
    }
}

#[test]
fn round_trips() {
    let payload = fresh_payload();
    let token = mint(&key(), &payload);
    let decoded = verify(&key(), &token).expect("verify should accept fresh token");
    assert_eq!(decoded, payload);
}

#[test]
fn rejects_expired() {
    let payload = SignedUrlPayload {
        exp: 0,
        ..fresh_payload()
    };
    let token = mint(&key(), &payload);
    let err = verify(&key(), &token).expect_err("expired token must be rejected");
    // Expired tokens render as 401; the precise enum variant is
    // important so the gateway returns the right HTTP status.
    assert!(
        matches!(err, sandbawks_api::AppError::Unauthorized { .. }),
        "expected Unauthorized, got {err:?}",
    );
}

#[test]
fn rejects_tampered_payload() {
    let token = mint(&key(), &fresh_payload());
    // Flip a character in the payload half — the `.` separates body
    // from tag, so editing index 0 of the string is guaranteed to
    // land in the body.
    let mut chars: Vec<char> = token.chars().collect();
    chars[0] = if chars[0] == 'A' { 'B' } else { 'A' };
    let tampered: String = chars.into_iter().collect();
    let err = verify(&key(), &tampered).expect_err("tampered payload must fail verify");
    // Tampering surfaces as either a signature mismatch (401) or a
    // base64/json decode error (400) depending on which byte flipped;
    // both are valid failure modes, so just assert the call errored.
    let _ = err;
}

#[test]
fn rejects_tampered_tag() {
    let token = mint(&key(), &fresh_payload());
    // Mutate the *last* character — it sits in the tag half, so the
    // body still decodes cleanly and the HMAC compare is what fails.
    // This isolates the constant-time tag comparison from the
    // payload-decode path.
    let mut chars: Vec<char> = token.chars().collect();
    let last = chars.len() - 1;
    chars[last] = if chars[last] == 'A' { 'B' } else { 'A' };
    let tampered: String = chars.into_iter().collect();
    let err = verify(&key(), &tampered).expect_err("tampered tag must fail verify");
    let _ = err;
}

#[test]
fn rejects_malformed_token() {
    let err = verify(&key(), "not-a-dot-separated-token")
        .expect_err("missing `.` must be rejected as BadRequest");
    assert!(
        matches!(err, sandbawks_api::AppError::BadRequest { .. }),
        "expected BadRequest, got {err:?}",
    );
}

/// The namespace a token is minted for survives the round trip (issue #13).
#[test]
fn round_trips_namespace() {
    let token = mint(&key(), &fresh_payload());
    let decoded = verify(&key(), &token).expect("verify should accept");
    assert_eq!(decoded.namespace.as_deref(), Some("team-acme"));
}

/// A token minted before the `namespace` field existed has no namespace in
/// its body; it still verifies, and decodes as `None` (the handler then uses
/// the gateway's default namespace).
#[test]
fn pre_namespace_token_still_verifies() {
    let token = mint(
        &key(),
        &SignedUrlPayload {
            namespace: None,
            ..fresh_payload()
        },
    );
    let body = token.split_once('.').unwrap().0;
    let json = String::from_utf8(
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, body).unwrap(),
    )
    .unwrap();
    assert!(
        !json.contains("namespace"),
        "None is omitted from the body: {json}"
    );
    let decoded = verify(&key(), &token).expect("verify should accept");
    assert_eq!(decoded.namespace, None);
}
