//! Hash-helper round-trip and prefix-shape tests for the
//! `TeamAPIKey` auth helpers.

use sandbawks_types::auth::{hash_key, verify_key, ApiKeyHash, KeyPrefix};

#[test]
fn round_trips() {
    let raw = "sbk_a1b2c3d4eeeeeeeeeeeeeeee";
    let hashed: ApiKeyHash = hash_key(raw);
    assert!(hashed.0.starts_with("sha256:"));
    assert!(verify_key(raw, &hashed));
}

#[test]
fn rejects_wrong_key() {
    let hashed = hash_key("sbk_correct_correct_correct");
    assert!(!verify_key("sbk_wrongwr_wrongwr_wrongwr", &hashed));
}

#[test]
fn prefix_is_first_8_chars() {
    let p: KeyPrefix = KeyPrefix::from_key("sbk_a1b2c3d4eeeeeeeeeeeeeeee");
    assert_eq!(p.as_str(), "sbk_a1b2");
}

#[test]
fn prefix_short_key_does_not_pad() {
    let p = KeyPrefix::from_key("sbk_a1b");
    assert_eq!(p.as_str(), "sbk_a1b");
}

#[test]
fn rejects_malformed_hash() {
    // Missing the `sha256:` discriminator.
    assert!(!verify_key("anything", &ApiKeyHash("deadbeef".into())));
    // Right prefix, wrong digest length.
    assert!(!verify_key("anything", &ApiKeyHash("sha256:abcd".into())));
    // Right prefix and length but non-hex bytes.
    let bogus = format!("sha256:{}", "zz".repeat(32));
    assert!(!verify_key("anything", &ApiKeyHash(bogus)));
}
