//! Shared `TeamAPIKey` hashing helpers.
//!
//! The full key is never stored or logged. The CR carries:
//!
//! - `spec.keyPrefix` — first 8 chars of the issued key, used as a
//!   cheap discriminator when listing CRs.
//! - `spec.keyHash`   — `sha256:<lowercase-hex>` of the full key.
//!
//! At verify time the API gateway:
//!
//! 1. Computes [`KeyPrefix::from_key`] and narrows candidate CRs by
//!    `spec.keyPrefix`.
//! 2. For each candidate, [`verify_key`] runs a constant-time compare
//!    of the freshly-presented key against the stored hash.
//!
//! Living in `sandbawks-types` lets the controller's admission code
//! (later plan) reuse these helpers — the api crate doesn't own them.

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Indexed prefix used to narrow `TeamAPIKey` lookups.
///
/// Always the first up to 8 characters of the raw key. We intentionally
/// take *up to* 8 rather than padding shorter keys — padding would
/// either collide across teams or mask malformed input; truncation does
/// neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPrefix(String);

impl KeyPrefix {
    /// Build a prefix by taking the first 8 characters of `raw`.
    ///
    /// Counts *characters*, not bytes, so multibyte input doesn't blow
    /// up on a non-char-boundary slice.
    pub fn from_key(raw: &str) -> Self {
        Self(raw.chars().take(8).collect())
    }

    /// Borrow the underlying prefix string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stored API-key hash.
///
/// Wire format is `sha256:<lowercase-hex>` so a CR can be inspected
/// with `kubectl get teamapikey -o yaml` without ambiguity about which
/// digest produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyHash(pub String);

/// Hash a freshly-issued API key into the value safe to put in the CR.
///
/// # Examples
///
/// ```
/// use sandbawks_types::auth::{hash_key, verify_key};
/// let hashed = hash_key("sbk_a1b2c3d4eeeeeeeeeeeeeeee");
/// assert!(hashed.0.starts_with("sha256:"));
/// assert!(verify_key("sbk_a1b2c3d4eeeeeeeeeeeeeeee", &hashed));
/// ```
pub fn hash_key(raw: &str) -> ApiKeyHash {
    let mut h = Sha256::new();
    h.update(raw.as_bytes());
    let digest = h.finalize();
    ApiKeyHash(format!("sha256:{}", hex_encode(&digest)))
}

/// Constant-time compare a presented key against a stored hash.
///
/// Returns `false` for any malformed stored hash (wrong prefix,
/// non-hex bytes, wrong digest length) so a bad CR can't accidentally
/// authenticate anyone.
pub fn verify_key(raw: &str, stored: &ApiKeyHash) -> bool {
    let Some(stored_hex) = stored.0.strip_prefix("sha256:") else {
        return false;
    };
    let Ok(stored_bytes) = hex_decode(stored_hex) else {
        return false;
    };
    if stored_bytes.len() != 32 {
        return false;
    }

    let mut h = Sha256::new();
    h.update(raw.as_bytes());
    let presented = h.finalize();

    // `ct_eq` returns `Choice` (0 or 1). The `.into()` lifts it to `bool`
    // without short-circuiting, which is what we want for constant-time.
    presented.as_slice().ct_eq(&stored_bytes).into()
}

/// Lowercase hex encode without pulling in a dependency.
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(nibble_to_hex(b >> 4));
        out.push(nibble_to_hex(b & 0x0f));
    }
    out
}

/// Decode an even-length lowercase/uppercase hex string into bytes.
///
/// Returns `Err(())` for any malformed input — callers treat this as a
/// signal to reject the stored hash entirely rather than try to recover.
fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        return Err(());
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    // `as_chunks::<2>()` splits into fixed-size `[u8; 2]` arrays (plus a
    // remainder, empty here since the length is even), so each pair
    // destructures without bounds checks.
    let (pairs, _remainder) = bytes.as_chunks::<2>();
    for &[high, low] in pairs {
        out.push(hex_pair_to_byte(high, low)?);
    }
    Ok(out)
}

fn nibble_to_hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => unreachable!("nibble must be 0..=15"),
    }
}

fn hex_pair_to_byte(hi: u8, lo: u8) -> Result<u8, ()> {
    Ok((hex_to_nibble(hi)? << 4) | hex_to_nibble(lo)?)
}

fn hex_to_nibble(c: u8) -> Result<u8, ()> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(10 + c - b'a'),
        b'A'..=b'F' => Ok(10 + c - b'A'),
        _ => Err(()),
    }
}
