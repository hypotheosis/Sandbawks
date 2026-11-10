use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A Kubernetes-style resource quantity, e.g. `"2"`, `"500m"`, `"1Gi"`.
///
/// We keep the original string verbatim instead of normalizing into a
/// numeric type. Kubernetes itself accepts the string form on the wire
/// and normalizes server-side, so storing the lexical form means we
/// don't round-trip through float and lose precision on units like
/// `"1500m"` or `"1.5Gi"`.
///
/// Parsing only validates the *shape* (non-empty, matches the regex
/// `^[0-9]+(\.[0-9]+)?[a-zA-Z]*$`). Numeric range / unit-correctness is
/// the API server's job — we deliberately don't reimplement it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ResourceQuantity(String);

impl ResourceQuantity {
    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResourceQuantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ResourceQuantity {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::InvalidQuantity(s.to_owned()));
        }
        // Shape check: digits, optional fractional part, optional unit suffix.
        // Avoids pulling in regex for one validator.
        let mut chars = s.chars().peekable();
        let mut saw_digit = false;
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                saw_digit = true;
                chars.next();
            } else {
                break;
            }
        }
        if !saw_digit {
            return Err(Error::InvalidQuantity(s.to_owned()));
        }
        if let Some(&'.') = chars.peek() {
            chars.next();
            let mut saw_frac = false;
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() {
                    saw_frac = true;
                    chars.next();
                } else {
                    break;
                }
            }
            if !saw_frac {
                return Err(Error::InvalidQuantity(s.to_owned()));
            }
        }
        // Anything left must be a unit suffix made of ASCII letters.
        for c in chars {
            if !c.is_ascii_alphabetic() {
                return Err(Error::InvalidQuantity(s.to_owned()));
            }
        }
        Ok(Self(s.to_owned()))
    }
}

/// Per-sandbox resource budget.
///
/// `cpu` and `memory` map directly onto a Pod's container `resources.limits`.
/// `ephemeral_storage` maps to the Pod's ephemeral-storage limit (the
/// snapshot delta of the sandbox's writable layer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequests {
    pub cpu: ResourceQuantity,
    pub memory: ResourceQuantity,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ephemeral_storage: Option<ResourceQuantity>,
}
