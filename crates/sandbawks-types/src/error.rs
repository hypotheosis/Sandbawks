use thiserror::Error;

/// Errors raised when parsing or validating sandbawks types.
///
/// The variants here are intentionally schema-level: they describe what's
/// wrong with a piece of CRD data, not what went wrong with an API call
/// or controller reconcile. Higher-level error types live in the
/// downstream `sandbawks-api`, `sandbawks-controller`, and
/// `sandbawks-node` crates.
#[derive(Debug, Error)]
pub enum Error {
    /// A quantity string (e.g. `"2"`, `"1Gi"`, `"500m"`) could not be
    /// parsed into a structured ResourceQuantity.
    #[error("invalid resource quantity: {0}")]
    InvalidQuantity(String),

    /// JSON (de)serialization failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// YAML (de)serialization failed.
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}
