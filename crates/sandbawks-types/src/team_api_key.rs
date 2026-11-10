use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authenticates a team at the sandbawks-api ingress and names the
/// tenant namespace it is scoped to.
///
/// The CR itself lives in the **system namespace** (alongside the
/// controller), out of tenant reach; `spec.namespace` names the tenant
/// namespace the key authorizes, and the api resolves
/// `TeamContext.namespace` from that field — not from the CR's own
/// `metadata.namespace` (#583).
///
/// We store **only the hash** of the key, never the key itself. The
/// 8-char prefix is indexed so a lookup is O(1) instead of scanning
/// every key on every API call; the rest of the key is then
/// constant-time-compared against the stored hash.
///
/// The full key is shown to the user exactly once at issue time and
/// is not recoverable.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.dev",
    version = "v1alpha1",
    kind = "TeamAPIKey",
    plural = "teamapikeys",
    shortname = "tak",
    namespaced,
    printcolumn = r#"{"name":"Prefix","type":"string","jsonPath":".spec.keyPrefix"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct TeamApiKeySpec {
    /// First 8 characters of the issued key, e.g. `"sbk_a1b2"`.
    pub key_prefix: String,

    /// SHA-256 hex digest of the full key, prefixed `"sha256:"`.
    pub key_hash: String,

    /// The tenant Kubernetes namespace this key authorizes. The api
    /// resolves `TeamContext.namespace` from this field (not from the
    /// CR's own `metadata.namespace`), so the key itself lives in the
    /// system namespace, out of tenant reach (#583).
    pub namespace: String,

    /// Scope strings. MVP only honours `sandboxes:*`; additional
    /// scopes are reserved for later plans.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
}
