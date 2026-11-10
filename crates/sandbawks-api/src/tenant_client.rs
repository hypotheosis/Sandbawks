//! Per-namespace impersonating kube clients (#442).
//!
//! Every tenant resource operation runs through a `Client` that
//! impersonates `system:serviceaccount:<ns>:sandbawks-tenant`, so the
//! apiserver — not application logic — confines the request to the
//! tenant's namespace (the tenant `RoleBinding` provisioned in Phase 2,
//! #583). Clients are memoized per namespace: building one clones the base
//! `Config` and sets the impersonation user, which is cheap, but caching
//! avoids re-running the TLS/connector setup in `Client::try_from`.

use dashmap::DashMap;
use kube::{Client, Config};

use crate::error::AppError;

/// The SA identity the api impersonates for `namespace`.
pub fn impersonation_user(namespace: &str) -> String {
    format!("system:serviceaccount:{namespace}:sandbawks-tenant")
}

/// Clone `base` and set the tenant impersonation user (user only — no group
/// impersonation; the tenant Role is bound directly to the SA).
pub fn impersonated_config(base: &Config, namespace: &str) -> Config {
    let mut cfg = base.clone();
    cfg.auth_info.impersonate = Some(impersonation_user(namespace));
    cfg.auth_info.impersonate_groups = None;
    cfg
}

/// Cache of per-namespace impersonating clients over a shared base config.
pub struct TenantClients {
    base: Config,
    cache: DashMap<String, Client>,
    /// When set, [`Self::for_namespace`] returns this client for every
    /// namespace, skipping impersonation. Only [`Self::fixed`] sets it — used
    /// by tests that inject a mock `Client` and by `AppState::for_test`.
    fixed: Option<Client>,
}

impl TenantClients {
    /// Build the cache over the gateway's base `Config` (from
    /// `Config::infer()` at startup).
    pub fn new(base: Config) -> Self {
        Self {
            base,
            cache: DashMap::new(),
            fixed: None,
        }
    }

    /// Build a cache that hands back `client` for **every** namespace,
    /// bypassing impersonation. For tests that inject a mock `Client` (and
    /// `AppState::for_test`), which never reach a real apiserver.
    pub fn fixed(client: Client) -> Self {
        Self {
            base: Config::new("https://unused.invalid".parse().expect("valid uri")),
            cache: DashMap::new(),
            fixed: Some(client),
        }
    }

    /// Return the impersonating client for `namespace`, building and caching
    /// it on first use.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Internal`] if `Client::try_from` fails (broken base
    /// config / TLS) — which would already have failed at startup, so in
    /// practice this is infallible after boot.
    pub fn for_namespace(&self, namespace: &str) -> Result<Client, AppError> {
        if let Some(c) = &self.fixed {
            return Ok(c.clone());
        }
        if let Some(c) = self.cache.get(namespace) {
            return Ok(c.clone());
        }
        let cfg = impersonated_config(&self.base, namespace);
        let client = Client::try_from(cfg)
            .map_err(|e| AppError::Internal(anyhow::anyhow!("build impersonated client: {e}")))?;
        self.cache.insert(namespace.to_owned(), client.clone());
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn impersonation_user_is_the_tenant_sa() {
        assert_eq!(
            impersonation_user("team-b"),
            "system:serviceaccount:team-b:sandbawks-tenant"
        );
    }

    #[test]
    fn impersonated_config_sets_user_only() {
        // Build a minimal base config without a cluster (no I/O).
        let base = Config::new("https://example.invalid".parse().expect("valid uri"));
        let cfg = impersonated_config(&base, "team-b");
        assert_eq!(
            cfg.auth_info.impersonate.as_deref(),
            Some("system:serviceaccount:team-b:sandbawks-tenant")
        );
        // User-only: we deliberately do NOT impersonate groups; the tenant
        // RoleBinding is bound directly to the SA.
        assert!(cfg.auth_info.impersonate_groups.is_none());
    }
}
