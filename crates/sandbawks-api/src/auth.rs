//! `TeamAPIKey`-backed authentication middleware.
//!
//! Wire-up:
//!
//! ```ignore
//! use std::sync::Arc;
//! use sandbawks_api::auth::{auth_middleware, KubeKeyStore};
//!
//! let store = Arc::new(KubeKeyStore::new(client, "sandbawks-system"));
//! let app = Router::new()
//!     .route("/sandboxes", post(handler))
//!     .layer(axum::middleware::from_fn_with_state(store, auth_middleware));
//! ```
//!
//! Downstream handlers extract `Extension<TeamContext>` (or one of
//! the typed extractors added in later tasks) to learn the caller's
//! namespace and scopes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    extract::{Request, State},
    http::HeaderMap,
    middleware::Next,
    response::Response,
};
use kube::{
    api::{DynamicObject, ListParams},
    core::ApiResource,
    Api, Client,
};
use sandbawks_types::{
    auth::{verify_key, ApiKeyHash, KeyPrefix},
    TeamAPIKey,
};

use crate::error::AppError;

/// Per-request authentication context attached to the request
/// extensions on successful auth.
///
/// Handlers receive this via `Extension<TeamContext>`; later tasks add
/// a typed extractor that also enforces a scope.
#[derive(Debug, Clone)]
pub struct TeamContext {
    /// Kubernetes Namespace the team owns. All Sandboxes the team is
    /// allowed to touch live here.
    pub namespace: String,

    /// Scope strings from `TeamAPIKey.spec.scopes`.
    ///
    /// MVP only honors `sandboxes:*`; finer-grained scopes are reserved
    /// for a later plan.
    pub scopes: Vec<String>,

    /// Name of the matching `TeamAPIKey` CR — useful for audit logging
    /// without leaking the raw key.
    pub key_name: String,
}

impl TeamContext {
    /// Whether the caller's scopes permit a specific operation.
    ///
    /// MVP rule: `sandboxes:*` grants everything; otherwise the scope
    /// must match the operation string exactly. We keep the match loose
    /// (string equality) here because the operation namespace is
    /// internal — later plans can swap in a structured scope grammar
    /// without breaking callers.
    pub fn allows(&self, op: &str) -> bool {
        self.scopes.iter().any(|s| s == "sandboxes:*" || s == op)
    }
}

/// Trait abstracting the `TeamAPIKey` lookup.
///
/// Production uses [`KubeKeyStore`]; tests use [`FakeKeyStore`]. Object
/// safety isn't a hard requirement — `auth_middleware` is generic over
/// `S: KeyStore + ?Sized` so it works with either `Arc<KubeKeyStore>`
/// or `Arc<dyn KeyStore>` depending on what the binary builds.
#[async_trait]
pub trait KeyStore: Send + Sync {
    /// Find a `TeamAPIKey` whose `keyPrefix` matches `prefix` and whose
    /// stored hash verifies `raw_key`. Returns the resolved namespace,
    /// scopes, and CR name on success; a uniform `unauthorized` error
    /// on any mismatch so timing doesn't leak which step failed.
    async fn resolve(&self, prefix: &KeyPrefix, raw_key: &str) -> Result<TeamContext, AppError>;
}

/// `kube::Api<TeamAPIKey>`-backed store.
///
/// Lists `TeamAPIKey`s in the **system namespace** (where keys live, out
/// of tenant reach — #583), not cluster-wide. The tenant namespace is
/// read from `spec.namespace`, so the api never needs blanket read across
/// every tenant namespace. For MVP scale (hundreds of keys in one
/// namespace) the namespaced list + client-side prefix filter is fine; a
/// later plan can enforce `metadata.name == "{keyPrefix}-{rand}"` via an
/// admission webhook so we can use a `metadata.name`-prefix selector for
/// cheaper listing.
///
/// ## Per-item fail-open listing (#597)
///
/// We deliberately list as [`DynamicObject`] (untyped JSON) and
/// deserialize each item into [`TeamAPIKey`] **independently**, rather
/// than letting `Api<TeamAPIKey>::list` deserialize the whole list as one
/// unit. kube's typed list deser is all-or-nothing: a single
/// un-deserializable item (e.g. a stale key created before a required
/// field was added — #583/#590) makes the entire list fail, which took
/// down auth for *every* tenant with a misleading `502 upstream_unavailable`.
/// By parsing per item we drop (and `WARN`-log, once) only the offending
/// key; every valid key keeps working.
pub struct KubeKeyStore {
    api: Api<DynamicObject>,
    /// Names of `TeamAPIKey` CRs we have already `WARN`-logged as
    /// un-deserializable, so a single malformed key is logged once rather
    /// than on every authenticated request (#597 acceptance criterion).
    ///
    /// Dedupe lifetime is the whole process: the set is never pruned (it is
    /// bounded by the count of distinct CR names, which is fine at MVP
    /// scale), and a key that is fixed and later re-broken will not re-log
    /// because its name is still present. If that ever matters, switch to a
    /// TTL/`resourceVersion`-keyed cache; for now the simplicity wins.
    logged_bad_keys: Mutex<HashSet<String>>,
}

impl KubeKeyStore {
    /// Build a store that lists `TeamAPIKey`s in `system_namespace`.
    pub fn new(client: Client, system_namespace: &str) -> Self {
        // Address the typed CR dynamically so the list response can be
        // parsed item-by-item (see the struct docs). `ApiResource::erase`
        // derives the GVK + plural from `TeamAPIKey`'s `Resource` impl, so
        // there is no hand-rolled GVK to drift from the type.
        let ar = ApiResource::erase::<TeamAPIKey>(&());
        Self {
            api: Api::namespaced_with(client, system_namespace, &ar),
            logged_bad_keys: Mutex::new(HashSet::new()),
        }
    }

    /// `WARN`-log a malformed `TeamAPIKey` exactly once, keyed by its CR
    /// name. Returns nothing; the caller skips the item regardless.
    fn warn_malformed_once(&self, name: &str, err: &serde_json::Error) {
        let mut seen = self
            .logged_bad_keys
            .lock()
            .expect("logged_bad_keys mutex poisoned");
        if seen.insert(name.to_owned()) {
            tracing::warn!(
                key_name = %name,
                error = %err,
                "skipping un-deserializable TeamAPIKey; this key is disabled \
                 until fixed but other keys continue to authenticate (#597)"
            );
        }
    }
}

#[async_trait]
impl KeyStore for KubeKeyStore {
    async fn resolve(&self, prefix: &KeyPrefix, raw_key: &str) -> Result<TeamContext, AppError> {
        // k8s field selectors don't index `spec.*` fields on custom
        // resources, so we filter client-side. `limit(500)` caps the
        // page size — at MVP scale a single page is more than enough.
        //
        // Listed as `DynamicObject` (untyped) so a single un-parseable
        // item can't fail the whole list (#597). A genuine apiserver
        // transport failure here still surfaces as `502 upstream_unavailable`
        // via the `kube::Error` → `AppError` conversion, which is correct:
        // that *is* an upstream outage. Per-item parse failures below are
        // not — they become a skip + a `401` for the presented key.
        let list = self.api.list(&ListParams::default().limit(500)).await?;

        for obj in list.items {
            let name = obj.metadata.name.clone().unwrap_or_default();

            // Deserialize this one item into the typed CR. A failure means
            // *this* key is malformed (e.g. missing a now-required field);
            // skip it, log once, and keep scanning the rest.
            //
            // `DynamicObject` serializes back to the canonical CR JSON
            // (apiVersion/kind from `types`, `metadata`, and the flattened
            // `data` carrying `spec`), so the round-trip through
            // `serde_json` is a faithful re-parse as the typed CR.
            let cr: TeamAPIKey =
                match serde_json::to_value(&obj).and_then(serde_json::from_value::<TeamAPIKey>) {
                    Ok(cr) => cr,
                    Err(err) => {
                        self.warn_malformed_once(&name, &err);
                        continue;
                    }
                };

            if cr.spec.key_prefix != prefix.as_str() {
                continue;
            }
            let stored = ApiKeyHash(cr.spec.key_hash.clone());
            if verify_key(raw_key, &stored) {
                // The tenant namespace is the authorized target named by
                // the key's spec (#583), NOT the CR's own metadata.namespace
                // (which is the system namespace where the key lives).
                let namespace = cr.spec.namespace.clone();
                if namespace.is_empty() {
                    // A misconfigured key is the *caller's* problem, not an
                    // upstream outage: return 401, not 502 (#597).
                    return Err(AppError::unauthorized(
                        "api_key_misconfigured",
                        "API key is misconfigured (empty namespace); contact an administrator",
                    ));
                }
                let key_name = cr.metadata.name.clone().unwrap_or_default();
                return Ok(TeamContext {
                    namespace,
                    scopes: cr.spec.scopes,
                    key_name,
                });
            }
        }

        Err(AppError::unauthorized("invalid_api_key", "no such api key"))
    }
}

/// In-memory store used by `tests/auth.rs`.
///
/// Pre-populate with [`FakeKeyStore::insert`] and pass it to
/// `auth_middleware` via `Arc`. No hashing is performed — the fake
/// stores raw keys and compares them directly so tests stay readable.
#[derive(Default)]
pub struct FakeKeyStore {
    inner: Mutex<Vec<(String, TeamContext)>>,
}

impl FakeKeyStore {
    /// Insert a `(raw_key → TeamContext)` mapping.
    ///
    /// `scopes` is a slice of `&str` for ergonomic call sites; we
    /// allocate once into owned `String`s on insert.
    pub fn insert(&self, raw_key: &str, namespace: &str, scopes: &[&str]) {
        let ctx = TeamContext {
            namespace: namespace.to_owned(),
            scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
            key_name: format!("fake-{}", &raw_key[..raw_key.len().min(8)]),
        };
        self.inner
            .lock()
            .expect("FakeKeyStore mutex poisoned")
            .push((raw_key.to_owned(), ctx));
    }
}

#[async_trait]
impl KeyStore for FakeKeyStore {
    async fn resolve(&self, prefix: &KeyPrefix, raw_key: &str) -> Result<TeamContext, AppError> {
        let guard = self.inner.lock().expect("FakeKeyStore mutex poisoned");
        for (stored_key, ctx) in guard.iter() {
            let stored_prefix = KeyPrefix::from_key(stored_key);
            if stored_prefix != *prefix {
                continue;
            }
            if stored_key == raw_key {
                return Ok(ctx.clone());
            }
        }
        Err(AppError::unauthorized("invalid_api_key", "no such api key"))
    }
}

/// `axum::middleware::from_fn_with_state` middleware that extracts the
/// `X-API-Key` header, resolves it to a [`TeamContext`] via the
/// supplied [`KeyStore`], and injects the context into the request
/// extensions.
///
/// On failure (missing header, unknown key) renders the standard E2B
/// 401 envelope via [`AppError::Unauthorized`].
pub async fn auth_middleware<S: KeyStore + ?Sized>(
    State(store): State<Arc<S>>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let raw_key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::unauthorized("missing_api_key", "X-API-Key header missing"))?;

    let prefix = KeyPrefix::from_key(raw_key);
    let ctx = store.resolve(&prefix, raw_key).await?;
    // Meter the request against the resolved team before handing off. This is the
    // single chokepoint every authenticated request flows through with a team
    // identity in scope, so recording here counts each request exactly once
    // without repeating a layer across every authenticated router — mirroring how
    // `warm_claim_metrics::record_claim` is driven from the claim path, not the
    // router (epic #480 Phase 1, issue #630). It is an anti-abuse signal only, not
    // a billing source — see `crate::usage_metrics`.
    crate::usage_metrics::record_request(&ctx.namespace, &ctx.key_name);
    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}
