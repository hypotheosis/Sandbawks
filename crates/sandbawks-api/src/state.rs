//! Shared `AppState` injected into every axum handler.
//!
//! Built once in `bin/sandbawks-api.rs`, cloned per request. All
//! fields are `Clone`-cheap (Arc'd internally) so axum can pass it
//! around freely.

use std::sync::Arc;

use kube::{Api, Client, Config};
use sandbawks_types::Sandbox;

use crate::activity::ActivityStamper;
use crate::auth::{KeyStore, KubeKeyStore};
use crate::envd_cache::EnvdCache;
use crate::envd_client::EnvdPool;
use crate::routes::sandboxes::{KubeSandboxOps, SandboxOps};
use crate::signing::SigningKey;
use crate::warm_claim::{KubeWarmClaim, WarmClaim};

/// The state every handler receives via `State<AppState>`.
///
/// Every field is `Clone`-cheap — fields that would otherwise allocate
/// on clone (`SigningKey`, `KeyStore`, `SandboxOps`) are `Arc`-wrapped
/// so the per-request `state.clone()` is just a handful of refcount
/// bumps.
#[derive(Clone)]
pub struct AppState {
    /// The gateway's own (non-impersonating) `kube::Client`, used only for
    /// the no-team-identity paths: the `KubeKeyStore` auth lookup, the
    /// `envd_cache` Sandbox watch, and the activity stamper. Tenant resource
    /// operations do NOT use this — they go through [`Self::tenant_client`]
    /// so the apiserver enforces namespace isolation (#442).
    pub kube: Client,

    /// Per-namespace impersonating client cache (#442). Tenant CRUD runs
    /// through `system:serviceaccount:<ns>:sandbawks-tenant`, so the
    /// apiserver confines each request to the tenant namespace.
    pub tenant_clients: Arc<crate::tenant_client::TenantClients>,

    /// Default namespace used when a request hasn't yet been
    /// authenticated. Tests inject `"team-acme"`; the binary reads
    /// `SANDBAWKS_DEFAULT_NAMESPACE`.
    pub default_namespace: Arc<str>,

    /// Base domain the gateway is served under when fronted by a
    /// wildcard DNS Ingress (e.g. `e2b.example.com`). When set, the
    /// per-sandbox proxy resolves the target sandbox from a
    /// `{port}-{sandbox_id}.{domain}` request `Host` — the scheme the
    /// official E2B SDKs use (issue #364) — falling back to the
    /// `E2b-Sandbox-Id` header. `None` (the default) keeps the gateway
    /// header-only, which is the right posture for localhost / kind /
    /// port-forward where there is no hostname to demux on. The binary
    /// reads it from `SANDBAWKS_SANDBOX_DOMAIN`. `Arc<str>` so the
    /// per-request `state.clone()` is a refcount bump, not a realloc.
    pub sandbox_domain: Option<Arc<str>>,

    /// Backing store for `auth_middleware`'s `TeamAPIKey` lookup.
    /// `Arc<dyn KeyStore>` so the binary uses `KubeKeyStore` while
    /// tests inject `FakeKeyStore` without recompiling the routes.
    pub key_store: Arc<dyn KeyStore>,

    /// Shared pool of pooled tonic channels keyed by `host:port`.
    /// Cloned per request — the underlying `DashMap` is `Arc`'d
    /// internally so every clone reuses the same cached channels.
    pub envd_pool: EnvdPool,

    /// Watcher-backed `sandbox_id -> envd address` cache consumed by
    /// `routes::envd_proxy::resolve_sandbox` (issue #201). Scoped to
    /// `default_namespace`; cloned per request — the backing map and
    /// notifier are `Arc`'d internally so every clone shares them.
    pub envd_cache: EnvdCache,

    /// CRD-ops abstraction used by `routes::sandboxes`. Hidden behind
    /// the `sandbox_ops()` accessor so the routes can swap the impl
    /// (real kube vs fake) via [`AppState::for_test`].
    sandbox_ops: Arc<dyn SandboxOps>,

    /// HMAC key backing the signed URL flow in `routes::files`. Loaded
    /// from `SANDBAWKS_SIGNING_SECRET` in [`AppState::from_kube`], and
    /// to a deterministic test value in [`AppState::for_test`]. `Arc`'d
    /// so the per-request clone doesn't reallocate the underlying
    /// secret bytes.
    pub signing_key: Arc<SigningKey>,

    /// Warm-pool claim backend consulted by `create_sandbox` before
    /// cold-booting (Plan 08 A7). Production wires [`KubeWarmClaim`];
    /// tests default to a no-pool fake so existing cases fall straight
    /// through to cold-boot. `Arc<dyn _>` so the impl is swappable
    /// without recompiling the routes.
    pub warm_claim: Arc<dyn WarmClaim>,

    /// Per-sandbox throttle behind [`AppState::note_activity`], which
    /// refreshes `status.lastActivityTimestamp` so the controller can run
    /// `spec.timeoutSeconds` as an idle window (issue #481). Cloned cheaply
    /// per request — the backing map is `Arc`'d internally.
    pub activity: ActivityStamper,
}

impl AppState {
    /// Production constructor: build state from a live `kube::Client`.
    ///
    /// Wires `KubeKeyStore` and `KubeSandboxOps` against the same
    /// client so both share the underlying HTTP/2 connection pool.
    ///
    /// # Panics
    ///
    /// Panics if `SANDBAWKS_SIGNING_SECRET` is unset — the gateway
    /// cannot mint or verify signed URLs without it, and the Helm
    /// chart in Plan 04 T13 guarantees the var is wired before the
    /// Pod starts. Failing fast at boot is preferable to surfacing
    /// 500s on the first `/files/mint` call hours later.
    pub fn from_kube(
        kube: Client,
        base_config: Config,
        default_namespace: impl Into<Arc<str>>,
        system_namespace: impl Into<Arc<str>>,
    ) -> Self {
        // Per-namespace impersonating clients over the same base config the
        // gateway's own client was built from (#442). Tenant CRUD routes
        // through these; the auth lookup / envd cache / activity stamper keep
        // using `kube` (no team identity to impersonate).
        let tenant_clients = Arc::new(crate::tenant_client::TenantClients::new(base_config));
        // Auth lists TeamAPIKeys in the system namespace (where keys live,
        // out of tenant reach — #583), not cluster-wide.
        let system_namespace: Arc<str> = system_namespace.into();
        let key_store: Arc<dyn KeyStore> =
            Arc::new(KubeKeyStore::new(kube.clone(), &system_namespace));
        // Tenant CRUD impersonates (via `tenant_clients`); the status-only
        // `stamp_activity` idle write uses the gateway's own `kube` client
        // (no team identity on the proxy path).
        let sandbox_ops: Arc<dyn SandboxOps> =
            Arc::new(KubeSandboxOps::new(tenant_clients.clone(), kube.clone()));
        let signing_key = Arc::new(
            SigningKey::from_env().expect("SANDBAWKS_SIGNING_SECRET must be set at startup"),
        );
        let default_namespace: Arc<str> = default_namespace.into();
        // Spawn the Sandbox watcher across every namespace (issue #13). A
        // per-sandbox envd call carries no team identity, but its access
        // token names the sandbox's namespace (see `envd_auth`), so the
        // cache must cover every tenant namespace, not just the default.
        // The gateway's ClusterRole already grants cluster-wide
        // `sandboxes: get/list/watch`.
        let envd_cache = EnvdCache::spawn(Api::<Sandbox>::all(kube.clone()));
        let warm_claim: Arc<dyn WarmClaim> = Arc::new(KubeWarmClaim::new(tenant_clients.clone()));
        // Bound the activity throttle map over the gateway's lifetime (#481
        // review): a periodic sweep drops entries for sandboxes idle longer
        // than the eviction window so ephemeral `sbx-<random>` ids don't
        // accumulate. Production-only — tests keep the map empty.
        let activity = ActivityStamper::new();
        activity.spawn_eviction();
        Self {
            kube,
            tenant_clients,
            default_namespace,
            // Default to header-only; the binary opts into hostname
            // routing via `with_sandbox_domain` when
            // `SANDBAWKS_SANDBOX_DOMAIN` is set.
            sandbox_domain: None,
            key_store,
            envd_pool: EnvdPool::new(),
            envd_cache,
            sandbox_ops,
            signing_key,
            warm_claim,
            activity,
        }
    }

    /// Test constructor that wires a [`FakeSandboxOps`] and a no-op
    /// `kube::Client`.
    ///
    /// The integration tests in `tests/sandboxes.rs` only drive the
    /// router, so the `kube::Client` field needs to *exist* but never
    /// gets called. We wrap a `tower::service_fn` that returns 204 so
    /// the type-checker is satisfied without standing up a real
    /// apiserver. Kept always-public (rather than `#[cfg(test)]` gated)
    /// to mirror the pattern already established by
    /// [`crate::auth::FakeKeyStore`] — the test surface is small,
    /// `Arc<dyn _>` already keeps prod dispatch monomorphic to
    /// `KubeSandboxOps`, and dropping the gate sidesteps the
    /// `--features test-util` dance for workspace-wide `cargo test`.
    ///
    /// [`FakeSandboxOps`]: crate::routes::sandboxes::FakeSandboxOps
    pub fn for_test(default_namespace: impl Into<Arc<str>>, key_store: Arc<dyn KeyStore>) -> Self {
        use crate::routes::sandboxes::FakeSandboxOps;

        // `kube::Client::new(service, default_ns)` requires a
        // `tower::Service<http::Request<kube::client::Body>>`. We never
        // call into this mock service in tests — the routes go through
        // `sandbox_ops` instead — but the field still has to be a
        // valid `Client` instance. A `service_fn` that returns 204 on
        // every request is enough to satisfy the type.
        let mock_service =
            tower::service_fn(|_req: axum::http::Request<kube::client::Body>| async {
                Ok::<_, std::convert::Infallible>(
                    axum::http::Response::builder()
                        .status(204)
                        .body(kube::client::Body::empty())
                        .expect("204 response is well-formed"),
                )
            });
        let kube = Client::new(mock_service, "test");
        // Tests drive the router through `FakeSandboxOps`/`FakeWarmClaim`, so
        // the impersonation cache is never exercised — but the field must
        // exist. Build it over a throwaway config that is never connected to.
        let tenant_clients = Arc::new(crate::tenant_client::TenantClients::new(Config::new(
            "https://test.invalid".parse().expect("valid uri"),
        )));
        Self {
            kube,
            tenant_clients,
            default_namespace: default_namespace.into(),
            sandbox_domain: None,
            key_store,
            envd_pool: EnvdPool::new(),
            // No watcher in tests — the cache starts empty and stays
            // that way unless a test seeds it via `EnvdCache::apply`, so
            // `resolve_sandbox` falls through to `FakeSandboxOps::get`.
            envd_cache: EnvdCache::new(),
            sandbox_ops: Arc::new(FakeSandboxOps::default()),
            signing_key: Arc::new(SigningKey(b"test-signing-key-do-not-use-in-prod".to_vec())),
            // No warm pool by default: existing tests fall straight
            // through to the cold-boot `create` path. Tests exercising
            // the claim path inject a populated fake via
            // [`AppState::with_warm_claim`].
            warm_claim: Arc::new(crate::warm_claim::FakeWarmClaim::empty()),
            activity: ActivityStamper::new(),
        }
    }

    /// Accessor for the `SandboxOps` impl. Returned by reference so
    /// callers can clone the `Arc` only when they need to.
    pub fn sandbox_ops(&self) -> &Arc<dyn SandboxOps> {
        &self.sandbox_ops
    }

    /// The impersonating client for `namespace`, built/cached lazily (#442).
    /// Use this for every tenant resource operation so the apiserver enforces
    /// namespace isolation; `self.kube` is reserved for the no-team-identity
    /// paths (auth lookup, envd cache, activity stamp).
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the impersonating client can't be built.
    pub fn tenant_client(&self, namespace: &str) -> Result<Client, crate::AppError> {
        self.tenant_clients.for_namespace(namespace)
    }

    /// Configure the wildcard base domain for hostname-based sandbox
    /// selection (issue #364). The binary calls this with the
    /// `SANDBAWKS_SANDBOX_DOMAIN` value (when set); tests use it to
    /// exercise the `{port}-{sandbox_id}.{domain}` parse path. Passing
    /// `None` is a no-op that leaves the gateway header-only.
    #[must_use]
    pub fn with_sandbox_domain(mut self, sandbox_domain: Option<Arc<str>>) -> Self {
        self.sandbox_domain = sandbox_domain;
        self
    }

    /// Override the warm-claim backend (test seam). Lets a test inject a
    /// populated [`crate::warm_claim::FakeWarmClaim`] so `create_sandbox`
    /// takes the claim path instead of cold-booting.
    #[must_use]
    pub fn with_warm_claim(mut self, warm_claim: Arc<dyn WarmClaim>) -> Self {
        self.warm_claim = warm_claim;
        self
    }
}
