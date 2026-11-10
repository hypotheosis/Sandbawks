# Multi-Namespace Tenancy (Phase 3): API Per-Tenant SA Impersonation + SA Tightening

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the api gateway perform every tenant resource operation through a kube client that impersonates the tenant's `sandbawks-tenant` ServiceAccount, so the apiserver enforces namespace isolation (defense in depth, #442), and tighten the api's own ServiceAccount so it is no longer a cross-namespace write authority.

**Architecture:** A new `TenantClients` cache holds the base `kube::Config` and lazily builds one impersonating `Client` per namespace (`auth_info.impersonate = "system:serviceaccount:<ns>:sandbawks-tenant"`), memoized in a `DashMap`. The CRD-ops layer (`KubeSandboxOps`, `KubeWarmClaim`) already builds `Api::namespaced(client, ns)` per call, so swapping their single shared client for the per-namespace impersonating client is contained to their constructors and `api()` helpers. Auth (`KubeKeyStore`) and the no-team-identity paths (the `envd_cache` Sandbox watch and the activity-stamper status patch, both in the gateway's default namespace) stay on the api's own client. The api SA then drops tenant write authority, keeping only the narrow reads those non-tenant paths need plus the `impersonate` verb.

**Tech Stack:** Rust, `kube` 0.97 (`Config::infer`, `Config.auth_info.impersonate`, `Client::try_from`), `dashmap`, `axum`. Phase 3 of the design spec `docs/superpowers/specs/2026-06-18-multi-namespace-tenancy-rbac-design.md`. Depends on Phase 2 (#583): the tenant RBAC and relocated keys must exist at runtime. Tracked by #442.

---

## Prerequisite

Phase 2 (#583) must be deployed: the `tenant_rbac` reconciler must have provisioned the `sandbawks-tenant` SA/Role/RoleBinding in each tenant namespace, and `TeamAPIKey`s must live in the system namespace with `spec.namespace` set. Without the tenant RBAC, every impersonated call 403s.

---

## File Structure

- `crates/sandbawks-api/src/tenant_client.rs` — **new**: `TenantClients` cache + impersonation config builder.
- `crates/sandbawks-api/src/lib.rs` — export the module.
- `crates/sandbawks-api/src/state.rs` — `AppState` holds `Arc<TenantClients>`; `from_kube` takes the base `Config`; add `tenant_client(&ns)` accessor; ops built from the cache.
- `crates/sandbawks-api/src/routes/sandboxes.rs` — `KubeSandboxOps` holds `Arc<TenantClients>`; the direct `Api::namespaced(state.kube, ...)` template lookup uses the tenant client.
- `crates/sandbawks-api/src/warm_claim.rs` — `KubeWarmClaim` holds `Arc<TenantClients>`.
- `crates/sandbawks-api/src/bin/sandbawks-api.rs` — build base `Config::infer()`, construct `TenantClients` + `AppState`.
- `deploy/helm/sandbawks/templates/rbac.yaml` — tighten the api SA, add `impersonate`.
- `deploy/helm/sandbawks/Chart.yaml` — version bump.
- `changelog/` — dated entry.

---

### Task 1: `TenantClients` impersonation cache

**Files:**
- Create: `crates/sandbawks-api/src/tenant_client.rs`
- Modify: `crates/sandbawks-api/src/lib.rs` (add `pub mod tenant_client;`)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test for the impersonation username + config mutation**

The cache build calls `Client::try_from` (needs valid TLS config), so unit-test the **pure** pieces: the username and the `Config` mutation.

```rust
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
        let base = kube::Config::new("https://example.invalid".parse().unwrap());
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
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p sandbawks-api --lib tenant_client 2>&1 | head -20`
Expected: FAIL — `impersonation_user` / `impersonated_config` not defined.

- [ ] **Step 3: Implement the module**

```rust
//! Per-namespace impersonating kube clients (#442).
//!
//! Every tenant resource operation runs through a `Client` that
//! impersonates `system:serviceaccount:<ns>:sandbawks-tenant`, so the
//! apiserver — not application logic — confines the request to the
//! tenant's namespace (the tenant `RoleBinding` provisioned in Phase 2).
//! Clients are memoized per namespace: building one clones the base
//! `Config` and sets the impersonation user, which is cheap, but caching
//! avoids re-running the TLS/connector setup in `Client::try_from`.

use std::sync::Arc;

use dashmap::DashMap;
use kube::{Client, Config};

use crate::error::AppError;

/// The SA identity the api impersonates for `namespace`.
pub fn impersonation_user(namespace: &str) -> String {
    format!("system:serviceaccount:{namespace}:sandbawks-tenant")
}

/// Clone `base` and set the tenant impersonation user (user only — no
/// group impersonation; the tenant Role is bound directly to the SA).
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
}

impl TenantClients {
    /// Build the cache over the gateway's base `Config` (from
    /// `Config::infer()` at startup).
    pub fn new(base: Config) -> Self {
        Self {
            base,
            cache: DashMap::new(),
        }
    }

    /// Return the impersonating client for `namespace`, building and
    /// caching it on first use.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Internal`] if `Client::try_from` fails (broken
    /// base config / TLS) — which would already have failed at startup,
    /// so in practice this is infallible after boot.
    pub fn for_namespace(&self, namespace: &str) -> Result<Client, AppError> {
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
```

(If `dashmap` is not already a workspace dep, the `EnvdPool`/`EnvdCache` use it — reuse the same dependency. Confirm with `grep -r "dashmap" crates/sandbawks-api/Cargo.toml`; add it if missing.)

- [ ] **Step 4: Export + run the test**

Add `pub mod tenant_client;` to `lib.rs`. Run: `cargo test -p sandbawks-api --lib tenant_client`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-api/src/tenant_client.rs crates/sandbawks-api/src/lib.rs
git commit -m "feat(api): per-namespace impersonating kube client cache (#442)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Route tenant ops through the impersonating client

**Files:**
- Modify: `crates/sandbawks-api/src/routes/sandboxes.rs` (`KubeSandboxOps`)
- Modify: `crates/sandbawks-api/src/warm_claim.rs` (`KubeWarmClaim`)
- Modify: `crates/sandbawks-api/src/state.rs` (`AppState`)

- [ ] **Step 1: `KubeSandboxOps` holds the tenant cache**

In `sandboxes.rs`, change the struct + `api()`:

```rust
pub struct KubeSandboxOps {
    clients: Arc<crate::tenant_client::TenantClients>,
}

impl KubeSandboxOps {
    pub fn new(clients: Arc<crate::tenant_client::TenantClients>) -> Self {
        Self { clients }
    }

    fn api(&self, namespace: &str) -> Result<Api<Sandbox>, AppError> {
        Ok(Api::namespaced(self.clients.for_namespace(namespace)?, namespace))
    }
}
```

Each `KubeSandboxOps` method that calls `self.api(namespace)` now does `self.api(namespace)?` (the methods already return `Result`). Update those call sites (the compiler lists them).

- [ ] **Step 2: `KubeWarmClaim` holds the tenant cache**

In `warm_claim.rs`, mirror the change: replace `client: Client` with `clients: Arc<crate::tenant_client::TenantClients>`, and make `sandboxes()`/`pools()` return `Result<Api<_>, AppError>` via `self.clients.for_namespace(namespace)?`. Update the call sites in the claim methods (they return `Result`).

- [ ] **Step 3: `AppState` holds `TenantClients` and exposes `tenant_client`**

In `state.rs`, add a field `pub tenant_clients: Arc<crate::tenant_client::TenantClients>` and an accessor:

```rust
    /// The impersonating client for `namespace` (built/cached lazily).
    /// Use for every tenant resource operation so the apiserver enforces
    /// namespace isolation (#442). The api's own client is reserved for
    /// the no-team-identity paths (auth lookup, envd cache, activity stamp).
    pub fn tenant_client(&self, namespace: &str) -> Result<kube::Client, AppError> {
        self.tenant_clients.for_namespace(namespace)
    }
```

- [ ] **Step 4: Swap the one direct shared-client use in `create_sandbox`**

In `sandboxes.rs::create_sandbox`, the template lookup currently builds `Api::namespaced(state.kube.clone(), &team.namespace)`. Replace with the impersonated client:

```rust
    let template_api: kube::Api<SandboxTemplate> =
        kube::Api::namespaced(state.tenant_client(&team.namespace)?, &team.namespace);
```

- [ ] **Step 5: Build + clippy**

Run: `cargo build -p sandbawks-api && cargo clippy -p sandbawks-api --all-targets -- -D warnings`
Expected: the `from_kube` constructor (Task 3) is the only remaining break — fix in Task 3. If iterating, comment Task 3 in first.

- [ ] **Step 6: Commit (with Task 3, since the constructor wiring is atomic)**

Defer the commit to the end of Task 3 — `AppState::from_kube` must build the cache before this compiles.

---

### Task 3: Build the base config + cache at startup

**Files:**
- Modify: `crates/sandbawks-api/src/state.rs` (`from_kube`, `for_test`)
- Modify: `crates/sandbawks-api/src/bin/sandbawks-api.rs`

- [ ] **Step 1: `from_kube` takes the base `Config`, builds the cache and impersonated ops**

Change `from_kube`'s signature to also accept the base `Config` (the api keeps `kube: Client` for the non-tenant paths — envd cache, auth, activity):

```rust
pub fn from_kube(
    kube: Client,
    base_config: Config,
    default_namespace: impl Into<Arc<str>>,
    system_namespace: impl Into<Arc<str>>,
) -> Self {
    let tenant_clients = Arc::new(crate::tenant_client::TenantClients::new(base_config));
    let system_namespace: Arc<str> = system_namespace.into();
    let key_store: Arc<dyn KeyStore> =
        Arc::new(KubeKeyStore::new(kube.clone(), &system_namespace)); // Phase 2: system-ns lookup
    let sandbox_ops: Arc<dyn SandboxOps> =
        Arc::new(KubeSandboxOps::new(tenant_clients.clone())); // impersonated
    let warm_claim: Arc<dyn WarmClaim> =
        Arc::new(KubeWarmClaim::new(tenant_clients.clone())); // impersonated
    // envd_cache stays on the api's own client + default namespace: the
    // envd proxy carries no team identity, so it cannot impersonate.
    let envd_cache =
        EnvdCache::spawn(Api::<Sandbox>::namespaced(kube.clone(), &default_namespace.into()));
    // ... (signing_key, activity, etc. unchanged) ...
    Self { kube, tenant_clients, system_namespace, /* ... */ }
}
```

(Adjust to the real field list — keep all existing fields; add `tenant_clients` and `system_namespace`.) `AppState::for_test` builds a `TenantClients` from a throwaway `Config::new(...)` so tests that never call `tenant_client` still construct.

- [ ] **Step 2: Binary builds the base `Config` and the base client from it**

In `bin/sandbawks-api.rs`, replace `Client::try_default()` with an explicit infer-then-build so the same validated `Config` seeds both the base client and the impersonation cache:

```rust
    let base_config = kube::Config::infer()
        .await
        .context("inferring kube config (kubeconfig or in-cluster service account)")?;
    let kube = Client::try_from(base_config.clone())
        .context("building kube client from inferred config")?;
```

Read `SANDBAWKS_SYSTEM_NAMESPACE` (Phase 2) and pass both namespaces + `base_config` into `from_kube`:

```rust
    let state = sandbawks_api::AppState::from_kube(
        kube,
        base_config,
        default_namespace,
        system_namespace,
    )
    .with_sandbox_domain(sandbox_domain);
```

- [ ] **Step 3: Full build + clippy + api tests**

Run: `cargo build -p sandbawks-api && cargo clippy -p sandbawks-api --all-targets -- -D warnings && cargo test -p sandbawks-api`
Expected: PASS. (Existing route tests use `AppState::for_test` with `FakeSandboxOps`/`FakeWarmClaim`, so they don't exercise impersonation — they only need to construct.)

- [ ] **Step 4: Commit Tasks 2+3 together**

```bash
git add crates/sandbawks-api/src/routes/sandboxes.rs crates/sandbawks-api/src/warm_claim.rs crates/sandbawks-api/src/state.rs crates/sandbawks-api/src/bin/sandbawks-api.rs
git commit -m "feat(api): route tenant ops through per-namespace impersonating client (#442)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Tighten the api SA, add `impersonate`, chart bump, changelog

**Files:**
- Modify: `deploy/helm/sandbawks/templates/rbac.yaml`
- Modify: `deploy/helm/sandbawks/Chart.yaml`
- Create: `changelog/<UTC>-issue-442-api-impersonation.md`

- [ ] **Step 1: Rewrite the `sandbawks-api` ClusterRole rules**

Tenant CRUD now flows through impersonation, so the api SA drops it. Keep only what the non-tenant paths need under the api's own identity (the `envd_cache` Sandbox watch in the default namespace, the activity-stamper `status` patch, the system-namespace `teamapikeys` read, and event reads), and add `impersonate`:

```yaml
rules:
  # Auth lookup (Phase 2: keys live in the system namespace). A namespaced
  # Role would be tighter, but keep the cluster read for now since the api
  # also resolves across the gateway's own namespace.
  - apiGroups: ["sandbawks.dev"]
    resources: ["teamapikeys"]
    verbs: ["get", "list", "watch"]
  # NON-tenant Sandbox access under the api's own identity: the envd-proxy
  # cache watches Sandboxes in the gateway default namespace (no team
  # identity to impersonate), and the activity stamper patches the `status`
  # subresource on the proxy path. Tenant CRUD is NOT here — it goes through
  # impersonation (#442).
  - apiGroups: ["sandbawks.dev"]
    resources: ["sandboxes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["sandbawks.dev"]
    resources: ["sandboxes/status"]
    verbs: ["get", "patch", "update"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["get", "list", "watch"]
  # Impersonate the per-namespace tenant SA so the apiserver enforces tenant
  # isolation (#442). The verb is cluster-wide on serviceaccounts (the
  # minimum to impersonate a SA in any tenant namespace); the tenant
  # RoleBinding is what actually confines the resulting access to one
  # namespace, so this grant is broad but powerless on its own.
  - apiGroups: [""]
    resources: ["serviceaccounts"]
    verbs: ["impersonate"]
```

Removed vs. before: `sandboxes` `create/delete/patch/update`, and the `sandboxtemplates` / `sandboxwarmpools` rules entirely (all now impersonated). Verify no remaining non-impersonated code path needs them: `grep -n "state.kube" crates/sandbawks-api/src/routes crates/sandbawks-api/src/warm_claim.rs` should show only the envd-cache/auth/activity uses after Task 2.

- [ ] **Step 2: Bump the chart version**

`deploy/helm/sandbawks/Chart.yaml` — patch increment.

- [ ] **Step 3: Render check**

Run: `helm template deploy/helm/sandbawks | grep -A2 impersonate`
Expected: the `impersonate` rule renders under the api ClusterRole. (If `helm` absent, note it in the PR.)

- [ ] **Step 4: Changelog**

Create `changelog/<UTC>-issue-442-api-impersonation.md` (timestamp via `date -u +%Y%m%dT%H%M%SZ`): the per-namespace impersonating client cache; tenant CRUD now apiserver-isolated; the api SA losing tenant write authority while retaining narrow non-tenant reads + `impersonate`; the Phase 2 prerequisite (tenant RBAC must exist or calls 403).

- [ ] **Step 5: Full gates + commit**

Run: `just fmt && cargo clippy --all-targets -- -D warnings && just test`
Expected: PASS.

```bash
git add deploy/ changelog/
git commit -m "deploy: tighten api SA to impersonate-only for tenant ops (#442)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Validation — the defense-in-depth proof (manual / kind)

The core security claim (a mis-threaded namespace is rejected by the apiserver, not silently allowed) requires a live apiserver and cannot be unit-tested with the fakes. The PR must run it on kind (or document it as not-yet-run with the exact steps):

1. Install the chart; create namespaces `team-a`, `team-b`; let Phase 2's `tenant_rbac` provision the tenant SA/Role/RoleBinding in each.
2. Create a `TeamAPIKey` in the system namespace with `spec.namespace: team-a`.
3. Happy path: authenticate with team-a's key, `POST /sandboxes` → 201; the Sandbox lands in `team-a`.
4. Isolation: with a build that deliberately threads `team.namespace = "team-b"` for team-a's key (or a unit harness that calls `tenant_client("team-b")` then writes), confirm the apiserver returns **403** (the impersonated `team-a` SA has no RoleBinding in `team-b`) — proving app logic is no longer the only backstop.

A lightweight automated stand-in (no cluster): a unit test asserting `impersonated_config(base, "team-b").auth_info.impersonate == Some("system:serviceaccount:team-b:sandbawks-tenant")` (Task 1) proves the identity wiring; the 403 itself is the manual gate above.

## Notes for the implementer

- **Do not impersonate for the envd proxy / activity stamper / auth lookup** — those carry no team identity and must stay on the api's own client. Only `team.namespace`-scoped CRUD impersonates.
- **`for_test`** must still construct a `TenantClients` (throwaway `Config`) so the ~all existing route tests (which use `FakeSandboxOps`) keep compiling without a cluster.
- **Order with Phase 2:** deploy #583 first. If the tenant RBAC isn't present, every tenant request 403s — surface that clearly in the PR and the rollout notes.
