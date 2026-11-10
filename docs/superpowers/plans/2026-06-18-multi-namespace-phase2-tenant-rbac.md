# Multi-Namespace Tenancy (Phase 2): TeamAPIKey Relocation + Tenant RBAC Provisioning

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Relocate `TeamAPIKey` to the system namespace (identity via a new `spec.namespace`), and add a controller reconciler that provisions the per-tenant `sandbawks-tenant` ServiceAccount + Role + RoleBinding into each tenant namespace.

**Architecture:** `TeamAPIKey` gains `spec.namespace` (the tenant namespace the key authorizes); the api lists keys namespaced in the system namespace instead of cluster-wide and reads `team.namespace` from `spec.namespace`. A new `tenant_rbac` reconciler — folded into the existing controller binary alongside the Sandbox and warm-pool controllers — watches `TeamAPIKey` cluster-wide and server-side-applies the tenant RBAC into `spec.namespace`, keyed/deduped by namespace. The controller SA gains `serviceaccounts` + `roles`/`rolebindings` write; the escalation guard is satisfied because the tenant Role's verbs are a subset of the controller's existing cluster-wide verbs.

**Tech Stack:** Rust, `kube`/`kube-runtime` (`CustomResource`, `Controller`, `watcher`, `Api`, `Patch::Apply`), `k8s-openapi` (`ServiceAccount`, `Role`, `RoleBinding`, `PolicyRule`, `Subject`, `RoleRef`), `async-trait`. Phase 2 of the design spec `docs/superpowers/specs/2026-06-18-multi-namespace-tenancy-rbac-design.md`. Builds on Phase 1 (#582, merged). Unblocks Phase 3 (#442). Tracked by #583.

---

## File Structure

- `crates/sandbawks-types/src/team_api_key.rs` — add `spec.namespace`.
- `deploy/crds/teamapikeys.sandbawks.dev.yaml` — regenerated CRD (via `crdgen`).
- `crates/sandbawks-api/src/auth.rs` — `KubeKeyStore` lists in the system namespace and reads `team.namespace` from `spec.namespace`.
- `crates/sandbawks-api/src/state.rs` + `bin/sandbawks-api.rs` — thread a `system_namespace` into `KubeKeyStore`.
- `crates/sandbawks-controller/src/tenant_rbac.rs` — **new** reconciler module.
- `crates/sandbawks-controller/src/lib.rs` — export the module.
- `crates/sandbawks-controller/src/bin/sandbawks-controller.rs` — run the new reconciler in the `tokio::select!`.
- `deploy/helm/sandbawks/templates/rbac.yaml` — controller SA gains SA/Role/RoleBinding write.
- `deploy/helm/sandbawks/templates/api-deployment.yaml` — inject `SANDBAWKS_SYSTEM_NAMESPACE` (downward API).
- `deploy/helm/sandbawks/Chart.yaml` — version bump.
- `changelog/` — dated entry.

---

### Task 1: Add `TeamAPIKey.spec.namespace` and regenerate the CRD

**Files:**
- Modify: `crates/sandbawks-types/src/team_api_key.rs`
- Modify (generated): `deploy/crds/teamapikeys.sandbawks.dev.yaml`
- Test: `crates/sandbawks-types/tests/team_api_key.rs`

- [ ] **Step 1: Add the field**

In `team_api_key.rs`, add to `TeamApiKeySpec` (after `key_hash`):

```rust
    /// The tenant Kubernetes namespace this key authorizes. The api
    /// resolves `TeamContext.namespace` from this field (not from the
    /// CR's own `metadata.namespace`), so the key itself lives in the
    /// system namespace, out of tenant reach (#583).
    pub namespace: String,
```

- [ ] **Step 2: Update the doc comment on the struct**

Change the struct doc's "= one Kubernetes namespace" line to note keys live in the system namespace and `spec.namespace` names the tenant namespace.

- [ ] **Step 3: Regenerate the CRD**

Run: `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`
Expected: `wrote deploy/crds/teamapikeys.sandbawks.dev.yaml` (now includes `namespace` under `spec.properties` and in `required`).

- [ ] **Step 4: Update the type's YAML round-trip test**

In `crates/sandbawks-types/tests/team_api_key.rs`, add `namespace: "team-acme"` to the `yaml_roundtrip` fixture's spec and assert it survives the round-trip. Run: `cargo test -p sandbawks-types --test team_api_key`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-types/src/team_api_key.rs deploy/crds/teamapikeys.sandbawks.dev.yaml crates/sandbawks-types/tests/team_api_key.rs
git commit -m "feat(types): add TeamAPIKey.spec.namespace for system-namespace keys (#583)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Resolve tenant namespace from `spec.namespace`, list in the system namespace

**Files:**
- Modify: `crates/sandbawks-api/src/auth.rs`
- Modify: `crates/sandbawks-api/src/state.rs`, `crates/sandbawks-api/src/bin/sandbawks-api.rs`
- Test: `crates/sandbawks-api/src/auth.rs` (existing `#[cfg(test)]` module) or `tests/`

- [ ] **Step 1: Make `KubeKeyStore` namespaced to the system namespace**

In `auth.rs`, change the struct + constructor (it currently does `Api::all(client)`):

```rust
pub struct KubeKeyStore {
    api: Api<TeamAPIKey>,
}

impl KubeKeyStore {
    /// Build a store that lists `TeamAPIKey`s in the **system namespace**
    /// (where keys live, out of tenant reach — #583), instead of
    /// cluster-wide. The tenant namespace is read from `spec.namespace`.
    pub fn new(client: Client, system_namespace: &str) -> Self {
        Self {
            api: Api::namespaced(client, system_namespace),
        }
    }
}
```

- [ ] **Step 2: Read `team.namespace` from `spec.namespace`**

In `resolve`, replace the `cr.metadata.namespace` extraction with the spec field, and drop the now-stale "lists cluster-wide" comment:

```rust
            if verify_key(raw_key, &stored) {
                let namespace = cr.spec.namespace.clone();
                if namespace.is_empty() {
                    return Err(AppError::Internal(anyhow::anyhow!(
                        "TeamAPIKey {} has empty spec.namespace",
                        cr.metadata.name.clone().unwrap_or_default()
                    )));
                }
                let key_name = cr.metadata.name.clone().unwrap_or_default();
                return Ok(TeamContext {
                    namespace,
                    scopes: cr.spec.scopes,
                    key_name,
                });
            }
```

- [ ] **Step 3: Thread the system namespace from `AppState`/binary**

In `state.rs::from_kube`, add a `system_namespace: impl Into<Arc<str>>` parameter (store it as `pub system_namespace: Arc<str>`), and build the key store with it:

```rust
    let system_namespace: Arc<str> = system_namespace.into();
    let key_store: Arc<dyn KeyStore> = Arc::new(KubeKeyStore::new(kube.clone(), &system_namespace));
```

In `bin/sandbawks-api.rs`, read it from env (add a clap arg / `std::env::var`) — `SANDBAWKS_SYSTEM_NAMESPACE`, defaulting to the api's own namespace — and pass it to `from_kube`. In `AppState::for_test`, default `system_namespace` to the test default namespace so existing tests are unaffected.

- [ ] **Step 4: Update auth tests**

The existing `FakeKeyStore` is unaffected (it doesn't read `spec.namespace`). For `KubeKeyStore` coverage, add/adjust a test asserting `resolve` returns `spec.namespace` as `team.namespace`. (If the existing tests build a `TeamAPIKey` fixture, add `namespace: "team-acme".into()` to its spec.)

Run: `cargo test -p sandbawks-api auth`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-api/src/auth.rs crates/sandbawks-api/src/state.rs crates/sandbawks-api/src/bin/sandbawks-api.rs
git commit -m "feat(api): resolve tenant namespace from TeamAPIKey.spec.namespace, list in system ns (#583)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Tenant RBAC provisioning reconciler

**Files:**
- Create: `crates/sandbawks-controller/src/tenant_rbac.rs`
- Modify: `crates/sandbawks-controller/src/lib.rs` (add `pub mod tenant_rbac;` and re-exports)
- Modify: `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`
- Test: inline `#[cfg(test)]` in `tenant_rbac.rs`

- [ ] **Step 1: Write the failing test for the desired-objects builder**

The reconcile side-effects (apply to the apiserver) need a live cluster, so unit-test the **pure builder** that produces the three objects. In `tenant_rbac.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_tenant_rbac_for_namespace() {
        let objs = tenant_rbac_objects("team-b");
        assert_eq!(objs.service_account.metadata.name.as_deref(), Some(TENANT_SA));
        assert_eq!(objs.service_account.metadata.namespace.as_deref(), Some("team-b"));
        assert_eq!(objs.role.metadata.namespace.as_deref(), Some("team-b"));
        // The RoleBinding ties the Role to the SA in the same namespace.
        assert_eq!(objs.role_binding.role_ref.name, TENANT_ROLE);
        assert_eq!(objs.role_binding.subjects.as_ref().unwrap()[0].name, TENANT_SA);
        // Tenant verbs must stay a subset of the controller's cluster-wide
        // verbs (escalation-guard invariant): no rbac/serviceaccount write.
        let resources: Vec<&str> = objs
            .role
            .rules
            .as_ref()
            .unwrap()
            .iter()
            .flat_map(|r| r.resources.as_deref().unwrap_or(&[]))
            .map(String::as_str)
            .collect();
        assert!(resources.contains(&"sandboxes"));
        assert!(!resources.iter().any(|r| r.contains("rolebinding")));
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p sandbawks-controller --lib tenant_rbac 2>&1 | head -20`
Expected: FAIL — `tenant_rbac_objects` / `TENANT_SA` not defined.

- [ ] **Step 3: Implement the module**

```rust
//! Provisions the per-tenant `sandbawks-tenant` ServiceAccount + Role +
//! RoleBinding into each namespace named by a `TeamAPIKey.spec.namespace`,
//! so the api can impersonate that SA with apiserver-enforced isolation
//! (#583, unblocks #442). Watches `TeamAPIKey` cluster-wide; reconcile is
//! keyed by the target namespace, so multiple keys for one namespace
//! converge to one set of objects.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::ServiceAccount;
use k8s_openapi::api::rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, Client, ResourceExt};
use sandbawks_types::TeamAPIKey;

/// Name shared by the provisioned SA, Role, and RoleBinding in each tenant ns.
pub const TENANT_SA: &str = "sandbawks-tenant";
pub const TENANT_ROLE: &str = "sandbawks-tenant";
/// Server-side-apply field manager that owns the provisioned objects.
const FIELD_MANAGER: &str = "sandbawks-tenant-rbac";

/// The three RBAC objects a tenant namespace needs.
pub struct TenantRbacObjects {
    pub service_account: ServiceAccount,
    pub role: Role,
    pub role_binding: RoleBinding,
}

/// Build the desired SA + Role + RoleBinding for `namespace` (pure; no I/O).
///
/// The Role grants exactly the sandbox verbs the api exercises under
/// impersonation. These MUST stay a subset of the controller SA's
/// cluster-wide verbs so creating the RoleBinding passes the apiserver's
/// privilege-escalation guard without `bind`/`escalate`.
pub fn tenant_rbac_objects(namespace: &str) -> TenantRbacObjects {
    let meta = |name: &str| ObjectMeta {
        name: Some(name.to_owned()),
        namespace: Some(namespace.to_owned()),
        ..Default::default()
    };
    let rule = |resources: &[&str], verbs: &[&str]| PolicyRule {
        api_groups: Some(vec!["sandbawks.dev".to_owned()]),
        resources: Some(resources.iter().map(|s| s.to_string()).collect()),
        verbs: verbs.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    TenantRbacObjects {
        service_account: ServiceAccount {
            metadata: meta(TENANT_SA),
            ..Default::default()
        },
        role: Role {
            metadata: meta(TENANT_ROLE),
            rules: Some(vec![
                rule(
                    &["sandboxes"],
                    &["get", "list", "watch", "create", "update", "patch", "delete"],
                ),
                rule(&["sandboxes/status"], &["get"]),
                rule(&["sandboxtemplates"], &["get", "list", "watch"]),
                rule(&["sandboxwarmpools"], &["get", "list", "watch"]),
            ]),
        },
        role_binding: RoleBinding {
            metadata: meta(TENANT_SA),
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_owned(),
                kind: "Role".to_owned(),
                name: TENANT_ROLE.to_owned(),
            },
            subjects: Some(vec![Subject {
                kind: "ServiceAccount".to_owned(),
                name: TENANT_SA.to_owned(),
                namespace: Some(namespace.to_owned()),
                ..Default::default()
            }]),
        },
    }
}

/// Shared reconcile context: just the client (Apis are built per-namespace).
pub struct TenantRbacContext {
    pub client: Client,
}

/// Reconcile one `TeamAPIKey`: ensure its `spec.namespace` has the tenant RBAC.
async fn reconcile(key: Arc<TeamAPIKey>, ctx: Arc<TenantRbacContext>) -> Result<Action, kube::Error> {
    let namespace = key.spec.namespace.trim();
    if namespace.is_empty() {
        // Misconfigured key — nothing to provision; don't requeue tightly.
        tracing::warn!(key = %key.name_any(), "TeamAPIKey has empty spec.namespace; skipping");
        return Ok(Action::await_change());
    }
    let objs = tenant_rbac_objects(namespace);
    let pp = PatchParams::apply(FIELD_MANAGER);

    let sa_api: Api<ServiceAccount> = Api::namespaced(ctx.client.clone(), namespace);
    sa_api
        .patch(TENANT_SA, &pp, &Patch::Apply(&objs.service_account))
        .await?;
    let role_api: Api<Role> = Api::namespaced(ctx.client.clone(), namespace);
    role_api
        .patch(TENANT_ROLE, &pp, &Patch::Apply(&objs.role))
        .await?;
    let rb_api: Api<RoleBinding> = Api::namespaced(ctx.client.clone(), namespace);
    rb_api
        .patch(TENANT_SA, &pp, &Patch::Apply(&objs.role_binding))
        .await?;

    tracing::debug!(namespace = %namespace, "tenant RBAC reconciled");
    // RBAC rarely changes; re-list periodically as a drift backstop.
    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy(_key: Arc<TeamAPIKey>, err: &kube::Error, _ctx: Arc<TenantRbacContext>) -> Action {
    tracing::warn!(error = %err, "tenant RBAC reconcile error");
    Action::requeue(Duration::from_secs(10))
}

/// Build the `tenant_rbac` controller future. Watches `TeamAPIKey`
/// cluster-wide; run it in the binary's `tokio::select!`.
pub fn controller(
    client: Client,
) -> impl std::future::Future<Output = ()> {
    let keys: Api<TeamAPIKey> = Api::all(client.clone());
    let ctx = Arc::new(TenantRbacContext { client });
    use futures::StreamExt;
    Controller::new(keys, watcher::Config::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "tenant RBAC reconciled"),
                Err(e) => tracing::warn!(error = %e, "tenant RBAC reconcile stream error"),
            }
        })
}
```

- [ ] **Step 4: Export from `lib.rs`**

Add `pub mod tenant_rbac;` to `crates/sandbawks-controller/src/lib.rs`.

- [ ] **Step 5: Run the builder test**

Run: `cargo test -p sandbawks-controller --lib tenant_rbac`
Expected: PASS.

- [ ] **Step 6: Wire the reconciler into the controller binary**

In `bin/sandbawks-controller.rs`, after `client` is built (before the `tokio::select!`), construct the future, and add a branch:

```rust
    let tenant_rbac_controller = sandbawks_controller::tenant_rbac::controller(client.clone());
```

```rust
    tokio::select! {
        _ = controller => {},
        _ = warm_controller => {},
        _ = tenant_rbac_controller => {},
        _ = signal::ctrl_c() => tracing::info!("SIGINT received, shutting down"),
    }
```

- [ ] **Step 7: Build + clippy**

Run: `cargo build -p sandbawks-controller && cargo clippy -p sandbawks-controller --all-targets -- -D warnings`
Expected: PASS. (If `futures::StreamExt` is not already a dep of the crate, it is — the existing controllers use `.for_each`; reuse the same import path they use.)

- [ ] **Step 8: Commit**

```bash
git add crates/sandbawks-controller/src/tenant_rbac.rs crates/sandbawks-controller/src/lib.rs crates/sandbawks-controller/src/bin/sandbawks-controller.rs
git commit -m "feat(controller): tenant_rbac reconciler provisions per-namespace SA+Role+RoleBinding (#583)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: RBAC manifests, system-namespace env, chart bump, changelog

**Files:**
- Modify: `deploy/helm/sandbawks/templates/rbac.yaml`
- Modify: `deploy/helm/sandbawks/templates/api-deployment.yaml`
- Modify: `deploy/helm/sandbawks/Chart.yaml`
- Create: `changelog/<UTC>-issue-583-tenant-rbac-provisioning.md`

- [ ] **Step 1: Grant the controller SA the provisioning verbs**

In the `sandbawks-controller` ClusterRole `rules:` (rbac.yaml), add:

```yaml
  # Tenant RBAC provisioning (#583): the tenant_rbac reconciler creates a
  # `sandbawks-tenant` ServiceAccount + Role + RoleBinding in each tenant
  # namespace. Creating the RoleBinding passes the apiserver escalation guard
  # without `bind`/`escalate` because the tenant Role's verbs are a subset of
  # this ClusterRole's existing cluster-wide sandbox verbs.
  - apiGroups: [""]
    resources: ["serviceaccounts"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["roles", "rolebindings"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
```

- [ ] **Step 2: Inject the system namespace into the api Deployment**

In `api-deployment.yaml`, add to the api container's `env:` (mirroring how the controller injects `SANDBAWKS_NAMESPACE`):

```yaml
            # Keys live in this (the api's own / release) namespace; the api
            # lists TeamAPIKeys here, not cluster-wide (#583).
            - name: SANDBAWKS_SYSTEM_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
```

- [ ] **Step 3: Bump the chart version**

In `deploy/helm/sandbawks/Chart.yaml`, bump `version:` (patch increment). Leave `appVersion` to the release tooling.

- [ ] **Step 4: Validate the chart renders**

Run: `helm template deploy/helm/sandbawks | grep -A2 SANDBAWKS_SYSTEM_NAMESPACE` and `helm template deploy/helm/sandbawks | grep -A3 "rolebindings"`
Expected: the env var and the new controller rule render. (If `helm` is absent, `helm lint deploy/helm/sandbawks` substitute or note it skipped in the PR.)

- [ ] **Step 5: Write the changelog**

Create `changelog/<UTC>-issue-583-tenant-rbac-provisioning.md` (timestamp via `date -u +%Y%m%dT%H%M%SZ`) describing: `TeamAPIKey.spec.namespace` + system-namespace key lookup; the `tenant_rbac` reconciler and its SSA provisioning; the controller RBAC additions and the subset-of-verbs escalation invariant; the **migration** (recreate existing keys in the system namespace with `spec.namespace`); and that CRD regen requires `kubectl apply -f deploy/crds`.

- [ ] **Step 6: Full gates + commit**

Run: `just fmt && cargo clippy --all-targets -- -D warnings && just test`
Expected: PASS.

```bash
git add deploy/ changelog/
git commit -m "deploy: controller tenant-RBAC perms, api system-namespace env, chart bump (#583)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Notes for the implementer

- **Migration is operational, not code:** existing `TeamAPIKey`s live in tenant namespaces. After this ships, recreate them in the system namespace with `spec.namespace` set to the original namespace. Until then, auth lookups in the system namespace find nothing — call this out in the changelog and the PR.
- **`kubectl apply -f deploy/crds`** is required for the `spec.namespace` field (Helm does not manage CRDs).
- **No GC** of tenant RBAC when the last key for a namespace is deleted — out of scope (noted follow-up), per the spec.
- **The reconcile apply path has no unit test** (needs a live apiserver); the pure `tenant_rbac_objects` builder is unit-tested, and the apply is verified by the manual/kind test plan in the PR. The escalation-guard invariant (tenant verbs ⊆ controller verbs) is asserted in the builder test.
