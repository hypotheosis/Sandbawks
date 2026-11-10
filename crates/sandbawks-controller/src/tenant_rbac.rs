//! Provisions the per-tenant `sandbawks-tenant` ServiceAccount + Role +
//! RoleBinding into each namespace named by a `TeamAPIKey.spec.namespace`,
//! so the api can impersonate that SA with apiserver-enforced isolation
//! (#583, unblocks #442). Watches `TeamAPIKey` cluster-wide; reconcile is
//! keyed by the target namespace, so multiple keys for one namespace
//! converge to one set of objects.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
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
/// Name of the provisioned tenant Role (same as the SA for symmetry).
pub const TENANT_ROLE: &str = "sandbawks-tenant";
/// Server-side-apply field manager that owns the provisioned objects.
const FIELD_MANAGER: &str = "sandbawks-tenant-rbac";

/// The three RBAC objects a tenant namespace needs.
pub struct TenantRbacObjects {
    /// The ServiceAccount the api impersonates.
    pub service_account: ServiceAccount,
    /// The Role granting the sandbox-tenant verbs.
    pub role: Role,
    /// The RoleBinding tying the Role to the SA.
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
        resources: Some(resources.iter().map(|s| (*s).to_owned()).collect()),
        verbs: verbs.iter().map(|s| (*s).to_owned()).collect(),
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
                    &[
                        "get", "list", "watch", "create", "update", "patch", "delete",
                    ],
                ),
                // `patch` is required for the warm-claim path: the api's
                // `apply_overrides` stamps `status.lastActivityTimestamp` at
                // claim time through the impersonated tenant client (#442).
                // Namespace-confined, and a subset of the controller
                // ClusterRole's `sandboxes/status: [get,update,patch]`, so the
                // escalation guard still holds.
                rule(&["sandboxes/status"], &["get", "patch"]),
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
    /// Cluster-wide client used to apply RBAC into each tenant namespace.
    pub client: Client,
}

/// Reconcile one `TeamAPIKey`: ensure its `spec.namespace` has the tenant RBAC.
async fn reconcile(
    key: Arc<TeamAPIKey>,
    ctx: Arc<TenantRbacContext>,
) -> Result<Action, kube::Error> {
    let namespace = key.spec.namespace.trim();
    if namespace.is_empty() {
        // Misconfigured key — nothing to provision; don't requeue tightly.
        tracing::warn!(
            key = %key.name_any(),
            "TeamAPIKey has empty spec.namespace; skipping tenant RBAC provisioning"
        );
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

/// Back off and retry on transient apiserver errors.
fn error_policy(_key: Arc<TeamAPIKey>, err: &kube::Error, _ctx: Arc<TenantRbacContext>) -> Action {
    tracing::warn!(error = %err, "tenant RBAC reconcile error");
    Action::requeue(Duration::from_secs(10))
}

/// Build the `tenant_rbac` controller future. Watches `TeamAPIKey`
/// cluster-wide; run it in the binary's `tokio::select!`.
pub fn controller(client: Client) -> impl std::future::Future<Output = ()> {
    let keys: Api<TeamAPIKey> = Api::all(client.clone());
    let ctx = Arc::new(TenantRbacContext { client });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_tenant_rbac_for_namespace() {
        let objs = tenant_rbac_objects("team-b");
        assert_eq!(
            objs.service_account.metadata.name.as_deref(),
            Some(TENANT_SA)
        );
        assert_eq!(
            objs.service_account.metadata.namespace.as_deref(),
            Some("team-b")
        );
        assert_eq!(objs.role.metadata.namespace.as_deref(), Some("team-b"));
        // The RoleBinding ties the Role to the SA in the same namespace.
        assert_eq!(objs.role_binding.role_ref.name, TENANT_ROLE);
        let subjects = objs.role_binding.subjects.as_ref().expect("subjects set");
        assert_eq!(subjects[0].name, TENANT_SA);
        assert_eq!(subjects[0].namespace.as_deref(), Some("team-b"));
        // Escalation-guard invariant: tenant verbs must stay a subset of the
        // controller's cluster-wide verbs — no rbac/serviceaccount write.
        let resources: Vec<&str> = objs
            .role
            .rules
            .as_ref()
            .expect("rules set")
            .iter()
            .flat_map(|r| r.resources.as_deref().unwrap_or(&[]))
            .map(String::as_str)
            .collect();
        assert!(resources.contains(&"sandboxes"));
        assert!(!resources.iter().any(|r| r.contains("rolebinding")));
        assert!(!resources.iter().any(|r| r.contains("serviceaccount")));

        // The warm-claim path's apply_overrides stamps status under
        // impersonation, so the tenant Role MUST allow patching
        // sandboxes/status — regression guard for the #442 warm-pool 403.
        let status_rule = objs
            .role
            .rules
            .as_ref()
            .expect("rules set")
            .iter()
            .find(|r| {
                r.resources
                    .as_deref()
                    .is_some_and(|res| res.iter().any(|s| s == "sandboxes/status"))
            })
            .expect("sandboxes/status rule present");
        assert!(
            status_rule.verbs.iter().any(|v| v == "patch"),
            "tenant Role must grant sandboxes/status patch for warm-claim apply_overrides, got {:?}",
            status_rule.verbs
        );
    }
}
