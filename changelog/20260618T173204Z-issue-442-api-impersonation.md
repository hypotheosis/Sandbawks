# API per-tenant SA impersonation + SA tightening (issue #442, Phase 3)

Phase 3 of the multi-namespace tenancy epic (#581). The api gateway now performs
every tenant resource operation through a kube client that impersonates the
tenant's `sandbawks-tenant` ServiceAccount (provisioned in Phase 2, #583), so the
**apiserver** enforces namespace isolation — application logic is no longer the
only thing standing between tenants.

## What changed

- **`TenantClients` cache** (`crates/sandbawks-api/src/tenant_client.rs`): holds
  the base `kube::Config` and lazily builds one impersonating `Client` per
  namespace (`auth_info.impersonate = system:serviceaccount:<ns>:sandbawks-tenant`,
  user-only — no group impersonation), memoized in a `DashMap`.
- **Ops route through impersonation**: `KubeSandboxOps` and `KubeWarmClaim` build
  their `Api`s from the tenant's impersonating client. `AppState::tenant_client(ns)`
  exposes it; the `create_sandbox` template read uses it too.
- **Tenant Role gains `sandboxes/status: patch`** (`tenant_rbac.rs`): the
  warm-claim path's `apply_overrides` stamps `status.lastActivityTimestamp` at
  claim time *under impersonation* (it runs in a team-context request), so the
  tenant Role must permit it. Namespace-confined and a subset of the controller
  ClusterRole's `sandboxes/status` verbs, so the escalation guard still holds.
  (Distinct from `KubeSandboxOps::stamp_activity`, which runs identity-less on
  the proxy path and stays on the api's own SA.)
- **Startup**: the binary builds the base `Config` via `Config::infer()` and
  derives both the gateway's own client and the impersonation cache from it.
- **API SA tightening** (`rbac.yaml`): the api ServiceAccount **drops** tenant
  write authority — `sandboxes` create/delete/patch/update and the
  `sandboxtemplates` / `sandboxwarmpools` rules are gone (all now impersonated).
  It **gains** `impersonate` on `serviceaccounts` (cluster-wide; the tenant
  RoleBinding confines the actual access). It **keeps** the narrow non-tenant
  access the gateway needs under its own identity.

## Deliberate non-impersonated exceptions (under the api's own SA)

These paths carry no team identity, so they cannot impersonate and stay on the
gateway SA:

- The `envd_cache` Sandbox watch in the gateway default namespace
  (`sandboxes: get/list/watch`).
- `stamp_activity`, the idle-timeout `status` write on the proxy hot path
  (`sandboxes/status: patch`) — status-only, not a tenant-isolation hole.
  `KubeSandboxOps` holds both the impersonating cache (tenant CRUD) and the
  gateway's own client (this write).
- Event streaming (`routes::events`) reads `Events` in the team namespace under
  the gateway SA, because the tenant Role does not grant `events`. Read-only;
  a documented follow-up can fold it into impersonation once the tenant Role
  gains `events`.

## Tests / validation

- `tenant_client` unit tests assert the impersonation username and that
  `impersonated_config` sets the user (and only the user, no groups).
- The cross-namespace **403** defense-in-depth proof needs a live apiserver and
  is covered by the PR's kind manual test plan, not a unit test.

## Prerequisite

Depends on Phase 2 (#583, merged): the `tenant_rbac` reconciler must have
provisioned the `sandbawks-tenant` SA/Role/RoleBinding in each tenant namespace,
and keys must live in the system namespace with `spec.namespace` set. Without the
tenant RBAC, every impersonated call 403s.
