# TeamAPIKey relocation + tenant RBAC provisioning (issue #583, Phase 2)

Phase 2 of the multi-namespace tenancy epic (#581). Moves `TeamAPIKey` out of
tenant namespaces and adds a controller reconciler that provisions the
per-tenant RBAC the api will impersonate in Phase 3 (#442).

## What changed

- **`TeamAPIKey.spec.namespace`** — a new required field naming the tenant
  namespace a key authorizes. Tenant identity now comes from `spec.namespace`,
  not the CR's own `metadata.namespace`, so keys live in the **system
  namespace**, out of tenant reach.
- **Auth lookup** (`crates/sandbawks-api/src/auth.rs`) — `KubeKeyStore` lists
  `TeamAPIKey`s namespaced in the system namespace (read from
  `SANDBAWKS_SYSTEM_NAMESPACE`, injected via the downward API) instead of
  cluster-wide, and resolves `TeamContext.namespace` from `spec.namespace`. An
  empty `spec.namespace` is rejected as an internal error.
- **`tenant_rbac` reconciler** (`crates/sandbawks-controller/src/tenant_rbac.rs`)
  — folded into the controller binary alongside the Sandbox and warm-pool
  controllers. Watches `TeamAPIKey` cluster-wide and server-side-applies a
  `sandbawks-tenant` ServiceAccount + Role + RoleBinding into each
  `spec.namespace` (field manager `sandbawks-tenant-rbac`), keyed by namespace
  so multiple keys for one namespace converge.
- **Controller RBAC** — the controller SA gains `serviceaccounts` and
  `roles`/`rolebindings` write. The apiserver privilege-escalation guard is
  satisfied without `bind`/`escalate` because the tenant Role's verbs are a
  subset of the controller's existing cluster-wide sandbox verbs.

## Operational notes

- **CRD change** → run `kubectl apply -f deploy/crds` on deploy (Helm does not
  manage CRDs).
- **Migration:** existing `TeamAPIKey`s live in tenant namespaces. Recreate them
  in the system namespace with `spec.namespace` set to the original namespace.
  Until then, auth lookups in the system namespace find nothing.
- **No GC:** tenant RBAC is left in place when the last key for a namespace is
  removed (noted follow-up, out of scope).

## Tests

- `tenant_rbac_objects` builder is unit-tested (object names/namespaces, the
  RoleBinding→Role/SA wiring, and the escalation-guard invariant that the tenant
  Role grants no rbac/serviceaccount write). The apply path needs a live
  apiserver and is covered by the PR's kind manual test plan.
- `TeamAPIKey` YAML round-trip updated for `spec.namespace`.

Phase 3 (#442, API per-tenant SA impersonation) depends on this: it impersonates
the `sandbawks-tenant` SA this reconciler provisions.
