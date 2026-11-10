# Multi-Namespace Tenancy with RBAC Isolation

- **Date:** 2026-06-18
- **Status:** Approved design, pending implementation
- **Issue:** #442 (per-namespace tenant isolation via ServiceAccount impersonation),
  plus a prerequisite control-plane fix surfaced during design.
- **Owner crates:** `crates/sandbawks-controller`, `crates/sandbawks-node`,
  `crates/sandbawks-api`, `crates/sandbawks-types`; `deploy/`.

## Problem

The MVP design spec §9 states that all Sandbox / SandboxTemplate operations are
namespace-scoped via standard Kubernetes RBAC, with the API gateway impersonating a
per-namespace ServiceAccount. Two gaps separate the codebase from that statement:

1. **The control plane is single-namespace.** The controller and node daemonset both
   construct every watch with `Api::namespaced(client, &SANDBAWKS_NAMESPACE)`
   (`sandbawks-controller.rs:104-105,309,347-349`; `node/src/reconcile.rs::run`).
   `SANDBAWKS_NAMESPACE` is wired to each pod's own namespace via the downward API
   (`controller-deployment.yaml:37-40`, `node-daemonset.yaml:133-136`), so a
   `SandboxWarmPool`, `Sandbox`, or `SandboxTemplate` created in any other namespace
   is **never observed** — its reconciler is never invoked, and nothing happens. This
   was confirmed empirically: a warm pool created in a non-default namespace produced
   no members and no error. The reconcile *logic* is already namespace-aware (it reads
   each object's `metadata.namespace` and routes children correctly, e.g.
   `warm_pool.rs:522,615`; SnapshotJobs are created in the Sandbox's own namespace,
   `reconcile.rs:2043`) — only the *watch scope* is the constraint.

2. **Tenant isolation is enforced in application logic, not by RBAC.** The API resolves
   `team.namespace` and scopes operations with `Api::namespaced(client, &team.namespace)`
   (`routes/sandboxes.rs:319-321`), but the kube `Client` runs under the API pod's single
   ServiceAccount with broad cross-namespace write authority and **no impersonation**
   (`bin/sandbawks-api.rs:90-92`; zero `Impersonation` references in the tree). The
   apiserver never sees a per-tenant identity, so a gateway bug (wrong namespace threaded
   through) could read or write another tenant's resources. The apiserver is not a backstop.

Additionally, `TeamAPIKey` CRs live **in the tenant namespace** today — tenant identity is
derived from `metadata.namespace` (`auth.rs:122`), and the lookup lists cluster-wide
(`auth.rs:110`). A secret that lives inside the tenant's own namespace is a hazard: if the
tenant Role ever grants `teamapikeys` read (or an operator misconfigures it), the tenant can
read its own key hash (offline-cracking surface) or delete/rotate it.

## Goals

1. **One controller and one node daemonset, cluster-wide.** Both reconcile Sandbox /
   SandboxTemplate / SandboxWarmPool / SnapshotJob resources in **every** namespace, with no
   per-namespace install. This fixes the warm-pool-in-another-namespace bug.
2. **RBAC-enforced tenant isolation (#442).** The API gateway impersonates a per-namespace
   `sandbawks-tenant` ServiceAccount for each tenant request. Even if the gateway threads the
   wrong namespace, the apiserver rejects cross-namespace access (defense in depth).
3. **Tenant RBAC is provisioned automatically.** A controller reconciler materializes the
   per-tenant ServiceAccount + Role + RoleBinding so onboarding a tenant does not require
   hand-applying RBAC.
4. **API keys are out of tenant reach.** `TeamAPIKey` CRs live in the system (controller)
   namespace; the impersonated tenant SA has no path to any `TeamAPIKey`.
5. **The API pod's own SA loses blanket cross-namespace write authority**, retaining only
   what it genuinely needs (namespaced `teamapikeys` read + `impersonate`).

## Non-goals

- **Key issuance.** Writing `TeamAPIKey` CRs (and provisioning tenant namespaces themselves)
  stays out-of-band / operator-driven, as today. The provisioning controller only *reacts* to
  a `TeamAPIKey`; it does not create namespaces or keys.
- **Tenant RBAC garbage collection.** When the last `TeamAPIKey` for a namespace is removed,
  the provisioned SA/Role/RoleBinding are left in place (harmless, namespaced). GC is a noted
  follow-up, not built here (YAGNI).
- **Reworking the auth lookup algorithm.** The TeamAPIKey prefix lookup + constant-time
  compare is already implemented and tested; only its *list scope* and the *namespace source*
  field change.
- **Group impersonation.** The API impersonates the SA user only
  (`system:serviceaccount:<ns>:sandbawks-tenant`), not its groups; the tenant Role is bound
  directly to the SA.
- **Network-level isolation between sandbox pods** (tracked separately via default-deny
  egress NetworkPolicy).

## Approach decision

The user chose to tackle the control-plane fix and #442 together, with cluster-wide node
daemonsets (single install) and an automatic provisioning controller (over a static
Helm-templated tenant list or a fully out-of-band documented contract). The work is three
coupled but independently reviewable phases. Two structural choices, with reasoning:

- **Provisioning watches `TeamAPIKey`** (vs. labeled Namespaces): it is already the per-tenant
  marker, so no new labeling contract is introduced.
- **Provisioning lives inside the existing controller binary** (vs. a new component): it
  reuses the controller's kube client and leader election, and is infra-level reconciliation
  like the rest of the controller.

## Phase 1 — Multi-namespace control plane (code-only)

Both the controller and node **already hold cluster-wide `ClusterRole`/`ClusterRoleBinding`**
(`rbac.yaml:1-65` controller, `:128-182` node), so widening the watches needs **no RBAC
change** — only code.

- **Controller** (`sandbawks-controller.rs`): change `Api::namespaced(client, &ns)` to
  `Api::all(client)` for the Sandbox (104), SnapshotJob (105), owned-Pod (309), SandboxWarmPool
  (347), warm-member Sandbox (348), and SandboxTemplate (349) watches.
- **Store refactor**: `KubeSandboxStore`, `KubeSandboxTemplateStore`, and `KubeSnapshotJobStore`
  currently bake the namespace into an `Api` stored at construction
  (`KubeSandboxStore::new(client, namespace)`). Refactor them to hold a `kube::Client` and
  accept `namespace: &str` per method — the exact shape `KubeWarmPoolStore` already uses
  (`warm_pool.rs:688-694`). Reconcilers pass each object's own `metadata.namespace`. SnapshotJob
  creation already uses `sandbox.metadata.namespace` (`reconcile.rs:2043`), so that path works
  unchanged once the store is namespace-per-call.
- **Node** (`node/src/reconcile.rs::run`): change the `SnapshotJob` watch to `Api::all(client)`
  and **keep** the `spec.nodeName={node_name}` field selector (preserves the per-node watch
  budget). `patch_status` already takes `namespace` per call. Result: **one node daemonset
  watches the whole cluster**.
- **`SANDBAWKS_NAMESPACE`** retains exactly two roles: the leader-election `Lease` namespace
  (controller's own namespace) and telemetry resource attributes. It no longer scopes watches —
  documented in both deployment templates and the binaries.

Phase 1 is independently shippable and on its own fixes the reported warm-pool bug.

## Phase 2 — TeamAPIKey relocation + tenant RBAC provisioning controller

This phase combines the data-model change and the provisioning reconciler because provisioning
keys on `spec.namespace`, which the relocation introduces — they cannot ship separately.

### TeamAPIKey relocation (data-model change)

- **CRD** (`crates/sandbawks-types`, regenerated to `deploy/crds/teamapikeys.sandbawks.dev.yaml`):
  add **`spec.namespace`** — the target tenant namespace this key authorizes. The CRD stays
  `Namespaced`; in steady state all instances live in the **system namespace**.
- **Tenant identity now comes from `spec.namespace`**, not `metadata.namespace`.
- **`auth.rs` `KubeKeyStore`**: list `TeamAPIKey`s **namespaced in the system namespace** (read
  from `SANDBAWKS_NAMESPACE`/Release namespace) instead of cluster-wide, and read `team.namespace`
  from `spec.namespace`. This is both tighter and cheaper than the current cluster-wide list.
- **CRD schema change** → regenerate CRDs and **`kubectl apply -f deploy/crds`** on deploy (the
  Helm chart does not manage CRDs).
- **Migration**: existing `TeamAPIKey`s live in tenant namespaces; recreate them in the system
  namespace with `spec.namespace` set to the original namespace. Documented as an explicit,
  one-time pre-prod step (low risk).

### Provisioning reconciler

A new reconciler folded into the existing controller binary.

- **New module** `crates/sandbawks-controller/src/tenant_rbac.rs`; an additional reconciler in
  the controller's `tokio::select!`, reusing the shared client and leader election.
- **Watches `TeamAPIKey` cluster-wide** (`Api::all`). In the steady state these live only in the
  system namespace, but the watch is cluster-wide so a misplaced key is still observed. Reconcile
  is **keyed and deduplicated by target namespace** (`spec.namespace`); multiple keys for one
  namespace converge to one set of RBAC objects.
- For each target tenant namespace, **idempotently ensures via server-side apply** (stable field
  manager `sandbawks-tenant-rbac`):
  - `ServiceAccount/sandbawks-tenant`
  - `Role/sandbawks-tenant` granting the sandbox-tenant verb set:
    - `sandboxes`: `get,list,watch,create,update,patch,delete`
    - `sandboxes/status`: `get`
    - `sandboxtemplates`: `get,list,watch`
    - `sandboxwarmpools`: `get,list,watch` (claim path patches member Sandboxes, not pools)
    - (final verb set verified against every impersonated call during implementation)
  - `RoleBinding/sandbawks-tenant` binding the Role to the SA.
- **Controller SA RBAC additions** (`rbac.yaml`): `serviceaccounts` (`get,list,watch,create,
  update,patch`) and `rbac.authorization.k8s.io` `roles`/`rolebindings`
  (`get,list,watch,create,update,patch`). The Kubernetes privilege-escalation guard is satisfied
  **without** `bind`/`escalate` because the controller SA already holds the sandbox verbs
  cluster-wide, so the tenant Role's verbs are a subset of what the controller holds. This subset
  invariant is asserted in code review and is the reason the tenant Role must not grant anything
  broader than the controller SA.
- **No GC** (see Non-goals).

## Phase 3 — API per-tenant impersonation + SA tightening (#442 core)

### Per-tenant impersonation

- At startup keep the base `kube::Config`. Add a **per-namespace impersonating `Client` cache**
  on `AppState` (`RwLock<HashMap<String, Client>>`), built lazily on first request for a
  namespace: clone the base config and set
  `auth_info.impersonate = Some("system:serviceaccount:<ns>:sandbawks-tenant")` (user only, no
  groups), then `Client::try_from(config)`.
- A `state.tenant_client(&team.namespace)` helper returns the cached client. Route handlers build
  their `*Ops` / `Api` from this client instead of the shared one. Handlers that currently build
  `Api::namespaced(state.kube.clone(), &team.namespace)` directly (e.g. `sandboxes.rs:829`) switch
  to the tenant client. The auth lookup itself stays on the API's own SA — it runs *before*
  impersonation.

### API SA tightening (`rbac.yaml`)

- **Keep**: namespaced `teamapikeys` `get,list,watch` in the system namespace (auth lookup).
- **Add**: `impersonate` on `serviceaccounts`, granted cluster-wide. Impersonation assumes the
  *tenant* SA identity (`system:serviceaccount:<tenant-ns>:sandbawks-tenant`), and a cluster-wide
  `impersonate` verb is the minimum the apiserver requires to impersonate a SA in any tenant
  namespace. The tenant SA's own RoleBinding is what actually confines the resulting access to
  one namespace — the API SA's impersonate grant is deliberately broad but powerless on its own.
- **Remove**: the blanket cluster-wide write on `sandboxes` / `sandboxtemplates` /
  `sandboxwarmpools` — all tenant operations now flow through impersonation. Implementation must
  verify no non-impersonated code path still relies on the API SA for these (the warm-claim read
  path also moves to the tenant client).

## Data flow (end to end, after all phases)

1. Operator creates a tenant namespace (out-of-band) and a `TeamAPIKey` in the **system
   namespace** with `spec.namespace: <tenant-ns>`.
2. The controller's `tenant_rbac` reconciler observes the key and provisions
   `sandbawks-tenant` SA + Role + RoleBinding into `<tenant-ns>`.
3. A tenant request arrives at the API with its key. Auth lists `TeamAPIKey`s in the system
   namespace, matches by prefix + constant-time hash compare, and resolves
   `team.namespace = spec.namespace`.
4. The API obtains the cached impersonating client for `<tenant-ns>` and performs the CRUD;
   the apiserver authorizes it as `system:serviceaccount:<tenant-ns>:sandbawks-tenant`,
   confined to `<tenant-ns>` by the provisioned RoleBinding.
5. The cluster-wide controller and node observe the resulting CRs in `<tenant-ns>` and
   reconcile them (Pods, SnapshotJobs, etc.) regardless of namespace.

## Error handling

- **Provisioning**: SSA conflicts are normal under multiple keys/namespace and are retried via
  the standard controller backoff; a failed apply requeues the `TeamAPIKey`. Provisioning failure
  never blocks Sandbox reconciliation (separate reconciler).
- **Impersonation**: a missing/incomplete tenant RBAC surfaces as a `403` from the apiserver,
  mapped to the existing `AppError` path (the apiserver is the authority, not app logic). A
  `TeamAPIKey` with an empty `spec.namespace` is rejected at auth time as an internal error,
  mirroring the current "TeamAPIKey without namespace" guard.

## Testing

- **Phase 1**: store unit tests against fakes asserting ops target the object's own namespace;
  a reconcile test creating a Sandbox/WarmPool in a non-default namespace and asserting children
  appear there. Node: watch-construction unit coverage that the field selector is preserved.
- **Phase 2**: reconciler unit tests against a fake that assert the three RBAC objects are
  server-side-applied with the expected names, namespace, and verb set, and that two keys in one
  namespace converge (idempotent, deduped).
- **Phase 3**: `auth.rs` tests for the system-namespace list + `spec.namespace` resolution.
  Impersonation integration test (fakes or kind): a request authed for namespace A but mis-threaded
  to namespace B is **rejected `403` by the apiserver**, not silently allowed; plus happy-path CRUD
  under impersonation.
- **Gates**: `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`.

## Deployment / cross-cutting

- **Helm chart version bump** (Chart.yaml `version`) — `deploy/helm/sandbawks/` is touched.
- **CRD regen + `kubectl apply -f deploy/crds`** for the `TeamAPIKey` `spec.namespace` field.
- **Dated changelog** markdown under `changelog/` per phase (project convention).

## Phasing summary

| Phase | Scope | Shippable alone | Depends on |
|-------|-------|-----------------|------------|
| 1 | Cluster-wide controller + node watches; store refactor | Yes (fixes warm-pool bug) | — |
| 2 | `TeamAPIKey` relocation (`spec.namespace`) + `tenant_rbac` provisioning reconciler + controller SA RBAC | Yes | — |
| 3 | API impersonation + API SA tightening | Needs Phase 2 RBAC + relocated keys at runtime | 2 |
