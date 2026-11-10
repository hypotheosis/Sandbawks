# Multi-namespace control plane (issue #582, Phase 1)

The controller and node daemonset now reconcile Sandbox, SnapshotJob,
SandboxTemplate, and SandboxWarmPool resources in **every** namespace from a
single install. Previously every watch was scoped to `SANDBAWKS_NAMESPACE`
(each pod's own namespace via the downward API), so a resource created in any
other namespace was never observed and silently never reconciled — the cause of
"a warm pool in another namespace does nothing".

## What changed

- The Sandbox-path stores (`SandboxStore`, `SnapshotJobStore`,
  `SandboxTemplateStore`) are now namespace-per-call, mirroring the existing
  `KubeWarmPoolStore`: each holds a `kube::Client` and builds
  `Api::namespaced` per operation instead of binding one namespace at
  construction. `reconcile` takes the object's namespace (sourced from the
  watched object's `metadata.namespace`) and threads it through every store
  call and the store-touching helper fns (`observe_snapshot_job`, `dispatch`,
  `resolve_pod_inputs`).
- All controller watches (`Sandbox`, `SnapshotJob`, owned `Pod`,
  `SandboxWarmPool`, warm members, `SandboxTemplate`) and the node's
  `SnapshotJob` watch use `Api::all`. The node keeps its `spec.nodeName` field
  selector, so per-node watch budget is unchanged — it now spans namespaces.
- `SANDBAWKS_NAMESPACE` now scopes only the leader-election Lease and telemetry
  attributes, not the watches.

## Not changed

No RBAC, CRD, or Helm changes: the controller and node already hold cluster-wide
`ClusterRole`/`ClusterRoleBinding`. This is a code-only phase.

## Tests

- New `reconcile_threads_object_namespace_into_store` proves the reconcile loop
  threads a non-default namespace ("team-b") through every store call, via a
  namespace-recording `FakeSandboxStore`.
- Existing controller and node suites pass unchanged under the new signatures.

This is Phase 1 of the multi-namespace tenancy design (epic #581). Phase 2
(TeamAPIKey relocation + tenant RBAC provisioning, #583) and Phase 3 (API
per-tenant SA impersonation, #442) follow separately.
