# Multi-Namespace Control Plane (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the controller and node daemonset reconcile Sandbox / SnapshotJob / SandboxTemplate / SandboxWarmPool resources in **every** namespace from a single install, fixing the bug where a resource created in a non-default namespace is silently never reconciled.

**Architecture:** The control plane is single-namespace only because its watches use `Api::namespaced(client, &SANDBAWKS_NAMESPACE)` and the Sandbox-path stores bind one namespace at construction. The warm-pool path is already namespace-per-call (`WarmPoolStore` methods take `namespace`, `KubeWarmPoolStore` holds a `Client`). Phase 1 (a) refactors the three Sandbox-path stores and the `reconcile` entrypoint to take `namespace` per call — mirroring `KubeWarmPoolStore` — and (b) flips all six watch constructions in the controller binary plus the node's SnapshotJob watch from `Api::namespaced` to `Api::all`. Both daemons already hold cluster-wide `ClusterRole`/`ClusterRoleBinding`, so **no RBAC, CRD, or Helm change is required**. `SANDBAWKS_NAMESPACE` is retained solely for the leader-election Lease and telemetry attributes.

**Tech Stack:** Rust, `kube` / `kube-runtime` (`Controller`, `watcher`, `Api`), `async-trait`, `tokio`, `tracing`. This is Phase 1 of the design spec `docs/superpowers/specs/2026-06-18-multi-namespace-tenancy-rbac-design.md`; Phases 2 (TeamAPIKey relocation + tenant RBAC provisioning) and 3 (API impersonation) follow in their own plans.

---

## File Structure

Files modified in this phase (no new files):

- `crates/sandbawks-controller/src/store.rs` — the three Sandbox-path store traits (`SandboxStore`, `SnapshotJobStore`, `SandboxTemplateStore`), their `Kube*` production impls, and `Fake*` test impls. Each trait method gains a leading `namespace: &str`; each `Kube*` store holds a `kube::Client` and builds `Api::namespaced` per call (mirroring `KubeWarmPoolStore` in `warm_pool.rs:678-695`).
- `crates/sandbawks-controller/src/reconcile.rs` — `reconcile` gains a leading `namespace: &str` param, threaded through the store call sites and the helper fns that touch stores.
- `crates/sandbawks-controller/src/bin/sandbawks-controller.rs` — six `Api::namespaced` → `Api::all`; the three Sandbox-path store constructors drop their `&namespace` argument; `reconcile_adapter` extracts the namespace from the watched object and passes it to `reconcile`.
- `crates/sandbawks-node/src/reconcile.rs` — the SnapshotJob watch `Api::namespaced` → `Api::all`, keeping the `spec.nodeName=` field selector.
- Test files: `crates/sandbawks-controller/tests/{reconcile,lifecycle,restore_config,sandbox_failure_metrics}.rs` — update `reconcile(...)` call sites to pass a namespace.
- `changelog/` — dated markdown per project convention.

The namespace-threading rule for `reconcile.rs`: changing the trait method signatures is a **compile-time fence** — `cargo build` flags every call site and every helper that must forward `namespace`. Use the compiler as the checklist; the enumerated sites below are the known set.

---

### Task 1: Namespace-aware Sandbox-path stores + reconcile threading

This is one atomic task because a trait-signature change is not independently compilable: the trait, both impls, the reconcile loop, and the tests must land together to keep the crate green.

**Files:**
- Modify: `crates/sandbawks-controller/src/store.rs`
- Modify: `crates/sandbawks-controller/src/reconcile.rs`
- Modify: `crates/sandbawks-controller/src/bin/sandbawks-controller.rs:624-638` (`reconcile_adapter`)
- Test: `crates/sandbawks-controller/src/store.rs` (existing `#[cfg(test)]` module) + `crates/sandbawks-controller/tests/reconcile.rs`

- [ ] **Step 1: Add a namespace-recording field to `FakeSandboxStore` and write the failing behavioral test**

In `crates/sandbawks-controller/src/store.rs`, extend the fake to record the namespaces it is called with (so we can prove `reconcile` threads the object's namespace through):

```rust
#[derive(Default, Clone)]
pub struct FakeSandboxStore {
    inner: Arc<Mutex<HashMap<String, Sandbox>>>,
    /// Namespaces passed to `get` / `patch_status`, in call order.
    /// Lets tests assert the reconcile loop threads the object's own
    /// namespace through the store (multi-namespace support, #442 Phase 1).
    pub seen_namespaces: Arc<Mutex<Vec<String>>>,
}
```

Add a test to `crates/sandbawks-controller/tests/reconcile.rs` (alongside the existing tests) that drives a reconcile in a non-default namespace and asserts the store saw it:

```rust
#[tokio::test(flavor = "current_thread")]
async fn reconcile_threads_object_namespace_into_store() {
    // A Sandbox reconciled out of the controller's install namespace must
    // have its OWN namespace ("team-b") threaded into every store call —
    // this is what makes a single controller reconcile all namespaces.
    let store = FakeSandboxStore::new();
    let mut sbx = sandbox("sbx-ns");
    sbx.metadata.namespace = Some("team-b".to_owned());
    store.insert(sbx);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    let seen = store.seen_namespaces.clone();

    reconcile("team-b", "sbx-ns", ctx(store, adapter, jobs))
        .await
        .expect("reconcile ok");

    assert!(
        seen.lock().expect("poisoned").iter().all(|ns| ns == "team-b"),
        "every store call must use the object's namespace, got {:?}",
        seen.lock().expect("poisoned")
    );
}
```

(`sandbox(name)` and `ctx(...)` helpers already exist in `tests/reconcile.rs`. The test references the new `reconcile(namespace, name, ctx)` signature introduced below, so it will not compile until Steps 2-6 land — that is the intended RED.)

- [ ] **Step 2: Verify it fails to compile**

Run: `cargo test -p sandbawks-controller --test reconcile reconcile_threads_object_namespace_into_store 2>&1 | head -20`
Expected: compile error — `reconcile` takes 2 args, not 3 / no field `seen_namespaces` until Step 3.

- [ ] **Step 3: Add `namespace: &str` to the three Sandbox-path traits**

In `crates/sandbawks-controller/src/store.rs`, update the trait method signatures (leave doc comments intact):

```rust
#[async_trait]
pub trait SandboxStore: Send + Sync {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<Sandbox>, StoreError>;
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError>;
}

#[async_trait]
pub trait SnapshotJobStore: Send + Sync {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<SnapshotJob>, StoreError>;
    async fn create(&self, namespace: &str, job: SnapshotJob) -> Result<(), StoreError>;
}

#[async_trait]
pub trait SandboxTemplateStore: Send + Sync {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<SandboxTemplate>, StoreError>;
}
```

- [ ] **Step 4: Convert the three `Kube*` impls to hold a `Client` and build `Api` per call**

Replace the `KubeSandboxStore` struct + `new` + impl in `store.rs` (mirror `KubeWarmPoolStore`):

```rust
pub struct KubeSandboxStore {
    client: kube::Client,
}

impl KubeSandboxStore {
    /// Build a store over the given client. Namespacing is per-call so a
    /// single store serves every namespace the cluster-wide watch delivers.
    pub fn new(client: kube::Client) -> Self {
        Self { client }
    }

    fn api(&self, namespace: &str) -> kube::Api<Sandbox> {
        kube::Api::namespaced(self.client.clone(), namespace)
    }
}

#[async_trait]
impl SandboxStore for KubeSandboxStore {
    #[tracing::instrument(skip_all, name = "store:controller::sandbox_get", err, fields(sandbox = %name, namespace = %namespace))]
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<Sandbox>, StoreError> {
        Ok(self.api(namespace).get_opt(name).await?)
    }

    #[tracing::instrument(
        skip_all,
        name = "store:controller::sandbox_patch_status",
        err,
        fields(sandbox = %name, namespace = %namespace, phase = ?new_status.phase)
    )]
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError> {
        let mut status_value = serde_json::to_value(&new_status)?;
        if let Some(obj) = status_value.as_object_mut() {
            for field in [
                "snapshotJobRef",
                "snapshotRef",
                "podRef",
                "envdAddress",
                "assignedNode",
                "podUid",
            ] {
                obj.entry(field).or_insert(serde_json::Value::Null);
            }
        }
        let patch = serde_json::json!({ "status": status_value });
        self.api(namespace)
            .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }
}
```

Replace `KubeSnapshotJobStore` the same way (the `create` 409-idempotency match is preserved verbatim, only `self.api(namespace)` replaces `self.api`):

```rust
pub struct KubeSnapshotJobStore {
    client: kube::Client,
}

impl KubeSnapshotJobStore {
    pub fn new(client: kube::Client) -> Self {
        Self { client }
    }

    fn api(&self, namespace: &str) -> kube::Api<SnapshotJob> {
        kube::Api::namespaced(self.client.clone(), namespace)
    }
}

#[async_trait]
impl SnapshotJobStore for KubeSnapshotJobStore {
    #[tracing::instrument(skip_all, name = "store:controller::job_get", err, fields(job = %name, namespace = %namespace))]
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<SnapshotJob>, StoreError> {
        Ok(self.api(namespace).get_opt(name).await?)
    }

    #[tracing::instrument(
        skip_all,
        name = "store:controller::job_create",
        err,
        fields(job = %job.metadata.name.as_deref().unwrap_or(""), namespace = %namespace, mode = ?job.spec.mode)
    )]
    async fn create(&self, namespace: &str, job: SnapshotJob) -> Result<(), StoreError> {
        match self.api(namespace).create(&PostParams::default(), &job).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(api)) if api.code == 409 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
```

And `KubeSandboxTemplateStore`:

```rust
pub struct KubeSandboxTemplateStore {
    client: kube::Client,
}

impl KubeSandboxTemplateStore {
    pub fn new(client: kube::Client) -> Self {
        Self { client }
    }

    fn api(&self, namespace: &str) -> kube::Api<SandboxTemplate> {
        kube::Api::namespaced(self.client.clone(), namespace)
    }
}

#[async_trait]
impl SandboxTemplateStore for KubeSandboxTemplateStore {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<SandboxTemplate>, StoreError> {
        Ok(self.api(namespace).get_opt(name).await?)
    }
}
```

- [ ] **Step 5: Update the three `Fake*` impls to accept (and record / ignore) `namespace`**

`FakeSandboxStore` records; the other two ignore (prefix `_namespace`), matching `FakeWarmPoolStore`'s existing convention:

```rust
#[async_trait]
impl SandboxStore for FakeSandboxStore {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<Sandbox>, StoreError> {
        self.seen_namespaces.lock().expect("poisoned").push(namespace.to_owned());
        Ok(self.inner.lock().expect("poisoned").get(name).cloned())
    }

    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError> {
        self.seen_namespaces.lock().expect("poisoned").push(namespace.to_owned());
        let mut guard = self.inner.lock().expect("poisoned");
        let sandbox = guard
            .get_mut(name)
            .ok_or_else(|| StoreError::NotFound(name.to_owned()))?;
        sandbox.status = Some(new_status);
        Ok(())
    }
}
```

For `FakeSnapshotJobStore::{get,create}` and `FakeSandboxTemplateStore::get`, add a leading `_namespace: &str` and leave bodies unchanged.

- [ ] **Step 6: Update the `store.rs` unit tests for the new signatures**

In the `#[cfg(test)] mod tests` of `store.rs`, the three direct calls become:

```rust
let s = store.get("default", "sbx-1").await.unwrap().expect("sbx-1 present");
// ...
assert!(store.get("default", "nope").await.unwrap().is_none());
// ...
store.patch_status("default", "sbx-1", new_status).await.unwrap();
```

- [ ] **Step 7: Thread `namespace` through `reconcile` and the store-touching helpers**

In `crates/sandbawks-controller/src/reconcile.rs`, change the entrypoint signature and add `namespace` to its instrument fields:

```rust
#[tracing::instrument(
    skip(ctx),
    name = "reconcile:controller::sandbox",
    err,
    fields(
        sandbox = %name,
        namespace = %namespace,
        otel.kind = "consumer",
        messaging.system = "kubernetes",
        messaging.operation = "process",
        messaging.destination.name = "Sandbox",
    )
)]
pub async fn reconcile(
    namespace: &str,
    name: &str,
    ctx: Arc<ReconcileContext>,
) -> Result<(), ControllerError> {
```

Update the store call sites (pass `namespace` as the new first arg):

| Line | Before | After |
|------|--------|-------|
| 367 | `ctx.store.get(name)` | `ctx.store.get(namespace, name)` |
| 491 | `ctx.store.patch_status(name, probe_status)` | `ctx.store.patch_status(namespace, name, probe_status)` |
| 498 | `ctx.store.get(name)` | `ctx.store.get(namespace, name)` |
| 794 | `ctx.store.patch_status(name, new_status.clone())` | `ctx.store.patch_status(namespace, name, new_status.clone())` |
| 827 | `ctx.store.patch_status(name, new_status)` | `ctx.store.patch_status(namespace, name, new_status)` |
| 852 | `ctx.store.patch_status(name, final_status)` | `ctx.store.patch_status(namespace, name, final_status)` |
| 911 | `ctx.snapshot_jobs.get(job_name)` | `ctx.snapshot_jobs.get(namespace, job_name)` |
| 2181 | `ctx.templates.get(template_ref)` | `ctx.templates.get(namespace, template_ref)` |
| 2579 | `ctx.snapshot_jobs.create(job)` | `ctx.snapshot_jobs.create(namespace, job)` |
| 2717 | `ctx.snapshot_jobs.create(job)` | `ctx.snapshot_jobs.create(namespace, job)` |

The helper fns that own those call sites — the ones at `reconcile.rs:899`, `:2175`, `:2458` (and any helper at `:1144,:1277,:1353,:1709` that transitively calls one of them) — each need a leading `namespace: &str` parameter, forwarded from `reconcile`. Add the parameter and pass `namespace` at each call. **Let the compiler enumerate them:** after editing the signatures, `cargo build -p sandbawks-controller` lists every site that still needs the argument.

- [ ] **Step 8: Update `reconcile_adapter` to source the namespace from the watched object**

In `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`, `reconcile_adapter` (lines 624-638):

```rust
    let name = sandbox
        .metadata
        .name
        .clone()
        .ok_or(ControllerError::Invariant("missing metadata.name"))?;
    let namespace = sandbox
        .metadata
        .namespace
        .clone()
        .ok_or(ControllerError::Invariant("missing metadata.namespace"))?;
    // ... (phase / start unchanged) ...
    let result = reconcile(&namespace, &name, ctx).await;
```

- [ ] **Step 9: Update the remaining `reconcile(...)` test call sites**

Pass a namespace as the first arg in every existing call. The controller's install-namespace default in tests is `"default"`:
- `tests/reconcile.rs`: `reconcile("default", "sbx-1", ctx(...))`, `reconcile("default", "sbx-gone", ctx(...))`.
- `tests/lifecycle.rs`: `reconcile("default", "sbx-lifecycle", ctx.clone())` (all 12 calls).
- `tests/restore_config.rs`: `reconcile("default", "sbx-cfg", ctx(...))` (both calls).
- `tests/sandbox_failure_metrics.rs`: `reconcile("default", "sbx-fail-0", ctx)`.

`cargo test -p sandbawks-controller` will flag any missed call site as a compile error.

- [ ] **Step 10: Build, lint, and run the controller tests**

Run: `cargo build -p sandbawks-controller && cargo clippy -p sandbawks-controller --all-targets -- -D warnings && cargo test -p sandbawks-controller`
Expected: PASS, including the new `reconcile_threads_object_namespace_into_store`.

- [ ] **Step 11: Commit**

```bash
git add crates/sandbawks-controller/src/store.rs crates/sandbawks-controller/src/reconcile.rs crates/sandbawks-controller/src/bin/sandbawks-controller.rs crates/sandbawks-controller/tests/
git commit -m "refactor(controller): thread namespace through Sandbox-path stores and reconcile

Make SandboxStore/SnapshotJobStore/SandboxTemplateStore namespace-per-call
(mirroring KubeWarmPoolStore) so one reconcile loop serves every namespace.
Pure refactor; behavior unchanged until the watches widen (next task).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Widen the controller watches to all namespaces

**Files:**
- Modify: `crates/sandbawks-controller/src/bin/sandbawks-controller.rs:104-112,309,347-349`

- [ ] **Step 1: Switch the six watch Apis and three store constructors**

In `sandbawks-controller.rs`, replace the namespaced constructions. Lines 104-112:

```rust
    let sandboxes: Api<Sandbox> = Api::all(client.clone());
    let snapshot_jobs_api: Api<SnapshotJob> = Api::all(client.clone());
    let store = Arc::new(KubeSandboxStore::new(client.clone()));
    let adapter: Arc<dyn RuntimeAdapter> = match cli.runtime {
        RuntimeChoice::Fake => Arc::new(FakeRuntimeAdapter::new("fake")),
        RuntimeChoice::Pod => Arc::new(PodAdapter::new(client.clone())),
    };
    let snapshot_jobs = Arc::new(KubeSnapshotJobStore::new(client.clone()));
    let templates = Arc::new(KubeSandboxTemplateStore::new(client.clone()));
```

The owned-Pod watch (line 309) and the three warm-pool Apis (lines 347-349):

```rust
    let sandbox_pods: Api<Pod> = Api::all(client.clone());
    // ...
    let warm_pools: Api<SandboxWarmPool> = Api::all(client.clone());
    let warm_members: Api<Sandbox> = Api::all(client.clone());
    let warm_templates: Api<SandboxTemplate> = Api::all(client.clone());
```

Leave the `namespace` binding (line 102) in place — it is still used for the leader-election `Lease` (lines 144, 200). Update the comment near line 102 to note it now scopes only the Lease and telemetry, not the watches.

- [ ] **Step 2: Confirm the watch mappers already carry namespace (no change needed)**

Read `sandbawks-controller.rs:316-439`. Verify the SnapshotJob mapper uses `job.namespace()` + `.within(&ns)` (line ~326), the warm-member mapper uses the Sandbox's namespace (line ~370), and the SandboxTemplate→pool mapper filters by matching namespace (line ~388). These already requeue `ObjectRef`s with the correct namespace, so a cluster-wide watch routes each event back to the right object. No edit; this step is a read-only verification.

- [ ] **Step 3: Build and lint**

Run: `cargo build -p sandbawks-controller && cargo clippy -p sandbawks-controller --all-targets -- -D warnings`
Expected: PASS (the store constructors now take one arg; the Task 1 refactor made that valid).

- [ ] **Step 4: Commit**

```bash
git add crates/sandbawks-controller/src/bin/sandbawks-controller.rs
git commit -m "feat(controller): watch all namespaces (Api::all)

Sandbox/SnapshotJob/Pod/WarmPool/member/template watches go cluster-wide so
resources in any namespace are reconciled by a single controller install.
SANDBAWKS_NAMESPACE now scopes only the leader-election Lease + telemetry.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Widen the node SnapshotJob watch to all namespaces

**Files:**
- Modify: `crates/sandbawks-node/src/reconcile.rs:252-292`

- [ ] **Step 1: Switch the watch to `Api::all`, keep the field selector**

In `crates/sandbawks-node/src/reconcile.rs`, the `run` body (the function signature and the `patch_status` impl, which already takes `namespace`, are unchanged):

```rust
    let api: Api<SnapshotJob> = Api::all(client);
    // Field-selector keeps the watch budget bounded: with N nodes and M
    // jobs cluster-wide, each node only streams its own jobs (spec.nodeName)
    // across ALL namespaces instead of all M.
    let cfg = watcher::Config::default().fields(&format!("spec.nodeName={}", ctx.node_name));

    tracing::info!(
        node = %ctx.node_name,
        install_namespace = %namespace,
        "sandbawks-node SnapshotJob controller starting (cluster-wide watch)"
    );
```

The `namespace: String` parameter is retained: it still labels the node's install namespace for telemetry via `record_watch_namespace` (line 271, unchanged) and is no longer a watch scope. The `patch_status` path already namespaces per-job from `SnapshotJob.metadata.namespace`, so cross-namespace status writes work unchanged.

- [ ] **Step 2: Build, lint, and test the node crate**

Run: `cargo build -p sandbawks-node && cargo clippy -p sandbawks-node --all-targets -- -D warnings && cargo test -p sandbawks-node`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/sandbawks-node/src/reconcile.rs
git commit -m "feat(node): watch SnapshotJobs cluster-wide (Api::all)

One node daemonset install reconciles its own jobs (spec.nodeName field
selector) across every namespace, instead of only its install namespace.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Workspace verification gate + changelog

**Files:**
- Create: `changelog/<UTC-timestamp>-issue-442-phase1-multi-namespace-control-plane.md`

- [ ] **Step 1: Run the full workspace gates**

Run: `just fmt && cargo clippy --all-targets -- -D warnings && just test`
Expected: PASS across the workspace. (Per the project's standalone-crate gotcha, `just test` is the canonical runner; do not rely on a bare `cargo test` at the root.)

- [ ] **Step 2: Write the changelog entry**

Create the file (generate the timestamp with `date -u +%Y%m%dT%H%M%SZ`):

```markdown
# Multi-namespace control plane (issue #442, Phase 1)

The controller and node daemonset now reconcile Sandbox, SnapshotJob,
SandboxTemplate, and SandboxWarmPool resources in **every** namespace from a
single install. Previously every watch was scoped to `SANDBAWKS_NAMESPACE`
(each pod's own namespace via the downward API), so a resource created in any
other namespace was never observed and silently never reconciled — the cause of
"a warm pool in another namespace does nothing".

## What changed

- The Sandbox-path stores (`SandboxStore`, `SnapshotJobStore`,
  `SandboxTemplateStore`) are now namespace-per-call, mirroring the existing
  `KubeWarmPoolStore`. `reconcile` takes the object's namespace and threads it
  to every store call.
- All controller watches and the node's SnapshotJob watch use `Api::all`. The
  node keeps its `spec.nodeName` field selector, so per-node watch budget is
  unchanged — it now spans namespaces.
- `SANDBAWKS_NAMESPACE` now scopes only the leader-election Lease and telemetry
  attributes, not the watches.

## Not changed

No RBAC, CRD, or Helm changes: the controller and node already hold cluster-wide
`ClusterRole`/`ClusterRoleBinding`. This is a code-only phase.

This is Phase 1 of the multi-namespace tenancy design. Phase 2 (TeamAPIKey
relocation + tenant RBAC provisioning) and Phase 3 (API per-tenant SA
impersonation) follow separately.
```

- [ ] **Step 3: Commit**

```bash
git add changelog/
git commit -m "docs: changelog for multi-namespace control plane (#442 Phase 1)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Notes for the implementer

- **Why a behavioral test but no kind/e2e test in this phase:** the store/reconcile refactor is provable with the namespace-recording fake (Task 1). The watch-widening (`Api::all`) is integration-level and is validated by the existing controller/node test suites still passing plus manual cluster verification; a cross-namespace kind smoke test belongs with Phase 3's impersonation validation, where the full tenant path exists.
- **The compiler is the call-site checklist.** Every signature change in Task 1 is a hard fence; trust `cargo build` over manual enumeration for transitive helper threading.
- **Do not touch `deploy/`** in this phase. If you find yourself editing a Helm template or CRD, stop — that work belongs to Phase 2/3. The spec calls for documenting the narrowed role of `SANDBAWKS_NAMESPACE` in the deployment templates too; that comment-only edit is **deliberately deferred to Phase 2**, which already modifies `deploy/helm/` and bumps `Chart.yaml`, so Phase 1 stays code-only and avoids a chart version bump for a comment. The binary/log comments updated in Tasks 2-3 carry the semantic note in the meantime.
