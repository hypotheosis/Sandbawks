//! `SandboxStore` — the trait that abstracts over `kube::Api<Sandbox>`
//! so the reconcile loop is unit-testable against an in-memory fake.
//!
//! Production wires this to [`KubeSandboxStore`], a thin wrapper over
//! `kube::Api<Sandbox>`. Tests use [`FakeSandboxStore`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use sandbawks_types::{Sandbox, SandboxStatus, SandboxTemplate, SnapshotJob};
use thiserror::Error;

/// Errors raised by the store.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A named resource was not found in the store. The name is carried
    /// in the payload for debugging but deliberately kept out of the
    /// `Display` string so observability backends group every NotFound
    /// under a single exception hash rather than one per name (#385). The
    /// name is also recorded as a structured span field at the call site.
    #[error("resource not found")]
    NotFound(String),
    /// Underlying kube client error.
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    /// Underlying serialization error when constructing patches.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Read/write the Sandboxes the controller cares about.
///
/// Namespacing is per-call: the reconcile loop passes the object's own
/// `namespace` (sourced from the cluster-wide watch) so a single store
/// serves every namespace, mirroring [`crate::warm_pool::KubeWarmPoolStore`].
#[async_trait]
pub trait SandboxStore: Send + Sync {
    /// Fetch a Sandbox by name. Returns `Ok(None)` if the Sandbox is
    /// absent so the caller can distinguish a routine deleted-while-queued
    /// race (e.g. a retired/claimed warm-pool member) from a transport
    /// error, and treat the former as a benign no-op rather than a fault
    /// (#385). Mirrors [`SnapshotJobStore::get`] / [`SandboxTemplateStore::get`].
    ///
    /// # Errors
    ///
    /// Returns a transport error from the underlying kube client.
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<Sandbox>, StoreError>;

    /// Patch only the `status` subresource of a Sandbox.
    ///
    /// # Errors
    ///
    /// Returns a transport error from the underlying kube client, or
    /// [`StoreError::NotFound`] (Fake impl) if the named Sandbox is
    /// absent.
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError>;

    /// Delete a Sandbox CR by name, cascading to its owned children
    /// (Pod, NetworkPolicy, and the retained PVCs of a `filesystem`-mode
    /// sandbox) via their ownerReferences.
    ///
    /// Used by the PVC-retention GC (epic #603 Phase 5 / issue #608) to reclaim
    /// a paused `filesystem` sandbox whose retained volumes have outlived their
    /// retention window. Deleting the owner is what frees the PVC storage:
    /// `PodAdapter::pvc_for` stamps each PVC with a controller ownerReference,
    /// so the namespace garbage collector cascade-deletes them once the Sandbox
    /// is gone.
    ///
    /// MUST treat an already-absent Sandbox as success so a retried GC pass (or
    /// a delete that raced a manual `kubectl delete`) is a benign no-op.
    ///
    /// # Errors
    ///
    /// Returns a transport error from the underlying kube client. A 404 from
    /// the apiserver is mapped to `Ok(())` rather than surfaced.
    async fn delete(&self, namespace: &str, name: &str) -> Result<(), StoreError>;
}

/// `kube::Api<Sandbox>`-backed implementation for production.
///
/// `patch_status` uses server-side apply via [`PatchParams::apply`] with
/// the field manager `sandbawks-controller`, so re-applying the same
/// patch is a no-op and conflicts surface as `kube::Error::Api` with a
/// 409 status.
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

/// `kube::Api<Sandbox>`-backed [`crate::usage_ledger::OpenIntervalSource`] (issue
/// #637).
///
/// Lists Sandboxes cluster-wide and projects each currently-`Running` one into an
/// [`crate::usage_ledger::OpenInterval`], so the usage rollup can account in-progress
/// usage up to `now` instead of only at close. A sandbox without a
/// `status.lastTransitionTime` anchor (it entered `Running` before the controller
/// began stamping it) is skipped — its interval can't be bounded.
pub struct KubeOpenIntervalSource {
    api: kube::Api<Sandbox>,
}

impl KubeOpenIntervalSource {
    /// Build a source over a cluster-wide Sandbox API (mirrors the controller's
    /// cluster-wide watch).
    pub fn new(client: kube::Client) -> Self {
        Self {
            api: kube::Api::all(client),
        }
    }
}

/// Project a `Sandbox` into an [`crate::usage_ledger::OpenInterval`] iff it is still
/// `Running` with a bounded interval start (issue #637).
///
/// `None` when the sandbox is not `Running`, or carries no parseable
/// `status.lastTransitionTime` anchor (it entered `Running` before the controller
/// began stamping it, or the value is malformed) — its interval can't be bounded.
///
/// **Every field projected here MUST match what `emit_running_usage`
/// (`reconcile.rs`) stamps on the eventual closed event** — `metadata.namespace`,
/// the [`sandbawks_types::API_KEY_NAME_ANNOTATION`], `metadata.uid`,
/// `spec.resources.{cpu,memory}`, and the `lastTransitionTime` anchor. If they ever
/// drift, the open-interval `[start, now]` and the closed event `[start, close]`
/// would compute different past-hour values and the open→close idempotency property
/// breaks. Factored out (and unit-tested) precisely to guard that contract.
fn sandbox_open_interval(sb: &Sandbox) -> Option<crate::usage_ledger::OpenInterval> {
    if sb.status.as_ref().map(|s| s.phase) != Some(sandbawks_types::SandboxPhase::Running) {
        return None;
    }
    let running_since = sb
        .status
        .as_ref()
        .and_then(|s| s.last_transition_time.as_deref())
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|d| d.with_timezone(&chrono::Utc))?;
    Some(crate::usage_ledger::OpenInterval {
        team: sb.metadata.namespace.clone().unwrap_or_default(),
        key_name: sb
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(sandbawks_types::API_KEY_NAME_ANNOTATION))
            .cloned()
            .unwrap_or_default(),
        sandbox_uid: sb.metadata.uid.clone().unwrap_or_default(),
        cpu_request: sb.spec.resources.cpu.as_str().to_owned(),
        mem_request: sb.spec.resources.memory.as_str().to_owned(),
        running_since,
    })
}

#[async_trait]
impl crate::usage_ledger::OpenIntervalSource for KubeOpenIntervalSource {
    #[tracing::instrument(skip_all, name = "store:controller::open_intervals", err)]
    async fn open_intervals(
        &self,
    ) -> Result<Vec<crate::usage_ledger::OpenInterval>, crate::usage_ledger::UsageLedgerError> {
        let list = self
            .api
            .list(&kube::api::ListParams::default())
            .await
            .map_err(|e| {
                crate::usage_ledger::UsageLedgerError::Store(format!("listing sandboxes: {e}"))
            })?;
        Ok(list
            .into_iter()
            .filter_map(|sb| sandbox_open_interval(&sb))
            .collect())
    }
}

#[async_trait]
impl SandboxStore for KubeSandboxStore {
    #[tracing::instrument(skip_all, name = "store:controller::sandbox_get", err, fields(sandbox = %name, namespace = %namespace))]
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<Sandbox>, StoreError> {
        // A missing Sandbox is `Ok(None)`, not an error: the only way to
        // get here with `None` is the apiserver confirming the object is
        // gone (a deleted-while-queued reconcile). Surfacing that as `Err`
        // would trip the `err` directive above and emit a spurious OTel
        // exception per name (#385). Transport errors still flow through
        // `?` and are still reported.
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
        // JSON merge patch on the `status` subresource. `PatchParams::default()`
        // (NOT `apply(...)`) is the right pair for `Patch::Merge` — combining
        // `apply()` with a merge body lets the first patch land but silently
        // drops subsequent patches from the same field manager.
        //
        // The status struct's `skip_serializing_if = "Option::is_none"`
        // attributes collude with merge-patch semantics to silently swallow
        // field clearing: an absent field means "leave alone" per RFC 7396,
        // so setting `new_status.snapshot_job_ref = None` and patching
        // leaves the on-wire value at whatever it was. We re-serialize and
        // explicitly inject `null` for the fields the controller is allowed
        // to clear/replace mid-lifecycle, so the merge patch carries an
        // unambiguous intent.
        //
        // Without this fix, observe_snapshot_job re-fires `SnapshotValidated`
        // against the stale Complete job on every reconcile after pause and
        // the sandbox never advances past Paused — caught by the
        // pause/resume smoke step on PR #169.
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

    #[tracing::instrument(skip_all, name = "store:controller::sandbox_delete", err, fields(sandbox = %name, namespace = %namespace))]
    async fn delete(&self, namespace: &str, name: &str) -> Result<(), StoreError> {
        // Background propagation: the apiserver returns immediately and its
        // garbage collector reaps the owned children (Pod, NetworkPolicy, and
        // the retained PVCs) asynchronously via their ownerReferences — the same
        // propagation the warm-pool member reaper uses. The reconcile only needs
        // the owning Sandbox CR gone.
        match self
            .api(namespace)
            .delete(name, &DeleteParams::background())
            .await
        {
            Ok(_) => Ok(()),
            // A Sandbox that vanished between the GC decision and this call
            // (retry, or a racing manual delete) is success, not a fault — and
            // mapping 404 to Ok keeps the `err` directive above from emitting a
            // spurious exception per reclaimed name (mirrors `get`'s rationale).
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// In-memory store for tests.
///
/// Wraps an `Arc<Mutex<HashMap<String, Sandbox>>>` so it is cheap to
/// clone across reconcile contexts and safe to share between the test
/// driver and the reconcile loop.
#[derive(Default, Clone)]
pub struct FakeSandboxStore {
    inner: Arc<Mutex<HashMap<String, Sandbox>>>,
    /// Namespaces passed to `get` / `patch_status`, in call order. Lets
    /// tests assert the reconcile loop threads the object's own namespace
    /// through the store (multi-namespace support, #582).
    pub seen_namespaces: Arc<Mutex<Vec<String>>>,
    /// Names passed to `delete`, in call order. Lets tests assert the
    /// PVC-retention GC reclaimed the expected Sandbox (issue #608).
    pub deleted: Arc<Mutex<Vec<String>>>,
}

impl FakeSandboxStore {
    /// Construct an empty fake store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-populate the store with a Sandbox.
    ///
    /// # Panics
    ///
    /// Panics if the Sandbox has no `metadata.name`. Tests are expected
    /// to construct Sandboxes via `Sandbox::new(name, spec)` which sets
    /// the name; this assertion catches obvious test-fixture bugs.
    pub fn insert(&self, sandbox: Sandbox) {
        let name = sandbox
            .metadata
            .name
            .clone()
            .expect("test sandbox needs metadata.name");
        self.inner.lock().expect("poisoned").insert(name, sandbox);
    }

    /// Read the current state of a Sandbox by name without going
    /// through the async trait method. Handy in tests for asserting on
    /// the post-reconcile status without an `.await`.
    pub fn peek(&self, name: &str) -> Option<Sandbox> {
        self.inner.lock().expect("poisoned").get(name).cloned()
    }
}

#[async_trait]
impl SandboxStore for FakeSandboxStore {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<Sandbox>, StoreError> {
        self.seen_namespaces
            .lock()
            .expect("poisoned")
            .push(namespace.to_owned());
        Ok(self.inner.lock().expect("poisoned").get(name).cloned())
    }

    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError> {
        self.seen_namespaces
            .lock()
            .expect("poisoned")
            .push(namespace.to_owned());
        let mut guard = self.inner.lock().expect("poisoned");
        let sandbox = guard
            .get_mut(name)
            .ok_or_else(|| StoreError::NotFound(name.to_owned()))?;
        sandbox.status = Some(new_status);
        Ok(())
    }

    async fn delete(&self, namespace: &str, name: &str) -> Result<(), StoreError> {
        self.seen_namespaces
            .lock()
            .expect("poisoned")
            .push(namespace.to_owned());
        self.deleted.lock().expect("poisoned").push(name.to_owned());
        // Idempotent like the kube impl: removing an absent Sandbox is success.
        self.inner.lock().expect("poisoned").remove(name);
        Ok(())
    }
}

/// Read/write the `SnapshotJob` CRs the controller dispatches per
/// pause/resume cycle.
///
/// The controller (Plan 05 T4) `create()`s a `SnapshotJob` per request
/// then polls `get()` on subsequent reconciles until the job lands in a
/// terminal phase. Production wires this to [`KubeSnapshotJobStore`];
/// tests use [`FakeSnapshotJobStore`] which lets the test driver mutate
/// the job's `status` to simulate per-node worker progress.
#[async_trait]
pub trait SnapshotJobStore: Send + Sync {
    /// Fetch a `SnapshotJob` by name. Returns `Ok(None)` if absent so
    /// callers can distinguish "job was created in a past reconcile but
    /// has since been GC'd" from a transport error.
    ///
    /// # Errors
    ///
    /// Returns a transport error from the underlying kube client.
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<SnapshotJob>, StoreError>;

    /// Create a new `SnapshotJob`. The controller picks the name
    /// deterministically per pause/resume cycle.
    ///
    /// # Errors
    ///
    /// Returns a transport error from the underlying kube client, or
    /// `StoreError::AlreadyExists` if a job with the same name already
    /// exists (idempotent retry path — callers treat this as success).
    async fn create(&self, namespace: &str, job: SnapshotJob) -> Result<(), StoreError>;
}

/// `kube::Api<SnapshotJob>`-backed implementation for production.
pub struct KubeSnapshotJobStore {
    client: kube::Client,
}

impl KubeSnapshotJobStore {
    /// Build a store over the given client. Namespacing is per-call so a
    /// single store serves every namespace the cluster-wide watch delivers.
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
        // `create` is idempotent at the controller level — a 409
        // AlreadyExists from a re-fired reconcile is the expected
        // outcome when `status.snapshot_job_ref` was patched but the
        // apiserver watch hasn't refreshed our view yet. Treat it as
        // success so the caller can converge.
        match self
            .api(namespace)
            .create(&PostParams::default(), &job)
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(api)) if api.code == 409 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// In-memory `SnapshotJob` store for tests.
///
/// Cloning is cheap (`Arc<Mutex<_>>`-backed) so tests can share the
/// store between the reconcile context and assertion code without
/// re-wrapping.
#[derive(Default, Clone)]
pub struct FakeSnapshotJobStore {
    inner: Arc<Mutex<HashMap<String, SnapshotJob>>>,
    /// Namespaces passed to `get` / `create`, in call order. Lets tests
    /// assert the dispatch path threads the object's own namespace into the
    /// snapshot-job store (multi-namespace support, #582).
    pub seen_namespaces: Arc<Mutex<Vec<String>>>,
}

impl FakeSnapshotJobStore {
    /// Construct an empty fake store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-populate the store with a `SnapshotJob`. Used by tests that
    /// want to simulate a per-node worker having already advanced a job
    /// to a particular `status.phase`.
    ///
    /// # Panics
    ///
    /// Panics if the job has no `metadata.name`. Test fixtures are
    /// expected to construct jobs via `SnapshotJob::new(name, spec)`
    /// which sets the name; this catches obvious fixture bugs.
    pub fn insert(&self, job: SnapshotJob) {
        let name = job
            .metadata
            .name
            .clone()
            .expect("test snapshot job needs metadata.name");
        self.inner.lock().expect("poisoned").insert(name, job);
    }

    /// Read the current state of a `SnapshotJob` by name without going
    /// through the async trait method.
    pub fn peek(&self, name: &str) -> Option<SnapshotJob> {
        self.inner.lock().expect("poisoned").get(name).cloned()
    }

    /// Snapshot every job currently in the store.
    pub fn list(&self) -> Vec<SnapshotJob> {
        self.inner
            .lock()
            .expect("poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Mutate a job's status in-place. Used by tests to simulate the
    /// per-node worker writing back `phase=Complete` etc.
    ///
    /// # Panics
    ///
    /// Panics if the named job is not in the store.
    pub fn set_status(&self, name: &str, status: sandbawks_types::SnapshotJobStatus) {
        let mut guard = self.inner.lock().expect("poisoned");
        let job = guard
            .get_mut(name)
            .expect("test set_status called on missing job");
        job.status = Some(status);
    }
}

#[async_trait]
impl SnapshotJobStore for FakeSnapshotJobStore {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<SnapshotJob>, StoreError> {
        self.seen_namespaces
            .lock()
            .expect("poisoned")
            .push(namespace.to_owned());
        Ok(self.inner.lock().expect("poisoned").get(name).cloned())
    }

    async fn create(&self, namespace: &str, job: SnapshotJob) -> Result<(), StoreError> {
        self.seen_namespaces
            .lock()
            .expect("poisoned")
            .push(namespace.to_owned());
        let name = job
            .metadata
            .name
            .clone()
            .ok_or_else(|| StoreError::NotFound("snapshot job missing metadata.name".into()))?;
        // Mirror the kube apiserver: a re-created CR with the same name
        // is treated as idempotent so the caller doesn't have to special
        // case retry.
        self.inner
            .lock()
            .expect("poisoned")
            .entry(name)
            .or_insert(job);
        Ok(())
    }
}

/// Read the `SandboxTemplate` a Sandbox references via
/// `spec.template_ref`.
///
/// The controller resolves the template on the `Action::CreatePod` /
/// `Action::CreateRestorePod` edges to pick the Pod's workload image
/// (`spec.image`). Abstracted as a trait so the reconcile loop is
/// unit-testable against an in-memory fake, mirroring [`SnapshotJobStore`].
#[async_trait]
pub trait SandboxTemplateStore: Send + Sync {
    /// Fetch a `SandboxTemplate` by name. Returns `Ok(None)` if absent
    /// so the caller can fail the Sandbox with a clear "template not
    /// found" error rather than silently cold-booting a default image.
    ///
    /// # Errors
    ///
    /// Returns a transport error from the underlying kube client.
    async fn get(&self, namespace: &str, name: &str)
        -> Result<Option<SandboxTemplate>, StoreError>;
}

/// `kube::Api<SandboxTemplate>`-backed implementation for production.
pub struct KubeSandboxTemplateStore {
    client: kube::Client,
}

impl KubeSandboxTemplateStore {
    /// Build a store over the given client. Namespacing is per-call so a
    /// single store serves every namespace the cluster-wide watch delivers.
    pub fn new(client: kube::Client) -> Self {
        Self { client }
    }

    fn api(&self, namespace: &str) -> kube::Api<SandboxTemplate> {
        kube::Api::namespaced(self.client.clone(), namespace)
    }
}

#[async_trait]
impl SandboxTemplateStore for KubeSandboxTemplateStore {
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<SandboxTemplate>, StoreError> {
        Ok(self.api(namespace).get_opt(name).await?)
    }
}

/// In-memory `SandboxTemplate` store for tests.
///
/// **Lenient by default:** [`Self::get`] returns a canned default
/// template for any name not explicitly registered, so the many
/// reconcile tests that drive `Action::CreatePod` don't each have to
/// register a template. Tests that need to exercise the missing-template
/// error path construct one with [`Self::strict`], where unregistered
/// names resolve to `None`.
///
/// Cloning is cheap (`Arc<Mutex<_>>`-backed) so tests can share the
/// store between the reconcile context and assertion code.
#[derive(Clone)]
pub struct FakeSandboxTemplateStore {
    inner: Arc<Mutex<HashMap<String, SandboxTemplate>>>,
    /// When `true`, unregistered names resolve to `None` instead of the
    /// canned default — used to simulate a missing template.
    strict: bool,
    /// Namespaces passed to `get`, in call order. Lets tests assert the
    /// cold-start path threads the object's own namespace into the template
    /// store (multi-namespace support, #582).
    pub seen_namespaces: Arc<Mutex<Vec<String>>>,
}

impl Default for FakeSandboxTemplateStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            strict: false,
            seen_namespaces: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl FakeSandboxTemplateStore {
    /// Construct a lenient fake (unregistered names get a canned default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a strict fake (unregistered names resolve to `None`).
    pub fn strict() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            strict: true,
            seen_namespaces: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a template under `name`, overriding the canned default.
    ///
    /// # Panics
    ///
    /// Panics if the template has no `metadata.name`.
    pub fn insert(&self, template: SandboxTemplate) {
        let name = template
            .metadata
            .name
            .clone()
            .expect("test sandbox template needs metadata.name");
        self.inner.lock().expect("poisoned").insert(name, template);
    }

    /// The canned default template handed back for unregistered names in
    /// lenient mode. Carries a placeholder workload image so `pod_spec_for`
    /// produces a valid Pod; `envd_image` is left `None` because node-staged
    /// envd delivery makes it a no-op on cold-start/restore (issue #273).
    fn default_template() -> SandboxTemplate {
        use sandbawks_types::{ResourceRequests, SandboxTemplateSpec};
        SandboxTemplate::new(
            "default",
            SandboxTemplateSpec {
                image: "ghcr.io/hypotheosis/test-workload:latest".into(),
                default_runtime_class: String::new(),
                default_resources: ResourceRequests {
                    cpu: "1".parse().expect("valid cpu quantity"),
                    memory: "1Gi".parse().expect("valid memory quantity"),
                    ephemeral_storage: None,
                },
                envd_image: None,
                default_timeout_seconds: 60,
                network_policy: None,
                tolerations: None,
                node_selector: None,
                security_context: None,
                persistence_mode: None,
                volume_claim_templates: None,
                volume_claim_templates_policy: None,
            },
        )
    }
}

#[async_trait]
impl SandboxTemplateStore for FakeSandboxTemplateStore {
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<SandboxTemplate>, StoreError> {
        self.seen_namespaces
            .lock()
            .expect("poisoned")
            .push(namespace.to_owned());
        if let Some(t) = self.inner.lock().expect("poisoned").get(name).cloned() {
            return Ok(Some(t));
        }
        if self.strict {
            Ok(None)
        } else {
            Ok(Some(Self::default_template()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sandbawks_types::{ResourceRequests, SandboxPhase, SandboxSpec};

    fn sandbox(name: &str) -> Sandbox {
        Sandbox::new(
            name,
            SandboxSpec {
                template_ref: "tpl".into(),
                runtime_class_name: None,
                resources: ResourceRequests {
                    cpu: "1".parse().unwrap(),
                    memory: "1Gi".parse().unwrap(),
                    ephemeral_storage: None,
                },
                timeout_seconds: 60,
                auto_pause: false,
                desired_phase: None,
                env: Default::default(),
                metadata: Default::default(),
                network_policy: None,
                snapshot_retention: None,
                pvc_retention: None,
                tolerations: None,
                node_selector: None,
                security_context: None,
                persistence_mode: None,
                volume_claim_templates: None,
                restore_from: None,
            },
        )
    }

    /// A `Running` sandbox with all fields set projects every one onto the
    /// `OpenInterval` — the mapping that must mirror `emit_running_usage` (#637).
    #[test]
    fn sandbox_open_interval_projects_a_running_sandbox() {
        let mut sb = sandbox("sbx-1");
        sb.metadata.namespace = Some("team-acme".into());
        sb.metadata.uid = Some("uid-1".into());
        sb.metadata.annotations = Some(std::collections::BTreeMap::from([(
            sandbawks_types::API_KEY_NAME_ANNOTATION.to_owned(),
            "key-1".to_owned(),
        )]));
        sb.spec.resources.cpu = "2".parse().unwrap();
        sb.spec.resources.memory = "4Gi".parse().unwrap();
        sb.status = Some(sandbawks_types::SandboxStatus {
            phase: SandboxPhase::Running,
            last_transition_time: Some("2026-06-21T10:00:00Z".into()),
            ..Default::default()
        });

        let oi = sandbox_open_interval(&sb).expect("running + anchor projects");
        assert_eq!(oi.team, "team-acme");
        assert_eq!(oi.key_name, "key-1");
        assert_eq!(oi.sandbox_uid, "uid-1");
        assert_eq!(oi.cpu_request, "2");
        assert_eq!(oi.mem_request, "4Gi");
        assert_eq!(
            oi.running_since,
            chrono::DateTime::parse_from_rfc3339("2026-06-21T10:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc)
        );
    }

    #[test]
    fn sandbox_open_interval_skips_non_running() {
        let mut paused = sandbox("sbx-1");
        paused.status = Some(sandbawks_types::SandboxStatus {
            phase: SandboxPhase::Paused,
            last_transition_time: Some("2026-06-21T10:00:00Z".into()),
            ..Default::default()
        });
        assert!(
            sandbox_open_interval(&paused).is_none(),
            "paused → no open interval"
        );

        let mut no_status = sandbox("sbx-2");
        no_status.status = None;
        assert!(
            sandbox_open_interval(&no_status).is_none(),
            "no status → None"
        );
    }

    #[test]
    fn sandbox_open_interval_skips_running_without_a_bounded_anchor() {
        // Running but no `lastTransitionTime` (entered Running before stamping).
        let mut no_anchor = sandbox("sbx-1");
        no_anchor.status = Some(sandbawks_types::SandboxStatus {
            phase: SandboxPhase::Running,
            last_transition_time: None,
            ..Default::default()
        });
        assert!(
            sandbox_open_interval(&no_anchor).is_none(),
            "no anchor → None"
        );

        // Running with an unparseable anchor.
        let mut bad_anchor = sandbox("sbx-2");
        bad_anchor.status = Some(sandbawks_types::SandboxStatus {
            phase: SandboxPhase::Running,
            last_transition_time: Some("not-a-timestamp".into()),
            ..Default::default()
        });
        assert!(
            sandbox_open_interval(&bad_anchor).is_none(),
            "unparseable anchor → None"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fake_get_returns_inserted_sandbox() {
        let store = FakeSandboxStore::new();
        store.insert(sandbox("sbx-1"));
        let s = store
            .get("default", "sbx-1")
            .await
            .unwrap()
            .expect("sbx-1 present");
        assert_eq!(s.metadata.name.as_deref(), Some("sbx-1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fake_get_missing_returns_none() {
        // A missing Sandbox is `Ok(None)`, not an error — this is what lets
        // the reconcile loop treat a deleted-while-queued object as a benign
        // no-op instead of emitting a spurious exception (#385).
        let store = FakeSandboxStore::new();
        assert!(store.get("default", "nope").await.unwrap().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fake_patch_status_updates_in_place() {
        let store = FakeSandboxStore::new();
        store.insert(sandbox("sbx-1"));
        // Struct-update form (vs. mut + field reassign) keeps clippy
        // happy under `-D warnings` once the lib build is unblocked
        // by T7.
        let new_status = SandboxStatus {
            phase: SandboxPhase::Running,
            ..Default::default()
        };
        store
            .patch_status("default", "sbx-1", new_status)
            .await
            .unwrap();
        let s = store.peek("sbx-1").unwrap();
        assert_eq!(s.status.unwrap().phase, SandboxPhase::Running);
    }
}
