//! Shared test doubles for the node-side `SnapshotJob` reconciler tests.
//!
//! These helpers are consumed by more than one integration-test binary
//! (`reconcile.rs` and `reconcile_span_emission.rs`), so they live in a
//! `common/mod.rs` module rather than being inlined into a single test file.
//!
//! `reconcile_span_emission.rs` is deliberately a *separate* binary: it
//! asserts that `#[tracing::instrument]` opens the `reconcile:node::snapshot_job`
//! span, and `tracing`'s callsite-interest cache is process-global. Keeping the
//! span-emission tests in their own process means no sibling `#[tokio::test]`
//! running `reconcile(...)` under the global `NoSubscriber` can register that
//! callsite as `Interest::never()` *before* the recording subscriber is
//! installed — which was the root cause of the flake in issue #391.
//!
//! Each integration-test binary uses a different subset of these helpers, so a
//! crate-wide `#![allow(dead_code)]` keeps `-D warnings` green without forcing
//! every binary to touch every helper. This is the standard idiom for a
//! `tests/common` support module.
#![allow(dead_code)]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kube::api::ObjectMeta;
use sandbawks_node::reconcile::{NodeReconcileError, ReconcileContext, SnapshotJobStatusStore};
use sandbawks_types::runtime::{RuntimeAdapter, RuntimeError, SnapshotManifest};
use sandbawks_types::snapshot::{SnapshotPart, SnapshotSink, SnapshotSource};
use sandbawks_types::testing::FakeSnapshotStore;
use sandbawks_types::{SnapshotJob, SnapshotJobMode, SnapshotJobSpec, SnapshotJobStatus};

pub const NAMESPACE: &str = "default";
pub const NODE_NAME: &str = "node-a";
pub const SANDBOX_NAME: &str = "sbx-abc";
pub const CONTAINER_ID: &str = "containerd://deadbeef";

/// Record of a single status patch emitted by the reconciler.
///
/// Tests inspect the recorded sequence to verify the in-flight → terminal
/// phase transition lands as expected. Storing the whole
/// [`SnapshotJobStatus`] (rather than just the phase) lets us assert on
/// the `message`/`manifest`/`completed_at` payload too. Fields are `pub`
/// so the assertions in the sibling test binaries can read them directly.
#[derive(Debug, Clone)]
pub struct PatchRecord {
    pub namespace: String,
    pub name: String,
    pub status: SnapshotJobStatus,
}

/// In-memory [`SnapshotJobStatusStore`] that records every patch the
/// reconciler emits, in order. Cloning is cheap (`Arc<Mutex<_>>`-backed)
/// so the test can share one store between the reconcile context and the
/// assertion code without re-wrapping.
#[derive(Default, Clone)]
pub struct FakeSnapshotJobStatusStore {
    patches: Arc<Mutex<Vec<PatchRecord>>>,
}

impl FakeSnapshotJobStatusStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the recorded patches. Cloning out of the mutex means we
    /// don't keep the lock held across `.expect` assertions.
    pub fn recorded(&self) -> Vec<PatchRecord> {
        self.patches.lock().expect("poisoned").clone()
    }
}

#[async_trait]
impl SnapshotJobStatusStore for FakeSnapshotJobStatusStore {
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        new_status: SnapshotJobStatus,
    ) -> Result<(), NodeReconcileError> {
        self.patches.lock().expect("poisoned").push(PatchRecord {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            status: new_status,
        });
        Ok(())
    }
}

/// Recording [`RuntimeAdapter`]. `outcome` controls whether the call
/// returns `Ok` (carrying a canned manifest for `pause` / `()` for
/// `resume`) or `Err` — the failure-path test wires `outcome=Err`.
#[derive(Clone)]
pub struct FakeRuntimeAdapter {
    inner: Arc<Mutex<FakeAdapterInner>>,
}

#[derive(Default)]
struct FakeAdapterInner {
    pause_calls: Vec<String>,
    resume_calls: Vec<String>,
    stage_calls: Vec<String>,
    // `target_path` passed alongside each `stage_snapshot` call (epic #484
    // / issue #487): `None` for legacy callers, `Some(path)` for
    // scheduler-placed restore. Parallel to `stage_calls`.
    stage_target_paths: Vec<Option<String>>,
    // sandbox_id of each `validate_restorable` call (issue #235).
    validate_calls: Vec<String>,
    pause_outcome: Option<Result<SnapshotManifest, RuntimeError>>,
    resume_outcome: Option<Result<(), RuntimeError>>,
    stage_outcome: Option<Result<String, RuntimeError>>,
    validate_outcome: Option<Result<(), RuntimeError>>,
}

impl FakeRuntimeAdapter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeAdapterInner::default())),
        }
    }

    pub fn with_pause_outcome(self, outcome: Result<SnapshotManifest, RuntimeError>) -> Self {
        self.inner.lock().expect("poisoned").pause_outcome = Some(outcome);
        self
    }

    pub fn with_resume_outcome(self, outcome: Result<(), RuntimeError>) -> Self {
        self.inner.lock().expect("poisoned").resume_outcome = Some(outcome);
        self
    }

    pub fn with_stage_outcome(self, outcome: Result<String, RuntimeError>) -> Self {
        self.inner.lock().expect("poisoned").stage_outcome = Some(outcome);
        self
    }

    pub fn with_validate_outcome(self, outcome: Result<(), RuntimeError>) -> Self {
        self.inner.lock().expect("poisoned").validate_outcome = Some(outcome);
        self
    }

    pub fn pause_calls(&self) -> Vec<String> {
        self.inner.lock().expect("poisoned").pause_calls.clone()
    }

    pub fn resume_calls(&self) -> Vec<String> {
        self.inner.lock().expect("poisoned").resume_calls.clone()
    }

    pub fn stage_calls(&self) -> Vec<String> {
        self.inner.lock().expect("poisoned").stage_calls.clone()
    }

    /// `target_path` recorded for each `stage_snapshot` call, in order
    /// (epic #484 / issue #487).
    pub fn stage_target_paths(&self) -> Vec<Option<String>> {
        self.inner
            .lock()
            .expect("poisoned")
            .stage_target_paths
            .clone()
    }

    pub fn validate_calls(&self) -> Vec<String> {
        self.inner.lock().expect("poisoned").validate_calls.clone()
    }
}

#[async_trait]
impl RuntimeAdapter for FakeRuntimeAdapter {
    fn runtime_class(&self) -> &str {
        "gvisor"
    }

    async fn pause(
        &self,
        pod_uid: &str,
        _sink: &dyn SnapshotSink,
    ) -> Result<SnapshotManifest, RuntimeError> {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.pause_calls.push(pod_uid.to_owned());
        inner
            .pause_outcome
            .take()
            .unwrap_or_else(|| Ok(default_manifest(pod_uid)))
    }

    async fn resume(
        &self,
        _manifest: &SnapshotManifest,
        pod_uid: &str,
        _source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.resume_calls.push(pod_uid.to_owned());
        inner.resume_outcome.take().unwrap_or(Ok(()))
    }

    async fn stage_snapshot(
        &self,
        sandbox_id: &str,
        _manifest: &SnapshotManifest,
        _source: &dyn SnapshotSource,
        target_path: Option<&str>,
    ) -> Result<String, RuntimeError> {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.stage_calls.push(sandbox_id.to_owned());
        inner
            .stage_target_paths
            .push(target_path.map(ToOwned::to_owned));
        inner
            .stage_outcome
            .take()
            .unwrap_or_else(|| Ok(format!("/var/lib/sandbawks/staging/{sandbox_id}/job-0")))
    }

    async fn validate_restorable(
        &self,
        manifest: &SnapshotManifest,
        _source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.validate_calls.push(manifest.sandbox_id.clone());
        // Default to Ok so the existing pause happy-path tests keep
        // round-tripping; the fail-closed test injects an Err.
        inner.validate_outcome.take().unwrap_or(Ok(()))
    }
}

pub fn default_manifest(sandbox_id: &str) -> SnapshotManifest {
    let part = SnapshotPart::from_bytes("memory.zst", b"fake-memory-bytes");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: sandbox_id.to_owned(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: "2026-05-25T12:00:00+00:00".into(),
        parts: vec![part],
        snapshot_ref: "s3://fake/manifest.json".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    manifest
}

pub fn make_job(
    name: &str,
    mode: SnapshotJobMode,
    snapshot_ref: Option<String>,
    status: Option<SnapshotJobStatus>,
) -> SnapshotJob {
    SnapshotJob {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(NAMESPACE.to_owned()),
            ..Default::default()
        },
        spec: SnapshotJobSpec {
            sandbox_name: SANDBOX_NAME.to_owned(),
            node_name: NODE_NAME.to_owned(),
            mode,
            container_id: CONTAINER_ID.to_owned(),
            snapshot_ref,
            expected_sha256: None,
            expected_envd_sha256: None,
            target_path: None,
        },
        status,
    }
}

pub fn make_ctx(
    adapter: Arc<FakeRuntimeAdapter>,
) -> (Arc<ReconcileContext>, FakeSnapshotJobStatusStore) {
    make_ctx_with_flag(adapter, Arc::new(AtomicBool::new(false)))
}

/// Like [`make_ctx`] but with a caller-supplied `runsc_root_failed` flag so
/// tests can inspect or pre-set the gate (#379).
pub fn make_ctx_with_flag(
    adapter: Arc<FakeRuntimeAdapter>,
    runsc_root_failed: Arc<AtomicBool>,
) -> (Arc<ReconcileContext>, FakeSnapshotJobStatusStore) {
    let store = FakeSnapshotJobStatusStore::new();
    let snapshot_store = FakeSnapshotStore::new();
    let sink: Arc<dyn SnapshotSink> = Arc::new(snapshot_store.clone());
    let source: Arc<dyn SnapshotSource> = Arc::new(snapshot_store);
    let ctx = Arc::new(ReconcileContext {
        node_name: NODE_NAME.to_owned(),
        adapter,
        sink,
        source,
        status_store: Arc::new(store.clone()),
        runsc_root_failed,
    });
    (ctx, store)
}
