//! A no-op `RuntimeAdapter` impl used by the controller's unit tests.
//!
//! After Plan 05 T4, the controller no longer calls `pause()` / `resume()`
//! directly — that work is offloaded to per-node `sandbawks-node`
//! workers via the `SnapshotJob` CRD. `FakeRuntimeAdapter` therefore
//! only needs to stand in for the Pod-lifecycle methods (`create_pod`,
//! `delete_pod`, `pod_ready`) and `runtime_class`. The `pause` and
//! `resume` trait methods fall through to the default impls in
//! [`sandbawks_types::RuntimeAdapter`] — sufficient because no test
//! exercises them.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use sandbawks_types::{
    PodReadiness, PodRef, PodTerminationCause, RuntimeAdapter, RuntimeError, Sandbox,
};

/// A test double for [`RuntimeAdapter`] whose behavior is configured via the
/// `with_*` builders. Most methods return canned values; a couple record their
/// invocations for assertions (e.g. [`Self::ensure_egress_calls`]).
///
/// Cloning the adapter is **not** supported — tests should wrap it in
/// `Arc<FakeRuntimeAdapter>` and share that.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use sandbawks_controller::FakeRuntimeAdapter;
/// use sandbawks_types::RuntimeAdapter;
///
/// let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
/// assert_eq!(adapter.runtime_class(), "fake");
/// ```
pub struct FakeRuntimeAdapter {
    runtime_class: String,
    /// Cause returned by [`RuntimeAdapter::pod_terminated`]. `None` means the
    /// Pod is still live (the default); `Some(cause)` simulates a terminated
    /// Pod with the given cause. Set via [`FakeRuntimeAdapter::with_pod_terminated`]
    /// or [`FakeRuntimeAdapter::with_pod_evicted`].
    pod_terminated: Option<PodTerminationCause>,
    /// Node returned by [`RuntimeAdapter::pod_scheduled_node`] (epic #484
    /// scheduler-placed restore discovery). `None` (the default) models a
    /// Pod the scheduler has not yet bound to a node; `Some(node)` lets a
    /// resume test drive the `(Resuming, NodeAssigned)` edge. Set via
    /// [`FakeRuntimeAdapter::with_scheduled_node`].
    scheduled_node: Option<String>,
    /// When `true`, [`RuntimeAdapter::pod_scheduled_node`] reports
    /// [`Self::scheduled_node`] on its **first** call and `None` on every
    /// call after — modelling a `spec.nodeName` that is briefly visible to
    /// node-discovery and then momentarily unreadable on the immediate
    /// re-read in `Action::StageSnapshot`. Set via
    /// [`FakeRuntimeAdapter::with_vanishing_scheduled_node`] to exercise the
    /// "re-read returns `None` ⇒ requeue, not fatal" path. The latch is
    /// [`Self::scheduled_node_seen`].
    scheduled_node_vanishes: bool,
    /// Latch backing [`Self::scheduled_node_vanishes`]: `false` until the
    /// first `pod_scheduled_node` call consumes it.
    scheduled_node_seen: AtomicBool,
    /// Whether [`RuntimeAdapter::pod_ready`] reports the Pod Ready.
    ///
    /// Interior-mutable so a multi-pass resume test can hold the restore
    /// Pod un-Ready across the schedule→stage window (modelling the
    /// stage-gate init container that blocks the sandbox container until
    /// the snapshot is staged) and then flip it Ready via
    /// [`FakeRuntimeAdapter::set_pod_ready`]. Defaults to `true` so existing
    /// `Creating → Running` tests, which expect an immediately-Ready Pod,
    /// are unaffected.
    pod_ready: AtomicBool,
    /// Whether [`RuntimeAdapter::envd_serving`] reports envd as serving
    /// `/__state` (issue #560). Interior-mutable so a resume test can model a
    /// restore Pod that is kubelet-Ready but checkpoint-wedged (TCP-accepting,
    /// not answering) by holding this `false` while `pod_ready` is `true`.
    /// Defaults to `true` so existing tests, which expect a healthy envd, are
    /// unaffected.
    envd_serving: AtomicBool,
    /// Number of times [`RuntimeAdapter::ensure_egress_policy`] has been called
    /// (issue #542). Lets a reconcile test assert the egress-sync hook fires
    /// exactly when expected (Running + a `spec.networkPolicy`) and is otherwise
    /// skipped. Read via [`FakeRuntimeAdapter::ensure_egress_calls`].
    ensure_egress_calls: AtomicUsize,
    /// When `true`, [`RuntimeAdapter::ensure_egress_policy`] returns an `Err`
    /// (issue #542). Lets a reconcile test prove the egress sync is *best-effort*
    /// — a failing hook is logged and the lifecycle reconcile still proceeds.
    /// Set via [`FakeRuntimeAdapter::with_ensure_egress_error`].
    ensure_egress_fails: bool,
}

impl FakeRuntimeAdapter {
    /// Construct a new adapter bound to a synthetic runtime class.
    ///
    /// The `runtime_class` is echoed back by [`RuntimeAdapter::runtime_class`].
    pub fn new(runtime_class: impl Into<String>) -> Self {
        Self {
            runtime_class: runtime_class.into(),
            pod_terminated: None,
            scheduled_node: None,
            scheduled_node_vanishes: false,
            scheduled_node_seen: AtomicBool::new(false),
            pod_ready: AtomicBool::new(true),
            envd_serving: AtomicBool::new(true),
            ensure_egress_calls: AtomicUsize::new(0),
            ensure_egress_fails: false,
        }
    }

    /// How many times [`RuntimeAdapter::ensure_egress_policy`] has been invoked
    /// on this adapter (issue #542).
    pub fn ensure_egress_calls(&self) -> usize {
        self.ensure_egress_calls.load(Ordering::SeqCst)
    }

    /// Make [`RuntimeAdapter::ensure_egress_policy`] return an `Err` (issue
    /// #542), so a reconcile test can prove the best-effort contract: a failing
    /// egress sync must not abort the lifecycle reconcile.
    #[must_use]
    pub fn with_ensure_egress_error(mut self) -> Self {
        self.ensure_egress_fails = true;
        self
    }

    /// Make [`RuntimeAdapter::pod_scheduled_node`] report the restore Pod as
    /// bound to `node` (epic #484). Without this the adapter reports the Pod
    /// as not-yet-scheduled, so `resuming_schedule_trigger` idles.
    #[must_use]
    pub fn with_scheduled_node(mut self, node: impl Into<String>) -> Self {
        self.scheduled_node = Some(node.into());
        self
    }

    /// Make [`RuntimeAdapter::pod_scheduled_node`] report `node` on its first
    /// call and `None` thereafter.
    ///
    /// Models the narrow window where node-discovery sees `spec.nodeName`
    /// (emitting `NodeAssigned`) but the immediate re-read inside
    /// `Action::StageSnapshot` does not — used to prove that re-read returns
    /// `None` ⇒ the Sandbox requeues in `Resuming` rather than folding to
    /// `Failed`.
    #[must_use]
    pub fn with_vanishing_scheduled_node(mut self, node: impl Into<String>) -> Self {
        self.scheduled_node = Some(node.into());
        self.scheduled_node_vanishes = true;
        self
    }

    /// Set the initial readiness [`RuntimeAdapter::pod_ready`] reports.
    ///
    /// A resume test starts the restore Pod un-Ready (`false`) so the
    /// reconcile loop reaches the node-discovery / staging steps instead of
    /// promoting straight to `Running`, then flips it with
    /// [`set_pod_ready`](Self::set_pod_ready) once the snapshot is staged.
    #[must_use]
    pub fn with_pod_ready(self, ready: bool) -> Self {
        self.pod_ready.store(ready, Ordering::SeqCst);
        self
    }

    /// Flip the readiness [`RuntimeAdapter::pod_ready`] reports mid-test.
    ///
    /// Takes `&self` (not `self`) so a test holding `Arc<FakeRuntimeAdapter>`
    /// can change readiness between reconcile passes without rebuilding the
    /// shared adapter.
    pub fn set_pod_ready(&self, ready: bool) {
        self.pod_ready.store(ready, Ordering::SeqCst);
    }

    /// Set the initial value [`RuntimeAdapter::envd_serving`] reports (issue
    /// #560). Set `false` to model a kubelet-Ready but checkpoint-wedged restore
    /// Pod whose envd accepts connections but never answers `/__state`.
    #[must_use]
    pub fn with_envd_serving(self, serving: bool) -> Self {
        self.envd_serving.store(serving, Ordering::SeqCst);
        self
    }

    /// Flip what [`RuntimeAdapter::envd_serving`] reports mid-test (`&self` so a
    /// test holding an `Arc` can change it between reconcile passes).
    pub fn set_envd_serving(&self, serving: bool) {
        self.envd_serving.store(serving, Ordering::SeqCst);
    }

    /// Make [`RuntimeAdapter::pod_terminated`] report a terminated Pod with
    /// [`PodTerminationCause::Other`] when `terminated` is `true`, or a live
    /// Pod when `false`.
    ///
    /// For eviction / OOMKill scenarios use [`with_pod_evicted`] /
    /// [`with_pod_oom_killed`] to get a cause with resource-detail fields set.
    ///
    /// [`with_pod_evicted`]: Self::with_pod_evicted
    /// [`with_pod_oom_killed`]: Self::with_pod_oom_killed
    #[must_use]
    pub fn with_pod_terminated(mut self, terminated: bool) -> Self {
        self.pod_terminated = terminated.then_some(PodTerminationCause::Other);
        self
    }

    /// Make [`RuntimeAdapter::pod_terminated`] report a kubelet eviction with
    /// the given `message` (mirrors `pod.status.message`).
    ///
    /// Used by tests that exercise the issue #418 eviction-message path.
    #[must_use]
    pub fn with_pod_evicted(mut self, message: impl Into<String>) -> Self {
        self.pod_terminated = Some(PodTerminationCause::Evicted {
            message: message.into(),
        });
        self
    }

    /// Make [`RuntimeAdapter::pod_terminated`] report an OOMKill with the
    /// given `memory_limit` (mirrors the container spec's `resources.limits.memory`).
    ///
    /// Used by tests that exercise the issue #418 OOMKill-message path.
    #[must_use]
    pub fn with_pod_oom_killed(mut self, memory_limit: Option<impl Into<String>>) -> Self {
        self.pod_terminated = Some(PodTerminationCause::OomKilled {
            memory_limit: memory_limit.map(Into::into),
        });
        self
    }
}

#[async_trait]
impl RuntimeAdapter for FakeRuntimeAdapter {
    fn runtime_class(&self) -> &str {
        &self.runtime_class
    }

    // `create_pod`, `delete_pod`, `pause`, `resume` inherit the
    // trait's default impls. `pod_ready` is overridden below to
    // surface a synthetic `pod_uid` so SnapshotJob dispatch assertions
    // have a stable, non-empty container ID to match against; the trait
    // default's stub address (`127.0.0.1:49983`) is mirrored here.

    /// Returns a deterministic synthetic [`PodReadiness`] so the
    /// `Creating → Running` *and* `Resuming → Running` reconcile
    /// paths both drive forward in unit tests. The synthetic
    /// `pod_uid` is scoped to this adapter; the trait default in
    /// [`sandbawks_types::RuntimeAdapter::pod_ready`] still returns
    /// `None` so third-party adapter impls aren't surprised by a
    /// fabricated container ID leaking into real SnapshotJob
    /// dispatch.
    async fn pod_ready(&self, pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        if !self.pod_ready.load(Ordering::SeqCst) {
            // Modelled un-Ready (e.g. a restore Pod still waiting on its
            // stage-gate before the snapshot is staged): the reconcile loop
            // treats `None` as "not ready, keep waiting".
            return Ok(None);
        }
        Ok(Some(PodReadiness {
            envd_address: "127.0.0.1:49983".into(),
            node_name: None,
            // Pin to the Pod name so test assertions on the dispatched
            // SnapshotJob's container_id stay readable. `Uuid::new_v4`
            // would be non-deterministic and force every assertion to
            // use `is_some()` or capture a value first.
            pod_uid: Some(format!("fake-uid-{}", pod_ref.name)),
        }))
    }

    /// Reports the configured serve-readiness (issue #560), defaulting to
    /// `true`. A resume test sets it `false` (via
    /// [`with_envd_serving`](Self::with_envd_serving) /
    /// [`set_envd_serving`](Self::set_envd_serving)) to model a kubelet-Ready
    /// but checkpoint-wedged restore Pod.
    async fn envd_serving(&self, _envd_address: &str) -> Result<bool, RuntimeError> {
        Ok(self.envd_serving.load(Ordering::SeqCst))
    }

    /// Reports the configured termination cause.
    ///
    /// `None` (the default) means the Pod is still live; `Some(cause)` means
    /// it has terminated with the given cause. Set via
    /// [`with_pod_terminated`](Self::with_pod_terminated),
    /// [`with_pod_evicted`](Self::with_pod_evicted), or
    /// [`with_pod_oom_killed`](Self::with_pod_oom_killed).
    async fn pod_terminated(
        &self,
        _pod_ref: &PodRef,
    ) -> Result<Option<PodTerminationCause>, RuntimeError> {
        Ok(self.pod_terminated.clone())
    }

    /// Reports the configured scheduled node (epic #484).
    ///
    /// `None` (the default) models a Pod not yet bound to a node; a resume
    /// test calls [`with_scheduled_node`](Self::with_scheduled_node) to make
    /// the controller's node-discovery step fire `NodeAssigned`.
    async fn pod_scheduled_node(&self, _pod_ref: &PodRef) -> Result<Option<String>, RuntimeError> {
        if self.scheduled_node_vanishes && self.scheduled_node_seen.swap(true, Ordering::SeqCst) {
            // Second and later calls: the node "vanished" from the read.
            return Ok(None);
        }
        Ok(self.scheduled_node.clone())
    }

    /// Records the call (issue #542) and succeeds — unless
    /// [`with_ensure_egress_error`](Self::with_ensure_egress_error) armed a
    /// synthetic failure, used to prove the reconcile's best-effort contract.
    /// Lets a reconcile test assert the egress-sync hook fired without standing
    /// up a kube apiserver; the real policy-reconcile logic is covered by the
    /// `PodAdapter` integration tests.
    async fn ensure_egress_policy(&self, _sandbox: &Sandbox) -> Result<(), RuntimeError> {
        self.ensure_egress_calls.fetch_add(1, Ordering::SeqCst);
        if self.ensure_egress_fails {
            return Err(RuntimeError::Transient(
                "synthetic ensure_egress_policy failure (test)".into(),
            ));
        }
        Ok(())
    }
}
