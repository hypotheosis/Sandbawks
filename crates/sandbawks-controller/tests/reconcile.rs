//! Reconcile-loop integration tests against in-memory fakes.
//!
//! After Plan 05 T4 these cover:
//!
//! 1. `Creating` + `pod-ready` annotation ⇒ `Running` (no adapter call)
//! 2. `Running` + `desired-phase=Paused` ⇒ `Pausing` + SnapshotJob created
//! 3. SnapshotJob `phase=Failed` ⇒ Sandbox transitions to `Failed`
//! 4. `Paused` + `desired-phase=Running` ⇒ `Resuming` + SnapshotJob created
//! 5. `metadata.deletionTimestamp` set ⇒ `Terminating`
//! 6. Re-running reconcile is idempotent (no duplicate SnapshotJob)
//! 7. SnapshotJob completion folds snapshot_ref + advances phase.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use sandbawks_controller::{
    reconcile, FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore,
    FakeSnapshotJobStore, PvcRetentionConfig, ReconcileContext,
};
use sandbawks_types::{
    NetworkPolicy, PodReadiness, PodRef, PodTerminationCause, ResourceRequests, RestorePlacement,
    RuntimeAdapter, RuntimeError, Sandbox, SandboxCondition, SandboxPhase, SandboxSpec,
    SandboxStatus, SandboxTemplate, SandboxTemplateSpec, SnapshotJob, SnapshotJobMode,
    SnapshotJobPhase, SnapshotJobSpec, SnapshotJobStatus, SnapshotManifest,
};

/// A minimal valid Sandbox the reconcile loop will accept. Tests layer
/// status + annotations on top via [`with_status`] / [`with_annotation`].
fn base_sandbox(name: &str) -> Sandbox {
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

/// Attach an observed `phase` to a Sandbox via `status`.
fn with_status(mut s: Sandbox, phase: SandboxPhase) -> Sandbox {
    s.status = Some(SandboxStatus {
        phase,
        ..SandboxStatus::default()
    });
    s
}

/// Attach one annotation, allocating the BTreeMap if absent.
fn with_annotation(mut s: Sandbox, k: &str, v: &str) -> Sandbox {
    s.metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(k.to_owned(), v.to_owned());
    s
}

/// Build the `Arc<ReconcileContext>` reconcile() expects. Takes the
/// concrete fakes; the trait-object coercions happen inline so tests
/// don't have to spell them out.
fn ctx(
    store: FakeSandboxStore,
    adapter: Arc<FakeRuntimeAdapter>,
    jobs: FakeSnapshotJobStore,
) -> Arc<ReconcileContext> {
    Arc::new(ReconcileContext {
        store: Arc::new(store),
        adapter,
        snapshot_jobs: Arc::new(jobs),
        templates: Arc::new(FakeSandboxTemplateStore::new()),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    })
}

#[tokio::test(flavor = "current_thread")]
async fn creating_with_pod_ready_annotation_transitions_to_running() {
    let store = FakeSandboxStore::new();
    let s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Creating),
        "sandbawks.dev/pod-ready",
        "true",
    );
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Running
    );
    // No SnapshotJob created for the Creating → Running path.
    assert!(jobs.list().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_threads_object_namespace_into_store() {
    // A Sandbox reconciled out of the controller's install namespace must have
    // its OWN namespace ("team-b") threaded into every store call — this is
    // what makes a single cluster-wide controller reconcile all namespaces
    // (#582). Before this change `reconcile` carried no namespace and the
    // store was pinned to one namespace at construction.
    let store = FakeSandboxStore::new();
    let mut s = with_annotation(
        with_status(base_sandbox("sbx-ns"), SandboxPhase::Creating),
        "sandbawks.dev/pod-ready",
        "true",
    );
    s.metadata.namespace = Some("team-b".to_owned());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    let seen = store.seen_namespaces.clone();

    reconcile(
        "team-b",
        "sbx-ns",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .expect("reconcile ok");

    let seen = seen.lock().expect("poisoned");
    assert!(
        !seen.is_empty(),
        "reconcile must touch the store at least once"
    );
    assert!(
        seen.iter().all(|ns| ns == "team-b"),
        "every store call must use the object's namespace, got {seen:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_of_deleted_sandbox_is_a_benign_noop() {
    // Issue #385: when a Sandbox CR is deleted between a reconcile being
    // enqueued and running (routine for retired/claimed warm-pool members),
    // the entry fetch yields `Ok(None)`. The reconcile must return `Ok(())`
    // rather than surfacing a NotFound error that would emit a spurious
    // per-name OTel exception and a pointless requeue.
    let store = FakeSandboxStore::new(); // empty — "sbx-gone" was never inserted
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-gone",
        ctx(store.clone(), adapter, jobs.clone()),
    )
    .await
    .expect("reconcile of an already-deleted Sandbox must be a no-op");
    // Nothing was created as a side effect of reconciling a ghost.
    assert!(store.peek("sbx-gone").is_none());
    assert!(jobs.list().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn creating_with_missing_template_transitions_to_failed() {
    // Issue #265: cold start resolves the SandboxTemplate to pick the
    // Pod's workload image (envd is staged onto the node, not part of the
    // image). A Sandbox pointing at a template that doesn't exist must
    // fail loudly rather than silently cold-booting a default image that
    // lacks the template's tooling.
    let store = FakeSandboxStore::new();
    store.insert(with_status(base_sandbox("sbx-1"), SandboxPhase::Creating));
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter,
        snapshot_jobs: Arc::new(jobs),
        // Strict: the referenced template ("tpl") is absent → None.
        templates: Arc::new(FakeSandboxTemplateStore::strict()),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", ctx).await.unwrap();

    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Failed,
        "a Sandbox referencing a missing template must fail, not cold-boot a default image"
    );
    assert!(
        status.conditions.iter().any(|c| c
            .message
            .as_deref()
            .is_some_and(|m| m.contains("SandboxTemplate") && m.contains("not found"))),
        "expected a Failed condition naming the missing template, got {:?}",
        status.conditions
    );
}

#[tokio::test(flavor = "current_thread")]
async fn running_with_terminated_pod_transitions_to_failed() {
    // Regression: a sandbox Pod runs envd with `restartPolicy: Never`, so
    // when envd exits on its own (its graceful snapshot-and-exit) the Pod
    // settles at `Succeeded` and stays there. That terminal transition
    // fires no Sandbox watch event, so without the `Running` liveness probe
    // the Sandbox lingers at `Running` over a dead Pod indefinitely. The
    // probe must fold a terminated Pod into `Failed`.
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Running);
    // The probe keys off `status.pod_ref`; without it there's nothing to
    // poll and the probe (correctly) no-ops.
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_terminated(true));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Failed,
        "a Running Sandbox whose backing Pod terminated must transition to Failed"
    );
    // This is a failure fold, not a pause — no SnapshotJob is created.
    assert!(jobs.list().is_empty());
}

/// Issue #407 (with the #418 message intent preserved): a Running sandbox with
/// **no snapshot** whose Pod is evicted by the kubelet for ephemeral-storage
/// exhaustion recovers by re-provisioning a fresh, clean-state Pod
/// (`Creating`), not by dead-ending at `Failed`. The `StateLost` condition
/// records that in-flight state was lost *and* still names the eviction cause
/// (the #418 message), so the user can see both what happened and why.
#[tokio::test(flavor = "current_thread")]
async fn running_evicted_without_snapshot_reprovisions_fresh_state_lost() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-evict"), SandboxPhase::Running);
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-evict".into());
    // No snapshot_ref: nothing to restore from.
    store.insert(s);

    let eviction_msg =
        "Pod ephemeral local storage usage exceeds the total limit of containers 256Mi.";
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_evicted(eviction_msg));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-evict",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let status = store.peek("sbx-evict").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Creating,
        "an evicted snapshot-less Sandbox must re-provision fresh (Creating), not fail"
    );
    assert!(
        status.conditions.iter().any(|c| c.kind == "StateLost"
            && c.status == "True"
            && c.reason.as_deref() == Some("EvictedNoSnapshot")
            && c.message
                .as_deref()
                .is_some_and(|m| m.contains("evicted") && m.contains("256Mi"))),
        "a StateLost condition naming the eviction cause is required; got {:?}",
        status.conditions
    );
    assert!(
        status.pod_ref.is_some(),
        "a fresh replacement Pod must be provisioned"
    );
    assert!(
        jobs.list().is_empty(),
        "no SnapshotJob for a snapshot-less re-provision"
    );
}

/// Issue #407: a Running sandbox **with a snapshot** (e.g. it was
/// paused-then-resumed) whose Pod is OOMKilled recovers by restoring from that
/// snapshot — re-entering the `Resuming` flow — and records a `StateRestored`
/// condition naming the memory budget (the #418 message intent).
#[tokio::test(flavor = "current_thread")]
async fn running_oomkilled_with_snapshot_restores_from_snapshot() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-oom"), SandboxPhase::Running);
    {
        let st = s.status.as_mut().unwrap();
        st.pod_ref = Some("sandbawks/sbx-oom".into());
        st.snapshot_ref = Some("s3://bucket/sbx-oom/manifest.json".into());
    }
    store.insert(s);

    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_oom_killed(Some("512Mi")));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-oom",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let status = store.peek("sbx-oom").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Resuming,
        "an OOMKilled Sandbox with a snapshot must restore (Resuming), not fail"
    );
    assert!(
        status.conditions.iter().any(|c| c.kind == "StateRestored"
            && c.status == "True"
            && c.reason.as_deref() == Some("EvictedRestoredFromSnapshot")
            && c.message
                .as_deref()
                .is_some_and(|m| m.contains("512Mi") && m.contains("OOMKilled"))),
        "a StateRestored condition naming the memory budget is required; got {:?}",
        status.conditions
    );
    // The snapshot we restore from must be preserved through the recovery.
    assert_eq!(
        status.snapshot_ref.as_deref(),
        Some("s3://bucket/sbx-oom/manifest.json"),
        "the snapshot reference must survive the restore recovery"
    );
    // A fresh resume starts the bounded restore budget at zero.
    assert_eq!(
        status.resume_restart_count, 0,
        "eviction restore must reset the resume restart budget"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn running_with_live_pod_stays_running() {
    // The liveness probe must not flap a healthy sandbox: a `Running`
    // Sandbox whose backing Pod is still live stays `Running`.
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Running);
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    // Default fake reports the Pod live (`pod_terminated == false`).
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Running,
        "a Running Sandbox with a live backing Pod must stay Running"
    );
    assert!(jobs.list().is_empty());
}

/// Attach an explicit `creationTimestamp` so the idle-timeout baseline
/// (issues #327 / #481) is exercised. With no `lastActivityTimestamp`
/// stamped, the controller measures the idle window from this creation
/// instant. `Sandbox::new` leaves it unset, which the timeout treats as
/// "no deadline".
fn with_creation_age(mut s: Sandbox, age: chrono::Duration) -> Sandbox {
    s.metadata.creation_timestamp = Some(Time(Utc::now() - age));
    s
}

/// Stamp `status.lastActivityTimestamp` to `age` ago (issue #481), so the
/// idle window is measured from that instant instead of `creationTimestamp`.
/// Requires the status to already exist (use after [`with_status`]).
fn with_last_activity(mut s: Sandbox, age: chrono::Duration) -> Sandbox {
    s.status
        .as_mut()
        .expect("with_last_activity requires an existing status (call with_status first)")
        .last_activity_timestamp = Some((Utc::now() - age).to_rfc3339());
    s
}

#[tokio::test(flavor = "current_thread")]
async fn running_past_timeout_with_auto_pause_transitions_to_pausing() {
    // Issue #327: a healthy Running Sandbox whose
    // `creationTimestamp + timeoutSeconds` deadline has passed must
    // auto-pause when `autoPause: true` — `Pausing` plus a Pause
    // SnapshotJob. Before this fix `derive_trigger` never emitted
    // `Trigger::Timeout`, so the sandbox ran forever.
    let store = FakeSandboxStore::new();
    let mut s = with_status(
        with_creation_age(base_sandbox("sbx-1"), chrono::Duration::hours(1)),
        SandboxPhase::Running,
    );
    s.spec.timeout_seconds = 30;
    s.spec.auto_pause = true;
    // `pod_ref` present so the liveness probe runs first; the default fake
    // reports the Pod live, so liveness yields nothing and the elapsed
    // timeout is what fires.
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Pausing,
        "an elapsed timeout with autoPause must drive the Sandbox to Pausing"
    );
    assert!(
        status.snapshot_job_ref.is_some(),
        "the timeout-driven pause must create a Pause SnapshotJob"
    );
    assert_eq!(jobs.list().len(), 1, "exactly one SnapshotJob created");
}

#[tokio::test(flavor = "current_thread")]
async fn running_past_timeout_without_auto_pause_transitions_to_terminating() {
    // Issue #327: the same elapsed deadline with `autoPause: false` must
    // auto-terminate instead — `Terminating`, with the Pod torn down rather
    // than snapshotted.
    let store = FakeSandboxStore::new();
    let mut s = with_status(
        with_creation_age(base_sandbox("sbx-1"), chrono::Duration::hours(1)),
        SandboxPhase::Running,
    );
    s.spec.timeout_seconds = 30;
    s.spec.auto_pause = false;
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Terminating,
        "an elapsed timeout without autoPause must drive the Sandbox to Terminating"
    );
    // Terminate deletes the Pod; it does not snapshot.
    assert!(
        jobs.list().is_empty(),
        "auto-terminate must not create a SnapshotJob"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn running_before_timeout_stays_running() {
    // A Running Sandbox still inside its timeout window must not be touched.
    let store = FakeSandboxStore::new();
    let mut s = with_status(
        with_creation_age(base_sandbox("sbx-1"), chrono::Duration::seconds(5)),
        SandboxPhase::Running,
    );
    s.spec.timeout_seconds = 3600;
    s.spec.auto_pause = true;
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Running,
        "a Sandbox inside its timeout window must stay Running"
    );
    assert!(jobs.list().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn running_with_zero_timeout_never_expires() {
    // Warm-pool members are minted with `timeout_seconds: 0`; that sentinel
    // means "no idle timeout", not "expire the instant it was created". An
    // hour-old Sandbox with timeout 0 must stay Running.
    let store = FakeSandboxStore::new();
    let mut s = with_status(
        with_creation_age(base_sandbox("sbx-1"), chrono::Duration::hours(1)),
        SandboxPhase::Running,
    );
    s.spec.timeout_seconds = 0;
    s.spec.auto_pause = true;
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Running,
        "timeout_seconds == 0 is the disabled sentinel and must not expire"
    );
    assert!(jobs.list().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn running_with_recent_activity_stays_running_past_creation_deadline() {
    // Issue #481: the timeout is an *idle* window, not an absolute deadline.
    // A Sandbox created an hour ago with a 30s timeout would expire under the
    // old creation-anchored model, but a `lastActivityTimestamp` from 5s ago
    // means it is still in active use and must stay Running.
    let store = FakeSandboxStore::new();
    let mut s = with_last_activity(
        with_status(
            with_creation_age(base_sandbox("sbx-1"), chrono::Duration::hours(1)),
            SandboxPhase::Running,
        ),
        chrono::Duration::seconds(5),
    );
    s.spec.timeout_seconds = 30;
    s.spec.auto_pause = true;
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Running,
        "recent activity must keep a past-creation-deadline Sandbox Running"
    );
    assert!(
        jobs.list().is_empty(),
        "an actively-used Sandbox must not be paused"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn running_with_stale_activity_transitions_to_pausing() {
    // Issue #481: the mirror case — the same Sandbox whose last activity is
    // now older than the idle window must auto-pause, even though its
    // `creationTimestamp` alone is irrelevant to the decision.
    let store = FakeSandboxStore::new();
    let mut s = with_last_activity(
        with_status(
            with_creation_age(base_sandbox("sbx-1"), chrono::Duration::hours(1)),
            SandboxPhase::Running,
        ),
        chrono::Duration::minutes(5),
    );
    s.spec.timeout_seconds = 30;
    s.spec.auto_pause = true;
    s.status.as_mut().unwrap().pod_ref = Some("sandbawks/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Pausing,
        "an idle-window-exceeded Sandbox must auto-pause"
    );
    assert!(
        status.snapshot_job_ref.is_some(),
        "the idle-timeout pause must create a Pause SnapshotJob"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn running_with_desired_paused_creates_snapshot_job() {
    let store = FakeSandboxStore::new();
    let s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Running),
        "sandbawks.dev/desired-phase",
        "Paused",
    );
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let s = store.peek("sbx-1").unwrap();
    let status = s.status.unwrap();
    // Phase stays `Pausing` — only the SnapshotJob's Complete status
    // advances us to `Paused`.
    assert_eq!(status.phase, SandboxPhase::Pausing);
    assert!(
        status.snapshot_job_ref.is_some(),
        "snapshot_job_ref should be populated by Action::Pause"
    );
    assert!(
        status.snapshot_ref.is_none(),
        "snapshot_ref is only set on SnapshotJob.Complete observation"
    );

    let created = jobs.list();
    assert_eq!(created.len(), 1, "exactly one SnapshotJob created");
    assert_eq!(created[0].spec.mode, SnapshotJobMode::Pause);
    assert_eq!(created[0].spec.sandbox_name, "sbx-1");
}

#[tokio::test(flavor = "current_thread")]
async fn pause_dispatch_threads_namespace_into_snapshot_job_store() {
    // Dispatch-path coverage for #582: the SandboxStore-only test above can't
    // catch a wrong-namespace bug in `snapshot_jobs.create(...)`. Drive a Pause
    // in "team-b" and assert both the created SnapshotJob's body namespace and
    // the namespace arg into the store are the sandbox's own — not the install
    // namespace. A node watching cluster-wide would otherwise look for the job
    // in the wrong namespace.
    let store = FakeSandboxStore::new();
    let mut s = with_annotation(
        with_status(base_sandbox("sbx-ns"), SandboxPhase::Running),
        "sandbawks.dev/desired-phase",
        "Paused",
    );
    s.metadata.namespace = Some("team-b".to_owned());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    let job_ns = jobs.seen_namespaces.clone();

    reconcile(
        "team-b",
        "sbx-ns",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .expect("reconcile ok");

    let created = jobs.list();
    assert_eq!(created.len(), 1, "exactly one Pause SnapshotJob created");
    assert_eq!(
        created[0].metadata.namespace.as_deref(),
        Some("team-b"),
        "the created SnapshotJob must land in the sandbox's own namespace"
    );
    let job_ns = job_ns.lock().expect("poisoned");
    assert!(
        !job_ns.is_empty() && job_ns.iter().all(|ns| ns == "team-b"),
        "snapshot_jobs.create must be called with the sandbox's namespace, got {job_ns:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cold_start_threads_namespace_into_template_store() {
    // Dispatch-path coverage for #582: cold start resolves the SandboxTemplate
    // via `templates.get`; the namespace arg must be the sandbox's own so a
    // template living in "team-b" is found. Build the ctx inline (rather than
    // the `ctx()` helper) so we can reach the template fake's recorded
    // namespaces.
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-ns"), SandboxPhase::Creating);
    s.metadata.namespace = Some("team-b".to_owned());
    store.insert(s);
    let templates = FakeSandboxTemplateStore::new();
    let tpl_ns = templates.seen_namespaces.clone();
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: Arc::new(FakeRuntimeAdapter::new("fake")),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("team-b", "sbx-ns", ctx)
        .await
        .expect("reconcile ok");

    let tpl_ns = tpl_ns.lock().expect("poisoned");
    assert!(
        !tpl_ns.is_empty() && tpl_ns.iter().all(|ns| ns == "team-b"),
        "templates.get must be called with the sandbox's namespace, got {tpl_ns:?}"
    );
}

/// Seed a `Pausing` Sandbox whose `snapshot_job_ref` points at a *failed*
/// Pause job — the state after a previous reconcile created the job and the
/// per-node worker reported a failure. `retry_count` pre-loads
/// `status.pause_restart_count` so a test can place the Sandbox anywhere in its
/// bounded tier-1 retry budget (issue #510).
fn pausing_with_failed_pause_job(
    store: &FakeSandboxStore,
    jobs: &FakeSnapshotJobStore,
    retry_count: u32,
    fail_message: &str,
) {
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Pausing);
    {
        let st = s.status.as_mut().unwrap();
        st.snapshot_job_ref = Some("sbx-1-pause-1".into());
        st.pause_restart_count = retry_count;
    }
    store.insert(s);
    jobs.insert(SnapshotJob {
        metadata: ObjectMeta {
            name: Some("sbx-1-pause-1".into()),
            ..Default::default()
        },
        spec: SnapshotJobSpec {
            sandbox_name: "sbx-1".into(),
            node_name: "node-a".into(),
            mode: SnapshotJobMode::Pause,
            container_id: "container-1".into(),
            snapshot_ref: None,
            expected_sha256: None,
            expected_envd_sha256: None,
            target_path: None,
        },
        status: Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Failed,
            message: Some(fail_message.into()),
            ..Default::default()
        }),
    });
}

#[tokio::test(flavor = "current_thread")]
async fn pause_checkpoint_failure_redispatches_under_retry_budget() {
    // Issue #510 tier 1: a first failed pause checkpoint re-dispatches the
    // checkpoint (a fresh Pause job) and stays `Pausing` rather than folding to
    // terminal `Failed` — a single transient `/__state` timeout is not terminal.
    let store = FakeSandboxStore::new();
    let jobs = FakeSnapshotJobStore::new();
    pausing_with_failed_pause_job(&store, &jobs, 0, "envd /__state timed out after 10s");

    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Pausing,
        "a retryable pause-checkpoint failure must stay Pausing, not fold to Failed"
    );
    assert!(
        status.snapshot_job_ref.is_some(),
        "a fresh Pause job should have been re-dispatched"
    );
    assert_eq!(
        status.pause_restart_count, 1,
        "the retry must spend one unit of the bounded budget"
    );
    assert!(
        status.failure_reason.is_none(),
        "a retried (non-terminal) failure must not persist a failure_reason"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn pause_checkpoint_failure_folds_recoverable_once_budget_exhausted() {
    // Issue #510 tier 1: once the retry budget is spent, the checkpoint failure
    // folds to `Failed` — but carries the *recoverable* PauseCheckpointFailed
    // reason (persisted to status.failureReason, with failedAt stamped) so the
    // Failed-phase probe can later recover or GC it.
    let store = FakeSandboxStore::new();
    let jobs = FakeSnapshotJobStore::new();
    // A count comfortably above any sane SANDBAWKS_PAUSE_MAX_RETRIES guarantees
    // exhaustion regardless of the (default 3) budget.
    pausing_with_failed_pause_job(&store, &jobs, 99, "disk full");

    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(status.phase, SandboxPhase::Failed);
    assert!(
        status.snapshot_job_ref.is_none(),
        "snapshot_job_ref must be cleared after the failure is folded in"
    );
    assert_eq!(
        status.failure_reason.as_deref(),
        Some("pause_checkpoint_failed"),
        "the recoverable failure class must be persisted for the Failed-phase probe"
    );
    assert!(
        status.failed_at.is_some(),
        "failed_at must be stamped as the GC grace anchor"
    );
    assert!(
        status.conditions.iter().any(|c| c
            .message
            .as_deref()
            .is_some_and(|m| m.contains("disk full"))),
        "Failed condition carrying the most recent failure expected, got {:?}",
        status.conditions
    );
}

#[tokio::test(flavor = "current_thread")]
async fn paused_with_desired_running_creates_pod_then_stages_then_runs() {
    // Epic #484 (scheduler-placed restore): resume is a create-then-stage
    // workflow —
    //   (1) `ResumeRequested` creates the restore Pod *unpinned* (no Stage
    //       job yet) and scrubs the pre-pause identity incl. `assigned_node`,
    //   (2) once the scheduler binds the Pod, node-discovery folds
    //       `NodeAssigned` and dispatches the node-local `Stage` job there,
    //   (3) the `Stage` job completing is a `Noop` (the Pod's stage-gate is
    //       already waiting; nothing for the controller to create),
    //   (4) the restore Pod reaching Ready promotes it to `Running`.
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Paused);
    // Seed snapshot_ref + fingerprints the way a prior pause completion
    // would have left them, a stale `assigned_node` from that pause (which
    // the unpinned recreate must clear), plus the stale Pod identity the
    // controller scrubs when it creates the restore Pod.
    s.status = Some(SandboxStatus {
        snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
        snapshot_sha256: Some(
            "a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771".into(),
        ),
        snapshot_envd_sha256: Some(
            "b1e894f45796b5fd45761fde57f92bed8afe1b7c2ea0948a23ca8b8cad4248882".into(),
        ),
        pod_ref: Some("default/sbx-1-stale".into()),
        pod_uid: Some("stale-uid".into()),
        envd_address: Some("10.0.0.1:49983".into()),
        assigned_node: Some("node-a".into()),
        ..s.status.unwrap()
    });
    let s = with_annotation(s, "sandbawks.dev/desired-phase", "Running");
    store.insert(s);
    // The scheduler places the restore Pod on `node-a`; it is not Ready
    // until the snapshot is staged (modelling the stage-gate init
    // container), so start un-Ready and flip it after staging completes.
    let adapter = Arc::new(
        FakeRuntimeAdapter::new("fake")
            .with_scheduled_node("node-a")
            .with_pod_ready(false),
    );
    let jobs = FakeSnapshotJobStore::new();

    // Pass 1: ResumeRequested → CreateRestorePod. The restore Pod is born
    // unpinned; the pre-pause identity (incl. assigned_node) is scrubbed;
    // NO Stage job yet.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let after_create = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after_create.phase, SandboxPhase::Resuming);
    assert_eq!(
        after_create.pod_ref.as_deref(),
        Some("default/sbx-1-fake"),
        "FakeRuntimeAdapter's create_pod synthesises the restore Pod ref"
    );
    assert!(
        after_create.assigned_node.is_none(),
        "the unpinned recreate must clear the pre-pause assigned_node so the scheduler re-picks"
    );
    assert!(
        after_create.snapshot_job_ref.is_none() && jobs.list().is_empty(),
        "no Stage job until the scheduler has placed the restore Pod"
    );
    assert!(
        after_create.pod_uid.is_none() && after_create.envd_address.is_none(),
        "stale pod_uid/envd_address from the pre-pause Pod must be cleared"
    );

    // Pass 2: node-discovery sees the Pod scheduled to node-a → NodeAssigned
    // → StageSnapshot. A Stage job lands pinned to node-a, targeting the
    // deterministic path; phase stays Resuming.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let after_stage = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after_stage.phase, SandboxPhase::Resuming);
    assert_eq!(
        after_stage.assigned_node.as_deref(),
        Some("node-a"),
        "node-discovery records the scheduler-chosen node"
    );
    let stage_job_name = after_stage
        .snapshot_job_ref
        .clone()
        .expect("Stage SnapshotJob must be stamped once the Pod is scheduled");
    assert!(
        after_stage.restore_staging_path.is_none(),
        "no staging path until the Stage job reports Complete"
    );
    let created = jobs.list();
    assert_eq!(created.len(), 1, "exactly one Stage SnapshotJob created");
    assert_eq!(created[0].spec.mode, SnapshotJobMode::Stage);
    assert_eq!(
        created[0].spec.node_name, "node-a",
        "Stage job must be pinned to the scheduler-chosen node"
    );
    assert_eq!(
        created[0].spec.target_path.as_deref(),
        Some("/var/lib/sandbawks/staging/sbx-1/restore"),
        "Stage job must target the deterministic per-sandbox restore path",
    );
    assert_eq!(
        created[0].spec.snapshot_ref.as_deref(),
        Some("s3://sandbawks/sbx-1/manifest.json"),
        "Stage jobs carry the snapshot_ref forward so the worker knows what to fetch",
    );
    assert_eq!(
        created[0].spec.expected_sha256.as_deref(),
        Some("a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771"),
        "Stage jobs must thread the controller's committed sha256 for cross-check",
    );
    assert_eq!(
        created[0].spec.expected_envd_sha256.as_deref(),
        Some("b1e894f45796b5fd45761fde57f92bed8afe1b7c2ea0948a23ca8b8cad4248882"),
        "Stage jobs must thread the controller's committed envd_sha256 for cross-check (#278)",
    );

    // Simulate the node worker completing the Stage at the target path.
    jobs.set_status(
        &stage_job_name,
        SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            local_path: Some("/var/lib/sandbawks/staging/sbx-1/restore".into()),
            ..Default::default()
        },
    );
    // The snapshot is now staged → the restore Pod's gate clears and it
    // becomes Ready.
    adapter.set_pod_ready(true);

    // Pass 3: StageComplete → Noop. The staged path folds into status and
    // the job ref clears; the restore Pod is NOT recreated (it already
    // exists) — no second SnapshotJob.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let after_staged = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after_staged.phase, SandboxPhase::Resuming);
    assert!(
        after_staged.snapshot_job_ref.is_none(),
        "Stage job ref must be cleared once its Complete status folds in"
    );
    assert_eq!(
        after_staged.restore_staging_path.as_deref(),
        Some("/var/lib/sandbawks/staging/sbx-1/restore"),
        "the Stage job's local_path must fold into status.restoreStagingPath"
    );
    assert_eq!(
        after_staged.pod_ref.as_deref(),
        Some("default/sbx-1-fake"),
        "the restore Pod created in pass 1 is untouched — StageComplete creates nothing"
    );
    assert_eq!(jobs.list().len(), 1, "no second SnapshotJob");

    // Pass 4: the readiness probe rehydrates envd_address/pod_uid and
    // `(Resuming, PodReady)` promotes the born-restored Pod to Running.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let after_ready = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after_ready.phase, SandboxPhase::Running);
    assert!(after_ready.snapshot_job_ref.is_none());
    assert_eq!(
        jobs.list().len(),
        1,
        "still only the Stage job — the new flow dispatches no Resume job"
    );
}

/// Epic #484: if the restore Pod's node is visible to node-discovery
/// (`NodeAssigned` fires) but the immediate re-read inside
/// `Action::StageSnapshot` comes back `None` — a momentary loss of
/// `spec.nodeName` — the Sandbox must NOT fold to `Failed`. The phase is
/// already committed to `Resuming` before dispatch, so a transient `None`
/// re-read has to be a "requeue and retry", not a terminal fold. Assert the
/// Sandbox stays `Resuming` with no Stage job and no `assigned_node`, ready
/// for the next pass to retry.
#[tokio::test(flavor = "current_thread")]
async fn stage_snapshot_requeues_when_scheduled_node_vanishes_on_reread() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Resuming);
    // A restore Pod exists (pod_ref set) and a snapshot to stage from, but
    // no node recorded yet — exactly the node-discovery window.
    s.status = Some(SandboxStatus {
        snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
        pod_ref: Some("default/sbx-1-fake".into()),
        ..s.status.unwrap()
    });
    store.insert(s);
    // First `pod_scheduled_node` (node-discovery) returns node-a → NodeAssigned;
    // the second (StageSnapshot re-read) returns None. Pod stays un-Ready so
    // the readiness probe can't pre-empt the node-discovery path.
    let adapter = Arc::new(
        FakeRuntimeAdapter::new("fake")
            .with_vanishing_scheduled_node("node-a")
            .with_pod_ready(false),
    );
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Resuming,
        "a transient None re-read must requeue in Resuming, never fold to Failed"
    );
    assert!(
        after.snapshot_job_ref.is_none() && jobs.list().is_empty(),
        "no Stage job is dispatched when the node re-read fails"
    );
    assert!(
        after.assigned_node.is_none(),
        "assigned_node stays unset so the next pass's node-discovery retries"
    );
    assert!(
        after.conditions.iter().all(|c| c.kind != "Failed"),
        "no Failed condition recorded"
    );
}

/// A `RuntimeAdapter` test double that records the `delete_pod` calls it
/// receives, so a test can assert a flow tears down the prior Pod before
/// creating a replacement. `create_pod` / `pod_ready` mirror
/// `FakeRuntimeAdapter` so the `Resuming → Running` path still drives forward.
///
/// `terminated` (default `None` = live Pod) lets a test additionally model a
/// terminated backing Pod, so the `Running`-phase liveness probe fires — used
/// by the issue #407 eviction-recovery delete assertion.
#[derive(Default)]
struct DeleteRecordingAdapter {
    deleted: Mutex<Vec<String>>,
    terminated: Option<PodTerminationCause>,
}

impl DeleteRecordingAdapter {
    /// A recording adapter whose backing Pod reports a kubelet eviction with
    /// `message`, so the `Running` liveness probe yields `EvictedRecover`.
    fn evicted(message: &str) -> Self {
        Self {
            deleted: Mutex::default(),
            terminated: Some(PodTerminationCause::Evicted {
                message: message.into(),
            }),
        }
    }
}

#[async_trait]
impl RuntimeAdapter for DeleteRecordingAdapter {
    fn runtime_class(&self) -> &str {
        "fake"
    }

    async fn create_pod(
        &self,
        sandbox: &Sandbox,
        _image: &str,
        _restore: Option<&RestorePlacement>,
    ) -> Result<PodRef, RuntimeError> {
        Ok(PodRef {
            name: format!(
                "{}-fake",
                sandbox.metadata.name.as_deref().unwrap_or("anon")
            ),
            namespace: sandbox
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into()),
        })
    }

    async fn delete_pod(&self, pod_ref: &PodRef) -> Result<(), RuntimeError> {
        self.deleted.lock().unwrap().push(pod_ref.name.clone());
        Ok(())
    }

    async fn pod_ready(&self, pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        Ok(Some(PodReadiness {
            envd_address: "127.0.0.1:49983".into(),
            node_name: None,
            pod_uid: Some(format!("fake-uid-{}", pod_ref.name)),
        }))
    }

    async fn pod_terminated(
        &self,
        _pod_ref: &PodRef,
    ) -> Result<Option<PodTerminationCause>, RuntimeError> {
        Ok(self.terminated.clone())
    }
}

/// Issue #196: the paused Pod survives `runsc checkpoint` in `Succeeded`
/// state. `create_pod` treats `409 AlreadyExists` as an idempotent no-op,
/// so unless `CreateRestorePod` deletes that stale Pod first, the restore
/// Pod is never born and the Sandbox hangs in `Resuming` (the resume-bench
/// deadlock). Assert the stale Pod is torn down during the restore step.
#[tokio::test(flavor = "current_thread")]
async fn create_restore_pod_deletes_the_stale_paused_pod() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Paused);
    s.status = Some(SandboxStatus {
        snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
        snapshot_sha256: Some(
            "a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771".into(),
        ),
        pod_ref: Some("default/sbx-1-stale".into()),
        pod_uid: Some("stale-uid".into()),
        envd_address: Some("10.0.0.1:49983".into()),
        assigned_node: Some("node-a".into()),
        ..s.status.unwrap()
    });
    let s = with_annotation(s, "sandbawks.dev/desired-phase", "Running");
    store.insert(s);
    let adapter = Arc::new(DeleteRecordingAdapter::default());
    let jobs = FakeSnapshotJobStore::new();
    let make_ctx = || {
        Arc::new(ReconcileContext {
            store: Arc::new(store.clone()),
            adapter: adapter.clone(),
            snapshot_jobs: Arc::new(jobs.clone()),
            templates: Arc::new(FakeSandboxTemplateStore::new()),
            client: None,
            pvc_retention: PvcRetentionConfig::default(),
            usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
            usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
        })
    };

    // Epic #484: `CreateRestorePod` is now the *first* resume step
    // (`Paused → ResumeRequested`), so the stale pre-pause Pod is deleted
    // on this single pass — before the unpinned restore Pod is created.
    reconcile("default", "sbx-1", make_ctx()).await.unwrap();

    let deleted = adapter.deleted.lock().unwrap().clone();
    assert!(
        deleted.contains(&"sbx-1-stale".to_string()),
        "CreateRestorePod must delete the stale pre-pause Pod before creating the restore Pod; deleted={deleted:?}"
    );
}

/// Issue #407: the snapshot-less eviction-recovery edge routes
/// `Running → Creating`, where `Action::CreatePod` must delete the lingering
/// evicted Pod *before* `create_pod` — otherwise `create_pod`'s 409
/// AlreadyExists idempotency hands back the evicted corpse and no fresh,
/// clean-state Pod is born. The default `FakeRuntimeAdapter::delete_pod` is a
/// no-op, so assert the delete with the recording adapter: a regression that
/// dropped the `delete_pod` call would otherwise pass silently.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_recovery_deletes_the_evicted_pod_before_provisioning_fresh() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-evict"), SandboxPhase::Running);
    // The evicted Pod lingers as a terminal object under this ref; no
    // snapshot_ref → the fresh re-provision path.
    s.status.as_mut().unwrap().pod_ref = Some("default/sbx-evict-old".into());
    store.insert(s);

    let adapter = Arc::new(DeleteRecordingAdapter::evicted(
        "Pod ephemeral local storage usage exceeds the total limit of containers 256Mi.",
    ));
    let jobs = FakeSnapshotJobStore::new();
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(jobs.clone()),
        templates: Arc::new(FakeSandboxTemplateStore::new()),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-evict", ctx).await.unwrap();

    let deleted = adapter.deleted.lock().unwrap().clone();
    assert!(
        deleted.contains(&"sbx-evict-old".to_string()),
        "CreatePod recovery must delete the evicted Pod before provisioning a fresh one; \
         deleted={deleted:?}"
    );
    let status = store.peek("sbx-evict").unwrap().status.unwrap();
    assert_eq!(
        status.phase,
        SandboxPhase::Creating,
        "a snapshot-less eviction must re-provision fresh (Creating)"
    );
    // The fresh Pod replaced the evicted one's ref (proof a genuinely new Pod
    // was created, not the corpse handed back).
    assert_eq!(
        status.pod_ref.as_deref(),
        Some("default/sbx-evict-fake"),
        "status.pod_ref must point at the freshly-created Pod"
    );
}

/// A `RuntimeAdapter` whose `pod_ready` always reports "no Ready Pod"
/// (`Ok(None)`) — modelling a `pod_ref` that names a deleted (or
/// not-yet-Ready) Pod. `create_pod` / `delete_pod` fall back to the trait
/// defaults; the issue #247 recovery path exercises only `pod_ready`.
struct NoReadyPodAdapter;

#[async_trait]
impl RuntimeAdapter for NoReadyPodAdapter {
    fn runtime_class(&self) -> &str {
        "fake"
    }

    async fn pod_ready(&self, _pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        Ok(None)
    }
}

/// A `RuntimeAdapter` whose `pod_ready` always fails with a transient
/// error — modelling an apiserver hiccup on the Pod `GET` during recovery.
/// The issue #247 guard must treat this like "no Ready Pod" (suppress the
/// stale address, don't promote), because the failure can be partial and a
/// promotion would persist `Running` on an address we never re-validated.
struct ProbeErrorPodAdapter;

#[async_trait]
impl RuntimeAdapter for ProbeErrorPodAdapter {
    fn runtime_class(&self) -> &str {
        "fake"
    }

    async fn pod_ready(&self, _pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        Err(RuntimeError::Transient("apiserver unreachable".into()))
    }
}

/// A `RuntimeAdapter` whose `pod_terminated` always fails transiently — used to
/// exercise the first probe-error arm of `failed_recovery_trigger` (issue
/// #510), where an unconfirmed terminal read must leave the Sandbox `Failed`
/// rather than GC its Pod on a read we could not complete. `pod_ready` is left
/// at the trait default (it is never reached when `pod_terminated` errors).
struct PodTerminatedErrAdapter;

#[async_trait]
impl RuntimeAdapter for PodTerminatedErrAdapter {
    fn runtime_class(&self) -> &str {
        "fake"
    }

    async fn pod_terminated(
        &self,
        _pod_ref: &PodRef,
    ) -> Result<Option<PodTerminationCause>, RuntimeError> {
        Err(RuntimeError::Transient("apiserver unreachable".into()))
    }
}

/// Backing-Pod liveness a [`CreatingPodAdapter`] reports for a `Creating`
/// Sandbox whose Pod never became Ready.
enum PodLiveness {
    /// `pod_terminated` → `Ok(true)`: the Pod is gone (404) or terminal.
    Gone,
    /// `pod_terminated` → `Ok(false)`: the Pod exists and is still booting.
    Booting,
    /// `pod_terminated` → `Err(_)`: a transient apiserver hiccup on the probe.
    ProbeError,
}

/// A `RuntimeAdapter` modelling a `Creating` Sandbox whose backing Pod
/// never reported Ready: `pod_ready` always returns `Ok(None)` (the
/// reconcile loop can't tell "gone" from "still booting" from this alone).
/// `pod_terminated` reports the configured [`PodLiveness`] so the issue
/// #337 `Creating`-liveness path can be exercised in isolation.
struct CreatingPodAdapter {
    liveness: PodLiveness,
}

#[async_trait]
impl RuntimeAdapter for CreatingPodAdapter {
    fn runtime_class(&self) -> &str {
        "fake"
    }

    async fn pod_ready(&self, _pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        Ok(None)
    }

    async fn pod_terminated(
        &self,
        _pod_ref: &PodRef,
    ) -> Result<Option<PodTerminationCause>, RuntimeError> {
        match self.liveness {
            PodLiveness::Gone => Ok(Some(PodTerminationCause::Other)),
            PodLiveness::Booting => Ok(None),
            PodLiveness::ProbeError => Err(RuntimeError::Transient("apiserver unreachable".into())),
        }
    }
}

/// A `Creating` Sandbox with a `pod_ref` stamped but no `envd_address` —
/// i.e. a Pod was created but never became Ready. This is the state the
/// issue #337 liveness check operates on.
fn creating_with_pod_ref(name: &str) -> Sandbox {
    let mut s = with_status(base_sandbox(name), SandboxPhase::Creating);
    s.status.as_mut().unwrap().pod_ref = Some(format!("sandbawks/{name}"));
    s
}

/// Drive one reconcile over [`creating_with_pod_ref`] with `adapter` and
/// return the Sandbox's observed status afterward.
async fn reconcile_creating_with(adapter: Arc<dyn RuntimeAdapter>) -> SandboxStatus {
    let store = FakeSandboxStore::new();
    store.insert(creating_with_pod_ref("sbx-1"));
    let context = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter,
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(FakeSandboxTemplateStore::new()),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });
    reconcile("default", "sbx-1", context).await.unwrap();
    store.peek("sbx-1").unwrap().status.unwrap()
}

/// Issue #337: a `Creating` Sandbox whose `pod_ref` names a Pod that has
/// vanished (node churn / Pod GC) must transition to `Failed` rather than
/// idle at `Creating` forever — otherwise a warm-pool member wedges a
/// `minReady` slot and the whole pool stalls at `ready=0`.
#[tokio::test(flavor = "current_thread")]
async fn creating_with_vanished_pod_transitions_to_failed() {
    let status = reconcile_creating_with(Arc::new(CreatingPodAdapter {
        liveness: PodLiveness::Gone,
    }))
    .await;
    assert_eq!(
        status.phase,
        SandboxPhase::Failed,
        "a Creating Sandbox whose backing Pod vanished must transition to Failed"
    );
    // Pin the message to the liveness code path, not just any Failed
    // condition: without this the test would still pass if some unrelated
    // failure (e.g. a template-not-found fold) produced the `Failed` phase.
    // The `creating_liveness_trigger` message is the only one containing
    // "vanished", so asserting on it binds the test to the path it exercises.
    assert!(
        status.conditions.iter().any(|c| c.kind == "Failed"
            && c.status == "True"
            && c.message
                .as_deref()
                .is_some_and(|m| m.contains("vanished") && m.contains("Creating"))),
        "the failure must be recorded as a Failed condition naming the \
         vanished-Pod-while-Creating path; got {:?}",
        status.conditions
    );
}

/// The `Creating`-liveness check must not flap a healthy boot: a Sandbox
/// whose Pod exists and is still coming up (`pod_terminated == false`)
/// stays `Creating` and waits for the readiness probe to promote it.
#[tokio::test(flavor = "current_thread")]
async fn creating_with_booting_pod_stays_creating() {
    let status = reconcile_creating_with(Arc::new(CreatingPodAdapter {
        liveness: PodLiveness::Booting,
    }))
    .await;
    assert_eq!(
        status.phase,
        SandboxPhase::Creating,
        "a Creating Sandbox whose Pod is still booting must stay Creating"
    );
}

/// A transient probe error must be treated as "unknown", not "gone": the
/// Sandbox stays `Creating` and retries on the next requeue rather than
/// being failed on an apiserver hiccup (mirrors the `Running`-liveness
/// `Err` handling).
#[tokio::test(flavor = "current_thread")]
async fn creating_liveness_probe_error_stays_creating() {
    let status = reconcile_creating_with(Arc::new(CreatingPodAdapter {
        liveness: PodLiveness::ProbeError,
    }))
    .await;
    assert_eq!(
        status.phase,
        SandboxPhase::Creating,
        "a transient liveness-probe error must not fail a Creating Sandbox"
    );
}

/// Seed the issue #247 crash-window durable state: the Stage job already
/// completed (`snapshot_job_ref` cleared, staging path recorded) and the
/// pre-pause Pod was deleted, but the in-memory scrub of `pod_ref` /
/// `pod_uid` / `envd_address` never reached durable status — so they still
/// name the dead pre-pause Pod while the phase sits at `Resuming`.
fn crash_window_resuming_sandbox() -> Sandbox {
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Resuming);
    s.status = Some(SandboxStatus {
        restore_staging_path: Some("/var/lib/sandbawks/stage/sbx-1".into()),
        pod_ref: Some("default/sbx-1".into()),
        pod_uid: Some("stale-uid".into()),
        envd_address: Some("10.0.0.1:49983".into()),
        assigned_node: Some("node-a".into()),
        ..s.status.unwrap()
    });
    s
}

/// Drive one reconcile over the crash-window state with `adapter` and
/// assert the Sandbox is NOT promoted: it stays `Resuming` and — because
/// the recovery path suppresses the stale address in memory rather than
/// writing — the durable status is left exactly as seeded.
async fn assert_crash_window_does_not_promote(adapter: Arc<dyn RuntimeAdapter>) {
    let store = FakeSandboxStore::new();
    store.insert(crash_window_resuming_sandbox());
    let context = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter,
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(FakeSandboxTemplateStore::new()),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", context).await.unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Resuming,
        "a Resuming Sandbox whose pod_ref names a deleted Pod must NOT be promoted to Running"
    );
    // The recovery path suppresses the unconfirmed address in memory only
    // (no `patch_status`), so the durable record is untouched — the safety
    // property is "did not promote", not "scrubbed the status".
    assert_eq!(
        after.envd_address.as_deref(),
        Some("10.0.0.1:49983"),
        "no durable write should occur on the suppression path"
    );
    assert_eq!(
        after.pod_uid.as_deref(),
        Some("stale-uid"),
        "no durable write should occur on the suppression path"
    );
}

/// Issue #247: a controller crash inside `Action::CreateRestorePod` —
/// after it deletes the pre-pause Pod but before the post-dispatch patch
/// commits the in-memory scrub — leaves the durable status carrying the
/// pre-pause `envd_address` while the phase is still `Resuming`. Without an
/// authoritative re-probe, `derive_trigger` reads the stale address as
/// readiness and the `(Resuming, PodReady)` edge promotes the Sandbox to
/// `Running` while its `pod_ref` names a Pod that was already deleted. When
/// the probe reports no Ready Pod (`Ok(None)`), the Sandbox must stay at
/// `Resuming`.
#[tokio::test(flavor = "current_thread")]
async fn resuming_with_stale_envd_address_and_no_live_pod_does_not_promote() {
    assert_crash_window_does_not_promote(Arc::new(NoReadyPodAdapter)).await;
}

/// Issue #247, `Err(_)` arm: a transient apiserver failure on the
/// readiness re-probe must be treated like "no Ready Pod" — the Sandbox
/// stays `Resuming` rather than promoting on an address we could not
/// re-validate (the failure may be partial, so a promotion could persist).
#[tokio::test(flavor = "current_thread")]
async fn resuming_with_stale_envd_address_and_probe_error_does_not_promote() {
    assert_crash_window_does_not_promote(Arc::new(ProbeErrorPodAdapter)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_job_complete_advances_pausing_to_paused_and_folds_snapshot_ref() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Pausing);
    s.status.as_mut().unwrap().snapshot_job_ref = Some("sbx-1-pause-1".into());
    store.insert(s);

    let jobs = FakeSnapshotJobStore::new();
    jobs.insert(SnapshotJob {
        metadata: ObjectMeta {
            name: Some("sbx-1-pause-1".into()),
            ..Default::default()
        },
        spec: SnapshotJobSpec {
            sandbox_name: "sbx-1".into(),
            node_name: "node-a".into(),
            mode: SnapshotJobMode::Pause,
            container_id: "container-1".into(),
            snapshot_ref: None,
            expected_sha256: None,
            expected_envd_sha256: None,
            target_path: None,
        },
        status: Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            manifest: Some(SnapshotManifest {
                envd_sha256: "b1e894f45796b5fd45761fde57f92bed8afe1b7c2ea0948a23ca8b8cad4248882"
                    .into(),
                runsc_version: "release-20260525.0".into(),
                envd_captured: false,
                sandbox_id: "sbx-1".into(),
                runtime_class: "gvisor".into(),
                sha256: "a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771".into(),
                created_at: "2026-05-25T00:00:00Z".into(),
                parts: Vec::new(),
                snapshot_ref: "s3://sandbawks/sbx-1/manifest.json".into(),
                template_ref: String::new(),
                template_image: String::new(),
            }),
            ..Default::default()
        }),
    });

    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(status.phase, SandboxPhase::Paused);
    assert_eq!(
        status.snapshot_ref.as_deref(),
        Some("s3://sandbawks/sbx-1/manifest.json"),
        "manifest.snapshot_ref must fold into Sandbox.status.snapshot_ref"
    );
    assert_eq!(
        status.snapshot_sha256.as_deref(),
        Some("a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771"),
        "manifest.sha256 must fold into Sandbox.status.snapshot_sha256 — \
         issue #171 hardening so the next Resume dispatch can bind \
         the URL to a fingerprint"
    );
    assert_eq!(
        status.snapshot_envd_sha256.as_deref(),
        Some("b1e894f45796b5fd45761fde57f92bed8afe1b7c2ea0948a23ca8b8cad4248882"),
        "manifest.envd_sha256 must fold into Sandbox.status.snapshot_envd_sha256 — \
         issue #278 hardening so the next Resume/Stage dispatch can bind \
         the envd dimension to a committed fingerprint"
    );
    assert_eq!(
        status.snapshot_runsc_version.as_deref(),
        Some("release-20260525.0"),
        "manifest.runsc_version must fold into Sandbox.status.snapshot_runsc_version — \
         #545 Capability B so the next restore Pod's nodeAffinity gates on the \
         checkpoint's runsc version"
    );
    assert!(
        status.snapshot_job_ref.is_none(),
        "snapshot_job_ref must be cleared after the job's terminal phase folds in"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn deletion_timestamp_set_transitions_to_terminating() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Running);
    // k8s_openapi::Time wraps a chrono::DateTime<Utc>; constructing one
    // from a fixed unix timestamp keeps the test deterministic.
    s.metadata.deletion_timestamp = Some(Time(
        chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
    ));
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Terminating
    );
}

// Regression for the Plan 04 T15 kind-smoke failure: a Sandbox in
// Creating with `status.pod_ref` already populated must not re-fire
// `Action::CreatePod`. With the FakeRuntimeAdapter's default
// `pod_ready` impl returning `Some("127.0.0.1:49983")`, the
// pre-trigger probe in `reconcile()` should patch `envd_address`,
// then the same call should advance the phase all the way to Running.
#[tokio::test(flavor = "current_thread")]
async fn creating_with_pod_ref_probes_and_advances_to_running() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Creating);
    s.status.as_mut().unwrap().pod_ref = Some("default/sbx-1".into());
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let observed = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        observed.phase,
        SandboxPhase::Running,
        "the pod_ready probe should have advanced Creating → Running"
    );
    assert_eq!(
        observed.envd_address.as_deref(),
        Some("127.0.0.1:49983"),
        "envd_address should have been patched from the probe"
    );
    assert!(
        jobs.list().is_empty(),
        "no SnapshotJob should be created for a Creating → Running probe"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_is_idempotent() {
    let store = FakeSandboxStore::new();
    let s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Running),
        "sandbawks.dev/desired-phase",
        "Paused",
    );
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    // First reconcile transitions Running → Pausing and creates one
    // SnapshotJob.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        jobs.list().len(),
        1,
        "first reconcile should create exactly one SnapshotJob"
    );
    // Second reconcile: snapshot_job_ref is set and the job is still
    // in flight (no status written by a worker) → idle. No new job
    // created.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        jobs.list().len(),
        1,
        "second reconcile must not duplicate the SnapshotJob"
    );
}

/// A `RuntimeAdapter` test double that records the effective
/// `runtimeClassName` of every Sandbox handed to `create_pod`, so a test
/// can assert the controller resolved `SandboxTemplate.defaultRuntimeClass`
/// before materializing the Pod (issue #289). `pod_ready` mirrors
/// `FakeRuntimeAdapter` so the `Creating → Running` path drives forward.
#[derive(Default)]
struct RuntimeClassRecordingAdapter {
    classes: Mutex<Vec<Option<String>>>,
    // Issue #592: also record the scheduling hints resolved onto each Sandbox
    // handed to `create_pod`, so a test can assert the controller folded the
    // template's `tolerations` / `nodeSelector` in before materializing the Pod.
    tolerations: Mutex<Vec<Option<Vec<sandbawks_types::Toleration>>>>,
    node_selectors: Mutex<Vec<Option<std::collections::BTreeMap<String, String>>>>,
    // Epic #603: also record the persistence selector / PVC requests resolved
    // onto each Sandbox handed to `create_pod`, so a test can assert the
    // controller folded the template's `persistenceMode` / `volumeClaimTemplates`
    // in before materializing the Pod.
    persistence_modes: Mutex<Vec<Option<sandbawks_types::PersistenceMode>>>,
    volume_claim_templates: Mutex<Vec<Option<Vec<sandbawks_types::VolumeClaimTemplate>>>>,
}

#[async_trait]
impl RuntimeAdapter for RuntimeClassRecordingAdapter {
    fn runtime_class(&self) -> &str {
        "fake"
    }

    async fn create_pod(
        &self,
        sandbox: &Sandbox,
        _image: &str,
        _restore: Option<&RestorePlacement>,
    ) -> Result<PodRef, RuntimeError> {
        self.classes
            .lock()
            .unwrap()
            .push(sandbox.spec.runtime_class_name.clone());
        self.tolerations
            .lock()
            .unwrap()
            .push(sandbox.spec.tolerations.clone());
        self.node_selectors
            .lock()
            .unwrap()
            .push(sandbox.spec.node_selector.clone());
        self.persistence_modes
            .lock()
            .unwrap()
            .push(sandbox.spec.persistence_mode);
        self.volume_claim_templates
            .lock()
            .unwrap()
            .push(sandbox.spec.volume_claim_templates.clone());
        Ok(PodRef {
            name: format!(
                "{}-fake",
                sandbox.metadata.name.as_deref().unwrap_or("anon")
            ),
            namespace: sandbox
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into()),
        })
    }

    async fn pod_ready(&self, pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        Ok(Some(PodReadiness {
            envd_address: "127.0.0.1:49983".into(),
            node_name: None,
            pod_uid: Some(format!("fake-uid-{}", pod_ref.name)),
        }))
    }
}

/// Build a `SandboxTemplate` named `tpl` (matching [`base_sandbox`]'s
/// `template_ref`) whose `defaultRuntimeClass` is `runtime_class`.
fn template_with_runtime_class(runtime_class: &str) -> SandboxTemplate {
    SandboxTemplate::new(
        "tpl",
        SandboxTemplateSpec {
            image: "ghcr.io/hypotheosis/test-workload:latest".into(),
            default_runtime_class: runtime_class.into(),
            default_resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
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

/// Issue #289: a `Creating` Sandbox that omits `runtimeClassName` must
/// inherit its template's `defaultRuntimeClass` on the controller's
/// `Action::CreatePod` edge — so hand-applied Sandboxes (and any non-gateway
/// path) boot under gVisor rather than silently falling back to the cluster
/// default runtime, which breaks pause/resume.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_inherits_template_default_runtime_class() {
    let store = FakeSandboxStore::new();
    // `base_sandbox` leaves `runtime_class_name` unset.
    store.insert(with_status(base_sandbox("sbx-1"), SandboxPhase::Creating));

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template_with_runtime_class("gvisor"));

    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", ctx).await.unwrap();

    let classes = adapter.classes.lock().unwrap();
    assert_eq!(
        classes.as_slice(),
        [Some("gvisor".to_string())],
        "CreatePod must stamp the template's defaultRuntimeClass onto the Pod's Sandbox"
    );
}

/// Issue #592: a `Creating` Sandbox that omits `tolerations` / `nodeSelector`
/// must inherit its template's scheduling hints on the controller's
/// `Action::CreatePod` edge — the single point both cold-start and warm-pool
/// members materialize their Pod through, so both inherit without per-mint
/// plumbing.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_inherits_template_scheduling_hints() {
    use sandbawks_types::Toleration;
    use std::collections::BTreeMap;

    let store = FakeSandboxStore::new();
    // `base_sandbox` leaves `tolerations` / `node_selector` unset.
    store.insert(with_status(base_sandbox("sbx-1"), SandboxPhase::Creating));

    let mut template = template_with_runtime_class("gvisor");
    template.spec.tolerations = Some(vec![Toleration {
        effect: Some("NoSchedule".into()),
        key: Some("kata".into()),
        operator: Some("Equal".into()),
        value: Some("true".into()),
        toleration_seconds: None,
    }]);
    let mut node_selector = BTreeMap::new();
    node_selector.insert("agentpool".to_owned(), "kata".to_owned());
    template.spec.node_selector = Some(node_selector.clone());

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template);

    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", ctx).await.unwrap();

    assert_eq!(
        adapter.tolerations.lock().unwrap().as_slice(),
        [Some(vec![Toleration {
            effect: Some("NoSchedule".into()),
            key: Some("kata".into()),
            operator: Some("Equal".into()),
            value: Some("true".into()),
            toleration_seconds: None,
        }])],
        "CreatePod must fold the template's tolerations onto the Pod's Sandbox"
    );
    assert_eq!(
        adapter.node_selectors.lock().unwrap().as_slice(),
        [Some(node_selector)],
        "CreatePod must fold the template's nodeSelector onto the Pod's Sandbox"
    );
}

/// Issue #592: an explicit `tolerations` / `nodeSelector` on the Sandbox CR
/// wins over the template's hints — the template only fills an empty slot, the
/// same override precedence as `runtimeClassName`.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_explicit_scheduling_hints_override_template() {
    use sandbawks_types::Toleration;
    use std::collections::BTreeMap;

    let store = FakeSandboxStore::new();
    let mut s = base_sandbox("sbx-1");
    s.spec.tolerations = Some(vec![Toleration {
        key: Some("explicit".into()),
        operator: Some("Exists".into()),
        ..Default::default()
    }]);
    let mut explicit_ns = BTreeMap::new();
    explicit_ns.insert("pool".to_owned(), "explicit".to_owned());
    s.spec.node_selector = Some(explicit_ns.clone());
    store.insert(with_status(s, SandboxPhase::Creating));

    let mut template = template_with_runtime_class("gvisor");
    template.spec.tolerations = Some(vec![Toleration {
        key: Some("kata".into()),
        operator: Some("Equal".into()),
        ..Default::default()
    }]);
    let mut tpl_ns = BTreeMap::new();
    tpl_ns.insert("agentpool".to_owned(), "kata".to_owned());
    template.spec.node_selector = Some(tpl_ns);

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template);

    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", ctx).await.unwrap();

    assert_eq!(
        adapter.tolerations.lock().unwrap().as_slice(),
        [Some(vec![Toleration {
            key: Some("explicit".into()),
            operator: Some("Exists".into()),
            ..Default::default()
        }])],
        "an explicit tolerations list must win over the template's"
    );
    assert_eq!(
        adapter.node_selectors.lock().unwrap().as_slice(),
        [Some(explicit_ns)],
        "an explicit nodeSelector must win over the template's"
    );
}

/// Epic #603: a `Creating` Sandbox that omits `persistenceMode` /
/// `volumeClaimTemplates` must inherit its template's values on the
/// `Action::CreatePod` edge — the same single resolver both cold-start and
/// warm-pool members materialize their Pod through, mirroring how the
/// scheduling hints flow down.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_inherits_template_persistence_fields() {
    use sandbawks_types::{
        PersistenceMode, VolumeClaimSpec, VolumeClaimTemplate, VolumeClaimTemplateMetadata,
    };

    let store = FakeSandboxStore::new();
    // `base_sandbox` leaves `persistence_mode` / `volume_claim_templates` unset.
    store.insert(with_status(base_sandbox("sbx-1"), SandboxPhase::Creating));

    let vct = VolumeClaimTemplate {
        metadata: VolumeClaimTemplateMetadata {
            name: "workspace".into(),
            ..Default::default()
        },
        spec: VolumeClaimSpec {
            storage: "10Gi".parse().unwrap(),
            access_modes: vec!["ReadWriteOnce".into()],
            storage_class_name: None,
            volume_mode: None,
        },
    };
    let mut template = template_with_runtime_class("gvisor");
    template.spec.persistence_mode = Some(PersistenceMode::Filesystem);
    template.spec.volume_claim_templates = Some(vec![vct.clone()]);

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template);

    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", ctx).await.unwrap();

    assert_eq!(
        adapter.persistence_modes.lock().unwrap().as_slice(),
        [Some(PersistenceMode::Filesystem)],
        "CreatePod must fold the template's persistenceMode onto the Pod's Sandbox"
    );
    assert_eq!(
        adapter.volume_claim_templates.lock().unwrap().as_slice(),
        [Some(vec![vct])],
        "CreatePod must fold the template's volumeClaimTemplates onto the Pod's Sandbox"
    );
}

/// Epic #603: an explicit `persistenceMode` on the Sandbox CR always wins over
/// the template's (the template only fills an empty slot, same precedence as
/// `runtimeClassName`). For `volumeClaimTemplates` the override is gated by the
/// template's `volumeClaimTemplatesPolicy` (epic #603 Phase 5 / issue #608):
/// under `Overrides`, a tenant claim that shares a name with a template claim
/// replaces it by name. (The default `Disallowed` instead suppresses tenant
/// claims — see `resolve_pod_inputs_enforces_vct_policy`.)
#[tokio::test(flavor = "current_thread")]
async fn create_pod_explicit_persistence_fields_override_template() {
    use sandbawks_types::{
        PersistenceMode, VolumeClaimSpec, VolumeClaimTemplate, VolumeClaimTemplateMetadata,
        VolumeClaimTemplatesPolicy,
    };

    fn workspace(storage: &str) -> VolumeClaimTemplate {
        VolumeClaimTemplate {
            metadata: VolumeClaimTemplateMetadata {
                name: "workspace".into(),
                ..Default::default()
            },
            spec: VolumeClaimSpec {
                storage: storage.parse().unwrap(),
                access_modes: vec![],
                storage_class_name: None,
                volume_mode: None,
            },
        }
    }

    let store = FakeSandboxStore::new();
    let mut s = base_sandbox("sbx-1");
    s.spec.persistence_mode = Some(PersistenceMode::Snapshot);
    // Tenant overrides the template's `workspace` claim with a smaller size.
    s.spec.volume_claim_templates = Some(vec![workspace("1Gi")]);
    store.insert(with_status(s, SandboxPhase::Creating));

    let mut template = template_with_runtime_class("gvisor");
    template.spec.persistence_mode = Some(PersistenceMode::Filesystem);
    template.spec.volume_claim_templates_policy = Some(VolumeClaimTemplatesPolicy::Overrides);
    template.spec.volume_claim_templates = Some(vec![workspace("10Gi")]);

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template);

    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", ctx).await.unwrap();

    assert_eq!(
        adapter.persistence_modes.lock().unwrap().as_slice(),
        [Some(PersistenceMode::Snapshot)],
        "an explicit persistenceMode must win over the template's"
    );
    assert_eq!(
        adapter.volume_claim_templates.lock().unwrap().as_slice(),
        [Some(vec![workspace("1Gi")])],
        "under Overrides, a tenant claim replaces the template's same-named claim"
    );
}

/// Issue #289: an explicit `runtimeClassName` on the Sandbox CR always
/// wins over the template's `defaultRuntimeClass` — the template only fills
/// an empty slot.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_explicit_runtime_class_overrides_template_default() {
    use sandbawks_types::PersistenceMode;

    let store = FakeSandboxStore::new();
    let mut s = base_sandbox("sbx-1");
    s.spec.runtime_class_name = Some("kata-fc".into());
    // A Kata runtime class requires `filesystem` persistence (epic #603 Phase 4
    // / issue #607 rejects `snapshot`-on-Kata at pod-input resolution). This
    // test pins the *runtime-class* override (#289), so pair the Kata class with
    // its only valid mode rather than tripping the unrelated guard.
    s.spec.persistence_mode = Some(PersistenceMode::Filesystem);
    store.insert(with_status(s, SandboxPhase::Creating));

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template_with_runtime_class("gvisor"));

    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", ctx).await.unwrap();

    let classes = adapter.classes.lock().unwrap();
    assert_eq!(
        classes.as_slice(),
        [Some("kata-fc".to_string())],
        "an explicit runtimeClassName must win over the template default"
    );
}

/// Issue #289 (restore path): the template default must be resolved on the
/// `Action::CreateRestorePod` edge too, not just cold start — so a Sandbox
/// resumed without an explicit `runtimeClassName` still lands on the
/// template's class. The PR calls this behavior-preserving (the gVisor
/// default maps to `sandbawks-runsc` via `pod_adapter.rs`), but that
/// mapping is `PodAdapter`'s concern; here we pin the *controller's* part:
/// it resolves `Some("gvisor")` onto the restore Pod's Sandbox.
#[tokio::test(flavor = "current_thread")]
async fn create_restore_pod_inherits_template_default_runtime_class() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Paused);
    // Seed the state a prior pause completion would have left: snapshot
    // refs, the staging node, and the stale pre-pause Pod identity the
    // controller scrubs when it creates the restore Pod.
    s.status = Some(SandboxStatus {
        snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
        snapshot_sha256: Some(
            "a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771".into(),
        ),
        snapshot_envd_sha256: Some(
            "b1e894f45796b5fd45761fde57f92bed8afe1b7c2ea0948a23ca8b8cad4248882".into(),
        ),
        pod_ref: Some("default/sbx-1-stale".into()),
        pod_uid: Some("stale-uid".into()),
        envd_address: Some("10.0.0.1:49983".into()),
        assigned_node: Some("node-a".into()),
        ..s.status.unwrap()
    });
    let s = with_annotation(s, "sandbawks.dev/desired-phase", "Running");
    store.insert(s);

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template_with_runtime_class("gvisor"));
    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    // Epic #484: `CreateRestorePod` is the *first* resume step
    // (`Paused → ResumeRequested`) and the only edge that calls
    // `create_pod`, so this single pass records the restore Pod's class.
    reconcile("default", "sbx-1", ctx).await.unwrap();

    let classes = adapter.classes.lock().unwrap();
    assert_eq!(
        classes.as_slice(),
        [Some("gvisor".to_string())],
        "CreateRestorePod must resolve the template default onto the restore Pod's Sandbox"
    );
}

/// Issue #592 (restore path): the template's scheduling hints must be resolved
/// on the `Action::CreateRestorePod` edge too, not just cold start — so a
/// resumed sandbox lands on the same tainted/labeled node pool a fresh one
/// would. Both edges share `resolve_pod_inputs`, so this is covered
/// transitively, but pinning it explicitly guards against the two edges
/// diverging in how they call the resolver (mirrors
/// `create_restore_pod_inherits_template_default_runtime_class`).
#[tokio::test(flavor = "current_thread")]
async fn create_restore_pod_inherits_template_scheduling_hints() {
    use sandbawks_types::Toleration;
    use std::collections::BTreeMap;

    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Paused);
    // Same prior-pause state the runtime-class restore test seeds.
    s.status = Some(SandboxStatus {
        snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
        snapshot_sha256: Some(
            "a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771".into(),
        ),
        snapshot_envd_sha256: Some(
            "b1e894f45796b5fd45761fde57f92bed8afe1b7c2ea0948a23ca8b8cad4248882".into(),
        ),
        pod_ref: Some("default/sbx-1-stale".into()),
        pod_uid: Some("stale-uid".into()),
        envd_address: Some("10.0.0.1:49983".into()),
        assigned_node: Some("node-a".into()),
        ..s.status.unwrap()
    });
    let s = with_annotation(s, "sandbawks.dev/desired-phase", "Running");
    store.insert(s);

    let mut template = template_with_runtime_class("gvisor");
    template.spec.tolerations = Some(vec![Toleration {
        effect: Some("NoSchedule".into()),
        key: Some("kata".into()),
        operator: Some("Equal".into()),
        value: Some("true".into()),
        toleration_seconds: None,
    }]);
    let mut node_selector = BTreeMap::new();
    node_selector.insert("agentpool".to_owned(), "kata".to_owned());
    template.spec.node_selector = Some(node_selector.clone());

    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template);
    let adapter = Arc::new(RuntimeClassRecordingAdapter::default());
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    // `CreateRestorePod` is the first resume step and the only edge that calls
    // `create_pod`, so this single pass records the restore Pod's hints.
    reconcile("default", "sbx-1", ctx).await.unwrap();

    assert_eq!(
        adapter.tolerations.lock().unwrap().as_slice(),
        [Some(vec![Toleration {
            effect: Some("NoSchedule".into()),
            key: Some("kata".into()),
            operator: Some("Equal".into()),
            value: Some("true".into()),
            toleration_seconds: None,
        }])],
        "CreateRestorePod must resolve the template's tolerations onto the restore Pod's Sandbox"
    );
    assert_eq!(
        adapter.node_selectors.lock().unwrap().as_slice(),
        [Some(node_selector)],
        "CreateRestorePod must resolve the template's nodeSelector onto the restore Pod's Sandbox"
    );
}

// ── Bounded `Resuming` recovery loop (epic #484, issue #489) ─────────────────
//
// These drive the recovery half of the scheduler-placed restore through the
// reconcile loop: a lost/wedged restore Pod is recreated under a finite budget,
// and an unrecoverable resume folds to `Failed (ResumeExhausted)` instead of
// hanging `resumingforever`. `reconcile()` reads `Utc::now()` internally, so the
// tests steer the deadlines/backoff by controlling the
// `status.resume_last_attempt` anchor — a far-past anchor makes any deadline and
// any backoff window already elapsed, deterministically.

/// A fixed far-past RFC 3339 anchor: 26 years before "now", so every resume
/// deadline and backoff window has long since elapsed regardless of the real
/// wall clock the reconcile loop reads.
const FAR_PAST: &str = "2000-01-01T00:00:00Z";

/// Overwrite `status.resume_last_attempt` between reconcile passes to simulate
/// the deadline/backoff window elapsing again (a `CreateRestorePod` dispatch
/// re-stamps the anchor to *now*, which would otherwise suppress the next
/// recovery).
fn rewind_resume_anchor(store: &FakeSandboxStore, name: &str) {
    let mut s = store.peek(name).expect("sandbox present");
    s.status
        .as_mut()
        .expect("status present")
        .resume_last_attempt = Some(FAR_PAST.to_owned());
    store.insert(s);
}

/// Seed a `Resuming` Sandbox mid-restore: a restore Pod exists (`pod_ref`), the
/// upstream snapshot URL is recorded, and the recovery anchor is far in the past
/// so a loss is immediately actionable.
fn resuming_mid_restore(name: &str, restart_count: u32) -> Sandbox {
    let mut s = with_status(base_sandbox(name), SandboxPhase::Resuming);
    s.status = Some(SandboxStatus {
        snapshot_ref: Some(format!("s3://sandbawks/{name}/manifest.json")),
        pod_ref: Some(format!("default/{name}-old")),
        resume_restart_count: restart_count,
        resume_last_attempt: Some(FAR_PAST.to_owned()),
        ..s.status.unwrap()
    });
    s
}

#[tokio::test(flavor = "current_thread")]
async fn restore_pod_terminal_mid_resume_recreates_then_runs() {
    // Issue #489: the restore Pod goes terminal/404 mid-`Resuming`. The recovery
    // loop must recreate it (unpinned, scrubbing the prior identity) under the
    // retry budget, then drive the fresh Pod through schedule → stage → ready to
    // `Running`.
    let store = FakeSandboxStore::new();
    store.insert(resuming_mid_restore("sbx-1", 0));
    let jobs = FakeSnapshotJobStore::new();

    // Pass 1: the prior restore Pod is terminal → RestorePodLost → recreate.
    let terminal = Arc::new(
        FakeRuntimeAdapter::new("fake")
            .with_pod_terminated(true)
            .with_pod_ready(false),
    );
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), terminal, jobs.clone()),
    )
    .await
    .unwrap();
    let recreated = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(recreated.phase, SandboxPhase::Resuming);
    assert_eq!(
        recreated.resume_restart_count, 1,
        "a recovery recreate must spend one retry from the budget"
    );
    assert_eq!(
        recreated.pod_ref.as_deref(),
        Some("default/sbx-1-fake"),
        "the lost Pod must be replaced with a freshly-created restore Pod"
    );
    assert!(
        recreated.assigned_node.is_none(),
        "the unpinned recreate must clear assigned_node so the scheduler re-picks"
    );
    assert!(
        recreated.resume_last_attempt.is_some(),
        "the recreate must stamp the recovery anchor"
    );

    // From here the recreated Pod is healthy: schedule → stage → ready. The
    // anchor stamped above is recent, so the deadlines stay dormant.
    let healthy = Arc::new(
        FakeRuntimeAdapter::new("fake")
            .with_scheduled_node("node-b")
            .with_pod_ready(false),
    );

    // Pass 2: node-discovery → NodeAssigned → Stage job dispatched.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), healthy.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let staged = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(staged.phase, SandboxPhase::Resuming);
    assert_eq!(staged.assigned_node.as_deref(), Some("node-b"));
    let stage_job = staged
        .snapshot_job_ref
        .clone()
        .expect("a Stage job is dispatched once the recreated Pod is scheduled");

    // Worker completes the stage; the Pod's gate clears and it becomes Ready.
    jobs.set_status(
        &stage_job,
        SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            local_path: Some("/var/lib/sandbawks/staging/sbx-1/restore".into()),
            ..Default::default()
        },
    );
    healthy.set_pod_ready(true);

    // Pass 3: StageComplete → Noop. Pass 4: readiness → PodReady → Running.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), healthy.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), healthy.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let running = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        running.phase,
        SandboxPhase::Running,
        "recovery must converge to Running once the recreated Pod is healthy"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn restore_pod_pending_past_deadline_is_bounded_to_failed() {
    // Issue #489: a restore Pod that no node can place sits `Pending` forever
    // under the old code (`resumingforever`). The bounded loop recreates it up
    // to RESUME_MAX_RESTARTS (5) times, then folds to `Failed (ResumeExhausted)`.
    let store = FakeSandboxStore::new();
    store.insert(resuming_mid_restore("sbx-1", 0));
    let jobs = FakeSnapshotJobStore::new();
    // Never scheduled (no `with_scheduled_node`), never Ready → always Pending.
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_ready(false));

    // Five recreate passes: each sees the Pod Pending past the schedule deadline
    // and spends one retry. Rewind the anchor between passes so the next
    // deadline/backoff window is elapsed again.
    for expected_count in 1..=5u32 {
        rewind_resume_anchor(&store, "sbx-1");
        reconcile(
            "default",
            "sbx-1",
            ctx(store.clone(), adapter.clone(), jobs.clone()),
        )
        .await
        .unwrap();
        let s = store.peek("sbx-1").unwrap().status.unwrap();
        assert_eq!(
            s.phase,
            SandboxPhase::Resuming,
            "still recovering at attempt {expected_count}"
        );
        assert_eq!(
            s.resume_restart_count, expected_count,
            "each Pending-past-deadline pass spends exactly one retry"
        );
    }

    // Sixth pass: the budget is spent → fold to Failed (ResumeExhausted).
    rewind_resume_anchor(&store, "sbx-1");
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let failed = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        failed.phase,
        SandboxPhase::Failed,
        "an unrecoverable resume must terminate, not hang resumingforever"
    );
    assert!(
        failed.conditions.iter().any(|c| c.kind == "Failed"
            && c.message
                .as_deref()
                .is_some_and(|m| m.contains("giving up"))),
        "a Failed condition explaining the exhausted recovery is expected, got {:?}",
        failed.conditions
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_stage_job_during_resume_recreates_the_restore_pod() {
    // Issue #489: a `Stage` job failing mid-`Resuming` is recoverable — the
    // restore Pod is recreated and the snapshot re-staged — rather than folding
    // the Sandbox straight to `Failed` (which is what a non-Stage failure does).
    let store = FakeSandboxStore::new();
    let mut s = resuming_mid_restore("sbx-1", 0);
    // A Stage job was in flight (node recorded) and has just failed.
    s.status.as_mut().unwrap().assigned_node = Some("node-a".into());
    s.status.as_mut().unwrap().snapshot_job_ref = Some("sbx-1-stage-1".into());
    store.insert(s);

    let jobs = FakeSnapshotJobStore::new();
    jobs.insert(SnapshotJob {
        metadata: ObjectMeta {
            name: Some("sbx-1-stage-1".into()),
            ..Default::default()
        },
        spec: SnapshotJobSpec {
            sandbox_name: "sbx-1".into(),
            node_name: "node-a".into(),
            mode: SnapshotJobMode::Stage,
            container_id: "container-1".into(),
            snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
            expected_sha256: None,
            expected_envd_sha256: None,
            target_path: Some("/var/lib/sandbawks/staging/sbx-1/restore".into()),
        },
        status: Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Failed,
            message: Some("staging download failed".into()),
            ..Default::default()
        }),
    });
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_ready(false));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Resuming,
        "a failed Stage job recovers in-place rather than folding to Failed"
    );
    assert_eq!(
        after.resume_restart_count, 1,
        "the Stage-failure recovery spends one retry"
    );
    assert!(
        after.snapshot_job_ref.is_none(),
        "the failed Stage job's ref must be cleared so the next pass re-stages cleanly"
    );
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1-fake"),
        "the restore Pod is recreated for the retry"
    );
    assert!(
        after.assigned_node.is_none(),
        "the unpinned recreate clears assigned_node so the scheduler re-picks"
    );
    assert!(
        after.conditions.iter().all(|c| c.kind != "Failed"),
        "a recoverable Stage failure records no Failed condition"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_resume_resets_the_restart_count() {
    // Issue #489: the retry budget is per-resume. A *new* `Paused → Resuming`
    // must start from zero even if the prior resume cycle left a stale count.
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Paused);
    s.status = Some(SandboxStatus {
        snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
        pod_ref: Some("default/sbx-1-stale".into()),
        // A stale, near-exhausted count from a previous resume cycle.
        resume_restart_count: 4,
        resume_last_attempt: Some(FAR_PAST.to_owned()),
        ..s.status.unwrap()
    });
    let s = with_annotation(s, "sandbawks.dev/desired-phase", "Running");
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_ready(false));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs_empty()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after.phase, SandboxPhase::Resuming);
    assert_eq!(
        after.resume_restart_count, 0,
        "a fresh Paused → Resuming must reset the per-resume retry budget"
    );
}

/// A throwaway empty `SnapshotJobStore` for tests that dispatch no jobs.
fn jobs_empty() -> FakeSnapshotJobStore {
    FakeSnapshotJobStore::new()
}

#[tokio::test(flavor = "current_thread")]
async fn failed_stage_job_within_backoff_idles_without_recreating() {
    // Issue #489 (review): the recoverable failed-`Stage` path returns
    // `InFlight` from `observe_snapshot_job` while still inside the recovery
    // backoff window — it must idle *without* clearing the job ref or
    // recreating the Pod, so the next requeue re-evaluates the same failed job
    // once the backoff elapses. This pins that branch end-to-end (the unit test
    // only covers the `resume_recovery_trigger` predicate).
    let store = FakeSandboxStore::new();
    let mut s = resuming_mid_restore("sbx-1", 0);
    s.status.as_mut().unwrap().assigned_node = Some("node-a".into());
    s.status.as_mut().unwrap().snapshot_job_ref = Some("sbx-1-stage-1".into());
    // A *recent* anchor keeps the backoff window open: backoff(0) is 5s, so an
    // anchor of ~now means `now < anchor + backoff` and the loop must idle.
    s.status.as_mut().unwrap().resume_last_attempt = Some(Utc::now().to_rfc3339());
    store.insert(s);

    let jobs = FakeSnapshotJobStore::new();
    jobs.insert(SnapshotJob {
        metadata: ObjectMeta {
            name: Some("sbx-1-stage-1".into()),
            ..Default::default()
        },
        spec: SnapshotJobSpec {
            sandbox_name: "sbx-1".into(),
            node_name: "node-a".into(),
            mode: SnapshotJobMode::Stage,
            container_id: "container-1".into(),
            snapshot_ref: Some("s3://sandbawks/sbx-1/manifest.json".into()),
            expected_sha256: None,
            expected_envd_sha256: None,
            target_path: Some("/var/lib/sandbawks/staging/sbx-1/restore".into()),
        },
        status: Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Failed,
            message: Some("staging download failed".into()),
            ..Default::default()
        }),
    });
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_ready(false));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Resuming,
        "within-backoff idle must not advance the phase"
    );
    assert_eq!(
        after.snapshot_job_ref.as_deref(),
        Some("sbx-1-stage-1"),
        "the failed Stage job's ref must be retained while inside the backoff window"
    );
    assert_eq!(
        after.resume_restart_count, 0,
        "no retry is spent while backing off"
    );
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1-old"),
        "the restore Pod must not be recreated while backing off"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn restore_pod_not_ready_past_readiness_deadline_recreates() {
    // Issue #489 (review): the scheduled-but-not-Ready-past-`RESUME_READY_DEADLINE`
    // arm — a restore Pod that schedules and stages but whose shim restore never
    // reaches Ready (a wedged restore). With the node already recorded
    // (`assigned_node` set, so node-discovery is done) and a far-past anchor, the
    // readiness deadline fires and the loop recreates the Pod.
    let store = FakeSandboxStore::new();
    let mut s = resuming_mid_restore("sbx-1", 0);
    // Staging already happened: a node is recorded and no Stage job is in flight.
    s.status.as_mut().unwrap().assigned_node = Some("node-a".into());
    store.insert(s);
    // Pod schedules (so node-discovery would be satisfied) but never becomes
    // Ready, and is not terminal — exactly a wedged restore.
    let adapter = Arc::new(
        FakeRuntimeAdapter::new("fake")
            .with_scheduled_node("node-a")
            .with_pod_ready(false),
    );
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Resuming,
        "a wedged restore recovers in-place, staying Resuming"
    );
    assert_eq!(
        after.resume_restart_count, 1,
        "the readiness-deadline recovery spends one retry"
    );
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1-fake"),
        "the wedged restore Pod is recreated"
    );
    assert!(
        after.assigned_node.is_none(),
        "the unpinned recreate clears assigned_node so the scheduler re-picks"
    );
}

// ── Failed-but-Pod-alive recovery (issue #510, tiers 2 & 3) ──────────────────

/// Seed a `Failed` Sandbox that folded with the given `failure_reason` label,
/// carrying a `pod_ref` (an orphaned workload Pod) and a `failed_at` anchor at
/// `failed_at_offset_secs` seconds before now. The recovery probe keys on these
/// fields to decide retry/recover/GC.
fn failed_with_orphan_pod(
    store: &FakeSandboxStore,
    failure_reason: &str,
    failed_at_offset_secs: i64,
) {
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Failed);
    let st = s.status.as_mut().unwrap();
    st.pod_ref = Some("default/sbx-1".into());
    st.envd_address = Some("10.0.0.1:49983".into());
    st.assigned_node = Some("node-a".into());
    st.failure_reason = Some(failure_reason.into());
    st.failed_at =
        Some((Utc::now() - chrono::Duration::seconds(failed_at_offset_secs)).to_rfc3339());
    store.insert(s);
}

#[tokio::test(flavor = "current_thread")]
async fn failed_pause_checkpoint_with_live_envd_recovers_to_running() {
    // Tier 2: a Failed (pause-checkpoint) Sandbox whose workload Pod is alive
    // and envd is serving is recovered to Running — the pause simply never
    // happened, so the live Pod is authoritative.
    let store = FakeSandboxStore::new();
    failed_with_orphan_pod(&store, "pause_checkpoint_failed", 0);
    let jobs = FakeSnapshotJobStore::new();
    // Pod alive (pod_terminated=None) and envd serving (pod_ready=true default).
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after.phase, SandboxPhase::Running);
    assert!(
        after.failure_reason.is_none() && after.failed_at.is_none(),
        "recovery must clear the failure breadcrumbs"
    );
    assert_eq!(
        after.pause_restart_count, 0,
        "recovery must reset the tier-1 retry budget"
    );
    assert!(
        after.last_activity_timestamp.is_some(),
        "recovery must re-arm the idle clock to avoid an immediate re-pause tight loop"
    );
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1"),
        "the live workload Pod is left untouched"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_pause_checkpoint_with_terminal_pod_gcs_the_orphan() {
    // Tier 3 (immediate): the Pod is already terminal/gone, so it is GC'd to
    // clear the dangling reference; the Sandbox stays Failed and is now
    // genuinely terminal (no pod_ref left for the probe to re-fire on).
    let store = FakeSandboxStore::new();
    failed_with_orphan_pod(&store, "pause_checkpoint_failed", 0);
    let jobs = FakeSnapshotJobStore::new();
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_terminated(true));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after.phase, SandboxPhase::Failed, "GC stays Failed");
    assert!(
        after.pod_ref.is_none() && after.assigned_node.is_none(),
        "the orphaned Pod identity must be scrubbed after GC"
    );
    assert!(
        after.conditions.iter().any(|c| c.kind == "OrphanReclaimed"),
        "a clear OrphanReclaimed condition must be recorded, got {:?}",
        after.conditions
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_pause_checkpoint_unreachable_envd_gcs_after_grace() {
    // Tier 3 (grace): the Pod is alive but envd has been unreachable past the
    // grace window (failed_at well in the past), so the orphan is GC'd.
    let store = FakeSandboxStore::new();
    // 1 hour ago — comfortably past the default 300s grace window.
    failed_with_orphan_pod(&store, "pause_checkpoint_failed", 3600);
    let jobs = FakeSnapshotJobStore::new();
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_ready(false));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after.phase, SandboxPhase::Failed);
    assert!(
        after.pod_ref.is_none(),
        "the unrecoverable orphan's Pod must be GC'd once the grace window elapses"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_pause_checkpoint_unreachable_envd_idles_within_grace() {
    // Tier 3 (within grace): the Pod is alive but envd is not yet serving and
    // the grace window has not elapsed — idle (no GC, no recover) so a
    // slow-to-recover envd gets a chance to come back.
    let store = FakeSandboxStore::new();
    failed_with_orphan_pod(&store, "pause_checkpoint_failed", 0);
    let jobs = FakeSnapshotJobStore::new();
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_ready(false));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after.phase, SandboxPhase::Failed);
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1"),
        "within the grace window the Pod must be left in place"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_non_recoverable_stays_terminal_even_with_live_pod() {
    // Terminal semantics preserved: a Sandbox folded for a genuine
    // create/terminate failure (not the pause-checkpoint class) is NOT touched
    // by the recovery probe, even though its Pod probes alive.
    let store = FakeSandboxStore::new();
    failed_with_orphan_pod(&store, "action_failure", 3600);
    let jobs = FakeSnapshotJobStore::new();
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Failed,
        "a non-recoverable Failed fold must stay terminal"
    );
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1"),
        "a non-recoverable fold must not be GC'd or recovered"
    );
}

/// Run one reconcile over a recoverable `Failed` Sandbox with `adapter` and
/// assert it stayed `Failed` with its Pod untouched — the conservative
/// behavior both probe-error arms of `failed_recovery_trigger` must exhibit.
async fn assert_failed_recovery_probe_error_is_a_noop(adapter: Arc<dyn RuntimeAdapter>) {
    let store = FakeSandboxStore::new();
    failed_with_orphan_pod(&store, "pause_checkpoint_failed", 3600);
    let context = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter,
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(FakeSandboxTemplateStore::new()),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-1", context).await.unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Failed,
        "a transient recovery-probe error must leave the Sandbox Failed"
    );
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1"),
        "an unconfirmed probe must not GC the Pod"
    );
    assert!(
        !after.conditions.iter().any(|c| c.kind == "OrphanReclaimed"),
        "no GC condition should be recorded on a probe error, got {:?}",
        after.conditions
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_recovery_pod_terminated_probe_error_is_a_noop() {
    // Issue #510: the first probe (`pod_terminated`) erroring must not be
    // mistaken for a dead Pod — stay Failed, GC nothing.
    assert_failed_recovery_probe_error_is_a_noop(Arc::new(PodTerminatedErrAdapter)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_recovery_pod_ready_probe_error_is_a_noop() {
    // Issue #510: the second probe (`pod_ready`) erroring (pod_terminated
    // returns Ok(None) via the trait default) must likewise be a no-op.
    assert_failed_recovery_probe_error_is_a_noop(Arc::new(ProbeErrorPodAdapter)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_pause_checkpoint_with_desired_paused_stays_failed() {
    // Issue #510 (review follow-up): when the original pause was an *explicit*
    // user request (`spec.desiredPhase=Paused`) that still stands, recovering
    // to Running would immediately re-trip PauseRequested and oscillate. So a
    // healthy-Pod recoverable Failed Sandbox with a pending pause stays Failed.
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Failed);
    s.spec.desired_phase = Some(SandboxPhase::Paused);
    {
        let st = s.status.as_mut().unwrap();
        st.pod_ref = Some("default/sbx-1".into());
        st.envd_address = Some("10.0.0.1:49983".into());
        st.failure_reason = Some("pause_checkpoint_failed".into());
        st.failed_at = Some(Utc::now().to_rfc3339());
    }
    store.insert(s);
    let jobs = FakeSnapshotJobStore::new();
    // Pod alive, container Ready (defaults) — the only thing holding recovery
    // back is the still-pending explicit pause.
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();

    let after = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after.phase,
        SandboxPhase::Failed,
        "an explicit-pause checkpoint failure must not recover to Running and oscillate"
    );
    assert_eq!(
        after.pod_ref.as_deref(),
        Some("default/sbx-1"),
        "the Pod is healthy (not unreachable), so it is neither recovered nor GC'd"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recovered_sandbox_does_not_immediately_re_pause() {
    // Issue #510 (review follow-up): the idle-clock re-arm in RecoverToRunning
    // must actually prevent the pause->fail->recover cycle it justifies. Recover
    // a Failed (auto-pause) Sandbox to Running, then reconcile again and assert
    // it stays Running with no fresh Pause SnapshotJob.
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Failed);
    // auto_pause idle timeout is the trigger the re-arm defends; a generous
    // window so the second reconcile is comfortably inside it.
    s.spec.auto_pause = true;
    s.spec.timeout_seconds = 3600;
    {
        let st = s.status.as_mut().unwrap();
        st.pod_ref = Some("default/sbx-1".into());
        st.envd_address = Some("10.0.0.1:49983".into());
        st.failure_reason = Some("pause_checkpoint_failed".into());
        st.failed_at = Some(Utc::now().to_rfc3339());
    }
    store.insert(s);
    let jobs = FakeSnapshotJobStore::new();
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));

    // Pass 1: recover to Running.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let after_recover = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(after_recover.phase, SandboxPhase::Running);

    // Pass 2: must NOT immediately re-pause — the re-armed idle clock keeps it
    // inside the timeout window, so no new Pause SnapshotJob is dispatched.
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let after_second = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(
        after_second.phase,
        SandboxPhase::Running,
        "a recovered Sandbox must stay Running, not re-enter Pausing"
    );
    assert!(
        after_second.snapshot_job_ref.is_none() && jobs.list().is_empty(),
        "no fresh Pause SnapshotJob should be dispatched right after recovery"
    );
}

/// Issue #542: a `Running` sandbox carrying a `spec.networkPolicy` (the union a
/// warm claim stamps) drives the egress-policy reconcile hook so the live
/// NetworkPolicy is synced to spec. The hook runs independently of any phase
/// trigger.
#[tokio::test(flavor = "current_thread")]
async fn running_sandbox_with_network_policy_syncs_egress() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Running);
    // Disabled idle timeout so the Running reconcile stays a quiescent no-op
    // (the egress hook is what we assert, not a phase transition).
    s.spec.timeout_seconds = 0;
    s.spec.network_policy = Some(NetworkPolicy {
        egress_allow: vec!["10.0.0.0/8".into()],
    });
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        adapter.ensure_egress_calls(),
        1,
        "a Running sandbox with a spec.networkPolicy must sync its egress policy"
    );
}

/// Issue #542: a `Running` sandbox with **no** `spec.networkPolicy` skips the
/// egress sync — its default-deny baseline was materialized at `create_pod` and
/// egress is only ever widened, never removed, so there is nothing to
/// reconcile. The gate keeps quiescent reconciles free of an extra apiserver
/// round-trip.
#[tokio::test(flavor = "current_thread")]
async fn running_sandbox_without_network_policy_skips_egress_sync() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Running);
    s.spec.timeout_seconds = 0;
    s.spec.network_policy = None;
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        adapter.ensure_egress_calls(),
        0,
        "a Running sandbox with no egress policy must not call the sync hook"
    );
}

/// Issue #542: the egress sync is gated on the `Running` phase — a `Paused`
/// sandbox (no live Pod, hence no live NetworkPolicy to keep in sync) carrying a
/// `spec.networkPolicy` must not invoke the hook.
#[tokio::test(flavor = "current_thread")]
async fn paused_sandbox_does_not_sync_egress() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Paused);
    s.spec.network_policy = Some(NetworkPolicy {
        egress_allow: vec!["10.0.0.0/8".into()],
    });
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        adapter.ensure_egress_calls(),
        0,
        "only Running sandboxes sync egress; a Paused one has no live policy"
    );
}

/// Issue #542: the egress sync is **best-effort** — a failing
/// `ensure_egress_policy` must be logged and swallowed, never abort the
/// lifecycle reconcile. A `Running` sandbox with `desired-phase=Paused` and an
/// egress policy whose sync errors must still transition to `Pausing` and create
/// its Pause SnapshotJob, exactly as the success-path test does.
#[tokio::test(flavor = "current_thread")]
async fn egress_sync_failure_does_not_abort_lifecycle_reconcile() {
    let store = FakeSandboxStore::new();
    let mut s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Running),
        "sandbawks.dev/desired-phase",
        "Paused",
    );
    // A spec.networkPolicy so the (failing) egress sync hook is invoked.
    s.spec.network_policy = Some(NetworkPolicy {
        egress_allow: vec!["10.0.0.0/8".into()],
    });
    store.insert(s);
    // The egress sync errors on this reconcile.
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_ensure_egress_error());
    let jobs = FakeSnapshotJobStore::new();

    reconcile(
        "default",
        "sbx-1",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .expect("a failing egress sync must not surface as a reconcile error");

    assert_eq!(
        adapter.ensure_egress_calls(),
        1,
        "the egress sync hook was attempted (and failed) for this reconcile"
    );
    // The lifecycle trigger still fired despite the hook error.
    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Pausing,
        "the pause lifecycle must proceed even when the egress sync fails"
    );
    assert_eq!(
        jobs.list().len(),
        1,
        "the Pause SnapshotJob is still created after a failed egress sync"
    );
}

/// Build a `Paused` filesystem sandbox carrying a `Suspended=True` condition
/// (the suspend marker + retention anchor) stamped `age_secs` ago, with an
/// explicit `spec.pvcRetention` window (issue #608).
fn paused_filesystem_sandbox(name: &str, pvc_retention: &str, age_secs: i64) -> Sandbox {
    let mut s = base_sandbox(name);
    s.spec.persistence_mode = Some(sandbawks_types::PersistenceMode::Filesystem);
    s.spec.pvc_retention = Some(pvc_retention.to_owned());
    let suspended_at = Utc::now() - chrono::Duration::seconds(age_secs);
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Paused,
        conditions: vec![SandboxCondition {
            kind: "Suspended".into(),
            status: "True".into(),
            reason: Some("PodReleased".into()),
            message: Some("filesystem-mode pause complete".into()),
            last_transition_time: Some(suspended_at.to_rfc3339()),
        }],
        ..SandboxStatus::default()
    });
    s
}

/// Context with PVC-retention GC enabled (a tiny default window; per-sandbox
/// `spec.pvcRetention` overrides it).
fn ctx_pvc_retention_enabled(
    store: FakeSandboxStore,
    adapter: Arc<FakeRuntimeAdapter>,
    jobs: FakeSnapshotJobStore,
) -> Arc<ReconcileContext> {
    Arc::new(ReconcileContext {
        store: Arc::new(store),
        adapter,
        snapshot_jobs: Arc::new(jobs),
        templates: Arc::new(FakeSandboxTemplateStore::new()),
        client: None,
        pvc_retention: PvcRetentionConfig {
            enabled: true,
            // Per-sandbox `spec.pvcRetention` overrides this in the tests below;
            // the default only needs to be a valid window.
            default_duration: std::time::Duration::from_secs(7 * 24 * 60 * 60),
        },
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    })
}

#[tokio::test(flavor = "current_thread")]
async fn paused_filesystem_sandbox_past_retention_is_gc_d() {
    // A filesystem sandbox paused for 2h with a 1h retention window is past
    // its deadline: the reconcile loop deletes the Sandbox CR (whose
    // ownerReferences cascade-delete the retained PVCs).
    let store = FakeSandboxStore::new();
    store.insert(paused_filesystem_sandbox("sbx-gc", "PT1H", 2 * 60 * 60));
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-gc",
        ctx_pvc_retention_enabled(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .expect("reconcile of an expired paused filesystem sandbox succeeds");

    assert!(
        store.peek("sbx-gc").is_none(),
        "the Sandbox CR must be deleted once its PVC-retention window elapses"
    );
    assert_eq!(
        *store.deleted.lock().expect("poisoned"),
        vec!["sbx-gc".to_owned()],
        "delete must be called exactly once, for the expired sandbox"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn paused_filesystem_sandbox_within_retention_is_kept() {
    // Same sandbox, but only 10 minutes into a 1h window: still inside
    // retention, so it must NOT be collected.
    let store = FakeSandboxStore::new();
    store.insert(paused_filesystem_sandbox("sbx-keep", "PT1H", 10 * 60));
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-keep",
        ctx_pvc_retention_enabled(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .expect("reconcile of a within-window paused filesystem sandbox succeeds");

    assert!(
        store.peek("sbx-keep").is_some(),
        "a paused sandbox still inside its retention window must be kept"
    );
    assert!(
        store.deleted.lock().expect("poisoned").is_empty(),
        "no delete must be issued while inside the retention window"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn paused_filesystem_sandbox_not_gc_d_when_retention_disabled() {
    // Default (disabled) retention config: even long past any window, a paused
    // filesystem sandbox is never auto-collected.
    let store = FakeSandboxStore::new();
    store.insert(paused_filesystem_sandbox(
        "sbx-off",
        "PT1H",
        10 * 24 * 60 * 60,
    ));
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-off",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .expect("reconcile succeeds with retention disabled");

    assert!(
        store.peek("sbx-off").is_some(),
        "retention disabled ⇒ the sandbox is never collected"
    );
    assert!(store.deleted.lock().expect("poisoned").is_empty());
}

// ---------------------------------------------------------------------------
// Clone-from-snapshot (issue #416)
// ---------------------------------------------------------------------------

/// Build the restore context with a registered `tpl` template so the
/// compatibility gate's template lookup resolves (gVisor by default).
fn restore_ctx(
    store: FakeSandboxStore,
    adapter: Arc<FakeRuntimeAdapter>,
    jobs: FakeSnapshotJobStore,
) -> Arc<ReconcileContext> {
    let templates = FakeSandboxTemplateStore::new();
    templates.insert(template_with_runtime_class("gvisor"));
    Arc::new(ReconcileContext {
        store: Arc::new(store),
        adapter,
        snapshot_jobs: Arc::new(jobs),
        templates: Arc::new(templates),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    })
}

/// A snapshot-mode source: `Paused` with a recorded snapshot ref + digests.
fn snapshot_source(name: &str) -> Sandbox {
    let mut s = with_status(base_sandbox(name), SandboxPhase::Paused);
    let st = s.status.as_mut().expect("status set");
    st.snapshot_ref = Some("s3://snaps/snapshots/sbx-src/manifest.json".into());
    st.snapshot_sha256 = Some("agg-sha".into());
    st.snapshot_envd_sha256 = Some("envd-sha".into());
    s
}

/// A filesystem-mode source: `Paused` with a `Suspended=True` condition (its
/// retained PVC is quiescent and clonable).
fn suspended_source(name: &str) -> Sandbox {
    let mut s = with_status(base_sandbox(name), SandboxPhase::Paused);
    s.status
        .as_mut()
        .expect("status set")
        .conditions
        .push(SandboxCondition {
            kind: "Suspended".into(),
            status: "True".into(),
            reason: Some("PodReleased".into()),
            message: None,
            last_transition_time: Some("2026-06-20T00:00:00Z".into()),
        });
    s
}

#[tokio::test(flavor = "current_thread")]
async fn clone_from_snapshot_resolves_source_and_enters_resuming() {
    let store = FakeSandboxStore::new();
    store.insert(snapshot_source("sbx-src"));
    let mut clone = with_status(base_sandbox("sbx-clone"), SandboxPhase::Creating);
    clone.spec.restore_from = Some("sbx-src".into());
    store.insert(clone);

    let adapter = Arc::new(FakeRuntimeAdapter::new("gvisor"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-clone",
        restore_ctx(store.clone(), adapter, jobs),
    )
    .await
    .unwrap();

    let st = store.peek("sbx-clone").unwrap().status.unwrap();
    // Entered the scheduler-placed restore flow...
    assert_eq!(st.phase, SandboxPhase::Resuming);
    // ...with the source's snapshot artifact stamped so `StageSnapshot` can
    // stage it onto the new restore Pod.
    assert_eq!(
        st.snapshot_ref.as_deref(),
        Some("s3://snaps/snapshots/sbx-src/manifest.json")
    );
    assert_eq!(st.snapshot_sha256.as_deref(), Some("agg-sha"));
    assert_eq!(st.snapshot_envd_sha256.as_deref(), Some("envd-sha"));
    // A restore Pod was created for the clone.
    assert!(st.pod_ref.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn clone_from_snapshot_missing_source_fails() {
    let store = FakeSandboxStore::new();
    let mut clone = with_status(base_sandbox("sbx-clone"), SandboxPhase::Creating);
    clone.spec.restore_from = Some("sbx-nope".into());
    store.insert(clone);

    let adapter = Arc::new(FakeRuntimeAdapter::new("gvisor"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-clone",
        restore_ctx(store.clone(), adapter, jobs),
    )
    .await
    .unwrap();

    let st = store.peek("sbx-clone").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Failed);
    assert!(st.conditions.iter().any(|c| c.kind == "Failed"
        && c.message
            .as_deref()
            .is_some_and(|m| m.contains("not found"))));
}

#[tokio::test(flavor = "current_thread")]
async fn clone_from_snapshot_source_without_snapshot_fails() {
    let store = FakeSandboxStore::new();
    // Source exists but was never paused in snapshot mode (no snapshot_ref).
    store.insert(with_status(base_sandbox("sbx-src"), SandboxPhase::Running));
    let mut clone = with_status(base_sandbox("sbx-clone"), SandboxPhase::Creating);
    clone.spec.restore_from = Some("sbx-src".into());
    store.insert(clone);

    let adapter = Arc::new(FakeRuntimeAdapter::new("gvisor"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-clone",
        restore_ctx(store.clone(), adapter, jobs),
    )
    .await
    .unwrap();

    let st = store.peek("sbx-clone").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Failed);
    assert!(st.conditions.iter().any(|c| c
        .message
        .as_deref()
        .is_some_and(|m| m.contains("no snapshot recorded"))));
}

#[tokio::test(flavor = "current_thread")]
async fn clone_from_snapshot_rejects_raw_s3_url() {
    let store = FakeSandboxStore::new();
    let mut clone = with_status(base_sandbox("sbx-clone"), SandboxPhase::Creating);
    clone.spec.restore_from = Some("s3://snaps/snapshots/sbx-x/manifest.json".into());
    store.insert(clone);

    let adapter = Arc::new(FakeRuntimeAdapter::new("gvisor"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-clone",
        restore_ctx(store.clone(), adapter, jobs),
    )
    .await
    .unwrap();

    let st = store.peek("sbx-clone").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Failed);
    assert!(st.conditions.iter().any(|c| c
        .message
        .as_deref()
        .is_some_and(|m| m.contains("not yet supported"))));
}

#[tokio::test(flavor = "current_thread")]
async fn filesystem_clone_from_suspended_source_enters_resuming() {
    let store = FakeSandboxStore::new();
    store.insert(suspended_source("sbx-src"));
    let mut clone = with_status(base_sandbox("sbx-clone"), SandboxPhase::Creating);
    clone.spec.restore_from = Some("sbx-src".into());
    // Force filesystem mode so the clone cold-boots over a CSI-cloned PVC.
    clone.spec.persistence_mode = Some(sandbawks_types::PersistenceMode::Filesystem);
    store.insert(clone);

    let adapter = Arc::new(FakeRuntimeAdapter::new("gvisor"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-clone",
        restore_ctx(store.clone(), adapter, jobs),
    )
    .await
    .unwrap();

    let st = store.peek("sbx-clone").unwrap().status.unwrap();
    // A filesystem clone cold-boots a fresh Pod (no snapshot staged).
    assert_eq!(st.phase, SandboxPhase::Resuming);
    assert!(st.snapshot_ref.is_none());
    assert!(st.pod_ref.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn filesystem_clone_requires_suspended_source() {
    let store = FakeSandboxStore::new();
    // Source is Running (not filesystem-paused) — its PVC is not quiescent.
    store.insert(with_status(base_sandbox("sbx-src"), SandboxPhase::Running));
    let mut clone = with_status(base_sandbox("sbx-clone"), SandboxPhase::Creating);
    clone.spec.restore_from = Some("sbx-src".into());
    clone.spec.persistence_mode = Some(sandbawks_types::PersistenceMode::Filesystem);
    store.insert(clone);

    let adapter = Arc::new(FakeRuntimeAdapter::new("gvisor"));
    let jobs = FakeSnapshotJobStore::new();
    reconcile(
        "default",
        "sbx-clone",
        restore_ctx(store.clone(), adapter, jobs),
    )
    .await
    .unwrap();

    let st = store.peek("sbx-clone").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Failed);
    assert!(st.conditions.iter().any(|c| c
        .message
        .as_deref()
        .is_some_and(|m| m.contains("not Suspended"))));
}
