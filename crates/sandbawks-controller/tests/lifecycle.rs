//! Full Sandbox lifecycle against fakes.
//!
//! Walks one Sandbox through every legal phase in spec §5 and asserts
//! both the phase observed at each step and the side effects dispatched.
//! After Plan 05 T4, pause/resume are mediated by `SnapshotJob` CRs the
//! controller creates; the per-node worker is simulated here by mutating
//! `SnapshotJob.status` between reconciles.

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use sandbawks_controller::{
    reconcile, FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore,
    FakeSnapshotJobStore, PvcRetentionConfig, ReconcileContext,
};
use sandbawks_types::{
    PersistenceMode, ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus,
    SnapshotJobMode, SnapshotJobPhase, SnapshotJobStatus, SnapshotManifest,
};

fn fresh_sandbox() -> Sandbox {
    Sandbox::new(
        "sbx-lifecycle",
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

fn annotate(s: &mut Sandbox, k: &str, v: &str) {
    s.metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(k.into(), v.into());
}

fn clear_annotation(s: &mut Sandbox, k: &str) {
    if let Some(ann) = s.metadata.annotations.as_mut() {
        ann.remove(k);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_lifecycle_creating_to_terminating() {
    let store = FakeSandboxStore::new();
    // Scheduler-placed restore (epic #484): the restore Pod created during
    // resume is placed on `node-a`. Readiness is toggled around the resume
    // (`set_pod_ready`) to model the stage-gate that holds the restore Pod
    // un-Ready until the snapshot is staged.
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_scheduled_node("node-a"));
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

    // Seed: Sandbox in Creating, no annotations.
    let mut s = fresh_sandbox();
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Creating,
        ..Default::default()
    });
    store.insert(s);

    // 1. Creating + PodReady -> Running
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        annotate(&mut s, "sandbawks.dev/pod-ready", "true");
        store.insert(s);
    }
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-lifecycle").unwrap().status.unwrap().phase,
        SandboxPhase::Running
    );
    assert!(
        jobs.list().is_empty(),
        "PodReady transition should not create a SnapshotJob"
    );

    // 2. Running + desired=Paused -> Pausing (job created, phase stays
    // Pausing until the worker completes).
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        annotate(&mut s, "sandbawks.dev/desired-phase", "Paused");
        store.insert(s);
    }
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    assert_eq!(s.status.as_ref().unwrap().phase, SandboxPhase::Pausing);
    let pause_job_name = s
        .status
        .as_ref()
        .and_then(|st| st.snapshot_job_ref.clone())
        .expect("snapshot_job_ref must be stamped");
    let created_jobs = jobs.list();
    assert_eq!(created_jobs.len(), 1);
    assert_eq!(created_jobs[0].spec.mode, SnapshotJobMode::Pause);

    // Simulate per-node worker: write `phase=Complete` + manifest into
    // the job's status. Next reconcile observes that and advances
    // Pausing → Paused, folding the manifest's snapshot_ref into the
    // Sandbox.
    // Use a deterministic-but-non-trivial aggregate so the round-trip
    // assertions distinguish the threaded fingerprint from a stub.
    let pause_aggregate_sha = "a0f783e34695a4ec34650ecd46e81adc79ed0a6b1df9837912b997bac3137771";
    jobs.set_status(
        &pause_job_name,
        SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            manifest: Some(SnapshotManifest {
                envd_sha256: String::new(),
                runsc_version: String::new(),
                envd_captured: false,
                sandbox_id: "sbx-lifecycle".into(),
                runtime_class: "gvisor".into(),
                sha256: pause_aggregate_sha.into(),
                created_at: "2026-05-25T00:00:00Z".into(),
                parts: Vec::new(),
                snapshot_ref: "s3://sandbawks/sbx-lifecycle/manifest.json".into(),
                template_ref: String::new(),
                template_image: String::new(),
            }),
            ..Default::default()
        },
    );
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    let status = s.status.as_ref().unwrap();
    assert_eq!(status.phase, SandboxPhase::Paused);
    assert_eq!(
        status.snapshot_ref.as_deref(),
        Some("s3://sandbawks/sbx-lifecycle/manifest.json"),
    );
    assert_eq!(
        status.snapshot_sha256.as_deref(),
        Some(pause_aggregate_sha),
        "Pause completion must fold manifest.sha256 into status — \
         the Resume dispatch in step 3 needs it to bind the URL to a \
         fingerprint",
    );
    assert!(status.snapshot_job_ref.is_none());

    // 3a. Paused + desired=Running -> Resuming (epic #484 scheduler-placed
    // restore). Step 1 creates the restore Pod *unpinned*; no Stage job yet.
    // Model the restore Pod as not-Ready until the snapshot is staged.
    adapter.set_pod_ready(false);
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        // Drop the step-1 `pod-ready` readiness shim: it stood in for the
        // cold-start Pod's readiness, but the restore Pod is a different Pod
        // and must not be treated as Ready before its snapshot is staged
        // (otherwise `derive_trigger` short-circuits node-discovery).
        clear_annotation(&mut s, "sandbawks.dev/pod-ready");
        annotate(&mut s, "sandbawks.dev/desired-phase", "Running");
        store.insert(s);
    }
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    let status_after_create = s.status.as_ref().unwrap();
    assert_eq!(status_after_create.phase, SandboxPhase::Resuming);
    assert!(
        status_after_create.pod_ref.is_some(),
        "CreateRestorePod must stamp the restore Pod's pod_ref"
    );
    assert!(
        status_after_create.assigned_node.is_none(),
        "the unpinned restore Pod has no assigned_node until the scheduler binds it"
    );
    assert_eq!(
        jobs.list().len(),
        1,
        "only the Pause job so far — no Stage job until the Pod is scheduled"
    );

    // 3b. node-discovery sees the Pod scheduled to node-a -> NodeAssigned ->
    // StageSnapshot. The Stage job carries the snapshot URL + committed
    // sha256 forward and targets the deterministic restore path.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    let status_after_stage = s.status.as_ref().unwrap();
    assert_eq!(status_after_stage.phase, SandboxPhase::Resuming);
    assert_eq!(
        status_after_stage.assigned_node.as_deref(),
        Some("node-a"),
        "node-discovery records the scheduler-chosen node"
    );
    let stage_job_name = status_after_stage
        .snapshot_job_ref
        .clone()
        .expect("StageSnapshot must stamp the Stage SnapshotJob ref");
    assert!(
        status_after_stage.restore_staging_path.is_none(),
        "no staging path until the Stage job reports one on Complete"
    );
    let all_jobs = jobs.list();
    assert_eq!(all_jobs.len(), 2, "the Pause job plus the new Stage job");
    let stage_job = all_jobs
        .iter()
        .find(|j| j.metadata.name.as_deref() == Some(&stage_job_name))
        .expect("stage job present in store");
    assert_eq!(stage_job.spec.mode, SnapshotJobMode::Stage);
    assert_eq!(
        stage_job.spec.node_name, "node-a",
        "the Stage job must be pinned to the scheduler-chosen node"
    );
    assert_eq!(
        stage_job.spec.target_path.as_deref(),
        Some("/var/lib/sandbawks/staging/sbx-lifecycle/restore"),
        "the Stage job must target the deterministic per-sandbox restore path"
    );
    assert_eq!(
        stage_job.spec.expected_sha256.as_deref(),
        Some(pause_aggregate_sha),
        "the pause aggregate sha256 must travel pause→status→stage dispatch \
         so the node worker can cross-check the re-fetched manifest"
    );

    // Simulate the worker completing the Stage at the target path; the
    // restore Pod's gate then clears and it becomes Ready.
    jobs.set_status(
        &stage_job_name,
        SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            local_path: Some("/var/lib/sandbawks/staging/sbx-lifecycle/restore".into()),
            ..Default::default()
        },
    );
    adapter.set_pod_ready(true);

    // 3c. StageComplete -> Noop. The staged path folds into status and the
    // job ref clears; the restore Pod (created in 3a) is NOT recreated.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    let status_after_staged = s.status.as_ref().unwrap();
    assert_eq!(status_after_staged.phase, SandboxPhase::Resuming);
    assert!(
        status_after_staged.snapshot_job_ref.is_none(),
        "Stage job ref must clear once its Complete status folds in"
    );
    assert_eq!(
        status_after_staged.restore_staging_path.as_deref(),
        Some("/var/lib/sandbawks/staging/sbx-lifecycle/restore"),
        "the Stage job's local_path must fold into status.restoreStagingPath"
    );
    assert_eq!(
        jobs.list().len(),
        2,
        "no third SnapshotJob — StageComplete creates nothing"
    );

    // 3d. The readiness probe (active for Resuming) rehydrates
    // pod_uid + envd_address from the fake's `pod_ready` impl, and
    // `(Resuming, PodReady)` promotes the born-restored Pod to Running.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    let status = s.status.as_ref().unwrap();
    assert_eq!(status.phase, SandboxPhase::Running);
    assert!(status.snapshot_job_ref.is_none());

    // 4. Running + deletionTimestamp set -> Terminating
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        clear_annotation(&mut s, "sandbawks.dev/desired-phase");
        s.metadata.deletion_timestamp = Some(Time(
            chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        ));
        store.insert(s);
    }
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-lifecycle").unwrap().status.unwrap().phase,
        SandboxPhase::Terminating
    );
}

/// #560 resume serve-readiness gate. A restore Pod that is kubelet-`Ready`
/// (TCP `:49983` accepting) but whose envd is checkpoint-wedged — it accepts
/// connections yet never answers `/__state` — must NOT be promoted to
/// `Running`. The Sandbox stays `Resuming` until envd actually serves, at which
/// point it promotes. Models the bug directly: `pod_ready` (kubelet) stays
/// `true` throughout; only `envd_serving` flips.
#[tokio::test(flavor = "current_thread")]
async fn resume_withholds_running_until_envd_serves() {
    let store = FakeSandboxStore::new();
    // Restore Pod scheduled + kubelet-Ready (defaults), but envd not serving.
    let adapter = Arc::new(
        FakeRuntimeAdapter::new("fake")
            .with_scheduled_node("node-a")
            .with_envd_serving(false),
    );
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

    // A Sandbox mid-resume past staging: restore Pod exists (pod_ref set) and is
    // bound (assigned_node set, so node-discovery doesn't re-fire), staging done
    // (no snapshot_job_ref), desired Running. The only thing standing between it
    // and `Running` is the readiness probe.
    let mut s = fresh_sandbox();
    annotate(&mut s, "sandbawks.dev/desired-phase", "Running");
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Resuming,
        pod_ref: Some("default/sbx-lifecycle".into()),
        assigned_node: Some("node-a".into()),
        snapshot_ref: Some("s3://sandbawks/sbx-lifecycle/manifest.json".into()),
        restore_staging_path: Some("/var/lib/sandbawks/staging/sbx-lifecycle/restore".into()),
        ..Default::default()
    });
    store.insert(s);

    // envd accepts TCP (kubelet Ready) but does not serve /__state → withhold
    // promotion; the Sandbox stays Resuming.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-lifecycle").unwrap().status.unwrap().phase,
        SandboxPhase::Resuming,
        "kubelet-Ready but non-serving envd must NOT promote to Running (#560)"
    );
    assert!(
        store
            .peek("sbx-lifecycle")
            .unwrap()
            .status
            .unwrap()
            .envd_address
            .is_none(),
        "the promotion-gating envd_address must be withheld while envd is not serving"
    );

    // envd starts serving → the next pass promotes to Running.
    adapter.set_envd_serving(true);
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-lifecycle").unwrap().status.unwrap().phase,
        SandboxPhase::Running,
        "once envd serves /__state, the resume promotes to Running"
    );
}

/// #563 warm-claim idle-timeout thrash. A claimed warm-pool member
/// (`desiredPhase=Running` + `autoPause`) that aged past its idle-timeout
/// deadline *before* a pause must NOT be re-paused the instant it resumes to
/// `Running`. The `(Resuming, PodReady)` edge re-arms `lastActivityTimestamp`
/// to *now* (`Action::ResumeToRunning`), so the resumed Sandbox gets a full
/// fresh idle window instead of tripping the idle edge immediately and
/// thrashing resume↔idle-pause. Models the bug directly: a far-past activity
/// baseline that, before the fix, fires `Timeout` the moment the Sandbox lands
/// in `Running`.
#[tokio::test(flavor = "current_thread")]
async fn resume_to_running_rearms_idle_clock_no_thrash() {
    let store = FakeSandboxStore::new();
    // Restore Pod scheduled, kubelet-Ready, and envd serving → this pass
    // promotes straight to Running.
    let adapter = Arc::new(
        FakeRuntimeAdapter::new("fake")
            .with_scheduled_node("node-a")
            .with_envd_serving(true),
    );
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

    // A long-lived claimed member mid-resume: autoPause on, a comfortable
    // 300s idle window, desired Running. Its activity baseline (and creation)
    // sit years in the past, so the idle-timeout deadline is long expired —
    // exactly the production state that, pre-#563, instant-paused on resume.
    let stale = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let mut s = fresh_sandbox();
    s.spec.auto_pause = true;
    s.spec.timeout_seconds = 300;
    s.metadata.creation_timestamp = Some(Time(stale));
    annotate(&mut s, "sandbawks.dev/desired-phase", "Running");
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Resuming,
        pod_ref: Some("default/sbx-lifecycle".into()),
        assigned_node: Some("node-a".into()),
        snapshot_ref: Some("s3://sandbawks/sbx-lifecycle/manifest.json".into()),
        restore_staging_path: Some("/var/lib/sandbawks/staging/sbx-lifecycle/restore".into()),
        last_activity_timestamp: Some(stale.to_rfc3339()),
        ..Default::default()
    });
    store.insert(s);

    // Pass 1: `(Resuming, PodReady)` promotes to Running and re-arms the clock.
    let before = chrono::Utc::now();
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let status = store.peek("sbx-lifecycle").unwrap().status.unwrap();
    assert_eq!(status.phase, SandboxPhase::Running);
    let rearmed = status
        .last_activity_timestamp
        .as_deref()
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .expect("resume must stamp a parseable lastActivityTimestamp")
        .with_timezone(&chrono::Utc);
    assert!(
        rearmed > stale,
        "resume must re-arm lastActivityTimestamp off the stale baseline ({stale} -> {rearmed})"
    );
    assert!(
        rearmed >= before,
        "the re-armed activity time must be the resume instant, not a stale value"
    );

    // Pass 2: still Running. The re-armed clock gives a full 300s window, so
    // the idle edge does NOT fire — without the fix, the stale baseline would
    // have folded this straight to `Pausing` here, starting the thrash.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-lifecycle").unwrap().status.unwrap().phase,
        SandboxPhase::Running,
        "a just-resumed desiredPhase=Running sandbox must not instantly re-pause (#563)"
    );
}

/// #606: a `filesystem`-mode Sandbox pauses by deleting its Pod (retaining the
/// Sandbox-owned PVCs) and resumes by cold-booting a fresh Pod — no SnapshotJob,
/// no `snapshot_ref`, no node-adapter involvement at any step. This is the
/// runtime-agnostic suspend that makes Kata pausable.
///
/// PVC provisioning/retention/mount is covered by the `PodAdapter` tests; this
/// walks the controller's pause/resume *state machine* end to end and proves it
/// never touches the snapshot machinery in `filesystem` mode.
#[tokio::test(flavor = "current_thread")]
async fn filesystem_suspend_resume_round_trip() {
    let store = FakeSandboxStore::new();
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
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

    // Seed: a Running, `filesystem`-mode Sandbox with a live Pod + envd.
    let mut s = fresh_sandbox();
    s.spec.persistence_mode = Some(PersistenceMode::Filesystem);
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Running,
        pod_ref: Some("default/sbx-lifecycle-pod".into()),
        envd_address: Some("127.0.0.1:49983".into()),
        ..Default::default()
    });
    store.insert(s);

    // 1. Running + desired=Paused -> Pausing: the Pod is deleted (pod_ref
    //    cleared) and NO SnapshotJob is dispatched. The draining state records
    //    a `Suspended=False` (Suspending) condition.
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        s.spec.desired_phase = Some(SandboxPhase::Paused);
        store.insert(s);
    }
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let st = store.peek("sbx-lifecycle").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Pausing);
    assert!(
        st.pod_ref.is_none(),
        "a filesystem suspend deletes the Pod (clears pod_ref)"
    );
    assert!(st.snapshot_ref.is_none(), "no snapshot is taken");
    assert!(
        st.snapshot_job_ref.is_none(),
        "no SnapshotJob is dispatched"
    );
    assert!(
        jobs.list().is_empty(),
        "a filesystem suspend never creates a SnapshotJob"
    );
    assert!(
        st.conditions
            .iter()
            .any(|c| c.kind == "Suspended" && c.status == "False"),
        "the draining state records Suspended=False (Suspending)"
    );

    // 2. Pausing + Pod gone -> Paused: the completion probe folds `PodReleased`.
    //    Still no snapshot; the suspended state records `Suspended=True`.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let st = store.peek("sbx-lifecycle").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Paused);
    assert!(
        st.snapshot_ref.is_none(),
        "a filesystem pause commits no snapshot_ref"
    );
    assert!(
        st.conditions
            .iter()
            .any(|c| c.kind == "Suspended" && c.status == "True"),
        "the suspended state records Suspended=True"
    );
    assert!(jobs.list().is_empty());

    // 3. Paused + desired=Running -> Resuming: a fresh Pod is cold-booted
    //    (pod_ref set again) with no Stage / restore job.
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        s.spec.desired_phase = Some(SandboxPhase::Running);
        store.insert(s);
    }
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let st = store.peek("sbx-lifecycle").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Resuming);
    assert!(
        st.pod_ref.is_some(),
        "resume cold-boots a fresh Pod re-mounting the retained PVC(s)"
    );
    assert!(
        jobs.list().is_empty(),
        "a filesystem resume dispatches no Stage / restore SnapshotJob"
    );

    // 4. Resuming + Pod Ready -> Running: promoted straight through, no restore.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let st = store.peek("sbx-lifecycle").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Running);
    assert!(st.envd_address.is_some(), "the cold-booted Pod serves envd");
    assert!(
        jobs.list().is_empty(),
        "no SnapshotJob is created at any point in a filesystem round trip"
    );
}

/// #606 + #560: a `filesystem` cold-boot resume has no checkpoint to wedge, but
/// it still flows through the shared resume serve-readiness gate — a fresh Pod
/// is not promoted to `Running` until envd actually answers `GET /__state`, not
/// merely accepts TCP. Beneficial for a cold boot too: don't declare `Running`
/// over an envd that hasn't started serving yet.
#[tokio::test(flavor = "current_thread")]
async fn filesystem_resume_withholds_running_until_envd_serves() {
    let store = FakeSandboxStore::new();
    // Pod is kubelet-Ready but envd is not yet serving `/__state`.
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_envd_serving(false));
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

    // Seed: a `filesystem` Sandbox mid-resume with a fresh cold-booted Pod and
    // no `snapshot_ref` (so the cold-boot probe path, not the restore path).
    let mut s = fresh_sandbox();
    s.spec.persistence_mode = Some(PersistenceMode::Filesystem);
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Resuming,
        pod_ref: Some("default/sbx-lifecycle-pod".into()),
        ..Default::default()
    });
    store.insert(s);

    // Pass 1: envd not serving -> promotion withheld, Sandbox stays Resuming.
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let st = store.peek("sbx-lifecycle").unwrap().status.unwrap();
    assert_eq!(
        st.phase,
        SandboxPhase::Resuming,
        "a cold-boot resume must not promote to Running before envd serves /__state"
    );
    assert!(
        st.envd_address.is_none(),
        "the serve-readiness gate withholds envd_address while envd is not serving"
    );

    // Pass 2: envd starts serving -> promotes straight to Running, no restore.
    adapter.set_envd_serving(true);
    reconcile("default", "sbx-lifecycle", ctx.clone())
        .await
        .unwrap();
    let st = store.peek("sbx-lifecycle").unwrap().status.unwrap();
    assert_eq!(st.phase, SandboxPhase::Running);
    assert!(st.envd_address.is_some());
    assert!(
        jobs.list().is_empty(),
        "a filesystem cold-boot resume dispatches no SnapshotJob"
    );
}
