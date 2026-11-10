//! End-to-end proof that the Helm-surfaced restore knobs (epic #484, issue
//! #490) actually re-tune the `Resuming` recovery loop at runtime.
//!
//! Issue #489 proved the bounded recovery works with the *compile-time*
//! defaults (budget = 5, deadlines = 120s). Issue #490 made those values
//! configurable through env vars the controller Deployment sets from Helm. This
//! test closes the wiring gap the deploy phase owns: it overrides the budget and
//! the schedule deadline via env, then drives the same Pending-forever recovery
//! flow and asserts the Sandbox folds to `Failed (ResumeExhausted)` after the
//! *overridden* number of restarts — not the default 5.
//!
//! It lives in its own integration-test binary on purpose: the controller reads
//! each knob once through a process-global `OnceLock`, so the env override must
//! be set before the first reconcile and must not leak into the default-valued
//! tests in `reconcile.rs`. A single test per binary guarantees both.

use std::sync::Arc;

use sandbawks_controller::{
    reconcile, FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore,
    FakeSnapshotJobStore, PvcRetentionConfig, ReconcileContext,
};
use sandbawks_types::{ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus};

/// A recovery anchor far enough in the past that every deadline/backoff window
/// is already elapsed, so each pass acts on the lost Pod immediately.
const FAR_PAST: &str = "2000-01-01T00:00:00Z";

/// The overridden restart budget — deliberately smaller than the default 5 so a
/// pass-through to the binary's compile-time default would fail this test.
const OVERRIDE_MAX_RESTARTS: u32 = 2;

/// Seed a `Resuming` Sandbox mid-restore: a restore Pod exists, the upstream
/// snapshot URL is recorded, and the recovery anchor is `FAR_PAST` so a loss is
/// immediately actionable.
fn resuming_mid_restore(name: &str) -> Sandbox {
    let mut s = Sandbox::new(
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
    );
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Resuming,
        snapshot_ref: Some(format!("s3://sandbawks/{name}/manifest.json")),
        pod_ref: Some(format!("default/{name}-old")),
        resume_restart_count: 0,
        resume_last_attempt: Some(FAR_PAST.to_owned()),
        ..SandboxStatus::default()
    });
    s
}

/// Rewind the recovery anchor to `FAR_PAST` so the next pass's deadline/backoff
/// window is elapsed again (the recreate stamps it to "now").
fn rewind_resume_anchor(store: &FakeSandboxStore, name: &str) {
    let mut s = store.peek(name).expect("sandbox present");
    s.status
        .as_mut()
        .expect("status present")
        .resume_last_attempt = Some(FAR_PAST.to_owned());
    store.insert(s);
}

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
async fn env_override_resizes_the_resume_recovery_budget() {
    // Set the knobs the controller Deployment renders from
    // `controller.restore.*` BEFORE the first reconcile, so the controller's
    // `OnceLock`-backed accessors cache the overrides rather than the defaults.
    // Edition 2021: `set_var` is safe (no other threads read these yet — this is
    // the only test in this binary).
    std::env::set_var(
        "SANDBAWKS_RESUME_MAX_RESTARTS",
        OVERRIDE_MAX_RESTARTS.to_string(),
    );
    // A 1s schedule deadline keeps the test honest about *which* deadline trips
    // (the Pod is never scheduled), independent of the default 120s.
    std::env::set_var("SANDBAWKS_RESUME_SCHEDULE_DEADLINE_SECS", "1");

    let store = FakeSandboxStore::new();
    store.insert(resuming_mid_restore("sbx-cfg"));
    let jobs = FakeSnapshotJobStore::new();
    // Never scheduled (no `with_scheduled_node`), never Ready → always Pending,
    // so every pass sees the Pod Pending past the (overridden) schedule deadline.
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_ready(false));

    // Each pass spends one retry, up to the OVERRIDDEN budget. With the default
    // budget of 5 this loop would still be `Resuming` here, so reaching the fold
    // below after only `OVERRIDE_MAX_RESTARTS` passes proves the env knob took.
    for expected_count in 1..=OVERRIDE_MAX_RESTARTS {
        rewind_resume_anchor(&store, "sbx-cfg");
        reconcile(
            "default",
            "sbx-cfg",
            ctx(store.clone(), adapter.clone(), jobs.clone()),
        )
        .await
        .unwrap();
        let s = store.peek("sbx-cfg").unwrap().status.unwrap();
        assert_eq!(
            s.phase,
            SandboxPhase::Resuming,
            "still recovering at attempt {expected_count} under the overridden budget"
        );
        assert_eq!(
            s.resume_restart_count, expected_count,
            "each Pending-past-deadline pass spends exactly one retry"
        );
    }

    // One more pass: the overridden budget is spent → fold to Failed. Under the
    // default budget of 5 this would still be `Resuming`.
    rewind_resume_anchor(&store, "sbx-cfg");
    reconcile(
        "default",
        "sbx-cfg",
        ctx(store.clone(), adapter.clone(), jobs.clone()),
    )
    .await
    .unwrap();
    let failed = store.peek("sbx-cfg").unwrap().status.unwrap();
    assert_eq!(
        failed.phase,
        SandboxPhase::Failed,
        "the env-overridden budget ({OVERRIDE_MAX_RESTARTS}) must bound recovery, \
         not the compile-time default of 5"
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
