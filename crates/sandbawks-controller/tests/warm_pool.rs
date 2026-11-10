//! Unit + reconcile tests for the `SandboxWarmPool` reconciler (issue #332).
//!
//! The pure decision functions (`status_for`, `plan_pool`,
//! `warm_sandbox`) are exercised directly; `reconcile_pool` runs against
//! the in-memory `FakeWarmPoolStore` so the create/trim edges are asserted
//! without a cluster. Members are held `Running` for an instant claim, so
//! readiness is `Running` and there is no auto-pause edge.

use std::collections::BTreeMap;

use kube::ResourceExt;
use sandbawks_controller::warm_pool::{
    plan_pool, reconcile_pool, status_for, warm_sandbox, FakeWarmPoolStore, PoolMembers,
    WarmSandboxDefaults,
};
use sandbawks_types::warm_pool::{TEMPLATE_RV_ANNOTATION, WARM_POOL_LABEL};
use sandbawks_types::{
    ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus, SandboxWarmPool,
    SandboxWarmPoolSpec,
};

/// A bare `Sandbox` carrying `name` and the warm-pool label for `pool`.
fn member(name: &str, pool: &str) -> Sandbox {
    let mut s = Sandbox::new(
        name,
        SandboxSpec {
            template_ref: "python-data-sci".into(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
                ephemeral_storage: None,
            },
            timeout_seconds: 0,
            auto_pause: true,
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
    let mut labels = BTreeMap::new();
    labels.insert(WARM_POOL_LABEL.to_owned(), pool.to_owned());
    s.metadata.labels = Some(labels);
    s
}

/// `member` with an observed `phase`.
fn member_in_phase(name: &str, pool: &str, phase: SandboxPhase) -> Sandbox {
    let mut s = member(name, pool);
    s.status = Some(SandboxStatus {
        phase,
        ..SandboxStatus::default()
    });
    s
}

/// Stamp the `TEMPLATE_RV_ANNOTATION` on a member (issue #397), mirroring what
/// `warm_sandbox` records at mint time so `bucket`/`reconcile_pool` version
/// checks can be exercised.
fn with_template_rv(mut s: Sandbox, rv: &str) -> Sandbox {
    s.metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(TEMPLATE_RV_ANNOTATION.to_owned(), rv.to_owned());
    s
}

fn test_pool(name: &str, template: &str, min: u32, max: u32) -> SandboxWarmPool {
    let mut p = SandboxWarmPool::new(
        name,
        SandboxWarmPoolSpec {
            template_ref: template.into(),
            min_ready: min,
            max_ready: max,
        },
    );
    // Reconcile resolves the namespace off the object; cluster-scoped
    // ownership of namespaced sandboxes is rejected, so fixtures must set
    // one to mirror a real (namespaced) `SandboxWarmPool`.
    p.metadata.namespace = Some("team-acme".into());
    // A watch-observed pool always carries an apiserver-stamped UID;
    // `reconcile_pool` refuses to create warm sandboxes without one (else
    // their ownerReference can't drive GC), so fixtures must set a
    // synthetic UID to mirror reality.
    p.metadata.uid = Some("pool-uid-0001".into());
    p
}

/// Stamp an RFC3339 `creationTimestamp` on a member (for trim-order tests).
fn with_creation(mut s: Sandbox, rfc3339: &str) -> Sandbox {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    let ts = chrono::DateTime::parse_from_rfc3339(rfc3339)
        .expect("valid rfc3339")
        .with_timezone(&chrono::Utc);
    s.metadata.creation_timestamp = Some(Time(ts));
    s
}

fn running_n(pool: &str, n: usize) -> Vec<Sandbox> {
    (0..n)
        .map(|i| member_in_phase(&format!("sbx-p-{i}"), pool, SandboxPhase::Running))
        .collect()
}

fn pending_n(pool: &str, n: usize) -> Vec<Sandbox> {
    (0..n)
        .map(|i| member_in_phase(&format!("sbx-c-{i}"), pool, SandboxPhase::Creating))
        .collect()
}

// ---------- status_for ----------

#[test]
fn status_counts_only_running_members() {
    let members = PoolMembers {
        ready: vec![
            member_in_phase("sbx-warm-a", "p", SandboxPhase::Running),
            member_in_phase("sbx-warm-b", "p", SandboxPhase::Running),
        ],
        pending: vec![member_in_phase("sbx-warm-c", "p", SandboxPhase::Creating)],
        ..Default::default()
    };
    let st = status_for(&members);
    assert_eq!(st.ready, 2);
    assert_eq!(
        st.members,
        vec!["sbx-warm-a".to_string(), "sbx-warm-b".to_string()]
    );
    // No failed members; total counts the in-flight one too (issue #457).
    assert_eq!(st.failed, 0);
    assert_eq!(st.total, 3);
}

#[test]
fn status_surfaces_failed_and_total_counts() {
    // Issue #457: a pool whose members are all Failed must report a healthy-
    // looking ready=0 *and* a non-zero failed/total, so the churn is visible
    // in `kubectl get`/`describe` rather than hidden.
    let members = PoolMembers {
        failed: vec![
            member_in_phase("sbx-warm-x", "p", SandboxPhase::Failed),
            member_in_phase("sbx-warm-y", "p", SandboxPhase::Terminating),
        ],
        ..Default::default()
    };
    let st = status_for(&members);
    assert_eq!(st.ready, 0);
    assert!(st.members.is_empty());
    assert_eq!(st.failed, 2);
    assert_eq!(st.total, 2);
}

#[test]
fn bucket_routes_failed_and_terminating_to_failed() {
    // Issue #457: Failed/Terminating members are NOT dropped — they land in
    // `failed` so the reconcile can reap them. Owner-GC never collects them
    // while the pool is alive, so dropping them silently leaked CRs without
    // bound on a transient pod-create failure.
    let all = vec![
        member_in_phase("ok", "p", SandboxPhase::Running),
        member_in_phase("boot", "p", SandboxPhase::Creating),
        member_in_phase("dead", "p", SandboxPhase::Failed),
        member_in_phase("gone", "p", SandboxPhase::Terminating),
    ];
    let members = PoolMembers::bucket(all, None);
    assert_eq!(members.ready.len(), 1); // only the Running one
    assert_eq!(members.pending.len(), 1); // only the Creating one
    assert!(members.stale.is_empty()); // nothing Paused/Pausing
    let mut failed: Vec<String> = members.failed.iter().map(ResourceExt::name_any).collect();
    failed.sort();
    assert_eq!(failed, vec!["dead", "gone"]);
    // All four members are counted in the true CR total, not just the two
    // useful ones.
    assert_eq!(members.total(), 4);
}

#[test]
fn bucket_routes_legacy_paused_members_to_stale() {
    // Rolling-deploy artifacts: the old controller auto-paused warm members,
    // so a fresh deploy can observe Paused/Pausing members. They must NOT
    // land in `pending` (that would count them toward the ceiling and wedge
    // top-up) — they go to `stale` for reclamation instead.
    let all = vec![
        member_in_phase("live", "p", SandboxPhase::Running),
        member_in_phase("legacy-paused", "p", SandboxPhase::Paused),
        member_in_phase("legacy-pausing", "p", SandboxPhase::Pausing),
    ];
    let members = PoolMembers::bucket(all, None);
    assert_eq!(members.ready.len(), 1);
    assert!(members.pending.is_empty());
    let mut stale: Vec<String> = members.stale.iter().map(ResourceExt::name_any).collect();
    stale.sort();
    assert_eq!(stale, vec!["legacy-paused", "legacy-pausing"]);
}

// ---------- bucket: template-version staleness (#397) ----------

#[test]
fn bucket_routes_version_mismatched_running_members_to_version_stale() {
    // current_rv = "100": a Running member stamped with an old RV ("99") is
    // version-stale; one stamped with the current RV ("100") stays ready; one
    // with no annotation (minted before #397) is also version-stale.
    let all = vec![
        with_template_rv(member_in_phase("old", "p", SandboxPhase::Running), "99"),
        with_template_rv(
            member_in_phase("current", "p", SandboxPhase::Running),
            "100",
        ),
        member_in_phase("unannotated", "p", SandboxPhase::Running),
    ];
    let members = PoolMembers::bucket(all, Some("100"));
    let ready: Vec<String> = members.ready.iter().map(ResourceExt::name_any).collect();
    assert_eq!(ready, vec!["current"]);
    let mut stale: Vec<String> = members
        .version_stale
        .iter()
        .map(ResourceExt::name_any)
        .collect();
    stale.sort();
    assert_eq!(stale, vec!["old", "unannotated"]);
}

#[test]
fn bucket_never_evicts_when_current_version_unknown() {
    // current_rv = None (template-lookup miss): the version is unknown, so no
    // member is evicted against it — even an unannotated one stays ready. This
    // is the lenient-miss posture that keeps a transient miss from churning the
    // whole pool.
    let all = vec![
        with_template_rv(member_in_phase("a", "p", SandboxPhase::Running), "99"),
        member_in_phase("b", "p", SandboxPhase::Running),
    ];
    let members = PoolMembers::bucket(all, None);
    assert_eq!(members.ready.len(), 2);
    assert!(members.version_stale.is_empty());
}

// ---------- plan_pool ----------

#[test]
fn plan_creates_up_to_min_counting_in_flight() {
    let m = PoolMembers {
        ready: running_n("p", 1),
        pending: pending_n("p", 1),
        ..Default::default()
    }; // total 2
    let plan = plan_pool(5, 10, &m);
    assert_eq!(plan.create, 3); // 5 - 2
    assert!(plan.delete.is_empty());
}

#[test]
fn plan_never_exceeds_max() {
    let m = PoolMembers {
        ready: running_n("p", 0),
        pending: pending_n("p", 0),
        ..Default::default()
    };
    let plan = plan_pool(20, 3, &m); // misconfig: min > max
    assert_eq!(plan.create, 3); // clamped to max
}

#[test]
fn plan_at_floor_creates_nothing() {
    let m = PoolMembers {
        ready: running_n("p", 5),
        pending: pending_n("p", 0),
        ..Default::default()
    };
    let plan = plan_pool(5, 10, &m);
    assert_eq!(plan.create, 0);
    assert!(plan.delete.is_empty());
}

#[test]
fn plan_trims_running_excess_oldest_first() {
    // No creationTimestamps set -> all sort equal on the timestamp key, so
    // the name tiebreak orders them: sbx-a, sbx-b, sbx-c -> trim a, b.
    let m = PoolMembers {
        ready: vec![
            member_in_phase("sbx-c", "p", SandboxPhase::Running),
            member_in_phase("sbx-a", "p", SandboxPhase::Running),
            member_in_phase("sbx-b", "p", SandboxPhase::Running),
        ],
        pending: vec![],
        ..Default::default()
    };
    let plan = plan_pool(1, 1, &m);
    assert_eq!(plan.create, 0);
    assert_eq!(plan.delete, vec!["sbx-a".to_string(), "sbx-b".to_string()]);
}

#[test]
fn plan_trims_by_creation_timestamp_over_name() {
    // Name order (zzz < ... ) would pick differently than age; the oldest
    // member by creationTimestamp must be trimmed first even though its
    // name sorts last.
    let older = with_creation(
        member_in_phase("sbx-zzz-old", "p", SandboxPhase::Running),
        "2026-06-01T00:00:00Z",
    );
    let newer = with_creation(
        member_in_phase("sbx-aaa-new", "p", SandboxPhase::Running),
        "2026-06-02T00:00:00Z",
    );
    let m = PoolMembers {
        ready: vec![newer, older],
        pending: vec![],
        ..Default::default()
    };
    let plan = plan_pool(1, 1, &m);
    assert_eq!(plan.create, 0);
    // Oldest (sbx-zzz-old) trimmed despite its name sorting last.
    assert_eq!(plan.delete, vec!["sbx-zzz-old".to_string()]);
}

// ---------- warm_sandbox constructor ----------

#[test]
fn warm_sandbox_carries_pool_label_and_owner_ref() {
    let pool = test_pool("python-pool", "python-data-sci", 5, 20);
    let defaults = WarmSandboxDefaults {
        runtime_class_name: Some("gvisor".into()),
        resources: Some(ResourceRequests {
            cpu: "2".parse().unwrap(),
            memory: "2048Mi".parse().unwrap(),
            ephemeral_storage: None,
        }),
        template_resource_version: Some("4242".into()),
        network_policy: None,
    };
    let sb = warm_sandbox(&pool, &pool.spec.template_ref, &defaults);
    assert_eq!(
        sb.labels().get(WARM_POOL_LABEL).map(String::as_str),
        Some("python-pool")
    );
    // Issue #397: the member records the template's `resourceVersion` so a
    // later reconcile can evict it once the template is edited.
    assert_eq!(
        sb.annotations()
            .get(TEMPLATE_RV_ANNOTATION)
            .map(String::as_str),
        Some("4242")
    );
    assert_eq!(sb.spec.template_ref, "python-data-sci");
    assert!(sb
        .owner_references()
        .iter()
        .any(|o| o.kind == "SandboxWarmPool" && o.name == "python-pool"));
    // No client-side name — the apiserver assigns one from generateName.
    assert!(sb.metadata.name.is_none());
    // Issue #495: the pool name is folded into the prefix so a member's
    // apiserver-assigned name self-identifies its owning pool.
    assert_eq!(
        sb.metadata.generate_name.as_deref(),
        Some("sbx-python-pool-")
    );
    // Load-bearing: the warm sandbox must adopt the template's runtime
    // class or a later requester pause/resume fails under runc.
    assert_eq!(sb.spec.runtime_class_name.as_deref(), Some("gvisor"));
    // Minted with the disabled-timeout sentinel so the member never
    // auto-pauses while sitting unclaimed in the pool (#380). The real
    // timeout is stamped at claim time by the gateway's `apply_overrides`.
    assert_eq!(sb.spec.timeout_seconds, 0);
    // Issue #395: the member is sized from the template's `defaultResources`,
    // not the hardcoded `1` CPU / `1Gi` placeholder.
    assert_eq!(sb.spec.resources.cpu.to_string(), "2");
    assert_eq!(sb.spec.resources.memory.to_string(), "2048Mi");
}

/// Issue #537: a warm member inherits the template's egress baseline at mint
/// time, so a pool-backed sandbox reaches its allowed destinations without the
/// (impossible-for-the-auto-claim-flow) hand-edit of the claimed CR. Before
/// this, the mint hardcoded `network_policy: None` and the only egress surface
/// was editing the live Sandbox.
#[test]
fn warm_sandbox_inherits_template_egress_baseline() {
    use sandbawks_types::NetworkPolicy;
    let pool = test_pool("egress-pool", "egress-tpl", 1, 3);
    let defaults = WarmSandboxDefaults {
        runtime_class_name: Some("gvisor".into()),
        resources: Some(ResourceRequests {
            cpu: "1".parse().unwrap(),
            memory: "1Gi".parse().unwrap(),
            ephemeral_storage: None,
        }),
        template_resource_version: Some("7".into()),
        network_policy: Some(NetworkPolicy {
            egress_allow: vec!["git.example.com".into(), "10.0.0.0/8".into()],
        }),
    };
    let sb = warm_sandbox(&pool, &pool.spec.template_ref, &defaults);
    let egress = sb
        .spec
        .network_policy
        .as_ref()
        .expect("warm member carries the template egress baseline");
    assert_eq!(egress.egress_allow, vec!["git.example.com", "10.0.0.0/8"]);
}

/// A template with no egress config leaves the minted member at the
/// default-deny `None` baseline — no empty `networkPolicy` is stamped.
#[test]
fn warm_sandbox_no_egress_when_template_sets_none() {
    let pool = test_pool("p", "tpl", 1, 1);
    let defaults = WarmSandboxDefaults {
        runtime_class_name: Some("gvisor".into()),
        resources: None,
        template_resource_version: None,
        network_policy: None,
    };
    let sb = warm_sandbox(&pool, &pool.spec.template_ref, &defaults);
    assert!(sb.spec.network_policy.is_none());
}

/// A template-lookup miss leaves `defaults.resources` `None`; the member
/// still carries the schema-shaped `1` CPU / `1Gi` placeholder so the pool
/// is not wedged by a momentarily-missing template (issue #395).
#[test]
fn warm_sandbox_falls_back_to_placeholder_resources_on_template_miss() {
    let pool = test_pool("p", "tpl", 1, 1);
    let defaults = WarmSandboxDefaults {
        runtime_class_name: None,
        resources: None,
        template_resource_version: None,
        network_policy: None,
    };
    let sb = warm_sandbox(&pool, &pool.spec.template_ref, &defaults);
    assert_eq!(sb.spec.resources.cpu.to_string(), "1");
    assert_eq!(sb.spec.resources.memory.to_string(), "1Gi");
    // Issue #397: a template-lookup miss carries no version, so the member is
    // left unannotated rather than stamped with a placeholder RV.
    assert!(sb.annotations().get(TEMPLATE_RV_ANNOTATION).is_none());
}

/// Issue #495: a near-max-length pool name must yield a `generateName` within
/// the apiserver's 58-char prefix budget (it appends a 5-char suffix) and
/// still end in a single clean `-`, so the apiserver never silently truncates
/// our prefix or strips the trailing separator.
#[test]
fn warm_sandbox_truncates_overlong_pool_name_in_generate_name() {
    // The pool segment is cut at `58 - len("sbx-") - len("-") = 53` chars.
    // Place a hyphen at index 52 (the 53rd char) so it is the *last kept*
    // char of the cut, then trimmed away — proving the trailing-hyphen trim
    // fires and the prefix can never read `sbx-...--<suffix>`. The 20 `b`s
    // past the cut are discarded.
    let long_name = format!("{}-{}", "a".repeat(52), "b".repeat(20));
    let pool = test_pool(&long_name, "tpl", 1, 1);
    let defaults = WarmSandboxDefaults {
        runtime_class_name: None,
        resources: None,
        template_resource_version: None,
        network_policy: None,
    };
    let sb = warm_sandbox(&pool, &pool.spec.template_ref, &defaults);
    let prefix = sb
        .metadata
        .generate_name
        .as_deref()
        .expect("warm member must carry a generateName");
    // Exact expected value: `sbx-` + the 52 kept `a`s + a single `-`. The
    // boundary hyphen and everything after it are gone. This pins down the
    // trim unambiguously — without it the prefix would be the 58-char
    // `sbx-<52 a's>--`, ending in a double separator.
    let expected = format!("sbx-{}-", "a".repeat(52));
    assert_eq!(prefix, expected);
    // ...and that exact value sits within the apiserver's 58-char budget
    // (here 57), so the assigned name is never silently truncated.
    assert_eq!(prefix.len(), 57);
    assert!(prefix.len() <= 58);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_stamps_runtime_class_on_created_warm_sandboxes() {
    let pool = test_pool("p", "tpl", 2, 5);
    let store = FakeWarmPoolStore::new().with_defaults(WarmSandboxDefaults {
        runtime_class_name: Some("gvisor".into()),
        resources: Some(ResourceRequests {
            cpu: "2".parse().unwrap(),
            memory: "2048Mi".parse().unwrap(),
            ephemeral_storage: None,
        }),
        template_resource_version: Some("7".into()),
        network_policy: None,
    });

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    let created = store.created.lock().unwrap();
    assert_eq!(created.len(), 2);
    assert!(
        created
            .iter()
            .all(|s| s.spec.runtime_class_name.as_deref() == Some("gvisor")),
        "every warm sandbox must carry the resolved runtimeClassName",
    );
    // Issue #395: members are sized from the template's `defaultResources`.
    assert!(
        created
            .iter()
            .all(|s| s.spec.resources.cpu.to_string() == "2"
                && s.spec.resources.memory.to_string() == "2048Mi"),
        "every warm sandbox must carry the resolved defaultResources",
    );
}

// ---------- reconcile_pool against the fake store ----------

#[tokio::test(flavor = "current_thread")]
async fn reconcile_tops_up_to_min() {
    let pool = test_pool("p", "tpl", 3, 10);
    let store = FakeWarmPoolStore::new();
    store.insert_member(member_in_phase("sbx-p-0", "p", SandboxPhase::Running));

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    // ready=1 observed, floor=3 → create 2 toward the floor.
    assert_eq!(store.created.lock().unwrap().len(), 2);
    assert!(store.deleted.lock().unwrap().is_empty());
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_trims_excess() {
    let pool = test_pool("p", "tpl", 1, 1);
    let store = FakeWarmPoolStore::new();
    for name in ["sbx-b", "sbx-a", "sbx-c"] {
        store.insert_member(member_in_phase(name, "p", SandboxPhase::Running));
    }

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    assert!(store.created.lock().unwrap().is_empty());
    // 3 running, max 1 → trim 2 oldest-by-name.
    assert_eq!(
        *store.deleted.lock().unwrap(),
        vec!["sbx-a".to_string(), "sbx-b".to_string()]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_holds_running_member_without_pausing() {
    // A booted Running member at the floor is left exactly as-is: counted
    // ready, never trimmed, and (critically for #332) never driven to
    // Paused. With min met, no fresh boot is needed either.
    //
    // Note: the *absence* of a pause is now a structural guarantee, not
    // something these assertions probe directly — `WarmPoolStore` no longer
    // has a `patch_desired_phase` method for the reconcile to call, so there
    // is no pause channel left to observe. The empty `created`/`deleted` and
    // the held `ready=1` below are the strongest assertions available.
    let pool = test_pool("p", "tpl", 1, 5);
    let store = FakeWarmPoolStore::new();
    store.insert_member(member_in_phase("sbx-run", "p", SandboxPhase::Running));

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    assert!(store.created.lock().unwrap().is_empty());
    assert!(store.deleted.lock().unwrap().is_empty());
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 1);
    assert_eq!(status.members, vec!["sbx-run".to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_reclaims_legacy_paused_members_and_refills() {
    // Rolling-deploy self-heal: a legacy `Paused` member (auto-paused by the
    // old controller) must be deleted, not left to wedge the pool below its
    // floor. With min=1 and only a Paused member observed, the reconcile
    // deletes the legacy member AND creates a fresh one toward the floor.
    let pool = test_pool("p", "tpl", 1, 2);
    let store = FakeWarmPoolStore::new();
    store.insert_member(member_in_phase("legacy", "p", SandboxPhase::Paused));

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    // Reclaimed (Paused members are excluded from `total`, so top-up still
    // fires) and a fresh member is created to restore the floor.
    assert_eq!(*store.deleted.lock().unwrap(), vec!["legacy".to_string()]);
    assert_eq!(store.created.lock().unwrap().len(), 1);
    // Status counts only Running members, so a pool of one Paused legacy
    // member reports ready=0 until the fresh boot comes up.
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_reaps_failed_members_and_refills() {
    // Issue #457: a transient pod-create rejection leaves Failed members that
    // owner-GC never collects while the pool is alive. The reconcile must
    // delete every Failed/Terminating member AND top up toward the floor, so
    // the standing Failed count is bounded instead of growing each pass. Here
    // min=2 with two Failed and zero Running observed: reap both, mint two.
    let pool = test_pool("p", "tpl", 2, 5);
    let store = FakeWarmPoolStore::new();
    store.insert_member(member_in_phase("dead-1", "p", SandboxPhase::Failed));
    store.insert_member(member_in_phase("dead-2", "p", SandboxPhase::Terminating));

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    let mut deleted = store.deleted.lock().unwrap().clone();
    deleted.sort();
    assert_eq!(deleted, vec!["dead-1".to_string(), "dead-2".to_string()]);
    // Failed members count toward neither ready nor pending, so top-up sees 0
    // ready against a floor of 2 and mints two fresh members.
    assert_eq!(store.created.lock().unwrap().len(), 2);
    // Status reports the failure: ready=0 but failed/total expose the churn
    // that was previously invisible.
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 0);
    assert_eq!(status.failed, 2);
    assert_eq!(status.total, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_reaps_all_failed_members_regardless_of_floor() {
    // Issue #457 bound: the reap must drain *every* observed Failed member in a
    // single pass, so the standing population is bounded by what the apiserver
    // shows — not by `min_ready`, `plan.create`, or any other accidental cap.
    // A regression that capped the reap loop (e.g. `.take(plan.create)`) would
    // still pass `reconcile_reaps_failed_members_and_refills` but fail here,
    // since 50 garbage members would only be whittled down 2-at-a-time.
    const FAILED: usize = 50;
    let pool = test_pool("p", "tpl", 2, 5);
    let store = FakeWarmPoolStore::new();
    for i in 0..FAILED {
        store.insert_member(member_in_phase(
            &format!("dead-{i}"),
            "p",
            SandboxPhase::Failed,
        ));
    }

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    // All 50 reaped this pass — not min_ready (2) of them.
    assert_eq!(store.deleted.lock().unwrap().len(), FAILED);
    // Top-up is still bounded by the floor, independent of the reap count.
    assert_eq!(store.created.lock().unwrap().len(), 2);
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 0);
    assert_eq!(status.failed, FAILED as u32);
    assert_eq!(status.total, FAILED as u32);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_ignores_foreign_pool_members() {
    let pool = test_pool("p", "tpl", 2, 5);
    let store = FakeWarmPoolStore::new();
    store.insert_member(member_in_phase("mine", "p", SandboxPhase::Running));
    store.insert_member(member_in_phase(
        "theirs",
        "other-pool",
        SandboxPhase::Running,
    ));

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    // Only "mine" counts → ready=1, create 1 toward floor of 2.
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 1);
    assert_eq!(store.created.lock().unwrap().len(), 1);
}

// ---------- reconcile_pool: template-version eviction (#397) ----------

#[tokio::test(flavor = "current_thread")]
async fn reconcile_evicts_version_stale_member_and_refills() {
    // The template was edited: its current RV is "200" but the lone Running
    // member was minted against "100". The reconcile must delete the stale
    // member AND create a fresh one toward the floor, so a requester never
    // claims a sandbox built from the superseded template.
    let pool = test_pool("p", "tpl", 1, 2);
    let store = FakeWarmPoolStore::new().with_defaults(WarmSandboxDefaults {
        runtime_class_name: None,
        resources: None,
        template_resource_version: Some("200".into()),
        network_policy: None,
    });
    store.insert_member(with_template_rv(
        member_in_phase("stale", "p", SandboxPhase::Running),
        "100",
    ));

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    assert_eq!(*store.deleted.lock().unwrap(), vec!["stale".to_string()]);
    // Version-stale members are excluded from `ready`, so top-up sees 0 ready
    // against a floor of 1 and mints one fresh member from the new template.
    assert_eq!(store.created.lock().unwrap().len(), 1);
    // The fresh member carries the current RV.
    assert_eq!(
        store.created.lock().unwrap()[0]
            .annotations()
            .get(TEMPLATE_RV_ANNOTATION)
            .map(String::as_str),
        Some("200")
    );
    // Only the surviving (none) Running members are counted ready.
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_leaves_current_version_member_untouched() {
    // The member's stamped RV matches the template's current RV: it is current,
    // so it is neither evicted nor re-minted. With the floor met, nothing is
    // created either.
    let pool = test_pool("p", "tpl", 1, 5);
    let store = FakeWarmPoolStore::new().with_defaults(WarmSandboxDefaults {
        runtime_class_name: None,
        resources: None,
        template_resource_version: Some("200".into()),
        network_policy: None,
    });
    store.insert_member(with_template_rv(
        member_in_phase("current", "p", SandboxPhase::Running),
        "200",
    ));

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    assert!(store.deleted.lock().unwrap().is_empty());
    assert!(store.created.lock().unwrap().is_empty());
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 1);
    assert_eq!(status.members, vec!["current".to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_never_touches_claimed_member_on_template_update() {
    // A claimed member has had `WARM_POOL_LABEL` removed at claim time, so the
    // label-selected `list_members` never returns it — a template edit can't
    // evict an in-use sandbox. Seed a member missing the label (as a claim
    // leaves it) carrying a stale RV; it must be invisible to the reconcile.
    let pool = test_pool("p", "tpl", 1, 5);
    let store = FakeWarmPoolStore::new().with_defaults(WarmSandboxDefaults {
        runtime_class_name: None,
        resources: None,
        template_resource_version: Some("200".into()),
        network_policy: None,
    });
    let mut claimed = with_template_rv(
        member_in_phase("claimed", "p", SandboxPhase::Running),
        "100",
    );
    claimed.metadata.labels = None; // claim removes WARM_POOL_LABEL
    store.insert_member(claimed);

    reconcile_pool(&pool, &store).await.expect("reconcile ok");

    // The claimed member is never listed, so it is never evicted; the pool just
    // sees an empty fleet and tops up toward the floor.
    assert!(store.deleted.lock().unwrap().is_empty());
    assert_eq!(store.created.lock().unwrap().len(), 1);
    let status = store.last_status.lock().unwrap().clone().unwrap();
    assert_eq!(status.ready, 0);
}
