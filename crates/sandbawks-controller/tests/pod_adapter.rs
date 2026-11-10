//! `PodAdapter` integration test using `tower-test::mock::pair` to
//! intercept the kube HTTP traffic.
//!
//! We don't want a real `kind`/`minikube` round-trip here — that lives
//! in the Plan 04 T15 end-to-end test. Instead we hand the controller's
//! `kube::Client` a `tower-test::mock::Mock` so we can assert on the
//! exact `POST /api/v1/namespaces/{ns}/pods` request `create_pod`
//! issues, including the rendered Pod JSON (image, owner-ref absent
//! when uid missing). `runtimeClassName` pass-through is asserted in
//! dedicated `pod_spec_for`-level unit tests further down.

use http::{Request, Response};
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, TypedLocalObjectReference};
use k8s_openapi::api::networking::v1::NetworkPolicy as NetworkPolicyResource;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::DynamicObject;
use kube::{client::Body, Client};
use sandbawks_controller::PodAdapter;
use sandbawks_types::{
    NetworkPolicy, ResourceRequests, RestorePlacement, RuntimeAdapter, Sandbox, SandboxSpec,
    VolumeClaimSpec, VolumeClaimTemplate, VolumeClaimTemplateMetadata,
};

/// The workload container image (the customer's rootfs, from
/// `SandboxTemplate.spec.image`). envd is staged onto the node and
/// mounted in read-only — it is not part of this image.
const WORKLOAD_IMAGE: &str = "ghcr.io/hypotheosis/workload:test";

/// Build a minimal Sandbox suitable for `create_pod`.
///
/// Mirrors what the API gateway will hand the reconcile loop in Plan
/// 04 T6 — minus the kube-server-generated `metadata.uid`, which we
/// leave unset so the test asserts the owner-ref path's "no uid → no
/// ownerReferences" behavior.
fn fixture_sandbox() -> Sandbox {
    let mut s = Sandbox::new(
        "sbx-test",
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
            env: Default::default(),
            metadata: Default::default(),
            network_policy: Some(NetworkPolicy::default()),
            desired_phase: None,
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
    s.metadata.namespace = Some("team-acme".into());
    s
}

/// A `workspace` VolumeClaimTemplate fixture for the PVC-clone tests.
fn workspace_vct() -> VolumeClaimTemplate {
    VolumeClaimTemplate {
        metadata: VolumeClaimTemplateMetadata {
            name: "workspace".into(),
            ..Default::default()
        },
        spec: VolumeClaimSpec {
            storage: "10Gi".parse().expect("valid storage quantity"),
            access_modes: vec!["ReadWriteOnce".into()],
            storage_class_name: None,
            volume_mode: None,
        },
    }
}

#[test]
fn pvc_for_clones_source_pvc_via_data_source_for_sandbox_id_restore() {
    // Issue #416, filesystem clone: a sandbox with `restore_from` naming a
    // *source sandbox* CSI-clones each PVC from the source's
    // correspondingly-named retained PVC via `spec.dataSource`.
    let mut sandbox = fixture_sandbox();
    sandbox.spec.restore_from = Some("sbx-source".into());
    let pvc = PodAdapter::pvc_for(&sandbox, &workspace_vct());

    // This sandbox's own PVC keeps the `{template}-{sandbox}` name...
    assert_eq!(pvc.metadata.name.as_deref(), Some("workspace-sbx-test"));
    // ...and clones from the source's correspondingly-named PVC.
    assert_eq!(
        pvc.spec.as_ref().and_then(|s| s.data_source.clone()),
        Some(TypedLocalObjectReference {
            api_group: None,
            kind: "PersistentVolumeClaim".into(),
            name: "workspace-sbx-source".into(),
        }),
    );
}

#[test]
fn pvc_for_omits_data_source_without_restore_from() {
    // A normal (non-clone) sandbox provisions an empty PVC: no `dataSource`.
    let sandbox = fixture_sandbox();
    let pvc = PodAdapter::pvc_for(&sandbox, &workspace_vct());
    assert!(pvc
        .spec
        .as_ref()
        .and_then(|s| s.data_source.as_ref())
        .is_none());
}

#[test]
fn pvc_for_omits_data_source_for_s3_restore_source() {
    // An `s3://` restore source is snapshot-mode (no PVC clone); a filesystem
    // sandbox would never pair with it, but `pvc_for` must not synthesize a
    // bogus PVC `dataSource` from a URL.
    let mut sandbox = fixture_sandbox();
    sandbox.spec.restore_from = Some("s3://snaps/snapshots/sbx-x/manifest.json".into());
    let pvc = PodAdapter::pvc_for(&sandbox, &workspace_vct());
    assert!(pvc
        .spec
        .as_ref()
        .and_then(|s| s.data_source.as_ref())
        .is_none());
}

/// Consume the egress NetworkPolicy POST that `create_pod` issues *before* the
/// Pod (issue #443) and ack it with `status`:
///
/// * `201` — the normal create-success path; a minimal NetworkPolicy object is
///   echoed back as the created resource.
/// * `409` — exercises the policy's create-or-409 idempotency branch (a stale
///   policy left by a racing reconcile), which `create_pod` must treat as
///   success and proceed to the Pod.
///
/// Asserts the request is the expected `POST .../networkpolicies`. Kept as a
/// helper so every `create_pod` mock test threads the same first hop without
/// duplicating the boilerplate.
async fn ack_network_policy_post(
    handle: &mut tower_test::mock::Handle<Request<Body>, Response<Body>>,
    status: u16,
) {
    let (req, send) = handle
        .next_request()
        .await
        .expect("create_pod should POST the egress NetworkPolicy first");
    assert_eq!(req.method(), "POST", "expected POST for NetworkPolicy");
    assert!(
        req.uri()
            .path()
            .ends_with("/namespaces/team-acme/networkpolicies"),
        "unexpected NetworkPolicy path: {}",
        req.uri().path()
    );
    let body = if status == 409 {
        serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "reason": "AlreadyExists",
            "message": "networkpolicies \"sandbox-sbx-test-egress\" already exists",
            "code": 409,
        })
    } else {
        // Echo a minimal NetworkPolicy back as the created resource so the kube
        // client's create call deserializes a response and returns Ok.
        serde_json::to_value(NetworkPolicyResource::default())
            .expect("serialize NetworkPolicy response")
    };
    send.send_response(
        Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&body).expect("serialize NetworkPolicy ack body"),
            ))
            .expect("build NetworkPolicy response"),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn create_pod_posts_expected_pod_to_kube() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let sandbox = fixture_sandbox();

    // The adapter call is awaited concurrently with the mock-server
    // side below — without spawning we'd deadlock waiting for the
    // mock to receive a request that we'd never send.
    let create_task =
        tokio::spawn(async move { adapter.create_pod(&sandbox, WORKLOAD_IMAGE, None).await });

    // Issue #443: `create_pod` POSTs the per-sandbox egress NetworkPolicy
    // *before* the Pod, so the egress trust boundary exists the instant the
    // Pod is admitted. Consume and ack that request first (201); its full
    // shape is asserted by the `network_policy_for` unit tests below.
    ack_network_policy_post(&mut handle, 201).await;

    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue the Pod request");

    assert_eq!(req.method(), "POST", "expected POST for Pod create");
    assert!(
        req.uri().path().ends_with("/namespaces/team-acme/pods"),
        "unexpected path: {}",
        req.uri().path()
    );

    let body_bytes = req
        .into_body()
        .collect_bytes()
        .await
        .expect("collect request body");
    let pod: Pod = serde_json::from_slice(&body_bytes).expect("decode posted pod");

    assert_eq!(pod.metadata.namespace.as_deref(), Some("team-acme"));
    assert_eq!(pod.metadata.name.as_deref(), Some("sbx-test"));

    let spec = pod.spec.as_ref().expect("pod.spec set");
    assert!(
        spec.runtime_class_name.is_none(),
        "PodAdapter must not set spec.runtimeClassName"
    );
    let envd = spec
        .containers
        .first()
        .expect("at least one container on the pod");
    assert_eq!(envd.name, "envd");
    // The container runs the customer's *workload* image; envd is staged
    // onto the node and mounted in, not baked into the image (issue #265).
    assert_eq!(
        envd.image.as_deref(),
        Some("ghcr.io/hypotheosis/workload:test")
    );
    // It execs the node-staged envd binary as its entrypoint (envd's own
    // default --state-file lands on the overlay rootfs).
    assert_eq!(
        envd.command.as_deref(),
        Some(["/sandbawks/bin/envd".to_string()].as_slice()),
        "container must run the node-staged envd binary as entrypoint"
    );
    let ports = envd.ports.as_ref().expect("envd container has ports");
    assert_eq!(ports.len(), 2, "envd container exposes gRPC + HTTP ports");
    assert_eq!(ports[0].container_port, 49_983);
    assert_eq!(ports[0].name.as_deref(), Some("envd"));
    assert_eq!(ports[1].container_port, 49_984);
    assert_eq!(ports[1].name.as_deref(), Some("envd-state"));

    // No init container — the binary is node-staged, so cold-start and
    // restore Pods stay topologically identical to the checkpoint.
    assert!(
        spec.init_containers.as_ref().is_none_or(|c| c.is_empty()),
        "injection model must not add an init container"
    );

    // No `uid` on the fixture → no ownerReferences emitted.
    assert!(
        pod.metadata.owner_references.is_none(),
        "owner_references should be unset when sandbox.metadata.uid is None"
    );

    // Echo the posted Pod back so the adapter call completes.
    let response_body = serde_json::to_vec(&pod).expect("serialize pod response");
    let resp = Response::builder()
        .status(201)
        .header("content-type", "application/json")
        .body(Body::from(response_body))
        .expect("build response");
    send.send_response(resp);

    let pod_ref = create_task
        .await
        .expect("join create_pod task")
        .expect("create_pod returns Ok");
    assert_eq!(pod_ref.namespace, "team-acme");
    assert_eq!(pod_ref.name, "sbx-test");
}

/// #603 Phase 2 (#605): a `filesystem`-mode sandbox declares
/// `volumeClaimTemplates`, so `create_pod` must POST the Sandbox-owned PVC
/// **before** the Pod (so the kubelet can bind the volume at admission), and
/// the posted Pod must carry the PVC-backed volume + container mount. This
/// pins the full ordering NetworkPolicy → PVC → Pod and the rendered wiring.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_provisions_pvc_before_pod() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let mut sandbox = fixture_sandbox();
    sandbox.spec.volume_claim_templates = Some(vec![VolumeClaimTemplate {
        metadata: VolumeClaimTemplateMetadata {
            name: "workspace".into(),
            ..Default::default()
        },
        spec: VolumeClaimSpec {
            storage: "10Gi".parse().expect("valid storage quantity"),
            access_modes: vec!["ReadWriteOnce".into()],
            storage_class_name: None,
            volume_mode: None,
        },
    }]);

    let create_task =
        tokio::spawn(async move { adapter.create_pod(&sandbox, WORKLOAD_IMAGE, None).await });

    // Hop 1: the egress NetworkPolicy (issue #443) is still posted first.
    ack_network_policy_post(&mut handle, 201).await;

    // Hop 2: the Sandbox-owned PVC, before the Pod.
    let (req, send) = handle
        .next_request()
        .await
        .expect("create_pod should POST the PVC before the Pod");
    assert_eq!(req.method(), "POST", "expected POST for PVC");
    assert!(
        req.uri()
            .path()
            .ends_with("/namespaces/team-acme/persistentvolumeclaims"),
        "unexpected PVC path: {}",
        req.uri().path()
    );
    let body_bytes = req
        .into_body()
        .collect_bytes()
        .await
        .expect("collect PVC request body");
    let pvc: PersistentVolumeClaim =
        serde_json::from_slice(&body_bytes).expect("decode posted PVC");
    assert_eq!(pvc.metadata.name.as_deref(), Some("workspace-sbx-test"));
    assert_eq!(
        pvc.spec
            .as_ref()
            .and_then(|s| s.resources.as_ref())
            .and_then(|r| r.requests.as_ref())
            .and_then(|req| req.get("storage"))
            .map(|q| q.0.as_str()),
        Some("10Gi")
    );
    // Echo the PVC back as the created resource.
    send.send_response(
        Response::builder()
            .status(201)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&pvc).expect("serialize PVC response"),
            ))
            .expect("build PVC response"),
    );

    // Hop 3: the Pod, mounting the PVC.
    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue the Pod request after the PVC");
    assert_eq!(req.method(), "POST", "expected POST for Pod create");
    assert!(req.uri().path().ends_with("/namespaces/team-acme/pods"));
    let body_bytes = req
        .into_body()
        .collect_bytes()
        .await
        .expect("collect Pod request body");
    let pod: Pod = serde_json::from_slice(&body_bytes).expect("decode posted pod");
    let spec = pod.spec.as_ref().expect("pod.spec set");

    let pvc_vol = spec
        .volumes
        .as_ref()
        .expect("pod has volumes")
        .iter()
        .find(|v| v.name == "workspace")
        .expect("PVC-backed volume injected");
    assert_eq!(
        pvc_vol
            .persistent_volume_claim
            .as_ref()
            .expect("volume is PVC-backed")
            .claim_name,
        "workspace-sbx-test"
    );
    let mount = spec.containers[0]
        .volume_mounts
        .as_ref()
        .expect("container has volume mounts")
        .iter()
        .find(|m| m.name == "workspace")
        .expect("PVC mount injected");
    assert_eq!(mount.mount_path, "/workspace");

    let response_body = serde_json::to_vec(&pod).expect("serialize pod response");
    send.send_response(
        Response::builder()
            .status(201)
            .header("content-type", "application/json")
            .body(Body::from(response_body))
            .expect("build response"),
    );

    let pod_ref = create_task
        .await
        .expect("join create_pod task")
        .expect("create_pod returns Ok");
    assert_eq!(pod_ref.name, "sbx-test");
}

/// #605 review (security-load-bearing): when the deterministic
/// `{template}-{sandbox}` PVC already exists but is owned by a *different*
/// Sandbox, `create_pod` must refuse — never mount data it doesn't own — and
/// must **not** create the Pod. Drives the 409 -> GET -> foreign -> error
/// sequence directly against the mock kube client and asserts the create fails
/// (the adoption guard's fail-closed contract), complementing the `pvc_owned_by`
/// unit tests which only cover the decision in isolation.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_refuses_foreign_owned_pvc_and_skips_pod() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let mut sandbox = fixture_sandbox();
    // A real uid so the ownership comparison is uid-vs-uid (our uid != the
    // existing PVC's owner uid), not the trivial "no uid owns nothing" path.
    sandbox.metadata.uid = Some("uid-mine".into());
    sandbox.spec.volume_claim_templates = Some(vec![VolumeClaimTemplate {
        metadata: VolumeClaimTemplateMetadata {
            name: "workspace".into(),
            ..Default::default()
        },
        spec: VolumeClaimSpec {
            storage: "10Gi".parse().expect("valid storage quantity"),
            access_modes: vec!["ReadWriteOnce".into()],
            storage_class_name: None,
            volume_mode: None,
        },
    }]);

    let create_task =
        tokio::spawn(async move { adapter.create_pod(&sandbox, WORKLOAD_IMAGE, None).await });

    // Hop 1: egress NetworkPolicy.
    ack_network_policy_post(&mut handle, 201).await;

    // Hop 2: PVC create → 409 AlreadyExists.
    let (req, send) = handle
        .next_request()
        .await
        .expect("create_pod should POST the PVC");
    assert!(req
        .uri()
        .path()
        .ends_with("/namespaces/team-acme/persistentvolumeclaims"));
    send.send_response(
        Response::builder()
            .status(409)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "metadata": {},
                    "status": "Failure",
                    "reason": "AlreadyExists",
                    "code": 409,
                }))
                .expect("serialize 409 body"),
            ))
            .expect("build 409 response"),
    );

    // Hop 3: the guard GETs the existing PVC to check ownership; return one
    // owned by a DIFFERENT Sandbox uid.
    let (req, send) = handle
        .next_request()
        .await
        .expect("the adoption guard should GET the existing PVC");
    assert_eq!(req.method(), "GET");
    assert!(req
        .uri()
        .path()
        .ends_with("/namespaces/team-acme/persistentvolumeclaims/workspace-sbx-test"));
    let foreign = PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some("workspace-sbx-test".into()),
            namespace: Some("team-acme".into()),
            owner_references: Some(vec![OwnerReference {
                api_version: "sandbawks.dev/v1alpha1".into(),
                kind: "Sandbox".into(),
                name: "sbx-other".into(),
                uid: "uid-other".into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        ..Default::default()
    };
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&foreign).expect("serialize foreign PVC"),
            ))
            .expect("build GET response"),
    );

    // The guard refuses: create_pod returns Err and never POSTs the Pod. (No
    // further request is pulled from `handle`; the code path returns before the
    // Pod create, so a Pod POST would be a bug surfaced as an unconsumed
    // request.)
    let result = create_task.await.expect("join create_pod task");
    assert!(
        result.is_err(),
        "create_pod must fail closed on a foreign-owned PVC, got {result:?}"
    );
}

/// Issue #265 / #468: the node-staged envd *seed* binary is mounted into the
/// workload container from a **read-only hostPath**. This shape (a) passes the
/// node's #237 pause mount-set gate (read-only mounts are exempt), (b) keeps
/// the seed binary out of the checkpoint (host bind), and (c) keeps the *envd*
/// container's mount topology identical between cold-start and restore so the
/// shim restore path doesn't choke. Asserted for both branches.
///
/// Epic #466 (capture-in-checkpoint): the relocate target (`ENVD_RUN_DIR`,
/// `/var/tmp/envd/run`) is a plain rootfs path under the captured root overlay —
/// NOT a separate volume. A `medium: Memory` emptyDir (tried in #468) presents
/// to the sandbox as a gofer-backed `bind`, which is (a) NOT captured in
/// `pages.img` and (b) a writable #237 mount-set violation that fails every
/// pause closed. This test pins that there is NO emptyDir and NO mount at the
/// relocate dir — only the read-only seed.
///
/// Neither a cold-start nor a restore Pod carries an init container (epic
/// #484): the restore's wait-for-staging gate lives in the shim, not the Pod,
/// because gVisor whole-sandbox restore rejects any container absent from the
/// checkpoint. That shim gate is covered by the shim's own Go tests; here we
/// only assert the injection model never adds one on either branch.
#[test]
fn envd_binary_mounted_read_only_from_node_hostpath() {
    for restore in [
        None,
        Some(RestorePlacement {
            staging_path: "/var/lib/sandbawks/stage/sbx-test".into(),
        }),
    ] {
        let sandbox = fixture_sandbox();
        let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, restore.as_ref());
        let spec = pod.spec.expect("pod.spec set");

        // Exactly one volume: a read-only hostPath carrying the binary.
        let volumes = spec.volumes.as_ref().expect("pod has volumes");
        let bin_vol = volumes
            .iter()
            .find(|v| v.name == "sandbawks-envd-bin")
            .expect("sandbawks-envd-bin volume present");
        assert_eq!(
            bin_vol
                .host_path
                .as_ref()
                .expect("envd binary volume is a hostPath")
                .path,
            "/var/lib/sandbawks/envd",
            "must mount the node-staged envd dir (matching the DaemonSet hostPathEnvd)"
        );
        // NO emptyDir: the relocate target is a rootfs path under the captured
        // root overlay (epic #466), not a separate volume. A `medium: Memory`
        // emptyDir would be a gofer-backed bind — uncaptured and a #237
        // violation.
        assert!(
            volumes.iter().all(|v| v.empty_dir.is_none()),
            "no emptyDir volume may be present"
        );
        // ...and NO mount at the relocate dir: it is a plain rootfs directory.
        let envd_for_run = spec.containers.first().expect("workload container");
        assert!(
            envd_for_run.volume_mounts.as_ref().is_none_or(|m| m
                .iter()
                .all(|m| m.mount_path != sandbawks_types::ENVD_RUN_DIR)),
            "there must be no volume mount at {} (rootfs path, not a volume)",
            sandbawks_types::ENVD_RUN_DIR
        );

        // Neither branch adds an init container (epic #484): cold-start and
        // restore Pods are topologically identical, and the restore's
        // wait-for-staging gate is shim-side, not a Pod container.
        assert!(
            spec.init_containers.as_ref().is_none_or(|c| c.is_empty()),
            "no init container on either the cold-start or restore Pod"
        );

        // The binary mount is read-only (→ exempt from the #237 gate).
        let envd = spec.containers.first().expect("workload container");
        let bin_mount = envd
            .volume_mounts
            .as_ref()
            .and_then(|m| m.iter().find(|m| m.mount_path == "/sandbawks/bin"))
            .expect("container mounts /sandbawks/bin");
        assert_eq!(
            bin_mount.read_only,
            Some(true),
            "the envd binary mount must be read-only so it is exempt from the \
             pause mount-set gate and stays out of the checkpoint"
        );
    }
}

/// Issue #272: every sandbox Pod must carry a
/// `requiredDuringSchedulingIgnoredDuringExecution` nodeAffinity with an
/// `Exists` match on `sandbawks.dev/envd-staged`. The DaemonSet sets that
/// label only after `stage-envd` has placed the binary, so the gate keeps
/// a sandbox off a node where the envd binary isn't staged yet — which
/// would otherwise mount an empty hostPath and CrashLoop on the missing
/// entrypoint. Asserted for both cold-start and restore branches — under
/// scheduler-placed restore (epic #484) the restore Pod is unpinned, so the
/// affinity is now load-bearing on that path too (it steers the scheduler
/// away from not-yet-staged nodes), not merely inert.
#[test]
fn pod_spec_for_gates_scheduling_on_envd_staged_node_label() {
    for restore in [
        None,
        Some(RestorePlacement {
            staging_path: "/var/lib/sandbawks/stage/sbx-test".into(),
        }),
    ] {
        let sandbox = fixture_sandbox();
        let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, restore.as_ref());
        let spec = pod.spec.expect("pod.spec set");

        let term = spec
            .affinity
            .as_ref()
            .and_then(|a| a.node_affinity.as_ref())
            .and_then(|na| {
                na.required_during_scheduling_ignored_during_execution
                    .as_ref()
            })
            .map(|ns| &ns.node_selector_terms)
            .and_then(|terms| terms.first())
            .expect("required nodeAffinity with at least one term");

        let req = term
            .match_expressions
            .as_ref()
            .and_then(|exprs| exprs.first())
            .expect("nodeAffinity term carries a match expression");

        assert_eq!(
            req.key, "sandbawks.dev/envd-staged",
            "the gate must key off the envd-staged node label"
        );
        assert_eq!(
            req.operator, "Exists",
            "the gate only requires the label key to exist, not a specific value"
        );
        assert!(
            req.values.is_none(),
            "an Exists requirement must not pin a value"
        );
    }
}

/// Issue #350: every sandbox Pod gates kubelet `Ready` on the envd gRPC
/// filesystem port (`:49983`) via a TCP-socket readinessProbe — so the
/// controller's `pod_ready` gate (Pod condition `Ready=True`) only stamps
/// `status.envdAddress` and promotes `Running` once the port the api's
/// `/files/*` routes dial is actually accepting, not merely once envd's
/// process has started. The probe must target `:49983` (the gRPC server),
/// NOT `:49984` (the `/__state` HTTP route the node DaemonSet's pause path
/// probes). Asserted for cold-start and restore Pods alike: a readinessProbe
/// is kubelet-side metadata, so it doesn't perturb the checkpoint topology.
#[test]
fn pod_spec_for_gates_readiness_on_envd_grpc_port() {
    for restore in [
        None,
        Some(RestorePlacement {
            staging_path: "/var/lib/sandbawks/stage/sbx-test".into(),
        }),
    ] {
        let sandbox = fixture_sandbox();
        let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, restore.as_ref());
        let spec = pod.spec.expect("pod.spec set");
        let envd = spec.containers.first().expect("workload container");

        let probe = envd
            .readiness_probe
            .as_ref()
            .expect("envd container must carry a readinessProbe (#350)");
        let tcp = probe
            .tcp_socket
            .as_ref()
            .expect("readinessProbe must be a TCP-socket probe on the gRPC port");
        // The probe targets the gRPC port by NAME ("envd"); assert that, plus
        // that the named port actually resolves to :49983 (the gRPC server) and
        // NOT :49984 (the /__state HTTP route) — the by-name indirection must
        // not let the probe silently point at the wrong surface.
        assert_eq!(
            tcp.port,
            IntOrString::String("envd".to_owned()),
            "readinessProbe must target the gRPC port by name"
        );
        let ports = envd.ports.as_ref().expect("envd container has ports");
        let named = ports
            .iter()
            .find(|p| p.name.as_deref() == Some("envd"))
            .expect("a containerPort named 'envd' must exist for the probe to resolve");
        assert_eq!(
            named.container_port, 49_983,
            "the 'envd'-named port must be the :49983 gRPC filesystem port, not :49984 /__state"
        );
        // A plain TCP probe — envd serves no grpc.health.v1.Health service,
        // so a kubelet `grpc`/`httpGet` probe would fail permanently.
        assert!(
            probe.grpc.is_none() && probe.http_get.is_none() && probe.exec.is_none(),
            "envd readiness must be a plain TCP probe (no health service exists to gRPC-probe)"
        );
    }
}

#[test]
fn pod_runtime_class_set_when_sandbox_specifies_gvisor() {
    let mut sandbox = fixture_sandbox();
    sandbox.spec.runtime_class_name = Some("gvisor".into());

    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, None);
    let spec = pod.spec.expect("pod.spec set");
    assert_eq!(
        spec.runtime_class_name.as_deref(),
        Some("gvisor"),
        "PodAdapter must pass spec.runtimeClassName through to the Pod"
    );
}

/// Issue #395: the Pod's container `resources` must mirror
/// `Sandbox.spec.resources` (both requests and limits) so a template's
/// `defaultResources` — resolved onto the Sandbox by the gateway and the
/// warm-pool minting paths — actually sizes the Pod. Regression guard for
/// the bug where a `2` CPU / `2048Mi` template still produced a `1` CPU /
/// `1Gi` Pod because the resource values never reached the Pod spec.
#[test]
fn pod_resources_mirror_sandbox_spec() {
    let mut sandbox = fixture_sandbox();
    sandbox.spec.resources = ResourceRequests {
        cpu: "2".parse().unwrap(),
        memory: "2048Mi".parse().unwrap(),
        ephemeral_storage: Some("256Mi".parse().unwrap()),
    };

    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, None);
    let spec = pod.spec.expect("pod.spec set");
    let container = spec
        .containers
        .first()
        .expect("at least one container on the pod");
    let resources = container
        .resources
        .as_ref()
        .expect("container resources set");

    // Memory carries the customer's request PLUS the relocated-envd budget
    // (epic #466): the envd copy in the captured overlay-tmpfs counts against
    // the container memory cgroup, so the controller adds a 64 MiB headroom
    // (emitted as a bare byte count) and the workload keeps its full 2048Mi.
    // 2048 MiB + 64 MiB = 2214592512 bytes. CPU and ephemeral-storage are
    // untouched.
    let expected_memory = (2048i128 * 1024 * 1024 + 64 * 1024 * 1024).to_string();

    let requests = resources.requests.as_ref().expect("requests set");
    assert_eq!(requests["cpu"].0, "2");
    assert_eq!(requests["memory"].0, expected_memory);

    let limits = resources.limits.as_ref().expect("limits set");
    assert_eq!(limits["cpu"].0, "2");
    assert_eq!(limits["memory"].0, expected_memory);
    // ephemeral-storage is a limit-only knob (the snapshot delta budget).
    assert_eq!(limits["ephemeral-storage"].0, "256Mi");
}

#[test]
fn pod_runtime_class_absent_when_sandbox_unset() {
    let sandbox = fixture_sandbox();
    assert!(
        sandbox.spec.runtime_class_name.is_none(),
        "fixture precondition"
    );

    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, None);
    let spec = pod.spec.expect("pod.spec set");
    assert!(
        spec.runtime_class_name.is_none(),
        "Pod must inherit cluster default runtime when Sandbox.spec.runtimeClassName is unset"
    );
}

/// Epic #484 (scheduler-placed restore): a restore Pod defaults its
/// runtime class to `sandbawks-runsc`, is left **unpinned** (no
/// `spec.nodeName`) so the scheduler does memory-aware placement, carries
/// the deterministic staging path in the `io.sandbawks/restore-image-path`
/// annotation, and injects **no** restore-mode env — the container is born
/// restored by the shim's start-time `runsc restore`.
#[test]
fn pod_spec_for_restore_sets_runtime_class_unpinned_and_annotation() {
    let sandbox = fixture_sandbox();
    let placement = RestorePlacement {
        staging_path: "/var/lib/sandbawks/stage/sbx-test".into(),
    };

    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, Some(&placement));

    assert_eq!(
        pod.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("io.sandbawks/restore-image-path"))
            .map(String::as_str),
        Some("/var/lib/sandbawks/stage/sbx-test"),
        "restore Pod must carry the staging path annotation for the shim"
    );

    let spec = pod.spec.expect("pod.spec set");
    assert_eq!(
        spec.runtime_class_name.as_deref(),
        Some("sandbawks-runsc"),
        "restore Pod defaults to the gVisor runsc runtime class"
    );
    assert_eq!(
        spec.node_name, None,
        "restore Pod must be unpinned so the scheduler places it (epic #484)"
    );
    assert!(
        !spec.containers[0]
            .env
            .clone()
            .unwrap_or_default()
            .iter()
            .any(|e| e.name == "SANDBAWKS_RESTORE_MODE"),
        "restore Pod must not inject SANDBAWKS_RESTORE_MODE — the container is born restored"
    );
}

/// Plan 07 T9: a Sandbox that pins a non-gVisor `runtimeClassName` (e.g.
/// a future `kata-fc` restore class with its own shim) keeps it on the
/// restore path. The `sandbawks-runsc` default fills in only when the
/// field is unset or pins the plain `gvisor` class — see
/// `pod_spec_for_restore_maps_gvisor_to_runsc` below.
#[test]
fn pod_spec_for_restore_respects_explicit_runtime_class() {
    let mut sandbox = fixture_sandbox();
    sandbox.spec.runtime_class_name = Some("kata-fc".into());
    let placement = RestorePlacement {
        staging_path: "/var/lib/sandbawks/stage/sbx-test".into(),
    };

    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, Some(&placement));
    let spec = pod.spec.expect("pod.spec set");
    assert_eq!(
        spec.runtime_class_name.as_deref(),
        Some("kata-fc"),
        "explicit Sandbox runtimeClassName must win over the restore default"
    );
}

/// Issue #196: a Sandbox pinned to the plain `gvisor` cold-start class
/// must have its restore Pod upgraded to `sandbawks-runsc`. The vanilla
/// `gvisor` containerd handler has no restore-capable shim, so the
/// `io.sandbawks/restore-image-path` annotation would be ignored and
/// `runsc restore` never run — hanging the sandbox in `Resuming`. The
/// two classes are the same gVisor runtime; `sandbawks-runsc` just adds
/// the sandbawks shim wrapper that drives the start-time restore, so the
/// gVisor cold-start class maps onto it (while a non-gVisor pin like
/// `kata-fc` is still honored — see the test above).
#[test]
fn pod_spec_for_restore_maps_gvisor_to_runsc() {
    let mut sandbox = fixture_sandbox();
    sandbox.spec.runtime_class_name = Some("gvisor".into());
    let placement = RestorePlacement {
        staging_path: "/var/lib/sandbawks/stage/sbx-test".into(),
    };

    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, Some(&placement));
    let spec = pod.spec.expect("pod.spec set");
    assert_eq!(
        spec.runtime_class_name.as_deref(),
        Some("sandbawks-runsc"),
        "a gvisor sandbox's restore Pod must be upgraded to the shim runtime class"
    );
}

/// A non-restore (cold-start) Pod carries no restore annotation, no
/// pinned node, and no restore-mode env.
#[test]
fn pod_spec_for_non_restore_has_no_restore_markers() {
    let sandbox = fixture_sandbox();
    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, None);

    // A cold-start Pod still carries the descheduler opt-out annotation
    // (#498); it must simply not carry the restore-image-path annotation.
    assert!(
        !pod.metadata
            .annotations
            .clone()
            .unwrap_or_default()
            .contains_key("io.sandbawks/restore-image-path"),
        "cold-start Pod must not carry the restore-image-path annotation"
    );
    let spec = pod.spec.expect("pod.spec set");
    assert!(
        spec.node_name.is_none(),
        "cold-start Pod must not pin spec.nodeName"
    );
    assert!(
        !spec.containers[0]
            .env
            .clone()
            .unwrap_or_default()
            .iter()
            .any(|e| e.name == "SANDBAWKS_RESTORE_MODE"),
        "cold-start Pod must not carry SANDBAWKS_RESTORE_MODE"
    );
}

#[test]
fn pod_spec_for_sets_owner_reference_when_uid_present() {
    let mut sandbox = fixture_sandbox();
    sandbox.metadata.uid = Some("abc-123".into());

    let pod = PodAdapter::pod_spec_for(&sandbox, WORKLOAD_IMAGE, None);
    let owners = pod
        .metadata
        .owner_references
        .expect("owner_references should be set when uid is present");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].uid, "abc-123");
    assert_eq!(owners[0].kind, "Sandbox");
    assert_eq!(owners[0].api_version, "sandbawks.dev/v1alpha1");
    assert_eq!(owners[0].controller, Some(true));
}

// PodAdapter advertises a different runtime_class() than
// FakeRuntimeAdapter so the controller can disambiguate at dispatch
// time once multiple adapters coexist (Plan 05). The test runs under
// tokio because `kube::Client::new` wires up a `tower::buffer::Buffer`
// internally, which requires a running tokio reactor to spawn its
// worker task — even when no requests are ever issued.
#[tokio::test(flavor = "current_thread")]
async fn runtime_class_is_pod() {
    let (mock_service, _handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);
    assert_eq!(adapter.runtime_class(), "pod");
}

/// Issue #171 review (#6791): `delete_pod` must not return until the
/// API server has actually removed the Pod resource — `Background`
/// propagation returns as soon as the DELETE is accepted, before
/// kubelet has finished tearing the container down. A caller that
/// recreates a Pod with the same name right after a delete would
/// collide at the API server (409) against the still-`Terminating`
/// Pod, and the 409-as-success shim would then return the dying Pod's
/// ref.
///
/// This test pins the fix: the adapter issues DELETE, then polls
/// `GET` until 404, then returns Ok. The mock first returns a GET that
/// finds the Pod (still terminating), then a GET that 404s — the
/// adapter must not return until that second GET.
#[tokio::test(flavor = "current_thread")]
async fn delete_pod_polls_until_apiserver_returns_404() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);
    let pod_ref = sandbawks_types::PodRef {
        name: "sbx-test".into(),
        namespace: "team-acme".into(),
    };

    let delete_task = tokio::spawn(async move { adapter.delete_pod(&pod_ref).await });

    // 1. Adapter issues DELETE. Apiserver acknowledges with the marked-
    //    for-deletion Pod (deletionTimestamp set).
    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue the DELETE first");
    assert_eq!(req.method(), "DELETE");
    assert!(
        req.uri().path().ends_with("/pods/sbx-test"),
        "DELETE should target the Pod by name, got {}",
        req.uri().path(),
    );
    let body_bytes = req
        .into_body()
        .collect_bytes()
        .await
        .expect("collect DELETE body");
    let body_str = std::str::from_utf8(&body_bytes).unwrap_or("");
    assert!(
        body_str.contains("Foreground"),
        "delete must use foreground propagation, got body: {body_str}",
    );
    let terminating_pod = serde_json::json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {
            "name": "sbx-test",
            "namespace": "team-acme",
            "deletionTimestamp": "2026-05-26T00:00:00Z",
        },
        "spec": { "containers": [] },
        "status": { "phase": "Running" },
    });
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&terminating_pod).unwrap()))
            .expect("build delete response"),
    );

    // 2. Adapter starts polling. First GET finds the Pod still there.
    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue a GET to poll for deletion");
    assert_eq!(req.method(), "GET");
    assert!(
        req.uri().path().ends_with("/pods/sbx-test"),
        "poll GET should target the same Pod, got {}",
        req.uri().path(),
    );
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&terminating_pod).unwrap()))
            .expect("build still-there response"),
    );

    // 3. Next GET 404s — Pod actually gone. Adapter must return Ok.
    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should poll a second time after still-present");
    assert_eq!(req.method(), "GET");
    let not_found = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "reason": "NotFound",
        "code": 404,
    });
    send.send_response(
        Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&not_found).unwrap()))
            .expect("build 404 response"),
    );

    delete_task
        .await
        .expect("join delete_pod task")
        .expect("delete_pod returns Ok once the apiserver reports 404");
}

// Reproduces the issue surfaced by the Plan 04 T15 kind-smoke: two
// reconciles racing on the same Sandbox can both reach `Action::CreatePod`
// before the first one's `status.pod_ref` patch is visible. The second
// kube `create` call then returns 409 AlreadyExists; without the
// idempotency shim that would bounce the Sandbox into `Failed`. The
// expected behavior is that `create_pod` swallows the 409 and returns
// the existing `PodRef` so the reconcile loop converges normally.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_treats_already_exists_as_success() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let sandbox = fixture_sandbox();

    let create_task =
        tokio::spawn(async move { adapter.create_pod(&sandbox, WORKLOAD_IMAGE, None).await });

    // Ack the egress NetworkPolicy POST with 201 first (issue #443): this test
    // targets the *Pod* 409 path, so the policy must succeed before we reach
    // the Pod create. (The policy's own 409 idempotency is covered separately
    // by `create_pod_treats_network_policy_already_exists_as_success`.)
    ack_network_policy_post(&mut handle, 201).await;

    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue the Pod create request");
    assert!(
        req.uri().path().ends_with("/namespaces/team-acme/pods"),
        "the 409 must be returned on the Pod POST, not the policy POST: {}",
        req.uri().path()
    );

    // Kube apiserver-shaped 409 AlreadyExists. `kube::Error::Api` is
    // built from this body by the client; the inner `code: 409` field
    // is what the adapter's match arm keys off of.
    let conflict_body = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": "pods \"sbx-test\" already exists",
        "reason": "AlreadyExists",
        "details": { "name": "sbx-test", "kind": "pods" },
        "code": 409
    });
    let resp = Response::builder()
        .status(409)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&conflict_body).unwrap()))
        .expect("build 409 response");
    send.send_response(resp);

    let pod_ref = create_task
        .await
        .expect("join create_pod task")
        .expect("create_pod swallows 409 AlreadyExists");
    assert_eq!(pod_ref.namespace, "team-acme");
    assert_eq!(pod_ref.name, "sbx-test");
}

/// Issue #443 idempotency: a 409 on the *NetworkPolicy* POST (a stale policy
/// left by a racing reconcile) must be swallowed too, with `create_pod`
/// proceeding to create the Pod and returning Ok — a leftover policy must
/// never fail sandbox creation on a reconcile retry. Mirrors the Pod 409
/// coverage in `create_pod_treats_already_exists_as_success`.
#[tokio::test(flavor = "current_thread")]
async fn create_pod_treats_network_policy_already_exists_as_success() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let sandbox = fixture_sandbox();
    let create_task =
        tokio::spawn(async move { adapter.create_pod(&sandbox, WORKLOAD_IMAGE, None).await });

    // Policy POST conflicts (409) — the adapter must treat it as success.
    ack_network_policy_post(&mut handle, 409).await;

    // ...and still go on to create the Pod, which we ack with 201.
    let (req, send) = handle
        .next_request()
        .await
        .expect("create_pod must POST the Pod even after a policy 409");
    assert!(
        req.uri().path().ends_with("/namespaces/team-acme/pods"),
        "expected the Pod POST after the swallowed policy 409: {}",
        req.uri().path()
    );
    let body_bytes = req
        .into_body()
        .collect_bytes()
        .await
        .expect("collect Pod request body");
    let pod: Pod = serde_json::from_slice(&body_bytes).expect("decode posted pod");
    send.send_response(
        Response::builder()
            .status(201)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&pod).expect("serialize pod")))
            .expect("build pod response"),
    );

    let pod_ref = create_task
        .await
        .expect("join create_pod task")
        .expect("create_pod swallows the NetworkPolicy 409 and returns Ok");
    assert_eq!(pod_ref.name, "sbx-test");
}

/// Drive `PodAdapter::pod_scheduled_node` against a mocked kube server that
/// answers the single `GET .../pods/{name}` with `response`, returning the
/// adapter's result. Asserts the request is the expected GET (epic #484).
async fn run_pod_scheduled_node(
    response: Response<Body>,
) -> Result<Option<String>, sandbawks_types::RuntimeError> {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);
    let pod_ref = sandbawks_types::PodRef {
        name: "sbx-test".into(),
        namespace: "team-acme".into(),
    };

    let task = tokio::spawn(async move { adapter.pod_scheduled_node(&pod_ref).await });

    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue one GET");
    assert_eq!(req.method(), "GET", "pod_scheduled_node must GET the Pod");
    assert!(
        req.uri()
            .path()
            .ends_with("/namespaces/team-acme/pods/sbx-test"),
        "unexpected path: {}",
        req.uri().path()
    );
    send.send_response(response);
    task.await.expect("join pod_scheduled_node task")
}

/// A `200 OK` Pod whose `spec.nodeName` is `node` (`None` → field absent).
fn pod_with_node_response(node: Option<&str>) -> Response<Body> {
    let pod = serde_json::json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "sbx-test", "namespace": "team-acme" },
        "spec": { "nodeName": node, "containers": [] },
        "status": { "phase": "Pending" },
    });
    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&pod).unwrap()))
        .expect("build pod response")
}

/// A kube apiserver-shaped `Status` error response with the given `code`.
fn status_error_response(code: u16, reason: &str) -> Response<Body> {
    let body = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "reason": reason,
        "code": code,
    });
    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .expect("build status error response")
}

/// Epic #484: a scheduled restore Pod's `spec.nodeName` is surfaced as
/// `Some(node)` — the signal node-discovery folds into `NodeAssigned`.
#[tokio::test(flavor = "current_thread")]
async fn pod_scheduled_node_returns_bound_node() {
    let r = run_pod_scheduled_node(pod_with_node_response(Some("node-a"))).await;
    assert_eq!(
        r.expect("scheduled Pod read succeeds").as_deref(),
        Some("node-a"),
    );
}

/// A Pod whose `spec.nodeName` is empty is *not yet scheduled* — the
/// adapter's `.filter(|n| !n.is_empty())` must collapse it to `None` so
/// node-discovery keeps waiting rather than dispatching a Stage job with an
/// empty node pin.
#[tokio::test(flavor = "current_thread")]
async fn pod_scheduled_node_empty_node_name_is_none() {
    let r = run_pod_scheduled_node(pod_with_node_response(Some(""))).await;
    assert_eq!(
        r.expect("read succeeds"),
        None,
        "empty spec.nodeName means not-yet-scheduled"
    );
}

/// A 404 (the Pod is gone) folds to `Ok(None)` — the separate liveness path,
/// not this probe, decides a lost Pod.
#[tokio::test(flavor = "current_thread")]
async fn pod_scheduled_node_missing_pod_is_none() {
    let r = run_pod_scheduled_node(status_error_response(404, "NotFound")).await;
    assert_eq!(r.expect("404 folds to Ok(None)"), None);
}

/// A non-404 apiserver error maps to `RuntimeError::Transient` so the
/// reconcile loop retries on the next requeue rather than mistaking a
/// hiccup for "unscheduled".
#[tokio::test(flavor = "current_thread")]
async fn pod_scheduled_node_transport_error_is_transient() {
    let r = run_pod_scheduled_node(status_error_response(500, "InternalError")).await;
    assert!(
        matches!(r, Err(sandbawks_types::RuntimeError::Transient(_))),
        "a non-404 error must map to RuntimeError::Transient, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Issue #443: per-sandbox default-deny egress NetworkPolicy.
// ---------------------------------------------------------------------------

/// Build a fixture Sandbox carrying the given `egressAllow` list (an empty
/// vec ⇒ deny-all) and an optional server-assigned `uid` (for the owner-ref
/// path). Reuses [`fixture_sandbox`] for everything else.
fn fixture_with_egress(egress_allow: &[&str], uid: Option<&str>) -> Sandbox {
    let mut s = fixture_sandbox();
    s.spec.network_policy = Some(NetworkPolicy {
        egress_allow: egress_allow.iter().map(|e| (*e).to_owned()).collect(),
    });
    s.metadata.uid = uid.map(|u| u.to_owned());
    s
}

/// Every sandbox's policy targets exactly *its* Pod via the per-sandbox
/// identity label and governs egress only — and an empty/absent `egressAllow`
/// produces a default-deny baseline (no egress rules under
/// `policyTypes: [Egress]`).
#[test]
fn network_policy_defaults_to_deny_all_egress() {
    // Both `Some(empty egressAllow)` and `None` must deny all egress.
    let mut none_np = fixture_sandbox();
    none_np.spec.network_policy = None;
    for sandbox in [fixture_with_egress(&[], None), none_np] {
        let policy = PodAdapter::network_policy_for(&sandbox, |_| None);
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("sandbox-sbx-test-egress"),
            "policy name is derived from the sandbox name"
        );
        assert_eq!(policy.metadata.namespace.as_deref(), Some("team-acme"));

        let spec = policy.spec.expect("policy has a spec");
        assert_eq!(
            spec.policy_types.as_deref(),
            Some(["Egress".to_string()].as_slice()),
            "the policy must govern egress only (ingress is out of scope)"
        );
        // podSelector targets the unique per-sandbox label, NOT the shared
        // app.kubernetes.io/name=sandbawks-sandbox (which would scope to every
        // sandbox in the namespace).
        let match_labels = spec
            .pod_selector
            .match_labels
            .as_ref()
            .expect("podSelector matchLabels set");
        assert_eq!(
            match_labels
                .get("sandbawks.dev/sandbox")
                .map(String::as_str),
            Some("sbx-test"),
            "podSelector must target this sandbox's identity label"
        );
        // Deny-all: no egress rules.
        assert!(
            spec.egress.is_none(),
            "empty egressAllow must yield no egress rules (deny all egress)"
        );
    }
}

/// IP and CIDR `egressAllow` entries become exact `ipBlock` egress rules (a
/// bare IP widened to a host CIDR), and need no DNS allowance.
#[test]
fn network_policy_cidr_entries_become_ipblocks() {
    let sandbox = fixture_with_egress(&["10.0.0.0/8", "203.0.113.5"], None);
    let policy = PodAdapter::network_policy_for(&sandbox, |_| None);
    let egress = policy
        .spec
        .expect("spec")
        .egress
        .expect("egress rules present for non-empty egressAllow");

    let cidrs: Vec<String> = egress
        .iter()
        .filter_map(|r| r.to.as_ref())
        .flatten()
        .filter_map(|peer| peer.ip_block.as_ref().map(|b| b.cidr.clone()))
        .collect();
    assert_eq!(
        cidrs,
        vec!["10.0.0.0/8".to_string(), "203.0.113.5/32".to_string()],
        "CIDR passes through; a bare IP is widened to /32"
    );
    // No DNS rule when every entry is an IP/CIDR literal.
    assert!(
        egress.iter().all(|r| r.ports.is_none()),
        "no DNS (port-only) rule should be emitted when no entry is a DNS name"
    );
}

/// A DNS-*name* `egressAllow` entry can't be encoded as a destination in a
/// standard NetworkPolicy, so it opens DNS egress (UDP+TCP 53) instead of an
/// ipBlock — the name-aware CNI layer enforces the resolved destination.
#[test]
fn network_policy_dns_name_opens_dns_egress() {
    let sandbox = fixture_with_egress(&["api.example.com"], None);
    let policy = PodAdapter::network_policy_for(&sandbox, |_| None);
    let egress = policy.spec.expect("spec").egress.expect("egress rules");

    // No ipBlock — a name has no L3 destination here.
    assert!(
        egress
            .iter()
            .filter_map(|r| r.to.as_ref())
            .flatten()
            .all(|peer| peer.ip_block.is_none()),
        "a DNS name must not produce an ipBlock destination"
    );
    // Exactly one DNS rule opening UDP+TCP 53 to any peer (no `to`).
    let dns_rule = egress
        .iter()
        .find(|r| r.ports.is_some())
        .expect("a DNS port rule must be present for a name entry");
    assert!(dns_rule.to.is_none(), "DNS rule allows port 53 to any peer");
    let ports = dns_rule.ports.as_ref().unwrap();
    let mut protos: Vec<String> = ports
        .iter()
        .map(|p| {
            assert_eq!(p.port, Some(IntOrString::Int(53)), "DNS port must be 53");
            p.protocol.clone().expect("protocol set")
        })
        .collect();
    protos.sort();
    assert_eq!(
        protos,
        vec!["TCP".to_string(), "UDP".to_string()],
        "DNS egress must cover both UDP and TCP on port 53"
    );
}

/// When the Sandbox has a server-assigned `uid`, the policy owner-references
/// the CR so kube garbage-collects it with the sandbox; without a uid (e.g. a
/// freshly-built CR in a unit test) no owner reference is emitted.
#[test]
fn network_policy_owner_references_sandbox_when_uid_present() {
    let with_uid = fixture_with_egress(&[], Some("uid-1234"));
    let owners = PodAdapter::network_policy_for(&with_uid, |_| None)
        .metadata
        .owner_references
        .expect("owner reference set when uid present");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].kind, "Sandbox");
    assert_eq!(owners[0].name, "sbx-test");
    assert_eq!(owners[0].uid, "uid-1234");
    assert_eq!(owners[0].controller, Some(true));

    let without_uid = fixture_with_egress(&[], None);
    assert!(
        PodAdapter::network_policy_for(&without_uid, |_| None)
            .metadata
            .owner_references
            .is_none(),
        "no owner reference when the sandbox has no uid"
    );
}

/// Collect the `ipBlock` CIDRs across all egress rules of a sandbox's policy.
fn egress_cidrs(sandbox: &Sandbox) -> Vec<String> {
    PodAdapter::network_policy_for(sandbox, |_| None)
        .spec
        .and_then(|s| s.egress)
        .into_iter()
        .flatten()
        .filter_map(|r| r.to)
        .flatten()
        .filter_map(|peer| peer.ip_block.map(|b| b.cidr))
        .collect()
}

/// True when the sandbox's policy carries the DNS (port-only) egress rule.
fn egress_has_dns_rule(sandbox: &Sandbox) -> bool {
    PodAdapter::network_policy_for(sandbox, |_| None)
        .spec
        .and_then(|s| s.egress)
        .into_iter()
        .flatten()
        .any(|r| r.ports.is_some())
}

/// The non-trivial interaction branch: a mixed `egressAllow` (one IP/CIDR plus
/// one DNS name) must emit **both** the exact `ipBlock` rule for the literal
/// and the DNS port rule for the name — not one or the other.
#[test]
fn network_policy_mixed_cidr_and_dns_emits_both() {
    let sandbox = fixture_with_egress(&["10.0.0.0/8", "api.example.com"], None);
    assert_eq!(
        egress_cidrs(&sandbox),
        vec!["10.0.0.0/8".to_string()],
        "the IP/CIDR entry must still become an ipBlock when mixed with a name"
    );
    assert!(
        egress_has_dns_rule(&sandbox),
        "the DNS name in the mixed list must still open the DNS port rule"
    );
}

/// Malformed CIDRs (out-of-range prefix, non-address left side) are NOT valid
/// `ipBlock` destinations, so `egress_entry_as_cidr` falls them back to the
/// DNS-name path: no `ipBlock`, DNS egress opened. This is asserted explicitly
/// so a future change to the parser can't silently start emitting a bogus
/// `ipBlock` (or silently narrow the trust boundary) without failing a test.
#[test]
fn network_policy_malformed_cidr_falls_back_to_dns_name() {
    for bad in [
        "10.0.0.0/33",
        "2001:db8::/129",
        "not-a-name/8",
        "10.0.0.0/abc",
    ] {
        let sandbox = fixture_with_egress(&[bad], None);
        assert!(
            egress_cidrs(&sandbox).is_empty(),
            "malformed CIDR {bad:?} must not produce an ipBlock"
        );
        assert!(
            egress_has_dns_rule(&sandbox),
            "malformed CIDR {bad:?} must fall back to the DNS-name path"
        );
    }
}

/// IPv6 entries: a bare address is widened to a `/128` host CIDR, and an
/// explicit IPv6 CIDR is preserved verbatim — the same shape as the IPv4 path,
/// guarding against an address-family regression in the widening logic.
#[test]
fn network_policy_ipv6_entries_widen_and_preserve() {
    let bare = fixture_with_egress(&["2001:db8::1"], None);
    assert_eq!(
        egress_cidrs(&bare),
        vec!["2001:db8::1/128".to_string()],
        "a bare IPv6 address must be widened to /128"
    );
    assert!(
        !egress_has_dns_rule(&bare),
        "a valid IPv6 literal needs no DNS rule"
    );

    let cidr = fixture_with_egress(&["2001:db8::/64"], None);
    assert_eq!(
        egress_cidrs(&cidr),
        vec!["2001:db8::/64".to_string()],
        "a valid IPv6 CIDR must pass through verbatim"
    );
}

// ---------------------------------------------------------------------------
// Issue #536: the policy always carves out envd's own infra egress (DNS + the
// OTel collector) when export is enabled, independent of `egressAllow`.
// ---------------------------------------------------------------------------

/// The in-cluster collector endpoint the Helm chart injects into sandbox envd.
const TEST_OTEL_ENDPOINT: &str = "http://sandbawks-otel-collector.sandbawks.svc:4318";

/// The `(namespace, port)` of the collector egress rule (the `to` peer carrying
/// a `namespaceSelector`), if present.
fn collector_rule_target(policy: &NetworkPolicyResource) -> Option<(String, i32)> {
    policy.spec.as_ref()?.egress.as_ref()?.iter().find_map(|r| {
        let peer =
            r.to.as_ref()?
                .iter()
                .find(|p| p.namespace_selector.is_some())?;
        let ns = peer
            .namespace_selector
            .as_ref()?
            .match_labels
            .as_ref()?
            .get("kubernetes.io/metadata.name")?
            .clone();
        let port = r.ports.as_ref()?.iter().find_map(|p| match p.port {
            Some(IntOrString::Int(n)) => Some(n),
            _ => None,
        })?;
        Some((ns, port))
    })
}

/// True when the policy carries the DNS rule (UDP+TCP 53 to any peer).
fn has_dns_egress(policy: &NetworkPolicyResource) -> bool {
    policy
        .spec
        .as_ref()
        .and_then(|s| s.egress.as_ref())
        .map(|rules| {
            rules.iter().any(|r| {
                r.to.is_none()
                    && r.ports.as_ref().is_some_and(|ports| {
                        ports.iter().any(|p| p.port == Some(IntOrString::Int(53)))
                    })
            })
        })
        .unwrap_or(false)
}

/// With OTel export enabled, an **empty** `egressAllow` must no longer be
/// deny-all: the policy opens DNS and the collector so envd's exporter can
/// reach it (without this, the blocked exporter starves envd on resume — #536).
#[test]
fn network_policy_opens_infra_egress_when_otel_enabled() {
    let sandbox = fixture_with_egress(&[], None);
    let policy = PodAdapter::network_policy_for(&sandbox, |name| {
        (name == "OTEL_EXPORTER_OTLP_ENDPOINT").then(|| TEST_OTEL_ENDPOINT.to_owned())
    });

    assert!(
        policy.spec.as_ref().unwrap().egress.is_some(),
        "empty egressAllow must NOT be deny-all when OTel export is enabled"
    );
    assert_eq!(
        collector_rule_target(&policy),
        Some(("sandbawks".to_string(), 4318)),
        "the collector rule must target the endpoint's namespace + port"
    );
    assert!(
        has_dns_egress(&policy),
        "DNS must be opened so the collector service name resolves"
    );
}

/// The infra carve-out is *additive*: a user `egressAllow` CIDR and the infra
/// DNS + collector rules must all be present together.
#[test]
fn network_policy_infra_egress_added_alongside_user_egress_allow() {
    let sandbox = fixture_with_egress(&["203.0.113.5"], None);
    let policy = PodAdapter::network_policy_for(&sandbox, |name| {
        (name == "OTEL_EXPORTER_OTLP_ENDPOINT").then(|| TEST_OTEL_ENDPOINT.to_owned())
    });

    let cidrs: Vec<String> = policy
        .spec
        .as_ref()
        .unwrap()
        .egress
        .as_ref()
        .unwrap()
        .iter()
        .filter_map(|r| r.to.as_ref())
        .flatten()
        .filter_map(|peer| peer.ip_block.as_ref().map(|b| b.cidr.clone()))
        .collect();
    assert_eq!(
        cidrs,
        vec!["203.0.113.5/32".to_string()],
        "the user CIDR must still be present alongside the infra rules"
    );
    assert!(
        collector_rule_target(&policy).is_some(),
        "collector rule present"
    );
    assert!(has_dns_egress(&policy), "DNS rule present");
}

/// The collector destination is derived from the *same* endpoint resolution the
/// Pod env uses: when split pipelines set the sandbox-endpoint override, the
/// rule follows the override (not the controller's internal endpoint), so the
/// policy and the exporter target can't drift.
#[test]
fn network_policy_collector_target_follows_split_pipeline_override() {
    let sandbox = fixture_with_egress(&[], None);
    let policy = PodAdapter::network_policy_for(&sandbox, |name| match name {
        "SANDBAWKS_OTEL_SANDBOX_ENDPOINT" => {
            Some("http://sandbawks-otel-collector.sbx-pipeline.svc:4318".to_owned())
        }
        "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://internal.observability.svc:4320".to_owned()),
        _ => None,
    });

    assert_eq!(
        collector_rule_target(&policy),
        Some(("sbx-pipeline".to_string(), 4318)),
        "the rule must follow the sandbox-pipeline override, not the internal endpoint"
    );
}

/// The `OTEL_SDK_DISABLED` kill-switch must keep deny-all: no exporter runs, so
/// no infra carve-out is added and an empty `egressAllow` stays deny-all.
#[test]
fn network_policy_no_infra_egress_when_otel_disabled() {
    let sandbox = fixture_with_egress(&[], None);
    let policy = PodAdapter::network_policy_for(&sandbox, |name| match name {
        "OTEL_SDK_DISABLED" => Some("true".to_owned()),
        "OTEL_EXPORTER_OTLP_ENDPOINT" => Some(TEST_OTEL_ENDPOINT.to_owned()),
        _ => None,
    });

    assert!(
        policy.spec.unwrap().egress.is_none(),
        "OTEL_SDK_DISABLED must leave the policy deny-all"
    );
}

/// An *external* (non-cluster) collector endpoint can't be expressed as a
/// `namespaceSelector`, so the carve-out skips the collector rule rather than
/// emitting a bogus one; with an empty `egressAllow` this stays deny-all (the
/// operator must open an external collector via an explicit CIDR).
#[test]
fn network_policy_external_collector_endpoint_emits_no_collector_rule() {
    let sandbox = fixture_with_egress(&[], None);
    let policy = PodAdapter::network_policy_for(&sandbox, |name| {
        (name == "OTEL_EXPORTER_OTLP_ENDPOINT").then(|| "http://otel.example.com:4318".to_owned())
    });

    assert!(
        collector_rule_target(&policy).is_none(),
        "a non-cluster endpoint must not produce a namespaceSelector collector rule"
    );
    assert!(
        policy.spec.unwrap().egress.is_none(),
        "with no expressible collector and empty egressAllow, the policy stays deny-all"
    );
}

// --- FQDN egress (issue #541): Cilium `toFQDNs` CiliumNetworkPolicy ---------

/// Lookup enabling FQDN egress with the stock CoreDNS resolver defaults.
fn fqdn_enabled(name: &str) -> Option<String> {
    (name == "SANDBAWKS_FQDN_EGRESS_ENABLED").then(|| "true".to_owned())
}

/// Extract the `egress` rule array from a materialized CiliumNetworkPolicy.
fn cilium_egress_rules(obj: &DynamicObject) -> &Vec<serde_json::Value> {
    obj.data["spec"]["egress"]
        .as_array()
        .expect("CiliumNetworkPolicy spec.egress is an array")
}

/// Collect every `toFQDNs[].matchName` across the policy's egress rules.
fn cilium_match_names(obj: &DynamicObject) -> Vec<String> {
    cilium_egress_rules(obj)
        .iter()
        .filter_map(|rule| rule.get("toFQDNs"))
        .filter_map(|f| f.as_array())
        .flatten()
        .filter_map(|entry| entry["matchName"].as_str().map(str::to_owned))
        .collect()
}

/// FQDN enforcement is off by default: with the flag unset the adapter emits no
/// Cilium policy even when `egressAllow` carries DNS names — the baseline
/// NetworkPolicy's DNS-port-only path is unchanged (no regression).
#[test]
fn fqdn_policy_none_when_flag_disabled() {
    let sandbox = fixture_with_egress(&["api.example.com"], None);
    assert!(
        PodAdapter::cilium_network_policy_for(&sandbox, |_| None).is_none(),
        "no Cilium policy unless SANDBAWKS_FQDN_EGRESS_ENABLED=true"
    );
}

/// The flag alone is not enough: with only IP/CIDR entries (already enforced as
/// `ipBlock` by the baseline policy) there is no name to back, so no Cilium
/// policy is emitted.
#[test]
fn fqdn_policy_none_when_no_dns_names() {
    let sandbox = fixture_with_egress(&["10.0.0.0/8", "203.0.113.5"], None);
    assert!(
        PodAdapter::cilium_network_policy_for(&sandbox, fqdn_enabled).is_none(),
        "a CIDR-only egressAllow needs no toFQDNs policy"
    );
}

/// With the flag on and a DNS name present, the adapter emits a
/// CiliumNetworkPolicy that (a) is named/scoped to this sandbox, (b) carries a
/// DNS-visibility rule routing port 53 through Cilium's DNS proxy, and (c) lists
/// the name under `toFQDNs`.
#[test]
fn fqdn_policy_emits_tofqdns_and_dns_visibility() {
    let sandbox = fixture_with_egress(&["api.example.com", "git.example.com"], None);
    let policy = PodAdapter::cilium_network_policy_for(&sandbox, fqdn_enabled)
        .expect("flag on + DNS name ⇒ a Cilium policy");

    // GVK + identity.
    let types = policy.types.as_ref().expect("typeMeta set");
    assert_eq!(types.api_version, "cilium.io/v2");
    assert_eq!(types.kind, "CiliumNetworkPolicy");
    assert_eq!(
        policy.metadata.name.as_deref(),
        Some("sandbox-sbx-test-fqdn"),
        "name derived from the sandbox, distinct -fqdn suffix"
    );
    assert_eq!(policy.metadata.namespace.as_deref(), Some("team-acme"));

    // endpointSelector targets this sandbox's unique identity label.
    assert_eq!(
        policy.data["spec"]["endpointSelector"]["matchLabels"]["sandbawks.dev/sandbox"].as_str(),
        Some("sbx-test"),
        "endpointSelector must scope the policy to this sandbox's Pod"
    );

    // DNS-visibility rule: a toPorts on 53 with a DNS proxy matchPattern so
    // toFQDNs can learn the name→IP mapping.
    let dns_rule = cilium_egress_rules(&policy)
        .iter()
        .find(|rule| rule.get("toPorts").is_some())
        .expect("a DNS-visibility toPorts rule must be present");
    assert_eq!(
        dns_rule["toPorts"][0]["ports"][0]["port"].as_str(),
        Some("53"),
        "DNS visibility must route port 53"
    );
    assert_eq!(
        dns_rule["toPorts"][0]["rules"]["dns"][0]["matchPattern"].as_str(),
        Some("*"),
        "DNS proxy rule (matchPattern) is mandatory for toFQDNs to populate"
    );
    // Default CoreDNS resolver placement.
    assert_eq!(
        dns_rule["toEndpoints"][0]["matchLabels"]["io.kubernetes.pod.namespace"].as_str(),
        Some("kube-system"),
    );
    assert_eq!(
        dns_rule["toEndpoints"][0]["matchLabels"]["k8s-app"].as_str(),
        Some("kube-dns"),
    );

    // Both names appear under toFQDNs (and nothing else).
    assert_eq!(
        cilium_match_names(&policy),
        vec!["api.example.com".to_string(), "git.example.com".to_string()],
    );
}

/// A mixed list materializes only the DNS *names* under `toFQDNs`; IP/CIDR
/// entries are left to the baseline NetworkPolicy's `ipBlock` rules.
#[test]
fn fqdn_policy_ignores_cidr_entries_in_mixed_list() {
    let sandbox = fixture_with_egress(&["10.0.0.0/8", "api.example.com", "203.0.113.5"], None);
    let policy = PodAdapter::cilium_network_policy_for(&sandbox, fqdn_enabled)
        .expect("a DNS name is present");
    assert_eq!(
        cilium_match_names(&policy),
        vec!["api.example.com".to_string()],
        "only DNS names belong in toFQDNs; CIDRs stay in the baseline policy"
    );
}

/// The DNS resolver namespace and pod label are configurable (no distro
/// hardcode): an override flows into the DNS-visibility selector, and a
/// malformed (no `=`) label override or an empty namespace override falls back
/// to the CoreDNS default rather than emitting a never-matching selector.
#[test]
fn fqdn_policy_dns_resolver_configurable() {
    let sandbox = fixture_with_egress(&["api.example.com"], None);
    let policy = PodAdapter::cilium_network_policy_for(&sandbox, |name| match name {
        "SANDBAWKS_FQDN_EGRESS_ENABLED" => Some("true".to_owned()),
        "SANDBAWKS_FQDN_DNS_NAMESPACE" => Some("dns-system".to_owned()),
        "SANDBAWKS_FQDN_DNS_POD_LABEL" => Some("app=coredns".to_owned()),
        _ => None,
    })
    .expect("flag on + DNS name");

    let dns_rule = cilium_egress_rules(&policy)
        .iter()
        .find(|rule| rule.get("toPorts").is_some())
        .expect("DNS-visibility rule");
    assert_eq!(
        dns_rule["toEndpoints"][0]["matchLabels"]["io.kubernetes.pod.namespace"].as_str(),
        Some("dns-system"),
    );
    assert_eq!(
        dns_rule["toEndpoints"][0]["matchLabels"]["app"].as_str(),
        Some("coredns"),
    );

    // Malformed label override (no '=') falls back to the default rather than
    // emitting a broken selector.
    let fallback = PodAdapter::cilium_network_policy_for(&sandbox, |name| match name {
        "SANDBAWKS_FQDN_EGRESS_ENABLED" => Some("true".to_owned()),
        "SANDBAWKS_FQDN_DNS_POD_LABEL" => Some("garbage-no-eq".to_owned()),
        _ => None,
    })
    .expect("flag on + DNS name");
    let fallback_rule = cilium_egress_rules(&fallback)
        .iter()
        .find(|rule| rule.get("toPorts").is_some())
        .expect("DNS-visibility rule");
    assert_eq!(
        fallback_rule["toEndpoints"][0]["matchLabels"]["k8s-app"].as_str(),
        Some("kube-dns"),
        "a malformed label override must fall back to the CoreDNS default",
    );

    // An empty namespace override (`dnsNamespace: ""`) must also fall back —
    // an empty selector value would never match the resolver.
    let empty_ns = PodAdapter::cilium_network_policy_for(&sandbox, |name| match name {
        "SANDBAWKS_FQDN_EGRESS_ENABLED" => Some("true".to_owned()),
        "SANDBAWKS_FQDN_DNS_NAMESPACE" => Some(String::new()),
        _ => None,
    })
    .expect("flag on + DNS name");
    let empty_ns_rule = cilium_egress_rules(&empty_ns)
        .iter()
        .find(|rule| rule.get("toPorts").is_some())
        .expect("DNS-visibility rule");
    assert_eq!(
        empty_ns_rule["toEndpoints"][0]["matchLabels"]["io.kubernetes.pod.namespace"].as_str(),
        Some("kube-system"),
        "an empty namespace override must fall back to the default",
    );
}

/// When the Sandbox has a server-assigned uid, the Cilium policy owner-references
/// the CR so kube garbage-collects it with the sandbox (mirroring the Pod, env
/// Secret, and baseline NetworkPolicy).
#[test]
fn fqdn_policy_owner_references_sandbox_when_uid_present() {
    let with_uid = fixture_with_egress(&["api.example.com"], Some("uid-123"));
    let owners = PodAdapter::cilium_network_policy_for(&with_uid, fqdn_enabled)
        .expect("policy emitted")
        .metadata
        .owner_references
        .expect("owner_references present when uid set");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].uid, "uid-123");
    assert_eq!(owners[0].kind, "Sandbox");
    assert_eq!(owners[0].controller, Some(true));

    let without_uid = fixture_with_egress(&["api.example.com"], None);
    assert!(
        PodAdapter::cilium_network_policy_for(&without_uid, fqdn_enabled)
            .expect("policy emitted")
            .metadata
            .owner_references
            .is_none(),
        "no owner reference when the sandbox has no uid yet"
    );
}

/// Drive `create_or_ack_cilium_network_policy` against a mocked kube server that
/// answers the single `POST .../ciliumnetworkpolicies` with `status`, returning
/// the adapter's result. Asserts the request is the expected POST to the Cilium
/// CRD path. Tests the error-handling branches without mutating the process
/// environment (the `create_pod` gate is an env var, which would race the other
/// mock tests in this binary).
async fn run_cilium_policy_create(status: u16) -> Result<(), sandbawks_types::RuntimeError> {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    // A DNS-name egressAllow + the flag on ⇒ a policy is built; we POST it
    // directly to exercise the create/ack branches.
    let sandbox = fixture_with_egress(&["api.example.com"], None);
    let policy =
        PodAdapter::cilium_network_policy_for(&sandbox, fqdn_enabled).expect("policy built");

    let task = tokio::spawn(async move {
        adapter
            .create_or_ack_cilium_network_policy("team-acme", &policy)
            .await
    });

    let (req, send) = handle
        .next_request()
        .await
        .expect("create_or_ack must POST the CiliumNetworkPolicy");
    assert_eq!(
        req.method(),
        "POST",
        "expected POST for CiliumNetworkPolicy"
    );
    assert!(
        req.uri()
            .path()
            .ends_with("/namespaces/team-acme/ciliumnetworkpolicies"),
        "unexpected CiliumNetworkPolicy path: {}",
        req.uri().path()
    );

    let response = if status == 201 {
        // Echo back a minimal valid CiliumNetworkPolicy so the create call
        // deserializes a DynamicObject and returns Ok.
        let body = serde_json::json!({
            "apiVersion": "cilium.io/v2",
            "kind": "CiliumNetworkPolicy",
            "metadata": { "name": "sandbox-sbx-test-fqdn", "namespace": "team-acme" },
            "spec": {},
        });
        Response::builder()
            .status(201)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .expect("build created response")
    } else {
        status_error_response(status, "Failure")
    };
    send.send_response(response);

    task.await.expect("join create_or_ack task")
}

/// A successful `201 Created` POST yields `Ok`.
#[tokio::test(flavor = "current_thread")]
async fn cilium_policy_create_201_succeeds() {
    run_cilium_policy_create(201)
        .await
        .expect("201 Created must yield Ok");
}

/// A `409 AlreadyExists` (a racing reconcile already created it) is swallowed as
/// idempotent success, mirroring the baseline NetworkPolicy path.
#[tokio::test(flavor = "current_thread")]
async fn cilium_policy_create_409_is_idempotent() {
    run_cilium_policy_create(409)
        .await
        .expect("409 AlreadyExists must be treated as idempotent success");
}

/// A `404 NotFound` (CRD absent — flag set but Cilium/CRD missing) degrades
/// gracefully to `Ok` so sandbox creation is not bricked; DNS-name egress falls
/// back to the baseline DNS-port-only path.
#[tokio::test(flavor = "current_thread")]
async fn cilium_policy_create_404_degrades_gracefully() {
    run_cilium_policy_create(404)
        .await
        .expect("404 (CRD absent) must degrade to Ok, not error");
}

/// Any other apiserver error (e.g. `500`) maps to `RuntimeError::Transient` so
/// the reconcile loop requeues rather than masking a real failure.
#[tokio::test(flavor = "current_thread")]
async fn cilium_policy_create_other_error_is_transient() {
    let result = run_cilium_policy_create(500).await;
    assert!(
        matches!(result, Err(sandbawks_types::RuntimeError::Transient(_))),
        "a non-409/404 error must map to RuntimeError::Transient, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Issue #542: `ensure_egress_policy` reconciles the live egress NetworkPolicy
// to `spec.networkPolicy` for an already-Running (warm-claimed) sandbox.
// ---------------------------------------------------------------------------

/// When the live policy's egress differs from what `spec.networkPolicy` now
/// implies (the warm-claim per-create union), the adapter PATCHes the live
/// NetworkPolicy with the desired egress rules. Drives a mocked kube server
/// over the GET (read live) → PATCH (write desired) sequence.
#[tokio::test(flavor = "current_thread")]
async fn ensure_egress_policy_patches_live_policy_when_egress_differs() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    // The spec now names a CIDR destination (the per-create override the claim
    // path unioned in). `ensure_egress_policy` must propagate it to the live
    // policy.
    let sandbox = fixture_with_egress(&["10.0.0.0/8"], Some("uid-1"));
    let task = tokio::spawn(async move { adapter.ensure_egress_policy(&sandbox).await });

    // 1. GET the live policy — answer with a default (empty-egress) policy so
    //    the adapter sees a difference and proceeds to patch.
    let (req, send) = handle
        .next_request()
        .await
        .expect("ensure_egress_policy must GET the live NetworkPolicy first");
    assert_eq!(req.method(), "GET", "first hop reads the live policy");
    assert!(
        req.uri()
            .path()
            .ends_with("/namespaces/team-acme/networkpolicies/sandbox-sbx-test-egress"),
        "unexpected GET path: {}",
        req.uri().path()
    );
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&NetworkPolicyResource::default())
                    .expect("serialize live policy"),
            ))
            .expect("build GET response"),
    );

    // 2. PATCH the live policy with the desired egress — assert the body carries
    //    the CIDR rule the spec now names.
    let (req, send) = handle
        .next_request()
        .await
        .expect("a differing egress must trigger a PATCH");
    assert_eq!(req.method(), "PATCH", "the live policy is patched to spec");
    assert!(
        req.uri()
            .path()
            .ends_with("/namespaces/team-acme/networkpolicies/sandbox-sbx-test-egress"),
        "unexpected PATCH path: {}",
        req.uri().path()
    );
    let body = req
        .into_body()
        .collect_bytes()
        .await
        .expect("collect PATCH body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("PATCH body is JSON");
    let egress = json["spec"]["egress"]
        .as_array()
        .expect("the patch sets spec.egress to an array");
    assert!(
        egress.iter().any(|rule| {
            rule["to"]
                .as_array()
                .map(|peers| {
                    peers
                        .iter()
                        .any(|p| p["ipBlock"]["cidr"] == serde_json::json!("10.0.0.0/8"))
                })
                .unwrap_or(false)
        }),
        "the patched egress must include the per-create CIDR rule, got {json}"
    );
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&NetworkPolicyResource::default())
                    .expect("serialize patched policy"),
            ))
            .expect("build PATCH response"),
    );

    task.await
        .expect("join ensure_egress_policy task")
        .expect("ensure_egress_policy succeeds");
}

/// When the live policy already matches `spec.networkPolicy`, the adapter issues
/// the read GET and then **no** write — proving the reconcile is idempotent and
/// a quiescent Running pass costs at most one GET (no resourceVersion churn).
#[tokio::test(flavor = "current_thread")]
async fn ensure_egress_policy_is_noop_when_live_policy_matches() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let sandbox = fixture_with_egress(&["10.0.0.0/8"], Some("uid-1"));
    // The live policy the GET returns is exactly what the adapter would build
    // from the same spec + environment — built with the same env lookup the
    // adapter uses, so the comparison is deterministic regardless of which OTel
    // vars the test host happens to set.
    let live = PodAdapter::network_policy_for(&sandbox, |n| std::env::var(n).ok());
    let live_body = serde_json::to_vec(&live).expect("serialize matching live policy");

    let task = tokio::spawn(async move { adapter.ensure_egress_policy(&sandbox).await });

    let (req, send) = handle
        .next_request()
        .await
        .expect("ensure_egress_policy GETs the live policy");
    assert_eq!(req.method(), "GET");
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(live_body))
            .expect("build GET response"),
    );

    // No second request: a matching live policy must not be patched. This is
    // proven by the task *completing* — had the adapter issued a PATCH it would
    // await a response this test never sends, hanging `task.await`. Cilium is
    // gated off without the FQDN env, so the GET is the only hop.
    task.await
        .expect("join ensure_egress_policy task")
        .expect("ensure_egress_policy is a no-op when the live policy matches");
    drop(handle);
}

/// Issue #542 + #541: a warm-claim reconcile whose `spec.networkPolicy` names a
/// DNS *host* (and the cluster opted into FQDN egress) must widen the companion
/// Cilium `toFQDNs` policy, not just the baseline NetworkPolicy. Drives
/// `reconcile_egress_policy` with an FQDN-enabling `lookup` (no process-env
/// mutation, mirroring the other Cilium tests in this binary) over the
/// baseline-GET → baseline-PATCH → cilium-PATCH sequence.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_egress_policy_patches_cilium_fqdn_companion() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let sandbox = fixture_with_egress(&["git.example.com"], Some("uid-1"));
    let task = tokio::spawn(async move {
        adapter
            .reconcile_egress_policy(&sandbox, fqdn_enabled)
            .await
    });

    // 1. GET baseline policy → answer empty so it differs and gets patched.
    let (req, send) = handle.next_request().await.expect("baseline GET");
    assert_eq!(req.method(), "GET");
    assert!(req
        .uri()
        .path()
        .ends_with("/networkpolicies/sandbox-sbx-test-egress"));
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&NetworkPolicyResource::default()).unwrap(),
            ))
            .expect("build baseline GET response"),
    );

    // 2. PATCH baseline policy (DNS name ⇒ a DNS egress rule ⇒ differs) → ack.
    let (req, send) = handle.next_request().await.expect("baseline PATCH");
    assert_eq!(req.method(), "PATCH");
    assert!(req
        .uri()
        .path()
        .ends_with("/networkpolicies/sandbox-sbx-test-egress"));
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&NetworkPolicyResource::default()).unwrap(),
            ))
            .expect("build baseline PATCH response"),
    );

    // 3. PATCH the Cilium FQDN companion — the surface this test exists to cover.
    let (req, send) = handle.next_request().await.expect("cilium PATCH");
    assert_eq!(
        req.method(),
        "PATCH",
        "the FQDN companion is updated via merge-patch, not create"
    );
    assert!(
        req.uri()
            .path()
            .ends_with("/ciliumnetworkpolicies/sandbox-sbx-test-fqdn"),
        "unexpected Cilium policy path: {}",
        req.uri().path()
    );
    let body = req.into_body().collect_bytes().await.expect("cilium body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("cilium patch is JSON");
    let egress = json["spec"]["egress"]
        .as_array()
        .expect("cilium patch sets spec.egress");
    assert!(
        egress.iter().any(|rule| {
            rule["toFQDNs"]
                .as_array()
                .map(|fqdns| {
                    fqdns
                        .iter()
                        .any(|f| f["matchName"] == serde_json::json!("git.example.com"))
                })
                .unwrap_or(false)
        }),
        "the Cilium patch must carry the per-create DNS name as a toFQDNs rule, got {json}"
    );
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "cilium.io/v2",
                    "kind": "CiliumNetworkPolicy",
                    "metadata": { "name": "sandbox-sbx-test-fqdn", "namespace": "team-acme" },
                    "spec": {},
                }))
                .unwrap(),
            ))
            .expect("build cilium PATCH response"),
    );

    task.await
        .expect("join reconcile_egress_policy task")
        .expect("reconcile_egress_policy succeeds with the FQDN companion");
}

/// Issue #542 + #541: when the Cilium companion doesn't exist yet, the merge
/// PATCH returns `404` and `ensure_cilium_network_policy` falls back to creating
/// it (a fresh object, not a missing CRD). Drives the
/// baseline-GET → baseline-PATCH → cilium-PATCH(404) → cilium-POST(201) sequence.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_egress_policy_creates_cilium_companion_on_404() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let sandbox = fixture_with_egress(&["git.example.com"], Some("uid-1"));
    let task = tokio::spawn(async move {
        adapter
            .reconcile_egress_policy(&sandbox, fqdn_enabled)
            .await
    });

    // 1+2: baseline GET (empty) then PATCH — same as the happy-path test.
    let (req, send) = handle.next_request().await.expect("baseline GET");
    assert_eq!(req.method(), "GET");
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&NetworkPolicyResource::default()).unwrap(),
            ))
            .expect("build baseline GET response"),
    );
    let (req, send) = handle.next_request().await.expect("baseline PATCH");
    assert_eq!(req.method(), "PATCH");
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&NetworkPolicyResource::default()).unwrap(),
            ))
            .expect("build baseline PATCH response"),
    );

    // 3. Cilium PATCH → 404 (the companion object isn't there yet).
    let (req, send) = handle.next_request().await.expect("cilium PATCH");
    assert_eq!(req.method(), "PATCH");
    assert!(req
        .uri()
        .path()
        .ends_with("/ciliumnetworkpolicies/sandbox-sbx-test-fqdn"));
    send.send_response(status_error_response(404, "NotFound"));

    // 4. Fallback create: POST the companion → 201.
    let (req, send) = handle.next_request().await.expect("cilium POST fallback");
    assert_eq!(
        req.method(),
        "POST",
        "a 404 on the merge PATCH must fall back to creating the companion"
    );
    assert!(
        req.uri().path().ends_with("/ciliumnetworkpolicies"),
        "unexpected Cilium create path: {}",
        req.uri().path()
    );
    send.send_response(
        Response::builder()
            .status(201)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "cilium.io/v2",
                    "kind": "CiliumNetworkPolicy",
                    "metadata": { "name": "sandbox-sbx-test-fqdn", "namespace": "team-acme" },
                    "spec": {},
                }))
                .unwrap(),
            ))
            .expect("build cilium POST response"),
    );

    task.await
        .expect("join reconcile_egress_policy task")
        .expect("reconcile_egress_policy creates the companion on a 404 PATCH");
}
