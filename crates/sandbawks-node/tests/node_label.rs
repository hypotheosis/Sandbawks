//! Wire-level tests for the `node_label` reconciler (#272, #282, #379, #417).
//!
//! The in-crate unit tests cover the pure helpers (patch body shape,
//! binary-presence gate, `runtime_handler_gate` classifier). These tests pin
//! the parts they can't reach: that the functions issue the right kube calls
//! against the cluster-scoped Node endpoint (`/api/v1/nodes/<name>`) — a `GET`
//! to read `status.runtimeHandlers` for the #417 gate, then a `PATCH` carrying
//! the `sandbawks.dev/envd-staged` label. Without them, a refactor that broke
//! the kube call — a namespaced `Api` scope, the wrong verb, a missing gate —
//! would slip past the unit tests. Mirrors `sandbawks-controller`'s
//! `tests/pod_adapter.rs`, which mocks the kube transport with
//! `tower-test::mock::pair`.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use http::{Request, Response};
use k8s_openapi::api::core::v1::Node;
use kube::{client::Body, Api, Client};
use sandbawks_node::node_label::{
    reconcile_envd_staged, run_envd_staged_reconciler, AdvertiseOutcome,
    DEFAULT_GVISOR_RUNTIME_HANDLER,
};
use sandbawks_types::ENVD_STAGED_LABEL;

/// JSON-pointer escaping for [`ENVD_STAGED_LABEL`] (`~` → `~0`, `/` → `~1`)
/// so it can index into a decoded merge-patch body.
fn label_pointer() -> String {
    format!(
        "/metadata/labels/{}",
        ENVD_STAGED_LABEL.replace('~', "~0").replace('/', "~1")
    )
}

/// Answer the Node `GET` the #417 gate issues to read
/// `status.runtimeHandlers`, responding with a Node whose handler list is
/// `runtime_handlers`. Pass `&["runc", "runsc"]` for a gVisor-capable node,
/// `&["runc"]` for the #417 failure node, or `&[]` for an Unknown (no-handler)
/// node.
async fn answer_node_get(
    handle: &mut tower_test::mock::Handle<Request<Body>, Response<Body>>,
    runtime_handlers: &[&str],
) {
    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue a GET to read runtime handlers");

    assert_eq!(
        req.method(),
        "GET",
        "reading status.runtimeHandlers is a GET"
    );
    assert!(
        req.uri().path().ends_with("/api/v1/nodes/node-a"),
        "GET must target the cluster-scoped Node by name, got {}",
        req.uri().path(),
    );

    let handlers: Vec<serde_json::Value> = runtime_handlers
        .iter()
        .map(|name| serde_json::json!({ "name": name }))
        .collect();
    let node = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": "node-a" },
        "status": { "runtimeHandlers": handlers },
    });
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&node).unwrap()))
            .expect("build node get response"),
    );
}

/// Pull the single Node PATCH the kube client issues off the mock handle
/// and return its decoded merge-patch body, echoing a minimal Node back so
/// the client's patch call resolves `Ok`.
async fn take_node_patch_body(
    handle: &mut tower_test::mock::Handle<Request<Body>, Response<Body>>,
) -> serde_json::Value {
    let (req, send) = handle
        .next_request()
        .await
        .expect("kube client should issue a PATCH");

    assert_eq!(req.method(), "PATCH", "labeling a Node is a PATCH");
    assert!(
        req.uri().path().ends_with("/api/v1/nodes/node-a"),
        "PATCH must target the cluster-scoped Node by name, got {}",
        req.uri().path(),
    );

    let body_bytes = req
        .into_body()
        .collect_bytes()
        .await
        .expect("collect PATCH body");
    let body: serde_json::Value =
        serde_json::from_slice(&body_bytes).expect("decode merge-patch body");

    let node = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": "node-a" },
    });
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&node).unwrap()))
            .expect("build node response"),
    );

    body
}

#[tokio::test(flavor = "current_thread")]
async fn advertise_envd_staged_patches_the_node_with_the_label() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");

    // `advertise_envd_staged` refuses to label unless the staged binary
    // is actually present, so back it with a real temp file.
    let envd_bin = tempfile::NamedTempFile::new().expect("create temp envd binary");
    let envd_path = envd_bin.path().to_path_buf();

    // The adapter call is awaited concurrently with the mock-server side
    // below — without spawning we'd deadlock waiting for a request we
    // never send.
    let advertise_task = tokio::spawn(async move {
        sandbawks_node::node_label::advertise_envd_staged(
            client,
            "node-a",
            &envd_path,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
        )
        .await
    });

    // #417: the gate first reads the node's runtime handlers — report a
    // gVisor-capable node so advertise proceeds to label.
    answer_node_get(&mut handle, &["runc", "runsc"]).await;

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer())
            .and_then(serde_json::Value::as_str),
        Some("true"),
        "patch body must set the envd-staged label, got: {body}"
    );

    assert_eq!(
        advertise_task
            .await
            .expect("join advertise task")
            .expect("advertise_envd_staged returns Ok once the apiserver accepts the patch"),
        AdvertiseOutcome::Advertised,
        "binary present + runsc handler registered → Advertised"
    );
}

/// #417: when the gVisor handler is absent from `status.runtimeHandlers`, the
/// boot advertise must withdraw the label (null patch) and return
/// `WithdrawnRunscAbsent` **without erroring** — restarting the Pod can't
/// install runsc, so crashing would just CrashLoop.
#[tokio::test(flavor = "current_thread")]
async fn advertise_withholds_label_when_runtime_handler_absent() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");

    let envd_bin = tempfile::NamedTempFile::new().expect("create temp envd binary");
    let envd_path = envd_bin.path().to_path_buf();

    let advertise_task = tokio::spawn(async move {
        sandbawks_node::node_label::advertise_envd_staged(
            client,
            "node-a",
            &envd_path,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
        )
        .await
    });

    // Node reports handlers WITHOUT runsc — the #417 failure node.
    answer_node_get(&mut handle, &["runc", "runhcs-wcow-process"]).await;

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer()),
        Some(&serde_json::Value::Null),
        "absent runsc handler must patch the label to null, got: {body}"
    );

    assert_eq!(
        advertise_task
            .await
            .expect("join advertise task")
            .expect("advertise returns Ok (no crash) when runsc absent"),
        AdvertiseOutcome::WithdrawnRunscAbsent,
        "binary present + runsc handler absent → WithdrawnRunscAbsent, no error"
    );
}

/// #282: when the staged binary is gone, a reconcile tick must PATCH the
/// label *removed* (merge-patch `null`) so the nodeAffinity gate stops
/// scheduling sandboxes onto a node that can no longer host them.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_withdraws_label_when_binary_missing() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);

    // Point at a path that does not exist: the binary has "disappeared".
    let dir = tempfile::tempdir().expect("create temp dir");
    let missing = dir.path().join("envd");

    let gate = AtomicBool::new(false);
    let task = tokio::spawn(async move {
        reconcile_envd_staged(
            &nodes,
            "node-a",
            &missing,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
            &gate,
        )
        .await
    });

    // runsc handler IS registered, so the reconcile passes the #417 gate and
    // reaches the binary-missing path.
    answer_node_get(&mut handle, &["runc", "runsc"]).await;

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer()),
        Some(&serde_json::Value::Null),
        "missing binary must patch the envd-staged label to null, got: {body}"
    );

    assert_eq!(
        task.await
            .expect("join reconcile task")
            .expect("reconcile Ok"),
        AdvertiseOutcome::Withdrawn,
        "a missing binary reconciles to Withdrawn"
    );
}

/// #282: when the staged binary is present, a reconcile tick re-asserts
/// the label (idempotent re-advertise), so a node that lost and regained
/// its binary is re-admitted to scheduling within one tick.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_readvertises_label_when_binary_present() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);

    let envd_bin = tempfile::NamedTempFile::new().expect("create temp envd binary");
    let envd_path = envd_bin.path().to_path_buf();

    let gate = AtomicBool::new(false);
    let task = tokio::spawn(async move {
        reconcile_envd_staged(
            &nodes,
            "node-a",
            &envd_path,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
            &gate,
        )
        .await
    });

    answer_node_get(&mut handle, &["runc", "runsc"]).await;

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer())
            .and_then(serde_json::Value::as_str),
        Some("true"),
        "present binary must patch the envd-staged label to its value, got: {body}"
    );

    assert_eq!(
        task.await
            .expect("join reconcile task")
            .expect("reconcile Ok"),
        AdvertiseOutcome::Advertised,
        "a present binary reconciles to Advertised"
    );
}

/// #417: when the gVisor handler is absent from `status.runtimeHandlers`, a
/// reconcile tick withdraws the label (null patch) and returns
/// `WithdrawnRunscAbsent` — even with the staged binary present and the
/// runsc-root gate clear, isolating the runtime-handler cause from #282/#379.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_withdraws_label_when_runtime_handler_absent() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);

    // Staged binary present: isolates the runsc-absent cause.
    let envd_bin = tempfile::NamedTempFile::new().expect("create temp envd binary");
    let envd_path = envd_bin.path().to_path_buf();

    let gate = AtomicBool::new(false);
    let task = tokio::spawn(async move {
        reconcile_envd_staged(
            &nodes,
            "node-a",
            &envd_path,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
            &gate,
        )
        .await
    });

    // Node reports a non-empty handler list WITHOUT runsc.
    answer_node_get(&mut handle, &["runc", "runhcs-wcow-process"]).await;

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer()),
        Some(&serde_json::Value::Null),
        "absent runsc handler must patch the label to null, got: {body}"
    );

    assert_eq!(
        task.await
            .expect("join reconcile task")
            .expect("reconcile Ok"),
        AdvertiseOutcome::WithdrawnRunscAbsent,
        "binary present + gate clear + runsc handler absent → WithdrawnRunscAbsent (#417)"
    );
}

/// #417 graceful degradation: a node reporting *no* handler list at all
/// (older CRI/kubelet) is `Unknown`, not `Absent` — the gate must not block,
/// so the reconcile advertises as long as the binary is present. Otherwise the
/// gate would strand every node on a cluster that doesn't surface handlers.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_advertises_when_runtime_handlers_unreported() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);

    let envd_bin = tempfile::NamedTempFile::new().expect("create temp envd binary");
    let envd_path = envd_bin.path().to_path_buf();

    let gate = AtomicBool::new(false);
    let task = tokio::spawn(async move {
        reconcile_envd_staged(
            &nodes,
            "node-a",
            &envd_path,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
            &gate,
        )
        .await
    });

    // Empty handler list → Unknown → do not block.
    answer_node_get(&mut handle, &[]).await;

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer())
            .and_then(serde_json::Value::as_str),
        Some("true"),
        "unreported handlers must not block; label asserted, got: {body}"
    );

    assert_eq!(
        task.await
            .expect("join reconcile task")
            .expect("reconcile Ok"),
        AdvertiseOutcome::Advertised,
        "binary present + handlers Unknown → Advertised (graceful degradation)"
    );
}

/// #282: drive the periodic loop itself (not just one `reconcile_envd_staged`
/// tick) under paused time, asserting the semantics that are the actual
/// point of #282: the immediate first interval tick is skipped (the boot
/// advertise already covered it), then the loop tracks the binary across
/// ticks — re-asserting `true` while present, withdrawing (`null`) once it
/// disappears. Each tick now reads `status.runtimeHandlers` (GET) before the
/// PATCH; the node reports a gVisor-capable handler set throughout.
///
/// `start_paused` auto-advances the clock to the next pending timer
/// whenever the runtime is otherwise idle, so rather than fight it with
/// manual `advance`, we let it drive the loop and observe two things: the
/// *logical time* elapsed before the first PATCH (which proves the skip —
/// it lands a full interval in, not at t=0), and the patch bodies across
/// the present→missing transition. The loop is naturally rate-limited by
/// the mock responses: it parks awaiting each request's reply before its
/// next `tick()`, so removing the binary between responses deterministically
/// sequences the two reconciles.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn run_reconciler_skips_first_tick_then_tracks_binary() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");

    // Staged binary present to start. A real dir+file (not NamedTempFile)
    // so the test controls exactly when it "disappears".
    let dir = tempfile::tempdir().expect("create temp dir");
    let envd_path = dir.path().join("envd");
    std::fs::write(&envd_path, b"#!/bin/true").expect("write fake staged binary");

    let interval = Duration::from_secs(30);
    let start = tokio::time::Instant::now();
    let reconciler = tokio::spawn(run_envd_staged_reconciler(
        client,
        "node-a".to_owned(),
        envd_path.clone(),
        DEFAULT_GVISOR_RUNTIME_HANDLER.to_owned(),
        interval,
        Arc::new(AtomicBool::new(false)),
    ));

    // First real reconcile: binary present, handler registered, label
    // re-asserted. The GET+PATCH land a full interval after start because the
    // immediate first tick is skipped — if that skip regressed, the first
    // request would arrive at t=0.
    answer_node_get(&mut handle, &["runc", "runsc"]).await;
    let body = take_node_patch_body(&mut handle).await;
    assert!(
        start.elapsed() >= interval,
        "first reconcile must land one full interval in (skip-first-tick), \
         elapsed={:?}",
        start.elapsed()
    );
    assert_eq!(
        body.pointer(&label_pointer())
            .and_then(serde_json::Value::as_str),
        Some("true"),
        "first reconcile re-asserts the label while the binary is present, got: {body}"
    );

    // Binary disappears between responses; the loop is parked on its next
    // tick timer, so this happens-before the second reconcile reads disk.
    std::fs::remove_file(&envd_path).expect("remove staged binary");
    answer_node_get(&mut handle, &["runc", "runsc"]).await;
    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer()),
        Some(&serde_json::Value::Null),
        "once the binary is gone the next tick withdraws the label, got: {body}"
    );

    reconciler.abort();
}

/// #379: when `runsc_root_failed` is set, the reconciler must withdraw the
/// label (regardless of binary or handler state) and return
/// `WithdrawnRunscRoot`. The sticky gate short-circuits *before* the #417
/// runtime-handler read, so NO `GET` is issued — only the removal PATCH.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_withdraws_label_when_runsc_root_failed_is_set() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);

    // Binary IS present — proves the gate fires from the capability flag, not
    // the binary-missing path.
    let envd_bin = tempfile::NamedTempFile::new().expect("create temp envd binary");
    let envd_path = envd_bin.path().to_path_buf();

    // Gate is set: a real container failed RunscRootUnresolved.
    let gate = AtomicBool::new(true);

    let task = tokio::spawn(async move {
        reconcile_envd_staged(
            &nodes,
            "node-a",
            &envd_path,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
            &gate,
        )
        .await
    });

    // No GET answered: the sticky gate must withdraw before reading handlers.
    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer()),
        Some(&serde_json::Value::Null),
        "runsc_root_failed gate must withdraw the label (null patch), got: {body}"
    );

    assert_eq!(
        task.await
            .expect("join reconcile task")
            .expect("reconcile Ok"),
        AdvertiseOutcome::WithdrawnRunscRoot,
        "gate set + binary present → WithdrawnRunscRoot outcome"
    );
}

/// #379 fresh-node no-regression: with the sticky gate clear, the binary
/// present, and the runsc handler registered, the reconciler advertises —
/// `Advertised`, not withdrawn.
///
/// A fresh node that has never hosted a container has no runsc root yet (the
/// shim creates it lazily). The gate must remain false at boot so the node
/// keeps advertising and sandboxes can be scheduled here (#366 review concern).
#[tokio::test(flavor = "current_thread")]
async fn reconcile_advertises_when_gate_clear_and_binary_present() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);

    let envd_bin = tempfile::NamedTempFile::new().expect("create temp envd binary");
    let envd_path = envd_bin.path().to_path_buf();

    // Gate is clear: no RunscRootUnresolved failure observed yet (fresh node).
    let gate = AtomicBool::new(false);

    let task = tokio::spawn(async move {
        reconcile_envd_staged(
            &nodes,
            "node-a",
            &envd_path,
            DEFAULT_GVISOR_RUNTIME_HANDLER,
            &gate,
        )
        .await
    });

    answer_node_get(&mut handle, &["runc", "runsc"]).await;

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer())
            .and_then(serde_json::Value::as_str),
        Some("true"),
        "fresh node with gate clear must advertise the label, got: {body}"
    );

    assert_eq!(
        task.await
            .expect("join reconcile task")
            .expect("reconcile Ok"),
        AdvertiseOutcome::Advertised,
        "gate clear + binary present + handler registered → Advertised (fresh-node no-regression)"
    );
}
