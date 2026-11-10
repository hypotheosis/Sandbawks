//! Wire-level tests for the privileged runsc fleet upgrader (#550).
//!
//! The in-crate unit tests cover the pure decision logic (`decide_lock`, the tick
//! orchestration with fakes, the installer staging). These pin the part they
//! can't reach: that [`KubeLeaseUpgradeCoordinator`] talks to the apiserver with
//! exactly the verbs the node RBAC grants — a GET-then-CREATE to *acquire* the
//! cluster-wide upgrade Lease, and a GET-then-DELETE to *release* it only when this
//! node holds it. If these drift from the granted verbs
//! (`coordination.k8s.io/leases: get,create,update,delete`) the upgrade silently
//! 403s. Mirrors `tests/runsc_reconcile.rs`, which mocks the kube transport with
//! `tower-test::mock::pair`.

use http::{Request, Response};
use kube::{client::Body, Client};
use sandbawks_node::runsc_upgrade::{KubeLeaseUpgradeCoordinator, LockState, UpgradeCoordinator};

const NS: &str = "sandbox-ns";
const LEASE: &str = "sandbawks-runsc-upgrade";
const NODE: &str = "node-a";
/// The raw desired version the coordinator is built with, and its canonical
/// `release-<ver>` token (what a converged holder's Node label carries).
const DESIRED: &str = "20260608.0";
const DESIRED_RELEASE: &str = "release-20260608.0";

/// The namespaced Lease collection + named-object URI paths the coordinator hits.
fn collection_path() -> String {
    format!("/apis/coordination.k8s.io/v1/namespaces/{NS}/leases")
}
fn object_path() -> String {
    format!("{}/{LEASE}", collection_path())
}
fn node_path(name: &str) -> String {
    format!("/api/v1/nodes/{name}")
}

/// A Node JSON carrying the given `sandbawks.dev/runsc-version` label (the
/// holder-convergence signal the coordinator reads, #558).
fn node_json(name: &str, runsc_version: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": { "name": name, "labels": { "sandbawks.dev/runsc-version": runsc_version } },
    })
}

/// A `coordination.k8s.io/v1` Lease JSON held by `holder`, renewed "now-ish" so it
/// reads as still-valid (the release path only inspects the holder, not expiry).
fn lease_json(holder: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": { "name": LEASE, "namespace": NS, "resourceVersion": "42" },
        "spec": {
            "holderIdentity": holder,
            "leaseDurationSeconds": 600,
            "renewTime": "2999-01-01T00:00:00.000000Z",
            "leaseTransitions": 0,
        },
    })
}

/// A 404 `Status` body so `get_opt` resolves to `None`.
fn not_found() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1", "kind": "Status", "status": "Failure",
        "reason": "NotFound", "code": 404,
    })
}

fn json_response(status: u16, body: &serde_json::Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .expect("build response")
}

fn coordinator(client: Client) -> KubeLeaseUpgradeCoordinator {
    KubeLeaseUpgradeCoordinator::new(client, NS, LEASE, NODE, 600, DESIRED)
}

#[tokio::test(flavor = "current_thread")]
async fn acquire_creates_the_lease_when_absent() {
    let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let coord = coordinator(Client::new(svc, NS));

    let task = tokio::spawn(async move { coord.acquire().await });

    // 1. GET the (absent) Lease.
    let (req, send) = handle.next_request().await.expect("expect a Lease GET");
    assert_eq!(req.method(), "GET");
    assert!(
        req.uri().path().ends_with(&object_path()),
        "GET must target the named Lease, got {}",
        req.uri().path()
    );
    send.send_response(json_response(404, &not_found()));

    // 2. CREATE it, holder = this node, TTL = configured.
    let (req, send) = handle.next_request().await.expect("expect a Lease CREATE");
    assert_eq!(req.method(), "POST", "claiming an absent Lease is a CREATE");
    assert!(
        req.uri().path().ends_with(&collection_path()),
        "CREATE must POST to the Lease collection, got {}",
        req.uri().path()
    );
    let body = req.into_body().collect_bytes().await.expect("collect body");
    let posted: serde_json::Value = serde_json::from_slice(&body).expect("decode POST body");
    assert_eq!(
        posted
            .pointer("/spec/holderIdentity")
            .and_then(|v| v.as_str()),
        Some(NODE),
        "lease holder must be this node, got: {posted}"
    );
    assert_eq!(
        posted
            .pointer("/spec/leaseDurationSeconds")
            .and_then(serde_json::Value::as_i64),
        Some(600)
    );
    send.send_response(json_response(201, &lease_json(NODE)));

    assert_eq!(
        task.await.expect("join").expect("acquire Ok"),
        LockState::Acquired
    );
}

#[tokio::test(flavor = "current_thread")]
async fn release_deletes_the_lease_when_held_by_this_node() {
    let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let coord = coordinator(Client::new(svc, NS));

    let task = tokio::spawn(async move { coord.release().await });

    // 1. GET the Lease — held by us.
    let (req, send) = handle.next_request().await.expect("expect a Lease GET");
    assert_eq!(req.method(), "GET");
    send.send_response(json_response(200, &lease_json(NODE)));

    // 2. DELETE it (we hold it, so hand off promptly).
    let (req, send) = handle.next_request().await.expect("expect a Lease DELETE");
    assert_eq!(
        req.method(),
        "DELETE",
        "releasing our own Lease is a DELETE"
    );
    assert!(
        req.uri().path().ends_with(&object_path()),
        "DELETE must target the named Lease, got {}",
        req.uri().path()
    );
    send.send_response(json_response(200, &lease_json(NODE)));

    task.await.expect("join").expect("release Ok");
}

#[tokio::test(flavor = "current_thread")]
async fn release_is_a_noop_when_another_node_holds_the_lease() {
    let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let coord = coordinator(Client::new(svc, NS));

    let task = tokio::spawn(async move { coord.release().await });

    // GET the Lease — held by another node. The coordinator must NOT delete it.
    let (req, send) = handle.next_request().await.expect("expect a Lease GET");
    assert_eq!(req.method(), "GET");
    send.send_response(json_response(200, &lease_json("node-b")));

    // The GET is the only call: once it returns a not-ours holder, release is a
    // no-op. Await the task to completion, then assert no further request was
    // issued — a lingering DELETE would surface here as `Ok(Some(..))`.
    task.await.expect("join").expect("release Ok");
    match tokio::time::timeout(std::time::Duration::from_millis(100), handle.next_request()).await {
        Err(_) => {}   // timed out idle — no second request (the expected path).
        Ok(None) => {} // mock transport closed with the dropped client — also fine.
        Ok(Some((req, _))) => {
            panic!(
                "release issued an unexpected {} on another node's Lease",
                req.method()
            )
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn acquire_takes_over_a_converged_holder_before_its_ttl() {
    // #558: another node holds a still-valid lease, but its Node label shows it has
    // already converged → take it over now (GET lease → GET holder Node → PUT lease).
    let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let coord = coordinator(Client::new(svc, NS));
    let task = tokio::spawn(async move { coord.acquire().await });

    // 1. GET the Lease — held by node-b, still valid.
    let (req, send) = handle.next_request().await.expect("expect a Lease GET");
    assert_eq!(req.method(), "GET");
    assert!(req.uri().path().ends_with(&object_path()));
    send.send_response(json_response(200, &lease_json("node-b")));

    // 2. GET the holder Node node-b — its runsc label == desired (converged).
    let (req, send) = handle
        .next_request()
        .await
        .expect("expect a holder Node GET");
    assert_eq!(
        req.method(),
        "GET",
        "convergence check reads the holder Node"
    );
    assert!(
        req.uri().path().ends_with(&node_path("node-b")),
        "must GET the holder Node, got {}",
        req.uri().path()
    );
    send.send_response(json_response(200, &node_json("node-b", DESIRED_RELEASE)));

    // 3. Take over: PUT the Lease with this node as holder.
    let (req, send) = handle
        .next_request()
        .await
        .expect("expect a Lease PUT takeover");
    assert_eq!(
        req.method(),
        "PUT",
        "taking over a converged holder is a replace"
    );
    assert!(req.uri().path().ends_with(&object_path()));
    let body = req.into_body().collect_bytes().await.expect("collect body");
    let put: serde_json::Value = serde_json::from_slice(&body).expect("decode PUT body");
    assert_eq!(
        put.pointer("/spec/holderIdentity").and_then(|v| v.as_str()),
        Some(NODE),
        "takeover must set us as holder, got: {put}"
    );
    send.send_response(json_response(200, &lease_json(NODE)));

    assert_eq!(
        task.await.expect("join").expect("acquire Ok"),
        LockState::Acquired
    );
}

#[tokio::test(flavor = "current_thread")]
async fn acquire_stands_by_for_an_unconverged_holder() {
    // A valid lease held by a node whose label is NOT yet desired → stand by, no
    // takeover (GET lease → GET holder Node → no write, Contended).
    let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let coord = coordinator(Client::new(svc, NS));
    let task = tokio::spawn(async move { coord.acquire().await });

    let (req, send) = handle.next_request().await.expect("expect a Lease GET");
    assert_eq!(req.method(), "GET");
    send.send_response(json_response(200, &lease_json("node-b")));

    let (req, send) = handle
        .next_request()
        .await
        .expect("expect a holder Node GET");
    assert!(req.uri().path().ends_with(&node_path("node-b")));
    send.send_response(json_response(200, &node_json("node-b", "release-OLD")));

    // Holder hasn't converged → Contended, no lease write.
    assert_eq!(
        task.await.expect("join").expect("acquire Ok"),
        LockState::Contended
    );
    match tokio::time::timeout(std::time::Duration::from_millis(100), handle.next_request()).await {
        Err(_) | Ok(None) => {}
        Ok(Some((req, _))) => panic!(
            "must not write the lease for an unconverged holder ({})",
            req.method()
        ),
    }
}
