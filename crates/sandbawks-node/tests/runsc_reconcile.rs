//! Wire-level tests for the runsc version reconciler (#546).
//!
//! The in-crate unit tests cover the pure helpers (version parse, the converge
//! decision gate, apt-channel disarm, atomic install). These pin the parts they
//! can't reach: that [`reconcile_runsc_once`] stamps the
//! `sandbawks.dev/runsc-version` label via a `PATCH` to the cluster-scoped Node
//! endpoint, and that [`KubeLiveSandboxCounter`] issues the right pod `LIST` and
//! counts only non-terminal sandboxes. Mirrors `tests/node_label.rs`, which mocks
//! the kube transport with `tower-test::mock::pair`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use http::{Request, Response};
use k8s_openapi::api::core::v1::Node;
use kube::{client::Body, Api, Client};
use sandbawks_node::runsc_reconcile::{
    reconcile_runsc_once, ConvergeDecision, KubeLiveSandboxCounter, LiveSandboxCounter,
    RunscReconcileConfig, RunscVersionReader, CONVERGE_BINARIES,
};
use sandbawks_types::RUNSC_VERSION_LABEL;

/// JSON-pointer escaping for [`RUNSC_VERSION_LABEL`] (`~` → `~0`, `/` → `~1`)
/// so it can index into a decoded merge-patch body.
fn label_pointer() -> String {
    format!(
        "/metadata/labels/{}",
        RUNSC_VERSION_LABEL.replace('~', "~0").replace('/', "~1")
    )
}

/// A [`RunscVersionReader`] that returns a fixed release for any path — stands in
/// for executing the host/bundled runsc binary.
struct FakeReader(Option<String>);

#[async_trait]
impl RunscVersionReader for FakeReader {
    async fn read_release(&self, _path: &Path) -> anyhow::Result<Option<String>> {
        Ok(self.0.clone())
    }
}

/// A [`LiveSandboxCounter`] returning a fixed count — exercises the convergence
/// gate without an apiserver.
struct FakeCounter(usize);

#[async_trait]
impl LiveSandboxCounter for FakeCounter {
    async fn count_live(&self) -> anyhow::Result<usize> {
        Ok(self.0)
    }
}

/// Build a config whose apt source points at `apt_source`; convergence off.
fn config(apt_source: &Path) -> RunscReconcileConfig {
    RunscReconcileConfig {
        node_name: "node-a".to_owned(),
        host_runsc_path: Path::new("/host/usr/local/bin/runsc").to_path_buf(),
        bundled_bin_dir: Path::new("/usr/local/bin").to_path_buf(),
        host_bin_dir: Path::new("/host/usr/local/bin").to_path_buf(),
        desired_version: "20260525.0".to_owned(),
        auto_reconcile: false,
        apt_source_path: apt_source.to_path_buf(),
        apt_keyring_path: None,
        interval: Duration::from_secs(300),
    }
}

/// Pull the single Node PATCH off the mock handle, echo a minimal Node back so
/// the client resolves `Ok`, and return the decoded merge-patch body.
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
        "apiVersion": "v1", "kind": "Node", "metadata": { "name": "node-a" },
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
async fn reconcile_reports_version_label_and_disarms_apt() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);

    // A present apt source file the tick must remove.
    let dir = tempfile::tempdir().expect("tempdir");
    let apt_source = dir.path().join("gvisor.list");
    std::fs::write(&apt_source, b"deb ... release main\n").expect("write source");
    let cfg = config(&apt_source);

    let reader = FakeReader(Some("release-20260525.0".to_owned()));
    let counter = FakeCounter(0);

    // Drive the reconcile concurrently with the mock-server side.
    let task =
        tokio::spawn(async move { reconcile_runsc_once(&nodes, &cfg, &reader, &counter).await });

    let body = take_node_patch_body(&mut handle).await;
    assert_eq!(
        body.pointer(&label_pointer())
            .and_then(serde_json::Value::as_str),
        Some("release-20260525.0"),
        "patch must set the runsc-version label, got: {body}"
    );

    let outcome = task.await.expect("join").expect("reconcile Ok");
    assert_eq!(outcome.host_release.as_deref(), Some("release-20260525.0"));
    assert!(outcome.apt.source_removed, "apt source must be removed");
    assert!(!apt_source.exists(), "gvisor.list must be gone on disk");
    // auto_reconcile is off → convergence is Disabled and never installs.
    assert_eq!(outcome.converge, ConvergeDecision::Disabled);
    assert!(!outcome.converged);
}

#[tokio::test(flavor = "current_thread")]
async fn live_sandbox_counter_lists_local_sandboxes_and_skips_terminal() {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "sandboxes");
    let counter = KubeLiveSandboxCounter::new(client, "sandboxes", "node-a");

    let task = tokio::spawn(async move { counter.count_live().await });

    let (req, send) = handle.next_request().await.expect("expect a pod LIST");
    assert_eq!(req.method(), "GET", "listing pods is a GET");
    let uri = req.uri().to_string();
    assert!(
        uri.contains("/api/v1/namespaces/sandboxes/pods"),
        "LIST must target the namespaced pods endpoint, got {uri}"
    );
    assert!(
        uri.contains("spec.nodeName%3Dnode-a") || uri.contains("spec.nodeName=node-a"),
        "LIST must field-select the local node, got {uri}"
    );
    assert!(
        uri.contains("sandbawks.dev%2Fsandbox") || uri.contains("sandbawks.dev/sandbox"),
        "LIST must label-select sandboxes, got {uri}"
    );

    let pod = |phase: &str| {
        serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": format!("p-{phase}") },
            "status": { "phase": phase },
        })
    };
    let list = serde_json::json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": { "resourceVersion": "1" },
        "items": [pod("Running"), pod("Pending"), pod("Succeeded"), pod("Failed")],
    });
    send.send_response(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&list).unwrap()))
            .expect("build pod list response"),
    );

    let live = task.await.expect("join").expect("count Ok");
    assert_eq!(
        live, 2,
        "Running + Pending are live; Succeeded + Failed are not"
    );
}

/// A path-aware [`RunscVersionReader`]: returns one release for the host runsc
/// path and another for any other path (the bundled `runsc`), so the convergence
/// branches can be driven end-to-end with distinct host/bundled versions.
struct PathReader {
    host_path: PathBuf,
    host_release: Option<String>,
    bundled_release: Option<String>,
}

#[async_trait]
impl RunscVersionReader for PathReader {
    async fn read_release(&self, path: &Path) -> anyhow::Result<Option<String>> {
        if path == self.host_path {
            Ok(self.host_release.clone())
        } else {
            Ok(self.bundled_release.clone())
        }
    }
}

/// Build a convergence-armed config: `auto_reconcile: true`, desired
/// `20260525.0`, host runsc read from `host_dir/runsc`, bundled source in
/// `bundled_dir`, and an apt source that does not exist (disarm no-ops).
fn converge_config(host_dir: &Path, bundled_dir: &Path) -> RunscReconcileConfig {
    RunscReconcileConfig {
        node_name: "node-a".to_owned(),
        host_runsc_path: host_dir.join("runsc"),
        bundled_bin_dir: bundled_dir.to_path_buf(),
        host_bin_dir: host_dir.to_path_buf(),
        desired_version: "20260525.0".to_owned(),
        auto_reconcile: true,
        apt_source_path: bundled_dir.join("no-such-gvisor.list"),
        apt_keyring_path: None,
        interval: Duration::from_secs(300),
    }
}

/// Drive `reconcile_runsc_once` to completion, answering the single Node
/// version-label PATCH it issues for a present host release, and return the
/// outcome. Shared by the convergence-branch tests below.
async fn run_reconcile(
    cfg: RunscReconcileConfig,
    reader: PathReader,
    counter: FakeCounter,
) -> sandbawks_node::runsc_reconcile::RunscReconcileOutcome {
    let (mock_service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "default");
    let nodes: Api<Node> = Api::all(client);
    let task =
        tokio::spawn(async move { reconcile_runsc_once(&nodes, &cfg, &reader, &counter).await });
    // Host release is present in every convergence test, so exactly one PATCH.
    take_node_patch_body(&mut handle).await;
    task.await.expect("join").expect("reconcile Ok")
}

#[tokio::test(flavor = "current_thread")]
async fn converge_installs_bundled_binaries_on_a_drained_mismatched_node() {
    let bundled = tempfile::tempdir().expect("bundled dir");
    let host = tempfile::tempdir().expect("host dir");
    // Real bundled binaries for the install to copy; a stale host runsc to overwrite.
    for name in CONVERGE_BINARIES {
        std::fs::write(bundled.path().join(name), format!("bundled-{name}")).expect("bundled");
    }
    std::fs::write(host.path().join("runsc"), b"stale").expect("stale host runsc");

    let cfg = converge_config(host.path(), bundled.path());
    let reader = PathReader {
        host_path: cfg.host_runsc_path.clone(),
        host_release: Some("release-OLD".to_owned()),
        bundled_release: Some("release-20260525.0".to_owned()),
    };
    let outcome = run_reconcile(cfg, reader, FakeCounter(0)).await;

    assert_eq!(outcome.converge, ConvergeDecision::Converge);
    assert!(outcome.converged, "drained mismatched node must install");
    for name in CONVERGE_BINARIES {
        assert_eq!(
            std::fs::read_to_string(host.path().join(name)).expect("installed"),
            format!("bundled-{name}"),
            "{name} must be the bundled build on the host"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn converge_is_a_noop_when_host_already_at_desired() {
    let bundled = tempfile::tempdir().expect("bundled dir");
    let host = tempfile::tempdir().expect("host dir");
    let cfg = converge_config(host.path(), bundled.path());
    let reader = PathReader {
        host_path: cfg.host_runsc_path.clone(),
        host_release: Some("release-20260525.0".to_owned()),
        bundled_release: Some("release-20260525.0".to_owned()),
    };
    let outcome = run_reconcile(cfg, reader, FakeCounter(0)).await;
    assert_eq!(outcome.converge, ConvergeDecision::AlreadyConverged);
    assert!(!outcome.converged);
    assert!(!host.path().join("runsc").exists(), "no install on a match");
}

#[tokio::test(flavor = "current_thread")]
async fn converge_is_deferred_while_live_sandboxes_present() {
    let bundled = tempfile::tempdir().expect("bundled dir");
    let host = tempfile::tempdir().expect("host dir");
    let cfg = converge_config(host.path(), bundled.path());
    let reader = PathReader {
        host_path: cfg.host_runsc_path.clone(),
        host_release: Some("release-OLD".to_owned()),
        bundled_release: Some("release-20260525.0".to_owned()),
    };
    let outcome = run_reconcile(cfg, reader, FakeCounter(3)).await;
    assert_eq!(outcome.converge, ConvergeDecision::LiveSandboxes(3));
    assert!(
        !outcome.converged,
        "must not swap the binary under live sandboxes"
    );
    assert!(!host.path().join("runsc").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn converge_refuses_when_bundled_release_mismatches_desired() {
    let bundled = tempfile::tempdir().expect("bundled dir");
    let host = tempfile::tempdir().expect("host dir");
    let cfg = converge_config(host.path(), bundled.path());
    let reader = PathReader {
        host_path: cfg.host_runsc_path.clone(),
        host_release: Some("release-OLD".to_owned()),
        bundled_release: Some("release-WRONG".to_owned()),
    };
    let outcome = run_reconcile(cfg, reader, FakeCounter(0)).await;
    assert_eq!(
        outcome.converge,
        ConvergeDecision::BundledMismatch {
            bundled: Some("release-WRONG".to_owned()),
            desired: "release-20260525.0".to_owned(),
        }
    );
    assert!(!outcome.converged, "must not install a mismatched bundle");
    assert!(!host.path().join("runsc").exists());
}
