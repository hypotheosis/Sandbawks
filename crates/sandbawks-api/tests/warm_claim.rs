//! Router-level tests for the warm-pool claim fast path on
//! `POST /sandboxes` (Plan 08 Workstream A, task A7).
//!
//! Driven through the real `sandboxes_router` with a `FakeWarmClaim`
//! injected into `AppState`, so they cover the handler's branch — claim
//! when a pool covers the template, cold-boot otherwise — without a kube
//! apiserver.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use sandbawks_api::auth::FakeKeyStore;
use sandbawks_api::routes::sandboxes::router as sandboxes_router;
use sandbawks_api::warm_claim::FakeWarmClaim;
use sandbawks_api::AppState;
use sandbawks_types::{ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus};
use serde_json::{json, Value};
use tower::ServiceExt;

fn key_store() -> Arc<FakeKeyStore> {
    let store = Arc::new(FakeKeyStore::default());
    store.insert("sbk_a1b2c3d4xxxxxxxx", "team-acme", &["sandboxes:*"]);
    store
}

/// A `Running` warm sandbox carrying the given name.
fn running(name: &str) -> Sandbox {
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
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Running,
        ..SandboxStatus::default()
    });
    s
}

fn create_request(body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/sandboxes")
        .header("content-type", "application/json")
        .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn create_claims_warm_sandbox_when_pool_covers_template() {
    // Pool covers `python-data-sci` with one ready member.
    let warm = Arc::new(FakeWarmClaim::with_pool(
        "python-data-sci",
        "py-pool",
        vec![running("sbx-warm-abc123")],
    ));
    let state = AppState::for_test("team-acme", key_store()).with_warm_claim(warm);
    let app = sandboxes_router(state);

    let resp = app
        .oneshot(create_request(json!({
            "templateID": "python-data-sci",
            "metadata": {"user": "alice"},
            "envVars": {"OPENAI_API_KEY": "sk-test"},
            "timeout": 3600,
        })))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    // The claim adopts the warm sandbox's existing name — NOT a fresh
    // `sbx-<random>` cold-boot id.
    assert_eq!(json["sandboxID"], "sbx-warm-abc123");
    assert_eq!(json["templateID"], "python-data-sci");
}

#[tokio::test]
async fn create_cold_boots_when_no_pool_covers_template() {
    // Pool covers a *different* template, so this request misses and
    // falls through to the cold-boot path (default `FakeSandboxOps`).
    let warm = Arc::new(FakeWarmClaim::with_pool(
        "other-template",
        "other-pool",
        vec![running("sbx-warm-nope")],
    ));
    let state = AppState::for_test("team-acme", key_store()).with_warm_claim(warm);
    let app = sandboxes_router(state);

    let resp = app
        .oneshot(create_request(json!({
            "templateID": "python-data-sci",
            "timeout": 3600,
        })))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    // Cold boot mints a fresh `sbx-<random>` id, never the warm name.
    let id = json["sandboxID"].as_str().expect("sandboxID is a string");
    assert!(id.starts_with("sbx-"), "got {id:?}");
    assert_ne!(id, "sbx-warm-nope");
}
