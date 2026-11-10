//! Auth-gate smoke tests for the files router (Plan 04 T11 + T12).
//!
//! Every route under `/files/...` that requires `X-API-Key` (mint plus
//! the six T12 metadata routes) must 401 when called without one.
//! Token-authed `/files/{upload,download}` are covered by their own
//! signing tests and are intentionally skipped here.
//!
//! End-to-end round-trips against a live envd land in T15 — these
//! checks just guard the router wiring so a copy/paste regression
//! that drops a route off `auth_middleware` is caught by `cargo test`
//! without standing up a cluster.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use sandbawks_api::auth::FakeKeyStore;
use sandbawks_api::routes::files::router as files_router;
use sandbawks_api::AppState;
use tower::ServiceExt;

/// `AppState` with an empty key store — every request without
/// `X-API-Key` falls through `auth_middleware`'s "missing_api_key"
/// branch, and any key sent in would resolve to `invalid_api_key`.
/// Both render as 401, which is exactly what we're asserting.
fn unauth_state() -> AppState {
    let store = Arc::new(FakeKeyStore::default());
    AppState::for_test("team-acme", store)
}

/// Send `method uri` (with an empty body) against a fresh router and
/// confirm the response is a 401. Body shape isn't checked here — the
/// envelope is shared with the lifecycle routes and already covered
/// by `tests/auth.rs`.
async fn assert_unauth(method: Method, uri: &str) {
    let app = files_router(unauth_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method(method.clone())
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router serves request");
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "expected 401 for {method} {uri}, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn list_requires_auth() {
    assert_unauth(Method::GET, "/files/sbx-abc/list?path=/tmp").await;
}

#[tokio::test]
async fn stat_requires_auth() {
    assert_unauth(Method::GET, "/files/sbx-abc/stat?path=/tmp/x").await;
}

#[tokio::test]
async fn remove_requires_auth() {
    assert_unauth(Method::DELETE, "/files/sbx-abc/remove?path=/tmp/x").await;
}

#[tokio::test]
async fn move_requires_auth() {
    assert_unauth(Method::POST, "/files/sbx-abc/move").await;
}

#[tokio::test]
async fn makedir_requires_auth() {
    assert_unauth(Method::POST, "/files/sbx-abc/makedir").await;
}

#[tokio::test]
async fn watch_requires_auth() {
    assert_unauth(Method::GET, "/files/sbx-abc/watch?path=/tmp").await;
}

#[tokio::test]
async fn mint_still_requires_auth() {
    assert_unauth(Method::POST, "/files/mint").await;
}

/// `/files/mint` binds the URL to the caller's team namespace (issue #13): the
/// upload/download handler resolves the sandbox there, so a key can only
/// mint URLs that reach its own team's sandboxes.
#[tokio::test]
async fn mint_binds_the_url_to_the_callers_namespace() {
    let store = Arc::new(FakeKeyStore::default());
    store.insert("sbk_teamb000xxxxxxxx", "team-b", &["sandboxes:*"]);
    let state = AppState::for_test("team-acme", store);
    let key = state.signing_key.clone();
    let resp = files_router(state)
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/files/mint")
                .header("X-API-Key", "sbk_teamb000xxxxxxxx")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"sandbox_id":"sbx-abc","path":"/tmp/x","method":"GET","ttl":60}"#,
                ))
                .unwrap(),
        )
        .await
        .expect("router serves request");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let url = json["url"].as_str().expect("url in response");
    let token = url.split_once("token=").expect("token in url").1;
    let payload = sandbawks_api::signing::verify(&key, token).expect("minted token verifies");
    assert_eq!(payload.sandbox_id, "sbx-abc");
    assert_eq!(payload.namespace.as_deref(), Some("team-b"));
}

/// Signed `/files/download` resolves the sandbox in the namespace the token
/// was minted for (issue #13). A sandbox that exists only in `team-b`, with no
/// envd address yet, answers `envd_not_ready` (502) when the token names
/// `team-b` — proof it was found there — and `sandbox_not_found` (404) for a
/// token that names another namespace or none (the default, `team-acme`).
#[tokio::test]
async fn signed_download_resolves_in_the_tokens_namespace() {
    use sandbawks_api::signing::{mint, SignedUrlPayload};
    use sandbawks_types::{ResourceQuantity, ResourceRequests, Sandbox, SandboxSpec};

    let state = AppState::for_test("team-acme", Arc::new(FakeKeyStore::default()));
    let spec = SandboxSpec {
        template_ref: "tpl".to_owned(),
        runtime_class_name: None,
        resources: ResourceRequests {
            cpu: "1".parse::<ResourceQuantity>().unwrap(),
            memory: "512Mi".parse::<ResourceQuantity>().unwrap(),
            ephemeral_storage: None,
        },
        timeout_seconds: 60,
        auto_pause: false,
        env: Default::default(),
        metadata: Default::default(),
        network_policy: None,
        desired_phase: None,
        snapshot_retention: None,
        pvc_retention: None,
        tolerations: None,
        node_selector: None,
        security_context: None,
        persistence_mode: None,
        volume_claim_templates: None,
        restore_from: None,
    };
    state
        .sandbox_ops()
        .create("team-b", Sandbox::new("sbx-tenant", spec))
        .await
        .unwrap();

    let status = |namespace: Option<&str>| {
        let state = state.clone();
        let token = mint(
            &state.signing_key,
            &SignedUrlPayload {
                sandbox_id: "sbx-tenant".into(),
                path: "/tmp/x".into(),
                method: "GET".into(),
                exp: chrono::Utc::now().timestamp() + 60,
                namespace: namespace.map(str::to_owned),
            },
        );
        async move {
            files_router(state)
                .oneshot(
                    Request::builder()
                        .method(Method::GET)
                        .uri(format!("/files/download?token={token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(status(Some("team-b")).await, StatusCode::BAD_GATEWAY);
    assert_eq!(status(Some("team-c")).await, StatusCode::NOT_FOUND);
    assert_eq!(status(None).await, StatusCode::NOT_FOUND);
}
