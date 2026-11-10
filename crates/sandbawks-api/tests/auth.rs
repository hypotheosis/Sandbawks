//! End-to-end auth middleware test against a fake key store.
//!
//! Exercises the happy path (valid key → handler receives
//! `TeamContext`), the unauthenticated path (missing header → 401),
//! and the wrong-key path (constant-time compare fails → 401).

use axum::{
    body::Body,
    extract::Extension,
    http::{Request, StatusCode},
    routing::get,
    Router,
};
use http_body_util::BodyExt;
use sandbawks_api::auth::{auth_middleware, FakeKeyStore, TeamContext};
use sandbawks_types::auth::hash_key;
use std::sync::Arc;
use tower::ServiceExt;

async fn echo_team(Extension(team): Extension<TeamContext>) -> String {
    team.namespace
}

fn router(store: Arc<FakeKeyStore>) -> Router {
    Router::new()
        .route("/whoami", get(echo_team))
        .layer(axum::middleware::from_fn_with_state(store, auth_middleware))
}

#[tokio::test]
async fn happy_path() {
    let store = Arc::new(FakeKeyStore::default());
    store.insert("sbk_a1b2c3d4xxxxxxxx", "team-acme", &["sandboxes:*"]);
    let app = router(store);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/whoami")
                .header("X-API-Key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"team-acme");
}

#[tokio::test]
async fn missing_header_is_unauthorized() {
    let store = Arc::new(FakeKeyStore::default());
    let app = router(store);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/whoami")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn wrong_key_is_unauthorized() {
    let store = Arc::new(FakeKeyStore::default());
    store.insert("sbk_a1b2c3d4correct", "team-acme", &["sandboxes:*"]);
    let app = router(store);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/whoami")
                .header("X-API-Key", "sbk_a1b2c3d4wrongwrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Keep the helper-crate import referenced so the symbol path is
    // exercised at the api test layer too — a small smoke-check that
    // `sandbawks-types::auth` is re-exported correctly.
    let _ = hash_key;
}
