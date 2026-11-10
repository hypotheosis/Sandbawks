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
