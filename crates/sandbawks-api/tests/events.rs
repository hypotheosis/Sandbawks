//! Smoke tests for the SSE events router.
//!
//! End-to-end event delivery requires a real kube apiserver (the
//! watcher resolves nothing useful against `AppState::for_test`'s
//! 204-returning mock service), so that path is exercised by the kind
//! integration test in T15. Here we just confirm the router boots and
//! enforces auth — enough to catch route-table or middleware
//! regressions in `cargo test` without standing up a cluster.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use sandbawks_api::auth::FakeKeyStore;
use sandbawks_api::routes::events::router as events_router;
use sandbawks_api::AppState;
use tower::ServiceExt;

#[tokio::test]
async fn missing_auth_is_unauthorized() {
    let store = Arc::new(FakeKeyStore::default());
    let state = AppState::for_test("team-acme", store);
    let app = events_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/events/sandboxes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn missing_auth_on_single_route_is_unauthorized() {
    let store = Arc::new(FakeKeyStore::default());
    let state = AppState::for_test("team-acme", store);
    let app = events_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/events/sandboxes/sbx-abc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
