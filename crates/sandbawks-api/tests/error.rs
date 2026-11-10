//! Verifies the E2B JSON error shape (spec §10) and the request-id
//! plumbing through axum's `IntoResponse`.

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    routing::get,
    Router,
};
use http_body_util::BodyExt;
use sandbawks_api::AppError;
use serde_json::Value;
use tower::ServiceExt;

async fn boom() -> Result<(), AppError> {
    Err(AppError::not_found("sandbox_not_found", "no such sandbox"))
}

#[tokio::test]
async fn renders_e2b_json_shape() {
    let app: Router = Router::new()
        .route("/boom", get(boom))
        .layer(sandbawks_api::middleware::request_id_layer());

    let resp = app
        .oneshot(Request::builder().uri("/boom").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let request_id = resp
        .headers()
        .get("x-request-id")
        .expect("middleware should attach x-request-id")
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["code"], "sandbox_not_found");
    assert_eq!(json["message"], "no such sandbox");
    assert_eq!(json["request_id"], request_id);
}
