//! Lifecycle CRUD smoke tests for the sandbox router.
//!
//! These run against a `FakeKeyStore` and a `FakeSandboxOps` (no real
//! kube client). They cover request/response shape; T15 covers the
//! round-trip against a real cluster.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use http_body_util::BodyExt;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use sandbawks_api::auth::FakeKeyStore;
use sandbawks_api::envd_auth::envd_access_token;
use sandbawks_api::routes::sandboxes::router as sandboxes_router;
use sandbawks_api::AppState;
use sandbawks_types::{ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus};
use serde_json::{json, Value};
use tower::ServiceExt;

/// Spin up an `AppState` with one known-valid API key. The key's
/// scope (`sandboxes:*`) grants all three operations the router
/// gates internally.
fn fixture_state() -> AppState {
    let store = Arc::new(FakeKeyStore::default());
    store.insert("sbk_a1b2c3d4xxxxxxxx", "team-acme", &["sandboxes:*"]);
    AppState::for_test("team-acme", store)
}

/// The `envdAccessToken` the gateway derives for `id` under the test key:
/// bound to the team's namespace (issue #13).
fn expected_token(id: &str) -> String {
    envd_access_token(&fixture_state().signing_key, "team-acme", id)
}

#[tokio::test]
async fn create_sandbox_returns_201_with_e2b_shape() {
    let app = sandboxes_router(fixture_state());
    let body = json!({
        "templateID": "python-data-sci",
        "metadata": {"user": "alice"},
        "envVars": {"OPENAI_API_KEY": "sk-test"},
        "timeout": 3600,
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes")
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        json["sandboxID"]
            .as_str()
            .expect("sandboxID is a string")
            .starts_with("sbx-"),
        "sandboxID must use sbx- prefix, got {:?}",
        json["sandboxID"],
    );
    assert_eq!(json["templateID"], "python-data-sci");
    assert_eq!(json["clientID"], "team-acme");
    assert_eq!(json["metadata"]["user"], "alice");
    // `alias` is always null in MVP — confirm the wire shape matches
    // what the E2B SDK expects.
    assert!(json["alias"].is_null());
    // `envdVersion` is required by the E2B Python SDK (>=2.21) — its
    // `Sandbox.from_dict` does an unconditional `d.pop("envdVersion")`
    // while parsing the response. Surfaced by the Plan 06 T2 SDK compat
    // harness against the kind cluster; unit-asserted here so a future
    // refactor of the DTO can't silently regress the SDK contract.
    assert!(
        json["envdVersion"]
            .as_str()
            .filter(|v| !v.is_empty())
            .is_some(),
        "envdVersion must be a non-empty string, got {:?}",
        json["envdVersion"],
    );
    // `state`, `cpuCount`, `memoryMB`, `diskSizeMB` are required by the
    // E2B Python SDK's `ListedSandbox.from_dict` (the model used by
    // `Sandbox.list`). Surfaced by the same harness pass that caught
    // `envdVersion` — `Sandbox.from_dict` ignores them via
    // `additional_properties`, but `ListedSandbox` `pop`s each one,
    // so list responses 500'd until we added them. Asserted on the
    // create-response shape so a refactor that drops them can't slip
    // through the (lighter-weight) per-handler tests below.
    assert!(
        matches!(json["state"].as_str(), Some("running" | "paused")),
        "state must be running/paused, got {:?}",
        json["state"],
    );
    for field in ["cpuCount", "memoryMB", "diskSizeMB"] {
        assert!(
            json[field].as_i64().is_some_and(|n| n >= 1),
            "{field} must be a positive integer, got {:?}",
            json[field],
        );
    }
    // Every sandbox is secured (issue #683): the SDK only sends
    // `X-Access-Token` / signs `/files` URLs when this field is present.
    let id = json["sandboxID"].as_str().unwrap();
    assert_eq!(
        json["envdAccessToken"],
        expected_token(id),
        "create must hand back the sandbox's envdAccessToken",
    );
}

#[tokio::test]
async fn list_sandboxes_returns_array() {
    let app = sandboxes_router(fixture_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/sandboxes")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json.is_array(), "GET /sandboxes must return a JSON array");
}

/// List entries omit `envdAccessToken`, as hosted E2B's `ListedSandbox` does;
/// the token is handed out only by create / connect / get (issue #683).
#[tokio::test]
async fn list_entries_omit_envd_access_token() {
    let app = sandboxes_router(fixture_state());
    create_and_get_id(&app).await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/sandboxes")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json: Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let entries = json.as_array().expect("list is an array");
    assert!(!entries.is_empty());
    assert!(entries.iter().all(|e| e.get("envdAccessToken").is_none()));
}

#[tokio::test]
async fn kill_returns_204() {
    let app = sandboxes_router(fixture_state());

    // First create one so we have something to kill.
    let body = json!({"templateID": "tpl", "timeout": 60});
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes")
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let create_body: Value =
        serde_json::from_slice(&create.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let id = create_body["sandboxID"].as_str().unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{id}/kill"))
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

/// The E2B Python SDK's `Sandbox.kill` calls `DELETE /sandboxes/:id`,
/// not `POST /sandboxes/:id/kill`. Without the DELETE route axum's
/// default handler returned an empty 404 body the SDK couldn't parse
/// (surfaced by the Plan 06 T2 compat harness, #184). This test pins
/// the alias so the SDK contract can't silently regress.
#[tokio::test]
async fn delete_returns_204() {
    let app = sandboxes_router(fixture_state());

    let body = json!({"templateID": "tpl", "timeout": 60});
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes")
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let create_body: Value =
        serde_json::from_slice(&create.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let id = create_body["sandboxID"].as_str().unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/sandboxes/{id}"))
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

/// The E2B Python SDK's `Sandbox.list` calls `GET /v2/sandboxes`, not
/// the unversioned `GET /sandboxes`. Pin the alias the same way as
/// the DELETE route above (surfaced by the Plan 06 T2 compat harness,
/// #184).
#[tokio::test]
async fn list_v2_returns_array() {
    let app = sandboxes_router(fixture_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v2/sandboxes")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        json.is_array(),
        "GET /v2/sandboxes must return a JSON array",
    );
}

#[tokio::test]
async fn pause_patches_desired_phase() {
    // Keep the state separately from the router so we can introspect
    // the `FakeSandboxOps`-backed CR after the handler runs — the
    // router consumes its own clone of the state when it's built.
    let state = fixture_state();
    let app = sandboxes_router(state.clone());

    // Create one sandbox so /pause has something to target. We could
    // pre-seed `FakeSandboxOps` directly via `SandboxOps::create`, but
    // routing through the public POST keeps the test honest about the
    // full request path.
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes")
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(
                    json!({"templateID": "tpl", "timeout": 60}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let create_body: Value =
        serde_json::from_slice(&create.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let id = create_body["sandboxID"].as_str().unwrap().to_owned();

    // Sanity-check: the freshly-created CR has no `desired_phase` yet
    // — pause is what writes it.
    let before = state.sandbox_ops().get("team-acme", &id).await.unwrap();
    assert_eq!(before.spec.desired_phase, None);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{id}/pause"))
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["sandboxID"], id);
    assert_eq!(body["state"], "paused");

    // The handler is purely declarative: it must have flipped
    // `spec.desired_phase` to `Paused` on the in-memory CR. The
    // controller (not exercised here) is what actually drives the
    // pause + snapshot.
    let after = state.sandbox_ops().get("team-acme", &id).await.unwrap();
    assert_eq!(after.spec.desired_phase, Some(SandboxPhase::Paused));
}

/// Helper: create a sandbox through the public POST and return its id.
async fn create_and_get_id(app: &axum::Router) -> String {
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes")
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(
                    json!({"templateID": "tpl", "timeout": 60}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let create_body: Value =
        serde_json::from_slice(&create.into_body().collect().await.unwrap().to_bytes()).unwrap();
    create_body["sandboxID"].as_str().unwrap().to_owned()
}

/// `GET /sandboxes/:id` backs the E2B SDK's `Sandbox.get_info` /
/// `Sandbox.is_running`. The SDK parses the body via
/// `SandboxDetail.from_dict`, which `pop`s the same required quartet of
/// fields (`sandboxID`/`templateID`/`clientID`/`envdVersion`) plus
/// `state`/`cpuCount`/`memoryMB`/`diskSizeMB`/`startedAt`/`endAt`. Pin
/// the full shape so a refactor can't silently drop one (#189).
#[tokio::test]
async fn get_sandbox_returns_e2b_detail_shape() {
    let app = sandboxes_router(fixture_state());
    let id = create_and_get_id(&app).await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/sandboxes/{id}"))
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(json["sandboxID"], id);
    assert_eq!(json["templateID"], "tpl");
    assert_eq!(json["clientID"], "team-acme");
    assert_eq!(json["envdAccessToken"], expected_token(&id));
    assert!(
        matches!(json["state"].as_str(), Some("running" | "paused")),
        "state must be running/paused, got {:?}",
        json["state"],
    );
    // `SandboxDetail.from_dict` raises `KeyError` if any of these are
    // absent, so the SDK would crash before the test ever asserts.
    for field in [
        "envdVersion",
        "startedAt",
        "endAt",
        "cpuCount",
        "memoryMB",
        "diskSizeMB",
    ] {
        assert!(!json[field].is_null(), "{field} must be present");
    }
}

/// A `GET` for a sandbox that doesn't exist must 404 — the SDK maps
/// that to `SandboxNotFoundException`.
#[tokio::test]
async fn get_missing_sandbox_returns_404() {
    let app = sandboxes_router(fixture_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/sandboxes/sbx-does-not-exist")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// `POST /sandboxes/:id/timeout` backs the E2B SDK's
/// `Sandbox.set_timeout` (body `{ "timeout": <secs> }`). The endpoint
/// keeps the sandbox alive for `timeout` seconds from now by bumping
/// `spec.timeoutSeconds`, and must return `204` (what the SDK's
/// generated client expects).
#[tokio::test]
async fn set_timeout_bumps_spec_and_returns_204() {
    let state = fixture_state();
    let app = sandboxes_router(state.clone());
    let id = create_and_get_id(&app).await;

    // Freshly created with `timeout: 60`.
    let before = state.sandbox_ops().get("team-acme", &id).await.unwrap();
    assert_eq!(before.spec.timeout_seconds, 60);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{id}/timeout"))
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(json!({"timeout": 600}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Under the idle model (#481) `set_timeout` writes the *raw* window and
    // re-anchors the activity baseline to now, so `timeoutSeconds` is exactly
    // 600 and the deadline lands ~600s out — no growing from creation.
    let after = state.sandbox_ops().get("team-acme", &id).await.unwrap();
    assert_eq!(
        after.spec.timeout_seconds, 600,
        "set_timeout writes the raw idle window, not a grown offset",
    );
    assert!(
        after
            .status
            .as_ref()
            .and_then(|s| s.last_activity_timestamp.as_ref())
            .is_some(),
        "set_timeout must re-anchor the idle baseline (lastActivityTimestamp)",
    );
    let remaining = seconds_until_deadline(&after);
    assert!(
        (590..=600).contains(&remaining),
        "deadline should be ~600s out, got {remaining}s",
    );
}

/// `POST /sandboxes/:id/refreshes` is the older wire alias for the same
/// operation (body `{ "duration": <secs> }`). Pin it so the alias
/// can't silently regress (#189).
#[tokio::test]
async fn refreshes_alias_bumps_spec_and_returns_204() {
    let state = fixture_state();
    let app = sandboxes_router(state.clone());
    let id = create_and_get_id(&app).await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{id}/refreshes"))
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(json!({"duration": 900}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let after = state.sandbox_ops().get("team-acme", &id).await.unwrap();
    assert_eq!(
        after.spec.timeout_seconds, 900,
        "the /refreshes alias writes the raw idle window too",
    );
}

/// A timeout refresh against a missing sandbox must 404.
#[tokio::test]
async fn set_timeout_missing_sandbox_returns_404() {
    let app = sandboxes_router(fixture_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes/sbx-nope/timeout")
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(json!({"timeout": 600}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Annotation key the gateway uses to persist the configured sandbox lifetime
/// (mirrors `TIMEOUT_DURATION_ANNOTATION` in the router). Pinned here so the
/// resume test asserts the on-CR contract, not an internal symbol.
const TIMEOUT_DURATION_ANNOTATION: &str = "sandbawks.dev/timeout-duration-seconds";

/// Build a paused, resumable sandbox seeded directly into `FakeSandboxOps`:
/// created `created_ago` in the past, a `timeout_seconds` window, and a
/// pre-set `status.envdAddress` so the `connect` poll returns immediately.
/// No duration annotation — exercising the fallback to `spec.timeoutSeconds`.
fn paused_sandbox(name: &str, created_ago: chrono::Duration, timeout_seconds: u32) -> Sandbox {
    let mut sandbox = Sandbox::new(
        name,
        SandboxSpec {
            template_ref: "tpl".into(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
                ephemeral_storage: None,
            },
            timeout_seconds,
            auto_pause: true,
            desired_phase: Some(SandboxPhase::Paused),
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
    sandbox.metadata.creation_timestamp = Some(Time(Utc::now() - created_ago));
    sandbox.status = Some(SandboxStatus {
        phase: SandboxPhase::Paused,
        envd_address: Some("10.0.0.1:49983".into()),
        ..Default::default()
    });
    sandbox
}

/// Seconds until the controller-honored idle deadline from now. The deadline
/// is `baseline + spec.timeoutSeconds`, where `baseline` is the last stamped
/// activity (`status.lastActivityTimestamp`) or `creationTimestamp` when none
/// has landed yet (issue #481) — mirroring the controller. Negative means the
/// deadline already passed.
fn seconds_until_deadline(sandbox: &Sandbox) -> i64 {
    let baseline = sandbox
        .status
        .as_ref()
        .and_then(|s| s.last_activity_timestamp.as_deref())
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|| sandbox.metadata.creation_timestamp.clone().unwrap().0);
    let deadline = baseline + chrono::Duration::seconds(sandbox.spec.timeout_seconds.into());
    (deadline - Utc::now()).num_seconds()
}

/// `POST /sandboxes/:id/connect` on a sandbox paused past its deadline must
/// re-anchor the idle clock so it gets a fresh future deadline instead of
/// immediately re-pausing (issue #358 under the #481 idle model).
///
/// A sandbox whose last activity is 2h ago with a 1h idle window has a
/// deadline an hour in the past; before the fix the first `Running` reconcile
/// fired `Trigger::Timeout` and re-paused it. Resume now stamps
/// `lastActivityTimestamp = now`, landing the deadline a full window out.
#[tokio::test]
async fn connect_extends_timeout_past_deadline() {
    let state = fixture_state();
    let app = sandboxes_router(state.clone());

    let sandbox = paused_sandbox("sbx-stale", chrono::Duration::hours(2), 3600);
    state
        .sandbox_ops()
        .create("team-acme", sandbox)
        .await
        .unwrap();

    // Precondition: the seeded deadline is already in the past.
    let before = state
        .sandbox_ops()
        .get("team-acme", "sbx-stale")
        .await
        .unwrap();
    assert!(
        seconds_until_deadline(&before) < 0,
        "precondition: deadline should already have passed",
    );

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes/sbx-stale/connect")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    // `Sandbox.connect` re-reads the token from this answer (issue #683).
    let json: Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(json["envdAccessToken"], expected_token("sbx-stale"));

    let after = state
        .sandbox_ops()
        .get("team-acme", "sbx-stale")
        .await
        .unwrap();
    // Resume was requested...
    assert_eq!(after.spec.desired_phase, Some(SandboxPhase::Running));
    // ...and the deadline now lands ~1h (the idle window) into the future, so
    // the controller's timeout trigger won't fire on the first Running
    // reconcile. `timeoutSeconds` is unchanged (the raw window); the fresh
    // deadline comes from the re-anchored activity baseline.
    assert_eq!(after.spec.timeout_seconds, 3600);
    let remaining = seconds_until_deadline(&after);
    assert!(
        (3500..=3600).contains(&remaining),
        "deadline should be ~3600s out after resume, got {remaining}s",
    );
    assert!(
        after
            .status
            .as_ref()
            .and_then(|s| s.last_activity_timestamp.as_ref())
            .is_some(),
        "resume must re-anchor the idle baseline (lastActivityTimestamp)",
    );

    // A second resume must NOT compound the window: it re-anchors the baseline
    // to now over the unchanged 3600s window, so the deadline stays ~1h out
    // rather than growing to ~2h.
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes/sbx-stale/connect")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let after2 = state
        .sandbox_ops()
        .get("team-acme", "sbx-stale")
        .await
        .unwrap();
    let remaining2 = seconds_until_deadline(&after2);
    assert!(
        (3500..=3600).contains(&remaining2),
        "second resume must not grow the window; expected ~3600s, got {remaining2}s",
    );
}

/// A warm-pool member is minted with `timeout_seconds: 0` (the "timeout
/// disabled" sentinel). Resuming one must leave the timeout disabled rather
/// than stamping a real deadline that would auto-pause the whole pool.
#[tokio::test]
async fn connect_leaves_disabled_timeout_untouched() {
    let state = fixture_state();
    let app = sandboxes_router(state.clone());

    let sandbox = paused_sandbox("sbx-warm", chrono::Duration::hours(2), 0);
    state
        .sandbox_ops()
        .create("team-acme", sandbox)
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes/sbx-warm/connect")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let after = state
        .sandbox_ops()
        .get("team-acme", "sbx-warm")
        .await
        .unwrap();
    assert_eq!(after.spec.desired_phase, Some(SandboxPhase::Running));
    // Still disabled: no deadline stamped, no duration annotation written.
    assert_eq!(after.spec.timeout_seconds, 0);
    assert!(
        after
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(TIMEOUT_DURATION_ANNOTATION))
            .is_none(),
        "disabled timeout must not gain a duration annotation",
    );
}

/// `POST /sandboxes/:id/connect` against an *already-Running* sandbox must NOT
/// re-anchor the idle clock — the baseline stamp is confined to the resume
/// case, so reconnecting doesn't silently behave as a keep-alive (explicit
/// keep-alive stays the job of `set_timeout` / `refreshes`).
#[tokio::test]
async fn connect_already_running_does_not_extend_timeout() {
    let state = fixture_state();
    let app = sandboxes_router(state.clone());

    // Already Running with a deadline an hour in the past. (A Running sandbox
    // past its deadline is a transient the controller resolves on its own; a
    // reconnect must not paper over it by resetting the clock.)
    let mut sandbox = paused_sandbox("sbx-live", chrono::Duration::hours(2), 3600);
    sandbox.status.as_mut().unwrap().phase = SandboxPhase::Running;
    state
        .sandbox_ops()
        .create("team-acme", sandbox)
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes/sbx-live/connect")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let after = state
        .sandbox_ops()
        .get("team-acme", "sbx-live")
        .await
        .unwrap();
    // Timeout untouched, and the idle clock was NOT re-anchored.
    assert_eq!(
        after.spec.timeout_seconds, 3600,
        "connect on an already-Running sandbox must not extend the timeout",
    );
    assert!(
        after
            .status
            .as_ref()
            .and_then(|s| s.last_activity_timestamp.as_ref())
            .is_none(),
        "reconnecting a Running sandbox must not stamp activity (no keep-alive)",
    );
}

/// Issue #662: current E2B SDKs (e2b >= 2.61) create through `POST
/// /v2/sandboxes`, not the unversioned `POST /sandboxes`. The v2 body adds
/// fields sandbawks doesn't model (`autoPause`, `autoPauseMemory`, `secure`,
/// ...); they must be ignored, not rejected.
#[tokio::test]
async fn create_v2_returns_201_with_e2b_shape() {
    let app = sandboxes_router(fixture_state());
    let body = json!({
        "templateID": "python-data-sci",
        "timeout": 300,
        "autoPause": false,
        "autoPauseMemory": true,
        "metadata": {},
        "envVars": {},
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/sandboxes")
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        json["sandboxID"]
            .as_str()
            .is_some_and(|id| id.starts_with("sbx-")),
        "v2 create must return the SDK Sandbox shape, got {json}",
    );
}

/// POST `uri` with the v2 connect body (`{timeout?, memory?}`) and return the
/// status plus the parsed `Sandbox` body.
async fn post_connect(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::from(json!({"timeout": 300}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// Issue #662: current E2B SDKs connect/resume through `POST
/// /v2/sandboxes/:id/connect`. Per E2B's OpenAPI spec a resume answers 201
/// with the `Sandbox` body, and the resume side effects (desiredPhase flip)
/// are the same as v1's.
#[tokio::test]
async fn connect_v2_resumes_paused_sandbox_with_201() {
    let state = fixture_state();
    let app = sandboxes_router(state.clone());
    let sandbox = paused_sandbox("sbx-v2", chrono::Duration::minutes(1), 3600);
    state
        .sandbox_ops()
        .create("team-acme", sandbox)
        .await
        .unwrap();

    let (status, json) = post_connect(app, "/v2/sandboxes/sbx-v2/connect").await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["sandboxID"], "sbx-v2");
    let after = state
        .sandbox_ops()
        .get("team-acme", "sbx-v2")
        .await
        .unwrap();
    assert_eq!(after.spec.desired_phase, Some(SandboxPhase::Running));
}

/// Connecting to an already-Running sandbox answers 200 (not 201) on both
/// the v1 and v2 routes, matching E2B's OpenAPI spec.
#[tokio::test]
async fn connect_already_running_returns_200_on_v1_and_v2() {
    let state = fixture_state();
    let mut sandbox = paused_sandbox("sbx-run", chrono::Duration::minutes(1), 3600);
    sandbox.status.as_mut().unwrap().phase = SandboxPhase::Running;
    state
        .sandbox_ops()
        .create("team-acme", sandbox)
        .await
        .unwrap();

    for uri in [
        "/sandboxes/sbx-run/connect",
        "/v2/sandboxes/sbx-run/connect",
    ] {
        let (status, json) = post_connect(sandboxes_router(state.clone()), uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(json["sandboxID"], "sbx-run", "{uri}");
    }
}
