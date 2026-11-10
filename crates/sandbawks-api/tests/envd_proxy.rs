//! Wiring guards for the per-sandbox envd proxy's `GET /health` route
//! (issue #189 follow-up — backs the E2B SDK's `Sandbox.is_running`).
//!
//! The `200` happy path needs a live envd to dial and is covered
//! end-to-end by the SDK compat suite (`test_is_running_after_create`).
//! These checks exercise the route wiring + `resolve_sandbox` error
//! branches, which don't require a cluster — the same philosophy as
//! `tests/files.rs`.
//!
//! The access-token tests (issue #683) assert the credential check runs
//! before the sandbox lookup: a missing or wrong credential is `401`, and a
//! valid one gets as far as the (empty) store's `404`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use sandbawks_api::auth::FakeKeyStore;
use sandbawks_api::envd_auth::{envd_access_token, legacy_envd_access_token};
use sandbawks_api::routes::envd_proxy::{
    unauthenticated_streaming_router, unauthenticated_unary_router,
};
use sandbawks_api::AppState;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

/// Sandbox id every test targets; the fake store never holds it.
const SANDBOX_ID: &str = "sbx-does-not-exist";

/// `AppState` over an empty `FakeSandboxOps` — every sandbox lookup
/// misses, which is exactly what the not-found path below asserts.
fn proxy_state() -> AppState {
    let store = Arc::new(FakeKeyStore::default());
    AppState::for_test(NAMESPACE, store)
}

/// Same, but with hostname-based selection enabled for a wildcard
/// `e2b.example.com` domain (issue #364).
fn proxy_state_with_domain() -> AppState {
    proxy_state().with_sandbox_domain(Some(Arc::from("e2b.example.com")))
}

/// Namespace the test gateway runs in (its default namespace).
const NAMESPACE: &str = "team-acme";

/// The `envdAccessToken` the gateway would hand out for [`SANDBOX_ID`] in
/// [`NAMESPACE`].
fn token(state: &AppState) -> String {
    envd_access_token(&state.signing_key, NAMESPACE, SANDBOX_ID)
}

/// The pre-#13, namespace-less token for [`SANDBOX_ID`]. Still accepted, for
/// the gateway's default namespace only.
fn legacy_token(state: &AppState) -> String {
    legacy_envd_access_token(&state.signing_key, SANDBOX_ID)
}

/// A v1 `/files` signature, computed the way the E2B SDK's
/// `get_signature` does (`e2b/sandbox/signature.py`).
fn sdk_signature(path: &str, operation: &str, token: &str, expiration: Option<i64>) -> String {
    let raw = match expiration {
        None => format!("{path}:{operation}::{token}"),
        Some(exp) => format!("{path}:{operation}::{token}:{exp}"),
    };
    format!(
        "v1_{}",
        STANDARD_NO_PAD.encode(Sha256::digest(raw.as_bytes()))
    )
}

/// Send `request` to the unary router over `state`, returning the status.
async fn unary_status(state: AppState, request: Request<Body>) -> StatusCode {
    unauthenticated_unary_router(state)
        .oneshot(request)
        .await
        .unwrap()
        .status()
}

/// `GET /health` without the `E2b-Sandbox-Id` header can't identify a
/// sandbox to probe → 400 `missing_sandbox_id` (the SDK never omits the
/// header for a created sandbox, but the guard keeps the contract).
#[tokio::test]
async fn health_without_sandbox_id_returns_400() {
    let app = unauthenticated_unary_router(proxy_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// `GET /health` for a sandbox that doesn't exist → 404. The E2B SDK
/// raises `NotFoundException` on this, which is the correct "not a
/// probeable running sandbox" signal.
#[tokio::test]
async fn health_for_missing_sandbox_returns_404() {
    let state = proxy_state();
    let token = token(&state);
    let app = unauthenticated_unary_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .header("E2b-Sandbox-Id", SANDBOX_ID)
                .header("X-Access-Token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// With a wildcard domain configured, the E2B `{port}-{id}.{domain}`
/// `Host` identifies the sandbox with no `E2b-Sandbox-Id` header — the
/// SDK-compliant path (issue #364). Reaching the sandbox lookup (which
/// misses → 404, not the 400 `missing_sandbox_id`) proves the host was
/// parsed and the id extracted.
#[tokio::test]
async fn health_via_hostname_resolves_sandbox_id() {
    let state = proxy_state_with_domain();
    let token = token(&state);
    let app = unauthenticated_unary_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .header("Host", "3000-sbx-does-not-exist.e2b.example.com")
                .header("X-Access-Token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// A `Host` that isn't a subdomain of the configured base domain falls
/// through to the `E2b-Sandbox-Id` header — the two mechanisms coexist
/// so localhost / port-forward clients keep working with a domain set.
#[tokio::test]
async fn health_non_matching_host_falls_back_to_header() {
    let state = proxy_state_with_domain();
    let token = token(&state);
    let app = unauthenticated_unary_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .header("Host", "localhost:8080")
                .header("E2b-Sandbox-Id", SANDBOX_ID)
                .header("X-Access-Token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Domain configured but neither a matching `Host` nor the header is
/// present → the `missing_sandbox_id` 400 contract is preserved.
#[tokio::test]
async fn health_domain_set_but_no_identifier_returns_400() {
    let app = unauthenticated_unary_router(proxy_state_with_domain());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .header("Host", "localhost:8080")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// No `X-Access-Token` → `401` before the lookup, so a caller without the
/// credential cannot tell whether the id exists (issue #683).
#[tokio::test]
async fn health_without_access_token_returns_401() {
    let request = Request::builder()
        .method("GET")
        .uri("/health")
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        unary_status(proxy_state(), request).await,
        StatusCode::UNAUTHORIZED
    );
}

/// Another sandbox's token does not open this one.
#[tokio::test]
async fn wrong_access_token_returns_401() {
    let state = proxy_state();
    let other = envd_access_token(&state.signing_key, NAMESPACE, "sbx-someone-else");
    let request = Request::builder()
        .method("POST")
        .uri("/filesystem.Filesystem/Stat")
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .header("X-Access-Token", other)
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"path":"/tmp"}"#))
        .unwrap();
    assert_eq!(unary_status(state, request).await, StatusCode::UNAUTHORIZED);
}

/// The CLM-0064 repro: an unsigned upload with no credentials at all, routed
/// by hostname, is refused.
#[tokio::test]
async fn unsigned_files_upload_without_token_returns_401() {
    let request = Request::builder()
        .method("POST")
        .uri("/files?path=/tmp/up.txt")
        .header("Host", "49983-sbx-does-not-exist.e2b.example.com")
        .header("Content-Type", "application/octet-stream")
        .body(Body::from("pwned"))
        .unwrap();
    assert_eq!(
        unary_status(proxy_state_with_domain(), request).await,
        StatusCode::UNAUTHORIZED
    );
}

/// A correctly signed download URL needs no header: it passes the credential
/// check and reaches the lookup (`404` from the empty store).
#[tokio::test]
async fn signed_files_download_passes_auth() {
    let state = proxy_state();
    let exp = chrono::Utc::now().timestamp() + 300;
    let sig = sdk_signature("/tmp/up.txt", "read", &legacy_token(&state), Some(exp));
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/files?path=/tmp/up.txt&signature={}&signature_expiration={exp}",
            urlencode(&sig)
        ))
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .body(Body::empty())
        .unwrap();
    assert_eq!(unary_status(state, request).await, StatusCode::NOT_FOUND);
}

/// A read signature does not authorise an upload to the same path.
#[tokio::test]
async fn read_signature_does_not_authorise_upload() {
    let state = proxy_state();
    let sig = sdk_signature("/tmp/up.txt", "read", &legacy_token(&state), None);
    let request = Request::builder()
        .method("POST")
        .uri(format!(
            "/files?path=/tmp/up.txt&signature={}",
            urlencode(&sig)
        ))
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .body(Body::from("x"))
        .unwrap();
    assert_eq!(unary_status(state, request).await, StatusCode::UNAUTHORIZED);
}

/// A URL whose `signature` was stripped (expiration left behind) is refused.
#[tokio::test]
async fn stripped_signature_returns_401() {
    let exp = chrono::Utc::now().timestamp() + 300;
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/files?path=/tmp/up.txt&signature_expiration={exp}"
        ))
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        unary_status(proxy_state(), request).await,
        StatusCode::UNAUTHORIZED
    );
}

/// An expired signature is refused even though it was genuine.
#[tokio::test]
async fn expired_signature_returns_401() {
    let state = proxy_state();
    let exp = chrono::Utc::now().timestamp() - 1;
    let sig = sdk_signature("/tmp/up.txt", "read", &legacy_token(&state), Some(exp));
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/files?path=/tmp/up.txt&signature={}&signature_expiration={exp}",
            urlencode(&sig)
        ))
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .body(Body::empty())
        .unwrap();
    assert_eq!(unary_status(state, request).await, StatusCode::UNAUTHORIZED);
}

/// The streaming `process.Start` route enforces the same credential.
#[tokio::test]
async fn process_start_without_access_token_returns_401() {
    let resp = unauthenticated_streaming_router(proxy_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/process.Process/Start")
                .header("E2b-Sandbox-Id", SANDBOX_ID)
                .header("Content-Type", "application/connect+json")
                .body(Body::from(connect_envelope(br#"{"process":{"cmd":"id"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Percent-encode the base64 alphabet's reserved characters for a query value.
fn urlencode(value: &str) -> String {
    value
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D")
}

/// Wrap `payload` in a single uncompressed Connect streaming envelope.
fn connect_envelope(payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(payload.len()).expect("test payload fits in u32");
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(payload);
    framed
}

/// Query strings copied verbatim from the real SDKs' `download_url` /
/// `upload_url` for [`SANDBOX_ID`] under the test signing key (Python e2b
/// 2.61.0 percent-encodes spaces as `%20`; JS e2b 2.62.0 uses `+`). Each must
/// pass auth through axum's query decoding and reach the lookup (`404`).
/// `username=user` appears when the SDK sees envd < 0.4.0, as it does
/// against today's gateway (#675).
const SDK_FILE_QUERIES: [(&str, &str); 8] = [
    // Python, envd < 0.4.0, no user.
    (
        "GET",
        "path=%2Ftmp%2Fmy%20file%20%C3%BC.txt&username=user\
         &signature=v1_PTB5WsGWD8MlNyzc7nOa2LZHAQssOjNtgkTHx9L%2Fk2U",
    ),
    // Python, envd >= 0.4.0, no user.
    (
        "GET",
        "path=%2Ftmp%2Fmy%20file%20%C3%BC.txt\
         &signature=v1_7k%2BPp6lwhG6QfjxL9N1P8rp3Xaao4hWivAaURHVOguo",
    ),
    // Python, envd >= 0.4.0, no user, upload.
    (
        "POST",
        "path=%2Ftmp%2Fmy%20file%20%C3%BC.txt\
         &signature=v1_K9%2BJ9fGKvrQj4jGVCDqs2mWrzNs5NuoEwNQV3PMbOQA",
    ),
    // Python, explicit user, upload.
    (
        "POST",
        "path=%2Ftmp%2Fmy%20file%20%C3%BC.txt&username=root\
         &signature=v1_F5hgw%2B7mOE4OGtc56LFb1lUvQQVIBaraKAaBfZKvSI0",
    ),
    // JS, envd < 0.4.0, no user.
    (
        "GET",
        "username=user&path=%2Ftmp%2Fmy+file+%C3%BC.txt\
         &signature=v1_PTB5WsGWD8MlNyzc7nOa2LZHAQssOjNtgkTHx9L%2Fk2U",
    ),
    // JS, envd >= 0.4.0, no user.
    (
        "GET",
        "path=%2Ftmp%2Fmy+file+%C3%BC.txt\
         &signature=v1_7k%2BPp6lwhG6QfjxL9N1P8rp3Xaao4hWivAaURHVOguo",
    ),
    // JS, envd < 0.4.0, no user, upload.
    (
        "POST",
        "username=user&path=%2Ftmp%2Fmy+file+%C3%BC.txt\
         &signature=v1_3hxuYyjDgDmw4uMbRtrY4rhh0B3PEVMQ900i2HV0Qnw",
    ),
    // JS, explicit user.
    (
        "GET",
        "username=root&path=%2Ftmp%2Fmy+file+%C3%BC.txt\
         &signature=v1_8gcaeSx32HkVR198v%2FuvTi0zX21O9IOE0AQnfuwPNpk",
    ),
];

/// Every real-SDK signed URL in [`SDK_FILE_QUERIES`] passes auth.
#[tokio::test]
async fn real_sdk_signed_urls_pass_auth() {
    let state = proxy_state();
    assert_eq!(
        legacy_token(&state),
        "2qDQP2X1AE2byUqjeUOSRxIaHE1cm6FLZZy3ct-Kghg",
        "vectors were generated for this (legacy, default-namespace) token",
    );
    for (method, query) in SDK_FILE_QUERIES {
        let request = Request::builder()
            .method(method)
            .uri(format!("/files?{query}"))
            .header("E2b-Sandbox-Id", SANDBOX_ID)
            .header("Content-Type", "application/octet-stream")
            .body(Body::from("x"))
            .unwrap();
        assert_eq!(
            unary_status(state.clone(), request).await,
            StatusCode::NOT_FOUND,
            "SDK URL rejected: {method} {query}",
        );
    }
}

// ---------------------------------------------------------------------------
// Tenant namespaces (issue #13). Sandbox ids are only unique per namespace;
// the access token names the namespace, and the proxy resolves the sandbox
// there rather than in the gateway's default namespace.
// ---------------------------------------------------------------------------

/// A `Sandbox` named `id` in `namespace`, with an optional envd address and
/// desired phase, for seeding the proxy's cache.
fn cached_sandbox(
    namespace: &str,
    id: &str,
    desired: Option<sandbawks_types::SandboxPhase>,
    addr: Option<&str>,
) -> sandbawks_types::Sandbox {
    use sandbawks_types::{
        ResourceQuantity, ResourceRequests, Sandbox, SandboxSpec, SandboxStatus,
    };
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
        desired_phase: desired,
        snapshot_retention: None,
        pvc_retention: None,
        tolerations: None,
        node_selector: None,
        security_context: None,
        persistence_mode: None,
        volume_claim_templates: None,
        restore_from: None,
    };
    let mut sandbox = Sandbox::new(id, spec);
    sandbox.metadata.namespace = Some(namespace.to_owned());
    sandbox.status = addr.map(|a| SandboxStatus {
        envd_address: Some(a.to_owned()),
        ..Default::default()
    });
    sandbox
}

/// `GET /health` for [`SANDBOX_ID`] with `token`, returning the status.
async fn health_status(state: AppState, token: &str) -> StatusCode {
    let request = Request::builder()
        .method("GET")
        .uri("/health")
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .header("X-Access-Token", token)
        .body(Body::empty())
        .unwrap();
    unary_status(state, request).await
}

/// A token issued for a tenant namespace other than the gateway's default
/// passes auth and the sandbox is resolved *in that namespace*: a paused
/// sandbox cached there answers `sandbox_paused` (403), where the old
/// default-namespace-only lookup answered 404.
#[tokio::test]
async fn tenant_namespace_token_resolves_in_its_namespace() {
    let state = proxy_state();
    state.envd_cache.apply(&cached_sandbox(
        "team-b",
        SANDBOX_ID,
        Some(sandbawks_types::SandboxPhase::Paused),
        None,
    ));
    let token = envd_access_token(&state.signing_key, "team-b", SANDBOX_ID);
    assert_eq!(health_status(state, &token).await, StatusCode::FORBIDDEN);
}

/// The same sandbox id in two namespaces: team-b's token never reaches
/// team-a's sandbox. Team-a's copy is cached as paused (403 if it were
/// reached); team-b has none, so the lookup misses and answers 404.
#[tokio::test]
async fn token_does_not_reach_the_same_id_in_another_namespace() {
    let state = proxy_state();
    state.envd_cache.apply(&cached_sandbox(
        "team-a",
        SANDBOX_ID,
        Some(sandbawks_types::SandboxPhase::Paused),
        None,
    ));
    let token = envd_access_token(&state.signing_key, "team-b", SANDBOX_ID);
    assert_eq!(health_status(state, &token).await, StatusCode::NOT_FOUND);
}

/// A token whose namespace label was swapped fails the MAC: 401.
#[tokio::test]
async fn relabelled_token_returns_401() {
    let state = proxy_state();
    let token = envd_access_token(&state.signing_key, "team-a", SANDBOX_ID);
    let forged = format!("team-b.{}", token.split_once('.').unwrap().1);
    assert_eq!(
        health_status(state, &forged).await,
        StatusCode::UNAUTHORIZED
    );
}

/// A legacy, namespace-less token keeps working for the default namespace
/// (clients that connected before the upgrade), and only there.
#[tokio::test]
async fn legacy_token_is_scoped_to_the_default_namespace() {
    let state = proxy_state();
    // Default namespace: passes auth, misses the empty store → 404.
    assert_eq!(
        health_status(state.clone(), &legacy_token(&state)).await,
        StatusCode::NOT_FOUND
    );
    // A paused sandbox of that id in another namespace is not reachable
    // with it: still 404 (looked up in the default namespace), never 403.
    state.envd_cache.apply(&cached_sandbox(
        "team-b",
        SANDBOX_ID,
        Some(sandbawks_types::SandboxPhase::Paused),
        None,
    ));
    assert_eq!(
        health_status(state.clone(), &legacy_token(&state)).await,
        StatusCode::NOT_FOUND
    );
}

/// A signed `/files` URL carries no namespace; the proxy checks the signature
/// against each namespace the cache holds that id in. A URL signed with
/// team-b's token resolves in team-b (paused → 403, not 401).
#[tokio::test]
async fn signed_url_resolves_the_namespace_from_the_cache() {
    let state = proxy_state();
    state.envd_cache.apply(&cached_sandbox(
        "team-b",
        SANDBOX_ID,
        Some(sandbawks_types::SandboxPhase::Paused),
        None,
    ));
    let token = envd_access_token(&state.signing_key, "team-b", SANDBOX_ID);
    let sig = sdk_signature("/tmp/up.txt", "read", &token, None);
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/files?path=/tmp/up.txt&signature={}",
            urlencode(&sig)
        ))
        .header("E2b-Sandbox-Id", SANDBOX_ID)
        .body(Body::empty())
        .unwrap();
    assert_eq!(unary_status(state, request).await, StatusCode::FORBIDDEN);
}
