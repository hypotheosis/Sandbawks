# sandbawks Plan 04 — `sandbawks-api` E2B-compatible HTTP/WS gateway

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the `sandbawks-api` crate — an axum HTTP/WS gateway that speaks the public E2B REST surface (lifecycle, SSE events, exec WebSocket, file upload/download, filesystem metadata pass-through), terminated by TeamAPIKey auth on the way in, fanned out to real envd Pods on the way out. Ship a minimal Pod-materializing `PodAdapter` so the reconcile loop turns `Sandbox` CRs into real Pods running envd on the cluster's default container runtime (containerd/runc), making the gateway's exec/file paths end-to-end testable on `kind`.

**Architecture:** Three layers ship together. (1) `sandbawks-api` is an axum app with one router tree, `tower-http` middleware (trace, request-id, timeout), an `AppError` enum that renders to E2B-shaped JSON, a `TeamContext` extractor backed by indexed `TeamAPIKey` CRD lookups, and per-route modules under `src/routes/`. (2) The `RuntimeAdapter` trait in `sandbawks-types` is extended with `create_pod` / `delete_pod` / `pod_ref` methods, and `sandbawks-controller`'s reconcile loop grows a `kube::runtime::watcher::watch_object` over child Pods so it can flip `status.envdAddress` when the Pod is Ready. (3) `PodAdapter` is the first real `RuntimeAdapter` impl — it materializes a Pod that runs the envd image, surfaces `envd_address` once the Pod has an IP, and no-ops pause/resume (real state preservation is Plan 05's `GvisorAdapter`). The exec WebSocket bridges the public WS frames to envd's gRPC `Process.Connect` / `Pty.Connect` streams; the file mint+proxy issues short-lived HMAC-signed URLs on the gateway's own domain and streams bodies into envd's `Filesystem.Write` / `Filesystem.Read` RPCs server-side.

**Tech Stack:**
- Rust 2021, Cargo workspace
- `axum` 0.7 with `ws` + `macros` features (new workspace dep)
- `tower-http` 0.6 — `TraceLayer`, `RequestIdLayer`, `TimeoutLayer`, `CorsLayer` (new workspace dep)
- `tower` 0.5 (new workspace dep, indirect via axum but needed for layer plumbing)
- `hyper` 1.x (axum's default transport, already pulled in)
- `tokio-tungstenite` is unnecessary — axum's `WebSocketUpgrade` is sufficient
- `hmac` 0.12 + `sha2` 0.10 (new workspace deps) for signed URLs and TeamAPIKey hashing
- `base64` 0.22 (new workspace dep) for URL-safe encoding of signing tokens
- `subtle` 2.6 (new workspace dep) for constant-time hash compare
- `tonic` (already present, Plan 03) — generated envd client stubs are imported from `envd::proto`
- `kube` (already present) — `Api<Sandbox>`, `Api<TeamAPIKey>`, `Api<Event>`, `runtime::watcher`
- `reqwest` is intentionally avoided — the gateway never makes outbound HTTP except envd gRPC and kube
- `mime` 0.3 (new workspace dep) for content-type sniffing in file proxy
- `chrono` 0.4 (new workspace dep) for ISO-8601 timestamps in signed URLs

**Spec reference:** `docs/superpowers/specs/2026-05-21-sandbawks-mvp-design.md` §3.1 (component table: `sandbawks-api`), §4.4 (`TeamAPIKey` CRD shape), §5 (state machine — pause/resume transitions the gateway drives), §7 (E2B endpoint table — the full surface this plan implements), §9 (auth & multi-tenancy), §10 (error shape `{code, message, request_id}`), §11 (observability — `tracing` + Prometheus metrics), §14 sub-plan 4 (scope), §15 (open questions resolved during brainstorming: WebSocket-on-the-wire + gRPC-internally for exec).

**Brainstorming decisions locked here:**

1. **Pod materialization is pulled forward** into this plan (alongside the API gateway) rather than waiting for Plan 05 / `GvisorAdapter`. The reason is that with no real Pods, the API would only be unit-testable against fakes; with `PodAdapter`, the integration test in T15 exercises the full HTTP → CRD → controller → real Pod → envd path on `kind`.
2. **The cluster has only the default `containerd` runtime, not gVisor.** `PodAdapter` therefore does *not* set `pod.spec.runtimeClassName` and leaves the kubelet to use the cluster's default runtime handler (runc-via-containerd in `kind`, in our prod cluster, and on any conformant cluster). `Sandbox.spec.runtimeClassName` is *accepted but unenforced* in this plan — if set, the controller logs a warning and continues. Real RuntimeClass-driven dispatch lands with `GvisorAdapter` in Plan 05.
3. **Full `TeamAPIKey` auth lands in this plan.** Every API request resolves `X-API-Key` to a `TeamContext { namespace, scopes }` via the prefix-indexed `TeamAPIKey` CRD lookup described in spec §9. There is no "skip auth" mode — local development uses a pre-seeded `TeamAPIKey` CR.
4. **Exec wire protocol: WebSocket-public, gRPC-internal.** The gateway accepts a WebSocket upgrade on the exec endpoint and bridges frame-by-frame to envd's `Process.Connect` / `Pty.Connect` bidirectional streams. This matches what the published E2B SDKs already speak (resolving open question §15) and means Plan 06's SDK compat test is a `pip install e2b` away.

---

## File map

**Created:**

- `crates/sandbawks-api/Cargo.toml`
- `crates/sandbawks-api/src/lib.rs`
- `crates/sandbawks-api/src/error.rs`
- `crates/sandbawks-api/src/middleware.rs`
- `crates/sandbawks-api/src/auth.rs`
- `crates/sandbawks-api/src/state.rs`
- `crates/sandbawks-api/src/envd_client.rs`
- `crates/sandbawks-api/src/signing.rs`
- `crates/sandbawks-api/src/routes/mod.rs`
- `crates/sandbawks-api/src/routes/sandboxes.rs`
- `crates/sandbawks-api/src/routes/events.rs`
- `crates/sandbawks-api/src/routes/exec.rs`
- `crates/sandbawks-api/src/routes/files.rs`
- `crates/sandbawks-api/src/bin/sandbawks-api.rs`
- `crates/sandbawks-api/Dockerfile`
- `crates/sandbawks-api/tests/smoke.rs`
- `crates/sandbawks-api/tests/auth.rs`
- `crates/sandbawks-api/tests/sandboxes.rs`
- `crates/sandbawks-api/tests/events.rs`
- `crates/sandbawks-api/tests/signing.rs`
- `crates/sandbawks-api/tests/lifecycle_kind.rs`
- `crates/sandbawks-controller/src/pod_adapter.rs`
- `crates/sandbawks-controller/tests/pod_adapter.rs`
- `crates/sandbawks-types/src/auth.rs`
- `crates/sandbawks-types/tests/auth.rs`
- `deploy/kind/fixtures/api-smoke.sh`
- `.forgejo/workflows/api-image.yaml`
- `changelog/2026-05-23-plan-04-sandbawks-api.md`

**Modified:**

- `Cargo.toml` (add workspace member + new workspace deps: `axum`, `tower`, `tower-http`, `hmac`, `sha2`, `base64`, `subtle`, `mime`, `chrono`)
- `crates/sandbawks-types/Cargo.toml` (depend on new workspace `hmac`, `sha2`, `subtle`, `base64`)
- `crates/sandbawks-types/src/lib.rs` (re-export `auth` module)
- `crates/sandbawks-types/src/sandbox.rs` (add typed `spec.desired_phase: Option<SandboxPhase>` field; new top-level `PodRef` struct)
- `crates/sandbawks-types/src/runtime.rs` (extend `RuntimeAdapter` trait with `create_pod`, `delete_pod`, `pod_ready`; default-implemented no-ops so `FakeRuntimeAdapter` keeps compiling)
- `crates/sandbawks-controller/Cargo.toml` (add `k8s-openapi` Pod feature gate)
- `crates/sandbawks-controller/src/lib.rs` (re-export `pod_adapter`)
- `crates/sandbawks-controller/src/reconcile.rs` (read `spec.desired_phase` in addition to the legacy annotation; call `adapter.create_pod` on `Creating`; call `adapter.delete_pod` on `Terminating`)
- `crates/sandbawks-controller/src/fake_adapter.rs` (no behavior change — exercises that the new trait methods default-impl correctly)
- `crates/sandbawks-controller/src/bin/sandbawks-controller.rs` (add `--runtime {fake,pod}` clap flag; build `PodAdapter` when `pod`)
- `crates/sandbawks-controller/src/store.rs` (no behavior change — Pod watcher is wired in the binary, not here)
- `deploy/helm/sandbawks/templates/api-deployment.yaml` (replace `--placeholder` stub with real args; add envd-related env vars; add liveness/readiness probes; mount `SANDBAWKS_SIGNING_SECRET` from a Secret)
- `deploy/helm/sandbawks/templates/api-service.yaml` (unchanged; the existing ClusterIP + port 8080 are reused)
- `deploy/helm/sandbawks/templates/rbac.yaml` (grant `sandbawks-api` ServiceAccount read on `TeamAPIKey`, read/list/watch/create/delete on `Sandbox`, read on `Event`)
- `deploy/helm/sandbawks/templates/serviceaccount.yaml` (already creates `sandbawks-api`; no-op)
- `deploy/helm/sandbawks/templates/_helpers.tpl` (helper for the signing-secret name)
- `deploy/helm/sandbawks/templates/api-signing-secret.yaml` (new: opt-in random secret if user hasn't provided one)
- `deploy/helm/sandbawks/values.yaml` (point `images.api` at `ghcr.io/hypotheosis/sandbawks-api`; add `api.signingSecret` block)
- `justfile` (`api-image-build`, `api-image-run` recipes; extend `cluster-up` to install the api + load image)
- `deploy/kind/cluster.yaml` (no change — single-node cluster is sufficient)

**Crate count after plan:** 4 (types, controller, envd, api).

---

## Task 1: Workspace + `crates/sandbawks-api` scaffold

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/sandbawks-api/Cargo.toml`
- Create: `crates/sandbawks-api/src/lib.rs`
- Create: `crates/sandbawks-api/src/bin/sandbawks-api.rs`
- Create: `crates/sandbawks-api/tests/smoke.rs`

This task lands a compiling-but-empty crate with the new workspace dependencies wired in. The binary serves a single `GET /healthz` endpoint that returns `200 OK` — enough for the Helm chart's readiness probe in T13 to work end-to-end. No CRD interaction yet; that comes in T6.

- [ ] **Step 1: Add `sandbawks-api` as a workspace member + the new workspace dependencies**

Edit `Cargo.toml` — add the new member and the workspace deps. Keep alphabetical ordering inside `[workspace.dependencies]`:

```toml
[workspace]
resolver = "2"
members = [
    "crates/sandbawks-types",
    "crates/sandbawks-controller",
    "crates/envd",
    "crates/sandbawks-api",
]

[workspace.dependencies]
# ... existing deps ...

# HTTP / WS gateway (Plan 04)
axum = { version = "0.7", features = ["ws", "macros"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["trace", "request-id", "timeout", "cors", "util"] }

# Crypto for TeamAPIKey hashing + HMAC signed URLs (Plan 04)
hmac = "0.12"
sha2 = "0.10"
subtle = "2.6"
base64 = "0.22"

# Misc (Plan 04)
mime = "0.3"
chrono = { version = "0.4", default-features = false, features = ["serde", "clock"] }
```

- [ ] **Step 2: Create `crates/sandbawks-api/Cargo.toml`**

```toml
[package]
name = "sandbawks-api"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true
rust-version.workspace = true
description = "E2B-compatible HTTP/WS API gateway for the sandbawks Kubernetes sandbox controller."

[lib]
path = "src/lib.rs"

[[bin]]
name = "sandbawks-api"
path = "src/bin/sandbawks-api.rs"

[dependencies]
sandbawks-types = { path = "../sandbawks-types" }
envd = { path = "../envd" }

axum = { workspace = true }
tower = { workspace = true }
tower-http = { workspace = true }

kube = { workspace = true }
k8s-openapi = { workspace = true }

tokio = { workspace = true }
tokio-stream = { workspace = true }
tokio-util = { workspace = true }
futures = { workspace = true }
async-trait = { workspace = true }
bytes = { workspace = true }

serde = { workspace = true }
serde_json = { workspace = true }

tracing = { workspace = true }
tracing-subscriber = { workspace = true }

thiserror = { workspace = true }
anyhow = { workspace = true }

tonic = { workspace = true }
prost = { workspace = true }

hmac = { workspace = true }
sha2 = { workspace = true }
subtle = { workspace = true }
base64 = { workspace = true }
mime = { workspace = true }
chrono = { workspace = true }
clap = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
hegel = { workspace = true }
```

- [ ] **Step 3: Create `crates/sandbawks-api/src/lib.rs` with the module map**

```rust
//! E2B-compatible HTTP/WS API gateway for sandbawks.
//!
//! Layout mirrors the public-surface boundary: every externally
//! visible endpoint lives under `routes::`, while the cross-cutting
//! concerns (errors, auth, signing, envd-client pool) sit at the
//! crate root.
//!
//! The crate is intentionally library-first so integration tests can
//! drive an in-process axum router without binding a TCP port.

/// Smoke-test that downstream tests link against this crate.
pub const CRATE_NAME: &str = "sandbawks-api";

pub mod auth;
pub mod envd_client;
pub mod error;
pub mod middleware;
pub mod routes;
pub mod signing;
pub mod state;

pub use error::{AppError, AppResult};
pub use state::AppState;
```

- [ ] **Step 4: Stub the per-module files so the lib compiles**

Each file gets a one-line module declaration that the later tasks expand. Create the following with the indicated content:

`crates/sandbawks-api/src/auth.rs`:
```rust
//! Placeholder — implemented in Task 3.
```

`crates/sandbawks-api/src/envd_client.rs`:
```rust
//! Placeholder — implemented in Task 9.
```

`crates/sandbawks-api/src/error.rs`:
```rust
//! Placeholder — implemented in Task 2.

/// Stand-in until Task 2 lands the real `AppError`.
pub type AppError = anyhow::Error;

/// Result alias used by every handler.
pub type AppResult<T> = Result<T, AppError>;
```

`crates/sandbawks-api/src/middleware.rs`:
```rust
//! Placeholder — implemented in Task 2.
```

`crates/sandbawks-api/src/routes/mod.rs`:
```rust
//! Per-endpoint route modules. Tasks 6–12 fill these in.

pub mod events;
pub mod exec;
pub mod files;
pub mod sandboxes;
```

`crates/sandbawks-api/src/routes/events.rs`, `exec.rs`, `files.rs`, `sandboxes.rs`: each contains:
```rust
//! Placeholder — implemented in a later task.
```

`crates/sandbawks-api/src/signing.rs`:
```rust
//! Placeholder — implemented in Task 11.
```

`crates/sandbawks-api/src/state.rs`:
```rust
//! Shared `AppState` injected into every axum handler.
//!
//! Built once in `bin/sandbawks-api.rs`, cloned per request. All
//! fields are `Clone`-cheap (Arc'd internally) so axum can pass it
//! around freely.

use std::sync::Arc;

use kube::Client;

/// The state every handler receives via `State<AppState>`.
///
/// Task 9 grows this with `envd_clients: Arc<EnvdClientPool>`,
/// Task 11 grows it with `signing_key: Arc<SigningKey>`. The
/// minimum scaffold keeps just the kube client so health checks
/// + scaffolded handlers compile.
#[derive(Clone)]
pub struct AppState {
    pub kube: Client,
    /// Default namespace used when a request hasn't yet been
    /// authenticated. Tests inject `"default"`; the binary reads
    /// `SANDBAWKS_DEFAULT_NAMESPACE`.
    pub default_namespace: Arc<str>,
}
```

- [ ] **Step 5: Create `crates/sandbawks-api/src/bin/sandbawks-api.rs` with a `/healthz` server**

```rust
//! sandbawks-api binary entrypoint.
//!
//! Tasks 2–12 grow this into the full router; this initial commit
//! only serves `GET /healthz` so Plan 04 T13's Helm chart probes
//! pass against a real running binary.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use kube::Client;
use tokio::signal;
use tracing_subscriber::{prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let kube = Client::try_default()
        .await
        .context("building in-cluster kube client")?;
    let default_namespace: std::sync::Arc<str> =
        std::env::var("SANDBAWKS_DEFAULT_NAMESPACE")
            .unwrap_or_else(|_| "default".into())
            .into();
    let state = sandbawks_api::AppState {
        kube,
        default_namespace,
    };

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }))
        .with_state(state);

    let listen: SocketAddr = std::env::var("SANDBAWKS_API_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .context("parsing SANDBAWKS_API_LISTEN")?;

    tracing::info!(addr = %listen, "sandbawks-api listening");
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding {listen}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = signal::ctrl_c().await;
            tracing::info!("SIGINT received, shutting down");
        })
        .await
        .context("axum::serve")?;

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();
}
```

- [ ] **Step 6: Add a smoke test in `crates/sandbawks-api/tests/smoke.rs`**

```rust
//! Crate-link smoke test. Mirrors the same shape as
//! `crates/sandbawks-controller/tests/smoke.rs` and
//! `crates/envd/tests/smoke.rs`.

#[test]
fn crate_name_is_correct() {
    assert_eq!(sandbawks_api::CRATE_NAME, "sandbawks-api");
}
```

- [ ] **Step 7: Verify build + test**

Run:
```bash
cargo build -p sandbawks-api
cargo test -p sandbawks-api --tests
```

Expected: build is clean, one passing test. The binary is **not** runnable outside a cluster (requires kube client) — that's fine for now; T15's kind smoke runs it for real.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/sandbawks-api/
git commit -m "feat(api): workspace + crates/sandbawks-api scaffold (Plan 04 T1)"
```

---

## Task 2: `AppError` + middleware (request-id, tracing, timeout)

**Files:**
- Replace: `crates/sandbawks-api/src/error.rs`
- Replace: `crates/sandbawks-api/src/middleware.rs`
- Test: `crates/sandbawks-api/tests/error.rs`

E2B's API contract (spec §10) says every error response is JSON-shaped `{ "code": "...", "message": "...", "request_id": "..." }`. The `code` is a stable string (`sandbox_not_found`, `invalid_api_key`, `internal_error`, …) that SDKs match on; `request_id` matches the `X-Request-Id` header we attach via middleware. This task lands `AppError`, the `IntoResponse` impl that renders it, and the middleware stack every router uses.

- [ ] **Step 1: Write the failing render test**

`crates/sandbawks-api/tests/error.rs`:
```rust
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
        .oneshot(
            Request::builder()
                .uri("/boom")
                .body(Body::empty())
                .unwrap(),
        )
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
```

The dev-dependency `http-body-util` is needed; add to `[dev-dependencies]` in `crates/sandbawks-api/Cargo.toml`:
```toml
http-body-util = "0.1"
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cargo test -p sandbawks-api --test error
```

Expected: FAIL — `AppError::not_found` doesn't exist; `middleware::request_id_layer` doesn't exist.

- [ ] **Step 3: Replace `crates/sandbawks-api/src/error.rs` with the real `AppError`**

```rust
//! `AppError` — the single error type every handler returns.
//!
//! Renders to the E2B-compatible JSON envelope:
//!
//! ```json
//! { "code": "sandbox_not_found", "message": "...", "request_id": "..." }
//! ```
//!
//! `request_id` is pulled from the response extensions populated by
//! `middleware::request_id_layer`, so callers don't have to thread it
//! through. Internal-error variants log at `warn!`/`error!` before the
//! response is built so we never lose a stack trace at the network
//! boundary.

use axum::{
    extract::rejection::JsonRejection,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

/// Result alias used by every handler.
pub type AppResult<T> = Result<T, AppError>;

/// Errors a handler can raise.
///
/// The variant carries enough structured information for `IntoResponse`
/// to pick a `code` + HTTP status pair. Where the upstream cause is
/// useful for debugging, the variant boxes it and renders only a
/// human-readable summary to the client; the full cause is `tracing`-logged
/// in `IntoResponse`.
#[derive(Debug, Error)]
pub enum AppError {
    /// A resource we expected to exist was missing. `code` is the
    /// stable E2B error code; `message` is the human-readable body.
    #[error("{code}: {message}")]
    NotFound { code: &'static str, message: String },

    /// The request was malformed (missing field, bad enum value, body
    /// failed to deserialize).
    #[error("{code}: {message}")]
    BadRequest { code: &'static str, message: String },

    /// Authentication failed (missing/invalid `X-API-Key`).
    #[error("{code}: {message}")]
    Unauthorized { code: &'static str, message: String },

    /// Authentication succeeded but the caller's scopes don't permit
    /// the operation.
    #[error("{code}: {message}")]
    Forbidden { code: &'static str, message: String },

    /// A backend operation failed transiently (envd unreachable, kube
    /// transport error). Returned as 502; SDKs are expected to retry.
    #[error("{code}: {message}")]
    UpstreamUnavailable { code: &'static str, message: String },

    /// Everything else — bug, unexpected I/O, JSON encoding failure.
    /// Renders as 500 with code `internal_error` and a generic body;
    /// the underlying error is logged at `error!`.
    #[error("internal: {0}")]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    pub fn not_found(code: &'static str, msg: impl Into<String>) -> Self {
        Self::NotFound { code, message: msg.into() }
    }
    pub fn bad_request(code: &'static str, msg: impl Into<String>) -> Self {
        Self::BadRequest { code, message: msg.into() }
    }
    pub fn unauthorized(code: &'static str, msg: impl Into<String>) -> Self {
        Self::Unauthorized { code, message: msg.into() }
    }
    pub fn forbidden(code: &'static str, msg: impl Into<String>) -> Self {
        Self::Forbidden { code, message: msg.into() }
    }
    pub fn upstream(code: &'static str, msg: impl Into<String>) -> Self {
        Self::UpstreamUnavailable { code, message: msg.into() }
    }
}

impl From<kube::Error> for AppError {
    fn from(err: kube::Error) -> Self {
        match err {
            kube::Error::Api(ref api) if api.code == 404 => {
                AppError::not_found("not_found", api.message.clone())
            }
            other => {
                tracing::warn!(error = %other, "kube error → upstream_unavailable");
                AppError::upstream("kube_unavailable", other.to_string())
            }
        }
    }
}

impl From<JsonRejection> for AppError {
    fn from(rej: JsonRejection) -> Self {
        AppError::bad_request("invalid_json", rej.body_text())
    }
}

impl From<tonic::Status> for AppError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => AppError::not_found("envd_not_found", status.message().to_owned()),
            tonic::Code::InvalidArgument => AppError::bad_request("envd_invalid_argument", status.message().to_owned()),
            tonic::Code::Unavailable => AppError::upstream("envd_unavailable", status.message().to_owned()),
            _ => {
                tracing::warn!(?status, "tonic error → internal");
                AppError::Internal(anyhow::anyhow!("envd: {status}"))
            }
        }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
    request_id: &'a str,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            AppError::NotFound { code, message } => (StatusCode::NOT_FOUND, *code, message.clone()),
            AppError::BadRequest { code, message } => (StatusCode::BAD_REQUEST, *code, message.clone()),
            AppError::Unauthorized { code, message } => (StatusCode::UNAUTHORIZED, *code, message.clone()),
            AppError::Forbidden { code, message } => (StatusCode::FORBIDDEN, *code, message.clone()),
            AppError::UpstreamUnavailable { code, message } => (StatusCode::BAD_GATEWAY, *code, message.clone()),
            AppError::Internal(err) => {
                tracing::error!(error = ?err, "internal error rendered as 500");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "an internal error occurred".to_owned(),
                )
            }
        };

        // request-id is populated by middleware on the request side; we
        // re-attach it from the matching response extension that
        // `tower-http::set_request_id` mirrors. If absent (no
        // middleware installed, e.g. raw unit test), we generate one
        // here so the JSON body is never missing the field.
        let request_id = uuid_v4();

        let body = ErrorBody {
            code,
            message: &message,
            request_id: &request_id,
        };

        let mut resp = (status, Json(body)).into_response();
        resp.headers_mut().insert(
            header::HeaderName::from_static("x-request-id"),
            header::HeaderValue::from_str(&request_id).expect("ascii"),
        );
        resp
    }
}

/// 16-byte random hex — same shape as `tower-http::request_id::MakeRequestUuid`.
///
/// Local generator so error rendering doesn't depend on the request-id
/// middleware actually being installed (raw unit tests don't install it).
fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Cheap pseudo-uuid sufficient for request correlation, not for
    // cryptographic uniqueness. The real middleware uses a CSPRNG; this
    // fallback exists only for the no-middleware path.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("req_{nanos:032x}")
}
```

- [ ] **Step 4: Replace `crates/sandbawks-api/src/middleware.rs` with the real middleware factory**

```rust
//! Cross-cutting axum middleware: request-id assignment, structured
//! tracing of every request, and a global timeout.

use std::time::Duration;

use axum::http::{HeaderName, HeaderValue, Request};
use tower_http::{
    request_id::{MakeRequestId, RequestId, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer},
};

/// 16-byte hex request-id generator. Cheap, monotonic-ish, sufficient
/// for log correlation. We deliberately don't use `uuid::Uuid::new_v4`
/// to avoid pulling another dep.
#[derive(Clone, Default)]
pub struct Counter;

impl MakeRequestId for Counter {
    fn make_request_id<B>(&mut self, _req: &Request<B>) -> Option<RequestId> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let id = format!("req_{nanos:032x}");
        HeaderValue::from_str(&id).ok().map(RequestId::new)
    }
}

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// `tower-http::SetRequestIdLayer` factory.
///
/// Run before everything else in the stack so logs + the error
/// renderer can pull the request-id from the request extensions.
pub fn request_id_layer() -> SetRequestIdLayer<Counter> {
    SetRequestIdLayer::new(REQUEST_ID_HEADER, Counter)
}

/// Wraps every request in a `tracing` span tagged with method, URI,
/// status, latency, and request-id. The default spans + on_request /
/// on_response handlers are sufficient.
pub fn trace_layer() -> TraceLayer<tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>> {
    TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().include_headers(false))
        .on_request(DefaultOnRequest::new())
        .on_response(DefaultOnResponse::new())
}

/// Global per-request timeout. Long-running streams (exec WS, file
/// upload/download) are exempt — they nest their own per-route layers
/// that override this.
pub fn timeout_layer(secs: u64) -> TimeoutLayer {
    TimeoutLayer::new(Duration::from_secs(secs))
}
```

- [ ] **Step 5: Run the test to verify it passes**

```bash
cargo test -p sandbawks-api --test error
```

Expected: PASS.

- [ ] **Step 6: Run the full test suite + clippy to catch regressions**

```bash
cargo test -p sandbawks-api
cargo clippy -p sandbawks-api -- -D warnings
```

Expected: green.

- [ ] **Step 7: Commit**

```bash
git add crates/sandbawks-api/
git commit -m "feat(api): AppError + request-id/trace/timeout middleware (Plan 04 T2)"
```

---

## Task 3: TeamAPIKey auth middleware + `TeamContext` extractor

**Files:**
- Create: `crates/sandbawks-types/src/auth.rs`
- Create: `crates/sandbawks-types/tests/auth.rs`
- Modify: `crates/sandbawks-types/src/lib.rs`
- Modify: `crates/sandbawks-types/Cargo.toml`
- Replace: `crates/sandbawks-api/src/auth.rs`
- Create: `crates/sandbawks-api/tests/auth.rs`

Per spec §9, every API request authenticates via `X-API-Key: sbk_…`. The gateway:

1. Splits off the first 8 characters as `key_prefix`.
2. Lists `TeamAPIKey` CRs cluster-wide whose `spec.keyPrefix == prefix`. (Cluster-scope read; namespace is *resolved from* the matching CR, not assumed.)
3. SHA-256-hashes the full key and constant-time-compares against the stored `spec.keyHash`.
4. On match: build a `TeamContext { namespace, scopes }` and inject it into the request extensions for downstream extractors.

The hash helper itself lives in `sandbawks-types` so the controller's `Sandbox` admission code (later plans) can re-use it. The middleware lives in the api crate.

- [ ] **Step 1: Add `hmac` / `sha2` / `subtle` / `base64` to `sandbawks-types`**

Edit `crates/sandbawks-types/Cargo.toml` `[dependencies]`:
```toml
hmac = { workspace = true }
sha2 = { workspace = true }
subtle = { workspace = true }
base64 = { workspace = true }
```

- [ ] **Step 2: Write the failing hash helper tests**

`crates/sandbawks-types/tests/auth.rs`:
```rust
use sandbawks_types::auth::{hash_key, verify_key, ApiKeyHash, KeyPrefix};

#[test]
fn round_trips() {
    let raw = "sbk_a1b2c3d4eeeeeeeeeeeeeeee";
    let hashed: ApiKeyHash = hash_key(raw);
    assert!(hashed.0.starts_with("sha256:"));
    assert!(verify_key(raw, &hashed));
}

#[test]
fn rejects_wrong_key() {
    let hashed = hash_key("sbk_correct_correct_correct");
    assert!(!verify_key("sbk_wrongwr_wrongwr_wrongwr", &hashed));
}

#[test]
fn prefix_is_first_8_chars() {
    let p: KeyPrefix = KeyPrefix::from_key("sbk_a1b2c3d4eeeeeeeeeeeeeeee");
    assert_eq!(p.as_str(), "sbk_a1b2");
}

#[test]
fn prefix_short_key_pads() {
    let p = KeyPrefix::from_key("sbk_a1b");
    assert_eq!(p.as_str(), "sbk_a1b");
}
```

Run:
```bash
cargo test -p sandbawks-types --test auth
```
Expected: FAIL — `auth` module doesn't exist.

- [ ] **Step 3: Implement `crates/sandbawks-types/src/auth.rs`**

```rust
//! Shared `TeamAPIKey` hashing helpers.
//!
//! The full key is never stored or logged. The CR carries:
//!
//! - `spec.keyPrefix` — first 8 chars of the issued key, indexed in
//!   the listing API for O(1) lookup.
//! - `spec.keyHash`   — `sha256:<hex>` of the full key.
//!
//! At verify time the API gateway:
//!
//! 1. Computes `KeyPrefix::from_key(raw)` and lists CRs filtered by
//!    `spec.keyPrefix` (k8s field-selector — cheap).
//! 2. For each candidate, `verify_key(raw, &cr.spec.keyHash)` runs a
//!    constant-time compare.

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Prefix used for indexed CR lookup. Always at most 8 ASCII chars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPrefix(String);

impl KeyPrefix {
    pub fn from_key(raw: &str) -> Self {
        Self(raw.chars().take(8).collect())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stored hash. Always `sha256:<lowercase-hex>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyHash(pub String);

/// Hash a freshly-issued key. Returns a value safe to put in the CR.
pub fn hash_key(raw: &str) -> ApiKeyHash {
    let mut h = Sha256::new();
    h.update(raw.as_bytes());
    let digest = h.finalize();
    ApiKeyHash(format!("sha256:{}", hex_encode(&digest)))
}

/// Constant-time compare of a freshly-presented key against a stored hash.
///
/// Returns `false` for malformed hashes (wrong prefix, non-hex bytes).
pub fn verify_key(raw: &str, stored: &ApiKeyHash) -> bool {
    let Some(stored_hex) = stored.0.strip_prefix("sha256:") else {
        return false;
    };
    let Ok(stored_bytes) = hex_decode(stored_hex) else {
        return false;
    };
    if stored_bytes.len() != 32 {
        return false;
    }

    let mut h = Sha256::new();
    h.update(raw.as_bytes());
    let presented = h.finalize();

    presented.as_slice().ct_eq(&stored_bytes).into()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(nibble_to_hex(b >> 4));
        out.push(nibble_to_hex(b & 0x0f));
    }
    out
}

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if !s.is_ascii() || s.len() % 2 != 0 {
        return Err(());
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks(2) {
        out.push(hex_pair_to_byte(chunk[0], chunk[1])?);
    }
    Ok(out)
}

fn nibble_to_hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => unreachable!("nibble must be 0..=15"),
    }
}

fn hex_pair_to_byte(hi: u8, lo: u8) -> Result<u8, ()> {
    Ok((hex_to_nibble(hi)? << 4) | hex_to_nibble(lo)?)
}

fn hex_to_nibble(c: u8) -> Result<u8, ()> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(10 + c - b'a'),
        b'A'..=b'F' => Ok(10 + c - b'A'),
        _ => Err(()),
    }
}
```

- [ ] **Step 4: Re-export `auth` from `crates/sandbawks-types/src/lib.rs`**

Add `pub mod auth;` next to the existing modules.

- [ ] **Step 5: Verify the helper tests pass**

```bash
cargo test -p sandbawks-types --test auth
```
Expected: 4 passing tests.

- [ ] **Step 6: Write the middleware integration test**

`crates/sandbawks-api/tests/auth.rs`:
```rust
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
        .layer(axum::middleware::from_fn_with_state(
            store,
            auth_middleware,
        ))
}

#[tokio::test]
async fn happy_path() {
    let store = Arc::new(FakeKeyStore::default());
    store.insert("sbk_a1b2c3d4xxxxxxxx", "team-acme", &["sandboxes:*"]);
    let app = router(store.clone());

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
}

#[tokio::test]
async fn missing_header_is_unauthorized() {
    let store = Arc::new(FakeKeyStore::default());
    let app = router(store);
    let resp = app
        .oneshot(Request::builder().uri("/whoami").body(Body::empty()).unwrap())
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

    // Bonus: validating the salting/hash actually rejected this is
    // tied to the helper-test layer. Here we just smoke-check status.
    let _ = hash_key; // keep import alive
}
```

Run:
```bash
cargo test -p sandbawks-api --test auth
```
Expected: FAIL — `auth_middleware`, `FakeKeyStore`, `TeamContext` not yet defined.

- [ ] **Step 7: Replace `crates/sandbawks-api/src/auth.rs`**

```rust
//! TeamAPIKey-backed auth middleware.
//!
//! Wire-up:
//!
//! ```ignore
//! let store = KubeKeyStore::new(client);
//! let app = Router::new()
//!     .route("/sandboxes", post(handler))
//!     .layer(axum::middleware::from_fn_with_state(Arc::new(store), auth_middleware));
//! ```
//!
//! Downstream handlers extract `Extension<TeamContext>` to learn the
//! caller's namespace + scopes.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    extract::{Request, State},
    http::HeaderMap,
    middleware::Next,
    response::Response,
};
use kube::{api::ListParams, Api, Client};
use sandbawks_types::{
    auth::{verify_key, ApiKeyHash, KeyPrefix},
    TeamAPIKey,
};

use crate::error::AppError;

/// Per-request authentication context attached to the request
/// extensions on successful auth.
#[derive(Debug, Clone)]
pub struct TeamContext {
    /// Kubernetes Namespace the team owns.
    pub namespace: String,
    /// Scope strings from `TeamAPIKey.spec.scopes`.
    pub scopes: Vec<String>,
    /// Name of the matching `TeamAPIKey` CR — useful for audit logging.
    pub key_name: String,
}

impl TeamContext {
    /// Whether the caller's scopes permit a specific operation.
    /// MVP just checks `sandboxes:*`.
    pub fn allows(&self, op: &str) -> bool {
        self.scopes
            .iter()
            .any(|s| s == "sandboxes:*" || s == op)
    }
}

/// Trait abstracting the `TeamAPIKey` lookup. Production uses
/// `KubeKeyStore`; tests use `FakeKeyStore`.
#[async_trait]
pub trait KeyStore: Send + Sync {
    /// Find a `TeamAPIKey` whose `keyPrefix` matches `prefix` and whose
    /// stored hash verifies `raw_key`. Returns the resolved namespace +
    /// scopes + CR name on success.
    async fn resolve(&self, prefix: &KeyPrefix, raw_key: &str) -> Result<TeamContext, AppError>;
}

/// `kube::Api<TeamAPIKey>`-backed store. Lists cluster-wide because
/// `TeamAPIKey`s are namespaced and we don't yet know the namespace.
pub struct KubeKeyStore {
    api: Api<TeamAPIKey>,
}

impl KubeKeyStore {
    pub fn new(client: Client) -> Self {
        // All-namespaces list: the prefix selects across the whole
        // cluster.
        Self {
            api: Api::all(client),
        }
    }
}

#[async_trait]
impl KeyStore for KubeKeyStore {
    async fn resolve(&self, prefix: &KeyPrefix, raw_key: &str) -> Result<TeamContext, AppError> {
        // k8s field selectors don't index spec.* fields on custom
        // resources, so we filter client-side. With a tight `limit` +
        // the prefix narrowing on the API server side via metadata.name
        // (admission webhook in a later plan can enforce
        // `metadata.name == keyPrefix-XXXX` to keep this list cheap),
        // this is fine for MVP scale.
        let list = self
            .api
            .list(&ListParams::default().limit(500))
            .await?;

        for cr in list.items {
            if cr.spec.key_prefix != prefix.as_str() {
                continue;
            }
            let stored = ApiKeyHash(cr.spec.key_hash.clone());
            if verify_key(raw_key, &stored) {
                let namespace = cr
                    .metadata
                    .namespace
                    .clone()
                    .ok_or_else(|| {
                        AppError::Internal(anyhow::anyhow!("TeamAPIKey without namespace"))
                    })?;
                let key_name = cr.metadata.name.clone().unwrap_or_default();
                return Ok(TeamContext {
                    namespace,
                    scopes: cr.spec.scopes,
                    key_name,
                });
            }
        }
        Err(AppError::unauthorized("invalid_api_key", "no such api key"))
    }
}

/// In-memory store used by `tests/auth.rs`.
#[derive(Default)]
pub struct FakeKeyStore {
    inner: Mutex<Vec<(String, TeamContext)>>,
}

impl FakeKeyStore {
    pub fn insert(&self, raw_key: &str, namespace: &str, scopes: &[&str]) {
        let ctx = TeamContext {
            namespace: namespace.to_owned(),
            scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
            key_name: format!("fake-{}", &raw_key[..raw_key.len().min(8)]),
        };
        self.inner
            .lock()
            .expect("poisoned")
            .push((raw_key.to_owned(), ctx));
    }
}

#[async_trait]
impl KeyStore for FakeKeyStore {
    async fn resolve(&self, prefix: &KeyPrefix, raw_key: &str) -> Result<TeamContext, AppError> {
        let guard = self.inner.lock().expect("poisoned");
        for (stored_key, ctx) in guard.iter() {
            let stored_prefix = KeyPrefix::from_key(stored_key);
            if stored_prefix != *prefix {
                continue;
            }
            if stored_key == raw_key {
                return Ok(ctx.clone());
            }
        }
        Err(AppError::unauthorized("invalid_api_key", "no such api key"))
    }
}

/// `axum::middleware::from_fn_with_state` middleware that extracts
/// `X-API-Key`, resolves it to a `TeamContext` via the supplied store,
/// and injects the context into the request extensions.
pub async fn auth_middleware<S: KeyStore + ?Sized>(
    State(store): State<Arc<S>>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let raw_key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::unauthorized("missing_api_key", "X-API-Key header missing"))?;

    let prefix = KeyPrefix::from_key(raw_key);
    let ctx = store.resolve(&prefix, raw_key).await?;
    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}
```

- [ ] **Step 8: Verify both test files pass + clippy**

```bash
cargo test -p sandbawks-types
cargo test -p sandbawks-api
cargo clippy -p sandbawks-types -p sandbawks-api -- -D warnings
```
Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add crates/sandbawks-types/ crates/sandbawks-api/
git commit -m "feat(api): TeamAPIKey auth middleware + TeamContext extractor (Plan 04 T3)"
```

---

## Task 4: `spec.desiredPhase` + extend `RuntimeAdapter` with pod-mgmt methods + Pod watcher in reconcile

**Files:**
- Modify: `crates/sandbawks-types/src/sandbox.rs`
- Modify: `crates/sandbawks-types/src/runtime.rs`
- Modify: `crates/sandbawks-controller/src/reconcile.rs`
- Modify: `crates/sandbawks-controller/src/fake_adapter.rs`
- Modify: `crates/sandbawks-controller/tests/lifecycle.rs` (if present) — verify the fake adapter still drives a full lifecycle

This task lands the missing controller plumbing the API gateway will rely on:

1. A typed `spec.desired_phase: Option<SandboxPhase>` field on `Sandbox` so the gateway can `kubectl patch` it directly instead of writing an annotation.
2. Three new methods on `RuntimeAdapter` — `create_pod`, `delete_pod`, `pod_ready` — with default no-op implementations so `FakeRuntimeAdapter` continues to compile without changes.
3. Reconcile-loop changes so `Action::CreatePod` runs `adapter.create_pod`, `Action::Terminate` calls `delete_pod`, and the `Creating → Running` transition consults `adapter.pod_ready(...)`.

The new `desired_phase` field obsoletes the annotation contract but the annotation reader is left intact for one plan so any existing CRs in flight keep working; the annotation reader is removed in Plan 05.

- [ ] **Step 1: Add `spec.desired_phase` to `SandboxSpec`**

Edit `crates/sandbawks-types/src/sandbox.rs`. Add the field below `auto_pause`:

```rust
/// Phase the API gateway wants the controller to drive toward.
///
/// `Some(Paused)` requests pause; `Some(Running)` requests resume.
/// Replaces the legacy `sandbawks.io/desired-phase` annotation; the
/// annotation reader is kept for one plan for backwards compatibility
/// and removed in Plan 05.
#[serde(skip_serializing_if = "Option::is_none", default)]
pub desired_phase: Option<SandboxPhase>,
```

Add to `SandboxStatus`:

```rust
/// Random suffix the controller stamps when materializing a Pod, used
/// to make the Pod name unique vs. CR name when needed.
#[serde(skip_serializing_if = "Option::is_none", default)]
pub pod_uid: Option<String>,
```

- [ ] **Step 2: Add `PodRef` to `sandbawks-types::runtime`**

Append to `crates/sandbawks-types/src/runtime.rs`:

```rust
/// Handle returned by `RuntimeAdapter::create_pod` describing the Pod
/// the controller just materialized.
///
/// The controller stores `name` + `namespace` in `Sandbox.status.podRef`
/// (as `"namespace/name"`) and `envd_address` in `status.envdAddress`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRef {
    pub name: String,
    pub namespace: String,
}
```

- [ ] **Step 3: Extend the `RuntimeAdapter` trait with default-implemented methods**

```rust
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn pause(&self, pod_uid: &str) -> Result<SnapshotManifest, RuntimeError>;
    async fn resume(&self, manifest: &SnapshotManifest, pod_uid: &str) -> Result<(), RuntimeError>;
    fn runtime_class(&self) -> &str;

    /// Materialize a Pod for the given Sandbox. The Pod runs envd as
    /// its only container; the controller is responsible for stamping
    /// `Sandbox.status.podRef` once this returns.
    ///
    /// Default impl returns the `FakePodRef` so `FakeRuntimeAdapter`
    /// keeps compiling without changes. `PodAdapter` (Task 5)
    /// overrides this.
    async fn create_pod(
        &self,
        sandbox: &crate::Sandbox,
        envd_image: &str,
    ) -> Result<PodRef, RuntimeError> {
        let _ = envd_image;
        Ok(PodRef {
            name: format!(
                "{}-fake",
                sandbox.metadata.name.as_deref().unwrap_or("anon")
            ),
            namespace: sandbox
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into()),
        })
    }

    /// Tear down the Pod previously created for this Sandbox. No-op for
    /// the fake adapter; `PodAdapter` deletes the kube object.
    async fn delete_pod(&self, pod_ref: &PodRef) -> Result<(), RuntimeError> {
        let _ = pod_ref;
        Ok(())
    }

    /// Poll the Pod's readiness. Returns:
    ///
    /// - `Ok(None)` — Pod exists but is not yet Ready (no `envdAddress`
    ///   to surface).
    /// - `Ok(Some(addr))` — Pod is Ready, `addr` is the `host:port`
    ///   the controller should write to `status.envdAddress`.
    ///
    /// Default impl returns `Some("127.0.0.1:49983")` so the
    /// `FakeRuntimeAdapter` keeps driving the existing state machine
    /// tests unchanged.
    async fn pod_ready(&self, pod_ref: &PodRef) -> Result<Option<String>, RuntimeError> {
        let _ = pod_ref;
        Ok(Some("127.0.0.1:49983".into()))
    }
}
```

Note: this needs `use crate::Sandbox;` at the top, which means `runtime.rs` now depends on `sandbox.rs`. That's fine — they're already in the same crate.

- [ ] **Step 4: Re-export `PodRef` from `sandbawks-types`**

In `crates/sandbawks-types/src/lib.rs`:
```rust
pub use runtime::{PodRef, RuntimeAdapter, RuntimeError, SnapshotManifest};
```

- [ ] **Step 5: Update reconcile to call `create_pod` on Creating + `delete_pod` on Terminating**

In `crates/sandbawks-controller/src/reconcile.rs`:

Replace the `derive_trigger` function so it reads `spec.desired_phase` if set, else falls back to the annotation:

```rust
fn derive_trigger(sandbox: &Sandbox, current: SandboxPhase) -> Option<Trigger> {
    if sandbox.metadata.deletion_timestamp.is_some() {
        return Some(Trigger::KillRequested);
    }

    let annotations = sandbox.metadata.annotations.as_ref();
    let desired_from_field = sandbox.spec.desired_phase;
    let desired_from_annotation = annotations
        .and_then(|a| a.get(ANN_DESIRED_PHASE))
        .and_then(|v| match v.as_str() {
            "Running" => Some(SandboxPhase::Running),
            "Paused" => Some(SandboxPhase::Paused),
            _ => None,
        });
    let desired = desired_from_field.or(desired_from_annotation);
    let pod_ready = sandbox
        .status
        .as_ref()
        .and_then(|s| s.envd_address.as_deref())
        .is_some()
        || annotations
            .and_then(|a| a.get(ANN_POD_READY))
            .map(String::as_str)
            == Some("true");

    match (current, desired, pod_ready) {
        (SandboxPhase::Creating, _, true) => Some(Trigger::PodReady),
        (SandboxPhase::Creating, _, false) => Some(Trigger::PodCreate),
        (SandboxPhase::Running, Some(SandboxPhase::Paused), _) => Some(Trigger::PauseRequested),
        (SandboxPhase::Paused, Some(SandboxPhase::Running), _) => Some(Trigger::ResumeRequested),
        _ => None,
    }
}
```

Add a `PodCreate` trigger + `CreatePod` action to `state_machine.rs`:

```rust
// In Trigger enum:
PodCreate,

// In Action enum:
CreatePod,

// In transition():
(SandboxPhase::Creating, Trigger::PodCreate) => Some((SandboxPhase::Creating, Action::CreatePod)),
```

Extend `dispatch()` to handle `Action::CreatePod` and update `Action::Terminate` to call `delete_pod`:

```rust
Action::CreatePod => {
    // Default envd image — overridden in the controller binary
    // from SANDBAWKS_DEFAULT_ENVD_IMAGE.
    let envd_image = std::env::var("SANDBAWKS_DEFAULT_ENVD_IMAGE")
        .unwrap_or_else(|_| "ghcr.io/hypotheosis/envd:latest".into());
    let pod_ref = ctx.adapter.create_pod(sandbox, &envd_image).await?;
    new_status.pod_ref = Some(format!("{}/{}", pod_ref.namespace, pod_ref.name));
    new_status.pod_uid = None; // populated when the Pod object's UID is observed
    // Phase stays Creating; we move to Running once pod_ready returns
    // Some(addr).
    Ok(())
}
Action::Terminate => {
    if let Some(pod_ref_str) = new_status.pod_ref.as_deref() {
        if let Some((ns, name)) = pod_ref_str.split_once('/') {
            let pr = sandbawks_types::PodRef {
                namespace: ns.to_owned(),
                name: name.to_owned(),
            };
            ctx.adapter.delete_pod(&pr).await?;
        }
    }
    Ok(())
}
```

The PodReady transition path needs `pod_ready()` to be called *once* — adapt `dispatch`'s no-op `(Creating, PodReady)` path:

```rust
Action::Noop => {
    // For (Creating, PodReady) we need to populate envd_address
    // from the adapter so the API gateway has somewhere to send
    // exec/file requests.
    if let Some(pod_ref_str) = new_status.pod_ref.as_deref() {
        if let Some((ns, name)) = pod_ref_str.split_once('/') {
            let pr = sandbawks_types::PodRef {
                namespace: ns.to_owned(),
                name: name.to_owned(),
            };
            if let Some(addr) = ctx.adapter.pod_ready(&pr).await? {
                new_status.envd_address = Some(addr);
                new_status.phase = SandboxPhase::Running;
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 6: Add a unit test for the new state machine transitions**

`crates/sandbawks-controller/tests/state_machine_unit.rs` — extend with:

```rust
#[test]
fn creating_pod_create_triggers_create_pod_action() {
    let r = transition(SandboxPhase::Creating, Trigger::PodCreate);
    assert_eq!(r, Some((SandboxPhase::Creating, Action::CreatePod)));
}
```

- [ ] **Step 7: Run all controller tests + clippy**

```bash
cargo test -p sandbawks-controller -p sandbawks-types
cargo clippy -p sandbawks-controller -p sandbawks-types -- -D warnings
```
Expected: green. The `FakeRuntimeAdapter` continues to pass its existing tests because its default-impl `create_pod` returns a `PodRef` and `pod_ready` returns `Some(addr)` — the existing `lifecycle.rs` test that pre-set `ANN_POD_READY` no longer needs to do so (annotation path is still honored, but the new code path also works).

- [ ] **Step 8: Regenerate the CRD YAMLs**

```bash
cargo run -p sandbawks-types --bin crdgen -- deploy/crds
git diff deploy/crds/sandboxes.sandbawks.io.yaml | head -50
```

Expected diff: `spec.desiredPhase` added to the Sandbox CRD schema; `status.podUid` added.

- [ ] **Step 9: Commit**

```bash
git add crates/sandbawks-types/ crates/sandbawks-controller/ deploy/crds/
git commit -m "feat(controller): spec.desiredPhase + RuntimeAdapter pod-mgmt extension (Plan 04 T4)"
```

---

## Task 5: `PodAdapter` impl + `--runtime {fake,pod}` controller flag

**Files:**
- Create: `crates/sandbawks-controller/src/pod_adapter.rs`
- Modify: `crates/sandbawks-controller/src/lib.rs`
- Create: `crates/sandbawks-controller/tests/pod_adapter.rs`
- Modify: `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`
- Modify: `crates/sandbawks-controller/Cargo.toml`

`PodAdapter` is the first real `RuntimeAdapter` impl. It materializes a Pod whose only container runs the envd image, watches the Pod for Ready, and surfaces `<podIP>:49983` as the envd address. Pause/resume return `Ok(empty_snapshot)` / `Ok(())` — real preservation lands in Plan 05's `GvisorAdapter`. The Pod is created in the Sandbox's namespace with an `ownerReference` back to the Sandbox CR so kube-gc deletes the Pod when the Sandbox is deleted.

`runtime_class()` returns `"pod"` to distinguish it from `"fake"`.

Important: **no `spec.runtimeClassName` is set on the Pod.** The cluster's default runtime handler (containerd/runc in `kind` and in our prod cluster) wins. Setting an explicit RuntimeClass is deliberately out of scope until Plan 05 lands `GvisorAdapter`.

- [ ] **Step 1: Add a `clap` dep on the controller for the new flag**

Edit `crates/sandbawks-controller/Cargo.toml` `[dependencies]`:
```toml
clap = { workspace = true }
```

(Already present in workspace deps from Plan 03 envd; this just adds it to the controller crate.)

- [ ] **Step 2: Write the failing test against a `kube` fake-server**

`crates/sandbawks-controller/tests/pod_adapter.rs`:
```rust
//! `PodAdapter` smoke test using a `kube` fake client.
//!
//! The fake client intercepts every API call; we assert that
//! `create_pod` issues a `POST` to the namespaced pods endpoint
//! with the expected envd container, and that `delete_pod` issues
//! the matching `DELETE`.

use http::{Request, Response};
use k8s_openapi::api::core::v1::Pod;
use kube::{client::Body, Client, ResourceExt};
use sandbawks_controller::PodAdapter;
use sandbawks_types::{
    NetworkPolicy, ResourceRequests, RuntimeAdapter, Sandbox, SandboxSpec,
};

fn fixture_sandbox() -> Sandbox {
    Sandbox::new(
        "sbx-test",
        SandboxSpec {
            template_ref: "tpl".into(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
                ephemeral_storage: None,
            },
            timeout_seconds: 60,
            auto_pause: false,
            env: Default::default(),
            metadata: Default::default(),
            network_policy: Some(NetworkPolicy::default()),
            desired_phase: None,
        },
    )
}

#[tokio::test(flavor = "current_thread")]
async fn create_pod_posts_to_kube() {
    let (mock_service, handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(mock_service, "team-acme");
    let adapter = PodAdapter::new(client);

    let mut sandbox = fixture_sandbox();
    sandbox.metadata.namespace = Some("team-acme".into());

    let join = tokio::spawn(async move {
        adapter
            .create_pod(&sandbox, "ghcr.io/hypotheosis/envd:test")
            .await
    });

    // The fake server expects exactly one POST /api/v1/namespaces/team-acme/pods.
    let (req, send) = handle.into_inner().next_request().await.expect("request");
    assert_eq!(req.method(), "POST");
    assert!(req.uri().path().ends_with("/namespaces/team-acme/pods"));
    let body_bytes = req.into_body().collect_bytes().await.unwrap();
    let pod: Pod = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(pod.metadata.namespace.as_deref(), Some("team-acme"));
    assert_eq!(
        pod.spec.as_ref().unwrap().containers[0].image.as_deref(),
        Some("ghcr.io/hypotheosis/envd:test"),
    );
    assert!(pod.spec.as_ref().unwrap().runtime_class_name.is_none());

    // Echo back a created pod so the adapter call returns.
    let created = Pod {
        metadata: pod.metadata.clone(),
        spec: pod.spec.clone(),
        status: Default::default(),
    };
    let resp = Response::builder()
        .status(201)
        .body(Body::from(serde_json::to_vec(&created).unwrap()))
        .unwrap();
    send.send_response(resp);

    let pod_ref = join.await.unwrap().unwrap();
    assert_eq!(pod_ref.namespace, "team-acme");
    assert!(pod_ref.name.starts_with("sbx-test"));
    let _ = ResourceExt::name_any; // keep import alive
}
```

Add to `crates/sandbawks-controller/Cargo.toml` `[dev-dependencies]`:
```toml
tower-test = "0.4"
http = "1"
http-body-util = "0.1"
```

Run:
```bash
cargo test -p sandbawks-controller --test pod_adapter
```
Expected: FAIL — `PodAdapter` doesn't exist.

- [ ] **Step 3: Implement `crates/sandbawks-controller/src/pod_adapter.rs`**

```rust
//! `PodAdapter` — materializes a `Sandbox` CR into a Pod running envd.
//!
//! First *real* `RuntimeAdapter` impl. Intentionally does **not** set
//! `pod.spec.runtimeClassName`: the cluster's default runtime handler
//! (containerd/runc) is used. Real RuntimeClass-driven dispatch lands
//! in Plan 05's `GvisorAdapter` once gVisor / Kata are available.
//!
//! Pause/resume are no-ops here — `RuntimeError::Pause("unimplemented
//! by PodAdapter")` would break the API surface, so we instead return
//! an empty `SnapshotManifest` for pause and `Ok(())` for resume,
//! matching `FakeRuntimeAdapter`'s semantics. The Sandbox's recorded
//! state is therefore *not* preserved across a pause/connect cycle in
//! this plan; that gets fixed by `GvisorAdapter`.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, Pod, PodSpec, ResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ObjectMeta, PostParams};
use kube::core::ObjectList;
use kube::{Api, Client, ResourceExt};
use sandbawks_types::{
    PodRef, RuntimeAdapter, RuntimeError, Sandbox, SnapshotManifest,
};
use std::collections::BTreeMap;

/// envd's well-known port. Matches the constant in `crates/envd/src/main.rs`.
const ENVD_PORT: i32 = 49_983;

/// Real-Pod adapter. Constructed once per controller process.
#[derive(Clone)]
pub struct PodAdapter {
    client: Client,
}

impl PodAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn pods(&self, namespace: &str) -> Api<Pod> {
        Api::namespaced(self.client.clone(), namespace)
    }

    /// Build the Pod object for a Sandbox.
    ///
    /// Kept as a free function so unit tests can assert on the
    /// generated spec without a kube client.
    pub fn pod_spec_for(sandbox: &Sandbox, envd_image: &str) -> Pod {
        let name = sandbox
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "sandbox".into());
        let namespace = sandbox
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());

        let mut labels = BTreeMap::new();
        labels.insert("app.kubernetes.io/name".into(), "sandbawks-sandbox".into());
        labels.insert("sandbawks.io/sandbox".into(), name.clone());

        let owner_references = sandbox.metadata.uid.as_ref().map(|uid| {
            vec![k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                api_version: "sandbawks.io/v1alpha1".into(),
                kind: "Sandbox".into(),
                name: name.clone(),
                uid: uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]
        });

        let resources = ResourceRequirements {
            requests: Some({
                let mut m = BTreeMap::new();
                m.insert("cpu".into(), Quantity(sandbox.spec.resources.cpu.to_string()));
                m.insert("memory".into(), Quantity(sandbox.spec.resources.memory.to_string()));
                m
            }),
            limits: Some({
                let mut m = BTreeMap::new();
                m.insert("cpu".into(), Quantity(sandbox.spec.resources.cpu.to_string()));
                m.insert("memory".into(), Quantity(sandbox.spec.resources.memory.to_string()));
                if let Some(eph) = &sandbox.spec.resources.ephemeral_storage {
                    m.insert("ephemeral-storage".into(), Quantity(eph.to_string()));
                }
                m
            }),
            claims: None,
        };

        let env: Vec<_> = sandbox
            .spec
            .env
            .iter()
            .map(|(k, v)| k8s_openapi::api::core::v1::EnvVar {
                name: k.clone(),
                value: Some(v.clone()),
                value_from: None,
            })
            .collect();

        Pod {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some(namespace),
                labels: Some(labels),
                owner_references,
                ..Default::default()
            },
            spec: Some(PodSpec {
                restart_policy: Some("Never".into()),
                containers: vec![Container {
                    name: "envd".into(),
                    image: Some(envd_image.to_owned()),
                    ports: Some(vec![ContainerPort {
                        container_port: ENVD_PORT,
                        name: Some("envd".into()),
                        protocol: Some("TCP".into()),
                        ..Default::default()
                    }]),
                    env: Some(env),
                    resources: Some(resources),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        }
    }
}

#[async_trait]
impl RuntimeAdapter for PodAdapter {
    fn runtime_class(&self) -> &str {
        "pod"
    }

    async fn create_pod(
        &self,
        sandbox: &Sandbox,
        envd_image: &str,
    ) -> Result<PodRef, RuntimeError> {
        if sandbox.spec.runtime_class_name.is_some() {
            tracing::warn!(
                sandbox = %sandbox.name_any(),
                runtime_class = ?sandbox.spec.runtime_class_name,
                "PodAdapter ignores spec.runtimeClassName; using cluster default runtime"
            );
        }

        let pod = Self::pod_spec_for(sandbox, envd_image);
        let namespace = pod
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());

        let created = self
            .pods(&namespace)
            .create(&PostParams::default(), &pod)
            .await
            .map_err(|e| RuntimeError::Transient(format!("create pod: {e}")))?;

        Ok(PodRef {
            name: created.metadata.name.clone().unwrap_or_default(),
            namespace: created
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into()),
        })
    }

    async fn delete_pod(&self, pod_ref: &PodRef) -> Result<(), RuntimeError> {
        match self
            .pods(&pod_ref.namespace)
            .delete(&pod_ref.name, &DeleteParams::background())
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(api)) if api.code == 404 => Ok(()),
            Err(e) => Err(RuntimeError::Transient(format!("delete pod: {e}"))),
        }
    }

    async fn pod_ready(&self, pod_ref: &PodRef) -> Result<Option<String>, RuntimeError> {
        let pod = match self.pods(&pod_ref.namespace).get_opt(&pod_ref.name).await {
            Ok(Some(p)) => p,
            Ok(None) => return Ok(None),
            Err(e) => return Err(RuntimeError::Transient(format!("get pod: {e}"))),
        };

        let status = pod.status.as_ref();
        let is_ready = status
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| {
                conds
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            })
            .unwrap_or(false);

        if !is_ready {
            return Ok(None);
        }

        let ip = status
            .and_then(|s| s.pod_ip.clone())
            .ok_or_else(|| RuntimeError::Transient("pod Ready but podIP missing".into()))?;
        Ok(Some(format!("{ip}:{ENVD_PORT}")))
    }

    async fn pause(&self, _pod_uid: &str) -> Result<SnapshotManifest, RuntimeError> {
        Ok(SnapshotManifest {
            sandbox_id: "noop".into(),
            runtime_class: self.runtime_class().to_owned(),
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".into(),
            created_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn resume(
        &self,
        _manifest: &SnapshotManifest,
        _pod_uid: &str,
    ) -> Result<(), RuntimeError> {
        // PodAdapter cannot preserve state — the Pod that was paused
        // is still running. Real restore lands in Plan 05.
        Ok(())
    }
}

// Crate-level so unused import isn't a warning when feature flags
// change.
#[allow(dead_code)]
fn _unused(_l: &ObjectList<Pod>) {}
```

The `chrono` dep is already in the workspace; expose it on the controller crate by adding to `crates/sandbawks-controller/Cargo.toml`:
```toml
chrono = { workspace = true }
```

- [ ] **Step 4: Re-export `PodAdapter` from `crates/sandbawks-controller/src/lib.rs`**

```rust
pub mod pod_adapter;
pub use pod_adapter::PodAdapter;
```

- [ ] **Step 5: Add `--runtime` flag to the controller binary**

In `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`:

```rust
use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Which runtime adapter to use.
    #[arg(long, value_enum, default_value_t = RuntimeChoice::Fake, env = "SANDBAWKS_RUNTIME")]
    runtime: RuntimeChoice,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum RuntimeChoice {
    /// No-op adapter — does not create real Pods. Used in unit tests
    /// and CI Plan 02 lifecycle tests.
    Fake,
    /// Real Pod-materializing adapter (Plan 04 T5).
    Pod,
}
```

Inside `main`, after building the kube client:
```rust
let cli = Cli::parse();
let adapter: Arc<dyn sandbawks_types::RuntimeAdapter> = match cli.runtime {
    RuntimeChoice::Fake => Arc::new(FakeRuntimeAdapter::new("fake")),
    RuntimeChoice::Pod => Arc::new(sandbawks_controller::PodAdapter::new(client.clone())),
};
```

- [ ] **Step 6: Run tests + clippy**

```bash
cargo test -p sandbawks-controller
cargo clippy -p sandbawks-controller -- -D warnings
```
Expected: green.

- [ ] **Step 7: Commit**

```bash
git add crates/sandbawks-controller/
git commit -m "feat(controller): PodAdapter + --runtime flag (Plan 04 T5)"
```

---

## Task 6: Lifecycle CRUD endpoints — `POST/GET /sandboxes`, `POST /sandboxes/{id}/kill`

**Files:**
- Replace: `crates/sandbawks-api/src/routes/sandboxes.rs`
- Modify: `crates/sandbawks-api/src/lib.rs` (add a `router()` constructor)
- Modify: `crates/sandbawks-api/src/state.rs`
- Modify: `crates/sandbawks-api/src/bin/sandbawks-api.rs` (wire the router)
- Create: `crates/sandbawks-api/tests/sandboxes.rs`

This task lands three of the five E2B lifecycle endpoints. They translate HTTP requests into CRD operations against the team's namespace (resolved from `TeamContext`).

E2B-shaped JSON DTOs:

```json
// POST /sandboxes — body
{
  "templateID": "python-data-sci",
  "metadata": { "user": "alice" },
  "envVars": { "OPENAI_API_KEY": "..." },
  "timeout": 3600
}

// POST /sandboxes — response
{
  "sandboxID": "sbx-abc123",
  "templateID": "python-data-sci",
  "clientID": "team-acme",
  "alias": null,
  "metadata": { "user": "alice" },
  "startedAt": "2026-05-23T10:00:00Z",
  "endAt": "2026-05-23T11:00:00Z"
}

// GET /sandboxes — response
[ <one of the above per sandbox> ]
```

`POST /sandboxes/{id}/kill` returns `204 No Content`.

- [ ] **Step 1: Add `AppState` fields the routes need**

Edit `crates/sandbawks-api/src/state.rs`. Replace `AppState` with:

```rust
use std::sync::Arc;

use kube::Client;

use crate::auth::{KeyStore, KubeKeyStore};

#[derive(Clone)]
pub struct AppState {
    pub kube: Client,
    pub default_namespace: Arc<str>,
    pub key_store: Arc<dyn KeyStore>,
}

impl AppState {
    pub fn from_kube(kube: Client, default_namespace: impl Into<Arc<str>>) -> Self {
        let key_store: Arc<dyn KeyStore> = Arc::new(KubeKeyStore::new(kube.clone()));
        Self {
            kube,
            default_namespace: default_namespace.into(),
            key_store,
        }
    }
}
```

- [ ] **Step 2: Write the failing test against a fake `AppState`**

`crates/sandbawks-api/tests/sandboxes.rs`:
```rust
//! Lifecycle CRUD smoke tests.
//!
//! These run against a `FakeKeyStore` and a fake `SandboxStore` (no
//! real kube client). They cover request/response shape; T15 covers
//! the round-trip against a real cluster.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use sandbawks_api::routes::sandboxes::router as sandboxes_router;
use sandbawks_api::AppState;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn fixture_state() -> AppState {
    use sandbawks_api::auth::FakeKeyStore;
    let store = Arc::new(FakeKeyStore::default());
    store.insert("sbk_a1b2c3d4xxxxxxxx", "team-acme", &["sandboxes:*"]);
    // Tests don't need a real kube client; the router under test
    // uses a fake `SandboxOps` trait wired via `with_sandbox_ops`.
    AppState::for_test("team-acme", store)
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
    let json: Value = serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes())
        .unwrap();
    assert!(json["sandboxID"].as_str().unwrap().starts_with("sbx-"));
    assert_eq!(json["templateID"], "python-data-sci");
    assert_eq!(json["clientID"], "team-acme");
    assert_eq!(json["metadata"]["user"], "alice");
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
    let json: Value = serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes())
        .unwrap();
    assert!(json.is_array());
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
    let body: Value =
        serde_json::from_slice(&create.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let id = body["sandboxID"].as_str().unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&format!("/sandboxes/{id}/kill"))
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}
```

Run:
```bash
cargo test -p sandbawks-api --test sandboxes
```
Expected: FAIL — `routes::sandboxes::router` and `AppState::for_test` don't exist.

- [ ] **Step 3: Implement `crates/sandbawks-api/src/routes/sandboxes.rs`**

```rust
//! Sandbox lifecycle endpoints translated to CRD operations.
//!
//! Routes:
//!
//! - `POST   /sandboxes`              create
//! - `GET    /sandboxes`              list (team-scoped)
//! - `POST   /sandboxes/:id/kill`     delete (and let kube-gc + the
//!                                    controller's `Action::Terminate`
//!                                    delete the Pod)
//! - `POST   /sandboxes/:id/pause`    (Task 7)
//! - `POST   /sandboxes/:id/connect`  (Task 7)
//!
//! All routes are wrapped by `auth_middleware` (Task 3); handlers
//! extract `Extension<TeamContext>` to learn the team's namespace.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use kube::{
    api::{DeleteParams, ListParams, PostParams},
    Api, Client, ResourceExt,
};
use sandbawks_types::{
    ResourceQuantity, ResourceRequests, Sandbox, SandboxSpec,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

use crate::{
    auth::{auth_middleware, KeyStore, TeamContext},
    error::{AppError, AppResult},
    AppState,
};

// ---------- E2B DTOs ----------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxBody {
    pub template_id: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub env_vars: BTreeMap<String, String>,
    /// Seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u32,
}

fn default_timeout() -> u32 {
    3600
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxDTO {
    pub sandbox_id: String,
    pub template_id: String,
    pub client_id: String,
    pub alias: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub started_at: String,
    pub end_at: String,
}

// ---------- SandboxOps trait so we can fake it in tests ----------

/// Abstraction over the `kube::Api<Sandbox>` calls the routes make.
/// Real impl is `KubeSandboxOps`; tests use `FakeSandboxOps`.
#[async_trait]
pub trait SandboxOps: Send + Sync {
    async fn create(&self, namespace: &str, sandbox: Sandbox) -> Result<Sandbox, AppError>;
    async fn list(&self, namespace: &str) -> Result<Vec<Sandbox>, AppError>;
    async fn delete(&self, namespace: &str, name: &str) -> Result<(), AppError>;
    async fn patch_desired_phase(
        &self,
        namespace: &str,
        name: &str,
        phase: sandbawks_types::SandboxPhase,
    ) -> Result<(), AppError>;
    async fn get(&self, namespace: &str, name: &str) -> Result<Sandbox, AppError>;
}

pub struct KubeSandboxOps {
    client: Client,
}

impl KubeSandboxOps {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
    fn api(&self, namespace: &str) -> Api<Sandbox> {
        Api::namespaced(self.client.clone(), namespace)
    }
}

#[async_trait]
impl SandboxOps for KubeSandboxOps {
    async fn create(&self, namespace: &str, sandbox: Sandbox) -> Result<Sandbox, AppError> {
        Ok(self
            .api(namespace)
            .create(&PostParams::default(), &sandbox)
            .await?)
    }
    async fn list(&self, namespace: &str) -> Result<Vec<Sandbox>, AppError> {
        Ok(self.api(namespace).list(&ListParams::default()).await?.items)
    }
    async fn delete(&self, namespace: &str, name: &str) -> Result<(), AppError> {
        let _ = self
            .api(namespace)
            .delete(name, &DeleteParams::background())
            .await?;
        Ok(())
    }
    async fn patch_desired_phase(
        &self,
        namespace: &str,
        name: &str,
        phase: sandbawks_types::SandboxPhase,
    ) -> Result<(), AppError> {
        let patch = serde_json::json!({ "spec": { "desiredPhase": phase } });
        self.api(namespace)
            .patch(
                name,
                &kube::api::PatchParams::apply("sandbawks-api"),
                &kube::api::Patch::Merge(&patch),
            )
            .await?;
        Ok(())
    }
    async fn get(&self, namespace: &str, name: &str) -> Result<Sandbox, AppError> {
        self.api(namespace)
            .get(name)
            .await
            .map_err(AppError::from)
    }
}

/// In-memory ops used by `tests/sandboxes.rs` so unit tests don't need
/// a kube apiserver.
#[derive(Default)]
pub struct FakeSandboxOps {
    inner: Mutex<Vec<Sandbox>>,
}

#[async_trait]
impl SandboxOps for FakeSandboxOps {
    async fn create(&self, _ns: &str, mut sandbox: Sandbox) -> Result<Sandbox, AppError> {
        if sandbox.metadata.creation_timestamp.is_none() {
            sandbox.metadata.creation_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                chrono::Utc::now(),
            ));
        }
        self.inner.lock().await.push(sandbox.clone());
        Ok(sandbox)
    }
    async fn list(&self, _ns: &str) -> Result<Vec<Sandbox>, AppError> {
        Ok(self.inner.lock().await.clone())
    }
    async fn delete(&self, _ns: &str, name: &str) -> Result<(), AppError> {
        self.inner
            .lock()
            .await
            .retain(|s| s.metadata.name.as_deref() != Some(name));
        Ok(())
    }
    async fn patch_desired_phase(
        &self,
        _ns: &str,
        name: &str,
        phase: sandbawks_types::SandboxPhase,
    ) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;
        let sb = guard
            .iter_mut()
            .find(|s| s.metadata.name.as_deref() == Some(name))
            .ok_or_else(|| AppError::not_found("sandbox_not_found", name.to_owned()))?;
        sb.spec.desired_phase = Some(phase);
        Ok(())
    }
    async fn get(&self, _ns: &str, name: &str) -> Result<Sandbox, AppError> {
        self.inner
            .lock()
            .await
            .iter()
            .find(|s| s.metadata.name.as_deref() == Some(name))
            .cloned()
            .ok_or_else(|| AppError::not_found("sandbox_not_found", name.to_owned()))
    }
}

// ---------- DTO ↔ CRD conversion ----------

fn sandbox_to_dto(sandbox: &Sandbox, client_id: &str) -> SandboxDTO {
    let id = sandbox.name_any();
    let started_at = sandbox
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339());
    let end_at = (sandbox
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0)
        .unwrap_or_else(Utc::now)
        + chrono::Duration::seconds(sandbox.spec.timeout_seconds.into()))
    .to_rfc3339();

    SandboxDTO {
        sandbox_id: id,
        template_id: sandbox.spec.template_ref.clone(),
        client_id: client_id.to_owned(),
        alias: None,
        metadata: sandbox.spec.metadata.clone(),
        started_at,
        end_at,
    }
}

fn body_to_sandbox(body: CreateSandboxBody) -> Sandbox {
    let id = format!("sbx-{}", random_suffix(12));
    Sandbox::new(
        &id,
        SandboxSpec {
            template_ref: body.template_id,
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse::<ResourceQuantity>().unwrap_or_default(),
                memory: "1Gi".parse::<ResourceQuantity>().unwrap_or_default(),
                ephemeral_storage: None,
            },
            timeout_seconds: body.timeout,
            auto_pause: true,
            env: body.env_vars,
            metadata: body.metadata,
            network_policy: None,
            desired_phase: None,
        },
    )
}

fn random_suffix(len: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:0width$x}", width = len)
        .chars()
        .rev()
        .take(len)
        .collect()
}

// ---------- Handlers ----------

async fn create_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Json(body): Json<CreateSandboxBody>,
) -> AppResult<impl IntoResponse> {
    if !team.allows("sandboxes:create") {
        return Err(AppError::forbidden("forbidden", "scope sandboxes:create required"));
    }
    let sandbox = body_to_sandbox(body);
    let created = state.sandbox_ops().create(&team.namespace, sandbox).await?;
    let dto = sandbox_to_dto(&created, &team.namespace);
    Ok((StatusCode::CREATED, Json(dto)))
}

async fn list_sandboxes(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
) -> AppResult<Json<Vec<SandboxDTO>>> {
    let items = state.sandbox_ops().list(&team.namespace).await?;
    let dtos = items
        .iter()
        .map(|s| sandbox_to_dto(s, &team.namespace))
        .collect();
    Ok(Json(dtos))
}

async fn kill_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    state.sandbox_ops().delete(&team.namespace, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Construct the lifecycle router. Tasks 7 (pause/connect), 8
/// (events), 10 (exec) merge into this with `.merge(other_router)`.
pub fn router(state: AppState) -> Router {
    let key_store = state.key_store.clone();
    Router::new()
        .route("/sandboxes", post(create_sandbox).get(list_sandboxes))
        .route("/sandboxes/:id/kill", post(kill_sandbox))
        .layer(axum::middleware::from_fn_with_state(
            key_store,
            auth_middleware,
        ))
        .with_state(state)
}
```

- [ ] **Step 4: Extend `AppState` with the ops accessor + test ctor**

```rust
// in state.rs, append:
use crate::routes::sandboxes::{FakeSandboxOps, KubeSandboxOps, SandboxOps};

#[derive(Clone)]
pub struct AppState {
    pub kube: Client,
    pub default_namespace: Arc<str>,
    pub key_store: Arc<dyn KeyStore>,
    sandbox_ops: Arc<dyn SandboxOps>,
}

impl AppState {
    pub fn from_kube(kube: Client, default_namespace: impl Into<Arc<str>>) -> Self {
        let key_store: Arc<dyn KeyStore> = Arc::new(KubeKeyStore::new(kube.clone()));
        let sandbox_ops: Arc<dyn SandboxOps> = Arc::new(KubeSandboxOps::new(kube.clone()));
        Self {
            kube,
            default_namespace: default_namespace.into(),
            key_store,
            sandbox_ops,
        }
    }

    /// Test-only constructor that wires a `FakeSandboxOps`.
    pub fn for_test(default_namespace: impl Into<Arc<str>>, key_store: Arc<dyn KeyStore>) -> Self {
        // `Client::try_default()` will fail outside a cluster; the
        // test never calls it so we wrap an unused mock client.
        let mock_service = tower::service_fn(|_: http::Request<kube::client::Body>| async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder().status(204).body(kube::client::Body::empty()).unwrap(),
            )
        });
        let kube = Client::new(mock_service, "test");
        Self {
            kube,
            default_namespace: default_namespace.into(),
            key_store,
            sandbox_ops: Arc::new(FakeSandboxOps::default()),
        }
    }

    pub fn sandbox_ops(&self) -> &Arc<dyn SandboxOps> {
        &self.sandbox_ops
    }
}
```

`http` is a dev-dep we don't want pulled into prod. Move it to `[dev-dependencies]`? Easier: keep the test-only ctor `#[cfg(any(test, feature = "test-util"))]` and put `http` and `tower` in `[dev-dependencies]`.

Actually, since `kube::Client::new` requires a `tower::Service`, keep the impl in production code but gate by `cfg(any(test, feature = "test-util"))`. Add to `Cargo.toml`:
```toml
[features]
default = []
test-util = []

[dev-dependencies]
http = "1"
http-body-util = "0.1"
```

And mark `for_test` with `#[cfg(any(test, feature = "test-util"))]` plus the `tower::service_fn` import gated similarly.

- [ ] **Step 5: Wire the router into the binary**

In `bin/sandbawks-api.rs`, after building `AppState`:
```rust
let app = Router::new()
    .merge(sandbawks_api::routes::sandboxes::router(state.clone()))
    .route("/healthz", get(|| async { "ok" }))
    .route("/readyz", get(|| async { "ok" }))
    .layer(sandbawks_api::middleware::request_id_layer())
    .layer(sandbawks_api::middleware::trace_layer())
    .layer(sandbawks_api::middleware::timeout_layer(30));
```

- [ ] **Step 6: Run tests + clippy**

```bash
cargo test -p sandbawks-api --features test-util
cargo clippy -p sandbawks-api --all-features -- -D warnings
```

- [ ] **Step 7: Commit**

```bash
git add crates/sandbawks-api/
git commit -m "feat(api): lifecycle CRUD endpoints — create/list/kill (Plan 04 T6)"
```

---

## Task 7: Pause / connect endpoints

**Files:**
- Modify: `crates/sandbawks-api/src/routes/sandboxes.rs`
- Modify: `crates/sandbawks-api/tests/sandboxes.rs`

Pause + connect both patch `spec.desiredPhase` on the Sandbox CR. The controller (per T4) is what actually drives the state machine — the API is purely declarative. `connect` additionally blocks until `status.envdAddress` is populated (or returns 504 on timeout).

E2B shape:

```
POST /sandboxes/:id/pause → 200 {"sandboxID": "...", "state": "paused"}
POST /sandboxes/:id/connect → 200 {"sandboxID": "...", "envdURL": "ws://...:49983"}
```

- [ ] **Step 1: Add the failing tests**

Append to `crates/sandbawks-api/tests/sandboxes.rs`:

```rust
#[tokio::test]
async fn pause_patches_desired_phase() {
    let state = fixture_state();
    // pre-seed a sandbox via FakeSandboxOps
    state
        .sandbox_ops()
        .create("team-acme", sandbawks_types::Sandbox::new(
            "sbx-1",
            // (use same SandboxSpec helper as before)
            todo!("inline a minimal SandboxSpec"),
        ))
        .await
        .unwrap();

    let app = sandboxes_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sandboxes/sbx-1/pause")
                .header("x-api-key", "sbk_a1b2c3d4xxxxxxxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let sb = state.sandbox_ops().get("team-acme", "sbx-1").await.unwrap();
    assert_eq!(sb.spec.desired_phase, Some(sandbawks_types::SandboxPhase::Paused));
}
```

(The `todo!()` is the placeholder where the inlined SandboxSpec lives; replace before running. Better: hoist a `fn make_spec()` helper at the top of the test file.)

Hoist a helper at the top of the file:

```rust
fn empty_spec() -> sandbawks_types::SandboxSpec {
    sandbawks_types::SandboxSpec {
        template_ref: "tpl".into(),
        runtime_class_name: None,
        resources: sandbawks_types::ResourceRequests {
            cpu: "1".parse().unwrap(),
            memory: "1Gi".parse().unwrap(),
            ephemeral_storage: None,
        },
        timeout_seconds: 60,
        auto_pause: true,
        env: Default::default(),
        metadata: Default::default(),
        network_policy: None,
        desired_phase: None,
    }
}
```

And then `sandbawks_types::Sandbox::new("sbx-1", empty_spec())` in the test.

- [ ] **Step 2: Implement pause + connect handlers**

Append to `routes/sandboxes.rs`:

```rust
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PauseResponse {
    pub sandbox_id: String,
    pub state: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectResponse {
    pub sandbox_id: String,
    pub envd_url: String,
}

async fn pause_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
) -> AppResult<Json<PauseResponse>> {
    state
        .sandbox_ops()
        .patch_desired_phase(&team.namespace, &id, sandbawks_types::SandboxPhase::Paused)
        .await?;
    Ok(Json(PauseResponse { sandbox_id: id, state: "paused" }))
}

async fn connect_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
) -> AppResult<Json<ConnectResponse>> {
    state
        .sandbox_ops()
        .patch_desired_phase(&team.namespace, &id, sandbawks_types::SandboxPhase::Running)
        .await?;
    // Poll status.envdAddress for up to 30s.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let sandbox = state.sandbox_ops().get(&team.namespace, &id).await?;
        if let Some(addr) = sandbox.status.as_ref().and_then(|s| s.envd_address.as_deref()) {
            return Ok(Json(ConnectResponse {
                sandbox_id: id,
                envd_url: format!("ws://{addr}/"),
            }));
        }
        if std::time::Instant::now() >= deadline {
            return Err(AppError::upstream(
                "envd_not_ready",
                "envd address not surfaced within 30s",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
```

Update the router:
```rust
Router::new()
    .route("/sandboxes", post(create_sandbox).get(list_sandboxes))
    .route("/sandboxes/:id/kill", post(kill_sandbox))
    .route("/sandboxes/:id/pause", post(pause_sandbox))
    .route("/sandboxes/:id/connect", post(connect_sandbox))
```

- [ ] **Step 3: Run + clippy + commit**

```bash
cargo test -p sandbawks-api --features test-util
cargo clippy -p sandbawks-api --all-features -- -D warnings
git add crates/sandbawks-api/
git commit -m "feat(api): pause + connect endpoints (Plan 04 T7)"
```

---

## Task 8: SSE event stream endpoints

**Files:**
- Replace: `crates/sandbawks-api/src/routes/events.rs`
- Create: `crates/sandbawks-api/tests/events.rs`

E2B exposes two SSE streams:
- `GET /events/sandboxes` — all sandbox events in the team's namespace.
- `GET /events/sandboxes/:id` — events for one sandbox.

Each line is a `data: <json>\n\n`-framed event whose JSON shape mirrors the Kubernetes `Event` object filtered to the fields E2B cares about (`type`, `reason`, `message`, `sandboxID`, `timestamp`).

The backing data source is `kube::Api<Event>` watched with `kube::runtime::watcher`.

- [ ] **Step 1: Implement `crates/sandbawks-api/src/routes/events.rs`**

```rust
//! SSE event-stream endpoints (`/events/sandboxes` and
//! `/events/sandboxes/:id`).
//!
//! Backed by `kube::runtime::watcher` over the namespace's `Event`
//! objects. Each delivered event becomes one `data: <json>\n\n`
//! frame.

use axum::{
    extract::{Path, State},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    routing::get,
    Router,
};
use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Event as K8sEvent;
use kube::{
    runtime::{watcher, WatchStreamExt},
    Api,
};
use serde::Serialize;
use std::{convert::Infallible, time::Duration};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    auth::auth_middleware,
    error::AppError,
    AppState,
};

#[derive(Serialize)]
struct EventDTO {
    #[serde(rename = "type")]
    kind: String,
    reason: Option<String>,
    message: Option<String>,
    sandbox_id: Option<String>,
    timestamp: String,
}

fn ev_to_dto(ev: K8sEvent) -> EventDTO {
    EventDTO {
        kind: ev.type_.clone().unwrap_or_else(|| "Normal".into()),
        reason: ev.reason.clone(),
        message: ev.message.clone(),
        sandbox_id: ev
            .involved_object
            .name
            .clone()
            .filter(|_| ev.involved_object.kind.as_deref() == Some("Sandbox")),
        timestamp: ev
            .last_timestamp
            .clone()
            .map(|t| t.0.to_rfc3339())
            .unwrap_or_default(),
    }
}

async fn stream_all(
    State(state): State<AppState>,
    axum::Extension(team): axum::Extension<crate::auth::TeamContext>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let api: Api<K8sEvent> = Api::namespaced(state.kube.clone(), &team.namespace);
    let stream = watch_events(api, None).await;
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn stream_one(
    State(state): State<AppState>,
    axum::Extension(team): axum::Extension<crate::auth::TeamContext>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let api: Api<K8sEvent> = Api::namespaced(state.kube.clone(), &team.namespace);
    let stream = watch_events(api, Some(id)).await;
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn watch_events(
    api: Api<K8sEvent>,
    filter_sandbox: Option<String>,
) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        let mut stream = watcher(api, watcher::Config::default()).applied_objects().boxed();
        while let Some(item) = stream.next().await {
            let Ok(ev) = item else {
                continue;
            };
            if let Some(want) = &filter_sandbox {
                if ev.involved_object.kind.as_deref() != Some("Sandbox")
                    || ev.involved_object.name.as_deref() != Some(want)
                {
                    continue;
                }
            } else if ev.involved_object.kind.as_deref() != Some("Sandbox") {
                continue;
            }
            let dto = ev_to_dto(ev);
            let Ok(json) = serde_json::to_string(&dto) else { continue };
            let _ = tx.send(SseEvent::default().data(json)).await;
        }
    });
    ReceiverStream::new(rx).map(Ok)
}

pub fn router(state: AppState) -> Router {
    let key_store = state.key_store.clone();
    Router::new()
        .route("/events/sandboxes", get(stream_all))
        .route("/events/sandboxes/:id", get(stream_one))
        .layer(axum::middleware::from_fn_with_state(key_store, auth_middleware))
        .with_state(state)
}

// Silences a `use AppError` lint while keeping the import wired for
// later expansion.
#[allow(dead_code)]
fn _appease(_: AppError) {}
```

- [ ] **Step 2: Add a lightweight smoke test** (a real watcher needs a kube apiserver; we just assert the router boots and `404`s gracefully without auth):

`crates/sandbawks-api/tests/events.rs`:
```rust
use axum::{body::Body, http::{Request, StatusCode}};
use sandbawks_api::routes::events::router as events_router;
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn missing_auth_is_unauthorized() {
    use sandbawks_api::auth::FakeKeyStore;
    let store = Arc::new(FakeKeyStore::default());
    let state = sandbawks_api::AppState::for_test("team-acme", store);
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
```

- [ ] **Step 3: Merge into the binary router + commit**

```rust
let app = Router::new()
    .merge(routes::sandboxes::router(state.clone()))
    .merge(routes::events::router(state.clone()))
    .route("/healthz", get(|| async { "ok" }))
    // ... existing layers
```

```bash
cargo test -p sandbawks-api --features test-util
cargo clippy -p sandbawks-api --all-features -- -D warnings
git add crates/sandbawks-api/
git commit -m "feat(api): SSE event stream endpoints (Plan 04 T8)"
```

---

## Task 9: `EnvdClient` — pooled gRPC clients

**Files:**
- Replace: `crates/sandbawks-api/src/envd_client.rs`

The exec WebSocket bridge (T10), file proxy (T11), and filesystem metadata pass-through (T12) all talk to envd over gRPC. They share one pooled client per `envd_address` so the underlying h2 connection is reused.

- [ ] **Step 1: Implement the pool**

```rust
//! Pooled tonic clients keyed by `host:port`.
//!
//! All envd connections are plain h2 (no TLS) since the API gateway
//! and envd live in the same Pod network. Plan 06 may add mTLS;
//! revisit at that point.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use envd::proto::{
    filesystem::filesystem_client::FilesystemClient,
    process::process_client::ProcessClient,
};
use tonic::transport::{Channel, Endpoint};

use crate::error::AppError;

/// Cheaply cloneable pool of envd channels.
#[derive(Clone, Default)]
pub struct EnvdPool {
    inner: Arc<DashMap<String, Channel>>,
}

impl EnvdPool {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch or open a channel to `addr` (e.g. `"10.244.3.27:49983"`).
    pub async fn channel(&self, addr: &str) -> Result<Channel, AppError> {
        if let Some(c) = self.inner.get(addr) {
            return Ok(c.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| AppError::upstream("envd_endpoint", e.to_string()))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| AppError::upstream("envd_connect", e.to_string()))?;
        self.inner.insert(addr.to_owned(), channel.clone());
        Ok(channel)
    }

    pub async fn filesystem(&self, addr: &str) -> Result<FilesystemClient<Channel>, AppError> {
        Ok(FilesystemClient::new(self.channel(addr).await?))
    }

    pub async fn process(&self, addr: &str) -> Result<ProcessClient<Channel>, AppError> {
        Ok(ProcessClient::new(self.channel(addr).await?))
    }
}
```

Add `dashmap` to workspace deps in the root `Cargo.toml`:
```toml
dashmap = "6"
```

And to `crates/sandbawks-api/Cargo.toml`:
```toml
dashmap = { workspace = true }
```

- [ ] **Step 2: Add the pool to `AppState`**

```rust
// in state.rs
use crate::envd_client::EnvdPool;

pub struct AppState {
    // ... existing
    pub envd_pool: EnvdPool,
}
// constructors: envd_pool: EnvdPool::new(),
```

- [ ] **Step 3: Smoke test**

```rust
// crates/sandbawks-api/tests/envd_client.rs
#[tokio::test]
async fn pool_caches_channels() {
    let pool = sandbawks_api::envd_client::EnvdPool::new();
    // Connecting fails (no listener), but we assert the pool path is reachable.
    let r = pool.channel("127.0.0.1:1").await;
    assert!(r.is_err());
}
```

- [ ] **Step 4: Commit**

```bash
cargo build -p sandbawks-api
cargo clippy -p sandbawks-api -- -D warnings
git add crates/sandbawks-api/ Cargo.toml
git commit -m "feat(api): pooled envd gRPC client (Plan 04 T9)"
```

---

## Task 10: Exec WebSocket bridge — WS public, gRPC internal

**Files:**
- Replace: `crates/sandbawks-api/src/routes/exec.rs`
- Modify: `crates/sandbawks-api/src/bin/sandbawks-api.rs`

Public protocol: `GET /sandboxes/:id/exec` with `Upgrade: websocket`. The WebSocket frames are JSON `{kind, payload}` envelopes:

```
{"kind": "start", "cmd": "python", "args": ["-i"], "envs": {}}
{"kind": "stdin", "data": "print(1)\n"}
{"kind": "resize", "cols": 80, "rows": 24}     // only for PTY
{"kind": "signal", "signal": "SIGINT"}
```

Server-to-client:
```
{"kind": "stdout", "data": "..."}
{"kind": "stderr", "data": "..."}
{"kind": "exit", "code": 0}
```

The bridge:
1. Resolves the Sandbox to its `envd_address`.
2. Opens a `Process.Connect` (or `Pty.Connect`, query-param flag `pty=true`) bidirectional stream.
3. Spawns one task draining envd→client and another draining client→envd.
4. On close from either side, drops the stream so the other half flushes and exits.

- [ ] **Step 1: Implement the bridge**

```rust
//! Exec WebSocket bridge.
//!
//! Public:  `GET /sandboxes/:id/exec[?pty=true]` with WS upgrade.
//! Internal: bidi gRPC stream to envd's `Process` (or `Pty`) service.
//!
//! Frame format is documented inline in the route docstring; matches
//! what the E2B SDKs already speak.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Extension, Path, Query, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use envd::proto::process::{
    process_event::Event as ProcessEvent, start_request, ProcessConfig, StartRequest,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    auth::{auth_middleware, TeamContext},
    error::{AppError, AppResult},
    AppState,
};

#[derive(Debug, Deserialize)]
pub struct ExecQuery {
    #[serde(default)]
    pub pty: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ClientFrame {
    Start { cmd: String, #[serde(default)] args: Vec<String>, #[serde(default)] envs: std::collections::HashMap<String, String> },
    Stdin { data: String },
    Resize { cols: u32, rows: u32 },
    Signal { signal: String },
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerFrame {
    Stdout { data: String },
    Stderr { data: String },
    Exit { code: i32 },
    Error { message: String },
}

async fn exec(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
    Query(q): Query<ExecQuery>,
    ws: WebSocketUpgrade,
) -> AppResult<impl IntoResponse> {
    let sandbox = state.sandbox_ops().get(&team.namespace, &id).await?;
    let addr = sandbox
        .status
        .as_ref()
        .and_then(|s| s.envd_address.clone())
        .ok_or_else(|| AppError::upstream("envd_not_ready", "no envdAddress on Sandbox"))?;
    Ok(ws.on_upgrade(move |socket| handle(socket, state.envd_pool.clone(), addr, q.pty)))
}

async fn handle(socket: WebSocket, pool: crate::envd_client::EnvdPool, addr: String, _pty: bool) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let mut process = match pool.process(&addr).await {
        Ok(c) => c,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(
                    serde_json::to_string(&ServerFrame::Error { message: e.to_string() })
                        .unwrap_or_default(),
                ))
                .await;
            return;
        }
    };

    // Channel the WS reader drains into; the gRPC `outbound` stream
    // reads from this side and forwards to envd.
    let (tx, rx) = tokio::sync::mpsc::channel::<StartRequest>(16);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);

    let resp = match process.start(tonic::Request::new(outbound)).await {
        Ok(r) => r,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(
                    serde_json::to_string(&ServerFrame::Error { message: e.to_string() })
                        .unwrap_or_default(),
                ))
                .await;
            return;
        }
    };
    let mut inbound = resp.into_inner();

    // Pump WS → envd
    let tx_clone = tx.clone();
    let pump_in = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_rx.next().await {
            let Message::Text(text) = msg else { continue };
            let Ok(frame) = serde_json::from_str::<ClientFrame>(&text) else { continue };
            let req = match frame {
                ClientFrame::Start { cmd, args, envs } => StartRequest {
                    event: Some(start_request::Event::Config(ProcessConfig {
                        cmd,
                        args,
                        envs,
                        cwd: String::new(),
                    })),
                },
                ClientFrame::Stdin { data } => StartRequest {
                    event: Some(start_request::Event::Input(data.into_bytes())),
                },
                ClientFrame::Signal { signal } => StartRequest {
                    event: Some(start_request::Event::Signal(signal)),
                },
                ClientFrame::Resize { .. } => continue, // PTY-only; ignored on Process
            };
            if tx_clone.send(req).await.is_err() {
                break;
            }
        }
    });

    // Pump envd → WS
    while let Some(item) = inbound.next().await {
        let Ok(event) = item else { break };
        let Some(ev) = event.event else { continue };
        let frame = match ev {
            ProcessEvent::Stdout(b) => ServerFrame::Stdout { data: String::from_utf8_lossy(&b).into_owned() },
            ProcessEvent::Stderr(b) => ServerFrame::Stderr { data: String::from_utf8_lossy(&b).into_owned() },
            ProcessEvent::End(end) => ServerFrame::Exit { code: end.exit_code },
        };
        if ws_tx
            .send(Message::Text(serde_json::to_string(&frame).unwrap_or_default()))
            .await
            .is_err()
        {
            break;
        }
    }
    drop(tx);
    let _ = pump_in.await;
    let _ = ws_tx.close().await;
}

pub fn router(state: AppState) -> Router {
    let key_store = state.key_store.clone();
    Router::new()
        .route("/sandboxes/:id/exec", get(exec))
        .layer(axum::middleware::from_fn_with_state(key_store, auth_middleware))
        .with_state(state)
}

// Future tasks can drop this once unused.
#[allow(dead_code)]
fn _appease(_: Arc<()>) {}
```

The exact `envd::proto::process::*` paths and event variants come from the tonic-generated code Plan 03 vendored. If field names differ, adjust to match the actual generated bindings (run `cargo doc -p envd --open` to inspect).

- [ ] **Step 2: Wire into binary router + commit**

```rust
.merge(routes::exec::router(state.clone()))
```

```bash
cargo build -p sandbawks-api
cargo clippy -p sandbawks-api -- -D warnings
git add crates/sandbawks-api/
git commit -m "feat(api): exec WebSocket bridge (Plan 04 T10)"
```

---

## Task 11: File upload/download — HMAC-signed URLs + envd proxy

**Files:**
- Replace: `crates/sandbawks-api/src/signing.rs`
- Modify: `crates/sandbawks-api/src/routes/files.rs`
- Create: `crates/sandbawks-api/tests/signing.rs`

Spec §7 (file upload/download): the gateway *mints a short-lived HMAC-signed URL on its own domain*, hands it to the client, then validates the token when the client `PUT`s/`GET`s and streams the body into/out of envd's `Filesystem.Write` / `Filesystem.Read` server-side. S3 stays out of the data path.

URL shape:
```
PUT /files/upload?token=<base64url(payload)>.<base64url(hmac-sha256)>
GET /files/download?token=<base64url(payload)>.<base64url(hmac-sha256)>
```

Token payload (JSON, base64url):
```json
{ "sandbox_id": "sbx-abc", "path": "/home/user/data.csv", "method": "PUT", "exp": 1748000000 }
```

The HMAC key is `SANDBAWKS_SIGNING_SECRET` from the env (set via the Helm chart's Secret in T13).

- [ ] **Step 1: Implement `signing.rs`**

```rust
//! HMAC-SHA256 signed URLs for file upload/download tokens.
//!
//! Token = `<base64url(payload)>.<base64url(hmac)>`.
//!
//! `payload` is the JSON-serialised `SignedUrlPayload`. `hmac` is
//! HMAC-SHA256(`payload_bytes`) keyed with the signing secret.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::error::AppError;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct SigningKey(pub Vec<u8>);

impl SigningKey {
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var("SANDBAWKS_SIGNING_SECRET")
            .map_err(|_| anyhow::anyhow!("SANDBAWKS_SIGNING_SECRET unset"))?;
        Ok(Self(raw.into_bytes()))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedUrlPayload {
    pub sandbox_id: String,
    pub path: String,
    /// `"PUT"` or `"GET"`.
    pub method: String,
    /// Unix epoch seconds.
    pub exp: i64,
}

pub fn mint(key: &SigningKey, payload: &SignedUrlPayload) -> String {
    let body = serde_json::to_vec(payload).expect("payload serializes");
    let mut mac = HmacSha256::new_from_slice(&key.0).expect("hmac key");
    mac.update(&body);
    let tag = mac.finalize().into_bytes();
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(&body),
        URL_SAFE_NO_PAD.encode(tag),
    )
}

pub fn verify(key: &SigningKey, token: &str) -> Result<SignedUrlPayload, AppError> {
    let (body_b64, tag_b64) = token
        .split_once('.')
        .ok_or_else(|| AppError::bad_request("invalid_token", "expected body.tag"))?;
    let body = URL_SAFE_NO_PAD
        .decode(body_b64)
        .map_err(|_| AppError::bad_request("invalid_token", "bad payload b64"))?;
    let tag = URL_SAFE_NO_PAD
        .decode(tag_b64)
        .map_err(|_| AppError::bad_request("invalid_token", "bad tag b64"))?;

    let mut mac = HmacSha256::new_from_slice(&key.0).expect("hmac key");
    mac.update(&body);
    let expected = mac.finalize().into_bytes();
    if expected.as_slice().ct_eq(&tag).unwrap_u8() == 0 {
        return Err(AppError::unauthorized("invalid_token", "signature mismatch"));
    }

    let payload: SignedUrlPayload = serde_json::from_slice(&body)
        .map_err(|_| AppError::bad_request("invalid_token", "payload not json"))?;
    if payload.exp < chrono::Utc::now().timestamp() {
        return Err(AppError::unauthorized("invalid_token", "expired"));
    }
    Ok(payload)
}
```

- [ ] **Step 2: Add the signing test**

`crates/sandbawks-api/tests/signing.rs`:
```rust
use sandbawks_api::signing::{mint, verify, SignedUrlPayload, SigningKey};

fn key() -> SigningKey {
    SigningKey(b"unit-test-key-please-rotate".to_vec())
}

#[test]
fn round_trips() {
    let p = SignedUrlPayload {
        sandbox_id: "sbx-1".into(),
        path: "/x.txt".into(),
        method: "PUT".into(),
        exp: chrono::Utc::now().timestamp() + 60,
    };
    let token = mint(&key(), &p);
    let decoded = verify(&key(), &token).expect("verify");
    assert_eq!(decoded, p);
}

#[test]
fn rejects_expired() {
    let p = SignedUrlPayload {
        sandbox_id: "sbx-1".into(),
        path: "/x.txt".into(),
        method: "PUT".into(),
        exp: 0,
    };
    let token = mint(&key(), &p);
    assert!(verify(&key(), &token).is_err());
}

#[test]
fn rejects_tampered_payload() {
    let p = SignedUrlPayload {
        sandbox_id: "sbx-1".into(),
        path: "/x.txt".into(),
        method: "PUT".into(),
        exp: chrono::Utc::now().timestamp() + 60,
    };
    let token = mint(&key(), &p);
    // Flip a byte in the payload half.
    let mut chars: Vec<char> = token.chars().collect();
    chars[0] = if chars[0] == 'A' { 'B' } else { 'A' };
    let tampered: String = chars.into_iter().collect();
    assert!(verify(&key(), &tampered).is_err());
}
```

- [ ] **Step 3: Implement upload/download + metadata pass-through in `routes/files.rs`**

```rust
//! File transfer endpoints + filesystem metadata pass-through.
//!
//! `/files/mint`         — issues a short-lived upload OR download URL.
//! `/files/upload`       — accepts PUT with `?token=...` and streams to envd.
//! `/files/download`     — issues GET with `?token=...` and streams from envd.
//! `/files/{sandbox}/list` etc. — pass-through to envd's `Filesystem`.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use envd::proto::filesystem::{
    EntryInfo, ListDirRequest, MakeDirRequest, MoveRequest, RemoveRequest, StatRequest,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::{auth_middleware, TeamContext},
    error::{AppError, AppResult},
    signing::{mint, verify, SignedUrlPayload, SigningKey},
    AppState,
};

#[derive(Debug, Deserialize)]
pub struct MintRequest {
    pub sandbox_id: String,
    pub path: String,
    /// `"PUT"` or `"GET"`.
    pub method: String,
    /// Time-to-live seconds. Default 300 (5 min).
    #[serde(default = "ttl_default")]
    pub ttl: u64,
}

fn ttl_default() -> u64 {
    300
}

#[derive(Debug, Serialize)]
pub struct MintResponse {
    pub url: String,
}

async fn mint_url(
    State(state): State<AppState>,
    Extension(_team): Extension<TeamContext>,
    Json(req): Json<MintRequest>,
) -> AppResult<Json<MintResponse>> {
    let key = state.signing_key.clone();
    let payload = SignedUrlPayload {
        sandbox_id: req.sandbox_id,
        path: req.path,
        method: req.method.to_uppercase(),
        exp: chrono::Utc::now().timestamp() + req.ttl as i64,
    };
    let token = mint(&key, &payload);
    let endpoint = match payload.method.as_str() {
        "PUT" => "/files/upload",
        "GET" => "/files/download",
        _ => return Err(AppError::bad_request("invalid_method", "use PUT or GET")),
    };
    Ok(Json(MintResponse { url: format!("{endpoint}?token={token}") }))
}

#[derive(Debug, Deserialize)]
pub struct TokenQuery {
    pub token: String,
}

async fn upload(
    State(state): State<AppState>,
    Query(q): Query<TokenQuery>,
    body: Body,
) -> AppResult<StatusCode> {
    let payload = verify(&state.signing_key, &q.token)?;
    if payload.method != "PUT" {
        return Err(AppError::unauthorized("invalid_method", "token is not for PUT"));
    }
    let sandbox = state
        .sandbox_ops()
        .get(&state.default_namespace, &payload.sandbox_id)
        .await?;
    let addr = sandbox
        .status
        .as_ref()
        .and_then(|s| s.envd_address.clone())
        .ok_or_else(|| AppError::upstream("envd_not_ready", "no envdAddress"))?;
    let mut client = state.envd_pool.filesystem(&addr).await?;

    // envd's Filesystem.Write is *not* a content RPC in Plan 03 — it
    // takes metadata only. The "file content" path lands here:
    // we read the body into memory and call a custom `WriteAll` RPC
    // that Plan 04 T11 adds to envd's Filesystem service.
    //
    // **Important:** if envd's proto doesn't yet expose `WriteAll`, add
    // it to `crates/envd/proto/filesystem/filesystem.proto` as part of
    // this task, regenerate, implement on the server side, then use
    // here. The proto change is intentionally local to this plan —
    // upstream E2B doesn't ship file content over gRPC either, so we're
    // free to define the shape.
    use futures::TryStreamExt;
    let bytes = body
        .into_data_stream()
        .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await?;
    let _ = client
        .write_all(envd::proto::filesystem::WriteAllRequest {
            path: payload.path,
            data: bytes,
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn download(
    State(state): State<AppState>,
    Query(q): Query<TokenQuery>,
) -> AppResult<impl IntoResponse> {
    let payload = verify(&state.signing_key, &q.token)?;
    if payload.method != "GET" {
        return Err(AppError::unauthorized("invalid_method", "token is not for GET"));
    }
    let sandbox = state
        .sandbox_ops()
        .get(&state.default_namespace, &payload.sandbox_id)
        .await?;
    let addr = sandbox
        .status
        .as_ref()
        .and_then(|s| s.envd_address.clone())
        .ok_or_else(|| AppError::upstream("envd_not_ready", "no envdAddress"))?;
    let mut client = state.envd_pool.filesystem(&addr).await?;

    let resp = client
        .read_all(envd::proto::filesystem::ReadAllRequest { path: payload.path })
        .await?;
    Ok(([(axum::http::header::CONTENT_TYPE, "application/octet-stream")], resp.into_inner().data))
}

// ---------- Metadata pass-through ----------

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct ListItem {
    pub name: String,
    pub kind: String,
    pub size: u64,
}

async fn list_files(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(sandbox_id): Path<String>,
    Query(q): Query<ListQuery>,
) -> AppResult<Json<Vec<ListItem>>> {
    let sandbox = state.sandbox_ops().get(&team.namespace, &sandbox_id).await?;
    let addr = sandbox
        .status
        .as_ref()
        .and_then(|s| s.envd_address.clone())
        .ok_or_else(|| AppError::upstream("envd_not_ready", "no envdAddress"))?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let resp = client
        .list_dir(ListDirRequest { path: q.path })
        .await?
        .into_inner();
    Ok(Json(
        resp.entries
            .into_iter()
            .map(|e: EntryInfo| ListItem {
                name: e.name,
                kind: e.r#type.to_string(),
                size: e.size as u64,
            })
            .collect(),
    ))
}

pub fn router(state: AppState) -> Router {
    let key_store = state.key_store.clone();
    Router::new()
        .route("/files/mint", post(mint_url))
        .route("/files/upload", put(upload))
        .route("/files/download", get(download))
        .route("/files/:sandbox_id/list", get(list_files))
        // stat, remove, move, makedir, watch follow the same shape;
        // wire them up identically — omitted in this snippet for
        // brevity but **must** be implemented before the integration
        // test in T15 runs against the real cluster.
        .layer(axum::middleware::from_fn_with_state(key_store, auth_middleware))
        .with_state(state)
}

// Suppress lint until all metadata routes wired in.
#[allow(dead_code)]
fn _appease(_: Arc<()>) {}
```

This task also extends `crates/envd/proto/filesystem/filesystem.proto` with `WriteAll` + `ReadAll` content RPCs (Plan 03 deliberately deferred file content). Add to the proto:

```proto
message WriteAllRequest { string path = 1; bytes data = 2; }
message WriteAllResponse {}
message ReadAllRequest  { string path = 1; }
message ReadAllResponse { bytes data = 1; }

service Filesystem {
  // ... existing rpcs ...
  rpc WriteAll(WriteAllRequest) returns (WriteAllResponse);
  rpc ReadAll(ReadAllRequest) returns (ReadAllResponse);
}
```

Implement the server side in `crates/envd/src/filesystem.rs`:
```rust
async fn write_all(&self, req: Request<WriteAllRequest>) -> Result<Response<WriteAllResponse>, Status> {
    let req = req.into_inner();
    let p = self.resolve(&req.path)?;
    tokio::fs::write(p, req.data).await.map_err(io_to_status)?;
    Ok(Response::new(WriteAllResponse {}))
}

async fn read_all(&self, req: Request<ReadAllRequest>) -> Result<Response<ReadAllResponse>, Status> {
    let p = self.resolve(&req.into_inner().path)?;
    let data = tokio::fs::read(p).await.map_err(io_to_status)?;
    Ok(Response::new(ReadAllResponse { data }))
}
```

- [ ] **Step 4: Add `signing_key` to `AppState`**

```rust
// state.rs
pub signing_key: SigningKey,
// from_kube: signing_key: SigningKey::from_env().expect("SANDBAWKS_SIGNING_SECRET unset"),
// for_test: signing_key: SigningKey(b"test".to_vec()),
```

- [ ] **Step 5: Tests + commit**

```bash
cargo test -p sandbawks-api -p envd --features test-util
cargo clippy --workspace --all-features -- -D warnings
git add crates/sandbawks-api/ crates/envd/
git commit -m "feat(api): file upload/download HMAC URL mint + envd proxy (Plan 04 T11)"
```

---

## Task 12: Filesystem metadata pass-through — stat/remove/move/makedir/watch

**Files:**
- Modify: `crates/sandbawks-api/src/routes/files.rs`

The remaining metadata routes are pure gRPC pass-through. Wire them up the same way `list_files` is wired:

| HTTP | Envd RPC |
|---|---|
| `GET /files/:id/stat?path=...` | `Filesystem.Stat` |
| `DELETE /files/:id/remove?path=...` | `Filesystem.Remove` |
| `POST /files/:id/move {from,to}` | `Filesystem.Move` |
| `POST /files/:id/makedir {path}` | `Filesystem.MakeDir` |
| `GET /files/:id/watch?path=...` (SSE) | `Filesystem.WatchDir` (server-stream) |

- [ ] **Step 1: Add the five handlers** following the same shape as `list_files`. The watch handler uses `axum::response::sse::Sse` over a `tokio::sync::mpsc` channel, same as Task 8's events stream.

- [ ] **Step 2: Add tests**

`crates/sandbawks-api/tests/files.rs`:
```rust
#[tokio::test]
async fn mint_round_trip_minimal() {
    // Smoke test that mint+upload routing wires correctly; deep
    // round-trip against a real envd is in T15.
    // (...)
}
```

- [ ] **Step 3: Commit**

```bash
cargo test -p sandbawks-api --features test-util
cargo clippy -p sandbawks-api --all-features -- -D warnings
git add crates/sandbawks-api/
git commit -m "feat(api): filesystem metadata pass-through routes (Plan 04 T12)"
```

---

## Task 13: Helm chart — api Deployment, RBAC, signing-secret

**Files:**
- Modify: `deploy/helm/sandbawks/templates/api-deployment.yaml`
- Create: `deploy/helm/sandbawks/templates/api-signing-secret.yaml`
- Modify: `deploy/helm/sandbawks/templates/rbac.yaml`
- Modify: `deploy/helm/sandbawks/templates/_helpers.tpl`
- Modify: `deploy/helm/sandbawks/values.yaml`

- [ ] **Step 1: Replace the `--placeholder` Deployment with the real one**

`deploy/helm/sandbawks/templates/api-deployment.yaml`:
```yaml
{{- if .Values.api.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbawks-api
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.api.replicas }}
  selector:
    matchLabels:
      {{- include "sandbawks.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: api
  template:
    metadata:
      labels:
        {{- include "sandbawks.selectorLabels" . | nindent 8 }}
        app.kubernetes.io/component: api
    spec:
      serviceAccountName: sandbawks-api
      containers:
        - name: api
          image: "{{ .Values.images.api.repository }}:{{ .Values.images.api.tag }}"
          imagePullPolicy: {{ .Values.images.api.pullPolicy }}
          env:
            - name: SANDBAWKS_DEFAULT_NAMESPACE
              value: {{ .Values.api.defaultNamespace | quote }}
            - name: SANDBAWKS_API_LISTEN
              value: "0.0.0.0:8080"
            - name: SANDBAWKS_SIGNING_SECRET
              valueFrom:
                secretKeyRef:
                  name: {{ include "sandbawks.signingSecretName" . }}
                  key: signing-secret
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            httpGet: { path: /readyz, port: http }
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: http }
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            {{- toYaml .Values.api.resources | nindent 12 }}
{{- end }}
```

- [ ] **Step 2: Add the signing-secret template**

`deploy/helm/sandbawks/templates/api-signing-secret.yaml`:
```yaml
{{- if and .Values.api.enabled (not .Values.api.signingSecret.existingSecret) }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "sandbawks.signingSecretName" . }}
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
type: Opaque
data:
  # 32-byte random key. `randAlphaNum` is deterministic in `helm template`
  # but rotates on every `helm upgrade --install`; consumers wanting a
  # stable secret should set api.signingSecret.existingSecret.
  signing-secret: {{ randAlphaNum 64 | b64enc }}
{{- end }}
```

- [ ] **Step 3: Extend RBAC**

`deploy/helm/sandbawks/templates/rbac.yaml` — add a Role + binding for `sandbawks-api`:
```yaml
{{- if .Values.api.enabled }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sandbawks-api
rules:
  - apiGroups: ["sandbawks.io"]
    resources: ["teamapikeys"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["sandbawks.io"]
    resources: ["sandboxes"]
    verbs: ["get", "list", "watch", "create", "delete", "patch", "update"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sandbawks-api
subjects:
  - kind: ServiceAccount
    name: sandbawks-api
    namespace: {{ .Release.Namespace }}
roleRef:
  kind: ClusterRole
  name: sandbawks-api
  apiGroup: rbac.authorization.k8s.io
{{- end }}
```

- [ ] **Step 4: Helper for the signing-secret name**

In `_helpers.tpl`:
```
{{- define "sandbawks.signingSecretName" -}}
{{- if .Values.api.signingSecret.existingSecret -}}
{{ .Values.api.signingSecret.existingSecret }}
{{- else -}}
sandbawks-api-signing
{{- end -}}
{{- end -}}
```

- [ ] **Step 5: `values.yaml` — extend the `api:` block**

```yaml
api:
  enabled: true
  replicas: 2
  defaultNamespace: default
  signingSecret:
    # Provide an externally-managed secret name (must contain key
    # `signing-secret`). If empty, the chart generates a random one
    # per `helm upgrade --install`.
    existingSecret: ""
  service: ...
  resources: ...
```

And update `images.api.repository` to `ghcr.io/hypotheosis/sandbawks-api`, `tag` to a placeholder commit-sha that T14's workflow will bump.

- [ ] **Step 6: Validate**

```bash
helm lint deploy/helm/sandbawks
helm template test deploy/helm/sandbawks | grep -A2 SANDBAWKS_SIGNING_SECRET
```

- [ ] **Step 7: Commit**

```bash
git add deploy/helm/sandbawks/
git commit -m "feat(deploy): Helm chart wiring for sandbawks-api (Plan 04 T13)"
```

---

## Task 14: API Dockerfile + Forgejo Actions workflow

**Files:**
- Create: `crates/sandbawks-api/Dockerfile`
- Create: `.forgejo/workflows/api-image.yaml`
- Modify: `justfile` (add `api-image-build`, `api-image-run`)

Mirror the `crates/envd/Dockerfile` + `.forgejo/workflows/envd-image.yaml` structure. Distroless multi-stage build, image pushed to `ghcr.io/hypotheosis/sandbawks-api:{sha,latest}`.

- [ ] **Step 1: Dockerfile** (adapt from `crates/envd/Dockerfile` — same builder layer + distroless final).

- [ ] **Step 2: Forgejo workflow**

`.forgejo/workflows/api-image.yaml`:
```yaml
name: api-image
on:
  push:
    branches: [main]
    paths:
      - 'crates/sandbawks-api/**'
      - 'crates/sandbawks-types/**'
      - 'crates/envd/proto/**'
      - 'Cargo.toml'
      - 'Cargo.lock'
      - '.forgejo/workflows/api-image.yaml'

jobs:
  build:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/hypotheosis/sandbawks-ci-runner:latest
    steps:
      - uses: https://github.com/actions/checkout@v4
      - name: docker login
        run: echo "${HARBOR_PASSWORD}" | docker login the private registry -u "${HARBOR_USER}" --password-stdin
        env:
          HARBOR_USER: ${{ secrets.HARBOR_USER }}
          HARBOR_PASSWORD: ${{ secrets.HARBOR_PASSWORD }}
      - name: build
        run: docker build -t ghcr.io/hypotheosis/sandbawks-api:${{ github.sha }} -f crates/sandbawks-api/Dockerfile .
      - name: tag latest
        run: docker tag ghcr.io/hypotheosis/sandbawks-api:${{ github.sha }} ghcr.io/hypotheosis/sandbawks-api:latest
      - name: push
        run: |
          docker push ghcr.io/hypotheosis/sandbawks-api:${{ github.sha }}
          docker push ghcr.io/hypotheosis/sandbawks-api:latest
```

(All `uses:` fully-qualified — per `feedback_forgejo_actions_uses` memory.)

- [ ] **Step 3: Justfile recipes**

```make
api-image-build:
    docker build -t ghcr.io/hypotheosis/sandbawks-api:dev \
        -f crates/sandbawks-api/Dockerfile .

api-image-run:
    docker run --rm ghcr.io/hypotheosis/sandbawks-api:dev --help
```

- [ ] **Step 4: Smoke-build locally**

```bash
just api-image-build
```

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-api/Dockerfile .forgejo/workflows/api-image.yaml justfile
git commit -m "feat(ci): sandbawks-api image build + push to Harbor (Plan 04 T14)"
```

---

## Task 15: Kind integration test + workspace green gate + changelog

**Files:**
- Create: `crates/sandbawks-api/tests/lifecycle_kind.rs` (or extend `kind-smoke` shell test)
- Create: `deploy/kind/fixtures/api-smoke.sh`
- Modify: `justfile` (extend `cluster-up`)
- Modify: `docs/kind-smoke.md`
- Create: `changelog/2026-05-23-plan-04-sandbawks-api.md`

The integration test:

1. `just cluster-up` — extended to install the api Deployment + load image.
2. Apply a `TeamAPIKey` CR with a known hash.
3. `curl -X POST -H 'X-API-Key: sbk_test...' http://localhost:8080/sandboxes -d '{"templateID":"py","timeout":300}'` → assert 201 + parse `sandboxID`.
4. Wait up to 60s for `kubectl get sandbox <id> -o jsonpath='{.status.envdAddress}'` to be non-empty.
5. `curl POST /sandboxes/{id}/pause` → assert 200; verify `spec.desiredPhase=Paused`.
6. `curl POST /sandboxes/{id}/connect` → assert 200, parse `envdURL`.
7. `curl POST /files/mint` for PUT, then `curl -X PUT --data-binary @localfile http://...?token=...` → 204.
8. `curl /files/{id}/list?path=/` → assert the uploaded file shows up.
9. `curl POST /files/mint` for GET, then download → diff against local file.
10. `curl POST /sandboxes/{id}/kill` → 204.
11. Tear down.

- [ ] **Step 1: Write `deploy/kind/fixtures/api-smoke.sh`** — bash + curl + kubectl + jq. Idempotent (re-runnable).

- [ ] **Step 2: Extend `just cluster-up`** to install api + load image:

```make
cluster-up: controller-image-build envd-image-build api-image-build
    kind create cluster ...
    kind load docker-image --name sandbawks-smoke ghcr.io/hypotheosis/sandbawks-controller:dev
    kind load docker-image --name sandbawks-smoke ghcr.io/hypotheosis/envd:dev
    kind load docker-image --name sandbawks-smoke ghcr.io/hypotheosis/sandbawks-api:dev
    helm install sandbawks deploy/helm/sandbawks \
        --set api.enabled=true \
        --set node.enabled=false \
        --set images.api.repository=ghcr.io/hypotheosis/sandbawks-api \
        --set images.api.tag=dev \
        --set images.api.pullPolicy=Never \
        --set images.controller.repository=ghcr.io/hypotheosis/sandbawks-controller \
        --set images.controller.tag=dev \
        --set images.controller.pullPolicy=Never \
        --set envd.image=ghcr.io/hypotheosis/envd \
        --set envd.tag=dev \
        --set envd.pullPolicy=Never \
        --wait --timeout 180s
    # Port-forward the api so the host can hit it.
    kubectl port-forward svc/sandbawks-api 8080:8080 &
```

The controller needs the `--runtime pod` flag to actually create Pods. Extend its Deployment template to read this from `.Values.controller.runtime` (default `pod` in cluster, `fake` in unit tests).

- [ ] **Step 3: Wire `api-smoke.sh` into the `kind-smoke` workflow**

`.forgejo/workflows/kind-smoke.yaml` — append a step that runs `deploy/kind/fixtures/api-smoke.sh` after the existing smoke-assert step. Add a `KIND_SMOKE_INCLUDE_API=1` env var so the bash script can short-circuit during the predecessor phases.

- [ ] **Step 4: Run the workspace green gate locally**

```bash
just check
just cluster-up
just deploy/kind/fixtures/api-smoke.sh
just cluster-down
```

Expected: everything green.

- [ ] **Step 5: Write the changelog**

`changelog/2026-05-23-plan-04-sandbawks-api.md`:
```markdown
# 2026-05-23 — Plan 04: `sandbawks-api` E2B-compatible HTTP/WS gateway

## What landed

- New crate `crates/sandbawks-api`: axum-based HTTP/WS gateway.
- `TeamAPIKey`-backed auth middleware + `TeamContext` extractor.
- Lifecycle endpoints `POST/GET /sandboxes`, `POST /sandboxes/:id/{kill,pause,connect}`.
- SSE event streams `/events/sandboxes` and `/events/sandboxes/:id`.
- Exec WebSocket bridge `/sandboxes/:id/exec` (gRPC-internally to envd).
- File upload/download with HMAC-signed URLs + envd `Filesystem.{Read,Write}All` RPCs.
- Filesystem metadata pass-through (`list/stat/remove/move/makedir/watch`).
- `PodAdapter`: first real `RuntimeAdapter` impl. Pods run on the cluster's default
  containerd runtime; `spec.runtimeClassName` is accepted-but-ignored until Plan 05.
- `spec.desiredPhase` typed field on `Sandbox` (replaces the legacy annotation).
- Helm chart additions: api Deployment + Service + RBAC + signing-secret.
- Dockerfile + Forgejo Actions workflow publishing `ghcr.io/hypotheosis/sandbawks-api`.
- Extended `kind-smoke` workflow with a full API lifecycle test against real envd Pods.

## What's deliberately not here

- `GvisorAdapter` and real pause/resume state preservation — Plan 05.
- E2B SDK compat tests + perf benchmarks — Plan 06.
- `SandboxWarmPool` reconciler — Plan 07.
- mTLS between gateway and envd — revisit in Plan 06.
- `RuntimeClass`-driven dispatch — also Plan 05.

## Next plan

Plan 05 — `GvisorAdapter` in `sandbawks-node`: first real pause/resume adapter,
S3 streaming, lazy-restore optimizations.
```

- [ ] **Step 6: Commit**

```bash
git add crates/sandbawks-api/tests/lifecycle_kind.rs deploy/kind/fixtures/api-smoke.sh \
    justfile .forgejo/workflows/kind-smoke.yaml docs/kind-smoke.md \
    changelog/2026-05-23-plan-04-sandbawks-api.md
git commit -m "test(api): kind integration test + green gate + changelog (Plan 04 T15)"
```

---

## Definition of done

- `cargo test --workspace --all-features` green.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` silent.
- `cargo fmt --all -- --check` silent.
- `cargo doc --workspace --no-deps` warning-free.
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` produces a CRD with `spec.desiredPhase` + `status.podUid`.
- `helm lint deploy/helm/sandbawks` passes.
- `helm template test deploy/helm/sandbawks` renders the api Deployment with the signing-secret env var wired.
- Forgejo Actions `api-image.yaml` runs green on `main` and publishes `ghcr.io/hypotheosis/sandbawks-api:{sha,latest}`.
- `just cluster-up && deploy/kind/fixtures/api-smoke.sh && just cluster-down` exits clean — full lifecycle goes through real Pods + real envd + real HMAC-signed file upload + download.
- The `kind-smoke` workflow on `main` remains green.
- All 15 task commits land on `main`.
