//! Sandbox lifecycle endpoints translated to CRD operations.
//!
//! Routes mounted here:
//!
//! - `POST   /sandboxes`              create
//! - `GET    /sandboxes`              list (team-scoped)
//! - `POST   /v2/sandboxes`, `GET /v2/sandboxes`  aliases of the two above;
//!   the paths current E2B SDKs (e2b >= 2.61) call
//! - `GET    /sandboxes/:id`          fetch one (backs SDK `get_info`)
//! - `POST   /sandboxes/:id/kill`     delete (and let kube-gc + the
//!   controller's `Action::Terminate` delete the Pod)
//! - `POST   /sandboxes/:id/pause`    patch `spec.desiredPhase=Paused`
//! - `POST   /sandboxes/:id/connect`  patch `spec.desiredPhase=Running`
//!   and return promptly (a short settle, not a block-until-ready); 201 when
//!   it resumed the sandbox, 200 when it was already running.
//!   `POST /v2/sandboxes/:id/connect` is the alias e2b >= 2.61 calls
//! - `POST   /sandboxes/:id/timeout`  bump `spec.timeoutSeconds` (backs
//!   SDK `set_timeout`); `POST /sandboxes/:id/refreshes` is an alias
//!
//! All routes are wrapped by `auth_middleware` (Task 3); handlers
//! extract `Extension<TeamContext>` to learn the team's namespace.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
    Api, Client, ResourceExt,
};
use sandbawks_types::{
    merge_egress_policies, NetworkPolicy, PersistenceMode, ResourceQuantity, ResourceRequests,
    Sandbox, SandboxPhase, SandboxSpec, SandboxTemplate,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    auth::{auth_middleware, TeamContext},
    envd_auth::envd_access_token,
    error::{AppError, AppResult},
    signing::SigningKey,
    AppState,
};

// `envdVersion` reported back to SDK clients on every sandbox create /
// list response. The E2B Python SDK (>=2.21) does an unconditional
// `dict.pop("envdVersion")` while parsing the response, so omitting the
// field makes every `Sandbox.create` round-trip explode in the client
// even though the wire status is 201. The gateway and the in-sandbox
// envd agent ship from the same Cargo workspace, so the workspace's
// own `CARGO_PKG_VERSION` is the most honest stable identifier — when
// envd grows an independent version channel this constant can be
// replaced with a value read off the running pod's `status.envdAddress`
// metadata.
const ENVD_VERSION: &str = env!("CARGO_PKG_VERSION");

// Human-facing record of the configured idle window (in seconds), stamped
// alongside the CR on create / `set_timeout` / warm-claim.
//
// Under the idle-timeout model (issue #481) `spec.timeoutSeconds` is the raw
// configured window — it no longer grows from `creationTimestamp` — so this
// annotation now simply mirrors `spec.timeoutSeconds`. It is retained as an
// operator-visible breadcrumb of the requested value (surfaced via `kubectl`
// alongside the spec); the controller reads only `spec.timeoutSeconds`, and
// the idle baseline lives in `status.lastActivityTimestamp`.
// `pub(crate)` so the warm-claim path ([`crate::warm_claim`]) can stamp the
// same annotation when it overrides a claimed member's `timeoutSeconds`.
pub(crate) const TIMEOUT_DURATION_ANNOTATION: &str = "sandbawks.dev/timeout-duration-seconds";

// ---------- E2B DTOs ----------

/// Body of `POST /sandboxes`. Field names match the public E2B API
/// so SDKs work unchanged against this gateway.
///
/// `serde(rename_all = "camelCase")` covers most fields, but E2B
/// spells acronyms uppercase (`templateID`, not `templateId`) so we
/// override those one by one — there's no `rename_all` mode that
/// preserves uppercase ID/URL acronyms.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxBody {
    /// Name of the `SandboxTemplate` to instantiate.
    #[serde(rename = "templateID")]
    pub template_id: String,

    /// Opaque K/V passed through to the CRD's `spec.metadata`. The
    /// controller doesn't interpret these; the gateway echoes them back
    /// on `GET /sandboxes`.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,

    /// Environment variables injected into the sandbox process.
    #[serde(default)]
    pub env_vars: BTreeMap<String, String>,

    /// Auto-terminate / auto-pause timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u32,

    /// Per-create workload egress override (issue #537, design option 2).
    ///
    /// Each entry is an egress destination — an IP/CIDR (`10.0.0.0/8`) or a
    /// DNS name (`git.example.com`) — matching `Sandbox.spec.networkPolicy
    /// .egressAllow`. These are merged as a **union** over the template's
    /// egress baseline ([`SandboxTemplateSpec::network_policy`]), so a request
    /// can *add* destinations but never revoke the template's. Empty (the
    /// default) means "no override"; the sandbox carries exactly the template
    /// baseline.
    ///
    /// E2B has no analog field, so this is a sandbawks extension; SDKs that
    /// don't send it are unaffected (`serde(default)`).
    ///
    /// Honored on **both** the cold-start and warm-pool-claim paths (issue
    /// #542): a claim stamps the union onto the claimed member's
    /// `spec.networkPolicy` and the controller reconciles the live
    /// `NetworkPolicy` to match, so the same request yields the same egress
    /// whether it cold-boots or hits a warm member.
    #[serde(default)]
    pub egress_allow: Vec<String>,

    /// Clone-from-snapshot source (issue #416): stand this new sandbox up from
    /// another sandbox's saved state instead of cold-booting. Maps verbatim onto
    /// [`SandboxSpec::restore_from`] — accepts a **source sandbox ID** (e.g.
    /// `sbx-warm-l5dkn`) or, for snapshot mode, a raw `s3://…/manifest.json`
    /// URL. The controller restores via the source's resolved persistence mode
    /// (gVisor S3 snapshot, or CSI-cloned PVC for filesystem mode).
    ///
    /// Accepted under both `restoreFrom` and the E2B-flavored `sourceSandboxID`
    /// alias. A sandbawks extension with no E2B analog; SDKs that omit it are
    /// unaffected (`serde(default)`). Empty/absent → a normal cold-booted
    /// sandbox.
    #[serde(default, alias = "sourceSandboxID")]
    pub restore_from: Option<String>,
}

/// MVP default sandbox lifetime. Matches the E2B SDK's default.
fn default_timeout() -> u32 {
    3600
}

/// Response shape for create + list. E2B-compatible.
///
/// Same uppercase-acronym caveat as [`CreateSandboxBody`] — `sandboxID`,
/// `templateID`, `clientID` need explicit renames.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxDTO {
    /// `sbx-<random>` identifier; also the `metadata.name` of the CR.
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,

    /// The `SandboxTemplate` this sandbox was created from.
    #[serde(rename = "templateID")]
    pub template_id: String,

    /// Stable team identifier (namespace name in MVP).
    #[serde(rename = "clientID")]
    pub client_id: String,

    /// Reserved for future per-sandbox alias. Always `None` for now.
    pub alias: Option<String>,

    /// Opaque K/V echoed from `CreateSandboxBody::metadata`.
    pub metadata: BTreeMap<String, String>,

    /// RFC 3339 timestamp when the sandbox was created.
    pub started_at: String,

    /// RFC 3339 timestamp when the sandbox will hit its timeout.
    pub end_at: String,

    /// Version of the in-sandbox `envd` agent reachable through this
    /// sandbox. The E2B SDK (>=2.21) `pop`s this field unconditionally
    /// while parsing the response, so omitting it breaks every
    /// `Sandbox.create` call in the client. Sourced from the workspace
    /// `CARGO_PKG_VERSION` because envd ships from the same workspace.
    #[serde(rename = "envdVersion")]
    pub envd_version: String,

    /// Credential for the sandbox's envd surface (issue #683). The SDK sends
    /// it as `X-Access-Token` on every per-sandbox call and signs `/files`
    /// URLs with it; the envd proxy refuses requests without one. Present on
    /// the create / connect / get answers and omitted from list entries, as
    /// on hosted E2B. Derived by [`crate::envd_auth::envd_access_token`].
    #[serde(rename = "envdAccessToken", skip_serializing_if = "Option::is_none")]
    pub envd_access_token: Option<String>,

    /// State per the E2B SDK's `SandboxState` enum: `"running"` or
    /// `"paused"`. Required by `ListedSandbox.from_dict`. Maps from
    /// `SandboxStatus.phase`; transient/terminal phases (Creating,
    /// Pausing, Resuming, Terminating, Failed) all surface as
    /// `"running"` because the SDK only knows the two stable states.
    pub state: &'static str,

    /// Integer vCPU count derived from `spec.resources.cpu`. Required
    /// by the E2B SDK's `ListedSandbox.from_dict` (a sub-millicore
    /// budget like `"500m"` rounds up to 1 — more useful to a UX
    /// consumer than truth-to-the-millicore).
    #[serde(rename = "cpuCount")]
    pub cpu_count: i32,

    /// Memory budget in MiB derived from `spec.resources.memory`.
    /// Required by the E2B SDK's `ListedSandbox.from_dict`. K8s
    /// decimal-prefix variants (`K`, `M`, `G`) are treated as binary
    /// (`Ki`, `Mi`, `Gi`) for simplicity — within ~7% for `G` vs
    /// `Gi`, which is fine for a UX-facing field.
    #[serde(rename = "memoryMB")]
    pub memory_mb: i32,

    /// Disk size in MiB derived from `spec.resources.ephemeral_storage`
    /// (default 1024 when the field is unset, matching the typical
    /// sandbox layout). Required by the E2B SDK's
    /// `ListedSandbox.from_dict`.
    #[serde(rename = "diskSizeMB")]
    pub disk_size_mb: i32,
}

/// Response shape for `POST /sandboxes/:id/pause`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PauseResponse {
    /// Echoes the sandbox id so SDK callers can re-resume by id.
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,

    /// Always `"paused"` — gives SDKs a stable string to assert against
    /// without having to round-trip the `SandboxPhase` enum spelling.
    pub state: &'static str,
}

/// Body of `POST /sandboxes/:id/timeout`. This is the endpoint the
/// official E2B Python SDK (>=2.21) hits for `Sandbox.set_timeout` —
/// the generated client posts `{ "timeout": <seconds> }` where the
/// value is "seconds from the current time after which the sandbox
/// should expire" (i.e. keep-alive relative to *now*, not an absolute
/// deadline and not an additive bump).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetTimeoutBody {
    /// Seconds from now until the sandbox should expire.
    pub timeout: u32,
}

/// Body of `POST /sandboxes/:id/refreshes`. The older E2B wire shape
/// for the same operation as [`SetTimeoutBody`]; the generated client
/// spells the field `duration` ("duration for which the sandbox should
/// be kept alive in seconds"). Kept as a back-compat alias alongside
/// `/timeout` the same way `POST /sandboxes/:id/kill` shadows
/// `DELETE /sandboxes/:id`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshBody {
    /// Seconds from now to keep the sandbox alive.
    pub duration: u32,
}

// `POST /sandboxes/:id/connect` returns a full `SandboxDTO` so the
// E2B SDK's `Sandbox.from_dict` can parse it. The earlier sketch used
// a narrow `{sandboxID, envdURL}` shape, but the SDK requires the
// `clientID` / `sandboxID` / `templateID` / `envdVersion` quartet
// (any missing field raises `KeyError`). The `envdURL` field was
// dropped — the SDK doesn't read it from the connect response and
// derives the per-sandbox URL itself from sandbox id + domain
// (or `E2B_SANDBOX_URL`).

// ---------- SandboxOps trait so we can fake it in tests ----------

/// Abstraction over the `kube::Api<Sandbox>` calls the routes make.
///
/// The real implementation is [`KubeSandboxOps`], which delegates
/// straight to `kube::Api`. Tests use [`FakeSandboxOps`] so they can
/// drive the router without standing up a kube apiserver.
#[async_trait]
pub trait SandboxOps: Send + Sync {
    /// Create the `Sandbox` CR in the given namespace.
    async fn create(&self, namespace: &str, sandbox: Sandbox) -> Result<Sandbox, AppError>;

    /// List all `Sandbox` CRs in the namespace.
    async fn list(&self, namespace: &str) -> Result<Vec<Sandbox>, AppError>;

    /// Delete the named `Sandbox` (background propagation — the
    /// controller cleans up child Pods).
    async fn delete(&self, namespace: &str, name: &str) -> Result<(), AppError>;

    /// Patch `spec.desiredPhase` (used by Task 7's pause/resume).
    async fn patch_desired_phase(
        &self,
        namespace: &str,
        name: &str,
        phase: SandboxPhase,
    ) -> Result<(), AppError>;

    /// Patch `spec.timeoutSeconds` (the raw idle window, issue #481), in a
    /// single merge patch. Used by the timeout-refresh routes (backing the
    /// E2B SDK's `Sandbox.set_timeout`). The [`TIMEOUT_DURATION_ANNOTATION`]
    /// is stamped to the same value as a human-facing record of the
    /// configured window. Callers re-anchor the idle baseline separately via
    /// [`SandboxOps::stamp_activity`].
    async fn patch_timeout(
        &self,
        namespace: &str,
        name: &str,
        timeout_seconds: u32,
    ) -> Result<(), AppError>;

    /// Fetch a single `Sandbox` by name. 404 maps to
    /// `AppError::NotFound` via the `kube::Error` conversion.
    async fn get(&self, namespace: &str, name: &str) -> Result<Sandbox, AppError>;

    /// Stamp `status.lastActivityTimestamp` (RFC 3339) so the controller
    /// measures the idle timeout from this instant (issue #481). Patches
    /// the `status` subresource — the controller owns `status`, so this is
    /// a narrow merge that touches only the one field. Called best-effort
    /// off the proxy hot path via [`crate::AppState::note_activity`].
    async fn stamp_activity(
        &self,
        namespace: &str,
        name: &str,
        timestamp_rfc3339: &str,
    ) -> Result<(), AppError>;
}

/// `kube::Api<Sandbox>`-backed implementation used by the production
/// binary. Holds the per-namespace impersonating client cache and builds a
/// namespaced `Api` from the tenant's impersonating client on each call, so
/// the apiserver enforces tenant isolation (#442). `Api` is cheap to build
/// (it just holds an `Arc`'d client) so this avoids caching `Api`s directly.
pub struct KubeSandboxOps {
    /// Per-namespace impersonating clients — used for tenant CRUD so the
    /// apiserver enforces isolation (#442).
    clients: Arc<crate::tenant_client::TenantClients>,
    /// The gateway's own client — used ONLY by `stamp_activity`, the
    /// idle-tracking `status` write that runs on the no-team-identity proxy
    /// hot path (and so cannot impersonate). Status-only; the api SA keeps
    /// just `sandboxes/status: patch` for it.
    own: Client,
}

impl KubeSandboxOps {
    /// Build a `KubeSandboxOps` over the impersonating client cache (tenant
    /// CRUD) and the gateway's own client (`stamp_activity`).
    pub fn new(clients: Arc<crate::tenant_client::TenantClients>, own: Client) -> Self {
        Self { clients, own }
    }

    /// Impersonating `Api` for the tenant `namespace` (tenant CRUD).
    fn api(&self, namespace: &str) -> Result<Api<Sandbox>, AppError> {
        Ok(Api::namespaced(
            self.clients.for_namespace(namespace)?,
            namespace,
        ))
    }

    /// `Api` under the gateway's own SA — `stamp_activity` only.
    fn own_api(&self, namespace: &str) -> Api<Sandbox> {
        Api::namespaced(self.own.clone(), namespace)
    }
}

#[async_trait]
impl SandboxOps for KubeSandboxOps {
    #[tracing::instrument(
        skip_all,
        name = "kube:api::sandbox_create",
        err,
        fields(otel.kind = "client", namespace = %namespace)
    )]
    async fn create(&self, namespace: &str, sandbox: Sandbox) -> Result<Sandbox, AppError> {
        // Metric counterpart of the `kube:api::sandbox_create` span (#373).
        crate::kube_op_metrics::timed("sandbox", "create", async move {
            Ok(self
                .api(namespace)?
                .create(&PostParams::default(), &sandbox)
                .await?)
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::sandbox_list",
        err,
        fields(otel.kind = "client", namespace = %namespace)
    )]
    async fn list(&self, namespace: &str) -> Result<Vec<Sandbox>, AppError> {
        // Metric counterpart of the `kube:api::sandbox_list` span (#373).
        crate::kube_op_metrics::timed("sandbox", "list", async move {
            Ok(self
                .api(namespace)?
                .list(&ListParams::default())
                .await?
                .items)
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::sandbox_delete",
        err,
        fields(otel.kind = "client", sandbox = %name, namespace = %namespace)
    )]
    async fn delete(&self, namespace: &str, name: &str) -> Result<(), AppError> {
        // Metric counterpart of the `kube:api::sandbox_delete` span (#373).
        crate::kube_op_metrics::timed("sandbox", "delete", async move {
            // `background` propagation lets the controller observe the
            // delete and clean up the backing Pod asynchronously — the
            // gateway only needs to confirm the CR delete succeeded.
            let _ = self
                .api(namespace)?
                .delete(name, &DeleteParams::background())
                .await?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::sandbox_patch_desired_phase",
        err,
        fields(otel.kind = "client", sandbox = %name, namespace = %namespace, desired_phase = ?phase)
    )]
    async fn patch_desired_phase(
        &self,
        namespace: &str,
        name: &str,
        phase: SandboxPhase,
    ) -> Result<(), AppError> {
        // `PatchParams::default()` (NOT `apply(...)`) pairs with
        // `Patch::Merge`. Combining `apply()` with a merge-patch body
        // forces the apiserver into server-side-apply mode against a
        // body it can't parse as apply YAML — the first patch happens
        // to land (initial ownership claim), but subsequent patches
        // from the same field manager are silently dropped, producing
        // the symptom where `pause` worked but `connect` couldn't
        // flip `spec.desiredPhase` back to Running.
        // Metric counterpart of the `kube:api::sandbox_patch_desired_phase`
        // span (#373).
        crate::kube_op_metrics::timed("sandbox", "patch_desired_phase", async move {
            let patch = serde_json::json!({ "spec": { "desiredPhase": phase } });
            self.api(namespace)?
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::sandbox_patch_timeout",
        err,
        fields(otel.kind = "client", sandbox = %name, namespace = %namespace, timeout_seconds = timeout_seconds)
    )]
    async fn patch_timeout(
        &self,
        namespace: &str,
        name: &str,
        timeout_seconds: u32,
    ) -> Result<(), AppError> {
        // Same merge-patch discipline as `patch_desired_phase` —
        // `PatchParams::default()` (not `apply`) pairs with
        // `Patch::Merge` so repeated refreshes from the gateway's field
        // manager all land instead of being dropped after the first.
        // `timeoutSeconds` is the raw idle window (issue #481); the
        // annotation mirrors it as a human-facing record of the configured
        // value. Annotation values are strings, so it's rendered as one.
        // Metric counterpart of the `kube:api::sandbox_patch_timeout` span
        // (#373).
        crate::kube_op_metrics::timed("sandbox", "patch_timeout", async move {
            let patch = serde_json::json!({
                "metadata": {
                    "annotations": {
                        TIMEOUT_DURATION_ANNOTATION: timeout_seconds.to_string(),
                    },
                },
                "spec": { "timeoutSeconds": timeout_seconds },
            });
            self.api(namespace)?
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::sandbox_get",
        err,
        fields(otel.kind = "client", sandbox = %name, namespace = %namespace)
    )]
    async fn get(&self, namespace: &str, name: &str) -> Result<Sandbox, AppError> {
        // Metric counterpart of the `kube:api::sandbox_get` span (#373).
        crate::kube_op_metrics::timed("sandbox", "get", async move {
            self.api(namespace)?.get(name).await.map_err(AppError::from)
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::sandbox_stamp_activity",
        err,
        fields(otel.kind = "client", sandbox = %name, namespace = %namespace)
    )]
    async fn stamp_activity(
        &self,
        namespace: &str,
        name: &str,
        timestamp_rfc3339: &str,
    ) -> Result<(), AppError> {
        // `patch_status` targets the `/status` subresource — a plain
        // `patch` to `.status` is silently dropped when the CRD declares a
        // status subresource (as Sandbox does). Same `Patch::Merge` +
        // `PatchParams::default()` discipline as `patch_timeout`: a merge
        // patch setting just this one field leaves the controller's other
        // status fields untouched, and last-write-wins is correct here
        // (the freshest activity stamp is the one we want).
        // Metric counterpart of the `kube:api::sandbox_stamp_activity` span.
        crate::kube_op_metrics::timed("sandbox", "stamp_activity", async move {
            let patch = serde_json::json!({
                "status": { "lastActivityTimestamp": timestamp_rfc3339 },
            });
            // Own SA, not impersonation: this runs on the no-team-identity
            // proxy path and is a status-only idle write (#442).
            self.own_api(namespace)
                .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await?;
            Ok(())
        })
        .await
    }
}

/// In-memory ops used by `tests/sandboxes.rs` so unit tests don't
/// need a kube apiserver. The `Mutex` is `tokio::sync::Mutex` because
/// the trait methods are `async` and the handlers may hold the guard
/// across an `.await` (e.g. cloning the vec).
#[derive(Default)]
pub struct FakeSandboxOps {
    inner: Mutex<Vec<Sandbox>>,
}

#[async_trait]
impl SandboxOps for FakeSandboxOps {
    async fn create(&self, ns: &str, mut sandbox: Sandbox) -> Result<Sandbox, AppError> {
        // Record the namespace the sandbox was created in, as the apiserver
        // would, so `get` can resolve it per namespace (issue #13).
        if sandbox.metadata.namespace.is_none() {
            sandbox.metadata.namespace = Some(ns.to_owned());
        }
        // Stamp a creation timestamp if one isn't already set so
        // downstream `sandbox_to_dto` can render a stable `startedAt`.
        if sandbox.metadata.creation_timestamp.is_none() {
            sandbox.metadata.creation_timestamp = Some(
                k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(chrono::Utc::now()),
            );
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
        phase: SandboxPhase,
    ) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;
        let sb = guard
            .iter_mut()
            .find(|s| s.metadata.name.as_deref() == Some(name))
            .ok_or_else(|| AppError::not_found("sandbox_not_found", name.to_owned()))?;
        sb.spec.desired_phase = Some(phase);
        Ok(())
    }

    async fn patch_timeout(
        &self,
        _ns: &str,
        name: &str,
        timeout_seconds: u32,
    ) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;
        let sb = guard
            .iter_mut()
            .find(|s| s.metadata.name.as_deref() == Some(name))
            .ok_or_else(|| AppError::not_found("sandbox_not_found", name.to_owned()))?;
        sb.spec.timeout_seconds = timeout_seconds;
        sb.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .insert(
                TIMEOUT_DURATION_ANNOTATION.to_owned(),
                timeout_seconds.to_string(),
            );
        Ok(())
    }

    async fn get(&self, ns: &str, name: &str) -> Result<Sandbox, AppError> {
        // Names are only unique per namespace (issue #13): match both.
        self.inner
            .lock()
            .await
            .iter()
            .find(|s| {
                s.metadata.name.as_deref() == Some(name)
                    && s.metadata.namespace.as_deref() == Some(ns)
            })
            .cloned()
            .ok_or_else(|| AppError::not_found("sandbox_not_found", name.to_owned()))
    }

    async fn stamp_activity(
        &self,
        _ns: &str,
        name: &str,
        timestamp_rfc3339: &str,
    ) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;
        let sb = guard
            .iter_mut()
            .find(|s| s.metadata.name.as_deref() == Some(name))
            .ok_or_else(|| AppError::not_found("sandbox_not_found", name.to_owned()))?;
        sb.status
            .get_or_insert_with(Default::default)
            .last_activity_timestamp = Some(timestamp_rfc3339.to_owned());
        Ok(())
    }
}

// ---------- DTO <-> CRD conversion ----------

/// Parse a Kubernetes-style CPU quantity into an integer vCPU count,
/// rounding up. `"2"` → 2; `"1.5"` → 2; `"500m"` → 1.
fn parse_cpu_count(q: &ResourceQuantity) -> i32 {
    let s = q.as_str();
    if let Some(stripped) = s.strip_suffix('m') {
        let millis: i32 = stripped.parse().unwrap_or(1000);
        // Stable ceil-divide; `i32::div_ceil` is nightly-only.
        ((millis + 999) / 1000).max(1)
    } else {
        s.parse::<f64>()
            .ok()
            .filter(|v| *v > 0.0)
            .map(|v| v.ceil() as i32)
            .unwrap_or(1)
    }
}

/// Parse a Kubernetes-style size quantity into MiB, rounding up.
/// `"512Mi"` → 512; `"1Gi"` → 1024; `"1500K"` (no unit / decimal
/// prefixes) are treated as their binary equivalents.
fn parse_size_mb(q: &ResourceQuantity) -> i32 {
    let s = q.as_str();
    let split_idx = s
        .char_indices()
        .find(|(_, c)| !c.is_ascii_digit() && *c != '.')
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    let (num_part, unit) = s.split_at(split_idx);
    let num: f64 = num_part.parse().unwrap_or(512.0);
    let multiplier_mib = match unit {
        "" => 1.0 / (1024.0 * 1024.0),
        "K" | "Ki" => 1.0 / 1024.0,
        "M" | "Mi" => 1.0,
        "G" | "Gi" => 1024.0,
        "T" | "Ti" => 1024.0 * 1024.0,
        _ => 1.0,
    };
    ((num * multiplier_mib).ceil() as i32).max(1)
}

/// Default disk size in MiB when `spec.resources.ephemeral_storage` is
/// unset. Matches the typical kind-fixture sandbox layout closely
/// enough for a UX-facing field; proper per-template defaults track
/// alongside the resource-reporting follow-up.
const DEFAULT_DISK_MB: i32 = 1024;

/// The instant the idle clock is measured from (issue #481): the last
/// stamped activity (`status.lastActivityTimestamp`), or `creationTimestamp`
/// when none has landed yet. Mirrors the controller's `activity_baseline`
/// so the gateway-rendered `endAt` and the controller's pause decision agree.
fn idle_baseline(sandbox: &Sandbox) -> Option<chrono::DateTime<Utc>> {
    let stamped = sandbox
        .status
        .as_ref()
        .and_then(|s| s.last_activity_timestamp.as_deref())
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&Utc));
    stamped.or_else(|| sandbox.metadata.creation_timestamp.as_ref().map(|t| t.0))
}

/// Render a `Sandbox` CR as the E2B JSON DTO. `client_id` is the
/// team's namespace (resolved from the request's `TeamContext`).
/// `token_key` is the gateway signing key when the answer should carry the
/// sandbox's `envdAccessToken` (create / connect / get), `None` for list. The
/// token is bound to the sandbox's namespace (issue #13): its
/// `metadata.namespace`, or the team's namespace when the object carries none.
fn sandbox_to_dto(
    sandbox: &Sandbox,
    client_id: &str,
    token_key: Option<&SigningKey>,
) -> SandboxDTO {
    let id = sandbox.name_any();
    let creation = sandbox
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0)
        .unwrap_or_else(Utc::now);
    let started_at = creation.to_rfc3339();
    // `endAt` is the idle deadline: `baseline + timeoutSeconds`, where the
    // baseline tracks activity (issue #481). For a never-touched sandbox the
    // baseline is creation, so this matches the old `creation + window` shape;
    // once activity is stamped it advances with use, mirroring the controller.
    let end_at = (idle_baseline(sandbox).unwrap_or(creation)
        + chrono::Duration::seconds(sandbox.spec.timeout_seconds.into()))
    .to_rfc3339();
    let state = match sandbox.status.as_ref().map(|s| s.phase) {
        Some(SandboxPhase::Paused) => "paused",
        _ => "running",
    };
    let resources = &sandbox.spec.resources;

    SandboxDTO {
        sandbox_id: id.clone(),
        template_id: sandbox.spec.template_ref.clone(),
        client_id: client_id.to_owned(),
        alias: None,
        metadata: sandbox.spec.metadata.clone(),
        started_at,
        end_at,
        envd_version: ENVD_VERSION.to_owned(),
        envd_access_token: token_key.map(|key| {
            let namespace = sandbox.metadata.namespace.as_deref().unwrap_or(client_id);
            envd_access_token(key, namespace, &id)
        }),
        state,
        cpu_count: parse_cpu_count(&resources.cpu),
        memory_mb: parse_size_mb(&resources.memory),
        disk_size_mb: resources
            .ephemeral_storage
            .as_ref()
            .map(parse_size_mb)
            .unwrap_or(DEFAULT_DISK_MB),
    }
}

/// Build a `Sandbox` CR from the create-body. The CRD `metadata.name`
/// is the generated `sbx-...` id (also returned to the caller as
/// `sandboxID`).
fn body_to_sandbox(body: CreateSandboxBody) -> Sandbox {
    let id = format!("sbx-{}", random_suffix(12));
    // `ResourceQuantity::from_str("1")` is infallible for these hardcoded
    // shape-valid strings. This is only a placeholder: `create_sandbox`
    // overwrites it with the template's `defaultResources` once the
    // template resolves (issue #395), keeping it only on a template miss.
    let cpu = "1"
        .parse::<ResourceQuantity>()
        .expect("hardcoded resource quantity is valid");
    let memory = "1Gi"
        .parse::<ResourceQuantity>()
        .expect("hardcoded resource quantity is valid");
    let mut sandbox = Sandbox::new(
        &id,
        SandboxSpec {
            template_ref: body.template_id,
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu,
                memory,
                ephemeral_storage: None,
            },
            timeout_seconds: body.timeout,
            auto_pause: true,
            env: body.env_vars,
            metadata: body.metadata,
            // Capture the per-create egress override (issue #537). The template
            // baseline is unioned over this in `create_sandbox` once the
            // template resolves; an empty list stays `None` (default-deny).
            network_policy: (!body.egress_allow.is_empty()).then_some(NetworkPolicy {
                egress_allow: body.egress_allow,
            }),
            desired_phase: None,
            snapshot_retention: None,
            pvc_retention: None,
            tolerations: None,
            node_selector: None,
            security_context: None,
            persistence_mode: None,
            volume_claim_templates: None,
            // Clone-from-snapshot source, threaded verbatim onto the CR; the
            // controller resolves + gates it at create time (issue #416).
            restore_from: body.restore_from,
        },
    );
    // Record the configured lifetime so a later resume can re-derive a fresh
    // deadline without compounding (issue #358); see
    // [`TIMEOUT_DURATION_ANNOTATION`]. `timeout == 0` is the "disabled"
    // sentinel — leave the annotation off so a disabled cold-create carries the
    // same annotation-free shape as a warm-pool member (a `0` value would
    // round-trip harmlessly but read inconsistently across the two paths).
    if body.timeout > 0 {
        sandbox
            .metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .insert(
                TIMEOUT_DURATION_ANNOTATION.to_owned(),
                body.timeout.to_string(),
            );
    }
    sandbox
}

/// Stamp the owning `TeamAPIKey` name onto a Sandbox as the
/// [`sandbawks_types::API_KEY_NAME_ANNOTATION`], for per-key usage attribution
/// (epic #480 Phase 2, issue #631).
///
/// A no-op for an empty `key_name` so the CR keeps the annotation-free shape of an
/// operator-applied (keyless) Sandbox rather than carrying a blank value — the
/// controller treats "annotation absent" as "unknown key", which a blank would
/// defeat. The namespace already identifies the team; this carries the key.
fn stamp_api_key_name(sandbox: &mut Sandbox, key_name: &str) {
    if key_name.is_empty() {
        return;
    }
    sandbox
        .metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(
            sandbawks_types::API_KEY_NAME_ANNOTATION.to_owned(),
            key_name.to_owned(),
        );
}

/// `sbx-<hex>` suffix derived from the current nanosecond clock.
///
/// Not cryptographically random — the sandbox id is a human-readable
/// handle, not a security identifier (the `TeamAPIKey` is the access
/// control boundary). For MVP traffic levels collisions are vanishingly
/// unlikely; a later plan can swap this for `uuid::Uuid::new_v4()` once
/// uuid is a workspace dependency.
fn random_suffix(len: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut hex = format!("{nanos:0width$x}", width = len);
    // Take the last `len` chars (most entropy lives in the low bits of
    // the nanosecond clock; chop the high padding zeros off the front).
    let extra = hex.len().saturating_sub(len);
    hex.drain(..extra);
    hex
}

// ---------- Handlers ----------

/// How long `connect_sandbox` lets a resume try to surface
/// `status.envdAddress` before handing the sandbox back optimistically.
///
/// This is deliberately short. The handler used to block up to 300s for the
/// controller to report the address, but the E2B SDKs wrap every request in an
/// `AbortSignal.timeout(requestTimeoutMs)` (~30s by default) — so any resume
/// slower than that aborted the client with an opaque "operation timed out"
/// and *no* sandbox handle, even though the resume completed server-side
/// (issue #652). A cold restore that needs a node scale-up plus a multi-hundred
/// -MB image pull is routinely ~90s, far past any per-request client timeout.
///
/// A warm resume still readies inside this window and returns fully ready; a
/// slower one returns the handle and lets the SDK's first per-sandbox RPC drive
/// readiness through the envd proxy, where the not-ready park / #641
/// keepalive-resignal bridge the remaining warm-up. The window also comfortably
/// outlasts the watch-fed `EnvdCache` catching up to the `desiredPhase=Running`
/// patch, so that first RPC parks rather than being rejected as `sandbox_paused`.
const RESUME_SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

/// Poll interval for `connect_sandbox`. 500ms is a compromise: fast
/// enough that warm sandboxes return on the second tick, slow enough
/// that the settle wait costs only a handful of `get` round-trips
/// against the apiserver.
const CONNECT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// One poll iteration's decision in [`connect_sandbox`].
///
/// Kept as a pure function of the observed `Sandbox` plus whether the settle
/// window elapsed ([`connect_step`]) so the branch logic is unit tested
/// without standing up an apiserver.
#[derive(Debug, PartialEq, Eq)]
enum ConnectStep {
    /// `status.envdAddress` is populated — the sandbox is serving; return the
    /// ready DTO.
    Ready,
    /// The sandbox is being torn down (`Terminating`) and will never serve —
    /// surface a give-up error rather than a dead handle.
    Terminal,
    /// Still resuming, inside the settle window — poll again.
    Wait,
    /// Still resuming, but the settle window elapsed — return the handle
    /// optimistically and let the envd proxy bridge the remaining warm-up.
    ReturnPending,
}

/// Decide one [`connect_sandbox`] poll iteration from the observed sandbox.
///
/// `settle_elapsed` is whether [`RESUME_SETTLE_TIMEOUT`] has passed. A serving
/// sandbox (address present) is always [`ConnectStep::Ready`]; a `Terminating`
/// one is [`ConnectStep::Terminal`].
///
/// `Failed` is intentionally *not* terminal here: re-requesting `Running` can
/// drive an evicted-sandbox recovery (issue #518), and the envd proxy
/// re-classifies terminal-vs-transient on the first RPC with fresher,
/// watch-fed data — so blocking a `Failed` sandbox at connect would wrongly
/// foreclose a recoverable resume.
fn connect_step(sandbox: &Sandbox, settle_elapsed: bool) -> ConnectStep {
    let status = sandbox.status.as_ref();
    if status.and_then(|s| s.envd_address.as_ref()).is_some() {
        return ConnectStep::Ready;
    }
    if status.map(|s| s.phase) == Some(SandboxPhase::Terminating) {
        return ConnectStep::Terminal;
    }
    if settle_elapsed {
        ConnectStep::ReturnPending
    } else {
        ConnectStep::Wait
    }
}

/// Resolve a create request's effective `persistenceMode` and reject it at
/// admission if the pairing with the resolved runtime class is unsupported
/// (epic #603 Phase 4 / issues #607, #616).
///
/// The effective mode is the per-create override (`per_create`, absent today —
/// the E2B body carries none) over the template default, then the #616 VCT
/// inference: when neither side sets the mode the effective mode is `filesystem`
/// if `has_volume_claims`, else the `snapshot` default. The rule is shared with
/// the controller's `resolve_pod_inputs` via
/// [`sandbawks_types::resolve_effective_persistence_mode`], so the gateway's
/// admission and the controller's pod-input resolution cannot disagree (gateway
/// admits a pairing the controller would reject, or vice-versa).
///
/// `has_volume_claims` is the template's `volumeClaimTemplates` non-emptiness:
/// the E2B create body carries no per-create VCTs, so the template list *is* the
/// effective set at this edge. Pure over its resolved inputs so it is
/// unit-testable without a kube client; [`create_sandbox`] calls it once the
/// template read has filled the runtime class.
///
/// # Errors
///
/// Returns [`AppError::bad_request`] with code `unsupported_persistence_mode`
/// when the mode is incompatible with the runtime class (e.g. `snapshot` on
/// Kata) — the message carries the actionable fix.
fn admit_persistence_mode(
    per_create: Option<PersistenceMode>,
    template_default: Option<PersistenceMode>,
    has_volume_claims: bool,
    runtime_class_name: Option<&str>,
) -> Result<(), AppError> {
    sandbawks_types::resolve_effective_persistence_mode(
        per_create,
        template_default,
        has_volume_claims,
    )
    .validate_for_runtime_class(runtime_class_name)
    .map_err(|e| AppError::bad_request("unsupported_persistence_mode", e.to_string()))
}

#[tracing::instrument(skip_all, name = "handler:sandboxes::create_sandbox")]
async fn create_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Json(body): Json<CreateSandboxBody>,
) -> AppResult<impl IntoResponse> {
    if !team.allows("sandboxes:create") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:create required",
        ));
    }
    let mut sandbox = body_to_sandbox(body);
    // Stamp the owning `TeamAPIKey` name for per-key usage attribution (epic #480
    // Phase 2, issue #631). The namespace already carries the team; this carries
    // the key so the controller can bill the exact key without re-reading the key
    // store. Stamped on the cold-create object here; the warm-claim fast path
    // below stamps the claimed member separately via `claim_for`, since that
    // returns a different (pre-minted) Sandbox. Skipped when empty so the CR shape
    // matches an operator-applied (keyless) Sandbox rather than carrying a blank.
    stamp_api_key_name(&mut sandbox, &team.key_name);
    // Whether the request carried a per-create `egressAllow` (issue #542).
    // `body_to_sandbox` captures it into `spec.networkPolicy` *before* the
    // template baseline is unioned in below, so this is the one point where a
    // per-create override is distinguishable from a template-only baseline —
    // needed to decide whether the warm-claim path patches the member's egress.
    let has_per_create_egress = sandbox.spec.network_policy.is_some();
    // Resolve the SandboxTemplate so we can propagate
    // `defaultRuntimeClass` into `Sandbox.spec.runtimeClassName`.
    // Without this, PodAdapter never stamps `runtimeClassName: gvisor`
    // on the Pod and the container runs under containerd's default
    // (runc) — which silently breaks pause/resume because runsc
    // has no state for the container. We *do not* hard-fail when the
    // template is missing: tests use a mock kube::Client that returns
    // 204, and operator workflows may briefly post a Sandbox before
    // the matching SandboxTemplate exists. The controller's later
    // PodAdapter.create_pod call surfaces the missing template as a
    // proper Sandbox-status error.
    // Impersonate the tenant SA for the template read too (#442), so the
    // gateway's own SA needs no cross-namespace SandboxTemplate read.
    let template_api: kube::Api<SandboxTemplate> =
        kube::Api::namespaced(state.tenant_client(&team.namespace)?, &team.namespace);
    match template_api.get(&sandbox.spec.template_ref).await {
        Ok(template) => {
            let class = template.spec.default_runtime_class.trim();
            if !class.is_empty() {
                sandbox.spec.runtime_class_name = Some(class.to_owned());
            }
            // Size the sandbox from the template's `defaultResources`
            // (issue #395). `body_to_sandbox` seeds a `1` CPU / `1Gi`
            // placeholder so the CR is always schema-valid; here we
            // overwrite it with the template's real budget so the Pod's
            // requests/limits — and the DTO returned below — reflect what
            // the operator configured. The E2B create body carries no
            // resource fields, so there is no caller override to preserve;
            // a template miss (the `Err` arm) keeps the placeholder.
            sandbox.spec.resources = template.spec.default_resources;
            // Layer the template's egress baseline (issue #537, option 1) under
            // the per-create override (option 2) captured in
            // `body_to_sandbox`: the materialized NetworkPolicy authorizes the
            // union of both. Template first so the baseline's destinations lead;
            // a template miss (the `Err` arm) keeps just the per-create
            // override. `take()` avoids cloning the override out of the spec.
            sandbox.spec.network_policy = merge_egress_policies(
                template.spec.network_policy,
                sandbox.spec.network_policy.take(),
            );
            // Admission guard (epic #603 Phase 4 / issue #607): reject
            // `persistenceMode: snapshot` on a Kata runtime class up front, so
            // the caller gets a synchronous, actionable 400 rather than a
            // Sandbox that later fail-closes deep in the node's pause path. The
            // runtime class is already resolved above. The controller re-checks
            // this at pod-input resolution as defense-in-depth for a hand-applied
            // CR that bypasses the gateway.
            admit_persistence_mode(
                sandbox.spec.persistence_mode,
                template.spec.persistence_mode,
                template
                    .spec
                    .volume_claim_templates
                    .as_deref()
                    .is_some_and(|vcts| !vcts.is_empty()),
                sandbox.spec.runtime_class_name.as_deref(),
            )?;
        }
        Err(e) => tracing::warn!(
            template = %sandbox.spec.template_ref,
            namespace = %team.namespace,
            error = %e,
            "SandboxTemplate lookup failed; proceeding with placeholder resources and no runtimeClassName"
        ),
    }
    // Warm-pool fast path: if a `SandboxWarmPool` covers this template and
    // has a live `Running` member to spare, claim (reassign) it instead of
    // cold-booting — no pod creation or resume on the claim path. The
    // claimed sandbox keeps its own name, which becomes the `sandboxID`. A
    // miss (no pool, or the pool drained) returns `None` and falls through
    // to the cold-boot `create` below.
    let claimed_at = Utc::now().to_rfc3339();
    // The egress override to stamp onto a claimed warm member (issue #542):
    // `Some` only when the request carried a per-create `egressAllow`, in which
    // case `sandbox.spec.network_policy` now holds the union of the template
    // baseline and the per-create destinations (computed above) — exactly what
    // a cold boot would enforce. `None` leaves a claimed member's template
    // baseline untouched.
    let egress_override = has_per_create_egress
        .then(|| sandbox.spec.network_policy.clone())
        .flatten();
    if let Some(warm) = crate::warm_claim::claim_for(
        state.warm_claim.as_ref(),
        &team.namespace,
        &sandbox,
        &claimed_at,
        egress_override.as_ref(),
    )
    .await?
    {
        let dto = sandbox_to_dto(&warm, &team.namespace, Some(&state.signing_key));
        return Ok((StatusCode::CREATED, Json(dto)));
    }
    // Stamp the active trace context onto the Sandbox so the controller's
    // reconcile — decoupled from this request by the apiserver and the watch
    // loop, where no header can reach it — continues this trace instead of
    // rooting a fresh one (OTel Phase 5, OBSERVABILITY.md §6.3). A no-op when
    // tracing is disabled, leaving the CR annotation-free.
    let mut trace_annotations = BTreeMap::new();
    sandbawks_observability::inject_into_annotations(&mut trace_annotations);
    if !trace_annotations.is_empty() {
        sandbox
            .metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .append(&mut trace_annotations);
    }
    let created = state.sandbox_ops().create(&team.namespace, sandbox).await?;
    let dto = sandbox_to_dto(&created, &team.namespace, Some(&state.signing_key));
    Ok((StatusCode::CREATED, Json(dto)))
}

#[tracing::instrument(skip_all, name = "handler:sandboxes::list_sandboxes")]
async fn list_sandboxes(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
) -> AppResult<Json<Vec<SandboxDTO>>> {
    if !team.allows("sandboxes:list") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:list required",
        ));
    }
    let items = state.sandbox_ops().list(&team.namespace).await?;
    let dtos = items
        .iter()
        .map(|s| sandbox_to_dto(s, &team.namespace, None))
        .collect();
    Ok(Json(dtos))
}

#[tracing::instrument(skip_all, name = "handler:sandboxes::kill_sandbox")]
async fn kill_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    if !team.allows("sandboxes:delete") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:delete required",
        ));
    }
    state.sandbox_ops().delete(&team.namespace, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /sandboxes/:id/pause` — request a pause by writing
/// `spec.desiredPhase = Paused`. The controller observes the change
/// and drives the actual pause via the runtime adapter (Plan 04 T4).
/// The gateway is purely declarative here — the endpoint is identical
/// across persistence modes; only what survives the pause differs.
///
/// # Persistence semantics (epic #603)
///
/// The sandbox's resolved `persistenceMode` (template default, overridable
/// per-sandbox) selects what a pause preserves:
///
/// - `snapshot` (default): a live-memory + filesystem checkpoint via the
///   runtime's snapshot path (gVisor `runsc checkpoint`). In-memory process
///   state survives, so resume restores the running program in place. Only
///   available on gVisor — `snapshot` on Kata is rejected at create time (the
///   pairing cannot be driven by the containerd shim).
/// - `filesystem`: no live-memory capture. The controller deletes the Pod and
///   retains the sandbox's `volumeClaimTemplates` PVCs; resume cold-boots a
///   fresh Pod that re-mounts the same PVCs. Only files written under a mounted
///   PVC survive — in-memory and non-PVC on-disk state are lost. Runtime-
///   agnostic, and the only pause mechanism available on Kata.
#[tracing::instrument(skip_all, name = "handler:sandboxes::pause_sandbox")]
async fn pause_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
) -> AppResult<Json<PauseResponse>> {
    if !team.allows("sandboxes:pause") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:pause required",
        ));
    }
    // Evict any pooled envd channel for this sandbox BEFORE patching
    // the desired phase. gVisor's `runsc checkpoint` rejects sandboxes
    // with connected TCP endpoints, and our h2 keep-alive pool to envd
    // is a common offender. Best-effort: a 404 on the sandbox just
    // means there's nothing to evict.
    if let Ok(sandbox) = state.sandbox_ops().get(&team.namespace, &id).await {
        if let Some(addr) = sandbox
            .status
            .as_ref()
            .and_then(|s| s.envd_address.as_ref())
        {
            state.envd_pool.evict(addr);
        }
    }
    state
        .sandbox_ops()
        .patch_desired_phase(&team.namespace, &id, SandboxPhase::Paused)
        .await?;
    Ok(Json(PauseResponse {
        sandbox_id: id,
        state: "paused",
    }))
}

/// `POST /sandboxes/:id/connect` (and its v2 alias
/// `POST /v2/sandboxes/:id/connect`) — patch `spec.desiredPhase = Running`
/// and return the sandbox to the caller **without** blocking for the full
/// resume.
///
/// Per E2B's OpenAPI spec the status is `200 OK` when the sandbox was
/// already running and `201 Created` when this call resumed it.
///
/// The handler gives the resume a brief [`RESUME_SETTLE_TIMEOUT`] to surface
/// `status.envdAddress` — a warm resume returns fully ready — then hands the
/// sandbox back optimistically (see [`connect_step`]) and lets the SDK's first
/// per-sandbox RPC drive readiness through the envd proxy, where the not-ready
/// park / #641 keepalive-resignal bridge the remaining warm-up. Blocking for
/// the whole resume is what let a slow (~90s cold) restore outrun the SDK's
/// per-request `AbortSignal.timeout` and strand the client with no handle
/// (issue #652). A `Terminating` sandbox will never serve, so it surfaces a
/// `sandbox_failed` precondition (412) rather than a dead handle.
///
/// The response is the standard [`SandboxDTO`]; the SDK derives the per-sandbox
/// envd URL itself from the sandbox id + domain (see the body comment below),
/// so no address or URL is encoded here.
///
/// # Persistence semantics (epic #603)
///
/// As with [`pause_sandbox`], the endpoint is identical across persistence
/// modes — it only flips `desiredPhase` — but the resume the controller drives
/// differs: a `snapshot` sandbox is restored in place (live memory intact),
/// while a `filesystem` sandbox cold-boots a fresh Pod re-mounting its retained
/// PVCs (only PVC-backed files survive; in-memory state is gone). Either way
/// the handler returns promptly per the settle behavior above rather than
/// waiting for `status.envdAddress`.
#[tracing::instrument(skip_all, name = "handler:sandboxes::connect_sandbox")]
async fn connect_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
) -> AppResult<(StatusCode, Json<SandboxDTO>)> {
    if !team.allows("sandboxes:connect") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:connect required",
        ));
    }
    // Re-anchor the idle clock *before* flipping to Running (issue #358 under
    // the #481 idle model). The controller's deadline is
    // `lastActivityTimestamp + spec.timeoutSeconds`, so a sandbox paused for
    // longer than its window resumes with a stale (pre-pause)
    // `lastActivityTimestamp` and the deadline already in the past: the first
    // `Running` reconcile would fire `Trigger::Timeout` and immediately
    // re-pause (autoPause) or terminate it. Resuming *is* activity, so we stamp
    // the baseline to now, which lands the deadline a full idle window into the
    // future. `spec.timeoutSeconds` (the raw window) is left untouched.
    //
    // Stamping while still `Paused` (the controller's `Trigger::Timeout` only
    // fires in `Running`) closes the race with the resume reconcile.
    //
    // Confine the stamp to an actual resume: a `/connect` against an
    // already-`Running` sandbox would otherwise reset the idle clock on every
    // call, turning `connect` into an undocumented keep-alive (and adding an
    // apiserver round-trip on the happy path). A `0` timeout is the "disabled"
    // sentinel (warm-pool members) with no clock to reset. Explicit keep-alive
    // stays the job of `set_timeout` / `refreshes`.
    let current = state.sandbox_ops().get(&team.namespace, &id).await?;
    let already_running = current.status.as_ref().map(|s| s.phase) == Some(SandboxPhase::Running);
    if !already_running && current.spec.timeout_seconds > 0 {
        state
            .sandbox_ops()
            .stamp_activity(&team.namespace, &id, &Utc::now().to_rfc3339())
            .await?;
    }
    state
        .sandbox_ops()
        .patch_desired_phase(&team.namespace, &id, SandboxPhase::Running)
        .await?;

    // `tokio::time::Instant` is monotonic — safe to compare against a
    // pre-computed deadline even if the wall clock jumps mid-loop.
    //
    // Response shape: the official E2B SDK's `Sandbox._cls_connect`
    // parses this via `Sandbox.from_dict`, which requires the full
    // `clientID` / `sandboxID` / `templateID` / `envdVersion` quartet
    // (any missing field raises `KeyError`). We therefore return the
    // standard `SandboxDTO` here rather than the narrower
    // `ConnectResponse` shape the original sketch used — the previous
    // `{sandboxID, envdURL}` body would crash the SDK in `from_dict`
    // before it ever got to read the URL. `envdURL` is intentionally
    // *not* in the SDK's `Sandbox` model; the SDK derives the per-
    // sandbox URL on its own from the sandbox id + domain (or the
    // `E2B_SANDBOX_URL` override).
    // Return as soon as the sandbox is serving, but do *not* block for the
    // whole resume: a cold restore can take far longer than the SDK's
    // per-request `AbortSignal.timeout`, which would abort the client with an
    // opaque "operation timed out" and no handle even as the resume completes
    // server-side (issue #652). A warm resume still surfaces `envdAddress`
    // inside the short settle window and returns fully ready; a slower one
    // hands the sandbox back optimistically and lets the SDK's first
    // per-sandbox RPC drive readiness through the envd proxy — where the
    // not-ready park / #641 keepalive-resignal bridge the remaining warm-up.
    let deadline = tokio::time::Instant::now() + RESUME_SETTLE_TIMEOUT;
    loop {
        let sandbox = state.sandbox_ops().get(&team.namespace, &id).await?;
        match connect_step(&sandbox, tokio::time::Instant::now() >= deadline) {
            ConnectStep::Ready | ConnectStep::ReturnPending => {
                let status = if already_running {
                    StatusCode::OK
                } else {
                    StatusCode::CREATED
                };
                return Ok((
                    status,
                    Json(sandbox_to_dto(
                        &sandbox,
                        &team.namespace,
                        Some(&state.signing_key),
                    )),
                ));
            }
            ConnectStep::Terminal => {
                return Err(AppError::failed_precondition(
                    "sandbox_failed",
                    "sandbox is terminating and cannot be resumed",
                ));
            }
            ConnectStep::Wait => tokio::time::sleep(CONNECT_POLL_INTERVAL).await,
        }
    }
}

/// `GET /sandboxes/:id` — fetch a single sandbox's current info.
///
/// Backs the E2B SDK's `Sandbox.get_info()` and `Sandbox.is_running()`
/// (the latter is `get_info().state == "running"`). The SDK parses the
/// body via `SandboxDetail.from_dict`, whose required fields are the
/// exact set [`SandboxDTO`] already emits — so we reuse the same DTO
/// the create/list/connect handlers return rather than minting a
/// narrower shape that would `KeyError` in the client.
#[tracing::instrument(skip_all, name = "handler:sandboxes::get_sandbox")]
async fn get_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
) -> AppResult<Json<SandboxDTO>> {
    if !team.allows("sandboxes:get") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:get required",
        ));
    }
    let sandbox = state.sandbox_ops().get(&team.namespace, &id).await?;
    Ok(Json(sandbox_to_dto(
        &sandbox,
        &team.namespace,
        Some(&state.signing_key),
    )))
}

/// Shared timeout-refresh logic behind `/timeout` and `/refreshes`.
///
/// `keep_alive_seconds` is the E2B `set_timeout` value: "the sandbox should
/// expire `N` seconds from now." Under the idle-timeout model (issue #481)
/// the deadline is `lastActivityTimestamp + spec.timeoutSeconds`, so we
/// write the **raw** window to `spec.timeoutSeconds` (no growing from
/// creation) and refresh the activity baseline to now. Together they land
/// the deadline at `now + keep_alive_seconds`. Returns `204 No Content`,
/// which is what the SDK's generated client expects.
async fn refresh_sandbox_timeout(
    state: &AppState,
    team: &TeamContext,
    id: &str,
    keep_alive_seconds: u32,
) -> AppResult<StatusCode> {
    if !team.allows("sandboxes:update") {
        return Err(AppError::forbidden(
            "forbidden",
            "scope sandboxes:update required",
        ));
    }
    // `get` first so a missing sandbox surfaces as 404 (the SDK maps a kube
    // 404 to `SandboxNotFoundException`).
    state.sandbox_ops().get(&team.namespace, id).await?;
    // Write the raw window, then re-anchor the idle clock to now so the
    // window is measured from this `set_timeout` call.
    state
        .sandbox_ops()
        .patch_timeout(&team.namespace, id, keep_alive_seconds)
        .await?;
    state
        .sandbox_ops()
        .stamp_activity(&team.namespace, id, &Utc::now().to_rfc3339())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /sandboxes/:id/timeout` — the endpoint the E2B SDK's
/// `Sandbox.set_timeout` actually calls (body `{ "timeout": <secs> }`).
#[tracing::instrument(skip_all, name = "handler:sandboxes::set_sandbox_timeout")]
async fn set_sandbox_timeout(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
    Json(body): Json<SetTimeoutBody>,
) -> AppResult<StatusCode> {
    refresh_sandbox_timeout(&state, &team, &id, body.timeout).await
}

/// `POST /sandboxes/:id/refreshes` — older E2B wire shape for the same
/// operation as `/timeout` (body `{ "duration": <secs> }`).
#[tracing::instrument(skip_all, name = "handler:sandboxes::refresh_sandbox")]
async fn refresh_sandbox(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(id): Path<String>,
    Json(body): Json<RefreshBody>,
) -> AppResult<StatusCode> {
    refresh_sandbox_timeout(&state, &team, &id, body.duration).await
}

/// Build the lifecycle router wired with `auth_middleware`.
///
/// Tasks 8 (events) and 10 (exec) will `.merge()` their own routers
/// in. Pulling the `key_store` out of `state` before `.with_state(state)`
/// keeps the middleware state and the handler state independent —
/// axum requires the layer's state type to match the layer-fn's
/// `State` extractor, not the router's.
pub fn router(state: AppState) -> Router {
    let key_store = Arc::clone(&state.key_store);
    Router::new()
        .route("/sandboxes", post(create_sandbox).get(list_sandboxes))
        // `DELETE /sandboxes/:id` is the canonical E2B SDK shape for
        // killing a sandbox; the original `POST /sandboxes/:id/kill`
        // shape is kept as a back-compat alias for callers that
        // adopted it before the SDK landed (Plan 04 T15 api-smoke and
        // any older clients). Both routes hand off to the same
        // handler. Surfaced by the Plan 06 T2 SDK compat harness
        // (#184) — without the DELETE route axum returned an empty
        // 404 that the SDK's `Sandbox.kill` could not parse.
        // `GET /sandboxes/:id` backs the SDK's `get_info` / `is_running`;
        // `DELETE` backs `kill`. Both hang off the same path.
        .route("/sandboxes/:id", get(get_sandbox).delete(kill_sandbox))
        .route("/sandboxes/:id/kill", post(kill_sandbox))
        .route("/sandboxes/:id/pause", post(pause_sandbox))
        // `/v2/sandboxes/:id/connect` is what current E2B SDKs (e2b >= 2.61)
        // hit for `Sandbox.connect` / resume (issue #662); the v1 path stays
        // for older SDKs. Same handler: neither reads the `{timeout?}` body.
        .route("/sandboxes/:id/connect", post(connect_sandbox))
        .route("/v2/sandboxes/:id/connect", post(connect_sandbox))
        // `POST /sandboxes/:id/timeout` is what the E2B SDK's
        // `Sandbox.set_timeout` hits; `/refreshes` is the older wire
        // shape for the same operation, kept as a back-compat alias.
        .route("/sandboxes/:id/timeout", post(set_sandbox_timeout))
        .route("/sandboxes/:id/refreshes", post(refresh_sandbox))
        // `GET /v2/sandboxes` is the path the E2B SDK's `Sandbox.list`
        // hits, and `POST /v2/sandboxes` is what current SDKs (e2b >= 2.61)
        // hit for `Sandbox.create` (issue #662); alias both to the same
        // handlers so the SDK works unchanged. The unversioned
        // `/sandboxes` routes stay for older SDKs, the api-smoke fixture
        // and any direct curl callers.
        .route("/v2/sandboxes", post(create_sandbox).get(list_sandboxes))
        .layer(axum::middleware::from_fn_with_state(
            key_store,
            auth_middleware,
        ))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(egress_allow: Vec<String>) -> CreateSandboxBody {
        CreateSandboxBody {
            template_id: "tpl".into(),
            metadata: BTreeMap::new(),
            env_vars: BTreeMap::new(),
            timeout: 3600,
            egress_allow,
            restore_from: None,
        }
    }

    /// Build a `Sandbox` carrying the given observed phase and optional
    /// `status.envdAddress`, for exercising [`connect_step`].
    fn sandbox_with(phase: SandboxPhase, envd_address: Option<&str>) -> Sandbox {
        let mut sandbox = body_to_sandbox(body(Vec::new()));
        sandbox.status = Some(sandbawks_types::SandboxStatus {
            phase,
            envd_address: envd_address.map(str::to_owned),
            ..Default::default()
        });
        sandbox
    }

    /// Issue #652: a serving sandbox (address present) returns ready
    /// immediately, regardless of whether the settle window elapsed.
    #[test]
    fn connect_step_ready_when_address_present() {
        let sandbox = sandbox_with(SandboxPhase::Running, Some("10.0.0.1:49983"));
        assert_eq!(connect_step(&sandbox, false), ConnectStep::Ready);
        assert_eq!(connect_step(&sandbox, true), ConnectStep::Ready);
    }

    /// Inside the settle window, an address-less resuming sandbox keeps polling.
    #[test]
    fn connect_step_waits_within_settle_window() {
        let sandbox = sandbox_with(SandboxPhase::Resuming, None);
        assert_eq!(connect_step(&sandbox, false), ConnectStep::Wait);
    }

    /// Once the settle window elapses, an address-less resuming sandbox is
    /// handed back optimistically (the envd proxy bridges the remaining wait).
    #[test]
    fn connect_step_returns_pending_after_settle_window() {
        let sandbox = sandbox_with(SandboxPhase::Resuming, None);
        assert_eq!(connect_step(&sandbox, true), ConnectStep::ReturnPending);
    }

    /// A `Terminating` sandbox will never serve, so connect surfaces a give-up
    /// error rather than a dead handle — even before the settle window elapses.
    #[test]
    fn connect_step_terminal_when_terminating() {
        let sandbox = sandbox_with(SandboxPhase::Terminating, None);
        assert_eq!(connect_step(&sandbox, false), ConnectStep::Terminal);
        assert_eq!(connect_step(&sandbox, true), ConnectStep::Terminal);
    }

    /// A `Failed` sandbox is deliberately *not* terminal at connect: requesting
    /// `Running` can drive an evicted-sandbox recovery (issue #518), so connect
    /// keeps waiting / hands back rather than foreclosing the resume.
    #[test]
    fn connect_step_failed_is_not_terminal() {
        let sandbox = sandbox_with(SandboxPhase::Failed, None);
        assert_eq!(connect_step(&sandbox, false), ConnectStep::Wait);
        assert_eq!(connect_step(&sandbox, true), ConnectStep::ReturnPending);
    }

    /// Issue #537 (option 2): a per-create `egressAllow` is captured into the
    /// CR's `spec.networkPolicy` so the create route can later union it with the
    /// template baseline.
    #[test]
    fn body_to_sandbox_captures_per_create_egress() {
        let sandbox = body_to_sandbox(body(vec!["git.example.com".into(), "10.0.0.0/8".into()]));
        let egress = sandbox
            .spec
            .network_policy
            .expect("per-create egress override is captured onto the spec");
        assert_eq!(egress.egress_allow, vec!["git.example.com", "10.0.0.0/8"]);
    }

    /// An empty `egressAllow` (the default) leaves the CR at the default-deny
    /// `None` baseline — no empty `networkPolicy` is stamped.
    #[test]
    fn body_to_sandbox_no_egress_when_absent() {
        let sandbox = body_to_sandbox(body(Vec::new()));
        assert!(sandbox.spec.network_policy.is_none());
    }

    /// Epic #480 Phase 2 / issue #631: a non-empty key name is stamped as the
    /// `api-key-name` annotation so the controller can attribute usage per key.
    #[test]
    fn stamp_api_key_name_sets_annotation() {
        let mut sandbox = body_to_sandbox(body(Vec::new()));
        stamp_api_key_name(&mut sandbox, "team-key-7");
        assert_eq!(
            sandbox
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(sandbawks_types::API_KEY_NAME_ANNOTATION))
                .map(String::as_str),
            Some("team-key-7")
        );
    }

    /// An empty key name leaves the CR annotation-free (matching a keyless,
    /// operator-applied Sandbox) rather than stamping a blank value.
    #[test]
    fn stamp_api_key_name_skips_empty() {
        let mut sandbox = body_to_sandbox(body(Vec::new()));
        stamp_api_key_name(&mut sandbox, "");
        let present = sandbox
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(sandbawks_types::API_KEY_NAME_ANNOTATION));
        assert!(!present, "empty key name must not stamp the annotation");
    }

    /// Epic #603 Phase 4 / issue #607: a create whose effective mode resolves to
    /// `snapshot` on a Kata runtime class is rejected at admission with a clear,
    /// actionable 400 — covering both an explicit `snapshot` and a Kata template
    /// that omits `persistenceMode` (which resolves to the `snapshot` default).
    #[test]
    fn admit_rejects_snapshot_on_kata() {
        // Kata template default unset, no VCTs → resolves to `snapshot` → rejected.
        let err = admit_persistence_mode(None, None, false, Some("kata-vm-isolation"))
            .expect_err("snapshot-on-Kata must be rejected at admission");
        match err {
            AppError::BadRequest { code, message } => {
                assert_eq!(code, "unsupported_persistence_mode");
                assert!(
                    message.contains("filesystem"),
                    "message must point at the filesystem fix; got: {message}"
                );
            }
            other => panic!("expected BadRequest, got: {other:?}"),
        }

        // An explicit per-create `snapshot` on Kata is rejected the same way —
        // even with VCTs declared, since the explicit mode wins over inference.
        assert!(admit_persistence_mode(
            Some(PersistenceMode::Snapshot),
            Some(PersistenceMode::Filesystem),
            true,
            Some("kata-fc"),
        )
        .is_err());
    }

    /// Issue #616: a Kata template that declares `volumeClaimTemplates` but
    /// leaves `persistenceMode` unset is admitted — the inference resolves the
    /// effective mode to `filesystem`, so the storage-declaring template "just
    /// works" past the #607 guard at the gateway, matching the controller. With
    /// no VCTs the same unset mode still resolves to `snapshot` and is rejected.
    #[test]
    fn admit_infers_filesystem_from_vcts_on_kata() {
        // VCTs declared, mode unset on both sides → inferred `filesystem` → ok.
        assert!(admit_persistence_mode(None, None, true, Some("kata-vm-isolation")).is_ok());
        // No VCTs, mode unset → `snapshot` default → still rejected on Kata.
        assert!(admit_persistence_mode(None, None, false, Some("kata-vm-isolation")).is_err());
    }

    /// `filesystem` on Kata, and any mode on gVisor / the cluster default, are
    /// admitted — the per-create override and template default both flow through
    /// the same precedence the controller's resolution uses.
    #[test]
    fn admit_accepts_supported_pairings() {
        // filesystem on Kata (explicit, and via the template default).
        assert!(admit_persistence_mode(
            Some(PersistenceMode::Filesystem),
            None,
            false,
            Some("kata-vm-isolation")
        )
        .is_ok());
        assert!(admit_persistence_mode(
            None,
            Some(PersistenceMode::Filesystem),
            false,
            Some("kata-vm-isolation"),
        )
        .is_ok());
        // snapshot (default) on gVisor and the cluster default — unchanged paths.
        assert!(admit_persistence_mode(None, None, false, Some("gvisor")).is_ok());
        assert!(admit_persistence_mode(None, None, false, None).is_ok());
    }
}
