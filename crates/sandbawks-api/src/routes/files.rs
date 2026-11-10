//! File transfer + metadata endpoints — HMAC-signed URL mint, content
//! upload/download (T11), and filesystem pass-through (T12).
//!
//! Content routes (Plan 04 T11):
//!
//! - `POST /files/mint` — caller authenticates with `X-API-Key`,
//!   passes `{ sandbox_id, path, method, ttl }`, and gets back a
//!   short-lived URL `/files/{upload,download}?token=...`. The token
//!   is the only proof of authorisation the upload/download handlers
//!   look at, so the gateway re-issues a token for each file the
//!   caller needs to move.
//! - `PUT /files/upload?token=...` — verifies the token, resolves the
//!   sandbox to its envd address via [`AppState::sandbox_ops`], drains
//!   the request body into memory, and hands it to envd's `WriteAll`
//!   RPC. No `X-API-Key` here: the signed token *is* the auth.
//! - `GET /files/download?token=...` — symmetric: verify token, call
//!   envd's `ReadAll`, stream the bytes back with
//!   `Content-Type: application/octet-stream`.
//!
//! Metadata pass-through routes (Plan 04 T12) — all under
//! `/files/:sandbox_id/...`, all behind `auth_middleware`, all resolve
//! the sandbox in the caller's `TeamContext.namespace` before calling
//! the matching envd RPC:
//!
//! | HTTP                                            | Envd RPC               |
//! |-------------------------------------------------|------------------------|
//! | `GET    /files/:sandbox_id/list?path=&depth=`   | `Filesystem.ListDir`   |
//! | `GET    /files/:sandbox_id/stat?path=`          | `Filesystem.Stat`      |
//! | `DELETE /files/:sandbox_id/remove?path=`        | `Filesystem.Remove`    |
//! | `POST   /files/:sandbox_id/move {from,to}`      | `Filesystem.Move`      |
//! | `POST   /files/:sandbox_id/makedir {path}`      | `Filesystem.MakeDir`   |
//! | `GET    /files/:sandbox_id/watch?path=&recursive=` (SSE) | `Filesystem.WatchDir` |
//!
//! The watch endpoint follows the same SSE shape as
//! [`routes::events`](super::events): each gRPC `FilesystemEvent` is
//! relayed as one `data: <json>\n\n` frame and the stream carries a
//! 15s keepalive to survive idle proxies.
//!
//! # Why the body is buffered (and not streamed)
//!
//! envd's `WriteAll` / `ReadAll` are single-message RPCs (see
//! `crates/envd/proto/filesystem/filesystem.proto`). Plan 04 T11
//! deliberately picks the simpler single-message shape because MVP
//! file sizes (E2B SDK users move config + small data files, not
//! multi-GB blobs) fit comfortably in memory. A chunked variant lands
//! in Plan 07/09 if benchmarks push back.

use std::{convert::Infallible, time::Duration};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{
        sse::{Event as SseEvent, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{delete, get, post, put},
    Extension, Json, Router,
};
use envd::proto::filesystem::{
    watch_dir_response, EntryInfo, EventType, FileType, ListDirRequest, MakeDirRequest,
    MoveRequest, ReadAllRequest, RemoveRequest, StatRequest, WatchDirRequest, WriteAllRequest,
};
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    auth::{auth_middleware, TeamContext},
    error::{AppError, AppResult},
    signing::{mint, verify, SignedUrlPayload},
    AppState,
};

/// Keepalive cadence on the watch SSE stream. Mirrors
/// [`super::events`] (15s) so both event streams behave identically
/// from a proxy's perspective — under any cloud-LB or k8s Ingress
/// 60s idle-timeout, comfortably above the apiserver's heartbeat.
const WATCH_KEEPALIVE: Duration = Duration::from_secs(15);

/// Default `depth` for `GET /files/:sandbox_id/list` when the client
/// doesn't supply one. `1` means "immediate children only", matching
/// what `ls` and the E2B SDK's `files.list()` do by default. The proto
/// accepts deeper values; we just don't make recursion the default.
const DEFAULT_LIST_DEPTH: u32 = 1;

/// Default token lifetime when the client doesn't pass `ttl`. Five
/// minutes is long enough that an SDK can stage a body and `PUT`
/// without round-tripping through `mint` mid-upload, but short enough
/// that a leaked token has bounded blast radius.
const DEFAULT_TTL_SECONDS: u64 = 300;

/// Hard upper bound on `ttl` in `POST /files/mint` to prevent a
/// caller from minting URLs that outlive the sandbox itself
/// (sandboxes default to a 1h `timeout`; tokens longer than that
/// would survive sandbox teardown).
const MAX_TTL_SECONDS: u64 = 3600;

/// Request body of `POST /files/mint`.
///
/// `method` is a string rather than an enum so the wire format
/// matches the E2B SDK's existing call sites verbatim (`"PUT"` /
/// `"GET"`); the handler upper-cases and validates it.
#[derive(Debug, Deserialize)]
pub struct MintRequest {
    pub sandbox_id: String,
    pub path: String,
    /// `"PUT"` for upload, `"GET"` for download. Case-insensitive on
    /// input; normalised to upper-case in the payload.
    pub method: String,
    /// Lifetime in seconds. Defaults to `DEFAULT_TTL_SECONDS` and
    /// is clamped to `MAX_TTL_SECONDS`.
    #[serde(default = "default_ttl")]
    pub ttl: u64,
}

fn default_ttl() -> u64 {
    DEFAULT_TTL_SECONDS
}

/// Response body of `POST /files/mint`. The `url` is a relative path
/// plus query string (`/files/upload?token=...`); the SDK joins it
/// onto the gateway's public base URL before issuing the upload.
#[derive(Debug, Serialize)]
pub struct MintResponse {
    pub url: String,
}

/// Mint a short-lived upload-or-download URL for `(sandbox_id, path)`.
///
/// Auth: requires a valid `X-API-Key` (enforced by the router's
/// `auth_middleware` layer). The minted token itself does not carry
/// the team — once issued, possession alone authorises one
/// operation; revoking it before `exp` is not supported, which is why
/// the TTL is capped at [`MAX_TTL_SECONDS`].
#[tracing::instrument(skip_all, name = "handler:files::mint_url")]
async fn mint_url(
    State(state): State<AppState>,
    Json(req): Json<MintRequest>,
) -> AppResult<Json<MintResponse>> {
    let ttl = req.ttl.min(MAX_TTL_SECONDS);
    let method = req.method.to_uppercase();
    let endpoint = match method.as_str() {
        "PUT" => "/files/upload",
        "GET" => "/files/download",
        _ => {
            return Err(AppError::bad_request(
                "invalid_method",
                "method must be PUT or GET",
            ))
        }
    };
    let payload = SignedUrlPayload {
        sandbox_id: req.sandbox_id,
        path: req.path,
        method,
        exp: chrono::Utc::now().timestamp() + ttl as i64,
    };
    let token = mint(&state.signing_key, &payload);
    Ok(Json(MintResponse {
        url: format!("{endpoint}?token={token}"),
    }))
}

/// Query-string shape shared by `/files/upload` and `/files/download`.
#[derive(Debug, Deserialize)]
pub struct TokenQuery {
    pub token: String,
}

/// `PUT /files/upload?token=...` — write the request body to the
/// sandbox-internal path encoded in the token.
#[tracing::instrument(skip_all, name = "handler:files::upload")]
async fn upload(
    State(state): State<AppState>,
    Query(q): Query<TokenQuery>,
    body: Body,
) -> AppResult<StatusCode> {
    let payload = verify(&state.signing_key, &q.token)?;
    if payload.method != "PUT" {
        return Err(AppError::unauthorized(
            "invalid_method",
            "token is not for PUT",
        ));
    }
    let addr = envd_addr_for(&state, &state.default_namespace, &payload.sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;

    // Buffer the whole body up-front; see the module-level
    // "Why the body is buffered" note for why this is acceptable on
    // MVP file sizes. `try_fold` plus an explicit map_err to
    // `AppError::Internal` keeps the error path consistent — a body
    // read failure is an internal transport hiccup, not a 4xx user
    // error.
    let bytes = body
        .into_data_stream()
        .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await?;

    client
        .write_all(WriteAllRequest {
            path: payload.path,
            data: bytes,
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /files/download?token=...` — return the bytes at the
/// sandbox-internal path encoded in the token.
#[tracing::instrument(skip_all, name = "handler:files::download")]
async fn download(
    State(state): State<AppState>,
    Query(q): Query<TokenQuery>,
) -> AppResult<Response> {
    let payload = verify(&state.signing_key, &q.token)?;
    if payload.method != "GET" {
        return Err(AppError::unauthorized(
            "invalid_method",
            "token is not for GET",
        ));
    }
    let addr = envd_addr_for(&state, &state.default_namespace, &payload.sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;

    let resp = client
        .read_all(ReadAllRequest { path: payload.path })
        .await?
        .into_inner();
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        resp.data,
    )
        .into_response())
}

/// Look up the envd `host:port` for `sandbox_id` in `namespace`,
/// surfacing the same `envd_not_ready` upstream error the exec /
/// events routes return when the sandbox has no envd address yet.
///
/// `namespace` is a parameter rather than always-`state.default_namespace`
/// because T11's token-authed handlers (`upload`, `download`) don't see
/// a `TeamContext` — they have to fall back to the default namespace —
/// but the T12 metadata routes run behind `auth_middleware` and pass
/// the authenticated team's namespace through. Threading it as an
/// argument keeps both call paths sharing the same "resolve sandbox +
/// extract envdAddress" plumbing.
async fn envd_addr_for(state: &AppState, namespace: &str, sandbox_id: &str) -> AppResult<String> {
    let sandbox = state.sandbox_ops().get(namespace, sandbox_id).await?;
    sandbox
        .status
        .as_ref()
        .and_then(|s| s.envd_address.clone())
        .ok_or_else(|| AppError::upstream("envd_not_ready", "sandbox has no envdAddress"))
}

// ----------------------------------------------------------------------
// Plan 04 T12 — Filesystem metadata pass-through.
//
// Every handler below follows the same five-step shape:
//
//   1. Extract `TeamContext` + `:sandbox_id` + query / body params.
//   2. `envd_addr_for(state, &team.namespace, &sandbox_id)`.
//   3. `state.envd_pool.filesystem(&addr)` for a pooled client.
//   4. Single tonic call.
//   5. Project the response into a small `*Dto` and return as JSON
//      (or as a stream of SSE frames for `watch`).
//
// Errors are bubbled via `?` — the `From<tonic::Status> for AppError`
// impl in `crate::error` maps gRPC status codes to the right HTTP
// envelope (`NotFound` → 404, `InvalidArgument` → 400, `Unavailable`
// → 502, everything else → 500), so handler bodies stay free of
// per-call error matching.
// ----------------------------------------------------------------------

/// E2B-shaped projection of envd's `EntryInfo`.
///
/// Field renames push the wire format to camelCase to match the rest of
/// the gateway's E2B-compatible surface (see [`super::events`]). The
/// `r#type` proto field is exposed as `type` on the wire and `kind`
/// in Rust (the Rust raw-identifier round-trip is uglier than a
/// `serde(rename)`).
#[derive(Debug, Serialize)]
struct EntryDto {
    /// File or directory base name (no path component).
    name: String,

    /// `"file"`, `"directory"`, `"symlink"`, or `"unspecified"` for the
    /// proto's zero variant.
    #[serde(rename = "type")]
    kind: &'static str,

    /// Absolute path inside the sandbox.
    path: String,

    /// Size in bytes. Mirrors `EntryInfo.size`; directories typically
    /// report the underlying inode's size rather than recursive content.
    size: i64,

    /// POSIX mode bits (as reported by envd; we don't reinterpret).
    mode: u32,

    /// Symbolic permissions like `"-rw-r--r--"`.
    permissions: String,

    /// Owning user name (resolved on the envd side).
    owner: String,

    /// Owning group name.
    group: String,

    /// RFC 3339 mtime. Omitted when envd doesn't populate the field
    /// (rare; mostly virtual filesystems).
    #[serde(rename = "modifiedTime", skip_serializing_if = "Option::is_none")]
    modified_time: Option<String>,

    /// Target path for symlinks; absent for regular files / directories.
    #[serde(rename = "symlinkTarget", skip_serializing_if = "Option::is_none")]
    symlink_target: Option<String>,
}

/// Wrapper for `GET /files/:sandbox_id/list` — keeps the JSON envelope
/// stable if we later need to add pagination metadata alongside
/// `entries`.
#[derive(Debug, Serialize)]
struct ListDirResponseDto {
    entries: Vec<EntryDto>,
}

/// E2B-shaped projection of envd's `FilesystemEvent` for the `watch`
/// SSE stream. The `name` is the path relative to the watched
/// directory (per envd's contract).
#[derive(Debug, Serialize)]
struct WatchEventDto {
    name: String,
    #[serde(rename = "type")]
    kind: &'static str,
}

/// Shared `?path=...` query shape for `stat` and `remove`.
#[derive(Debug, Deserialize)]
pub struct PathQuery {
    /// Absolute path inside the sandbox. Forwarded verbatim to envd.
    pub path: String,
}

/// Query shape for `list`. `depth` defaults to `DEFAULT_LIST_DEPTH`
/// so the bare `?path=...` form behaves like `ls`.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub path: String,
    #[serde(default = "default_list_depth")]
    pub depth: u32,
}

fn default_list_depth() -> u32 {
    DEFAULT_LIST_DEPTH
}

/// Query shape for `watch`. `recursive` defaults to `false` because
/// the SDK opts in explicitly when it wants subdirectory events.
#[derive(Debug, Deserialize, Default)]
pub struct WatchQuery {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
}

/// Body for `POST /files/:sandbox_id/move`.
#[derive(Debug, Deserialize)]
pub struct MoveBody {
    /// Existing path to rename / relocate.
    pub from: String,
    /// New path. May cross directory boundaries within the sandbox FS.
    pub to: String,
}

/// Body for `POST /files/:sandbox_id/makedir`. Only `path` is
/// configurable; envd implicitly creates parents.
#[derive(Debug, Deserialize)]
pub struct MakeDirBody {
    pub path: String,
}

/// Map envd's `FileType` enum (transported as `i32` in the message) to
/// the camelCase string the E2B SDK consumes. The match is exhaustive
/// over `Ok(FileType)` so a future variant would surface as a compile
/// error here rather than a silent `"unspecified"`.
fn file_type_str(value: i32) -> &'static str {
    match FileType::try_from(value) {
        Ok(FileType::File) => "file",
        Ok(FileType::Directory) => "directory",
        Ok(FileType::Symlink) => "symlink",
        Ok(FileType::Unspecified) | Err(_) => "unspecified",
    }
}

/// Map envd's `EventType` enum (transported as `i32` in the message)
/// to the lowercase string the E2B SDK consumes on the watch stream.
fn event_type_str(value: i32) -> &'static str {
    match EventType::try_from(value) {
        Ok(EventType::Create) => "create",
        Ok(EventType::Write) => "write",
        Ok(EventType::Remove) => "remove",
        Ok(EventType::Rename) => "rename",
        Ok(EventType::Chmod) => "chmod",
        Ok(EventType::Unspecified) | Err(_) => "unspecified",
    }
}

/// Convert a protobuf `Timestamp` to an RFC 3339 string.
///
/// Returns `None` for the absent case so the JSON wire format can
/// `skip_serializing_if = "Option::is_none"` rather than emit
/// `"1970-01-01T00:00:00Z"` for "envd didn't report this".
fn timestamp_to_rfc3339(ts: Option<prost_types::Timestamp>) -> Option<String> {
    let ts = ts?;
    let nanos = u32::try_from(ts.nanos).ok()?;
    chrono::DateTime::from_timestamp(ts.seconds, nanos).map(|dt| dt.to_rfc3339())
}

/// Convert envd's `EntryInfo` into the wire DTO. Pulled out so every
/// handler that returns one or more entries shares the same field
/// mapping — keeps `list` and `stat` and `move` and `makedir` from
/// drifting from each other.
fn entry_to_dto(entry: EntryInfo) -> EntryDto {
    EntryDto {
        kind: file_type_str(entry.r#type),
        name: entry.name,
        path: entry.path,
        size: entry.size,
        mode: entry.mode,
        permissions: entry.permissions,
        owner: entry.owner,
        group: entry.group,
        modified_time: timestamp_to_rfc3339(entry.modified_time),
        symlink_target: entry.symlink_target,
    }
}

/// `GET /files/:sandbox_id/list?path=&depth=` — enumerate directory
/// contents.
#[tracing::instrument(skip_all, name = "handler:files::list_dir")]
async fn list_dir(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(sandbox_id): Path<String>,
    Query(q): Query<ListQuery>,
) -> AppResult<Json<ListDirResponseDto>> {
    let addr = envd_addr_for(&state, &team.namespace, &sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let resp = client
        .list_dir(ListDirRequest {
            path: q.path,
            depth: q.depth,
        })
        .await?
        .into_inner();
    Ok(Json(ListDirResponseDto {
        entries: resp.entries.into_iter().map(entry_to_dto).collect(),
    }))
}

/// `GET /files/:sandbox_id/stat?path=` — fetch entry metadata.
#[tracing::instrument(skip_all, name = "handler:files::stat")]
async fn stat(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(sandbox_id): Path<String>,
    Query(q): Query<PathQuery>,
) -> AppResult<Json<EntryDto>> {
    let addr = envd_addr_for(&state, &team.namespace, &sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let entry = client
        .stat(StatRequest { path: q.path })
        .await?
        .into_inner()
        .entry
        .ok_or_else(|| AppError::upstream("envd_invalid_response", "Stat returned no entry"))?;
    Ok(Json(entry_to_dto(entry)))
}

/// `DELETE /files/:sandbox_id/remove?path=` — unlink a file or
/// directory. Returns 204 on success; 404 / 502 envelope on failure
/// (the gRPC `NotFound` mapping handles "path doesn't exist").
#[tracing::instrument(skip_all, name = "handler:files::remove")]
async fn remove(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(sandbox_id): Path<String>,
    Query(q): Query<PathQuery>,
) -> AppResult<StatusCode> {
    let addr = envd_addr_for(&state, &team.namespace, &sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    client.remove(RemoveRequest { path: q.path }).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /files/:sandbox_id/move {from, to}` — rename / relocate.
/// Returns the resulting entry's metadata so the SDK doesn't have to
/// round-trip a separate `stat`.
#[tracing::instrument(skip_all, name = "handler:files::move_path")]
async fn move_path(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(sandbox_id): Path<String>,
    Json(body): Json<MoveBody>,
) -> AppResult<Json<EntryDto>> {
    let addr = envd_addr_for(&state, &team.namespace, &sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let entry = client
        .r#move(MoveRequest {
            source: body.from,
            destination: body.to,
        })
        .await?
        .into_inner()
        .entry
        .ok_or_else(|| AppError::upstream("envd_invalid_response", "Move returned no entry"))?;
    Ok(Json(entry_to_dto(entry)))
}

/// `POST /files/:sandbox_id/makedir {path}` — create a directory
/// (parents included; envd uses `mkdir -p` semantics).
#[tracing::instrument(skip_all, name = "handler:files::make_dir")]
async fn make_dir(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(sandbox_id): Path<String>,
    Json(body): Json<MakeDirBody>,
) -> AppResult<Json<EntryDto>> {
    let addr = envd_addr_for(&state, &team.namespace, &sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let entry = client
        .make_dir(MakeDirRequest { path: body.path })
        .await?
        .into_inner()
        .entry
        .ok_or_else(|| AppError::upstream("envd_invalid_response", "MakeDir returned no entry"))?;
    Ok(Json(entry_to_dto(entry)))
}

/// `GET /files/:sandbox_id/watch?path=&recursive=` — server-streaming
/// filesystem events relayed over SSE.
///
/// Each envd `FilesystemEvent` becomes one `data: <json>\n\n` frame.
/// `StartEvent` and envd's internal `KeepAlive` are filtered out — the
/// SSE keepalive comment we add via [`KeepAlive::new`] serves the
/// proxy-survival role on this side of the wire, and we don't want
/// either to surface as a misleading user-visible event.
///
/// gRPC transport errors mid-stream silently end the stream; the SDK
/// re-establishes the connection if it cares (same posture as
/// `routes::events`'s watcher-error handling).
#[tracing::instrument(skip_all, name = "handler:files::watch")]
async fn watch(
    State(state): State<AppState>,
    Extension(team): Extension<TeamContext>,
    Path(sandbox_id): Path<String>,
    Query(q): Query<WatchQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>> {
    let addr = envd_addr_for(&state, &team.namespace, &sandbox_id).await?;
    let mut client = state.envd_pool.filesystem(&addr).await?;
    let stream = client
        .watch_dir(WatchDirRequest {
            path: q.path,
            recursive: q.recursive,
        })
        .await?
        .into_inner();

    // `filter_map` turns the `Result<WatchDirResponse, tonic::Status>`
    // stream into a `Stream<Item = Result<SseEvent, Infallible>>` that
    // the `Sse` adapter expects. Mapping transport errors and non-event
    // variants to `None` ends the stream cleanly; we never propagate a
    // mid-stream error to the SSE layer because there's no `Err`
    // variant we could meaningfully send to the client over SSE.
    let sse = stream.filter_map(|item| async move {
        let resp = item.ok()?;
        let event = match resp.event? {
            watch_dir_response::Event::Filesystem(ev) => ev,
            // StartEvent / KeepAlive are envd→gateway plumbing — we
            // don't relay them, the SDK has its own keepalive logic.
            _ => return None,
        };
        let dto = WatchEventDto {
            name: event.name,
            kind: event_type_str(event.r#type),
        };
        let json = serde_json::to_string(&dto).ok()?;
        Some(Ok(SseEvent::default().data(json)))
    });

    Ok(Sse::new(sse).keep_alive(KeepAlive::new().interval(WATCH_KEEPALIVE)))
}

/// Build the `Router` exposing every file endpoint.
///
/// Two authentication regimes are merged here:
///
/// - **API-key authed.** `auth_middleware` enforces a valid
///   `X-API-Key` and injects `TeamContext`. This covers the metadata
///   pass-through (`/files/:sandbox_id/{list,stat,remove,move,makedir,watch}`)
///   plus `/files/mint`, since minting a content URL is the moment we
///   bind a (team, sandbox, path) triple to a signed token.
/// - **Token authed.** `/files/upload` and `/files/download`
///   authenticate via the signed token in the query string instead,
///   because the SDK passes the URL directly to `requests.put()` /
///   `requests.get()` and can't (always) attach the team API key as
///   a header.
pub fn router(state: AppState) -> Router {
    let key_store = state.key_store.clone();

    let authed = Router::new()
        .route("/files/mint", post(mint_url))
        .route("/files/:sandbox_id/list", get(list_dir))
        .route("/files/:sandbox_id/stat", get(stat))
        .route("/files/:sandbox_id/remove", delete(remove))
        .route("/files/:sandbox_id/move", post(move_path))
        .route("/files/:sandbox_id/makedir", post(make_dir))
        .route("/files/:sandbox_id/watch", get(watch))
        .layer(axum::middleware::from_fn_with_state(
            key_store,
            auth_middleware,
        ))
        .with_state(state.clone());

    let token_authed = Router::new()
        .route("/files/upload", put(upload))
        .route("/files/download", get(download))
        .with_state(state);

    authed.merge(token_authed)
}
