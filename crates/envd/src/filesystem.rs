//! Server-side implementation of the `filesystem` gRPC service.
//!
//! Mounts at a sandbox-internal root and serves the metadata RPCs defined
//! in `proto/filesystem/filesystem.proto` against the local filesystem
//! using `tokio::fs`. Filesystem watching is backed by the `notify`
//! crate's `RecommendedWatcher` (inotify on Linux).
//!
//! # File content transfer
//!
//! Upstream E2B's `filesystem` proto carries metadata RPCs only and
//! tunnels file content over HTTP. sandbawks instead exposes two
//! additive RPCs on the same `Filesystem` service — `WriteAll` and
//! `ReadAll` — so the gateway can stream bytes server-side after
//! verifying an HMAC-signed URL (Plan 04 T11). The extension is
//! recorded in `proto/SOURCE` / `proto/NOTICE`; existing upstream
//! RPCs and their field tags are unchanged, so the published E2B
//! SDKs continue to interoperate with the metadata surface. The
//! single-message bodies (no streaming) are sufficient for MVP file
//! sizes — Plan 07/09 can land a chunked variant if benchmarks call
//! for it.
//!
//! # Path-traversal guard
//!
//! Every request path is funneled through `FilesystemService::resolve`,
//! which strips a leading `/`, canonicalizes the deepest existing
//! ancestor, and rejects any result that does not start with the
//! service's root. This stops `../etc/passwd`-style escapes regardless
//! of intermediate symlinks. New paths (e.g., the target of `MakeDir`)
//! canonicalize their parent rather than the leaf, so a symlink in the
//! prefix is still followed but the not-yet-created leaf can still be
//! computed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::SystemTime;

use futures::Stream;
use notify::{event::ModifyKind, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

use crate::proto::filesystem::filesystem_server::Filesystem;
use crate::proto::filesystem::{
    watch_dir_response, CreateWatcherRequest, CreateWatcherResponse, EntryInfo, EventType,
    FileType, FilesystemEvent, GetWatcherEventsRequest, GetWatcherEventsResponse, ListDirRequest,
    ListDirResponse, MakeDirRequest, MakeDirResponse, MoveRequest, MoveResponse, ReadAllRequest,
    ReadAllResponse, RemoveRequest, RemoveResponse, RemoveWatcherRequest, RemoveWatcherResponse,
    StatRequest, StatResponse, WatchDirRequest, WatchDirResponse, WriteAllRequest,
    WriteAllResponse,
};
use crate::state::WatchSnapshot;

/// A registered non-streaming watcher.
///
/// `_watcher` is held only to keep the underlying inotify subscription
/// alive — drop it and notify stops delivering events. `events` is the
/// callback's drop target; `GetWatcherEvents` drains it. `path` and
/// `subscriber_id` are duplicated from the registration call so the
/// snapshot path ([`FilesystemService::collect_watch_snapshots`]) can
/// surface them without re-parsing the watcher_id string.
struct WatcherEntry {
    _watcher: RecommendedWatcher,
    events: Arc<StdMutex<Vec<FilesystemEvent>>>,
    /// Request-side path the client registered for. Stored verbatim
    /// (not `resolve`d) so the snapshot is portable across resume — a
    /// host-side canonical path would embed the old envd's mount
    /// prefix and break a resumed lookup.
    path: PathBuf,
    /// Numeric id corresponding to the registration's `watcher_id`
    /// string (`w{id}`). Kept separately so the snapshot's
    /// `subscriber_id: u64` doesn't need to re-parse the prefix.
    subscriber_id: u64,
}

/// gRPC `Filesystem` service rooted at a single directory.
///
/// Every RPC's path argument is resolved relative to this root and
/// guarded against `../` traversal before any I/O happens. The same
/// instance backs both the streaming `WatchDir` RPC and the polling
/// `CreateWatcher` / `GetWatcherEvents` / `RemoveWatcher` trio, sharing
/// a `watchers` map keyed by an opaque id minted from
/// `next_watcher_id`.
///
/// `Clone` is cheap (every field is `Arc`-wrapped or a small owned
/// `PathBuf` / `Vec`) so the binary's HTTP `/__state` handler can hold
/// a snapshot-capture handle in parallel with the gRPC server.
#[derive(Clone)]
pub struct FilesystemService {
    root: PathBuf,
    watchers: Arc<Mutex<HashMap<String, WatcherEntry>>>,
    /// Monotonic source for [`WatcherEntry::subscriber_id`]. Behind
    /// `Arc` so clones share the same counter — two service handles
    /// must not re-issue the same id.
    next_watcher_id: Arc<AtomicU64>,
    /// Boot-time filesystem watch snapshots restored from
    /// `envd-state.json`.
    ///
    /// Read-only for the life of the service. Exposed via
    /// [`Self::boot_seed_watches`] for the future rehydration path
    /// (re-arming inotify subscriptions on resume; see issue #178).
    /// Deliberately **not** blended into
    /// [`Self::collect_watch_snapshots`] — if it were, each pause/resume
    /// cycle would persist `live + seed` back out, and the seed would
    /// grow monotonically across cycles (issue #178).
    seeded_watches: Arc<Vec<WatchSnapshot>>,
}

impl FilesystemService {
    /// Construct a service rooted at `root`.
    ///
    /// `root` is canonicalized eagerly so the traversal guard has a
    /// stable absolute prefix to compare against; any symlinks in
    /// `root` itself are resolved exactly once at construction.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error if `root` does not exist or
    /// cannot be canonicalized (e.g., permission denied on a parent).
    pub fn new(root: PathBuf) -> std::io::Result<Self> {
        Self::with_seed(root, Vec::new())
    }

    /// Construct a service pre-seeded with boot-time watch snapshots.
    ///
    /// Used by the binary entrypoint when `--state-file` exists on
    /// boot. The seed is held read-only for the life of the service
    /// and is exposed via [`Self::boot_seed_watches`] for the future
    /// rehydration path (re-arming inotify subscriptions on resume).
    ///
    /// The seed is **not** blended into [`Self::collect_watch_snapshots`]
    /// — that would persist `live + seed` on shutdown and grow the
    /// snapshot monotonically across pause/resume cycles. See issue
    /// #178 for the lifecycle rationale.
    pub fn with_seed(root: PathBuf, seeded_watches: Vec<WatchSnapshot>) -> std::io::Result<Self> {
        let root = std::fs::canonicalize(&root)?;
        // Start the subscriber-id counter past the largest seeded id so
        // a fresh `CreateWatcher` can never collide with a snapshot
        // entry the previous envd already minted. `+1` over `max`
        // (defaulting to 0 when the seed is empty) keeps the first
        // fresh id at 1. The collision concern survives even though the
        // seed no longer appears in `collect_watch_snapshots`: future
        // rehydration code will re-arm watchers under the seeded ids,
        // and a concurrent fresh `CreateWatcher` must not race onto the
        // same id.
        let next_id = seeded_watches
            .iter()
            .map(|s| s.subscriber_id)
            .max()
            .unwrap_or(0)
            + 1;
        Ok(Self {
            root,
            watchers: Arc::default(),
            next_watcher_id: Arc::new(AtomicU64::new(next_id)),
            seeded_watches: Arc::new(seeded_watches),
        })
    }

    /// Read-only view of the boot-time watch seed.
    ///
    /// Returns the snapshots originally passed to [`Self::with_seed`],
    /// untouched by any live watcher churn. Empty for services built
    /// via [`Self::new`].
    ///
    /// Future rehydration code reads this to re-arm inotify
    /// subscriptions from the pre-pause record. All other call sites —
    /// `/__state`, the shutdown snapshot writer — must use
    /// [`Self::collect_watch_snapshots`] instead, which reports live
    /// state only.
    pub fn boot_seed_watches(&self) -> &[WatchSnapshot] {
        &self.seeded_watches
    }

    /// Walk the live watcher registry and return a fresh vector of
    /// [`WatchSnapshot`] entries.
    ///
    /// Boot-time seed entries are deliberately **not** included here —
    /// see [`Self::with_seed`] for the lifecycle rationale (issue #178).
    /// Callers that need the pre-pause record should read
    /// [`Self::boot_seed_watches`] directly.
    ///
    /// Ordering is `(subscriber_id, path)` ascending so the JSON
    /// encoding stays byte-comparable across probes.
    pub async fn collect_watch_snapshots(&self) -> Vec<WatchSnapshot> {
        let watchers = self.watchers.lock().await;
        let mut snapshots: Vec<WatchSnapshot> = watchers
            .values()
            .map(|e| WatchSnapshot {
                path: e.path.clone(),
                subscriber_id: e.subscriber_id,
            })
            .collect();
        drop(watchers);
        snapshots.sort_by(|a, b| {
            a.subscriber_id
                .cmp(&b.subscriber_id)
                .then_with(|| a.path.cmp(&b.path))
        });
        snapshots
    }

    /// Map a request-side path to an absolute server path under
    /// `self.root`, rejecting traversal escapes.
    ///
    /// Strips one leading `/` so request paths can use a sandbox-style
    /// absolute form (`/home/user/foo`) while still resolving under
    /// the (potentially different) host-side root we mounted. If the
    /// leaf does not exist yet — e.g. `MakeDir`'s target — the deepest
    /// existing ancestor is canonicalized and the missing tail is
    /// re-attached.
    // `tonic::Status` is the canonical error type for every gRPC handler in
    // this service; the trait we implement forces it. Boxing it just here
    // would clash with that contract and add an alloc on every hop.
    #[allow(clippy::result_large_err)]
    fn resolve(&self, raw: &str) -> Result<PathBuf, Status> {
        let trimmed = raw.trim_start_matches('/');
        let candidate = self.root.join(trimmed);

        let canonical = if candidate.exists() {
            candidate.canonicalize().map_err(io_to_status)?
        } else {
            // Collect trailing components into a `Vec` and re-push them
            // onto the canonicalized ancestor at the end. The earlier
            // `Path::new(name).join(&missing)` form silently added a
            // trailing `/` when `missing` was empty, which made
            // `rename` then refuse the operation with ENOTDIR.
            let mut ancestor = candidate.as_path();
            let mut missing: Vec<&std::ffi::OsStr> = Vec::new();
            while !ancestor.exists() {
                match (ancestor.parent(), ancestor.file_name()) {
                    (Some(p), Some(name)) => {
                        missing.push(name);
                        ancestor = p;
                    }
                    _ => return Err(Status::permission_denied("path escapes root")),
                }
            }
            let mut canonical = ancestor.canonicalize().map_err(io_to_status)?;
            for name in missing.iter().rev() {
                canonical.push(*name);
            }
            canonical
        };

        if !canonical.starts_with(&self.root) {
            return Err(Status::permission_denied("path escapes root"));
        }
        Ok(canonical)
    }

    /// Build an `EntryInfo` for a server-side path, mapping the path
    /// back into the request-side (`/`-rooted) namespace so clients
    /// don't see the host mount prefix.
    fn entry_for(&self, path: &Path, md: &std::fs::Metadata) -> EntryInfo {
        use std::os::unix::fs::PermissionsExt;

        let kind = if md.file_type().is_symlink() {
            FileType::Symlink
        } else if md.is_dir() {
            FileType::Directory
        } else if md.is_file() {
            FileType::File
        } else {
            FileType::Unspecified
        };

        let mode = md.permissions().mode();
        let permissions = unix_permissions_string(mode);
        let modified_time = md.modified().ok().map(systemtime_to_timestamp);
        let symlink_target = if md.file_type().is_symlink() {
            std::fs::read_link(path)
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        } else {
            None
        };

        let displayed_path = match path.strip_prefix(&self.root) {
            Ok(rel) if rel.as_os_str().is_empty() => "/".to_string(),
            Ok(rel) => format!("/{}", rel.to_string_lossy()),
            Err(_) => path.to_string_lossy().into_owned(),
        };
        EntryInfo {
            name: path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            r#type: kind as i32,
            path: displayed_path,
            size: md.len() as i64,
            // The high bits of `mode` encode the file-type (S_IFDIR
            // etc.); the proto's `mode` field is the POSIX permission
            // word, so mask down to the bottom 12 bits.
            mode: mode & 0o7777,
            permissions,
            owner: String::new(),
            group: String::new(),
            modified_time,
            symlink_target,
        }
    }
}

/// Translate a `std::io::Error` into the closest `tonic::Status` code.
///
/// Anything that doesn't have a clean mapping bubbles up as
/// `Status::internal` — callers see the raw kernel error text, which is
/// fine for the in-pod boundary we control on both sides.
fn io_to_status(e: std::io::Error) -> Status {
    match e.kind() {
        std::io::ErrorKind::NotFound => Status::not_found(e.to_string()),
        std::io::ErrorKind::PermissionDenied => Status::permission_denied(e.to_string()),
        std::io::ErrorKind::AlreadyExists => Status::already_exists(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

/// Render a Unix mode word as the 9-character `rwxr-xr-x` form.
fn unix_permissions_string(mode: u32) -> String {
    let mut s = String::with_capacity(9);
    for shift in [6u32, 3, 0] {
        let bits = (mode >> shift) & 0o7;
        s.push(if bits & 0o4 != 0 { 'r' } else { '-' });
        s.push(if bits & 0o2 != 0 { 'w' } else { '-' });
        s.push(if bits & 0o1 != 0 { 'x' } else { '-' });
    }
    s
}

/// Convert a `SystemTime` into the protobuf `Timestamp` shape.
fn systemtime_to_timestamp(t: SystemTime) -> prost_types::Timestamp {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => prost_types::Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        },
        Err(_) => prost_types::Timestamp::default(),
    }
}

/// Map a `notify::EventKind` to the proto's coarser `EventType`. Events
/// we don't understand (`EventKind::Access`, `EventKind::Other`,
/// `EventKind::Any`) are filtered out by returning `None`.
fn notify_kind_to_event_type(kind: &EventKind) -> Option<EventType> {
    match kind {
        EventKind::Create(_) => Some(EventType::Create),
        EventKind::Modify(ModifyKind::Name(_)) => Some(EventType::Rename),
        EventKind::Modify(ModifyKind::Metadata(_)) => Some(EventType::Chmod),
        EventKind::Modify(_) => Some(EventType::Write),
        EventKind::Remove(_) => Some(EventType::Remove),
        _ => None,
    }
}

#[async_trait]
impl Filesystem for FilesystemService {
    #[tracing::instrument(skip_all, name = "handler:filesystem::stat")]
    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let req = request.into_inner();
        let path = self.resolve(&req.path)?;
        // `symlink_metadata` so `stat("/some/symlink")` reports the
        // symlink itself rather than its target; `entry_for` then
        // populates `symlink_target` from `read_link`.
        let md = tokio::fs::symlink_metadata(&path)
            .await
            .map_err(io_to_status)?;
        Ok(Response::new(StatResponse {
            entry: Some(self.entry_for(&path, &md)),
        }))
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::make_dir")]
    async fn make_dir(
        &self,
        request: Request<MakeDirRequest>,
    ) -> Result<Response<MakeDirResponse>, Status> {
        let req = request.into_inner();
        let call_id = crate::gen_ai::next_call_id();
        let args = crate::gen_ai::capture_content()
            .then(|| crate::gen_ai::redact_path(&req.path).into_owned());
        crate::gen_ai::instrument_tool("fs.make_dir", call_id, args.as_deref(), async move {
            let path = self.resolve(&req.path)?;
            // `create_dir_all` mirrors what the upstream E2B SDK's
            // `files.makeDir` does — it creates parent dirs as needed
            // rather than erroring on a missing prefix.
            tokio::fs::create_dir_all(&path)
                .await
                .map_err(io_to_status)?;
            let md = tokio::fs::symlink_metadata(&path)
                .await
                .map_err(io_to_status)?;
            Ok(Response::new(MakeDirResponse {
                entry: Some(self.entry_for(&path, &md)),
            }))
        })
        .await
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::move")]
    async fn r#move(
        &self,
        request: Request<MoveRequest>,
    ) -> Result<Response<MoveResponse>, Status> {
        let req = request.into_inner();
        let call_id = crate::gen_ai::next_call_id();
        let args = crate::gen_ai::capture_content().then(|| {
            format!(
                "{} -> {}",
                crate::gen_ai::redact_path(&req.source),
                crate::gen_ai::redact_path(&req.destination)
            )
        });
        crate::gen_ai::instrument_tool("fs.move", call_id, args.as_deref(), async move {
            let src = self.resolve(&req.source)?;
            let dst = self.resolve(&req.destination)?;
            tokio::fs::rename(&src, &dst).await.map_err(io_to_status)?;
            let md = tokio::fs::symlink_metadata(&dst)
                .await
                .map_err(io_to_status)?;
            Ok(Response::new(MoveResponse {
                entry: Some(self.entry_for(&dst, &md)),
            }))
        })
        .await
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::list_dir")]
    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirResponse>, Status> {
        let req = request.into_inner();
        let path = self.resolve(&req.path)?;
        // The proto's `depth` field is honored as "0/1 = direct
        // children only"; deeper recursive listings aren't part of the
        // MVP and can be added when a caller needs them.
        let mut entries = Vec::new();
        let mut rd = tokio::fs::read_dir(&path).await.map_err(io_to_status)?;
        while let Some(child) = rd.next_entry().await.map_err(io_to_status)? {
            let cp = child.path();
            // Per-entry metadata read can race against concurrent
            // deletes; skip the entry rather than failing the whole
            // listing if the child has vanished mid-iteration.
            let md = match tokio::fs::symlink_metadata(&cp).await {
                Ok(m) => m,
                Err(_) => continue,
            };
            entries.push(self.entry_for(&cp, &md));
        }
        Ok(Response::new(ListDirResponse { entries }))
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::remove")]
    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        let req = request.into_inner();
        let call_id = crate::gen_ai::next_call_id();
        let args = crate::gen_ai::capture_content()
            .then(|| crate::gen_ai::redact_path(&req.path).into_owned());
        crate::gen_ai::instrument_tool("fs.remove", call_id, args.as_deref(), async move {
            let path = self.resolve(&req.path)?;
            let md = tokio::fs::symlink_metadata(&path)
                .await
                .map_err(io_to_status)?;
            if md.is_dir() {
                // `remove_dir` rather than `remove_dir_all` — per the issue
                // scope, Remove only handles empty directories. Recursive
                // delete is a separate concern; let it error noisily here.
                tokio::fs::remove_dir(&path).await.map_err(io_to_status)?;
            } else {
                tokio::fs::remove_file(&path).await.map_err(io_to_status)?;
            }
            Ok(Response::new(RemoveResponse {}))
        })
        .await
    }

    type WatchDirStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<WatchDirResponse, Status>> + Send>>;

    #[tracing::instrument(skip_all, name = "handler:filesystem::watch_dir")]
    async fn watch_dir(
        &self,
        request: Request<WatchDirRequest>,
    ) -> Result<Response<Self::WatchDirStream>, Status> {
        let req = request.into_inner();
        let path = self.resolve(&req.path)?;
        let mode = if req.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };

        // 128-slot channel is the same buffer size tonic itself uses
        // for server-streaming RPCs; events beyond that backpressure
        // the watcher callback rather than dropping silently.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<WatchDirResponse, Status>>(128);

        // Per the proto's WatchDirResponse oneof, emit a `Start` event
        // immediately so clients can synchronize "stream is live" with
        // their own setup before triggering filesystem activity.
        tx.send(Ok(WatchDirResponse {
            event: Some(watch_dir_response::Event::Start(
                watch_dir_response::StartEvent {},
            )),
        }))
        .await
        .map_err(|_| Status::internal("client dropped before start"))?;

        let tx_for_watch = tx.clone();
        let root = self.root.clone();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            let Ok(ev) = res else {
                return;
            };
            let Some(kind) = notify_kind_to_event_type(&ev.kind) else {
                return;
            };
            for p in ev.paths {
                let name = match p.strip_prefix(&root) {
                    Ok(rel) if rel.as_os_str().is_empty() => "/".to_string(),
                    Ok(rel) => format!("/{}", rel.to_string_lossy()),
                    Err(_) => p.to_string_lossy().into_owned(),
                };
                let resp = WatchDirResponse {
                    event: Some(watch_dir_response::Event::Filesystem(FilesystemEvent {
                        name,
                        r#type: kind as i32,
                    })),
                };
                // `blocking_send` is correct here: this closure
                // runs on `notify`'s dedicated event thread (not a
                // tokio worker), so we are allowed to block on the
                // channel — backpressure is the desired behavior.
                let _ = tx_for_watch.blocking_send(Ok(resp));
            }
        })
        .map_err(|e| Status::internal(format!("watcher init: {e}")))?;

        watcher
            .watch(&path, mode)
            .map_err(|e| Status::internal(format!("watch start: {e}")))?;

        // Hold the watcher alive on a dedicated task; drop it once the
        // client closes the stream (detected when the last `tx` clone
        // is dropped, which happens when this very task exits).
        tokio::spawn(async move {
            tx.closed().await;
            drop(watcher);
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::WatchDirStream))
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::create_watcher")]
    async fn create_watcher(
        &self,
        request: Request<CreateWatcherRequest>,
    ) -> Result<Response<CreateWatcherResponse>, Status> {
        let req = request.into_inner();
        let path = self.resolve(&req.path)?;
        let mode = if req.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };

        let buffer: Arc<StdMutex<Vec<FilesystemEvent>>> = Arc::default();
        let buffer_for_watch = Arc::clone(&buffer);
        let root = self.root.clone();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            let Ok(ev) = res else {
                return;
            };
            let Some(kind) = notify_kind_to_event_type(&ev.kind) else {
                return;
            };
            // Lock failure here would mean another notify callback
            // panicked while holding the buffer; nothing useful we
            // can do besides drop the event, so `expect` is the
            // honest signal.
            let mut buf = buffer_for_watch.lock().expect("watcher buffer poisoned");
            for p in ev.paths {
                let name = match p.strip_prefix(&root) {
                    Ok(rel) if rel.as_os_str().is_empty() => "/".to_string(),
                    Ok(rel) => format!("/{}", rel.to_string_lossy()),
                    Err(_) => p.to_string_lossy().into_owned(),
                };
                buf.push(FilesystemEvent {
                    name,
                    r#type: kind as i32,
                });
            }
        })
        .map_err(|e| Status::internal(format!("watcher init: {e}")))?;
        watcher
            .watch(&path, mode)
            .map_err(|e| Status::internal(format!("watch start: {e}")))?;

        let id = self.next_watcher_id.fetch_add(1, Ordering::Relaxed);
        let watcher_id = format!("w{id}");
        self.watchers.lock().await.insert(
            watcher_id.clone(),
            WatcherEntry {
                _watcher: watcher,
                events: buffer,
                path: PathBuf::from(&req.path),
                subscriber_id: id,
            },
        );
        Ok(Response::new(CreateWatcherResponse { watcher_id }))
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::get_watcher_events")]
    async fn get_watcher_events(
        &self,
        request: Request<GetWatcherEventsRequest>,
    ) -> Result<Response<GetWatcherEventsResponse>, Status> {
        let req = request.into_inner();
        let watchers = self.watchers.lock().await;
        let entry = watchers
            .get(&req.watcher_id)
            .ok_or_else(|| Status::not_found(format!("watcher {} unknown", req.watcher_id)))?;
        let mut buf = entry.events.lock().expect("watcher buffer poisoned");
        let events = std::mem::take(&mut *buf);
        Ok(Response::new(GetWatcherEventsResponse { events }))
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::remove_watcher")]
    async fn remove_watcher(
        &self,
        request: Request<RemoveWatcherRequest>,
    ) -> Result<Response<RemoveWatcherResponse>, Status> {
        let req = request.into_inner();
        self.watchers
            .lock()
            .await
            .remove(&req.watcher_id)
            .ok_or_else(|| Status::not_found(format!("watcher {} unknown", req.watcher_id)))?;
        Ok(Response::new(RemoveWatcherResponse {}))
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::write_all")]
    async fn write_all(
        &self,
        request: Request<WriteAllRequest>,
    ) -> Result<Response<WriteAllResponse>, Status> {
        let req = request.into_inner();
        let call_id = crate::gen_ai::next_call_id();
        let args = crate::gen_ai::capture_content()
            .then(|| crate::gen_ai::redact_path(&req.path).into_owned());
        crate::gen_ai::instrument_tool("fs.write", call_id, args.as_deref(), async move {
            // `resolve` guards against `../` traversal: the path comes from
            // a client-controlled signed URL, so the same chroot logic the
            // metadata RPCs use is the only thing keeping a leaked-key
            // attacker from clobbering files outside the sandbox root.
            let path = self.resolve(&req.path)?;
            tokio::fs::write(&path, &req.data)
                .await
                .map_err(io_to_status)?;
            Ok(Response::new(WriteAllResponse {}))
        })
        .await
    }

    #[tracing::instrument(skip_all, name = "handler:filesystem::read_all")]
    async fn read_all(
        &self,
        request: Request<ReadAllRequest>,
    ) -> Result<Response<ReadAllResponse>, Status> {
        let req = request.into_inner();
        let call_id = crate::gen_ai::next_call_id();
        let args = crate::gen_ai::capture_content()
            .then(|| crate::gen_ai::redact_path(&req.path).into_owned());
        crate::gen_ai::instrument_tool("fs.read", call_id, args.as_deref(), async move {
            let path = self.resolve(&req.path)?;
            let data = tokio::fs::read(&path).await.map_err(io_to_status)?;
            Ok(Response::new(ReadAllResponse { data }))
        })
        .await
    }
}
