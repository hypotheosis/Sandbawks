//! Integration tests for [`envd::filesystem::FilesystemService`].
//!
//! These tests drive the `Filesystem` gRPC trait directly (no transport,
//! no codec) against a temporary directory used as the service root.
//! That keeps the tests fast and deterministic: the surface under test
//! is the handler code, not the gRPC machinery.

use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use tempfile::TempDir;
use tonic::Request;

use envd::filesystem::FilesystemService;
use envd::proto::filesystem::filesystem_server::Filesystem;
use envd::proto::filesystem::{
    watch_dir_response, CreateWatcherRequest, EventType, FileType, GetWatcherEventsRequest,
    ListDirRequest, MakeDirRequest, MoveRequest, ReadAllRequest, RemoveRequest,
    RemoveWatcherRequest, StatRequest, WatchDirRequest, WriteAllRequest,
};

/// Build a service rooted at a fresh tempdir. Returns the service and
/// the `TempDir` guard — the guard must stay alive for the whole test
/// or the directory is removed and `notify` watches start firing
/// spurious Remove events.
fn make_service() -> (FilesystemService, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let svc = FilesystemService::new(dir.path().to_path_buf()).expect("service");
    (svc, dir)
}

/// Resolve a request-side path against a `TempDir` root for the test
/// to write/read directly with `std::fs` (verifying server-side
/// changes show up on disk).
fn host_path(root: &TempDir, p: &str) -> PathBuf {
    root.path().join(p.trim_start_matches('/'))
}

#[tokio::test]
async fn list_nonempty_dir_returns_files_and_subdirs() {
    let (svc, dir) = make_service();
    std::fs::write(host_path(&dir, "alpha.txt"), b"hello").unwrap();
    std::fs::write(host_path(&dir, "beta.bin"), b"world!!").unwrap();
    std::fs::create_dir(host_path(&dir, "nested")).unwrap();

    let resp = svc
        .list_dir(Request::new(ListDirRequest {
            path: "/".into(),
            depth: 0,
        }))
        .await
        .expect("list_dir")
        .into_inner();

    let mut names: Vec<_> = resp.entries.iter().map(|e| e.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["alpha.txt", "beta.bin", "nested"]);

    let nested = resp
        .entries
        .iter()
        .find(|e| e.name == "nested")
        .expect("nested entry");
    assert_eq!(nested.r#type, FileType::Directory as i32);

    let alpha = resp
        .entries
        .iter()
        .find(|e| e.name == "alpha.txt")
        .expect("alpha entry");
    assert_eq!(alpha.r#type, FileType::File as i32);
    assert_eq!(alpha.size, 5);
}

#[tokio::test]
async fn stat_returns_size_and_mtime() {
    let (svc, dir) = make_service();
    let payload = b"twelve bytes";
    std::fs::write(host_path(&dir, "f"), payload).unwrap();

    let entry = svc
        .stat(Request::new(StatRequest { path: "/f".into() }))
        .await
        .expect("stat")
        .into_inner()
        .entry
        .expect("entry");

    assert_eq!(entry.size, payload.len() as i64);
    assert_eq!(entry.r#type, FileType::File as i32);
    assert!(entry.modified_time.is_some(), "mtime should be populated");
    let t = entry.modified_time.unwrap();
    assert!(t.seconds > 0, "mtime should be a real epoch second");
}

#[tokio::test]
async fn remove_file_then_stat_returns_not_found() {
    let (svc, dir) = make_service();
    std::fs::write(host_path(&dir, "doomed"), b"x").unwrap();

    svc.remove(Request::new(RemoveRequest {
        path: "/doomed".into(),
    }))
    .await
    .expect("remove");

    let err = svc
        .stat(Request::new(StatRequest {
            path: "/doomed".into(),
        }))
        .await
        .expect_err("stat after remove must fail");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn mkdir_recursive_creates_chain() {
    let (svc, dir) = make_service();

    svc.make_dir(Request::new(MakeDirRequest {
        path: "/a/b/c".into(),
    }))
    .await
    .expect("make_dir");

    assert!(host_path(&dir, "a").is_dir());
    assert!(host_path(&dir, "a/b").is_dir());
    assert!(host_path(&dir, "a/b/c").is_dir());
}

#[tokio::test]
async fn move_renames_file() {
    let (svc, dir) = make_service();
    std::fs::write(host_path(&dir, "src"), b"contents").unwrap();

    svc.r#move(Request::new(MoveRequest {
        source: "/src".into(),
        destination: "/dst".into(),
    }))
    .await
    .expect("move");

    assert!(!host_path(&dir, "src").exists());
    assert_eq!(std::fs::read(host_path(&dir, "dst")).unwrap(), b"contents");
}

#[tokio::test]
async fn watch_observes_create_modify_delete() {
    let (svc, dir) = make_service();

    let id = svc
        .create_watcher(Request::new(CreateWatcherRequest {
            path: "/".into(),
            recursive: false,
        }))
        .await
        .expect("create_watcher")
        .into_inner()
        .watcher_id;

    // Drive create + modify + delete from a separate task to mimic
    // independent filesystem activity, then poll for events with a
    // bounded deadline.
    let host = dir.path().to_path_buf();
    tokio::spawn(async move {
        // Slight delay so the watcher is definitely armed before the
        // first event lands — `notify` registers the inotify watch
        // synchronously in `Watcher::watch`, but the kernel may not
        // deliver the *very first* event on some setups otherwise.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let path = host.join("scratch.txt");
        std::fs::write(&path, b"initial").unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        std::fs::write(&path, b"changed-payload").unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        std::fs::remove_file(&path).unwrap();
    });

    let mut saw_create = false;
    let mut saw_write = false;
    let mut saw_remove = false;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
    while tokio::time::Instant::now() < deadline {
        let events = svc
            .get_watcher_events(Request::new(GetWatcherEventsRequest {
                watcher_id: id.clone(),
            }))
            .await
            .expect("get_watcher_events")
            .into_inner()
            .events;
        for ev in events {
            match EventType::try_from(ev.r#type).unwrap_or(EventType::Unspecified) {
                EventType::Create => saw_create = true,
                EventType::Write => saw_write = true,
                EventType::Remove => saw_remove = true,
                _ => {}
            }
        }
        if saw_create && saw_write && saw_remove {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(saw_create, "expected a Create event");
    assert!(saw_write, "expected a Write event");
    assert!(saw_remove, "expected a Remove event");

    svc.remove_watcher(Request::new(RemoveWatcherRequest { watcher_id: id }))
        .await
        .expect("remove_watcher");
}

#[tokio::test]
async fn watch_dir_streams_start_then_filesystem_event() {
    let (svc, dir) = make_service();

    let mut stream = svc
        .watch_dir(Request::new(WatchDirRequest {
            path: "/".into(),
            recursive: false,
        }))
        .await
        .expect("watch_dir")
        .into_inner();

    // First yield must be the Start event per the proto.
    let first = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("start timed out")
        .expect("start present")
        .expect("start ok")
        .event
        .expect("start event payload");
    assert!(
        matches!(first, watch_dir_response::Event::Start(_)),
        "first event must be Start, got {first:?}"
    );

    let host = dir.path().to_path_buf();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        std::fs::write(host.join("touched.txt"), b"hi").unwrap();
    });

    // Drain the stream until we see at least one Filesystem event.
    let mut saw_fs_event = false;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
    while tokio::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        if let Ok(Some(Ok(resp))) = next {
            if let Some(watch_dir_response::Event::Filesystem(_)) = resp.event {
                saw_fs_event = true;
                break;
            }
        }
    }
    assert!(saw_fs_event, "expected a Filesystem event on the stream");
}

#[tokio::test]
async fn write_all_then_read_all_round_trips() {
    let (svc, dir) = make_service();
    let payload = b"the quick brown fox jumps over the lazy dog";

    svc.write_all(Request::new(WriteAllRequest {
        path: "/note.txt".into(),
        data: payload.to_vec(),
    }))
    .await
    .expect("write_all");

    // Side-channel check: bytes also landed on disk under the root.
    let on_disk = std::fs::read(host_path(&dir, "note.txt")).expect("file on disk");
    assert_eq!(on_disk, payload);

    let resp = svc
        .read_all(Request::new(ReadAllRequest {
            path: "/note.txt".into(),
        }))
        .await
        .expect("read_all")
        .into_inner();
    assert_eq!(resp.data, payload);
}

#[tokio::test]
async fn write_all_rejects_path_traversal() {
    let (svc, _dir) = make_service();
    let err = svc
        .write_all(Request::new(WriteAllRequest {
            path: "../escape.txt".into(),
            data: b"nope".to_vec(),
        }))
        .await
        .expect_err("traversal must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn read_all_missing_path_returns_not_found() {
    let (svc, _dir) = make_service();
    let err = svc
        .read_all(Request::new(ReadAllRequest {
            path: "/no-such-file".into(),
        }))
        .await
        .expect_err("missing file must error");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn collect_watch_snapshots_reports_active_watcher() {
    // Issue #174 contract: after a CreateWatcher, the next snapshot
    // capture must surface the registration with its request-side path
    // and subscriber id.
    let (svc, _dir) = make_service();
    let id_str = svc
        .create_watcher(Request::new(CreateWatcherRequest {
            path: "/".into(),
            recursive: true,
        }))
        .await
        .expect("create_watcher")
        .into_inner()
        .watcher_id;
    // `watcher_id` is `"w{n}"`; the snapshot reports the numeric n.
    let expected_id: u64 = id_str
        .strip_prefix('w')
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("unexpected watcher_id format {id_str}"));

    let snapshots = svc.collect_watch_snapshots().await;
    let entry = snapshots
        .iter()
        .find(|s| s.subscriber_id == expected_id)
        .unwrap_or_else(|| panic!("snapshot must contain id {expected_id}; got {snapshots:?}"));
    assert_eq!(entry.path, PathBuf::from("/"));

    // RemoveWatcher must clear the snapshot too.
    svc.remove_watcher(Request::new(RemoveWatcherRequest { watcher_id: id_str }))
        .await
        .expect("remove_watcher");
    let snapshots = svc.collect_watch_snapshots().await;
    assert!(
        !snapshots.iter().any(|s| s.subscriber_id == expected_id),
        "removed watcher must disappear from snapshot; got {snapshots:?}",
    );
}

#[tokio::test]
async fn boot_seed_is_addressable_but_not_in_collected_snapshots() {
    // Issue #178: the boot-time seed must NOT bleed into
    // `collect_watch_snapshots` — that's what stops the shutdown
    // snapshot from growing across pause/resume cycles. It remains
    // addressable via `boot_seed_watches` for the future rehydration
    // path.
    let dir = tempfile::tempdir().expect("tempdir");
    let seed = envd::state::WatchSnapshot {
        path: PathBuf::from("/seeded"),
        subscriber_id: 99,
    };
    let svc = FilesystemService::with_seed(dir.path().to_path_buf(), vec![seed.clone()])
        .expect("service");

    let snapshots = svc.collect_watch_snapshots().await;
    assert!(
        snapshots.is_empty(),
        "boot seed must not appear in collect_watch_snapshots (issue #178); got {snapshots:?}",
    );

    let seed_view = svc.boot_seed_watches();
    assert_eq!(seed_view.len(), 1);
    assert_eq!(seed_view[0], seed);
}

#[tokio::test]
async fn path_traversal_rejected() {
    let (svc, _dir) = make_service();

    let err = svc
        .stat(Request::new(StatRequest {
            path: "../etc/passwd".into(),
        }))
        .await
        .expect_err("traversal must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    let err2 = svc
        .stat(Request::new(StatRequest {
            path: "/../../../../etc/passwd".into(),
        }))
        .await
        .expect_err("nested traversal must be rejected");
    assert_eq!(err2.code(), tonic::Code::PermissionDenied);
}
