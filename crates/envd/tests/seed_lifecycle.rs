//! Integration test for the boot-seed lifecycle policy (issue #178).
//!
//! In the original `with_seed` design the boot seed was blended into
//! every `collect_*_snapshots` call. That meant a Pod that booted from
//! a seed would persist `live + seed` back to `envd-state.json` on
//! shutdown, and the next Pod's seed would be `previous live + previous
//! seed` — growing monotonically every pause/resume cycle. (PR #177 /
//! commit `45fb627` later added a dedup against live PIDs and PTY
//! `(id, pid)` keys on top of that blended collector, but did not
//! address the underlying lifetime question this test pins.)
//!
//! Issue #178 retired that policy: the seed is now held read-only on the
//! service, addressable via the `boot_seed_*` accessors for the future
//! rehydration path, and **never** appears in `collect_*_snapshots`. This
//! test pins that invariant by exercising a full two-boot cycle and
//! asserting the second-boot snapshot is empty when nothing live churned
//! either boot.

use std::collections::BTreeMap;
use std::path::PathBuf;

use envd::filesystem::FilesystemService;
use envd::process::ProcessService;
use envd::proto::filesystem::filesystem_server::Filesystem;
use envd::proto::filesystem::CreateWatcherRequest;
use envd::state::{EnvdState, ProcessSnapshot, PtySnapshot, WatchSnapshot, SCHEMA_VERSION};
use tonic::Request;

/// Build an `EnvdState` from the two services' live registries — the
/// same call sequence the binary's `SnapshotHandle::capture` runs at
/// shutdown. Kept in this test (rather than reaching into the bin
/// module) so the assertion is decoupled from the binary's wiring.
async fn capture(process: &ProcessService, filesystem: &FilesystemService) -> EnvdState {
    EnvdState {
        version: SCHEMA_VERSION,
        processes: process.collect_process_snapshots().await,
        ptys: process.collect_pty_snapshots().await,
        watches: filesystem.collect_watch_snapshots().await,
        // This helper mirrors the registry walk only; the rootfs
        // fingerprint (#236) is folded in separately by the binary.
        rootfs_fingerprint: None,
    }
}

#[tokio::test]
async fn two_boot_cycle_does_not_persist_seed() {
    // Stage 1: stand in for what a prior envd's shutdown left on disk.
    // One ghost entry in each of the three registries, with distinct
    // identifiers so we can pinpoint which leaked if the assertion
    // fails.
    let first_seed_process = ProcessSnapshot {
        pid: 4242,
        cmd: "ghost-process".to_string(),
        args: vec!["arg1".to_string()],
        env: BTreeMap::new(),
        cwd: PathBuf::from("/var/empty"),
        started_at: 1_700_000_000,
    };
    let first_seed_pty = PtySnapshot {
        id: 77,
        pid: 4242,
        rows: 24,
        cols: 80,
        cmd: "ghost-pty".to_string(),
        args: vec![],
    };
    let first_seed_watch = WatchSnapshot {
        path: PathBuf::from("/seeded/path"),
        subscriber_id: 99,
    };

    // Stage 2: first boot reads that seed into the live services.
    // FilesystemService::with_seed canonicalizes `root`, so we feed it
    // a real tempdir instead of an arbitrary path.
    let fs_root = tempfile::tempdir().expect("tempdir");
    let first_process = ProcessService::with_seed(
        vec![first_seed_process.clone()],
        vec![first_seed_pty.clone()],
    );
    let first_filesystem =
        FilesystemService::with_seed(fs_root.path().to_path_buf(), vec![first_seed_watch.clone()])
            .expect("filesystem service");

    // Stage 3: confirm the seed is still addressable via the dedicated
    // accessors — that's the contract the future rehydration path
    // depends on (see issue #178).
    assert_eq!(first_process.boot_seed_processes(), &[first_seed_process]);
    assert_eq!(first_process.boot_seed_ptys(), &[first_seed_pty]);
    assert_eq!(first_filesystem.boot_seed_watches(), &[first_seed_watch]);

    // Stage 4: simulate first-boot shutdown. With no live churn, the
    // capture must be empty — the seed is held but deliberately not
    // surfaced through `collect_*_snapshots`.
    let first_shutdown = capture(&first_process, &first_filesystem).await;
    assert!(
        first_shutdown.processes.is_empty(),
        "first-boot shutdown leaked seed processes: {:?}",
        first_shutdown.processes,
    );
    assert!(
        first_shutdown.ptys.is_empty(),
        "first-boot shutdown leaked seed ptys: {:?}",
        first_shutdown.ptys,
    );
    assert!(
        first_shutdown.watches.is_empty(),
        "first-boot shutdown leaked seed watches: {:?}",
        first_shutdown.watches,
    );

    // Stage 5: second boot — feed first-boot's shutdown capture in as
    // the new seed, just like `load_boot_seed` would on the next Pod.
    // Since first_shutdown is empty, the second boot's seed is empty,
    // and there's nothing for monotonic growth to compound on.
    let second_process = ProcessService::with_seed(
        first_shutdown.processes.clone(),
        first_shutdown.ptys.clone(),
    );
    let second_filesystem =
        FilesystemService::with_seed(fs_root.path().to_path_buf(), first_shutdown.watches.clone())
            .expect("filesystem service");

    let second_shutdown = capture(&second_process, &second_filesystem).await;

    // Stage 6: the second-boot snapshot must not contain any of the
    // original ghost identifiers. This is the literal acceptance
    // criterion from issue #178.
    assert!(
        !second_shutdown.processes.iter().any(|s| s.pid == 4242),
        "stale first-boot process pid resurrected in second-boot snapshot: {:?}",
        second_shutdown.processes,
    );
    assert!(
        !second_shutdown.ptys.iter().any(|s| s.id == 77),
        "stale first-boot pty id resurrected in second-boot snapshot: {:?}",
        second_shutdown.ptys,
    );
    assert!(
        !second_shutdown
            .watches
            .iter()
            .any(|s| s.subscriber_id == 99),
        "stale first-boot watch subscriber_id resurrected in second-boot snapshot: {:?}",
        second_shutdown.watches,
    );
}

#[tokio::test]
async fn live_watch_survives_alongside_dropped_seed() {
    // Companion to `two_boot_cycle_does_not_persist_seed` — that test
    // covers the empty-live case (issue #178's literal acceptance
    // criterion), but the more interesting property is that on a second
    // boot with *real* live churn, the live entries appear in the
    // capture while the seed entries still don't. Exercises that here
    // via the filesystem service because `CreateWatcher` registers
    // synchronously and the snapshot reflects it immediately — no child
    // process, no retention window, no flakiness from the lifecycle
    // bridge.
    let fs_root = tempfile::tempdir().expect("tempdir");

    let seed_watch = WatchSnapshot {
        path: PathBuf::from("/seeded/path"),
        subscriber_id: 99,
    };
    let svc = FilesystemService::with_seed(fs_root.path().to_path_buf(), vec![seed_watch.clone()])
        .expect("filesystem service");

    // Register a real inotify subscription against a path in the
    // tempdir. The minted subscriber id is allocated past the seeded
    // max (99) by `with_seed`'s counter-init, so the live id is
    // guaranteed not to collide with the seed.
    let live_id_str = svc
        .create_watcher(Request::new(CreateWatcherRequest {
            path: "/".into(),
            recursive: false,
        }))
        .await
        .expect("create_watcher")
        .into_inner()
        .watcher_id;
    let live_id_num: u64 = live_id_str
        .strip_prefix('w')
        .and_then(|s| s.parse().ok())
        .expect("watcher_id parses as w{n}");
    assert!(
        live_id_num > seed_watch.subscriber_id,
        "live id {live_id_num} must be allocated past the seeded max {}",
        seed_watch.subscriber_id,
    );

    // Snapshot must contain exactly the live entry — the seed is held
    // on the service but never blended in.
    let snapshots = svc.collect_watch_snapshots().await;
    assert_eq!(
        snapshots.len(),
        1,
        "expected exactly the live watcher, got {snapshots:?}",
    );
    assert_eq!(
        snapshots[0].subscriber_id, live_id_num,
        "snapshot's sole entry should be the live one, not the seed: {snapshots:?}",
    );
    assert!(
        snapshots.iter().all(|s| s.subscriber_id != 99),
        "seed watch id 99 leaked into the live capture: {snapshots:?}",
    );

    // And the seed remains addressable via the dedicated accessor for
    // the future rehydration path.
    assert_eq!(svc.boot_seed_watches(), &[seed_watch]);
}
