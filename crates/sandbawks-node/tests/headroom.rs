//! Integration test (#238): the per-node resource-headroom guard fails
//! the SnapshotJob path closed when node disk / memory is insufficient,
//! rather than staging or restoring a snapshot that would ENOSPC/OOM.
//!
//! Drives [`GvisorAdapter::stage_snapshot`] / [`resume`] with a stubbed
//! low-headroom [`NodeStats`] reading and a [`FakeSnapshotStore`] seeded
//! with a manifest whose `memory.zst` part claims a large size, then
//! asserts the typed fail-closed error names the offending resource and
//! that no bundle was materialized.
//!
//! [`resume`]: sandbawks_types::runtime::RuntimeAdapter::resume

use std::path::Path;
use std::sync::Arc;

use sandbawks_node::gvisor::{
    fake::{FakeContainerd, FakeEnvdState, FixedClock},
    GvisorAdapter, PodResolution,
};
use sandbawks_node::headroom::NodeStats;
use sandbawks_types::runtime::{RuntimeAdapter, RuntimeError, SnapshotManifest};
use sandbawks_types::snapshot::{SnapshotPart, SnapshotSink};
use sandbawks_types::testing::FakeSnapshotStore;

const GIB: u64 = 1 << 30;
const FIXED_NOW: &str = "2026-05-31T12:00:00+00:00";

/// [`NodeStats`] stub returning fixed disk/memory readings so the gate's
/// fail-closed path is exercised without a real `statvfs` / `/proc`.
struct StubNodeStats {
    disk: u64,
    mem: u64,
}

impl NodeStats for StubNodeStats {
    fn available_disk_bytes(&self, _path: &Path) -> Result<u64, String> {
        Ok(self.disk)
    }
    fn available_memory_bytes(&self) -> Result<u64, String> {
        Ok(self.mem)
    }
}

/// Seed `store` with a manifest whose `memory.zst` part claims
/// `memory_size_bytes` compressed (and, when `uncompressed_bytes` is
/// `Some`, that uncompressed working-set size — #243), and return the
/// controller-style placeholder manifest (empty sha256, so the aggregate
/// cross-check is skipped and the headroom gate is what the test
/// exercises) the caller hands to the adapter.
async fn seed(
    store: &FakeSnapshotStore,
    sandbox_id: &str,
    memory_size_bytes: u64,
    uncompressed_bytes: Option<u64>,
) -> SnapshotManifest {
    let part = SnapshotPart {
        name: "memory.zst".into(),
        sha256: "0".repeat(64),
        size_bytes: memory_size_bytes,
        uncompressed_bytes,
    };
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: sandbox_id.to_owned(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![part],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: sandbox_id.to_owned(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: String::new(),
        parts: Vec::new(),
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    }
}

/// Count `*.img` files anywhere under `root` (absent dir → 0).
fn count_img_files(root: &Path) -> usize {
    fn rec(dir: &Path, n: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                rec(&path, n);
            } else if path.extension().and_then(|s| s.to_str()) == Some("img") {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    rec(root, &mut n);
    n
}

#[tokio::test]
async fn stage_snapshot_fails_closed_on_insufficient_disk() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let staging_root = tmp.path().join("staging");
    let work_root = tmp.path().join("snapshots");

    let store = FakeSnapshotStore::new();
    // A 100 GiB bundle against 1 GiB of free disk: the disk gate trips
    // before any part bytes are fetched.
    let placeholder = seed(&store, "sbx-headroom-disk", 100 * GIB, None).await;

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused".into(),
            pod_ip: "0.0.0.0".into(),
            sandbox_id: "sbx-headroom-disk".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_staging_root(&staging_root)
        .with_node_stats(Arc::new(StubNodeStats {
            disk: GIB,
            mem: 64 * GIB,
        }));

    let err = adapter
        .stage_snapshot("sbx-headroom-disk", &placeholder, &store, None)
        .await
        .expect_err("stage must fail closed when disk headroom is insufficient");
    match err {
        RuntimeError::SnapshotDownload(msg) => {
            assert!(
                msg.contains("headroom") && msg.contains("disk"),
                "stage disk shortfall must name disk headroom, got: {msg}"
            );
        }
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }

    // Fail-closed: no part bytes were ever laid out. `stage_snapshot`
    // removes its partial staging dir on error, so no `*.img` survive.
    assert_eq!(
        count_img_files(&staging_root),
        0,
        "no checkpoint images may be staged on a headroom failure"
    );
}

#[tokio::test]
async fn resume_fails_closed_on_insufficient_memory() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");

    let store = FakeSnapshotStore::new();
    // Ample disk, but the 10 GiB `pages.img` working set dwarfs the 1 MiB
    // of available memory: disk passes, the memory gate trips.
    let placeholder = seed(&store, "sbx-headroom-mem", 10 * GIB, None).await;

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: "sbx-headroom-mem".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd.clone(), envd, &work_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_node_stats(Arc::new(StubNodeStats {
            disk: 1 << 40,
            mem: 1 << 20,
        }));

    let err = adapter
        .resume(&placeholder, "pod-uid", &store)
        .await
        .expect_err("resume must fail closed when memory headroom is insufficient");
    match err {
        RuntimeError::RestoreFailed(msg) => {
            assert!(
                msg.contains("headroom") && msg.contains("memory"),
                "resume memory shortfall must name memory headroom, got: {msg}"
            );
        }
        other => panic!("expected RestoreFailed, got {other:?}"),
    }

    // The restore never reached the snapshotter Apply step.
    let applies = containerd.apply_calls.lock().expect("not poisoned").clone();
    assert!(
        applies.is_empty(),
        "resume must bail before applying the rootfs delta on a headroom failure"
    );

    // Fail-closed cleanup (#238 review): resume removes the work-dir it
    // created, so a headroom failure leaves no orphan `resume-<ts>/`
    // behind — mirroring stage_snapshot. The per-sandbox parent is
    // either absent or empty.
    let per_sandbox = work_root.join("sbx-headroom-mem");
    let leftover = std::fs::read_dir(&per_sandbox)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(
        leftover, 0,
        "resume must not leave a work-dir behind on a headroom failure"
    );
}

#[tokio::test]
async fn resume_memory_gate_uses_uncompressed_size_when_present() {
    // #243: the memory gate must admit against the *uncompressed*
    // `pages.img` working set, not the zstd-shrunk `memory.zst` size.
    //
    // Compressed `memory.zst` = 1 GiB, uncompressed `*.img` total = 8 GiB,
    // available memory = 5 GiB (with the default 0.8 fraction the cap is
    // 4 GiB). The compressed proxy (1 GiB) would *pass* the gate; the true
    // 8 GiB working set must *fail* it. A trip therefore proves the gate
    // read `uncompressed_bytes`, not `size_bytes`.
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");

    let store = FakeSnapshotStore::new();
    let placeholder = seed(&store, "sbx-uncompressed", GIB, Some(8 * GIB)).await;

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: "sbx-uncompressed".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd.clone(), envd, &work_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_node_stats(Arc::new(StubNodeStats {
            disk: 1 << 40,
            mem: 5 * GIB,
        }));

    let err = adapter
        .resume(&placeholder, "pod-uid", &store)
        .await
        .expect_err("resume must gate on the 8 GiB uncompressed working set, not the 1 GiB proxy");
    match err {
        RuntimeError::RestoreFailed(msg) => {
            assert!(
                msg.contains("headroom") && msg.contains("memory"),
                "gate must name memory headroom when the uncompressed size trips it, got: {msg}"
            );
            // The shortfall message reports `needs <bytes>` — pin that it
            // is the 8 GiB uncompressed figure, not the 1 GiB compressed
            // one, so a regression that drops back to `size_bytes` is loud.
            assert!(
                msg.contains(&(8 * GIB).to_string()),
                "shortfall must report the uncompressed need ({} bytes), got: {msg}",
                8 * GIB
            );
        }
        other => panic!("expected RestoreFailed, got {other:?}"),
    }

    // Fail-closed: the restore never applied the rootfs delta.
    let applies = containerd.apply_calls.lock().expect("not poisoned").clone();
    assert!(
        applies.is_empty(),
        "resume must bail before applying the rootfs delta on a headroom failure"
    );
}

#[tokio::test]
async fn validate_fails_closed_on_insufficient_disk_for_expansion_peak() {
    // #331: the pause-time restorability round-trip
    // (`validate_restorable`) now gates on disk headroom (previously
    // `HeadroomGate::Skip`), and that gate accounts for the *transient
    // expansion peak* — the compressed `memory.zst` plus the uncompressed
    // `*.img` it expands into, which coexist on disk before the `.zst` is
    // removed.
    //
    // Compressed `memory.zst` = 1 GiB, uncompressed `*.img` total = 8 GiB,
    // available disk = 5 GiB (default 0.8 fraction → 4 GiB cap). The
    // compressed-only need (1 GiB) would *pass* the gate; the 9 GiB peak
    // (1 GiB compressed + 8 GiB expansion) must *fail* it. A trip therefore
    // proves both that validate gates at all and that the disk figure is the
    // expansion peak, not the wire total — a regression reverting either
    // change makes the gate not fire here and the round-trip then fails
    // later on the (absent) part bytes with a non-headroom error.
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");

    let store = FakeSnapshotStore::new();
    let manifest = seed(&store, "sbx-val-disk", GIB, Some(8 * GIB)).await;

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused".into(),
            pod_ip: "0.0.0.0".into(),
            sandbox_id: "sbx-val-disk".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        // The disk-headroom gate for the expansion peak only runs in the `full`
        // validate level (which re-materializes the bundle); pin it (the
        // default is now `head`, which transfers no bodies).
        .with_validate_level("full")
        .with_node_stats(Arc::new(StubNodeStats {
            disk: 5 * GIB,
            mem: 1 << 40,
        }));

    let err = adapter
        .validate_restorable(&manifest, &store)
        .await
        .expect_err("validate must fail closed when disk can't hold the expansion peak");
    match err {
        RuntimeError::SnapshotDownload(msg) => {
            assert!(
                msg.contains("headroom") && msg.contains("disk"),
                "validate disk shortfall must name disk headroom, got: {msg}"
            );
            // Pin the reported need to the 9 GiB expansion peak (1 GiB
            // compressed + 8 GiB uncompressed), so a regression dropping back
            // to the compressed-only total is loud.
            assert!(
                msg.contains(&(9 * GIB).to_string()),
                "shortfall must report the {}-byte expansion peak, got: {msg}",
                9 * GIB
            );
        }
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }

    // Fail-closed: the gate trips before any part is fetched, and validate
    // tears its scratch dir down on every exit path — no images survive.
    assert_eq!(
        count_img_files(&work_root),
        0,
        "no checkpoint images may be staged on a validate headroom failure"
    );
}

#[tokio::test]
async fn ample_headroom_does_not_trip_the_gate() {
    // Sanity: with generous disk + memory the stage path proceeds past
    // the gate and only then fails fetching the (deliberately absent)
    // part bytes — proving the gate is not what stopped it.
    let tmp = tempfile::tempdir().expect("tempdir");
    let staging_root = tmp.path().join("staging");
    let work_root = tmp.path().join("snapshots");

    let store = FakeSnapshotStore::new();
    let placeholder = seed(&store, "sbx-ample", GIB, None).await;

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused".into(),
            pod_ip: "0.0.0.0".into(),
            sandbox_id: "sbx-ample".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_staging_root(&staging_root)
        .with_node_stats(Arc::new(StubNodeStats {
            disk: 1 << 40,
            mem: 1 << 40,
        }));

    let err = adapter
        .stage_snapshot("sbx-ample", &placeholder, &store, None)
        .await
        .expect_err("the seeded part has no stored bytes, so the fetch fails after the gate");
    match err {
        RuntimeError::SnapshotDownload(msg) => {
            assert!(
                !msg.contains("headroom"),
                "ample headroom must not trip the gate, got: {msg}"
            );
        }
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }
}
