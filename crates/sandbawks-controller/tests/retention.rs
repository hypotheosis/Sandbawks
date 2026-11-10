//! Integration test for the snapshot retention reconciler (issue #186).
//!
//! Seeds a [`FakeRetentionStore`] with N old + M young objects and
//! asserts that exactly N are deleted in one pass, leaving the M young
//! ones untouched. This is the "FakeS3Sink seeded with N old + M young
//! objects" check called out in the issue's Tests section — it covers
//! the integration contract that the trait + reconciler agree on
//! "which objects to delete."

use std::time::{Duration, SystemTime};

use chrono::Utc;
use sandbawks_controller::{run_retention_pass, FakeRetentionStore, RetentionConfig};

/// One scan over a mixed-age fixture deletes only the expired objects.
///
/// 7 old + 5 young exercises the boundary in both directions: the
/// reconciler must walk every object (no early termination on first
/// young/old) and the deleted/kept tallies must match exactly.
#[tokio::test]
async fn one_pass_deletes_only_expired_objects() {
    let now = Utc::now();
    let store = FakeRetentionStore::new();

    // 7 "old" — past the retention horizon (every entry is at least
    // `retention + 1s` older than `now`).
    for i in 0..7 {
        let age = chrono::Duration::seconds(3_600 + i + 1); // 3601..3608s
        store.insert(format!("old/{i:02}"), now - age);
    }
    // 5 "young" — strictly inside the retention horizon.
    for i in 0..5 {
        let age = chrono::Duration::seconds(60 * i + 1); // 1..241s
        store.insert(format!("young/{i:02}"), now - age);
    }
    assert_eq!(store.len(), 12);

    let cfg = RetentionConfig {
        enabled: true,
        default_duration: Duration::from_secs(3_600),
        scan_interval: Duration::from_secs(60),
        prefix: String::new(),
    };

    let report = run_retention_pass(&store, &cfg, cfg.default_duration, SystemTime::from(now))
        .await
        .expect("retention pass");

    assert_eq!(report.deleted, 7, "expected exactly 7 expired deletions");
    assert_eq!(report.kept, 5, "expected exactly 5 young survivors");
    assert!(!report.skipped);

    // Survivors are exactly the young/* keys; no old/* should remain.
    let remaining = store.keys();
    assert_eq!(remaining.len(), 5);
    assert!(remaining.iter().all(|k| k.starts_with("young/")));
}

/// The prefix filter actually restricts the LIST scope.
///
/// Two namespace-shaped prefixes; the reconciler should only see and
/// only delete from the one we ask it to. This pins the "the prefix
/// argument matters" property before we hook in real per-namespace
/// listing.
#[tokio::test]
async fn prefix_scopes_the_sweep() {
    let now = Utc::now();
    let store = FakeRetentionStore::new();
    let old = now - chrono::Duration::seconds(3_601);

    store.insert("ns-a/old-1", old);
    store.insert("ns-a/old-2", old);
    store.insert("ns-b/old-1", old);

    let cfg = RetentionConfig {
        enabled: true,
        default_duration: Duration::from_secs(3_600),
        scan_interval: Duration::from_secs(60),
        prefix: "ns-a/".into(),
    };
    let report = run_retention_pass(&store, &cfg, cfg.default_duration, SystemTime::from(now))
        .await
        .expect("retention pass");

    assert_eq!(report.deleted, 2);
    assert_eq!(report.kept, 0);
    // `ns-b/old-1` should still be present — outside the prefix.
    assert_eq!(store.keys(), vec!["ns-b/old-1".to_owned()]);
}

/// Empty bucket: a sweep should be a no-op and report zero deletions.
#[tokio::test]
async fn empty_bucket_is_a_noop() {
    let store = FakeRetentionStore::new();
    let cfg = RetentionConfig {
        enabled: true,
        default_duration: Duration::from_secs(3_600),
        scan_interval: Duration::from_secs(60),
        prefix: String::new(),
    };
    let report = run_retention_pass(&store, &cfg, cfg.default_duration, SystemTime::now())
        .await
        .expect("retention pass");
    assert_eq!(report.deleted, 0);
    assert_eq!(report.kept, 0);
}
