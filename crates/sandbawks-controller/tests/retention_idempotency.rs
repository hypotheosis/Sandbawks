//! Hegel property test: two retention sweeps in a row leave the same
//! post-state as one.
//!
//! Issue #186's tests section calls for "two retention scans in a row
//! produce identical post-state — proves idempotency." This test
//! exercises that property over arbitrary mixed-age fixtures: any
//! object that would have been deleted in the first pass is gone
//! before the second pass even sees it, so the second pass must be a
//! no-op (`deleted == 0`) and the surviving key set must be unchanged.
//!
//! The harness is `hegel`, aliased from `hegeltest` per the workspace
//! Cargo.toml declaration.

use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use hegel::generators;
use hegel::TestCase;

use sandbawks_controller::{run_retention_pass, FakeRetentionStore, RetentionConfig};

/// Cap on objects per generated fixture. The reconciler is linear in
/// the object count; 32 keeps the per-case cost low enough that the
/// default 256 hegel cases run in well under a second.
const MAX_OBJECTS: usize = 32;

/// Cap on the age delta (in seconds) on either side of the retention
/// window. The retention horizon is fixed at 3600s below; ±7200s
/// straddles it widely enough that both "old" and "young" outcomes
/// turn up in most cases.
const MAX_AGE_DELTA_SECS: i64 = 7_200;

/// Generate one `(key, last_modified)` pair. Keys are short opaque
/// strings; `last_modified` is offset from a fixed test epoch by an
/// arbitrary i64 in `[-MAX_AGE_DELTA_SECS, MAX_AGE_DELTA_SECS]`.
#[hegel::composite]
fn object_pair(tc: TestCase) -> (String, i64) {
    let key = tc.draw(generators::text().max_size(16));
    let delta = tc.draw(
        generators::integers::<i64>()
            .min_value(-MAX_AGE_DELTA_SECS)
            .max_value(MAX_AGE_DELTA_SECS),
    );
    (key, delta)
}

/// Generate a small Vec of object pairs. The integration test handles
/// the empty case separately, but property-test-wise the empty Vec is
/// a perfectly fine input (yields a no-op sweep).
#[hegel::composite]
fn object_pairs(tc: TestCase) -> Vec<(String, i64)> {
    tc.draw(generators::vecs(object_pair()).max_size(MAX_OBJECTS))
}

#[hegel::test]
fn two_passes_have_identical_post_state(tc: TestCase) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let pairs = tc.draw(object_pairs());
        // Fixed reference time so the test is deterministic across
        // hegel re-runs — `Utc::now()` would couple the property to
        // wall-clock drift.
        let now: DateTime<Utc> = "2026-05-27T12:00:00Z".parse().expect("RFC3339 epoch");

        let store = FakeRetentionStore::new();
        // De-dup by key so two pairs with the same key collapse to
        // the last-written timestamp (matches BTreeMap semantics).
        let mut deduped: std::collections::BTreeMap<String, DateTime<Utc>> =
            std::collections::BTreeMap::new();
        for (key, delta) in pairs {
            let ts = now + chrono::Duration::seconds(delta);
            deduped.insert(key, ts);
        }
        for (key, ts) in &deduped {
            store.insert(key.clone(), *ts);
        }

        let cfg = RetentionConfig {
            enabled: true,
            default_duration: Duration::from_secs(3_600),
            scan_interval: Duration::from_secs(60),
            prefix: String::new(),
        };

        let first = run_retention_pass(&store, &cfg, cfg.default_duration, SystemTime::from(now))
            .await
            .expect("first retention pass");
        let keys_after_first = store.keys();

        let second = run_retention_pass(&store, &cfg, cfg.default_duration, SystemTime::from(now))
            .await
            .expect("second retention pass");
        let keys_after_second = store.keys();

        // The idempotency property: the second pass deletes nothing
        // and the on-disk key set is unchanged.
        assert_eq!(
            second.deleted, 0,
            "second pass deleted {} objects, expected 0",
            second.deleted
        );
        assert_eq!(
            keys_after_first, keys_after_second,
            "key set changed between passes",
        );
        // Sanity: first pass's kept count + deleted count == input size.
        assert_eq!(first.deleted + first.kept, deduped.len());
        // Sanity: second pass's kept count equals first pass's kept count.
        assert_eq!(second.kept, first.kept);
    });
}
