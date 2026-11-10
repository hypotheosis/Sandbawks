//! Snapshot retention garbage collector.
//!
//! The controller periodically sweeps the configured snapshot store
//! and deletes objects older than the effective retention window. The
//! window is resolved per-Sandbox at GC time:
//!
//! 1. `Sandbox.spec.snapshotRetention` (the CRD field added by issue
//!    #186), if present.
//! 2. The namespace-level annotation
//!    `sandbawks.dev/snapshot-retention` (recommended in the issue
//!    over a dedicated `SandboxNamespaceConfig` CRD — annotation
//!    matches how `runtimeClassName: gvisor` already gets selected).
//! 3. The chart-wide default
//!    (`snapshotStore.retention.defaultDuration` Helm value).
//!
//! ## Why an abstract trait
//!
//! The production sink is `rust-s3`-backed; the controller wires that
//! up in `src/bin/sandbawks-controller.rs`. Everything in this module
//! is written against the [`SnapshotRetentionStore`] trait so unit and
//! property tests can drive the reconciler with [`FakeRetentionStore`]
//! without standing up MinIO/garage.
//!
//! ## Layout
//!
//! `S3SnapshotSink` (in `sandbawks-node`) nests each sandbox's bundle
//! under `<key_prefix>/<sandbox-id>/...` (`SnapshotSink::scoped`), so the
//! base `key_prefix` is the parent of every sandbox's objects. The
//! `list_namespace_prefix` argument supplied by the binary is the *whole
//! bucket prefix*: a delimiter-less S3 LIST under it recurses into the
//! per-sandbox subtrees and returns every object, so age-based GC still
//! sees the full set with no algorithmic change. Per-namespace iteration
//! (enumerate namespaces, sweep each under `format!("{ns}/")`) remains an
//! out-of-scope follow-up.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sandbawks_types::{parse_iso8601_duration, DurationParseError};
use thiserror::Error;

/// Well-known namespace annotation overriding the chart default for
/// every Sandbox in the namespace.
///
/// Picked over a dedicated `SandboxNamespaceConfig` CRD per the
/// issue's recommendation: fewer moving parts, same shape as the
/// existing `runtimeClassName` per-namespace selection. The value MUST
/// parse via [`parse_iso8601_duration`]; an unparseable value
/// surfaces as [`RetentionError::InvalidDuration`] and is logged at
/// `WARN` so the operator notices.
pub const NAMESPACE_RETENTION_ANNOTATION: &str = "sandbawks.dev/snapshot-retention";

/// One object listed by [`SnapshotRetentionStore::list_prefix`].
///
/// `last_modified` is whatever the backend reports — for S3 that's
/// the `LastModified` HTTP header on the LIST response, which is the
/// most recent PUT time. Tests can synthesize this directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionObject {
    /// Full object key, suitable for handing straight back to
    /// [`SnapshotRetentionStore::delete`].
    pub key: String,
    /// Server-reported last-modified time. Compared against `now` to
    /// decide whether the object is past the retention horizon.
    pub last_modified: DateTime<Utc>,
}

/// Read-then-delete abstraction over the snapshot store.
///
/// The production impl wraps `rust-s3`'s `Bucket::list` /
/// `Bucket::delete_object`; the test impl is [`FakeRetentionStore`].
/// Only LIST and DELETE are exposed — the reconciler never reads
/// object bodies, so HEAD/GET are intentionally absent.
#[async_trait]
pub trait SnapshotRetentionStore: Send + Sync {
    /// List all objects whose key begins with `prefix`. May return an
    /// empty `Vec` if the prefix is unknown.
    ///
    /// Pagination, if any, is the impl's problem — callers see the
    /// fully-flattened object set in a single `Vec`. Snapshot buckets
    /// are expected to hold thousands of objects at most, not tens of
    /// millions, so collecting into one allocation is fine.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<RetentionObject>, RetentionError>;

    /// Delete a single object by exact key.
    ///
    /// MUST be idempotent: deleting an object that has already been
    /// deleted (or never existed) returns `Ok(())`. The reconciler
    /// relies on this to make repeated passes a no-op.
    async fn delete(&self, key: &str) -> Result<(), RetentionError>;
}

/// Errors surfaced by the retention reconciler.
#[derive(Debug, Error)]
pub enum RetentionError {
    /// A `Sandbox.spec.snapshotRetention` or namespace annotation
    /// value didn't parse as an ISO-8601 duration.
    #[error("invalid retention duration: {0}")]
    InvalidDuration(#[from] DurationParseError),
    /// The configured snapshot store rejected a LIST or DELETE.
    #[error("snapshot store error: {0}")]
    Store(String),
}

/// Chart-wide retention configuration assembled at controller startup
/// from `snapshotStore.retention.*` Helm values surfaced via env vars.
///
/// `enabled=false` short-circuits [`run_retention_pass`] so a
/// misconfigured `defaultDuration` can't accidentally start deleting
/// things in a cluster where the operator hasn't opted in.
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// Master kill-switch. Off → reconciler returns
    /// [`RetentionPassReport::skipped`] without touching the store.
    pub enabled: bool,
    /// Fallback when neither the Sandbox CR nor the namespace
    /// annotation provides one. Default 168h (7 days) via Helm.
    pub default_duration: Duration,
    /// How often the binary spawns a pass. Default 1h via Helm.
    pub scan_interval: Duration,
    /// Prefix to LIST under. For the current single-prefix layout
    /// this is the whole bucket-key prefix; once the node sink moves
    /// to per-namespace prefixes (follow-up), the controller will
    /// iterate namespaces and pass `<base>/<namespace>/` here.
    pub prefix: String,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        // 168h (7 days) + 1h scan interval matches the Helm defaults
        // documented in the issue. `enabled=false` so a `Default::default()`
        // call in a test or main() that forgets to set this can't trip
        // a destructive sweep.
        Self {
            enabled: false,
            default_duration: Duration::from_secs(7 * 24 * 60 * 60),
            scan_interval: Duration::from_secs(60 * 60),
            prefix: String::new(),
        }
    }
}

/// Summary of what one pass of [`run_retention_pass`] did. Returned for
/// observability and tests; the binary logs the deleted/kept/failed
/// counts.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RetentionPassReport {
    /// Objects whose `last_modified` was older than the effective
    /// retention and were successfully deleted (idempotent).
    pub deleted: usize,
    /// Objects still inside the retention window — left untouched.
    pub kept: usize,
    /// Objects past the retention horizon whose DELETE returned an
    /// error this pass. Non-fatal: the sweep continues so a single
    /// rate-limit blip or transient backend error doesn't throw away
    /// the rest of the LIST's worth of work. The next tick re-LISTs
    /// and retries.
    pub failed: usize,
    /// `true` iff the pass returned early because the config had
    /// `enabled=false`. Distinct from `deleted=0 kept=0` (bucket
    /// empty) so the binary can log a clear "retention disabled"
    /// line on every tick instead of a misleading "swept 0 objects".
    pub skipped: bool,
}

impl RetentionPassReport {
    /// Construct a "did nothing because retention is disabled" report.
    pub fn skipped() -> Self {
        Self {
            deleted: 0,
            kept: 0,
            failed: 0,
            skipped: true,
        }
    }
}

/// Resolve the effective retention duration for one Sandbox.
///
/// Order: per-Sandbox `spec.snapshotRetention` → namespace annotation
/// → chart default. Any explicit value that fails to parse short-
/// circuits with [`RetentionError::InvalidDuration`] — silently
/// falling through to a more permissive layer would be surprising
/// (operator types `P7d` lowercase, expects 7 days, gets 30 days
/// because the namespace annotation parsed and theirs didn't).
pub fn effective_retention(
    spec_retention: Option<&str>,
    namespace_annotations: &BTreeMap<String, String>,
    default: Duration,
) -> Result<Duration, RetentionError> {
    if let Some(raw) = spec_retention {
        return Ok(parse_iso8601_duration(raw)?);
    }
    if let Some(raw) = namespace_annotations.get(NAMESPACE_RETENTION_ANNOTATION) {
        return Ok(parse_iso8601_duration(raw)?);
    }
    Ok(default)
}

/// Run one sweep over the configured prefix.
///
/// Algorithm:
/// 1. LIST every object under `cfg.prefix`.
/// 2. For each: compute age = `now - last_modified`. If `age >
///    retention`, DELETE.
/// 3. Tally deleted vs. kept and return.
///
/// `retention` is the *already-resolved* per-Sandbox /
/// per-namespace / default value — the caller does the resolution
/// (today the binary passes `cfg.default_duration` directly; once
/// per-namespace prefixing lands the binary will resolve per
/// namespace before calling this).
///
/// Idempotency: deleting an object that's already gone is a no-op at
/// the trait level ([`SnapshotRetentionStore::delete`] contract), so
/// re-running this immediately yields `deleted=0`. Property-tested in
/// `tests/retention_idempotency.rs`.
pub async fn run_retention_pass(
    store: &dyn SnapshotRetentionStore,
    cfg: &RetentionConfig,
    retention: Duration,
    now: SystemTime,
) -> Result<RetentionPassReport, RetentionError> {
    if !cfg.enabled {
        return Ok(RetentionPassReport::skipped());
    }
    let objects = store.list_prefix(&cfg.prefix).await?;
    let now_chrono: DateTime<Utc> = now.into();
    let retention_chrono = chrono::Duration::from_std(retention)
        .map_err(|e| RetentionError::Store(format!("retention duration overflow: {e}")))?;
    let mut deleted = 0;
    let mut kept = 0;
    let mut failed = 0;
    for obj in objects {
        // `now - last_modified` could be negative if a clock skew or
        // a freshly-written object lands a moment after the LIST; we
        // treat that as "kept" rather than "delete the future".
        let age = now_chrono.signed_duration_since(obj.last_modified);
        if age > retention_chrono {
            // Per-object failures are non-fatal: log and continue so a
            // single rate-limit / transient backend error doesn't
            // throw away the rest of the LIST. The next tick re-LISTs
            // and retries the survivors, so all-or-nothing semantics
            // would just delay forward progress on large buckets.
            match store.delete(&obj.key).await {
                Ok(()) => deleted += 1,
                Err(e) => {
                    tracing::warn!(
                        key = %obj.key,
                        error = %e,
                        "retention delete failed; continuing sweep"
                    );
                    failed += 1;
                }
            }
        } else {
            kept += 1;
        }
    }
    Ok(RetentionPassReport {
        deleted,
        kept,
        failed,
        skipped: false,
    })
}

/// In-memory [`SnapshotRetentionStore`] used by unit and property
/// tests. Backed by a single shared `BTreeMap` so the reconciler and
/// the test fixture see the same state across `.await`s.
///
/// The store is intentionally minimal: it doesn't simulate S3
/// pagination, eventual consistency, or NotFound errors. The trait
/// contract guarantees idempotent delete; this impl honours it via
/// `BTreeMap::remove`, which returns `None` (silently) when the key is
/// already absent.
#[derive(Debug, Clone, Default)]
pub struct FakeRetentionStore {
    inner: Arc<std::sync::Mutex<BTreeMap<String, DateTime<Utc>>>>,
}

impl FakeRetentionStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a single object. Returns `&self` for chaining in test
    /// setup blocks.
    pub fn insert(&self, key: impl Into<String>, last_modified: DateTime<Utc>) -> &Self {
        self.inner
            .lock()
            .expect("FakeRetentionStore poisoned")
            .insert(key.into(), last_modified);
        self
    }

    /// Current number of stored objects. Tests assert on this after a
    /// pass to confirm the right number of deletions happened.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("FakeRetentionStore poisoned")
            .len()
    }

    /// `true` iff no objects remain. Paired with [`Self::len`] so
    /// clippy's `len_without_is_empty` lint stays green; tests prefer
    /// `len() == 0` or `keys().is_empty()` for readability, but the
    /// method is here for symmetry with `FakeSnapshotStore` /
    /// `InMemoryS3`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot the current keys (sorted). Useful for asserting on
    /// the exact survivors after a pass.
    pub fn keys(&self) -> Vec<String> {
        self.inner
            .lock()
            .expect("FakeRetentionStore poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

#[async_trait]
impl SnapshotRetentionStore for FakeRetentionStore {
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<RetentionObject>, RetentionError> {
        let guard = self.inner.lock().expect("FakeRetentionStore poisoned");
        Ok(guard
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, ts)| RetentionObject {
                key: k.clone(),
                last_modified: *ts,
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), RetentionError> {
        // `remove` returns the old value or `None`; either way the
        // post-state is "key absent", which is the idempotency
        // contract.
        self.inner
            .lock()
            .expect("FakeRetentionStore poisoned")
            .remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(secs_ago: i64, now: DateTime<Utc>) -> DateTime<Utc> {
        now - chrono::Duration::seconds(secs_ago)
    }

    /// A test store that fails every DELETE — used to assert
    /// `run_retention_pass` tallies failures and keeps sweeping
    /// rather than aborting on the first error.
    #[derive(Debug, Default)]
    struct AlwaysFailDelete {
        inner: FakeRetentionStore,
    }

    #[async_trait]
    impl SnapshotRetentionStore for AlwaysFailDelete {
        async fn list_prefix(&self, prefix: &str) -> Result<Vec<RetentionObject>, RetentionError> {
            self.inner.list_prefix(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), RetentionError> {
            Err(RetentionError::Store(format!("simulated failure: {key}")))
        }
    }

    #[tokio::test]
    async fn skipped_when_disabled() {
        let store = FakeRetentionStore::new();
        store.insert("a", Utc::now() - chrono::Duration::days(30));
        let cfg = RetentionConfig {
            enabled: false,
            ..Default::default()
        };
        let report = run_retention_pass(&store, &cfg, Duration::from_secs(60), SystemTime::now())
            .await
            .unwrap();
        assert!(report.skipped);
        // Disabled means we don't touch the store.
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn deletes_only_objects_past_retention() {
        let now = Utc::now();
        let store = FakeRetentionStore::new();
        // 3 old (3600s + 1) + 2 young (3600s - 1).
        store.insert("old-1", ts(3_601, now));
        store.insert("old-2", ts(7_200, now));
        store.insert("old-3", ts(9_999, now));
        store.insert("young-1", ts(60, now));
        store.insert("young-2", ts(3_599, now));

        let cfg = RetentionConfig {
            enabled: true,
            prefix: String::new(),
            ..Default::default()
        };
        let report = run_retention_pass(
            &store,
            &cfg,
            Duration::from_secs(3_600),
            SystemTime::from(now),
        )
        .await
        .unwrap();

        assert_eq!(report.deleted, 3, "expected 3 expired objects deleted");
        assert_eq!(report.kept, 2, "expected 2 young objects kept");
        // Survivors are exactly the young ones.
        assert_eq!(
            store.keys(),
            vec!["young-1".to_owned(), "young-2".to_owned()]
        );
    }

    #[tokio::test]
    async fn boundary_object_exactly_at_retention_is_kept() {
        // An object whose age == retention is *kept* (the `>`
        // comparison in `run_retention_pass`). Pinning this boundary
        // here so a refactor to `>=` is loud — operators tend to set
        // retention to "exactly one week" and don't expect the
        // first-second-of-week-eight delete.
        let now = Utc::now();
        let store = FakeRetentionStore::new();
        store.insert("boundary", now - chrono::Duration::seconds(3_600));
        let cfg = RetentionConfig {
            enabled: true,
            ..Default::default()
        };
        let report = run_retention_pass(
            &store,
            &cfg,
            Duration::from_secs(3_600),
            SystemTime::from(now),
        )
        .await
        .unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.kept, 1);
    }

    #[tokio::test]
    async fn delete_failures_are_tallied_and_sweep_continues() {
        // Per-object DELETE errors must NOT abort the sweep: every old
        // object is attempted, the survivors tally up in `failed`, and
        // the next tick re-LISTs and retries. All-or-nothing semantics
        // would throw away the rest of the LIST's work on the first
        // transient blip — a hostile property on large buckets.
        let now = Utc::now();
        let store = AlwaysFailDelete::default();
        store.inner.insert("old-1", ts(7_200, now));
        store.inner.insert("old-2", ts(7_201, now));
        store.inner.insert("young", ts(60, now));

        let cfg = RetentionConfig {
            enabled: true,
            ..Default::default()
        };
        let report = run_retention_pass(
            &store,
            &cfg,
            Duration::from_secs(3_600),
            SystemTime::from(now),
        )
        .await
        .expect("retention pass should not error on per-object DELETE failures");
        assert_eq!(report.deleted, 0);
        assert_eq!(report.kept, 1, "young object still tallied as kept");
        assert_eq!(report.failed, 2, "both old objects' DELETE errors tallied");
    }

    #[tokio::test]
    async fn future_timestamps_are_kept_not_deleted() {
        // Defends against a clock-skew foot-gun: an object stamped in
        // the future shouldn't get deleted just because the math
        // wraps negative.
        let now = Utc::now();
        let store = FakeRetentionStore::new();
        store.insert("future", now + chrono::Duration::seconds(60));
        let cfg = RetentionConfig {
            enabled: true,
            ..Default::default()
        };
        let report =
            run_retention_pass(&store, &cfg, Duration::from_secs(1), SystemTime::from(now))
                .await
                .unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.kept, 1);
    }

    #[tokio::test]
    async fn second_pass_is_a_noop() {
        // Trait-contract-level: re-running immediately after a sweep
        // deletes nothing because the only old objects are already
        // gone. Property-tested at scale in
        // `tests/retention_idempotency.rs`.
        let now = Utc::now();
        let store = FakeRetentionStore::new();
        store.insert("old", ts(7_200, now));
        store.insert("young", ts(60, now));
        let cfg = RetentionConfig {
            enabled: true,
            ..Default::default()
        };
        let _first = run_retention_pass(
            &store,
            &cfg,
            Duration::from_secs(3_600),
            SystemTime::from(now),
        )
        .await
        .unwrap();
        let second = run_retention_pass(
            &store,
            &cfg,
            Duration::from_secs(3_600),
            SystemTime::from(now),
        )
        .await
        .unwrap();
        assert_eq!(second.deleted, 0);
        assert_eq!(second.kept, 1);
    }

    #[test]
    fn effective_retention_resolution_order() {
        let default = Duration::from_secs(7 * 86_400);
        let mut ns_anno = BTreeMap::new();
        ns_anno.insert(
            NAMESPACE_RETENTION_ANNOTATION.to_owned(),
            "PT24H".to_owned(),
        );

        // Spec wins over annotation.
        assert_eq!(
            effective_retention(Some("PT1H"), &ns_anno, default).unwrap(),
            Duration::from_secs(3_600)
        );
        // No spec → annotation.
        assert_eq!(
            effective_retention(None, &ns_anno, default).unwrap(),
            Duration::from_secs(24 * 3_600)
        );
        // No spec, no annotation → default.
        let empty: BTreeMap<String, String> = BTreeMap::new();
        assert_eq!(effective_retention(None, &empty, default).unwrap(), default);
    }

    #[test]
    fn effective_retention_surfaces_parse_errors() {
        let default = Duration::from_secs(60);
        let empty: BTreeMap<String, String> = BTreeMap::new();
        let err = effective_retention(Some("nope"), &empty, default).unwrap_err();
        assert!(matches!(err, RetentionError::InvalidDuration(_)));
    }

    #[test]
    fn effective_retention_rejects_invalid_namespace_annotation() {
        // Annotation typo shouldn't silently fall through to default —
        // an operator who set the annotation almost certainly meant it.
        let mut ns_anno = BTreeMap::new();
        ns_anno.insert(NAMESPACE_RETENTION_ANNOTATION.to_owned(), "P1Y".to_owned());
        let err = effective_retention(None, &ns_anno, Duration::from_secs(60)).unwrap_err();
        assert!(matches!(err, RetentionError::InvalidDuration(_)));
    }
}
