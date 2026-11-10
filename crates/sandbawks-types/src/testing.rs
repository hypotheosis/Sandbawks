//! Test doubles for snapshot storage.
//!
//! Gated by the `testing` cargo feature. Downstream crates opt in via:
//!
//! ```toml
//! [dev-dependencies]
//! sandbawks-types = { path = "...", features = ["testing"] }
//! ```
//!
//! The crate is intentionally tiny: only [`FakeSnapshotStore`], which
//! implements both [`crate::snapshot::SnapshotSink`] and
//! [`crate::snapshot::SnapshotSource`] against an in-memory
//! `HashMap<String, Vec<u8>>`. Plan 05 T4-T8 reuse it in unit tests so
//! the controller never has to bring up a real S3 bucket to exercise
//! the pause/resume code paths.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::runtime::{RuntimeError, SnapshotManifest};
use crate::snapshot::{SnapshotPart, SnapshotSink, SnapshotSource};

/// Reserved key under which [`FakeSnapshotStore`] stores the manifest
/// JSON inside its single `HashMap`. Distinct from any plausible part
/// name (no part contains `::` in production), so [`get_part`] /
/// [`put_part`] can't collide with [`get_manifest`] / [`put_manifest`].
///
/// [`get_part`]: SnapshotSource::get_part
/// [`put_part`]: SnapshotSink::put_part
/// [`get_manifest`]: SnapshotSource::get_manifest
/// [`put_manifest`]: SnapshotSink::put_manifest
const MANIFEST_KEY: &str = "::manifest::";

/// In-memory snapshot store used in unit tests.
///
/// Backed by a single `Arc<Mutex<HashMap<String, Vec<u8>>>>` so the
/// same store can be cloned, passed to a sink, then borrowed by a
/// source — exactly the pattern the controller's pause-then-resume
/// integration tests need.
///
/// The mutex is `std::sync::Mutex` rather than `tokio::sync::Mutex`
/// because the guard is never held across an `.await`: each trait
/// method locks, reads/writes the map, drops the guard, then returns.
///
/// # Examples
///
/// ```ignore
/// use sandbawks_types::snapshot::{SnapshotSink, SnapshotSource};
/// use sandbawks_types::testing::FakeSnapshotStore;
///
/// let store = FakeSnapshotStore::new();
/// let part = store.put_part("memory.zst", vec![0xCAu8; 16]).await.unwrap();
/// assert_eq!(part.size_bytes, 16);
/// let bytes = store.get_part("memory.zst").await.unwrap();
/// assert_eq!(bytes, vec![0xCAu8; 16]);
/// ```
#[derive(Debug, Clone, Default)]
pub struct FakeSnapshotStore {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl FakeSnapshotStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current number of stored keys (parts + manifest if
    /// present). Useful for tests asserting "no extra writes happened".
    pub fn len(&self) -> usize {
        self.inner.lock().expect("FakeSnapshotStore poisoned").len()
    }

    /// `true` iff no parts and no manifest have been written.
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .expect("FakeSnapshotStore poisoned")
            .is_empty()
    }
}

#[async_trait]
impl SnapshotSink for FakeSnapshotStore {
    async fn put_part(&self, name: &str, bytes: Vec<u8>) -> Result<SnapshotPart, RuntimeError> {
        let part = SnapshotPart::from_bytes(name, &bytes);
        self.inner
            .lock()
            .expect("FakeSnapshotStore poisoned")
            .insert(name.to_owned(), bytes);
        Ok(part)
    }

    async fn put_manifest(&self, manifest: &SnapshotManifest) -> Result<(), RuntimeError> {
        let bytes = serde_json::to_vec(manifest)
            .map_err(|e| RuntimeError::SnapshotUpload(format!("manifest serialize: {e}")))?;
        self.inner
            .lock()
            .expect("FakeSnapshotStore poisoned")
            .insert(MANIFEST_KEY.to_owned(), bytes);
        Ok(())
    }

    // The fake keys by bare part name, not by prefix, so scoping is a no-op
    // that shares the same backing map: a single sandbox's pause→resume (the
    // only thing the reconcile tests drive) still round-trips because the
    // read side scopes with the same id. Cross-sandbox isolation is a
    // property of the real `S3SnapshotSink` and is unit-tested there.
    fn scoped(&self, _sub_prefix: &str) -> Arc<dyn SnapshotSink> {
        Arc::new(self.clone())
    }
}

#[async_trait]
impl SnapshotSource for FakeSnapshotStore {
    async fn get_part(&self, name: &str) -> Result<Vec<u8>, RuntimeError> {
        self.inner
            .lock()
            .expect("FakeSnapshotStore poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::SnapshotDownload(format!("FakeSnapshotStore: no part {name}"))
            })
    }

    async fn get_manifest(&self) -> Result<SnapshotManifest, RuntimeError> {
        let bytes = self
            .inner
            .lock()
            .expect("FakeSnapshotStore poisoned")
            .get(MANIFEST_KEY)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::SnapshotDownload("FakeSnapshotStore: no manifest".into())
            })?;
        serde_json::from_slice(&bytes)
            .map_err(|e| RuntimeError::SnapshotDownload(format!("manifest deserialize: {e}")))
    }

    fn scoped(&self, _sub_prefix: &str) -> Arc<dyn SnapshotSource> {
        Arc::new(self.clone())
    }
}
