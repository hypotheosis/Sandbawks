//! Durable spill + replay queue for dropped usage events (epic #480, issue #636).
//!
//! Usage emission is fail-open: a bounded retry rides out a transient object-store
//! blip, but a *sustained* outage would otherwise drop every interval that closes
//! during it (silently, pre-#636; observably via the `dropped` counter after). This
//! adds the durable-recovery layer: on retry exhaustion the reconcile loop **spills**
//! the event to a local durable queue (a PVC-backed directory) instead of dropping
//! it, and a background **drainer** replays the queue through the sink once the store
//! recovers — so a multi-minute outage costs latency, not data.
//!
//! ## The contract
//!
//! - [`UsageSpillQueue::spill`] parks one event durably, keyed by its idempotency
//!   key (`(sandbox_uid, interval_start)`) so re-spilling the same interval
//!   overwrites rather than piling up duplicates.
//! - [`UsageSpillQueue::drain`] replays every parked event through a
//!   [`UsageEventSink`], removing each on a successful PUT and leaving the rest for
//!   the next pass. Replay is safe because the event's object key is deterministic in
//!   the same idempotency key — a re-emit overwrites and never double-bills.
//!
//! The production impl is [`FsUsageSpillQueue`] (a directory on the controller's
//! spill PVC); [`NoopUsageSpillQueue`] is the default when no spill dir is
//! configured (emission falls back to the pre-spill drop behaviour); tests drive
//! [`RecordingUsageSpillQueue`]. Mirrors the trait/impl split of
//! [`crate::retention::SnapshotRetentionStore`].

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::usage::{UsageEvent, UsageEventSink};
use crate::usage_metrics::{record_usage_emit, UsageEmitOutcome};

/// Errors surfaced by a [`UsageSpillQueue`].
#[derive(Debug, Error)]
pub enum UsageSpillError {
    /// No spill directory is configured — the queue is a no-op. The caller treats
    /// this as "could not park; drop" (the pre-#636 fail-open behaviour).
    #[error("usage spill queue disabled (no spill directory configured)")]
    Disabled,
    /// The event could not be serialized to NDJSON.
    #[error("usage spill serialize error: {0}")]
    Serialize(String),
    /// A filesystem operation against the spill directory failed.
    #[error("usage spill io error: {0}")]
    Io(String),
}

/// Outcome of one [`UsageSpillQueue::drain`] pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DrainReport {
    /// Events replayed to the sink and removed from the queue this pass.
    pub replayed: usize,
    /// Parked events whose replay failed this pass (left for the next drain).
    pub failed: usize,
}

/// Durable park-and-replay queue for usage events that failed live emission.
#[async_trait]
pub trait UsageSpillQueue: Send + Sync {
    /// Durably park `event` for later replay. Idempotent on the event's
    /// idempotency key: re-spilling the same closed interval overwrites.
    async fn spill(&self, event: &UsageEvent) -> Result<(), UsageSpillError>;

    /// Replay every parked event through `sink`, removing each on success and
    /// leaving failures for the next pass. Records the `replayed` outcome per
    /// successfully re-emitted event ([`crate::usage_metrics`]).
    async fn drain(&self, sink: &dyn UsageEventSink) -> Result<DrainReport, UsageSpillError>;
}

/// Stable, filesystem-safe filename for an event: its idempotency key with any
/// character outside `[A-Za-z0-9._-]` replaced by `_`, plus a `.json` suffix.
///
/// Deterministic in `(sandbox_uid, interval_start)`, so a re-spill targets the same
/// file (idempotent overwrite).
fn spill_filename(event: &UsageEvent) -> String {
    let key: String = event
        .idempotency_key()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{key}.json")
}

/// No-op queue: the default when no spill directory is configured.
///
/// `spill` reports [`UsageSpillError::Disabled`] so the caller falls back to the
/// pre-#636 drop behaviour; `drain` is an empty no-op.
#[derive(Debug, Default, Clone)]
pub struct NoopUsageSpillQueue;

#[async_trait]
impl UsageSpillQueue for NoopUsageSpillQueue {
    async fn spill(&self, _event: &UsageEvent) -> Result<(), UsageSpillError> {
        Err(UsageSpillError::Disabled)
    }
    async fn drain(&self, _sink: &dyn UsageEventSink) -> Result<DrainReport, UsageSpillError> {
        Ok(DrainReport::default())
    }
}

/// Filesystem-backed queue: one NDJSON file per parked event under `dir` (the
/// controller's spill PVC mount).
#[derive(Debug, Clone)]
pub struct FsUsageSpillQueue {
    dir: PathBuf,
}

impl FsUsageSpillQueue {
    /// Build a queue rooted at `dir`, creating it (best-effort) up front so the
    /// first spill on a fresh PVC doesn't race the mkdir.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let _ = std::fs::create_dir_all(&dir);
        Self { dir }
    }
}

#[async_trait]
impl UsageSpillQueue for FsUsageSpillQueue {
    async fn spill(&self, event: &UsageEvent) -> Result<(), UsageSpillError> {
        tokio::fs::create_dir_all(&self.dir).await.map_err(|e| {
            UsageSpillError::Io(format!("create_dir_all {}: {e}", self.dir.display()))
        })?;
        let body = event
            .to_ndjson_line()
            .map_err(|e| UsageSpillError::Serialize(e.to_string()))?;
        let name = spill_filename(event);
        let final_path = self.dir.join(&name);
        // Write to a temp sibling then rename, so a crash mid-write can't leave a
        // half-written file the drainer would choke on; rename is atomic on POSIX.
        let tmp_path = self.dir.join(format!(".{name}.tmp"));
        tokio::fs::write(&tmp_path, body.as_bytes())
            .await
            .map_err(|e| UsageSpillError::Io(format!("write {}: {e}", tmp_path.display())))?;
        tokio::fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|e| UsageSpillError::Io(format!("rename {}: {e}", final_path.display())))?;
        Ok(())
    }

    async fn drain(&self, sink: &dyn UsageEventSink) -> Result<DrainReport, UsageSpillError> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(rd) => rd,
            // A not-yet-created dir is just an empty queue.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(DrainReport::default()),
            Err(e) => {
                return Err(UsageSpillError::Io(format!(
                    "read_dir {}: {e}",
                    self.dir.display()
                )))
            }
        };
        let mut report = DrainReport::default();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| UsageSpillError::Io(format!("read_dir entry: {e}")))?
        {
            let path = entry.path();
            // Only replay completed `.json` files; skip in-flight `.tmp` writes.
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let bytes = match tokio::fs::read(&path).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "reading spilled usage event failed; will retry");
                    report.failed += 1;
                    continue;
                }
            };
            let Ok(event) = serde_json::from_slice::<UsageEvent>(&bytes) else {
                // A corrupt spill file would block forever; drop it (it cannot be
                // billed) but log loudly so the loss is visible.
                tracing::error!(path = %path.display(), "discarding unparseable spilled usage event");
                let _ = tokio::fs::remove_file(&path).await;
                report.failed += 1;
                continue;
            };
            match sink.emit(&event).await {
                Ok(()) => {
                    // Replay succeeded — remove the parked copy. A failed remove is
                    // non-fatal: the next drain re-emits (idempotent overwrite) and
                    // retries the delete.
                    if let Err(e) = tokio::fs::remove_file(&path).await {
                        tracing::warn!(path = %path.display(), error = %e, "replayed spilled usage event but could not remove it; will re-replay (idempotent)");
                    }
                    record_usage_emit(UsageEmitOutcome::Replayed);
                    report.replayed += 1;
                }
                Err(e) => {
                    // Store still unhealthy — leave the file for the next pass.
                    tracing::debug!(path = %path.display(), error = %e, "spilled usage event replay failed; leaving for next drain");
                    report.failed += 1;
                }
            }
        }
        Ok(report)
    }
}

/// In-memory [`UsageSpillQueue`] for tests: records spilled events, and on drain
/// replays them through the sink, removing each on success.
#[derive(Debug, Default, Clone)]
pub struct RecordingUsageSpillQueue {
    spilled: Arc<std::sync::Mutex<Vec<UsageEvent>>>,
}

impl RecordingUsageSpillQueue {
    /// Construct an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of events currently parked.
    pub fn len(&self) -> usize {
        self.spilled
            .lock()
            .expect("RecordingUsageSpillQueue poisoned")
            .len()
    }

    /// `true` iff no events are parked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl UsageSpillQueue for RecordingUsageSpillQueue {
    async fn spill(&self, event: &UsageEvent) -> Result<(), UsageSpillError> {
        let mut g = self
            .spilled
            .lock()
            .expect("RecordingUsageSpillQueue poisoned");
        // Idempotent on the idempotency key, mirroring the Fs impl's filename.
        if !g
            .iter()
            .any(|e| e.idempotency_key() == event.idempotency_key())
        {
            g.push(event.clone());
        }
        Ok(())
    }

    async fn drain(&self, sink: &dyn UsageEventSink) -> Result<DrainReport, UsageSpillError> {
        let parked: Vec<UsageEvent> = {
            let g = self
                .spilled
                .lock()
                .expect("RecordingUsageSpillQueue poisoned");
            g.clone()
        };
        let mut report = DrainReport::default();
        for event in parked {
            match sink.emit(&event).await {
                Ok(()) => {
                    self.spilled
                        .lock()
                        .expect("RecordingUsageSpillQueue poisoned")
                        .retain(|e| e.idempotency_key() != event.idempotency_key());
                    record_usage_emit(UsageEmitOutcome::Replayed);
                    report.replayed += 1;
                }
                Err(_) => report.failed += 1,
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::{RecordingUsageSink, UsageEventSink, UsageSinkError};
    use chrono::{TimeZone, Utc};

    fn event(uid: &str, start_min: u32) -> UsageEvent {
        let start = Utc.with_ymd_and_hms(2026, 6, 21, 10, start_min, 0).unwrap();
        crate::usage::build_usage_event(
            "team-a",
            "key-1",
            uid,
            uid,
            "1",
            "1Gi",
            Some(start),
            start + chrono::Duration::minutes(5),
        )
        .expect("event")
    }

    /// A sink that always fails — to exercise the "store still down" drain path.
    struct AlwaysFailSink;
    #[async_trait]
    impl UsageEventSink for AlwaysFailSink {
        async fn emit(&self, _e: &UsageEvent) -> Result<(), UsageSinkError> {
            Err(UsageSinkError::Store("down".into()))
        }
    }

    #[tokio::test]
    async fn fs_spill_then_drain_replays_and_removes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let q = FsUsageSpillQueue::new(dir.path());
        q.spill(&event("sbx-1", 0)).await.expect("spill 1");
        q.spill(&event("sbx-2", 0)).await.expect("spill 2");

        // Two files parked.
        let count = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(count, 2, "two events parked on the queue");

        let sink = RecordingUsageSink::new();
        let report = q.drain(&sink).await.expect("drain");
        assert_eq!(report.replayed, 2);
        assert_eq!(report.failed, 0);
        assert_eq!(sink.events().len(), 2, "both events replayed to the sink");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "replayed files removed"
        );
    }

    #[tokio::test]
    async fn fs_spill_is_idempotent_on_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let q = FsUsageSpillQueue::new(dir.path());
        // Same (uid, interval_start) twice → one file.
        q.spill(&event("sbx-1", 0)).await.expect("spill");
        q.spill(&event("sbx-1", 0)).await.expect("re-spill");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn fs_drain_leaves_events_when_sink_still_failing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let q = FsUsageSpillQueue::new(dir.path());
        q.spill(&event("sbx-1", 0)).await.expect("spill");

        // Store still down → file stays parked for the next pass.
        let report = q.drain(&AlwaysFailSink).await.expect("drain");
        assert_eq!(report.replayed, 0);
        assert_eq!(report.failed, 1);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        // Store recovers → the same parked event replays and is removed.
        let sink = RecordingUsageSink::new();
        let report = q.drain(&sink).await.expect("drain");
        assert_eq!(report.replayed, 1);
        assert_eq!(sink.events().len(), 1);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn noop_queue_reports_disabled() {
        let q = NoopUsageSpillQueue;
        assert!(matches!(
            q.spill(&event("sbx-1", 0)).await,
            Err(UsageSpillError::Disabled)
        ));
        let report = q.drain(&RecordingUsageSink::new()).await.expect("drain");
        assert_eq!(report, DrainReport::default());
    }

    #[tokio::test]
    async fn recording_queue_parks_and_replays() {
        let q = RecordingUsageSpillQueue::new();
        q.spill(&event("sbx-1", 0)).await.expect("spill");
        q.spill(&event("sbx-1", 0)).await.expect("re-spill"); // idempotent
        assert_eq!(q.len(), 1);
        let sink = RecordingUsageSink::new();
        let report = q.drain(&sink).await.expect("drain");
        assert_eq!(report.replayed, 1);
        assert!(q.is_empty(), "replayed events leave the queue");
    }
}
