//! Durable hourly usage ledger — Phase 3 of the metering epic (#480, issue #632).
//!
//! A periodic rollup reads Phase 2's authoritative [`UsageEvent`] objects
//! ([`crate::usage`]) and aggregates them into idempotent per-`(team, key_name,
//! hour)` [`UsageRecord`]s written as NDJSON to durable object storage. This rolled
//! ledger — not the raw events, and emphatically **not** OTel (epic trust-model
//! decision #480) — is what billing reads from.
//!
//! ## Why a rollup, not the raw events
//!
//! The raw events are one-per-closed-`Running`-interval, keyed for idempotency on
//! `(sandbox_uid, interval_start)`. Invoicing wants a small, stable, per-period
//! total per team+key, safe to re-derive. The rollup provides exactly that, while
//! the raw events stay the replayable source of truth.
//!
//! ## Hourly bucketing across interval boundaries
//!
//! A `Running` interval can straddle hour boundaries (e.g. `10:30 → 11:20`).
//! [`rollup_events`] splits each interval into per-hour slices *proportionally* and
//! attributes each slice's weighted seconds to that hour, so an interval's billable
//! time lands in the hours it was actually running — the precision the **hourly**
//! granularity (answer 3 of #480) exists to give mid-cycle plan changes. Because the
//! slice factors sum to 1 over a fully-covered interval, the ledger **reconciles**:
//! the sum of all records' `weighted_running_seconds` equals the sum of the source
//! events' `weighted_seconds` (over closed hours).
//!
//! ## Provisional current hour + open intervals (#637)
//!
//! Each pass also accounts every still-`Running` sandbox's open interval up to `now`
//! (via [`OpenIntervalSource`]), so a long-lived sandbox accrues billable usage as
//! time passes rather than only when it finally closes — and a never-closing sandbox
//! is still billed. The record for the **current** (in-progress) wall-clock hour is
//! marked `provisional` (still accumulating, not yet invoiceable); past-hour records
//! are `final`, because a fully-elapsed hour's slice has the same value whether
//! computed from the open interval or the eventual closed event. Re-running the
//! rollup re-derives the same records and overwrites them at a deterministic key
//! ([`UsageRecord::object_key`]), so replays, overlapping ticks, and the open→closed
//! transition never double-bill (idempotent on `(team, key_name, period)`).
//!
//! ## Scope note (creates / concurrency)
//!
//! Phase 2 emits only running-interval events, so the ledger carries the dimensions
//! those events support — weighted/raw running-seconds, the contributing interval
//! count, and the distinct-sandbox count. `sandbox_creates` and `peak_concurrent`
//! (epic wishlist) require Phase 2 to additionally emit create/active events and are
//! a deliberate follow-up; deriving them from OTel is forbidden by the trust model.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, DurationRound, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::usage::UsageEvent;

/// Schema version stamped on every [`UsageRecord`] so the billing reader can evolve
/// the record shape unambiguously.
pub const USAGE_RECORD_SCHEMA: u32 = 1;

/// One rolled-up billable record for a `(team, key_name, hour)`.
///
/// Field names are snake_case and stable — a record is the cross-system contract
/// billing reads from. Written one-per-line as NDJSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// Record-shape version (see [`USAGE_RECORD_SCHEMA`]).
    pub schema: u32,
    /// Owning team — the sandbox namespace.
    pub team: String,
    /// Owning `TeamAPIKey` name (the per-key breakdown). Empty string aggregates
    /// the events that carried no owning key.
    pub key_name: String,
    /// RFC 3339 start of the rolled hour (inclusive), truncated to `:00:00`.
    pub period_start: String,
    /// RFC 3339 end of the rolled hour (exclusive) — `period_start + 1h`.
    pub period_end: String,
    /// Resource-weighted running-seconds in this hour (the billable figure).
    pub weighted_running_seconds: f64,
    /// Raw (unweighted) running-seconds in this hour, for auditing / re-weighting.
    pub running_seconds: f64,
    /// Distinct sandboxes (by `sandbox_uid`) that ran in this hour for this key.
    pub distinct_sandboxes: u64,
    /// Number of source `Running` intervals that contributed to this record — the
    /// reconciliation/audit handle back to the raw events.
    pub source_interval_count: u64,
    /// `true` while this is the in-progress wall-clock hour (still accumulating) —
    /// **not yet safe to invoice** (issue #637). `false` once the hour has fully
    /// elapsed. A record for the current hour is recomputed each rollup pass (it
    /// grows as the hour progresses and as still-`Running` sandboxes are accounted up
    /// to `now`); when the hour closes the next pass re-derives it as final. Past-hour
    /// records are final even when a contributing sandbox is still running, because a
    /// fully-elapsed hour's slice has the same value whether computed from the open
    /// interval or the eventual closed event.
    ///
    /// `#[serde(default)]` (→ `false`) keeps the schema backward-compatible without a
    /// `USAGE_RECORD_SCHEMA` bump: a record written before this field existed
    /// (pre-#637, only ever for elapsed/closed hours) deserializes as `final`, which
    /// is the correct invoiceable interpretation for such a record.
    #[serde(default)]
    pub provisional: bool,
}

impl UsageRecord {
    /// Object-store key this record is written under, rooted at `prefix`.
    ///
    /// `"<prefix>/<team>/<key_name>/<period_start>.json"` — deterministic in
    /// `(team, key_name, period)`, so a re-run overwrites the same object
    /// (idempotent PUT, never a duplicate). Colons in the timestamp are replaced so
    /// the key is portable across object stores that dislike them; an empty
    /// `key_name` folds to `_nokey` so the path segment is always present.
    pub fn object_key(&self, prefix: &str) -> String {
        let base = prefix.trim_end_matches('/');
        let key = if self.key_name.is_empty() {
            "_nokey"
        } else {
            &self.key_name
        };
        let stamp = self.period_start.replace(':', "-");
        if base.is_empty() {
            format!("{}/{key}/{stamp}.json", self.team)
        } else {
            format!("{base}/{}/{key}/{stamp}.json", self.team)
        }
    }

    /// Serialize to a single NDJSON line (compact JSON + trailing newline).
    pub fn to_ndjson_line(&self) -> Result<String, UsageLedgerError> {
        let mut line =
            serde_json::to_string(self).map_err(|e| UsageLedgerError::Serialize(e.to_string()))?;
        line.push('\n');
        Ok(line)
    }
}

/// A still-open `Running` interval for a sandbox that has not yet closed (issue
/// #637). Accounted by [`rollup_events`] up to `now` so long-lived sandboxes accrue
/// billable usage as time passes, not only when they finally pause/terminate.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenInterval {
    /// Owning team — the sandbox namespace.
    pub team: String,
    /// Owning `TeamAPIKey` name (empty when unstamped).
    pub key_name: String,
    /// Sandbox `metadata.uid`.
    pub sandbox_uid: String,
    /// CPU request verbatim (e.g. `"500m"`).
    pub cpu_request: String,
    /// Memory request verbatim (e.g. `"4Gi"`).
    pub mem_request: String,
    /// Instant the sandbox entered `Running` (`status.lastTransitionTime`).
    pub running_since: DateTime<Utc>,
}

/// Per-bucket accumulator used while rolling up; not serialized.
#[derive(Default)]
struct Bucket {
    weighted: f64,
    running: f64,
    sandboxes: BTreeSet<String>,
    intervals: u64,
}

/// Truncate an instant to the start of its hour.
fn hour_floor(dt: DateTime<Utc>) -> DateTime<Utc> {
    // `duration_trunc` rounds toward the epoch on a whole-hour boundary, which is
    // exactly an hour-floor for the non-negative timestamps we deal with.
    dt.duration_trunc(Duration::hours(1))
        .expect("hour is a valid truncation unit")
}

/// Roll authoritative closed [`UsageEvent`]s and still-open [`OpenInterval`]s into
/// hourly [`UsageRecord`]s.
///
/// Each interval is split into per-hour slices, contributing its proportional share
/// of the weighted + raw seconds to that `(team, key_name, hour)` bucket. An
/// [`OpenInterval`] is accounted as if it were an event ending at `now`, so a
/// long-lived sandbox accrues billable usage every pass rather than only at close
/// (issue #637).
///
/// **Provisional vs final.** The record for the current (in-progress) wall-clock
/// hour is marked `provisional` — it is still accumulating and is recomputed each
/// pass. Past-hour records are `final`: a fully-elapsed hour's slice has the same
/// value whether computed from an open interval `[start, now]` or the eventual
/// closed event `[start, close]`, so a still-running sandbox's *past* hours are
/// safe to invoice (and a never-closing sandbox is therefore still billed for its
/// elapsed hours). Strictly-future slices (clock skew) are dropped.
///
/// Pure and deterministic: the returned records are sorted by
/// `(team, key_name, period_start)`, so a re-run yields byte-identical output —
/// the basis for the idempotent ledger write. Intervals with an unparseable or
/// non-positive span are skipped (they cannot be bounded/billed).
pub fn rollup_events(
    events: &[UsageEvent],
    open: &[OpenInterval],
    now: DateTime<Utc>,
) -> Vec<UsageRecord> {
    let current_hour = hour_floor(now);
    let hour = Duration::hours(1);
    // Key: (team, key_name, period_start as sortable epoch-millis).
    let mut buckets: BTreeMap<(String, String, i64), Bucket> = BTreeMap::new();

    // Synthesize each open interval as an event ending at `now`, then account it
    // through exactly the same slicing as a closed event — `build_usage_event`
    // applies the identical resource weighting and drops non-positive spans.
    let synthetic: Vec<UsageEvent> = open
        .iter()
        .filter_map(|o| {
            crate::usage::build_usage_event(
                &o.team,
                &o.key_name,
                &o.sandbox_uid,
                &o.sandbox_uid,
                &o.cpu_request,
                &o.mem_request,
                Some(o.running_since),
                now,
            )
        })
        .collect();

    for event in events.iter().chain(synthetic.iter()) {
        let (Ok(start), Ok(end)) = (
            DateTime::parse_from_rfc3339(&event.interval_start),
            DateTime::parse_from_rfc3339(&event.interval_end),
        ) else {
            continue;
        };
        let start = start.with_timezone(&Utc);
        let end = end.with_timezone(&Utc);
        let total_secs = (end - start).num_milliseconds() as f64 / 1000.0;
        if total_secs <= 0.0 {
            continue;
        }
        // Weighted seconds per real second — lets each hour-slice carry its
        // proportional share without re-parsing the resource request.
        let weight_per_sec = event.weighted_seconds / total_secs;

        let mut slice_start = hour_floor(start);
        while slice_start < end {
            let slice_end = slice_start + hour;
            // Account up to and including the current (in-progress) hour; drop only
            // strictly-future slices (clock skew). The current hour is emitted as a
            // provisional record below.
            if slice_start > current_hour {
                break;
            }
            let overlap_start = slice_start.max(start);
            let overlap_end = slice_end.min(end);
            let overlap_secs = (overlap_end - overlap_start).num_milliseconds() as f64 / 1000.0;
            if overlap_secs > 0.0 {
                let bucket = buckets
                    .entry((
                        event.team.clone(),
                        event.key_name.clone(),
                        slice_start.timestamp_millis(),
                    ))
                    .or_default();
                bucket.weighted += overlap_secs * weight_per_sec;
                bucket.running += overlap_secs;
                bucket.sandboxes.insert(event.sandbox_uid.clone());
                bucket.intervals += 1;
            }
            slice_start = slice_end;
        }
    }

    let current_hour_ms = current_hour.timestamp_millis();
    buckets
        .into_iter()
        .map(|((team, key_name, epoch_ms), b)| {
            let period_start = DateTime::from_timestamp_millis(epoch_ms)
                .expect("epoch_ms came from a valid DateTime");
            UsageRecord {
                schema: USAGE_RECORD_SCHEMA,
                team,
                key_name,
                period_start: period_start.to_rfc3339(),
                period_end: (period_start + hour).to_rfc3339(),
                weighted_running_seconds: b.weighted,
                running_seconds: b.running,
                distinct_sandboxes: b.sandboxes.len() as u64,
                source_interval_count: b.intervals,
                // The in-progress hour is still accumulating → not yet invoiceable.
                provisional: epoch_ms == current_hour_ms,
            }
        })
        .collect()
}

/// Errors surfaced by the ledger rollup.
#[derive(Debug, Error)]
pub enum UsageLedgerError {
    /// A record could not be serialized, or an event body could not be parsed.
    #[error("usage ledger serialize/parse error: {0}")]
    Serialize(String),
    /// The backing object store rejected a LIST / GET / PUT.
    #[error("usage ledger store error: {0}")]
    Store(String),
}

/// Read side: lists and parses the authoritative event objects Phase 2 wrote.
///
/// The production impl is object-store-backed ([`crate::s3_usage::S3UsageSource`]);
/// tests use [`FakeUsageEventSource`]. Mirrors the trait/impl split of
/// [`crate::retention::SnapshotRetentionStore`].
#[async_trait]
pub trait UsageEventSource: Send + Sync {
    /// List every authoritative usage event under the source prefix, parsed.
    ///
    /// Snapshot/usage buckets hold thousands of objects at most, so collecting into
    /// one `Vec` is fine; a malformed object is skipped (logged by the impl), never
    /// failing the whole pass.
    async fn list_events(&self) -> Result<Vec<UsageEvent>, UsageLedgerError>;
}

/// Write side: persists a rolled [`UsageRecord`] idempotently.
#[async_trait]
pub trait UsageLedgerSink: Send + Sync {
    /// Write one record, overwriting any existing record at the same
    /// `(team, key_name, period)` key — the idempotency contract that makes
    /// re-runs safe.
    async fn write_record(&self, record: &UsageRecord) -> Result<(), UsageLedgerError>;
}

/// Live side: enumerates still-`Running` sandboxes as [`OpenInterval`]s so the
/// rollup can account in-progress usage (issue #637).
///
/// The production impl lists Sandboxes from the apiserver
/// ([`crate::KubeOpenIntervalSource`]); [`NoopOpenIntervalSource`] (always empty)
/// restores the close-only behaviour, and tests use [`FakeOpenIntervalSource`].
#[async_trait]
pub trait OpenIntervalSource: Send + Sync {
    /// Snapshot every currently-`Running` sandbox's open interval. A failure is
    /// surfaced to the caller, which logs and skips the open-interval portion of the
    /// pass rather than aborting (closed events still roll up).
    async fn open_intervals(&self) -> Result<Vec<OpenInterval>, UsageLedgerError>;
}

/// [`OpenIntervalSource`] that reports no open intervals — rollup accounts only
/// closed events (the pre-#637 behaviour).
#[derive(Debug, Default, Clone)]
pub struct NoopOpenIntervalSource;

#[async_trait]
impl OpenIntervalSource for NoopOpenIntervalSource {
    async fn open_intervals(&self) -> Result<Vec<OpenInterval>, UsageLedgerError> {
        Ok(Vec::new())
    }
}

/// Summary of one [`run_usage_rollup_pass`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RollupReport {
    /// Authoritative closed events read from the source this pass.
    pub events_read: usize,
    /// Still-`Running` sandboxes accounted as open intervals this pass (#637).
    pub open_intervals: usize,
    /// Records written (one per `(team, key_name, hour)`).
    pub records_written: usize,
    /// Records whose write failed this pass (non-fatal; the next tick retries by
    /// re-deriving and overwriting).
    pub failed: usize,
    /// `true` iff the pass returned early because the rollup was disabled.
    pub skipped: bool,
}

impl RollupReport {
    /// Construct a "did nothing because the rollup is disabled" report.
    pub fn skipped() -> Self {
        Self {
            skipped: true,
            ..Default::default()
        }
    }
}

/// Run one rollup pass: list closed events + snapshot open intervals → roll up →
/// write each record.
///
/// Open-interval enumeration failing is non-fatal: it is logged and the pass
/// proceeds on closed events alone, so a transient apiserver hiccup degrades the
/// in-progress-hour accounting (#637) rather than the whole ledger. Per-record write
/// failures are likewise tallied and the pass continues; the next tick re-derives
/// and overwrites, so nothing is lost or double-counted. `now` is injected for
/// deterministic testing of the hour boundary.
pub async fn run_usage_rollup_pass(
    source: &dyn UsageEventSource,
    open_source: &dyn OpenIntervalSource,
    ledger: &dyn UsageLedgerSink,
    now: DateTime<Utc>,
) -> Result<RollupReport, UsageLedgerError> {
    let events = source.list_events().await?;
    let open = match open_source.open_intervals().await {
        Ok(open) => open,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "listing open intervals failed; rolling up closed events only this pass"
            );
            Vec::new()
        }
    };
    // An open interval whose `running_since` is at/after `now` (clock skew) produces
    // a non-positive span and is silently dropped by `rollup_events`. Surface it so a
    // systematically-skewed clock under-billing live sandboxes is observable rather
    // than silent (review on #639).
    let skewed = open.iter().filter(|o| o.running_since >= now).count();
    if skewed > 0 {
        tracing::warn!(
            skewed,
            "dropped open intervals with a future `running_since` (clock skew); \
             those live sandboxes are not billed for the in-progress hour this pass"
        );
    }
    let records = rollup_events(&events, &open, now);
    let mut written = 0;
    let mut failed = 0;
    for record in &records {
        match ledger.write_record(record).await {
            Ok(()) => written += 1,
            Err(e) => {
                tracing::warn!(
                    team = %record.team,
                    key_name = %record.key_name,
                    period = %record.period_start,
                    error = %e,
                    "usage ledger record write failed; continuing pass"
                );
                failed += 1;
            }
        }
    }
    Ok(RollupReport {
        events_read: events.len(),
        open_intervals: open.len(),
        records_written: written,
        failed,
        skipped: false,
    })
}

/// In-memory [`UsageEventSource`] for tests.
#[derive(Debug, Default, Clone)]
pub struct FakeUsageEventSource {
    events: Arc<std::sync::Mutex<Vec<UsageEvent>>>,
}

impl FakeUsageEventSource {
    /// Construct a source seeded with `events`.
    pub fn new(events: Vec<UsageEvent>) -> Self {
        Self {
            events: Arc::new(std::sync::Mutex::new(events)),
        }
    }
}

#[async_trait]
impl UsageEventSource for FakeUsageEventSource {
    async fn list_events(&self) -> Result<Vec<UsageEvent>, UsageLedgerError> {
        Ok(self
            .events
            .lock()
            .expect("FakeUsageEventSource poisoned")
            .clone())
    }
}

/// In-memory [`OpenIntervalSource`] for tests.
#[derive(Debug, Default, Clone)]
pub struct FakeOpenIntervalSource {
    open: Vec<OpenInterval>,
}

impl FakeOpenIntervalSource {
    /// Construct a source seeded with `open` intervals.
    pub fn new(open: Vec<OpenInterval>) -> Self {
        Self { open }
    }
}

#[async_trait]
impl OpenIntervalSource for FakeOpenIntervalSource {
    async fn open_intervals(&self) -> Result<Vec<OpenInterval>, UsageLedgerError> {
        Ok(self.open.clone())
    }
}

/// In-memory [`UsageLedgerSink`] for tests: idempotent on the object key, so a
/// re-run overwrites rather than appends — mirroring the object-store contract.
#[derive(Debug, Default, Clone)]
pub struct RecordingUsageLedger {
    records: Arc<std::sync::Mutex<BTreeMap<String, UsageRecord>>>,
}

impl RecordingUsageLedger {
    /// Construct an empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the stored records, sorted by object key.
    pub fn records(&self) -> Vec<UsageRecord> {
        self.records
            .lock()
            .expect("RecordingUsageLedger poisoned")
            .values()
            .cloned()
            .collect()
    }
}

#[async_trait]
impl UsageLedgerSink for RecordingUsageLedger {
    async fn write_record(&self, record: &UsageRecord) -> Result<(), UsageLedgerError> {
        // Key on the same object key the S3 impl uses, so a re-run overwrites the
        // same logical record (idempotent), exactly like an object-store PUT.
        self.records
            .lock()
            .expect("RecordingUsageLedger poisoned")
            .insert(record.object_key("ledger"), record.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::{build_usage_event, USAGE_EVENT_SCHEMA};

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }

    /// Build an event directly (bypassing `build_usage_event`'s `now` gating) for a
    /// fixed interval, so tests can place intervals across specific hours.
    fn event(team: &str, key: &str, uid: &str, start: &str, end: &str, weight: f64) -> UsageEvent {
        let running = (dt(end) - dt(start)).num_milliseconds() as f64 / 1000.0;
        UsageEvent {
            schema: USAGE_EVENT_SCHEMA,
            team: team.to_owned(),
            key_name: key.to_owned(),
            sandbox_id: uid.to_owned(),
            sandbox_uid: uid.to_owned(),
            interval_start: start.to_owned(),
            interval_end: end.to_owned(),
            running_seconds: running,
            cpu_request: "1".to_owned(),
            mem_request: "1Gi".to_owned(),
            weighted_seconds: running * weight,
        }
    }

    /// A still-open interval for `uid`, `Running` since `since` (cpu 1 / mem 1Gi →
    /// weight 1, so weighted == raw running-seconds).
    fn oi(team: &str, key: &str, uid: &str, since: &str) -> OpenInterval {
        OpenInterval {
            team: team.to_owned(),
            key_name: key.to_owned(),
            sandbox_uid: uid.to_owned(),
            cpu_request: "1".to_owned(),
            mem_request: "1Gi".to_owned(),
            running_since: dt(since),
        }
    }

    #[test]
    fn rolls_one_interval_into_its_hour() {
        // 10:00–10:30 (1800s) at weight 2 → 3600 weighted in hour 10.
        let events = [event(
            "team-a",
            "key-1",
            "sbx-1",
            "2026-06-21T10:00:00Z",
            "2026-06-21T10:30:00Z",
            2.0,
        )];
        let records = rollup_events(&events, &[], dt("2026-06-21T12:00:00Z"));
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.team, "team-a");
        assert_eq!(r.key_name, "key-1");
        assert_eq!(r.period_start, dt("2026-06-21T10:00:00Z").to_rfc3339());
        assert_eq!(r.running_seconds, 1800.0);
        assert_eq!(r.weighted_running_seconds, 3600.0);
        assert_eq!(r.distinct_sandboxes, 1);
        assert_eq!(r.source_interval_count, 1);
        assert!(!r.provisional, "hour 10 has elapsed by now=12:00 → final");
    }

    #[test]
    fn splits_interval_across_hour_boundary() {
        // 10:30–11:30 spans two hours, 1800s each. Weight 1 → 1800 weighted/hour.
        let events = [event(
            "team-a",
            "key-1",
            "sbx-1",
            "2026-06-21T10:30:00Z",
            "2026-06-21T11:30:00Z",
            1.0,
        )];
        let records = rollup_events(&events, &[], dt("2026-06-21T13:00:00Z"));
        assert_eq!(records.len(), 2, "one record per spanned hour");
        assert_eq!(
            records[0].period_start,
            dt("2026-06-21T10:00:00Z").to_rfc3339()
        );
        assert_eq!(records[0].running_seconds, 1800.0);
        assert_eq!(
            records[1].period_start,
            dt("2026-06-21T11:00:00Z").to_rfc3339()
        );
        assert_eq!(records[1].running_seconds, 1800.0);
    }

    #[test]
    fn current_hour_is_provisional_past_hours_final() {
        // now = 11:30 → hour 10 has fully elapsed (final), hour 11 is in progress
        // (provisional, accounted up to now).
        let events = [event(
            "team-a",
            "key-1",
            "sbx-1",
            "2026-06-21T10:30:00Z",
            "2026-06-21T11:30:00Z",
            1.0,
        )];
        let records = rollup_events(&events, &[], dt("2026-06-21T11:30:00Z"));
        assert_eq!(
            records.len(),
            2,
            "both the elapsed and in-progress hours roll"
        );
        assert_eq!(
            records[0].period_start,
            dt("2026-06-21T10:00:00Z").to_rfc3339()
        );
        assert_eq!(records[0].running_seconds, 1800.0);
        assert!(!records[0].provisional, "hour 10 has elapsed → final");
        assert_eq!(
            records[1].period_start,
            dt("2026-06-21T11:00:00Z").to_rfc3339()
        );
        assert_eq!(records[1].running_seconds, 1800.0);
        assert!(
            records[1].provisional,
            "hour 11 is in progress → provisional"
        );
    }

    #[test]
    fn aggregates_team_key_and_counts_distinct_sandboxes() {
        let events = [
            event(
                "t",
                "k",
                "sbx-1",
                "2026-06-21T10:00:00Z",
                "2026-06-21T10:20:00Z",
                1.0,
            ),
            event(
                "t",
                "k",
                "sbx-2",
                "2026-06-21T10:10:00Z",
                "2026-06-21T10:40:00Z",
                1.0,
            ),
            // Different key → its own record.
            event(
                "t",
                "k2",
                "sbx-3",
                "2026-06-21T10:00:00Z",
                "2026-06-21T10:30:00Z",
                1.0,
            ),
        ];
        let records = rollup_events(&events, &[], dt("2026-06-21T12:00:00Z"));
        assert_eq!(records.len(), 2);
        let k = records
            .iter()
            .find(|r| r.key_name == "k")
            .expect("key k record");
        assert_eq!(k.running_seconds, 1200.0 + 1800.0);
        assert_eq!(k.distinct_sandboxes, 2);
        assert_eq!(k.source_interval_count, 2);
    }

    #[test]
    fn reconciles_against_source_events() {
        // The ledger total must equal the sum of the source events' weighted
        // seconds over closed hours — the issue's reconciliation property.
        let events = [
            event(
                "t",
                "k",
                "sbx-1",
                "2026-06-21T10:15:00Z",
                "2026-06-21T11:45:00Z",
                2.0,
            ),
            event(
                "t",
                "k",
                "sbx-2",
                "2026-06-21T09:00:00Z",
                "2026-06-21T10:05:00Z",
                0.5,
            ),
        ];
        let records = rollup_events(&events, &[], dt("2026-06-21T13:00:00Z"));
        let ledger_total: f64 = records.iter().map(|r| r.weighted_running_seconds).sum();
        let source_total: f64 = events.iter().map(|e| e.weighted_seconds).sum();
        assert!(
            (ledger_total - source_total).abs() < 1e-6,
            "ledger {ledger_total} must reconcile with source {source_total}"
        );
    }

    #[test]
    fn rollup_is_deterministic_for_idempotent_writes() {
        let events = [
            event(
                "b",
                "k",
                "s2",
                "2026-06-21T10:00:00Z",
                "2026-06-21T10:30:00Z",
                1.0,
            ),
            event(
                "a",
                "k",
                "s1",
                "2026-06-21T10:00:00Z",
                "2026-06-21T10:30:00Z",
                1.0,
            ),
        ];
        let now = dt("2026-06-21T12:00:00Z");
        let first = rollup_events(&events, &[], now);
        let second = rollup_events(&events, &[], now);
        assert_eq!(first, second, "pure rollup must be deterministic");
        // Sorted by (team, key, period): team `a` before `b`.
        assert_eq!(first[0].team, "a");
        assert_eq!(first[1].team, "b");
    }

    #[test]
    fn build_usage_event_round_trips_through_rollup() {
        // An event produced by the real Phase 2 builder rolls up cleanly.
        let ev = build_usage_event(
            "team-acme",
            "key-1",
            "sbx-9",
            "uid-9",
            "2",
            "2Gi",
            Some(dt("2026-06-21T08:00:00Z")),
            dt("2026-06-21T08:30:00Z"),
        )
        .expect("event");
        let records = rollup_events(&[ev], &[], dt("2026-06-21T10:00:00Z"));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].weighted_running_seconds, 1800.0 * 2.0);
    }

    #[test]
    fn usage_record_deserializes_pre_637_json_as_final() {
        // A ledger object written before the `provisional` field existed must still
        // parse (it's the cross-system billing contract, and `Deserialize`), with the
        // missing field defaulting to `false` = final — the safe invoiceable reading
        // for such an (always-elapsed-hour) record. Guards the `#[serde(default)]`.
        let pre_637 = r#"{"schema":1,"team":"t","key_name":"k",
            "period_start":"2026-06-21T10:00:00+00:00",
            "period_end":"2026-06-21T11:00:00+00:00",
            "weighted_running_seconds":3600.0,"running_seconds":3600.0,
            "distinct_sandboxes":1,"source_interval_count":1}"#;
        let rec: UsageRecord = serde_json::from_str(pre_637).expect("pre-#637 record parses");
        assert!(
            !rec.provisional,
            "a record missing the field is read as final"
        );
    }

    #[test]
    fn accounts_open_interval_up_to_now_as_provisional() {
        // No closed events; one sandbox running since 10:00, now 10:40.
        let open = [oi("t", "k", "sbx-1", "2026-06-21T10:00:00Z")];
        let records = rollup_events(&[], &open, dt("2026-06-21T10:40:00Z"));
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].period_start,
            dt("2026-06-21T10:00:00Z").to_rfc3339()
        );
        assert_eq!(
            records[0].running_seconds, 2400.0,
            "billed up to now (40 min)"
        );
        assert_eq!(records[0].distinct_sandboxes, 1);
        assert!(
            records[0].provisional,
            "the in-progress hour for a still-running sandbox is provisional"
        );
    }

    #[test]
    fn open_interval_past_hours_are_final_and_unchanged_by_eventual_close() {
        // The finality + no-double-bill property: a still-running sandbox's *past*
        // hours, billed while it is open, are identical to what the eventual closed
        // event produces. Running since 09:30.
        let open = [oi("t", "k", "sbx-1", "2026-06-21T09:30:00Z")];
        let while_open = rollup_events(&[], &open, dt("2026-06-21T11:30:00Z"));
        // Sandbox closes at 11:45; a later rollup reads the authoritative event.
        let closed = [event(
            "t",
            "k",
            "sbx-1",
            "2026-06-21T09:30:00Z",
            "2026-06-21T11:45:00Z",
            1.0,
        )];
        let after_close = rollup_events(&closed, &[], dt("2026-06-21T12:30:00Z"));

        let by_hour = |recs: &[UsageRecord], h: &str| {
            recs.iter()
                .find(|r| r.period_start == dt(h).to_rfc3339())
                .cloned()
                .expect("hour record present")
        };
        for h in ["2026-06-21T09:00:00Z", "2026-06-21T10:00:00Z"] {
            let open_rec = by_hour(&while_open, h);
            let closed_rec = by_hour(&after_close, h);
            assert!(
                !open_rec.provisional && !closed_rec.provisional,
                "past hours are final both while open and after close"
            );
            assert_eq!(
                open_rec.running_seconds, closed_rec.running_seconds,
                "past-hour value is unchanged by the close — no double-bill"
            );
            assert_eq!(
                open_rec.weighted_running_seconds,
                closed_rec.weighted_running_seconds
            );
        }
    }

    #[test]
    fn never_closing_sandbox_bills_elapsed_past_hours() {
        // Running since 08:00, still open at 12:30, never closed — its elapsed hours
        // must still be billed (and final).
        let open = [oi("t", "k", "sbx-1", "2026-06-21T08:00:00Z")];
        let records = rollup_events(&[], &open, dt("2026-06-21T12:30:00Z"));

        let final_hours: Vec<_> = records.iter().filter(|r| !r.provisional).collect();
        assert_eq!(
            final_hours.len(),
            4,
            "hours 08–11 elapsed and are billed without ever closing"
        );
        assert!(
            final_hours.iter().all(|r| r.running_seconds == 3600.0),
            "each elapsed hour bills a full hour"
        );
        let provisional: Vec<_> = records.iter().filter(|r| r.provisional).collect();
        assert_eq!(provisional.len(), 1, "only the in-progress hour 12");
        assert_eq!(provisional[0].running_seconds, 1800.0, "12:00–12:30");
    }

    #[test]
    fn pause_resume_bills_both_intervals_not_the_pause_gap() {
        // First interval ran 10:00–10:20 and closed (authoritative event written).
        // The sandbox paused (10:20–10:40 — not billed), then resumed; its open
        // interval anchors at the *resume* (10:40), because `last_transition_time`
        // is overwritten on resume. The two intervals must both bill, the paused gap
        // must not, and there's no overlap/double-count.
        let closed = [event(
            "t",
            "k",
            "sbx-1",
            "2026-06-21T10:00:00Z",
            "2026-06-21T10:20:00Z",
            1.0,
        )];
        let open = [oi("t", "k", "sbx-1", "2026-06-21T10:40:00Z")];
        let records = rollup_events(&closed, &open, dt("2026-06-21T10:50:00Z"));

        assert_eq!(records.len(), 1, "both intervals land in hour 10");
        let r = &records[0];
        assert_eq!(
            r.running_seconds, 1800.0,
            "20 min + 10 min billed; the 20 min paused gap is not"
        );
        assert_eq!(r.distinct_sandboxes, 1, "same sandbox across pause/resume");
        assert_eq!(r.source_interval_count, 2, "the two intervals contributed");
        assert!(r.provisional, "hour 10 is still in progress at now=10:50");
    }

    #[tokio::test]
    async fn run_pass_accounts_open_intervals() {
        let source = FakeUsageEventSource::new(vec![]);
        let open_source =
            FakeOpenIntervalSource::new(vec![oi("t", "k", "sbx-1", "2026-06-21T10:00:00Z")]);
        let ledger = RecordingUsageLedger::new();

        let report =
            run_usage_rollup_pass(&source, &open_source, &ledger, dt("2026-06-21T11:30:00Z"))
                .await
                .expect("pass");
        assert_eq!(report.events_read, 0);
        assert_eq!(report.open_intervals, 1);
        // Hour 10 (final) + hour 11 (provisional) written from the open interval.
        assert_eq!(report.records_written, 2);
        assert_eq!(ledger.records().len(), 2);
    }

    #[tokio::test]
    async fn run_pass_writes_records_idempotently() {
        let events = vec![
            event(
                "t",
                "k",
                "s1",
                "2026-06-21T10:00:00Z",
                "2026-06-21T10:30:00Z",
                1.0,
            ),
            event(
                "t",
                "k",
                "s2",
                "2026-06-21T11:00:00Z",
                "2026-06-21T11:30:00Z",
                1.0,
            ),
        ];
        let source = FakeUsageEventSource::new(events);
        let ledger = RecordingUsageLedger::new();
        let now = dt("2026-06-21T13:00:00Z");

        let r1 = run_usage_rollup_pass(&source, &NoopOpenIntervalSource, &ledger, now)
            .await
            .expect("pass");
        assert_eq!(r1.events_read, 2);
        assert_eq!(r1.records_written, 2);
        assert_eq!(ledger.records().len(), 2);

        // Re-run: same events → same keys overwritten, NOT duplicated.
        let r2 = run_usage_rollup_pass(&source, &NoopOpenIntervalSource, &ledger, now)
            .await
            .expect("pass");
        assert_eq!(r2.records_written, 2);
        assert_eq!(
            ledger.records().len(),
            2,
            "re-run overwrites the same (team,key,hour) records — no double-bill"
        );
    }
}
