//! Authoritative per-team sandbox usage events (epic #480 Phase 2, issue #631).
//!
//! The controller's reconcile loop is the only vantage point that observes every
//! `running` / `paused` / `terminated` transition, so it is the authoritative
//! source of *billable sandbox-seconds*. On each edge that closes a `Running`
//! interval (pause, terminate, fail) the loop emits a [`UsageEvent`] — a durable,
//! append-only record of one billable interval — through a pluggable
//! [`UsageEventSink`].
//!
//! ## Authoritative, not OTel
//!
//! Per the epic's trust-model decision (#480, comment #16264), **OpenTelemetry is
//! not a valid authoritative source for billing**: metric retention, downsampling
//! and reset semantics make raw counters unsafe to invoice from. This event stream
//! is therefore the *system of record* the durable NDJSON ledger (Phase 3, #632)
//! rolls up from — distinct from any OTel `usage.sandbox.*` counters, which are an
//! explicitly non-authoritative observability mirror.
//!
//! ## Idempotency
//!
//! Each event is keyed by `(sandbox_uid, interval_start)` — the Running-entry
//! timestamp, which is stable across reconciles and controller restarts because it
//! is read from the persisted `status.lastTransitionTime` *before* the close-edge
//! status patch advances it. A crash between emitting and the status patch re-runs
//! the same edge on the next reconcile and re-emits the *same* key, so an
//! idempotent sink (object-store PUT to a key derived from
//! [`UsageEvent::idempotency_key`]) overwrites rather than double-bills. Once the
//! status patch lands, the phase is no longer `Running` and the edge cannot
//! re-fire. See [`UsageEvent::object_key`].
//!
//! ## Resource weighting
//!
//! `weighted_seconds = running_seconds * resource_weight`, where the weight is the
//! sandbox's CPU/RAM **request** normalized to compute-equivalent units (answer 1
//! of #480): `max(cpu_cores, mem_gib / `[`MEM_GIB_PER_COMPUTE_UNIT`]`)`. This is a
//! resource *normalization*, not a price — a 4-vCPU box weighs ~4x a 0.5-vCPU one,
//! and a memory-heavy box is weighted by its RAM when that dominates. Pricing
//! itself is out of scope; the raw `cpu_request` / `mem_request` strings ride on
//! the event so a later pricing model can re-weight independently.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// GiB of memory treated as one compute-equivalent unit when normalizing the
/// memory request against the CPU request for [`resource_weight`].
///
/// 4 GiB/core is a common general-purpose node ratio; it is a normalization
/// constant (so RAM-heavy boxes are not undercounted), explicitly **not** a price.
pub const MEM_GIB_PER_COMPUTE_UNIT: f64 = 4.0;

/// Schema version stamped on every [`UsageEvent`] so the Phase 3 ledger reader can
/// evolve the event shape without ambiguity.
pub const USAGE_EVENT_SCHEMA: u32 = 1;

/// One authoritative billable interval: the time a single sandbox spent `Running`
/// between two lifecycle transitions, attributed to its owning team and key.
///
/// Serialized as a single JSON object (one NDJSON line) by the sink. Field names
/// are snake_case and stable — they are a cross-phase contract the Phase 3 ledger
/// reader (#632) depends on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageEvent {
    /// Event-shape version (see [`USAGE_EVENT_SCHEMA`]).
    pub schema: u32,
    /// Owning team — the sandbox's namespace (1 team = 1 namespace).
    pub team: String,
    /// Owning `TeamAPIKey` name (the per-key breakdown dimension). Empty string
    /// when the sandbox carries no `sandbawks.dev/api-key-name` annotation
    /// (operator-applied or pre-#631 sandbox) — billed to the team, key "unknown".
    pub key_name: String,
    /// Sandbox CR name (the human-facing `sbx-...` id).
    pub sandbox_id: String,
    /// Sandbox CR `metadata.uid` — the idempotency anchor, stable across renames
    /// (there are none) and unique across a name reused after deletion.
    pub sandbox_uid: String,
    /// RFC 3339 instant the sandbox entered `Running` (interval start).
    pub interval_start: String,
    /// RFC 3339 instant the sandbox left `Running` (interval end).
    pub interval_end: String,
    /// Wall-clock seconds in `Running` for this interval (`end - start`).
    pub running_seconds: f64,
    /// CPU request verbatim (e.g. `"500m"`, `"2"`), for re-weighting downstream.
    pub cpu_request: String,
    /// Memory request verbatim (e.g. `"512Mi"`, `"4Gi"`), for re-weighting.
    pub mem_request: String,
    /// `running_seconds * resource_weight` — the resource-weighted billable figure.
    pub weighted_seconds: f64,
}

impl UsageEvent {
    /// Stable idempotency key for this interval: `"<uid>:<interval_start>"`.
    ///
    /// Two emissions of the same closed interval (e.g. a re-driven reconcile after
    /// a crash) produce the same key, so the ledger never double-counts.
    pub fn idempotency_key(&self) -> String {
        format!("{}:{}", self.sandbox_uid, self.interval_start)
    }

    /// Object-store key this event is written under, rooted at `prefix`.
    ///
    /// `"<prefix>/<team>/<uid>/<interval_start>.json"`. Deterministic in the
    /// idempotency key, so re-emitting overwrites the same object (idempotent PUT)
    /// rather than appending a duplicate. The `team`/`uid` path segments keep the
    /// Phase 3 rollup's per-team LIST cheap. Colons in the RFC 3339 timestamp are
    /// replaced so the key is portable across object stores that dislike them.
    pub fn object_key(&self, prefix: &str) -> String {
        let base = prefix.trim_end_matches('/');
        let stamp = self.interval_start.replace(':', "-");
        if base.is_empty() {
            format!("{}/{}/{stamp}.json", self.team, self.sandbox_uid)
        } else {
            format!("{base}/{}/{}/{stamp}.json", self.team, self.sandbox_uid)
        }
    }

    /// Serialize to a single NDJSON line (compact JSON + trailing newline).
    pub fn to_ndjson_line(&self) -> Result<String, UsageSinkError> {
        let mut line =
            serde_json::to_string(self).map_err(|e| UsageSinkError::Serialize(e.to_string()))?;
        line.push('\n');
        Ok(line)
    }
}

/// Parse a Kubernetes CPU quantity into cores.
///
/// `"1"`/`"2"` → whole cores; a trailing `m` is millicores (`"500m"` → `0.5`);
/// a bare decimal (`"0.5"`) is cores. Returns `None` for an unparseable value.
pub fn parse_cpu_cores(raw: &str) -> Option<f64> {
    let raw = raw.trim();
    if let Some(millis) = raw.strip_suffix('m') {
        return millis.parse::<f64>().ok().map(|m| m / 1000.0);
    }
    raw.parse::<f64>().ok()
}

/// Parse a Kubernetes memory quantity into GiB.
///
/// Handles binary suffixes (`Ki`/`Mi`/`Gi`/`Ti`/`Pi`/`Ei`), decimal SI suffixes
/// (`k`/`M`/`G`/`T`/`P`/`E`, case-insensitive on the leading letter), and a bare
/// byte count. Returns `None` for an unparseable value.
pub fn parse_mem_gib(raw: &str) -> Option<f64> {
    const GIB: f64 = 1_073_741_824.0; // 1024^3
    let raw = raw.trim();
    // Binary (power-of-two) suffixes first — they end in `i`.
    let binary = [
        ("Ki", 1024f64),
        ("Mi", 1024f64.powi(2)),
        ("Gi", 1024f64.powi(3)),
        ("Ti", 1024f64.powi(4)),
        ("Pi", 1024f64.powi(5)),
        ("Ei", 1024f64.powi(6)),
    ];
    for (suffix, mult) in binary {
        if let Some(num) = raw.strip_suffix(suffix) {
            return num.parse::<f64>().ok().map(|n| n * mult / GIB);
        }
    }
    // Decimal SI suffixes (power-of-ten). Single trailing letter.
    let decimal = [
        ("k", 1e3),
        ("K", 1e3),
        ("M", 1e6),
        ("G", 1e9),
        ("T", 1e12),
        ("P", 1e15),
        ("E", 1e18),
    ];
    for (suffix, mult) in decimal {
        if let Some(num) = raw.strip_suffix(suffix) {
            return num.parse::<f64>().ok().map(|n| n * mult / GIB);
        }
    }
    // Bare byte count.
    raw.parse::<f64>().ok().map(|n| n / GIB)
}

/// Resource-normalization weight for a sandbox's CPU/RAM request.
///
/// `max(cpu_cores, mem_gib / `[`MEM_GIB_PER_COMPUTE_UNIT`]`)`. When neither field
/// parses (or both resolve to ≤ 0) the weight falls back to `1.0` so a malformed
/// request bills flat rather than free — the raw strings stay on the event so the
/// anomaly is auditable and re-weightable downstream.
pub fn resource_weight(cpu_request: &str, mem_request: &str) -> f64 {
    let cpu = parse_cpu_cores(cpu_request).unwrap_or(0.0);
    let mem_units = parse_mem_gib(mem_request).unwrap_or(0.0) / MEM_GIB_PER_COMPUTE_UNIT;
    let weight = cpu.max(mem_units);
    if weight > 0.0 {
        weight
    } else {
        1.0
    }
}

/// Build the [`UsageEvent`] for a closed `Running` interval.
///
/// Returns `None` when `interval_start` is absent (the sandbox entered `Running`
/// before this controller began stamping `lastTransitionTime`, so the interval
/// cannot be bounded — better to skip than bill a guessed start) or when the
/// computed duration is not strictly positive (clock skew / a zero-length
/// interval), which would be a meaningless zero-second bill.
#[allow(clippy::too_many_arguments)]
pub fn build_usage_event(
    team: &str,
    key_name: &str,
    sandbox_id: &str,
    sandbox_uid: &str,
    cpu_request: &str,
    mem_request: &str,
    interval_start: Option<DateTime<Utc>>,
    interval_end: DateTime<Utc>,
) -> Option<UsageEvent> {
    let start = interval_start?;
    let running_seconds = (interval_end - start).num_milliseconds() as f64 / 1000.0;
    if running_seconds <= 0.0 {
        return None;
    }
    let weighted_seconds = running_seconds * resource_weight(cpu_request, mem_request);
    Some(UsageEvent {
        schema: USAGE_EVENT_SCHEMA,
        team: team.to_owned(),
        key_name: key_name.to_owned(),
        sandbox_id: sandbox_id.to_owned(),
        sandbox_uid: sandbox_uid.to_owned(),
        interval_start: start.to_rfc3339(),
        interval_end: interval_end.to_rfc3339(),
        running_seconds,
        cpu_request: cpu_request.to_owned(),
        mem_request: mem_request.to_owned(),
        weighted_seconds,
    })
}

/// Errors surfaced by a [`UsageEventSink`].
#[derive(Debug, Error)]
pub enum UsageSinkError {
    /// The event could not be serialized to NDJSON.
    #[error("usage event serialize error: {0}")]
    Serialize(String),
    /// The backing durable store rejected the write.
    #[error("usage sink store error: {0}")]
    Store(String),
}

/// Durable, append-only sink for authoritative [`UsageEvent`]s.
///
/// The production impl is object-store-backed
/// ([`crate::s3_usage::S3UsageSink`]); [`NoopUsageSink`] is the default when usage
/// metering is disabled, and tests drive [`RecordingUsageSink`]. The trait is the
/// seam that keeps the reconcile loop testable without standing up object storage,
/// the same pattern as [`crate::retention::SnapshotRetentionStore`].
#[async_trait]
pub trait UsageEventSink: Send + Sync {
    /// Persist one usage event. MUST be idempotent on
    /// [`UsageEvent::idempotency_key`]: re-emitting a closed interval (crash
    /// re-drive) overwrites rather than duplicates, so the ledger never
    /// double-bills.
    async fn emit(&self, event: &UsageEvent) -> Result<(), UsageSinkError>;
}

/// No-op sink: the default when usage metering is not configured.
///
/// `emit` is infallible and does nothing, so the reconcile loop can call the sink
/// unconditionally with zero cost when metering is off — mirroring how the no-op
/// OTel meter lets metric calls stay on the hot path.
#[derive(Debug, Default, Clone)]
pub struct NoopUsageSink;

#[async_trait]
impl UsageEventSink for NoopUsageSink {
    async fn emit(&self, _event: &UsageEvent) -> Result<(), UsageSinkError> {
        Ok(())
    }
}

/// In-memory [`UsageEventSink`] for tests: records every emitted event.
///
/// Kept always-public (not `#[cfg(test)]`-gated) so integration tests in sibling
/// modules and the reconcile loop's own tests can assert on emissions, mirroring
/// [`crate::retention::FakeRetentionStore`].
#[derive(Debug, Default, Clone)]
pub struct RecordingUsageSink {
    events: Arc<std::sync::Mutex<Vec<UsageEvent>>>,
}

impl RecordingUsageSink {
    /// Construct an empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the events emitted so far, in emission order.
    pub fn events(&self) -> Vec<UsageEvent> {
        self.events
            .lock()
            .expect("RecordingUsageSink poisoned")
            .clone()
    }
}

#[async_trait]
impl UsageEventSink for RecordingUsageSink {
    async fn emit(&self, event: &UsageEvent) -> Result<(), UsageSinkError> {
        self.events
            .lock()
            .expect("RecordingUsageSink poisoned")
            .push(event.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_quantities() {
        assert_eq!(parse_cpu_cores("1"), Some(1.0));
        assert_eq!(parse_cpu_cores("2"), Some(2.0));
        assert_eq!(parse_cpu_cores("500m"), Some(0.5));
        assert_eq!(parse_cpu_cores("1500m"), Some(1.5));
        assert_eq!(parse_cpu_cores("0.5"), Some(0.5));
        assert_eq!(parse_cpu_cores("garbage"), None);
    }

    #[test]
    fn parses_memory_quantities_to_gib() {
        assert_eq!(parse_mem_gib("1Gi"), Some(1.0));
        assert_eq!(parse_mem_gib("512Mi"), Some(0.5));
        assert_eq!(parse_mem_gib("2Gi"), Some(2.0));
        assert_eq!(parse_mem_gib("1048576Ki"), Some(1.0));
        // 1 GiB exactly, as a bare byte count.
        assert_eq!(parse_mem_gib("1073741824"), Some(1.0));
        assert_eq!(parse_mem_gib("nope"), None);
    }

    #[test]
    fn resource_weight_takes_the_dominant_dimension() {
        // CPU-bound: 4 vCPU, 4Gi → cpu 4 vs mem 1 → 4.
        assert_eq!(resource_weight("4", "4Gi"), 4.0);
        // Memory-bound: 1 vCPU, 16Gi → cpu 1 vs mem 16/4=4 → 4.
        assert_eq!(resource_weight("1", "16Gi"), 4.0);
        // Sub-core: 500m, 1Gi → cpu 0.5 vs mem 0.25 → 0.5.
        assert_eq!(resource_weight("500m", "1Gi"), 0.5);
        // Unparseable both → flat 1.0 fallback (bill, don't drop).
        assert_eq!(resource_weight("?", "?"), 1.0);
    }

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }

    #[test]
    fn builds_a_weighted_event_for_a_bounded_interval() {
        let event = build_usage_event(
            "team-acme",
            "key-1",
            "sbx-1",
            "uid-1",
            "2",
            "2Gi",
            Some(dt("2026-06-21T10:00:00Z")),
            dt("2026-06-21T10:01:00Z"),
        )
        .expect("a bounded positive interval yields an event");
        assert_eq!(event.running_seconds, 60.0);
        // weight = max(2 cores, 2Gi/4) = 2 → 120 weighted seconds.
        assert_eq!(event.weighted_seconds, 120.0);
        assert_eq!(event.team, "team-acme");
        assert_eq!(event.key_name, "key-1");
        assert_eq!(event.idempotency_key(), "uid-1:2026-06-21T10:00:00+00:00");
    }

    #[test]
    fn skips_when_interval_start_is_unknown() {
        // No Running-entry anchor (pre-#631 sandbox) → nothing to bill.
        assert!(build_usage_event(
            "t",
            "k",
            "s",
            "u",
            "1",
            "1Gi",
            None,
            dt("2026-06-21T10:01:00Z"),
        )
        .is_none());
    }

    #[test]
    fn skips_non_positive_intervals() {
        // end <= start (clock skew / zero-length) → no meaningless zero bill.
        assert!(build_usage_event(
            "t",
            "k",
            "s",
            "u",
            "1",
            "1Gi",
            Some(dt("2026-06-21T10:01:00Z")),
            dt("2026-06-21T10:01:00Z"),
        )
        .is_none());
    }

    #[test]
    fn object_key_is_deterministic_and_colon_safe() {
        let event = build_usage_event(
            "team-acme",
            "key-1",
            "sbx-1",
            "uid-1",
            "1",
            "1Gi",
            Some(dt("2026-06-21T10:00:00Z")),
            dt("2026-06-21T10:01:00Z"),
        )
        .expect("event");
        assert_eq!(
            event.object_key("usage"),
            "usage/team-acme/uid-1/2026-06-21T10-00-00+00-00.json"
        );
        // Same closed interval → same key (idempotent overwrite, no double-bill).
        let again = event.clone();
        assert_eq!(event.object_key("usage"), again.object_key("usage"));
    }

    #[tokio::test]
    async fn recording_sink_captures_emitted_events() {
        let sink = RecordingUsageSink::new();
        let event = build_usage_event(
            "t",
            "k",
            "s",
            "u",
            "1",
            "1Gi",
            Some(dt("2026-06-21T10:00:00Z")),
            dt("2026-06-21T10:01:00Z"),
        )
        .expect("event");
        sink.emit(&event).await.expect("noop-safe emit");
        assert_eq!(sink.events(), vec![event]);
    }
}
