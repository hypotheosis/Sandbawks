//! OTel metrics for the envd-binary determinism check on restore.
//!
//! One instrument, under the meter `sandbawks-node.envd_digest`, recorded once
//! per call to [`crate::gvisor::GvisorAdapter::verify_envd_digest`] (issue #270):
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `envd_digest.checks_total` | `Counter<u64>` | --- | outcome, reason, path |
//!
//! The check has five mutually-exclusive exits, all folded into the single
//! counter so the gate is fully accountable (issue #277):
//!
//! | `outcome` | `reason` | Exit | Meaning |
//! |---|---|---|---|
//! | `verified` | `match` | `Ok(())` | both digests known and byte-identical |
//! | `skipped` | `empty_manifest` | `Ok(())` | snapshot recorded no digest (v1 manifest, or a pause whose worker couldn't read the binary) |
//! | `skipped` | `local_unreadable` | `Ok(())` | this node's staged envd binary could not be read |
//! | `skipped` | `envd_captured` | `Ok(())` | capture-mode snapshot (epic #466): envd's pages ride in `pages.img`, so the host binary is irrelevant |
//! | `failed` | `mismatch` | `Err(..)` | both digests known and differ — fail-closed restore abort |
//!
//! Counting every exit (not just the two `skipped` fall-throughs the issue
//! names) turns the warn-then-proceed `skipped` branches — previously visible
//! only as a `tracing::warn!` — into alertable series. The `reason` label
//! preserves the issue's proposed `empty_manifest` / `local_unreadable`
//! vocabulary and adds `envd_captured` for the capture-mode skip.
//!
//! The `path` label records *which* of [`verify_envd_digest`]'s three callers
//! ran the check (`resume` | `stage` | `pause_validate`). It exists because the
//! check also fires on the pause node's restorability round-trip
//! (`validate_restorable`), where — by that function's own docstring — it is a
//! tautology that always records `verified` / `match` and so would otherwise
//! inflate the restore SLO. The operator SLO *"fraction of restores where
//! determinism was actually verified"* is therefore
//! `sum(outcome="verified", path=~"resume|stage") / sum(path=~"resume|stage")`,
//! i.e. with the pause-side tautology filtered out.
//!
//! [`verify_envd_digest`]: crate::gvisor::GvisorAdapter::verify_envd_digest
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in tests) the global meter is a no-op, so this call
//! stays on the restore path unconditionally.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.envd_digest";

/// The result of one envd-digest determinism check, carrying the exact
/// `(outcome, reason)` label pair it maps to.
///
/// Modelling the four exits as an enum (rather than passing two `&str`s at each
/// call site) keeps the label vocabulary closed and the mapping
/// exhaustively checked by the compiler — a Rust idiom a Python developer might
/// otherwise express as loose string constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestCheckOutcome {
    /// Both digests known and byte-identical — determinism actually verified.
    Verified,
    /// Snapshot carried no recorded digest; nothing to verify against.
    SkippedEmptyManifest,
    /// This node's staged envd binary could not be read; nothing to verify with.
    SkippedLocalUnreadable,
    /// Snapshot is capture-mode (`envd_captured`, epic #466): the checkpointed
    /// envd's code pages ride in `pages.img`, so the host binary is irrelevant
    /// and the comparison is deliberately skipped. The per-part sha256 gate
    /// still protects the bytes.
    SkippedEnvdCaptured,
    /// Both digests known and differ — restore aborted fail-closed.
    FailedMismatch,
}

impl DigestCheckOutcome {
    /// Stable `outcome` label: `verified` | `skipped` | `failed`.
    pub(crate) fn outcome(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::SkippedEmptyManifest
            | Self::SkippedLocalUnreadable
            | Self::SkippedEnvdCaptured => "skipped",
            Self::FailedMismatch => "failed",
        }
    }

    /// Stable `reason` label disambiguating the `outcome`.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Verified => "match",
            Self::SkippedEmptyManifest => "empty_manifest",
            Self::SkippedLocalUnreadable => "local_unreadable",
            Self::SkippedEnvdCaptured => "envd_captured",
            Self::FailedMismatch => "mismatch",
        }
    }
}

/// Which caller of [`verify_envd_digest`] ran the determinism check.
///
/// Recorded as the `path` label so the pause-side tautology
/// (`PauseValidate`) can be filtered out of the restore SLO. The three
/// variants correspond one-to-one to the call sites of
/// [`crate::gvisor::GvisorAdapter::fetch_verify_and_lay_out`].
///
/// [`verify_envd_digest`]: crate::gvisor::GvisorAdapter::verify_envd_digest
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckPath {
    /// Live memory restore (`resume`) — the path this gate exists for.
    Resume,
    /// envd-staged restore prep (`stage_snapshot`, #286).
    Stage,
    /// Pause-node restorability round-trip (`validate_restorable`), where the
    /// check is a tautology — excluded from the restore SLO.
    PauseValidate,
}

impl CheckPath {
    /// Stable, lowercase `path` label.
    fn label(self) -> &'static str {
        match self {
            Self::Resume => "resume",
            Self::Stage => "stage",
            Self::PauseValidate => "pause_validate",
        }
    }
}

/// Cached envd-digest instruments, built once via [`metrics`].
struct EnvdDigestMetrics {
    /// `envd_digest.checks_total` — determinism checks by outcome, reason, path.
    checks: Counter<u64>,
}

impl EnvdDigestMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            checks: meter
                .u64_counter("envd_digest.checks_total")
                .with_description(
                    "envd-binary determinism checks on restore, by outcome, reason, and path.",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::envd_digest_metrics`]).
fn metrics() -> &'static EnvdDigestMetrics {
    static METRICS: OnceLock<EnvdDigestMetrics> = OnceLock::new();
    METRICS.get_or_init(EnvdDigestMetrics::new)
}

/// Record one envd-digest determinism check at the exit it took, labelled by
/// the caller `path` that ran it.
///
/// Each `verify_envd_digest` call increments the counter exactly once. The
/// determinism-verified SLO filters the pause-side tautology:
/// `sum(outcome="verified", path=~"resume|stage") / sum(path=~"resume|stage")`.
pub fn record_check(path: CheckPath, outcome: DigestCheckOutcome) {
    let attrs = [
        KeyValue::new("outcome", outcome.outcome()),
        KeyValue::new("reason", outcome.reason()),
        KeyValue::new("path", path.label()),
    ];
    metrics().checks.add(1, &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_cover_every_outcome() {
        // Exhaustive over the four exits; the compiler guarantees the `match`
        // arms in `outcome`/`reason` stay in sync with the enum.
        let cases = [
            (DigestCheckOutcome::Verified, "verified", "match"),
            (
                DigestCheckOutcome::SkippedEmptyManifest,
                "skipped",
                "empty_manifest",
            ),
            (
                DigestCheckOutcome::SkippedLocalUnreadable,
                "skipped",
                "local_unreadable",
            ),
            (
                DigestCheckOutcome::SkippedEnvdCaptured,
                "skipped",
                "envd_captured",
            ),
            (DigestCheckOutcome::FailedMismatch, "failed", "mismatch"),
        ];
        for (outcome, expect_outcome, expect_reason) in cases {
            assert_eq!(outcome.outcome(), expect_outcome);
            assert_eq!(outcome.reason(), expect_reason);
        }
    }

    #[test]
    fn path_label_covers_every_caller() {
        assert_eq!(CheckPath::Resume.label(), "resume");
        assert_eq!(CheckPath::Stage.label(), "stage");
        assert_eq!(CheckPath::PauseValidate.label(), "pause_validate");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; `record_check` must still be safe to invoke on every exit.
        record_check(CheckPath::Resume, DigestCheckOutcome::Verified);
        record_check(CheckPath::Stage, DigestCheckOutcome::SkippedEmptyManifest);
        record_check(
            CheckPath::Resume,
            DigestCheckOutcome::SkippedLocalUnreadable,
        );
        record_check(CheckPath::Resume, DigestCheckOutcome::FailedMismatch);
        record_check(CheckPath::Resume, DigestCheckOutcome::SkippedEnvdCaptured);
        record_check(CheckPath::PauseValidate, DigestCheckOutcome::Verified);
    }
}
