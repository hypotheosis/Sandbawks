//! OTel metrics for the warm-pool claim fast path (OBSERVABILITY.md §8.6,
//! issue #288).
//!
//! Two instruments, under the meter `sandbawks-api.warm_claim`, recorded by
//! [`crate::warm_claim::claim_for`] — the gateway's headline warm-vs-cold SLI:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `warm.claims_total` | `Counter<u64>` | --- | pool, outcome |
//! | `warm.claim.duration` | `Histogram<f64>` | s | pool |
//!
//! ## The `outcome` label
//!
//! `warm.claims_total` extends the issue's four-value vocabulary with a fifth
//! `error` outcome so the counter accounts for *every* claim attempt, not just
//! the ones that completed without a backing-store failure:
//!
//! | `outcome` | When |
//! |---|---|
//! | `hit` | A live warm member was claimed (reassigned) for the request. |
//! | `miss_no_pool` | No [`SandboxWarmPool`] covers the requested template. |
//! | `miss_drained` | A covering pool existed but every candidate was claimed by a concurrent request before this one could (cold-boot fallback). |
//! | `error` | A backing `kube` call failed; the gateway propagated a 5xx instead of serving warm or cold-booting. |
//! | `race_lost` | One lost compare-and-swap on a single candidate. |
//!
//! Four of these — `hit`, `miss_no_pool`, `miss_drained`, `error` — are
//! **terminal**: each [`claim_for`] call records exactly one of them, so they
//! partition every claim attempt. The headline SLI is
//!
//! ```text
//! warm hit rate = sum(warm_claims_total{outcome="hit"})
//!               / sum(warm_claims_total{outcome=~"hit|miss_no_pool|miss_drained"})
//! ```
//!
//! The denominator pins three of the four terminal outcomes: it excludes `error`
//! because an errored claim served neither a warm member nor a cold-boot
//! fallthrough — it is already visible as a failure in the api's
//! `http.server.request.count`, so folding it into the warm-vs-cold ratio would
//! understate the hit rate for an outage unrelated to pool sizing. `error` still
//! completes the partition and is independently alertable; it is simply not part
//! of the warm-vs-cold ratio.
//!
//! `race_lost` is **orthogonal**: it is emitted once per *individual* lost CAS
//! inside the retry loop (a request that loses two candidates then claims a
//! third records `race_lost`, `race_lost`, `hit`). It is a contention signal —
//! a sustained `rate(warm_claims_total{outcome="race_lost"})` flags the
//! create↔delete thrash the issue calls out — and is likewise **excluded** from
//! the hit-rate denominator.
//!
//! ## The histogram
//!
//! `warm.claim.duration` times the sub-second handoff the feature promises:
//! [`claim_for`] entry to [`crate::warm_claim::WarmClaim::apply_overrides`]
//! return. It is recorded **only on a `hit`** — the miss paths claim nothing
//! and have no handoff to measure — so the histogram's count equals
//! `warm_claims_total{outcome="hit"}` and it needs no `outcome` attribute.
//!
//! The instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in unit tests) the global meter is a no-op, so these
//! calls stay on the claim path unconditionally at zero cost.

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-api.warm_claim";

/// `pool` label used when no pool covers the request (the `miss_no_pool` case).
///
/// A constant rather than an empty string so the series is self-describing on a
/// dashboard and the `pool` attribute is always present — keeping one stable
/// schema across all four outcomes.
const NO_POOL: &str = "none";

/// Terminal/contention outcome of the warm-claim path, carrying the exact
/// `outcome` label it maps to.
///
/// Modelling the vocabulary as an enum (rather than passing a bare `&str` at
/// each call site) keeps the label set closed and the mapping exhaustively
/// checked by the compiler — a Rust idiom a Python developer might otherwise
/// express as loose string constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// A live warm member was claimed/reassigned (terminal).
    Hit,
    /// No covering pool for the requested template (terminal).
    MissNoPool,
    /// A pool existed but drained under contention before this request could
    /// claim a member (terminal — cold-boot fallback).
    MissDrained,
    /// The request carries per-create `volumeClaimTemplates`, which a
    /// pre-warmed member cannot satisfy, so the claim is skipped before any
    /// candidate is examined (terminal — cold-boot fallback). Epic #603 Phase 4
    /// / issue #607.
    MissIncompatible,
    /// A backing `kube` call failed and the gateway propagated the error
    /// (terminal — neither warm-served nor cold-boot fallthrough).
    Error,
    /// One lost compare-and-swap on a single candidate (per-CAS, non-terminal).
    RaceLost,
}

impl ClaimOutcome {
    /// Stable, lowercase `outcome` label.
    fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::MissNoPool => "miss_no_pool",
            Self::MissDrained => "miss_drained",
            Self::MissIncompatible => "miss_incompatible",
            Self::Error => "error",
            Self::RaceLost => "race_lost",
        }
    }
}

/// Cached warm-claim instruments, built once via [`metrics`].
struct WarmClaimMetrics {
    /// `warm.claims_total` — claim outcomes (see [`ClaimOutcome`]).
    claims: Counter<u64>,
    /// `warm.claim.duration` — hit-only handoff latency in seconds.
    duration: Histogram<f64>,
}

impl WarmClaimMetrics {
    /// Build both instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            claims: meter
                .u64_counter("warm.claims_total")
                .with_description("Warm-pool claim attempts by pool and outcome.")
                .build(),
            duration: meter
                .f64_histogram("warm.claim.duration")
                .with_unit("s")
                .with_description("Warm-pool claim handoff latency (hits only).")
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::warm_claim_metrics`]).
fn metrics() -> &'static WarmClaimMetrics {
    static METRICS: OnceLock<WarmClaimMetrics> = OnceLock::new();
    METRICS.get_or_init(WarmClaimMetrics::new)
}

/// Record one warm-claim `outcome`, labelled by `pool`.
///
/// `pool` is `None` only for [`ClaimOutcome::MissNoPool`] (there is no covering
/// pool to name); it is folded to the [`NO_POOL`] marker so the `pool` attribute
/// is always present. Call once per terminal outcome and once per lost CAS — see
/// the module docs for how the four outcomes compose.
pub fn record_claim(pool: Option<&str>, outcome: ClaimOutcome) {
    let attrs = [
        KeyValue::new("pool", pool.unwrap_or(NO_POOL).to_owned()),
        KeyValue::new("outcome", outcome.label()),
    ];
    metrics().claims.add(1, &attrs);
}

/// Record the claim handoff latency for a `hit`, labelled by `pool`.
///
/// Recorded once, immediately after the overrides patch returns, so the
/// histogram's count tracks `warm_claims_total{outcome="hit"}`.
pub fn record_handoff(pool: &str, elapsed: Duration) {
    let attrs = [KeyValue::new("pool", pool.to_owned())];
    metrics().duration.record(elapsed.as_secs_f64(), &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_label_covers_every_variant() {
        assert_eq!(ClaimOutcome::Hit.label(), "hit");
        assert_eq!(ClaimOutcome::MissNoPool.label(), "miss_no_pool");
        assert_eq!(ClaimOutcome::MissDrained.label(), "miss_drained");
        assert_eq!(ClaimOutcome::MissIncompatible.label(), "miss_incompatible");
        assert_eq!(ClaimOutcome::Error.label(), "error");
        assert_eq!(ClaimOutcome::RaceLost.label(), "race_lost");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; both recorders must still be safe to invoke on the claim path,
        // including the `None` (miss_no_pool) pool label.
        record_claim(None, ClaimOutcome::MissNoPool);
        record_claim(Some("py-pool"), ClaimOutcome::RaceLost);
        record_claim(Some("py-pool"), ClaimOutcome::Hit);
        record_handoff("py-pool", Duration::from_millis(420));
    }
}
