//! OTel metrics for the `SandboxWarmPool` reconciler (OBSERVABILITY.md §8.7,
//! issue #288).
//!
//! Six instruments, under the meter `sandbawks-controller.warm_pool`, recorded
//! once per [`crate::warm_pool::reconcile_pool`] pass and all labelled by `pool`
//! so a dashboard can plot ready-vs-target and catch top-up thrash per pool:
//!
//! | Instrument | Type | Unit | Attributes | Meaning |
//! |---|---|---|---|---|
//! | `warm_pool.ready` | `Gauge<u64>` | --- | pool | `Running` members ready to claim |
//! | `warm_pool.failed` | `Gauge<u64>` | --- | pool | members observed in a terminal `Failed`/`Terminating` phase this pass, then reaped (issue #457) |
//! | `warm_pool.min_ready` | `Gauge<u64>` | --- | pool | configured floor (`spec.minReady`) |
//! | `warm_pool.max_ready` | `Gauge<u64>` | --- | pool | configured ceiling (`spec.maxReady`) |
//! | `warm_pool.created_total` | `Counter<u64>` | --- | pool | warm sandboxes created |
//! | `warm_pool.deleted_total` | `Counter<u64>` | --- | pool | warm sandboxes deleted (ceiling trims + reclaim + evict + Failed reap) |
//!
//! ## Why a gauge *and* the two target gauges
//!
//! `warm_pool.ready` against `warm_pool.min_ready` is the warm-pool's health
//! SLI: a pool sitting below its floor (`ready < min_ready`) means the next
//! claim cold-boots. Exporting `min_ready` / `max_ready` as their own series —
//! rather than leaving an operator to cross-reference the CR spec — lets a
//! single dashboard query plot the band the `ready` line should stay inside.
//! The reconciler re-records all three every pass, so a deleted pool's series
//! simply goes stale (standard gauge behaviour) and a config change to the
//! bounds shows up on the next reconcile.
//!
//! ## Why the two counters
//!
//! `created_total` / `deleted_total` are the thrash detector the issue calls
//! out: when `min_ready == max_ready` and a member flickers, a buggy plan would
//! oscillate create↔delete, visible as *both* counters climbing together —
//! `rate(...created_total) > 0 and rate(...deleted_total) > 0` on the same pool
//! is the alert. `deleted_total` counts *every* delete a reconcile issues —
//! ceiling trims plus the legacy-paused reclaim, version-stale eviction
//! (#397), and Failed reap (#457) — not just trims, so a pod-create-failure
//! storm (reap-then-remint at the same rate) keeps both counters climbing in
//! lock-step rather than reading as a one-sided `created_total` runaway.
//!
//! ## Why the failed gauge
//!
//! `warm_pool.failed` is the signal the pre-#457 leak lacked: a sustained
//! `rate`-positive or simply non-zero `failed` series flags a pod-create
//! failure storm (e.g. a transient admission-webhook outage) churning the
//! pool — previously invisible because `status` reported only `ready`. Paired
//! with `created_total` climbing while `ready` stays flat, it is the alert for
//! a pool burning create budget on members that never reach `Running`.
//!
//! The instruments are cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in unit tests) the global meter is a no-op, so these
//! calls stay on the reconcile path unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-controller.warm_pool";

/// One reconcile pass's observed counts and planned actions for a pool.
///
/// A plain data struct (not five positional args) so the call site in
/// [`crate::warm_pool::reconcile_pool`] reads as a labelled record and the
/// argument order can't be transposed — the kind of footgun a five-`usize`
/// signature invites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolObservation<'a> {
    /// The pool's name (the `pool` label on every series).
    pub pool: &'a str,
    /// `Running` members observed this pass (`warm_pool.ready`).
    pub ready: u32,
    /// Members observed in a terminal `Failed`/`Terminating` phase this pass
    /// (`warm_pool.failed`), issue #457. This is the *observed* count recorded
    /// before the reap deletes are issued — equal to the number reaped while
    /// the reap is unconditional, but the gauge measures what was seen, not
    /// what was confirmed deleted.
    pub failed: u32,
    /// Configured floor, `spec.minReady` (`warm_pool.min_ready`).
    pub min_ready: u32,
    /// Configured ceiling, `spec.maxReady` (`warm_pool.max_ready`).
    pub max_ready: u32,
    /// Warm sandboxes created this pass (`warm_pool.created_total` delta).
    pub created: u32,
    /// Members deleted this pass — ceiling trims plus the legacy-paused
    /// reclaim, version-stale eviction, and Failed reap
    /// (`warm_pool.deleted_total` delta).
    pub deleted: u32,
}

/// Cached warm-pool instruments, built once via [`metrics`].
struct WarmPoolMetrics {
    /// `warm_pool.ready` — current `Running`-member count.
    ready: Gauge<u64>,
    /// `warm_pool.failed` — terminal members observed (then reaped) this pass
    /// (issue #457).
    failed: Gauge<u64>,
    /// `warm_pool.min_ready` — configured floor.
    min_ready: Gauge<u64>,
    /// `warm_pool.max_ready` — configured ceiling.
    max_ready: Gauge<u64>,
    /// `warm_pool.created_total` — warm-sandbox creations.
    created: Counter<u64>,
    /// `warm_pool.deleted_total` — warm-sandbox deletes (trims + reclaim +
    /// evict + reap).
    deleted: Counter<u64>,
}

impl WarmPoolMetrics {
    /// Build all six instruments off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            ready: meter
                .u64_gauge("warm_pool.ready")
                .with_description("Warm-pool members ready (Running) to claim.")
                .build(),
            failed: meter
                .u64_gauge("warm_pool.failed")
                .with_description(
                    "Warm-pool members observed in a terminal (Failed/Terminating) phase this reconcile (then reaped).",
                )
                .build(),
            min_ready: meter
                .u64_gauge("warm_pool.min_ready")
                .with_description("Configured warm-pool ready floor (spec.minReady).")
                .build(),
            max_ready: meter
                .u64_gauge("warm_pool.max_ready")
                .with_description("Configured warm-pool ready ceiling (spec.maxReady).")
                .build(),
            created: meter
                .u64_counter("warm_pool.created_total")
                .with_description("Warm sandboxes created to top up a pool.")
                .build(),
            deleted: meter
                .u64_counter("warm_pool.deleted_total")
                .with_description(
                    "Warm sandboxes deleted from a pool (ceiling trims plus reclaim/evict/reap).",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::warm_pool_metrics`]).
fn metrics() -> &'static WarmPoolMetrics {
    static METRICS: OnceLock<WarmPoolMetrics> = OnceLock::new();
    METRICS.get_or_init(WarmPoolMetrics::new)
}

/// Record one warm-pool reconcile pass: set the three gauges to the observed
/// state and bump the two action counters by this pass's deltas.
///
/// Counters are only `add`ed when their delta is non-zero. This is **not** a
/// wire-level effect: `add(0, ..)` is already a no-op for the time series
/// (cumulative temporality re-exports the prior value, delta temporality omits
/// the zero), so a quiet steady-state reconcile (a full pool, no create/trim)
/// would export the same series either way. The guard only skips the `add()`
/// call's aggregator lookup on those passes — a micro-saving, since the shared
/// `attrs` array is already built below for the always-recorded gauges, so it
/// does not even save an attribute allocation. `u64::from` widens the `u32`
/// counts to the counter's value type without a fallible cast.
pub fn record_reconcile(obs: PoolObservation<'_>) {
    let metrics = metrics();
    let attrs = [KeyValue::new("pool", obs.pool.to_owned())];

    metrics.ready.record(u64::from(obs.ready), &attrs);
    metrics.failed.record(u64::from(obs.failed), &attrs);
    metrics.min_ready.record(u64::from(obs.min_ready), &attrs);
    metrics.max_ready.record(u64::from(obs.max_ready), &attrs);

    if obs.created > 0 {
        metrics.created.add(u64::from(obs.created), &attrs);
    }
    if obs.deleted > 0 {
        metrics.deleted.add(u64::from(obs.deleted), &attrs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; `record_reconcile` must still be safe to invoke — both an
        // action-bearing pass and a quiet steady-state one (all deltas zero, so
        // the counter `add` branches are skipped).
        record_reconcile(PoolObservation {
            pool: "py-pool",
            ready: 1,
            failed: 3,
            min_ready: 2,
            max_ready: 4,
            created: 1,
            deleted: 0,
        });
        record_reconcile(PoolObservation {
            pool: "py-pool",
            ready: 2,
            failed: 0,
            min_ready: 2,
            max_ready: 4,
            created: 0,
            deleted: 0,
        });
    }
}
