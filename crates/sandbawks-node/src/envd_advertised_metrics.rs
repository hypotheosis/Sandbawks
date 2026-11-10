//! OTel metrics for the node's `envd-staged` label reconciler (#286).
//!
//! One instrument, under the meter `sandbawks-node.envd_advertised`, recorded
//! once per [`crate::node_label::run_envd_staged_reconciler`] tick:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `envd_advertised.total` | `Counter<u64>` | --- | result |
//!
//! This promotes the per-tick `result=` structured `tracing` event #285 added as
//! a placeholder into an alertable metric series, now that the workspace has an
//! OTel metrics pipeline (OTel Phase 6). The `result` label keeps the exact
//! vocabulary the tracing event used, so dashboards/alerts written against the
//! log field carry straight over:
//!
//! | `result` | Tick outcome | Meaning |
//! |---|---|---|
//! | `ok` | [`AdvertiseOutcome::Advertised`] | staged binary present + configured runtime handler registered; label re-asserted |
//! | `missing` | [`AdvertiseOutcome::Withdrawn`] | staged binary gone; label withdrawn |
//! | `runsc_root_failed` | [`AdvertiseOutcome::WithdrawnRunscRoot`] | runsc-root unresolved for a real container; label withdrawn (sticky, #379) |
//! | `runsc_absent` | [`AdvertiseOutcome::WithdrawnRunscAbsent`] | configured runtime handler missing from node `status.runtimeHandlers`; label withdrawn (#417). The label keeps the `runsc_` prefix for dashboard/alert continuity even though the gate is handler-agnostic (#598); a rename is a deliberate follow-up. |
//! | `patch_failed` | reconcile `Err(..)` | Node get/patch failed (RBAC/apiserver/conflict) |
//!
//! Naming: the issue proposed `sandbawks_node_envd_advertised_total`, the
//! pre-OTel metric name. This crate's OTel instruments drop the `sandbawks_node_`
//! prefix (it is the instrumentation scope, not part of the instrument name —
//! cf. [`crate::snapshotjob_metrics`] `snapshotjob.executions_total`), so the
//! collector exports this as `envd_advertised_total{result=...}` after the OTLP
//! dotted-name → Prometheus underscore translation.
//!
//! [`AdvertiseOutcome::Advertised`]: crate::node_label::AdvertiseOutcome::Advertised
//! [`AdvertiseOutcome::Withdrawn`]: crate::node_label::AdvertiseOutcome::Withdrawn
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in tests) the global meter is a no-op, so this call
//! stays on the reconcile path unconditionally.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name (the emitting library surface, per OTel guidance).
const METER_NAME: &str = "sandbawks-node.envd_advertised";

/// Outcome of one `envd-staged` reconcile tick, carrying the exact `result`
/// label it maps to.
///
/// Modelling the four outcomes as an enum (rather than passing a `&str` at each
/// call site) keeps the label vocabulary closed and the mapping exhaustively
/// checked by the compiler — a Rust idiom a Python developer might otherwise
/// express as loose string constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileResult {
    /// Staged binary present; the label was (re-)asserted.
    Ok,
    /// Staged binary gone; the label was withdrawn.
    Missing,
    /// The label patch itself failed; retried on the next tick.
    PatchFailed,
    /// A `RunscRootUnresolved` failure was observed for a real container;
    /// the label was withdrawn to stop the warm pool from scheduling further
    /// pause jobs on this node (#379).
    RunscRootFailed,
    /// The configured runtime handler (`required_handler`, e.g. `runsc` on a
    /// gVisor pool or `kata` on a Kata pool) is absent from this node's
    /// `status.runtimeHandlers`; the label was withdrawn so sandbox Pods stop
    /// scheduling onto a node that would loop on `FailedCreatePodSandBox: no
    /// runtime for "<handler>" is configured` (#417). The `runsc_absent` label
    /// string is retained for dashboard/alert continuity even though the gate
    /// is handler-agnostic (#598); renaming it is a deliberate follow-up.
    RunscAbsent,
}

impl ReconcileResult {
    /// Stable, lowercase `result` label: `ok` | `missing` | `patch_failed` |
    /// `runsc_root_failed` | `runsc_absent`.
    fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Missing => "missing",
            Self::PatchFailed => "patch_failed",
            Self::RunscRootFailed => "runsc_root_failed",
            Self::RunscAbsent => "runsc_absent",
        }
    }
}

/// Cached envd-advertised instruments, built once via [`metrics`].
struct EnvdAdvertisedMetrics {
    /// `envd_advertised.total` — reconcile-tick outcomes by result.
    advertised: Counter<u64>,
}

impl EnvdAdvertisedMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            advertised: meter
                .u64_counter("envd_advertised.total")
                .with_description(
                    "envd-staged label reconcile-tick outcomes, by result \
                     (ok | missing | runsc_root_failed | runsc_absent | patch_failed).",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton (see [`crate::envd_advertised_metrics`]).
fn metrics() -> &'static EnvdAdvertisedMetrics {
    static METRICS: OnceLock<EnvdAdvertisedMetrics> = OnceLock::new();
    METRICS.get_or_init(EnvdAdvertisedMetrics::new)
}

/// Record one `envd-staged` reconcile tick, labelled by its `result`.
///
/// Each [`crate::node_label::run_envd_staged_reconciler`] tick increments the
/// counter exactly once, mirroring the `result=` field on the per-tick tracing
/// event so the two never diverge.
///
/// Named for the specific tick it records rather than a generic
/// `record_reconcile`: `sandbawks-controller`'s `reconcile_metrics` already
/// exports a `record_reconcile(kind, result, duration)` with a different
/// signature, and a self-documenting name keeps the two unambiguous if a shared
/// counter helper is ever extracted (changelog follow-up).
pub fn record_envd_staged_tick(result: ReconcileResult) {
    metrics()
        .advertised
        .add(1, &[KeyValue::new("result", result.label())]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_covers_every_result() {
        // Exhaustive over every outcome; the compiler guarantees the `match`
        // arm in `label` stays in sync with the enum.
        assert_eq!(ReconcileResult::Ok.label(), "ok");
        assert_eq!(ReconcileResult::Missing.label(), "missing");
        assert_eq!(ReconcileResult::PatchFailed.label(), "patch_failed");
        assert_eq!(
            ReconcileResult::RunscRootFailed.label(),
            "runsc_root_failed"
        );
        assert_eq!(ReconcileResult::RunscAbsent.label(), "runsc_absent");
    }

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in tests, so the global meter is a
        // no-op; `record_envd_staged_tick` must still be safe to invoke on every
        // tick outcome.
        record_envd_staged_tick(ReconcileResult::Ok);
        record_envd_staged_tick(ReconcileResult::Missing);
        record_envd_staged_tick(ReconcileResult::PatchFailed);
        record_envd_staged_tick(ReconcileResult::RunscRootFailed);
        record_envd_staged_tick(ReconcileResult::RunscAbsent);
    }
}
