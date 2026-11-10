//! Per-team API usage metering for anti-abuse (epic #480 Phase 1, issue #630).
//!
//! One instrument, under the meter `sandbawks-api.usage`, recorded once per
//! authenticated request from [`crate::auth::auth_middleware`] — the single
//! chokepoint every authenticated request passes through with a freshly
//! resolved [`crate::auth::TeamContext`]:
//!
//! | Instrument | Type | Unit | Attributes |
//! |---|---|---|---|
//! | `usage.api.requests.count` | `Counter<u64>` | --- | team, key_name |
//!
//! ## Why a dedicated meter, not `http.server.*`
//!
//! The operational HTTP-server instruments in [`crate::http_metrics`] are
//! deliberately cardinality-bounded by `method` / `route` / `status` and carry
//! **no** team identity, so they stay a comprehensible ops dashboard. Billing
//! and anti-abuse need the opposite — a per-team breakdown. Mixing `team` into
//! `http.server.*` would multiply every operational series by the client count.
//! Keeping this on a separate `sandbawks-api.usage` meter isolates that
//! cardinality from the ops surface.
//!
//! ## Signal, not ledger
//!
//! This counter is an **anti-abuse / rate-limit signal only**, emitted via OTel.
//! It is explicitly **not** the billing source of record: per the epic's
//! trust-model decision (#480), OpenTelemetry is not a valid authoritative
//! source for billing. The authoritative, resource-weighted sandbox-seconds come
//! from the controller (Phase 2, #631) and the durable ledger derives only from
//! those (Phase 3, #632) — never from this counter.
//!
//! ## Cardinality budget
//!
//! The active series count is bounded by `team` x `key_name`. `team` is the
//! owning namespace (1 team = 1 namespace, per the `TeamAPIKey` model), so its
//! cardinality is the client count; `key_name` is the number of live keys a team
//! has issued. At SaaS-MVP scale (hundreds of teams, a handful of keys each) this
//! is a few thousand series — comfortably within budget. If the client count ever
//! explodes, the pre-roll-up belongs in the Phase 3 aggregation layer, **not**
//! here: dropping `key_name` or `team` from this signal would blind the
//! anti-abuse view that justifies the dimension.
//!
//! The instrument is cached in a [`OnceLock`]; with `OTEL_SDK_DISABLED` (or no
//! installed provider, as in unit tests) the global meter is a no-op, so the
//! call stays on the auth path unconditionally at zero cost.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

/// Instrumentation-scope name: the *library* surface emitting these points, per
/// OTel guidance (not the service name — that is a resource attribute). Distinct
/// from `sandbawks-api.http.server` so team cardinality never lands on the ops
/// meter (see the module docs).
const METER_NAME: &str = "sandbawks-api.usage";

/// Cached usage instruments, built once via [`metrics`].
struct UsageMetrics {
    /// `usage.api.requests.count` — authenticated requests, by team and key.
    requests: Counter<u64>,
}

impl UsageMetrics {
    /// Build the instrument off the global meter. Called once.
    fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            requests: meter
                .u64_counter("usage.api.requests.count")
                .with_description(
                    "Authenticated API requests per team (anti-abuse signal, not billing).",
                )
                .build(),
        }
    }
}

/// The process-wide instrument singleton.
///
/// `get_or_init` runs [`UsageMetrics::new`] exactly once on first authenticated
/// request; every later call is a cheap pointer read, keeping the per-request
/// cost to recording a point rather than rebuilding the instrument.
fn metrics() -> &'static UsageMetrics {
    static METRICS: OnceLock<UsageMetrics> = OnceLock::new();
    METRICS.get_or_init(UsageMetrics::new)
}

/// Record one authenticated API request, attributed to its owning `team` (the
/// tenant namespace) with a per-`key_name` breakdown.
///
/// Called once per request from [`crate::auth::auth_middleware`] after the key
/// resolves, so it counts exactly the requests that authenticated — regardless
/// of the handler's eventual status, which is the right denominator for an
/// abuse signal. `team` is the billing/grouping key; `key_name` is a breakdown
/// dimension only (a team rotates/issues multiple keys, so it must never be the
/// billing key — epic #480).
pub fn record_request(team: &str, key_name: &str) {
    let attrs = [
        KeyValue::new("team", team.to_owned()),
        KeyValue::new("key_name", key_name.to_owned()),
    ];
    metrics().requests.add(1, &attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_against_the_noop_meter_does_not_panic() {
        // No meter provider is installed in unit tests, so the global meter is a
        // no-op; the recorder must still be safe to invoke on the auth path,
        // including with empty attribute values (a misconfigured key resolves to
        // an empty namespace upstream, though auth rejects it before recording).
        record_request("team-a", "key-1");
        record_request("", "");
    }
}
