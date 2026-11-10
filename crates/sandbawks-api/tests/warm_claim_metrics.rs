//! Real-recorder emission test for the warm-claim metrics (#288).
//!
//! The unit tests in `warm_claim_metrics.rs` only prove the recorders are safe
//! against the no-op global meter. This test installs a *live* SDK meter
//! provider backed by an [`InMemoryMetricExporter`], drives the real
//! [`claim_for`] orchestration (hit, miss-no-pool, and lost-race → drained
//! paths), then scrapes the exporter to prove the instruments actually emit on a
//! real recorder under the meter scope the dashboards query.
//!
//! It lives in its own integration-test binary on purpose: the production
//! `metrics()` singleton caches its instruments off whichever global meter is
//! installed on *first* use. A sibling test that drove the no-op meter first
//! would bind that cache to the no-op provider and this assertion would see
//! nothing. A dedicated binary gives a fresh process where this test installs
//! the real provider before the first claim records a point.

use std::collections::BTreeMap;

use kube::ResourceExt;
use opentelemetry::global;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use sandbawks_api::warm_claim::{claim_for, FakeWarmClaim};
use sandbawks_types::{ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus};

/// The instrumentation scope the warm-claim instruments are emitted under.
const SCOPE: &str = "sandbawks-api.warm_claim";

/// A minimal `Running` warm-pool candidate / requester for template `py`.
fn sandbox(name: &str) -> Sandbox {
    let mut s = Sandbox::new(
        name,
        SandboxSpec {
            template_ref: "py".into(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse().expect("valid cpu"),
                memory: "1Gi".parse().expect("valid memory"),
                ephemeral_storage: None,
            },
            timeout_seconds: 0,
            auto_pause: true,
            desired_phase: None,
            env: BTreeMap::new(),
            metadata: BTreeMap::new(),
            network_policy: None,
            snapshot_retention: None,
            pvc_retention: None,
            tolerations: None,
            node_selector: None,
            security_context: None,
            persistence_mode: None,
            volume_claim_templates: None,
            restore_from: None,
        },
    );
    s.status = Some(SandboxStatus {
        phase: SandboxPhase::Running,
        ..SandboxStatus::default()
    });
    s
}

/// `warm.claims_total` counter values keyed by their `outcome` attribute.
///
/// Asserting on attribute *values* (not just the instrument name) is what
/// catches a refactor that, say, records `race_lost` as a terminal outcome and
/// silently breaks the hit-rate denominator — the exact regression the closed
/// `outcome` vocabulary exists to prevent.
fn claims_by_outcome(exporter: &InMemoryMetricExporter) -> BTreeMap<String, u64> {
    let mut by_outcome = BTreeMap::new();
    for rm in exporter
        .get_finished_metrics()
        .expect("in-memory exporter yields finished metrics")
    {
        for scope in rm.scope_metrics().filter(|s| s.scope().name() == SCOPE) {
            for metric in scope.metrics().filter(|m| m.name() == "warm.claims_total") {
                // The counter is a `u64` Sum; pull each point's `outcome` label.
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                    for point in sum.data_points() {
                        let outcome = point
                            .attributes()
                            .find(|kv| kv.key.as_str() == "outcome")
                            .map(|kv| kv.value.as_str().into_owned())
                            .unwrap_or_default();
                        *by_outcome.entry(outcome).or_default() += point.value();
                    }
                }
            }
        }
    }
    by_outcome
}

/// Total observation count across all `warm.claim.duration` histogram points.
fn handoff_count(exporter: &InMemoryMetricExporter) -> u64 {
    let mut count = 0;
    for rm in exporter
        .get_finished_metrics()
        .expect("in-memory exporter yields finished metrics")
    {
        for scope in rm.scope_metrics().filter(|s| s.scope().name() == SCOPE) {
            for metric in scope
                .metrics()
                .filter(|m| m.name() == "warm.claim.duration")
            {
                if let AggregatedMetrics::F64(MetricData::Histogram(hist)) = metric.data() {
                    count += hist.data_points().map(|p| p.count()).sum::<u64>();
                }
            }
        }
    }
    count
}

#[tokio::test(flavor = "current_thread")]
async fn claim_for_emits_warm_claim_instruments_on_a_real_recorder() {
    // Install a live meter provider BEFORE the first claim, so the cached
    // instruments bind to this provider rather than the no-op global meter.
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());

    // 1. Hit: a covering pool with one running candidate → claim/reassign.
    //    Emits `warm.claims_total{outcome="hit"}` and the handoff histogram.
    let hit = FakeWarmClaim::with_pool("py", "py-pool", vec![sandbox("sbx-warm-1")]);
    let claimed = claim_for(&hit, "ns", &sandbox("req"), "now", None)
        .await
        .expect("claim ok")
        .expect("a warm member is claimed");
    assert_eq!(claimed.name_any(), "sbx-warm-1");

    // 2. Miss (no pool): emits `warm.claims_total{outcome="miss_no_pool"}`.
    let none = FakeWarmClaim::empty();
    assert!(claim_for(&none, "ns", &sandbox("req"), "now", None)
        .await
        .expect("claim ok")
        .is_none());

    // 3. Lost race then drained: the single candidate loses its CAS, so the
    //    loop records `race_lost` and then `miss_drained` before falling
    //    through to cold-boot.
    let drained = FakeWarmClaim::with_pool("py", "py-pool", vec![sandbox("sbx-warm-2")])
        .losing_race_on("sbx-warm-2");
    assert!(claim_for(&drained, "ns", &sandbox("req"), "now", None)
        .await
        .expect("claim ok")
        .is_none());

    // Push the recorded points into the exporter and assert on the emitted
    // attribute *values*, not just the instrument names: the three calls above
    // produced exactly one hit, one miss_no_pool, one race_lost (the lost CAS),
    // and one miss_drained. No backing-store call errored, so `error` is absent.
    provider
        .force_flush()
        .expect("flush to the in-memory exporter");
    let claims = claims_by_outcome(&exporter);
    assert_eq!(claims.get("hit"), Some(&1), "one hit; saw {claims:?}");
    assert_eq!(
        claims.get("miss_no_pool"),
        Some(&1),
        "one miss_no_pool; saw {claims:?}"
    );
    assert_eq!(
        claims.get("race_lost"),
        Some(&1),
        "one race_lost (the single lost CAS); saw {claims:?}"
    );
    assert_eq!(
        claims.get("miss_drained"),
        Some(&1),
        "one miss_drained; saw {claims:?}"
    );
    assert_eq!(
        claims.get("error"),
        None,
        "no backing-store call errored; saw {claims:?}"
    );

    // The handoff histogram is recorded only on a hit, so its observation count
    // must equal the `outcome="hit"` counter — the parity that guards against a
    // refactor double-recording or dropping the handoff timing.
    assert_eq!(
        handoff_count(&exporter),
        claims["hit"],
        "warm.claim.duration count must equal warm_claims_total{{outcome=\"hit\"}}"
    );
}
