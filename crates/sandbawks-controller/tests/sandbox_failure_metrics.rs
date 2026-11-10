//! Real-recorder emission test for the sandbox failure metrics (issue #339).
//!
//! The unit tests in `sandbox_failure_metrics.rs` only prove `record_failure`
//! is safe against the no-op global meter. This test installs a *live* SDK meter
//! provider backed by an [`InMemoryMetricExporter`], drives the real
//! [`reconcile`] loop through a `Running` Sandbox whose backing Pod has
//! terminated (the `pod_terminated_running` fold), and scrapes the exporter to
//! prove `controller.sandbox.failure_total` emits on a real recorder under the
//! meter scope the dashboards query.
//!
//! It lives in its own integration-test binary on purpose: the production
//! `metrics()` singleton caches its instruments off whichever global meter is
//! installed on *first* use. A sibling test that drove the no-op meter first
//! would bind that cache to the no-op provider and this assertion would see
//! nothing. A dedicated binary gives a fresh process where this test installs
//! the real provider before the first reconcile records a point.

use std::sync::Arc;

use opentelemetry::global;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use sandbawks_controller::{
    reconcile, FakeRuntimeAdapter, FakeSandboxStore, FakeSandboxTemplateStore,
    FakeSnapshotJobStore, PvcRetentionConfig, ReconcileContext,
};
use sandbawks_types::{ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus};

/// The instrumentation scope the sandbox-failure instruments are emitted under.
const SCOPE: &str = "sandbawks-controller.sandbox";

/// A `Running` Sandbox with a `pod_ref` so the liveness probe has a target.
fn running_sandbox_with_pod(name: &str) -> Sandbox {
    let mut s = Sandbox::new(
        name,
        SandboxSpec {
            template_ref: "tpl".into(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse().expect("valid cpu"),
                memory: "1Gi".parse().expect("valid memory"),
                ephemeral_storage: None,
            },
            timeout_seconds: 0,
            auto_pause: false,
            desired_phase: None,
            env: Default::default(),
            metadata: Default::default(),
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
        pod_ref: Some("sandbawks/sbx-fail-0".into()),
        ..SandboxStatus::default()
    });
    s
}

/// Collect the metric names emitted under [`SCOPE`] from the exporter.
fn emitted_names(exporter: &InMemoryMetricExporter) -> Vec<String> {
    let mut names = Vec::new();
    for rm in exporter
        .get_finished_metrics()
        .expect("in-memory exporter yields finished metrics")
    {
        for scope in rm.scope_metrics() {
            if scope.scope().name() == SCOPE {
                names.extend(scope.metrics().map(|m| m.name().to_string()));
            }
        }
    }
    names
}

/// Collect the `reason` attribute values on `controller.sandbox.failure_total`
/// data points emitted under [`SCOPE`].
fn emitted_failure_reasons(exporter: &InMemoryMetricExporter) -> Vec<String> {
    let mut reasons = Vec::new();
    for rm in exporter
        .get_finished_metrics()
        .expect("in-memory exporter yields finished metrics")
    {
        for scope in rm.scope_metrics() {
            if scope.scope().name() != SCOPE {
                continue;
            }
            for metric in scope.metrics() {
                if metric.name() != "controller.sandbox.failure_total" {
                    continue;
                }
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                    for dp in sum.data_points() {
                        for kv in dp.attributes() {
                            if kv.key.as_str() == "reason" {
                                reasons.push(kv.value.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    reasons
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_emits_failure_total_on_a_real_recorder() {
    // Install a live meter provider BEFORE the first reconcile, so the cached
    // instruments bind to this provider rather than the no-op global meter.
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());

    // Set up a Running Sandbox with a pod_ref.
    let store = FakeSandboxStore::new();
    store.insert(running_sandbox_with_pod("sbx-fail-0"));

    // Adapter: `pod_terminated` returns true so `running_liveness_trigger`
    // fires the `pod_terminated_running` fold into Failed.
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pod_terminated(true));

    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter,
        snapshot_jobs: Arc::new(FakeSnapshotJobStore::new()),
        templates: Arc::new(FakeSandboxTemplateStore::new()),
        client: None,
        pvc_retention: PvcRetentionConfig::default(),
        usage_sink: Arc::new(sandbawks_controller::usage::NoopUsageSink),
        usage_spill: Arc::new(sandbawks_controller::usage_spill::NoopUsageSpillQueue),
    });

    reconcile("default", "sbx-fail-0", ctx)
        .await
        .expect("reconcile ok");

    // Confirm the fold actually happened (Sandbox is now Failed) — if this
    // assertion fails the test setup is wrong, not the metric emission.
    let phase = store
        .peek("sbx-fail-0")
        .expect("sandbox still exists")
        .status
        .expect("status populated")
        .phase;
    assert_eq!(
        phase,
        SandboxPhase::Failed,
        "sandbox must have folded to Failed for the metric to emit"
    );

    // Push the recorded points into the exporter and assert the counter
    // emitted under the expected scope on a live recorder.
    provider
        .force_flush()
        .expect("flush to the in-memory exporter");
    let names = emitted_names(&exporter);
    assert!(
        names
            .iter()
            .any(|n| n == "controller.sandbox.failure_total"),
        "controller.sandbox.failure_total must emit on a real recorder; saw {names:?}"
    );

    // Also assert the `reason` attribute is correctly attached to the data point.
    // This proves attribute plumbing end-to-end, not just counter registration.
    let reasons = emitted_failure_reasons(&exporter);
    assert!(
        reasons.iter().any(|r| r == "pod_terminated_running"),
        "reason attribute must contain pod_terminated_running; saw {reasons:?}"
    );
}
