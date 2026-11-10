//! Real-recorder emission test for the warm-pool metrics (#288).
//!
//! The unit tests in `warm_pool_metrics.rs` only prove `record_reconcile` is
//! safe against the no-op global meter. This test installs a *live* SDK meter
//! provider backed by an [`InMemoryMetricExporter`], drives the real
//! [`reconcile_pool`] loop through both a top-up pass and a trim pass, and
//! scrapes the exporter to prove all five instruments emit on a real recorder
//! under the meter scope the dashboards query.
//!
//! It lives in its own integration-test binary on purpose: the production
//! `metrics()` singleton caches its instruments off whichever global meter is
//! installed on *first* use. A sibling test that drove the no-op meter first
//! would bind that cache to the no-op provider and this assertion would see
//! nothing. A dedicated binary gives a fresh process where this test installs
//! the real provider before the first reconcile records a point.

use std::collections::BTreeMap;

use opentelemetry::global;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use sandbawks_controller::warm_pool::{reconcile_pool, FakeWarmPoolStore};
use sandbawks_types::warm_pool::WARM_POOL_LABEL;
use sandbawks_types::{
    ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus, SandboxWarmPool,
    SandboxWarmPoolSpec,
};

/// The instrumentation scope the warm-pool instruments are emitted under.
const SCOPE: &str = "sandbawks-controller.warm_pool";

/// A warm-pool member in `phase`, carrying the pool label.
fn member(name: &str, pool: &str, phase: SandboxPhase) -> Sandbox {
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
    let mut labels = BTreeMap::new();
    labels.insert(WARM_POOL_LABEL.to_owned(), pool.to_owned());
    s.metadata.labels = Some(labels);
    s.status = Some(SandboxStatus {
        phase,
        ..SandboxStatus::default()
    });
    s
}

/// A namespaced, UID-stamped pool — both are required by `reconcile_pool`.
fn pool(name: &str, min: u32, max: u32) -> SandboxWarmPool {
    let mut p = SandboxWarmPool::new(
        name,
        SandboxWarmPoolSpec {
            template_ref: "py".into(),
            min_ready: min,
            max_ready: max,
        },
    );
    p.metadata.namespace = Some("team-acme".into());
    p.metadata.uid = Some("pool-uid-0288".into());
    p
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

#[tokio::test(flavor = "current_thread")]
async fn reconcile_pool_emits_warm_pool_instruments_on_a_real_recorder() {
    // Install a live meter provider BEFORE the first reconcile, so the cached
    // instruments bind to this provider rather than the no-op global meter.
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());

    // Pass 1 — top-up. min=3, observed = 1 Running + 1 Creating (in-flight):
    // total 2 < 3 → create 1. Emits the three gauges and `created_total`.
    let topup = FakeWarmPoolStore::new();
    topup.insert_member(member("sbx-p-0", "topup", SandboxPhase::Running));
    topup.insert_member(member("sbx-c-0", "topup", SandboxPhase::Creating));
    reconcile_pool(&pool("topup", 3, 4), &topup)
        .await
        .expect("top-up reconcile ok");

    // Pass 2 — trim. max=1, observed = 3 Running → trim 2. Emits `deleted_total`
    // (and re-records the gauges) so all six series are present after flush.
    let trim = FakeWarmPoolStore::new();
    for i in 0..3 {
        trim.insert_member(member(&format!("sbx-t-{i}"), "trim", SandboxPhase::Running));
    }
    reconcile_pool(&pool("trim", 0, 1), &trim)
        .await
        .expect("trim reconcile ok");

    // Push the recorded points into the exporter and assert every instrument
    // emitted under the expected scope on a live recorder.
    provider
        .force_flush()
        .expect("flush to the in-memory exporter");
    let names = emitted_names(&exporter);
    for expected in [
        "warm_pool.ready",
        "warm_pool.failed",
        "warm_pool.min_ready",
        "warm_pool.max_ready",
        "warm_pool.created_total",
        "warm_pool.deleted_total",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "{expected} must emit on a real recorder; saw {names:?}"
        );
    }
}
