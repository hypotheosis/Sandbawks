//! Real-recorder emission test for the per-team usage counter (#480 Phase 1,
//! issue #630).
//!
//! The unit test in `usage_metrics.rs` only proves the recorder is safe against
//! the no-op global meter. This test installs a *live* SDK meter provider backed
//! by an [`InMemoryMetricExporter`], drives real authenticated requests through
//! [`auth_middleware`] (the production recording site), then scrapes the exporter
//! to prove `usage.api.requests.count` actually emits under the meter scope the
//! dashboards query, with the `{ team, key_name }` attribute values the
//! anti-abuse breakdown relies on.
//!
//! It lives in its own integration-test binary on purpose: the production
//! `metrics()` singleton caches its instrument off whichever global meter is
//! installed on *first* use, so this binary installs the real provider before the
//! first authenticated request records a point.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Extension;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use opentelemetry::global;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use sandbawks_api::auth::{auth_middleware, FakeKeyStore, TeamContext};
use tower::ServiceExt; // brings `oneshot` into scope

/// The instrumentation scope the usage instrument is emitted under.
const SCOPE: &str = "sandbawks-api.usage";

/// A trivial authenticated handler — the request only needs to reach a handler
/// for `auth_middleware` to have recorded against the resolved team first.
async fn ok(Extension(_team): Extension<TeamContext>) -> &'static str {
    "ok"
}

/// Build the authenticated router under test. Cloned per request because
/// `oneshot` consumes the service.
fn app(store: Arc<FakeKeyStore>) -> Router {
    Router::new()
        .route("/whoami", get(ok))
        .layer(axum::middleware::from_fn_with_state(store, auth_middleware))
}

/// `usage.api.requests.count` values keyed by their `(team, key_name)` attribute
/// pair. Asserting on the attribute *values* (not just the instrument name) is
/// what proves the per-team / per-key breakdown the anti-abuse signal exists for.
fn requests_by_team_key(exporter: &InMemoryMetricExporter) -> BTreeMap<(String, String), u64> {
    let mut by_key = BTreeMap::new();
    for rm in exporter
        .get_finished_metrics()
        .expect("in-memory exporter yields finished metrics")
    {
        for scope in rm.scope_metrics().filter(|s| s.scope().name() == SCOPE) {
            for metric in scope
                .metrics()
                .filter(|m| m.name() == "usage.api.requests.count")
            {
                // The instrument is a `u64` Sum; pull each point's team + key.
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                    for point in sum.data_points() {
                        let attr = |name: &str| {
                            point
                                .attributes()
                                .find(|kv| kv.key.as_str() == name)
                                .map(|kv| kv.value.as_str().into_owned())
                                .unwrap_or_default()
                        };
                        let team = attr("team");
                        let key_name = attr("key_name");
                        *by_key.entry((team, key_name)).or_default() += point.value();
                    }
                }
            }
        }
    }
    by_key
}

#[tokio::test]
async fn auth_middleware_emits_usage_counter_per_team_and_key() {
    // Install a live meter provider BEFORE the first authenticated request, so
    // the cached instrument binds to this provider rather than the no-op global
    // meter.
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());

    let store = Arc::new(FakeKeyStore::default());
    // `FakeKeyStore::insert` derives `key_name = "fake-<first 8 chars of key>"`,
    // so this key resolves to team `team-acme`, key_name `fake-sbk_a1b2`.
    store.insert("sbk_a1b2c3d4xxxxxxxx", "team-acme", &["sandboxes:*"]);

    // Two authenticated requests with the same key → the team+key series counts 2.
    for _ in 0..2 {
        let resp = app(Arc::clone(&store))
            .oneshot(
                Request::builder()
                    .uri("/whoami")
                    .header("X-API-Key", "sbk_a1b2c3d4xxxxxxxx")
                    .body(Body::empty())
                    .expect("building a test request"),
            )
            .await
            .expect("router responds");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // An unauthenticated request (missing key → 401) never reaches the recording
    // site, so it must NOT add to any series — the counter is the count of
    // *authenticated* requests, the correct anti-abuse denominator.
    let resp = app(Arc::clone(&store))
        .oneshot(
            Request::builder()
                .uri("/whoami")
                .body(Body::empty())
                .expect("building a test request"),
        )
        .await
        .expect("router responds");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    provider
        .force_flush()
        .expect("flush to the in-memory exporter");

    let by_key = requests_by_team_key(&exporter);
    assert_eq!(
        by_key.get(&("team-acme".to_owned(), "fake-sbk_a1b2".to_owned())),
        Some(&2),
        "two authenticated requests for one team+key; saw {by_key:?}"
    );
    // Exactly one series: the unauthenticated request added nothing.
    assert_eq!(
        by_key.len(),
        1,
        "only the authenticated team+key series should exist; saw {by_key:?}"
    );
}
