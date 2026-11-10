//! Behavioral tests for `FakeRuntimeAdapter`.
//!
//! After Plan 05 T4 the controller no longer calls `pause()`/`resume()`
//! through the adapter — those are mediated by `SnapshotJob` CRs. The
//! fake therefore only needs to expose `runtime_class` and inherit the
//! default-impl Pod lifecycle methods.

use std::sync::Arc;

use sandbawks_controller::FakeRuntimeAdapter;
use sandbawks_types::RuntimeAdapter;

#[test]
fn runtime_class_matches_constructor() {
    let a = FakeRuntimeAdapter::new("kata-fc");
    assert_eq!(a.runtime_class(), "kata-fc");
}

/// Compile-time check that `Arc<FakeRuntimeAdapter>` coerces to
/// `Arc<dyn RuntimeAdapter>` — the call site shape the controller's
/// reconcile loop uses to hold a polymorphic adapter.
#[test]
fn arc_dyn_runtime_adapter_object_safety() {
    let adapter: Arc<dyn RuntimeAdapter> = Arc::new(FakeRuntimeAdapter::new("fake"));
    assert_eq!(adapter.runtime_class(), "fake");
}
