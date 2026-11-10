//! Reconcile loop, state machine, and runtime-adapter wiring for the
//! sandbawks Kubernetes sandbox controller.
//!
//! The crate is intentionally layered so the state machine module is
//! testable without any kube or tokio dependency:
//!
//! - [`state_machine`] — pure transitions + actions (no I/O).
//! - [`store`] — trait abstraction over `kube::Api<Sandbox>` so the
//!   reconcile loop is unit-testable against an in-memory fake.
//! - [`fake_adapter`] — a no-op `RuntimeAdapter` impl that records
//!   calls. Used until Plan 05 lands `GvisorAdapter`.
//! - [`mod@reconcile`] — glue that reads spec/status, derives a
//!   [`state_machine::Trigger`], runs the transition, patches status
//!   *before* any side effect, and dispatches the resulting action.

/// Crate name string — smoke test that downstream tests link.
pub const CRATE_NAME: &str = "sandbawks-controller";

pub mod azure_retention;
pub mod dispatch_metrics;
pub mod fake_adapter;
pub mod leader_election;
pub mod pod_adapter;
pub mod pod_op_metrics;
pub mod pvc_retention_metrics;
pub mod reconcile;
pub mod reconcile_metrics;
pub mod retention;
pub mod s3_retention;
pub mod s3_usage;
pub mod sandbox_failure_metrics;
pub mod sandbox_recovery_metrics;
pub mod state_machine;
pub mod store;
pub mod tenant_rbac;
pub mod usage;
pub mod usage_ledger;
pub mod usage_metrics;
pub mod usage_spill;
pub mod warm_pool;
pub mod warm_pool_metrics;

pub use azure_retention::AzureRetentionStore;
pub use fake_adapter::FakeRuntimeAdapter;
pub use pod_adapter::{PodAdapter, SANDBOX_POD_LABEL_KEY, SANDBOX_POD_LABEL_VALUE};
pub use reconcile::{reconcile, ControllerError, PvcRetentionConfig, ReconcileContext};
pub use retention::{
    effective_retention, run_retention_pass, FakeRetentionStore, RetentionConfig, RetentionError,
    RetentionObject, RetentionPassReport, SnapshotRetentionStore, NAMESPACE_RETENTION_ANNOTATION,
};
pub use s3_retention::S3RetentionStore;
pub use state_machine::{transition, Action, FailureReason, Trigger};
pub use store::{
    FakeSandboxStore, FakeSandboxTemplateStore, FakeSnapshotJobStore, KubeOpenIntervalSource,
    KubeSandboxStore, KubeSandboxTemplateStore, KubeSnapshotJobStore, SandboxStore,
    SandboxTemplateStore, SnapshotJobStore, StoreError,
};
pub use warm_pool::{
    plan_pool, reconcile_pool, status_for, warm_sandbox, FakeWarmPoolStore, KubeWarmPoolStore,
    PoolMembers, PoolPlan, WarmPoolStore, WarmSandboxDefaults,
};
