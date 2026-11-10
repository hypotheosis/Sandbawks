//! Shared types for the sandbawks Kubernetes sandbox controller.
//!
//! This crate is intentionally schema-only: it defines the CRDs and DTOs
//! that every other sandbawks crate consumes, but contains no runtime
//! behavior. Keeping it leaf-of-the-dependency-graph means everything
//! else can be tested against it cheaply.

/// Crate name string — used as a smoke test that downstream crates linked
/// against this one successfully.
pub const CRATE_NAME: &str = "sandbawks-types";

pub mod auth;
pub mod error;
pub mod labels;
pub mod net;
pub mod resources;
pub mod runtime;
pub mod sandbox;
pub mod sandbox_template;
pub mod snapshot;
pub mod snapshot_job;
pub mod team_api_key;
#[cfg(feature = "testing")]
pub mod testing;
pub mod warm_pool;

pub use error::Error;
pub use labels::{
    API_KEY_NAME_ANNOTATION, CLONE_IDENTITY_RESET_ANNOTATION, CLONE_IDENTITY_RESET_VALUE,
    ENVD_STAGED_LABEL, ENVD_STAGED_LABEL_VALUE, RUNSC_VERSION_LABEL, SANDBOX_NAME_LABEL,
};
pub use net::host_port;
pub use resources::{ResourceQuantity, ResourceRequests};
pub use runtime::{
    noop_sink, noop_source, PodReadiness, PodRef, PodTerminationCause, RestorePlacement,
    RuntimeAdapter, RuntimeError, SnapshotManifest, ENVD_RELOCATE_TARGET, ENVD_RELOCATE_TARGET_ENV,
    ENVD_RUN_DIR,
};
pub use sandbox::{
    clone_needs_guest_identity_reset, is_kata_runtime_class, merge_egress_policies,
    merge_volume_claim_templates, resolve_effective_persistence_mode, NetworkPolicy,
    PersistenceMode, PodSecurityContext, PodStatusSummary, RestoreSource, Sandbox,
    SandboxCondition, SandboxPhase, SandboxSpec, SandboxStatus, Toleration,
    UnsupportedPersistenceMode, VctMergeOutcome, VctSuppression, VctSuppressionReason,
    VolumeClaimSpec, VolumeClaimTemplate, VolumeClaimTemplateMetadata, VolumeClaimTemplatesPolicy,
};
pub use sandbox_template::{SandboxTemplate, SandboxTemplateSpec};
pub use snapshot::{
    aggregate_sha256, parse_iso8601_duration, DurationParseError, NoopSnapshotStore, SnapshotPart,
    SnapshotProvider, SnapshotProviderParseError, SnapshotSink, SnapshotSource,
};
pub use snapshot_job::{
    SnapshotJob, SnapshotJobMode, SnapshotJobPhase, SnapshotJobSpec, SnapshotJobStatus,
    STAGED_MARKER_FILENAME,
};
pub use team_api_key::{TeamAPIKey, TeamApiKeySpec};
pub use warm_pool::{
    SandboxWarmPool, SandboxWarmPoolSpec, SandboxWarmPoolStatus, CLAIMED_AT_ANNOTATION,
    TEMPLATE_RV_ANNOTATION, WARM_POOL_LABEL,
};
