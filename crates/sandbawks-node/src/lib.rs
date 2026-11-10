//! Per-node DaemonSet driving sandbox pause/resume.
//!
//! `sandbawks-node` runs as a privileged DaemonSet on every cluster
//! node; it reconciles [`sandbawks_types::SnapshotJob`] CRs whose
//! `spec.nodeName` matches the local `$NODE_NAME` (from the downward
//! API), invokes `runsc checkpoint` / `runsc restore` against the
//! local containerd socket, and streams the resulting snapshot blobs
//! to S3-compatible object storage. See `docs/superpowers/plans/2026-05-24-sandbawks-05-gvisor-node.md`.
//!
//! This crate is library-first so integration tests can drive the
//! reconcile function with fakes (T9) instead of a real kube
//! apiserver + containerd socket + S3 endpoint.
//!
//! The module layout mirrors the implementation phasing:
//! - [`gvisor`] — the [`sandbawks_types::RuntimeAdapter`] impl that
//!   shells out to `runsc` and computes rootfs deltas (T7/T8).
//! - [`headroom`] — the fail-closed resource-headroom policy that gates
//!   stage/restore on available node disk + memory (#238).
//! - [`s3`] — chunked, parallel, sha256-verified
//!   [`sandbawks_types::SnapshotSink`] / `Source` impls (T6).
//! - [`stage_gate`] — the cross-runtime restore staging gate: the shared Rust
//!   counterpart to the Go shim's `waitForStagedImage`, so a new
//!   [`sandbawks_types::RuntimeAdapter`] honors the `.staged` marker contract by
//!   calling one tested helper rather than re-deriving it (#511).
//! - [`reconcile`] — the `SnapshotJob` controller that ties them
//!   together (T9).
//! - [`node_label`] — advertises node-staging readiness via a Node label
//!   so the controller's nodeAffinity keeps sandboxes off not-yet-staged
//!   nodes (#272).
//! - [`runsc_reconcile`] — restart-free host runsc version reporting +
//!   legacy-apt-channel cleanup + opt-in binary convergence (#546).
//! - [`runsc_upgrade`] — opt-in, privileged, restart-bearing in-place runsc fleet
//!   upgrade: a node with no live sandboxes that holds the cluster-wide upgrade
//!   lease runs the full host installer (CRI handler rewrite + k3s restart)
//!   detached so it survives the Pod the restart kills (#550, Capability C).

/// Smoke-test that downstream tests link against this crate.
///
/// Mirrors the same constant on `sandbawks-api`, `sandbawks-controller`,
/// and `envd`. Used by `tests/smoke.rs`.
pub const CRATE_NAME: &str = "sandbawks-node";

pub mod azure_blob;
pub mod envd_advertised_metrics;
pub mod envd_digest_metrics;
pub mod envd_lifecycle_metrics;
pub mod gvisor;
pub mod headroom;
pub mod kata;
pub mod memory_pressure;
pub mod memory_pressure_metrics;
pub mod node_label;
pub mod reconcile;
pub mod runsc_reconcile;
pub mod runsc_upgrade;
pub mod runsc_upgrade_metrics;
pub mod runsc_version_metrics;
pub mod s3;
pub mod sandbox_pressure;
pub mod sandbox_pressure_metrics;
pub mod sandbox_stats;
pub mod snapshot_phase_metrics;
pub mod snapshotjob_metrics;
pub mod stage_gate;
pub mod validate_metrics;
