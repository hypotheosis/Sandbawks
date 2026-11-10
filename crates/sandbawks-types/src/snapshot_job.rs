//! `SnapshotJob` CRD: bridges the controller to per-node
//! `sandbawks-node` DaemonSet workers.
//!
//! The controller creates a `SnapshotJob` CR when it wants a specific
//! node to either checkpoint (`Pause`) or restore (`Resume`) a
//! sandbox's container. The per-node reconciler watches `SnapshotJob`s
//! filtered to its own `spec.nodeName`, executes the work via the
//! runtime adapter, and writes results back to `status`. The
//! controller observes terminal `status.phase` (`Complete` or
//! `Failed`) and folds the manifest back into
//! [`crate::SandboxStatus::snapshot_ref`].
//!
//! `SnapshotJob`s are intentionally short-lived: one job per
//! pause/resume cycle, named `<sandbox>-<mode>-<epoch-millis>` by the
//! controller so multiple in-flight jobs don't collide.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::SnapshotManifest;

/// Filename of the atomic completion marker the node DaemonSet drops
/// inside a scheduler-placed restore's [`SnapshotJobSpec::target_path`]
/// once every part is on disk (epic #484, issue #487).
///
/// # The cross-runtime restore seam
///
/// This marker is the **runtime-agnostic contract** between staging and
/// restore. Scheduler-placed restore is create-then-stage: the controller
/// creates the restore Pod *before* the snapshot exists on any node, the
/// scheduler places it, and only then does the node stage the bundle onto
/// that node — dropping `<target_path>/<STAGED_MARKER_FILENAME>` *last*, after
/// every part is on disk. So the restore must not begin until this marker
/// appears, or it would restore from an empty/partial directory.
///
/// **Where that wait lives is runtime-specific; the marker is not.** Each
/// `RuntimeAdapter`'s restore mechanism is responsible for waiting on this
/// marker at its own restore-trigger point:
///
/// - **gVisor** (`GvisorAdapter` + `containerd-shim-sandbawks-v1`): the shim
///   blocks the root container's `runsc restore` on the marker at container
///   `Start`. It can NOT be a Pod init container — a gVisor memory checkpoint
///   restores as a *whole sandbox* and runsc reconstitutes exactly the
///   checkpoint's container set, rejecting any container absent from it.
/// - **runc + CRIU** (planned): CRIU restore is per-container, not
///   whole-sandbox, so that adapter may wait via a Pod init container or at its
///   CRIU-restore call.
/// - **Kata / Firecracker** (planned): wherever that runtime's restore is
///   driven (its own shim/path).
///
/// A new restore adapter therefore does NOT inherit the wait for free — it must
/// honor this marker. Both the writer (node) and every waiter MUST agree on
/// this name, so it lives here rather than being duplicated as a string literal
/// across crates (and, by convention, the Go shim mirrors it). The leading dot
/// keeps it from colliding with any `*.img` bundle part the same directory holds.
pub const STAGED_MARKER_FILENAME: &str = ".staged";

/// Direction of the snapshot operation.
///
/// `Pause` means "checkpoint the container and upload the snapshot";
/// `Resume` means "download a previously-uploaded snapshot and restore
/// the container from it"; `Stage` means "download a snapshot and
/// materialize its parts onto local disk *without* restoring a
/// container", leaving the resulting path in
/// [`SnapshotJobStatus::local_path`] for a downstream consumer (e.g. a
/// `containerd-shim` start-time `runsc restore`).
///
/// The variants serialize verbatim in PascalCase (`"Pause"`,
/// `"Resume"`, `"Stage"`). **Do not** add `#[serde(rename_all = ...)]`:
/// deployed `Pause`/`Resume` `SnapshotJob` resources encode those exact
/// tags, and renaming would break round-tripping them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum SnapshotJobMode {
    Pause,
    Resume,
    Stage,
}

/// Lifecycle phase of a `SnapshotJob`.
///
/// `Pending` → (`Checkpointing` → `Validating` | `Restoring`) →
/// (`Complete` | `Failed`). The per-mode middle phases are observable
/// so operators can tell *what* a long-running job is stuck on.
///
/// `Validating` is the [`SnapshotJobMode::Pause`] fail-closed gate (issue
/// #235): after `runsc checkpoint` + upload, the node re-fetches the
/// just-uploaded bundle from storage and proves it restorable (a
/// throwaway `runsc restore` + envd health probe) *before* reporting
/// `Complete`. The controller only advances `Pausing → Paused` — making
/// the source Pod reclaimable — on `Complete`, so a snapshot that fails
/// to validate lands the job `Failed` while the source is still
/// recoverable, rather than surfacing as unrecoverable data loss at
/// resume time. (`Uploading` is currently folded into the adapter's
/// `pause` and not stamped separately; T10's `--progress` flag revisits.)
///
/// `Restoring` is overloaded: both [`SnapshotJobMode::Resume`] (the live
/// `runsc restore`) and [`SnapshotJobMode::Stage`] (download +
/// materialize onto local disk for the containerd-shim restore path)
/// report it, since `Stage` has no dedicated phase. A `kubectl get
/// sbxsj` reader sees `Restoring` for either and disambiguates by
/// `spec.mode`; a dedicated `Staging` variant is a possible follow-up if
/// the overload proves confusing in practice. (The note lives on the
/// enum type rather than the variant on purpose: a `///` on the variant
/// makes `schemars` reorder the generated CRD `enum` list without adding
/// any per-value description k8s can carry.)
///
/// Like [`SnapshotJobMode`], these variants serialize verbatim in
/// PascalCase and are load-bearing on the wire (the node worker writes
/// them, the controller matches on them). **Do not** add
/// `#[serde(rename_all = ...)]`: it would silently break status
/// round-tripping for in-flight `SnapshotJob` resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
pub enum SnapshotJobPhase {
    #[default]
    Pending,
    Checkpointing,
    Uploading,
    Validating,
    Restoring,
    Complete,
    Failed,
}

/// Desired state of a single snapshot operation on one node.
///
/// One `SnapshotJob` corresponds to exactly one container on one node.
/// The controller is responsible for picking `node_name` (from
/// `Sandbox.status.assigned_node`) and `container_id` (from
/// `Sandbox.status.pod_uid` or the underlying CRI lookup).
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.dev",
    version = "v1alpha1",
    kind = "SnapshotJob",
    plural = "snapshotjobs",
    shortname = "sbxsj",
    namespaced,
    status = "SnapshotJobStatus",
    // KEP-4358 selectableFields. Without this, the per-node reconciler's
    // `watcher::Config::default().fields("spec.nodeName=$NODE_NAME")` is
    // rejected by the apiserver (only built-in fields like
    // metadata.name/namespace are selectable on CRDs by default).
    // Requires k8s >= 1.30; the smoke runs 1.31 and prod targets >=
    // 1.30 per the workspace k8s-openapi feature pin.
    //
    // Leading dot is required by the apiextensions validator
    // (`is an invalid path: expected [ or . but got: spec`).
    // kube-derive 0.97 passes the string through verbatim into the
    // emitted `jsonPath`, so we include the dot ourselves rather than
    // relying on the macro to add it.
    selectable = ".spec.nodeName",
    printcolumn = r#"{"name":"State","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Mode","type":"string","jsonPath":".spec.mode"}"#,
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".spec.nodeName"}"#,
    printcolumn = r#"{"name":"Sandbox","type":"string","jsonPath":".spec.sandboxName"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotJobSpec {
    /// Name of the `Sandbox` CR this job operates on (same namespace).
    pub sandbox_name: String,

    /// `Node.metadata.name` the per-node worker must run on. The
    /// node-side reconciler filters its watch by this field.
    pub node_name: String,

    /// Direction of the operation. See [`SnapshotJobMode`].
    pub mode: SnapshotJobMode,

    /// CRI container ID for the sandbox's primary container. Populated
    /// by the controller from `Sandbox.status.pod_uid` (or a CRI
    /// lookup) at job-creation time so the node worker doesn't have to
    /// re-resolve it.
    pub container_id: String,

    /// For `Resume`, the snapshot URL to restore from (typically
    /// `s3://...`). `None` for `Pause` jobs.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_ref: Option<String>,

    /// For `Resume`, the aggregate sha256 the controller expects the
    /// fetched manifest to match — sourced from
    /// `Sandbox.status.snapshot_sha256`, which was folded in from the
    /// completing Pause job's manifest. `None` for `Pause` jobs and
    /// for legacy callers that don't commit a fingerprint (the node
    /// then skips the manifest-level cross-check; per-part sha256
    /// verification still runs).
    ///
    /// Issue #171 hardening: closes the manifest-tamper window between
    /// pause and resume by binding the URL to a controller-committed
    /// fingerprint.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub expected_sha256: Option<String>,

    /// For `Resume`/`Stage`, the envd-binary sha256 the controller
    /// observed at pause and commits forward — sourced from
    /// `Sandbox.status.snapshot_envd_sha256`, which was folded in from
    /// the completing Pause job's manifest `envd_sha256`. `None` for
    /// `Pause` jobs and for legacy callers / v1 manifests that recorded
    /// no envd digest (the node then skips the envd-level cross-check;
    /// the restore-side `verify_envd_digest` local-binary check still
    /// runs).
    ///
    /// Issue #278 hardening (follow-up to #270 / #171): mirrors
    /// [`Self::expected_sha256`] for the envd dimension so a manifest
    /// tampered in storage can't redefine "the expected envd" on the
    /// restore node. Without it, a stored manifest that flips both
    /// `sha256` and `envd_sha256` to an attacker-controlled binary would
    /// pass the envd dimension of the pause-side commit cross-check.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub expected_envd_sha256: Option<String>,

    /// For [`SnapshotJobMode::Stage`] resume jobs, the *deterministic*
    /// local filesystem path the node worker must materialize the
    /// snapshot's parts at — `<stagingRoot>/<sandbox-id>/restore` (epic
    /// #484, scheduler-placed restore).
    ///
    /// Under the new resume ordering the controller creates the restore
    /// Pod first (unpinned) with its `io.sandbawks/restore-image-path`
    /// annotation already set to this path, then dispatches the `Stage`
    /// job *targeting the same path* once the scheduler has placed the
    /// Pod. Pinning the path here — rather than letting the node pick a
    /// timestamped dir and reporting it back — is what lets the restore
    /// Pod's annotation be set before the snapshot exists.
    ///
    /// `None` for `Pause` jobs and for legacy `Stage` callers that let the
    /// node choose the path (it then reports the chosen path back in
    /// [`SnapshotJobStatus::local_path`] as before).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub target_path: Option<String>,
}

/// Observed state of a `SnapshotJob`, written by the per-node worker.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotJobStatus {
    /// Current phase. See [`SnapshotJobPhase`].
    #[serde(default)]
    pub phase: SnapshotJobPhase,

    /// On `Complete` for a `Pause` job, the manifest the controller
    /// folds into `Sandbox.status.snapshot_ref`. `None` while in
    /// flight, and for `Resume` jobs.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub manifest: Option<SnapshotManifest>,

    /// Human-readable diagnostic message — populated on `Failed` with
    /// the underlying error, or on long-running phases to surface
    /// progress (e.g. "uploaded 3/12 chunks").
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message: Option<String>,

    /// RFC 3339 timestamp the worker first picked the job up.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub started_at: Option<String>,

    /// RFC 3339 timestamp the worker entered a terminal phase
    /// (`Complete` or `Failed`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub completed_at: Option<String>,

    /// On `Complete` for a [`SnapshotJobMode::Stage`] job, the local
    /// filesystem path where the snapshot's parts were materialized
    /// (the return value of [`crate::RuntimeAdapter::stage_snapshot`]).
    /// A downstream consumer — e.g. the `containerd-shim` performing a
    /// start-time `runsc restore` — reads from this path. `None` while
    /// in flight and for `Pause`/`Resume` jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire tags must stay PascalCase and verbatim — deployed
    /// `Pause`/`Resume` `SnapshotJob` resources depend on them, and the
    /// shim restore path keys off `"Stage"`.
    #[test]
    fn mode_serializes_to_pascal_case_tags() {
        assert_eq!(
            serde_json::to_string(&SnapshotJobMode::Pause).unwrap(),
            "\"Pause\""
        );
        assert_eq!(
            serde_json::to_string(&SnapshotJobMode::Resume).unwrap(),
            "\"Resume\""
        );
        assert_eq!(
            serde_json::to_string(&SnapshotJobMode::Stage).unwrap(),
            "\"Stage\""
        );
    }

    #[test]
    fn mode_stage_round_trips() {
        let parsed: SnapshotJobMode = serde_json::from_str("\"Stage\"").unwrap();
        assert_eq!(parsed, SnapshotJobMode::Stage);
    }

    /// `local_path` uses the crate-wide camelCase rename and is omitted
    /// from the wire form when `None`, matching the other optional
    /// status fields.
    #[test]
    fn local_path_is_omitted_when_none() {
        let status = SnapshotJobStatus::default();
        let json = serde_json::to_string(&status).unwrap();
        assert!(!json.contains("localPath"), "got: {json}");
    }

    /// `expected_envd_sha256` (issue #278) is a camelCase optional that
    /// is omitted from the wire form when `None`, matching its sibling
    /// `expected_sha256` so legacy `SnapshotJob` resources that predate
    /// the field round-trip unchanged.
    #[test]
    fn expected_envd_sha256_is_omitted_when_none() {
        let spec = SnapshotJobSpec {
            sandbox_name: "sbx-1".into(),
            node_name: "node-a".into(),
            mode: SnapshotJobMode::Resume,
            container_id: "cid".into(),
            snapshot_ref: Some("s3://b/k".into()),
            expected_sha256: None,
            expected_envd_sha256: None,
            target_path: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(!json.contains("expectedEnvdSha256"), "got: {json}");
        assert!(!json.contains("targetPath"), "got: {json}");
    }

    #[test]
    fn expected_envd_sha256_serializes_as_camel_case_and_round_trips() {
        let spec = SnapshotJobSpec {
            sandbox_name: "sbx-1".into(),
            node_name: "node-a".into(),
            mode: SnapshotJobMode::Resume,
            container_id: "cid".into(),
            snapshot_ref: Some("s3://b/k".into()),
            expected_sha256: Some("a".repeat(64)),
            expected_envd_sha256: Some("b".repeat(64)),
            target_path: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains(&format!("\"expectedEnvdSha256\":\"{}\"", "b".repeat(64))));

        let parsed: SnapshotJobSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.expected_envd_sha256.as_deref(),
            Some("b".repeat(64).as_str())
        );
    }

    #[test]
    fn local_path_serializes_as_camel_case_and_round_trips() {
        let status = SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            local_path: Some("/var/lib/sandbawks/stage/sbx-1".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"localPath\":\"/var/lib/sandbawks/stage/sbx-1\""));

        let parsed: SnapshotJobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.local_path.as_deref(),
            Some("/var/lib/sandbawks/stage/sbx-1")
        );
    }

    #[test]
    fn target_path_serializes_as_camel_case_and_round_trips() {
        // Epic #484: a `Stage` job carries the deterministic restore path
        // the controller stamped onto the Pod's annotation; it must
        // round-trip verbatim so the node stages exactly where the shim
        // restores from.
        let spec = SnapshotJobSpec {
            sandbox_name: "sbx-1".into(),
            node_name: "node-a".into(),
            mode: SnapshotJobMode::Stage,
            container_id: "cid".into(),
            snapshot_ref: Some("s3://b/k".into()),
            expected_sha256: None,
            expected_envd_sha256: None,
            target_path: Some("/var/lib/sandbawks/staging/sbx-1/restore".into()),
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"targetPath\":\"/var/lib/sandbawks/staging/sbx-1/restore\""));

        let parsed: SnapshotJobSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.target_path.as_deref(),
            Some("/var/lib/sandbawks/staging/sbx-1/restore")
        );
    }
}
