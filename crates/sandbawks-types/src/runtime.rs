//! The `RuntimeAdapter` trait — the contract between
//! `sandbawks-controller` (caller) and `sandbawks-node` (caller, via
//! per-runtime impls landed in plans 05/08/09).
//!
//! Plan 05 T3 evolves the trait by:
//!
//! 1. Threading a [`crate::snapshot::SnapshotSink`] (write side) into
//!    [`RuntimeAdapter::pause`] and a [`crate::snapshot::SnapshotSource`]
//!    (read side) into [`RuntimeAdapter::resume`], so impls can stream
//!    snapshot bytes through a storage abstraction without taking on
//!    the dependency themselves.
//! 2. Splitting [`RuntimeError`] into snapshot-specific variants so the
//!    controller can distinguish "checkpoint failed" (rerun pause from
//!    scratch) from "upload failed" (retry just the transfer).
//! 3. Evolving [`SnapshotManifest`] into a multi-part shape with
//!    [`crate::snapshot::SnapshotPart`] entries plus an aggregate
//!    `sha256` over the sorted part hashes.

use std::borrow::Cow;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::snapshot::{
    aggregate_sha256, NoopSnapshotStore, SnapshotPart, SnapshotSink, SnapshotSource,
};
use crate::PodStatusSummary;
use crate::Sandbox;

/// In-container parent directory of the relocated envd binary (epic #466).
///
/// This is a plain directory in the *workload rootfs* — NOT a separate volume.
/// envd `mkdir -p`s it and copies itself onto [`ENVD_RELOCATE_TARGET`]; under the
/// cluster's `--overlay2=root:self` the rootfs is the sandbox-internal
/// overlay-tmpfs that `runsc checkpoint` walks into `pages.img`, so the relocated
/// binary's code pages are captured (and survive a host-side envd upgrade). A
/// `medium: Memory` emptyDir here would instead be a gofer-backed `bind` — not
/// captured, and a #237 mount-set violation — which is why no volume is mounted
/// at this path. Capture-mode is detected at pause from the relocate env, not a
/// mount ([`ENVD_RELOCATE_TARGET_ENV`], [`SnapshotManifest::envd_captured`]).
///
/// Nested under `/var/tmp/envd` (alongside the `--state-file`) rather than a
/// top-level `/sandbawks/run` (#656): envd runs as the workload image's `USER`,
/// which is routinely non-root, so `mkdir`ing a fresh top-level directory fails
/// `EACCES` under root-owned `/` — crash-looping cold start and wedging warm
/// pools at `ready=0`. `/var/tmp` is sticky `1777`, so the non-root envd can
/// create its own subtree there; this is the same reasoning that placed the
/// `--state-file` under `/var/tmp/envd`. It stays a plain rootfs path (no mount),
/// so the `runsc checkpoint` capture property above is unchanged.
pub const ENVD_RUN_DIR: &str = "/var/tmp/envd/run";

/// Absolute in-container rootfs path envd relocates (copies + re-execs) itself
/// onto on cold start — [`ENVD_RUN_DIR`] + `/envd`. Value of
/// [`ENVD_RELOCATE_TARGET_ENV`]; the controller sets that env on every
/// relocate-mode Pod and the node reads it back as the capture-mode marker.
pub const ENVD_RELOCATE_TARGET: &str = "/var/tmp/envd/run/envd";

/// Env var on the envd container telling it where to relocate itself (#467/#468).
/// Its presence on a paused container is the capture-mode marker the node records
/// as [`SnapshotManifest::envd_captured`].
pub const ENVD_RELOCATE_TARGET_ENV: &str = "SANDBAWKS_ENVD_RELOCATE_TARGET";

/// Metadata describing one snapshot of a sandbox's filesystem + memory.
///
/// Plan 05 T3 evolves the v1 single-blob shape into a v2 multi-part
/// manifest:
///
/// - `parts` enumerates the individual blobs that make up the snapshot
///   (memory image, rootfs delta, envd-state, …) each with its own
///   sha256.
/// - `snapshot_ref` is the storage URL of the manifest itself
///   (e.g. `s3://sandbawks-snapshots/<sandbox>/<ts>/manifest.json`).
///   It replaces the retired `inline://manifest/<sha256>` scheme the
///   Plan 04 controller wrote into `Sandbox.status.snapshotRef`.
/// - `sha256` is repurposed as the aggregate over sorted part hashes
///   (see [`crate::snapshot::aggregate_sha256`]). For empty `parts`
///   it's the empty string; this matches the v1 absent-snapshot shape.
///
/// Both `parts` and `snapshot_ref` are `#[serde(default)]` so a v1
/// JSON document deserializes successfully into a v2 manifest with
/// `parts: vec![]` and `snapshot_ref: ""`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotManifest {
    /// The Sandbox the snapshot belongs to (the CR `metadata.name`).
    pub sandbox_id: String,
    /// Runtime that produced the snapshot (`gvisor`, `kata-fc`, `runc-criu`).
    pub runtime_class: String,
    /// Aggregate SHA-256 over the sorted part hashes (see
    /// [`crate::snapshot::aggregate_sha256`]). Empty when `parts` is empty.
    pub sha256: String,
    /// RFC 3339 timestamp.
    pub created_at: String,
    /// Individual parts of the snapshot bundle. Defaults to empty for
    /// backwards-compat deserialization of v1 single-sha256 manifests.
    #[serde(default)]
    pub parts: Vec<SnapshotPart>,
    /// Storage URL of the manifest (e.g. `s3://bucket/key`). Defaults
    /// to the empty string for backwards-compat deserialization.
    #[serde(default)]
    pub snapshot_ref: String,
    /// Lower-case-hex SHA-256 of the node-staged `envd` binary the
    /// paused sandbox was executing (issue #270).
    ///
    /// Under the node-staging delivery model (issue #265), the workload
    /// container execs the cluster-wide `envd` binary mounted read-only
    /// from the node, so its executable pages are mmap-backed by that
    /// on-disk file. A memory-only `runsc restore` reconnects those
    /// mappings, so the binary the *restore* node stages must be
    /// byte-identical to the one the *pause* node checkpointed against;
    /// otherwise the restored process's code pages mismatch the file and
    /// the restore corrupts. Recording the digest here makes the resume
    /// path self-describing: the restore node fails closed when its
    /// staged binary has drifted (a mutable `envd.image` tag re-pushed
    /// between pause and resume).
    ///
    /// Empty for v1 manifests and for a pause that could not read the
    /// staged binary (e.g. a node worker without the envd mount); the
    /// restore path then warns and falls back to per-part sha256
    /// verification only, rather than hard-failing. `#[serde(default)]`
    /// so a manifest written before this field existed deserializes to
    /// the empty string.
    #[serde(default)]
    pub envd_sha256: String,
    /// Host `runsc` (gVisor) release token (e.g. `release-20260525.0`) the
    /// checkpoint in this snapshot was taken against (#545, Capability B).
    ///
    /// gVisor checkpoint/restore requires the *exact* same runsc build on
    /// the pausing and restoring node — a mismatch makes `runsc restore`
    /// fail with `runsc version does not match across checkpoint restore`
    /// and crash-loops the restore Pod's sandbox (`FailedCreatePodSandBox`).
    /// Recording the pausing node's runsc release here makes the snapshot
    /// self-describing: the controller folds it into
    /// `Sandbox.status.snapshotRuntimeVersion` and constrains the restore
    /// Pod's `nodeAffinity` to nodes advertising the matching
    /// [`crate::RUNSC_VERSION_LABEL`], turning a fatal mismatch into a
    /// bounded, diagnosable `Pending`.
    ///
    /// Empty for v1 manifests and for a pause whose node could not read its
    /// host runsc version (an unparseable/absent binary); the restore gate
    /// then degrades to envd-staged affinity only — no worse than today's
    /// ungated placement — rather than pinning the restore to nowhere.
    /// `#[serde(default)]` so a manifest written before this field existed
    /// deserializes to the empty string.
    #[serde(default)]
    pub runsc_version: String,
    /// Whether the checkpointed `envd` was running from the relocate target
    /// (the `/var/tmp/envd/run/envd` rootfs path) rather than the node-staged
    /// host binary at pause time (epic #466, capture-in-checkpoint).
    ///
    /// Under the relocation model (#467/#468) a cold-started sandbox copies
    /// `envd` onto a plain directory in its rootfs and re-execs it there. Under
    /// the cluster's `--overlay2=root:self` the rootfs is the sandbox-internal
    /// overlay-tmpfs, so those code pages live in the checkpoint's `pages.img`.
    /// A memory-only `runsc restore` rebuilds them from the snapshot itself —
    /// the restore node's host `envd` binary is no longer the backing store for
    /// the workload's mappings — so the snapshot survives a later host-side
    /// `envd` upgrade that would otherwise re-fault a hostPath-bound binary's
    /// pages and corrupt the restore.
    ///
    /// `true` tells the restore path it is safe to *skip* the #270 host-binary
    /// determinism comparison ([`crate::RuntimeAdapter`] impls'
    /// `verify_envd_digest`): the host is irrelevant, and the per-part sha256
    /// gate still protects the snapshot bytes. `false`/absent preserves the
    /// fail-closed host comparison for legacy (pre-relocation) snapshots,
    /// which remain mmap-backed by the staged host binary.
    ///
    /// `#[serde(default)]` so a v1/legacy manifest written before this field
    /// existed deserializes to `false` and keeps today's fail-closed guard.
    #[serde(default)]
    pub envd_captured: bool,
    /// Name of the `SandboxTemplate` the snapshotted sandbox was created from
    /// (issue #416). Recorded so a *clone-from-snapshot* restore can validate
    /// the source against the new sandbox's template before staging.
    ///
    /// Empty for snapshots written before this field existed; the clone path's
    /// template gate then degrades to warn-only rather than hard-failing.
    /// `#[serde(default)]` so a legacy manifest deserializes to the empty string.
    #[serde(default)]
    pub template_ref: String,
    /// Workload image ref/digest (`SandboxTemplate.spec.image`) the snapshotted
    /// sandbox was checkpointed from (issue #416). The compatibility-critical
    /// half of [`Self::template_ref`]: a memory-image restore reconnects the
    /// workload's code-page mappings, so a clone whose template image differs
    /// from the snapshot's would restore corrupt. The controller's clone gate
    /// compares this against the new sandbox's resolved template image.
    ///
    /// Empty for legacy snapshots (warn-only image gate). `#[serde(default)]` so
    /// a manifest written before this field existed deserializes to the empty
    /// string.
    #[serde(default)]
    pub template_image: String,
}

impl SnapshotManifest {
    /// Recompute the aggregate `sha256` from `parts`.
    ///
    /// Call after mutating `parts` in place; lets callers build a
    /// manifest by pushing parts and then sealing the aggregate, rather
    /// than constructing the part list up-front.
    pub fn recompute_aggregate(&mut self) {
        self.sha256 = aggregate_sha256(&self.parts);
    }
}

/// Errors a `RuntimeAdapter` impl can raise.
///
/// The snapshot-specific variants (`SnapshotUpload`, `SnapshotDownload`,
/// `CheckpointFailed`, `RestoreFailed`) added in Plan 05 T3 let the
/// controller's retry policy distinguish transfer failures (retry the
/// transfer) from runtime failures (re-run the whole pause/resume).
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("pause failed: {0}")]
    Pause(String),
    #[error("resume failed: {0}")]
    Resume(String),
    /// Transient failure — caller should retry with backoff.
    #[error("transient: {0}")]
    Transient(String),
    /// Uploading a snapshot part or manifest to the store failed.
    /// Retry the transfer; the underlying checkpoint may still be valid.
    #[error("snapshot upload failed: {0}")]
    SnapshotUpload(String),
    /// Downloading a snapshot part or manifest from the store failed.
    /// Retry the transfer; the stored bytes are presumed intact.
    #[error("snapshot download failed: {0}")]
    SnapshotDownload(String),
    /// The runtime's checkpoint step (e.g. `runsc checkpoint`,
    /// `criu dump`) failed. The pause must be re-attempted from
    /// scratch — partial state is not recoverable.
    #[error("checkpoint failed: {0}")]
    CheckpointFailed(String),
    /// The runtime's restore step failed despite the manifest +
    /// parts downloading successfully. Usually indicates a corrupt
    /// or version-incompatible snapshot.
    #[error("restore failed: {0}")]
    RestoreFailed(String),
    /// The adapter does not implement the requested operation. The
    /// payload names the method (e.g. `"stage_snapshot"`). Returned by
    /// the default trait impls so partial adapters surface a clear
    /// "not wired up" error instead of silently succeeding.
    ///
    /// `Cow<'static, str>` so the common case — a static method name —
    /// stays zero-alloc (`Borrowed`), while a caller that needs a
    /// runtime-formatted detail (e.g. `"resume on runtime class
    /// kata-fc"`) can pass an owned `String` without a new variant.
    #[error("unsupported operation: {0}")]
    Unsupported(Cow<'static, str>),
    /// No runsc `--root` could be resolved for `container_id` — none of
    /// the explicit candidates held the container's state, and generic
    /// discovery across the configured search directories found nothing.
    ///
    /// This is a **terminal**, **greppable** condition distinct from
    /// [`CheckpointFailed`]: the pause cannot succeed on this node
    /// regardless of retries because the structural precondition (a
    /// resolvable runsc root) is absent. It is only emitted for a **real,
    /// live container** — a fresh node where the shim has not yet created
    /// the state root lazily is never affected (issue #379).
    ///
    /// On receipt the node must withdraw snapshot-capability advertising
    /// (the `sandbawks.dev/envd-staged` label) so the controller stops
    /// scheduling new pause jobs here until a root becomes resolvable.
    #[error(
        "no runsc --root resolved for container {container_id}: \
         checked candidates {candidates:?}, searched {search_dirs:?}"
    )]
    RunscRootUnresolved {
        container_id: String,
        candidates: Vec<std::path::PathBuf>,
        search_dirs: Vec<std::path::PathBuf>,
    },
}

/// Handle returned by [`RuntimeAdapter::create_pod`] describing the Pod
/// the controller just materialized.
///
/// The controller stores `"{namespace}/{name}"` in
/// [`crate::SandboxStatus::pod_ref`] and `envd_address` in
/// [`crate::SandboxStatus::envd_address`]. Plan 05's `GvisorAdapter`
/// reuses the same handle to address the Pod for CRI-side calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRef {
    /// Pod name as it appears in `metadata.name`.
    pub name: String,
    /// Namespace the Pod lives in.
    pub namespace: String,
}

/// Placement directives for a *restore* Pod — the shim-driven
/// start-time `runsc restore` path (epic #484, scheduler-placed restore).
///
/// The controller builds one of these on the `Action::CreateRestorePod`
/// edge at the *start* of a resume — before any snapshot has been staged.
/// Passing `Some(&RestorePlacement)` to [`RuntimeAdapter::create_pod`]
/// tells the adapter to:
/// - default `spec.runtimeClassName` to `sandbawks-runsc` (so the gVisor
///   shim handles the container) when the Sandbox doesn't pin its own,
/// - carry [`Self::staging_path`] — a *deterministic*, per-sandbox path
///   the snapshot will later be staged at — in the
///   `io.sandbawks/restore-image-path` annotation the shim reads to
///   restore from local disk,
/// - **not** inject any restore-mode env var — the container is born
///   restored, so envd never enters the legacy idle-hold.
///
/// Note the absence of any node pin. The old design hard-pinned the
/// restore Pod to the pre-pause node (via `node_name` here and a
/// `spec.nodeName` on the Pod); that re-pinned a recreated Pod onto the
/// same — possibly memory-starved — node and was the root of the
/// `resumingforever` hang (epic #484). The restore Pod is now
/// *scheduler-placed*: the controller reads `spec.nodeName` back once the
/// scheduler binds the Pod and stages the snapshot onto *that* node, so no
/// pin is needed and the scheduler does memory-aware placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePlacement {
    /// Deterministic, per-sandbox local filesystem path the snapshot will
    /// be staged at (`<stagingRoot>/<sandbox-id>/restore`). Threaded into
    /// the restore Pod's `io.sandbawks/restore-image-path` annotation at
    /// create time; the node-local `Stage` job later materializes the
    /// snapshot's parts here on whichever node the scheduler picked.
    pub staging_path: String,
}

/// The contract a runtime backend must satisfy for the controller to
/// pause and resume sandboxes.
///
/// `pause` / `resume` are required to be **idempotent**: the controller
/// may re-invoke them after a crash or requeue. Implementations must
/// detect existing snapshots by manifest content-hash and short-circuit.
///
/// All methods ship with default impls so test fakes and partial
/// adapters can pick only the operations they implement. The default
/// `pause` / `resume` are no-ops that ignore their `sink` / `source`
/// parameter — Plan 05's `GvisorAdapter` (T7/T8) overrides them with
/// real CRI calls. The `create_pod`, `delete_pod`, `pod_ready` defaults
/// are inherited from Plan 04 T5's contract.
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    /// Quiesce the sandbox running in `pod_uid`, capture its state by
    /// streaming each part through `sink`, and return a manifest the
    /// caller can record in `Sandbox.status`.
    ///
    /// The default impl returns an empty placeholder manifest and
    /// ignores `sink`. `FakeRuntimeAdapter` and `PodAdapter` keep their
    /// existing no-storage behavior by overriding this with the new
    /// signature; Plan 05 T7's `GvisorAdapter` implements real
    /// checkpointing.
    async fn pause(
        &self,
        pod_uid: &str,
        sink: &dyn SnapshotSink,
    ) -> Result<SnapshotManifest, RuntimeError> {
        let _ = (pod_uid, sink);
        Ok(SnapshotManifest {
            sandbox_id: pod_uid.to_owned(),
            runtime_class: self.runtime_class().to_owned(),
            sha256: String::new(),
            created_at: "1970-01-01T00:00:00Z".into(),
            parts: Vec::new(),
            snapshot_ref: String::new(),
            envd_sha256: String::new(),
            runsc_version: String::new(),
            envd_captured: false,
            template_ref: String::new(),
            template_image: String::new(),
        })
    }

    /// Restore a sandbox from `manifest` into `pod_uid`, streaming each
    /// part out of `source`. The pod is expected to already exist in a
    /// `Paused`-ready slot; provisioning the pod itself is the
    /// controller's responsibility.
    ///
    /// The default impl is a no-op that ignores both `manifest` and
    /// `source` — sufficient for adapters that don't preserve state.
    async fn resume(
        &self,
        manifest: &SnapshotManifest,
        pod_uid: &str,
        source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        let _ = (manifest, pod_uid, source);
        Ok(())
    }

    /// Materialize `manifest`'s parts onto local disk for `sandbox_id`,
    /// streaming each out of `source`, and return the local filesystem
    /// path the bytes were staged at — **without** restoring a
    /// container. The caller records that path in
    /// [`crate::SnapshotJobStatus::local_path`] for a downstream
    /// consumer (e.g. a `containerd-shim` performing a start-time
    /// `runsc restore`).
    ///
    /// This is the read-side counterpart to [`Self::pause`] for the
    /// shim-driven restore path, where the container does not yet exist
    /// at stage time, so [`Self::resume`]'s "pod already exists" contract
    /// can't apply.
    ///
    /// # Scheduler-placed restore (`target_path`)
    ///
    /// When `target_path` is `Some`, the controller has pinned a
    /// *deterministic* directory up front (epic #484): it created the
    /// restore Pod first with its `restore-image-path` annotation already
    /// pointing there, so the implementation MUST materialize the bundle
    /// at exactly that path — overwriting any prior attempt — and drop a
    /// [`crate::STAGED_MARKER_FILENAME`] completion marker inside it
    /// *last*, atomically, so the credential-less stage-gate init
    /// container can poll for it. The returned path equals `target_path`.
    ///
    /// When `target_path` is `None` (legacy callers), the implementation
    /// picks its own path and reports it back in
    /// [`crate::SnapshotJobStatus::local_path`] as before; no marker is
    /// written.
    ///
    /// # Errors
    ///
    /// The default impl makes no attempt to stage and returns
    /// [`RuntimeError::Unsupported`]. Real implementations return
    /// [`RuntimeError::SnapshotDownload`] for transfer failures.
    async fn stage_snapshot(
        &self,
        sandbox_id: &str,
        manifest: &SnapshotManifest,
        source: &dyn SnapshotSource,
        target_path: Option<&str>,
    ) -> Result<String, RuntimeError> {
        let _ = (sandbox_id, manifest, source, target_path);
        Err(RuntimeError::Unsupported(Cow::Borrowed("stage_snapshot")))
    }

    /// Prove the snapshot described by `manifest` is restorable, by
    /// re-fetching the *uploaded* copy from `source` and verifying it
    /// materializes back into a complete checkpoint bundle — **before**
    /// the caller reports the pause complete and lets the source sandbox
    /// be reclaimed (issue #235, fail-closed teardown).
    ///
    /// The node worker calls this between [`Self::pause`] returning Ok
    /// and patching the `SnapshotJob` to `Complete`. A returned `Err`
    /// lands the job `Failed`, so the controller never advances
    /// `Pausing → Paused` and the still-present source Pod stays
    /// recoverable — converting "snapshot turns out unrestorable at
    /// resume time, source long gone" (unrecoverable data loss) into
    /// "pause failed, source intact" (a retriable, recoverable error).
    ///
    /// Implementations re-fetch every part out of `source` (proving the
    /// stored copy, not merely the local pause work-dir, round-trips and
    /// verifies its sha256s) and assert the materialized memory image is
    /// structurally complete. A *live* throwaway `runsc restore` was
    /// tried and rejected: it must replay the pod-sandbox container's OCI
    /// bundle, which containerd reaps within seconds of the checkpoint —
    /// racy and flaky for a fail-closed gate (see the `GvisorAdapter`
    /// impl). The storage round-trip needs no bundle and boots no second
    /// sandbox, so it is robust and cheap.
    ///
    /// The default impl is a no-op that ignores its arguments and returns
    /// `Ok(())` — sufficient for test fakes and partial adapters that
    /// don't preserve state, mirroring the no-op [`Self::pause`] /
    /// [`Self::resume`] defaults. Plan 07's `GvisorAdapter` overrides it
    /// with the real storage round-trip proof.
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::SnapshotDownload`] for
    /// a re-fetch / verification failure (the uploaded snapshot is corrupt
    /// or truncated) or [`RuntimeError::CheckpointFailed`] when the parts
    /// verify but the round-tripped image set is structurally incomplete.
    async fn validate_restorable(
        &self,
        manifest: &SnapshotManifest,
        source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        let _ = (manifest, source);
        Ok(())
    }

    /// The RuntimeClass name this adapter binds to (`gvisor`, `kata-fc`,
    /// `runc-criu`). Used by the controller to match Sandboxes to the
    /// right adapter at dispatch time.
    fn runtime_class(&self) -> &str;

    /// Materialize a Pod for `sandbox`. The Pod runs `image` (the
    /// workload rootfs from `SandboxTemplate.spec.image`) as its single
    /// container; the static envd binary is staged onto the node by the
    /// `sandbawks-node` DaemonSet and mounted read-only into the
    /// container, so envd execs inside the workload's filesystem without
    /// the image having to bundle it. The controller stamps
    /// [`crate::SandboxStatus::pod_ref`] from the returned [`PodRef`].
    ///
    /// `restore` selects the materialization mode (epic #484):
    /// - `None` — an ordinary fresh sandbox; the container starts cold.
    /// - `Some(&RestorePlacement)` — a restore Pod for the shim-driven
    ///   start-time `runsc restore`. The impl leaves the Pod *unpinned*
    ///   (scheduler-placed), defaults the runtime class to
    ///   `sandbawks-runsc`, and threads the deterministic staging path
    ///   through the `io.sandbawks/restore-image-path` annotation. See
    ///   [`RestorePlacement`] for the full contract. No restore-mode env
    ///   var is injected — the container is born restored.
    ///
    /// The default impl synthesizes a fake [`PodRef`] without making any
    /// cluster calls so adapters that never materialize Pods (test
    /// doubles, snapshot-only adapters) compile unchanged. Plan 04
    /// Task 5's `PodAdapter` overrides this with a real `POST /pods`.
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::Transient`] for
    /// retriable kube transport failures. The default impl is infallible.
    async fn create_pod(
        &self,
        sandbox: &Sandbox,
        image: &str,
        restore: Option<&RestorePlacement>,
    ) -> Result<PodRef, RuntimeError> {
        // Suppress the unused-arg warnings without dropping them from the
        // signature — real impls (Task 5) consume `image`, and the
        // production `PodAdapter` consumes `restore`.
        let _ = (image, restore);
        Ok(PodRef {
            name: format!(
                "{}-fake",
                sandbox.metadata.name.as_deref().unwrap_or("anon")
            ),
            namespace: sandbox
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into()),
        })
    }

    /// Tear down the Pod previously created for this Sandbox. No-op for
    /// the default impl; `PodAdapter` issues a `DELETE /pods/{name}`.
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::Transient`] for
    /// retriable failures. The default impl is infallible.
    async fn delete_pod(&self, pod_ref: &PodRef) -> Result<(), RuntimeError> {
        let _ = pod_ref;
        Ok(())
    }

    /// Poll the Pod's readiness.
    ///
    /// Returns:
    /// - `Ok(None)` — Pod exists but is not yet `Ready`; nothing to
    ///   write back to `Sandbox.status`.
    /// - `Ok(Some(readiness))` — Pod is `Ready`; the reconcile loop
    ///   folds `readiness.envd_address` into
    ///   [`crate::SandboxStatus::envd_address`] and
    ///   `readiness.node_name` / `readiness.pod_uid` into the
    ///   matching status fields. The node name and pod UID are
    ///   load-bearing for SnapshotJob dispatch: per-node workers
    ///   field-select jobs by `spec.nodeName`, and
    ///   `SnapshotJob.spec.containerId` is derived from the pod UID.
    ///
    /// The default impl returns a `PodReadiness` with a stub address
    /// and `None` node/uid so test fakes keep driving the
    /// `Creating → Running` transition without a real Pod watcher.
    /// Adapters that need a non-empty `pod_uid` (e.g. so SnapshotJob
    /// dispatch resolves a concrete container ID) should override this
    /// method — see `FakeRuntimeAdapter` in the controller crate for a
    /// synthetic-UID example.
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::Transient`] for
    /// kube watch / get failures. The default impl is infallible.
    async fn pod_ready(&self, pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        let _ = pod_ref;
        Ok(Some(PodReadiness {
            envd_address: "127.0.0.1:49983".into(),
            node_name: None,
            pod_uid: None,
        }))
    }

    /// Report whether the Pod backing a `Running` Sandbox has reached a
    /// terminal state — its `status.phase` is `Succeeded`/`Failed`, *any* of
    /// its containers has `state.terminated` set, or the Pod no longer exists.
    ///
    /// Unlike [`RuntimeAdapter::pod_ready`] (which the reconcile loop only
    /// consults while `Creating`/`Resuming`), this covers the `Running`
    /// phase: a sandbox Pod runs `envd` with `restartPolicy: Never`, so
    /// when `envd` exits — e.g. its graceful snapshot-and-exit path — the
    /// Pod goes `Succeeded` and stays there. Nothing else observes that, so
    /// the controller folds a `Some(_)` here into [`crate::SandboxPhase::Failed`]
    /// instead of leaving the Sandbox falsely `Running` over a dead Pod.
    ///
    /// The container-`terminated` signal is reported *before* the phase
    /// settles: the kubelet writes it the moment the container exits, while
    /// `status.phase` lags a sync cycle. Implementations must treat it as
    /// terminal too, so a checkpoint is not dispatched against an already-
    /// stopped container during that lag (issue #382).
    ///
    /// Returns `Ok(Some(cause))` when the Pod has terminated, where `cause`
    /// carries actionable details (eviction message, OOMKill memory limit)
    /// the reconcile loop can embed in `status.conditions` so the user
    /// sees a clear error rather than an opaque transport message (issue
    /// #418). `Ok(None)` means the Pod is still live.
    ///
    /// The default impl returns `Ok(None)` (never terminated) so existing
    /// test fakes keep driving the lifecycle unchanged; `PodAdapter`
    /// overrides it with a real `status.phase` + container-status check.
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::Transient`] for kube
    /// get failures (so the probe is retried on the next requeue rather
    /// than mistaken for a terminated Pod). The default impl is infallible.
    async fn pod_terminated(
        &self,
        pod_ref: &PodRef,
    ) -> Result<Option<PodTerminationCause>, RuntimeError> {
        let _ = pod_ref;
        Ok(None)
    }

    /// Distill the backing Pod's observed phase into the minimal
    /// [`PodStatusSummary`] the API gateway needs to render an actionable
    /// "still starting / not ready yet" notice (issue #625).
    ///
    /// The reconcile loop calls this while a Sandbox is *not yet serving*
    /// (`Creating` / `Resuming`, before `status.envdAddress` is recorded) and
    /// stamps the result into [`crate::SandboxStatus::pod_status`]. The
    /// gateway — which holds no pod-read RBAC (#442) — reads it from its
    /// existing `Sandbox` watch to tell a transient "retry shortly" apart from
    /// a stuck `ImagePullBackOff` / `CrashLoopBackOff`.
    ///
    /// Returns `Ok(Some(summary))` when the Pod exists (its phase is always
    /// surfaceable, the blocking `reason` only when one is present), `Ok(None)`
    /// when the Pod no longer exists or carries no `status` at all.
    ///
    /// The default impl returns `Ok(None)` so existing test fakes and
    /// third-party adapters compile unchanged; [`PodAdapter`] overrides it with
    /// a real `status.phase` + container-status read.
    ///
    /// [`PodAdapter`]: https://docs.rs/sandbawks-controller
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::Transient`] for kube get
    /// failures (retried on the next requeue rather than mistaken for a
    /// vanished Pod). The default impl is infallible.
    async fn pod_status_summary(
        &self,
        pod_ref: &PodRef,
    ) -> Result<Option<PodStatusSummary>, RuntimeError> {
        let _ = pod_ref;
        Ok(None)
    }

    /// Report the node the scheduler has bound the Pod to, i.e. its
    /// `spec.nodeName`, while the Pod is still `Pending`/initializing.
    ///
    /// This is the discovery hook for *scheduler-placed restore* (epic
    /// #484). A restore Pod is created **unpinned**; once the scheduler
    /// binds it to a node, `spec.nodeName` is populated long before the
    /// container — let alone envd — is up, so the controller can read it
    /// here to learn *where* to stage the snapshot (it dispatches the
    /// node-local `Stage` job pinned to the returned node).
    ///
    /// Returns:
    /// - `Ok(Some(node))` — the Pod is bound to `node`.
    /// - `Ok(None)` — the Pod exists but is not yet scheduled (no
    ///   `spec.nodeName`), or no longer exists. The reconcile loop treats
    ///   both as "keep waiting"; a genuinely lost Pod is caught by the
    ///   separate liveness path, not here.
    ///
    /// Unlike [`RuntimeAdapter::pod_ready`], this does **not** require the
    /// container to be Ready — `spec.nodeName` lands at bind time, which is
    /// the whole point (stage the snapshot *before* the container starts so
    /// the shim can restore from local disk).
    ///
    /// The default impl returns `Ok(None)` (never scheduled) so existing
    /// test fakes that don't model scheduling compile unchanged;
    /// `PodAdapter` overrides it with a real `spec.nodeName` read, and
    /// `FakeRuntimeAdapter` overrides it to model a placed Pod.
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::Transient`] for kube get
    /// failures (retried on the next requeue). The default impl is
    /// infallible.
    async fn pod_scheduled_node(&self, pod_ref: &PodRef) -> Result<Option<String>, RuntimeError> {
        let _ = pod_ref;
        Ok(None)
    }

    /// Probe whether envd is actually **serving** — not merely TCP-accepting —
    /// by issuing a real request to its HTTP control port and checking for a
    /// `200`. `envd_address` is the gRPC `host:port` from
    /// [`PodReadiness::envd_address`]; the implementation derives envd's HTTP
    /// control port (`:49984`) from the same host.
    ///
    /// This exists to close issue #560: an envd that was `runsc checkpoint`ed
    /// with a live external gRPC connection comes back from `runsc restore`
    /// **bound but wedged** — the kubelet TCP readiness probe on `:49983`
    /// passes (the listener accepts the connection) yet envd answers no
    /// request on either port. Gating the `Resuming → Running` promotion on a
    /// real serve check, rather than kubelet readiness alone, stops the
    /// controller from declaring such a sandbox `Running` and instead lets the
    /// bounded resume-readiness deadline recover it (recreate the restore Pod).
    ///
    /// # Returns
    ///
    /// `Ok(true)` when envd answered `200`, `Ok(false)` when it accepted the
    /// connection but did not answer a healthy response within the probe
    /// timeout (the wedge), and `Err` only for a setup failure the caller
    /// should retry. The default impl returns `Ok(true)` (assume serving) so
    /// adapters that don't model envd — including existing test fakes and
    /// third-party adapters — are unchanged.
    async fn envd_serving(&self, envd_address: &str) -> Result<bool, RuntimeError> {
        let _ = envd_address;
        Ok(true)
    }

    /// Reconcile the sandbox's enforced egress policy to match its current
    /// `spec.networkPolicy` (issue #542).
    ///
    /// The per-sandbox egress `NetworkPolicy` is materialized **once**, at
    /// [`Self::create_pod`] time, and never reconciled afterwards — fine while
    /// `spec.networkPolicy` was set-once. The warm-pool *claim* path breaks
    /// that assumption: a `POST /sandboxes` carrying a per-create `egressAllow`
    /// served by claiming a live warm member patches the member's
    /// `spec.networkPolicy` to the union of the template baseline and the
    /// per-create destinations **without** going through `create_pod`, so the
    /// already-materialized `NetworkPolicy` would otherwise stay at the
    /// template baseline and the override would be silently dropped.
    ///
    /// The controller calls this for a `Running` sandbox so the live policy
    /// (and, on an FQDN-opted cluster, its companion Cilium policy) is brought
    /// into line with the spec, matching what a cold-booted sandbox of the same
    /// spec would enforce. Implementations **must** be idempotent: it runs on
    /// every qualifying reconcile, so a no-op when the live policy already
    /// matches is the common case.
    ///
    /// The default impl is a no-op (`Ok(())`) so test fakes and adapters that
    /// don't materialize a `NetworkPolicy` are unchanged; [`Self::create_pod`]
    /// remains responsible for the initial materialization.
    ///
    /// # Errors
    ///
    /// Real implementations return [`RuntimeError::Transient`] for retriable
    /// apiserver failures so the sync is retried on the next requeue. The
    /// default impl is infallible.
    async fn ensure_egress_policy(&self, sandbox: &Sandbox) -> Result<(), RuntimeError> {
        let _ = sandbox;
        Ok(())
    }
}

/// Cause of a Pod's termination, returned by [`RuntimeAdapter::pod_terminated`].
///
/// Enumerates actionable failure modes that the reconcile loop embeds in
/// `status.conditions[].message` (issue #418) so the user sees a clear,
/// resource-specific message instead of an opaque h2 transport error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodTerminationCause {
    /// The kubelet evicted the Pod (ephemeral-storage exhaustion, general
    /// node-pressure eviction). `message` is the kubelet-written
    /// `pod.status.message` — e.g.
    /// `"Pod ephemeral local storage usage exceeds the total limit of containers 256Mi."`.
    Evicted {
        /// Human-readable eviction message from `pod.status.message`.
        message: String,
    },
    /// The container was killed by the OOM killer (`state.terminated.reason =
    /// "OOMKilled"`). `memory_limit` carries the container's configured
    /// memory limit from the Pod spec when available.
    OomKilled {
        /// Memory limit from the container's resource spec (e.g. `"512Mi"`).
        memory_limit: Option<String>,
    },
    /// Any other terminal state: envd exited normally/abnormally, or the Pod
    /// was deleted out from under a Running Sandbox (node churn, manual
    /// deletion). No actionable resource-budget detail to surface.
    Other,
}

/// Pod-readiness snapshot returned by [`RuntimeAdapter::pod_ready`].
///
/// `envd_address` is the gRPC `host:port` the gateway routes to once
/// the sandbox is Running. `node_name` and `pod_uid` are the
/// kubelet-assigned identifiers the controller stamps into
/// [`crate::SandboxStatus::assigned_node`] /
/// [`crate::SandboxStatus::pod_uid`] respectively — both load-bearing
/// for Plan 05 SnapshotJob dispatch (the per-node worker
/// field-selects on `spec.nodeName`, and the worker uses the pod UID
/// to resolve the CRI container ID for `runsc checkpoint`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodReadiness {
    /// `host:port` of the envd gRPC endpoint inside the Pod.
    pub envd_address: String,
    /// `Pod.spec.nodeName` after kubelet binds the Pod to a node.
    /// `None` when the implementation can't (or doesn't need to)
    /// surface it — e.g. the default test impl.
    pub node_name: Option<String>,
    /// `Pod.metadata.uid`. `None` when unsurfaceable.
    pub pod_uid: Option<String>,
}

/// A shared zero-cost handle to a [`NoopSnapshotStore`].
///
/// Used by callers that have to pass *some* sink/source to satisfy the
/// trait signature but don't have a real store wired up yet (the
/// controller's reconcile loop, until Plan 05 T4 replaces the direct
/// pause/resume call sites with `SnapshotJob` CR creation).
///
/// Returning a static reference keeps the call sites tidy:
///
/// ```ignore
/// adapter.pause(name, noop_sink()).await?;
/// ```
pub fn noop_sink() -> &'static NoopSnapshotStore {
    static NOOP: NoopSnapshotStore = NoopSnapshotStore;
    &NOOP
}

/// Symmetric to [`noop_sink`] — see that function for usage.
pub fn noop_source() -> &'static NoopSnapshotStore {
    static NOOP: NoopSnapshotStore = NoopSnapshotStore;
    &NOOP
}

#[cfg(test)]
mod relocate_target_tests {
    use super::{ENVD_RELOCATE_TARGET, ENVD_RUN_DIR};

    /// Regression for #656: the envd relocate target must live somewhere the
    /// workload image's (routinely non-root) `USER` can `mkdir`. A fresh
    /// top-level directory under root-owned `/` (the old `/sandbawks/run`)
    /// `EACCES`'d cold start and crash-looped warm pools at `ready=0`; nesting
    /// under sticky-`1777` `/var/tmp` lets the non-root envd create its subtree.
    #[test]
    fn relocate_target_is_non_root_writable() {
        assert!(
            ENVD_RUN_DIR.starts_with("/var/tmp/"),
            "ENVD_RUN_DIR must nest under /var/tmp (sticky 1777, non-root-writable), got {ENVD_RUN_DIR}"
        );
        assert!(
            !ENVD_RUN_DIR.starts_with("/sandbawks"),
            "ENVD_RUN_DIR must not be a root-owned top-level dir (#656), got {ENVD_RUN_DIR}"
        );
        assert_eq!(
            ENVD_RELOCATE_TARGET,
            format!("{ENVD_RUN_DIR}/envd"),
            "the relocate target must be the run dir plus /envd"
        );
    }
}
