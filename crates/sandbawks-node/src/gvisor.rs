//! `GvisorAdapter` — `runsc checkpoint` / `runsc restore` backed
//! [`sandbawks_types::RuntimeAdapter`] implementation.
//!
//! Plan 05 T7 landed the **pause** side; T8 fills in `resume` as its
//! symmetric counterpart.
//!
//! ## Pause sequence (T7)
//!
//! 1. Resolve `pod_uid` → `(container_id, pod_ip, sandbox_id)` via the
//!    kube API + `pod.status.containerStatuses[0].containerID` (with the
//!    `containerd://` prefix stripped).
//! 2. Allocate a fresh work-dir `<work_root>/<sandbox-id>/<rfc3339-ts>/`.
//! 3. Shell out to `runsc checkpoint --image-path <work-dir> <container-id>`;
//!    surface stderr as [`RuntimeError::CheckpointFailed`] on non-zero exit.
//! 4. Tar + zstd every `*.img` file `runsc` dropped under `<work-dir>`
//!    into a staged `memory.zst` on disk; stream it to the sink via
//!    [`SnapshotSink::put_part_file`] so node DaemonSet RAM stays flat at
//!    GiB scale (#293), then delete the staged copy. gVisor 20240603+ writes
//!    `checkpoint.img` (saved sentry state), `pages.img` (MemoryFile
//!    pages — this is where the sandbox-internal overlay tmpfs lives,
//!    so it carries the actual rootfs writes the workload made), and
//!    `pages_meta.img` (MemoryFile metadata). All three are required
//!    for a successful restore; bundling them under one part keeps the
//!    manifest v2 wire shape stable (issue #193).
//! 5. Ask containerd's snapshotter for the rootfs diff against the
//!    sandbox-template snapshot; tar + zstd; upload as
//!    `rootfs-delta.tar.zst`.
//! 6. HTTP GET `http://<pod_ip>:49984/__state` from envd's secondary
//!    HTTP listener; upload the response body as `envd-state.json`.
//! 7. Assemble a [`SnapshotManifest`] v2 (the three parts + aggregate
//!    sha256), persist via [`SnapshotSink::put_manifest`], and return it
//!    so the controller can record `Sandbox.status.snapshotRef`.
//!
//! ## Resume sequence (T8)
//!
//! Symmetric counterpart of pause. The controller passes the manifest
//! it stamped on `Sandbox.status.snapshotRef`; the adapter re-fetches
//! the authoritative copy from storage and verifies every part's
//! sha256 before touching `runsc`:
//!
//! 1. Resolve `pod_uid` → `(container_id, pod_ip, sandbox_id)` the same
//!    way as pause.
//! 2. `SnapshotSource::get_manifest` to fetch the authoritative manifest
//!    from storage; cross-check its aggregate sha256 *and* its
//!    `envd_sha256` against the controller-supplied manifest (issue #171
//!    and #278 respectively). Either mismatch → [`RuntimeError::SnapshotDownload`].
//! 3. Allocate a fresh `<work_root>/<sandbox-id>/resume-<rfc3339-ts>/`
//!    work-dir.
//! 4. For each part in the fetched manifest:
//!    - `SnapshotSource::get_part(name)` → bytes.
//!    - Recompute sha256 over the bytes; mismatch → [`RuntimeError::SnapshotDownload`].
//!    - Dispatch by part name:
//!      - `memory.zst` → decompress + un-tar into `<work-dir>` so
//!        `runsc restore --image-path` finds the memory image files
//!        exactly where pause wrote them.
//!      - `rootfs-delta.tar.zst` → hand to
//!        [`ContainerdHandle::apply_rootfs_delta`] so the snapshotter
//!        applies the overlayfs upperdir onto a fresh snapshot.
//!      - `envd-state.json` → write to `<work-dir>/envd-state.json`
//!        verbatim; the restored envd reads it on boot.
//! 5. Shell out to `runsc restore --image-path <work-dir> <container-id>`;
//!    surface stderr as [`RuntimeError::RestoreFailed`] on non-zero exit.
//! 6. Wait for the restored process to bind its envd port: poll
//!    `<pod_ip>:<envd_port>` via `tokio::net::TcpStream::connect` with
//!    a 30 s budget at 500 ms intervals.
//!
//! ## Testability
//!
//! Production hits a real `containerd_client::Client`, a hand-rolled
//! HTTP/1.1 client, and a real `runsc` binary. None of those are easy
//! to bring up in a unit test. The module factors the three external
//! systems behind traits:
//!
//! - [`ContainerdHandle`] — resolves `pod_uid` → `PodResolution` and
//!   computes the rootfs delta. The production impl
//!   ([`ContainerdClient`]) wraps `containerd_client::Client`; the test
//!   double [`fake::FakeContainerd`] returns canned values.
//! - [`EnvdStateClient`] — fetches the envd `/__state` JSON. Production
//!   ([`HttpEnvdProbe`]) issues a one-shot HTTP/1.1 GET over a raw
//!   `tokio::net::TcpStream` with `SO_LINGER(0)` so the connection
//!   aborts via TCP RST (see the type's docs for why a FIN close
//!   doesn't work); tests use [`fake::FakeEnvdState`].
//! - The `runsc` binary path is configurable via
//!   [`GvisorAdapter::with_runsc_binary`]. Tests put a shell-script stub
//!   on `PATH` and leave the default `runsc` name in place; the lookup
//!   is by `$PATH` so the stub wins.
//!
//! This mirrors the [`crate::s3::BlobStore`] pattern T6 established for
//! the S3 sink.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use containerd_client::services::v1::snapshots::MountsRequest;
use containerd_client::services::v1::{
    Container, GetContainerRequest, GetRequest as TaskGetRequest,
};
use containerd_client::types::Mount;
use containerd_client::{with_namespace, Client as ContainerdGrpcClient};
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client as KubeClient};
use sandbawks_types::runtime::{
    PodRef, RuntimeAdapter, RuntimeError, SnapshotManifest, ENVD_RELOCATE_TARGET_ENV,
};
use sandbawks_types::snapshot::{sha256_hex, SnapshotPart, SnapshotSink, SnapshotSource};
use sandbawks_types::{host_port, Sandbox, SandboxPhase, STAGED_MARKER_FILENAME};
use serde::{Deserialize, Serialize};
use tonic::Request;

// `verify_envd_digest`'s observability: the determinism-check counter, its
// outcome enum, and the caller `path` label (issues #270 / #277).
use crate::envd_digest_metrics::{record_check, CheckPath, DigestCheckOutcome};

/// containerd's CRI plugin scopes all Kubernetes containers under the
/// `k8s.io` namespace. Containers, Snapshotter, Tasks queries that target
/// kubelet-managed workloads must pass this metadata header (the
/// `with_namespace!` macro injects it).
const K8S_NAMESPACE: &str = "k8s.io";

/// Label kubelet sets on every Pod whose owning Sandbox CR is named.
/// Populated by `PodAdapter::create_pod` so the per-node worker can
/// look up the Sandbox name from the Pod alone. Sourced from the shared
/// [`sandbawks_types::SANDBOX_NAME_LABEL`] contract so the key the
/// controller stamps and the key this worker reads cannot drift.
const SANDBOX_LABEL: &str = sandbawks_types::SANDBOX_NAME_LABEL;

/// Default `runsc` binary name. Overridden by
/// [`GvisorAdapter::with_runsc_binary`] in unit tests so a stubbed
/// script on `PATH` (or an absolute path to one) drives the test
/// runs instead of a real gVisor install.
pub const DEFAULT_RUNSC_BIN: &str = "runsc";

/// envd gRPC port. Matches the Plan 03 envd binary's `--listen`
/// default; gRPC is envd's primary RPC surface. The resume path's
/// TCP-bind wait deliberately does *not* probe this port — a restored
/// process answers here before its Tokio accept loop is scheduled, a
/// false-ready signal — so readiness gates on [`ENVD_HTTP_PORT`]
/// instead (issue #533, Bug 2; see the `envd_port` field).
pub const ENVD_GRPC_PORT: u16 = 49983;

/// envd HTTP port for the `/__state` route. Matches the Plan 03 envd
/// binary's `--http-listen` default. The pause path's pre-checkpoint
/// state probe targets this port (issue #170 — second-listener design
/// over h2c sniffing for simplicity).
pub const ENVD_HTTP_PORT: u16 = 49984;

/// How long [`GvisorAdapter::pause`] waits after fetching envd state
/// before invoking `runsc checkpoint`. The probe closes its TCP
/// connection with `SO_LINGER(0)` (RST), so the sandbox-side endpoint
/// transitions to `CLOSED` as soon as the RST arrives — well under a
/// millisecond inside a kind cluster. The wait covers the RST in-
/// flight window and any scheduling jitter on a CPU-saturated CI
/// runner; 250 ms is generous without being noticeable on the human
/// pause-latency budget.
pub const POST_PROBE_TCP_DRAIN: std::time::Duration = std::time::Duration::from_millis(250);

/// Lightweight stopwatch for emitting a single structured per-phase timing
/// line from the snapshot pipeline.
///
/// Each [`mark`](PhaseTimer::mark) records the wall-clock milliseconds since
/// the previous mark (or since [`start`](PhaseTimer::start) for the first),
/// keyed by a static phase name. [`rendered`](PhaseTimer::rendered) joins
/// them into a `name=ms name=ms …` string so a pause/resume emits **one**
/// `… phases="checkpoint=4203 compress=1100 upload_memory=19847 …"` line
/// rather than a flurry of per-step logs — greppable from
/// `kubectl logs ds/sandbawks-node` and awk-able for aggregation.
///
/// Diagnostic only: it never influences control flow, and the lone cost on
/// the hot path is a panic-free `Instant::now()` per mark. Because each mark
/// is "time since the previous mark", call them in pipeline order right after
/// the phase they name completes; an early `?` return simply skips the final
/// emit (we only report fully-successful runs, which is what the perf
/// breakdown is about).
struct PhaseTimer {
    start: std::time::Instant,
    last: std::time::Instant,
    phases: Vec<(&'static str, u64)>,
}

impl PhaseTimer {
    fn start() -> Self {
        let now = std::time::Instant::now();
        Self {
            start: now,
            last: now,
            phases: Vec::new(),
        }
    }

    /// Record the elapsed time since the previous mark under `name`.
    fn mark(&mut self, name: &'static str) {
        let now = std::time::Instant::now();
        self.phases
            .push((name, now.duration_since(self.last).as_millis() as u64));
        self.last = now;
    }

    /// Render the recorded phases as space-joined `name=ms` pairs.
    fn rendered(&self) -> String {
        self.phases
            .iter()
            .map(|(name, ms)| format!("{name}={ms}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Total wall-clock milliseconds since [`start`](PhaseTimer::start).
    fn total_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Record each phase's elapsed time into the `node.snapshot.phase.duration`
    /// histogram (#373) — the metric counterpart of the structured `phases=…`
    /// log line. Call once, alongside the emit, on a fully-successful pipeline.
    fn record_metrics(&self) {
        crate::snapshot_phase_metrics::record_phases(&self.phases);
    }
}

/// How thoroughly the pause path re-proves the just-uploaded snapshot is
/// restorable before the controller reclaims the source (issue #235).
///
/// Ordered weakest → strongest. The GiB bench showed the strong `Full` level
/// (re-download + decompress the whole bundle) is ~40% of a pause, so the
/// **default is [`Head`](ValidateLevel::Head)** — confirm objects exist and
/// are the right size without moving bytes. This is a deliberate relaxation of
/// the original fail-closed default; raise it via `SANDBAWKS_S3_VALIDATE_LEVEL`
/// where stronger guarantees are worth the latency.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ValidateLevel {
    /// Skip the post-upload check entirely (trust the write path).
    Off,
    /// Default. HEAD each object + check sizes (and the index/manifest
    /// identity); no bodies transferred, no decompress. Catches missing /
    /// truncated chunks and a crossed bundle, but not in-chunk byte
    /// corruption.
    Head,
    /// Re-download every part and verify per-chunk + per-part sha256; no
    /// decompress / lay-out. Adds byte-integrity over `Head`.
    Sha,
    /// Re-download, sha-verify, decompress + un-tar into a scratch dir, and
    /// assert the `*.img` checkpoint postconditions (#232). Strongest; the
    /// original behaviour.
    Full,
}

impl ValidateLevel {
    /// Stable, lowercase label for the `level` metric attribute (#373).
    ///
    /// Free `&'static str` mapping (rather than `Debug`) so the
    /// `node.validate.outcome_total{level}` series stays a closed, bounded set.
    fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Head => "head",
            Self::Sha => "sha",
            Self::Full => "full",
        }
    }
}

/// Parse a validate-level string; unknown / empty → the [`ValidateLevel::Head`]
/// default. Shared by the env reader and [`GvisorAdapter::with_validate_level`].
fn parse_validate_level(s: &str) -> ValidateLevel {
    match s.trim().to_ascii_lowercase().as_str() {
        "off" | "none" => ValidateLevel::Off,
        "sha" | "sha256" => ValidateLevel::Sha,
        "full" => ValidateLevel::Full,
        // "" (unset) and any "head"/"head+size"/unknown → the default.
        _ => ValidateLevel::Head,
    }
}

/// Effective pause-validate level, read once from `SANDBAWKS_S3_VALIDATE_LEVEL`
/// (default [`ValidateLevel::Head`]). Used as the adapter's construction-time
/// default; tests pin a level via [`GvisorAdapter::with_validate_level`].
fn effective_validate_level() -> ValidateLevel {
    static CELL: std::sync::OnceLock<ValidateLevel> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        parse_validate_level(&std::env::var("SANDBAWKS_S3_VALIDATE_LEVEL").unwrap_or_default())
    })
}

/// runsc state root. The containerd `io.containerd.runsc.v1` shim
/// keeps per-container state directories under this path; without
/// `--root` runsc defaults to `/var/run/runsc` and reports
/// `loading container: file does not exist`.
///
/// The `k8s.io` suffix is the containerd namespace the CRI plugin
/// scopes Kubernetes containers under; the shim composes the root
/// from `/run/containerd/runsc/` + containerd-namespace.
pub const DEFAULT_RUNSC_ROOT: &str = "/run/containerd/runsc/k8s.io";

/// Separator for the `SANDBAWKS_RUNSC_ROOT_FALLBACKS` env list, mirroring
/// the convention for `$PATH`-style filesystem-path lists. Each entry is an
/// additional in-container runsc `--root` candidate
/// [`GvisorAdapter::resolve_runsc_root`] probes after the primary
/// [`DEFAULT_RUNSC_ROOT`] / `SANDBAWKS_RUNSC_ROOT`.
pub const RUNSC_ROOT_FALLBACKS_SEPARATOR: char = ':';

/// Root under which [`RuntimeAdapter::stage_snapshot`] materializes a
/// snapshot bundle for the containerd-shim restore path (Plan 07).
///
/// Distinct from [`GvisorAdapter`]'s `work_root` (which holds the
/// transient pause/resume work-dirs): the staging dir is *source-only*
/// input the shim's memory `Restore` reads directly off the host
/// filesystem (the staged `*.img` checkpoint files), so the DaemonSet
/// hostPath-mounts this path and the resume Pod is scheduled onto the
/// same node. Tests override via [`GvisorAdapter::with_staging_root`] to
/// point at a `tempfile::tempdir()`.
///
/// **Two call sites read this in lockstep (Plan 08 B2):** the
/// [`GvisorAdapter`] stages bundles under it, and the node binary's
/// [`run_staging_sweeper`] reclaims them from it. The binary's single
/// `SANDBAWKS_STAGING_ROOT` flag (issue #512) threads one resolved value into
/// *both* the adapter's [`GvisorAdapter::with_staging_root`] and the sweeper,
/// so the two can never desync; this constant is only its default. Helm sources
/// that flag, the DaemonSet `mountPath`, and the controller's
/// `SANDBAWKS_RESTORE_STAGING_ROOT` annotation from one `node.hostPathStagingCache`
/// value, so the three agree by construction (see the `sandbawks.stagingRoot`
/// chart helper).
pub const DEFAULT_STAGING_ROOT: &str = "/var/lib/sandbawks/staging";

/// Absolute path of the node-staged `envd` binary as the node worker
/// sees it (issue #270).
///
/// The `sandbawks-node` DaemonSet's `stage-envd` init container copies
/// the cluster-wide `envd` binary to `<node.hostPathEnvd>/envd` on every
/// node; sandbox Pods mount that host directory read-only and exec the
/// binary (see `ENVD_STAGE_HOSTPATH` in the controller's `pod_adapter`).
/// The worker container hostPath-mounts the same directory at the same
/// path so it can hash the binary at pause time (to record
/// [`SnapshotManifest::envd_sha256`]) and verify it at restore time. Must
/// match the controller's `ENVD_STAGE_HOSTPATH` + `/envd`. Tests override
/// via [`GvisorAdapter::with_envd_bin_path`] to point at a temp file.
pub const DEFAULT_ENVD_BIN_PATH: &str = "/var/lib/sandbawks/envd/envd";

/// Resolved metadata about a sandbox's Pod and container.
///
/// Returned by [`ContainerdHandle::resolve_pod`]. The container ID is
/// the raw containerd identifier (the `containerd://` prefix from
/// `pod.status.containerStatuses[].containerID` has been stripped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodResolution {
    /// Containerd container ID (no `containerd://` prefix).
    pub container_id: String,
    /// Pod IP address, used to reach envd at
    /// `http://<pod_ip>:49983/__state`.
    pub pod_ip: String,
    /// The Sandbox CR name this Pod belongs to. Becomes
    /// [`SnapshotManifest::sandbox_id`].
    pub sandbox_id: String,
}

/// External-system bridge to containerd + the kube API.
///
/// Plan 05 T7 only declares the trait shape and a fake; the production
/// impl ([`ContainerdClient`]) is a thin wrapper that holds a
/// `containerd_client::Client` and returns
/// [`RuntimeError::CheckpointFailed`] for both methods until Plan 05
/// T14 wires up the real containerd snapshotter Diff + kube lookup.
/// Decoupling the surface this way lets the test double drive the
/// pause workflow without standing up a containerd socket.
#[async_trait]
pub trait ContainerdHandle: Send + Sync {
    /// Resolve a Pod UID to its containerd container ID, pod IP, and
    /// the Sandbox name it belongs to.
    async fn resolve_pod(&self, pod_uid: &str) -> Result<PodResolution, RuntimeError>;

    /// Compute the rootfs delta for `container_id` and return it as
    /// already-tar+zstd-compressed bytes ready for
    /// [`SnapshotSink::put_part`]. The byte format is opaque to the
    /// caller; the matching read side (T8 `resume`) hands it back to
    /// the snapshotter's Apply API verbatim.
    async fn rootfs_delta(&self, container_id: &str) -> Result<Vec<u8>, RuntimeError>;

    /// Apply a previously-uploaded rootfs delta to a fresh containerd
    /// snapshot for `container_id`.
    ///
    /// `bytes` is the verbatim payload pause uploaded under
    /// `rootfs-delta.tar.zst` (opaque tar+zstd at this layer). The
    /// production impl ([`ContainerdClient`]) drives the snapshotter
    /// Apply API; the test double [`fake::FakeContainerd`] records the
    /// call so the resume test can assert byte-identity round-trip.
    ///
    /// Returns [`RuntimeError::RestoreFailed`] on snapshotter failures.
    async fn apply_rootfs_delta(
        &self,
        container_id: &str,
        bytes: Vec<u8>,
    ) -> Result<(), RuntimeError>;

    /// Enumerate the container's OCI runtime-spec mounts.
    ///
    /// The pause path (#237) classifies each writable mount as
    /// overlay-backed (capturable by the memory checkpoint) or not, and
    /// fails the pause if any writable mount escapes the overlay. The
    /// production impl ([`ContainerdClient`]) sources the mounts from the
    /// container's stored OCI spec (`Container.spec`); the test double
    /// [`fake::FakeContainerd`] returns a canned set.
    ///
    /// Returns [`RuntimeError::CheckpointFailed`] if the spec is missing
    /// or unparseable — fail-closed, since an unverifiable mount set
    /// cannot be proven safe.
    async fn container_mounts(&self, container_id: &str) -> Result<Vec<SpecMount>, RuntimeError>;

    /// The captured-envd relocate target (`SANDBAWKS_ENVD_RELOCATE_TARGET`,
    /// epic #466) read from the container's OCI `process.env`, or `None` if the
    /// var is absent.
    ///
    /// Its presence is the capture-mode marker the pause path records as
    /// [`SnapshotManifest::envd_captured`]: the controller sets this env on every
    /// relocate-mode Pod, so envd copied itself onto a rootfs path under the
    /// captured root overlay and its code pages ride in `pages.img` — the restore
    /// node need not (and must not) gate on its host-staged envd binary. A legacy
    /// Pod (older deploy, pre-relocation) has no such env and yields `None`,
    /// keeping the fail-closed host-binary guard on its snapshots.
    ///
    /// Unlike [`Self::container_mounts`] this is *not* fail-closed on a missing
    /// or unparseable spec: it degrades to `None` (legacy/no-capture), which only
    /// *retains* the host-binary determinism check rather than dropping it — the
    /// safe direction.
    async fn container_relocate_target(
        &self,
        container_id: &str,
    ) -> Result<Option<String>, RuntimeError>;
}

/// HTTP probe of envd's `/__state`, `/__quiesce`, and `/__rearm` endpoints.
#[async_trait]
pub trait EnvdStateClient: Send + Sync {
    /// GET `http://<pod_ip>:49984/__state` and return the response body
    /// bytes (JSON, opaque at this layer).
    async fn fetch_state(&self, pod_ip: &str) -> Result<Vec<u8>, RuntimeError>;

    /// POST `http://<pod_ip>:49984/__quiesce` to flush in-flight OTel spans
    /// before `runsc checkpoint`. Best-effort: the pause continues on failure.
    /// The connection closes with TCP RST (SO_LINGER(0)) so the sandbox-side
    /// endpoint reaches `CLOSED` before checkpoint inspects the TCP stack (#403).
    async fn quiesce(&self, pod_ip: &str) -> Result<(), RuntimeError>;

    /// POST `http://<pod_ip>:49984/__rearm` to re-arm envd's OTel provider
    /// after `runsc restore`. Best-effort: the resume continues on failure
    /// with degraded telemetry (#403).
    async fn rearm(&self, pod_ip: &str) -> Result<(), RuntimeError>;
}

/// Wall-clock abstraction so the work-dir timestamp can be made
/// deterministic in tests.
///
/// Production uses [`SystemClock`] (real RFC 3339 of `now()`); tests
/// inject a [`fake::FixedClock`] so the work-dir path is predictable.
pub trait Clock: Send + Sync {
    /// Return the current wall-clock time as an RFC 3339 string. The
    /// production impl uses [`chrono::Utc::now`]; tests inject a fixed
    /// string.
    fn now_rfc3339(&self) -> String;
}

/// Production [`Clock`] backed by `chrono::Utc::now`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_rfc3339(&self) -> String {
        chrono::Utc::now().to_rfc3339()
    }
}

/// Production [`ContainerdHandle`] wrapping `containerd_client::Client`
/// + a `kube::Client` for Pod-side lookups.
///
/// `resolve_pod` is kube-only: kubelet records the application
/// container's containerd ID at `pod.status.containerStatuses[0].containerID`
/// in the form `containerd://<id>`. The DaemonSet lists Pods on its own
/// node (`spec.nodeName=$NODE_NAME`) and filters client-side by
/// `metadata.uid` — kube-rs / apiserver don't support a `metadata.uid`
/// field selector for Pods.
///
/// `rootfs_delta` / `apply_rootfs_delta` are containerd-only: ask the
/// snapshotter for the container's mount options (containing a *host*-absolute
/// `upperdir=<containerd-data-root>/.../fs`), then tar+zstd the upperdir
/// (pause) or untar into it (resume). containerd runs on the host, so that
/// path is host-absolute and the data root varies by distro (stock
/// `/var/lib/containerd`, k3s `/var/lib/rancher/k3s/agent/containerd`, ...).
/// The DaemonSet mounts the host containerd data root at `mountPath ==
/// hostPath` (chart `node.hostPathContainerdData`, issue #389) so the reported
/// path resolves verbatim inside the container with no translation here.
pub struct ContainerdClient {
    grpc: ContainerdGrpcClient,
    kube: KubeClient,
    namespace: String,
    node_name: String,
}

impl ContainerdClient {
    /// Wrap an already-connected `containerd_client::Client` + an
    /// in-cluster `kube::Client`.
    ///
    /// `namespace` is the Sandbox CR namespace the DaemonSet watches
    /// (it also scopes the kube Pod LIST in `resolve_pod`);
    /// `node_name` is the kubelet-assigned node name from the downward
    /// API — used as the `spec.nodeName` field selector on the Pod
    /// LIST so each DaemonSet replica only sees its own node's Pods.
    pub fn new(
        grpc: ContainerdGrpcClient,
        kube: KubeClient,
        namespace: impl Into<String>,
        node_name: impl Into<String>,
    ) -> Self {
        Self {
            grpc,
            kube,
            namespace: namespace.into(),
            node_name: node_name.into(),
        }
    }
}

#[async_trait]
impl ContainerdHandle for ContainerdClient {
    #[tracing::instrument(skip_all, name = "resolve:node::pod", err, fields(pod_uid = %pod_uid))]
    async fn resolve_pod(&self, pod_uid: &str) -> Result<PodResolution, RuntimeError> {
        // List Pods on our node, then filter by metadata.uid client-side.
        // The apiserver only honors a small set of field selectors for
        // Pods (metadata.name, metadata.namespace, spec.nodeName,
        // status.phase) — `metadata.uid` is not one of them, so we
        // can't push the filter server-side.
        let pods: Api<Pod> = Api::namespaced(self.kube.clone(), &self.namespace);
        let lp = ListParams::default().fields(&format!("spec.nodeName={}", self.node_name));
        let list = pods
            .list(&lp)
            .await
            .map_err(|e| RuntimeError::CheckpointFailed(format!("kube list pods: {e}")))?;

        let pod = list
            .items
            .into_iter()
            .find(|p| p.metadata.uid.as_deref() == Some(pod_uid))
            .ok_or_else(|| {
                RuntimeError::CheckpointFailed(format!(
                    "no pod with uid {pod_uid} on node {}",
                    self.node_name
                ))
            })?;

        // `pod.status.containerStatuses[0]` is the application container.
        // (For pods with multiple containers the contract is undefined;
        // sandbawks Pods only ever have one in Plan 05.)
        let cstatus = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|cs| cs.first())
            .ok_or_else(|| {
                RuntimeError::CheckpointFailed(format!("pod {pod_uid} has no containerStatuses"))
            })?;
        let container_id = cstatus
            .container_id
            .as_deref()
            .and_then(|s| s.strip_prefix("containerd://"))
            .ok_or_else(|| {
                RuntimeError::CheckpointFailed(format!(
                    "pod {pod_uid}: containerID is not containerd://..."
                ))
            })?
            .to_string();

        let pod_ip = pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.clone())
            .ok_or_else(|| RuntimeError::CheckpointFailed(format!("pod {pod_uid} has no podIP")))?;

        // Sandbox name comes from the label `PodAdapter` stamps at
        // create time; fall back to `metadata.name` so a Pod without
        // the label (e.g. a manually-applied fixture) still resolves.
        let sandbox = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(SANDBOX_LABEL).cloned())
            .or_else(|| pod.metadata.name.clone())
            .ok_or_else(|| {
                RuntimeError::CheckpointFailed(format!(
                    "pod {pod_uid} has no name or sandbox label"
                ))
            })?;

        Ok(PodResolution {
            container_id,
            pod_ip,
            sandbox_id: sandbox,
        })
    }

    async fn rootfs_delta(&self, container_id: &str) -> Result<Vec<u8>, RuntimeError> {
        let upperdir = self.lookup_upperdir(container_id).await?;
        tracing::debug!(
            container = %container_id,
            upperdir = %upperdir.display(),
            "rootfs_delta: tar+zstd upperdir"
        );

        // tar + zstd is CPU + sync I/O; run on a blocking worker.
        // The upperdir may be empty (the container has written nothing
        // since boot) — that's a valid 0-entry tar, which restore
        // happily applies as a no-op.
        let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, std::io::Error> {
            let mut encoder =
                zstd::stream::write::Encoder::new(Vec::with_capacity(4 * 1024 * 1024), 3)?;
            {
                let mut builder = tar::Builder::new(&mut encoder);
                builder.follow_symlinks(false);
                builder.append_dir_all(".", &upperdir)?;
                builder.finish()?;
            }
            encoder.finish()
        })
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("tar+zstd join: {e}")))?
        .map_err(|e| RuntimeError::CheckpointFailed(format!("tar+zstd upperdir: {e}")))?;

        Ok(bytes)
    }

    async fn apply_rootfs_delta(
        &self,
        container_id: &str,
        bytes: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        let upperdir = self.lookup_upperdir(container_id).await?;
        tracing::debug!(
            container = %container_id,
            upperdir = %upperdir.display(),
            "apply_rootfs_delta: zstd+untar into upperdir"
        );

        tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
            let decoder = zstd::stream::read::Decoder::new(&bytes[..])?;
            let mut archive = tar::Archive::new(decoder);
            archive.set_overwrite(true);
            archive.set_preserve_permissions(true);
            archive.unpack(&upperdir)
        })
        .await
        .map_err(|e| RuntimeError::RestoreFailed(format!("untar join: {e}")))?
        .map_err(|e| RuntimeError::RestoreFailed(format!("untar into upperdir: {e}")))?;

        Ok(())
    }

    #[tracing::instrument(skip_all, name = "inspect:node::container_mounts", err, fields(container_id = %container_id))]
    async fn container_mounts(&self, container_id: &str) -> Result<Vec<SpecMount>, RuntimeError> {
        // containerd stores the OCI runtime spec verbatim in
        // `Container.spec` as a protobuf `Any` whose `value` is the
        // `config.json` JSON bytes — so we read the mount set straight
        // off the container record rather than the on-disk bundle.
        let spec = self
            .get_container(container_id)
            .await?
            .spec
            .ok_or_else(|| {
                RuntimeError::CheckpointFailed(format!("container {container_id} has no OCI spec"))
            })?;
        parse_oci_mounts(&spec.value)
    }

    #[tracing::instrument(skip_all, name = "inspect:node::container_relocate_target", err, fields(container_id = %container_id))]
    async fn container_relocate_target(
        &self,
        container_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        // Same `Container.spec` source as `container_mounts`, parsed for
        // `process.env` instead of `mounts`. A missing/unparseable spec
        // degrades to `None` (legacy) rather than failing the pause — see the
        // trait docs for why that's the safe direction.
        let spec = match self.get_container(container_id).await?.spec {
            Some(spec) => spec,
            None => return Ok(None),
        };
        Ok(parse_oci_relocate_target(&spec.value))
    }
}

impl ContainerdClient {
    /// Fetch the containerd `Container` record for `container_id`.
    ///
    /// Shared by [`Self::lookup_upperdir`] (which reads `snapshotter` +
    /// `snapshot_key`) and the `container_mounts` trait method (which reads
    /// the OCI `spec`). The two callers run at different points in the pause
    /// path, so this does not collapse them into a single round-trip — it
    /// removes the duplicated `Containers.Get` boilerplate and its error
    /// mapping (#241 review).
    ///
    /// `Client::containers` constructs a fresh service client per call from
    /// the inner `Channel` (itself cheaply-cloneable via tonic), so this
    /// borrows `&self` without cloning the wrapper.
    async fn get_container(&self, container_id: &str) -> Result<Container, RuntimeError> {
        self.grpc
            .containers()
            .get(with_namespace!(
                GetContainerRequest {
                    id: container_id.to_string(),
                },
                K8S_NAMESPACE
            ))
            .await
            .map_err(|e| RuntimeError::CheckpointFailed(format!("containerd Containers.Get: {e}")))?
            .into_inner()
            .container
            .ok_or_else(|| {
                RuntimeError::CheckpointFailed(format!(
                    "containerd: container {container_id} missing"
                ))
            })
    }

    /// Resolve a container's overlayfs upperdir on the host. Shared by
    /// `rootfs_delta` (read) and `apply_rootfs_delta` (write).
    ///
    /// Pipeline:
    ///   1. `Containers.Get(id)` returns `snapshotter` + `snapshot_key`.
    ///   2. `Snapshotter.Mounts(snapshotter, key)` returns a Vec<Mount>
    ///      where the overlay mount's `options` contains `upperdir=...`.
    ///   3. Parse the upperdir path out of options.
    ///
    /// Bind-mount fallback: a non-overlay snapshotter (native, btrfs)
    /// returns a single bind mount whose `source` IS the rootfs root —
    /// we treat that as the "upperdir" for symmetry. Overlayfs is the
    /// kind/kindest default and the only configuration the smoke
    /// exercises today.
    async fn lookup_upperdir(&self, container_id: &str) -> Result<PathBuf, RuntimeError> {
        let container = self.get_container(container_id).await?;
        let snapshotter = container.snapshotter;
        let snapshot_key = container.snapshot_key;
        if snapshot_key.is_empty() {
            return Err(RuntimeError::CheckpointFailed(format!(
                "container {container_id} has no snapshot_key (read-only image?)"
            )));
        }

        let mut snapshots = self.grpc.snapshots();
        let mounts = snapshots
            .mounts(with_namespace!(
                MountsRequest {
                    snapshotter,
                    key: snapshot_key,
                },
                K8S_NAMESPACE
            ))
            .await
            .map_err(|e| RuntimeError::CheckpointFailed(format!("Snapshotter.Mounts: {e}")))?
            .into_inner()
            .mounts;

        upperdir_from_mounts(&mounts).ok_or_else(|| {
            RuntimeError::CheckpointFailed(format!(
                "no upperdir / bind mount in snapshotter response for {container_id}"
            ))
        })
    }

    /// Resolve a container's host init PID via the containerd `Tasks.Get` API.
    ///
    /// For a gVisor workload this is the host-side PID of the sandbox's init
    /// process, which lives in the pod network namespace — so reading
    /// `/proc/<pid>/net/dev` for it yields the pod's `eth0` counters.
    ///
    /// Both "no task" outcomes collapse to `Ok(None)` so a container without a
    /// running task is a clean skip rather than a noisy error for the phase-2
    /// network signal: containerd answers a never-started / already-exited
    /// container with a gRPC `NotFound` status, and a torn-down task with a
    /// `pid` of `0`. Any other gRPC failure is a genuine error.
    ///
    /// Mirrors [`Self::get_container`]: `Client::tasks` builds a fresh service
    /// client from the cheaply-cloneable inner `Channel`, so this borrows
    /// `&self` without cloning the wrapper.
    async fn task_init_pid(&self, container_id: &str) -> Result<Option<u32>, RuntimeError> {
        let resp = match self
            .grpc
            .tasks()
            .get(with_namespace!(
                TaskGetRequest {
                    container_id: container_id.to_string(),
                    // Empty exec_id targets the task's init process.
                    exec_id: String::new(),
                },
                K8S_NAMESPACE
            ))
            .await
        {
            Ok(r) => r,
            // No task for this container — a clean "no PID to read", not a fault.
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => {
                return Err(RuntimeError::CheckpointFailed(format!(
                    "containerd Tasks.Get: {status}"
                )))
            }
        };
        let process = resp.into_inner().process;
        Ok(process.map(|p| p.pid).filter(|&pid| pid != 0))
    }
}

/// Bridge the containerd-backed PID lookup to the sandbox-pressure data layer's
/// runtime-agnostic [`SandboxPidResolver`] trait (#473). Maps the internal
/// [`RuntimeError`] to the trait's `String` error so [`crate::sandbox_stats`]
/// stays free of any containerd types.
#[async_trait]
impl crate::sandbox_stats::SandboxPidResolver for ContainerdClient {
    async fn init_pid(&self, container_id: &str) -> Result<Option<u32>, String> {
        self.task_init_pid(container_id)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Bridge the containerd upperdir lookup to the sandbox-pressure data layer's
/// runtime-agnostic [`crate::sandbox_stats::UpperdirResolver`] (#474). Reuses the
/// same `Containers.Get` + `Snapshotter.Mounts` path the pause/resume rootfs
/// delta uses, mapping [`RuntimeError`] to the trait's `String` error.
#[async_trait]
impl crate::sandbox_stats::UpperdirResolver for ContainerdClient {
    async fn upperdir(&self, container_id: &str) -> Result<Option<PathBuf>, String> {
        match self.lookup_upperdir(container_id).await {
            Ok(path) => Ok(Some(path)),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// Locate the overlayfs upperdir in a `Snapshotter.Mounts` response.
///
/// Pure function so unit tests can exercise the option-parsing
/// without standing up a containerd gRPC server.
fn upperdir_from_mounts(mounts: &[Mount]) -> Option<PathBuf> {
    let m = mounts
        .iter()
        .find(|m| m.r#type == "overlay" || m.r#type == "bind")?;
    for opt in &m.options {
        if let Some(path) = opt.strip_prefix("upperdir=") {
            return Some(PathBuf::from(path));
        }
    }
    // Non-overlay snapshotter: bind mount whose source is the rootfs.
    if m.r#type == "bind" {
        return Some(PathBuf::from(&m.source));
    }
    None
}

/// One entry of the container's OCI runtime spec `mounts` array, reduced
/// to the fields the pause-time overlay-backing assertion needs (#237).
///
/// Deserialized from the OCI `config.json` (containerd stores it verbatim
/// in `Container.spec`). `destination`, `type`, and `options` drive the
/// classification; `source` is carried only to enrich the fail-closed
/// error message. Any other keys are ignored by serde.
///
/// Rust note: `#[serde(rename = "type")]` maps the JSON `type` key onto a
/// non-keyword field name, since `type` is a reserved word in Rust.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SpecMount {
    /// In-container mount point (e.g. `/data`). Named in the fail-closed
    /// error so an operator can locate the offending mount.
    pub destination: String,
    /// OCI mount type (e.g. `overlay`, `bind`, `tmpfs`, `proc`). Absent
    /// in the spec → empty string, which the classifier treats as a
    /// non-capturable type (fail-closed-conservative).
    #[serde(rename = "type", default)]
    pub mount_type: String,
    /// Host-side source. For a `bind` this is the host path being exposed;
    /// it is what tells an operator *which* hostPath an offending mount
    /// leaked (e.g. distinguishing two competing binds onto the same
    /// `destination`), so it is included in the error message. Empty for
    /// sourceless mounts (`tmpfs`, `proc`, …).
    #[serde(default)]
    pub source: String,
    /// OCI mount options. The presence of `ro` marks the mount
    /// read-only; everything else is writable.
    #[serde(default)]
    pub options: Vec<String>,
}

/// Minimal projection of the OCI runtime spec: just the `mounts` array.
///
/// Deserializing into this rather than the full `oci_spec::runtime::Spec`
/// keeps the dependency surface tiny and tolerates spec fields we don't
/// model (serde ignores unknown keys by default).
#[derive(Debug, Deserialize)]
struct OciSpecMounts {
    #[serde(default)]
    mounts: Vec<SpecMount>,
}

/// Parse the `mounts` array out of an OCI runtime spec JSON document.
///
/// Pure function over the spec bytes so the parsing is unit-testable
/// without a live containerd, mirroring [`upperdir_from_mounts`].
///
/// # Errors
///
/// Returns [`RuntimeError::CheckpointFailed`] if the bytes are not the
/// expected OCI spec JSON — a malformed spec means we cannot prove the
/// mount set is safe, so we fail-closed.
fn parse_oci_mounts(spec_json: &[u8]) -> Result<Vec<SpecMount>, RuntimeError> {
    let spec: OciSpecMounts = serde_json::from_slice(spec_json).map_err(|e| {
        RuntimeError::CheckpointFailed(format!("parse OCI runtime spec mounts: {e}"))
    })?;
    Ok(spec.mounts)
}

/// Minimal projection of the OCI runtime spec's `process.env` for relocate
/// detection (epic #466). Mirrors [`OciSpecMounts`]; serde ignores the rest of
/// the (large) spec. `process` is optional so a spec without it deserializes.
#[derive(Debug, Default, Deserialize)]
struct OciSpecProcess {
    #[serde(default)]
    process: OciProcess,
}

#[derive(Debug, Default, Deserialize)]
struct OciProcess {
    /// OCI stores env as `KEY=VALUE` strings.
    #[serde(default)]
    env: Vec<String>,
}

/// Extract the value of [`ENVD_RELOCATE_TARGET_ENV`] from an OCI spec's
/// `process.env`, or `None` if the var (or a parseable spec) is absent.
///
/// Pure function over the spec bytes, mirroring [`parse_oci_mounts`]. Unlike
/// that one it does **not** fail-closed on a malformed spec: a parse error
/// returns `None` (treated as legacy / no-capture downstream), which only keeps
/// the host-binary determinism guard rather than dropping it — the safe
/// direction. See [`ContainerdHandle::container_relocate_target`].
fn parse_oci_relocate_target(spec_json: &[u8]) -> Option<String> {
    let spec: OciSpecProcess = serde_json::from_slice(spec_json).ok()?;
    let prefix = format!("{ENVD_RELOCATE_TARGET_ENV}=");
    spec.process
        .env
        .iter()
        .find_map(|entry| entry.strip_prefix(&prefix).map(str::to_owned))
}

/// Kernel pseudo-filesystems: writable, but they carry no checkpointable
/// persistent state. The gVisor sentry (or host kernel) reconstructs them
/// on restore, so a writable mount of one of these types is not a
/// "filesystem lives outside the checkpoint" escape. Excluding them keeps
/// the [`mounts_lost_on_restore`] assertion from false-positiving on the
/// `proc` / `sysfs` / `cgroup` mounts every OCI spec carries.
const PSEUDO_FS_TYPES: &[&str] = &[
    "proc",
    "sysfs",
    "cgroup",
    "cgroup2",
    "devpts",
    "mqueue",
    "devtmpfs",
    "bpf",
    "tracefs",
    "debugfs",
    "securityfs",
    "fusectl",
    "configfs",
    "binfmt_misc",
    "pstore",
    "nsfs",
    "hugetlbfs",
];

/// Destinations of the writable bind mounts the kubelet / CRI inject into
/// every Pod (the "mount furniture"). Their contents are managed by the
/// runtime and re-materialized (or are ephemeral by contract) on the
/// *fresh* Pod a memory-only restore lands in, so losing the sandbox-side
/// copy is harmless — they are not workload state. Excluding them keeps the
/// assertion from failing every real Pod (none of these furniture binds is
/// overlay-backed).
///
/// Verified against a live gVisor Pod's OCI spec (`crictl inspect …
/// .info.runtimeSpec.mounts`): containerd injects these as `rw` `bind`
/// mounts — the networking dotfiles, the per-container termination-log, and
/// the pod-level POSIX shared-memory mount. `/dev/shm` in particular is a
/// gofer-backed `bind` (NOT a `tmpfs`), so it is not captured in `pages.img`
/// and must be exempted by destination; it is ephemeral pod-scoped shm that
/// a restored Pod re-creates empty, never workload state to preserve.
const RUNTIME_MANAGED_BIND_DESTS: &[&str] = &[
    "/etc/hosts",
    "/etc/hostname",
    "/etc/resolv.conf",
    "/dev/termination-log",
    "/dev/shm",
];

/// A mount is writable unless its OCI options explicitly include `ro`.
/// (OCI's default when neither `ro` nor `rw` is present is read-write.)
///
/// Deliberate precedence: a malformed mount carrying *both* `ro` and `rw`
/// (forbidden by OCI, but seen in the wild) is treated as read-only. That
/// is the safe direction here — misclassifying a read-only mount as
/// writable would only fail-close an extra pause, whereas the reverse would
/// let a writable escape through. Do not "fix" this to prefer `rw`.
fn mount_is_writable(m: &SpecMount) -> bool {
    !m.options.iter().any(|opt| opt == "ro")
}

/// Whether a writable mount's state survives a memory-only restore — either
/// captured by the checkpoint or re-materialized on the restored Pod.
///
/// Qualifying cases:
/// - `overlay`: under `--overlay2=root:self` the writable upper lives in
///   the sandbox MemoryFile, so it rides along in `pages.img`.
/// - `tmpfs`: gVisor's tmpfs is sentry-internal and MemoryFile-backed — the
///   *same* mechanism that lets the root overlay survive a memory-only
///   restore (#232) — so its contents are saved in `pages.img` too. This
///   refines #237's "out-of-overlay tmpfs" framing: in gVisor a tmpfs is
///   never gofer/host-backed, so it is not an escape. (A `medium: Memory`
///   emptyDir is a tmpfs and is likewise captured; a *default-medium*
///   emptyDir is a host-backed `bind` and is caught below.)
/// - a [`PSEUDO_FS_TYPES`] kernel filesystem: no persistent state to lose;
///   rebuilt by the sentry on restore.
/// - a [`RUNTIME_MANAGED_BIND_DESTS`] furniture bind: re-materialized by the
///   kubelet on the fresh restored Pod.
///
/// Everything else that is writable — a gofer-backed `bind` to a hostPath /
/// PVC / default-medium `emptyDir`, or `9p` / `virtiofs` / `nfs` — passes
/// writes through to the host and is invisible to the memory checkpoint.
fn mount_state_survives_restore(m: &SpecMount) -> bool {
    m.mount_type == "overlay"
        || m.mount_type == "tmpfs"
        || PSEUDO_FS_TYPES.contains(&m.mount_type.as_str())
        || RUNTIME_MANAGED_BIND_DESTS.contains(&m.destination.as_str())
}

/// Pure classifier (#237): the subset of `mounts` whose writes would be
/// silently lost on a memory-only restore — writable mounts that are
/// neither captured by the checkpoint (overlay / tmpfs / pseudo-fs) nor
/// runtime-managed furniture re-materialized on the restored Pod.
///
/// Factored out of [`assert_writable_mounts_overlay_backed`] so it is
/// unit-testable without constructing a `RuntimeError`, mirroring the
/// `upperdir_from_mounts` pattern.
fn mounts_lost_on_restore(mounts: &[SpecMount]) -> Vec<&SpecMount> {
    mounts
        .iter()
        .filter(|m| mount_is_writable(m) && !mount_state_survives_restore(m))
        .collect()
}

/// Fail-closed mount-set assertion (#237).
///
/// Memory-only restore reconstructs the rootfs only because, under
/// `--overlay2=root:self`, the writable upper lives in the sandbox
/// MemoryFile (→ `pages.img`). A writable mount whose state is neither
/// captured by the memory checkpoint (overlay / tmpfs / pseudo-fs) nor
/// runtime-managed furniture re-materialized on the restored Pod is a
/// gofer-backed host passthrough — invisible to the checkpoint and silently
/// lost on restore, the `--overlay2=none` failure measured in #231. See
/// [`mount_state_survives_restore`] for the full classification. This is a
/// config property knowable at pause time, so we assert it *before* the
/// checkpoint and before any upload.
///
/// # Errors
///
/// Returns [`RuntimeError::CheckpointFailed`] naming every offending mount
/// (destination + type) when any writable mount would be lost, so the
/// caller fails the SnapshotJob with the store left untouched.
fn assert_writable_mounts_overlay_backed(mounts: &[SpecMount]) -> Result<(), RuntimeError> {
    let lost = mounts_lost_on_restore(mounts);
    if lost.is_empty() {
        return Ok(());
    }
    let detail = lost
        .iter()
        .map(|m| {
            let ty = if m.mount_type.is_empty() {
                "?"
            } else {
                m.mount_type.as_str()
            };
            // Include the host source when present (bind mounts) so the
            // operator can see *which* host path leaked, not just where it
            // landed. Sourceless mounts (tmpfs/proc/…) omit it.
            if m.source.is_empty() {
                format!("{dst} (type={ty})", dst = m.destination)
            } else {
                format!(
                    "{dst} (type={ty}, source={src})",
                    dst = m.destination,
                    src = m.source
                )
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    Err(RuntimeError::CheckpointFailed(format!(
        "mount-set assertion: {n} writable mount(s) are not overlay-backed and would be \
         silently lost on memory-only restore: {detail}",
        n = lost.len(),
    )))
}

/// Production [`EnvdStateClient`].
///
/// Issues a one-shot HTTP/1.1 `GET /__state` over a raw
/// [`tokio::net::TcpStream`] (not `reqwest`) and forcibly aborts the
/// connection with a TCP RST on the way out.
///
/// ## Why not `reqwest`
///
/// `runsc checkpoint` refuses to serialise a sandbox whose TCP stack
/// has any endpoint not in the `CLOSED` state — including
/// `ESTABLISHED`, `CLOSE_WAIT`, *and* `TIME_WAIT`. The probe runs
/// inside the pre-checkpoint window, so the connection it opens
/// **must** leave the sandbox-side endpoint in `CLOSED` before
/// checkpoint fires.
///
/// A graceful FIN/ACK close always leaves one side in `TIME_WAIT` for
/// 2 × MSL (≈60s on gVisor). Whichever side initiates the close gets
/// the `TIME_WAIT`; the other side gets clean `CLOSED`. There's no
/// permutation of headers / client config that puts both sides
/// straight into `CLOSED` via FIN.
///
/// The way out is to **RST** the connection from the node side. The
/// kernel sends a TCP reset instead of a FIN; on receipt, the
/// sandbox-side endpoint transitions immediately to `CLOSED` (no
/// `CLOSE_WAIT`, no `TIME_WAIT`, no waiting on the 2-MSL timer).
/// `SO_LINGER(0)` is how userspace asks the kernel to do this on
/// `close()`. Neither `reqwest` nor `hyper`'s default `TcpConnector`
/// expose `SO_LINGER`, so we manage the socket directly.
///
/// ## Why a one-shot connection
///
/// A long-lived pooled connection has the same TCP-state problem —
/// the socket sits in `ESTABLISHED` until we close, and the same RST
/// dance applies. There's no value in pooling a connection we only
/// use once per pause.
///
/// ## Why not Unix-socket / file-based
///
/// Both are cleaner sidesteps of the TCP-state issue (no network
/// connection ⇒ nothing to checkpoint), but they need:
/// - envd-side: a UDS listener or periodic-state-write loop;
/// - node-side: an `nsenter` / overlayfs-upperdir reader.
///
/// Either is a Plan 06+ scope change; the RST trick lands the
/// `/__state` contract this PR is about with no new envd surface.
pub struct HttpEnvdProbe {
    /// Hard cap on the TCP connect + HTTP exchange. Keeps a hung
    /// envd from stalling pause indefinitely. Matches the pre-PR
    /// reqwest client's 10 s timeout.
    timeout: std::time::Duration,
}

impl Default for HttpEnvdProbe {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(10),
        }
    }
}

impl HttpEnvdProbe {
    /// Build a probe with the default 10 s timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the connect + exchange timeout. Production leaves
    /// the default; tests don't construct this type (they use
    /// [`fake::FakeEnvdState`]).
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl EnvdStateClient for HttpEnvdProbe {
    /// `GET http://<pod_ip>:49984/__state` → response body bytes.
    ///
    /// Implementation walks the request/response by hand over a raw
    /// `tokio::net::TcpStream` so we can set `SO_LINGER(0)` and abort
    /// the connection with a TCP RST when dropping the stream. See
    /// the [`HttpEnvdProbe`] type-level docs for the rationale; the
    /// short version is "any FIN-based close leaves TIME_WAIT
    /// somewhere, and TIME_WAIT fails `runsc checkpoint`."
    ///
    /// `#[instrument]` gives the probe a named span whose context is injected
    /// into the outbound request, so envd's `/__state` span continues this
    /// trace (issue #298). `skip_all` keeps `self` out of attributes; `pod_ip`
    /// is recorded explicitly as the one useful, low-cardinality field.
    #[tracing::instrument(skip_all, name = "probe:node::fetch_state", fields(pod_ip = %pod_ip))]
    async fn fetch_state(&self, pod_ip: &str) -> Result<Vec<u8>, RuntimeError> {
        // `host_port` brackets IPv6 literals so the authority is valid
        // for both `TcpStream::connect` and the HTTP `Host` header.
        let addr = host_port(pod_ip, ENVD_HTTP_PORT);
        let fetched = tokio::time::timeout(self.timeout, fetch_state_inner(&addr))
            .await
            .map_err(|_| {
                RuntimeError::CheckpointFailed(format!(
                    "envd /__state timed out after {:?}",
                    self.timeout,
                ))
            })??;
        Ok(fetched)
    }

    /// POST `/__quiesce` with RST close (SO_LINGER(0)) so the sandbox-side
    /// TCP endpoint reaches `CLOSED` before `runsc checkpoint` inspects the
    /// stack. Timeout-bounded by the same [`HttpEnvdProbe::timeout`] as the
    /// state probe.
    #[tracing::instrument(skip_all, name = "probe:node::quiesce", fields(pod_ip = %pod_ip))]
    async fn quiesce(&self, pod_ip: &str) -> Result<(), RuntimeError> {
        let addr = host_port(pod_ip, ENVD_HTTP_PORT);
        tokio::time::timeout(self.timeout, post_endpoint_inner(&addr, "/__quiesce", true))
            .await
            .map_err(|_| {
                RuntimeError::CheckpointFailed(format!(
                    "envd /__quiesce timed out after {:?}",
                    self.timeout,
                ))
            })?
    }

    /// POST `/__rearm` to restore the OTel provider after `runsc restore`.
    /// Timeout-bounded by the same [`HttpEnvdProbe::timeout`] as the state
    /// probe. No RST required: the restore is already complete when this runs.
    #[tracing::instrument(skip_all, name = "probe:node::rearm", fields(pod_ip = %pod_ip))]
    async fn rearm(&self, pod_ip: &str) -> Result<(), RuntimeError> {
        let addr = host_port(pod_ip, ENVD_HTTP_PORT);
        tokio::time::timeout(self.timeout, post_endpoint_inner(&addr, "/__rearm", false))
            .await
            .map_err(|_| {
                RuntimeError::RestoreFailed(format!(
                    "envd /__rearm timed out after {:?}",
                    self.timeout,
                ))
            })?
            .map_err(|e| match e {
                // Rearm is on the restore path; remap the error variant so
                // any caller that pattern-matches on it sees RestoreFailed.
                RuntimeError::CheckpointFailed(m) => RuntimeError::RestoreFailed(m),
                other => other,
            })
    }
}

/// The actual probe sequence, split out so the outer
/// [`HttpEnvdProbe::fetch_state`] can wrap the whole thing in a single
/// `tokio::time::timeout` and surface a clean error message.
///
/// Steps:
/// 1. TCP-connect to envd's HTTP listener.
/// 2. `SO_LINGER(0)` — load-bearing for the RST close, see the
///    type-level docs.
/// 3. Write a minimal HTTP/1.1 GET. No `Connection: close` header:
///    that would invite axum to FIN-close from the server side,
///    which is exactly the TIME_WAIT-on-sandbox scenario we're
///    avoiding. We control the close.
/// 4. Read headers until `\r\n\r\n`; parse status line + Content-
///    Length; read exactly that many body bytes. axum's `Json`
///    extractor always emits Content-Length, so we don't need to
///    cover chunked transfer.
/// 5. Drop the stream → kernel sends RST → sandbox-side endpoint
///    immediately CLOSED.
async fn fetch_state_inner(addr: &str) -> Result<Vec<u8>, RuntimeError> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("connect: {e}")))?;
    // `SO_LINGER(0)` makes the eventual close send a TCP RST instead
    // of a FIN, immediately closing the sandbox-side endpoint with
    // no TIME_WAIT — see the [`HttpEnvdProbe`] type-level docs.
    // socket2 is the stable path: std's `set_linger` is still
    // unstable (rust-lang/rust#88494) and tokio's `set_linger` is
    // deprecated.
    socket2::SockRef::from(&stream)
        .set_linger(Some(std::time::Duration::ZERO))
        .map_err(|e| RuntimeError::CheckpointFailed(format!("set SO_LINGER: {e}")))?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Inject the active span's W3C `traceparent` so envd's `/__state` handler
    // can parent its span to ours and the probe continues the node's pause
    // trace instead of opening a disconnected one (issue #298). The probe
    // hand-builds its request over a raw socket (it needs `SO_LINGER(0)`, which
    // `reqwest`/`hyper` don't expose — see the type docs), so we serialise the
    // context into a `MapCarrier` and splice each emitted header in. Outside any
    // span the carrier is empty and this is a no-op.
    let mut carrier = sandbawks_observability::MapCarrier::new();
    sandbawks_observability::inject_current(&mut carrier);
    let trace_headers: String = carrier
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();

    let request = format!(
        "GET /__state HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Accept: application/json\r\n\
         User-Agent: sandbawks-node/{ver}\r\n\
         {trace_headers}\
         \r\n",
        ver = env!("CARGO_PKG_VERSION"),
    );
    write_half
        .write_all(request.as_bytes())
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("write request: {e}")))?;

    // Read headers line-by-line until the blank-line terminator.
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("read status: {e}")))?;
    if !status_line.contains(" 200 ") {
        return Err(RuntimeError::CheckpointFailed(format!(
            "envd /__state returned non-200 status: {}",
            status_line.trim_end()
        )));
    }
    let mut content_length: Option<usize> = None;
    loop {
        let mut line = String::new();
        let n = reader
            .read_line(&mut line)
            .await
            .map_err(|e| RuntimeError::CheckpointFailed(format!("read header: {e}")))?;
        if n == 0 {
            return Err(RuntimeError::CheckpointFailed(
                "EOF mid-headers".to_string(),
            ));
        }
        if line == "\r\n" {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse::<usize>().ok();
            }
        }
    }
    let len = content_length.ok_or_else(|| {
        RuntimeError::CheckpointFailed("envd /__state returned no Content-Length".to_string())
    })?;

    let mut body = vec![0u8; len];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("read body: {e}")))?;

    // Explicit drop ordering: kill the reader first (no more data
    // coming our way), then the writer. The underlying `TcpStream`
    // closes when *both* halves are dropped; SO_LINGER(0) makes that
    // close send a RST rather than a FIN.
    drop(reader);
    drop(write_half);

    Ok(body)
}

/// POST `path` to `addr` and discard the response body.
///
/// Shared by [`HttpEnvdProbe::quiesce`] and [`HttpEnvdProbe::rearm`]. The
/// `use_rst` flag controls whether `SO_LINGER(0)` is set on the socket:
///
/// - **quiesce** (before `runsc checkpoint`): `use_rst = true`. The same
///   reasoning as [`fetch_state_inner`] applies — any TCP endpoint not in
///   `CLOSED` before the checkpoint fails `runsc checkpoint`. RST-closing
///   the quiesce connection ensures the sandbox-side endpoint reaches
///   `CLOSED` immediately.
/// - **rearm** (after `runsc restore` / `wait_for_tcp_bind`): `use_rst =
///   false`. Close without setting `SO_LINGER(0)`. Since only the status
///   line is read, any unread response body bytes still in the receive
///   queue make the close emit RST rather than FIN — acceptable here
///   because the restore is already complete and the sandbox-side endpoint
///   no longer needs to reach `CLOSED` before a checkpoint.
///
/// Only the HTTP status line is parsed; a non-2xx response is returned as
/// an error so the caller can log it. The response body is not consumed.
async fn post_endpoint_inner(addr: &str, path: &str, use_rst: bool) -> Result<(), RuntimeError> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("connect: {e}")))?;
    if use_rst {
        // RST-close: forces the sandbox-side endpoint to CLOSED immediately
        // rather than lingering in TIME_WAIT after a graceful FIN (see
        // [`fetch_state_inner`] for the full rationale).
        socket2::SockRef::from(&stream)
            .set_linger(Some(std::time::Duration::ZERO))
            .map_err(|e| RuntimeError::CheckpointFailed(format!("set SO_LINGER: {e}")))?;
    }

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Inject the active span's W3C traceparent so envd can parent its handler
    // span to the node's pause/resume trace (mirrors the /__state probe).
    let mut carrier = sandbawks_observability::MapCarrier::new();
    sandbawks_observability::inject_current(&mut carrier);
    let trace_headers: String = carrier
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();

    let request = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Length: 0\r\n\
         User-Agent: sandbawks-node/{ver}\r\n\
         {trace_headers}\
         \r\n",
        ver = env!("CARGO_PKG_VERSION"),
    );
    write_half
        .write_all(request.as_bytes())
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("write POST {path}: {e}")))?;

    // Read the status line only; we don't need the response body.
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("read status: {e}")))?;
    if status_line.is_empty() {
        return Err(RuntimeError::CheckpointFailed(format!(
            "envd {path}: empty response"
        )));
    }
    // Accept any 2xx; anything else is a best-effort signal the caller logs.
    let is_2xx = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .is_some_and(|code| (200..300).contains(&code));
    if !is_2xx {
        return Err(RuntimeError::CheckpointFailed(format!(
            "envd {path} returned non-2xx status: {}",
            status_line.trim_end()
        )));
    }

    drop(reader);
    drop(write_half);
    Ok(())
}

/// gVisor (`runsc`) backed [`RuntimeAdapter`] impl.
///
/// Construct via [`GvisorAdapter::new`] with injectable trait objects;
/// the binary's main entry uses [`ContainerdClient`] +
/// [`ReqwestEnvdState`]. Tests substitute the [`fake`] doubles.
pub struct GvisorAdapter {
    containerd: Arc<dyn ContainerdHandle>,
    envd: Arc<dyn EnvdStateClient>,
    clock: Arc<dyn Clock>,
    work_root: PathBuf,
    runsc_binary: PathBuf,
    /// In-container path to the **host** `runsc` binary — the one containerd
    /// uses as the runtime handler to actually run and restore sandboxes, and
    /// the one Capability A's reconciler reads to advertise the
    /// [`sandbawks_types::RUNSC_VERSION_LABEL`] node label (#545/#546). Used
    /// only by [`read_runsc_release`](Self::read_runsc_release) to stamp the
    /// snapshot's `runsc_version`, so the recorded version and the node label
    /// derive from the *same* binary and the restore-placement gate's
    /// `In [version]` match is apples-to-apples even on a node where the
    /// host runsc has drifted from the image-bundled `runsc_binary` (the
    /// `BundledMismatch` state #546's convergence exists to close).
    ///
    /// `None` falls back to [`Self::runsc_binary`] — correct for unit tests
    /// (which point both at one stub) and standalone runs, but production
    /// MUST set it via [`Self::with_host_runsc_binary`] so a drifted node
    /// records its host version, not the bundled one.
    host_runsc_binary: Option<PathBuf>,
    /// Passed as `--root` to every `runsc` invocation so the binary
    /// finds the per-container state directory the containerd shim
    /// created at container-start time. See [`DEFAULT_RUNSC_ROOT`].
    /// Tests can override via [`GvisorAdapter::with_runsc_root`] to
    /// point at a `tempfile::tempdir()`.
    runsc_root: PathBuf,
    /// Additional runsc `--root` candidates probed (in order, after
    /// [`runsc_root`](Self::runsc_root)) by
    /// [`resolve_runsc_root`](Self::resolve_runsc_root) when the primary root
    /// does not hold the target container. Populated per-environment from
    /// `SANDBAWKS_RUNSC_ROOT_FALLBACKS` / `node.runscRootFallbacks` so a
    /// cluster whose gVisor shim keeps runsc state on a non-default subtree
    /// (e.g. k3s/RKE2) can list the alternate in-container path without any
    /// distro knowledge baked into the binary (#366). Empty by default, which
    /// preserves the single-root behavior. Tests set it via
    /// [`GvisorAdapter::with_extra_runsc_roots`].
    extra_runsc_roots: Vec<PathBuf>,
    /// In-container directories searched, recursively and bounded, by
    /// [`resolve_runsc_root`](Self::resolve_runsc_root) to *discover* the
    /// runsc state root when no explicit candidate
    /// ([`runsc_root`](Self::runsc_root) / [`extra_runsc_roots`](Self::extra_runsc_roots))
    /// holds the target container (#377).
    ///
    /// This is the distro-agnostic mechanism: instead of an operator having
    /// to know and list the exact `runsc/<namespace>` suffix their shim uses,
    /// they declare the neutral fact "here is this cluster's containerd
    /// run/state root" (mounted into the DaemonSet) and the node finds the
    /// `<root>/<container_id>` state dir wherever the shim placed it. The
    /// binary carries no per-distro path. Populated per-environment from
    /// `SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS` / `node.runscRootSearchDirs`; empty
    /// by default, which disables discovery and preserves the explicit-only
    /// behavior. Tests set it via
    /// [`GvisorAdapter::with_runsc_root_search_dirs`].
    runsc_root_search_dirs: Vec<PathBuf>,
    /// Root under which [`RuntimeAdapter::stage_snapshot`] lays out a
    /// snapshot bundle as `<staging_root>/<sandbox-id>/<job-segment>/`.
    /// Defaults to [`DEFAULT_STAGING_ROOT`]; tests override via
    /// [`GvisorAdapter::with_staging_root`].
    staging_root: PathBuf,
    /// Absolute path to the node-staged `envd` binary (issue #270).
    /// Hashed at pause to record [`SnapshotManifest::envd_sha256`] and
    /// re-hashed at restore to fail closed on a drifted binary. Defaults
    /// to [`DEFAULT_ENVD_BIN_PATH`]; tests override via
    /// [`GvisorAdapter::with_envd_bin_path`].
    envd_bin_path: PathBuf,
    /// TCP port the resume-side wait probes on `<pod_ip>:<port>`.
    /// Defaults to [`ENVD_HTTP_PORT`] — readiness must reflect the HTTP
    /// server that serves `/__rearm` and `/__state`, which the resume
    /// path drives next. The gRPC port ([`ENVD_GRPC_PORT`]) is *not*
    /// used here: a restored process completes the TCP handshake and
    /// emits HTTP/2 frames on the gRPC port before its starved Tokio
    /// accept loop is scheduled, so probing it returns a false-ready
    /// signal while the HTTP server is still unresponsive (issue #533,
    /// Bug 2). Tests override via [`GvisorAdapter::with_envd_port`] so
    /// they can bind a local listener on an ephemeral port instead of
    /// fighting for 49984.
    envd_port: u16,
    /// Post-probe TCP drain wait inserted between the envd state
    /// fetch and `runsc checkpoint`. See [`POST_PROBE_TCP_DRAIN`] for
    /// the rationale; tests inject [`std::time::Duration::ZERO`] via
    /// [`GvisorAdapter::with_post_probe_drain`] because their
    /// [`fake::FakeEnvdState`] doesn't open a real TCP connection.
    post_probe_drain: std::time::Duration,
    /// Node resource introspection for the #238 fail-closed headroom
    /// guard. Defaults to [`crate::headroom::SysNodeStats`] (`statvfs` +
    /// `/proc/meminfo`); tests inject a fixed-reading stub via
    /// [`GvisorAdapter::with_node_stats`].
    node_stats: Arc<dyn crate::headroom::NodeStats>,
    /// Fail-closed resource-headroom policy (#238). Defaults to
    /// [`crate::headroom::HeadroomPolicy::default`]; the node binary
    /// overrides it from flags/env via
    /// [`GvisorAdapter::with_headroom_policy`].
    headroom: crate::headroom::HeadroomPolicy,

    /// Pause restorability validate level (#235). Defaults from
    /// `SANDBAWKS_S3_VALIDATE_LEVEL` at construction (binary default
    /// [`ValidateLevel::Head`]); tests override it via
    /// [`GvisorAdapter::with_validate_level`].
    validate_level: ValidateLevel,
}

impl GvisorAdapter {
    /// Build an adapter from injected dependencies.
    ///
    /// `work_root` is the parent directory under which per-sandbox
    /// per-pause work-dirs are staged (e.g.
    /// `/var/lib/sandbawks/snapshots/`). The DaemonSet pod mounts a
    /// hostPath volume here in production.
    pub fn new(
        containerd: Arc<dyn ContainerdHandle>,
        envd: Arc<dyn EnvdStateClient>,
        work_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            containerd,
            envd,
            clock: Arc::new(SystemClock),
            work_root: work_root.into(),
            runsc_binary: PathBuf::from(DEFAULT_RUNSC_BIN),
            host_runsc_binary: None,
            runsc_root: PathBuf::from(DEFAULT_RUNSC_ROOT),
            extra_runsc_roots: Vec::new(),
            runsc_root_search_dirs: Vec::new(),
            staging_root: PathBuf::from(DEFAULT_STAGING_ROOT),
            envd_bin_path: PathBuf::from(DEFAULT_ENVD_BIN_PATH),
            // Probe the HTTP port (49984), not gRPC (49983): the HTTP server is
            // what serves `/__rearm` and `/__state` after restore, and the gRPC
            // port answers before the Tokio accept loop is scheduled — a
            // false-ready signal (issue #533, Bug 2).
            envd_port: ENVD_HTTP_PORT,
            post_probe_drain: POST_PROBE_TCP_DRAIN,
            node_stats: Arc::new(crate::headroom::SysNodeStats),
            headroom: crate::headroom::HeadroomPolicy::default(),
            validate_level: effective_validate_level(),
        }
    }

    /// Override the pause restorability validate level (#235).
    ///
    /// Accepts `off` | `head` | `sha` | `full` (case-insensitive; unknown →
    /// the `head` default). Production sets it from `SANDBAWKS_S3_VALIDATE_LEVEL`
    /// via `new()`; tests use this to pin a specific level.
    pub fn with_validate_level(mut self, level: &str) -> Self {
        self.validate_level = parse_validate_level(level);
        self
    }

    /// Override the `runsc` binary path/name.
    ///
    /// Tests point this at a shell-script stub written to a
    /// `tempfile::tempdir()` so `tokio::process::Command` invokes the
    /// stub rather than searching `$PATH`. Production deploys leave
    /// the default `runsc` in place and rely on `$PATH`.
    pub fn with_runsc_binary(mut self, path: impl Into<PathBuf>) -> Self {
        self.runsc_binary = path.into();
        self
    }

    /// Override the **host** `runsc` binary path used to stamp the snapshot's
    /// `runsc_version` (#545). See [`Self::host_runsc_binary`]. Production wires
    /// this to the host hostPath mount (e.g. `/host/usr/local/bin/runsc`) so the
    /// recorded checkpoint version matches the host-derived node label the
    /// restore-placement gate keys off; tests point it at a stub.
    pub fn with_host_runsc_binary(mut self, path: impl Into<PathBuf>) -> Self {
        self.host_runsc_binary = Some(path.into());
        self
    }

    /// Override the `--root` argument passed to every `runsc`
    /// invocation. See [`DEFAULT_RUNSC_ROOT`] for the production
    /// default; tests point this at a `tempfile::tempdir()` so the
    /// stubbed runsc finds whatever the test wants it to find.
    pub fn with_runsc_root(mut self, path: impl Into<PathBuf>) -> Self {
        self.runsc_root = path.into();
        self
    }

    /// Set additional runsc `--root` candidates probed after the primary
    /// [`with_runsc_root`](Self::with_runsc_root) value (#366).
    ///
    /// Production fills this from `SANDBAWKS_RUNSC_ROOT_FALLBACKS` /
    /// `node.runscRootFallbacks` so each environment lists the in-container
    /// path(s) where its gVisor shim actually keeps runsc state, instead of
    /// the binary assuming any one distro's layout. Tests point entries at
    /// `tempfile::tempdir()`s to exercise the probe's selection order.
    pub fn with_extra_runsc_roots(
        mut self,
        roots: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        self.extra_runsc_roots = roots.into_iter().map(Into::into).collect();
        self
    }

    /// Set the in-container directories searched to *discover* the runsc state
    /// root when no explicit candidate holds the target container (#377).
    ///
    /// Production fills this from `SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS` /
    /// `node.runscRootSearchDirs` — typically the cluster's mounted containerd
    /// run/state root — so the node discovers where the gVisor shim actually
    /// keeps per-container state instead of the operator hand-listing the exact
    /// `runsc/<namespace>` path. Tests point entries at `tempfile::tempdir()`s
    /// laid out like a real shim tree.
    pub fn with_runsc_root_search_dirs(
        mut self,
        dirs: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        self.runsc_root_search_dirs = dirs.into_iter().map(Into::into).collect();
        self
    }

    /// Override the staging root [`RuntimeAdapter::stage_snapshot`]
    /// materializes bundles under. See [`DEFAULT_STAGING_ROOT`] for the
    /// production default; tests point this at a `tempfile::tempdir()`
    /// so the staged bundle lands somewhere writable.
    pub fn with_staging_root(mut self, path: impl Into<PathBuf>) -> Self {
        self.staging_root = path.into();
        self
    }

    /// Override the wall-clock provider. Used by unit tests to pin the
    /// work-dir timestamp so the resulting snapshot key is predictable.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Override the node-staged envd binary path (issue #270). Production
    /// leaves [`DEFAULT_ENVD_BIN_PATH`]; tests point this at a temp file
    /// whose bytes they control so the recorded /verified digest is
    /// predictable.
    pub fn with_envd_bin_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.envd_bin_path = path.into();
        self
    }

    /// Override the envd TCP port used by [`RuntimeAdapter::resume`]
    /// when waiting for the restored process to bind. Production leaves
    /// the default [`ENVD_HTTP_PORT`]; tests inject an ephemeral port
    /// after binding a `std::net::TcpListener` on it so the resume
    /// wait completes deterministically.
    pub fn with_envd_port(mut self, port: u16) -> Self {
        self.envd_port = port;
        self
    }

    /// Override the post-probe TCP drain wait. Production leaves
    /// [`POST_PROBE_TCP_DRAIN`] in place; tests using
    /// [`fake::FakeEnvdState`] (which doesn't open a real TCP
    /// connection) inject [`std::time::Duration::ZERO`] so the unit
    /// suite isn't paying 500 ms per pause case.
    pub fn with_post_probe_drain(mut self, drain: std::time::Duration) -> Self {
        self.post_probe_drain = drain;
        self
    }

    /// Override the node-stats provider (#238). Production leaves the
    /// real [`crate::headroom::SysNodeStats`]; tests inject a stub that
    /// returns a fixed disk/memory reading so the headroom gate's
    /// fail-closed path is exercised without a real `statvfs` /
    /// `/proc/meminfo`.
    pub fn with_node_stats(mut self, node_stats: Arc<dyn crate::headroom::NodeStats>) -> Self {
        self.node_stats = node_stats;
        self
    }

    /// Override the fail-closed resource-headroom policy (#238). The node
    /// binary builds this from `--headroom-fraction` /
    /// `--headroom-min-free-bytes` (and their env equivalents); the
    /// default ([`crate::headroom::HeadroomPolicy::default`]) is
    /// conservative.
    pub fn with_headroom_policy(mut self, headroom: crate::headroom::HeadroomPolicy) -> Self {
        self.headroom = headroom;
        self
    }

    /// Ordered, de-duplicated list of runsc `--root` candidates to probe.
    ///
    /// The configured root ([`with_runsc_root`](Self::with_runsc_root) /
    /// `SANDBAWKS_RUNSC_ROOT` / `node.runscRoot`) comes first so an explicit
    /// override always wins, followed by the per-environment
    /// [`extra_runsc_roots`](Self::extra_runsc_roots). De-duplication keeps a
    /// root from being probed twice when a fallback repeats the primary.
    fn candidate_runsc_roots(&self) -> Vec<PathBuf> {
        let mut roots = Vec::with_capacity(1 + self.extra_runsc_roots.len());
        roots.push(self.runsc_root.clone());
        for candidate in &self.extra_runsc_roots {
            if !roots.contains(candidate) {
                roots.push(candidate.clone());
            }
        }
        roots
    }

    /// Resolve which runsc `--root` actually holds `container_id`'s state.
    ///
    /// runsc keeps per-container state as flat files directly under the root:
    /// `<root>/<full-id>.state` (+ a `.lock`), where `<full-id>` is the bare
    /// container id for a single container or, in the containerd
    /// multi-container model, `<container-id>_sandbox:<sandbox-id>` — *not* a
    /// `<root>/<container-id>/` subdirectory (verified on k3s/containerd, #377).
    /// [`root_holds_container`] detects either that file layout or a legacy
    /// `<container-id>` subdirectory.
    ///
    /// Resolution is three-tiered: explicit candidates from
    /// [`candidate_runsc_roots`](Self::candidate_runsc_roots) first (an operator
    /// override always wins), then generic discovery across the configured
    /// search dirs (#377), then a warn + fallback to the configured primary. The
    /// binary carries no distro-specific path assumptions: an environment whose
    /// shim keeps runsc state on an alternate subtree either lists it in
    /// `node.runscRootFallbacks` or — preferably — points
    /// `node.runscRootSearchDirs` at its mounted containerd run/state root.
    ///
    /// When nothing matches, `runsc` still fails with its own `FetchSpec failed:
    /// loading container: file does not exist` diagnostic exactly as before, so
    /// the behavior is never worse than the static-root path it replaces; the
    /// warn turns that opaque failure into an actionable one.
    /// Resolve the runsc `--root` that holds `container_id`'s state, or
    /// return [`RuntimeError::RunscRootUnresolved`] if no root can be found.
    ///
    /// Resolution is three-tiered (cheapest first):
    ///
    /// 1. Explicit candidates (`primary` + per-environment fallbacks) — an
    ///    operator override always wins and costs a single directory read per
    ///    candidate (#366).
    /// 2. Generic discovery across the configured search dirs (#377) — lets
    ///    one image work across stock containerd, k3s, RKE2, etc. without
    ///    distro-specific path knowledge baked in.
    /// 3. If neither tier finds the container, returns
    ///    [`RuntimeError::RunscRootUnresolved`] — a distinct, terminal error
    ///    that the node reconciler uses to withdraw snapshot-capability
    ///    advertising (#379). The silent warn-and-fallback that was here
    ///    previously hid misconfiguration: every subsequent `runsc checkpoint`
    ///    failed with an opaque "FetchSpec failed: loading container: file
    ///    does not exist", turning a one-time fix into an infinite retry loop.
    fn resolve_runsc_root(&self, container_id: &str) -> Result<PathBuf, RuntimeError> {
        // 1. Explicit candidates (primary + per-environment fallbacks): an
        //    operator override always wins and is the cheapest check —
        //    a single directory read per candidate (#366).
        let candidates = self.candidate_runsc_roots();
        for root in &candidates {
            if root_holds_container(root, container_id) {
                return Ok(root.clone());
            }
        }
        // 2. Generic discovery (#377): no explicit candidate matched, so search
        //    the operator-declared containerd run/state root(s) for wherever
        //    this distro's shim actually placed the container's runsc state.
        //    This is what lets one image work across stock containerd, k3s,
        //    RKE2, etc. without any distro path baked in — the operator only
        //    declares (and mounts) a neutral run/state root, not the exact
        //    `runsc/<namespace>` suffix.
        if let Some(discovered) = self.discover_runsc_root(container_id) {
            tracing::info!(
                container_id,
                discovered_root = %discovered.display(),
                search_dirs = ?self.runsc_root_search_dirs,
                "discovered runsc --root by searching the configured containerd \
                 run/state root(s) (#377)"
            );
            return Ok(discovered);
        }
        // 3. No candidate held the container and discovery found nothing.
        //    Return a typed error instead of silently falling back to the
        //    configured primary (#379): the previous fallback-and-warn let
        //    runsc emit its own opaque "FetchSpec failed" diagnostic, which
        //    the reconciler treated as a generic CheckpointFailed and retried
        //    forever — one misconfigured node turned into an infinite
        //    per-sandbox fail loop that wedged the warm pool. The caller
        //    (node reconciler) must now withdraw snapshot-capability
        //    advertising on this error so the loop breaks.
        Err(RuntimeError::RunscRootUnresolved {
            container_id: container_id.to_owned(),
            candidates,
            search_dirs: self.runsc_root_search_dirs.clone(),
        })
    }

    /// Read the **host** `runsc` binary release token (e.g.
    /// `release-20260525.0`) by shelling `runsc --version` (#545, Capability B).
    ///
    /// Reads [`Self::host_runsc_binary`] (falling back to [`Self::runsc_binary`]
    /// when unset) — NOT the image-bundled `runsc_binary` — so the version
    /// stamped into the snapshot describes the host runsc that actually ran the
    /// sandbox and will perform the restore, and matches the host-derived
    /// [`sandbawks_types::RUNSC_VERSION_LABEL`] node label the placement gate
    /// keys off. Reading the bundled binary would record the wrong version on a
    /// node where host runsc has drifted from the bundle (#546 `BundledMismatch`),
    /// stranding every restore `Pending`.
    ///
    /// Returns `None` when the binary can't be executed or its output carries
    /// no parseable `release-` token, so the caller (pause) records an empty
    /// snapshot runsc version and the restore-placement gate degrades to
    /// envd-staged affinity only rather than failing the pause. Reuses
    /// [`crate::runsc_reconcile::parse_runsc_release`] so the parse here and the
    /// node-label reconcile agree on the canonical token.
    async fn read_runsc_release(&self) -> Option<String> {
        let runsc = self
            .host_runsc_binary
            .as_deref()
            .unwrap_or(&self.runsc_binary);
        // `--version` writes the `release-<ver>` line to stdout; merge stderr in
        // too for robustness across runsc versions (mirrors the reconcile reader).
        let output = tokio::process::Command::new(runsc)
            .arg("--version")
            .output()
            .await
            .ok()?;
        let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
        text.push('\n');
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        crate::runsc_reconcile::parse_runsc_release(&text)
    }

    /// Discover the runsc `--root` for `container_id` by searching the
    /// configured [`runsc_root_search_dirs`](Self::runsc_root_search_dirs).
    ///
    /// Returns the directory that holds the container's runsc state — i.e. the
    /// value to pass as `runsc --root` — or `None` when no search dir is
    /// configured or none contains the container. Search dirs are tried in
    /// order; the first match wins.
    fn discover_runsc_root(&self, container_id: &str) -> Option<PathBuf> {
        self.runsc_root_search_dirs
            .iter()
            .find_map(|base| find_runsc_root(base, container_id, RUNSC_ROOT_SEARCH_MAX_DEPTH))
    }
}

/// Filename suffix runsc gives each container's state file (`<full-id>.state`).
const RUNSC_STATE_FILE_SUFFIX: &str = ".state";

/// Maximum directory depth descended below each search dir when discovering a
/// runsc root (#377).
///
/// runsc lays per-container state out at `<run>/runsc/<namespace>/<full-id>.state`
/// — depth 3 below the containerd run root — so a small bound finds it on every
/// known layout while keeping the walk from straying across an unexpectedly
/// large subtree if a search dir is mis-pointed.
const RUNSC_ROOT_SEARCH_MAX_DEPTH: usize = 6;

/// Whether a directory entry is the runsc state file for `container_id`.
///
/// runsc names each container's state file `<full-id>.state`, where `<full-id>`
/// is either the bare container id (single-container) or, in the containerd
/// multi-container model verified on k3s, `<container-id>_sandbox:<sandbox-id>`
/// (e.g. `c42e…_sandbox:bd70….state`). Matching the `_sandbox:` separator
/// exactly — rather than a bare prefix — avoids ever mistaking one container's
/// file for another's.
fn name_is_runsc_state_for(name: &OsStr, container_id: &str) -> bool {
    let Some(stem) = name
        .to_str()
        .and_then(|s| s.strip_suffix(RUNSC_STATE_FILE_SUFFIX))
    else {
        return false;
    };
    stem == container_id
        || stem
            .strip_prefix(container_id)
            .is_some_and(|rest| rest.starts_with("_sandbox:"))
}

/// Whether `root` is the runsc `--root` holding `container_id`'s state.
///
/// Accepts runsc's real flat-file layout (`<root>/<full-id>.state`, verified on
/// k3s/containerd, #377) and, defensively, a legacy `<root>/<container-id>/`
/// subdirectory. A single directory read; no recursion.
fn root_holds_container(root: &Path, container_id: &str) -> bool {
    if root.join(container_id).is_dir() {
        return true;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    entries
        .flatten()
        .any(|entry| name_is_runsc_state_for(&entry.file_name(), container_id))
}

/// Bounded, symlink-free search for the runsc `--root` holding `container_id`.
///
/// Walks `base` up to `max_depth` levels deep and returns the first directory
/// that (a) lies under a `runsc` path component and (b) holds `container_id`'s
/// state per [`root_holds_container`]. The `runsc` component — the runtime's own
/// name, not a distro-specific path — distinguishes the real runsc root from the
/// `runc` root and containerd's identically-keyed *task bundle* under
/// `io.containerd.runtime.v2.task/`, either of which would otherwise be a false
/// positive that points `runsc --root` at the wrong tree.
///
/// Symlinks are never followed (entry types are read without dereferencing), so
/// the walk cannot cycle or escape the mounted subtree.
fn find_runsc_root(base: &Path, container_id: &str, max_depth: usize) -> Option<PathBuf> {
    let runsc_component = OsStr::new("runsc");
    // Iterative DFS with an explicit (dir, depth) stack — no recursion, so a
    // deep tree can't blow the stack, and unreadable/raced dirs are skipped
    // rather than aborting the whole search.
    let mut stack = vec![(base.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let under_runsc = dir.components().any(|c| c.as_os_str() == runsc_component);
        let mut children = Vec::new();
        for entry in entries.flatten() {
            // `file_type()` does not follow symlinks, so a symlinked dir reads
            // as a symlink (not a dir) and is never descended — no cycles.
            let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
            if under_runsc {
                let name = entry.file_name();
                // File layout: `<dir>/<full-id>.state`. Legacy dir layout:
                // `<dir>/<container-id>/`. Either means `dir` is the root.
                if name_is_runsc_state_for(&name, container_id)
                    || (is_dir && name.as_os_str() == OsStr::new(container_id))
                {
                    return Some(dir);
                }
            }
            if is_dir && depth + 1 < max_depth {
                children.push((entry.path(), depth + 1));
            }
        }
        stack.extend(children);
    }
    None
}

/// Replace every occurrence of `container_id` in `text` with a stable
/// placeholder so an error built from runsc's stderr groups as a single
/// Traceway exception class instead of fragmenting one-per-container (#384).
///
/// runsc echoes the container id we passed straight back in its stderr (e.g.
/// `cannot checkpoint container "<id>" in state stopped`), and that text
/// becomes the `RuntimeError` `Display` recorded by the `pause`/`resume`
/// `#[instrument(err)]` span — i.e. the signal Traceway hashes to group
/// exceptions. The id is high-cardinality, so leaving it in the message gives
/// every sandbox its own exception group and the count never climbs past 1.
/// The raw id is still recorded as a structured span field (`container_id`)
/// for per-occurrence lookup; only the *grouped message* is normalised.
///
/// Replacing the bare `container_id` substring also covers runsc's
/// `<container-id>_sandbox:<sandbox-id>` full-id form, since that string is
/// prefixed by the bare id.
///
/// This is a plain substring replace, so it assumes a real containerd
/// container id (a 64-hex SHA). A short or caller-chosen id could appear as a
/// substring of unrelated text and over-scrub — but over-redaction only
/// loosens grouping, never leaks the id, so that is the safe failure
/// direction; the shape is not enforced here.
fn scrub_container_id(text: &str, container_id: &str) -> String {
    // An empty id (no resolution happened) would replace nothing; guard
    // against `str::replace("", ..)` which would splice the placeholder
    // between every char.
    if container_id.is_empty() {
        return text.to_owned();
    }
    text.replace(container_id, "<container-id>")
}

#[async_trait]
impl RuntimeAdapter for GvisorAdapter {
    fn runtime_class(&self) -> &str {
        "gvisor"
    }

    /// `runsc checkpoint`-backed pause.
    ///
    /// See the module-level docs for the full sequence. Each external
    /// call uses the injected trait so the workflow is exercised end
    /// to end in unit tests against [`fake::FakeContainerd`] +
    /// [`fake::FakeEnvdState`] + a stubbed `runsc` script.
    #[tracing::instrument(
        skip_all,
        name = "pause:node::snapshot",
        err,
        fields(pod_uid = %pod_uid, container_id = tracing::field::Empty)
    )]
    async fn pause(
        &self,
        pod_uid: &str,
        sink: &dyn SnapshotSink,
    ) -> Result<SnapshotManifest, RuntimeError> {
        // Per-phase timing for the pause pipeline (pause/resume perf
        // breakdown). Each `mark` is the time since the previous one; the
        // whole set is emitted as a single structured line just before `Ok`.
        let mut timer = PhaseTimer::start();

        // 1. Resolve pod_uid → container_id + pod_ip + sandbox_id.
        let PodResolution {
            container_id,
            pod_ip,
            sandbox_id,
        } = self.containerd.resolve_pod(pod_uid).await?;
        // Record the resolved id as a span field (#384): it stays queryable
        // per-occurrence without entering the scrubbed error message that
        // Traceway groups on.
        tracing::Span::current().record("container_id", container_id.as_str());
        timer.mark("resolve_pod");

        // 1b. #237: assert every writable mount is overlay-backed before
        // committing to a checkpoint. Memory-only restore reconstructs the
        // rootfs only because the overlay upper rides in the sandbox
        // MemoryFile; a writable bind / gofer-passthrough is invisible to
        // that checkpoint and would be silently lost on restore (#231's
        // `--overlay2=none` failure). This is a config property knowable
        // now, so we fail-closed *before* the expensive checkpoint and
        // before any upload — the store stays untouched.
        //
        // Precedence: this runs *after* `resolve_pod`, so a sandbox whose
        // Pod was deleted between SnapshotJob spawn and reconcile surfaces
        // the resolve-pod error rather than a mount-set error even if its
        // spec was also bad. That ordering is intentional and consistent
        // with the rest of the function (you cannot inspect the mounts of a
        // container that no longer exists).
        let mounts = self.containerd.container_mounts(&container_id).await?;
        assert_writable_mounts_overlay_backed(&mounts)?;
        timer.mark("mount_check");

        // Capture-mode marker (epic #466): if the container carries the relocate
        // env (`SANDBAWKS_ENVD_RELOCATE_TARGET`), envd copied itself onto a
        // rootfs path under the captured root overlay, so its code pages ride in
        // the checkpoint's `pages.img` — the restore node need not (and must not)
        // gate on its host-staged envd binary. Read from the container's OCI
        // `process.env`; absent on a legacy Pod (older deploy), which keeps the
        // fail-closed host-binary guard on its snapshots.
        let envd_captured = self
            .containerd
            .container_relocate_target(&container_id)
            .await?
            .is_some();
        timer.mark("relocate_check");

        // 2. Allocate a fresh work-dir. Embedding the RFC 3339 timestamp
        // keeps consecutive pauses on the same sandbox from colliding,
        // and the timestamp doubles as the `SnapshotManifest::created_at`.
        let created_at = self.clock.now_rfc3339();
        let work_dir = self.pause_work_dir(&sandbox_id, &created_at);
        tokio::fs::create_dir_all(&work_dir).await.map_err(|e| {
            RuntimeError::CheckpointFailed(format!("create work-dir {}: {e}", work_dir.display()))
        })?;

        // 3. Fetch envd state JSON — MUST happen before runsc
        // checkpoint because `runsc checkpoint` (without
        // `--leave-running`) terminates the sandbox process, killing
        // envd. We *fetch* now but *upload* after a successful
        // checkpoint, so a checkpoint failure still leaves the sink
        // untouched (preserves the "no partial uploads on failure"
        // invariant the test suite locks in).
        let envd_bytes = self.envd.fetch_state(&pod_ip).await?;
        timer.mark("envd_fetch");

        // 3a. Wait for the probe's TCP RST to land on the sandbox
        // side before checkpoint. `runsc checkpoint` will refuse to
        // save a sandbox whose TCP stack has any endpoint not in
        // CLOSED — including ESTABLISHED *and* TIME_WAIT. The probe
        // in step 3 uses `SO_LINGER(0)`, so its close sends a TCP
        // RST instead of a FIN; on receipt, the sandbox-side
        // endpoint transitions immediately to CLOSED with no
        // TIME_WAIT. Sub-millisecond in a kind cluster but a CPU-
        // saturated CI runner can eat scheduling latency, so we wait
        // [`POST_PROBE_TCP_DRAIN`]. Tests override this to ZERO via
        // `with_post_probe_drain` since `FakeEnvdState` doesn't open
        // a real TCP connection.
        if !self.post_probe_drain.is_zero() {
            tokio::time::sleep(self.post_probe_drain).await;
        }
        timer.mark("probe_drain");

        // 3b. Best-effort OTel quiesce before checkpoint (#403).
        // Signals envd to flush in-flight spans so telemetry is not split
        // across the checkpoint boundary. Failure is logged and pause
        // continues — abandoning a wedged provider and checkpointing anyway
        // is no worse than today. The connection RST-closes (SO_LINGER(0),
        // same as the state probe) so the sandbox-side endpoint reaches
        // CLOSED before runsc checkpoint inspects the TCP stack.
        let quiesce_result = self.envd.quiesce(&pod_ip).await;
        crate::envd_lifecycle_metrics::record_lifecycle(
            crate::envd_lifecycle_metrics::EnvdLifecycleCall::Quiesce,
            &quiesce_result,
        );
        if let Err(e) = quiesce_result {
            tracing::warn!(
                sandbox = %sandbox_id,
                pod_ip = %pod_ip,
                error = %e,
                "pause: envd /__quiesce failed; proceeding with checkpoint"
            );
        }
        timer.mark("envd_quiesce");

        // 4. runsc checkpoint --image-path <work_dir> <container_id>.
        // Probe the configured + per-environment fallback roots for the one
        // that actually holds this container's runsc state (#366) so a distro
        // whose shim keeps state off the configured subtree still checkpoints.
        // `?` propagates `RunscRootUnresolved` — a typed terminal error the
        // node reconciler uses to withdraw snapshot-capability advertising and
        // break the infinite-retry loop that the old warn-and-fallback caused
        // (#379). The caller (adapter.pause caller) sees `RuntimeError`, so no
        // new wrapping is needed.
        let runsc_root = self.resolve_runsc_root(&container_id)?;
        let output = tokio::process::Command::new(&self.runsc_binary)
            .arg("--root")
            .arg(&runsc_root)
            .arg("checkpoint")
            .arg("--image-path")
            .arg(&work_dir)
            .arg(&container_id)
            .output()
            .await
            .map_err(|e| {
                RuntimeError::CheckpointFailed(format!(
                    "spawn {bin:?}: {e}",
                    bin = self.runsc_binary,
                ))
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            // Scrub the per-container id from the grouped message (#384) so all
            // checkpoint failures of a kind — including the `in state stopped`
            // race with an idle-timeout auto-pause (#382) — collapse into one
            // Traceway exception class. The raw id rides on the span field.
            return Err(RuntimeError::CheckpointFailed(format!(
                "runsc checkpoint exit {status}: {scrubbed}",
                status = output.status,
                scrubbed = scrub_container_id(&stderr, &container_id),
            )));
        }
        timer.mark("checkpoint");

        // 4a. Fail-closed checkpoint postconditions (#232). A zero exit
        // is necessary but not sufficient: a partial/truncated checkpoint
        // can leave a missing or empty image file that fails `runsc
        // restore` with an opaque EOF *at resume time*, after the source
        // sandbox is gone. Validate the three required `*.img` files now,
        // while the source is still alive, so a bad snapshot surfaces as a
        // failed pause rather than unrecoverable data loss. Async
        // (`tokio::fs::metadata`) so the three stats don't block the runtime.
        verify_checkpoint_postconditions(&work_dir).await?;
        timer.mark("verify_postconditions");

        // 5. Tar + zstd the `*.img` files runsc dropped into work_dir.
        // gVisor 20240603+ writes `checkpoint.img` (saved sentry state),
        // `pages.img` (MemoryFile pages — carries the sandbox-internal
        // overlay-tmpfs upper layer, where workload rootfs writes
        // actually land), and `pages_meta.img` (MemoryFile metadata).
        // `tar` + `zstd` are sync crates; wrap in spawn_blocking so the
        // CPU work doesn't stall the runtime under N concurrent pauses.
        let (memory_path, memory_uncompressed) = tar_zstd_checkpoint_files(&work_dir).await?;
        timer.mark("compress");
        // Stream the staged archive to the sink straight off disk
        // (`put_part_file`) so neither the build nor the upload ever holds
        // the whole `memory.zst` in RAM — the GiB-scale pause OOM fix
        // (#293).
        let upload = sink.put_part_file("memory.zst", &memory_path).await;
        // Drop the on-disk copy *regardless of upload outcome*, before
        // propagating any error. A GiB-scale `memory.zst` left on the
        // snapshot cache volume by a failed upload is never reclaimed
        // (pause has no work_dir GC, and the staging sweeper covers a
        // different root), and each retried pause lands in a fresh
        // timestamped work_dir, so orphans accumulate on the very disk the
        // headroom gate keys on. Best-effort: a leaked file is wasteful but
        // not correctness-affecting. (The raw `*.img` files share this
        // pre-existing leak; a pause-side work_dir GC for those is tracked
        // separately — this just stops the PR from amplifying it with a
        // full compressed copy.)
        if let Err(e) = tokio::fs::remove_file(&memory_path).await {
            tracing::debug!(
                path = %memory_path.display(),
                error = %e,
                "pause: failed to remove staged memory.zst",
            );
        }
        // Record the uncompressed `*.img` total on the returned part so the
        // resume memory headroom gate admits against the real working set,
        // not the zstd-shrunk wire size (#243); the sink only knows the
        // compressed length it just wrote.
        let memory_part = upload?.with_uncompressed_bytes(memory_uncompressed);
        timer.mark("upload_memory");

        // 6. Rootfs delta via containerd snapshotter Diff API. The
        // returned bytes are already tar+zstd; the trait gates the
        // format choice rather than the call site.
        let rootfs_bytes = self.containerd.rootfs_delta(&container_id).await?;
        let rootfs_part = sink.put_part("rootfs-delta.tar.zst", rootfs_bytes).await?;
        timer.mark("rootfs_delta");

        // 7. Upload the envd state bytes captured in step 3.
        let envd_part = sink.put_part("envd-state.json", envd_bytes).await?;
        timer.mark("envd_upload");

        // 7. Build manifest with all three parts + aggregate sha256.
        // `snapshot_ref` is sourced from the sink itself
        // ([`SnapshotSink::manifest_url`]) so the controller's
        // observation of `SnapshotJob.status.manifest.snapshot_ref`
        // surfaces a real `s3://…` URL without the adapter needing to
        // know the bucket name or wire layout.
        // 7a. Record the digest of the node-staged envd binary (issue #270).
        // For a *legacy* (non-relocated) sandbox the workload execs
        // `self.envd_bin_path` directly, so its code pages are mmap-backed by
        // that file and the restore node fails closed if its staged binary
        // has drifted (see `verify_envd_digest`). Under capture-mode
        // relocation (`envd_captured`, epic #466) this digest is no longer
        // load-bearing — the running envd lives on the tmpfs and may already
        // differ from `self.envd_bin_path`, and the restore branch skips the
        // host comparison entirely — but it is still recorded for diagnostics
        // and so a snapshot mis-flagged as legacy degrades safely. Best-effort:
        // a worker without the envd mount (older deploy) records an empty
        // digest, and the restore path then warns rather than verifying.
        let envd_sha256 = match hash_file(&self.envd_bin_path).await {
            Ok(digest) => digest,
            Err(e) => {
                tracing::warn!(
                    sandbox = %sandbox_id,
                    path = %self.envd_bin_path.display(),
                    error = %e,
                    "pause: could not read node-staged envd binary to record its \
                     digest; restore-time envd determinism check will be skipped \
                     (issue #270)"
                );
                String::new()
            }
        };

        timer.mark("envd_digest");

        // 7b. Record the host runsc release this checkpoint was taken
        // against (#545, Capability B). gVisor C/R requires the *exact*
        // same runsc build on the restoring node, so the controller folds
        // this into `Sandbox.status.snapshotRuntimeVersion` and constrains
        // the next restore Pod's nodeAffinity to a matching node — turning
        // a fatal `runsc version does not match` into a bounded `Pending`.
        // Best-effort, mirroring the envd digest above: a node whose runsc
        // version can't be read records an empty string and the restore
        // gate degrades to envd-staged affinity only (no worse than the
        // pre-#545 ungated placement) rather than failing the pause.
        let runsc_version = self.read_runsc_release().await.unwrap_or_else(|| {
            tracing::warn!(
                sandbox = %sandbox_id,
                runsc = %self
                    .host_runsc_binary
                    .as_deref()
                    .unwrap_or(&self.runsc_binary)
                    .display(),
                "pause: could not read host runsc version to stamp the snapshot; \
                 restore-placement version gate will be skipped for this snapshot (#545)"
            );
            String::new()
        });

        timer.mark("runsc_version");

        let parts = vec![memory_part, rootfs_part, envd_part];
        let mut manifest = SnapshotManifest {
            sandbox_id,
            runtime_class: self.runtime_class().to_owned(),
            sha256: String::new(),
            created_at,
            parts,
            snapshot_ref: sink.manifest_url(),
            envd_sha256,
            runsc_version,
            envd_captured,
            // Issue #416: the clone-from-snapshot template gate uses the source
            // Sandbox CR's resolved template image for the sandbox-ID restore
            // form (which the controller has directly), so node-side population
            // is not required for that path. Left empty here pending the
            // manifest-fetch `s3://` recovery form, where the source CR may be
            // gone and the manifest is the only template record; an empty value
            // degrades that future gate to warn-only.
            template_ref: String::new(),
            template_image: String::new(),
        };
        manifest.recompute_aggregate();
        sink.put_manifest(&manifest).await?;
        timer.mark("manifest_upload");

        // One structured line for the whole pause. `bytes_memory` lets a
        // reader divide by `upload_memory` for an effective MB/s without
        // cross-referencing the s3 module's own per-part line.
        let bytes_memory = manifest
            .parts
            .iter()
            .find(|p| p.name == "memory.zst")
            .map(|p| p.size_bytes)
            .unwrap_or(0);
        timer.record_metrics();
        tracing::info!(
            sandbox = %manifest.sandbox_id,
            bytes_memory,
            phases = %timer.rendered(),
            total_ms = timer.total_ms(),
            "pause pipeline phase timings (ms)",
        );

        Ok(manifest)
    }

    /// `runsc restore`-backed resume.
    ///
    /// See the module-level docs for the full sequence. The
    /// controller-supplied `manifest` is treated as an identity hint
    /// — the authoritative copy is fetched from `source` and
    /// cross-checked by aggregate sha256, so a stale or tampered
    /// `Sandbox.status.snapshotRef` can't drive the adapter into a
    /// restore against the wrong bundle.
    #[tracing::instrument(
        skip_all,
        name = "resume:node::snapshot",
        err,
        fields(
            pod_uid = %pod_uid,
            sandbox = %manifest.sandbox_id,
            container_id = tracing::field::Empty
        )
    )]
    async fn resume(
        &self,
        manifest: &SnapshotManifest,
        pod_uid: &str,
        source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        // Plan 07 Decision (revised by #232): the preferred resume path
        // stages the bundle via `stage_snapshot` and lets the
        // containerd-shim drive a memory-only `runsc restore` of a
        // *fresh* kubelet-managed container. This in-process `runsc
        // restore` against an already-running container is the
        // shim-not-installed fallback; flag it so a cluster that
        // unexpectedly takes it is visible in the logs.
        tracing::warn!(
            pod_uid = %pod_uid,
            "GvisorAdapter::resume: taking the shim-not-installed fallback path \
             (in-process `runsc restore`); the containerd-shim-sandbawks-v1 \
             stage path (Plan 07) is preferred"
        );
        let mut timer = PhaseTimer::start();

        // 1. Resolve pod_uid → container_id + pod_ip + sandbox_id. The
        // same containerd handle pause used; resume's restore target is
        // a *fresh* container in the same Pod, so the IDs match.
        let PodResolution {
            container_id,
            pod_ip,
            sandbox_id,
        } = self.containerd.resolve_pod(pod_uid).await?;
        // Record the resolved id as a span field (#384), mirroring pause.
        tracing::Span::current().record("container_id", container_id.as_str());
        timer.mark("resolve_pod");

        // 2. Allocate a fresh work-dir. The `resume-` prefix
        // disambiguates from the pause-side `<rfc3339>` paths under
        // the same `<sandbox_id>/` parent.
        let created_at = self.clock.now_rfc3339();
        let work_dir = self
            .work_root
            .join(&sandbox_id)
            .join(format!("resume-{}", sanitize_path_segment(&created_at)));
        tokio::fs::create_dir_all(&work_dir).await.map_err(|e| {
            RuntimeError::RestoreFailed(format!("create work-dir {}: {e}", work_dir.display()))
        })?;

        // 3. Fetch the authoritative manifest, cross-check it against the
        // controller's committed fingerprint, then stream + verify every
        // part into `work_dir`. The shared helper is the single
        // definition of the on-disk layout (also used by
        // `stage_snapshot`); on the resume path the `rootfs-delta` is
        // applied to the live container via the containerd snapshotter.
        // Best-effort cleanup on failure, mirroring `stage_snapshot`: a
        // headroom rejection (or any fetch/verify error) would otherwise
        // leave the freshly-created `resume-<ts>/` work-dir behind, and
        // each retry lands in a new clock-segment subdir — so orphans
        // would accumulate over repeated failures (#238 review).
        if let Err(e) = self
            .fetch_verify_and_lay_out(
                manifest,
                source,
                &work_dir,
                RootfsDelta::ApplyToContainer {
                    container_id: &container_id,
                },
                HeadroomGate::DiskAndMemory,
                CheckPath::Resume,
            )
            .await
        {
            if let Err(cleanup) = tokio::fs::remove_dir_all(&work_dir).await {
                tracing::warn!(
                    work_dir = %work_dir.display(),
                    error = %cleanup,
                    "resume: failed to remove partial work-dir after error"
                );
            }
            return Err(e);
        }
        timer.mark("fetch_verify");

        // 4a. Issue #171 diagnostic note.
        //
        // Earlier iterations of this branch tried a defensive
        // `runsc pause <cid>` here before restore, on the theory that
        // restore requires a non-running container. The cluster smoke
        // then failed with
        //   "cannot restore container ... in state paused"
        // — gVisor's restore accepts `stopped` or `created` state, not
        // `paused`. We've removed the pre-pause; restore now runs
        // against the live sandbox (envd is the idle shim from issue
        // #171's restore-mode contract, so the sandbox carries no
        // real work). The cluster-side error message from this layer
        // is the empirical signal we need to decide whether the next
        // iteration is `runsc kill`-then-restore or a deeper
        // containerd-CRI restore integration.

        // 5. runsc restore --image-path <work_dir> <container_id>.
        // Same per-environment root probe as the pause path (#366): the restore
        // Pod's shim creates the container's runsc state dir before we run, so
        // the probe selects whichever root the shim used on this distro. `?`
        // propagates `RunscRootUnresolved` the same way as the pause path (#379).
        let runsc_root = self.resolve_runsc_root(&container_id)?;
        let output = tokio::process::Command::new(&self.runsc_binary)
            .arg("--root")
            .arg(&runsc_root)
            .arg("restore")
            .arg("--image-path")
            .arg(&work_dir)
            .arg(&container_id)
            .output()
            .await
            .map_err(|e| {
                RuntimeError::RestoreFailed(format!("spawn {bin:?}: {e}", bin = self.runsc_binary,))
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            // Scrub the per-container id from the grouped message (#384), same
            // as the checkpoint path; the raw id rides on the span field.
            return Err(RuntimeError::RestoreFailed(format!(
                "runsc restore exit {status}: {scrubbed}",
                status = output.status,
                scrubbed = scrub_container_id(&stderr, &container_id),
            )));
        }
        timer.mark("restore");

        // 6. Wait for the restored envd to bind its HTTP port
        // ([`ENVD_HTTP_PORT`], via `self.envd_port`). A successful
        // `connect` on the HTTP surface is the readiness signal because
        // the very next step posts `/__rearm` there; gating on the gRPC
        // port instead would return ready while the HTTP accept loop is
        // still unscheduled, silently skipping the re-arm (issue #533,
        // Bug 2).
        wait_for_tcp_bind(
            &pod_ip,
            self.envd_port,
            std::time::Duration::from_secs(30),
            std::time::Duration::from_millis(500),
        )
        .await?;
        timer.mark("envd_ready");

        // 6a. Best-effort OTel rearm after restore (#403).
        // Signals envd to re-arm its OTel SDK so telemetry resumes on the
        // restored process. Failure is logged and resume continues — degraded
        // telemetry must never fail a resume.
        let rearm_result = self.envd.rearm(&pod_ip).await;
        crate::envd_lifecycle_metrics::record_lifecycle(
            crate::envd_lifecycle_metrics::EnvdLifecycleCall::Rearm,
            &rearm_result,
        );
        if let Err(e) = rearm_result {
            tracing::warn!(
                sandbox = %sandbox_id,
                pod_ip = %pod_ip,
                error = %e,
                "resume: envd /__rearm failed; continuing with degraded telemetry"
            );
        }
        timer.mark("envd_rearm");
        timer.record_metrics();
        tracing::info!(
            sandbox = %sandbox_id,
            phases = %timer.rendered(),
            total_ms = timer.total_ms(),
            "resume pipeline phase timings (ms)",
        );
        Ok(())
    }

    /// Materialize a snapshot bundle onto local disk for the
    /// containerd-shim restore path — see the trait docs on
    /// [`RuntimeAdapter::stage_snapshot`].
    ///
    /// Unlike [`Self::resume`], no container exists at stage time: the
    /// returned directory IS the `--image-path` the shim hands to the
    /// memory `runsc restore`, which reconstructs the container's memory
    /// *and* rootfs from the staged `*.img` files (#232 — under
    /// `--overlay2=root:self` the overlay-tmpfs upper rides along in
    /// `pages.img`). So the layout is identical to resume's (the shared
    /// [`Self::fetch_verify_and_lay_out`] helper unpacks `memory.zst` and
    /// writes `envd-state.json`), except the `rootfs-delta.tar.zst` part
    /// is written into the staging dir verbatim. That delta is **not**
    /// consumed by the memory restore — it is retained only as the
    /// carrier for any committed lower-layer writes (empty for the
    /// overlay-tmpfs model; see #231/#233).
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::SnapshotDownload`] for a manifest /
    /// part fetch or verification failure, or for a filesystem error
    /// allocating the staging dir or writing a part.
    #[tracing::instrument(skip_all, name = "stage:node::snapshot", err, fields(sandbox = %sandbox_id))]
    async fn stage_snapshot(
        &self,
        sandbox_id: &str,
        manifest: &SnapshotManifest,
        source: &dyn SnapshotSource,
        target_path: Option<&str>,
    ) -> Result<String, RuntimeError> {
        // Resolve the directory to materialize into.
        //
        // - `Some(path)` — scheduler-placed restore (epic #484/#487): the
        //   controller pinned a deterministic path and already stamped the
        //   restore Pod's `restore-image-path` annotation with it, so we
        //   stage *exactly* there. Clear any prior attempt's bytes first;
        //   the controller dispatches one `Stage` per sandbox at a time
        //   (single-flight), so there is no concurrent writer to race, and
        //   a leftover partial dir from an earlier failed attempt must not
        //   pollute the fresh layout. The `.staged` marker is written last.
        // - `None` — legacy caller: allocate a fresh
        //   `<staging_root>/<sandbox-id>/<clock-segment>/` dir and report it
        //   back in `status.localPath`. The clock-derived segment keeps
        //   consecutive stages from colliding, exactly as the pause/resume
        //   work-dirs do, and no marker is written.
        let staging_dir = match target_path {
            Some(path) => {
                let dir = PathBuf::from(path);
                // Defense-in-depth: we are about to `remove_dir_all` (and then
                // write into) a path that arrived verbatim on the `SnapshotJob`
                // CRD. In normal flow the controller derives
                // `<staging_root>/<sandbox-id>/restore`, but the only thing
                // standing between a malformed/hostile `target_path` and a
                // recursive delete of an arbitrary node-mounted host tree is
                // RBAC on the CRD plus controller correctness. Assert the path
                // resolves under `staging_root` *before* touching it, so a bad
                // path fails closed instead of deleting an unrelated tree.
                ensure_under_staging_root(&dir, &self.staging_root).await?;
                match tokio::fs::remove_dir_all(&dir).await {
                    Ok(()) => {}
                    // First stage for this sandbox: nothing to clear.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(RuntimeError::SnapshotDownload(format!(
                            "clear restore target dir {}: {e}",
                            dir.display()
                        )))
                    }
                }
                dir
            }
            None => {
                let job_segment = sanitize_path_segment(&self.clock.now_rfc3339());
                self.staging_root.join(sandbox_id).join(&job_segment)
            }
        };
        tokio::fs::create_dir_all(&staging_dir).await.map_err(|e| {
            RuntimeError::SnapshotDownload(format!(
                "create staging dir {}: {e}",
                staging_dir.display()
            ))
        })?;

        // Fetch + verify + lay out the bundle. `WriteToStagingDir` keeps
        // the rootfs delta as a file in the staging dir (no live
        // container to apply it to); the shim consumes the whole dir.
        //
        // Best-effort cleanup on failure: a partway error (sha mismatch
        // on a later part, transfer failure) would otherwise leave a
        // partial staging dir behind, and each stuck-job retry lands in
        // a fresh clock-segment subdir — so orphans would accumulate
        // over repeated failures. Removing the partial dir is safe
        // because `status.localPath` (the shim's only handle on it) is
        // set on `Complete` alone, never on the error path. For the
        // pinned-path case the marker is likewise never written on the
        // error path, so the stage-gate keeps blocking. Stage runs on
        // every restore, not once per pause cycle, so this matters more
        // here than it does for the pause/resume work-dirs.
        if let Err(e) = self
            .fetch_verify_and_lay_out(
                manifest,
                source,
                &staging_dir,
                RootfsDelta::WriteToStagingDir,
                HeadroomGate::Disk,
                CheckPath::Stage,
            )
            .await
        {
            if let Err(cleanup) = tokio::fs::remove_dir_all(&staging_dir).await {
                tracing::warn!(
                    staging_dir = %staging_dir.display(),
                    error = %cleanup,
                    "stage_snapshot: failed to remove partial staging dir after error"
                );
            }
            return Err(e);
        }

        // Scheduler-placed restore only: drop the `.staged` completion
        // marker *last*, after every part is on disk, so the
        // credential-less stage-gate init container releases the sandbox
        // container only once the bundle is whole (issue #487). The
        // write-temp + atomic-rename in [`write_staged_marker`] means a
        // poller can never observe a half-written marker.
        //
        // On failure, clean up the now-fully-laid-out-but-unmarked dir,
        // mirroring the `fetch_verify_and_lay_out` arm above: it would
        // self-heal anyway (the next attempt's top-of-function clear) and
        // the gate stays blocked regardless, but leaving the fail-closed
        // path consistent avoids an unmarked bundle lingering on disk.
        if target_path.is_some() {
            if let Err(e) = write_staged_marker(&staging_dir).await {
                if let Err(cleanup) = tokio::fs::remove_dir_all(&staging_dir).await {
                    tracing::warn!(
                        staging_dir = %staging_dir.display(),
                        error = %cleanup,
                        "stage_snapshot: failed to remove staging dir after marker-write error"
                    );
                }
                return Err(e);
            }
        }

        // The shim's restore annotation carries this path as a string,
        // so a non-UTF-8 staging path can't be represented — surface it
        // rather than lossily round-tripping through `to_string_lossy`.
        staging_dir.into_os_string().into_string().map_err(|os| {
            RuntimeError::SnapshotDownload(format!(
                "staging dir path is not valid UTF-8: {}",
                std::path::Path::new(&os).display()
            ))
        })
    }

    /// Fail-closed restorability proof (issue #235): a storage round-trip.
    ///
    /// Re-fetches the just-uploaded snapshot from `source` into a throwaway
    /// scratch dir and proves it materializes back into a complete,
    /// restorable checkpoint bundle — *before* the node worker reports the
    /// pause `Complete`, which is what lets the controller advance
    /// `Pausing → Paused` and reclaim the source. On failure the worker
    /// lands the job `Failed` with the source still recoverable.
    ///
    /// Two layered checks, both via the shared [`Self::fetch_verify_and_lay_out`]
    /// + [`verify_checkpoint_postconditions`] helpers (so the on-disk layout
    /// and the postcondition floor have one definition each):
    ///
    /// 1. **Integrity** — the manifest re-fetches, its aggregate sha256
    ///    cross-checks (issue #171), every part's per-chunk + per-part
    ///    sha256/size verifies, and `memory.zst` decompresses + un-tars
    ///    cleanly. This catches upload corruption, truncation, missing
    ///    parts, and undecompressable archives.
    /// 2. **Completeness** — the round-tripped `*.img` files satisfy the
    ///    same checkpoint postconditions a live pause enforces (all three
    ///    present and non-empty; #232). This catches a snapshot whose
    ///    parts verify individually but whose memory image set is
    ///    structurally incomplete.
    ///
    /// ## Why not a live `runsc restore`?
    ///
    /// An earlier iteration booted a throwaway sandbox from the snapshot.
    /// The kind/bench CI proved that inherently racy: a whole-sandbox
    /// restore must replay the pod-sandbox (root) container's OCI bundle,
    /// but containerd reaps that bundle within seconds of the checkpoint
    /// stopping the sandbox — sometimes before validation runs (it won on
    /// a roomy node, lost on a loaded one). A fail-closed gate that flakes
    /// on reap timing would spuriously fail legitimate pauses under load.
    /// This round-trip needs no bundle and boots no second sandbox, so it
    /// is robust across environments and adds no per-pause sandbox cost. A
    /// deeper semantic-restorability oracle (the envd rootfs fingerprint)
    /// is tracked separately under #232 and composes on top of this floor.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::SnapshotDownload`] if the uploaded snapshot fails to
    /// re-fetch or verify, or if the scratch filesystem lacks the disk
    /// headroom for the bundle's transient expansion peak (#331 — gated via
    /// [`HeadroomGate::Disk`]); [`RuntimeError::CheckpointFailed`] if the
    /// round-tripped image set is incomplete.
    #[tracing::instrument(skip_all, name = "validate:node::restorable", err, fields(sandbox = %manifest.sandbox_id))]
    async fn validate_restorable(
        &self,
        manifest: &SnapshotManifest,
        source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        let level = self.validate_level;
        tracing::info!(
            sandbox = %manifest.sandbox_id,
            level = ?level,
            "pause restorability validate",
        );
        // Record the validation outcome (#373) — the metric counterpart of the
        // `validate:node::restorable` span, labelled by level. Wrap the body so
        // every exit path (the `Off` / `Head` / `Sha` early returns and the
        // `Full` round-trip tail) funnels through one record call.
        let result: Result<(), RuntimeError> = async {
            match level {
                // Trust the write path entirely.
                ValidateLevel::Off => return Ok(()),
                // No materialization: fetch the manifest, cross-check its aggregate
                // + envd digests (#171/#278) and the node's staged envd binary
                // (#270), then check each part — head+size (no bodies) or sha
                // (download + per-chunk/part sha, no decompress). Cheaper than
                // `Full`, which re-expands the whole bundle to disk.
                ValidateLevel::Head | ValidateLevel::Sha => {
                    let fetched = source.get_manifest().await?;
                    if !manifest.sha256.is_empty() && fetched.sha256 != manifest.sha256 {
                        return Err(RuntimeError::SnapshotDownload(format!(
                            "manifest sha256 mismatch: controller={controller} storage={storage}",
                            controller = manifest.sha256,
                            storage = fetched.sha256,
                        )));
                    }
                    if !manifest.envd_sha256.is_empty()
                        && fetched.envd_sha256 != manifest.envd_sha256
                    {
                        return Err(RuntimeError::SnapshotDownload(format!(
                        "manifest envd_sha256 mismatch: controller={controller} storage={storage}",
                        controller = manifest.envd_sha256,
                        storage = fetched.envd_sha256,
                    )));
                    }
                    self.verify_envd_digest(&fetched, CheckPath::PauseValidate)
                        .await?;
                    for part in &fetched.parts {
                        if level == ValidateLevel::Sha {
                            // Download + verify per-chunk/part sha256, discard bytes.
                            let bytes = source.get_part(&part.name).await?;
                            verify_observed_part(
                                part,
                                &SnapshotPart::from_bytes(&part.name, &bytes),
                            )?;
                        } else {
                            source.verify_part_present(part).await?;
                        }
                    }
                    return Ok(());
                }
                // Fall through to the full re-materialize + postconditions below.
                ValidateLevel::Full => {}
            }

            // Throwaway scratch dir under work_root, removed on every exit path.
            let ts = sanitize_path_segment(&self.clock.now_rfc3339());
            let scratch = self
                .work_root
                .join(&manifest.sandbox_id)
                .join(format!("validate-{ts}"));
            tokio::fs::create_dir_all(&scratch).await.map_err(|e| {
                RuntimeError::SnapshotDownload(format!(
                    "create validation scratch dir {}: {e}",
                    scratch.display()
                ))
            })?;

            // Re-fetch + verify + lay out the *uploaded* bundle (not the local
            // pause work-dir), then assert the round-tripped image set is
            // complete. `WriteToStagingDir` keeps the rootfs delta as a file —
            // there is no container to apply it to and the memory image carries
            // the rootfs (#232).
            //
            // #331: gate on disk headroom (not `Skip`). Post-#330 this round-trip
            // streams `memory.zst` to the scratch dir and expands it into the
            // `*.img` files *before* removing the `.zst`, so compressed + expanded
            // coexist on disk — peak transient disk is roughly compressed +
            // uncompressed bundle size. On a node whose snapshot-cache volume is
            // tight relative to a GiB-scale bundle, the #329 RAM-OOM fix would
            // otherwise shift the failure to ENOSPC mid-expansion *here*, during
            // the pause's own validation. `Disk` makes that fail the SnapshotJob
            // cleanly with a `SnapshotDownload` headroom error instead. Memory is
            // not gated: validate expands to disk and runs no in-process `runsc
            // restore` that would fault `pages.img` into RAM (that is the resume
            // path's `DiskAndMemory`).
            let outcome = async {
                self.fetch_verify_and_lay_out(
                    manifest,
                    source,
                    &scratch,
                    RootfsDelta::WriteToStagingDir,
                    HeadroomGate::Disk,
                    CheckPath::PauseValidate,
                )
                .await?;
                verify_checkpoint_postconditions(&scratch).await
            }
            .await;

            // Best-effort cleanup — a leaked scratch dir is wasteful but
            // harmless, and must never mask the validation outcome.
            if let Err(e) = tokio::fs::remove_dir_all(&scratch).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        dir = %scratch.display(),
                        error = %e,
                        "validation cleanup: failed to remove scratch dir"
                    );
                }
            }
            outcome
        }
        .await;
        crate::validate_metrics::record_validate(level.label(), &result);
        result
    }
}

/// Disposition for the `rootfs-delta.tar.zst` part — the one piece of
/// the snapshot layout that differs between the live-restore
/// ([`RuntimeAdapter::resume`]) and stage-only
/// ([`RuntimeAdapter::stage_snapshot`]) paths. Everything else
/// (`memory.zst` unpack, `envd-state.json` write, per-part sha256 / size
/// verification) is identical, so [`GvisorAdapter::fetch_verify_and_lay_out`]
/// owns it and takes this enum to route just the delta.
///
/// `Copy` so the `match` inside the per-part loop can read it on each
/// iteration without a move (only one `rootfs-delta` part is ever
/// matched, but the borrow checker can't know that).
#[derive(Clone, Copy)]
enum RootfsDelta<'a> {
    /// [`RuntimeAdapter::resume`]: a fresh container already exists, so
    /// hand the delta to the containerd snapshotter's Apply API for
    /// `container_id`.
    ApplyToContainer { container_id: &'a str },
    /// [`RuntimeAdapter::stage_snapshot`]: no container exists yet. The
    /// staging dir is the source bundle the shim's memory `Restore`
    /// reconstructs memory + rootfs from (#232), so the delta is written
    /// into it verbatim under its part name but is not itself consumed by
    /// the restore (see the apply-side comment in
    /// [`GvisorAdapter::fetch_verify_and_lay_out`]).
    WriteToStagingDir,
}

/// Which resources [`GvisorAdapter::fetch_verify_and_lay_out`] gates
/// against before writing the bundle to disk (the fail-closed
/// resource-headroom gate, #238). Every read-side path enforces the gate
/// — there is no skip: the pause-time restorability round-trip
/// ([`RuntimeAdapter::validate_restorable`]) also stages + expands the
/// bundle on disk, so it gates too (#331).
///
/// `Copy` for the same reason as [`RootfsDelta`]: the helper reads it
/// once up front, but threading it by value keeps the call sites terse.
#[derive(Clone, Copy, Debug)]
enum HeadroomGate {
    /// Gate on available disk only ([`RuntimeAdapter::stage_snapshot`] and
    /// the pause-time [`RuntimeAdapter::validate_restorable`] round-trip):
    /// the bundle lands + expands on disk, but neither path runs an
    /// in-process `runsc restore` that would fault `pages.img` into RAM.
    Disk,
    /// Gate on available disk *and* memory ([`RuntimeAdapter::resume`]):
    /// the in-process `runsc restore` faults the saved `pages.img`
    /// working set into RAM, so a memory shortfall is an OOM risk.
    DiskAndMemory,
}

impl GvisorAdapter {
    /// Fetch the authoritative manifest from `source`, cross-check its
    /// aggregate sha256 against the controller-supplied `manifest`, then
    /// stream every part out of `source` — verifying each part's sha256
    /// + size as bytes land — and lay the bundle out under `work_dir`:
    ///
    /// - `memory.zst` → un-zstd + un-tar into `work_dir` so
    ///   `checkpoint.img` / `pages.img` / `pages_meta.img` land where
    ///   `runsc restore --image-path` expects them (issue #195).
    /// - `envd-state.json` → written verbatim into `work_dir`.
    /// - `rootfs-delta.tar.zst` → routed per `rootfs_delta`: applied to a
    ///   live container ([`RuntimeAdapter::resume`]) or written into the
    ///   staging dir as a file ([`RuntimeAdapter::stage_snapshot`]).
    ///
    /// Shared by both read-side paths so the on-disk layout has exactly
    /// one definition and the two can't drift (DRY).
    ///
    /// The aggregate-sha256 cross-check (issue #171) only fires when the
    /// controller committed a non-empty `manifest.sha256`; the per-part
    /// sha256 verification below is the always-on integrity gate. See
    /// the resume-path module docs for the two empty-sha256 fallthrough
    /// cases.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::SnapshotDownload`] on a manifest /
    /// aggregate mismatch, a per-part sha256 / size mismatch, an unknown
    /// part name, or a filesystem / transfer error materializing a part.
    /// Fail closed if this node's staged envd binary has drifted from the
    /// one the snapshot was checkpointed against (issue #270).
    ///
    /// Under node-staging (issue #265) the workload container execs the
    /// cluster-wide envd binary mounted read-only from the node, so its
    /// executable pages are mmap-backed by that on-disk file. A memory
    /// `runsc restore` reconnects those mappings to whatever binary the
    /// *restore* node has staged — if a mutable `envd.image` tag was
    /// re-pushed between pause and resume, that file differs and the
    /// restored process's code pages mismatch it, corrupting the restore.
    /// Comparing the digest recorded at pause against the local binary
    /// turns that silent corruption into a clean, retriable failure.
    ///
    /// A deliberate short-circuit precedes the comparison: a *capture-mode*
    /// snapshot (`envd_captured`, epic #466) had its envd checkpointed from
    /// the captured tmpfs, so the restored process's code pages come from the
    /// snapshot's own `pages.img` rather than the host binary — the comparison
    /// is not just unnecessary but *wrong* (the host binary is expected to
    /// drift after an envd upgrade, which is the whole point). It returns
    /// `Ok(())` early, recording `skipped` / `envd_captured`.
    ///
    /// Two further graceful fall-throughs keep this from breaking existing
    /// (legacy, non-relocated) deployments, both `Ok(())` with a warning:
    /// - the snapshot carries no recorded digest (a v1 manifest, or a
    ///   pause whose worker couldn't read the binary) — nothing to verify
    ///   against;
    /// - the local binary can't be read (a worker without the envd
    ///   mount) — nothing to verify with.
    ///
    /// In every skipped case the per-part sha256 gate still protects the
    /// snapshot bytes; only the cross-binary determinism guarantee is forfeited.
    ///
    /// Called from [`Self::fetch_verify_and_lay_out`], so it runs on all
    /// three of its consumers: `stage_snapshot` and `resume` (the restore
    /// nodes this guard exists for) and [`Self::validate_restorable`] (the
    /// pause-node restorability round-trip). On the pause node the check
    /// is a tautology — `pause` just hashed the same local binary the
    /// manifest records — so it always passes there; it costs one extra
    /// read of the (tens-of-MB) binary per pause and exists for free as a
    /// side-effect of sharing the lay-out helper, not because the pause
    /// node needs it. The `caller` argument ([`CheckPath`]) is threaded
    /// through so the determinism-check counter carries a `path` label,
    /// letting that pause-side tautology be filtered out of the restore
    /// SLO (issue #277) — the metric counterpart of the
    /// `HeadroomGate`-style enum this docstring previously foreshadowed.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::RestoreFailed`] when both digests are known and
    /// differ.
    #[tracing::instrument(
        skip_all,
        name = "verify:node::envd_digest",
        err,
        fields(sandbox = %manifest.sandbox_id, caller = ?caller)
    )]
    async fn verify_envd_digest(
        &self,
        manifest: &SnapshotManifest,
        caller: CheckPath,
    ) -> Result<(), RuntimeError> {
        // Capture-mode short-circuit (epic #466): the checkpointed envd ran
        // from the captured tmpfs, so its code pages are reconstructed from
        // the snapshot's own `pages.img` on restore — the restore node's host
        // binary is not the backing store for the workload's mappings and is
        // therefore irrelevant. Skip the host comparison entirely; the
        // per-part sha256 gate still protects the snapshot bytes. Legacy
        // (`false`/absent) snapshots fall through to the #270 fail-closed
        // comparison below.
        if manifest.envd_captured {
            let outcome = DigestCheckOutcome::SkippedEnvdCaptured;
            record_check(caller, outcome);
            tracing::debug!(
                sandbox = %manifest.sandbox_id,
                outcome = outcome.outcome(),
                reason = outcome.reason(),
                "restore: capture-mode snapshot — envd was checkpointed from the \
                 captured tmpfs, so the host-binary determinism comparison is skipped \
                 (epic #466); per-part sha256 verification still applies"
            );
            return Ok(());
        }
        if manifest.envd_sha256.is_empty() {
            let outcome = DigestCheckOutcome::SkippedEmptyManifest;
            record_check(caller, outcome);
            tracing::warn!(
                sandbox = %manifest.sandbox_id,
                outcome = outcome.outcome(),
                reason = outcome.reason(),
                "restore: snapshot records no envd digest; cannot verify the \
                 node-staged envd binary is byte-identical to checkpoint time — \
                 pin envd.image to an immutable digest for deterministic restore \
                 (issue #270)"
            );
            return Ok(());
        }
        match hash_file(&self.envd_bin_path).await {
            Ok(local) if local == manifest.envd_sha256 => {
                record_check(caller, DigestCheckOutcome::Verified);
                Ok(())
            }
            Ok(local) => {
                record_check(caller, DigestCheckOutcome::FailedMismatch);
                Err(RuntimeError::RestoreFailed(format!(
                    "node-staged envd binary at {path} (sha256 {local}) differs from the \
                 checkpoint's envd (sha256 {expected}); restoring would reconnect the \
                 workload's mmap'd code pages to a different binary and corrupt the \
                 restore — pin envd.image to an immutable digest so every node stages a \
                 byte-identical envd (issue #270)",
                    path = self.envd_bin_path.display(),
                    expected = manifest.envd_sha256,
                )))
            }
            Err(e) => {
                let outcome = DigestCheckOutcome::SkippedLocalUnreadable;
                record_check(caller, outcome);
                tracing::warn!(
                    sandbox = %manifest.sandbox_id,
                    path = %self.envd_bin_path.display(),
                    outcome = outcome.outcome(),
                    reason = outcome.reason(),
                    error = %e,
                    "restore: could not read node-staged envd binary to verify it \
                     matches checkpoint; proceeding without the determinism check \
                     (issue #270)"
                );
                Ok(())
            }
        }
    }

    #[tracing::instrument(
        skip_all,
        name = "fetch_verify:node::snapshot",
        err,
        fields(sandbox = %manifest.sandbox_id, caller = ?caller, gate = ?gate)
    )]
    async fn fetch_verify_and_lay_out(
        &self,
        manifest: &SnapshotManifest,
        source: &dyn SnapshotSource,
        work_dir: &Path,
        rootfs_delta: RootfsDelta<'_>,
        gate: HeadroomGate,
        caller: CheckPath,
    ) -> Result<(), RuntimeError> {
        // Per-phase timing for the read side (pause-validate / resume-stage /
        // resume). `caller` (Debug-rendered below) tells which path a line
        // belongs to. Emitted as one structured line just before `Ok`.
        let mut timer = PhaseTimer::start();
        let fetched = source.get_manifest().await?;
        timer.mark("manifest_fetch");
        if !manifest.sha256.is_empty() && fetched.sha256 != manifest.sha256 {
            return Err(RuntimeError::SnapshotDownload(format!(
                "manifest sha256 mismatch: controller={controller} storage={storage}",
                controller = manifest.sha256,
                storage = fetched.sha256,
            )));
        }

        // #278: cross-check the re-fetched manifest's envd digest against
        // the controller-committed value (`expected_envd_sha256`, threaded
        // in via `manifest.envd_sha256`) *before* `verify_envd_digest`
        // compares it to the local binary. This is the envd analogue of
        // the aggregate-sha256 commit cross-check above (#171): without it,
        // a manifest tampered in storage to flip *both* `sha256` and
        // `envd_sha256` to an attacker-controlled binary could redefine
        // "the expected envd" on the restore node and pass the local-binary
        // check. No-op when the controller committed no value (legacy
        // callers / v1 manifests) — the per-part sha256 gate and the
        // local-binary determinism check still run.
        if !manifest.envd_sha256.is_empty() && fetched.envd_sha256 != manifest.envd_sha256 {
            return Err(RuntimeError::SnapshotDownload(format!(
                "manifest envd_sha256 mismatch: controller={controller} storage={storage}",
                controller = manifest.envd_sha256,
                storage = fetched.envd_sha256,
            )));
        }

        // #270: fail closed if this node's staged envd binary differs from
        // the one the checkpoint was taken against, *before* materializing
        // any bytes — a drifted binary would corrupt the restore (the
        // workload's mmap'd code pages would reconnect to a different
        // file). No-op when the snapshot carries no recorded digest.
        self.verify_envd_digest(&fetched, caller).await?;

        // #238: fail closed on insufficient node headroom *before* writing
        // any part, so a too-big bundle fails its SnapshotJob rather than
        // ENOSPC/OOM mid-materialization. The authoritative `fetched`
        // manifest is the size source; `work_dir` already exists (the
        // caller created it), so `statvfs` reflects the staging filesystem.
        self.enforce_headroom(&fetched, work_dir, gate)?;
        timer.mark("preflight");

        for part in &fetched.parts {
            // `memory.zst` is the part that scales with the workload (the
            // CRIU `*.img` image, multiple GiB). Stream it straight to a
            // staged file and expand it from disk so node RAM stays flat at
            // the 1Gi default — #329: #293 streamed only the *upload* side,
            // leaving this *download* path buffering the whole part in a
            // `Vec<u8>` and OOM-killing the node DaemonSet mid-validate. The
            // streaming source verifies each chunk + the aggregate part
            // digest as it writes; cross-check the observed digest/length
            // against the controller-committed manifest part on top.
            if part.name == "memory.zst" {
                // The staged file's name is intentionally the part name
                // (`memory.zst`) inside `work_dir`. For `stage_snapshot`,
                // `work_dir` *is* the staging dir the shim later consumes
                // (`RootfsDelta::WriteToStagingDir`, #232), but the shim reads
                // only the expanded `*.img` files; the `.zst` extension keeps
                // it clear of any `*.img` glob (mirroring the pause-side
                // `MEMORY_PART_FILE` note) and it is removed below before the
                // shim ever sees the directory.
                let staged = work_dir.join(&part.name);
                let observed = source.get_part_to_file(&part.name, &staged).await?;
                timer.mark("memory_download");
                verify_observed_part(part, &observed)?;
                // Un-zstd + un-tar back into the work-dir so the file layout
                // matches what `runsc checkpoint` left behind on the pause
                // side (which is what `runsc restore --image-path` expects),
                // reading from the staged file rather than from RAM.
                let dest = work_dir.to_owned();
                let src = staged.clone();
                tokio::task::spawn_blocking(move || untar_zstd_file_to_dir_blocking(&src, &dest))
                    .await
                    .map_err(|e| {
                        RuntimeError::SnapshotDownload(format!("memory un-tar+zstd join: {e}"))
                    })??;
                timer.mark("memory_decompress");
                // The staged archive has been expanded into the `*.img` files
                // runsc reads; drop it so the bundle isn't resident on disk
                // twice (compressed + expanded) while restore proceeds.
                if let Err(e) = tokio::fs::remove_file(&staged).await {
                    tracing::warn!(
                        staged = %staged.display(),
                        error = %e,
                        "could not remove staged memory.zst after expansion; leaving it in place",
                    );
                }
                continue;
            }

            let bytes = source.get_part(&part.name).await?;
            let observed = SnapshotPart::from_bytes(&part.name, &bytes);
            verify_observed_part(part, &observed)?;
            match part.name.as_str() {
                "rootfs-delta.tar.zst" => match rootfs_delta {
                    RootfsDelta::ApplyToContainer { container_id } => {
                        // #232: for the `--overlay2=root:self` model this
                        // delta is empty and the memory restore already
                        // carries the overlay-tmpfs upper (#231/#233), so
                        // this apply is a no-op. It is retained only on the
                        // deprecated in-process resume() fallback as the
                        // carrier for committed lower-layer writes (e.g. a
                        // non-overlay config). The Apply API call lives
                        // behind the trait so the fake can intercept it;
                        // production drives the containerd snapshotter.
                        self.containerd
                            .apply_rootfs_delta(container_id, bytes)
                            .await?;
                    }
                    RootfsDelta::WriteToStagingDir => {
                        // #232: the staging dir feeds the shim's memory
                        // `runsc restore`, which restores rootfs + memory
                        // from the `*.img` files alone and never reads this
                        // delta. We keep staging it (cheap, and the only
                        // carrier for committed lower-layer writes) but it
                        // is intentionally unconsumed for the overlay-tmpfs
                        // model — see #231/#233.
                        let dest = work_dir.join(&part.name);
                        tokio::fs::write(&dest, &bytes).await.map_err(|e| {
                            RuntimeError::SnapshotDownload(format!(
                                "write rootfs delta {}: {e}",
                                dest.display()
                            ))
                        })?;
                    }
                },
                "envd-state.json" => {
                    let dest = work_dir.join("envd-state.json");
                    // Intentional error-class unification: pre-T8 the
                    // resume path surfaced an envd-state write failure as
                    // `RestoreFailed`. Sharing this helper recategorizes
                    // it as `SnapshotDownload` for both paths, since this
                    // is the download / lay-out phase, not the `runsc
                    // restore` invocation. Log analysis or alerting that
                    // buckets on the error variant will see resume-time
                    // filesystem errors move from `RestoreFailed` to
                    // `SnapshotDownload` (also noted in the T8 changelog).
                    tokio::fs::write(&dest, &bytes).await.map_err(|e| {
                        RuntimeError::SnapshotDownload(format!(
                            "write envd-state {}: {e}",
                            dest.display()
                        ))
                    })?;
                }
                other => {
                    return Err(RuntimeError::SnapshotDownload(format!(
                        "unknown snapshot part: {other}"
                    )));
                }
            }
        }
        timer.mark("small_parts");

        timer.record_metrics();
        tracing::info!(
            sandbox = %fetched.sandbox_id,
            caller = ?caller,
            phases = %timer.rendered(),
            total_ms = timer.total_ms(),
            "snapshot fetch/verify/lay-out phase timings (ms)",
        );
        Ok(())
    }

    /// Fail-closed resource-headroom gate (#238): refuse to materialize a
    /// snapshot bundle that would exhaust node disk (always) or memory
    /// (restore path) under [`Self::headroom`].
    ///
    /// `needed` disk is the bundle's *transient expansion peak* (#331), not
    /// just its on-the-wire byte count: [`Self::fetch_verify_and_lay_out`]
    /// stages `memory.zst` compressed and un-zstd+un-tars it into the
    /// `*.img` files in the same dir *before* deleting the `.zst`, so the
    /// compressed part and its uncompressed expansion are resident at once.
    /// We bound that peak as `Σ(compressed parts) + uncompressed memory`.
    /// It is deliberately conservative — the compressed `memory.zst` and
    /// the other parts are not all on disk simultaneously with the full
    /// expansion — but a fail-closed gate should over- not under-estimate,
    /// and the [`crate::headroom::HeadroomPolicy`] fraction + floor add the
    /// cushion (see [`crate::headroom::DEFAULT_MIN_FREE_BYTES`]). Before
    /// #330 this path decompressed straight to disk with no compressed copy
    /// resident, so the compressed total alone sufficed; the #329/#330
    /// RAM-OOM fix traded that for this disk peak, which #331 admits against
    /// so a disk-tight node fails its `SnapshotJob` with a clean headroom
    /// error rather than ENOSPC mid-expansion at GiB scale.
    ///
    /// The memory figure (restore path only) is the same uncompressed
    /// `*.img` total — the `pages.img` working set an in-process `runsc
    /// restore` faults into RAM — recorded at pause
    /// ([`SnapshotPart::uncompressed_bytes`], #243), falling back to the
    /// compressed `size_bytes` for pre-#243 snapshots.
    ///
    /// Errors map to the variant each call path already uses for its other
    /// failures, so existing log/alert bucketing is unperturbed:
    /// [`RuntimeError::SnapshotDownload`] for the stage / validate paths,
    /// [`RuntimeError::RestoreFailed`] for the restore path.
    #[tracing::instrument(
        skip_all,
        name = "headroom:node::enforce",
        err,
        fields(sandbox = %fetched.sandbox_id, gate = ?gate)
    )]
    fn enforce_headroom(
        &self,
        fetched: &SnapshotManifest,
        target_dir: &Path,
        gate: HeadroomGate,
    ) -> Result<(), RuntimeError> {
        // Pick the per-path error constructor up front. Tuple-variant
        // constructors coerce to `fn(String) -> RuntimeError`.
        let (check_memory, wrap): (bool, fn(String) -> RuntimeError) = match gate {
            HeadroomGate::Disk => (false, RuntimeError::SnapshotDownload),
            HeadroomGate::DiskAndMemory => (true, RuntimeError::RestoreFailed),
        };

        // The uncompressed `*.img` working set carried by `memory.zst`.
        // Prefer the figure the pauser recorded (#243) and fall back to the
        // compressed `size_bytes` for snapshots paused before it was
        // recorded; absent only for a malformed bundle — treat as zero, the
        // per-part lay-out rejects unknown/missing parts on its own. Both
        // the disk peak (#331) and the memory gate (#243) key on it.
        let uncompressed_mem: u64 = fetched
            .parts
            .iter()
            .find(|p| p.name == "memory.zst")
            .map(|p| p.uncompressed_bytes.unwrap_or(p.size_bytes))
            .unwrap_or(0);

        // #331: the transient expansion peak — compressed bundle plus the
        // uncompressed memory image that coexists with it during un-tar —
        // not just the compressed wire total. This deliberately
        // double-counts the compressed `memory.zst` (it is already in the
        // parts sum, and `uncompressed_mem` is added on top); for #243+
        // snapshots `uncompressed_mem` strictly exceeds the compressed part
        // it shadows, so the over-count is in the conservative direction a
        // fail-closed gate wants. Do NOT "tighten" this by subtracting the
        // compressed `memory.zst` from the sum — that collapses the margin
        // toward zero on small-overhead nodes. The fold saturates so a
        // pathological manifest whose parts sum past `u64::MAX` fails the
        // gate rather than wrapping to a tiny `needed` (symmetric with the
        // outer `saturating_add`).
        let needed_disk: u64 = fetched
            .parts
            .iter()
            .map(|p| p.size_bytes)
            .fold(0u64, u64::saturating_add)
            .saturating_add(uncompressed_mem);
        let avail_disk = self
            .node_stats
            .available_disk_bytes(target_dir)
            .map_err(|e| {
                wrap(format!(
                    "headroom: read available disk for {}: {e}",
                    target_dir.display()
                ))
            })?;
        self.headroom
            .check_disk(needed_disk, avail_disk)
            .map_err(|s| wrap(s.to_string()))?;

        if check_memory {
            // The `pages.img` working set an in-process `runsc restore`
            // faults into RAM is exactly the `memory.zst` uncompressed total
            // computed above.
            let needed_mem = uncompressed_mem;
            let avail_mem = self
                .node_stats
                .available_memory_bytes()
                .map_err(|e| wrap(format!("headroom: read available memory: {e}")))?;
            self.headroom
                .check_memory(needed_mem, avail_mem)
                .map_err(|s| wrap(s.to_string()))?;
        }
        Ok(())
    }

    /// Per-pause work-dir: `<work_root>/<sandbox_id>/<sanitized created_at>`.
    /// Holds the `runsc checkpoint` images before they're tar+zstd'd and
    /// uploaded. The clock-derived segment keeps consecutive pauses on the
    /// same sandbox from colliding.
    fn pause_work_dir(&self, sandbox_id: &str, created_at: &str) -> PathBuf {
        self.work_root
            .join(sandbox_id)
            .join(sanitize_path_segment(created_at))
    }
}

/// Tar + zstd every `*.img` file under `work_dir`, writing the
/// compressed archive to `<work_dir>/memory.zst` and returning that
/// path (ready for [`SnapshotSink::put_part_file`]) paired with the
/// summed *uncompressed* byte length of those `*.img` files (the
/// `pages.img` working set the resume memory headroom gate admits
/// against — see #243).
///
/// The archive is staged to disk rather than accumulated in a `Vec` so
/// the node DaemonSet's pause-path memory is flat regardless of the
/// sandbox size: a 4 GiB sandbox's `memory.zst` lands on the
/// `hostPathSnapshotCache` volume (already disk-headroom-gated) instead
/// of in a `Vec<u8>` that OOM-kills the 1Gi-capped DaemonSet (#293). The
/// caller streams it to the sink chunk-by-chunk and then deletes it.
///
/// `runsc checkpoint --image-path <work_dir>` writes exactly three
/// `.img` files for gVisor 20240603+:
///
/// - `checkpoint.img` — saved sentry state (kernel, task tree, fds).
/// - `pages.img` — MemoryFile pages. With gVisor's default
///   `--overlay2=root:self`, the sandbox rootfs is overlay-tmpfs and
///   the workload's writes live here (NOT in containerd's snapshotter
///   upperdir).
/// - `pages_meta.img` — MemoryFile metadata.
///
/// All three are required for a successful `runsc restore`. The
/// pre-fix glob (`memory*`) matched none of them, which is why issue
/// #193 observed an 18-byte `memory.zst` (the framing of an empty
/// zstd stream) regardless of workload size.
///
/// The function runs `tar` + `zstd` via `tokio::task::spawn_blocking`
/// since both crates are sync; under N concurrent pauses the blocking
/// pool absorbs the CPU work instead of stalling the runtime.
#[tracing::instrument(skip_all, name = "compress:node::memory_images", err)]
async fn tar_zstd_checkpoint_files(work_dir: &Path) -> Result<(PathBuf, u64), RuntimeError> {
    let work_dir = work_dir.to_owned();
    tokio::task::spawn_blocking(move || tar_zstd_checkpoint_files_blocking(&work_dir))
        .await
        .map_err(|e| RuntimeError::CheckpointFailed(format!("checkpoint tar+zstd join: {e}")))?
}

/// Name of the staged archive [`tar_zstd_checkpoint_files_blocking`]
/// writes into the pause work-dir. The `.zst` extension keeps it clear
/// of the `*.img` glob the same function scans, so a retried pause never
/// folds a previous run's `memory.zst` back into the new archive.
const MEMORY_PART_FILE: &str = "memory.zst";

/// Writes the compressed archive to `<work_dir>/memory.zst` and returns
/// that path plus the summed *uncompressed* byte length of the `*.img`
/// files that went into it. The latter is the `pages.img` working set
/// figure the resume-path memory headroom gate needs (#243); zstd hides
/// 3-5x of it in the compressed `size_bytes`.
fn tar_zstd_checkpoint_files_blocking(work_dir: &Path) -> Result<(PathBuf, u64), RuntimeError> {
    let entries = std::fs::read_dir(work_dir).map_err(|e| {
        RuntimeError::CheckpointFailed(format!("read work-dir {}: {e}", work_dir.display()))
    })?;
    // Filter to `*.img` files, collect first so the tar build order
    // is stable across runs. The sort doubles as a determinism guard
    // for the aggregate sha256 in the manifest.
    let mut checkpoint_files: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            RuntimeError::CheckpointFailed(format!(
                "read work-dir entry {}: {e}",
                work_dir.display()
            ))
        })?;
        let path = entry.path();
        let is_img = path
            .extension()
            .and_then(OsStr::to_str)
            .map(|ext| ext.eq_ignore_ascii_case("img"))
            .unwrap_or(false);
        if is_img && path.is_file() {
            checkpoint_files.push(path);
        }
    }
    checkpoint_files.sort();

    // The archive is streamed straight to disk (`memory.zst` on the
    // pause work-dir's `hostPathSnapshotCache` volume) rather than into a
    // `Vec<u8>`, so node DaemonSet RAM stays flat regardless of the
    // sandbox payload — a 4 GiB `pages.img` no longer OOM-kills the
    // 1Gi-capped DS (#293). A `BufWriter` coalesces zstd's many small
    // writes into volume-sized syscalls.
    //
    // Encoder ownership flows tar → zstd → BufWriter → File: `tar::Builder`
    // owns the `zstd::Encoder`, which owns the `BufWriter<File>`. We unwind
    // that on the way out with `into_inner` (recover the zstd encoder),
    // `finish` (flush the compression frame, recover the `BufWriter`), and
    // `into_inner` again (flush the buffer, recover the `File`).
    let out_path = work_dir.join(MEMORY_PART_FILE);
    let out_file = std::fs::File::create(&out_path).map_err(|e| {
        RuntimeError::CheckpointFailed(format!(
            "create snapshot archive {}: {e}",
            out_path.display()
        ))
    })?;
    let buf_writer = std::io::BufWriter::new(out_file);
    let zstd_writer = zstd::stream::write::Encoder::new(buf_writer, 0)
        .map_err(|e| RuntimeError::CheckpointFailed(format!("zstd encoder: {e}")))?;
    let mut tar_builder = tar::Builder::new(zstd_writer);
    // Sum the uncompressed file sizes as we append them. This is the RAM
    // working set `runsc restore` faults back in — the figure the resume
    // memory headroom gate must admit against, not the compressed total.
    let mut uncompressed_bytes: u64 = 0;
    for path in &checkpoint_files {
        let mut file = std::fs::File::open(path).map_err(|e| {
            RuntimeError::CheckpointFailed(format!("open checkpoint file {}: {e}", path.display()))
        })?;
        // `metadata()` reads the inode size without moving the read
        // cursor, so the subsequent `append_file` still streams the
        // whole file.
        let len = file.metadata().map_err(|e| {
            RuntimeError::CheckpointFailed(format!("stat checkpoint file {}: {e}", path.display()))
        })?;
        uncompressed_bytes = uncompressed_bytes.saturating_add(len.len());
        let name = path.file_name().and_then(OsStr::to_str).ok_or_else(|| {
            RuntimeError::CheckpointFailed(format!(
                "checkpoint file has non-utf8 name: {}",
                path.display()
            ))
        })?;
        tar_builder.append_file(name, &mut file).map_err(|e| {
            RuntimeError::CheckpointFailed(format!("tar append {}: {e}", path.display()))
        })?;
    }
    let zstd_writer = tar_builder
        .into_inner()
        .map_err(|e| RuntimeError::CheckpointFailed(format!("tar finish: {e}")))?;
    let buf_writer = zstd_writer
        .finish()
        .map_err(|e| RuntimeError::CheckpointFailed(format!("zstd finish: {e}")))?;
    // `BufWriter::into_inner` flushes the last partial buffer to the file
    // (and surfaces any deferred write error) before we hand the path to
    // the streaming uploader.
    buf_writer
        .into_inner()
        .map_err(|e| RuntimeError::CheckpointFailed(format!("flush snapshot archive: {e}")))?;
    Ok((out_path, uncompressed_bytes))
}

/// The three `*.img` files a successful `runsc checkpoint` must leave
/// behind for gVisor 20240603+ (see [`tar_zstd_checkpoint_files`]). All
/// three are required for a `runsc restore`, so
/// [`verify_checkpoint_postconditions`] fails the pause if any is missing
/// or empty.
const REQUIRED_CHECKPOINT_FILES: [&str; 3] = ["checkpoint.img", "pages.img", "pages_meta.img"];

/// Fail-closed postcondition check on a `runsc checkpoint` work-dir (#232).
///
/// `runsc checkpoint` exiting 0 is necessary but not sufficient: a
/// partially-failed or truncated checkpoint can leave a zero-byte (or
/// missing) image file that then fails `runsc restore` with an opaque
/// `EOF` *at resume time*, long after the source sandbox has been torn
/// down. We would rather treat the pause as failed now — while the source
/// is still alive and the snapshot has not been staged — than commit a
/// snapshot that cannot be restored.
///
/// The check requires that each of [`REQUIRED_CHECKPOINT_FILES`] exists,
/// is a regular file, and is non-empty. That is the strongest structural
/// guarantee obtainable without parsing gVisor's internal image format:
/// `pages.img` (the MemoryFile pages) and `pages_meta.img` (their metadata
/// index) are written as a pair, so an asymmetric truncation — one present
/// and non-empty while its partner is empty or absent — is exactly what
/// the non-empty requirement on *both* catches. End-to-end content
/// integrity (spill, silent corruption) is the job of the envd rootfs
/// fingerprint (separately tracked under #232); this gate is the cheap,
/// always-on structural floor.
///
/// # Errors
///
/// Returns [`RuntimeError::CheckpointFailed`] naming the first offending
/// file, so the caller marks the SnapshotJob Failed and stages nothing.
#[tracing::instrument(skip_all, name = "verify:node::checkpoint_postconditions", err)]
async fn verify_checkpoint_postconditions(work_dir: &Path) -> Result<(), RuntimeError> {
    for name in REQUIRED_CHECKPOINT_FILES {
        let path = work_dir.join(name);
        let meta = tokio::fs::metadata(&path).await.map_err(|e| {
            RuntimeError::CheckpointFailed(format!(
                "checkpoint postcondition: required image {} missing or unreadable: {e}",
                path.display()
            ))
        })?;
        if !meta.is_file() {
            return Err(RuntimeError::CheckpointFailed(format!(
                "checkpoint postcondition: {} is not a regular file",
                path.display()
            )));
        }
        if meta.len() == 0 {
            return Err(RuntimeError::CheckpointFailed(format!(
                "checkpoint postcondition: required image {} is empty \
                 (truncated/partial checkpoint)",
                path.display()
            )));
        }
    }
    Ok(())
}

/// Decompress a zstd-wrapped tarball *file* into `dest` so every entry
/// lands as `<dest>/<entry-path>`.
///
/// This is the resume-side counterpart of
/// [`tar_zstd_checkpoint_files_blocking`]: pause builds an archive of
/// every `*.img` file in its work-dir; resume hands the same archive
/// to `runsc restore --image-path` by writing those files back into a
/// fresh work-dir at the same names.
///
/// Reads the archive through a streaming `zstd` decoder over the staged
/// file rather than from an in-RAM `Vec<u8>`, so node memory stays flat
/// even when the `memory.zst` image is multiple GiB (#329).
/// `tar::Archive::unpack` re-creates the file tree; the `runsc` stub
/// used in unit tests reads them back via plain `std::fs::read`.
///
/// Runs blocking I/O — call from inside `spawn_blocking`.
fn untar_zstd_file_to_dir_blocking(src: &Path, dest: &Path) -> Result<(), RuntimeError> {
    let file = std::fs::File::open(src).map_err(|e| {
        RuntimeError::SnapshotDownload(format!("open staged memory image {}: {e}", src.display()))
    })?;
    // `Decoder::new` wraps its reader in a `BufReader` internally, so the
    // `File` is fed to the zstd frame parser without per-read syscalls.
    let decoder = zstd::stream::read::Decoder::new(file)
        .map_err(|e| RuntimeError::SnapshotDownload(format!("zstd decoder: {e}")))?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(dest).map_err(|e| {
        RuntimeError::SnapshotDownload(format!("tar unpack into {}: {e}", dest.display()))
    })?;
    Ok(())
}

/// Cross-check a downloaded part's observed digest and length against the
/// controller-committed manifest entry.
///
/// Shared by the streamed `memory.zst` path and the buffered small-part
/// path in [`GvisorAdapter::fetch_verify_and_lay_out`] so both enforce the
/// same integrity contract (DRY). The per-chunk and aggregate checks inside
/// the `SnapshotSource` already guard storage-level corruption; this is the
/// defense-in-depth cross-check that what storage returned matches what the
/// controller recorded in the manifest.
fn verify_observed_part(
    expected: &SnapshotPart,
    observed: &SnapshotPart,
) -> Result<(), RuntimeError> {
    if observed.sha256 != expected.sha256 {
        return Err(RuntimeError::SnapshotDownload(format!(
            "sha256 mismatch for part {name}: expected {exp} got {got}",
            name = expected.name,
            exp = expected.sha256,
            got = observed.sha256,
        )));
    }
    if observed.size_bytes != expected.size_bytes {
        return Err(RuntimeError::SnapshotDownload(format!(
            "size mismatch for part {name}: expected {exp} got {got}",
            name = expected.name,
            exp = expected.size_bytes,
            got = observed.size_bytes,
        )));
    }
    Ok(())
}

/// Poll `<host>:<port>` for TCP-accept-readiness until it succeeds or
/// `budget` elapses, sleeping `interval` between attempts.
///
/// Returns `Ok(())` on the first successful `connect`. On timeout,
/// returns [`RuntimeError::RestoreFailed`] with the elapsed budget so
/// the caller can distinguish "the restored process never bound" from
/// other restore failures.
///
/// Uses `tokio::net::TcpStream::connect`. The connect itself can hang
/// for a while if the host is unreachable, so we wrap each attempt in
/// a per-iteration timeout equal to the `interval` — that way the
/// loop bounds total wall-clock time at roughly `budget` even when the
/// pod IP isn't bound to anything yet.
#[tracing::instrument(skip_all, name = "readiness:node::envd_bind", err, fields(host = %host, port = port))]
async fn wait_for_tcp_bind(
    host: &str,
    port: u16,
    budget: std::time::Duration,
    interval: std::time::Duration,
) -> Result<(), RuntimeError> {
    // `host_port` brackets IPv6 literals so `TcpStream::connect` parses
    // the authority on IPv6 clusters.
    let addr = host_port(host, port);
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        // Attempt the connect under a per-iteration timeout so an
        // OS-level connect-hang can't blow past the overall budget.
        let attempt = tokio::time::timeout(interval, tokio::net::TcpStream::connect(&addr)).await;
        if let Ok(Ok(_stream)) = attempt {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RuntimeError::RestoreFailed(format!(
                "envd bind timeout: {addr} did not accept within {budget:?}"
            )));
        }
        tokio::time::sleep(interval).await;
    }
}

/// Replace `/` and `:` in an RFC 3339 timestamp with `-` so it can
/// safely be used as a path segment (colons are fine on Linux but
/// surprise developers on Windows-mounted volumes, and we already pay
/// the cost on the way in).
///
/// Inlined rather than pulling in `slug` since the transformation is
/// trivially three-character-class.
fn sanitize_path_segment(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            ':' | '/' => '-',
            other => other,
        })
        .collect()
}

/// Fail closed unless `target` resolves to a descendant of `staging_root`
/// (issue #487 review — defense-in-depth for the CRD-supplied restore path).
///
/// The scheduler-placed restore path `remove_dir_all`s and then writes into
/// a `target_path` carried verbatim on the `SnapshotJob` CRD. A malformed or
/// hostile value (an absolute path elsewhere, or one escaping via symlink)
/// would otherwise let the node recursively delete an arbitrary tree it has
/// mounted. This guard rejects any such path *before* it is touched:
///
/// 1. Any `..` (parent-dir) component is refused outright — even a path that
///    textually begins with `staging_root` could climb out via embedded
///    `..`.
/// 2. The deepest *existing* ancestor of `target` is canonicalized (resolving
///    every symlink) and must sit under the canonicalized `staging_root`. For
///    a not-yet-created target that ancestor is a parent dir; for one that
///    already exists it is the target itself. The unresolved remainder
///    carries no `..` (step 1), so it cannot escape the resolved ancestor.
///
/// # Errors
///
/// Returns [`RuntimeError::SnapshotDownload`] if `target` contains a `..`
/// component, escapes `staging_root`, or an ancestor cannot be canonicalized
/// for a reason other than non-existence.
async fn ensure_under_staging_root(target: &Path, staging_root: &Path) -> Result<(), RuntimeError> {
    if target
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(RuntimeError::SnapshotDownload(format!(
            "refusing to stage: restore target path {} contains a `..` component",
            target.display()
        )));
    }

    // Canonicalize the staging root once; fall back to the literal path if it
    // can't be resolved (e.g. the host mount is missing) so a legitimate
    // first stage isn't blocked by an unrelated lookup failure.
    let canonical_root = tokio::fs::canonicalize(staging_root)
        .await
        .unwrap_or_else(|_| staging_root.to_path_buf());

    // Canonicalize the deepest ancestor that exists on disk (the target
    // itself if it already exists, else its nearest existing parent),
    // resolving every symlink along the way. `ancestors()` walks
    // target → parent → … → root, so the first that resolves is the deepest.
    let mut resolved = None;
    for ancestor in target.ancestors() {
        match tokio::fs::canonicalize(ancestor).await {
            Ok(p) => {
                resolved = Some(p);
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(RuntimeError::SnapshotDownload(format!(
                    "canonicalize restore target ancestor {}: {e}",
                    ancestor.display()
                )))
            }
        }
    }
    // No ancestor existed at all (not even root): fail closed below.
    let resolved = resolved.unwrap_or_else(|| target.to_path_buf());

    if !resolved.starts_with(&canonical_root) {
        return Err(RuntimeError::SnapshotDownload(format!(
            "refusing to stage: restore target path {} escapes staging root {}",
            target.display(),
            canonical_root.display()
        )));
    }
    Ok(())
}

/// Atomically drop the `.staged` completion marker into `staging_dir`
/// (epic #484 / issue #487).
///
/// The credential-less stage-gate init container (issue #486) polls for
/// `<target_path>/.staged` and releases the restore Pod's sandbox
/// container the instant the marker exists. To guarantee the poller
/// never sees a half-written or empty marker, the bytes are written to a
/// sibling temp file and then `rename`d into place — `rename(2)` within a
/// single directory is atomic on POSIX, so the marker appears whole or
/// not at all. It is written *last*, after every snapshot part is on
/// disk, so its presence proves the bundle is complete.
///
/// The marker body is the staging dir's own path — not load-bearing
/// (the gate only checks for existence), just a breadcrumb for anyone
/// inspecting the node by hand.
///
/// # Errors
///
/// Returns [`RuntimeError::SnapshotDownload`] if the temp write or the
/// rename fails — the caller treats that as a stage failure and never
/// reports the job `Complete`, so the gate keeps blocking rather than
/// releasing onto an unmarked bundle.
async fn write_staged_marker(staging_dir: &Path) -> Result<(), RuntimeError> {
    let marker = staging_dir.join(STAGED_MARKER_FILENAME);
    // Distinct temp name in the same dir so the rename is intra-directory
    // (hence atomic) and can't collide with the `.staged` target itself.
    let tmp = staging_dir.join(format!("{STAGED_MARKER_FILENAME}.tmp"));
    tokio::fs::write(&tmp, marker.to_string_lossy().as_bytes())
        .await
        .map_err(|e| {
            RuntimeError::SnapshotDownload(format!(
                "write staged marker temp {}: {e}",
                tmp.display()
            ))
        })?;
    tokio::fs::rename(&tmp, &marker).await.map_err(|e| {
        RuntimeError::SnapshotDownload(format!(
            "rename staged marker into place {}: {e}",
            marker.display()
        ))
    })
}

/// Lower-case-hex SHA-256 of a file's full contents (issue #270).
///
/// Used to fingerprint the node-staged envd binary at pause and restore.
/// The binary is tens of MB and hashed once per pause/restore, so a
/// single buffered read is fine; the read runs through `tokio::fs` so it
/// doesn't block the async runtime.
///
/// # Errors
///
/// Propagates the underlying [`std::io::Error`] (e.g. the binary is not
/// mounted into this worker container, or is unreadable). Callers decide
/// whether a read failure is fatal or merely skips the digest check.
async fn hash_file(path: &Path) -> Result<String, std::io::Error> {
    let bytes = tokio::fs::read(path).await?;
    Ok(sha256_hex(&bytes))
}

/// Wire-compatible representation of envd's `/__state` JSON body.
///
/// Plan 05 T7 only treats the body as opaque bytes (it's
/// uploaded verbatim as `envd-state.json`), but a typed snapshot of
/// the schema lives here so resume-side validation (T8) has a single
/// source of truth. The struct is deliberately minimal — additional
/// envd fields are forward-compatible because of the `#[serde(default)]`
/// hatches.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EnvdState {
    /// Snapshot revision the envd binary published.
    #[serde(default)]
    pub revision: String,
    /// Free-form per-process registry; opaque at this layer.
    #[serde(default)]
    pub processes: serde_json::Value,
}

impl GvisorAdapter {
    /// Stable hint for callers that want to inspect the `PodRef` the
    /// adapter would surface for a given Sandbox; the gVisor pause path
    /// never materializes Pods itself (the controller does, via
    /// `PodAdapter`), so this is only useful for tests asserting
    /// metadata flows correctly.
    pub fn pod_ref_for(sandbox: &Sandbox) -> PodRef {
        PodRef {
            name: sandbox
                .metadata
                .name
                .clone()
                .unwrap_or_else(|| "anon".into()),
            namespace: sandbox
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into()),
        }
    }
}

// --- Lazy-restore staging-dir garbage collection (Plan 08 B2) ---

/// Default interval between staging-dir sweeps in the production node
/// binary. Overridable via `--staging-sweep-secs` /
/// `SANDBAWKS_STAGING_SWEEP_SECS`. A reclaim is never latency-sensitive
/// (it only frees disk after a sandbox is done with its bundle), so a
/// coarse minute-granularity tick keeps the kube-API list traffic
/// negligible while still bounding how long an orphaned bundle lingers.
pub const DEFAULT_STAGING_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Whether the staged restore bundle for a sandbox in the given phase must
/// be retained on the node.
///
/// With lazy / page-fault restore (Plan 08 B1, `RestoreConfig.Background =
/// true`), gVisor reads pages from the staged `--image-path` bundle *after*
/// `runsc restore` returns from `Start`, for as long as the restored
/// container runs. The bundle therefore must outlive container start and may
/// only be reclaimed once the owning [`Sandbox`] no longer has a live (or
/// soon-to-be-live) container faulting pages from it:
///
/// - `Resuming` — `runsc restore` is in flight (or the restore Pod is being
///   created); the bundle is the `--image-path` it reads. **Keep.**
/// - `Running` — the restored container is live and may fault a page from
///   the bundle at any moment. **Keep.**
/// - `Pausing` — the container is still live while `runsc checkpoint` freezes
///   it; it can still touch pages until the checkpoint completes. **Keep.**
/// - `Creating` — a cold boot, which never restores from a staged bundle (the
///   resume path goes `Paused → Resuming`, never through `Creating`), so any
///   subtree seen here is a stray orphan. **Drop.**
/// - `Paused` / `Failed` / `Terminating` — no container reads the bundle any
///   more. The state machine flips `Paused → Resuming` *before* staging runs
///   (`StageSnapshot` is the action on that edge), so a freshly-staged bundle
///   never coexists with `Paused`; a `Paused` sandbox with a staged subtree
///   was re-paused after running and no longer needs it. **Drop.**
/// - `None` — the `Sandbox` CR is gone (killed and deleted), orphaning its
///   staging subtree. **Drop.**
///
/// `phase` is `None` precisely when the `Sandbox` CR does not exist; an
/// existing CR with no `status` yet reads back as the default
/// [`SandboxPhase::Creating`].
pub fn staging_bundle_needed(phase: Option<SandboxPhase>) -> bool {
    // Exhaustive match (no catch-all) so a future SandboxPhase variant forces
    // a conscious keep/drop decision here rather than silently defaulting.
    match phase {
        Some(SandboxPhase::Running)
        | Some(SandboxPhase::Resuming)
        | Some(SandboxPhase::Pausing) => true,
        Some(SandboxPhase::Creating)
        | Some(SandboxPhase::Paused)
        | Some(SandboxPhase::Failed)
        | Some(SandboxPhase::Terminating)
        | None => false,
    }
}

/// The owning `Sandbox` state the staging sweep needs to decide a subtree's
/// fate: its observed phase and the node currently hosting its Pod.
///
/// Returned (wrapped in `Option`, `None` = CR absent) by
/// [`SandboxOwnerLookup::owner_of`]. Carrying `assigned_node` alongside the
/// phase is what lets the sweep distinguish "this node hosts the live Pod"
/// (keep) from "the Pod moved to another node" (reclaim) — see issue #483 and
/// [`staging_bundle_retained_on_node`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxStagingOwner {
    /// Observed `status.phase`. A CR with no `status` yet reads back as the
    /// default [`SandboxPhase::Creating`].
    pub phase: SandboxPhase,
    /// `status.assigned_node` — the node currently hosting the sandbox Pod, or
    /// `None` when unscheduled (Paused, or pre-bind while a resume restore Pod
    /// is still being placed).
    pub assigned_node: Option<String>,
}

/// Whether the node running the sweep must retain a staged bundle for the
/// owning sandbox described by `owner`, given `this_node` (the sweeper's own
/// node name from the `NODE_NAME` downward API).
///
/// Two gates, both must hold to keep:
///
/// 1. **Phase gate** ([`staging_bundle_needed`]): does any container still
///    read the bundle in the sandbox's current phase?
/// 2. **Locality gate (#483)**: is the sandbox's Pod actually hosted on *this*
///    node? Resume is scheduler-placed — a recovery retry can reschedule the
///    restore Pod onto a different node, which re-stages the snapshot locally.
///    The node it abandoned still holds a `<stagingRoot>/<sandbox-id>` subtree,
///    but the sandbox stays `Running`/`Resuming` on the new node, so the phase
///    gate alone would pin the orphan (multi-GiB) until the sandbox terminates.
///    Reclaiming when the live Pod is on a *different* node frees it promptly.
///
/// Retain iff the phase needs the bundle **and** the Pod is on this node — or
/// its host node is not yet known (`assigned_node == None`), in which case we
/// keep rather than reclaim on incomplete information. That mirrors the sweep's
/// per-subtree "never reclaim on incomplete information" default and trusts
/// `status.assigned_node` to track the live Pod's host, the same trust the
/// phase gate already places in `status.phase`.
///
/// `owner` is `None` precisely when the `Sandbox` CR does not exist (killed and
/// deleted) — an orphan, so **reclaim**.
pub fn staging_bundle_retained_on_node(
    owner: Option<&SandboxStagingOwner>,
    this_node: &str,
) -> bool {
    match owner {
        // CR gone: orphaned subtree, reclaim.
        None => false,
        Some(owner) => {
            // Phase gate first: an idle/terminal phase reclaims regardless of
            // which node the (former) Pod was on.
            if !staging_bundle_needed(Some(owner.phase)) {
                return false;
            }
            // Locality gate: reclaim only when the live Pod is on a *known*
            // other node; keep when it is on this node or its host is not yet
            // known (pre-bind), never reclaiming on incomplete information.
            match owner.assigned_node.as_deref() {
                None => true,
                Some(node) => node == this_node,
            }
        }
    }
}

/// Looks up the [`SandboxStagingOwner`] (phase + assigned node) of a `Sandbox`
/// by name for the staging-dir sweep.
///
/// Factored behind a trait so [`sweep_staging_dirs`] is exhaustively
/// unit-testable without standing up a kube API server: tests pass a fake
/// that maps sandbox ids to owner state. The production impl is
/// [`KubeSandboxOwnerLookup`].
#[async_trait]
pub trait SandboxOwnerLookup: Send + Sync {
    /// The [`SandboxStagingOwner`] of the named `Sandbox`, or `None` if the
    /// `Sandbox` CR does not exist (the common reclaim trigger — a killed
    /// sandbox is deleted, orphaning its staging subtree).
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Transient`] on a kube transport error so the
    /// sweep skips this tick and retries rather than reclaiming a bundle on
    /// incomplete information.
    async fn owner_of(&self, sandbox_id: &str)
        -> Result<Option<SandboxStagingOwner>, RuntimeError>;
}

/// Production [`SandboxOwnerLookup`] over a namespaced `kube::Api<Sandbox>`.
///
/// **Single-namespace assumption (load-bearing for data safety).** This binds
/// to exactly one namespace (the node's `SANDBAWKS_NAMESPACE`). The sweep
/// treats an `owner_of` of `Ok(None)` — "no such `Sandbox` CR in this
/// namespace" — as "owner deleted, reclaim the bundle". That is correct only
/// while every sandbox whose bundle can land in this node's staging root lives
/// in the bound namespace, which holds today: the controller is single-
/// namespace and a node hosts only its own namespace's sandboxes. If a node
/// ever hosts staged bundles for sandboxes in *another* namespace, a still-
/// `Running` sandbox there would read back as `Ok(None)` and have its bundle
/// reclaimed mid-run — silent data loss. A multi-namespace deployment must
/// widen this lookup (e.g. an all-namespaces `Api` or a per-bundle namespace
/// hint) before the sweeper can be trusted across namespaces.
pub struct KubeSandboxOwnerLookup {
    api: Api<Sandbox>,
}

impl KubeSandboxOwnerLookup {
    /// Bind a lookup to the `Sandbox` CRs in `namespace`. See the type-level
    /// note: the bound namespace must contain every sandbox whose bundle can
    /// reach this node's staging root, or the sweep can reclaim a live
    /// sandbox's bundle.
    pub fn new(client: KubeClient, namespace: &str) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
        }
    }
}

#[async_trait]
impl SandboxOwnerLookup for KubeSandboxOwnerLookup {
    async fn owner_of(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<SandboxStagingOwner>, RuntimeError> {
        // `get_opt` distinguishes absent (`Ok(None)` → CR deleted → reclaim)
        // from a transport error (propagated → skip this sweep tick).
        let sandbox = self.api.get_opt(sandbox_id).await.map_err(|e| {
            RuntimeError::Transient(format!("get sandbox {sandbox_id} for staging sweep: {e}"))
        })?;
        // An existing CR with no `status` block yet is a brand-new sandbox:
        // `unwrap_or_default()` yields phase `Creating` (and no assigned node),
        // which `staging_bundle_retained_on_node` treats as a reclaim — correct,
        // since `Creating` never restores from a staged bundle.
        Ok(sandbox.map(|s| {
            let status = s.status.unwrap_or_default();
            SandboxStagingOwner {
                phase: status.phase,
                assigned_node: status.assigned_node,
            }
        }))
    }
}

/// Outcome of one [`sweep_staging_dirs`] pass, for logging and test
/// assertions. Names are the `<sandbox-id>` staging subtrees the sweep
/// looked at, bucketed by the action taken.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StagingSweepReport {
    /// Subtrees reclaimed (owner absent, no longer needs lazy page reads, or
    /// its live Pod moved to another node).
    pub reclaimed: Vec<String>,
    /// Subtrees kept (owner still `Running`/`Resuming`/`Pausing` *and* hosted
    /// on this node, or its host node not yet known).
    pub kept: Vec<String>,
}

/// Reclaim staged lazy-restore bundles whose owning `Sandbox` no longer needs
/// them *on this node*, keying retention on the sandbox's phase **and** the
/// node hosting its Pod rather than on `SnapshotJob` deletion.
///
/// Each immediate child of `staging_root` is a `<sandbox-id>` subtree (see
/// [`RuntimeAdapter::stage_snapshot`]'s
/// `<staging_root>/<sandbox-id>/<job-segment>/` layout). For each, the owning
/// sandbox's [`SandboxStagingOwner`] (phase + assigned node) is resolved via
/// `lookup` and the whole subtree is removed iff
/// [`staging_bundle_retained_on_node`] (against `this_node`) returns `false` —
/// i.e. the phase no longer needs the bundle, or the live Pod has moved to a
/// different node (issue #483: resume retries are scheduler-placed and may
/// re-stage on another node, orphaning this node's copy).
///
/// Processing is **best-effort per subtree**: a stat error, a `lookup`
/// transport error, or a removal error on one subtree is logged and skipped,
/// never aborting the pass, so one bad inode or one unreachable sandbox can't
/// starve the others of a reclaim this tick. A stat / lookup failure keeps the
/// subtree (the safe default — we never reclaim on incomplete information); a
/// concurrent reclaim that already deleted the subtree is treated as success.
/// `run_staging_sweeper` re-runs the whole pass on the next tick.
///
/// A missing `staging_root` (nothing staged on this node yet) is an empty
/// sweep, not an error.
///
/// # Errors
///
/// Returns [`RuntimeError::Transient`] only for an error reading
/// `staging_root` itself — opening it or iterating its entries — which
/// prevents the pass from running at all. Per-subtree errors (stat, `lookup`,
/// removal) are logged and skipped, not propagated.
pub async fn sweep_staging_dirs(
    staging_root: &Path,
    lookup: &dyn SandboxOwnerLookup,
    this_node: &str,
) -> Result<StagingSweepReport, RuntimeError> {
    let mut report = StagingSweepReport::default();

    let mut entries = match tokio::fs::read_dir(staging_root).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(e) => {
            return Err(RuntimeError::Transient(format!(
                "read staging root {}: {e}",
                staging_root.display()
            )))
        }
    };

    while let Some(entry) = entries.next_entry().await.map_err(|e| {
        RuntimeError::Transient(format!(
            "iterate staging root {}: {e}",
            staging_root.display()
        ))
    })? {
        let path = entry.path();
        // Skip stray non-directory files defensively; the layout only ever
        // creates `<sandbox-id>` directories directly under the root.
        // Best-effort per subtree: a stat failure on one entry is logged and
        // skipped, never aborting the pass, so a single bad inode can't starve
        // the other subtrees of a reclaim this tick.
        let file_type = match entry.file_type().await {
            Ok(ft) => ft,
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "staging sweep: stat failed; skipping entry this tick"
                );
                continue;
            }
        };
        if !file_type.is_dir() {
            continue;
        }
        let sandbox_id = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(os) => {
                tracing::warn!(
                    path = %Path::new(&os).display(),
                    "staging sweep: skipping non-UTF-8 staging dir name"
                );
                continue;
            }
        };

        // Best-effort per subtree: a transient kube transport error on one
        // sandbox's lookup is logged and skipped, never aborting the pass, so
        // one unreachable sandbox can't starve the others of a reclaim this
        // tick. Skipping leaves the subtree in place — the safe default, since
        // we never reclaim on incomplete information.
        let owner = match lookup.owner_of(&sandbox_id).await {
            Ok(owner) => owner,
            Err(e) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %e,
                    "staging sweep: owner lookup failed; keeping bundle this tick"
                );
                continue;
            }
        };
        if staging_bundle_retained_on_node(owner.as_ref(), this_node) {
            report.kept.push(sandbox_id);
            continue;
        }

        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    owner = ?owner,
                    this_node = %this_node,
                    staging_dir = %path.display(),
                    "staging sweep: reclaimed lazy-restore bundle"
                );
                report.reclaimed.push(sandbox_id);
            }
            // A concurrent reclaim / manual cleanup already removed it: the
            // post-condition ("subtree is gone") holds, so count it reclaimed.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                report.reclaimed.push(sandbox_id);
            }
            Err(e) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    staging_dir = %path.display(),
                    error = %e,
                    "staging sweep: failed to reclaim staging dir; will retry next sweep"
                );
            }
        }
    }

    Ok(report)
}

/// Run [`sweep_staging_dirs`] on a fixed interval, forever.
///
/// Supervised like the envd-staged label reconciler (#282): this never
/// returns under normal operation, so the binary treats its resolution as a
/// fatal death and exits non-zero for kubelet to restart the Pod. A
/// per-tick sweep error is logged and retried on the next tick — only a
/// panic ends the loop.
pub async fn run_staging_sweeper(
    staging_root: PathBuf,
    lookup: Arc<dyn SandboxOwnerLookup>,
    this_node: String,
    interval: std::time::Duration,
) {
    // `Skip` missed-tick behavior: if a sweep ever runs long, coalesce the
    // backlog to a single catch-up tick rather than bursting (the reclaim is
    // not time-critical).
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match sweep_staging_dirs(&staging_root, lookup.as_ref(), &this_node).await {
            Ok(report) if !report.reclaimed.is_empty() => tracing::info!(
                reclaimed = report.reclaimed.len(),
                kept = report.kept.len(),
                "staging sweep reclaimed lazy-restore bundles"
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "staging sweep failed; retrying next tick"),
        }
    }
}

/// Test doubles for [`ContainerdHandle`] / [`EnvdStateClient`] /
/// [`Clock`]. Exposed publicly (gated on `#[cfg(any(test, feature = "testing"))]`
/// would be cleaner, but the node crate has no `testing` feature yet)
/// so the integration test in `tests/gvisor.rs` and any future
/// downstream tests can construct a [`GvisorAdapter`] without a real
/// containerd / envd / wall clock.
pub mod fake {
    use std::sync::Mutex;

    use super::*;

    /// [`ContainerdHandle`] double that returns a canned
    /// [`PodResolution`] and a canned rootfs-delta byte buffer.
    ///
    /// The fields are public so tests can introspect what was returned
    /// without going through a getter; the type is intentionally
    /// minimal.
    pub struct FakeContainerd {
        /// Resolution returned for every `resolve_pod` call. Tests that
        /// need per-call behavior can subclass via the trait directly.
        pub resolution: PodResolution,
        /// Bytes returned for every `rootfs_delta` call.
        pub rootfs_delta_bytes: Vec<u8>,
        /// Records every `pod_uid` `resolve_pod` was invoked with so a
        /// test can assert exactly one call (idempotency check) without
        /// needing mockall or similar.
        pub resolve_calls: Mutex<Vec<String>>,
        /// Records every container_id `rootfs_delta` was invoked with.
        pub delta_calls: Mutex<Vec<String>>,
        /// Records every `(container_id, bytes)` pair the resume side
        /// passed to `apply_rootfs_delta`. The bytes recording lets
        /// the T8 resume test assert byte-identity round-trip from
        /// `SnapshotSource::get_part("rootfs-delta.tar.zst")` through
        /// to the snapshotter Apply entry point.
        pub apply_calls: Mutex<Vec<(String, Vec<u8>)>>,
        /// Mount set returned by every `container_mounts` call (#237).
        /// Defaults to empty (no extra mounts → the rootfs is the only
        /// state and is overlay-backed, so the pause-time assertion
        /// passes). Override via [`FakeContainerd::with_spec_mounts`] to
        /// drive the offending-mount path.
        pub spec_mounts: Vec<SpecMount>,
        /// Records every container_id `container_mounts` was invoked with.
        pub mounts_calls: Mutex<Vec<String>>,
        /// Value returned by `container_relocate_target` (epic #466). Defaults
        /// to `None` (legacy Pod, no relocate env → not capture-mode). Override
        /// via [`FakeContainerd::with_relocate_target`].
        pub relocate_target: Option<String>,
    }

    impl FakeContainerd {
        /// Build a fake that always returns `resolution` from
        /// `resolve_pod` and `rootfs_delta_bytes` from `rootfs_delta`.
        pub fn new(resolution: PodResolution, rootfs_delta_bytes: Vec<u8>) -> Self {
            Self {
                resolution,
                rootfs_delta_bytes,
                resolve_calls: Mutex::new(Vec::new()),
                delta_calls: Mutex::new(Vec::new()),
                apply_calls: Mutex::new(Vec::new()),
                spec_mounts: Vec::new(),
                mounts_calls: Mutex::new(Vec::new()),
                relocate_target: None,
            }
        }

        /// Override the mount set `container_mounts` returns so a test can
        /// exercise the #237 pause-time overlay-backing assertion.
        pub fn with_spec_mounts(mut self, mounts: Vec<SpecMount>) -> Self {
            self.spec_mounts = mounts;
            self
        }

        /// Set the relocate target `container_relocate_target` returns, marking
        /// the container capture-mode (epic #466). Default `None` (legacy).
        pub fn with_relocate_target(mut self, target: impl Into<String>) -> Self {
            self.relocate_target = Some(target.into());
            self
        }
    }

    #[async_trait]
    impl ContainerdHandle for FakeContainerd {
        async fn resolve_pod(&self, pod_uid: &str) -> Result<PodResolution, RuntimeError> {
            self.resolve_calls
                .lock()
                .expect("FakeContainerd poisoned")
                .push(pod_uid.to_owned());
            Ok(self.resolution.clone())
        }

        async fn rootfs_delta(&self, container_id: &str) -> Result<Vec<u8>, RuntimeError> {
            self.delta_calls
                .lock()
                .expect("FakeContainerd poisoned")
                .push(container_id.to_owned());
            Ok(self.rootfs_delta_bytes.clone())
        }

        async fn apply_rootfs_delta(
            &self,
            container_id: &str,
            bytes: Vec<u8>,
        ) -> Result<(), RuntimeError> {
            self.apply_calls
                .lock()
                .expect("FakeContainerd poisoned")
                .push((container_id.to_owned(), bytes));
            Ok(())
        }

        async fn container_mounts(
            &self,
            container_id: &str,
        ) -> Result<Vec<SpecMount>, RuntimeError> {
            self.mounts_calls
                .lock()
                .expect("FakeContainerd poisoned")
                .push(container_id.to_owned());
            Ok(self.spec_mounts.clone())
        }

        async fn container_relocate_target(
            &self,
            _container_id: &str,
        ) -> Result<Option<String>, RuntimeError> {
            Ok(self.relocate_target.clone())
        }
    }

    /// [`EnvdStateClient`] double returning canned JSON bytes.
    ///
    /// Supports optional failure injection for `quiesce` and `rearm` via
    /// [`FakeEnvdState::with_quiesce_fail`] and
    /// [`FakeEnvdState::with_rearm_fail`] so tests can verify the adapter
    /// tolerates best-effort failures on both paths without affecting the
    /// pause/resume outcome (#403).
    pub struct FakeEnvdState {
        /// Bytes returned for every `fetch_state` call.
        pub state_bytes: Vec<u8>,
        /// Records every `pod_ip` `fetch_state` was invoked with.
        pub fetch_calls: Mutex<Vec<String>>,
        /// Records every `pod_ip` `quiesce` was invoked with.
        pub quiesce_calls: Mutex<Vec<String>>,
        /// Records every `pod_ip` `rearm` was invoked with.
        pub rearm_calls: Mutex<Vec<String>>,
        /// When `true`, `quiesce` returns a [`RuntimeError::CheckpointFailed`].
        quiesce_fail: bool,
        /// When `true`, `rearm` returns a [`RuntimeError::RestoreFailed`].
        rearm_fail: bool,
        /// When `Some`, `quiesce` asserts this path does **not** yet exist.
        /// The runsc-checkpoint stub `touch`es it, so an absent marker proves
        /// quiesce ran *before* `runsc checkpoint` (#403 ordering guard).
        quiesce_marker_absent: Option<std::path::PathBuf>,
        /// When `Some`, `rearm` asserts this path **does** exist. The
        /// runsc-restore stub `touch`es it, so a present marker proves rearm
        /// ran *after* `runsc restore` + the bind wait (#403 ordering guard).
        rearm_marker_present: Option<std::path::PathBuf>,
    }

    impl FakeEnvdState {
        /// Build a fake returning `state_bytes` on every `fetch_state` call.
        pub fn new(state_bytes: Vec<u8>) -> Self {
            Self {
                state_bytes,
                fetch_calls: Mutex::new(Vec::new()),
                quiesce_calls: Mutex::new(Vec::new()),
                rearm_calls: Mutex::new(Vec::new()),
                quiesce_fail: false,
                rearm_fail: false,
                quiesce_marker_absent: None,
                rearm_marker_present: None,
            }
        }

        /// Configure `quiesce` to return an error on every call. Used to
        /// verify the pause path tolerates a failed quiesce (#403).
        pub fn with_quiesce_fail(mut self) -> Self {
            self.quiesce_fail = true;
            self
        }

        /// Configure `rearm` to return an error on every call. Used to
        /// verify the resume path tolerates a failed rearm (#403).
        pub fn with_rearm_fail(mut self) -> Self {
            self.rearm_fail = true;
            self
        }

        /// Arm a temporal-ordering guard: `quiesce` will assert `marker` does
        /// not yet exist when invoked. Pair with a checkpoint stub that
        /// `touch`es `marker` to prove quiesce precedes `runsc checkpoint`.
        pub fn with_quiesce_before_marker(mut self, marker: impl AsRef<std::path::Path>) -> Self {
            self.quiesce_marker_absent = Some(marker.as_ref().to_path_buf());
            self
        }

        /// Arm a temporal-ordering guard: `rearm` will assert `marker` already
        /// exists when invoked. Pair with a restore stub that `touch`es
        /// `marker` to prove rearm follows `runsc restore` + the bind wait.
        pub fn with_rearm_after_marker(mut self, marker: impl AsRef<std::path::Path>) -> Self {
            self.rearm_marker_present = Some(marker.as_ref().to_path_buf());
            self
        }
    }

    #[async_trait]
    impl EnvdStateClient for FakeEnvdState {
        async fn fetch_state(&self, pod_ip: &str) -> Result<Vec<u8>, RuntimeError> {
            self.fetch_calls
                .lock()
                .expect("FakeEnvdState poisoned")
                .push(pod_ip.to_owned());
            Ok(self.state_bytes.clone())
        }

        async fn quiesce(&self, pod_ip: &str) -> Result<(), RuntimeError> {
            if let Some(marker) = &self.quiesce_marker_absent {
                assert!(
                    !marker.exists(),
                    "quiesce must run before runsc checkpoint, but the checkpoint \
                     marker {} already exists",
                    marker.display()
                );
            }
            self.quiesce_calls
                .lock()
                .expect("FakeEnvdState poisoned")
                .push(pod_ip.to_owned());
            if self.quiesce_fail {
                return Err(RuntimeError::CheckpointFailed(
                    "FakeEnvdState: injected quiesce failure".into(),
                ));
            }
            Ok(())
        }

        async fn rearm(&self, pod_ip: &str) -> Result<(), RuntimeError> {
            if let Some(marker) = &self.rearm_marker_present {
                assert!(
                    marker.exists(),
                    "rearm must run after runsc restore, but the restore marker \
                     {} does not exist yet",
                    marker.display()
                );
            }
            self.rearm_calls
                .lock()
                .expect("FakeEnvdState poisoned")
                .push(pod_ip.to_owned());
            if self.rearm_fail {
                return Err(RuntimeError::RestoreFailed(
                    "FakeEnvdState: injected rearm failure".into(),
                ));
            }
            Ok(())
        }
    }

    /// [`Clock`] double returning a fixed RFC 3339 string. Pins the
    /// `created_at` timestamp + the work-dir path segment so the test
    /// can predict both.
    pub struct FixedClock {
        /// Timestamp returned by every `now_rfc3339` call.
        pub now: String,
    }

    impl FixedClock {
        /// Build a fixed clock returning `now` on every call.
        pub fn new(now: impl Into<String>) -> Self {
            Self { now: now.into() }
        }
    }

    impl Clock for FixedClock {
        fn now_rfc3339(&self) -> String {
            self.now.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real `cannot checkpoint container "<id>" in state stopped` stderr
    /// has its id scrubbed to a fixed placeholder, so the resulting message is
    /// stable across containers (#384).
    #[test]
    fn scrub_container_id_replaces_id_with_placeholder() {
        let id = "7cf67eb670c1f09950761ac6d29b5e815678c4caa7df80016bb3a26d641f10fb";
        let stderr =
            format!("checkpoint failed: cannot checkpoint container \"{id}\" in state stopped\n");
        let scrubbed = scrub_container_id(&stderr, id);
        assert!(!scrubbed.contains(id), "raw id must not survive scrubbing");
        assert_eq!(
            scrubbed,
            "checkpoint failed: cannot checkpoint container \"<container-id>\" in state stopped\n"
        );
    }

    /// The grouping property: two stderrs that differ only by container id
    /// scrub to byte-identical strings, so Traceway hashes them into one
    /// exception class (#384).
    #[test]
    fn scrub_container_id_makes_distinct_ids_group() {
        let id_a = "7cf67eb670c1f09950761ac6d29b5e815678c4caa7df80016bb3a26d641f10fb";
        let id_b = "e14aa2e185322979980954f6aea7b0b9a3ae184454200d1ab79905b5b17d29d8";
        let msg = |id: &str| format!("cannot checkpoint container \"{id}\" in state stopped");
        assert_eq!(
            scrub_container_id(&msg(id_a), id_a),
            scrub_container_id(&msg(id_b), id_b),
        );
    }

    /// The `<id>_sandbox:<sandbox-id>` full-id form is covered because it is
    /// prefixed by the bare id we replace.
    #[test]
    fn scrub_container_id_covers_full_id_form() {
        let id = "c42edeadbeef";
        let stderr = format!("loading container {id}_sandbox:bd70cafef00d: nope");
        let scrubbed = scrub_container_id(&stderr, id);
        assert!(!scrubbed.contains(id));
        assert_eq!(
            scrubbed,
            "loading container <container-id>_sandbox:bd70cafef00d: nope"
        );
    }

    /// An empty id (resolution never happened) is a no-op — guarding against
    /// `str::replace("", ..)` splicing the placeholder between every char.
    #[test]
    fn scrub_container_id_empty_is_noop() {
        let stderr = "some unrelated runsc error";
        assert_eq!(scrub_container_id(stderr, ""), stderr);
    }

    /// Minimal adapter whose only meaningful state is its runsc-root
    /// configuration; the containerd/envd fakes are never exercised by the
    /// runsc-root probe tests, so canned values suffice.
    fn adapter_for_root_probe() -> GvisorAdapter {
        let containerd = Arc::new(fake::FakeContainerd::new(
            PodResolution {
                container_id: "cid".into(),
                pod_ip: "10.0.0.1".into(),
                sandbox_id: "sbx".into(),
            },
            Vec::new(),
        ));
        let envd = Arc::new(fake::FakeEnvdState::new(b"{}".to_vec()));
        GvisorAdapter::new(containerd, envd, "/tmp/unused-work-root")
    }

    #[test]
    fn default_resume_readiness_probe_targets_http_port() {
        // Regression for #533, Bug 2: the resume-side `wait_for_tcp_bind` gate
        // must default to the HTTP port (49984) — which serves `/__rearm` and
        // `/__state` — not the gRPC port (49983), which answers before the Tokio
        // accept loop is scheduled and so reports a false-ready signal.
        let adapter = adapter_for_root_probe();
        assert_eq!(adapter.envd_port, ENVD_HTTP_PORT);
        assert_ne!(adapter.envd_port, ENVD_GRPC_PORT);
    }

    #[test]
    fn with_envd_port_overrides_resume_readiness_probe() {
        // The override seam tests rely on (binding an ephemeral listener) must
        // keep working after the #533 default change.
        let adapter = adapter_for_root_probe().with_envd_port(12345);
        assert_eq!(adapter.envd_port, 12345);
    }

    #[test]
    fn validate_level_label_maps_every_variant() {
        // The `node.validate.outcome_total{level}` label set is closed; assert
        // every variant maps to its stable lowercase string (#373), including a
        // round-trip through `parse_validate_level` for the string forms.
        assert_eq!(ValidateLevel::Off.label(), "off");
        assert_eq!(ValidateLevel::Head.label(), "head");
        assert_eq!(ValidateLevel::Sha.label(), "sha");
        assert_eq!(ValidateLevel::Full.label(), "full");
        assert_eq!(parse_validate_level("off").label(), "off");
        assert_eq!(parse_validate_level("sha256").label(), "sha");
        assert_eq!(parse_validate_level("full").label(), "full");
        // Unknown / empty falls back to the `Head` default.
        assert_eq!(parse_validate_level("").label(), "head");
        assert_eq!(parse_validate_level("bogus").label(), "head");
    }

    #[test]
    fn candidate_runsc_roots_lists_primary_then_fallbacks_deduped() {
        let adapter = adapter_for_root_probe()
            .with_runsc_root("/run/containerd/runsc/k8s.io")
            .with_extra_runsc_roots([
                // A duplicate of the primary must be dropped...
                "/run/containerd/runsc/k8s.io",
                // ...while a genuinely distinct fallback is preserved in order.
                "/run/stock-containerd/runsc/k8s.io",
            ]);
        assert_eq!(
            adapter.candidate_runsc_roots(),
            vec![
                PathBuf::from("/run/containerd/runsc/k8s.io"),
                PathBuf::from("/run/stock-containerd/runsc/k8s.io"),
            ],
        );
    }

    #[test]
    fn resolve_runsc_root_prefers_primary_when_it_holds_the_container() {
        let primary = tempfile::tempdir().expect("primary tempdir");
        let fallback = tempfile::tempdir().expect("fallback tempdir");
        // Both roots contain the container; the primary must win regardless.
        std::fs::create_dir(primary.path().join("cid")).unwrap();
        std::fs::create_dir(fallback.path().join("cid")).unwrap();

        let adapter = adapter_for_root_probe()
            .with_runsc_root(primary.path())
            .with_extra_runsc_roots([fallback.path().to_path_buf()]);

        assert_eq!(
            adapter
                .resolve_runsc_root("cid")
                .expect("primary holds container"),
            primary.path()
        );
    }

    #[test]
    fn resolve_runsc_root_selects_fallback_when_primary_is_empty() {
        // The k3s case: the configured primary exists but holds no container
        // state (the socket subtree), while the real state lives under a
        // per-environment fallback the operator mounted in.
        let primary = tempfile::tempdir().expect("primary tempdir");
        let fallback = tempfile::tempdir().expect("fallback tempdir");
        std::fs::create_dir(fallback.path().join("cid")).unwrap();

        let adapter = adapter_for_root_probe()
            .with_runsc_root(primary.path())
            .with_extra_runsc_roots([fallback.path().to_path_buf()]);

        assert_eq!(
            adapter
                .resolve_runsc_root("cid")
                .expect("fallback holds container"),
            fallback.path()
        );
    }

    /// Nothing holds the target container — neither candidates nor discovery.
    ///
    /// The old behavior was a silent warn-and-fallback to the configured primary,
    /// which let `runsc` emit an opaque "FetchSpec failed" error and turned every
    /// misconfigured pause into an infinite retry loop (#379). The new behavior
    /// returns a typed `RunscRootUnresolved` error so the reconciler can withdraw
    /// snapshot-capability advertising and break the loop.
    #[test]
    fn resolve_runsc_root_returns_err_when_no_candidate_matches() {
        let primary = tempfile::tempdir().expect("primary tempdir");
        let fallback = tempfile::tempdir().expect("fallback tempdir");

        let adapter = adapter_for_root_probe()
            .with_runsc_root(primary.path())
            .with_extra_runsc_roots([fallback.path().to_path_buf()]);

        let err = adapter
            .resolve_runsc_root("absent-cid")
            .expect_err("no candidate holds the container; must return Err");
        assert!(
            matches!(err, RuntimeError::RunscRootUnresolved { .. }),
            "expected RunscRootUnresolved, got {err:?}"
        );
    }

    /// Lay out a fake shim tree `<base>/runsc/<namespace>/<container_id>` and
    /// return the `<base>` tempdir. Mirrors the real
    /// `/run/containerd/runsc/k8s.io/<cid>` layout the discovery walk targets.
    fn shim_tree_with_container(namespace: &str, container_id: &str) -> tempfile::TempDir {
        let base = tempfile::tempdir().expect("search-base tempdir");
        let state = base.path().join("runsc").join(namespace).join(container_id);
        std::fs::create_dir_all(&state).expect("create runsc state dir");
        base
    }

    #[test]
    fn resolve_runsc_root_discovers_state_dir_under_search_dir() {
        // The #377 case: neither the primary nor any fallback holds the
        // container, but the operator pointed a search dir at the mounted
        // containerd run/state root. Discovery must find the real runsc root
        // (`<base>/runsc/k8s.io`) without that exact suffix being configured.
        let primary = tempfile::tempdir().expect("primary tempdir");
        let base = shim_tree_with_container("k8s.io", "cid");

        let adapter = adapter_for_root_probe()
            .with_runsc_root(primary.path())
            .with_runsc_root_search_dirs([base.path().to_path_buf()]);

        assert_eq!(
            adapter
                .resolve_runsc_root("cid")
                .expect("discovery finds container under search dir"),
            base.path().join("runsc").join("k8s.io"),
        );
    }

    #[test]
    fn discovery_ignores_containerd_task_bundle_false_positive() {
        // containerd's task bundle dir is *also* named by container id
        // (`<base>/io.containerd.runtime.v2.task/k8s.io/<cid>`). Requiring a
        // `runsc` path component keeps discovery from selecting it. With only
        // the task bundle present (no runsc tree), discovery finds nothing and
        // resolve returns `RunscRootUnresolved` — the caller must not silently
        // fall back to the configured primary and retry forever (#379).
        let primary = tempfile::tempdir().expect("primary tempdir");
        let base = tempfile::tempdir().expect("search-base tempdir");
        let bundle = base
            .path()
            .join("io.containerd.runtime.v2.task")
            .join("k8s.io")
            .join("cid");
        std::fs::create_dir_all(&bundle).expect("create task bundle dir");

        let adapter = adapter_for_root_probe()
            .with_runsc_root(primary.path())
            .with_runsc_root_search_dirs([base.path().to_path_buf()]);

        let err = adapter
            .resolve_runsc_root("cid")
            .expect_err("task bundle must not be selected; must return Err");
        assert!(
            matches!(err, RuntimeError::RunscRootUnresolved { .. }),
            "expected RunscRootUnresolved, got {err:?}"
        );
    }

    #[test]
    fn discovery_picks_runsc_tree_when_both_trees_present() {
        // When the runsc state dir and the (false-positive) task bundle coexist
        // under the same search root, discovery selects the runsc one.
        let base = shim_tree_with_container("k8s.io", "cid");
        let bundle = base
            .path()
            .join("io.containerd.runtime.v2.task")
            .join("k8s.io")
            .join("cid");
        std::fs::create_dir_all(&bundle).expect("create task bundle dir");

        let adapter = adapter_for_root_probe()
            .with_runsc_root(tempfile::tempdir().unwrap().path())
            .with_runsc_root_search_dirs([base.path().to_path_buf()]);

        assert_eq!(
            adapter
                .resolve_runsc_root("cid")
                .expect("runsc tree found; task bundle must not interfere"),
            base.path().join("runsc").join("k8s.io"),
        );
    }

    #[test]
    fn discovery_matches_real_k3s_layout_skipping_runc_and_task_bundle() {
        // Reproduces the VERIFIED k3s-libvirt layout under the stock containerd
        // run root the operator mounts + searches. runsc keeps a FLAT
        // `<root>/<cid>_sandbox:<sandbox>.state` file (NOT a `<cid>/` dir);
        // runc and the task bundle use `<cid>/` dirs on sibling subtrees that
        // lack a `runsc` path component. Discovery must select the runsc root.
        let cid = "c42e8401fb791733216660ad64a5f98a4674925e4c7bcf10dafbaaa72d3b1321";
        let base = tempfile::tempdir().expect("search-base tempdir");
        // runc runtime state (dir layout) + task bundle (dir) — both skipped
        // (no `runsc` path component).
        std::fs::create_dir_all(base.path().join("runc").join("k8s.io").join(cid)).unwrap();
        std::fs::create_dir_all(
            base.path()
                .join("io.containerd.runtime.v2.task")
                .join("k8s.io")
                .join(cid),
        )
        .unwrap();
        // runsc runtime state (real flat-file layout) — the match.
        let runsc_root = base.path().join("runsc").join("k8s.io");
        std::fs::create_dir_all(&runsc_root).unwrap();
        std::fs::write(
            runsc_root.join(format!("{cid}_sandbox:bd70d45c.state")),
            b"{}",
        )
        .unwrap();
        std::fs::write(runsc_root.join(format!("{cid}_sandbox:bd70d45c.lock")), b"").unwrap();
        std::fs::write(runsc_root.join("runsc-bd70d45c.sock"), b"").unwrap();

        let adapter = adapter_for_root_probe()
            .with_runsc_root(tempfile::tempdir().unwrap().path())
            .with_runsc_root_search_dirs([base.path().to_path_buf()]);

        assert_eq!(
            adapter.resolve_runsc_root(cid).expect("k3s layout found"),
            runsc_root
        );
    }

    #[test]
    fn resolve_runsc_root_detects_file_based_state_in_explicit_candidate() {
        // The #366 explicit-candidate path must recognize runsc's real flat-file
        // state layout, not just a legacy `<cid>/` directory (#377). A `.lock`
        // sibling and another container's `.state` must not confuse the match.
        let root = tempfile::tempdir().expect("root tempdir");
        let cid = "abc123def456abc123def456abc123def456abc123def456abc123def4560000";
        std::fs::write(root.path().join(format!("{cid}_sandbox:sb.state")), b"{}").unwrap();
        std::fs::write(root.path().join(format!("{cid}_sandbox:sb.lock")), b"").unwrap();
        std::fs::write(root.path().join("0000other_sandbox:sb.state"), b"{}").unwrap();

        let adapter = adapter_for_root_probe().with_runsc_root(root.path());
        assert_eq!(
            adapter
                .resolve_runsc_root(cid)
                .expect("flat-file state found in explicit candidate"),
            root.path()
        );
    }

    #[test]
    fn explicit_candidate_wins_over_discovery() {
        // An explicit fallback that holds the container short-circuits the more
        // expensive search, even when a search dir would also find it.
        let primary = tempfile::tempdir().expect("primary tempdir");
        let fallback = tempfile::tempdir().expect("fallback tempdir");
        std::fs::create_dir(fallback.path().join("cid")).unwrap();
        let base = shim_tree_with_container("k8s.io", "cid");

        let adapter = adapter_for_root_probe()
            .with_runsc_root(primary.path())
            .with_extra_runsc_roots([fallback.path().to_path_buf()])
            .with_runsc_root_search_dirs([base.path().to_path_buf()]);

        assert_eq!(
            adapter
                .resolve_runsc_root("cid")
                .expect("explicit fallback holds container"),
            fallback.path()
        );
    }

    #[test]
    fn find_runsc_root_respects_depth_bound() {
        // A runsc root buried below the depth bound is not found, so a
        // mis-pointed search dir can't trigger an unbounded full-subtree walk.
        let base = tempfile::tempdir().expect("base tempdir");
        // base/a/b/c/runsc/<cid>.state -> the `runsc` dir sits at depth 4.
        let runsc_root = base.path().join("a").join("b").join("c").join("runsc");
        std::fs::create_dir_all(&runsc_root).expect("create deep runsc root");
        std::fs::write(runsc_root.join("cid.state"), b"{}").unwrap();

        assert!(find_runsc_root(base.path(), "cid", 3).is_none());
        assert_eq!(
            find_runsc_root(base.path(), "cid", 6).as_deref(),
            Some(runsc_root.as_path()),
        );
    }

    #[test]
    fn sanitize_replaces_colons_and_slashes() {
        assert_eq!(
            sanitize_path_segment("2026-05-25T12:34:56+00:00"),
            "2026-05-25T12-34-56+00-00",
        );
        assert_eq!(sanitize_path_segment("a/b:c"), "a-b-c");
    }

    #[test]
    fn envd_state_round_trips_defaults() {
        let raw = b"{}";
        let parsed: EnvdState = serde_json::from_slice(raw).expect("default parse");
        assert!(parsed.revision.is_empty());
    }

    /// Writes the three required checkpoint images (all non-empty) into a
    /// fresh temp dir and returns the dir handle so the caller can mutate
    /// it before re-checking.
    fn write_valid_checkpoint() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for name in REQUIRED_CHECKPOINT_FILES {
            std::fs::write(dir.path().join(name), b"nonempty").unwrap();
        }
        dir
    }

    #[tokio::test]
    async fn verify_checkpoint_postconditions_accepts_three_nonempty_imgs() {
        let dir = write_valid_checkpoint();
        // A stray extra .img must not affect the verdict — only the three
        // required names are checked.
        std::fs::write(dir.path().join("extra.img"), b"x").unwrap();
        verify_checkpoint_postconditions(dir.path())
            .await
            .expect("valid checkpoint must pass");
    }

    #[tokio::test]
    async fn verify_checkpoint_postconditions_rejects_missing_file() {
        let dir = write_valid_checkpoint();
        std::fs::remove_file(dir.path().join("pages_meta.img")).unwrap();
        let err = verify_checkpoint_postconditions(dir.path())
            .await
            .expect_err("a missing required image must fail");
        match err {
            RuntimeError::CheckpointFailed(msg) => {
                assert!(
                    msg.contains("pages_meta.img"),
                    "error must name the file: {msg}"
                );
            }
            other => panic!("expected CheckpointFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn verify_checkpoint_postconditions_rejects_empty_file() {
        // An asymmetric truncation: pages.img is full but its metadata
        // index was truncated to zero bytes — the exact partial-checkpoint
        // shape #232's fail-closed gate exists to catch.
        let dir = write_valid_checkpoint();
        std::fs::write(dir.path().join("pages_meta.img"), b"").unwrap();
        let err = verify_checkpoint_postconditions(dir.path())
            .await
            .expect_err("an empty required image must fail");
        match err {
            RuntimeError::CheckpointFailed(msg) => {
                assert!(msg.contains("empty"), "error must explain emptiness: {msg}");
                assert!(
                    msg.contains("pages_meta.img"),
                    "error must name the file: {msg}"
                );
            }
            other => panic!("expected CheckpointFailed, got {other:?}"),
        }
    }

    #[test]
    fn tar_zstd_checkpoint_files_skips_non_img_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        // The three files real `runsc checkpoint --image-path` writes
        // for gVisor 20240603+ — see [`tar_zstd_checkpoint_files`].
        std::fs::write(dir.path().join("checkpoint.img"), b"sentry-state").unwrap();
        std::fs::write(dir.path().join("pages.img"), b"memoryfile-pages").unwrap();
        std::fs::write(dir.path().join("pages_meta.img"), b"memoryfile-meta").unwrap();
        // A non-.img file (e.g. a runsc debug log) that must not appear
        // in the tarball. Pre-fix this would have matched the old
        // `memory*` glob if named `memory-debug.log`; we pin the new
        // `*.img` extension filter with a file whose name would have
        // passed the old check too.
        std::fs::write(dir.path().join("memory-debug.log"), b"noise").unwrap();
        // And a regular non-img file that never matched anything.
        std::fs::write(dir.path().join("random-other"), b"hijkl").unwrap();

        let (archive_path, uncompressed) =
            tar_zstd_checkpoint_files_blocking(dir.path()).expect("tar+zstd");
        // The archive is staged to `memory.zst` inside the work-dir (#293),
        // not returned as a `Vec`.
        assert_eq!(
            archive_path,
            dir.path().join("memory.zst"),
            "archive must be staged as memory.zst in the work-dir"
        );
        // The uncompressed figure sums only the `.img` payload sizes
        // (`sentry-state` = 12, `memoryfile-pages` = 16, `memoryfile-meta`
        // = 15 → 43), excluding the decoy `.log`/other files and tar
        // framing. It is the working-set proxy the memory headroom gate
        // admits against (#243).
        assert_eq!(
            uncompressed, 43,
            "uncompressed total must sum the .img payload sizes, not the archive"
        );
        // Decompress and re-read the staged tarball from disk to confirm
        // only the three .img files made it through.
        let archive = std::fs::File::open(&archive_path).expect("open staged archive");
        let zstd_reader = zstd::stream::read::Decoder::new(archive).expect("zstd decoder");
        let mut tar_reader = tar::Archive::new(zstd_reader);
        let mut names: Vec<String> = Vec::new();
        for entry in tar_reader.entries().expect("tar entries") {
            let entry = entry.expect("tar entry");
            let name = entry
                .path()
                .expect("tar entry path")
                .to_string_lossy()
                .into_owned();
            names.push(name);
        }
        names.sort();
        assert_eq!(names, vec!["checkpoint.img", "pages.img", "pages_meta.img"],);
    }

    /// Build a `SpecMount` tersely for the classifier tests. Source is left
    /// empty; the source-in-error behavior has its own dedicated test.
    fn mount(destination: &str, mount_type: &str, options: &[&str]) -> SpecMount {
        SpecMount {
            destination: destination.to_owned(),
            mount_type: mount_type.to_owned(),
            source: String::new(),
            options: options.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn mount_set_all_overlay_is_accepted() {
        // Every writable mount is overlay-backed (writes ride in the
        // sandbox MemoryFile), plus a read-only bind and the proc/sys
        // pseudo-fs that every spec carries — all safe.
        let mounts = vec![
            mount("/data", "overlay", &["rw"]),
            mount("/scratch", "overlay", &[]),
            mount("/etc/config", "bind", &["ro", "rbind"]),
            mount("/proc", "proc", &[]),
            mount("/sys", "sysfs", &["ro"]),
            mount("/sys/fs/cgroup", "cgroup2", &[]),
        ];
        assert!(mounts_lost_on_restore(&mounts).is_empty());
        assert_writable_mounts_overlay_backed(&mounts).expect("all-overlay set must be accepted");
    }

    #[test]
    fn writable_bind_mount_is_rejected_and_named() {
        let mounts = vec![
            mount("/data", "overlay", &["rw"]),
            mount("/host-state", "bind", &["rw", "rbind"]),
        ];
        let lost = mounts_lost_on_restore(&mounts);
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].destination, "/host-state");

        let err = assert_writable_mounts_overlay_backed(&mounts)
            .expect_err("a writable bind mount must fail the pause");
        match err {
            RuntimeError::CheckpointFailed(msg) => {
                assert!(
                    msg.contains("/host-state"),
                    "error must name the mount: {msg}"
                );
                assert!(msg.contains("bind"), "error must name the type: {msg}");
            }
            other => panic!("expected CheckpointFailed, got {other:?}"),
        }
    }

    #[test]
    fn writable_tmpfs_is_accepted_as_memoryfile_backed() {
        // gVisor tmpfs is sentry-internal and rides in `pages.img` (the
        // same mechanism the root overlay relies on), so it is captured —
        // not an escape. Covers both the `/dev` furniture tmpfs and a
        // `medium: Memory` emptyDir.
        let mounts = vec![
            mount("/dev", "tmpfs", &["rw", "nosuid"]),
            mount("/scratch-mem", "tmpfs", &["rw"]),
        ];
        assert!(mounts_lost_on_restore(&mounts).is_empty());
    }

    #[test]
    fn runtime_managed_furniture_binds_are_accepted() {
        // The full set of kubelet/CRI rw bind furniture (verified against a
        // live gVisor Pod's `crictl inspect` mount set) is re-materialized
        // or ephemeral on the restored Pod, so none may trip the gate even
        // though each is a writable, non-overlay bind. `/dev/shm` in
        // particular arrives as a `bind` (pod-level POSIX shm), NOT a
        // `tmpfs`, which is why it needs a destination exemption.
        let mounts = vec![
            mount("/etc/hosts", "bind", &["rw", "rbind"]),
            mount("/etc/hostname", "bind", &["rw", "rbind"]),
            mount("/etc/resolv.conf", "bind", &["rw", "rbind"]),
            mount("/dev/termination-log", "bind", &["rw", "rbind"]),
            mount("/dev/shm", "bind", &["rw", "rbind", "rprivate"]),
        ];
        assert!(mounts_lost_on_restore(&mounts).is_empty());
    }

    #[test]
    fn offending_mount_error_names_the_host_source() {
        // For a bind escape, the host source is what lets an operator see
        // *which* hostPath leaked — it must appear in the error (#241 review).
        let mounts = vec![SpecMount {
            destination: "/data".to_owned(),
            mount_type: "bind".to_owned(),
            source: "/host/secrets".to_owned(),
            options: vec!["rw".to_owned(), "rbind".to_owned()],
        }];
        let err = assert_writable_mounts_overlay_backed(&mounts).expect_err("offending bind");
        match err {
            RuntimeError::CheckpointFailed(msg) => {
                assert!(msg.contains("/data"), "must name destination: {msg}");
                assert!(
                    msg.contains("source=/host/secrets"),
                    "must name the host source: {msg}"
                );
            }
            other => panic!("expected CheckpointFailed, got {other:?}"),
        }
    }

    #[test]
    fn read_only_bind_mount_is_accepted() {
        // Read-only mounts have no writes to lose, regardless of type.
        let mounts = vec![mount("/image-config", "bind", &["ro", "rbind"])];
        assert!(mounts_lost_on_restore(&mounts).is_empty());
    }

    #[test]
    fn pseudo_filesystems_are_exempt_even_when_writable() {
        // proc/sys/cgroup/devpts/mqueue are writable but stateless — the
        // sentry rebuilds them on restore, so they must not trip the gate.
        let mounts = vec![
            mount("/proc", "proc", &["rw"]),
            mount("/dev/pts", "devpts", &["rw"]),
            mount("/dev/mqueue", "mqueue", &["rw"]),
            mount("/sys/fs/cgroup", "cgroup2", &["rw"]),
        ];
        assert!(mounts_lost_on_restore(&mounts).is_empty());
    }

    #[test]
    fn multiple_offenders_are_all_named() {
        let mounts = vec![
            mount("/a", "bind", &["rw"]),
            mount("/b", "bind", &["rw", "rbind"]),
            mount("/ok", "overlay", &["rw"]),
        ];
        let err = assert_writable_mounts_overlay_backed(&mounts).expect_err("two offenders");
        match err {
            RuntimeError::CheckpointFailed(msg) => {
                assert!(msg.contains("/a"), "must name /a: {msg}");
                assert!(msg.contains("/b"), "must name /b: {msg}");
                assert!(!msg.contains("/ok"), "must not name the safe mount: {msg}");
            }
            other => panic!("expected CheckpointFailed, got {other:?}"),
        }
    }

    #[test]
    fn parse_oci_mounts_extracts_destination_type_options() {
        let spec = br#"{
            "ociVersion": "1.0.0",
            "root": { "path": "rootfs" },
            "mounts": [
                { "destination": "/proc", "type": "proc", "source": "proc" },
                {
                    "destination": "/data",
                    "type": "bind",
                    "source": "/var/lib/host/data",
                    "options": ["rbind", "rw"]
                }
            ]
        }"#;
        let mounts = parse_oci_mounts(spec).expect("valid OCI spec parses");
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[1].destination, "/data");
        assert_eq!(mounts[1].mount_type, "bind");
        assert_eq!(mounts[1].source, "/var/lib/host/data");
        assert_eq!(mounts[1].options, vec!["rbind".to_owned(), "rw".to_owned()]);
        // The bind mount is the lone offender.
        let lost = mounts_lost_on_restore(&mounts);
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].destination, "/data");
    }

    #[test]
    fn parse_oci_mounts_tolerates_missing_mounts_array() {
        // A spec with no `mounts` key is a valid (if unusual) zero-mount
        // set, not a parse error.
        let mounts = parse_oci_mounts(br#"{"ociVersion": "1.0.0"}"#).expect("missing mounts is ok");
        assert!(mounts.is_empty());
    }

    #[test]
    fn parse_oci_mounts_rejects_malformed_json() {
        let err = parse_oci_mounts(b"not json").expect_err("malformed spec must fail-closed");
        assert!(matches!(err, RuntimeError::CheckpointFailed(_)));
    }

    #[test]
    fn parse_oci_relocate_target_reads_env_value() {
        // OCI stores process env as `KEY=VALUE` strings; the relocate target is
        // extracted by key (epic #466).
        let spec = br#"{
            "ociVersion": "1.0.0",
            "process": {
                "env": [
                    "PATH=/usr/bin",
                    "SANDBAWKS_ENVD_RELOCATE_TARGET=/var/tmp/envd/run/envd"
                ]
            }
        }"#;
        assert_eq!(
            parse_oci_relocate_target(spec).as_deref(),
            Some("/var/tmp/envd/run/envd")
        );
    }

    #[test]
    fn parse_oci_relocate_target_absent_is_none() {
        // A legacy Pod with no relocate env → None (not capture-mode), keeping
        // the fail-closed host-binary guard.
        let spec = br#"{"ociVersion":"1.0.0","process":{"env":["PATH=/usr/bin"]}}"#;
        assert_eq!(parse_oci_relocate_target(spec), None);
        // A spec with no `process` at all is likewise None, not a panic.
        assert_eq!(
            parse_oci_relocate_target(br#"{"ociVersion":"1.0.0"}"#),
            None
        );
    }

    #[test]
    fn parse_oci_relocate_target_malformed_json_is_none() {
        // Unlike `parse_oci_mounts`, a malformed spec degrades to None (legacy /
        // keep the host check) rather than failing the pause — the safe
        // direction for the capture-mode marker.
        assert_eq!(parse_oci_relocate_target(b"not json"), None);
    }

    // --- HttpEnvdProbe error-message grouping tests ---
    //
    // Traceway hashes on `exception.message` (the `RuntimeError` Display). Any
    // per-sandbox token in the message splits what is logically one failure class
    // into N one-off groups (one per pod IP). The tests below confirm that the
    // three timeout wrapper messages and both inner-helper connect-error messages
    // no longer embed the target address, and that two calls with distinct
    // addresses produce bit-for-bit identical Display strings (the grouping
    // property).

    /// `Duration::ZERO` fires `tokio::time::timeout` immediately, before the inner
    /// future's first poll, so this reliably exercises the timeout code path without
    /// needing a real network listener.
    #[tokio::test]
    async fn fetch_state_timeout_excludes_pod_ip() {
        let probe = HttpEnvdProbe {
            timeout: std::time::Duration::ZERO,
        };
        let err_a = probe
            .fetch_state("fdcd:4a83:f883:dd::7ab9")
            .await
            .expect_err("zero timeout must fire immediately");
        let err_b = probe
            .fetch_state("fdcd:4a83:f883:52::3f86")
            .await
            .expect_err("zero timeout must fire immediately");
        let msg_a = err_a.to_string();
        let msg_b = err_b.to_string();
        assert_eq!(
            msg_a, msg_b,
            "grouping property: different pod IPs must produce identical messages"
        );
        assert!(
            !msg_a.contains("fdcd"),
            "error must not embed pod IP prefix"
        );
        assert!(!msg_a.contains("49984"), "error must not embed envd port");
    }

    #[tokio::test]
    async fn quiesce_timeout_excludes_pod_ip() {
        let probe = HttpEnvdProbe {
            timeout: std::time::Duration::ZERO,
        };
        let err_a = probe
            .quiesce("fdcd:4a83:f883:dd::7ab9")
            .await
            .expect_err("zero timeout must fire immediately");
        let err_b = probe
            .quiesce("fdcd:4a83:f883:52::3f86")
            .await
            .expect_err("zero timeout must fire immediately");
        let msg_a = err_a.to_string();
        let msg_b = err_b.to_string();
        assert_eq!(
            msg_a, msg_b,
            "grouping property: different pod IPs must produce identical messages"
        );
        assert!(
            !msg_a.contains("fdcd"),
            "error must not embed pod IP prefix"
        );
        assert!(!msg_a.contains("49984"), "error must not embed envd port");
    }

    #[tokio::test]
    async fn rearm_timeout_excludes_pod_ip() {
        let probe = HttpEnvdProbe {
            timeout: std::time::Duration::ZERO,
        };
        let err_a = probe
            .rearm("fdcd:4a83:f883:dd::7ab9")
            .await
            .expect_err("zero timeout must fire immediately");
        let err_b = probe
            .rearm("fdcd:4a83:f883:52::3f86")
            .await
            .expect_err("zero timeout must fire immediately");
        let msg_a = err_a.to_string();
        let msg_b = err_b.to_string();
        assert_eq!(
            msg_a, msg_b,
            "grouping property: different pod IPs must produce identical messages"
        );
        assert!(
            !msg_a.contains("fdcd"),
            "error must not embed pod IP prefix"
        );
        assert!(!msg_a.contains("49984"), "error must not embed envd port");
    }

    /// Bind two ephemeral listeners, record their ports, then drop them so that
    /// subsequent connects are refused. Two distinct (IP, port) pairs that both
    /// fail at the connect step must produce the same error message.
    #[tokio::test]
    async fn fetch_state_inner_connect_error_excludes_addr() {
        let (port_a, port_b) = {
            let la = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            (
                la.local_addr().unwrap().port(),
                lb.local_addr().unwrap().port(),
            )
            // `la` and `lb` drop here; both ports are now closed.
        };
        let err_a = fetch_state_inner(&format!("127.0.0.1:{port_a}"))
            .await
            .expect_err("connect to closed port must fail");
        let err_b = fetch_state_inner(&format!("127.0.0.1:{port_b}"))
            .await
            .expect_err("connect to closed port must fail");
        let msg_a = err_a.to_string();
        let msg_b = err_b.to_string();
        assert_eq!(
            msg_a, msg_b,
            "grouping property: different addresses must produce identical messages"
        );
        assert!(!msg_a.contains("127.0.0.1"), "error must not embed IP");
        assert!(
            !msg_a.contains(&port_a.to_string()),
            "error must not embed port"
        );
    }

    #[tokio::test]
    async fn post_endpoint_inner_connect_error_excludes_addr() {
        let (port_a, port_b) = {
            let la = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            (
                la.local_addr().unwrap().port(),
                lb.local_addr().unwrap().port(),
            )
        };
        let err_a = post_endpoint_inner(&format!("127.0.0.1:{port_a}"), "/__quiesce", true)
            .await
            .expect_err("connect to closed port must fail");
        let err_b = post_endpoint_inner(&format!("127.0.0.1:{port_b}"), "/__quiesce", true)
            .await
            .expect_err("connect to closed port must fail");
        let msg_a = err_a.to_string();
        let msg_b = err_b.to_string();
        assert_eq!(
            msg_a, msg_b,
            "grouping property: different addresses must produce identical messages"
        );
        assert!(!msg_a.contains("127.0.0.1"), "error must not embed IP");
        assert!(
            !msg_a.contains(&port_a.to_string()),
            "error must not embed port"
        );
    }
}
