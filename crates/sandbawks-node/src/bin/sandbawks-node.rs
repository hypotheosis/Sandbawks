//! `sandbawks-node` binary entrypoint.
//!
//! Plan 05 T9 wires up the real reconcile loop:
//!
//! - Reads `NODE_NAME` (downward API; Plan 05 T12 sets it on the
//!   DaemonSet Pod) and the `SANDBAWKS_S3_*` family from env.
//! - Connects an `S3SnapshotSink` + `S3SnapshotSource` pair, a
//!   `GvisorAdapter` over a containerd UDS, and a
//!   `KubeSnapshotJobStatusStore` over an in-cluster `kube::Client`.
//! - Spawns the [`sandbawks_node::reconcile::run`] controller and
//!   blocks on it, exiting cleanly on SIGINT/SIGTERM.
//!
//! The legacy `--placeholder` flag survives so the Plan 04 Helm chart
//! (still passing `args: ["--placeholder"]` until T12 cuts over) keeps
//! Pods in Running rather than CrashLoopBackOff.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use kube::Client;
use s3::{creds::Credentials, Bucket, Region};
use sandbawks_node::azure_blob::AzureBlobStore;
use sandbawks_node::gvisor::{ContainerdClient, GvisorAdapter, HttpEnvdProbe, SystemClock};
use sandbawks_node::memory_pressure::{
    self, PressurePolicy, PressureWatcherConfig, SysCgroupMemoryReader, DEFAULT_CGROUP_ROOT,
    DEFAULT_HIGH_WATER_FRACTION, DEFAULT_INTERVAL_SECS,
};
use sandbawks_node::reconcile::{
    self, KubeSnapshotJobStatusStore, ReconcileContext, SnapshotJobStatusStore,
};
use sandbawks_node::s3::{BlobStore, S3SnapshotSink, S3SnapshotSource};
use sandbawks_node::sandbox_pressure::{
    self, PressureThresholds, SandboxPressureConfig, DEFAULT_CHECK_INTERVAL_MS,
    DEFAULT_CLEAR_RATIO, DEFAULT_CPU_WARN_FRACTION, DEFAULT_DISK_WARN_FRACTION,
    DEFAULT_MEM_WARN_FRACTION, DEFAULT_PROC_ROOT, DEFAULT_PSI_WARN_PERCENT,
    DEFAULT_REPORT_INTERVAL_MS, DEFAULT_WALK_INTERVAL_SECS,
};
use sandbawks_node::sandbox_stats::{SysCgroupPressureReader, SysProcNetReader, POD_NET_IFACE};
use sandbawks_types::runtime::RuntimeAdapter;
use sandbawks_types::snapshot::{SnapshotProvider, SnapshotSink, SnapshotSource};
use tokio::signal;

/// Default work-dir root for `runsc checkpoint` / `runsc restore`
/// staging. Production overrides via [`SANDBAWKS_WORK_ROOT_ENV`]; the
/// Plan 04 Helm chart mounts a hostPath here.
const DEFAULT_WORK_ROOT: &str = "/var/lib/sandbawks/snapshots";

/// Default UDS path the containerd gRPC client connects to. Matches
/// what `containerd` ships out of the box.
const DEFAULT_CONTAINERD_SOCKET: &str = "/run/containerd/containerd.sock";

/// Env var names. Centralized as constants so the Helm chart and this
/// file have a single source of truth for the public surface.
const NODE_NAME_ENV: &str = "NODE_NAME";
const NAMESPACE_ENV: &str = "SANDBAWKS_NAMESPACE";
const S3_ENDPOINT_ENV: &str = "SANDBAWKS_S3_ENDPOINT";
const S3_BUCKET_ENV: &str = "SANDBAWKS_S3_BUCKET";
const S3_REGION_ENV: &str = "SANDBAWKS_S3_REGION";
const S3_ACCESS_KEY_ENV: &str = "SANDBAWKS_S3_ACCESS_KEY";
const S3_SECRET_KEY_ENV: &str = "SANDBAWKS_S3_SECRET_KEY";
const S3_KEY_PREFIX_ENV: &str = "SANDBAWKS_S3_KEY_PREFIX";
const SNAPSHOT_PROVIDER_ENV: &str = "SANDBAWKS_SNAPSHOT_PROVIDER";
const AZURE_ACCOUNT_NAME_ENV: &str = "SANDBAWKS_AZURE_ACCOUNT_NAME";
const AZURE_ACCOUNT_KEY_ENV: &str = "SANDBAWKS_AZURE_ACCOUNT_KEY";
const AZURE_CONTAINER_ENV: &str = "SANDBAWKS_AZURE_CONTAINER";
const AZURE_ENDPOINT_ENV: &str = "SANDBAWKS_AZURE_ENDPOINT";
const AZURE_KEY_PREFIX_ENV: &str = "SANDBAWKS_AZURE_KEY_PREFIX";
const SANDBAWKS_WORK_ROOT_ENV: &str = "SANDBAWKS_WORK_ROOT";
const CONTAINERD_SOCKET_ENV: &str = "SANDBAWKS_CONTAINERD_SOCKET";
const RUNSC_ROOT_ENV: &str = "SANDBAWKS_RUNSC_ROOT";
const RUNSC_ROOT_FALLBACKS_ENV: &str = "SANDBAWKS_RUNSC_ROOT_FALLBACKS";
const RUNSC_ROOT_SEARCH_DIRS_ENV: &str = "SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS";
// Memory-pressure watcher (#307), opt-in via the chart's node.memoryPressure.*.
const MEM_PRESSURE_ENABLED_ENV: &str = "SANDBAWKS_MEMORY_PRESSURE_ENABLED";
const MEM_PRESSURE_CGROUP_ROOT_ENV: &str = "SANDBAWKS_MEMORY_PRESSURE_CGROUP_ROOT";
const MEM_PRESSURE_FRACTION_ENV: &str = "SANDBAWKS_MEMORY_PRESSURE_HIGH_WATER_FRACTION";
const MEM_PRESSURE_INTERVAL_ENV: &str = "SANDBAWKS_MEMORY_PRESSURE_INTERVAL_SECS";
// Sandbox-pressure watcher (epic #472), opt-in via node.sandboxPressure.*.
const SBP_ENABLED_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_ENABLED";
const SBP_CHECK_INTERVAL_MS_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_CHECK_INTERVAL_MS";
const SBP_REPORT_INTERVAL_MS_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_REPORT_INTERVAL_MS";
const SBP_WALK_INTERVAL_SECS_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_WALK_INTERVAL_SECS";
const SBP_CPU_WARN_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_CPU_WARN_FRACTION";
const SBP_MEM_WARN_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_MEM_WARN_FRACTION";
const SBP_DISK_WARN_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_DISK_WARN_FRACTION";
const SBP_PSI_WARN_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_PSI_WARN_PERCENT";
const SBP_CLEAR_RATIO_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_CLEAR_RATIO";
const SBP_CGROUP_ROOT_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_CGROUP_ROOT";
const SBP_PROC_ROOT_ENV: &str = "SANDBAWKS_SANDBOX_PRESSURE_PROC_ROOT";
// runsc version reporting + restart-free convergence (#546), configured by the
// chart's `node.runscReconcile.*` / `node.runscVersion` / `node.runscAutoReconcile`.
const RUNSC_RECONCILE_ENABLED_ENV: &str = "SANDBAWKS_RUNSC_RECONCILE_ENABLED";
const RUNSC_AUTO_RECONCILE_ENV: &str = "SANDBAWKS_RUNSC_AUTO_RECONCILE";
const RUNSC_DESIRED_VERSION_ENV: &str = "SANDBAWKS_RUNSC_VERSION";
const RUNSC_HOST_BIN_DIR_ENV: &str = "SANDBAWKS_RUNSC_HOST_BIN_DIR";
const RUNSC_BUNDLED_BIN_DIR_ENV: &str = "SANDBAWKS_RUNSC_BUNDLED_BIN_DIR";
const RUNSC_APT_SOURCE_ENV: &str = "SANDBAWKS_RUNSC_APT_SOURCE";
const RUNSC_APT_KEYRING_ENV: &str = "SANDBAWKS_RUNSC_APT_KEYRING";
// Privileged, restart-bearing in-place runsc fleet upgrade (#550), configured by
// the chart's `node.runscUpgrade.*` / `node.runscAutoUpgrade`.
const RUNSC_UPGRADE_ENABLED_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_ENABLED";
const RUNSC_AUTO_UPGRADE_ENV: &str = "SANDBAWKS_RUNSC_AUTO_UPGRADE";
const RUNSC_UPGRADE_LEASE_NAME_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_LEASE_NAME";
const RUNSC_UPGRADE_LEASE_DURATION_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_LEASE_DURATION_SECS";
const RUNSC_UPGRADE_NSENTER_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_NSENTER";
const RUNSC_UPGRADE_BUNDLED_INSTALLER_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_BUNDLED_INSTALLER";
const RUNSC_UPGRADE_STAGING_DIR_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_STAGING_DIR";
const RUNSC_UPGRADE_HOST_EXEC_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_HOST_EXEC";
const RUNSC_UPGRADE_SYSTEMD_UNIT_ENV: &str = "SANDBAWKS_RUNSC_UPGRADE_SYSTEMD_UNIT";

/// DaemonSet that drives sandbox pause/resume via `runsc checkpoint` +
/// S3-streamed snapshots. T9 wires up the real `SnapshotJob` reconciler;
/// `--placeholder` survives so the Plan 04 chart keeps Pods Running
/// until T12 cuts over to the production args.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Idle-loop mode used by the Plan 04 Helm chart's `node` DaemonSet
    /// until T12 swaps the args. Holds the process open against
    /// SIGTERM so kubelet does not crash-loop the Pod between T10
    /// (image published) and T12 (real binary args wired).
    #[arg(long)]
    placeholder: bool,

    /// Fail-closed resource-headroom guard (#238): the maximum fraction
    /// `(0.0, 1.0]` of the staging filesystem's available disk — and, on
    /// the in-process restore path, of available memory — a single
    /// snapshot may consume. A bundle exceeding this fails its SnapshotJob
    /// rather than risk ENOSPC/OOM. Also settable via the env var (clap's
    /// `env` attribute reads it when the flag is absent); defaulted
    /// conservatively by [`sandbawks_node::headroom::DEFAULT_MAX_FRACTION`].
    #[arg(
        long,
        env = "SANDBAWKS_HEADROOM_FRACTION",
        default_value_t = sandbawks_node::headroom::DEFAULT_MAX_FRACTION
    )]
    headroom_fraction: f64,

    /// Fail-closed resource-headroom guard (#238): the absolute minimum
    /// number of bytes that must remain free after staging/restoring a
    /// snapshot, independent of `--headroom-fraction` (both gates must
    /// pass). Also settable via `SANDBAWKS_HEADROOM_MIN_FREE_BYTES`;
    /// defaulted by [`sandbawks_node::headroom::DEFAULT_MIN_FREE_BYTES`].
    #[arg(
        long,
        env = "SANDBAWKS_HEADROOM_MIN_FREE_BYTES",
        default_value_t = sandbawks_node::headroom::DEFAULT_MIN_FREE_BYTES
    )]
    headroom_min_free_bytes: u64,

    /// Interval, in seconds, between `envd-staged` label reconcile ticks
    /// (#282). After the fail-closed boot advertise, a background task
    /// re-checks the staged binary every tick: it re-asserts the label
    /// while the binary is present and *withdraws* it the moment the
    /// binary disappears (ephemeral hostPath across a reboot, operator
    /// cleanup), so the controller's nodeAffinity gate stops scheduling
    /// sandboxes onto a node that can no longer host them. Also settable
    /// via `SANDBAWKS_ENVD_STAGED_RECONCILE_SECS`; defaults to
    /// [`sandbawks_node::node_label::DEFAULT_RECONCILE_INTERVAL`].
    ///
    /// Must be `>= 1`: `tokio::time::interval(Duration::ZERO)` panics, and
    /// because the reconciler runs as a supervised background task a `0`
    /// would otherwise orphan a dead reconciler — the exact stale-label
    /// fail-open #282 exists to prevent. The `range(1..)` parser rejects
    /// it at startup instead.
    #[arg(
        long,
        env = "SANDBAWKS_ENVD_STAGED_RECONCILE_SECS",
        value_parser = clap::value_parser!(u64).range(1..),
        default_value_t = sandbawks_node::node_label::DEFAULT_RECONCILE_INTERVAL.as_secs()
    )]
    envd_staged_reconcile_secs: u64,

    /// Interval, in seconds, between lazy-restore staging-dir sweeps (Plan
    /// 08 B2). With lazy / page-fault restore (Plan 08 B1) gVisor reads
    /// pages from a staged `--image-path` bundle for the resumed
    /// container's whole lifetime, so a bundle may only be reclaimed once
    /// its owning `Sandbox` leaves `Running`/`Resuming`. A background sweep
    /// reclaims orphaned bundles (owner re-paused, killed, or deleted) by
    /// keying retention to sandbox phase. Also settable via
    /// `SANDBAWKS_STAGING_SWEEP_SECS`; defaults to
    /// [`sandbawks_node::gvisor::DEFAULT_STAGING_SWEEP_INTERVAL`].
    ///
    /// Must be `>= 1` for the same reason `envd_staged_reconcile_secs` is:
    /// `tokio::time::interval(Duration::ZERO)` panics, and the sweeper is a
    /// supervised long-lived task.
    #[arg(
        long,
        env = "SANDBAWKS_STAGING_SWEEP_SECS",
        value_parser = clap::value_parser!(u64).range(1..),
        default_value_t = sandbawks_node::gvisor::DEFAULT_STAGING_SWEEP_INTERVAL.as_secs()
    )]
    staging_sweep_secs: u64,

    /// In-container root under which [`GvisorAdapter::stage_snapshot`]
    /// materializes a restore bundle as `<root>/<sandbox-id>/<job-segment>/`,
    /// the [`run_staging_sweeper`] reclaims orphaned bundles from, and
    /// `ensure_under_staging_root` confines every staged target to (issue #512).
    ///
    /// One flag feeds all three so they cannot desync. It MUST equal the host
    /// path the DaemonSet hostPath-mounts here (`mountPath == hostPath`) and the
    /// controller's `SANDBAWKS_RESTORE_STAGING_ROOT`, because the host-side shim
    /// reads the recorded `io.sandbawks/restore-image-path` directly off the node
    /// filesystem — Helm sources all three from `node.hostPathStagingCache` so
    /// they agree by construction. Must be absolute (asserted at startup): a
    /// relative root cannot resolve verbatim across the worker and the shim.
    /// Also settable via `SANDBAWKS_STAGING_ROOT`; defaults to
    /// [`sandbawks_node::gvisor::DEFAULT_STAGING_ROOT`].
    #[arg(
        long,
        env = "SANDBAWKS_STAGING_ROOT",
        default_value = sandbawks_node::gvisor::DEFAULT_STAGING_ROOT
    )]
    staging_root: PathBuf,

    /// In-container `--root` directory passed to every `runsc checkpoint`
    /// / `runsc restore` invocation: where the `io.containerd.runsc.v1`
    /// shim keeps each container's state dir. The hardcoded default
    /// ([`sandbawks_node::gvisor::DEFAULT_RUNSC_ROOT`],
    /// `/run/containerd/runsc/k8s.io`) matches the stock-containerd shim
    /// layout, but k3s/RKE2 place the shim's runsc state under a different
    /// subtree, so a pause there fails with `runsc checkpoint ... FetchSpec
    /// failed: loading container: file does not exist` even though the
    /// container ID is correct (issue #343). Override this to the real root
    /// (find it on a node with
    /// `find /run -maxdepth 4 -name "<container-id>" -type d`) and mount the
    /// matching host path into the DaemonSet so the directory is non-empty
    /// inside the container. Also settable via `SANDBAWKS_RUNSC_ROOT`.
    #[arg(
        long,
        env = "SANDBAWKS_RUNSC_ROOT",
        default_value = sandbawks_node::gvisor::DEFAULT_RUNSC_ROOT
    )]
    runsc_root: PathBuf,

    /// Additional in-container runsc `--root` candidates, probed in order
    /// *after* `--runsc-root` when the primary root does not hold the target
    /// container (#366). On some clusters the gVisor `io.containerd.runsc.v1`
    /// shim keeps its per-container state on a subtree other than
    /// `--runsc-root` even though the container ID is correct (e.g. k3s/RKE2
    /// relocate the containerd socket but the shim's runsc root keeps runsc's
    /// compiled-in default). Rather than bake any one distro's layout into the
    /// binary, list this environment's alternate in-container path(s) here and
    /// mount the matching host dir into the DaemonSet so the directory is
    /// visible. The node auto-selects whichever candidate actually holds the
    /// container, so the same image works across stock containerd, k3s, RKE2,
    /// etc. Colon-separated when set via `SANDBAWKS_RUNSC_ROOT_FALLBACKS`;
    /// empty by default (identical single-root behavior to before).
    #[arg(
        long,
        env = "SANDBAWKS_RUNSC_ROOT_FALLBACKS",
        value_delimiter = sandbawks_node::gvisor::RUNSC_ROOT_FALLBACKS_SEPARATOR
    )]
    runsc_root_fallbacks: Vec<PathBuf>,

    /// In-container directories the node searches to *discover* the runsc state
    /// root when neither `--runsc-root` nor `--runsc-root-fallbacks` holds the
    /// target container (#377). This is the distro-agnostic alternative to
    /// hand-listing exact roots: point it at this cluster's mounted containerd
    /// run/state root (e.g. the in-container path the host containerd run dir is
    /// mounted at) and the node recursively finds the `<root>/<container_id>`
    /// state dir wherever the gVisor shim placed it — so the same image works on
    /// stock containerd, k3s, RKE2, etc. without baking any distro's layout into
    /// the binary. The matching host path must be mounted into the DaemonSet for
    /// the directory to be visible. Colon-separated when set via
    /// `SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS`; empty by default (discovery off,
    /// identical behavior to before).
    #[arg(
        long,
        env = "SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS",
        value_delimiter = sandbawks_node::gvisor::RUNSC_ROOT_FALLBACKS_SEPARATOR
    )]
    runsc_root_search_dirs: Vec<PathBuf>,

    /// The gVisor runtime-handler name the node-readiness gate requires in this
    /// node's `status.runtimeHandlers` before advertising `envd-staged` (#417).
    /// A node whose containerd has not registered this handler reports a
    /// handler list without it, so the gate withholds the label and sandbox
    /// Pods are never scheduled there — rather than looping forever on
    /// `FailedCreatePodSandBox: no runtime for "runsc" is configured`.
    ///
    /// This is the `handler:` value declared by the `RuntimeClass: gvisor`
    /// resource, **not** a filesystem path: the kubelet reports it in Node
    /// status only once containerd's CRI plugin has the handler registered.
    /// Defaults to [`sandbawks_node::node_label::DEFAULT_GVISOR_RUNTIME_HANDLER`]
    /// (`runsc`); override only if the RuntimeClass handler is renamed. Also
    /// settable via `SANDBAWKS_GVISOR_RUNTIME_HANDLER`.
    #[arg(
        long,
        env = "SANDBAWKS_GVISOR_RUNTIME_HANDLER",
        default_value = sandbawks_node::node_label::DEFAULT_GVISOR_RUNTIME_HANDLER
    )]
    gvisor_runtime_handler: String,

    /// Interval, in seconds, between host-runsc version reconcile ticks (#546).
    /// Each tick reports the host `runsc --version` (Node label + info gauge),
    /// removes the legacy gVisor apt channel so a host `apt-get upgrade` can't
    /// bump runsc off its pin, and — only when `--runsc-auto-reconcile` is set
    /// and the node has no live sandboxes — atomically converges the host
    /// binary to the pinned version with no k3s restart. Also settable via
    /// `SANDBAWKS_RUNSC_RECONCILE_SECS`; defaults to
    /// [`sandbawks_node::runsc_reconcile::DEFAULT_RECONCILE_INTERVAL`].
    ///
    /// Must be `>= 1`: `tokio::time::interval(Duration::ZERO)` panics, and the
    /// reconciler is a supervised long-lived task.
    #[arg(
        long,
        env = "SANDBAWKS_RUNSC_RECONCILE_SECS",
        value_parser = clap::value_parser!(u64).range(1..),
        default_value_t = sandbawks_node::runsc_reconcile::DEFAULT_RECONCILE_INTERVAL.as_secs()
    )]
    runsc_reconcile_secs: u64,

    /// Interval, in seconds, between privileged runsc fleet-upgrade ticks (#550).
    /// Each tick checks whether this drained node should run the restart-bearing
    /// host installer (CRI handler rewrite + k3s restart), gated on the
    /// cluster-wide one-at-a-time upgrade lease. Only acts when
    /// `node.runscAutoUpgrade` is set; see `crates/sandbawks-node/src/runsc_upgrade.rs`.
    /// Also settable via `SANDBAWKS_RUNSC_UPGRADE_SECS`; defaults to
    /// [`sandbawks_node::runsc_upgrade::DEFAULT_UPGRADE_INTERVAL`].
    ///
    /// Must be `>= 1`: `tokio::time::interval(Duration::ZERO)` panics, and the
    /// upgrader is a supervised long-lived task.
    #[arg(
        long,
        env = "SANDBAWKS_RUNSC_UPGRADE_SECS",
        value_parser = clap::value_parser!(u64).range(1..),
        default_value_t = sandbawks_node::runsc_upgrade::DEFAULT_UPGRADE_INTERVAL.as_secs()
    )]
    runsc_upgrade_secs: u64,

    /// Which pause/resume runtime adapter this node drives (epic #594).
    ///
    /// - `gvisor` (default) — `runsc checkpoint`/`restore`, the existing path.
    /// - `kata` / `kata-fc` — Kata: cold start works, but pause/resume is
    ///   **unsupported** and fails closed (a CH-direct snapshot destroys the
    ///   shim-managed VM, validated on AKS; see `crates/sandbawks-node/src/kata.rs`
    ///   and #594). Use on a Kata node pool; pair with `node.runtimeHandler: kata`
    ///   so the staging gate matches.
    ///
    /// Cluster-wide per DaemonSet, so a cluster mixing gVisor and Kata sandbox
    /// nodes runs a separate node deployment per pool. Also settable via
    /// `SANDBAWKS_RUNTIME_ADAPTER`.
    #[arg(long, env = "SANDBAWKS_RUNTIME_ADAPTER", default_value = "gvisor")]
    runtime_adapter: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI before initialising telemetry so `--help` / `--version` /
    // arg-parse errors short-circuit (clap calls `process::exit`, which does
    // not run the guard's `Drop`) *before* any OTLP exporter is built — and
    // so the four binaries share one ordering (`sandbawks-api` does the same).
    let args = Args::parse();

    // Initialise the shared OpenTelemetry bootstrap (logs, metrics, traces over
    // OTLP) and hold the guard for the whole of `main`. The guard MUST stay
    // bound to a named variable: the batch exporters flush asynchronously, so
    // dropping it early — or with `let _ = ...`, which drops immediately —
    // exports nothing. It flushes all three pipelines when `main` returns.
    //
    // With `OTEL_SDK_DISABLED` set (the local default) this installs only the
    // stderr fmt subscriber and builds no exporter, so a `cargo run` smoke
    // needs no collector. Root spans for the reconcile loop come from
    // `#[instrument]` (Plan 04 Phase 4), not a `TraceLayer`; this phase only
    // establishes the provider + subscriber.
    //
    // Ordering note (mirrors `sandbawks-api`): the first OTLP exporter is built
    // here, *before* `Client::try_default()` below resolves in-cluster
    // credentials, which can take non-trivial time. If you set
    // `OTEL_EXPORTER_OTLP_TIMEOUT`, keep it generous enough to cover that kube
    // startup or the earliest exports can spuriously time out during boot.
    // The re-arm handle is only used by checkpoint/restore-capable binaries
    // (envd); the node never pauses, so it ignores it.
    let (_telemetry, _rearm) =
        sandbawks_observability::init_telemetry("sandbawks-node", env!("CARGO_PKG_VERSION"))
            .context("initialising OpenTelemetry SDK")?;

    if args.placeholder {
        tracing::info!("sandbawks-node started in --placeholder mode; waiting for SIGTERM");
        let _ = signal::ctrl_c().await;
        tracing::info!("SIGINT/SIGTERM received, exiting placeholder mode");
        return Ok(());
    }

    let node_name = std::env::var(NODE_NAME_ENV)
        .with_context(|| format!("{NODE_NAME_ENV} must be set (downward API)"))?;

    let work_root = std::env::var(SANDBAWKS_WORK_ROOT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_WORK_ROOT));
    let containerd_socket =
        std::env::var(CONTAINERD_SOCKET_ENV).unwrap_or_else(|_| DEFAULT_CONTAINERD_SOCKET.into());

    let kube_client = Client::try_default()
        .await
        .context("building in-cluster kube client")?;

    // Namespace we watch SnapshotJob CRs in. An explicit `SANDBAWKS_NAMESPACE`
    // override wins; otherwise fall back to the namespace this Pod actually
    // runs in (the in-cluster service-account namespace the kube client was
    // configured from) rather than a hard-coded "default". The node, the
    // controller, and the SnapshotJob CRs are always co-located, so the Pod's
    // own namespace is the correct watch target. A bare "default" fallback let
    // a DaemonSet that forgot to inject SANDBAWKS_NAMESPACE silently watch the
    // wrong namespace, so every pause SnapshotJob went unclaimed and warm-pool
    // members hung in `Pausing` forever. `default_namespace()` reads
    // /var/run/secrets/kubernetes.io/serviceaccount/namespace in-cluster.
    let namespace =
        std::env::var(NAMESPACE_ENV).unwrap_or_else(|_| kube_client.default_namespace().to_owned());

    // #272/#417: advertise that this node can host sandboxes so the
    // controller's nodeAffinity gate lets sandbox Pods schedule here. The
    // `stage-envd` init container has already run (init containers must
    // succeed before this process starts), but `advertise_envd_staged`
    // re-asserts the staged binary is present *and* that the gVisor `runsc`
    // handler is registered in this node's containerd (via
    // `status.runtimeHandlers`) before labeling — either gap would let a
    // sandbox Pod schedule onto a node that then CrashLoops (#272) or loops
    // on `FailedCreatePodSandBox: no runtime for "runsc" is configured`
    // (#417). A missing binary fails closed by erroring (kubelet restarts
    // the Pod, re-running `stage-envd`); a missing runsc handler does NOT
    // error — restarting can't install runsc — it just doesn't advertise,
    // and the reconcile loop below re-asserts once runsc appears.
    use sandbawks_node::node_label::AdvertiseOutcome;
    let boot_outcome = sandbawks_node::node_label::advertise_envd_staged(
        kube_client.clone(),
        &node_name,
        std::path::Path::new(sandbawks_node::gvisor::DEFAULT_ENVD_BIN_PATH),
        &args.gvisor_runtime_handler,
    )
    .await
    .context("advertising envd-staged node label (#272, #417)")?;
    match boot_outcome {
        AdvertiseOutcome::Advertised => tracing::info!(
            node = %node_name,
            label = sandbawks_types::ENVD_STAGED_LABEL,
            "node advertised as envd-staged; sandbox Pods may now schedule here"
        ),
        AdvertiseOutcome::WithdrawnRunscAbsent => tracing::warn!(
            node = %node_name,
            label = sandbawks_types::ENVD_STAGED_LABEL,
            handler = %args.gvisor_runtime_handler,
            "node lacks the configured runtime handler '{}' in \
             status.runtimeHandlers; NOT advertising envd-staged{} (the \
             reconcile loop re-checks every tick) (#417)",
            args.gvisor_runtime_handler,
            sandbawks_node::node_label::runsc_remediation_hint(
                &args.gvisor_runtime_handler
            )
        ),
        // Boot can only Advertise or withhold on runsc-absence: the binary gate
        // errors out above (handled by `?`), and the sticky runsc-root gate
        // (#379) starts clear. These arms are unreachable but kept explicit
        // rather than swallowed by a wildcard.
        AdvertiseOutcome::Withdrawn | AdvertiseOutcome::WithdrawnRunscRoot => tracing::warn!(
            node = %node_name,
            outcome = ?boot_outcome,
            "unexpected boot advertise outcome; the reconcile loop governs from here"
        ),
    }

    // #379: shared flag — set by the SnapshotJob reconciler when
    // `RunscRootUnresolved` fires for a real container; read by the
    // envd-staged label reconciler each tick to keep the label withdrawn
    // until the DaemonSet is restarted with a corrected runsc root mount.
    // `Arc` lets both tasks hold a reference without a mutex.
    let runsc_root_failed: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    // #282: keep the label honest for the life of the process. The boot
    // advertise above is a one-shot snapshot; this background task
    // re-checks the staged binary every `envd_staged_reconcile_secs` and
    // withdraws the label if the binary later disappears, so a node whose
    // staging dir is on non-persistent storage can't strand a stale
    // "ready" label and let #272's CrashLoop recur. Also withdraws when
    // `runsc_root_failed` is set (#379). The `JoinHandle` is kept (not
    // detached) and supervised alongside the reconcile loop below: the
    // reconciler never returns under normal operation, so if its handle
    // ever resolves it has died (panic/cancel) and the process must exit
    // non-zero for kubelet to restart it rather than fail open.
    let envd_label_reconciler =
        tokio::spawn(sandbawks_node::node_label::run_envd_staged_reconciler(
            kube_client.clone(),
            node_name.clone(),
            PathBuf::from(sandbawks_node::gvisor::DEFAULT_ENVD_BIN_PATH),
            args.gvisor_runtime_handler.clone(),
            std::time::Duration::from_secs(args.envd_staged_reconcile_secs),
            Arc::clone(&runsc_root_failed),
        ));

    // #546: restart-free host runsc version reporting + legacy-apt-channel
    // cleanup + opt-in binary convergence. Reporting (Node label + info gauge)
    // and apt-channel disarm are read-only / always safe; the binary swap is
    // gated on `node.runscAutoReconcile` and a drained node. Spawned
    // unconditionally so the `select!` arm below stays static (mirrors the
    // pressure watchers); when the feature is disabled the task parks on a
    // never-ready future instead of waking every interval to no-op. Disabled is
    // the chart's `node.runscReconcile.enabled=false` path, where the host
    // mounts + the rest of the env are absent anyway.
    let runsc_reconciler = {
        let enabled = std::env::var(RUNSC_RECONCILE_ENABLED_ENV)
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            // Unset → enabled, matching the chart's `runscReconcile.enabled: true`
            // default; the chart emits this env explicitly for both states.
            .unwrap_or(true);
        let cfg = runsc_reconcile_config(
            &node_name,
            std::time::Duration::from_secs(args.runsc_reconcile_secs),
        );
        let reader: Arc<dyn sandbawks_node::runsc_reconcile::RunscVersionReader> =
            Arc::new(sandbawks_node::runsc_reconcile::ExecRunscVersionReader);
        let counter: Arc<dyn sandbawks_node::runsc_reconcile::LiveSandboxCounter> = Arc::new(
            sandbawks_node::runsc_reconcile::KubeLiveSandboxCounter::new(
                kube_client.clone(),
                &namespace,
                node_name.clone(),
            ),
        );
        let client = kube_client.clone();
        tokio::spawn(async move {
            if !enabled {
                tracing::info!(
                    "runsc version reconciler disabled; set \
                     {RUNSC_RECONCILE_ENABLED_ENV}=true to enable (#546)"
                );
                return std::future::pending::<()>().await;
            }
            sandbawks_node::runsc_reconcile::run_runsc_reconciler(client, cfg, reader, counter)
                .await
        })
    };

    // #550: opt-in, privileged, restart-bearing in-place runsc fleet upgrade
    // (Capability C). Spawned unconditionally so the `select!` arm stays static;
    // when the master toggle is off the task parks on a never-ready future. When
    // on, a drained node holding the cluster-wide upgrade lease runs the full
    // host installer (CRI handler rewrite + k3s restart) detached so it survives
    // the Pod the restart kills. The actual install is additionally gated on
    // `node.runscAutoUpgrade`; with the master on but auto-upgrade off the loop
    // only reports + releases a stale lease.
    let runsc_upgrader = {
        let enabled = env_flag(RUNSC_UPGRADE_ENABLED_ENV, false);
        let cfg = runsc_upgrade_config(
            &node_name,
            std::time::Duration::from_secs(args.runsc_upgrade_secs),
        );
        let reader: Arc<dyn sandbawks_node::runsc_reconcile::RunscVersionReader> =
            Arc::new(sandbawks_node::runsc_reconcile::ExecRunscVersionReader);
        let counter: Arc<dyn sandbawks_node::runsc_reconcile::LiveSandboxCounter> = Arc::new(
            sandbawks_node::runsc_reconcile::KubeLiveSandboxCounter::new(
                kube_client.clone(),
                &namespace,
                node_name.clone(),
            ),
        );
        let coordinator: Arc<dyn sandbawks_node::runsc_upgrade::UpgradeCoordinator> = Arc::new(
            sandbawks_node::runsc_upgrade::KubeLeaseUpgradeCoordinator::new(
                kube_client.clone(),
                &namespace,
                cfg.lease_name.clone(),
                node_name.clone(),
                cfg.lease_duration_secs,
                cfg.desired_version.clone(),
            ),
        );
        let runner: Arc<dyn sandbawks_node::runsc_upgrade::HostUpgradeRunner> =
            Arc::new(build_nsenter_runner());
        tokio::spawn(async move {
            if !enabled {
                tracing::info!(
                    "privileged runsc fleet-upgrader disabled; set \
                     {RUNSC_UPGRADE_ENABLED_ENV}=true to enable (#550)"
                );
                return std::future::pending::<()>().await;
            }
            sandbawks_node::runsc_upgrade::run_runsc_upgrader(
                cfg,
                reader,
                counter,
                coordinator,
                runner,
            )
            .await
        })
    };

    // Plan 08 B2: with lazy/page-fault restore (B1) the staged `--image-path`
    // bundle must outlive container start — gVisor reads pages from it for the
    // resumed container's whole lifetime. Reclaim is therefore keyed to the
    // owning Sandbox's phase, not to `SnapshotJob` deletion: this background
    // sweep removes a `<sandbox-id>` staging subtree once its sandbox is no
    // longer Running/Resuming (re-paused, killed, or the CR deleted). The
    // sweeper and the `GvisorAdapter` below are both handed the same
    // `args.staging_root` (issue #512) — one resolved value, so they can never
    // sweep / stage different trees. Supervised below alongside the reconcile
    // loop, identically to the #282 envd-staged reconciler.
    //
    // Fail fast on a relative root: the host-side shim reads the staged path
    // verbatim off the node filesystem, so a non-absolute root would stage to a
    // path the shim can't resolve and break every restore. Helm already renders
    // an absolute path, but a standalone/misconfigured invocation must not boot.
    if args.staging_root.is_relative() {
        anyhow::bail!(
            "--staging-root / SANDBAWKS_STAGING_ROOT must be an absolute path (got {}): \
             the restore shim reads it verbatim off the node filesystem",
            args.staging_root.display()
        );
    }
    let staging_lookup: Arc<dyn sandbawks_node::gvisor::SandboxOwnerLookup> = Arc::new(
        sandbawks_node::gvisor::KubeSandboxOwnerLookup::new(kube_client.clone(), &namespace),
    );
    let staging_sweeper = tokio::spawn(sandbawks_node::gvisor::run_staging_sweeper(
        args.staging_root.clone(),
        staging_lookup,
        node_name.clone(),
        std::time::Duration::from_secs(args.staging_sweep_secs),
    ));

    // #238: build the fail-closed headroom policy, validating the
    // operator-supplied fraction on the type itself (`try_new`). A
    // fraction outside (0.0, 1.0] would silently disable (> 1.0) or
    // universally trip (<= 0.0) the guard, so fail fast at startup.
    let headroom = sandbawks_node::headroom::HeadroomPolicy::try_new(
        args.headroom_fraction,
        args.headroom_min_free_bytes,
    )
    .map_err(|e| anyhow::anyhow!("--headroom-fraction: {e}"))?;

    // #343/#366: fail loud at boot if *none* of the configured runsc state
    // roots (primary + per-environment fallbacks) is present. On stock
    // containerd the default `/run/containerd/runsc/k8s.io` is correct, but
    // k3s/RKE2 place the shim's state elsewhere; with every root wrong, every
    // `runsc checkpoint` fails per-pause with the opaque "FetchSpec failed:
    // loading container: file does not exist". The check is candidate-aware so
    // an operator who leaves `runscRoot` at the default and supplies the real
    // root via `runscRootFallbacks` is not nagged every restart (#366 review).
    // A missing root is only warned (not fatal): the shim creates it lazily on
    // first gVisor container start, so a freshly-booted node that has not yet
    // run a sandbox legitimately has no such dir yet.
    warn_if_runsc_root_missing(
        &args.runsc_root,
        &args.runsc_root_fallbacks,
        &args.runsc_root_search_dirs,
    );

    let (sink, source, blob) =
        build_snapshot_pair().context("constructing snapshot sink/source")?;
    // Select the pause/resume adapter (#594). gVisor is the default; a Kata pool
    // gets the fail-closed Kata adapter (pause/resume unsupported — see
    // `kata::KataAdapter`). The choice is cluster-wide per DaemonSet.
    let adapter: Arc<dyn RuntimeAdapter> = match args.runtime_adapter.as_str() {
        "gvisor" => build_gvisor_adapter(
            &containerd_socket,
            AdapterFsRoots {
                work_root: &work_root,
                staging_root: &args.staging_root,
            },
            RunscRootConfig {
                primary: &args.runsc_root,
                fallbacks: &args.runsc_root_fallbacks,
                search_dirs: &args.runsc_root_search_dirs,
            },
            kube_client.clone(),
            &namespace,
            &node_name,
            headroom,
        )
        .await
        .context("constructing GvisorAdapter")?,
        "kata" | "kata-fc" => Arc::new(sandbawks_node::kata::KataAdapter::new()),
        other => anyhow::bail!(
            "unknown SANDBAWKS_RUNTIME_ADAPTER {other:?}: expected `gvisor`, `kata`, or `kata-fc`"
        ),
    };
    let status_store: Arc<dyn SnapshotJobStatusStore> =
        Arc::new(KubeSnapshotJobStatusStore::new(kube_client.clone()));

    let ctx = Arc::new(ReconcileContext {
        node_name: node_name.clone(),
        adapter,
        sink,
        source,
        status_store,
        runsc_root_failed,
    });

    tracing::info!(
        node = %node_name,
        namespace = %namespace,
        work_root = %work_root.display(),
        staging_root = %args.staging_root.display(),
        containerd = %containerd_socket,
        runsc_root = %args.runsc_root.display(),
        runsc_root_fallbacks = ?args.runsc_root_fallbacks,
        runsc_root_search_dirs = ?args.runsc_root_search_dirs,
        headroom_fraction = args.headroom_fraction,
        headroom_min_free_bytes = args.headroom_min_free_bytes,
        "sandbawks-node booting reconcile loop"
    );

    // Memory-pressure watcher (#307): opt-in host-side reaction that captures a
    // pressured sandbox's envd `/__state` to S3 before the OOM kill. Spawned
    // unconditionally so the `select!` arm below is static; when disabled the
    // task parks on a never-ready future and its arm never fires.
    let pressure_task = {
        let mp = memory_pressure_config()?;
        let client = kube_client.clone();
        let node = node_name.clone();
        let ns = namespace.clone();
        let s3_key_prefix = std::env::var(S3_KEY_PREFIX_ENV).unwrap_or_else(|_| "snapshots".into());
        tokio::spawn(async move {
            if !mp.enabled {
                tracing::info!(
                    "memory-pressure watcher disabled; set {MEM_PRESSURE_ENABLED_ENV}=true to enable (#307)"
                );
                return std::future::pending::<anyhow::Result<()>>().await;
            }
            let reader = Arc::new(SysCgroupMemoryReader::new(mp.cgroup_root));
            let envd = Arc::new(HttpEnvdProbe::new());
            let clock = Arc::new(SystemClock);
            memory_pressure::run_memory_pressure_watcher(
                client,
                reader,
                envd,
                blob,
                clock,
                PressureWatcherConfig {
                    node_name: node,
                    namespace: ns,
                    interval: mp.interval,
                    policy: mp.policy,
                    s3_key_prefix,
                },
            )
            .await
        })
    };

    // Sandbox-pressure watcher (epic #472): opt-in per-sandbox cpu/mem/disk/net
    // pressure telemetry. Spawned unconditionally so the `select!` arm is static;
    // when disabled the task parks on a never-ready future. When enabled it opens
    // its own containerd connection (PID + upperdir resolution) so it stays fully
    // decoupled from the pause/resume adapter.
    let sandbox_pressure_task = {
        let sbp = sandbox_pressure_settings()?;
        let client = kube_client.clone();
        let node = node_name.clone();
        let ns = namespace.clone();
        let socket = containerd_socket.clone();
        tokio::spawn(async move {
            if !sbp.enabled {
                tracing::info!(
                    "sandbox-pressure watcher disabled; set {SBP_ENABLED_ENV}=true to enable (#472)"
                );
                return std::future::pending::<anyhow::Result<()>>().await;
            }
            let grpc = containerd_client::Client::from_path(&socket)
                .await
                .with_context(|| format!("sandbox-pressure: connect containerd UDS {socket}"))?;
            let containerd = Arc::new(ContainerdClient::new(
                grpc,
                client.clone(),
                ns.clone(),
                node.clone(),
            ));
            let cgroup = Arc::new(SysCgroupPressureReader::new(sbp.cgroup_root.clone()));
            let memory = Arc::new(SysCgroupMemoryReader::new(sbp.cgroup_root));
            let net = Arc::new(SysProcNetReader::new(sbp.proc_root));
            let cfg = SandboxPressureConfig::from_intervals(
                node,
                ns,
                sbp.check_interval,
                sbp.report_interval,
                sbp.walk_interval,
                sbp.thresholds,
                POD_NET_IFACE.to_string(),
            );
            // The single `ContainerdClient` serves both runtime-agnostic traits.
            sandbox_pressure::run_sandbox_pressure_watcher(
                client,
                cgroup,
                memory,
                net,
                containerd.clone(),
                containerd,
                cfg,
            )
            .await
        })
    };

    // Supervise the envd-staged reconciler alongside the main reconcile
    // loop. Both run for the life of the process; whichever resolves
    // first ends the binary. A reconciler that resolves has died (it
    // never returns otherwise), so treat that as fatal — exiting non-zero
    // lets kubelet restart the Pod rather than running on with a dead
    // scheduling gate (the #282 fail-open).
    tokio::select! {
        result = reconcile::run(kube_client, namespace, ctx) => result?,
        joined = envd_label_reconciler => match joined {
            Ok(()) => anyhow::bail!(
                "envd-staged reconciler (#282) returned unexpectedly; \
                 a long-lived background loop must not terminate"
            ),
            Err(err) => return Err(anyhow::Error::new(err)
                .context("envd-staged reconciler task (#282) failed")),
        },
        joined = runsc_reconciler => match joined {
            Ok(()) => anyhow::bail!(
                "runsc version reconciler (#546) returned unexpectedly; \
                 a long-lived background loop must not terminate"
            ),
            Err(err) => return Err(anyhow::Error::new(err)
                .context("runsc version reconciler task (#546) failed")),
        },
        joined = runsc_upgrader => match joined {
            Ok(()) => anyhow::bail!(
                "runsc fleet-upgrader (#550) returned unexpectedly; \
                 a long-lived background loop must not terminate"
            ),
            Err(err) => return Err(anyhow::Error::new(err)
                .context("runsc fleet-upgrader task (#550) failed")),
        },
        joined = staging_sweeper => match joined {
            Ok(()) => anyhow::bail!(
                "staging-dir sweeper (Plan 08 B2) returned unexpectedly; \
                 a long-lived background loop must not terminate"
            ),
            Err(err) => return Err(anyhow::Error::new(err)
                .context("staging-dir sweeper task (Plan 08 B2) failed")),
        },
        joined = pressure_task => match joined {
            Ok(Ok(())) => anyhow::bail!(
                "memory-pressure watcher (#307) returned unexpectedly; \
                 a long-lived background loop must not terminate"
            ),
            Ok(Err(err)) => return Err(err.context("memory-pressure watcher (#307) failed")),
            Err(err) => return Err(anyhow::Error::new(err)
                .context("memory-pressure watcher task (#307) panicked")),
        },
        joined = sandbox_pressure_task => match joined {
            Ok(Ok(())) => anyhow::bail!(
                "sandbox-pressure watcher (#472) returned unexpectedly; \
                 a long-lived background loop must not terminate"
            ),
            Ok(Err(err)) => return Err(err.context("sandbox-pressure watcher (#472) failed")),
            Err(err) => return Err(anyhow::Error::new(err)
                .context("sandbox-pressure watcher task (#472) panicked")),
        },
    }
    Ok(())
}

/// Construct a paired [`S3SnapshotSink`] / [`S3SnapshotSource`] from the
/// `SANDBAWKS_S3_*` env family. Both halves share the same `rust-s3`
/// `Bucket` handle (one `Arc<dyn BlobStore>` under the hood) so HTTP
/// keepalives and the credential cache are reused across pause/resume.
/// The snapshot sink/source pair plus the shared `BlobStore` handle the
/// memory-pressure watcher (#307) puts one-off pressure artifacts through.
type S3Trio = (
    Arc<dyn SnapshotSink>,
    Arc<dyn SnapshotSource>,
    Arc<dyn BlobStore>,
);

/// Construct the paired snapshot sink/source (and shared `BlobStore` for the
/// memory-pressure watcher) for whichever backend
/// `SANDBAWKS_SNAPSHOT_PROVIDER` selects. Defaults to S3 so existing
/// deployments are unaffected.
fn build_snapshot_pair() -> Result<S3Trio> {
    let provider = std::env::var(SNAPSHOT_PROVIDER_ENV)
        .ok()
        .map(|v| v.parse::<SnapshotProvider>())
        .transpose()
        .context("parsing SANDBAWKS_SNAPSHOT_PROVIDER")?
        .unwrap_or_default();
    match provider {
        SnapshotProvider::S3 => build_s3_pair(),
        SnapshotProvider::Azure => build_azure_pair(),
    }
}

/// Resolved Azure backend settings, parsed from the `SANDBAWKS_AZURE_*`
/// env family. Split out from env reads so the validation is unit-testable.
struct AzureSnapshotConfig {
    account: String,
    access_key: String,
    container: String,
    /// `None` -> public Azure cloud; `Some(uri)` -> Azurite / sovereign cloud.
    endpoint: Option<String>,
    key_prefix: String,
}

impl AzureSnapshotConfig {
    /// Validate raw env values. `account`, `access_key`, and `container` are
    /// required; a missing one fails fast naming the env var so the operator
    /// sees the misconfiguration at node boot.
    fn from_values(
        account: Option<String>,
        access_key: Option<String>,
        container: Option<String>,
        endpoint: Option<String>,
        key_prefix: String,
    ) -> Result<Self> {
        let account = account
            .filter(|s| !s.is_empty())
            .with_context(|| format!("{AZURE_ACCOUNT_NAME_ENV} must be set for azure provider"))?;
        let access_key = access_key
            .filter(|s| !s.is_empty())
            .with_context(|| format!("{AZURE_ACCOUNT_KEY_ENV} must be set for azure provider"))?;
        let container = container
            .filter(|s| !s.is_empty())
            .with_context(|| format!("{AZURE_CONTAINER_ENV} must be set for azure provider"))?;
        Ok(Self {
            account,
            access_key,
            container,
            endpoint: endpoint.filter(|s| !s.is_empty()),
            key_prefix,
        })
    }

    /// Read the config straight from the process environment.
    fn from_env() -> Result<Self> {
        Self::from_values(
            std::env::var(AZURE_ACCOUNT_NAME_ENV).ok(),
            std::env::var(AZURE_ACCOUNT_KEY_ENV).ok(),
            std::env::var(AZURE_CONTAINER_ENV).ok(),
            std::env::var(AZURE_ENDPOINT_ENV).ok(),
            std::env::var(AZURE_KEY_PREFIX_ENV).unwrap_or_else(|_| "snapshots".into()),
        )
    }
}

// Manual `Debug` (NOT derived) so the Azure account key is never printed
// in cleartext if this struct is ever `{:?}`-formatted. CLAUDE.md: never
// log secrets.
impl std::fmt::Debug for AzureSnapshotConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureSnapshotConfig")
            .field("account", &self.account)
            .field("access_key", &"[redacted]")
            .field("container", &self.container)
            .field("endpoint", &self.endpoint)
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

/// Build the snapshot pair over [`AzureBlobStore`]. Reuses the generic
/// `S3SnapshotSink`/`S3SnapshotSource` (backend-agnostic) with an `azure://`
/// manifest-URL scheme.
fn build_azure_pair() -> Result<S3Trio> {
    let cfg = AzureSnapshotConfig::from_env()?;
    let store = AzureBlobStore::new(
        cfg.account,
        cfg.access_key,
        cfg.container.clone(),
        cfg.endpoint,
    )
    .map_err(|e| anyhow::anyhow!("build Azure blob store: {e}"))?;
    let blob: Arc<dyn BlobStore> = Arc::new(store);

    let sink: Arc<dyn SnapshotSink> = Arc::new(S3SnapshotSink::new_with_scheme(
        blob.clone(),
        cfg.container,
        cfg.key_prefix.clone(),
        "azure",
    ));
    let source: Arc<dyn SnapshotSource> =
        Arc::new(S3SnapshotSource::new(blob.clone(), cfg.key_prefix));
    Ok((sink, source, blob))
}

fn build_s3_pair() -> Result<S3Trio> {
    let endpoint =
        std::env::var(S3_ENDPOINT_ENV).with_context(|| format!("{S3_ENDPOINT_ENV} must be set"))?;
    let bucket_name =
        std::env::var(S3_BUCKET_ENV).with_context(|| format!("{S3_BUCKET_ENV} must be set"))?;
    let region_name = std::env::var(S3_REGION_ENV).unwrap_or_else(|_| "us-east-1".into());
    let access_key = std::env::var(S3_ACCESS_KEY_ENV)
        .with_context(|| format!("{S3_ACCESS_KEY_ENV} must be set"))?;
    let secret_key = std::env::var(S3_SECRET_KEY_ENV)
        .with_context(|| format!("{S3_SECRET_KEY_ENV} must be set"))?;
    let key_prefix = std::env::var(S3_KEY_PREFIX_ENV).unwrap_or_else(|_| "snapshots".into());

    let region = Region::Custom {
        region: region_name,
        endpoint,
    };
    let creds = Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)
        .map_err(|e| anyhow::anyhow!("build S3 credentials: {e}"))?;
    // `Bucket::new` returns `Box<Bucket>`; `with_path_style` returns a
    // fresh `Box<Bucket>` with `path_style = true`. Path-style addressing
    // matches MinIO's default (the production smoke target); AWS S3
    // also accepts path-style on modern endpoints.
    let bucket: Box<Bucket> = Bucket::new(&bucket_name, region, creds)
        .map_err(|e| anyhow::anyhow!("build S3 bucket: {e}"))?
        .with_path_style();
    // Unsize `Box<Bucket>` → `Box<dyn BlobStore>` first, then convert
    // into `Arc<dyn BlobStore>` (the `impl From<Box<T: ?Sized>> for
    // Arc<T>` blanket impl handles the rest). The single Arc is shared
    // between sink and source so they reuse the underlying HTTP pool.
    let bucket: Box<dyn sandbawks_node::s3::BlobStore> = bucket;
    let bucket_arc: Arc<dyn sandbawks_node::s3::BlobStore> = Arc::from(bucket);

    let sink: Arc<dyn SnapshotSink> = Arc::new(S3SnapshotSink::new(
        bucket_arc.clone(),
        bucket_name.clone(),
        key_prefix.clone(),
    ));
    let source: Arc<dyn SnapshotSource> =
        Arc::new(S3SnapshotSource::new(bucket_arc.clone(), key_prefix));
    // The same `Bucket` handle is also handed to the memory-pressure watcher
    // (#307) so it can `put` a one-off pressure artifact under its own key
    // without re-authenticating or opening a second HTTP pool.
    Ok((sink, source, bucket_arc))
}

/// Resolved memory-pressure watcher (#307) settings from the
/// `SANDBAWKS_MEMORY_PRESSURE_*` env family (set by the chart's
/// `node.memoryPressure.*`).
struct MemoryPressureConfig {
    /// Opt-in toggle; the watcher is off unless this is truthy.
    enabled: bool,
    /// Mounted host cgroup-v2 root to read pod-cgroup memory from.
    cgroup_root: PathBuf,
    /// Validated high-water policy.
    policy: PressurePolicy,
    /// Poll interval.
    interval: std::time::Duration,
}

/// Parse the memory-pressure env family, applying defaults and validating
/// bounds. Fails fast on a malformed fraction/interval so a misconfiguration
/// surfaces at boot rather than silently disabling the guard.
fn memory_pressure_config() -> Result<MemoryPressureConfig> {
    let enabled = std::env::var(MEM_PRESSURE_ENABLED_ENV)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let cgroup_root: PathBuf = std::env::var(MEM_PRESSURE_CGROUP_ROOT_ENV)
        .unwrap_or_else(|_| DEFAULT_CGROUP_ROOT.to_string())
        .into();
    let fraction = match std::env::var(MEM_PRESSURE_FRACTION_ENV) {
        Ok(s) => s
            .trim()
            .parse::<f64>()
            .with_context(|| format!("{MEM_PRESSURE_FRACTION_ENV} must be a float"))?,
        Err(_) => DEFAULT_HIGH_WATER_FRACTION,
    };
    let policy = PressurePolicy::try_new(fraction)
        .map_err(|e| anyhow::anyhow!("{MEM_PRESSURE_FRACTION_ENV}: {e}"))?;
    let interval_secs = match std::env::var(MEM_PRESSURE_INTERVAL_ENV) {
        Ok(s) => s
            .trim()
            .parse::<u64>()
            .with_context(|| format!("{MEM_PRESSURE_INTERVAL_ENV} must be a positive integer"))?,
        Err(_) => DEFAULT_INTERVAL_SECS,
    };
    if interval_secs == 0 {
        anyhow::bail!("{MEM_PRESSURE_INTERVAL_ENV} must be > 0");
    }
    Ok(MemoryPressureConfig {
        enabled,
        cgroup_root,
        policy,
        interval: std::time::Duration::from_secs(interval_secs),
    })
}

/// Resolved sandbox-pressure watcher (epic #472) settings from the
/// `SANDBAWKS_SANDBOX_PRESSURE_*` env family (set by the chart's
/// `node.sandboxPressure.*`). Mirrors [`MemoryPressureConfig`] — env-driven,
/// validated at boot.
struct SandboxPressureSettings {
    enabled: bool,
    cgroup_root: PathBuf,
    proc_root: PathBuf,
    check_interval: std::time::Duration,
    report_interval: std::time::Duration,
    walk_interval: std::time::Duration,
    thresholds: PressureThresholds,
}

/// Read an env var as `f64`, falling back to `default` when unset; errors only
/// on a present-but-unparseable value (fail-fast on a typo'd threshold).
fn env_f64(name: &str, default: f64) -> Result<f64> {
    match std::env::var(name) {
        Ok(s) => s
            .trim()
            .parse::<f64>()
            .with_context(|| format!("{name} must be a float")),
        Err(_) => Ok(default),
    }
}

/// Read an env var as `u64`, falling back to `default` when unset; errors only
/// on a present-but-unparseable value.
fn env_u64(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(s) => s
            .trim()
            .parse::<u64>()
            .with_context(|| format!("{name} must be a non-negative integer")),
        Err(_) => Ok(default),
    }
}

/// Parse + validate the sandbox-pressure env family. Fails fast on a malformed
/// threshold/interval so a misconfiguration surfaces at boot, not silently.
fn sandbox_pressure_settings() -> Result<SandboxPressureSettings> {
    let enabled = std::env::var(SBP_ENABLED_ENV)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let cgroup_root: PathBuf = std::env::var(SBP_CGROUP_ROOT_ENV)
        .unwrap_or_else(|_| DEFAULT_CGROUP_ROOT.to_string())
        .into();
    let proc_root: PathBuf = std::env::var(SBP_PROC_ROOT_ENV)
        .unwrap_or_else(|_| DEFAULT_PROC_ROOT.to_string())
        .into();

    let thresholds = PressureThresholds::try_new(
        env_f64(SBP_CPU_WARN_ENV, DEFAULT_CPU_WARN_FRACTION)?,
        env_f64(SBP_MEM_WARN_ENV, DEFAULT_MEM_WARN_FRACTION)?,
        env_f64(SBP_DISK_WARN_ENV, DEFAULT_DISK_WARN_FRACTION)?,
        env_f64(SBP_PSI_WARN_ENV, DEFAULT_PSI_WARN_PERCENT)?,
        env_f64(SBP_CLEAR_RATIO_ENV, DEFAULT_CLEAR_RATIO)?,
    )
    .map_err(|e| anyhow::anyhow!("sandbox-pressure thresholds: {e}"))?;

    let check_ms = env_u64(SBP_CHECK_INTERVAL_MS_ENV, DEFAULT_CHECK_INTERVAL_MS)?;
    let report_ms = env_u64(SBP_REPORT_INTERVAL_MS_ENV, DEFAULT_REPORT_INTERVAL_MS)?;
    let walk_secs = env_u64(SBP_WALK_INTERVAL_SECS_ENV, DEFAULT_WALK_INTERVAL_SECS)?;
    if check_ms == 0 {
        anyhow::bail!("{SBP_CHECK_INTERVAL_MS_ENV} must be > 0");
    }
    if report_ms == 0 {
        anyhow::bail!("{SBP_REPORT_INTERVAL_MS_ENV} must be > 0");
    }
    if walk_secs == 0 {
        anyhow::bail!("{SBP_WALK_INTERVAL_SECS_ENV} must be > 0");
    }

    Ok(SandboxPressureSettings {
        enabled,
        cgroup_root,
        proc_root,
        check_interval: std::time::Duration::from_millis(check_ms),
        report_interval: std::time::Duration::from_millis(report_ms),
        walk_interval: std::time::Duration::from_secs(walk_secs),
        thresholds,
    })
}

/// Default in-container dir where the host `/usr/local/bin` is mounted (#546).
/// A `/host`-prefixed path so it never shadows the node image's own
/// `/usr/local/bin` (which holds `sandbawks-node` + the *bundled* runsc).
const RUNSC_HOST_BIN_DIR_DEFAULT: &str = "/host/usr/local/bin";
/// Default in-container dir holding the image's *bundled* runsc + shim — the
/// convergence source (the node image's own `/usr/local/bin`).
const RUNSC_BUNDLED_BIN_DIR_DEFAULT: &str = "/usr/local/bin";
/// Default in-container path to the host gVisor apt source removed each tick.
const RUNSC_APT_SOURCE_DEFAULT: &str = "/host/etc/apt/sources.list.d/gvisor.list";

/// In-container dir holding the **host** `runsc` binary (the `/host`-prefixed
/// hostPath mount), from `SANDBAWKS_RUNSC_HOST_BIN_DIR` or the `/host/usr/local/bin`
/// default. Shared by [`runsc_reconcile_config`] (Capability A's node-label
/// reconciler) and [`build_gvisor_adapter`] (the #545 snapshot runsc-version
/// stamp) so both read the *same* host binary — the recorded checkpoint version
/// and the node label can never derive from different binaries.
fn host_runsc_bin_dir() -> PathBuf {
    std::env::var(RUNSC_HOST_BIN_DIR_ENV)
        .unwrap_or_else(|_| RUNSC_HOST_BIN_DIR_DEFAULT.to_string())
        .into()
}

/// Build the [`RunscReconcileConfig`] from the `SANDBAWKS_RUNSC_*` env family
/// (set by the chart's `node.runscReconcile.*`), defaulting every path so a
/// standalone `cargo run` reports nothing and converges nothing rather than
/// failing. `interval` is the validated `--runsc-reconcile-secs` flag.
fn runsc_reconcile_config(
    node_name: &str,
    interval: std::time::Duration,
) -> sandbawks_node::runsc_reconcile::RunscReconcileConfig {
    let host_bin_dir: PathBuf = host_runsc_bin_dir();
    let bundled_bin_dir: PathBuf = std::env::var(RUNSC_BUNDLED_BIN_DIR_ENV)
        .unwrap_or_else(|_| RUNSC_BUNDLED_BIN_DIR_DEFAULT.to_string())
        .into();
    let apt_source_path: PathBuf = std::env::var(RUNSC_APT_SOURCE_ENV)
        .unwrap_or_else(|_| RUNSC_APT_SOURCE_DEFAULT.to_string())
        .into();
    // Keyring removal is opt-in: unset → None so the default host mount surface
    // is exactly the apt source dir + /usr/local/bin (the #546 acceptance scope).
    let apt_keyring_path = std::env::var(RUNSC_APT_KEYRING_ENV).ok().map(PathBuf::from);
    let desired_version = std::env::var(RUNSC_DESIRED_VERSION_ENV).unwrap_or_default();
    let auto_reconcile = std::env::var(RUNSC_AUTO_RECONCILE_ENV)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);

    sandbawks_node::runsc_reconcile::RunscReconcileConfig {
        node_name: node_name.to_owned(),
        host_runsc_path: host_bin_dir.join("runsc"),
        bundled_bin_dir,
        host_bin_dir,
        desired_version,
        auto_reconcile,
        apt_source_path,
        apt_keyring_path,
        interval,
    }
}

/// Read an env var as a boolean flag (`1|true|yes|on` → true), falling back to
/// `default` when unset. The shared parse for the `#550` toggles, matching the
/// truthy vocabulary the `#546`/`#307` toggles already use.
fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

/// Default in-container path to the bundled `nsenter` (the node Dockerfile ships
/// a static busybox copied here — busybox dispatches by `argv[0]` basename, so a
/// copy named `nsenter` IS the applet; no symlink, which distroless can't create
/// at runtime). Used to enter PID 1's namespaces for the detached host install
/// (#550).
const RUNSC_UPGRADE_NSENTER_DEFAULT: &str = "/usr/local/bin/nsenter";
/// Default in-container path to the installer script bundled in the node image.
const RUNSC_UPGRADE_BUNDLED_INSTALLER_DEFAULT: &str = "/usr/local/bin/install-runsc-k3s.sh";
/// Default in-container dir the host installer directory is mounted at (host
/// `/opt/sandbawks`, a `/host`-style prefix so it never shadows the image tree).
const RUNSC_UPGRADE_STAGING_DIR_DEFAULT: &str = "/host/opt/sandbawks";
/// Default host-namespace path the staged installer resolves to — what
/// `systemd-run` executes after `nsenter`.
const RUNSC_UPGRADE_HOST_EXEC_DEFAULT: &str = "/opt/sandbawks/install-runsc-k3s.sh";
/// Default transient systemd unit name for the detached host install.
const RUNSC_UPGRADE_SYSTEMD_UNIT_DEFAULT: &str = "sandbawks-runsc-upgrade";

/// Build the [`RunscUpgradeConfig`] from the `SANDBAWKS_RUNSC_*` env family (set
/// by the chart's `node.runscUpgrade.*` / `node.runscAutoUpgrade`). The host
/// runsc path is the SAME `/host/usr/local/bin/runsc` the #546 reconciler reads,
/// so both convergence paths judge "already at the pinned version" off one binary.
fn runsc_upgrade_config(
    node_name: &str,
    interval: std::time::Duration,
) -> sandbawks_node::runsc_upgrade::RunscUpgradeConfig {
    let host_runsc_path = host_runsc_bin_dir().join("runsc");
    let desired_version = std::env::var(RUNSC_DESIRED_VERSION_ENV).unwrap_or_default();
    let auto_upgrade = env_flag(RUNSC_AUTO_UPGRADE_ENV, false);
    let lease_name = std::env::var(RUNSC_UPGRADE_LEASE_NAME_ENV)
        .unwrap_or_else(|_| sandbawks_node::runsc_upgrade::DEFAULT_UPGRADE_LEASE_NAME.to_string());
    let lease_duration_secs = env_u64(
        RUNSC_UPGRADE_LEASE_DURATION_ENV,
        sandbawks_node::runsc_upgrade::DEFAULT_UPGRADE_LEASE_DURATION_SECS as u64,
    )
    .unwrap_or(sandbawks_node::runsc_upgrade::DEFAULT_UPGRADE_LEASE_DURATION_SECS as u64)
    // The Lease API field is an i32; clamp the parsed u64 so an absurd override
    // can't wrap. Any realistic TTL is far below i32::MAX.
    .min(i32::MAX as u64) as i32;

    sandbawks_node::runsc_upgrade::RunscUpgradeConfig {
        node_name: node_name.to_owned(),
        host_runsc_path,
        desired_version,
        auto_upgrade,
        interval,
        lease_name,
        lease_duration_secs,
    }
}

/// Construct the production [`NsenterUpgradeRunner`] from the
/// `SANDBAWKS_RUNSC_UPGRADE_*` path env (set by the chart), defaulting every path
/// to its fixed in-container mount point so a standalone `cargo run` is harmless.
fn build_nsenter_runner() -> sandbawks_node::runsc_upgrade::NsenterUpgradeRunner {
    let nsenter = std::env::var(RUNSC_UPGRADE_NSENTER_ENV)
        .unwrap_or_else(|_| RUNSC_UPGRADE_NSENTER_DEFAULT.to_string())
        .into();
    let paths = sandbawks_node::runsc_upgrade::HostInstallerPaths {
        bundled_installer: std::env::var(RUNSC_UPGRADE_BUNDLED_INSTALLER_ENV)
            .unwrap_or_else(|_| RUNSC_UPGRADE_BUNDLED_INSTALLER_DEFAULT.to_string())
            .into(),
        staging_dir: std::env::var(RUNSC_UPGRADE_STAGING_DIR_ENV)
            .unwrap_or_else(|_| RUNSC_UPGRADE_STAGING_DIR_DEFAULT.to_string())
            .into(),
        host_exec_path: std::env::var(RUNSC_UPGRADE_HOST_EXEC_ENV)
            .unwrap_or_else(|_| RUNSC_UPGRADE_HOST_EXEC_DEFAULT.to_string())
            .into(),
    };
    let systemd_unit = std::env::var(RUNSC_UPGRADE_SYSTEMD_UNIT_ENV)
        .unwrap_or_else(|_| RUNSC_UPGRADE_SYSTEMD_UNIT_DEFAULT.to_string());
    sandbawks_node::runsc_upgrade::NsenterUpgradeRunner::new(nsenter, paths, systemd_unit)
}

/// runsc `--root` resolution inputs for [`build_gvisor_adapter`] (#366).
///
/// Grouped into one struct so the builder stays within a sane argument
/// count: the `primary` in-container root (`--runsc-root`) plus the ordered
/// per-environment `fallbacks` (`--runsc-root-fallbacks`) the adapter probes
/// when the primary does not hold the target container.
struct RunscRootConfig<'a> {
    primary: &'a std::path::Path,
    fallbacks: &'a [PathBuf],
    /// Containerd run/state root(s) the adapter recursively searches to
    /// discover the runsc state root when no explicit candidate matches (#377).
    search_dirs: &'a [PathBuf],
}

/// The two host filesystem roots the [`GvisorAdapter`] operates over, grouped
/// so [`build_gvisor_adapter`] stays within clippy's argument budget.
struct AdapterFsRoots<'a> {
    /// Parent dir under which per-sandbox per-pause work-dirs are staged
    /// (`SANDBAWKS_WORK_ROOT`). The DaemonSet hostPath-mounts it.
    work_root: &'a std::path::Path,
    /// Root restore bundles are staged/swept/confined under
    /// (`SANDBAWKS_STAGING_ROOT`, issue #512). Threaded into
    /// [`GvisorAdapter::with_staging_root`]; the sweeper is handed the same
    /// value so the two never desync.
    staging_root: &'a std::path::Path,
}

/// Wire a [`GvisorAdapter`] over a real containerd UDS + kube client +
/// raw-TCP `/__state` probe.
async fn build_gvisor_adapter(
    containerd_socket: &str,
    fs_roots: AdapterFsRoots<'_>,
    runsc_roots: RunscRootConfig<'_>,
    kube_client: Client,
    namespace: &str,
    node_name: &str,
    headroom: sandbawks_node::headroom::HeadroomPolicy,
) -> Result<Arc<dyn RuntimeAdapter>> {
    let grpc = containerd_client::Client::from_path(containerd_socket)
        .await
        .with_context(|| format!("connect containerd UDS {containerd_socket}"))?;
    let containerd = Arc::new(ContainerdClient::new(
        grpc,
        kube_client,
        namespace.to_owned(),
        node_name.to_owned(),
    ));
    let envd = Arc::new(HttpEnvdProbe::new());
    // #545: stamp the snapshot's runsc version from the **host** runsc binary —
    // the same one Capability A's reconciler labels the node with — so the
    // restore-placement gate's version match is apples-to-apples even when the
    // host runsc has drifted from the image-bundled binary.
    let host_runsc_bin = host_runsc_bin_dir().join("runsc");
    Ok(Arc::new(
        GvisorAdapter::new(containerd, envd, fs_roots.work_root)
            .with_runsc_root(runsc_roots.primary)
            .with_extra_runsc_roots(runsc_roots.fallbacks.iter().cloned())
            .with_runsc_root_search_dirs(runsc_roots.search_dirs.iter().cloned())
            .with_staging_root(fs_roots.staging_root)
            .with_host_runsc_binary(host_runsc_bin)
            .with_headroom_policy(headroom),
    ))
}

/// Emit a loud startup warning if *none* of the configured runsc state
/// roots — the primary `--root` plus the per-environment fallbacks
/// (issues #343, #366) — is a directory inside the node container.
///
/// Returns `true` when at least one candidate root is present (a
/// directory), `false` when every candidate is missing. The check is
/// candidate-aware so an operator who legitimately leaves `--runsc-root`
/// at the (wrong-for-this-distro) default and supplies the real root via
/// `--runsc-root-fallbacks` is not warned on every restart even though
/// the setup works (#366 review).
///
/// The caller logs based on the result rather than failing the process:
/// the `io.containerd.runsc.v1` shim creates the per-namespace root
/// lazily on the first gVisor container start, so a freshly-booted node
/// that has not yet hosted a sandbox legitimately has no such directory —
/// a hard failure would crash-loop an otherwise healthy node. A
/// *persistently* empty candidate set, on the other hand, means the paths
/// are wrong for this distro (the k3s/RKE2 case), and every `runsc
/// checkpoint` will fail per-pause until corrected via `SANDBAWKS_RUNSC_ROOT`
/// / `--runsc-root` (or the fallbacks / search dirs) plus the matching
/// DaemonSet mounts.
///
/// `search_dirs` (#377) count as present too: when discovery is configured the
/// node finds the real root under one of them at pause time, so a node that has
/// a search dir mounted is not nagged just because the (possibly wrong-for-this-
/// distro) primary default is absent.
fn warn_if_runsc_root_missing(
    runsc_root: &std::path::Path,
    fallbacks: &[PathBuf],
    search_dirs: &[PathBuf],
) -> bool {
    if runsc_root.is_dir()
        || fallbacks.iter().any(|p| p.is_dir())
        || search_dirs.iter().any(|p| p.is_dir())
    {
        return true;
    }
    tracing::warn!(
        runsc_root = %runsc_root.display(),
        runsc_root_fallbacks = ?fallbacks,
        runsc_root_search_dirs = ?search_dirs,
        env = RUNSC_ROOT_ENV,
        fallbacks_env = RUNSC_ROOT_FALLBACKS_ENV,
        search_dirs_env = RUNSC_ROOT_SEARCH_DIRS_ENV,
        "no configured runsc --root (primary, fallbacks, or search dirs) is a \
         directory; pause/resume will fail with 'FetchSpec failed: loading \
         container: file does not exist' once a sandbox runs here unless the \
         shim creates one. If this persists, the paths are wrong for this distro \
         (e.g. k3s/RKE2) — point the search-dirs env at this cluster's mounted \
         containerd run/state root (preferred), or set the primary / list the \
         runsc state dir(s) in the fallbacks env, and mount the matching host \
         path(s) into the node DaemonSet (#343, #366, #377)"
    );
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With no `--runsc-root` flag the binary falls back to the stock
    /// containerd state root (#343 default), keeping the pre-fix behavior
    /// intact on clusters that never needed the override.
    #[test]
    fn runsc_root_defaults_to_stock_containerd_layout() {
        // `try_parse_from` with the program name only — no flag supplied.
        let args = Args::try_parse_from(["sandbawks-node"]).expect("parse defaults");
        assert_eq!(
            args.runsc_root,
            PathBuf::from(sandbawks_node::gvisor::DEFAULT_RUNSC_ROOT)
        );
    }

    /// The `--runsc-root` flag overrides the default so operators can point
    /// the node at the k3s/RKE2 shim state layout (#343).
    #[test]
    fn runsc_root_flag_overrides_default() {
        let args = Args::try_parse_from([
            "sandbawks-node",
            "--runsc-root",
            "/run/k3s/containerd/runsc/k8s.io",
        ])
        .expect("parse override");
        assert_eq!(
            args.runsc_root,
            PathBuf::from("/run/k3s/containerd/runsc/k8s.io")
        );
    }

    /// With no fallbacks flag the list is empty, preserving the single-root
    /// behavior on clusters that never needed per-environment probing (#366).
    #[test]
    fn runsc_root_fallbacks_default_to_empty() {
        let args = Args::try_parse_from(["sandbawks-node"]).expect("parse defaults");
        assert!(args.runsc_root_fallbacks.is_empty());
    }

    /// `SANDBAWKS_RUNSC_ROOT_FALLBACKS` is split on the colon delimiter into an
    /// ordered candidate list, so an operator can enumerate a distro's runsc
    /// state subtree(s) without the binary baking in any layout (#366).
    #[test]
    fn runsc_root_fallbacks_split_on_colon() {
        let args = Args::try_parse_from([
            "sandbawks-node",
            "--runsc-root-fallbacks",
            "/run/stock-containerd/runsc/k8s.io:/run/host-containerd/runsc/k8s.io",
        ])
        .expect("parse fallbacks");
        assert_eq!(
            args.runsc_root_fallbacks,
            vec![
                PathBuf::from("/run/stock-containerd/runsc/k8s.io"),
                PathBuf::from("/run/host-containerd/runsc/k8s.io"),
            ]
        );
    }

    /// With no `--staging-root` flag the binary falls back to the compile-time
    /// [`DEFAULT_STAGING_ROOT`], preserving pre-#512 behavior on clusters that
    /// leave `node.hostPathStagingCache` at its default.
    #[test]
    fn staging_root_defaults_to_compile_time_const() {
        let args = Args::try_parse_from(["sandbawks-node"]).expect("parse defaults");
        assert_eq!(
            args.staging_root,
            PathBuf::from(sandbawks_node::gvisor::DEFAULT_STAGING_ROOT)
        );
    }

    /// The `--staging-root` flag overrides the default so an operator can
    /// relocate the staging root in lockstep with the DaemonSet mountPath and
    /// the controller annotation (#512). One value feeds the adapter and the
    /// sweeper, so they cannot desync.
    #[test]
    fn staging_root_flag_overrides_default() {
        let args = Args::try_parse_from(["sandbawks-node", "--staging-root", "/mnt/fast/staging"])
            .expect("parse override");
        assert_eq!(args.staging_root, PathBuf::from("/mnt/fast/staging"));
    }

    /// An existing primary root is reported present (no warning path taken).
    #[test]
    fn warn_if_runsc_root_missing_returns_true_for_existing_dir() {
        let dir = tempfile::tempdir().expect("create tempdir");
        assert!(warn_if_runsc_root_missing(dir.path(), &[], &[]));
    }

    /// A missing primary is rescued by a present fallback so the operator who
    /// supplies the real root via `--runsc-root-fallbacks` is not warned (#366).
    #[test]
    fn warn_if_runsc_root_missing_returns_true_when_a_fallback_exists() {
        let primary = tempfile::tempdir().expect("primary tempdir");
        let fallback = tempfile::tempdir().expect("fallback tempdir");
        let absent_primary = primary.path().join("does-not-exist");
        assert!(warn_if_runsc_root_missing(
            &absent_primary,
            &[fallback.path().to_path_buf()],
            &[],
        ));
    }

    /// A missing primary + fallbacks is rescued by a present search dir, so a
    /// node configured for generic discovery is not nagged just because the
    /// (wrong-for-this-distro) primary default is absent (#377).
    #[test]
    fn warn_if_runsc_root_missing_returns_true_when_a_search_dir_exists() {
        let primary = tempfile::tempdir().expect("primary tempdir");
        let search = tempfile::tempdir().expect("search tempdir");
        let absent_primary = primary.path().join("does-not-exist");
        assert!(warn_if_runsc_root_missing(
            &absent_primary,
            &[],
            &[search.path().to_path_buf()],
        ));
    }

    /// Only when *every* configured path (primary + fallbacks + search dirs) is
    /// absent does the caller log the deploy-time warning (#343/#366/#377).
    #[test]
    fn warn_if_runsc_root_missing_returns_false_when_all_absent() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let absent_primary = dir.path().join("does-not-exist");
        let absent_fallback = dir.path().join("also-missing");
        let absent_search = dir.path().join("search-missing");
        assert!(!warn_if_runsc_root_missing(
            &absent_primary,
            &[absent_fallback],
            &[absent_search],
        ));
    }

    /// With no search-dirs flag the list is empty, so generic discovery is off
    /// and behavior is identical to before this knob existed (#377).
    #[test]
    fn runsc_root_search_dirs_default_to_empty() {
        let args = Args::try_parse_from(["sandbawks-node"]).expect("parse defaults");
        assert!(args.runsc_root_search_dirs.is_empty());
    }

    /// `SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS` is split on the colon delimiter so an
    /// operator can list more than one mounted containerd run/state root (#377).
    #[test]
    fn runsc_root_search_dirs_split_on_colon() {
        let args = Args::try_parse_from([
            "sandbawks-node",
            "--runsc-root-search-dirs",
            "/run/containerd:/run/host-containerd",
        ])
        .expect("parse search dirs");
        assert_eq!(
            args.runsc_root_search_dirs,
            vec![
                PathBuf::from("/run/containerd"),
                PathBuf::from("/run/host-containerd"),
            ]
        );
    }
}

#[cfg(test)]
mod provider_tests {
    use super::*;

    #[test]
    fn azure_config_requires_account_and_container() {
        let cfg = AzureSnapshotConfig::from_values(
            Some("acct".into()),
            Some("key".into()),
            Some("cont".into()),
            None,
            "snapshots".into(),
        )
        .expect("complete config parses");
        assert_eq!(cfg.account, "acct");
        assert_eq!(cfg.container, "cont");
        assert_eq!(cfg.key_prefix, "snapshots");
        assert!(cfg.endpoint.is_none());

        let err = AzureSnapshotConfig::from_values(
            None,
            Some("key".into()),
            Some("cont".into()),
            None,
            "snapshots".into(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("SANDBAWKS_AZURE_ACCOUNT_NAME"));

        // Empty string is treated as absent (Helm renders `value: ""` under the
        // S3 provider), so a blank required field still errors.
        let err = AzureSnapshotConfig::from_values(
            Some("".into()),
            Some("key".into()),
            Some("cont".into()),
            None,
            "snapshots".into(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("SANDBAWKS_AZURE_ACCOUNT_NAME"));

        // An empty endpoint collapses to None (public Azure cloud), not Some("").
        let cfg = AzureSnapshotConfig::from_values(
            Some("acct".into()),
            Some("key".into()),
            Some("cont".into()),
            Some("".into()),
            "snapshots".into(),
        )
        .expect("empty endpoint is valid");
        assert!(cfg.endpoint.is_none());
    }
}
