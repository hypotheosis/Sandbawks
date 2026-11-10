//! Restart-free host `runsc` version reporting + convergence (#546).
//!
//! Sub-issue of the #545 fleet-upgrade epic. This module carves out the **safe,
//! restart-free, `nsenter`-free** slice: report the host runsc version, disarm
//! the legacy rolling apt channel that lets a host `apt-get upgrade` bump runsc
//! off its pin (#455), and — opt-in, and only on a node with no live sandboxes —
//! converge the host `/usr/local/bin/runsc` (+ its shim) to the pinned version
//! by atomically replacing the binary.
//!
//! ## Why this is restart-free
//!
//! containerd execs `runsc` *fresh* per sandbox, so swapping the host binary is
//! a plain file write: new sandboxes pick up the new binary with no k3s restart
//! and no `nsenter`. Replacing the file under a *running* sandbox is safe — and
//! this is specifically why we stage to a temp file and `rename(2)` over the
//! destination rather than truncating it in place. On Linux `rename(2)`/`unlink(2)`
//! of a path whose old target is a *currently-executing* binary succeed: the old
//! inode stays alive (unlinked-but-open) for every process that has it mapped, so
//! a running `runsc` keeps reading its original text while new `exec`s pick up the
//! new inode. `ETXTBSY` ("text file busy") is raised by `open(O_WRONLY)`/`execve`
//! on a running text file — **not** by `rename`/`unlink` — so the rename approach
//! never hits it (an in-place write would). The rename is also atomic, so a
//! concurrent `exec` sees either the whole old file or the whole new one, never a
//! torn write. The privileged, k3s-restarting parts of the epic (CRI handler
//! registration) stay in `scripts/install-runsc-k3s.sh` / cloud-init — see #545.
//!
//! ## The three steps of a tick
//!
//! 1. **Report** — shell `runsc --version` against the host binary, parse the
//!    `release-<ver>` token, stamp it on the Node as
//!    [`sandbawks_types::RUNSC_VERSION_LABEL`] and export it via
//!    [`crate::runsc_version_metrics`]. Read-only; always runs.
//! 2. **Disarm the rolling channel** — remove the gVisor apt source (and,
//!    optionally, its keyring). With no source, a host `apt-get upgrade` has no
//!    candidate to promote `/usr/bin/runsc` to, so drift stops even if a stale
//!    binary lingers. Idempotent; always runs.
//! 3. **Converge** — when opt-in ([`RunscReconcileConfig::auto_reconcile`]) and
//!    the host release differs from the pinned version *and* the node has no live
//!    sandboxes, atomically install the DaemonSet image's **bundled** runsc + shim
//!    (same build as the checkpoint-side runsc, satisfying the C/R version-match
//!    requirement) to the host `/usr/local/bin`.
//!
//! The `apt-get purge runsc` that would give a clean dpkg state needs host exec
//! (`nsenter`); step 2's source removal is sufficient to stop drift, so the
//! DaemonSet deliberately skips the purge to stay `nsenter`-free.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{ListParams, Patch, PatchParams};
use kube::{Api, Client};
use sandbawks_types::{RUNSC_VERSION_LABEL, SANDBOX_NAME_LABEL};
use serde_json::{json, Value};

use crate::runsc_version_metrics::{
    record_converge_decision, record_reconcile_error, record_runsc_version, record_version_read,
};

/// kube field-manager recorded on the runsc-version label patch. Distinct from
/// the `sandbawks-node-staging` manager so ownership of this label is attributed
/// to this code path (mirrors `node_label`'s split).
const FIELD_MANAGER: &str = "sandbawks-node-runsc-version";

/// Bundled binaries the DaemonSet image carries and converges onto the host, in
/// install order. Both are baked into the node image at
/// [`RunscReconcileConfig::bundled_bin_dir`] (see `crates/sandbawks-node/Dockerfile`)
/// so the host-side runsc and the DaemonSet's checkpoint-side runsc are the same
/// build — directly serving the gVisor C/R version-match requirement.
pub const CONVERGE_BINARIES: [&str; 2] = ["runsc", "containerd-shim-runsc-v1"];

/// Default interval between [`run_runsc_reconciler`] ticks.
///
/// runsc drift is a slow, host-`apt`-timer-driven event (the rolling channel is
/// promoted at most daily), so a 5-minute cadence is ample to keep the version
/// label fresh and re-disarm a channel an operator might have re-added, while
/// staying far below any churn that would matter.
pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(300);

/// Reads the `release-<ver>` token from a `runsc` binary's `--version` output.
///
/// Abstracted behind a trait so the reconcile decision logic is unit-testable
/// without a real host binary; production uses [`ExecRunscVersionReader`].
#[async_trait]
pub trait RunscVersionReader: Send + Sync {
    /// Resolve the `release-<ver>` token for the runsc binary at `path`.
    ///
    /// Returns `Ok(None)` when the binary is absent or its output carries no
    /// parseable `release-` token (a freshly-provisioned node may have no host
    /// runsc yet) so a missing binary is a quiet skip, not a hard error. Returns
    /// `Err` only when the binary exists but could not be executed at all.
    async fn read_release(&self, path: &Path) -> anyhow::Result<Option<String>>;
}

/// Counts the live (non-terminal) sandbox Pods on the local node.
///
/// Abstracted so [`reconcile_runsc_once`]'s "converge only with no live
/// sandboxes" gate is testable without an apiserver; production uses
/// [`KubeLiveSandboxCounter`].
#[async_trait]
pub trait LiveSandboxCounter: Send + Sync {
    /// Number of sandbox Pods on this node not in a terminal phase.
    async fn count_live(&self) -> anyhow::Result<usize>;
}

/// Parse the `release-<ver>` token out of `runsc --version` output.
///
/// The first line of `runsc --version` looks like `runsc version release-20260525.0`;
/// a `spec:` line follows. We scan whitespace-separated tokens across the whole
/// output for the first one starting with `release-` so the parse is robust to
/// the exact line layout. Returns the full token (e.g. `release-20260525.0`),
/// which is what we stamp as the label value verbatim.
///
/// # Examples
///
/// ```
/// use sandbawks_node::runsc_reconcile::parse_runsc_release;
/// let out = "runsc version release-20260525.0\nspec: 1.1.0-rc.1\n";
/// assert_eq!(parse_runsc_release(out).as_deref(), Some("release-20260525.0"));
/// assert_eq!(parse_runsc_release("garbage"), None);
/// ```
pub fn parse_runsc_release(version_output: &str) -> Option<String> {
    version_output
        .split_whitespace()
        .find(|tok| tok.starts_with("release-") && tok.len() > "release-".len())
        .map(str::to_owned)
}

/// The full `release-<ver>` token for a pinned version string (e.g.
/// `20260525.0` → `release-20260525.0`).
///
/// Kept as a single helper so the desired-version comparison and the
/// already-converged short-circuit agree on the canonical form. Accepts an
/// already-prefixed value idempotently, in case an operator sets
/// `node.runscVersion` to the full token.
///
/// `pub(crate)` so the Capability C upgrader ([`crate::runsc_upgrade`]) compares
/// the host release against `node.runscVersion` using the exact same canonical
/// form this reconciler does — the two convergence paths can never disagree on
/// what "already at the pinned version" means.
pub(crate) fn release_token(version: &str) -> String {
    if version.starts_with("release-") {
        version.to_owned()
    } else {
        format!("release-{version}")
    }
}

/// Why a convergence tick did or did not replace the host runsc binary.
///
/// Models the decision as a closed enum so the policy is exhaustively checked by
/// the compiler and the reason can be surfaced in logs/metrics verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvergeDecision {
    /// `auto_reconcile` is off; convergence is opt-in. Reporting + apt-disarm
    /// still run — only the binary swap is gated.
    Disabled,
    /// The image's bundled runsc release does not match the desired pinned
    /// version, so converging would install a binary that *still* mismatches and
    /// re-trigger next tick. Refuse and warn: the Dockerfile pin and the
    /// `node.runscVersion` Helm value have drifted (the #455 lockstep is broken).
    BundledMismatch {
        /// What the image actually bundles, or `None` if even the bundled binary
        /// could not be read.
        bundled: Option<String>,
        /// The desired `release-<ver>` from `node.runscVersion`.
        desired: String,
    },
    /// The host already runs the desired version; nothing to do.
    AlreadyConverged,
    /// The node hosts live sandboxes, so a binary swap would make their
    /// in-flight, current-version snapshots unrestorable on this node mid-rollout.
    /// Deferred until the node drains (composes with the #545 placement gate).
    LiveSandboxes(usize),
    /// All gates pass: install the bundled binaries onto the host.
    Converge,
}

impl ConvergeDecision {
    /// Stable, lowercase `decision` metric label. Closed over the enum so the
    /// `runsc_converge_total{decision}` vocabulary stays compiler-checked.
    pub fn decision_label(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::BundledMismatch { .. } => "bundled_mismatch",
            Self::AlreadyConverged => "already_converged",
            Self::LiveSandboxes(_) => "live_sandboxes",
            Self::Converge => "converge",
        }
    }
}

/// Decide whether to converge the host runsc binary this tick. Pure (no I/O) so
/// the gate ordering is unit-testable.
///
/// Gate order matters: `Disabled` and `BundledMismatch` are evaluated before the
/// host/live-sandbox checks so a misconfigured node logs the *actionable* reason
/// rather than a misleading "already converged".
fn decide_converge(
    auto_reconcile: bool,
    host_release: Option<&str>,
    desired_release: &str,
    bundled_release: Option<&str>,
    live_sandboxes: usize,
) -> ConvergeDecision {
    if !auto_reconcile {
        return ConvergeDecision::Disabled;
    }
    if bundled_release != Some(desired_release) {
        return ConvergeDecision::BundledMismatch {
            bundled: bundled_release.map(str::to_owned),
            desired: desired_release.to_owned(),
        };
    }
    if host_release == Some(desired_release) {
        return ConvergeDecision::AlreadyConverged;
    }
    if live_sandboxes > 0 {
        return ConvergeDecision::LiveSandboxes(live_sandboxes);
    }
    ConvergeDecision::Converge
}

/// Outcome of removing the legacy gVisor apt channel.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AptDisarmOutcome {
    /// The apt source file (`gvisor.list`) was present and removed this tick.
    pub source_removed: bool,
    /// The apt keyring was present and removed this tick.
    pub keyring_removed: bool,
}

impl AptDisarmOutcome {
    /// Whether this tick removed anything (drives a one-shot info log).
    fn changed(self) -> bool {
        self.source_removed || self.keyring_removed
    }
}

/// Remove the gVisor rolling-channel apt source (and, if configured, its keyring).
///
/// Idempotent: a missing file is a no-op, not an error — the steady state after
/// the first successful disarm. Removing the *source* is the load-bearing step:
/// with no `release main` source, a host `apt-get upgrade` has no candidate to
/// promote `/usr/bin/runsc` to, so drift stops even if the stale binary lingers.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] if a present file cannot be removed
/// (e.g. the host path is mounted read-only) so the caller can surface a
/// misconfiguration rather than silently failing to disarm.
fn disarm_apt_channel(source: &Path, keyring: Option<&Path>) -> std::io::Result<AptDisarmOutcome> {
    let source_removed = remove_if_present(source)?;
    let keyring_removed = match keyring {
        Some(path) => remove_if_present(path)?,
        None => false,
    };
    Ok(AptDisarmOutcome {
        source_removed,
        keyring_removed,
    })
}

/// `rm -f` semantics: remove `path` if it exists, reporting whether it did.
/// A `NotFound` race (file vanished between the check and the unlink) is folded
/// into "already gone" rather than surfaced as an error.
fn remove_if_present(path: &Path) -> std::io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

/// Atomically install every [`CONVERGE_BINARIES`] entry from `bundled_dir` onto
/// the host `host_dir`.
///
/// Each binary is staged to a sibling temp file in `host_dir` (so the final
/// rename is same-filesystem and therefore atomic), made executable, then
/// renamed over the destination. A concurrent containerd `exec` of the path thus
/// always sees a complete binary — the old one or the new one, never a torn
/// write. Blocking filesystem work; the caller runs it on a blocking thread.
///
/// # Errors
///
/// Propagates any read/write/rename [`std::io::Error`]; on the first failing
/// binary the rest are skipped (the host is left with whatever was already
/// atomically renamed, each individually valid).
fn install_bundled_binaries(bundled_dir: &Path, host_dir: &Path) -> std::io::Result<()> {
    for name in CONVERGE_BINARIES {
        install_one(&bundled_dir.join(name), &host_dir.join(name))?;
    }
    Ok(())
}

/// Atomically replace `dst` with the contents of `src`, mode `0755`.
///
/// The temp-file + `rename(2)` shape (rather than truncating `dst` in place) is
/// what makes replacing a *running* runsc safe: on Linux `rename`/`unlink` over a
/// currently-executing binary succeed and never raise `ETXTBSY` — that error is
/// for `open(O_WRONLY)`/`execve` on a running text file, which this code path
/// never does. See the module-level docs for the full rationale.
fn install_one(src: &Path, dst: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let bytes = std::fs::read(src)?;
    let dir = dst.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("destination {} has no parent directory", dst.display()),
        )
    })?;
    let file_name = dst.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("destination {} has no file name", dst.display()),
        )
    })?;
    // Single writer per node (the reconcile loop is serial), so a fixed temp
    // name in the destination dir is collision-free and keeps the rename
    // same-filesystem. The leading dot keeps it out of `PATH` globs.
    let tmp = dir.join(format!(".{file_name}.sandbawks-tmp"));
    std::fs::write(&tmp, &bytes)?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
    std::fs::rename(&tmp, dst)?;
    Ok(())
}

/// Resolved configuration for [`run_runsc_reconciler`].
///
/// All host-facing paths are *in-container* paths (the DaemonSet hostPath-mounts
/// the matching host dirs); keeping them configurable rather than baked in
/// follows the project's no-distro-hardcode rule, since the host `runsc` and apt
/// layout vary across distros.
#[derive(Debug, Clone)]
pub struct RunscReconcileConfig {
    /// This node's name (`NODE_NAME` downward API); the label/metric subject.
    pub node_name: String,
    /// In-container path to the **host** runsc binary (the hostPath mount of the
    /// node's `/usr/local/bin/runsc`). Read for reporting and overwritten on
    /// convergence.
    pub host_runsc_path: PathBuf,
    /// In-container dir holding the image's **bundled** runsc + shim — the
    /// convergence *source* (the node image's own `/usr/local/bin`).
    pub bundled_bin_dir: PathBuf,
    /// In-container dir mapping to the host `/usr/local/bin` — the convergence
    /// *destination* (hostPath mount).
    pub host_bin_dir: PathBuf,
    /// Pinned runsc version from `node.runscVersion` (e.g. `20260525.0`); the
    /// single source of truth per #545.
    pub desired_version: String,
    /// Opt-in gate for the binary swap (`node.runscAutoReconcile`, default off).
    /// Reporting + apt-disarm run regardless.
    pub auto_reconcile: bool,
    /// In-container path to the host gVisor apt source (`gvisor.list`), removed
    /// each tick.
    pub apt_source_path: PathBuf,
    /// Optional in-container path to the host gVisor apt keyring; `None` (the
    /// default) skips keyring removal so the default mount surface is exactly the
    /// apt source dir + `/usr/local/bin` per the issue's acceptance criteria.
    pub apt_keyring_path: Option<PathBuf>,
    /// Gap between ticks; see [`DEFAULT_RECONCILE_INTERVAL`].
    pub interval: Duration,
}

/// Counts live sandbox Pods on the local node via the apiserver.
///
/// "Live" = a sandbox Pod (carrying [`SANDBOX_NAME_LABEL`]) on this node whose
/// phase is not terminal (`Succeeded`/`Failed`). A field selector narrows to the
/// local node server-side; the terminal-phase filter is applied client-side
/// because the apiserver only supports `=`/`!=` on `status.phase`, not a set.
pub struct KubeLiveSandboxCounter {
    pods: Api<Pod>,
    node_name: String,
}

impl KubeLiveSandboxCounter {
    /// Build a counter over the given namespace (where sandbox Pods live).
    pub fn new(client: Client, namespace: &str, node_name: impl Into<String>) -> Self {
        Self {
            pods: Api::namespaced(client, namespace),
            node_name: node_name.into(),
        }
    }
}

#[async_trait]
impl LiveSandboxCounter for KubeLiveSandboxCounter {
    async fn count_live(&self) -> anyhow::Result<usize> {
        let lp = ListParams::default()
            .fields(&format!("spec.nodeName={}", self.node_name))
            .labels(SANDBOX_NAME_LABEL);
        let pods = self.pods.list(&lp).await?;
        Ok(pods
            .into_iter()
            .filter(|pod| !pod_phase_is_terminal(pod))
            .count())
    }
}

/// Whether a Pod is in a terminal phase (`Succeeded`/`Failed`), and so does not
/// count as a live sandbox blocking convergence.
fn pod_phase_is_terminal(pod: &Pod) -> bool {
    matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded" | "Failed")
    )
}

/// Executes the host runsc binary to read its version. Production
/// [`RunscVersionReader`].
pub struct ExecRunscVersionReader;

#[async_trait]
impl RunscVersionReader for ExecRunscVersionReader {
    async fn read_release(&self, path: &Path) -> anyhow::Result<Option<String>> {
        if !path.exists() {
            return Ok(None);
        }
        // runsc is a static Go binary, so executing the host copy from inside the
        // distroless node image needs no host libraries. `--version` writes the
        // `release-<ver>` line to stdout; the installer reads it with `2>&1`, so
        // we merge stderr in too for robustness across runsc versions.
        let output = tokio::process::Command::new(path)
            .arg("--version")
            .output()
            .await?;
        let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
        text.push('\n');
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        Ok(parse_runsc_release(&text))
    }
}

/// Summary of one [`reconcile_runsc_once`] tick, for logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunscReconcileOutcome {
    /// The host runsc release this tick resolved, or `None` if the host binary
    /// was absent/unparseable (reporting skipped).
    pub host_release: Option<String>,
    /// What the apt-disarm step removed.
    pub apt: AptDisarmOutcome,
    /// The convergence decision and, on `Converge`, whether the install ran.
    pub converge: ConvergeDecision,
    /// Whether the binary install actually executed (only on `Converge`).
    pub converged: bool,
}

/// Run one reconcile tick: report version, disarm the apt channel, and maybe
/// converge the host binary. Errors from the individual steps are returned so
/// the loop can log + retry; partial progress (e.g. reported but apt-disarm
/// failed) is reflected in the returned outcome on success.
///
/// # Errors
///
/// Returns an error if the host-version read fails to *execute* (not merely
/// absent), the Node label patch fails, the apt-disarm hits a non-`NotFound`
/// I/O error, the live-sandbox count fails, or the binary install fails. Each
/// failing step bumps `runsc_reconcile_errors_total{step}` before propagating, so
/// the failure mode is alertable from the metric rather than only the logs.
#[tracing::instrument(
    skip_all,
    fields(node = %cfg.node_name, auto_reconcile = cfg.auto_reconcile)
)]
pub async fn reconcile_runsc_once(
    nodes: &Api<Node>,
    cfg: &RunscReconcileConfig,
    reader: &dyn RunscVersionReader,
    counter: &dyn LiveSandboxCounter,
) -> anyhow::Result<RunscReconcileOutcome> {
    // 1. Report (read-only). A missing host binary is a quiet skip — but still
    // counted (`absent`) so operators can tell it apart from a dead reconciler.
    let host_release = reader
        .read_release(&cfg.host_runsc_path)
        .await
        .inspect_err(|_| record_reconcile_error("version_read"))?;
    record_version_read(host_release.is_some());
    if let Some(release) = host_release.as_deref() {
        record_runsc_version(&cfg.node_name, release);
        patch_runsc_version_label(nodes, &cfg.node_name, release)
            .await
            .inspect_err(|_| record_reconcile_error("label_patch"))?;
    }

    // 2. Disarm the rolling channel (always; idempotent).
    let apt = disarm_apt_channel(&cfg.apt_source_path, cfg.apt_keyring_path.as_deref())
        .inspect_err(|_| record_reconcile_error("apt_disarm"))?;

    // 3. Converge (opt-in; only on a drained node at a mismatched version).
    let desired = release_token(&cfg.desired_version);
    let bundled = reader
        .read_release(&cfg.bundled_bin_dir.join("runsc"))
        .await
        .inspect_err(|_| record_reconcile_error("version_read"))?;
    let live = if cfg.auto_reconcile {
        counter
            .count_live()
            .await
            .inspect_err(|_| record_reconcile_error("count_live"))?
    } else {
        // Skip the apiserver round-trip when convergence is disabled outright.
        0
    };
    let converge = decide_converge(
        cfg.auto_reconcile,
        host_release.as_deref(),
        &desired,
        bundled.as_deref(),
        live,
    );
    record_converge_decision(converge.decision_label());
    let converged = if converge == ConvergeDecision::Converge {
        let bundled_dir = cfg.bundled_bin_dir.clone();
        let host_dir = cfg.host_bin_dir.clone();
        tokio::task::spawn_blocking(move || install_bundled_binaries(&bundled_dir, &host_dir))
            .await
            .map_err(|join| anyhow::anyhow!("runsc install task panicked: {join}"))?
            .inspect_err(|_| record_reconcile_error("install"))?;
        true
    } else {
        false
    };

    Ok(RunscReconcileOutcome {
        host_release,
        apt,
        converge,
        converged,
    })
}

/// Patch this node's [`RUNSC_VERSION_LABEL`] to `release` via a bare
/// field-manager merge patch — adds/updates only that one label, leaving
/// operator-set labels untouched (RFC 7386 merge semantics).
async fn patch_runsc_version_label(
    nodes: &Api<Node>,
    node_name: &str,
    release: &str,
) -> Result<(), kube::Error> {
    let patch: Value = json!({ "metadata": { "labels": { RUNSC_VERSION_LABEL: release } } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.to_owned()),
        ..PatchParams::default()
    };
    nodes
        .patch(node_name, &params, &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Periodically reconcile the host runsc version for the life of the process.
///
/// Intended to be `tokio::spawn`ed and supervised alongside the node's other
/// long-lived loops: it never returns under normal operation. A failing tick is
/// logged and retried on the next interval rather than crashing the DaemonSet —
/// a transient apiserver blip or a momentarily read-only host mount must not take
/// down pause/resume. The boot log records the resolved config so an operator can
/// see whether convergence is armed.
///
/// # Panics
///
/// `cfg.interval` must be non-zero: `tokio::time::interval(Duration::ZERO)`
/// panics. The binary's CLI rejects a zero interval at parse time.
pub async fn run_runsc_reconciler(
    client: Client,
    cfg: RunscReconcileConfig,
    reader: Arc<dyn RunscVersionReader>,
    counter: Arc<dyn LiveSandboxCounter>,
) {
    let nodes: Api<Node> = Api::all(client);
    tracing::info!(
        node = %cfg.node_name,
        label = RUNSC_VERSION_LABEL,
        desired_version = %cfg.desired_version,
        auto_reconcile = cfg.auto_reconcile,
        host_runsc = %cfg.host_runsc_path.display(),
        apt_source = %cfg.apt_source_path.display(),
        interval_secs = cfg.interval.as_secs(),
        "runsc version reconciler started (#546)"
    );
    let mut ticker = tokio::time::interval(cfg.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // Tick first so the disarm/report run immediately at boot, then every
        // interval — unlike the envd-staged loop there is no boot-time one-shot
        // ahead of this, so we want the first action without an interval's delay.
        ticker.tick().await;
        match reconcile_runsc_once(&nodes, &cfg, reader.as_ref(), counter.as_ref()).await {
            Ok(outcome) => log_outcome(&cfg, &outcome),
            Err(err) => tracing::warn!(
                node = %cfg.node_name,
                error = %err,
                "runsc version reconcile tick failed; retrying next interval (#546)"
            ),
        }
    }
}

/// Emit the per-tick log lines for a successful reconcile, at severities tuned
/// so a steady-state node is near-silent and the actionable events (a removed
/// apt channel, a binary swap, a config mismatch) stand out.
fn log_outcome(cfg: &RunscReconcileConfig, outcome: &RunscReconcileOutcome) {
    match &outcome.host_release {
        Some(release) => tracing::debug!(
            node = %cfg.node_name, release = %release,
            "reported host runsc version (#546)"
        ),
        None => tracing::debug!(
            node = %cfg.node_name, host_runsc = %cfg.host_runsc_path.display(),
            "no host runsc binary to report (#546)"
        ),
    }
    if outcome.apt.changed() {
        tracing::info!(
            node = %cfg.node_name,
            source = %cfg.apt_source_path.display(),
            source_removed = outcome.apt.source_removed,
            keyring_removed = outcome.apt.keyring_removed,
            "removed legacy gVisor apt channel; a host apt-get upgrade can no \
             longer bump runsc off its pin (#455/#546)"
        );
    }
    match &outcome.converge {
        ConvergeDecision::Converge if outcome.converged => tracing::info!(
            node = %cfg.node_name, version = %cfg.desired_version,
            "converged host runsc + shim to pinned version with no k3s restart (#546)"
        ),
        ConvergeDecision::BundledMismatch { bundled, desired } => tracing::warn!(
            node = %cfg.node_name, ?bundled, desired = %desired,
            "refusing to converge runsc: image-bundled release does not match \
             node.runscVersion — the Dockerfile pin and Helm value have drifted \
             (the #455 lockstep is broken); fix the pin before enabling \
             convergence (#546)"
        ),
        ConvergeDecision::LiveSandboxes(n) => tracing::info!(
            node = %cfg.node_name, live_sandboxes = n,
            "deferring runsc convergence: node has live sandboxes whose \
             current-version snapshots must stay restorable here (#546)"
        ),
        // Disabled / AlreadyConverged are steady-state; nothing to log.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn parse_release_from_typical_two_line_output() {
        let out = "runsc version release-20260525.0\nspec: 1.1.0-rc.1\n";
        assert_eq!(
            parse_runsc_release(out).as_deref(),
            Some("release-20260525.0")
        );
    }

    #[test]
    fn parse_release_ignores_output_without_a_release_token() {
        assert_eq!(parse_runsc_release("runsc version unknown"), None);
        assert_eq!(parse_runsc_release(""), None);
        // A bare `release-` with no version is rejected (len guard).
        assert_eq!(parse_runsc_release("release-"), None);
    }

    #[test]
    fn release_token_prefixes_a_bare_version_and_is_idempotent() {
        assert_eq!(release_token("20260525.0"), "release-20260525.0");
        assert_eq!(release_token("release-20260525.0"), "release-20260525.0");
    }

    #[test]
    fn converge_decision_is_gated_in_priority_order() {
        let desired = "release-20260525.0";
        let bundled = Some(desired);

        // Disabled wins over everything, even a clean drained mismatch.
        assert_eq!(
            decide_converge(false, Some("release-old"), desired, bundled, 0),
            ConvergeDecision::Disabled
        );
        // Bundled drift is reported even when the host mismatches and is drained.
        assert_eq!(
            decide_converge(true, Some("release-old"), desired, Some("release-wrong"), 0),
            ConvergeDecision::BundledMismatch {
                bundled: Some("release-wrong".to_owned()),
                desired: desired.to_owned(),
            }
        );
        // Already at the desired version: no-op.
        assert_eq!(
            decide_converge(true, Some(desired), desired, bundled, 0),
            ConvergeDecision::AlreadyConverged
        );
        // Mismatched but the node still hosts live sandboxes: defer.
        assert_eq!(
            decide_converge(true, Some("release-old"), desired, bundled, 3),
            ConvergeDecision::LiveSandboxes(3)
        );
        // Mismatched, drained, bundle matches desired: converge.
        assert_eq!(
            decide_converge(true, Some("release-old"), desired, bundled, 0),
            ConvergeDecision::Converge
        );
        // A node with no host runsc at all (None) still converges when drained.
        assert_eq!(
            decide_converge(true, None, desired, bundled, 0),
            ConvergeDecision::Converge
        );
    }

    #[test]
    fn disarm_apt_channel_removes_present_files_and_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("gvisor.list");
        let keyring = dir.path().join("gvisor-archive-keyring.gpg");
        std::fs::write(&source, b"deb ... release main\n").expect("write source");
        std::fs::write(&keyring, b"key").expect("write keyring");

        let first = disarm_apt_channel(&source, Some(&keyring)).expect("disarm");
        assert_eq!(
            first,
            AptDisarmOutcome {
                source_removed: true,
                keyring_removed: true,
            }
        );
        assert!(!source.exists());
        assert!(!keyring.exists());

        // Second pass: nothing to remove, still Ok.
        let second = disarm_apt_channel(&source, Some(&keyring)).expect("disarm idempotent");
        assert_eq!(second, AptDisarmOutcome::default());
    }

    #[test]
    fn disarm_apt_channel_skips_keyring_when_unconfigured() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("gvisor.list");
        std::fs::write(&source, b"deb").expect("write source");
        let out = disarm_apt_channel(&source, None).expect("disarm");
        assert_eq!(
            out,
            AptDisarmOutcome {
                source_removed: true,
                keyring_removed: false,
            }
        );
    }

    #[test]
    fn install_bundled_binaries_atomically_replaces_with_exec_perms() {
        let src = tempfile::tempdir().expect("src dir");
        let dst = tempfile::tempdir().expect("dst dir");
        for name in CONVERGE_BINARIES {
            std::fs::write(src.path().join(name), format!("new-{name}")).expect("write bundled");
        }
        // Pre-existing stale destination to prove the swap overwrites it.
        std::fs::write(dst.path().join("runsc"), b"old-runsc").expect("write stale");

        install_bundled_binaries(src.path(), dst.path()).expect("install");

        for name in CONVERGE_BINARIES {
            let installed = dst.path().join(name);
            assert_eq!(
                std::fs::read_to_string(&installed).expect("read installed"),
                format!("new-{name}")
            );
            let mode = std::fs::metadata(&installed)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o755, "{name} must be 0755");
        }
        // The temp staging file must not linger in the destination dir.
        assert!(!dst.path().join(".runsc.sandbawks-tmp").exists());
    }

    #[test]
    fn pod_phase_terminal_classification() {
        use k8s_openapi::api::core::v1::PodStatus;
        let with_phase = |phase: Option<&str>| Pod {
            status: Some(PodStatus {
                phase: phase.map(str::to_owned),
                ..PodStatus::default()
            }),
            ..Pod::default()
        };
        assert!(pod_phase_is_terminal(&with_phase(Some("Succeeded"))));
        assert!(pod_phase_is_terminal(&with_phase(Some("Failed"))));
        assert!(!pod_phase_is_terminal(&with_phase(Some("Running"))));
        assert!(!pod_phase_is_terminal(&with_phase(Some("Pending"))));
        // No status / no phase is treated as non-terminal (count it as live).
        assert!(!pod_phase_is_terminal(&with_phase(None)));
        assert!(!pod_phase_is_terminal(&Pod::default()));
    }

    /// A missing bundled source binary surfaces the read error rather than
    /// silently installing nothing — the misconfigured-host-mount case.
    #[test]
    fn install_bundled_binaries_errors_when_source_missing() {
        let src = tempfile::tempdir().expect("src dir"); // intentionally empty
        let dst = tempfile::tempdir().expect("dst dir");
        let err = install_bundled_binaries(src.path(), dst.path())
            .expect_err("missing bundled runsc must error");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        // Nothing was installed.
        assert!(!dst.path().join("runsc").exists());
    }

    /// Installing to a destination whose parent dir does not exist errors on the
    /// temp-file write (the host `/usr/local/bin` mount being absent).
    #[test]
    fn install_one_errors_when_destination_parent_missing() {
        let src = tempfile::NamedTempFile::new().expect("src file");
        let dir = tempfile::tempdir().expect("base dir");
        let dst = dir.path().join("does-not-exist").join("runsc");
        let err = install_one(src.path(), &dst).expect_err("missing parent must error");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    /// `remove_if_present` propagates a non-`NotFound` I/O error (here EACCES from
    /// a read-only parent dir) rather than swallowing it — so a read-only host
    /// apt mount surfaces loudly instead of silently failing to disarm. Skipped
    /// when the test runs as root, where DAC permission bits are bypassed.
    #[test]
    fn remove_if_present_errors_on_permission_denied() {
        let dir = tempfile::tempdir().expect("tempdir");
        let victim = dir.path().join("gvisor.list");
        std::fs::write(&victim, b"deb").expect("write victim");

        // Make the parent dir non-writable: unlinking a child needs write on the
        // dir, so this denies the removal — unless we are root.
        let ro = std::fs::Permissions::from_mode(0o500);
        std::fs::set_permissions(dir.path(), ro).expect("chmod ro");

        // Self-detect root (DAC bypass) without a uid dependency: if we can still
        // create a file in the read-only dir, permission checks don't apply here.
        let is_root = std::fs::write(dir.path().join(".probe"), b"x").is_ok();

        let result = remove_if_present(&victim);

        // Restore write so the TempDir can clean itself up regardless of outcome.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("restore perms");

        if is_root {
            // Root bypasses the bits; the removal simply succeeds.
            assert!(result.is_ok());
        } else {
            let err = result.expect_err("permission-denied removal must error");
            assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        }
    }
}
