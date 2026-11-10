//! Node-staging readiness label (#272).
//!
//! Closes the cold-start scheduling race. A sandbox Pod execs the
//! node-staged `envd` binary as its entrypoint
//! (`crates/sandbawks-controller/src/pod_adapter.rs`), but nothing tied
//! the Pod's *scheduling* to the `sandbawks-node` DaemonSet having
//! finished staging that binary on the node. A Pod landing on a
//! not-yet-staged node mounted an empty `DirectoryOrCreate` hostPath and
//! CrashLooped on `exec: /sandbawks/bin/envd: no such file or directory`
//! (cluster bootstrap, node bring-up, DaemonSet rollout).
//!
//! The fix is a node-label + nodeAffinity gate. This module owns the
//! DaemonSet side: once the `stage-envd` init container has placed the
//! binary — a precondition for this process starting at all, since init
//! containers must succeed before the main container runs — the node
//! binary stamps [`sandbawks_types::ENVD_STAGED_LABEL`] on its own Node.
//! The controller stamps a matching `Exists` nodeAffinity onto every
//! sandbox Pod, so the scheduler only places sandboxes onto nodes that
//! have advertised a staged binary.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use sandbawks_types::{ENVD_STAGED_LABEL, ENVD_STAGED_LABEL_VALUE};
use serde_json::{json, Value};

use crate::envd_advertised_metrics::{record_envd_staged_tick, ReconcileResult};

/// kube field-manager recorded on the node-label merge patch. Distinct
/// from the snapshot-status field manager so ownership of the label is
/// attributed to this code path.
const FIELD_MANAGER: &str = "sandbawks-node-staging";

/// Default interval between [`run_envd_staged_reconciler`] ticks (#282).
///
/// Sized to match the acceptance window in #282 ("removed within one
/// tick of the staged binary becoming unavailable"): 30s bounds how long
/// a node can advertise a stale label after the binary disappears, which
/// is well inside a sandbox's tolerance for a scheduling delay while
/// being short enough that the gate tracks reality.
pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// Failure modes when advertising node-staging readiness.
#[derive(Debug, thiserror::Error)]
pub enum AdvertiseError {
    /// The staged `envd` binary was not found on disk, so the node is
    /// *not* actually ready. Refusing to label fails closed — labeling
    /// anyway would re-introduce the #272 CrashLoop the gate prevents.
    #[error("node-staged envd binary not found at {0}; refusing to advertise readiness")]
    BinaryMissing(String),
    /// A Node API request failed (RBAC, apiserver, conflict). Covers both the
    /// label merge-patch and the Node `get` that reads
    /// `status.runtimeHandlers` for the gVisor-handler gate (#417).
    #[error("node API request failed: {0}")]
    NodeApi(#[from] kube::Error),
}

/// Required gVisor runtime-handler name, matched against
/// `Node.status.runtimeHandlers[].name` (#417).
///
/// This is the `handler:` value declared by the `RuntimeClass: gvisor`
/// resource (`deploy/helm/sandbawks/templates/runtimeclass-gvisor.yaml`), not a
/// filesystem path — the kubelet reports it in Node status once containerd's
/// CRI plugin has the handler registered. The node binary's CLI defaults its
/// `--gvisor-runtime-handler` flag to this so an operator who renames the
/// RuntimeClass handler can keep the gate in sync.
pub const DEFAULT_GVISOR_RUNTIME_HANDLER: &str = "runsc";

/// Runsc-specific remediation suffix for the "runtime handler absent" WARN,
/// returned only when the configured handler actually *is* `runsc`.
///
/// The handler-absent gate is generic — `required_handler` is an operator-set
/// value (`SANDBAWKS_GVISOR_RUNTIME_HANDLER` / `node.runtimeHandler`) that on a
/// Kata pool is `kata`, not `runsc` (#598). The "install runsc +
/// containerd-shim-runsc-v1" advice is only correct for the default `runsc`
/// handler, so it is appended conditionally; for any other handler the caller
/// emits a handler-agnostic message naming `required_handler` with no
/// runsc-specific prescription (which on a Kata node would send operators
/// chasing a non-existent runsc problem).
pub fn runsc_remediation_hint(required_handler: &str) -> &'static str {
    if required_handler == DEFAULT_GVISOR_RUNTIME_HANDLER {
        " — install runsc + containerd-shim-runsc-v1 and register the handler \
         in containerd (see ci/kind-node/Dockerfile)"
    } else {
        ""
    }
}

/// Build the strategic-merge patch body that adds
/// [`ENVD_STAGED_LABEL`] to a Node's `metadata.labels`.
///
/// Kept pure (no I/O) so the wire shape can be unit-tested without an
/// apiserver. A merge patch only *adds* the key — it leaves any other
/// labels the operator set on the node untouched.
fn staged_label_patch() -> Value {
    json!({
        "metadata": {
            "labels": { ENVD_STAGED_LABEL: ENVD_STAGED_LABEL_VALUE }
        }
    })
}

/// Build the strategic-merge patch body that *removes*
/// [`ENVD_STAGED_LABEL`] from a Node's `metadata.labels`.
///
/// RFC 7386 (JSON merge patch) semantics: a member whose value is `null`
/// deletes the corresponding key from the target. Like
/// [`staged_label_patch`] this touches only the one label, so operator-set
/// labels survive untouched. Used by [`reconcile_envd_staged`] to withdraw
/// readiness the instant the staged binary disappears, so the controller's
/// nodeAffinity gate stops scheduling sandboxes onto a node that can no
/// longer host them (#282).
fn staged_label_removal_patch() -> Value {
    json!({
        "metadata": {
            "labels": { ENVD_STAGED_LABEL: Value::Null }
        }
    })
}

/// Issue a merge patch against the cluster-scoped Node `node_name`,
/// attributing the change to [`FIELD_MANAGER`].
///
/// Shared by the add ([`staged_label_patch`]) and remove
/// ([`staged_label_removal_patch`]) paths so the field-manager and
/// patch-type choice live in exactly one place. See
/// [`advertise_envd_staged`] for why this is a bare-field-manager
/// `Patch::Merge` rather than a server-side `Patch::Apply`.
async fn patch_node_labels(
    nodes: &Api<Node>,
    node_name: &str,
    patch: &Value,
) -> Result<(), kube::Error> {
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.to_owned()),
        ..PatchParams::default()
    };
    nodes
        .patch(node_name, &params, &Patch::Merge(patch))
        .await?;
    Ok(())
}

/// Verify the staged `envd` binary is present at `envd_bin_path`.
///
/// The `stage-envd` init container must have succeeded for the node
/// binary to start, but this is the explicit, fail-closed assertion that
/// the binary is really there before the node advertises readiness.
fn ensure_binary_present(envd_bin_path: &Path) -> Result<(), AdvertiseError> {
    if envd_bin_path.exists() {
        Ok(())
    } else {
        Err(AdvertiseError::BinaryMissing(
            envd_bin_path.display().to_string(),
        ))
    }
}

/// Whether this node's kubelet-reported CRI runtime handlers include the
/// gVisor handler (#417).
///
/// The signal is `Node.status.runtimeHandlers`, which the kubelet derives
/// directly from containerd's CRI `Status` response — the same ground truth
/// the scheduler sees. It reflects what containerd *actually* has registered
/// (`io.containerd.runsc.v1`), which is the real precondition for creating a
/// `RuntimeClass: gvisor` sandbox. A binary on `$PATH` is neither necessary
/// nor sufficient: the node DaemonSet image bakes its own `runsc` in for
/// `runsc checkpoint`/`restore`, so a filesystem probe would pass on every
/// node regardless of host containerd state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeHandlerGate {
    /// The required handler is registered — the node can host gVisor sandboxes.
    Present,
    /// The node reports a non-empty handler list that does **not** include the
    /// required handler — positive evidence the handler is missing (#417).
    Absent,
    /// The node reports no handler list at all (a CRI / kubelet too old to
    /// surface `runtimeHandlers`). Presence can't be determined, so the gate
    /// does not block — graceful degradation to pre-#417 behavior rather than
    /// stranding every node on such a cluster.
    Unknown,
}

/// Classify a Node's `status.runtimeHandlers` against the `required` handler
/// name. Pure (no I/O) so the three-way decision is unit-testable without an
/// apiserver.
fn runtime_handler_gate(node: &Node, required: &str) -> RuntimeHandlerGate {
    match node
        .status
        .as_ref()
        .and_then(|status| status.runtime_handlers.as_deref())
    {
        None | Some([]) => RuntimeHandlerGate::Unknown,
        Some(handlers) => {
            if handlers
                .iter()
                .any(|handler| handler.name.as_deref() == Some(required))
            {
                RuntimeHandlerGate::Present
            } else {
                RuntimeHandlerGate::Absent
            }
        }
    }
}

/// `get` this node and classify its runtime-handler gate (#417).
///
/// One extra Node read per reconcile tick (default 30s); negligible. The
/// `sandbawks-node` ClusterRole already grants `get` on `nodes` (it labels
/// them), so no RBAC change is needed.
async fn fetch_runtime_handler_gate(
    nodes: &Api<Node>,
    node_name: &str,
    required: &str,
) -> Result<RuntimeHandlerGate, kube::Error> {
    let node = nodes.get(node_name).await?;
    Ok(runtime_handler_gate(&node, required))
}

/// Advertise that this node can host sandboxes by labeling its own Node
/// object, so the controller's nodeAffinity gate lets sandbox Pods schedule
/// here — gated on both the staged `envd` binary (#272) and the gVisor
/// `runsc` runtime handler being registered in containerd (#417).
///
/// # Arguments
///
/// * `client` — in-cluster kube client (the `sandbawks-node`
///   ServiceAccount; its ClusterRole grants `get`/`patch` on `nodes`).
/// * `node_name` — this node's name, from the `NODE_NAME` downward-API
///   env var.
/// * `envd_bin_path` — where the `stage-envd` init container placed the
///   binary (`gvisor::DEFAULT_ENVD_BIN_PATH` in production).
/// * `required_handler` — the gVisor handler name to require in
///   `Node.status.runtimeHandlers` ([`DEFAULT_GVISOR_RUNTIME_HANDLER`] in
///   production).
///
/// # Returns
///
/// [`AdvertiseOutcome::Advertised`] if the node was labeled, or
/// [`AdvertiseOutcome::WithdrawnRunscAbsent`] if the `runsc` handler is absent
/// — in which case the label is (idempotently) removed and **no error is
/// returned**. This is the key difference from the envd-binary gate: a missing
/// `runsc` handler must *not* crash the process, because restarting the
/// DaemonSet Pod will not install runsc — it would just CrashLoop forever.
/// Instead the node simply doesn't advertise, and [`run_envd_staged_reconciler`]
/// re-asserts the label automatically once `runsc` appears.
///
/// # Errors
///
/// Returns [`AdvertiseError::BinaryMissing`] if the staged binary is absent —
/// this *is* fail-closed-by-crash, because restarting the Pod re-runs
/// `stage-envd` and can fix it — or [`AdvertiseError::NodeApi`] if the Node
/// `get`/patch fails.
pub async fn advertise_envd_staged(
    client: Client,
    node_name: &str,
    envd_bin_path: &Path,
    required_handler: &str,
) -> Result<AdvertiseOutcome, AdvertiseError> {
    ensure_binary_present(envd_bin_path)?;

    // Nodes are cluster-scoped, so `Api::all` is the correct handle.
    //
    // A merge patch (`application/merge-patch+json`) carrying an explicit
    // field manager. Deliberately *not* `PatchParams::apply`: that is for
    // server-side apply (`Patch::Apply`) and injects a `force` query param
    // the apiserver ignores for a merge patch — pairing the two records
    // the field manager but muddies the intent. A bare field-manager
    // attributes ownership without the apply/force confusion.
    let nodes: Api<Node> = Api::all(client);

    // #417: gate on the gVisor handler actually being registered in
    // containerd before advertising. `Absent` removes any stale label
    // (a prior run may have set it) rather than crashing.
    if fetch_runtime_handler_gate(&nodes, node_name, required_handler).await?
        == RuntimeHandlerGate::Absent
    {
        patch_node_labels(&nodes, node_name, &staged_label_removal_patch()).await?;
        return Ok(AdvertiseOutcome::WithdrawnRunscAbsent);
    }

    patch_node_labels(&nodes, node_name, &staged_label_patch()).await?;
    Ok(AdvertiseOutcome::Advertised)
}

/// Outcome of a single [`reconcile_envd_staged`] tick.
///
/// Drives both the structured `result=` tracing field and the
/// `envd_advertised.total{result=...}` OTel counter emitted per tick by
/// [`run_envd_staged_reconciler`] (the counter proposed in #282, promoted in
/// #286 once the workspace gained an OTel metrics pipeline). See
/// [`crate::envd_advertised_metrics`] for the `result` label vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvertiseOutcome {
    /// The staged binary is present; the label was (re-)asserted.
    Advertised,
    /// The staged binary is gone; the label was withdrawn so the
    /// nodeAffinity gate stops scheduling sandboxes onto this node.
    Withdrawn,
    /// A `RunscRootUnresolved` failure was observed for a real container;
    /// the label was withdrawn to stop the warm pool from scheduling further
    /// pause jobs here (#379). Unlike [`Self::Withdrawn`], the binary may
    /// still be present — the gate fires on a structural inability to resolve
    /// a runsc root, not on the staged binary's absence.
    WithdrawnRunscRoot,
    /// The node's `status.runtimeHandlers` does not include the gVisor `runsc`
    /// handler, so containerd cannot create a sandbox here — a Pod scheduled
    /// onto this node would loop on `FailedCreatePodSandBox: no runtime for
    /// "runsc" is configured`. The label was withdrawn so the nodeAffinity
    /// gate stops scheduling sandboxes here (#417). Unlike
    /// [`Self::WithdrawnRunscRoot`] this is **not** sticky — the gate clears
    /// the instant the kubelet reports the handler (e.g. gVisor finishes
    /// provisioning the node), and the next tick re-asserts the label.
    WithdrawnRunscAbsent,
}

/// Reconcile the `envd-staged` label against the on-disk reality of the
/// staged binary and the runtime capability gate — one tick (#282, #379).
///
/// Unlike [`advertise_envd_staged`], a missing binary is **not** an error
/// here: it is the very condition the periodic reconciler exists to
/// react to. The label tracks three independent gates, in priority order:
///
/// 1. `runsc_root_failed` — set by the node reconciler when
///    [`RuntimeError::RunscRootUnresolved`] fires for a real container
///    (#379). When true the label is immediately withdrawn regardless of
///    the other gates, so the warm pool stops scheduling new pause jobs
///    on this node. This gate is sticky: it stays set until the node
///    process restarts (clearing the `AtomicBool`), which is the
///    intentional recovery path (operator fixes the mount, rolls the
///    DaemonSet).
/// 2. gVisor handler registration (#417) — `runsc` absent from this node's
///    `status.runtimeHandlers` → label withdrawn
///    ([`AdvertiseOutcome::WithdrawnRunscAbsent`]). A sandbox Pod scheduled
///    onto a node whose containerd lacks the handler loops forever on
///    `FailedCreatePodSandBox: no runtime for "runsc" is configured`. Unlike
///    gate (1) this is not sticky: it clears the instant the kubelet reports
///    the handler. A node that reports *no* handler list at all
///    ([`RuntimeHandlerGate::Unknown`]) is not blocked — see that variant.
/// 3. Binary presence — binary missing → label removed ([`AdvertiseOutcome::Withdrawn`]);
///    binary present → label (re-)asserted ([`AdvertiseOutcome::Advertised`]).
///    Gates (1) and (2) take priority: even if the binary is present, a prior
///    `RunscRootUnresolved` or an absent `runsc` handler keeps the label
///    withdrawn.
///
/// All branches are idempotent, so a steady-state node just re-asserts
/// the same label every tick.
///
/// # Errors
///
/// Returns [`kube::Error`] if the Node `get` (gate 2) or the label patch
/// fails (RBAC, apiserver, conflict). The caller ([`run_envd_staged_reconciler`])
/// logs and retries on the next tick rather than tearing down the
/// process — a transient apiserver blip should not crash the DaemonSet.
pub async fn reconcile_envd_staged(
    nodes: &Api<Node>,
    node_name: &str,
    envd_bin_path: &Path,
    required_handler: &str,
    runsc_root_failed: &AtomicBool,
) -> Result<AdvertiseOutcome, kube::Error> {
    if runsc_root_failed.load(Ordering::Relaxed) {
        // A RunscRootUnresolved error was observed for a real container on this
        // node (#379). Withdraw the label to prevent the warm pool from
        // scheduling more pause jobs here — the failure is structural and every
        // retry would just re-fail. The flag is cleared only by a process
        // restart (operator re-rolls the DaemonSet after fixing the mount).
        patch_node_labels(nodes, node_name, &staged_label_removal_patch()).await?;
        return Ok(AdvertiseOutcome::WithdrawnRunscRoot);
    }
    if fetch_runtime_handler_gate(nodes, node_name, required_handler).await?
        == RuntimeHandlerGate::Absent
    {
        // The gVisor handler is not registered in this node's containerd
        // (#417). Withdraw the label so the scheduler stops placing sandbox
        // Pods here. Not sticky — the next tick re-asserts once the kubelet
        // reports the handler (e.g. gVisor finishes provisioning the node).
        patch_node_labels(nodes, node_name, &staged_label_removal_patch()).await?;
        return Ok(AdvertiseOutcome::WithdrawnRunscAbsent);
    }
    match ensure_binary_present(envd_bin_path) {
        Ok(()) => {
            patch_node_labels(nodes, node_name, &staged_label_patch()).await?;
            Ok(AdvertiseOutcome::Advertised)
        }
        Err(_) => {
            patch_node_labels(nodes, node_name, &staged_label_removal_patch()).await?;
            Ok(AdvertiseOutcome::Withdrawn)
        }
    }
}

/// Consecutive failed reconcile ticks before the per-tick failure log
/// escalates from `WARN` to `ERROR` (#363).
///
/// At [`DEFAULT_RECONCILE_INTERVAL`] (30s) three ticks is ~90s — long
/// enough that a single-tick apiserver blip (the cached HTTP/2 connection
/// dying, re-dialed on the next request) never reaches `ERROR`, short
/// enough that a genuinely sustained connectivity loss still surfaces
/// loudly. Alerting keys off the
/// `envd_advertised_total{result="patch_failed"}` counter rate, not these
/// log lines, so this threshold tunes operator-facing log severity only.
const FAILURE_ESCALATION_THRESHOLD: u32 = 3;

/// Severity at which a failed reconcile tick should be logged, decided by
/// how long the failure streak has run (#363).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureLogLevel {
    /// Streak still below [`FAILURE_ESCALATION_THRESHOLD`]: a transient blip
    /// the loop self-heals by retrying next tick — log at `WARN`.
    Warn,
    /// Streak has reached the threshold: sustained failure — log at `ERROR`.
    Error,
}

/// Consecutive-failure streak tracker for the reconcile loop (#363).
///
/// Pure state machine (no I/O) so the escalation-then-recover policy is
/// unit-testable directly, without capturing log output. The loop owns one
/// instance across ticks: each failure escalates the log level once the
/// streak is long enough, and the first success after a streak yields the
/// streak length so the caller can emit a single "recovered" line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FailureStreak {
    /// Number of consecutive failed ticks observed so far.
    consecutive: u32,
}

impl FailureStreak {
    /// Record a failed tick, returning the level it should log at.
    ///
    /// Escalates to [`FailureLogLevel::Error`] once the streak reaches
    /// [`FAILURE_ESCALATION_THRESHOLD`] consecutive failures, and stays
    /// there for the rest of the streak.
    fn record_failure(&mut self) -> FailureLogLevel {
        // Saturating so an extraordinarily long outage can't wrap the
        // counter; past the threshold the exact value only feeds the log.
        self.consecutive = self.consecutive.saturating_add(1);
        if self.consecutive >= FAILURE_ESCALATION_THRESHOLD {
            FailureLogLevel::Error
        } else {
            FailureLogLevel::Warn
        }
    }

    /// Record a successful tick, resetting the streak.
    ///
    /// Returns `Some(n)` with the number of failures that just ended when a
    /// streak was in progress — the caller emits one `INFO` recovery line —
    /// or `None` in steady state (success following success), so a healthy
    /// node logs nothing extra.
    fn record_success(&mut self) -> Option<u32> {
        let ended = (self.consecutive > 0).then_some(self.consecutive);
        self.consecutive = 0;
        ended
    }
}

/// Periodically reconcile the `envd-staged` label so it tracks the staged
/// binary and the runtime capability gate for the life of the process,
/// rather than the boot-time snapshot [`advertise_envd_staged`] takes
/// (#282, #379).
///
/// Closes the footgun where a node advertises readiness once at boot and
/// keeps it forever: if the staged binary later disappears (ephemeral
/// hostPath across a reboot, operator cleanup, corruption) the gate would
/// otherwise keep scheduling sandboxes that then CrashLoop on the missing
/// entrypoint — i.e. #272 recurs. Each tick re-runs
/// [`reconcile_envd_staged`], withdrawing the label within one interval
/// of the binary vanishing and re-asserting it within one interval of it
/// being restored.
///
/// The `runsc_root_failed` gate (#379) triggers the same withdrawal: once
/// the node reconciler proves, for a real container, that no runsc root is
/// resolvable, the label stays withdrawn for the life of the process so
/// the warm pool stops scheduling further doomed pause jobs on this node.
///
/// Intended to be `tokio::spawn`ed after the boot-time advertise; it
/// never returns under normal operation — the `loop` only ends if the
/// task is cancelled (runtime shutdown) or panics. Patch failures are
/// logged and retried on the next tick rather than escalated — a
/// transient apiserver error must not take down the DaemonSet's reconcile
/// loop. To keep a self-healing blip from reading as an incident (#363),
/// the per-tick failure log starts at `WARN` and only escalates to `ERROR`
/// after [`FAILURE_ESCALATION_THRESHOLD`] consecutive failures; the first
/// successful tick after a streak emits one `INFO` "recovered" line. The
/// `envd_advertised_total{result="patch_failed"}` counter increments on
/// every failed tick regardless, so alerting keys off the metric rate, not
/// the log severity. The caller is expected to *supervise* the returned task so a
/// panic (rather than fail open silently) brings the process down for
/// kubelet to restart; see the binary's spawn site.
///
/// # Panics
///
/// `interval` must be non-zero: `tokio::time::interval(Duration::ZERO)`
/// panics. The binary's CLI rejects a zero interval at parse time so this
/// is unreachable in production; callers constructing the duration
/// directly must uphold the same invariant.
///
/// # Arguments
///
/// * `client` — in-cluster kube client (same ServiceAccount as the boot
///   advertise; its ClusterRole already grants `patch` on `nodes`).
/// * `node_name` — this node's name (`NODE_NAME` downward-API env var).
/// * `envd_bin_path` — where `stage-envd` places the binary.
/// * `required_handler` — the gVisor handler name to require in
///   `Node.status.runtimeHandlers` ([`DEFAULT_GVISOR_RUNTIME_HANDLER`] in
///   production); an absent handler withdraws the label (#417).
/// * `interval` — gap between ticks; see [`DEFAULT_RECONCILE_INTERVAL`].
/// * `runsc_root_failed` — shared gate set by the SnapshotJob reconciler
///   when [`RuntimeError::RunscRootUnresolved`] fires for a real container
///   (#379). When true the label is withdrawn every tick regardless of
///   binary presence.
pub async fn run_envd_staged_reconciler(
    client: Client,
    node_name: String,
    envd_bin_path: PathBuf,
    required_handler: String,
    interval: Duration,
    runsc_root_failed: Arc<AtomicBool>,
) {
    let nodes: Api<Node> = Api::all(client);
    // One-shot startup line at `info` so "is the reconciler alive?" is
    // answerable at the default log level — the per-tick `Advertised`
    // event logs at `debug` to avoid steady-state spam.
    tracing::info!(
        node = %node_name,
        label = ENVD_STAGED_LABEL,
        interval_secs = interval.as_secs(),
        path = %envd_bin_path.display(),
        "envd-staged label reconciler started (#282)"
    );
    let mut ticker = tokio::time::interval(interval);
    // `MissedTickBehavior::Delay` keeps the spacing stable if a tick is
    // missed (slow apiserver, task starvation): the next tick lands one
    // full interval after the late one rather than burst-firing to catch
    // up. Matches the controller's retention sweep
    // (`sandbawks-controller/src/bin/sandbawks-controller.rs`).
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The boot-time advertise already fired before this task was spawned,
    // so skip burning the immediate first tick re-asserting the same
    // label; the first reconcile then lands one full interval later.
    ticker.tick().await;
    // Tracks the consecutive `patch_failed` streak so a transient apiserver
    // blip logs at WARN and only a sustained outage escalates to ERROR (#363).
    let mut failures = FailureStreak::default();
    loop {
        ticker.tick().await;
        let outcome = reconcile_envd_staged(
            &nodes,
            &node_name,
            &envd_bin_path,
            &required_handler,
            &runsc_root_failed,
        )
        .await;
        // Close any failure streak before logging the outcome: the first
        // successful tick after a run of failures emits one INFO line so an
        // operator reading logs sees the blip resolve, then the streak resets.
        if outcome.is_ok() {
            if let Some(failed_ticks) = failures.record_success() {
                tracing::info!(
                    node = %node_name,
                    label = ENVD_STAGED_LABEL,
                    failed_ticks,
                    "envd-staged label reconcile recovered after {failed_ticks} failed tick(s)"
                );
            }
        }
        match outcome {
            Ok(AdvertiseOutcome::Advertised) => {
                record_envd_staged_tick(ReconcileResult::Ok);
                tracing::debug!(
                    result = "ok",
                    node = %node_name,
                    label = ENVD_STAGED_LABEL,
                    "envd-staged label re-asserted (binary present)"
                );
            }
            Ok(AdvertiseOutcome::Withdrawn) => {
                record_envd_staged_tick(ReconcileResult::Missing);
                tracing::warn!(
                    result = "missing",
                    node = %node_name,
                    label = ENVD_STAGED_LABEL,
                    path = %envd_bin_path.display(),
                    "staged envd binary missing; withdrew envd-staged label so \
                     sandboxes stop scheduling here (#282)"
                );
            }
            Ok(AdvertiseOutcome::WithdrawnRunscRoot) => {
                record_envd_staged_tick(ReconcileResult::RunscRootFailed);
                tracing::warn!(
                    result = "runsc_root_failed",
                    node = %node_name,
                    label = ENVD_STAGED_LABEL,
                    "runsc-root-unresolved failure observed; keeping envd-staged \
                     label withdrawn — warm pool will not schedule here until the \
                     DaemonSet is restarted with a corrected runsc root mount (#379)"
                );
            }
            Ok(AdvertiseOutcome::WithdrawnRunscAbsent) => {
                record_envd_staged_tick(ReconcileResult::RunscAbsent);
                tracing::warn!(
                    result = "runsc_absent",
                    node = %node_name,
                    label = ENVD_STAGED_LABEL,
                    handler = %required_handler,
                    "configured runtime handler '{required_handler}' not in node \
                     status.runtimeHandlers; withdrew envd-staged label so \
                     sandboxes stop scheduling here{} (#417)",
                    runsc_remediation_hint(&required_handler)
                );
            }
            Err(err) => {
                // Counter increments on every failed tick regardless of log
                // severity, so alerting keys off the metric rate (#363).
                record_envd_staged_tick(ReconcileResult::PatchFailed);
                let level = failures.record_failure();
                let count = failures.consecutive;
                // First failures in a streak read as a transient apiserver blip
                // the loop self-heals next tick; only a sustained streak escalates
                // to ERROR. Two macro arms because `tracing` fixes the level at
                // compile time.
                match level {
                    FailureLogLevel::Warn => tracing::warn!(
                        result = "patch_failed",
                        node = %node_name,
                        label = ENVD_STAGED_LABEL,
                        consecutive_failures = count,
                        error = %err,
                        "failed to reconcile envd-staged label; retrying next \
                         tick (transient — escalates to ERROR after \
                         {FAILURE_ESCALATION_THRESHOLD} consecutive failures)"
                    ),
                    FailureLogLevel::Error => tracing::error!(
                        result = "patch_failed",
                        node = %node_name,
                        label = ENVD_STAGED_LABEL,
                        consecutive_failures = count,
                        error = %err,
                        "envd-staged label reconcile failing for {count} \
                         consecutive ticks; sustained apiserver connectivity \
                         loss (retrying every tick)"
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sandbawks_types::ENVD_STAGED_LABEL;

    /// The patch body targets `metadata.labels.<ENVD_STAGED_LABEL>` and
    /// nothing else, so a merge patch leaves operator-set labels intact.
    #[test]
    fn staged_label_patch_sets_only_the_staged_label() {
        let patch = staged_label_patch();
        let labels = patch
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(Value::as_object)
            .expect("patch carries metadata.labels object");
        assert_eq!(labels.len(), 1, "patch must touch exactly one label");
        assert_eq!(
            labels.get(ENVD_STAGED_LABEL).and_then(Value::as_str),
            Some(ENVD_STAGED_LABEL_VALUE),
            "patch must set the envd-staged label to its well-known value"
        );
    }

    /// The removal patch sets `metadata.labels.<ENVD_STAGED_LABEL>` to
    /// JSON `null` (RFC 7386 key deletion) and touches nothing else, so a
    /// merge patch only withdraws the one label.
    #[test]
    fn staged_label_removal_patch_nulls_only_the_staged_label() {
        let patch = staged_label_removal_patch();
        let labels = patch
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(Value::as_object)
            .expect("patch carries metadata.labels object");
        assert_eq!(labels.len(), 1, "patch must touch exactly one label");
        assert!(
            labels
                .get(ENVD_STAGED_LABEL)
                .expect("patch references the envd-staged label")
                .is_null(),
            "removal patch must set the envd-staged label to null"
        );
    }

    /// A present binary passes the fail-closed gate.
    #[test]
    fn ensure_binary_present_ok_when_file_exists() {
        let file = tempfile::NamedTempFile::new().expect("create temp file");
        assert!(ensure_binary_present(file.path()).is_ok());
    }

    /// A missing binary is rejected so the node never advertises a
    /// false-ready label.
    #[test]
    fn ensure_binary_present_errors_when_file_missing() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let missing = dir.path().join("envd");
        let err = ensure_binary_present(&missing).expect_err("missing binary must error");
        assert!(
            matches!(err, AdvertiseError::BinaryMissing(_)),
            "expected BinaryMissing, got {err:?}"
        );
    }

    /// Build a `Node` whose `status.runtimeHandlers` carries the named
    /// handlers, for the gate classifier tests.
    fn node_with_handlers(names: &[&str]) -> Node {
        use k8s_openapi::api::core::v1::{NodeRuntimeHandler, NodeStatus};
        Node {
            status: Some(NodeStatus {
                runtime_handlers: Some(
                    names
                        .iter()
                        .map(|name| NodeRuntimeHandler {
                            name: Some((*name).to_owned()),
                            features: None,
                        })
                        .collect(),
                ),
                ..NodeStatus::default()
            }),
            ..Node::default()
        }
    }

    /// A node whose reported handlers include `runsc` is `Present` (#417).
    #[test]
    fn runtime_handler_gate_present_when_handler_listed() {
        let node = node_with_handlers(&["runc", "runsc", "sandbawks-runsc"]);
        assert_eq!(
            runtime_handler_gate(&node, DEFAULT_GVISOR_RUNTIME_HANDLER),
            RuntimeHandlerGate::Present
        );
    }

    /// A node reporting a non-empty list *without* `runsc` is `Absent` —
    /// positive evidence the handler is missing (the #417 failure node).
    #[test]
    fn runtime_handler_gate_absent_when_handler_not_listed() {
        let node = node_with_handlers(&["runc", "runhcs-wcow-process"]);
        assert_eq!(
            runtime_handler_gate(&node, DEFAULT_GVISOR_RUNTIME_HANDLER),
            RuntimeHandlerGate::Absent
        );
    }

    /// The failure streak escalates WARN→ERROR at the threshold, reports the
    /// streak length once on recovery, then resets so steady state is silent
    /// (#363). Asserts the pure state machine directly, not log output.
    #[test]
    fn failure_streak_escalates_then_recovers() {
        let mut streak = FailureStreak::default();
        // Failures below the threshold stay at WARN: a transient blip.
        assert_eq!(streak.record_failure(), FailureLogLevel::Warn);
        assert_eq!(streak.record_failure(), FailureLogLevel::Warn);
        // The threshold-th consecutive failure escalates to ERROR...
        assert_eq!(streak.record_failure(), FailureLogLevel::Error);
        // ...and stays there for the rest of the streak.
        assert_eq!(streak.record_failure(), FailureLogLevel::Error);
        // The first success after the streak reports its length exactly once.
        assert_eq!(streak.record_success(), Some(4));
        // The streak is now reset: further successes are silent (steady state).
        assert_eq!(streak.record_success(), None);
        // A fresh failure starts a new streak back at WARN, not ERROR.
        assert_eq!(streak.record_failure(), FailureLogLevel::Warn);
    }

    /// The escalation threshold is the documented `WARN`→`ERROR` boundary: the
    /// first `THRESHOLD - 1` failures are WARN, the `THRESHOLD`-th is ERROR.
    #[test]
    fn failure_streak_escalates_exactly_at_threshold() {
        let mut streak = FailureStreak::default();
        for tick in 1..FAILURE_ESCALATION_THRESHOLD {
            assert_eq!(
                streak.record_failure(),
                FailureLogLevel::Warn,
                "failure #{tick} below threshold must stay WARN"
            );
        }
        assert_eq!(
            streak.record_failure(),
            FailureLogLevel::Error,
            "failure #{FAILURE_ESCALATION_THRESHOLD} must escalate to ERROR"
        );
    }

    /// A node reporting no handler list (older CRI/kubelet) is `Unknown` and
    /// must not be blocked — graceful degradation to pre-#417 behavior.
    #[test]
    fn runtime_handler_gate_unknown_when_status_absent_or_empty() {
        assert_eq!(
            runtime_handler_gate(&Node::default(), DEFAULT_GVISOR_RUNTIME_HANDLER),
            RuntimeHandlerGate::Unknown,
            "no status at all → Unknown"
        );
        assert_eq!(
            runtime_handler_gate(&node_with_handlers(&[]), DEFAULT_GVISOR_RUNTIME_HANDLER),
            RuntimeHandlerGate::Unknown,
            "empty handler list → Unknown"
        );
    }
}
