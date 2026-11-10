use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Label stamped on every `Sandbox` the warm-pool reconciler manages.
///
/// Its value is the owning `SandboxWarmPool` name. The reconciler counts
/// `Running` sandboxes carrying it to compute `status.ready`; the claim
/// path removes this label (atomically, guarded by `resourceVersion`) to
/// take a sandbox out of the pool. Because the label is an external
/// contract that outlives any single controller/api version, the string
/// is pinned by a test (`warm_pool_label_and_annotation_consts_are_stable`).
pub const WARM_POOL_LABEL: &str = "sandbawks.dev/warm-pool";

/// Annotation set on a sandbox at the instant it is claimed out of a pool.
///
/// Value is an RFC 3339 timestamp. Purely observational — the *atomic*
/// act of claiming is removing [`WARM_POOL_LABEL`]; this annotation only
/// records when the claim happened so operators can audit handoffs.
pub const CLAIMED_AT_ANNOTATION: &str = "sandbawks.dev/claimed-at";

/// Annotation recording the `SandboxTemplate.resourceVersion` a warm member
/// was minted against (issue #397).
///
/// The warm-pool reconciler stamps the template's live `resourceVersion`
/// here at mint time, then on each reconcile compares it to the template's
/// current `resourceVersion`. An unclaimed member whose stamp no longer
/// matches (the template was edited — new image, runtime class, or
/// resources) is evicted and re-minted from the updated template, so a
/// requester never claims a sandbox built from a superseded spec. Members
/// minted before this feature carry no annotation and are treated as stale
/// on the first reconcile that knows the current version. Like the other
/// warm-pool keys this is an external contract pinned by a stability test
/// (`warm_pool_label_and_annotation_consts_are_stable`).
pub const TEMPLATE_RV_ANNOTATION: &str = "sandbawks.dev/warm-pool-template-rv";

/// Pool of `Running`, ready-to-claim sandboxes for instant cold-start
/// handoff.
///
/// The controller keeps `status.ready` between `spec.min_ready` and
/// `spec.max_ready` by booting fresh sandboxes from `spec.template_ref`
/// and holding them `Running`. A claim is a control-plane reassignment —
/// the gateway re-owns a live member and hands back its `envd` address —
/// so there is no pod creation or restore on the claim path.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.dev",
    version = "v1alpha1",
    kind = "SandboxWarmPool",
    plural = "sandboxwarmpools",
    shortname = "sbxwp",
    namespaced,
    status = "SandboxWarmPoolStatus",
    printcolumn = r#"{"name":"Template","type":"string","jsonPath":".spec.templateRef"}"#,
    printcolumn = r#"{"name":"Min","type":"integer","jsonPath":".spec.minReady"}"#,
    printcolumn = r#"{"name":"Max","type":"integer","jsonPath":".spec.maxReady"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"Failed","type":"integer","jsonPath":".status.failed"}"#,
    printcolumn = r#"{"name":"Total","type":"integer","jsonPath":".status.total"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SandboxWarmPoolSpec {
    /// Name of a `SandboxTemplate` in the same namespace whose image,
    /// resources, and envd build define the pre-paused sandboxes.
    pub template_ref: String,

    /// Lower bound on `status.ready`. The controller will create new
    /// sandboxes until the warm count reaches this floor.
    pub min_ready: u32,

    /// Upper bound on `status.ready`. The controller will not warm
    /// additional sandboxes once the count reaches this ceiling.
    pub max_ready: u32,
}

/// Observed state of a `SandboxWarmPool`, written by the controller.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SandboxWarmPoolStatus {
    /// Number of sandboxes in the pool currently `Running` and ready for
    /// handoff.
    #[serde(default)]
    pub ready: u32,

    /// Members observed this reconcile in a terminal `Failed`/`Terminating`
    /// phase. The reconciler reaps (deletes) these every pass, so a sustained
    /// non-zero value flags a pod-create failure storm — e.g. a transient
    /// admission-webhook outage — churning the pool. Surfacing it makes the
    /// condition visible in `kubectl get`/`describe`, which previously
    /// reported only `ready` (issue #457).
    #[serde(default)]
    pub failed: u32,

    /// Total members observed this reconcile across every phase
    /// (`ready` + in-flight + reclaimed/evicted + `failed`) — the true count
    /// of warm `Sandbox` CRs the pool owns. `maxReady` bounds only `ready`,
    /// so surfacing the total is what makes runaway CR growth visible
    /// (issue #457).
    ///
    /// Unlike `ready`, this field has **no ceiling**: `spec.maxReady` does not
    /// bound it, and that is intentional — `total` is the leak indicator, so a
    /// spike above `maxReady` is the signal, not an anomaly to clamp. A
    /// dashboard should plot it raw, not assume `total <= maxReady`.
    #[serde(default)]
    pub total: u32,

    /// Names of the `Running` members currently ready to claim.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<String>,

    /// Running count of consecutive mint failures the breaker has observed
    /// (issue #615).
    ///
    /// Incremented each reconcile that reaps one or more `Failed`/`Terminating`
    /// members (a "mint failure" — a member that was minted and then folded to
    /// a terminal phase), and reset to `0` on a reconcile that reaps none. When
    /// it reaches the breaker's trip threshold the pool stops minting and enters
    /// backoff. Persisted in `status` because the warm-pool reconcile is
    /// otherwise stateless across passes — the breaker's memory lives here, the
    /// same way the Sandbox reconcile anchors its retry budgets in
    /// `SandboxStatus`.
    #[serde(default)]
    pub consecutive_mint_failures: u32,

    /// Whether the mint-failure circuit breaker is currently tripped — the pool
    /// is backing off and minting zero new members (issue #615).
    ///
    /// Surfaced so a pool stuck below `minReady` because the breaker is open is
    /// not silently mistaken for a healthy pool that merely lost members. Pairs
    /// with [`Self::degraded_reason`] for the human-readable cause and
    /// [`Self::backoff_until`] for when the next probe mint is allowed.
    #[serde(default)]
    pub degraded: bool,

    /// Human-readable reason the breaker tripped — the dominant failure message
    /// from the reaped members (issue #615). `None` while the breaker is closed.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub degraded_reason: Option<String>,

    /// RFC 3339 timestamp until which the breaker suppresses minting (issue
    /// #615). While `now < backoff_until` the pool mints zero members; once it
    /// elapses a single **probe** mint is allowed. `None` when the breaker is
    /// closed. Stored as an RFC 3339 string to match the Sandbox reconcile's
    /// sibling timestamp convention.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub backoff_until: Option<String>,

    /// Number of times the breaker has tripped (or re-tripped after a failed
    /// probe) in the current open episode — the exponent driving the capped
    /// exponential backoff window (issue #615). Reset to `0` when the breaker
    /// closes. A probe that re-trips increments it so the window grows.
    #[serde(default)]
    pub backoff_count: u32,
}
