use std::collections::{BTreeMap, HashMap};

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::resources::{ResourceQuantity, ResourceRequests};

/// The primary resource: one Sandbox = one E2B sandbox = one Pod (when running).
///
/// Naming the CRD plural `sandboxes` matches the english spelling
/// expected by `kubectl get sandboxes` and by the E2B `/sandboxes`
/// endpoint. Short name `sbx` matches E2B's sandbox ID prefix.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.dev",
    version = "v1alpha1",
    kind = "Sandbox",
    plural = "sandboxes",
    shortname = "sbx",
    namespaced,
    status = "SandboxStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    // Issue #624: a warm-pool member sits in `Running` both while unclaimed
    // and after it is handed out, so `Phase` alone cannot distinguish the
    // two. The claim path stamps `sandbawks.dev/claimed-at` (RFC 3339) at the
    // instant of claim, so this column is empty for an unclaimed pool member
    // and shows the claim time once it is in use. Placed next to `Phase` so
    // the claim distinction reads beside it.
    //
    // The annotation key contains a dot, so the JSONPath escapes it as `\.`
    // (backslash-dotted-field notation). This is the form `client-go`'s
    // `util/jsonpath` — the engine the apiserver's CR table printer uses for
    // `additionalPrinterColumns` — actually resolves. Verified empirically
    // against `kubectl` v1.31 (`kubectl create --dry-run=client -o jsonpath`):
    // this `\.` path yields the timestamp on a claimed object and empty on an
    // unclaimed one, whereas the bracket-quoted form
    // (`annotations['sandbawks.dev/claimed-at']`) renders empty for *every*
    // row — the silent-failure mode we must avoid. The `\\` in this Rust raw
    // string collapses to a single backslash when kube-rs JSON-parses the
    // `printcolumn` literal, so the emitted YAML carries exactly one.
    printcolumn = r#"{"name":"Claimed","type":"date","jsonPath":".metadata.annotations.sandbawks\\.dev/claimed-at"}"#,
    printcolumn = r#"{"name":"Template","type":"string","jsonPath":".spec.templateRef"}"#,
    printcolumn = r#"{"name":"Runtime","type":"string","jsonPath":".spec.runtimeClassName"}"#,
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".status.assignedNode"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SandboxSpec {
    /// Name of a `SandboxTemplate` in the same namespace.
    pub template_ref: String,

    /// Override the template's default RuntimeClass.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub runtime_class_name: Option<String>,

    pub resources: ResourceRequests,

    /// The **idle window** in seconds: how long the sandbox may go with no
    /// client activity before it is auto-paused (if `auto_pause`) or killed
    /// (if not). Issue #481.
    ///
    /// The controller measures the window from
    /// [`SandboxStatus::last_activity_timestamp`] (falling back to
    /// `creationTimestamp` until the first activity stamp lands), so this is
    /// the *raw configured duration* — it does not grow with elapsed time.
    /// The gateway writes the caller's requested value verbatim on create,
    /// `set_timeout`, and warm-claim, and refreshes the activity baseline at
    /// each of those engagement points so "`N` seconds from now" holds.
    ///
    /// `0` is the "idle timeout disabled" sentinel (warm-pool members are
    /// minted with `0` and never auto-pause while unclaimed).
    pub timeout_seconds: u32,

    /// If true, the timeout transitions the sandbox to `Paused`. If
    /// false, the timeout transitions to `Terminating`.
    #[serde(default)]
    pub auto_pause: bool,

    /// Phase the API gateway wants the controller to drive toward.
    ///
    /// `Some(Paused)` requests pause; `Some(Running)` requests resume.
    /// Replaces the legacy `sandbawks.dev/desired-phase` annotation; the
    /// annotation reader is kept for one plan for backwards compatibility
    /// and removed in Plan 05.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub desired_phase: Option<SandboxPhase>,

    /// Environment variables injected into the sandbox process.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,

    /// Opaque K/V passed through from the E2B API. Not interpreted by
    /// the controller — surfaced back via `GET /sandboxes`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,

    /// Optional egress policy. The controller materializes a
    /// `networking.v1.NetworkPolicy` selecting this sandbox's Pod for
    /// **every** sandbox (issue #443); `None` here — like an empty
    /// `egressAllow` — leaves that policy at its default-deny-egress
    /// baseline. A non-empty `egressAllow` opens the listed destinations.
    /// Enforcement requires a NetworkPolicy-aware CNI (Cilium).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub network_policy: Option<NetworkPolicy>,

    /// How long to retain this sandbox's snapshot in the snapshot store
    /// after the controller writes it. Encoded as an ISO-8601 duration
    /// string (`P7D`, `PT24H`, `PT30M`, …) so the wire shape is the
    /// same one Kubernetes uses for `pod.spec.activeDeadlineSeconds`-
    /// adjacent time fields without locking us into a single unit.
    ///
    /// Resolution order at GC time:
    ///
    /// 1. `Sandbox.spec.snapshotRetention` (this field).
    /// 2. Per-namespace annotation `sandbawks.dev/snapshot-retention`.
    /// 3. Controller-wide default
    ///    (`snapshotStore.retention.defaultDuration` Helm value).
    ///
    /// `None` means "fall through to the namespace / controller
    /// default"; explicit `Some` always wins.
    ///
    /// Parsed by [`crate::snapshot::parse_iso8601_duration`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_retention: Option<String>,

    /// How long a *paused* [`PersistenceMode::Filesystem`] sandbox may keep its
    /// retained PVC(s) before the controller garbage-collects the whole Sandbox
    /// (cascading the PVCs via their ownerReferences) — epic #603 Phase 5 /
    /// issue #608. The `filesystem`-mode analog of [`Self::snapshot_retention`]:
    /// a snapshot-mode pause parks state as an S3 object under the existing
    /// snapshot retention sweep, but a filesystem-mode pause keeps a live PVC
    /// whose storage bills until something reclaims it.
    ///
    /// Encoded as an ISO-8601 duration string (`P7D`, `PT24H`, …), same wire
    /// shape as [`Self::snapshot_retention`]. The window is measured from the
    /// moment the sandbox finished suspending (the `Suspended` status
    /// condition's transition time), not from creation, so an actively
    /// resumed-and-re-paused sandbox keeps resetting its clock.
    ///
    /// Resolution order at GC time:
    ///
    /// 1. `Sandbox.spec.pvcRetention` (this field).
    /// 2. Controller-wide default (`pvcRetention.defaultDuration` Helm value),
    ///    gated behind the controller's `pvcRetention.enabled` switch.
    ///
    /// `None` means "fall through to the controller default"; explicit `Some`
    /// always wins. When PVC retention is disabled controller-wide (the
    /// default), paused filesystem sandboxes are never auto-collected and this
    /// field is inert.
    ///
    /// Parsed by [`crate::snapshot::parse_iso8601_duration`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pvc_retention: Option<String>,

    /// Scheduling tolerations stamped onto the sandbox Pod's
    /// `spec.tolerations` so it can land on tainted nodes (e.g. a dedicated
    /// Kata/Azure node pool that repels untolerated Pods) — issue #592.
    ///
    /// Resolved from the template's [`crate::SandboxTemplateSpec::tolerations`]
    /// at the controller's pod-creation edge ([`resolve_pod_inputs`] in
    /// `sandbawks-controller`), so both cold-start and warm-pool members
    /// inherit it. An explicit value on the CR wins; the template default only
    /// fills an empty slot — the same override precedence as
    /// [`Self::runtime_class_name`].
    ///
    /// `None` (the default) leaves the Pod with no extra tolerations, scheduled
    /// exactly as before. `Option<T>` keeps the field non-required in the
    /// generated CRD schema and `skip_serializing_if` keeps `None` out of
    /// serialized YAML rather than emitting `tolerations: null`.
    ///
    /// [`resolve_pod_inputs`]: ../../sandbawks_controller/reconcile/fn.resolve_pod_inputs.html
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerations: Option<Vec<Toleration>>,

    /// Node-selector labels stamped onto the sandbox Pod's
    /// `spec.nodeSelector` so it is constrained to a labeled node pool
    /// (issue #592). Resolved from the template's
    /// [`crate::SandboxTemplateSpec::node_selector`] alongside
    /// [`Self::tolerations`]; same override precedence and `None`-default
    /// semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<BTreeMap<String, String>>,

    /// Pod-level security context stamped onto the sandbox Pod's
    /// `spec.securityContext` (issue #617) — most importantly `fsGroup`, so a
    /// non-root container can write to a dynamically provisioned
    /// `volumeClaimTemplates` PVC (which otherwise mounts `root:root`).
    ///
    /// Resolved from the template's
    /// [`crate::SandboxTemplateSpec::security_context`] at the controller's
    /// pod-creation edge ([`resolve_pod_inputs`]) alongside [`Self::tolerations`]
    /// / [`Self::node_selector`]; same override precedence (an explicit value on
    /// the CR wins; the template default only fills an empty slot) and
    /// `None`-default semantics — `None` leaves the Pod with no `securityContext`,
    /// exactly as before.
    ///
    /// [`resolve_pod_inputs`]: ../../sandbawks_controller/reconcile/fn.resolve_pod_inputs.html
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_context: Option<PodSecurityContext>,

    /// How this sandbox's writable state survives a pause/resume cycle
    /// (epic #603). `None` inherits the template's
    /// [`crate::SandboxTemplateSpec::persistence_mode`] at the controller's
    /// pod-creation edge ([`resolve_pod_inputs`]); if neither is set the
    /// effective mode is [`PersistenceMode::Snapshot`] — the current behavior,
    /// unchanged. An explicit value on the CR always wins, the same override
    /// precedence as [`Self::runtime_class_name`] / [`Self::tolerations`].
    ///
    /// `Option<T>` keeps the field non-required in the generated CRD schema and
    /// `skip_serializing_if` keeps `None` out of serialized YAML rather than
    /// emitting `persistenceMode: null`.
    ///
    /// [`resolve_pod_inputs`]: ../../sandbawks_controller/reconcile/fn.resolve_pod_inputs.html
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence_mode: Option<PersistenceMode>,

    /// Sandbox-owned `PersistentVolumeClaim` requests whose volumes outlive the
    /// Pod across a `filesystem`-mode pause (epic #603). `None` inherits the
    /// template's [`crate::SandboxTemplateSpec::volume_claim_templates`] at the
    /// pod-creation edge alongside [`Self::persistence_mode`]; an explicit value
    /// on the CR wins and the template default only fills an empty slot — same
    /// precedence as the scheduling hints.
    ///
    /// No controller behavior yet: the PVC objects are provisioned and mounted
    /// in #603 Phase 2. `None` (the default) leaves a sandbox with no extra
    /// volumes, exactly as today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_claim_templates: Option<Vec<VolumeClaimTemplate>>,

    /// Clone-from-snapshot: stand this *new* sandbox up from the saved state of
    /// a *different* sandbox rather than cold-booting (issue #416).
    ///
    /// When set on a freshly-created sandbox, the controller — instead of
    /// booting a fresh container — restores the referenced source's last-good
    /// state, then transitions straight to `Running`. The mechanism follows the
    /// sandbox's [resolved persistence mode](resolve_effective_persistence_mode):
    ///
    /// - **`snapshot` mode** (gVisor S3 snapshot): the controller resolves the
    ///   source's `status.snapshotRef` / digests, stamps them onto this
    ///   sandbox's status, and drives the existing scheduler-placed restore flow
    ///   (`Creating → Resuming → Stage → CreateRestorePod → Running`, epic #484).
    /// - **`filesystem` mode** (retained PVC, epic #603): each PVC this sandbox
    ///   provisions is CSI-cloned from the source sandbox's correspondingly-named
    ///   retained PVC via the PVC `dataSource`, then a fresh Pod cold-boots over
    ///   the cloned volumes. The source must be `Suspended` (filesystem-paused)
    ///   so its PVC is quiescent.
    ///
    /// The value is parsed by [`RestoreSource::parse`]:
    ///
    /// - a **source sandbox ID** (e.g. `sbx-warm-l5dkn`) — the controller looks
    ///   up that Sandbox CR in the same namespace and reads the artifact it needs
    ///   for the resolved mode. Works even if the source is `Failed`, as long as
    ///   the source CR still exists with a snapshot recorded (snapshot mode) or
    ///   its retained PVC(s) (filesystem mode).
    /// - a raw **`s3://…/manifest.json` URL** (snapshot mode, recovery path for a
    ///   *deleted* source) — recognized but not yet actioned by the controller;
    ///   it folds the sandbox to `Failed` with a message directing the operator
    ///   to pass the source sandbox ID instead.
    ///
    /// Before restoring, the controller validates the source against this sandbox
    /// (runtime class; template image, warn-only for legacy snapshots) and
    /// refuses (`Failed` + condition) on mismatch. `None` (the default) is a
    /// normal cold-booted sandbox, unchanged.
    ///
    /// # Known limitation: cloned guest identity (snapshot mode)
    ///
    /// A gVisor `snapshot`-mode restore reconstructs the entire Sentry (guest
    /// kernel) from the checkpoint, so the **source's** in-kernel identity rides
    /// along: `gethostname(2)` / `uname -n`, the netstack interface MAC + IP, and
    /// any captured `/etc/machine-id` / boot-id all return the *source's* values
    /// in the clone. Meanwhile the kubelet-managed furniture binds
    /// (`/etc/hostname`, `/etc/hosts`, `/etc/resolv.conf`) reflect the *new*
    /// Pod — see [`RUNTIME_MANAGED_BIND_DESTS`] in the node's gVisor adapter — so
    /// a clone sees a split between its kernel hostname (source) and `/etc/hostname`
    /// (destination). This does **not** fail the restore (the resume path already
    /// restores into a fresh Pod with a different IP/MAC), but a workload that
    /// keys on hostname / machine-id / MAC (service registration, clustering,
    /// licensing) inherits the source's identity rather than getting a unique one.
    /// v1 avoids live *collisions* because a clone source is always quiescent
    /// (`Paused` / `Failed` for snapshot, `Suspended` for filesystem), so two
    /// identical identities are never on the wire at once.
    ///
    /// The reconciliation strategy for this is decided in the design spec
    /// `docs/superpowers/specs/2026-06-24-clone-guest-identity-reconcile-design.md`
    /// (issue #643): an `envd`-driven one-shot reset on snapshot-mode clones that
    /// rewrites the UTS hostname and regenerates `/etc/machine-id` & friends,
    /// while *accepting* the inherited netstack MAC/IP (the host veth carries the
    /// real L2/L3 identity). The controller marks a clone needing this reset with
    /// [`crate::labels::CLONE_IDENTITY_RESET_ANNOTATION`], gated by the pure
    /// predicate [`clone_needs_guest_identity_reset`].
    ///
    /// [`RUNTIME_MANAGED_BIND_DESTS`]: ../../sandbawks_node/gvisor/index.html
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_from: Option<String>,
}

/// A parsed [`SandboxSpec::restore_from`] reference (issue #416).
///
/// `restore_from` accepts two disambiguated-by-prefix forms; this enum is the
/// pure classifier the controller branches on. Borrowing from the input keeps
/// it allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreSource<'a> {
    /// A source Sandbox **CR name** in the same namespace (e.g.
    /// `sbx-warm-l5dkn`). The controller reads the artifact it needs (snapshot
    /// digests, or retained PVCs) from that sibling CR.
    SandboxId(&'a str),

    /// A raw `s3://bucket/key/manifest.json` snapshot-manifest URL — the
    /// recovery path decoupled from the source CR's lifetime (snapshot mode
    /// only).
    SnapshotUrl(&'a str),
}

impl<'a> RestoreSource<'a> {
    /// Classify a [`SandboxSpec::restore_from`] value.
    ///
    /// An `s3://` prefix (case-insensitive scheme, per RFC 3986) selects
    /// [`Self::SnapshotUrl`]; anything else is treated as a
    /// [`Self::SandboxId`]. The raw value is carried through unmodified — the
    /// caller validates the sandbox ID against the cluster and parses the URL's
    /// bucket/key downstream.
    ///
    /// # Examples
    ///
    /// ```
    /// use sandbawks_types::sandbox::RestoreSource;
    ///
    /// assert_eq!(
    ///     RestoreSource::parse("sbx-warm-l5dkn"),
    ///     RestoreSource::SandboxId("sbx-warm-l5dkn"),
    /// );
    /// assert_eq!(
    ///     RestoreSource::parse("s3://snaps/snapshots/sbx-x/manifest.json"),
    ///     RestoreSource::SnapshotUrl("s3://snaps/snapshots/sbx-x/manifest.json"),
    /// );
    /// // The scheme match is case-insensitive.
    /// assert!(matches!(
    ///     RestoreSource::parse("S3://snaps/m.json"),
    ///     RestoreSource::SnapshotUrl(_),
    /// ));
    /// ```
    pub fn parse(value: &'a str) -> Self {
        // A fixed 5-byte case-insensitive scheme compare is the cheapest
        // correct check (cheaper than building a lowercased copy).
        let scheme = "s3://";
        if value.len() >= scheme.len() && value[..scheme.len()].eq_ignore_ascii_case(scheme) {
            RestoreSource::SnapshotUrl(value)
        } else {
            RestoreSource::SandboxId(value)
        }
    }
}

/// A pod scheduling toleration: a serde/schemars-friendly mirror of
/// `k8s_openapi::api::core::v1::Toleration` (issue #592).
///
/// We mirror rather than reuse the `k8s_openapi` type because this crate's
/// CRDs derive [`schemars::JsonSchema`] to emit their OpenAPI v3 schema, and
/// the `k8s-openapi` types do not implement `JsonSchema` under the features
/// this workspace enables. The field set and wire shape (camelCase
/// `tolerationSeconds`) match the upstream type exactly, so the controller
/// converts losslessly into the `k8s_openapi` `Toleration` when building the
/// `PodSpec`. See the same mirror rationale on [`NetworkPolicy`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// Taint effect to match. Empty matches all effects. One of
    /// `NoSchedule`, `PreferNoSchedule`, `NoExecute`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub effect: Option<String>,

    /// Taint key the toleration applies to. Empty (with `operator: Exists`)
    /// matches all keys.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key: Option<String>,

    /// Relationship between key and value. `Exists` (value must be empty) or
    /// `Equal` (the default when unset, per Kubernetes).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub operator: Option<String>,

    /// For a `NoExecute` toleration, how long (seconds) the Pod stays bound
    /// after the taint is added. Unset means tolerate forever.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub toleration_seconds: Option<i64>,

    /// Taint value the toleration matches (empty with `operator: Exists`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub value: Option<String>,
}

/// Pod-level security context: a serde/schemars-friendly mirror of the subset
/// of `k8s_openapi::api::core::v1::PodSecurityContext` a sandbox needs
/// (issue #617).
///
/// We mirror rather than reuse the `k8s_openapi` type for the same reason as
/// [`Toleration`] / [`VolumeClaimTemplate`]: this crate's CRDs derive
/// [`schemars::JsonSchema`] to emit their OpenAPI v3 schema, and the
/// `k8s-openapi` types do not implement `JsonSchema` under the features this
/// workspace enables. The field set and wire shape (camelCase) match the
/// upstream type, so the controller converts losslessly into the `k8s_openapi`
/// `PodSecurityContext` when building the `PodSpec`.
///
/// The load-bearing field is [`Self::fs_group`]: with a dynamically provisioned
/// `volumeClaimTemplates` PVC, the kubelet chowns the volume to that GID (with
/// setgid) and adds it to every container's supplemental groups, so a non-root
/// workload can write to its own workspace volume. Without it the PVC mounts
/// `root:root` and a non-root container cannot write (epic #603 filesystem
/// mode). Applied by the kubelet/CSI before the container starts, so it is
/// runtime-agnostic (works under gVisor `runsc` and Kata alike).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodSecurityContext {
    /// Supplemental GID that owns the Pod's volumes. The kubelet recursively
    /// chowns each volume to `root:fsGroup` with the setgid bit, so a container
    /// running as any UID that has this GID can write — the fix for a
    /// root-owned `volumeClaimTemplates` PVC.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fs_group: Option<i64>,

    /// UID every container in the Pod runs as (unless a container overrides it).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub run_as_user: Option<i64>,

    /// Primary GID every container in the Pod runs as.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub run_as_group: Option<i64>,

    /// Require all containers to run as a non-root user; the kubelet refuses to
    /// start the Pod if a container would run as UID 0.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub run_as_non_root: Option<bool>,

    /// When the kubelet applies [`Self::fs_group`] ownership: `Always` (chown
    /// every mount) or `OnRootMismatch` (skip when the top-level dir already
    /// matches — far cheaper for large, already-correct volumes).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fs_group_change_policy: Option<String>,

    /// Extra GIDs added to every container's process, beyond the primary group.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub supplemental_groups: Vec<i64>,
}

/// Egress policy for the sandbox pod.
///
/// The controller materializes a default-deny-egress
/// `networking.v1.NetworkPolicy` for every sandbox (issue #443); this
/// struct's `egress_allow` list expands the permitted destinations on top
/// of that baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicy {
    /// Egress destinations to permit. Empty means deny-all egress (the
    /// default-deny baseline is unchanged).
    ///
    /// IP/CIDR entries (`10.0.0.0/8`, `2001:db8::1`) become exact `ipBlock`
    /// egress rules. DNS-name entries cannot be encoded as destinations in a
    /// standard NetworkPolicy, so their presence instead opens DNS egress
    /// (port 53) and relies on the cluster's name-aware CNI layer (Cilium
    /// FQDN policy) to enforce the resolved destination.
    #[serde(default)]
    pub egress_allow: Vec<String>,
}

/// Combine a baseline egress policy with an overlay, taking the **union** of
/// their `egress_allow` destinations (issue #537).
///
/// This layers a per-create egress override (design option 2) over a
/// template-level baseline (option 1): the result authorizes every destination
/// either side names, so a sandbox reaches the template's standing allow-list
/// *plus* anything the create request added. Precedence is union, not replace —
/// neither side can revoke the other's destinations.
///
/// Order is preserved with `base`'s entries first, then any `overlay` entries
/// not already present, and exact-string duplicates are dropped so the
/// materialized `NetworkPolicy` carries no redundant egress rules. Returns
/// `None` when the union is empty (both inputs absent or carrying empty
/// allow-lists) so an egress-free sandbox keeps the `None` default-deny shape
/// rather than serializing an empty `networkPolicy: {}`.
///
/// The allow-lists are expected to hold a handful of destinations, so the
/// `contains` membership check stays O(n·m) on tiny `n`/`m` and avoids
/// allocating a `HashSet` (which would also lose insertion order).
///
/// # Examples
///
/// ```
/// use sandbawks_types::sandbox::{merge_egress_policies, NetworkPolicy};
///
/// let template = NetworkPolicy { egress_allow: vec!["git.example.com".into()] };
/// let per_create = NetworkPolicy {
///     egress_allow: vec!["git.example.com".into(), "10.0.0.0/8".into()],
/// };
/// let merged = merge_egress_policies(Some(template), Some(per_create)).unwrap();
/// assert_eq!(merged.egress_allow, vec!["git.example.com", "10.0.0.0/8"]);
/// ```
pub fn merge_egress_policies(
    base: Option<NetworkPolicy>,
    overlay: Option<NetworkPolicy>,
) -> Option<NetworkPolicy> {
    match (base, overlay) {
        (None, None) => None,
        // At most one side present: hand it back as-is, still normalizing an
        // empty allow-list to `None` so the result is canonical. No new `Vec`.
        (Some(np), None) | (None, Some(np)) => (!np.egress_allow.is_empty()).then_some(np),
        (Some(base), Some(overlay)) => {
            let mut egress_allow = base.egress_allow;
            egress_allow.reserve(overlay.egress_allow.len());
            for dest in overlay.egress_allow {
                // De-dup against what's already accumulated; preserves the
                // baseline-first ordering the doc promises.
                if !egress_allow.contains(&dest) {
                    egress_allow.push(dest);
                }
            }
            (!egress_allow.is_empty()).then_some(NetworkPolicy { egress_allow })
        }
    }
}

/// How a sandbox's writable state is persisted across a pause/resume cycle
/// (epic #603).
///
/// Selected on the [`SandboxSpec`] / [`crate::SandboxTemplateSpec`] and resolved
/// at the controller's pod-creation edge. The default, [`Self::Snapshot`],
/// reproduces the pre-#603 behavior exactly, so an existing template that never
/// sets the field keeps snapshotting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
pub enum PersistenceMode {
    /// Memory + filesystem checkpoint via the runtime's snapshot path (gVisor
    /// `runsc checkpoint`), restored in place on resume. The current behavior
    /// and the default — selecting it changes nothing.
    #[default]
    Snapshot,

    /// Runtime-agnostic filesystem persistence: pause deletes the Pod but
    /// retains the sandbox-owned PVC(s); resume cold-boots a fresh Pod that
    /// re-mounts the same PVC(s). No live-memory capture — state survives on
    /// disk, not in RAM. The only pause mechanism available on Kata (whose
    /// VMM-level snapshot the shim cannot drive) and an opt-in alternative on
    /// gVisor. Behavior lands across #603 Phases 2-4; this field is the
    /// selector only.
    Filesystem,
}

/// True if `runtime_class_name` selects a Kata Containers runtime (a
/// hardware-virtualized guest) — epic #603 Phase 4 / issue #607.
///
/// Deployments name the Kata `RuntimeClass` variously: `kata` and `kata-fc`
/// (the node adapter's selectors) and `kata-vm-isolation` (the AKS pool's
/// name). Rather than bake an environment-specific exact name into the binary
/// — which would need re-editing per cluster — membership keys off the shared
/// `kata` handler family: the exact name `kata`, or any `kata-` prefixed class.
///
/// Normalized case- and whitespace-insensitively: a `RuntimeClass` name is a
/// DNS label so production values are already lowercase, but normalizing keeps
/// the guard robust against a hand-applied CR with stray casing or padding.
pub fn is_kata_runtime_class(runtime_class_name: &str) -> bool {
    let normalized = runtime_class_name.trim().to_ascii_lowercase();
    normalized == "kata" || normalized.starts_with("kata-")
}

/// A [`PersistenceMode`] that is incompatible with the sandbox's resolved
/// runtime class (epic #603 Phase 4 / issue #607).
///
/// Returned by [`PersistenceMode::validate_for_runtime_class`] so the
/// pod-materializing edges (the E2B gateway at create, the controller at
/// pod-input resolution) can reject the pairing up front with an actionable
/// message rather than letting it fail-closed deep in the node's pause path.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnsupportedPersistenceMode {
    /// `snapshot` (live-memory checkpoint) requested on a Kata runtime class.
    /// Kata's VMM-level snapshot cannot be driven by the `containerd-shim-kata`
    /// — the out-of-band Cloud-Hypervisor path destroys the VM (epic #603
    /// findings) — so the only pause mechanism on Kata is `filesystem`.
    #[error(
        "persistenceMode 'snapshot' is unsupported on Kata runtime class {runtime_class:?}: \
         Kata's live-memory checkpoint cannot be driven by the containerd shim. Set \
         persistenceMode 'filesystem' on the SandboxTemplate or Sandbox so this sandbox \
         is pausable via the runtime-agnostic Pod-lifecycle path."
    )]
    SnapshotOnKata {
        /// The offending Kata runtime class name, echoed back for the operator.
        runtime_class: String,
    },
}

impl PersistenceMode {
    /// Validate this *already-resolved* persistence mode against the sandbox's
    /// *already-resolved* runtime class (epic #603 Phase 4 / issue #607).
    ///
    /// `runtime_class_name` is `None` when the cluster default applies (gVisor
    /// in this deployment) — never Kata, so always permitted. The only rejected
    /// pairing is [`PersistenceMode::Snapshot`] on a Kata class (see
    /// [`is_kata_runtime_class`]); every other combination — including the
    /// default `snapshot` on gVisor and `filesystem` on any runtime — is valid.
    ///
    /// Callers must pass the values *after* template-default resolution, since
    /// the default mode is `snapshot`: a Kata template that omits
    /// `persistenceMode` resolves to `snapshot` and is correctly rejected,
    /// forcing the operator to opt into `filesystem` to make Kata pausable.
    ///
    /// # Errors
    ///
    /// Returns [`UnsupportedPersistenceMode::SnapshotOnKata`] if `self` is
    /// [`PersistenceMode::Snapshot`] and `runtime_class_name` is a Kata class.
    pub fn validate_for_runtime_class(
        self,
        runtime_class_name: Option<&str>,
    ) -> Result<(), UnsupportedPersistenceMode> {
        if self == PersistenceMode::Snapshot {
            if let Some(class) = runtime_class_name {
                if is_kata_runtime_class(class) {
                    return Err(UnsupportedPersistenceMode::SnapshotOnKata {
                        runtime_class: class.to_owned(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Resolve the effective [`PersistenceMode`] for a sandbox, inferring
/// `filesystem` from a declared `volumeClaimTemplates` set when the mode is left
/// unset (epic #603 / issue #616).
///
/// `volumeClaimTemplates` and `persistenceMode` are independent fields, and
/// `persistenceMode` defaults to [`PersistenceMode::Snapshot`]. But VCTs are
/// *only* meaningful in [`PersistenceMode::Filesystem`] — in `snapshot` mode a
/// declared PVC is provisioned yet never part of the runtime checkpoint, and on
/// Kata the resolved `snapshot` is rejected outright by the #607 guard. So a
/// template/Sandbox that declares storage but omits the mode almost certainly
/// means `filesystem`. This helper makes declaring `volumeClaimTemplates` itself
/// the signal for filesystem persistence, removing the footgun of having to set
/// a second, separate field.
///
/// Resolution order — the *first* explicit value wins, inference fills only the
/// otherwise-`snapshot` default:
///
/// 1. an explicit `sandbox_mode` (the Sandbox CR's `persistenceMode`), else
/// 2. an explicit `template_mode` (the SandboxTemplate's `persistenceMode`), else
/// 3. `filesystem` if `has_volume_claims`, else the `snapshot` default.
///
/// Centralizing the rule here keeps the two callers — the controller's
/// `resolve_pod_inputs` and the gateway's `admit_persistence_mode` — from
/// drifting: if they applied the inference independently they could disagree
/// (gateway admits, controller rejects, or vice-versa).
///
/// `has_volume_claims` is the *effective* VCT set's non-emptiness (after the
/// Sandbox-over-template merge), not the raw template list: an explicit value
/// short-circuits before it is consulted, so it only matters in the inference
/// arm where the two callers must compute it the same way.
pub fn resolve_effective_persistence_mode(
    sandbox_mode: Option<PersistenceMode>,
    template_mode: Option<PersistenceMode>,
    has_volume_claims: bool,
) -> PersistenceMode {
    sandbox_mode
        .or(template_mode)
        .unwrap_or(if has_volume_claims {
            PersistenceMode::Filesystem
        } else {
            PersistenceMode::Snapshot
        })
}

/// Whether a sandbox is a **snapshot-mode clone** whose inherited guest identity
/// must be reconciled on first boot (issue #643).
///
/// A gVisor `snapshot`-mode restore reconstructs the entire Sentry (guest kernel)
/// from the *source's* checkpoint, so the clone inherits the source's in-kernel
/// identity — UTS hostname (`uname -n`) and any `/etc/machine-id` / boot-id
/// captured in the overlay. Meanwhile the kubelet re-materializes `/etc/hostname`
/// & friends from the *destination* Pod (see `RUNTIME_MANAGED_BIND_DESTS` in the
/// node's gVisor adapter), so a clone runs with a hostname split and a machine-id
/// duplicated across the source and every clone of it. The reconciliation
/// strategy and its rationale live in the design spec
/// `docs/superpowers/specs/2026-06-24-clone-guest-identity-reconcile-design.md`.
///
/// This is the pure predicate the controller branches on to decide whether to
/// stamp [`crate::labels::CLONE_IDENTITY_RESET_ANNOTATION`] on the restore Pod.
/// It is `true` iff **both** hold:
///
/// 1. `restore_from` is set — the sandbox is a clone, not a cold boot; and
/// 2. `effective_mode` is [`PersistenceMode::Snapshot`] — the inherited-identity
///    problem is specific to the in-kernel checkpoint restore. A *filesystem*-mode
///    clone cold-boots a fresh Pod over CSI-cloned volumes, so its kernel identity
///    is freshly generated and only on-disk state (which a workload owns) carries
///    over; nothing to reconcile at the runtime layer.
///
/// Callers pass the *resolved* mode (after the Sandbox-over-template merge and
/// `volumeClaimTemplates` inference — see [`resolve_effective_persistence_mode`]),
/// matching the values the pod-materializing edge already computes.
///
/// # Examples
///
/// ```
/// use sandbawks_types::sandbox::{clone_needs_guest_identity_reset, PersistenceMode};
///
/// // A snapshot-mode clone needs reconciliation.
/// assert!(clone_needs_guest_identity_reset(
///     Some("sbx-warm-l5dkn"),
///     PersistenceMode::Snapshot,
/// ));
/// // A filesystem-mode clone cold-boots fresh identity — no reset.
/// assert!(!clone_needs_guest_identity_reset(
///     Some("sbx-warm-l5dkn"),
///     PersistenceMode::Filesystem,
/// ));
/// // A plain cold boot (no restoreFrom) is never a clone.
/// assert!(!clone_needs_guest_identity_reset(None, PersistenceMode::Snapshot));
/// ```
pub fn clone_needs_guest_identity_reset(
    restore_from: Option<&str>,
    effective_mode: PersistenceMode,
) -> bool {
    restore_from.is_some() && effective_mode == PersistenceMode::Snapshot
}

/// A request for a sandbox-owned `PersistentVolumeClaim`, modeled on a
/// StatefulSet's / `kubernetes-sigs/agent-sandbox`'s `volumeClaimTemplates`
/// (epic #603). Each entry yields one PVC the controller provisions and mounts
/// into the sandbox Pod (#603 Phase 2); the PVC outlives the Pod across a
/// `filesystem`-mode pause so the sandbox's files survive resume.
///
/// [`Self::spec`] is a serde/schemars-friendly mirror of the subset of
/// `k8s_openapi::api::core::v1::PersistentVolumeClaimSpec` a dynamically
/// provisioned workspace volume needs. We mirror rather than embed the upstream
/// type for the same reason as [`Toleration`] / [`NetworkPolicy`]: this crate's
/// CRDs derive [`schemars::JsonSchema`] to emit their OpenAPI v3 schema, and the
/// `k8s-openapi` types do not implement `JsonSchema` under the features this
/// workspace enables. The field semantics match the upstream type, so the
/// controller converts into a `k8s_openapi` PVC spec when provisioning
/// (#603 Phase 2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeClaimTemplate {
    /// PVC object metadata. The `name` distinguishes this claim within the
    /// sandbox and seeds the materialized PVC's name (#603 Phase 2).
    pub metadata: VolumeClaimTemplateMetadata,

    /// The volume's provisioning spec (size, access modes, storage class).
    pub spec: VolumeClaimSpec,
}

/// Object metadata for a [`VolumeClaimTemplate`] — the `name`/`labels`/
/// `annotations` subset of `ObjectMeta` carried onto the materialized PVC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct VolumeClaimTemplateMetadata {
    /// Volume name, unique within the sandbox. Combined with the sandbox name
    /// to form the materialized PVC's name and used as the Pod volume name when
    /// mounted (#603 Phase 2).
    ///
    /// Constrained to a **DNS-1123 label** (lowercase alphanumerics plus `-`,
    /// starting/ending alphanumeric, ≤63 chars) by the `pattern`/`maxLength`
    /// emitted into the CRD schema (#605 review). This is the *stricter* of the
    /// two downstream Kubernetes name rules the value must satisfy — it is used
    /// verbatim as the Pod **volume name** (a DNS-1123 label, no dots) and, with
    /// the `sbx-<id>` suffix, as the **PVC name** (a DNS-1123 subdomain) — so
    /// validating it as a label keeps both valid. Rejecting an invalid name at
    /// `kubectl apply` (admission) makes the failure diagnosable instead of
    /// surfacing later as an opaque apiserver rejection when the controller
    /// creates the PVC/Pod; the controller re-checks it (and cross-entry
    /// uniqueness) as defense in depth.
    #[schemars(regex(pattern = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"), length(max = 63))]
    pub name: String,

    /// Extra labels stamped onto the materialized PVC.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,

    /// Extra annotations stamped onto the materialized PVC.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// Provisioning spec for a [`VolumeClaimTemplate`] — the dynamically
/// provisioned subset of `PersistentVolumeClaimSpec`.
///
/// No `Default`: [`Self::storage`] is required (a PVC must request a size) and
/// [`ResourceQuantity`] has no meaningful empty default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeClaimSpec {
    /// Requested storage size, mapped to `resources.requests.storage` on the
    /// materialized PVC (e.g. `10Gi`). Reuses [`ResourceQuantity`] so the
    /// lexical Kubernetes quantity form round-trips without precision loss.
    pub storage: ResourceQuantity,

    /// Access modes (e.g. `ReadWriteOnce`). Empty defers to the
    /// StorageClass/provisioner default.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,

    /// StorageClass to provision from. `None` uses the cluster's default class.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,

    /// Volume mode (`Filesystem` or `Block`). `None` defers to the Kubernetes
    /// default (`Filesystem`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mode: Option<String>,
}

/// Platform-owner control over whether a tenant-authored `Sandbox` may
/// contribute its own [`VolumeClaimTemplate`]s on top of the ones its
/// `SandboxTemplate` defines (epic #603 Phase 5 / issue #608).
///
/// Mirrors `kubernetes-sigs/agent-sandbox`'s `volumeClaimTemplatesPolicy`. Set
/// on the [`crate::SandboxTemplateSpec`] (the platform owner's surface) and
/// enforced at the controller's pod-creation edge by
/// [`merge_volume_claim_templates`], the single place a Sandbox's explicit
/// claims meet the template baseline. Claims the policy forbids are
/// *suppressed* — dropped from the effective set and surfaced as a status
/// condition, never a hard failure — so a tenant over-reaching its grant
/// degrades to the template baseline instead of wedging the Sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
pub enum VolumeClaimTemplatesPolicy {
    /// Tenant claims are ignored entirely: the effective set is exactly the
    /// template's. The safe default — a tenant cannot provision storage the
    /// platform owner did not sanction, which matters once a namespace is
    /// shared across tenants (ties into multi-namespace tenancy, #442).
    #[default]
    Disallowed,

    /// Tenant may *add* claims under names the template does not use, but may
    /// not redefine a template-owned claim. A tenant claim whose name collides
    /// with a template claim is suppressed (the template's definition wins).
    Allowed,

    /// Tenant may both add new-named claims and override template-owned claims
    /// by name. Nothing is suppressed — the most permissive grant.
    Overrides,
}

/// Why a tenant-supplied [`VolumeClaimTemplate`] was dropped from the effective
/// set by [`merge_volume_claim_templates`] (issue #608).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VctSuppressionReason {
    /// The template's policy is [`VolumeClaimTemplatesPolicy::Disallowed`]: no
    /// tenant claims are honored at all.
    PolicyDisallowed,

    /// Policy is [`VolumeClaimTemplatesPolicy::Allowed`] and the tenant claim's
    /// name collides with a template-owned claim it is not permitted to
    /// override.
    OverrideForbidden,
}

impl VctSuppressionReason {
    /// Stable machine-readable label for the status-condition message and for
    /// log/metric attribution. Kept distinct from the `Display`/`Debug` form so
    /// downstream string-matching never breaks if the doc comments change.
    pub const fn label(self) -> &'static str {
        match self {
            Self::PolicyDisallowed => "PolicyDisallowed",
            Self::OverrideForbidden => "OverrideForbidden",
        }
    }
}

/// One tenant claim that [`merge_volume_claim_templates`] suppressed, paired
/// with the reason — the raw material for the `VolumeClaimTemplatesPolicy`
/// status condition the controller stamps (issue #608).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VctSuppression {
    /// The suppressed claim's [`VolumeClaimTemplateMetadata::name`].
    pub name: String,
    /// Why it was dropped.
    pub reason: VctSuppressionReason,
}

/// Outcome of merging a Sandbox's explicit claims onto its template baseline
/// under a [`VolumeClaimTemplatesPolicy`] (issue #608).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VctMergeOutcome {
    /// The claims the controller will actually provision, in a deterministic
    /// order: template claims first (in template order, keeping their
    /// position), then any tenant additions the policy admitted (in tenant
    /// order). Under [`VolumeClaimTemplatesPolicy::Overrides`] a tenant claim
    /// that overrides a template-owned claim keeps the template claim's
    /// *position* but carries the tenant's spec.
    pub effective: Vec<VolumeClaimTemplate>,

    /// Tenant claims the policy dropped, for the status condition. Empty on the
    /// permissive paths (`Overrides`, or any policy when the tenant supplied
    /// nothing).
    pub suppressed: Vec<VctSuppression>,
}

/// Merge a Sandbox's explicit [`VolumeClaimTemplate`]s onto the template
/// baseline under the given [`VolumeClaimTemplatesPolicy`] (issue #608).
///
/// This is the policy choke point — the controller calls it at the single edge
/// (`resolve_pod_inputs`) where a Sandbox's claims meet its template's, so
/// cold-start, warm-pool, and hand-applied-CR paths all enforce the same rule.
/// It never errors: a forbidden tenant claim is recorded in
/// [`VctMergeOutcome::suppressed`] (for a status condition) and the effective
/// set degrades to what the policy permits, so an over-reaching tenant gets the
/// template baseline rather than a wedged Sandbox.
///
/// The result is deterministic regardless of map iteration order: template
/// claims hold their input positions and admitted tenant additions append in
/// tenant order. Duplicate names *within* the tenant list collapse the same way
/// a tenant-vs-template collision does (second occurrence suppressed under
/// `Allowed`, last-wins under `Overrides`), which keeps the materialized PVC
/// set free of the duplicate volume names Kubernetes would otherwise reject.
///
/// # Examples
///
/// ```
/// use sandbawks_types::{
///     merge_volume_claim_templates, VolumeClaimSpec, VolumeClaimTemplate,
///     VolumeClaimTemplateMetadata, VolumeClaimTemplatesPolicy,
/// };
///
/// fn vct(name: &str) -> VolumeClaimTemplate {
///     VolumeClaimTemplate {
///         metadata: VolumeClaimTemplateMetadata { name: name.into(), ..Default::default() },
///         spec: VolumeClaimSpec {
///             storage: "1Gi".parse().unwrap(),
///             access_modes: vec![],
///             storage_class_name: None,
///             volume_mode: None,
///         },
///     }
/// }
///
/// // Disallowed (the default): the tenant's `cache` claim is dropped.
/// let out = merge_volume_claim_templates(
///     &[vct("workspace")],
///     &[vct("cache")],
///     VolumeClaimTemplatesPolicy::Disallowed,
/// );
/// assert_eq!(out.effective.len(), 1);
/// assert_eq!(out.suppressed.len(), 1);
/// ```
pub fn merge_volume_claim_templates(
    template_vcts: &[VolumeClaimTemplate],
    tenant_vcts: &[VolumeClaimTemplate],
    policy: VolumeClaimTemplatesPolicy,
) -> VctMergeOutcome {
    // Template claims are the baseline and keep their order/position. Clone them
    // once into the effective set; index by name so a tenant claim can either
    // collide (Allowed), replace in place (Overrides), or extend (both) in O(1).
    let mut effective: Vec<VolumeClaimTemplate> = template_vcts.to_vec();
    let mut position: HashMap<&str, usize> = template_vcts
        .iter()
        .enumerate()
        .map(|(i, v)| (v.metadata.name.as_str(), i))
        .collect();
    let mut suppressed = Vec::new();

    for tenant in tenant_vcts {
        let name = tenant.metadata.name.as_str();
        match policy {
            // No tenant claim is honored — record each as suppressed so the
            // condition message can name what was dropped.
            VolumeClaimTemplatesPolicy::Disallowed => suppressed.push(VctSuppression {
                name: name.to_owned(),
                reason: VctSuppressionReason::PolicyDisallowed,
            }),
            // Additions allowed, redefinitions are not. `contains_key` (rather
            // than `get`) keeps the immutable borrow off the mutating arm.
            VolumeClaimTemplatesPolicy::Allowed => {
                if position.contains_key(name) {
                    suppressed.push(VctSuppression {
                        name: name.to_owned(),
                        reason: VctSuppressionReason::OverrideForbidden,
                    });
                } else {
                    position.insert(name, effective.len());
                    effective.push(tenant.clone());
                }
            }
            // Override-by-name in place (preserving position) or append a new
            // claim. `pos` is copied out, so the borrow ends before the push.
            VolumeClaimTemplatesPolicy::Overrides => {
                if let Some(&pos) = position.get(name) {
                    effective[pos] = tenant.clone();
                } else {
                    position.insert(name, effective.len());
                    effective.push(tenant.clone());
                }
            }
        }
    }

    VctMergeOutcome {
        effective,
        suppressed,
    }
}

/// Lifecycle phase. See spec §5 for transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
pub enum SandboxPhase {
    #[default]
    Creating,
    Running,
    Pausing,
    Paused,
    Resuming,
    Terminating,
    Failed,
}

/// Observed state of a Sandbox, written by the controller.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SandboxStatus {
    #[serde(default)]
    pub phase: SandboxPhase,

    /// Node currently hosting the sandbox Pod (None when Paused or
    /// Creating before scheduling).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub assigned_node: Option<String>,

    /// Reference to the backing Pod, if any.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pod_ref: Option<String>,

    /// UID stamped onto the Pod by the kube API server (mirrored from
    /// `Pod.metadata.uid` once the controller observes the Pod). Lets
    /// the gateway disambiguate Pods if the CR name is reused.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pod_uid: Option<String>,

    /// `host:port` of the in-sandbox envd agent when running.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub envd_address: Option<String>,

    /// S3 URL of the snapshot when Paused.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_ref: Option<String>,

    /// Aggregate sha256 of the snapshot referenced by `snapshot_ref`.
    /// Folded in from the completing Pause `SnapshotJob`'s
    /// `status.manifest.sha256` so the controller has a stable
    /// fingerprint to commit to when dispatching the next Resume job
    /// (issue #171 hardening — binds the snapshot URL to a value the
    /// node DaemonSet can cross-check against the re-fetched manifest).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_sha256: Option<String>,

    /// sha256 of the node-staged envd binary the snapshot referenced by
    /// `snapshot_ref` was checkpointed against. Folded in from the
    /// completing Pause `SnapshotJob`'s `status.manifest.envd_sha256` so
    /// the controller has a stable value to commit when dispatching the
    /// next Resume/Stage job (issue #278 hardening — binds the envd
    /// dimension to a value the node DaemonSet cross-checks against the
    /// re-fetched manifest before `verify_envd_digest`, mirroring
    /// `snapshot_sha256` / issue #171 for the aggregate digest).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_envd_sha256: Option<String>,

    /// Host `runsc` (gVisor) release token the snapshot referenced by
    /// `snapshot_ref` was checkpointed against (e.g. `release-20260525.0`).
    /// Folded in from the completing Pause `SnapshotJob`'s
    /// `status.manifest.runsc_version` (#545, Capability B).
    ///
    /// gVisor checkpoint/restore requires the *exact* same runsc build on
    /// both ends, so on the next Resume the controller constrains the
    /// restore Pod's `nodeAffinity` to nodes advertising this version via
    /// [`crate::RUNSC_VERSION_LABEL`]. A mismatched fleet then leaves the
    /// restore Pod `Pending` (diagnosable, recoverable) instead of
    /// crash-looping `runsc restore` with a fatal version mismatch.
    ///
    /// `None` for Sandboxes paused before this field existed and for a
    /// pause whose node could not read its host runsc version; the restore
    /// gate then degrades to envd-staged affinity only, matching the
    /// pre-#545 ungated placement.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_runsc_version: Option<String>,

    /// Name of the in-flight `SnapshotJob` CR driving the current
    /// pause or resume, when one exists. Cleared by the controller
    /// when the job reaches a terminal phase.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_job_ref: Option<String>,

    /// Local filesystem path on the staging node where a `Stage`
    /// `SnapshotJob` materialized the snapshot's parts, copied from the
    /// completing job's [`crate::SnapshotJobStatus::local_path`] (Plan 07
    /// T9). The controller threads this into the restore Pod's
    /// `io.sandbawks/restore-image-path` annotation so the
    /// `containerd-shim` performs a start-time `runsc restore` from it —
    /// the sandbox is born restored rather than restored after boot.
    /// Set on the `(Resuming, StageComplete)` edge and consumed by
    /// `Action::CreateRestorePod`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub restore_staging_path: Option<String>,

    /// Number of times the controller has (re)created the restore Pod for
    /// the *current* resume, used by the bounded `Resuming` recovery loop
    /// (epic #484). A fresh `Paused → Resuming` transition resets this to
    /// `0`; each `RestorePodLost` recovery increments it. Once it exceeds
    /// `RESUME_MAX_RESTARTS` the controller folds the Sandbox to `Failed`
    /// with [`crate::FailureReason`]`::ResumeExhausted` rather than retrying
    /// forever (the `resumingforever` hang this epic fixes).
    ///
    /// `u32`, not `Option<u32>`: the count is meaningful (and zero) the
    /// moment a Sandbox enters `Resuming`, so a sentinel "unset" state
    /// would be indistinguishable from "no restarts yet". `#[serde(default)]`
    /// makes a `SandboxStatus` deserialized from an older controller's CR
    /// (which never wrote the field) read back as `0`.
    #[serde(default)]
    pub resume_restart_count: u32,

    /// RFC 3339 timestamp of the most recent restore-Pod (re)creation
    /// attempt for the current resume — the anchor the recovery loop's
    /// capped exponential backoff measures its delay from (epic #484), and
    /// an observability breadcrumb for how long a `Resuming` Sandbox has
    /// been retrying. `None` until the first attempt is stamped.
    ///
    /// Stored as an RFC 3339 string to match the sibling
    /// [`Self::last_transition_time`] / [`Self::last_activity_timestamp`]
    /// convention (the controller parses it back via
    /// `DateTime::parse_from_rfc3339`) rather than a typed `Time`, so all
    /// status timestamps round-trip through one code path.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub resume_last_attempt: Option<String>,

    /// Number of times the controller has re-dispatched the pause checkpoint
    /// for the *current* `Pausing` cycle, used by the bounded pause-checkpoint
    /// retry loop (issue #510, tier 1). A fresh `Running → Pausing` transition
    /// resets this to `0`; each `PauseRetry` recovery increments it. Once it
    /// reaches `PAUSE_MAX_RETRIES` the controller folds the Sandbox to `Failed`
    /// with [`crate::FailureReason`]`::PauseCheckpointFailed` instead of
    /// retrying forever — but, unlike a genuine create/terminate failure, that
    /// fold stays *recoverable* (see [`Self::failure_reason`]).
    ///
    /// `u32`, not `Option<u32>`, for the same reason as
    /// [`Self::resume_restart_count`]: the count is meaningful (and zero) the
    /// moment a Sandbox enters `Pausing`. `#[serde(default)]` makes a
    /// `SandboxStatus` deserialized from an older controller's CR read back as
    /// `0`.
    #[serde(default)]
    pub pause_restart_count: u32,

    /// RFC 3339 timestamp of the most recent pause-checkpoint (re)dispatch for
    /// the current `Pausing` cycle — the anchor the tier-1 retry loop's capped
    /// exponential backoff measures from (issue #510). `None` until the first
    /// attempt is stamped. Stored as an RFC 3339 string to match the sibling
    /// [`Self::resume_last_attempt`] convention.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pause_last_attempt: Option<String>,

    /// Stable [`crate::FailureReason`] label of the fold that last drove this
    /// Sandbox into `phase=Failed` (issue #510). Distinguishes the
    /// *recoverable* pause-checkpoint failure class
    /// (`pause_checkpoint_failed`) — where the workload Pod may still be alive
    /// and the checkpoint simply did not happen — from genuinely terminal
    /// create/terminate failures, so the controller's `Failed`-phase recovery
    /// probe can scope its retry/recover/GC behavior to the former and leave
    /// the latter terminal. `None` for a Sandbox that has never folded to
    /// `Failed` (and for CRs written by a controller predating this field).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub failure_reason: Option<String>,

    /// RFC 3339 timestamp the Sandbox most recently entered `phase=Failed`
    /// (issue #510) — the anchor the tier-3 grace-period GC measures from when
    /// deciding whether an orphaned-but-unreachable Pod has been stuck long
    /// enough to reclaim. Stamped alongside [`Self::failure_reason`] at every
    /// `Failed` fold; cleared when a recoverable Sandbox is recovered back to
    /// `Running`. Stored as an RFC 3339 string to match the sibling timestamp
    /// fields. `None` until the first fold.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub failed_at: Option<String>,

    /// RFC 3339 timestamp of the last phase transition.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_transition_time: Option<String>,

    /// RFC 3339 timestamp of the last observed client activity against the
    /// sandbox (issue #481). The API gateway stamps this (throttled) on
    /// every proxied envd request — filesystem, process, exec/pty streams —
    /// so the controller can treat `spec.timeoutSeconds` as an *idle*
    /// window rather than an absolute deadline: a `Running` sandbox is
    /// auto-paused / auto-terminated only once `now - lastActivityTimestamp`
    /// exceeds the window, not once `creationTimestamp + timeoutSeconds`
    /// elapses regardless of use.
    ///
    /// `None` means "no activity stamped yet"; the controller then falls
    /// back to `creationTimestamp` as the idle baseline so a
    /// freshly-claimed-but-untouched sandbox still ages out (no regression
    /// for abandoned claims). Stored as an RFC 3339 string to match the
    /// sibling [`Self::last_transition_time`] convention (the controller
    /// parses it back via `DateTime::parse_from_rfc3339`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_activity_timestamp: Option<String>,

    /// Projection of the backing Pod's observed phase while the Sandbox is
    /// not yet serving envd, written by the controller for the API gateway's
    /// "still starting / not ready yet" notice (issue #625).
    ///
    /// The gateway watches `Sandbox` CRs but deliberately holds no pod-read
    /// RBAC (#442), so it cannot inspect the Pod itself. The controller —
    /// which already observes the Pod every reconcile — distills the
    /// pod-level signal the gateway needs to turn a tool call racing a
    /// not-yet-ready sandbox into an actionable notice (and to tell a
    /// transient "still starting, retry shortly" apart from a stuck
    /// `ImagePullBackOff` / `CrashLoopBackOff` that will not recover without
    /// intervention).
    ///
    /// `None` once envd is serving (the gateway routes normally and never
    /// consults it) and for CRs written by a controller predating this field.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pod_status: Option<PodStatusSummary>,

    /// Free-form conditions in the conventional Kubernetes style.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<SandboxCondition>,
}

/// Distilled projection of the backing Pod's observed phase, written by the
/// controller into [`SandboxStatus::pod_status`] (issue #625).
///
/// Built from `Pod.status.phase` plus the dominant blocking container
/// `reason`, this is the minimal pod-level signal the API gateway needs to
/// surface an actionable notice on per-sandbox tool calls that race a Pod
/// which is not yet serving envd — without the gateway needing pod-read RBAC
/// of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodStatusSummary {
    /// `Pod.status.phase` verbatim — `Pending` / `Running` / `Succeeded` /
    /// `Failed` / `Unknown`. `Unknown` when the kubelet has not reported a
    /// phase yet (the Pod exists but has no `status.phase`).
    pub phase: String,

    /// The most significant container *waiting* or *terminated* reason that
    /// is blocking the Pod from serving, when one exists — e.g.
    /// `ImagePullBackOff`, `ErrImagePull`, `CrashLoopBackOff`,
    /// `CreateContainerError`, `RunContainerError`. `None` while the Pod is
    /// merely scheduling / pulling with no error reported yet (a healthy,
    /// still-progressing cold start or restore).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,

    /// Human-readable detail accompanying [`Self::reason`] — the kubelet's
    /// container `state.waiting.message` / `state.terminated.message` — when
    /// present. Carried verbatim into the gateway notice so the LLM driving a
    /// harness sees the concrete cause (e.g. the unpullable image ref).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message: Option<String>,
}

/// Conventional Kubernetes-style status condition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SandboxCondition {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_transition_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        is_kata_runtime_class, merge_volume_claim_templates, PersistenceMode,
        UnsupportedPersistenceMode, VctSuppressionReason, VolumeClaimSpec, VolumeClaimTemplate,
        VolumeClaimTemplateMetadata, VolumeClaimTemplatesPolicy,
    };

    /// Build a minimal claim with `name` and a `storage` size, so tests can
    /// assert the merge picked the *template's* vs the *tenant's* spec by size.
    fn vct(name: &str, storage: &str) -> VolumeClaimTemplate {
        VolumeClaimTemplate {
            metadata: VolumeClaimTemplateMetadata {
                name: name.to_owned(),
                ..Default::default()
            },
            spec: VolumeClaimSpec {
                storage: storage.parse().expect("valid quantity"),
                access_modes: Vec::new(),
                storage_class_name: None,
                volume_mode: None,
            },
        }
    }

    fn names(vcts: &[VolumeClaimTemplate]) -> Vec<&str> {
        vcts.iter().map(|v| v.metadata.name.as_str()).collect()
    }

    #[test]
    fn pod_security_context_serializes_camel_case_and_omits_empty() {
        use super::PodSecurityContext;

        // A fsGroup-only context: camelCase key, and every unset Option /
        // empty Vec is omitted from the wire form.
        let sc = PodSecurityContext {
            fs_group: Some(1000),
            ..Default::default()
        };
        let json = serde_json::to_value(&sc).expect("serialize");
        assert_eq!(json, serde_json::json!({ "fsGroup": 1000 }));

        // Full round-trip preserves every field with camelCase keys.
        let full = PodSecurityContext {
            fs_group: Some(1000),
            run_as_user: Some(1001),
            run_as_group: Some(1002),
            run_as_non_root: Some(true),
            fs_group_change_policy: Some("OnRootMismatch".into()),
            supplemental_groups: vec![2000, 3000],
        };
        let round: PodSecurityContext =
            serde_json::from_value(serde_json::to_value(&full).expect("serialize"))
                .expect("deserialize");
        assert_eq!(round, full);
    }

    #[test]
    fn policy_default_is_disallowed() {
        assert_eq!(
            VolumeClaimTemplatesPolicy::default(),
            VolumeClaimTemplatesPolicy::Disallowed
        );
    }

    #[test]
    fn disallowed_keeps_only_template_claims_and_suppresses_tenant() {
        let template = [vct("workspace", "10Gi")];
        let tenant = [vct("cache", "1Gi"), vct("scratch", "2Gi")];
        let out = merge_volume_claim_templates(
            &template,
            &tenant,
            VolumeClaimTemplatesPolicy::Disallowed,
        );
        // Effective set is exactly the template's — no tenant claim survives.
        assert_eq!(names(&out.effective), ["workspace"]);
        // Both tenant claims recorded as suppressed for the status condition.
        assert_eq!(out.suppressed.len(), 2);
        assert!(out
            .suppressed
            .iter()
            .all(|s| s.reason == VctSuppressionReason::PolicyDisallowed));
        assert_eq!(
            names(&out.effective).len() + out.suppressed.len(),
            template.len() + tenant.len()
        );
    }

    #[test]
    fn allowed_admits_new_names_but_forbids_overrides() {
        let template = [vct("workspace", "10Gi")];
        // `cache` is a new name (admitted); `workspace` collides (suppressed).
        let tenant = [vct("cache", "1Gi"), vct("workspace", "99Gi")];
        let out =
            merge_volume_claim_templates(&template, &tenant, VolumeClaimTemplatesPolicy::Allowed);
        // Template first (unchanged spec), then the admitted tenant addition.
        assert_eq!(names(&out.effective), ["workspace", "cache"]);
        assert_eq!(
            out.effective[0].spec.storage.as_str(),
            "10Gi",
            "template `workspace` must not be overridden under Allowed"
        );
        assert_eq!(out.suppressed.len(), 1);
        assert_eq!(out.suppressed[0].name, "workspace");
        assert_eq!(
            out.suppressed[0].reason,
            VctSuppressionReason::OverrideForbidden
        );
    }

    #[test]
    fn overrides_replaces_in_place_and_appends() {
        let template = [vct("workspace", "10Gi"), vct("data", "5Gi")];
        // Override `workspace` by name, add a new `cache`.
        let tenant = [vct("workspace", "99Gi"), vct("cache", "1Gi")];
        let out =
            merge_volume_claim_templates(&template, &tenant, VolumeClaimTemplatesPolicy::Overrides);
        // `workspace` keeps its leading position but carries the tenant's spec;
        // `data` untouched; `cache` appended. Nothing suppressed.
        assert_eq!(names(&out.effective), ["workspace", "data", "cache"]);
        assert_eq!(out.effective[0].spec.storage.as_str(), "99Gi");
        assert_eq!(out.effective[1].spec.storage.as_str(), "5Gi");
        assert!(out.suppressed.is_empty());
    }

    #[test]
    fn empty_tenant_list_never_suppresses_under_any_policy() {
        let template = [vct("workspace", "10Gi")];
        for policy in [
            VolumeClaimTemplatesPolicy::Disallowed,
            VolumeClaimTemplatesPolicy::Allowed,
            VolumeClaimTemplatesPolicy::Overrides,
        ] {
            let out = merge_volume_claim_templates(&template, &[], policy);
            assert_eq!(names(&out.effective), ["workspace"]);
            assert!(out.suppressed.is_empty(), "{policy:?} suppressed nothing");
        }
    }

    #[test]
    fn duplicate_tenant_names_collapse_to_one_volume() {
        // Two tenant claims share a new name. Kubernetes would reject duplicate
        // volume names, so the merge must not emit both.
        let tenant = [vct("cache", "1Gi"), vct("cache", "2Gi")];
        let allowed =
            merge_volume_claim_templates(&[], &tenant, VolumeClaimTemplatesPolicy::Allowed);
        // First admitted, second suppressed as a redefinition.
        assert_eq!(names(&allowed.effective), ["cache"]);
        assert_eq!(allowed.effective[0].spec.storage.as_str(), "1Gi");
        assert_eq!(allowed.suppressed.len(), 1);

        let overrides =
            merge_volume_claim_templates(&[], &tenant, VolumeClaimTemplatesPolicy::Overrides);
        // Last-wins, single volume, nothing suppressed.
        assert_eq!(names(&overrides.effective), ["cache"]);
        assert_eq!(overrides.effective[0].spec.storage.as_str(), "2Gi");
        assert!(overrides.suppressed.is_empty());
    }

    #[test]
    fn kata_runtime_class_matches_handler_family() {
        // The three names seen across the node adapter and the AKS pool.
        for class in ["kata", "kata-fc", "kata-vm-isolation"] {
            assert!(is_kata_runtime_class(class), "{class} should be Kata");
        }
        // Case- and whitespace-insensitive for hand-applied CRs.
        assert!(is_kata_runtime_class("  Kata-FC  "));
    }

    #[test]
    fn non_kata_runtime_classes_are_not_kata() {
        // gVisor, the cluster default (empty), and a look-alike that is not a
        // `kata-` member must all read as non-Kata so snapshot stays allowed.
        for class in ["gvisor", "runc", "", "katalog", "my-kata"] {
            assert!(!is_kata_runtime_class(class), "{class} must not be Kata");
        }
    }

    #[test]
    fn snapshot_on_kata_is_rejected() {
        let err = PersistenceMode::Snapshot
            .validate_for_runtime_class(Some("kata-vm-isolation"))
            .expect_err("snapshot on Kata must be rejected");
        assert_eq!(
            err,
            UnsupportedPersistenceMode::SnapshotOnKata {
                runtime_class: "kata-vm-isolation".to_owned(),
            }
        );
    }

    #[test]
    fn filesystem_on_kata_is_allowed() {
        assert!(PersistenceMode::Filesystem
            .validate_for_runtime_class(Some("kata-vm-isolation"))
            .is_ok());
    }

    #[test]
    fn snapshot_on_gvisor_and_default_runtime_is_allowed() {
        // The default mode (`snapshot`) on gVisor, and on the cluster-default
        // runtime (`None`), are the unchanged pre-#603 paths.
        assert!(PersistenceMode::Snapshot
            .validate_for_runtime_class(Some("gvisor"))
            .is_ok());
        assert!(PersistenceMode::Snapshot
            .validate_for_runtime_class(None)
            .is_ok());
    }

    #[test]
    fn filesystem_on_any_runtime_is_allowed() {
        // filesystem is runtime-agnostic — valid on gVisor and the default too.
        assert!(PersistenceMode::Filesystem
            .validate_for_runtime_class(Some("gvisor"))
            .is_ok());
        assert!(PersistenceMode::Filesystem
            .validate_for_runtime_class(None)
            .is_ok());
    }

    /// Issue #616: an explicit `persistenceMode` always wins over the inference
    /// — on the Sandbox CR or the template — including an explicit `snapshot`
    /// alongside declared `volumeClaimTemplates` (a deliberate, if unusual,
    /// choice). The inference fills only the otherwise-`snapshot` default slot.
    #[test]
    fn explicit_persistence_mode_wins_over_vct_inference() {
        use super::resolve_effective_persistence_mode;

        // Explicit Sandbox mode wins even with VCTs present, both directions.
        assert_eq!(
            resolve_effective_persistence_mode(Some(PersistenceMode::Snapshot), None, true),
            PersistenceMode::Snapshot,
        );
        assert_eq!(
            resolve_effective_persistence_mode(Some(PersistenceMode::Filesystem), None, false),
            PersistenceMode::Filesystem,
        );

        // Explicit template mode wins (Sandbox unset) even with VCTs present.
        assert_eq!(
            resolve_effective_persistence_mode(None, Some(PersistenceMode::Snapshot), true),
            PersistenceMode::Snapshot,
        );

        // Sandbox precedence over the template when both are explicit.
        assert_eq!(
            resolve_effective_persistence_mode(
                Some(PersistenceMode::Filesystem),
                Some(PersistenceMode::Snapshot),
                false,
            ),
            PersistenceMode::Filesystem,
        );
    }

    /// Issue #616: an *unset* mode with non-empty effective
    /// `volumeClaimTemplates` infers `filesystem`; without VCTs it stays
    /// `snapshot` (no behavior change for the default case).
    #[test]
    fn unset_persistence_mode_infers_filesystem_from_vcts() {
        use super::resolve_effective_persistence_mode;

        // Declaring storage opts into filesystem.
        assert_eq!(
            resolve_effective_persistence_mode(None, None, true),
            PersistenceMode::Filesystem,
        );
        // No storage declared ⇒ unchanged `snapshot` default.
        assert_eq!(
            resolve_effective_persistence_mode(None, None, false),
            PersistenceMode::Snapshot,
        );
    }

    /// Issue #624: the generated CRD must carry a `Claimed` printer column
    /// keyed on the `sandbawks.dev/claimed-at` annotation so `kubectl get
    /// sandbox` can distinguish an unclaimed warm-pool member (empty column)
    /// from a claimed sandbox (a claim timestamp).
    ///
    /// The dotted annotation key must reach the apiserver's CR table printer
    /// (`client-go util/jsonpath`) as `…annotations.sandbawks\.dev/claimed-at`
    /// — exactly one backslash before the dot. kube-rs JSON-parses the
    /// `printcolumn` literal, collapsing the `\\` in the Rust raw string to a
    /// single backslash; a double backslash would make the engine read the key
    /// as a nested path (`sandbawks` → `dev/claimed-at`) and the column would
    /// silently render empty for everyone.
    ///
    /// This asserts the column exists, is typed `date`, and carries the
    /// single-backslash path built from the shared const. It does **not** prove
    /// the apiserver projects the annotation into the column at runtime — that
    /// end-to-end behavior was checked manually against `kubectl` v1.31 (see
    /// the PR's manual test plan / review thread) since this crate has no
    /// cluster harness.
    #[test]
    fn sandbox_crd_has_claimed_printer_column() {
        use crate::warm_pool::CLAIMED_AT_ANNOTATION;
        use kube::CustomResourceExt;

        let crd = super::Sandbox::crd();
        let version = crd
            .spec
            .versions
            .first()
            .expect("at least one served version");
        let columns = version
            .additional_printer_columns
            .as_ref()
            .expect("printer columns declared");

        let claimed = columns
            .iter()
            .find(|c| c.name == "Claimed")
            .expect("a `Claimed` printer column is present");
        assert_eq!(claimed.type_, "date");

        // Single-backslash escape of the dotted key, derived from the const so
        // a rename of the annotation moves this assertion in lockstep.
        let (head, tail) = CLAIMED_AT_ANNOTATION
            .split_once('.')
            .expect("annotation key contains a dot");
        let expected = format!(".metadata.annotations.{head}\\.{tail}");
        assert_eq!(claimed.json_path, expected);
        // The escape must collapse to exactly one backslash: a double backslash
        // breaks resolution under the apiserver's JSONPath (column empty for
        // all rows).
        assert!(
            !claimed.json_path.contains("\\\\"),
            "claimed column JSONPath must not contain a double backslash: {}",
            claimed.json_path
        );
    }

    #[test]
    fn clone_identity_reset_needed_only_for_snapshot_clones() {
        use super::{clone_needs_guest_identity_reset, PersistenceMode};

        // A snapshot-mode clone is the one case that needs reconciliation: the
        // Sentry is rebuilt from the source's checkpoint, inheriting its
        // in-kernel hostname / machine-id.
        assert!(clone_needs_guest_identity_reset(
            Some("sbx-warm-l5dkn"),
            PersistenceMode::Snapshot,
        ));

        // A filesystem-mode clone cold-boots a fresh Pod over CSI-cloned
        // volumes, so its kernel identity is freshly generated — nothing to
        // reconcile at the runtime layer.
        assert!(!clone_needs_guest_identity_reset(
            Some("sbx-warm-l5dkn"),
            PersistenceMode::Filesystem,
        ));

        // A plain cold boot (no restoreFrom) is never a clone, regardless of
        // persistence mode.
        assert!(!clone_needs_guest_identity_reset(
            None,
            PersistenceMode::Snapshot,
        ));
        assert!(!clone_needs_guest_identity_reset(
            None,
            PersistenceMode::Filesystem,
        ));

        // An empty-but-present restoreFrom is still "set": the predicate keys on
        // the field's presence, leaving value validation (a sandbox ID vs an
        // s3:// URL) to RestoreSource::parse downstream. An empty string never
        // reaches this predicate in practice (serde skips a None), but the
        // boundary is documented here so a future caller cannot assume it
        // filters empties.
        assert!(clone_needs_guest_identity_reset(
            Some(""),
            PersistenceMode::Snapshot,
        ));
    }
}
