use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::resources::ResourceRequests;
use crate::sandbox::{
    NetworkPolicy, PersistenceMode, PodSecurityContext, Toleration, VolumeClaimTemplate,
    VolumeClaimTemplatesPolicy,
};

/// A reusable definition for a class of sandboxes.
///
/// Templates capture the OCI image + sensible defaults (resources,
/// runtime class, timeout, envd build). Per-Sandbox spec values
/// override the template defaults.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.dev",
    version = "v1alpha1",
    kind = "SandboxTemplate",
    plural = "sandboxtemplates",
    shortname = "sbxtpl",
    namespaced,
    printcolumn = r#"{"name":"Image","type":"string","jsonPath":".spec.image"}"#,
    printcolumn = r#"{"name":"Runtime","type":"string","jsonPath":".spec.defaultRuntimeClass"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateSpec {
    /// OCI image reference for the sandbox's rootfs (the customer's
    /// tooling). envd does **not** need to be baked in: the
    /// `sandbawks-node` DaemonSet stages the static envd binary onto the
    /// node and the controller mounts it read-only into this image,
    /// running it as the container entrypoint (issue #265).
    pub image: String,

    /// RuntimeClass name to use unless the Sandbox overrides it.
    pub default_runtime_class: String,

    /// Resource defaults applied when the Sandbox doesn't specify them.
    pub default_resources: ResourceRequests,

    /// envd-rs binary carrier image (separate from [`Self::image`] so
    /// envd can be updated independently of the customer's rootfs).
    ///
    /// NOTE (issue #265/#273): with node-staged envd delivery, the
    /// cold-start and restore paths use the *cluster-wide* envd binary the
    /// `sandbawks-node` DaemonSet stages (Helm `envd.image`); this
    /// per-template field is currently **not** consumed on those paths.
    /// It is therefore **optional** — omit it unless and until per-template
    /// envd staging is revived (issue #270). Setting it today is a no-op.
    ///
    /// `Option<T>` makes the field non-required in the generated CRD
    /// schema; `skip_serializing_if` keeps `None` out of serialized YAML
    /// rather than emitting an explicit `envdImage: null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envd_image: Option<String>,

    /// Default timeout in seconds.
    pub default_timeout_seconds: u32,

    /// Workload egress baseline for every sandbox minted from this template
    /// (issue #537). Propagated by the controller/gateway into each minted
    /// `Sandbox.spec.networkPolicy`, so both cold-start *and* warm-pool members
    /// inherit it without hand-editing the resulting CR — closing the gap where
    /// `egressAllow` existed on `Sandbox` but no surface wired it through.
    ///
    /// `None` (or an empty `egressAllow`) leaves minted sandboxes at the
    /// default-deny-egress baseline (issue #443). A per-create request may add
    /// further destinations on top of this baseline; the two are combined as a
    /// union by [`crate::merge_egress_policies`].
    ///
    /// `Option<T>` keeps the field non-required in the generated CRD schema and
    /// `skip_serializing_if` keeps `None` out of serialized YAML rather than
    /// emitting `networkPolicy: null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy: Option<NetworkPolicy>,

    /// Scheduling tolerations every sandbox minted from this template
    /// inherits (issue #592), letting its Pod schedule onto tainted nodes —
    /// e.g. a dedicated Kata/Azure node pool whose taint repels untolerated
    /// Pods. Propagated by the controller into each minted Pod's
    /// `spec.tolerations` at the pod-creation edge (`resolve_pod_inputs`), so
    /// both cold-start *and* warm-pool members pick it up without hand-editing
    /// the resulting CR — mirroring how `networkPolicy` / `defaultRuntimeClass`
    /// flow down. Composes with the controller's internal runsc-version node
    /// affinity rather than replacing it.
    ///
    /// `None` (the default) leaves minted Pods with no extra tolerations,
    /// scheduled exactly as today. `Option<T>` keeps the field non-required in
    /// the generated CRD schema and `skip_serializing_if` keeps `None` out of
    /// serialized YAML rather than emitting `tolerations: null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerations: Option<Vec<Toleration>>,

    /// Node-selector labels every sandbox minted from this template inherits
    /// (issue #592), pinning its Pod to a labeled node pool. Propagated into
    /// each minted Pod's `spec.nodeSelector` alongside [`Self::tolerations`];
    /// same propagation path and `None`-default semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<BTreeMap<String, String>>,

    /// Pod-level security context every sandbox minted from this template
    /// inherits (issue #617), stamped onto its Pod's `spec.securityContext` —
    /// most importantly `fsGroup`, so a non-root container can write to a
    /// dynamically provisioned `volumeClaimTemplates` PVC. Propagated into each
    /// minted Sandbox's [`crate::SandboxSpec::security_context`] at the
    /// controller's pod-creation edge ([`resolve_pod_inputs`]) alongside
    /// [`Self::tolerations`] / [`Self::node_selector`]; same propagation path and
    /// `None`-default semantics (`None` ⇒ no `securityContext`, unchanged).
    ///
    /// [`resolve_pod_inputs`]: ../../sandbawks_controller/reconcile/fn.resolve_pod_inputs.html
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_context: Option<PodSecurityContext>,

    /// Persistence strategy every sandbox minted from this template inherits
    /// (epic #603), unless the Sandbox overrides it. Propagated into each
    /// minted Sandbox's [`crate::SandboxSpec::persistence_mode`] at the
    /// controller's pod-creation edge (`resolve_pod_inputs`), so both
    /// cold-start *and* warm-pool members pick it up without hand-editing the
    /// CR — mirroring how `tolerations` / `nodeSelector` flow down.
    ///
    /// `None` (the default) leaves minted sandboxes at the effective default
    /// [`PersistenceMode::Snapshot`] — current behavior, unchanged. `Option<T>`
    /// keeps the field non-required in the generated CRD schema and
    /// `skip_serializing_if` keeps `None` out of serialized YAML rather than
    /// emitting `persistenceMode: null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence_mode: Option<PersistenceMode>,

    /// PVC requests every sandbox minted from this template inherits (epic
    /// #603), unless the Sandbox sets its own. Propagated into each minted
    /// Sandbox's [`crate::SandboxSpec::volume_claim_templates`] alongside
    /// [`Self::persistence_mode`]; same propagation path and `None`-default
    /// semantics. The PVC objects are provisioned and mounted in #603 Phase 2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_claim_templates: Option<Vec<VolumeClaimTemplate>>,

    /// Whether a Sandbox minted from this template may contribute its own
    /// [`VolumeClaimTemplate`]s on top of [`Self::volume_claim_templates`]
    /// (epic #603 Phase 5 / issue #608). Enforced at the controller's
    /// pod-creation edge by [`crate::merge_volume_claim_templates`], so every
    /// path that materializes a Pod — cold-start, warm-pool, and a hand-applied
    /// Sandbox CR — obeys the same rule. Tenant claims the policy forbids are
    /// suppressed (and surfaced as a status condition), never a hard failure.
    ///
    /// `None` resolves to the default
    /// [`VolumeClaimTemplatesPolicy::Disallowed`]: a Sandbox's explicit claims
    /// are ignored and only this template's claims are provisioned. The platform
    /// owner opts a template into tenant-supplied storage by setting `Allowed`
    /// (additions only) or `Overrides` (additions plus by-name override).
    ///
    /// `Option<T>` keeps the field non-required in the generated CRD schema and
    /// `skip_serializing_if` keeps `None` out of serialized YAML rather than
    /// emitting `volumeClaimTemplatesPolicy: null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_claim_templates_policy: Option<VolumeClaimTemplatesPolicy>,
}
