//! `PodAdapter` — materializes a `Sandbox` CR into a Pod running envd.
//!
//! First *real* `RuntimeAdapter` impl. Honors `Sandbox.spec.runtimeClassName`
//! by passing it through to `pod.spec.runtimeClassName`; when unset, the
//! cluster's default runtime handler (containerd/runc) is used. The
//! corresponding `RuntimeClass` resource (e.g. `gvisor`) must be installed
//! in the cluster — that ships with the Plan 05 Helm chart.
//!
//! Pause/resume are no-ops here — `RuntimeError::Pause("unimplemented
//! by PodAdapter")` would break the API surface, so we instead return
//! an empty `SnapshotManifest` for pause and `Ok(())` for resume,
//! matching `FakeRuntimeAdapter`'s semantics. The Sandbox's recorded
//! state is therefore *not* preserved across a pause/connect cycle in
//! this plan; that gets fixed by `GvisorAdapter`.

use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::api::core::v1::{
    Affinity, Container, ContainerPort, EnvVar, EnvVarSource, HostPathVolumeSource, NodeAffinity,
    NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, ObjectFieldSelector,
    PersistentVolumeClaim, PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, Pod,
    PodSecurityContext as K8sPodSecurityContext, PodSpec, Probe, Secret, SecretKeySelector,
    TCPSocketAction, Toleration as K8sToleration, TypedLocalObjectReference, Volume, VolumeMount,
    VolumeResourceRequirements,
};
// `networking::v1` carries the egress NetworkPolicy types (issue #443). The
// k8s `NetworkPolicy` is aliased so it never collides with the CRD-side
// `sandbawks_types::NetworkPolicy` (the `spec.networkPolicy` field type).
use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy as K8sNetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{
    ApiResource, DeleteParams, DynamicObject, ObjectMeta, Patch, PatchParams, PostParams,
    PropagationPolicy, TypeMeta,
};
use kube::{Api, Client, ResourceExt};
use sandbawks_types::{
    PodReadiness, PodRef, PodStatusSummary, PodTerminationCause, RestorePlacement, RestoreSource,
    RuntimeAdapter, RuntimeError, Sandbox, SnapshotManifest, SnapshotSink, SnapshotSource,
    VolumeClaimTemplate, ENVD_RELOCATE_TARGET, ENVD_RELOCATE_TARGET_ENV, ENVD_STAGED_LABEL,
    RUNSC_VERSION_LABEL, SANDBOX_NAME_LABEL,
};

/// envd's well-known gRPC port. Matches `DEFAULT_LISTEN` in
/// `crates/envd/src/bin/envd.rs` — the primary RPC surface the
/// controller's `envd_address` points at.
const ENVD_PORT: i32 = 49_983;

/// Standard `app.kubernetes.io/name` label key stamped on every sandbox
/// Pod. Promoted to a shared constant (issue #328) so the controller's
/// owned-Pod watch can build its label selector from the same source as
/// the Pod that carries the label — the key and value can never drift.
pub const SANDBOX_POD_LABEL_KEY: &str = "app.kubernetes.io/name";

/// Value paired with [`SANDBOX_POD_LABEL_KEY`] on sandbox Pods. Scoping
/// the owned-Pod watch to `KEY=VALUE` keeps unrelated Pod churn (node /
/// api Pods) from driving needless Sandbox reconciles.
pub const SANDBOX_POD_LABEL_VALUE: &str = "sandbawks-sandbox";

// The per-sandbox identity label key ([`SANDBOX_NAME_LABEL`]) lives in
// `sandbawks_types::labels` (a cross-crate contract — the controller stamps
// it on every sandbox Pod and the `sandbawks-node` DaemonSet reads it back),
// so it is imported rather than redefined here. Unlike [`SANDBOX_POD_LABEL_KEY`]
// (shared by *all* sandbox Pods), its value is unique per sandbox, which is
// what lets the egress `NetworkPolicy`'s `podSelector` target exactly one
// sandbox's Pod (issue #443).

/// DNS resolver port permitted out of a sandbox when its
/// `spec.networkPolicy.egressAllow` lists at least one DNS *name* (issue
/// #443). A standard `networking.v1` NetworkPolicy cannot encode an FQDN
/// destination, so name-based `egressAllow` entries need the cluster
/// resolver reachable for the name to resolve at all; the resolved L3
/// destination is then enforced by the cluster's name-aware CNI layer
/// (Cilium FQDN policy) rather than this baseline policy. See
/// [`PodAdapter::network_policy_for`].
const DNS_PORT: i32 = 53;

/// Controller env var that opts a cluster into FQDN egress enforcement
/// (issue #541). Only when this is `"true"` does the adapter materialize a
/// Cilium `toFQDNs` [`CiliumNetworkPolicy`](PodAdapter::cilium_network_policy_for)
/// alongside the baseline `networking.v1.NetworkPolicy`, so a DNS-*name* entry
/// in `egressAllow` authorizes the *resolved* L3 connection rather than merely
/// opening DNS (today's behavior).
///
/// An explicit flag is chosen over runtime CNI capability detection: it is
/// deterministic, unit-testable without an apiserver, and lets clusters lacking
/// FQDN-aware policy stay on the DNS-port-only path with no regression. The
/// operator sets it via Helm only on a Cilium cluster whose `CiliumNetworkPolicy`
/// CRD is installed.
const FQDN_EGRESS_ENABLED_ENV: &str = "SANDBAWKS_FQDN_EGRESS_ENABLED";

/// Controller env var naming the namespace the cluster DNS resolver runs in,
/// used to scope the `CiliumNetworkPolicy`'s DNS-visibility rule (issue #541).
/// Defaults to [`DEFAULT_FQDN_DNS_NAMESPACE`]. Configurable rather than
/// hardcoded so non-stock clusters (different resolver placement) work without
/// a rebuild.
const FQDN_DNS_NAMESPACE_ENV: &str = "SANDBAWKS_FQDN_DNS_NAMESPACE";

/// Controller env var carrying the `key=value` pod label that selects the
/// cluster DNS resolver, used by the `CiliumNetworkPolicy`'s DNS-visibility
/// rule (issue #541). Defaults to [`DEFAULT_FQDN_DNS_POD_LABEL`] (the standard
/// CoreDNS label). Configurable for the same no-hardcode reason as
/// [`FQDN_DNS_NAMESPACE_ENV`].
const FQDN_DNS_POD_LABEL_ENV: &str = "SANDBAWKS_FQDN_DNS_POD_LABEL";

/// Default namespace the cluster DNS resolver runs in when
/// [`FQDN_DNS_NAMESPACE_ENV`] is unset (the stock kubeadm/k3s placement).
const DEFAULT_FQDN_DNS_NAMESPACE: &str = "kube-system";

/// Default `key=value` pod label selecting the cluster DNS resolver when
/// [`FQDN_DNS_POD_LABEL_ENV`] is unset (the label CoreDNS and kube-dns both
/// carry).
const DEFAULT_FQDN_DNS_POD_LABEL: &str = "k8s-app=kube-dns";

/// `apiVersion` of the Cilium `CiliumNetworkPolicy` CRD the FQDN-egress path
/// materializes (issue #541). The CRD ships with Cilium itself, not this chart.
const CILIUM_NP_API_VERSION: &str = "cilium.io/v2";

/// API group of [`CILIUM_NP_API_VERSION`].
const CILIUM_NP_GROUP: &str = "cilium.io";

/// API version (without group) of [`CILIUM_NP_API_VERSION`].
const CILIUM_NP_VERSION: &str = "v2";

/// `kind` of the Cilium FQDN-egress CRD.
const CILIUM_NP_KIND: &str = "CiliumNetworkPolicy";

/// Lower-cased plural the apiserver routes `CiliumNetworkPolicy` requests under.
const CILIUM_NP_PLURAL: &str = "ciliumnetworkpolicies";

/// Name of the gRPC `containerPort`. The readinessProbe targets the port
/// by this name rather than the raw [`ENVD_PORT`] number, so the probe
/// and the port declaration can never silently drift apart (the
/// kubernetes-house-style way to write a probe).
const ENVD_PORT_NAME: &str = "envd";

/// envd's well-known HTTP port for the `/__state` route. Matches
/// `DEFAULT_HTTP_LISTEN` in `crates/envd/src/bin/envd.rs`; declared
/// here so kubelet sees both surfaces on the container and the node
/// DaemonSet's pause path probe (`http://<pod_ip>:49984/__state`) has
/// the same in-cluster reachability story as the gRPC port.
const ENVD_HTTP_PORT: i32 = 49_984;

/// Timeout for the resume serve-readiness probe (`GET /__version`, issues
/// #560/#620). The probe distinguishes a healthy envd (answers `200` promptly)
/// from a checkpoint-wedged one (accepts the TCP connection but never answers);
/// a few seconds is ample for a healthy localhost-fast envd — `/__version`
/// answers in ~2ms — while bounding how long a wedged one stalls a reconcile
/// pass.
///
/// #620: the gate deliberately targets `/__version`, NOT `/__state`. `/__state`
/// recomputes a full rootfs fingerprint (the #236 resume oracle) on every
/// request — ~33s on a real sandbox — so reusing it as a per-reconcile liveness
/// probe timed out forever and hung filesystem-mode resumes in `Resuming`.
/// `/__version` is a content-free "is the HTTP control plane serving?" signal on
/// the same `:49984` listener, which is exactly what this gate needs.
const ENVD_SERVE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Whether an envd `/__version` HTTP response head classifies as "serving" — a
/// `200`. The status line is `HTTP/1.x 200 ...`, so bytes 9..12 are the code.
///
/// `starts_with(b"HTTP/1.")` deliberately treats an HTTP/2 reply as
/// **not-serving**: envd's `:49984` control plane is plain HTTP/1 (axum) today,
/// so an HTTP/2 status line would be anomalous and is safer classified as a
/// wedge than trusted. This is intentional — not a missing HTTP/2 case.
fn classify_serving_response(head: &[u8]) -> bool {
    head.starts_with(b"HTTP/1.") && head.get(9..12).is_some_and(|code| code == b"200")
}

/// Connect to envd's HTTP control port at `http_addr`, send `GET /__version`,
/// and return whether it answered `200` within [`ENVD_SERVE_PROBE_TIMEOUT`]
/// (issues #560/#620). A checkpoint-wedged envd accepts the connection but never
/// answers; that, a connect/read failure, a timeout, or a reply that closes
/// before a full status line all classify as `false` ("not serving") — the
/// resume readiness deadline (not a transient-error retry) owns recovery from a
/// persistent wedge.
///
/// #620: probes `/__version` rather than `/__state`. `/__version` returns
/// envd's `BUILD_ID` in ~2ms; `/__state` fingerprints the entire rootfs (~33s)
/// and is the wrong target for a frequent liveness check — see
/// [`ENVD_SERVE_PROBE_TIMEOUT`].
async fn probe_envd_serving(http_addr: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let probe = async {
        let mut stream = tokio::net::TcpStream::connect(http_addr).await?;
        stream
            .write_all(b"GET /__version HTTP/1.0\r\nHost: envd\r\nConnection: close\r\n\r\n")
            .await?;
        // The status line arrives first; read just enough to classify it. The
        // loop guards a short/fragmented first read — a server dripping the head
        // one byte at a time still accumulates to >= 12 before we classify.
        let mut head = Vec::with_capacity(64);
        let mut buf = [0u8; 64];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            head.extend_from_slice(&buf[..n]);
            if head.len() >= 12 {
                break;
            }
        }
        std::io::Result::Ok(head)
    };
    match tokio::time::timeout(ENVD_SERVE_PROBE_TIMEOUT, probe).await {
        Ok(Ok(head)) => classify_serving_response(&head),
        // Connected but no healthy answer in time (the #560 wedge), the peer
        // reset/closed early, or the connect itself failed: not serving.
        Ok(Err(_)) | Err(_) => false,
    }
}

/// readinessProbe cadence for the envd `:49983` gate (issue #350). Two
/// seconds: a Sandbox still flips to `Ready` within ~2s of the gRPC
/// listener accepting — negligible against a multi-second GiB-scale
/// restore — while halving the kubelet `Unhealthy: connection refused`
/// event volume a faster 1s cadence would emit on every miss during the
/// whole pre-listener window. See the readinessProbe construction for the
/// full noise/latency rationale.
const ENVD_READINESS_PERIOD_SECS: i32 = 2;

/// RuntimeClass a restore Pod defaults to when the Sandbox doesn't pin
/// its own (Plan 07 T9). Binds the Pod to the gVisor `runsc` shim that
/// performs the start-time `runsc restore` from the staged image. Must
/// match the `RuntimeClass` name shipped by the Plan 07 T6 Helm
/// template.
const DEFAULT_RESTORE_RUNTIME_CLASS: &str = "sandbawks-runsc";

/// The plain gVisor cold-start runtime class. Its containerd handler has
/// no restore-capable shim, so a restore Pod for a `gvisor` sandbox is
/// mapped onto [`DEFAULT_RESTORE_RUNTIME_CLASS`] — the same gVisor
/// runtime plus the sandbawks shim wrapper that drives the start-time
/// `runsc restore` (issue #196).
const GVISOR_RUNTIME_CLASS: &str = "gvisor";

/// Annotation key carrying the local filesystem path the `Stage` job
/// materialized the snapshot at. The `containerd-shim-sandbawks-v1`
/// reads this at container start to drive `runsc restore` from local
/// disk. Must match the key the shim looks up (Plan 07 T4/T8).
const ANNOTATION_RESTORE_IMAGE_PATH: &str = "io.sandbawks/restore-image-path";

// Restore wait-for-staging gate (epic #484): there is deliberately no Pod-level
// stage-gate constant here. A scheduler-placed restore Pod is created before its
// snapshot is staged, but the wait for the node's `.staged` marker lives in the
// restore shim (`containerd-shim-sandbawks-v1`), not in a Pod init container — a
// gVisor memory checkpoint restores as a whole sandbox and runsc reconstitutes
// exactly the checkpoint's container set, so any extra init container is rejected
// by restore-spec validation. See `pod_spec_for`'s staging comment.

/// Annotation key the `sigs.k8s.io/descheduler` `DefaultEvictor` honors
/// to opt a Pod out of descheduler-initiated eviction (#498).
///
/// The cluster's descheduler (v0.35.0) runs `DefaultEvictor` with
/// `noEvictionPolicy: Mandatory`, which turns this annotation into a
/// **hard** skip across every descheduler plugin — most importantly
/// `LowNodeUtilization`, which evicted a warm, momentarily-idle sandbox
/// (`sbx-warm-zqv22`, 2026-06-11) because its low CPU/memory footprint
/// looked like a wasted pod. A sandbox Pod is never a safe descheduler
/// target: evicting it kills the gVisor process and loses the live
/// (or warm, pre-started) session, and no controller live-migrates it.
///
/// Why this key and not `descheduler.alpha.kubernetes.io/evict`: in the
/// deployed descheduler the `evict` annotation is presence-checked and
/// means *opt-in* (force a normally-protected Pod to be evictable), so
/// `evict: "false"` would have the catastrophic opposite effect of
/// force-evicting every sandbox. `prefer-no-eviction` is the opt-*out*
/// signal paired with `noEvictionPolicy`.
///
/// Scope: this only suppresses the *descheduler*. Involuntary disruptions
/// (node failure, `NoExecute` taint eviction by the node controller) and
/// `kubectl drain` (which calls the Eviction API directly, ignoring this
/// annotation) are unaffected, so legitimate node-maintenance eviction
/// still works. The protection's lifecycle is tied to Pod existence: a
/// paused sandbox is checkpointed to storage with no Pod, so there is
/// nothing to evict and nothing to annotate.
const ANNOTATION_PREFER_NO_EVICTION: &str = "descheduler.alpha.kubernetes.io/prefer-no-eviction";

/// Value written for [`ANNOTATION_PREFER_NO_EVICTION`]. The descheduler's
/// `DefaultEvictor` parses this as a bool, so it must be `"true"`.
const ANNOTATION_PREFER_NO_EVICTION_VALUE: &str = "true";

/// Annotation on a [`VolumeClaimTemplate`]'s metadata naming the container
/// path its PVC mounts at (#603 Phase 2, issue #605).
///
/// The CRD's `volumeClaimTemplate` mirrors StatefulSet semantics — it declares
/// the *volume*, not where the workload mounts it — but unlike a StatefulSet,
/// a Sandbox exposes no per-container `volumeMounts` surface for the user to
/// supply a mount path on. This annotation is that surface: set it to mount the
/// volume at an explicit path (e.g. `/home/user/workspace`). When absent, the
/// path defaults to `/<volume-name>` (see [`vct_mount_path`]), so a template
/// named `workspace` mounts at `/workspace` with no annotation — the headline
/// acceptance shape for epic #603. It lives on `metadata.annotations` (a
/// free-form map already in the Phase 1 CRD) so Phase 2 adds the mount path
/// without a schema change.
const VCT_MOUNT_PATH_ANNOTATION: &str = "sandbawks.dev/mount-path";

/// Host directory the `sandbawks-node` DaemonSet stages the static
/// `envd` binary into (as `<dir>/envd`). The sandbox Pod mounts this
/// directory read-only and execs the binary, so envd runs *inside* the
/// workload's filesystem without the customer image bundling it.
///
/// Must match the DaemonSet's staging path — see
/// `deploy/helm/sandbawks/templates/node-daemonset.yaml`
/// (`node.hostPathEnvd`). A host bind keeps the (tens-of-MB) binary out
/// of the `runsc checkpoint` memory image, and a read-only mount keeps
/// it out of the node's pause-time mount-set assertion (#237); on
/// restore gVisor reconnects the same node-provided file. The
/// cold-start and restore Pods mount it identically, so the restore
/// Pod's topology matches the checkpoint.
///
/// Scheduling is gated on the node having staged the binary (#272): the
/// `sandbawks-node` DaemonSet stamps [`ENVD_STAGED_LABEL`] on a node only
/// after `stage-envd` completes, and every sandbox Pod carries a matching
/// `Exists` nodeAffinity (see [`PodAdapter::pod_spec_for`]). Without that
/// gate, `DirectoryOrCreate` + no scheduling constraint let a Pod land on
/// a node before its first staging, mount an empty dir, and CrashLoop on
/// the missing binary (cluster bootstrap / node bring-up / initial
/// DaemonSet roll).
///
/// On a *subsequent* DaemonSet image roll the label persists while
/// `stage-envd` re-runs, but `envd self-install` writes to a temp sibling
/// and `rename`s it into place (inode swap), so a Pod scheduling mid-roll
/// execs a complete binary — the old inode or the new one, never a
/// half-written file. Matching the exact post-roll *content* is #270's
/// manifest-hash gate, not this label; clearing a stale label if the
/// staged binary later disappears is #282.
const ENVD_STAGE_HOSTPATH: &str = "/var/lib/sandbawks/envd";

/// In-Pod mount point for [`ENVD_STAGE_HOSTPATH`].
const ENVD_BIN_DIR: &str = "/sandbawks/bin";

/// Absolute path of the staged `envd` binary inside the container —
/// [`ENVD_BIN_DIR`] + `/envd`.
const ENVD_BIN_PATH: &str = "/sandbawks/bin/envd";

/// Volume name for the node-staged envd binary mount.
const ENVD_BIN_VOLUME: &str = "sandbawks-envd-bin";

/// Headroom, in bytes, added to a sandbox Pod's memory request/limit to cover
/// the relocated envd copy (epic #466). envd copies itself onto
/// [`ENVD_RELOCATE_TARGET`] in the workload rootfs, which under
/// `--overlay2=root:self` lives in the sandbox-internal overlay-tmpfs — RAM that
/// counts against the container's memory cgroup. Without this the relocated envd
/// (a resident binary of tens of MB) would silently eat the customer's workload
/// headroom. 64 MiB covers the resident binary plus the temp sibling envd writes
/// before its inode-swap self-update.
const ENVD_RELOCATE_BUDGET_BYTES: i64 = 64 * 1024 * 1024;

/// Downward-API env var carrying the sandbox Pod's own name. `envd`'s
/// `init_telemetry` reads it for a stable `service.instance.id` (issue
/// #295); without it the SDK mints a fresh random UUID per restart, which
/// churns the per-sandbox identity in dashboards. Injected on the envd
/// container directly (not forwarded from the controller's environment —
/// the controller's `POD_NAME` is the *controller's* Pod, not the
/// sandbox's). Matches the name the shared `sandbawks.observabilityEnv`
/// Helm helper uses for the api/controller/node workloads.
const POD_NAME_ENV: &str = "POD_NAME";

/// OpenTelemetry environment variables the controller forwards from its
/// own process environment into every sandbox's `envd` container, so envd
/// exports to the same collector — and honors the same `OTEL_SDK_DISABLED`
/// kill-switch — as the api, controller, and node workloads (all wired by
/// the shared `sandbawks.observabilityEnv` Helm helper, which also sets
/// these vars on the controller process). Without this forwarding the
/// envd container inherits none of them and the SDK silently falls back to
/// its built-in `http://localhost:4318`, which is unreachable from inside
/// the sandbox and floods export-retry warnings.
///
/// Deliberately excluded:
/// * `OTEL_EXPORTER_OTLP_HEADERS` — may carry the collector's auth token
///   (the Helm helper sources it from a Secret). The sandbox Pod is
///   customer-controlled, so forwarding the token would leak a credential
///   into an untrusted environment. The recommended topology keeps the
///   token on an in-cluster collector and points workloads at its
///   plaintext intake, so envd needs only the endpoint, not credentials.
/// * `POD_NAME` — injected via the downward API ([`POD_NAME_ENV`]) so
///   `service.instance.id` is the *sandbox* Pod's name, not the
///   controller's.
/// * `OTEL_RESOURCE_ATTRIBUTES` — handled by [`per_sandbox_observability_env`],
///   which reads the controller's base value and appends per-sandbox identity
///   attrs (pod name, namespace, template, CR uid) before injecting the merged
///   string. Forwarding it verbatim here would clobber the per-sandbox attrs
///   on key collision, or produce a duplicate `OTEL_RESOURCE_ATTRIBUTES` entry.
/// * `OTEL_SERVICE_NAME` — set to [`ENVD_SERVICE_NAME`] by
///   [`per_sandbox_observability_env`] so that envd spans and the inner Claude
///   Code agent spans (when the trace beta is enabled) land under a named service
///   in Traceway. Forwarding the controller's own service name here would be
///   incorrect (the controller is `sandbawks-controller`, not envd).
const FORWARDED_OTEL_VARS: [&str; 7] = [
    // First so the disabled-by-default case (controller has only this var)
    // still forwards the kill-switch and envd short-circuits cleanly.
    "OTEL_SDK_DISABLED",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TIMEOUT",
    "OTEL_TRACES_SAMPLER",
    "OTEL_TRACES_SAMPLER_ARG",
    // GenAI content-capture gate (issue #426). When the chart sets this on the
    // controller (`observability.captureGenaiContent=true`), forwarding it lets
    // a sandbox's envd record the (redacted) command line on its `execute_tool`
    // spans. Off by default and value-only — never a credential — so it is safe
    // to forward into the customer-controlled sandbox Pod.
    "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
];

/// `OTEL_RESOURCE_ATTRIBUTES` env var name — the OTel SDK reads this to merge
/// extra key/value pairs into the resource describing the running service.
const OTEL_RESOURCE_ATTRIBUTES_ENV: &str = "OTEL_RESOURCE_ATTRIBUTES";

/// `OTEL_SERVICE_NAME` env var name — overrides the `service.name` resource
/// attribute set by the SDK's compile-time default.
const OTEL_SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";

/// OTel `service.name` stamped on every sandbox Pod (issue #435). Keeps envd
/// spans and inner Claude Code agent spans (when the trace beta is enabled)
/// under a named, distinguishable service in Traceway rather than the SDK's
/// `unknown_service` placeholder or the controller's own service name.
const ENVD_SERVICE_NAME: &str = "sandbawks-envd";

/// When split pipelines are enabled (issue #424), the Helm chart sets this env
/// var on the controller Deployment to the sandbox collector endpoint (port
/// 4318). The controller's own `OTEL_EXPORTER_OTLP_ENDPOINT` then points at
/// the internal pipeline (port 4320), and `forwarded_observability_env` uses
/// this override so sandbox envd reaches the scrubbed sandbox pipeline instead.
const SANDBOX_OTEL_ENDPOINT_OVERRIDE_ENV: &str = "SANDBAWKS_OTEL_SANDBOX_ENDPOINT";

/// Build the `env:` entries that forward the controller's observability
/// configuration to an envd container.
///
/// `lookup` resolves an environment-variable name to its value (in
/// production, [`std::env::var`] coerced to `Option`); only the keys in
/// [`FORWARDED_OTEL_VARS`] that resolve to a value are emitted, preserving
/// declaration order. Taking the lookup as a parameter keeps this pure and
/// unit-testable without mutating the process environment.
///
/// When [`SANDBOX_OTEL_ENDPOINT_OVERRIDE_ENV`] is present in the environment
/// (set by the Helm chart when split pipelines are enabled), it overrides the
/// `OTEL_EXPORTER_OTLP_ENDPOINT` forwarded to sandbox envd so signals land in
/// the scrubbed sandbox pipeline rather than the internal one.
fn forwarded_observability_env<F>(lookup: F) -> Vec<EnvVar>
where
    F: Fn(&str) -> Option<String>,
{
    // When the split-pipeline flag is active (issue #424), the controller's
    // OTEL_EXPORTER_OTLP_ENDPOINT points at the internal pipeline receiver.
    // The Helm chart sets SANDBAWKS_OTEL_SANDBOX_ENDPOINT to the sandbox
    // pipeline receiver; use it to override the forwarded endpoint so envd
    // hits the scrubbed pipeline rather than the internal one.
    let sandbox_endpoint_override = lookup(SANDBOX_OTEL_ENDPOINT_OVERRIDE_ENV);

    FORWARDED_OTEL_VARS
        .iter()
        .filter_map(|&name| {
            let value = if name == "OTEL_EXPORTER_OTLP_ENDPOINT" {
                // Prefer the sandbox-pipeline override; fall back to the
                // controller's own endpoint when split pipelines are off.
                sandbox_endpoint_override.clone().or_else(|| lookup(name))
            } else {
                lookup(name)
            };
            value.map(|v| EnvVar {
                name: name.to_owned(),
                value: Some(v),
                value_from: None,
            })
        })
        .collect()
}

/// Build the `env:` entries that carry per-sandbox OTel resource attributes.
///
/// Produces up to two env vars:
///
/// * `OTEL_RESOURCE_ATTRIBUTES` — a base value merged with per-sandbox identity
///   attrs:
///
///   | Key | Value | Convention |
///   |-----|-------|-----------|
///   | `k8s.pod.name` | sandbox Pod name | OTel k8s semconv |
///   | `k8s.namespace.name` | Pod namespace | OTel k8s semconv |
///   | `sandbox.template` | `spec.templateRef` | Sandbawks custom |
///   | `sandbox.uid` | Sandbox CR `metadata.uid` | Sandbawks custom |
///
///   The base is the customer's `spec.env` pin (`customer_attrs`) if present,
///   else the controller's cluster-level value (`lookup(OTEL_RESOURCE_ATTRIBUTES)`).
///   Per-sandbox identity attrs are *always* appended after the base so they win
///   on key collision (OTel spec: last definition for a key takes precedence) and
///   can never be nulled out — a customer who pins `OTEL_RESOURCE_ATTRIBUTES`
///   cannot accidentally strip a sandbox's identity, only override individual
///   keys (issue #435). This is why the caller does *not* run the customer_keys
///   filter over this var; it folds the customer's value into the merge instead.
///
/// * `OTEL_SERVICE_NAME` — always set to [`ENVD_SERVICE_NAME`], regardless of
///   any `OTEL_SERVICE_NAME` on the controller (that names the *controller*,
///   never envd). Makes both envd spans and the inner Claude Code agent spans
///   (when the trace beta is enabled via `CLAUDE_CODE_ENABLE_TELEMETRY=1`) land
///   under a named, distinguishable service in Traceway rather than the SDK
///   placeholder. A customer can still override it via `spec.env`, which
///   `pod_spec_for`'s customer_keys filter honours.
///
/// # Arguments
///
/// * `sandbox` — the Sandbox CR. `metadata.name`, `metadata.namespace`,
///   `metadata.uid`, and `spec.templateRef` are read directly; the CR is
///   already in hand at pod-build time so no extra downward-API wiring is
///   needed.
/// * `customer_attrs` — the customer's `spec.env` pin of `OTEL_RESOURCE_ATTRIBUTES`,
///   if any. Takes the place of the controller's cluster-level base (customer-wins,
///   as for every forwarded var) while identity attrs are still appended after it.
/// * `lookup` — resolves an environment-variable name to its value. Pure and
///   unit-testable; production callers pass
///   `|name| std::env::var(name).ok()`.
fn per_sandbox_observability_env<F>(
    sandbox: &Sandbox,
    customer_attrs: Option<&str>,
    lookup: F,
) -> Vec<EnvVar>
where
    F: Fn(&str) -> Option<String>,
{
    // A reconciled Sandbox always carries a server-assigned name; the literal
    // `"sandbox"` fallback below exists only for unit-test stubs. The
    // debug_assert makes that invariant explicit — in a debug/test build a stub
    // (or a real regression) that would emit a non-unique `k8s.pod.name=sandbox`
    // across every sandbox fails loudly, while release builds keep the
    // well-formed fallback. (Namespace is intentionally not asserted: its
    // `"default"` fallback carries no cross-sandbox cardinality hazard, and some
    // stubs legitimately leave it unset.)
    debug_assert!(
        sandbox.metadata.name.is_some(),
        "Sandbox missing metadata.name; k8s.pod.name would collide across sandboxes"
    );
    let pod_name = sandbox.metadata.name.as_deref().unwrap_or("sandbox");
    let namespace = sandbox.metadata.namespace.as_deref().unwrap_or("default");

    // Both OTel k8s semconv keys (tooling compatibility) and sandbox.* custom
    // keys (Traceway product grouping). The CR uid is omitted for Sandboxes
    // that have not yet been admitted by the apiserver (e.g. unit test stubs
    // without a server-assigned uid) so the output is always well-formed.
    let mut per_sandbox_parts: Vec<String> = vec![
        format!("k8s.pod.name={pod_name}"),
        format!("k8s.namespace.name={namespace}"),
        format!("sandbox.template={}", sandbox.spec.template_ref),
    ];
    if let Some(uid) = &sandbox.metadata.uid {
        per_sandbox_parts.push(format!("sandbox.uid={uid}"));
    }
    let per_sandbox_attrs = per_sandbox_parts.join(",");

    // Base attrs, appended *before* per-sandbox identity so identity wins on key
    // collision. A customer-pinned `OTEL_RESOURCE_ATTRIBUTES` (spec.env) takes the
    // place of the controller's cluster-level value — customer-wins for the base
    // portion — but either way identity is folded in last so it cannot be lost.
    let base = customer_attrs
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .or_else(|| lookup(OTEL_RESOURCE_ATTRIBUTES_ENV).filter(|s| !s.is_empty()));
    let merged = match base {
        Some(base) => format!("{base},{per_sandbox_attrs}"),
        None => per_sandbox_attrs,
    };

    let mut vars = vec![EnvVar {
        name: OTEL_RESOURCE_ATTRIBUTES_ENV.into(),
        value: Some(merged),
        value_from: None,
    }];

    // Always set OTEL_SERVICE_NAME to envd's own name. We deliberately do *not*
    // consult the controller's `OTEL_SERVICE_NAME`: that value is the
    // controller's identity (`sandbawks-controller`) and is never the right
    // service name for envd inside a sandbox Pod, and the var is intentionally
    // absent from `FORWARDED_OTEL_VARS`. Skipping injection when the controller
    // had it set would leave envd with no service name at all (SDK
    // `unknown_service` placeholder) — defeating #435. A customer who genuinely
    // wants a different name can still pin it in `spec.env`, since
    // `pod_spec_for`'s customer_keys filter drops this var when present there.
    vars.push(EnvVar {
        name: OTEL_SERVICE_NAME_ENV.into(),
        value: Some(ENVD_SERVICE_NAME.into()),
        value_from: None,
    });

    vars
}

/// Parse a Kubernetes memory quantity (e.g. `"256Mi"`, `"1Gi"`, `"512M"`,
/// or a bare byte count) into bytes.
///
/// Splits the leading numeric mantissa from the unit suffix and multiplies
/// by the suffix's factor: binary IEC (`Ki`/`Mi`/`Gi`/`Ti`/`Pi`/`Ei`,
/// powers of 1024) or decimal SI (`k`/`M`/`G`/`T`/`P`/`E`, powers of 1000).
/// An empty suffix means bytes.
///
/// Returns `None` for any unrecognized unit (e.g. exponent notation like
/// `"1e3"`, whose `e` is read as a suffix) so the caller can fall back to
/// the verbatim quantity rather than emit a wrong byte count. The mantissa
/// is parsed as `f64`, which is exact for every realistic memory size
/// (`f64` represents integers exactly up to 2^53 ≈ 9 PB).
fn quantity_to_bytes(quantity: &str) -> Option<f64> {
    let quantity = quantity.trim();
    // The suffix starts at the first ASCII letter; everything before it is the
    // numeric mantissa (digits and an optional decimal point).
    let split = quantity
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(quantity.len());
    let (mantissa, suffix) = quantity.split_at(split);
    let value: f64 = mantissa.parse().ok()?;
    let multiplier = match suffix {
        "" => 1.0,
        "Ki" => 1024.0,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "Ei" => 1024f64.powi(6),
        "k" | "K" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        _ => return None,
    };
    Some(value * multiplier)
}

/// Add the relocated-envd memory budget ([`ENVD_RELOCATE_BUDGET_BYTES`]) to a
/// sandbox's requested memory, so the envd copy in the overlay-tmpfs doesn't eat
/// the customer's workload headroom (epic #466).
///
/// The result is emitted as a bare byte count, which Kubernetes accepts as a
/// valid quantity. If `base` can't be parsed into bytes (an unrecognized
/// unit — shouldn't happen for a shape-validated [`ResourceQuantity`]), the
/// original quantity is returned unchanged: budgeting is best-effort and must
/// never reshape an otherwise-valid memory request into something the API
/// server rejects.
fn memory_with_relocate_budget(base: &sandbawks_types::ResourceQuantity) -> Quantity {
    match quantity_to_bytes(base.as_str()) {
        // `ceil` guards the (sub-byte) fractional case; every integer memory
        // value round-trips exactly through the `f64` in `quantity_to_bytes`.
        Some(bytes) => {
            let total = bytes.ceil() as i128 + ENVD_RELOCATE_BUDGET_BYTES as i128;
            Quantity(total.to_string())
        }
        None => Quantity(base.to_string()),
    }
}

/// Upper bound on how long `delete_pod` waits for kubelet to finish
/// tearing the container down and the API server to remove the Pod
/// resource. Default kubelet `terminationGracePeriodSeconds` is 30 s;
/// 90 s covers that plus apiserver finalization plus a slow kubelet
/// under CPU pressure on a kind cluster. A timeout surfaces as a
/// `RuntimeError::Transient` so the reconcile loop can retry.
const POD_DELETE_WAIT: std::time::Duration = std::time::Duration::from_secs(90);

/// Poll interval while waiting for the Pod to disappear from the API
/// server. 250 ms balances responsiveness against load on the kube
/// API server when several sandboxes are deleted in close succession
/// (e.g. a batch teardown).
const POD_DELETE_POLL: std::time::Duration = std::time::Duration::from_millis(250);

/// Real-Pod adapter. Constructed once per controller process.
///
/// Cloning the adapter is cheap because the inner [`kube::Client`] is
/// `Arc`-backed internally — the reconcile loop holds one
/// `Arc<dyn RuntimeAdapter>` for the whole controller lifetime, but a
/// `Clone` impl is provided so callers can hand the adapter out to
/// helper tasks without re-`Arc`-wrapping it.
#[derive(Clone)]
pub struct PodAdapter {
    client: Client,
}

impl PodAdapter {
    /// Construct a `PodAdapter` bound to the given kube `Client`.
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Namespaced `Api<Pod>` handle.
    fn pods(&self, namespace: &str) -> Api<Pod> {
        Api::namespaced(self.client.clone(), namespace)
    }

    /// Namespaced `Api<Secret>` handle for the per-sandbox env Secret
    /// (issue #494).
    fn secrets(&self, namespace: &str) -> Api<Secret> {
        Api::namespaced(self.client.clone(), namespace)
    }

    /// Namespaced `Api<NetworkPolicy>` handle for the per-sandbox
    /// default-deny egress policy (issue #443).
    fn network_policies(&self, namespace: &str) -> Api<K8sNetworkPolicy> {
        Api::namespaced(self.client.clone(), namespace)
    }

    /// Namespaced `Api<PersistentVolumeClaim>` handle for the Sandbox-owned
    /// PVCs that back `filesystem`-mode durable paths (#603 Phase 2, #605).
    fn pvcs(&self, namespace: &str) -> Api<PersistentVolumeClaim> {
        Api::namespaced(self.client.clone(), namespace)
    }

    /// Create the Sandbox-owned PVCs for a Sandbox's `volumeClaimTemplates`,
    /// idempotently and without ever clobbering a PVC we don't own (#603 Phase
    /// 2, issue #605).
    ///
    /// For each PVC [`Self::pvcs_for`] builds:
    ///
    /// * **Created** → success.
    /// * **`409 AlreadyExists`** → GET it and check ownership ([`pvc_owned_by`]).
    ///   Our own PVC from a prior reconcile is idempotent success; a *foreign*
    ///   PVC sharing the deterministic `{template}-{sandbox}` name is refused —
    ///   we never adopt or overwrite storage we don't own. A PVC is created
    ///   once and never reconciled/resized (create-once, per the epic's
    ///   out-of-scope), so there is no update path here.
    ///
    /// Returns the first error encountered, mirroring the env-Secret /
    /// NetworkPolicy create sequence so `create_pod` aborts before the Pod when
    /// a volume can't be provisioned.
    async fn ensure_pvcs(&self, sandbox: &Sandbox, namespace: &str) -> Result<(), RuntimeError> {
        // Validate the template names *before* touching the apiserver (#605
        // review). The name feeds both the Pod volume name (a DNS-1123 label)
        // and the `{template}-{sandbox}` PVC name; an invalid or duplicate name
        // would otherwise surface as an opaque apiserver rejection deep in
        // PVC/Pod creation. The CRD's `pattern`/`maxLength` already reject a
        // malformed name at admission — this is the controller-side
        // defense-in-depth (older CRD with the validation pruned, hand-patched
        // object) turning the same failure into a clear, actionable message.
        // The failure is structural (no retry can fix a bad name); it reuses
        // `Transient` because the error enum has no permanent variant and a
        // `create_pod` error folds the Sandbox to terminal `Failed` regardless,
        // carrying this message into the `Failed` condition.
        if let Some(templates) = sandbox.spec.volume_claim_templates.as_deref() {
            validate_volume_claim_template_names(templates).map_err(RuntimeError::Transient)?;
        }

        let api = self.pvcs(namespace);
        for pvc in Self::pvcs_for(sandbox) {
            let name = pvc.metadata.name.clone().unwrap_or_default();
            match api.create(&PostParams::default(), &pvc).await {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 409 => {
                    // Exists already — adopt only if it is ours. GET so the
                    // ownership decision is made against the live object, not
                    // the one we tried to create. A `404` here is benign rather
                    // than a bug: it means the PVC was deleted between our
                    // create-409 and this read (a racing teardown), so the
                    // wrapped `Transient` correctly drives a requeue that
                    // re-creates it from scratch.
                    let existing = api.get(&name).await.map_err(|e| {
                        RuntimeError::Transient(format!("get existing pvc {name}: {e}"))
                    })?;
                    if pvc_owned_by(&existing, sandbox) {
                        tracing::debug!(
                            pvc = %name,
                            namespace = %namespace,
                            "sandbox-owned PVC already exists; treating create as idempotent"
                        );
                    } else {
                        // A foreign PVC at our name — refuse rather than mount
                        // someone else's data into the sandbox. Loud + greppable
                        // because it should never happen (names are
                        // sandbox-uid-scoped); a stuck reconcile here is the
                        // intended fail-closed posture, not silent adoption.
                        tracing::error!(
                            pvc = %name,
                            namespace = %namespace,
                            "refusing to adopt foreign-owned PVC; sandbox will not start"
                        );
                        return Err(RuntimeError::Transient(format!(
                            "PVC {name} exists but is not owned by this sandbox; \
                             refusing to clobber it"
                        )));
                    }
                }
                Err(e) => {
                    return Err(RuntimeError::Transient(format!("create pvc {name}: {e}")));
                }
            }
        }
        Ok(())
    }

    /// Namespaced `Api<DynamicObject>` handle for the per-sandbox Cilium
    /// `CiliumNetworkPolicy` that backs FQDN egress (issue #541).
    ///
    /// `CiliumNetworkPolicy` is a Cilium-owned CRD with no `k8s-openapi` type,
    /// so it is addressed dynamically via an [`ApiResource`] built from the
    /// CRD's GVK constants rather than a compile-time generated client.
    fn cilium_network_policies(&self, namespace: &str) -> Api<DynamicObject> {
        Api::namespaced_with(
            self.client.clone(),
            namespace,
            &Self::cilium_network_policy_api_resource(),
        )
    }

    /// The [`ApiResource`] describing the `cilium.io/v2 CiliumNetworkPolicy`
    /// CRD, used to route dynamic requests for it (issue #541).
    fn cilium_network_policy_api_resource() -> ApiResource {
        ApiResource {
            group: CILIUM_NP_GROUP.into(),
            version: CILIUM_NP_VERSION.into(),
            api_version: CILIUM_NP_API_VERSION.into(),
            kind: CILIUM_NP_KIND.into(),
            plural: CILIUM_NP_PLURAL.into(),
        }
    }

    /// Deterministic name of the per-sandbox env Secret (issue #494).
    ///
    /// Customer `spec.env` values are stored in this Secret and referenced
    /// from the Pod via `valueFrom.secretKeyRef`, so the plaintext value
    /// never appears in the Pod object (and thus never in
    /// `kubectl describe pod` / `get -o yaml`). The name is derived purely
    /// from the Sandbox name so the Pod's `secretKeyRef` and the Secret the
    /// controller creates can never drift.
    fn env_secret_name(sandbox_name: &str) -> String {
        format!("sandbox-{sandbox_name}-env")
    }

    /// Build the per-sandbox env Secret for a Sandbox (issue #494).
    ///
    /// Returns `None` when there is nothing to store so the controller never
    /// leaves a dangling empty Secret behind. The Secret carries the customer
    /// env in `string_data` and owner-references the Sandbox CR (when
    /// `metadata.uid` is set) so kube's garbage collector tears it down with
    /// the sandbox — mirroring the Pod's owner reference.
    ///
    /// `OTEL_RESOURCE_ATTRIBUTES` is excluded from the Secret: `pod_spec_for`
    /// folds a customer-pinned value into an *inline* merged var (#435) rather
    /// than referencing it via `secretKeyRef`, so storing it here would leave
    /// an unreferenced key. Excluding it keeps the Secret holding exactly the
    /// keys the Pod consumes via `secretKeyRef`.
    ///
    /// Kept as an associated function so unit tests can assert on the
    /// generated Secret without a kube client.
    pub fn env_secret_for(sandbox: &Sandbox) -> Option<Secret> {
        // `into_iter().collect()` (vs. cloning the whole map) lets us drop the
        // inline-consumed `OTEL_RESOURCE_ATTRIBUTES` key in one pass.
        let string_data: BTreeMap<String, String> = sandbox
            .spec
            .env
            .iter()
            .filter(|(k, _)| k.as_str() != OTEL_RESOURCE_ATTRIBUTES_ENV)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if string_data.is_empty() {
            return None;
        }

        let name = sandbox
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "sandbox".into());
        let namespace = sandbox
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());

        // Same owner reference shape the Pod carries: when the Sandbox has a
        // uid, kube garbage-collects the Secret with the CR.
        let owner_references = sandbox_owner_references(sandbox, &name);

        Some(Secret {
            metadata: ObjectMeta {
                name: Some(Self::env_secret_name(&name)),
                namespace: Some(namespace),
                owner_references,
                ..Default::default()
            },
            // `string_data` takes plain UTF-8 (the apiserver base64-encodes
            // it into `data`), matching the `BTreeMap<String, String>` shape
            // of `spec.env` without a manual encode step.
            string_data: Some(string_data),
            ..Default::default()
        })
    }

    /// Deterministic name of the per-sandbox egress NetworkPolicy (issue
    /// #443).
    ///
    /// Derived purely from the Sandbox name so the policy the controller
    /// creates in [`Self::create_pod`] and the one kube garbage-collects via
    /// the owner reference can never drift. Distinct suffix from
    /// [`Self::env_secret_name`] (`-env`) so the two per-sandbox objects never
    /// collide.
    fn network_policy_name(sandbox_name: &str) -> String {
        format!("sandbox-{sandbox_name}-egress")
    }

    /// Deterministic name of the per-sandbox FQDN `CiliumNetworkPolicy` (issue
    /// #541).
    ///
    /// Derived purely from the Sandbox name, with an `-fqdn` suffix distinct
    /// from the baseline policy's `-egress` ([`Self::network_policy_name`]) and
    /// the env Secret's `-env`, so the per-sandbox children never collide and
    /// the object the controller creates matches the one kube garbage-collects
    /// via the owner reference.
    fn cilium_network_policy_name(sandbox_name: &str) -> String {
        format!("sandbox-{sandbox_name}-fqdn")
    }

    /// Build the default-deny egress `NetworkPolicy` for a Sandbox (issue
    /// #443).
    ///
    /// MVP spec §9 mandates that every sandbox Pod start from a **deny-all
    /// egress** posture — the trust boundary for running arbitrary tenant
    /// code — with `spec.networkPolicy.egressAllow` opening specific
    /// destinations. The CRD surface existed but nothing materialized the
    /// policy; this builder closes that gap.
    ///
    /// The returned `networking.v1.NetworkPolicy`:
    ///
    /// * selects exactly this sandbox's Pod via the per-sandbox
    ///   [`SANDBOX_NAME_LABEL`] label (not the shared
    ///   [`SANDBOX_POD_LABEL_KEY`], which would scope the policy to *every*
    ///   sandbox in the namespace),
    /// * sets `policyTypes: [Egress]` so it governs egress only (ingress is
    ///   out of scope — the spec calls only for egress), and
    /// * owner-references the Sandbox CR (when `metadata.uid` is set) so kube
    ///   garbage-collects it with the sandbox, mirroring the Pod and env
    ///   Secret.
    ///
    /// # Egress rules
    ///
    /// Independent of `egressAllow`, whenever OTel export is enabled the policy
    /// always opens the **infra egress envd itself requires** (issue #536): DNS
    /// (to resolve the collector service) and the OTel collector endpoint.
    /// Without this carve-out the default-deny policy blackholes envd's own
    /// telemetry, and a blocked exporter starves envd's tokio runtime across a
    /// resume. The collector destination is derived from the *same* endpoint
    /// resolution [`forwarded_observability_env`] injects into the Pod, so the
    /// policy and the exporter target cannot drift. When `OTEL_SDK_DISABLED` is
    /// `true` (or no endpoint is configured) no infra rule is added. `lookup`
    /// resolves env-var names (production passes `|n| std::env::var(n).ok()`),
    /// keeping the function pure and unit-testable.
    ///
    /// On top of that baseline:
    ///
    /// * **Empty `egressAllow` (or no `networkPolicy`)** → no *user* rules; with
    ///   OTel disabled this leaves `egress: None`, so `policyTypes: [Egress]`
    ///   with no rules denies *all* egress (the strict default-deny baseline).
    /// * **`egressAllow` with IP/CIDR entries** → one `ipBlock` egress rule
    ///   per entry (a bare IP is widened to a host CIDR), opening all ports to
    ///   that destination. This is the exact, enforceable case under a
    ///   standard NetworkPolicy.
    /// * **`egressAllow` with DNS-*name* entries** → a standard
    ///   `networking.v1` NetworkPolicy egress rule has no FQDN field, so a
    ///   name cannot be encoded as a destination here. When any entry is a
    ///   name, the policy instead opens **DNS egress** (UDP+TCP
    ///   [`DNS_PORT`]) so the name can be resolved; enforcing the *resolved*
    ///   L3 destination is delegated to the cluster's name-aware CNI layer
    ///   (Cilium FQDN policy). Encoding the FQDN destination itself — a
    ///   `CiliumNetworkPolicy` `toFQDNs` rule, or reconcile-time
    ///   name→CIDR resolution — is deliberate follow-up, not part of this
    ///   baseline.
    ///
    /// # CNI dependency
    ///
    /// A NetworkPolicy is inert unless the CNI enforces it. The deployment
    /// targets Cilium, which does; on a CNI without NetworkPolicy support the
    /// policy is silently ignored and egress stays open.
    ///
    /// Kept as an associated function so unit tests can assert on the
    /// generated policy without a kube client.
    pub fn network_policy_for<F>(sandbox: &Sandbox, lookup: F) -> K8sNetworkPolicy
    where
        F: Fn(&str) -> Option<String>,
    {
        let name = sandbox
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "sandbox".into());
        let namespace = sandbox
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());

        // Same owner reference shape the Pod and env Secret carry: when the
        // Sandbox has a uid, kube garbage-collects the policy with the CR.
        let owner_references = sandbox_owner_references(sandbox, &name);

        // Target exactly this sandbox's Pod via its unique identity label.
        let mut match_labels = BTreeMap::new();
        match_labels.insert(SANDBOX_NAME_LABEL.into(), name.clone());

        // `egressAllow` is `None` (no `networkPolicy` block at all) or a list.
        let egress_allow = sandbox
            .spec
            .network_policy
            .as_ref()
            .map(|np| np.egress_allow.as_slice())
            .unwrap_or(&[]);

        // Infra egress envd itself requires (issue #536), independent of
        // `egressAllow`: resolve the collector destination from the *same*
        // endpoint `forwarded_observability_env` injects into the Pod so the
        // policy and the exporter can't drift. Skipped when the OTel kill-switch
        // is set, no endpoint is configured, or the endpoint isn't an in-cluster
        // service (an external collector needs an explicit CIDR/FQDN rule).
        let otel_enabled = lookup("OTEL_SDK_DISABLED").as_deref() != Some("true");
        let collector_target = otel_enabled
            .then(|| {
                lookup(SANDBOX_OTEL_ENDPOINT_OVERRIDE_ENV)
                    .or_else(|| lookup("OTEL_EXPORTER_OTLP_ENDPOINT"))
            })
            .flatten()
            .as_deref()
            .and_then(cluster_collector_target);

        let mut rules: Vec<NetworkPolicyEgressRule> = Vec::new();
        // DNS is opened when either a user DNS-*name* entry or the infra
        // collector name needs resolution.
        let mut needs_dns = false;

        // User-declared destinations (issue #443).
        for entry in egress_allow {
            match egress_entry_as_cidr(entry) {
                // IP/CIDR literal: an exact L3 destination this policy can
                // enforce directly (all ports to that block).
                Some(cidr) => rules.push(NetworkPolicyEgressRule {
                    to: Some(vec![NetworkPolicyPeer {
                        ip_block: Some(IPBlock { cidr, except: None }),
                        ..Default::default()
                    }]),
                    ports: None,
                }),
                // DNS name: not encodable as a destination in a standard
                // NetworkPolicy. Record that DNS must be opened so the name
                // resolves; L3 enforcement is the CNI's name-aware layer.
                None => needs_dns = true,
            }
        }

        // Infra collector rule + the DNS it depends on (issue #536).
        if let Some((collector_ns, collector_port)) = collector_target {
            rules.push(collector_egress_rule(collector_ns, collector_port));
            needs_dns = true;
        }
        if needs_dns {
            rules.push(dns_egress_rule());
        }

        // No rules at all ⇒ leave `egress` unset so `policyTypes: [Egress]`
        // denies everything (the strict default-deny baseline).
        let egress = (!rules.is_empty()).then_some(rules);

        K8sNetworkPolicy {
            metadata: ObjectMeta {
                name: Some(Self::network_policy_name(&name)),
                namespace: Some(namespace),
                owner_references,
                ..Default::default()
            },
            spec: Some(NetworkPolicySpec {
                pod_selector: LabelSelector {
                    match_labels: Some(match_labels),
                    match_expressions: None,
                },
                policy_types: Some(vec!["Egress".into()]),
                egress,
                ingress: None,
            }),
        }
    }

    /// Build the Sandbox-owned `PersistentVolumeClaim`s for a Sandbox's
    /// `volumeClaimTemplates` (#603 Phase 2, issue #605).
    ///
    /// One PVC per declared template. Each is named [`pvc_name`]
    /// (`{template}-{sandbox}`, mirroring StatefulSet / `agent-sandbox`
    /// semantics) and owner-references the Sandbox CR via
    /// [`sandbox_owner_references`] (`controller: true`, `block_owner_deletion:
    /// true`) so kube's garbage collector cascade-deletes the PVC — and its
    /// data — when the Sandbox is deleted. An empty / absent
    /// `volume_claim_templates` yields no PVCs (the pre-#603 behavior).
    ///
    /// Kept as an associated function so unit tests can assert on the generated
    /// PVCs (name, ownerRef, spec) without a kube client, mirroring
    /// [`Self::network_policy_for`].
    pub fn pvcs_for(sandbox: &Sandbox) -> Vec<PersistentVolumeClaim> {
        sandbox
            .spec
            .volume_claim_templates
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|template| Self::pvc_for(sandbox, template))
            .collect()
    }

    /// Build a single Sandbox-owned `PersistentVolumeClaim` from one
    /// [`VolumeClaimTemplate`] (#603 Phase 2, issue #605). See
    /// [`Self::pvcs_for`] for the naming / ownership contract.
    pub fn pvc_for(sandbox: &Sandbox, template: &VolumeClaimTemplate) -> PersistentVolumeClaim {
        let sandbox_name = sandbox
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "sandbox".into());
        let namespace = sandbox
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());

        let name = pvc_name(&template.metadata.name, &sandbox_name);

        // Issue #416, filesystem clone-from-snapshot: when this sandbox carries
        // `spec.restore_from` naming a *source sandbox*, CSI-clone each PVC from
        // the source's correspondingly-named retained PVC via `spec.dataSource`.
        // The clone takes effect only at first creation — on a later resume the
        // PVC already exists, so `ensure_pvcs`' `409`-idempotent path adopts it
        // and the (immutable) `dataSource` is never re-applied. An `s3://`
        // restore source is snapshot-mode and provisions no PVCs, so it never
        // reaches a clone here.
        let data_source =
            sandbox
                .spec
                .restore_from
                .as_deref()
                .and_then(|rf| match RestoreSource::parse(rf) {
                    RestoreSource::SandboxId(source) => Some(TypedLocalObjectReference {
                        api_group: None,
                        kind: "PersistentVolumeClaim".into(),
                        name: pvc_name(&template.metadata.name, source),
                    }),
                    RestoreSource::SnapshotUrl(_) => None,
                });

        // Same owner reference shape the Pod / env Secret / NetworkPolicy carry:
        // when the Sandbox has a uid, kube garbage-collects the PVC with the CR.
        let owner_references = sandbox_owner_references(sandbox, &sandbox_name);

        // Stamp the per-sandbox identity label (for traceability / selection)
        // alongside any user-declared labels; user labels win on a key clash.
        let mut labels = BTreeMap::new();
        labels.insert(SANDBOX_NAME_LABEL.into(), sandbox_name);
        labels.extend(
            template
                .metadata
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );

        let annotations = (!template.metadata.annotations.is_empty())
            .then(|| template.metadata.annotations.clone());

        // The CRD's `VolumeClaimSpec` is a deliberate subset of the upstream
        // PVC spec (#603 Phase 1); translate it into the `k8s_openapi` type.
        // `storage` is required and maps to `resources.requests.storage`; empty
        // `access_modes` / `None` storage-class defer to the cluster default.
        let mut requests = BTreeMap::new();
        requests.insert(
            "storage".to_string(),
            Quantity(template.spec.storage.to_string()),
        );
        let access_modes =
            (!template.spec.access_modes.is_empty()).then(|| template.spec.access_modes.clone());

        PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(namespace),
                labels: Some(labels),
                annotations,
                owner_references,
                ..Default::default()
            },
            spec: Some(PersistentVolumeClaimSpec {
                access_modes,
                resources: Some(VolumeResourceRequirements {
                    requests: Some(requests),
                    ..Default::default()
                }),
                storage_class_name: template.spec.storage_class_name.clone(),
                volume_mode: template.spec.volume_mode.clone(),
                data_source,
                ..Default::default()
            }),
            status: None,
        }
    }

    /// Build the FQDN-egress `CiliumNetworkPolicy` for a Sandbox (issue #541).
    ///
    /// A standard `networking.v1.NetworkPolicy` has no FQDN field, so
    /// [`Self::network_policy_for`] can only open DNS (port 53) for a DNS-*name*
    /// `egressAllow` entry — it cannot authorize the *resolved* L3 connection.
    /// This builder closes that gap on a name-aware CNI (Cilium) by emitting a
    /// `toFQDNs` policy **alongside** the baseline NetworkPolicy: Cilium merges
    /// the two additively, so the name's resolved address is permitted while the
    /// default-deny posture holds for everything else.
    ///
    /// Returns `None` (no Cilium policy, today's DNS-port-only behavior) when:
    ///
    /// * [`FQDN_EGRESS_ENABLED_ENV`] is not `"true"` — the cluster has not
    ///   opted in (e.g. a non-Cilium CNI, where the CRD is absent), so the path
    ///   degrades gracefully with no regression; or
    /// * no `egressAllow` entry is a DNS name — every entry is an IP/CIDR the
    ///   baseline NetworkPolicy already enforces as an `ipBlock`, so a Cilium
    ///   policy would add nothing.
    ///
    /// # Policy shape
    ///
    /// The returned object selects exactly this sandbox's Pod (via the
    /// per-sandbox [`SANDBOX_NAME_LABEL`]) and carries two egress rules:
    ///
    /// 1. **DNS visibility** — routes the sandbox's DNS to the cluster resolver
    ///    through Cilium's DNS proxy (`toPorts` with `rules.dns: matchPattern
    ///    "*"`). This is mandatory: `toFQDNs` only learns a name→IP mapping from
    ///    DNS answers the proxy observes, so without this rule the allowance
    ///    never populates. The resolver's namespace and pod label are read from
    ///    [`FQDN_DNS_NAMESPACE_ENV`] / [`FQDN_DNS_POD_LABEL_ENV`] (defaulting to
    ///    the stock CoreDNS placement) so non-standard clusters configure it
    ///    rather than needing a rebuild.
    /// 2. **`toFQDNs`** — one `matchName` per DNS-name `egressAllow` entry,
    ///    permitting all ports to each resolved host (mirroring the baseline
    ///    policy's all-ports `ipBlock` semantics for CIDR entries).
    ///
    /// `lookup` resolves env-var names (production passes
    /// `|n| std::env::var(n).ok()`), keeping the function pure and unit-testable
    /// without a kube client. Returns a [`DynamicObject`] because
    /// `CiliumNetworkPolicy` is a Cilium-owned CRD with no `k8s-openapi` type.
    pub fn cilium_network_policy_for<F>(sandbox: &Sandbox, lookup: F) -> Option<DynamicObject>
    where
        F: Fn(&str) -> Option<String>,
    {
        // Gate: only on a cluster that has explicitly opted into FQDN policy.
        if lookup(FQDN_EGRESS_ENABLED_ENV).as_deref() != Some("true") {
            return None;
        }

        // Collect the DNS-*name* entries: those `egress_entry_as_cidr` rejects
        // (an IP/CIDR is already enforced by the baseline NetworkPolicy, so it
        // needs no `toFQDNs` rule). Trimmed to match the baseline's parsing.
        let fqdns: Vec<String> = sandbox
            .spec
            .network_policy
            .as_ref()
            .map(|np| np.egress_allow.as_slice())
            .unwrap_or(&[])
            .iter()
            .filter(|entry| egress_entry_as_cidr(entry).is_none())
            .map(|entry| entry.trim().to_owned())
            .filter(|entry| !entry.is_empty())
            .collect();
        if fqdns.is_empty() {
            return None;
        }

        let name = sandbox
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "sandbox".into());
        let namespace = sandbox
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());
        let owner_references = sandbox_owner_references(sandbox, &name);

        // Cluster DNS resolver location for the DNS-visibility rule. Both
        // overrides fall back to the CoreDNS default rather than emitting a
        // broken selector: an empty namespace (`dnsNamespace: ""`) would never
        // match the resolver, and a malformed label (no `=`) can't be split.
        let dns_namespace = lookup(FQDN_DNS_NAMESPACE_ENV)
            .filter(|ns| !ns.is_empty())
            .unwrap_or_else(|| DEFAULT_FQDN_DNS_NAMESPACE.to_owned());
        let dns_label = lookup(FQDN_DNS_POD_LABEL_ENV)
            .filter(|label| label.contains('='))
            .unwrap_or_else(|| DEFAULT_FQDN_DNS_POD_LABEL.to_owned());
        let (dns_label_key, dns_label_value) = dns_label
            .split_once('=')
            .expect("dns_label is guaranteed to contain '=' by the filter/default above");

        // Cilium selectors take string-keyed label maps; build them explicitly
        // because the keys (the resolver label key, the identity label) are
        // dynamic and `serde_json::json!` only accepts literal object keys.
        let mut endpoint_match = serde_json::Map::new();
        endpoint_match.insert(SANDBOX_NAME_LABEL.to_owned(), name.clone().into());
        let mut dns_match = serde_json::Map::new();
        dns_match.insert(
            "io.kubernetes.pod.namespace".to_owned(),
            dns_namespace.into(),
        );
        dns_match.insert(dns_label_key.to_owned(), dns_label_value.to_owned().into());

        let to_fqdns: Vec<serde_json::Value> = fqdns
            .into_iter()
            .map(|host| serde_json::json!({ "matchName": host }))
            .collect();

        let data = serde_json::json!({
            "spec": {
                "endpointSelector": { "matchLabels": serde_json::Value::Object(endpoint_match) },
                "egress": [
                    {
                        "toEndpoints": [
                            { "matchLabels": serde_json::Value::Object(dns_match) }
                        ],
                        "toPorts": [{
                            "ports": [{ "port": DNS_PORT.to_string(), "protocol": "ANY" }],
                            "rules": { "dns": [{ "matchPattern": "*" }] }
                        }]
                    },
                    { "toFQDNs": to_fqdns }
                ]
            }
        });

        Some(DynamicObject {
            types: Some(TypeMeta {
                api_version: CILIUM_NP_API_VERSION.to_owned(),
                kind: CILIUM_NP_KIND.to_owned(),
            }),
            metadata: ObjectMeta {
                name: Some(Self::cilium_network_policy_name(&name)),
                namespace: Some(namespace),
                owner_references,
                ..Default::default()
            },
            data,
        })
    }

    /// Create the per-sandbox FQDN `CiliumNetworkPolicy`, tolerating the two
    /// non-fatal apiserver responses (issue #541).
    ///
    /// Extracted from [`Self::create_pod`] so its error-handling branches are
    /// integration-testable against a mock kube server without mutating the
    /// process environment (the gate is an env var read in `create_pod`).
    ///
    /// * `Ok` — created, or one of:
    ///   * `409 AlreadyExists` — a racing reconcile already created it
    ///     (idempotent success, matching the baseline NetworkPolicy path), or
    ///   * `404 NotFound` — the apiserver doesn't know the `CiliumNetworkPolicy`
    ///     resource type (flag set but Cilium/CRD absent). Degrades to the
    ///     baseline DNS-port-only path rather than bricking sandbox creation.
    /// * [`RuntimeError::Transient`] — any other apiserver error, so the
    ///   reconcile loop requeues.
    pub async fn create_or_ack_cilium_network_policy(
        &self,
        namespace: &str,
        policy: &DynamicObject,
    ) -> Result<(), RuntimeError> {
        let policy_name = policy.metadata.name.as_deref().unwrap_or_default();
        match self
            .cilium_network_policies(namespace)
            .create(&PostParams::default(), policy)
            .await
        {
            Ok(_) => Ok(()),
            // Racing reconcile already created it — idempotent success.
            Err(kube::Error::Api(api)) if api.code == 409 => {
                tracing::debug!(
                    policy = %policy_name,
                    namespace = %namespace,
                    "FQDN CiliumNetworkPolicy already exists; treating create as idempotent"
                );
                Ok(())
            }
            // CRD absent (flag set but Cilium/CRD missing): degrade to the
            // baseline DNS-port-only path rather than bricking sandbox creation.
            // A 404 here is the apiserver reporting the `CiliumNetworkPolicy`
            // resource type is unknown.
            Err(kube::Error::Api(api)) if api.code == 404 => {
                tracing::warn!(
                    policy = %policy_name,
                    namespace = %namespace,
                    "FQDN egress enabled but CiliumNetworkPolicy CRD absent; \
                     skipping — DNS-name egress falls back to DNS-port-only"
                );
                Ok(())
            }
            Err(e) => Err(RuntimeError::Transient(format!(
                "create FQDN network policy: {e}"
            ))),
        }
    }

    /// Create-or-update the per-sandbox FQDN `CiliumNetworkPolicy` so its
    /// `toFQDNs` rules match `policy` (issue #542).
    ///
    /// The companion to [`Self::create_or_ack_cilium_network_policy`] for the
    /// *update* path: a warm-pool claim that unions a per-create DNS-name
    /// `egressAllow` over the member's template baseline must widen an
    /// already-materialized Cilium policy, which the create-or-409 path cannot
    /// do. A JSON merge patch on `spec` replaces the policy's egress rules with
    /// the desired set; a `404` on the patch is ambiguous (the policy object
    /// doesn't exist yet *or* the `CiliumNetworkPolicy` CRD is absent), so it
    /// delegates to [`Self::create_or_ack_cilium_network_policy`], which
    /// distinguishes a fresh create from a missing-CRD graceful-degrade.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Transient`] for any non-404 apiserver error so the
    /// reconcile loop requeues.
    pub async fn ensure_cilium_network_policy(
        &self,
        namespace: &str,
        policy: &DynamicObject,
    ) -> Result<(), RuntimeError> {
        let policy_name = policy.metadata.name.as_deref().unwrap_or_default();
        // Patch only the spec so the apiserver-managed metadata (uid,
        // resourceVersion, ownerReferences set at create) is untouched.
        let spec = policy
            .data
            .get("spec")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let spec_patch = serde_json::json!({ "spec": spec });
        match self
            .cilium_network_policies(namespace)
            .patch(
                policy_name,
                &PatchParams::default(),
                &Patch::Merge(&spec_patch),
            )
            .await
        {
            Ok(_) => Ok(()),
            // 404 = the policy object isn't there yet, or the CRD is absent.
            // The create-or-ack path handles both: it creates when the object
            // is merely missing, and warns-and-degrades when the CRD is absent.
            Err(kube::Error::Api(api)) if api.code == 404 => {
                self.create_or_ack_cilium_network_policy(namespace, policy)
                    .await
            }
            Err(e) => Err(RuntimeError::Transient(format!(
                "patch FQDN network policy {policy_name}: {e}"
            ))),
        }
    }

    /// Reconcile the sandbox's live egress policy to its current
    /// `spec.networkPolicy` (issue #542) — the implementation behind the
    /// [`RuntimeAdapter::ensure_egress_policy`] trait method.
    ///
    /// `lookup` resolves env-var names (production passes
    /// `|n| std::env::var(n).ok()` via the trait wrapper); taking it explicitly
    /// — like [`Self::network_policy_for`] and [`Self::cilium_network_policy_for`]
    /// — lets tests drive the FQDN companion path without mutating the shared
    /// process environment.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Transient`] for retriable apiserver failures so the sync
    /// is retried on the next requeue.
    pub async fn reconcile_egress_policy<F>(
        &self,
        sandbox: &Sandbox,
        lookup: F,
    ) -> Result<(), RuntimeError>
    where
        // `Copy` so the same `lookup` can be handed to both `network_policy_for`
        // and `cilium_network_policy_for` (each takes it by value). The two
        // production/test closures capture nothing, so they are `Copy`.
        F: Fn(&str) -> Option<String> + Copy,
    {
        let name = sandbox
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "sandbox".into());
        let namespace = sandbox
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());

        // Desired baseline policy from the *current* spec — the union the warm
        // claim stamped. Built by the same function `create_pod` uses, so the
        // shape (podSelector, policyTypes, infra OTel/DNS egress rules) is
        // identical to a cold-booted sandbox carrying this spec; only the
        // user-destination rules differ.
        let desired = Self::network_policy_for(sandbox, lookup);
        let policy_name = Self::network_policy_name(&name);
        let api = self.network_policies(&namespace);

        // Compare-then-patch: read the live policy and write only when its
        // egress rules differ. A quiescent Running reconcile (the common case,
        // since the policy was materialized at `create_pod` and a claim is a
        // one-shot widening) then costs at most one GET and never churns the
        // object's resourceVersion.
        match api.get_opt(&policy_name).await.map_err(|e| {
            RuntimeError::Transient(format!("get egress network policy {policy_name}: {e}"))
        })? {
            Some(live) => {
                let desired_egress = desired.spec.as_ref().and_then(|s| s.egress.as_ref());
                let live_egress = live.spec.as_ref().and_then(|s| s.egress.as_ref());
                if desired_egress != live_egress {
                    // JSON merge patch (RFC 7396): the `egress` array is
                    // replaced wholesale with the desired rules — the
                    // set-to-desired semantics we want. A `null` (no rules)
                    // restores the strict default-deny baseline. podSelector /
                    // policyTypes / ownerReferences are untouched.
                    let patch = serde_json::json!({
                        "spec": { "egress": desired.spec.as_ref().and_then(|s| s.egress.clone()) }
                    });
                    api.patch(&policy_name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await
                        .map_err(|e| {
                            RuntimeError::Transient(format!(
                                "patch egress network policy {policy_name}: {e}"
                            ))
                        })?;
                    tracing::info!(
                        sandbox = %name,
                        namespace = %namespace,
                        policy = %policy_name,
                        "reconciled egress NetworkPolicy to spec (warm-claim per-create \
                         egress override, #542)"
                    );
                }
            }
            // Missing policy — unexpected for a Running sandbox (`create_pod`
            // materializes it before the Pod), but self-heal by creating it.
            // Tolerate a racing create the same way `create_pod` does.
            None => {
                if let Err(e) = api.create(&PostParams::default(), &desired).await {
                    match e {
                        kube::Error::Api(api_err) if api_err.code == 409 => {}
                        other => {
                            return Err(RuntimeError::Transient(format!(
                                "create egress network policy {policy_name}: {other}"
                            )));
                        }
                    }
                }
                tracing::info!(
                    sandbox = %name,
                    namespace = %namespace,
                    policy = %policy_name,
                    "materialized missing egress NetworkPolicy during reconcile (#542)"
                );
            }
        }

        // FQDN companion (issue #541): when the union now names a DNS host and
        // the cluster opted into FQDN egress, widen the Cilium `toFQDNs` policy
        // too so DNS-name per-create destinations get the same enforcement a
        // cold boot would give them. `None` ⇒ feature off or no name ⇒ nothing
        // to do (the baseline policy alone governs egress).
        if let Some(fqdn_policy) = Self::cilium_network_policy_for(sandbox, lookup) {
            self.ensure_cilium_network_policy(&namespace, &fqdn_policy)
                .await?;
        }
        Ok(())
    }

    /// Build the Pod object for a Sandbox.
    ///
    /// Kept as an associated function so unit tests can assert on the
    /// generated spec without a kube client.
    ///
    /// The Pod owner-references the Sandbox CR when `metadata.uid` is
    /// set, so kube's garbage collector tears the Pod down when the
    /// Sandbox is deleted. On a freshly-constructed Sandbox (no `uid`
    /// yet, e.g. in unit tests) the owner-reference is omitted.
    ///
    /// # Arguments
    ///
    /// * `sandbox` — the Sandbox CR driving the Pod's name, namespace,
    ///   labels, owner-reference, env, and resource requests.
    /// * `image` — the workload container image (the customer's rootfs,
    ///   `SandboxTemplate.spec.image`). The static envd binary is staged
    ///   onto the node by the `sandbawks-node` DaemonSet and mounted
    ///   read-only at [`ENVD_BIN_DIR`]; the container's entrypoint is the
    ///   staged binary, so envd execs inside the workload's filesystem
    ///   without the image bundling it.
    /// * `restore` — `Some` for the shim-restore path (epic #484). The
    ///   Pod's runtime class becomes [`DEFAULT_RESTORE_RUNTIME_CLASS`]
    ///   when the Sandbox leaves it unset or pins the plain
    ///   [`GVISOR_RUNTIME_CLASS`] (whose handler has no restore shim);
    ///   any other explicit pin is honored. The Pod is left **unpinned**
    ///   (no `spec.nodeName`) so the scheduler places it, and carries the
    ///   deterministic staging path in the [`ANNOTATION_RESTORE_IMAGE_PATH`]
    ///   annotation. The wait for the node to finish staging lives in the
    ///   restore shim (it blocks the root container's `runsc restore` on the
    ///   `.staged` marker), NOT in a Pod init container — gVisor whole-sandbox
    ///   restore rejects any container absent from the checkpoint. `None`
    ///   materializes an ordinary cold-start Pod. Both shapes are therefore
    ///   topologically identical (no init container).
    pub fn pod_spec_for(sandbox: &Sandbox, image: &str, restore: Option<&RestorePlacement>) -> Pod {
        let name = sandbox
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "sandbox".into());
        let namespace = sandbox
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());

        let mut labels = BTreeMap::new();
        labels.insert(SANDBOX_POD_LABEL_KEY.into(), SANDBOX_POD_LABEL_VALUE.into());
        // Per-sandbox identity label the egress NetworkPolicy's podSelector
        // targets (issue #443). Built from the same constant the policy uses.
        labels.insert(SANDBOX_NAME_LABEL.into(), name.clone());

        // Every sandbox Pod opts out of descheduler-initiated eviction
        // (#498): a warm/idle or actively-claimed sandbox registers as
        // low-utilization and was being evicted by `LowNodeUtilization`,
        // which destroys the live session. The annotation is a hard skip
        // under the cluster's `noEvictionPolicy: Mandatory` and leaves
        // node-drain / node-failure eviction untouched. See
        // [`ANNOTATION_PREFER_NO_EVICTION`]. Applied to cold-start and
        // restore Pods alike — the protection is keyed to the Pod
        // existing, and a paused sandbox has no Pod.
        let mut annotations = BTreeMap::new();
        annotations.insert(
            ANNOTATION_PREFER_NO_EVICTION.into(),
            ANNOTATION_PREFER_NO_EVICTION_VALUE.into(),
        );
        // Plan 07 T9: a restore Pod additionally carries the staging path
        // in an annotation the containerd-shim reads to drive `runsc
        // restore` from local disk.
        if let Some(r) = restore {
            annotations.insert(ANNOTATION_RESTORE_IMAGE_PATH.into(), r.staging_path.clone());
        }
        let annotations = Some(annotations);

        let owner_references = sandbox_owner_references(sandbox, &name);

        let cpu = Quantity(sandbox.spec.resources.cpu.to_string());
        // Capture-in-checkpoint relocation (epic #466) is a gVisor-only
        // mechanism: it exists so `runsc checkpoint` captures envd's code pages
        // out of the root overlay. Other runtimes don't need it — Kata's Cloud
        // Hypervisor snapshots the whole guest RAM wherever envd execs (#594) —
        // so on a non-gVisor runtime the relocate would be a pointless self-copy
        // plus a wasted memory-budget bump. Gate the relocate env (below) and its
        // memory budget on gVisor, reusing the same runtime-class predicate the
        // restore mapping uses (`None` = the gVisor default, `Some("gvisor")` =
        // an explicit pin). (The relocate target itself is non-root-writable on
        // every runtime since #656 — see [`ENVD_RUN_DIR`] — so this gate is now
        // an optimization, no longer load-bearing against a cold-start crash.)
        let uses_gvisor_capture = matches!(
            sandbox.spec.runtime_class_name.as_deref(),
            None | Some(GVISOR_RUNTIME_CLASS)
        );
        // Budget the relocated envd copy (epic #466): under gVisor envd's copy in
        // the overlay-tmpfs counts against the container's memory cgroup, so the
        // request/limit carries the customer's memory *plus* the relocate budget,
        // leaving the workload its full headroom. Non-gVisor runtimes don't
        // relocate, so they carry the bare request.
        let memory = if uses_gvisor_capture {
            memory_with_relocate_budget(&sandbox.spec.resources.memory)
        } else {
            Quantity(sandbox.spec.resources.memory.to_string())
        };
        let mut requests = BTreeMap::new();
        requests.insert("cpu".into(), cpu.clone());
        requests.insert("memory".into(), memory.clone());
        let mut limits = BTreeMap::new();
        limits.insert("cpu".into(), cpu);
        limits.insert("memory".into(), memory);
        if let Some(eph) = &sandbox.spec.resources.ephemeral_storage {
            limits.insert("ephemeral-storage".into(), Quantity(eph.to_string()));
        }
        let resources = ResourceRequirements {
            requests: Some(requests),
            limits: Some(limits),
            claims: None,
        };

        // Plan 07 T9: the restore path injects **no** env markers — the
        // container is born restored by the shim, so envd never enters
        // the legacy idle-hold.
        //
        // Issue #494 (security): customer `spec.env` values are *not*
        // inlined into the Pod spec — doing so prints any secret a customer
        // passes (API keys, tokens) in cleartext under `kubectl describe
        // pod`. Instead each entry is emitted as a `valueFrom.secretKeyRef`
        // against the per-sandbox env Secret ([`Self::env_secret_name`],
        // server-side-created by `create_pod` before the Pod), so
        // `kubectl describe` renders `<set to the key '…' in secret '…'>`
        // and the literal value lives only in the Secret. This mirrors the
        // downward-API pattern already used for `POD_NAME_ENV` below
        // (`value: None` + a populated `value_from`). Applied uniformly to
        // cold-start and restore Pods so the two shapes stay topologically
        // identical for `runsc checkpoint`/restore.
        let env_secret_name = Self::env_secret_name(&name);
        let mut env: Vec<EnvVar> = sandbox
            .spec
            .env
            .keys()
            .map(|k| EnvVar {
                name: k.clone(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        // `name` is `String` (not `Option`) in the pinned
                        // k8s-openapi 0.23 / v1_31 used here.
                        name: env_secret_name.clone(),
                        key: k.clone(),
                        optional: None,
                    }),
                    ..Default::default()
                }),
            })
            .collect();

        // Forward the controller's own OpenTelemetry configuration so
        // envd exports to the same collector (or honors the same
        // `OTEL_SDK_DISABLED` kill-switch) as the rest of the stack,
        // instead of silently defaulting to the unreachable in-process
        // `http://localhost:4318`. Customer-supplied `spec.env` wins on
        // key collisions — we fill the gap but never clobber explicit
        // sandbox config. On a *restore* Pod these spec entries are inert
        // for the already-checkpointed process (it keeps the env it had at
        // checkpoint time), so this changes only the cold-start path; it's
        // applied uniformly to keep the two Pod shapes identical.
        let customer_keys: HashSet<String> = env.iter().map(|e| e.name.clone()).collect();
        env.extend(
            forwarded_observability_env(|name| std::env::var(name).ok())
                .into_iter()
                .filter(|e| !customer_keys.contains(&e.name)),
        );
        // Per-sandbox OTel identity (issue #435): inject OTEL_RESOURCE_ATTRIBUTES
        // built from the Sandbox CR (k8s semconv + sandbox.* custom keys) merged
        // with the base, and set OTEL_SERVICE_NAME so envd spans and inner Claude
        // Code agent spans land under a named service.
        //
        // OTEL_RESOURCE_ATTRIBUTES is *not* run through the customer_keys filter:
        // a customer who pins it must not be able to null out the sandbox's
        // identity attrs (#435). Instead the customer's pinned value is folded
        // into the merge as the base (identity appended after it), so the raw
        // spec.env entry is dropped here to avoid a duplicate env var; the merged
        // value re-adds it. OTEL_SERVICE_NAME stays customer-overridable via the
        // filter (a customer can still pin a different service name in spec.env).
        let customer_attrs = sandbox
            .spec
            .env
            .get(OTEL_RESOURCE_ATTRIBUTES_ENV)
            .map(String::as_str);
        env.retain(|e| e.name != OTEL_RESOURCE_ATTRIBUTES_ENV);
        env.extend(
            per_sandbox_observability_env(sandbox, customer_attrs, |name| std::env::var(name).ok())
                .into_iter()
                .filter(|e| {
                    e.name == OTEL_RESOURCE_ATTRIBUTES_ENV || !customer_keys.contains(&e.name)
                }),
        );
        // Stable `service.instance.id` from the downward API (issue #295);
        // see [`POD_NAME_ENV`]. Skipped if the customer pinned `POD_NAME`.
        if !customer_keys.contains(POD_NAME_ENV) {
            env.push(EnvVar {
                name: POD_NAME_ENV.into(),
                value: None,
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.name".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            });
        }

        // Capture-in-checkpoint relocate target (issue #468/#469 / epic #466):
        // tell envd to copy + re-exec itself onto [`ENVD_RELOCATE_TARGET`]
        // (`/var/tmp/envd/run/envd`). That path is a plain directory in the
        // *workload rootfs* — NOT a separate volume — so under the cluster's
        // `--overlay2=root:self` it lives in the sandbox-internal overlay-tmpfs
        // that `runsc checkpoint` walks into `pages.img`. The relocated envd's
        // code pages are therefore captured, and the snapshot survives a later
        // host-side envd upgrade (which would otherwise re-fault the
        // hostPath-bound seed). envd `mkdir -p`s the parent itself, exactly as it
        // already does for its `--state-file` under `/var/tmp/envd`.
        //
        // A `medium: Memory` emptyDir was tried (#468) but is wrong: kubelet
        // presents it to the sandbox as a *gofer-backed* `bind`, not a
        // sentry-internal tmpfs, so its pages are NOT in `pages.img` (it would
        // re-fault from the host, defeating the epic) and a writable one trips
        // the node's #237 pause-time mount-set gate. A rootfs path under the
        // root overlay is the only surface gVisor actually captures.
        //
        // Unlike the observability vars this is a load-bearing internal
        // contract, NOT customer-overridable: a `spec.env` pin of the same key
        // is dropped here (avoiding a duplicate env entry) so the controller's
        // value always wins. Set identically on cold-start and restore Pods; the
        // relocate step only runs on cold start (a restore Pod is born
        // restored), so on the restore path the var is inert — but the node's
        // pause path reads it as the capture-mode marker (#469).
        //
        // gVisor-only (`uses_gvisor_capture`, above): a non-gVisor runtime has no
        // overlay for `runsc checkpoint` to capture, so injecting the marker
        // would only buy a pointless self-copy + memory-budget bump. (Since #656
        // the target nests under sticky-`1777` `/var/tmp/envd`, so it no longer
        // crashes a non-root cold start — the gate is now an optimization.)
        if uses_gvisor_capture {
            env.retain(|e| e.name != ENVD_RELOCATE_TARGET_ENV);
            env.push(EnvVar {
                name: ENVD_RELOCATE_TARGET_ENV.into(),
                value: Some(ENVD_RELOCATE_TARGET.into()),
                value_from: None,
            });
        }

        // A restore Pod must run under a runtime class whose containerd
        // handler routes to a restore-capable shim. Unlike the old design
        // it is *not* pinned to a node — the scheduler places it (epic
        // #484), and the snapshot is staged onto whichever node it lands on.
        let runtime_class_name = match restore {
            Some(_) => match sandbox.spec.runtime_class_name.as_deref() {
                // Unset, or the vanilla gVisor class (whose handler has
                // no restore shim): both restore under `sandbawks-runsc`,
                // the gVisor-plus-sandbawks-shim class. Mapping `gvisor`
                // onto it is what makes a `gvisor` sandbox resumable at
                // all (issue #196) — without it the restore Pod would
                // route to the plain gVisor handler, the
                // `restore-image-path` annotation would be ignored, and
                // the sandbox would hang in `Resuming`.
                None | Some(GVISOR_RUNTIME_CLASS) => Some(DEFAULT_RESTORE_RUNTIME_CLASS.into()),
                // A non-gVisor pin (e.g. a future `kata-fc` restore
                // class) carries its own restore shim — honor it as-is.
                Some(other) => Some(other.to_owned()),
            },
            None => sandbox.spec.runtime_class_name.clone(),
        };

        // envd delivery (issue #265): rather than forcing customers to
        // bake envd into their images, the `sandbawks-node` DaemonSet
        // stages the static envd binary at `ENVD_STAGE_HOSTPATH` on every
        // node, and the workload container (`image` — the customer's
        // rootfs) mounts it **read-only** and runs it as its entrypoint.
        // envd execs inside the customer's filesystem, so processes it
        // spawns see the template's tooling (git, node, …) and inherit
        // `sandbox.spec.env`.
        //
        // Why a read-only hostPath seed rather than an init container + shared
        // volume: it keeps cold-start and restore Pods topologically identical
        // and avoids decoding a sub-container that was never checkpointed. The
        // *seed* binary stays out of `pages.img` (host bind), a read-only mount
        // is exempt from the node's #237 pause-time mount-set gate, and on
        // restore gVisor reconnects the same node-provided file. envd writes its
        // `--state-file` to the default `/var/tmp/envd/state.json`, which it
        // creates on the container's overlay rootfs (captured in the checkpoint)
        // — so no writable mount is needed for state either. The path nests
        // under `/var/tmp` (sticky `1777`) rather than `/var/lib/envd`
        // (root-owned) because the container runs envd as the image's `USER`,
        // which is routinely non-root; a root-owned parent made the shutdown
        // snapshot flush fail with `EACCES` and crashed warm-pool Pods on
        // auto-pause.
        let bin_mount = VolumeMount {
            name: ENVD_BIN_VOLUME.into(),
            mount_path: ENVD_BIN_DIR.into(),
            read_only: Some(true),
            ..Default::default()
        };
        // The read-only envd seed mount always comes first; the PVC-backed
        // mounts (#603 Phase 2, #605) follow. `volume_mounts` below is built
        // from these two together once `pvc_mounts` is known.
        // No volume for the relocate target (`/var/tmp/envd/run`, epic #466): it is
        // a plain rootfs directory under the captured root overlay (see the
        // relocate-env comment above for why a `medium: Memory` emptyDir was
        // wrong). The only Pod volume is the read-only hostPath envd seed, which
        // is exempt from the node's #237 mount-set gate.
        let mut volumes = vec![Volume {
            name: ENVD_BIN_VOLUME.into(),
            host_path: Some(HostPathVolumeSource {
                path: ENVD_STAGE_HOSTPATH.into(),
                // The DaemonSet creates the dir; tolerate it not yet
                // existing so the kubelet doesn't fail the mount.
                type_: Some("DirectoryOrCreate".into()),
            }),
            ..Default::default()
        }];

        // #603 Phase 2 (#605): back the declared `volumeClaimTemplates` paths
        // with the Sandbox-owned PVCs `create_pod` provisions, so those paths
        // survive a `filesystem`-mode pause (Pod deleted, PVC kept). The Pod
        // volume name is the template's `metadata.name` (StatefulSet contract);
        // it points at the materialized `{template}-{sandbox}` PVC. The
        // container mount is added to `volume_mounts` below at the same name.
        // `None` ⇒ no PVCs, so the root FS stays fully ephemeral exactly as
        // before. Built here (not gated on `persistence_mode`) so a PVC's data
        // is durable for a *running* sandbox regardless of mode — Phase 2 only
        // makes paths durable; the pause/resume branch is Phase 3 (#606).
        let pvc_mounts: Vec<VolumeMount> = sandbox
            .spec
            .volume_claim_templates
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|template| {
                let volume_name = template.metadata.name.clone();
                // StatefulSet semantics: a volumeClaimTemplate volume takes
                // priority over a same-named volume. There is no user-supplied
                // volume surface today, but dedupe defensively so a future one
                // (or a duplicate template name) can't double-declare the name.
                volumes.retain(|v| v.name != volume_name);
                volumes.push(Volume {
                    name: volume_name.clone(),
                    persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                        claim_name: pvc_name(&template.metadata.name, &name),
                        read_only: None,
                    }),
                    ..Default::default()
                });
                VolumeMount {
                    name: volume_name,
                    mount_path: vct_mount_path(template),
                    ..Default::default()
                }
            })
            .collect();

        // Epic #484: a scheduler-placed restore Pod is created *before* its
        // snapshot is staged, so the restore must wait for the node's `Stage`
        // job to finish. That wait lives in the restore shim, NOT in a Pod init
        // container: a gVisor memory checkpoint restores as a whole sandbox and
        // runsc reconstitutes exactly the checkpoint's container set, so any
        // extra init container is rejected by restore-spec validation
        // ("checkpoint image does not contain spec for container"). Instead,
        // `containerd-shim-sandbawks-v1` blocks the root container's Restore on
        // the `.staged` marker (#487) at the staged image path before
        // dispatching. The restore Pod therefore carries no init container and
        // stays topologically identical to a cold-start Pod — exactly what the
        // whole-sandbox restore needs.

        // #272: gate scheduling on the node having staged envd. The
        // container execs the node-staged binary as its entrypoint
        // (`ENVD_BIN_PATH`); without this affinity a Pod can land on a node
        // whose `sandbawks-node` DaemonSet hasn't finished `stage-envd`
        // yet, mount an empty `DirectoryOrCreate` hostPath, and CrashLoop
        // on the missing binary (bootstrap / node bring-up / DaemonSet
        // rollout). The DaemonSet stamps `ENVD_STAGED_LABEL` only after the
        // binary is present (`crates/sandbawks-node/src/node_label.rs`), so
        // an `Exists` requirement keeps sandboxes off not-yet-staged nodes.
        //
        // Applied to cold-start *and* restore Pods. Under scheduler-placed
        // restore (epic #484) the restore Pod is unpinned, so this affinity
        // is now load-bearing on that path too: it keeps the scheduler from
        // placing the restore Pod onto a node whose `sandbawks-node`
        // DaemonSet hasn't staged envd yet (the same CrashLoop the gate
        // closes for cold start).
        let mut match_expressions = vec![NodeSelectorRequirement {
            key: ENVD_STAGED_LABEL.into(),
            operator: "Exists".into(),
            values: None,
        }];

        // #545 Capability B: a restore Pod additionally requires the node's
        // host runsc version to match the one the snapshot was checkpointed
        // against. gVisor checkpoint/restore is fatal across a runsc-version
        // mismatch (`runsc version does not match across checkpoint restore`
        // → `FailedCreatePodSandBox` crash-loop), so on a drifted fleet we
        // turn that into a *scheduling* decision: an `In` match on
        // `RUNSC_VERSION_LABEL` keeps the restore Pod off mismatched nodes,
        // leaving it `Pending` (diagnosable, recoverable) until a matching
        // node exists. Both requirements sit in the same `NodeSelectorTerm`
        // so they are AND-ed.
        //
        // Gated on `restore.is_some()`: a cold start has no checkpoint to
        // match, so it must remain schedulable across the whole fleet. Gated
        // on a non-empty recorded version too: a snapshot taken before #545
        // (or by a node that couldn't read its runsc version) records none,
        // and pinning to an empty label value would strand every restore —
        // degrade to envd-staged affinity only instead, matching the
        // pre-#545 ungated placement.
        if restore.is_some() {
            if let Some(version) = sandbox
                .status
                .as_ref()
                .and_then(|s| s.snapshot_runsc_version.as_deref())
                .filter(|v| !v.is_empty())
            {
                match_expressions.push(NodeSelectorRequirement {
                    key: RUNSC_VERSION_LABEL.into(),
                    operator: "In".into(),
                    values: Some(vec![version.to_owned()]),
                });
            }
        }

        let affinity = Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_expressions: Some(match_expressions),
                        match_fields: None,
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        // Issue #592: thread the template-resolved scheduling hints onto the
        // Pod so it can land on a tainted/labeled node pool (e.g. a dedicated
        // Kata/Azure pool). `resolve_pod_inputs` has already filled
        // `spec.tolerations` / `spec.nodeSelector` from the template (an
        // explicit value on the CR wins), so here we only translate the CRD's
        // serde-mirror `Toleration` into the `k8s_openapi` type the `PodSpec`
        // expects. These compose with the runsc-version `affinity` above —
        // `nodeSelector` (AND-ed with node affinity by Kubernetes) narrows
        // placement, `tolerations` only *permit* tainted nodes — rather than
        // replacing it. `None` leaves both Pod fields unset, so a template
        // without scheduling hints yields exactly the pre-#592 PodSpec.
        let tolerations = sandbox.spec.tolerations.as_ref().map(|tols| {
            tols.iter()
                .map(|t| K8sToleration {
                    effect: t.effect.clone(),
                    key: t.key.clone(),
                    operator: t.operator.clone(),
                    toleration_seconds: t.toleration_seconds,
                    value: t.value.clone(),
                })
                .collect::<Vec<_>>()
        });
        let node_selector = sandbox.spec.node_selector.clone();

        // Pod-level securityContext (issue #617). Resolved from the template by
        // `resolve_pod_inputs` (explicit CR value wins), so here we only
        // translate the CRD's serde-mirror `PodSecurityContext` into the
        // `k8s_openapi` type. The load-bearing field is `fsGroup`: it makes the
        // kubelet chown the `volumeClaimTemplates` PVC to that GID so a non-root
        // container can write to its workspace volume. `None` leaves
        // `PodSpec.securityContext` unset — exactly the pre-#617 PodSpec.
        let security_context = sandbox.spec.security_context.as_ref().map(|sc| {
            K8sPodSecurityContext {
                fs_group: sc.fs_group,
                run_as_user: sc.run_as_user,
                run_as_group: sc.run_as_group,
                run_as_non_root: sc.run_as_non_root,
                fs_group_change_policy: sc.fs_group_change_policy.clone(),
                // The mirror carries a (possibly empty) Vec; the upstream type
                // wants `Option<Vec>`, where empty and absent are equivalent.
                supplemental_groups: (!sc.supplemental_groups.is_empty())
                    .then(|| sc.supplemental_groups.clone()),
                ..Default::default()
            }
        });

        // Issue #350: gate kubelet `Ready` on the gRPC filesystem port
        // (`:49983`) that the api's `/files/*` routes actually dial, rather
        // than leaving the container probe-less. Without a readinessProbe a
        // sandbox Pod reports its container `Ready` the instant envd's
        // process starts — *before* the `:49983` gRPC listener is accepting
        // — so the controller's `pod_ready` gate (which keys off Pod
        // condition `Ready=True`) can stamp `status.envdAddress` and promote
        // the Sandbox to `Running` while a download to `:49983` would still
        // hang. A TCP-socket probe on `:49983` makes "container Ready" mean
        // "the gRPC port is accepting connections".
        //
        // TCP, not a gRPC health probe: envd serves only the `Filesystem`
        // and `Process` services and registers no `grpc.health.v1.Health`
        // service, so kubelet's native `grpc` probe would fail permanently.
        // A TCP probe is the faithful "is `:49983` listening" signal without
        // adding a health service to envd.
        //
        // Scope / limitation: a TCP probe confirms the listener accepts a
        // connection, not that a unary RPC returns — so it does NOT by itself
        // close the case where the listener is up but a handler (e.g. a
        // filesystem read off the gVisor-restored rootfs) hangs (see #350's
        // 2026-06-05 evidence). The self-classifying probe added to
        // `pause-resume-smoke.sh` distinguishes those two regimes.
        //
        // Applied uniformly to cold-start and restore Pods: a readinessProbe
        // is kubelet-side metadata, not part of the `runsc` sandbox, so it
        // does not perturb the checkpoint/restore image — the two Pod shapes
        // stay topologically identical to the checkpoint.
        let readiness_probe = Probe {
            tcp_socket: Some(TCPSocketAction {
                // Target the gRPC port by NAME, not by number: kubelet
                // resolves `IntOrString::String` against the container's named
                // ports, so the probe and the `containerPort` declaration below
                // can't silently drift apart if the number ever changes.
                // `host: None` means the Pod IP (kubelet's default).
                port: IntOrString::String(ENVD_PORT_NAME.to_owned()),
                host: None,
            }),
            // Probe from the first second: on a fast cold start the listener is
            // up almost immediately, and on a slow GiB restore we want `Ready`
            // to flip soon after `:49983` starts accepting.
            //
            // `period_seconds = 2` (not 1) is a deliberate noise/latency
            // tradeoff: kubelet emits a `Warning Unhealthy: ... connection
            // refused` event on every miss during the whole pre-listener window,
            // so on a slow GiB restore a 1s cadence produces dozens-to-hundreds
            // of events per Pod (noising up `kubectl describe pod` and kubelet's
            // event stream). 2s halves that while still flipping `Ready` within
            // ~2s of the listener coming up — negligible against a multi-second
            // restore.
            initial_delay_seconds: Some(0),
            period_seconds: Some(ENVD_READINESS_PERIOD_SECS),
            timeout_seconds: Some(1),
            success_threshold: Some(1),
            // A readinessProbe never restarts the container; failureThreshold
            // only governs how many consecutive misses flip a previously-Ready
            // container back to NotReady. Keep the conventional 3 so a
            // transient blip under a heavy restore doesn't thrash the
            // Sandbox's `Running`/`Ready` edge.
            failure_threshold: Some(3),
            ..Default::default()
        };

        Pod {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(namespace),
                labels: Some(labels),
                annotations,
                owner_references,
                ..Default::default()
            },
            spec: Some(PodSpec {
                runtime_class_name,
                // No `node_name`: a restore Pod is scheduler-placed (epic
                // #484), not pinned. Cold-start Pods were never pinned here
                // either, so this field is simply absent for both shapes.
                affinity: Some(affinity),
                // Template-resolved scheduling hints (#592); `None` when the
                // template (and CR) set none, leaving the pre-#592 shape.
                tolerations,
                node_selector,
                // Pod-level securityContext (#617); `None` leaves the Pod
                // root-owned exactly as before, so a non-#617 template is
                // unchanged.
                security_context,
                restart_policy: Some("Never".into()),
                volumes: Some(volumes),
                // No init container on either shape (epic #484): the restore's
                // wait-for-staging gate lives in the shim, not the Pod, because
                // gVisor whole-sandbox restore rejects any container absent from
                // the checkpoint. See the staging comment above.
                init_containers: None,
                containers: vec![Container {
                    name: "envd".into(),
                    image: Some(image.to_owned()),
                    // Run the node-staged envd binary as the container's
                    // entrypoint, overriding the customer image's own CMD.
                    command: Some(vec![ENVD_BIN_PATH.into()]),
                    ports: Some(vec![
                        ContainerPort {
                            container_port: ENVD_PORT,
                            name: Some(ENVD_PORT_NAME.into()),
                            protocol: Some("TCP".into()),
                            ..Default::default()
                        },
                        ContainerPort {
                            container_port: ENVD_HTTP_PORT,
                            name: Some("envd-state".into()),
                            protocol: Some("TCP".into()),
                            ..Default::default()
                        },
                    ]),
                    env: Some(env),
                    resources: Some(resources),
                    // envd seed mount first, then the PVC-backed mounts (#605).
                    volume_mounts: Some(std::iter::once(bin_mount).chain(pvc_mounts).collect()),
                    // Issue #350: kubelet `Ready` reflects the `:49983` gRPC
                    // listener accepting, not just "process started".
                    readiness_probe: Some(readiness_probe),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        }
    }
}

/// Build the owner-reference list tying a per-sandbox child object (Pod, env
/// Secret, egress NetworkPolicy, FQDN CiliumNetworkPolicy) back to its Sandbox
/// CR, so kube's garbage collector tears the child down with the sandbox.
///
/// Returns `None` when the Sandbox has no `metadata.uid` yet (e.g. a freshly
/// constructed CR in unit tests), mirroring the Pod's "no uid → no
/// ownerReferences" behavior. Factored out (issue #541) so every per-sandbox
/// child stamps the identical reference shape from one source.
fn sandbox_owner_references(sandbox: &Sandbox, name: &str) -> Option<Vec<OwnerReference>> {
    sandbox.metadata.uid.as_ref().map(|uid| {
        vec![OwnerReference {
            api_version: "sandbawks.dev/v1alpha1".into(),
            kind: "Sandbox".into(),
            name: name.to_owned(),
            uid: uid.clone(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]
    })
}

/// Deterministic name of the Sandbox-owned PVC materialized for one
/// `volumeClaimTemplate` (#603 Phase 2, issue #605).
///
/// `{template}-{sandbox}` — the StatefulSet / `agent-sandbox` contract. The
/// template name distinguishes the claim within the sandbox; the sandbox name
/// scopes it, so two sandboxes minted from the same `SandboxTemplate` (which
/// share `volumeClaimTemplate` names) get distinct PVCs. Derived purely from
/// the two names so the PVC the controller creates and the one the Pod's
/// volume references (and kube garbage-collects via the owner reference) can
/// never drift.
fn pvc_name(template_name: &str, sandbox_name: &str) -> String {
    format!("{template_name}-{sandbox_name}")
}

/// Validate a Sandbox's `volumeClaimTemplate` names before any reach the
/// apiserver (#605 review): each must be a DNS-1123 label and unique across the
/// list.
///
/// The name is used verbatim as the Pod **volume name** (a DNS-1123 label) and,
/// joined with the `sbx-<id>` suffix, as the **PVC name**, so an invalid or
/// duplicated name would otherwise fail deep in PVC/Pod creation with an opaque
/// apiserver "Invalid value" — a terminal `Failed` whose cause the operator has
/// to reverse-engineer. The CRD's `pattern`/`maxLength` reject a malformed name
/// at admission; this re-check is the controller-side backstop that yields a
/// clear message naming the offending template. Cross-entry uniqueness is
/// enforced here rather than in the CRD because a list-uniqueness CEL rule is
/// disproportionate next to this O(n) check over a handful of short names.
///
/// `{template}-{sandbox}` stays collision-free across sandboxes despite the
/// bare `-` join: the sandbox half is always a fixed-format `sbx-<random>`
/// suffix, so a PVC name's trailing token unambiguously identifies its sandbox.
/// The only real exposure is therefore *invalid* or *duplicate* names, which
/// this rejects.
fn validate_volume_claim_template_names(templates: &[VolumeClaimTemplate]) -> Result<(), String> {
    let mut seen = HashSet::with_capacity(templates.len());
    for template in templates {
        let name = template.metadata.name.as_str();
        if !is_dns1123_label(name) {
            return Err(format!(
                "volumeClaimTemplate name {name:?} is not a valid Kubernetes name \
                 (must be a lowercase DNS-1123 label: alphanumerics and '-', \
                 starting and ending alphanumeric, at most 63 characters)"
            ));
        }
        if !seen.insert(name) {
            return Err(format!(
                "duplicate volumeClaimTemplate name {name:?}: \
                 names must be unique within a sandbox"
            ));
        }
    }
    Ok(())
}

/// Whether `s` is a valid DNS-1123 label — the Kubernetes name rule a Pod
/// volume name must satisfy: 1..=63 chars, lowercase `[a-z0-9-]`, with the
/// first and last character alphanumeric.
///
/// Implemented by hand (no `regex` dependency) since it runs over a handful of
/// short strings; it mirrors the `pattern`/`maxLength` the CRD schema emits for
/// the same field, so the controller and apiserver agree on what is valid.
fn is_dns1123_label(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return false;
    }
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    // First and last must be alphanumeric (a `-` may not lead or trail).
    if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    bytes.iter().all(|&b| is_alnum(b) || b == b'-')
}

/// Resolve the container mount path for one `volumeClaimTemplate` (#603 Phase
/// 2, issue #605).
///
/// A Sandbox has no per-container `volumeMounts` surface (unlike a
/// StatefulSet's pod template), so the mount path is taken from the optional
/// [`VCT_MOUNT_PATH_ANNOTATION`] on the template metadata, falling back to
/// `/<volume-name>`. The fallback makes the common case ergonomic — a template
/// named `workspace` mounts at `/workspace` — and the annotation covers
/// arbitrary paths (`/home/user/data`) without a CRD change. A blank/whitespace
/// annotation value is ignored and falls back, so an empty override can never
/// produce an invalid empty `mountPath`.
fn vct_mount_path(template: &VolumeClaimTemplate) -> String {
    template
        .metadata
        .annotations
        .get(VCT_MOUNT_PATH_ANNOTATION)
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("/{}", template.metadata.name))
}

/// Whether `pvc` is controller-owned by `sandbox` — i.e. carries a controlling
/// owner reference whose `uid` matches the Sandbox's (#603 Phase 2, issue
/// #605).
///
/// The PVC adoption guard uses this on the `409 AlreadyExists` path to
/// distinguish "our own PVC from a prior reconcile" (idempotent success) from a
/// foreign PVC that happens to share the deterministic `{template}-{sandbox}`
/// name (refuse — never clobber data we don't own). Matching on `uid` rather
/// than name defends against a recreated Sandbox reusing a name: a stale PVC
/// from a deleted predecessor is *not* ours.
fn pvc_owned_by(pvc: &PersistentVolumeClaim, sandbox: &Sandbox) -> bool {
    let Some(uid) = sandbox.metadata.uid.as_deref() else {
        return false;
    };
    pvc.metadata
        .owner_references
        .iter()
        .flatten()
        .any(|o| o.controller == Some(true) && o.kind == "Sandbox" && o.uid == uid)
}

/// Classify one `egressAllow` entry for the egress NetworkPolicy (issue #443).
///
/// A standard `networking.v1` NetworkPolicy egress rule can only name an L3
/// destination as an `ipBlock` CIDR — it has no FQDN/DNS-name field. So an
/// entry is usable as a `to` destination only when it is an IP address or
/// CIDR:
///
/// * A bare IPv4/IPv6 address (`"10.0.0.5"`, `"2001:db8::1"`) is widened to a
///   host CIDR (`/32` or `/128`).
/// * An explicit CIDR (`"10.0.0.0/8"`, `"2001:db8::/64"`) is passed through
///   verbatim once its address parses and its prefix length fits the address
///   family.
/// * Anything else is a DNS *name*: it yields no `ipBlock`, and its presence
///   instead signals that DNS egress must be opened (see
///   [`PodAdapter::network_policy_for`]).
///
/// Returns `Some(cidr)` for an IP/CIDR literal, `None` for a DNS name.
fn egress_entry_as_cidr(entry: &str) -> Option<String> {
    use std::net::IpAddr;

    let entry = entry.trim();
    // Bare address → widen to a host CIDR for the right family.
    if let Ok(ip) = entry.parse::<IpAddr>() {
        let prefix = if ip.is_ipv4() { 32 } else { 128 };
        return Some(format!("{entry}/{prefix}"));
    }
    // `<addr>/<prefix>`: both halves must be well-formed and the prefix must
    // not exceed the address family's bit-length.
    if let Some((addr, prefix)) = entry.split_once('/') {
        if let (Ok(ip), Ok(bits)) = (addr.parse::<IpAddr>(), prefix.parse::<u8>()) {
            let max_bits = if ip.is_ipv4() { 32 } else { 128 };
            if bits <= max_bits {
                return Some(entry.to_owned());
            }
        }
    }
    None
}

/// Parse an in-cluster OTLP collector endpoint into the `(namespace, port)` an
/// egress rule needs (issue #536).
///
/// Accepts the cluster-DNS service form the Helm chart emits —
/// `[scheme://]<svc>.<ns>.svc[.cluster.local]:<port>[/path]` — and returns the
/// service's namespace and port. Returns `None` for anything else (a bare IP,
/// an external host, or a missing port): those can't be expressed as a
/// `namespaceSelector`, so the infra carve-out skips them and the operator must
/// open such a collector via an explicit `egressAllow` CIDR.
fn cluster_collector_target(endpoint: &str) -> Option<(String, i32)> {
    // Strip an optional scheme, then take the authority up to any path.
    let after_scheme = endpoint.rsplit("://").next()?;
    let authority = after_scheme.split('/').next()?;
    // Collector service DNS names are never IPv6 literals, so the last `:`
    // separates host from port.
    let (host, port) = authority.rsplit_once(':')?;
    let port: i32 = port.parse().ok()?;
    // Cluster service DNS: `<svc>.<ns>.svc[.cluster.local]`.
    let labels: Vec<&str> = host.split('.').collect();
    match labels.as_slice() {
        [_svc, ns, "svc", ..] => Some(((*ns).to_owned(), port)),
        _ => None,
    }
}

/// The OTel-collector egress rule for the infra carve-out (issue #536): allow
/// TCP to `port` for pods in the collector's namespace. Namespace-and-port
/// scoped rather than pod-label scoped so it stays derivable from the injected
/// endpoint alone (the collector pod's labels aren't encoded in the endpoint),
/// and because only the collector listens on the OTLP port in that namespace.
fn collector_egress_rule(namespace: String, port: i32) -> NetworkPolicyEgressRule {
    let mut ns_labels = BTreeMap::new();
    // Every namespace carries this immutable label automatically (k8s ≥ 1.21).
    ns_labels.insert("kubernetes.io/metadata.name".to_owned(), namespace);
    NetworkPolicyEgressRule {
        to: Some(vec![NetworkPolicyPeer {
            namespace_selector: Some(LabelSelector {
                match_labels: Some(ns_labels),
                match_expressions: None,
            }),
            ..Default::default()
        }]),
        ports: Some(vec![NetworkPolicyPort {
            port: Some(IntOrString::Int(port)),
            protocol: Some("TCP".into()),
            end_port: None,
        }]),
    }
}

/// The DNS egress rule (UDP+TCP [`DNS_PORT`] to any destination) shared by the
/// user DNS-name path and the infra carve-out.
fn dns_egress_rule() -> NetworkPolicyEgressRule {
    NetworkPolicyEgressRule {
        to: None,
        ports: Some(vec![
            NetworkPolicyPort {
                port: Some(IntOrString::Int(DNS_PORT)),
                protocol: Some("UDP".into()),
                end_port: None,
            },
            NetworkPolicyPort {
                port: Some(IntOrString::Int(DNS_PORT)),
                protocol: Some("TCP".into()),
                end_port: None,
            },
        ]),
    }
}

/// Determine the termination cause for a fetched Pod for
/// [`PodAdapter::pod_terminated`].
///
/// Returns `Some(cause)` when the Pod has reached a terminal state, `None`
/// when it is still live. The returned [`PodTerminationCause`] carries
/// actionable details so the reconcile loop can record a user-readable message
/// in `status.conditions` rather than surfacing an opaque h2 transport error
/// (issue #418).
///
/// Terminal signals checked in priority order:
///
/// 1. `status.reason == "Evicted"` — the kubelet evicted the Pod for
///    resource exhaustion (ephemeral-storage, memory pressure). The
///    `status.message` carries the eviction reason from the kubelet (e.g.
///    `"Pod ephemeral local storage usage exceeds the total limit of
///    containers 256Mi."`).
///
/// 2. Any `containerStatuses[].state.terminated.reason == "OOMKilled"` — the
///    container was killed by the OOM killer. The memory limit is read from
///    the first container's `resources.limits["memory"]` in the Pod spec.
///
/// 3. `status.phase` is `Succeeded` or `Failed`, or any container has
///    `state.terminated` set — the conventional terminal state. Returned as
///    [`PodTerminationCause::Other`] (no actionable resource detail to
///    surface).
///
/// The container-terminated signal assumes `restartPolicy: Never`, per
/// [`PodAdapter::pod_spec_for`]: under that policy a terminated *main*
/// container is permanent. Under any other restart policy a terminated
/// container can be transient, and this helper would mis-classify it as
/// terminal.
///
/// A restore Pod (epic #484) carries no init container — the wait-for-staging
/// gate lives in the restore shim, not the Pod — so it is topologically
/// identical to a cold-start Pod and this check needs no special handling for
/// it. A shim staging-gate *timeout* surfaces as the restore container failing
/// to start, which drives `status.phase` to `Failed`; Priority 3 classifies
/// that as a terminal `Other` and the reconcile loop recovers it as
/// `RestorePodLost`. If `pod_spec_for` ever grows a non-`Never` restart policy
/// or a *sidecar* (a long-lived init container with `restartPolicy: Always`,
/// whose status appears under `containerStatuses`), revisit this check.
fn pod_termination_cause(pod: &Pod) -> Option<PodTerminationCause> {
    let status = pod.status.as_ref()?;

    // Priority 1: kubelet eviction (ephemeral-storage, memory pressure, etc.).
    // The eviction manager sets `status.reason = "Evicted"` and writes the
    // human-readable reason into `status.message` before the kubelet issues
    // the SIGKILL — this is the most actionable signal for the user.
    if status.reason.as_deref() == Some("Evicted") {
        let message = status
            .message
            .clone()
            .unwrap_or_else(|| "sandbox was evicted by the kubelet".to_string());
        return Some(PodTerminationCause::Evicted { message });
    }

    // Priority 2: OOMKill — container-level signal that the OOM killer fired.
    // The kubelet sets `state.terminated.reason = "OOMKilled"` on the affected
    // container. The pod-level `status.phase` may still read `Running` during
    // the kubelet's phase-update lag (#382), so we check container statuses
    // first rather than waiting for the phase to flip.
    if let Some(container_statuses) = &status.container_statuses {
        for cs in container_statuses {
            let terminated = cs.state.as_ref().and_then(|st| st.terminated.as_ref());
            if let Some(t) = terminated {
                if t.reason.as_deref() == Some("OOMKilled") {
                    // Pull the memory limit from the first container spec so
                    // the error message can name the budget the sandbox blew.
                    let memory_limit = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.containers.first())
                        .and_then(|c| c.resources.as_ref())
                        .and_then(|r| r.limits.as_ref())
                        .and_then(|l| l.get("memory"))
                        .map(|q| q.0.clone());
                    return Some(PodTerminationCause::OomKilled { memory_limit });
                }
            }
        }
    }

    // Priority 3: conventional terminal state — phase `Succeeded`/`Failed`, or
    // any container with `state.terminated` set (covers the phase-lag window
    // for non-OOM exits). No resource-budget detail to surface, so `Other`.
    let phase_terminal = matches!(status.phase.as_deref(), Some("Succeeded") | Some("Failed"));
    let container_terminated = status.container_statuses.as_ref().is_some_and(|cs| {
        cs.iter()
            .any(|c| c.state.as_ref().is_some_and(|st| st.terminated.is_some()))
    });

    if phase_terminal || container_terminated {
        return Some(PodTerminationCause::Other);
    }

    None
}

/// Distill a Pod into the [`PodStatusSummary`] the API gateway uses to render
/// a "still starting / not ready yet" notice (issue #625).
///
/// Returns `None` only when the Pod carries no `status` at all (nothing to
/// project). Otherwise the coarse `status.phase` is always surfaced — even a
/// bare `Pending` with no reason yet is useful context ("Pod is Pending") —
/// and the dominant *blocking* container reason is attached when one exists.
///
/// "Dominant reason" is the first signal found, in the order the kubelet would
/// surface a problem: init containers before app containers (init runs first,
/// and the cold-start envd injection rides an init container — #265), and for a
/// given container its `waiting.reason` (the actionable backoff state:
/// `ImagePullBackOff`, `CrashLoopBackOff`, ...) before a bare `terminated`. A
/// `Pending` Pod with no container reason yet falls back to the
/// `PodScheduled=False` condition, so an unschedulable restore Pod (no node
/// with a matching runsc version — #545) surfaces its `Unschedulable` reason
/// rather than an opaque "Pending".
fn summarize_pod_status(pod: &Pod) -> Option<PodStatusSummary> {
    let status = pod.status.as_ref()?;
    let phase = status.phase.clone().unwrap_or_else(|| "Unknown".to_owned());

    // Init containers first, then app containers — a blocked init container is
    // what holds a Pod in `Pending`, so it is the more actionable signal.
    let container_reason = status
        .init_container_statuses
        .iter()
        .flatten()
        .chain(status.container_statuses.iter().flatten())
        .find_map(container_blocking_reason);

    let (reason, message) = match container_reason {
        Some(rm) => (Some(rm.0), rm.1),
        // No container-level reason yet — surface a scheduling failure if the
        // Pod is stuck unscheduled (`PodScheduled=False`, e.g. `Unschedulable`).
        None => scheduling_block_reason(status).map_or((None, None), |(r, m)| (Some(r), m)),
    };

    Some(PodStatusSummary {
        phase,
        reason,
        message,
    })
}

/// Extract a single container's blocking `(reason, message)` — its
/// `state.waiting.reason` (preferred: the actionable backoff state) or, failing
/// that, its `state.terminated.reason`. `None` for a `Running` container or a
/// `waiting`/`terminated` state with no reason string.
fn container_blocking_reason(
    cs: &k8s_openapi::api::core::v1::ContainerStatus,
) -> Option<(String, Option<String>)> {
    let state = cs.state.as_ref()?;
    if let Some(waiting) = state.waiting.as_ref() {
        if let Some(reason) = waiting.reason.as_ref() {
            return Some((reason.clone(), waiting.message.clone()));
        }
    }
    if let Some(terminated) = state.terminated.as_ref() {
        if let Some(reason) = terminated.reason.as_ref() {
            return Some((reason.clone(), terminated.message.clone()));
        }
    }
    None
}

/// Surface a `PodScheduled=False` condition's `(reason, message)` — e.g.
/// `Unschedulable` for a Pod the scheduler can't place — so a Pod stuck
/// `Pending` for want of a node carries a concrete cause rather than a bare
/// phase. `None` when the Pod is scheduled (or has no such condition).
fn scheduling_block_reason(
    status: &k8s_openapi::api::core::v1::PodStatus,
) -> Option<(String, Option<String>)> {
    status.conditions.as_ref()?.iter().find_map(|c| {
        (c.type_ == "PodScheduled" && c.status == "False").then(|| {
            (
                c.reason
                    .clone()
                    .unwrap_or_else(|| "Unschedulable".to_owned()),
                c.message.clone(),
            )
        })
    })
}

/// Poll `api.get_opt(name)` until it returns `Ok(None)` (resource
/// gone) or `budget` elapses, sleeping `interval` between attempts.
///
/// Used by [`PodAdapter::delete_pod`] to honor its documented contract
/// that "after delete_pod returns Ok, the pod is gone" — without this
/// the call returns as soon as the API server accepts the DELETE,
/// before kubelet has finished tearing the container down. Callers that
/// recreate a Pod with the same name immediately after a delete (e.g.
/// `Action::Terminate` followed by a fresh `Action::CreatePod`) depend
/// on this barrier to avoid a `409 AlreadyExists` against a still-
/// `Terminating` Pod.
///
/// A timeout surfaces as [`RuntimeError::Transient`] so the controller
/// can requeue: the Pod will likely be gone by the next reconcile and
/// the next pass picks up where this one left off.
async fn wait_for_pod_gone(
    api: &Api<Pod>,
    name: &str,
    budget: std::time::Duration,
    interval: std::time::Duration,
) -> Result<(), RuntimeError> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        match api.get_opt(name).await {
            Ok(None) => return Ok(()),
            Ok(Some(_)) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(RuntimeError::Transient(format!(
                        "pod {name} still present after {budget:?} — kubelet termination stuck?",
                    )));
                }
                tokio::time::sleep(interval).await;
            }
            Err(e) => {
                // Treat transport errors as "we don't know" and keep
                // polling within the budget; a transient apiserver
                // hiccup shouldn't force the resume into Failed when
                // the next poll likely succeeds.
                if tokio::time::Instant::now() >= deadline {
                    return Err(RuntimeError::Transient(format!(
                        "get pod while waiting for delete: {e}"
                    )));
                }
                tokio::time::sleep(interval).await;
            }
        }
    }
}

#[async_trait]
impl RuntimeAdapter for PodAdapter {
    fn runtime_class(&self) -> &str {
        "pod"
    }

    #[tracing::instrument(
        skip_all,
        name = "pod:controller::create",
        err,
        fields(
            sandbox = %sandbox.name_any(),
            restore = restore.is_some(),
            runtime_class = sandbox.spec.runtime_class_name.as_deref().unwrap_or("default"),
        )
    )]
    async fn create_pod(
        &self,
        sandbox: &Sandbox,
        image: &str,
        restore: Option<&RestorePlacement>,
    ) -> Result<PodRef, RuntimeError> {
        // Metric counterpart of the `pod:controller::create` span (#373).
        crate::pod_op_metrics::timed("create", async move {
            let pod = Self::pod_spec_for(sandbox, image, restore);
            let namespace = pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into());
            let name = pod
                .metadata
                .name
                .clone()
                .unwrap_or_else(|| sandbox.name_any());

            // Issue #494: materialize the per-sandbox env Secret *before*
            // the Pod that references it via `secretKeyRef`, so the Pod's
            // env vars resolve. Empty `spec.env` ⇒ no Secret (and no
            // secret-backed vars), so this is a no-op then. Idempotent the
            // same way the Pod create is: a `409 AlreadyExists` from a
            // racing reconcile is treated as success.
            //
            // Note: create-or-409 means the Secret is materialized once and
            // never reconciled. `spec.env` is set-once in this codebase, so a
            // stale Secret cannot arise today; if mutable env is ever
            // supported, the 409 branch must become an update/patch path so a
            // changed `spec.env` propagates to the Secret rather than persisting
            // the original values.
            if let Some(secret) = Self::env_secret_for(sandbox) {
                match self
                    .secrets(&namespace)
                    .create(&PostParams::default(), &secret)
                    .await
                {
                    Ok(_) => {}
                    Err(kube::Error::Api(api)) if api.code == 409 => {
                        tracing::debug!(
                            secret = %Self::env_secret_name(&name),
                            namespace = %namespace,
                            "env Secret already exists; treating create as idempotent"
                        );
                    }
                    Err(e) => {
                        return Err(RuntimeError::Transient(format!("create env secret: {e}")));
                    }
                }
            }

            // Issue #443: materialize the per-sandbox default-deny egress
            // NetworkPolicy *before* the Pod, so the sandbox's egress is
            // constrained from the instant its Pod is admitted rather than
            // racing an open window. The policy owner-references the Sandbox
            // CR (when uid is set) so kube garbage-collects it on sandbox
            // deletion. Applied uniformly to cold-start and restore Pods — a
            // restored sandbox needs the same trust boundary as a fresh one.
            // Idempotent the same way the Pod/Secret creates are: a `409
            // AlreadyExists` from a racing reconcile is treated as success.
            //
            // Note: like the env Secret, the policy is create-or-409 and never
            // reconciled. `spec.networkPolicy` is set-once in this codebase, so
            // a stale policy cannot arise today; if mutable egress policy is
            // ever supported, the 409 branch must become an update/patch path.
            let policy = Self::network_policy_for(sandbox, |name| std::env::var(name).ok());
            match self
                .network_policies(&namespace)
                .create(&PostParams::default(), &policy)
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(api)) if api.code == 409 => {
                    tracing::debug!(
                        policy = %Self::network_policy_name(&name),
                        namespace = %namespace,
                        "egress NetworkPolicy already exists; treating create as idempotent"
                    );
                }
                Err(e) => {
                    return Err(RuntimeError::Transient(format!(
                        "create egress network policy: {e}"
                    )));
                }
            }

            // Issue #541: on a cluster that opted into FQDN egress, additionally
            // materialize a Cilium `toFQDNs` policy so DNS-*name* entries in
            // `egressAllow` authorize the resolved L3 connection (not just DNS).
            // `None` ⇒ the feature is off or no entry is a name, so this is a
            // no-op and the baseline NetworkPolicy alone governs egress. Created
            // before the Pod for the same reason as the baseline policy.
            if let Some(fqdn_policy) =
                Self::cilium_network_policy_for(sandbox, |name| std::env::var(name).ok())
            {
                self.create_or_ack_cilium_network_policy(&namespace, &fqdn_policy)
                    .await?;
            }

            // #603 Phase 2 (#605): materialize the Sandbox-owned PVCs backing
            // the declared `volumeClaimTemplates` *before* the Pod that mounts
            // them, so the kubelet can bind the volumes when the Pod is
            // admitted. No templates ⇒ no PVCs, so this is a no-op for every
            // pre-#603 sandbox. Each create is idempotent with an adoption
            // guard (see `ensure_pvc`).
            self.ensure_pvcs(sandbox, &namespace).await?;

            // `create_pod` must be idempotent. The reconcile loop can race
            // with itself when the apiserver's watch stream re-fires before
            // the first reconcile finishes patching `status.pod_ref` — the
            // second reconcile then re-derives `Trigger::PodCreate` with the
            // pre-patch view of the Sandbox. Treat `409 AlreadyExists` as
            // success and return the existing Pod's identity so the caller
            // can converge the status without entering `Failed`.
            match self
                .pods(&namespace)
                .create(&PostParams::default(), &pod)
                .await
            {
                Ok(created) => Ok(PodRef {
                    name: created.metadata.name.clone().unwrap_or(name),
                    namespace: created.metadata.namespace.clone().unwrap_or(namespace),
                }),
                Err(kube::Error::Api(api)) if api.code == 409 => {
                    tracing::debug!(
                        pod = %name,
                        namespace = %namespace,
                        "Pod already exists; treating create_pod as idempotent"
                    );
                    Ok(PodRef { name, namespace })
                }
                Err(e) => Err(RuntimeError::Transient(format!("create pod: {e}"))),
            }
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "pod:controller::ensure_egress_policy",
        err,
        fields(
            otel.kind = "client",
            sandbox = %sandbox.metadata.name.as_deref().unwrap_or("anon"),
            namespace = %sandbox.metadata.namespace.as_deref().unwrap_or("default")
        )
    )]
    async fn ensure_egress_policy(&self, sandbox: &Sandbox) -> Result<(), RuntimeError> {
        // Resolve the FQDN / infra-egress feature gates from the process
        // environment in production. The work itself lives in the inherent
        // [`PodAdapter::reconcile_egress_policy`], which takes the `lookup`
        // explicitly so tests can drive the Cilium FQDN path without mutating
        // shared process env (mirroring `network_policy_for`'s `lookup` param).
        self.reconcile_egress_policy(sandbox, |n| std::env::var(n).ok())
            .await
    }

    #[tracing::instrument(
        skip_all,
        name = "pod:controller::delete",
        err,
        fields(pod = %pod_ref.name, namespace = %pod_ref.namespace)
    )]
    async fn delete_pod(&self, pod_ref: &PodRef) -> Result<(), RuntimeError> {
        // Metric counterpart of the `pod:controller::delete` span (#373).
        crate::pod_op_metrics::timed("delete", async move {
            let api = self.pods(&pod_ref.namespace);
            // Foreground propagation marks the Pod with `deletionTimestamp`
            // *and* the `foregroundDeletion` finalizer, so the API server
            // doesn't fully remove the resource until kubelet has finished
            // tearing the container down. We still poll explicitly below —
            // the DELETE call itself returns as soon as the finalizer is
            // set, which is before kubelet has actually finished — but the
            // foreground policy is the most-correct semantic for what
            // we're asking for.
            let delete_params = DeleteParams {
                propagation_policy: Some(PropagationPolicy::Foreground),
                ..DeleteParams::default()
            };
            match api.delete(&pod_ref.name, &delete_params).await {
                Ok(_) => {}
                // 404 is treated as success: the controller's contract is
                // "after delete_pod returns Ok, the pod is gone". Already-gone
                // is the desired state, no need to poll.
                Err(kube::Error::Api(e)) if e.code == 404 => return Ok(()),
                Err(e) => return Err(RuntimeError::Transient(format!("delete pod: {e}"))),
            }

            // Poll until the API server reports 404. Without this the
            // contract documented above is a lie — `DELETE` returns
            // immediately and the Pod sits in `Terminating` for as long as
            // kubelet's graceful-shutdown dance takes. Any caller that
            // recreates a Pod with the same name right after this returns
            // would collide at the API server (`409 AlreadyExists`) against
            // the still-`Terminating` Pod, and the 409-as-success shim in
            // `create_pod` would hand back the *dying* Pod's ref.
            wait_for_pod_gone(&api, &pod_ref.name, POD_DELETE_WAIT, POD_DELETE_POLL).await
        })
        .await
    }

    // No `err`: a transient GET failure here is swallowed by the callers
    // (`running_liveness_trigger` / `creating_liveness_trigger` log it at
    // debug and retry on the next requeue), so marking the span errored would
    // turn every apiserver hiccup into a false Issue — exactly the
    // retry-chatty case `docs/observability.md` warns against. Genuine
    // failures still surface via the parent reconcile span's `err`.
    #[tracing::instrument(
        skip_all,
        name = "pod:controller::ready",
        fields(pod = %pod_ref.name, namespace = %pod_ref.namespace)
    )]
    async fn pod_ready(&self, pod_ref: &PodRef) -> Result<Option<PodReadiness>, RuntimeError> {
        // Metric counterpart of the `pod:controller::ready` span (#373). A
        // not-ready probe (`Ok(None)`) records `success` — the call succeeded.
        crate::pod_op_metrics::timed("ready", async move {
            let pod = match self.pods(&pod_ref.namespace).get_opt(&pod_ref.name).await {
                Ok(Some(p)) => p,
                Ok(None) => return Ok(None),
                Err(e) => return Err(RuntimeError::Transient(format!("get pod: {e}"))),
            };

            let status = pod.status.as_ref();
            let is_ready = status
                .and_then(|s| s.conditions.as_ref())
                .map(|conds| {
                    conds
                        .iter()
                        .any(|c| c.type_ == "Ready" && c.status == "True")
                })
                .unwrap_or(false);

            if !is_ready {
                return Ok(None);
            }

            let ip = status
                .and_then(|s| s.pod_ip.clone())
                .ok_or_else(|| RuntimeError::Transient("pod Ready but podIP missing".into()))?;
            // `Pod.spec.nodeName` is populated by the scheduler before the
            // kubelet starts containers, so by the time we observe
            // condition Ready=True it's reliably present. `metadata.uid`
            // is set by the apiserver at create time. Surfacing both into
            // `SandboxStatus` so the controller can stamp them on the
            // SnapshotJob it creates at pause time — without
            // `spec.nodeName` the per-node worker's field selector never
            // matches.
            let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
            let pod_uid = pod.metadata.uid.clone();
            Ok(Some(PodReadiness {
                envd_address: sandbawks_types::host_port(&ip, ENVD_PORT),
                node_name,
                pod_uid,
            }))
        })
        .await
    }

    /// Probe whether envd is actually **serving** by issuing a real
    /// `GET /__version` to its HTTP control port (`:49984`) and checking for a
    /// `200` (issues #560/#620). A checkpoint-wedged envd accepts the TCP connection
    /// (so the kubelet readiness probe on `:49983` passes) but never answers —
    /// that surfaces here as `Ok(false)`, distinct from a genuine `200`
    /// (`Ok(true)`). A malformed address is the only `Err`; a connect/read
    /// failure or timeout is reported as `Ok(false)` ("not serving"), since the
    /// resume readiness deadline — not a transient-error retry — owns recovery
    /// from a persistently wedged restore.
    #[tracing::instrument(skip_all, name = "pod:controller::serving", fields(envd = %envd_address))]
    async fn envd_serving(&self, envd_address: &str) -> Result<bool, RuntimeError> {
        crate::pod_op_metrics::timed("serving", async move {
            // `envd_address` is the gRPC `host:port`; the HTTP control plane is
            // one port up on the same host. Split on the LAST ':' so a bracketed
            // IPv6 literal's own colons are preserved (`[fdcd::1]:49983` → host
            // `[fdcd::1]`, still connectable as `[fdcd::1]:49984`).
            let Some((host, _grpc_port)) = envd_address.rsplit_once(':') else {
                return Err(RuntimeError::Transient(format!(
                    "malformed envd address {envd_address:?}"
                )));
            };
            Ok(probe_envd_serving(&format!("{host}:{ENVD_HTTP_PORT}")).await)
        })
        .await
    }

    // No `err`: like `pod_ready`, a transient GET error is propagated for the
    // reconcile loop to retry next requeue (and the liveness-trigger callers
    // swallow it at debug). `err` here would surface routine apiserver hiccups
    // as Issues; the parent reconcile span still reports real failures.
    #[tracing::instrument(
        skip_all,
        name = "pod:controller::terminated",
        fields(pod = %pod_ref.name, namespace = %pod_ref.namespace)
    )]
    async fn pod_terminated(
        &self,
        pod_ref: &PodRef,
    ) -> Result<Option<PodTerminationCause>, RuntimeError> {
        // Metric counterpart of the `pod:controller::terminated` span (#373).
        crate::pod_op_metrics::timed("terminated", async move {
            // A `Running` sandbox Pod runs envd with `restartPolicy: Never`.
            // When envd exits (e.g. its graceful snapshot-and-exit), the Pod
            // settles at `status.phase: Succeeded` and never restarts; a kill
            // or eviction lands it at `Failed`. Either is terminal. A Pod that
            // has been deleted out from under a `Running` Sandbox is likewise
            // gone-for-good, so a 404 (`get_opt` → `None`) is treated as
            // `Other` (deleted). A transient GET error is propagated, not
            // swallowed, so the reconcile loop retries on the next requeue
            // rather than declaring a live Sandbox failed on an apiserver
            // hiccup.
            //
            // #382: a terminal *container* status is also terminal, even while
            // `status.phase` is still `Running`. See `pod_termination_cause`
            // for the full priority ordering (eviction → OOMKill → other).
            //
            // #418: `pod_termination_cause` extracts eviction / OOMKill details
            // so the reconcile loop can write an actionable `status.conditions`
            // message rather than letting the opaque h2 transport error surface
            // to the caller.
            match self.pods(&pod_ref.namespace).get_opt(&pod_ref.name).await {
                Ok(Some(pod)) => Ok(pod_termination_cause(&pod)),
                Ok(None) => Ok(Some(PodTerminationCause::Other)),
                Err(e) => Err(RuntimeError::Transient(format!("get pod: {e}"))),
            }
        })
        .await
    }

    // No `err`: like `pod_terminated`, a transient GET error is propagated for
    // the reconcile loop to retry; the not-ready-notice projection is advisory,
    // so a routine apiserver hiccup must not surface as an Issue.
    #[tracing::instrument(
        skip_all,
        name = "pod:controller::status_summary",
        fields(pod = %pod_ref.name, namespace = %pod_ref.namespace)
    )]
    async fn pod_status_summary(
        &self,
        pod_ref: &PodRef,
    ) -> Result<Option<PodStatusSummary>, RuntimeError> {
        crate::pod_op_metrics::timed("status_summary", async move {
            // A vanished Pod (`Ok(None)`) projects no summary — the separate
            // liveness path owns "Pod is gone"; here we only describe a Pod that
            // exists but is not yet serving.
            match self.pods(&pod_ref.namespace).get_opt(&pod_ref.name).await {
                Ok(Some(pod)) => Ok(summarize_pod_status(&pod)),
                Ok(None) => Ok(None),
                Err(e) => Err(RuntimeError::Transient(format!("get pod: {e}"))),
            }
        })
        .await
    }

    // No `err`: a transient GET error is propagated for the reconcile loop to
    // retry next requeue (the node-discovery caller swallows it at debug), so
    // marking the span errored would surface routine apiserver hiccups as
    // Issues; the parent reconcile span still reports real failures.
    #[tracing::instrument(
        skip_all,
        name = "pod:controller::scheduled_node",
        fields(pod = %pod_ref.name, namespace = %pod_ref.namespace)
    )]
    async fn pod_scheduled_node(&self, pod_ref: &PodRef) -> Result<Option<String>, RuntimeError> {
        // Epic #484 scheduler-placed restore: read `spec.nodeName` as soon
        // as the scheduler binds the (unpinned) restore Pod — this is set at
        // bind time, long before the container is Ready, which is precisely
        // the window the controller needs (stage the snapshot before the Pod
        // restores). A missing `spec.nodeName` means "not scheduled yet"
        // (`Ok(None)`); a 404 (Pod gone) also folds to `Ok(None)` — the
        // separate liveness path, not this probe, decides a lost Pod.
        crate::pod_op_metrics::timed("scheduled_node", async move {
            match self.pods(&pod_ref.namespace).get_opt(&pod_ref.name).await {
                Ok(Some(pod)) => Ok(pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.clone())
                    .filter(|n| !n.is_empty())),
                Ok(None) => Ok(None),
                Err(e) => Err(RuntimeError::Transient(format!("get pod: {e}"))),
            }
        })
        .await
    }

    async fn pause(
        &self,
        pod_uid: &str,
        _sink: &dyn SnapshotSink,
    ) -> Result<SnapshotManifest, RuntimeError> {
        // PodAdapter cannot preserve state. Returning Ok with an empty
        // manifest matches FakeRuntimeAdapter so the reconcile loop's
        // (Pausing → Paused) transition still drives forward; real
        // preservation lands in Plan 05's GvisorAdapter.
        Ok(SnapshotManifest {
            sandbox_id: pod_uid.to_owned(),
            runtime_class: self.runtime_class().to_owned(),
            sha256: format!("{:0>64}", "0"),
            created_at: chrono::Utc::now().to_rfc3339(),
            parts: Vec::new(),
            snapshot_ref: String::new(),
            envd_sha256: String::new(),
            runsc_version: String::new(),
            envd_captured: false,
            template_ref: String::new(),
            template_image: String::new(),
        })
    }

    async fn resume(
        &self,
        _manifest: &SnapshotManifest,
        _pod_uid: &str,
        _source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the keys present in the (mocked) environment are forwarded,
    /// and they keep [`FORWARDED_OTEL_VARS`] declaration order regardless
    /// of which subset is set.
    #[test]
    fn forwards_only_present_vars_in_declaration_order() {
        // Mock the enabled-observability environment: endpoint + protocol
        // set, samplers/timeout/attrs unset.
        let env: BTreeMap<&str, &str> = [
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "http://otel-collector.observability.svc:4318",
            ),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
        ]
        .into_iter()
        .collect();

        let forwarded = forwarded_observability_env(|name| env.get(name).map(|v| (*v).to_owned()));

        let names: Vec<&str> = forwarded.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(
            names,
            ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"],
            "only set vars forward, in declaration order"
        );
        assert_eq!(
            forwarded[0].value.as_deref(),
            Some("http://otel-collector.observability.svc:4318")
        );
        // Forwarded vars carry a literal value, never a downward-API ref.
        assert!(forwarded.iter().all(|e| e.value_from.is_none()));
    }

    /// The disabled-by-default case: the controller has only
    /// `OTEL_SDK_DISABLED=true`, which must forward so envd short-circuits
    /// to fmt-only logging instead of probing `localhost:4318`.
    #[test]
    fn forwards_sdk_disabled_kill_switch() {
        let forwarded = forwarded_observability_env(|name| {
            (name == "OTEL_SDK_DISABLED").then(|| "true".to_owned())
        });
        assert_eq!(forwarded.len(), 1);
        assert_eq!(forwarded[0].name, "OTEL_SDK_DISABLED");
        assert_eq!(forwarded[0].value.as_deref(), Some("true"));
    }

    /// The GenAI content-capture gate (issue #426) is forwarded when the
    /// controller has it set, so a sandbox's envd records the redacted
    /// command line on its `execute_tool` spans. It is not a credential, so
    /// — unlike `OTEL_EXPORTER_OTLP_HEADERS` — forwarding it is safe.
    #[test]
    fn forwards_genai_capture_gate_when_set() {
        let forwarded = forwarded_observability_env(|name| {
            (name == "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT")
                .then(|| "true".to_owned())
        });
        assert_eq!(forwarded.len(), 1);
        assert_eq!(
            forwarded[0].name,
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"
        );
        assert_eq!(forwarded[0].value.as_deref(), Some("true"));
    }

    /// The collector auth token (`OTEL_EXPORTER_OTLP_HEADERS`) must never
    /// be forwarded into the customer-controlled sandbox Pod.
    #[test]
    fn never_forwards_otlp_headers_credential() {
        let forwarded = forwarded_observability_env(|name| match name {
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://collector:4318".to_owned()),
            "OTEL_EXPORTER_OTLP_HEADERS" => Some("authorization=Bearer secret-token".to_owned()),
            _ => None,
        });
        assert!(
            forwarded
                .iter()
                .all(|e| e.name != "OTEL_EXPORTER_OTLP_HEADERS"),
            "OTLP auth headers must not leak into the sandbox Pod spec"
        );
        assert_eq!(forwarded.len(), 1, "only the non-secret endpoint forwards");
    }

    /// When split pipelines are active (issue #424), the chart sets
    /// `SANDBAWKS_OTEL_SANDBOX_ENDPOINT` on the controller to the sandbox
    /// pipeline receiver. The forwarded `OTEL_EXPORTER_OTLP_ENDPOINT` must
    /// be replaced by that override so sandbox envd hits the scrubbed pipeline
    /// rather than inheriting the controller's internal endpoint.
    #[test]
    fn sandbox_endpoint_override_replaces_forwarded_endpoint() {
        let forwarded = forwarded_observability_env(|name| match name {
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://collector:4320".to_owned()),
            "OTEL_EXPORTER_OTLP_PROTOCOL" => Some("http/protobuf".to_owned()),
            "SANDBAWKS_OTEL_SANDBOX_ENDPOINT" => Some("http://collector:4318".to_owned()),
            _ => None,
        });
        let endpoint = forwarded
            .iter()
            .find(|e| e.name == "OTEL_EXPORTER_OTLP_ENDPOINT")
            .expect("endpoint must be forwarded");
        assert_eq!(
            endpoint.value.as_deref(),
            Some("http://collector:4318"),
            "sandbox override (port 4318) must replace the internal endpoint (port 4320)"
        );
        // The override env var itself must NOT appear in the sandbox Pod spec.
        assert!(
            forwarded
                .iter()
                .all(|e| e.name != "SANDBAWKS_OTEL_SANDBOX_ENDPOINT"),
            "SANDBAWKS_OTEL_SANDBOX_ENDPOINT must not be forwarded verbatim"
        );
    }

    /// Without the split-pipeline override, the forwarded endpoint is the
    /// controller's own `OTEL_EXPORTER_OTLP_ENDPOINT` unchanged.
    #[test]
    fn no_sandbox_override_forwards_controller_endpoint() {
        let forwarded = forwarded_observability_env(|name| match name {
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://collector:4318".to_owned()),
            _ => None,
        });
        let endpoint = forwarded
            .iter()
            .find(|e| e.name == "OTEL_EXPORTER_OTLP_ENDPOINT")
            .expect("endpoint must be forwarded");
        assert_eq!(endpoint.value.as_deref(), Some("http://collector:4318"));
    }

    /// When the split-pipeline override is set but the controller's own
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, the override alone is forwarded
    /// as the endpoint. This pins the `or_else` precedence: a future refactor
    /// that reordered the fallback could otherwise silently drop endpoint
    /// forwarding entirely when split pipelines are on and the controller's own
    /// obs export was left unconfigured (issue #424 review).
    #[test]
    fn sandbox_override_alone_forwards_as_endpoint() {
        let forwarded = forwarded_observability_env(|name| match name {
            "SANDBAWKS_OTEL_SANDBOX_ENDPOINT" => Some("http://collector:4318".to_owned()),
            _ => None,
        });
        let endpoint = forwarded
            .iter()
            .find(|e| e.name == "OTEL_EXPORTER_OTLP_ENDPOINT")
            .expect("override must be forwarded as the endpoint even with no controller endpoint");
        assert_eq!(endpoint.value.as_deref(), Some("http://collector:4318"));
        // The override env var itself must never leak into the sandbox Pod spec.
        assert!(
            forwarded
                .iter()
                .all(|e| e.name != "SANDBAWKS_OTEL_SANDBOX_ENDPOINT"),
            "SANDBAWKS_OTEL_SANDBOX_ENDPOINT must not be forwarded verbatim"
        );
    }

    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStatus, PodStatus,
    };

    /// Build a Pod with the given `phase` and, optionally, a single
    /// container whose state is terminated (`true`) or running (`false`).
    fn pod_with(phase: Option<&str>, container_terminated: Option<bool>) -> Pod {
        let container_statuses = container_terminated.map(|terminated| {
            let state = if terminated {
                ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 0,
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            } else {
                ContainerState {
                    running: Some(ContainerStateRunning::default()),
                    ..Default::default()
                }
            };
            vec![ContainerStatus {
                name: "envd".into(),
                state: Some(state),
                ..Default::default()
            }]
        });
        Pod {
            status: Some(PodStatus {
                phase: phase.map(str::to_owned),
                container_statuses,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn terminal_phase_is_terminal() {
        assert!(pod_termination_cause(&pod_with(Some("Succeeded"), None)).is_some());
        assert!(pod_termination_cause(&pod_with(Some("Failed"), None)).is_some());
    }

    #[test]
    fn running_pod_with_live_container_is_not_terminal() {
        assert!(pod_termination_cause(&pod_with(Some("Running"), Some(false))).is_none());
        assert!(pod_termination_cause(&pod_with(Some("Running"), None)).is_none());
        assert!(pod_termination_cause(&pod_with(Some("Pending"), None)).is_none());
    }

    /// #382: a terminated container is terminal even while `phase` still
    /// reads `Running` — the kubelet phase update lags the container exit, and
    /// without this the idle-timeout auto-pause races a stopped container.
    #[test]
    fn running_phase_with_terminated_container_is_terminal() {
        assert!(pod_termination_cause(&pod_with(Some("Running"), Some(true))).is_some());
    }

    /// The label `pod_spec_for` stamps MUST be exactly the
    /// `app.kubernetes.io/name=sandbawks-sandbox` pair the controller's
    /// owned-Pod watch selector is built from (issue #328). If this drifts,
    /// the watch silently stops matching sandbox Pods and detection falls
    /// back to the 30s heartbeat. The literal assertions also guard the
    /// wire contract: changing either value orphans the watch over
    /// already-running Pods, so it must be a conscious edit here.
    ///
    /// The label is necessary but not sufficient: `.owns::<Pod>` maps a Pod
    /// event to its owner via a *controlling* `ownerReference`, so the test
    /// also pins that `pod_spec_for` stamps `controller: Some(true)` pointing
    /// at the Sandbox whenever `metadata.uid` is present — a labelled Pod
    /// with no controlling owner ref would never requeue its Sandbox.
    #[test]
    fn sandbox_pod_carries_watch_selector_label() {
        use sandbawks_types::{ResourceRequests, SandboxSpec};

        assert_eq!(SANDBOX_POD_LABEL_KEY, "app.kubernetes.io/name");
        assert_eq!(SANDBOX_POD_LABEL_VALUE, "sandbawks-sandbox");

        let mut sandbox = Sandbox::new(
            "sbx-label-test",
            SandboxSpec {
                template_ref: "tpl".into(),
                runtime_class_name: None,
                resources: ResourceRequests {
                    cpu: "1".parse().expect("valid cpu"),
                    memory: "1Gi".parse().expect("valid memory"),
                    ephemeral_storage: None,
                },
                timeout_seconds: 60,
                auto_pause: false,
                desired_phase: None,
                env: Default::default(),
                metadata: Default::default(),
                network_policy: None,
                snapshot_retention: None,
                pvc_retention: None,
                tolerations: None,
                node_selector: None,
                security_context: None,
                persistence_mode: None,
                volume_claim_templates: None,
                restore_from: None,
            },
        );
        // `pod_spec_for` only stamps owner references when the Sandbox has a
        // server-assigned uid (a freshly-built CR has none), so set one to
        // exercise the owner-ref path the `.owns()` watch depends on.
        sandbox.metadata.uid = Some("11111111-2222-3333-4444-555555555555".into());

        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        let labels = pod.metadata.labels.expect("pod carries labels");
        assert_eq!(
            labels.get(SANDBOX_POD_LABEL_KEY).map(String::as_str),
            Some(SANDBOX_POD_LABEL_VALUE),
            "owned-Pod watch selector must match the stamped label"
        );

        // The `.owns::<Pod>` watch maps a Pod event to its owner via a
        // *controlling* owner reference; assert exactly one, pointing at this
        // Sandbox with `controller: Some(true)`.
        let owners = pod
            .metadata
            .owner_references
            .expect("pod carries owner references when the Sandbox has a uid");
        assert_eq!(owners.len(), 1, "exactly one owner reference");
        let owner = &owners[0];
        assert_eq!(owner.kind, "Sandbox");
        assert_eq!(owner.name, "sbx-label-test");
        assert_eq!(
            owner.controller,
            Some(true),
            "owner ref must be controlling for `.owns()` to map Pod events"
        );
    }

    /// Issue #592: scheduling hints on the (template-resolved) Sandbox spec are
    /// stamped onto the Pod's `spec.tolerations` / `spec.nodeSelector` so it can
    /// land on a tainted/labeled node pool — and a Sandbox without them yields a
    /// PodSpec with both fields unset (no regression to existing placement). The
    /// hints must *compose* with the controller's runsc-version node affinity,
    /// not replace it (explicitly out of scope in the issue).
    #[test]
    fn pod_carries_template_scheduling_hints() {
        use sandbawks_types::Toleration;
        use std::collections::BTreeMap;

        // No hints → Pod leaves both scheduling fields unset.
        let bare = minimal_sandbox("sbx-sched-none");
        let bare_spec = PodAdapter::pod_spec_for(&bare, "img:latest", None)
            .spec
            .expect("pod has a spec");
        assert!(bare_spec.tolerations.is_none(), "no tolerations by default");
        assert!(
            bare_spec.node_selector.is_none(),
            "no nodeSelector by default"
        );

        // Hints present → translated onto the PodSpec losslessly.
        let mut sandbox = minimal_sandbox("sbx-sched-set");
        sandbox.spec.tolerations = Some(vec![Toleration {
            effect: Some("NoSchedule".into()),
            key: Some("kata".into()),
            operator: Some("Equal".into()),
            value: Some("true".into()),
            toleration_seconds: None,
        }]);
        let mut node_selector = BTreeMap::new();
        node_selector.insert("agentpool".to_owned(), "kata".to_owned());
        sandbox.spec.node_selector = Some(node_selector.clone());

        let spec = PodAdapter::pod_spec_for(&sandbox, "img:latest", None)
            .spec
            .expect("pod has a spec");
        let tols = spec.tolerations.expect("tolerations stamped");
        assert_eq!(tols.len(), 1);
        assert_eq!(tols[0].key.as_deref(), Some("kata"));
        assert_eq!(tols[0].effect.as_deref(), Some("NoSchedule"));
        assert_eq!(tols[0].operator.as_deref(), Some("Equal"));
        assert_eq!(tols[0].value.as_deref(), Some("true"));
        assert_eq!(spec.node_selector, Some(node_selector));

        // The runsc-version node affinity (the controller's internal placement)
        // still composes alongside the new hints rather than being replaced.
        assert!(
            spec.affinity.and_then(|a| a.node_affinity).is_some(),
            "scheduling hints must compose with the existing node affinity (#592)"
        );
    }

    /// Issue #617: a (template-resolved) `securityContext` on the Sandbox spec is
    /// translated onto the Pod's `spec.securityContext` so a non-root container
    /// can write to its `volumeClaimTemplates` PVC (`fsGroup`), and a Sandbox
    /// without one yields a PodSpec with the field unset (no regression).
    #[test]
    fn pod_carries_pod_security_context() {
        use sandbawks_types::PodSecurityContext;

        // No securityContext → PodSpec leaves the field unset (root-owned, as
        // before).
        let bare = minimal_sandbox("sbx-sc-none");
        let bare_spec = PodAdapter::pod_spec_for(&bare, "img:latest", None)
            .spec
            .expect("pod has a spec");
        assert!(
            bare_spec.security_context.is_none(),
            "no securityContext by default"
        );

        // securityContext present → translated onto the PodSpec losslessly.
        let mut sandbox = minimal_sandbox("sbx-sc-set");
        sandbox.spec.security_context = Some(PodSecurityContext {
            fs_group: Some(1000),
            run_as_user: Some(1000),
            run_as_group: Some(1000),
            run_as_non_root: Some(true),
            fs_group_change_policy: Some("OnRootMismatch".into()),
            supplemental_groups: vec![2000, 3000],
        });
        let sc = PodAdapter::pod_spec_for(&sandbox, "img:latest", None)
            .spec
            .expect("pod has a spec")
            .security_context
            .expect("securityContext stamped");
        assert_eq!(sc.fs_group, Some(1000));
        assert_eq!(sc.run_as_user, Some(1000));
        assert_eq!(sc.run_as_group, Some(1000));
        assert_eq!(sc.run_as_non_root, Some(true));
        assert_eq!(sc.fs_group_change_policy.as_deref(), Some("OnRootMismatch"));
        assert_eq!(sc.supplemental_groups, Some(vec![2000, 3000]));

        // An empty supplementalGroups list maps to the upstream `None`, not an
        // empty Vec, so the wire shape matches a hand-authored Pod.
        let mut empty_groups = minimal_sandbox("sbx-sc-empty-groups");
        empty_groups.spec.security_context = Some(PodSecurityContext {
            fs_group: Some(42),
            ..Default::default()
        });
        let sc = PodAdapter::pod_spec_for(&empty_groups, "img:latest", None)
            .spec
            .expect("pod has a spec")
            .security_context
            .expect("securityContext stamped");
        assert_eq!(sc.fs_group, Some(42));
        assert_eq!(sc.supplemental_groups, None);
    }

    fn minimal_sandbox(name: &str) -> Sandbox {
        use sandbawks_types::{ResourceRequests, SandboxSpec};
        Sandbox::new(
            name,
            SandboxSpec {
                template_ref: "tpl".into(),
                runtime_class_name: None,
                resources: ResourceRequests {
                    cpu: "1".parse().expect("valid cpu"),
                    memory: "1Gi".parse().expect("valid memory"),
                    ephemeral_storage: None,
                },
                timeout_seconds: 60,
                auto_pause: false,
                desired_phase: None,
                env: Default::default(),
                metadata: Default::default(),
                network_policy: None,
                snapshot_retention: None,
                pvc_retention: None,
                tolerations: None,
                node_selector: None,
                security_context: None,
                persistence_mode: None,
                volume_claim_templates: None,
                restore_from: None,
            },
        )
    }

    /// Build a `VolumeClaimTemplate` for the PVC tests (#603 Phase 2, #605).
    fn vct(name: &str, storage: &str) -> sandbawks_types::VolumeClaimTemplate {
        use sandbawks_types::{VolumeClaimSpec, VolumeClaimTemplate, VolumeClaimTemplateMetadata};
        VolumeClaimTemplate {
            metadata: VolumeClaimTemplateMetadata {
                name: name.into(),
                ..Default::default()
            },
            spec: VolumeClaimSpec {
                storage: storage.parse().expect("valid storage quantity"),
                access_modes: vec!["ReadWriteOnce".into()],
                storage_class_name: Some("fast".into()),
                volume_mode: None,
            },
        }
    }

    /// A sandbox carrying the given `volumeClaimTemplates` (#605).
    fn sandbox_with_vcts(name: &str, vcts: Vec<sandbawks_types::VolumeClaimTemplate>) -> Sandbox {
        let mut s = minimal_sandbox(name);
        s.spec.volume_claim_templates = Some(vcts);
        s
    }

    /// #605: `pvc_for` materializes a `{template}-{sandbox}`-named PVC that
    /// owner-references the Sandbox (controlling, block-owner-deletion) so it
    /// cascade-GCs with the CR, and faithfully translates the CRD's
    /// `VolumeClaimSpec` subset (storage request, access modes, storage class)
    /// into the `k8s_openapi` PVC spec.
    #[test]
    fn pvc_for_builds_named_owned_claim() {
        let mut s = sandbox_with_vcts("sbx-pvc", vec![vct("workspace", "10Gi")]);
        s.metadata.uid = Some("uid-123".into());
        s.metadata.namespace = Some("team-acme".into());

        let pvcs = PodAdapter::pvcs_for(&s);
        assert_eq!(pvcs.len(), 1, "one PVC per template");
        let pvc = &pvcs[0];

        assert_eq!(pvc.metadata.name.as_deref(), Some("workspace-sbx-pvc"));
        assert_eq!(pvc.metadata.namespace.as_deref(), Some("team-acme"));

        let owners = pvc
            .metadata
            .owner_references
            .as_ref()
            .expect("PVC carries owner references when the Sandbox has a uid");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "Sandbox");
        assert_eq!(owners[0].name, "sbx-pvc");
        assert_eq!(owners[0].uid, "uid-123");
        assert_eq!(owners[0].controller, Some(true), "controlling owner ref");
        assert_eq!(
            owners[0].block_owner_deletion,
            Some(true),
            "block_owner_deletion so the PVC is torn down with the Sandbox"
        );

        // Identity label for traceability/selection.
        assert_eq!(
            pvc.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(SANDBOX_NAME_LABEL))
                .map(String::as_str),
            Some("sbx-pvc")
        );

        let spec = pvc.spec.as_ref().expect("PVC spec set");
        assert_eq!(
            spec.access_modes.as_deref(),
            Some(["ReadWriteOnce".to_string()].as_slice())
        );
        assert_eq!(spec.storage_class_name.as_deref(), Some("fast"));
        let storage = spec
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .and_then(|req| req.get("storage"))
            .expect("storage request set");
        assert_eq!(storage.0.as_str(), "10Gi");
    }

    /// #605: `pod_spec_for` injects one PVC-backed volume + container mount per
    /// template — the volume named for the template, pointing at the
    /// `{template}-{sandbox}` claim, mounted at the resolved path — while the
    /// read-only envd seed volume/mount and the ephemeral root FS are untouched.
    #[test]
    fn pod_spec_injects_pvc_volume_and_mount() {
        let s = sandbox_with_vcts("sbx-mnt", vec![vct("workspace", "5Gi")]);
        let pod = PodAdapter::pod_spec_for(&s, "img:latest", None);
        let spec = pod.spec.expect("pod has a spec");

        let volumes = spec.volumes.expect("volumes set");
        // envd seed (hostPath) + the PVC volume.
        assert_eq!(volumes.len(), 2);
        assert!(
            volumes.iter().any(|v| v.host_path.is_some()),
            "the read-only envd seed hostPath volume must be retained"
        );
        let pvc_vol = volumes
            .iter()
            .find(|v| v.name == "workspace")
            .expect("PVC-backed volume named for the template");
        assert_eq!(
            pvc_vol
                .persistent_volume_claim
                .as_ref()
                .expect("volume is PVC-backed")
                .claim_name,
            "workspace-sbx-mnt"
        );

        let mounts = spec.containers[0]
            .volume_mounts
            .as_ref()
            .expect("container has volume mounts");
        // The envd seed mount comes first, then the PVC mount.
        assert_eq!(mounts.len(), 2);
        assert_eq!(
            mounts[0].name, ENVD_BIN_VOLUME,
            "envd seed mount stays first"
        );
        let pvc_mount = mounts
            .iter()
            .find(|m| m.name == "workspace")
            .expect("PVC mount present");
        assert_eq!(
            pvc_mount.mount_path, "/workspace",
            "default mount path is /<template-name>"
        );
    }

    /// #605: a sandbox without `volumeClaimTemplates` yields exactly the
    /// pre-#603 PodSpec — only the envd seed volume + mount, no PVC injection.
    #[test]
    fn pod_spec_unchanged_without_volume_claim_templates() {
        let bare = minimal_sandbox("sbx-bare");
        let spec = PodAdapter::pod_spec_for(&bare, "img:latest", None)
            .spec
            .expect("pod has a spec");
        assert_eq!(
            spec.volumes.as_ref().map(Vec::len),
            Some(1),
            "only the envd seed volume"
        );
        assert_eq!(
            spec.containers[0].volume_mounts.as_ref().map(Vec::len),
            Some(1),
            "only the envd seed mount"
        );
    }

    /// #605: the mount path defaults to `/<template-name>` but is overridden by
    /// the `sandbawks.dev/mount-path` annotation; a blank override is ignored
    /// (falls back) so a PVC can never produce an invalid empty `mountPath`.
    #[test]
    fn vct_mount_path_uses_annotation_then_falls_back() {
        let mut t = vct("data", "1Gi");
        assert_eq!(vct_mount_path(&t), "/data", "default is /<name>");

        t.metadata
            .annotations
            .insert(VCT_MOUNT_PATH_ANNOTATION.into(), "/home/user/data".into());
        assert_eq!(vct_mount_path(&t), "/home/user/data", "annotation wins");

        t.metadata
            .annotations
            .insert(VCT_MOUNT_PATH_ANNOTATION.into(), "   ".into());
        assert_eq!(vct_mount_path(&t), "/data", "blank override falls back");
    }

    /// #605 adoption guard: a PVC is "ours" only when it carries a controlling
    /// owner reference whose uid matches the Sandbox's — so the controller
    /// reuses its own PVC (idempotent) but refuses a foreign one sharing the
    /// deterministic name, and a Sandbox without a uid owns nothing.
    #[test]
    fn pvc_owned_by_matches_controller_uid() {
        let mut s = minimal_sandbox("sbx-own");
        s.metadata.uid = Some("uid-xyz".into());
        let ours = PodAdapter::pvc_for(&s, &vct("workspace", "1Gi"));
        assert!(pvc_owned_by(&ours, &s), "reuse our own PVC");

        // A PVC owned by a different Sandbox uid is foreign — never adopt it.
        let mut other = minimal_sandbox("sbx-own");
        other.metadata.uid = Some("uid-other".into());
        let foreign = PodAdapter::pvc_for(&other, &vct("workspace", "1Gi"));
        assert!(!pvc_owned_by(&foreign, &s), "refuse a foreign-owned PVC");

        // A Sandbox with no server-assigned uid can own nothing.
        let no_uid = minimal_sandbox("sbx-own");
        assert!(!pvc_owned_by(&ours, &no_uid));
    }

    /// #605 review: the controller-side name guard accepts valid DNS-1123-label
    /// template names and rejects malformed ones (uppercase, underscores, dots,
    /// leading/trailing `-`, empty, over-length) and duplicates — each with a
    /// clear message — so a bad name fails diagnosably before the apiserver
    /// rather than as an opaque "Invalid value" deep in PVC/Pod creation.
    #[test]
    fn validate_volume_claim_template_names_rejects_bad_and_duplicate() {
        // Valid: a typical label and a single-char name.
        assert!(
            validate_volume_claim_template_names(&[vct("workspace", "1Gi"), vct("a", "1Gi")])
                .is_ok()
        );

        // Each invalid form is rejected, naming the offender.
        for bad in [
            "Workspace",
            "my_vol",
            "my.vol",
            "-lead",
            "trail-",
            "",
            &"x".repeat(64),
        ] {
            let err = validate_volume_claim_template_names(&[vct(bad, "1Gi")])
                .expect_err("invalid DNS-1123 label must be rejected");
            assert!(
                err.contains("not a valid Kubernetes name"),
                "unexpected error for {bad:?}: {err}"
            );
        }

        // Duplicate names within one sandbox are rejected.
        let dup = validate_volume_claim_template_names(&[vct("data", "1Gi"), vct("data", "2Gi")])
            .expect_err("duplicate template names must be rejected");
        assert!(
            dup.contains("duplicate"),
            "unexpected duplicate error: {dup}"
        );

        // Boundary: exactly 63 chars is allowed.
        assert!(validate_volume_claim_template_names(&[vct(&"x".repeat(63), "1Gi")]).is_ok());
    }

    /// #498: every sandbox Pod must opt out of descheduler eviction so a
    /// warm/idle sandbox is not reaped by `LowNodeUtilization`. The cold-start
    /// Pod carries `prefer-no-eviction=true` and nothing else.
    #[test]
    fn cold_start_pod_opts_out_of_descheduler_eviction() {
        let sandbox = minimal_sandbox("sbx-evict-cold");
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        let annotations = pod
            .metadata
            .annotations
            .expect("cold-start Pod carries the descheduler opt-out annotation");
        assert_eq!(
            annotations
                .get(ANNOTATION_PREFER_NO_EVICTION)
                .map(String::as_str),
            Some(ANNOTATION_PREFER_NO_EVICTION_VALUE),
            "cold-start Pod must opt out of descheduler eviction (#498)"
        );
        // A cold-start Pod has no restore image, so the opt-out is the only
        // annotation present.
        assert!(
            !annotations.contains_key(ANNOTATION_RESTORE_IMAGE_PATH),
            "cold-start Pod must not carry a restore-image-path annotation"
        );
    }

    /// A restore Pod keeps both the descheduler opt-out (#498) and the
    /// shim's restore-image-path annotation (Plan 07 T9) — the two must not
    /// clobber each other.
    #[test]
    fn restore_pod_keeps_both_eviction_and_restore_annotations() {
        let sandbox = minimal_sandbox("sbx-evict-restore");
        let placement = RestorePlacement {
            staging_path: "/var/lib/sandbawks/staging/sbx-evict-restore".into(),
        };
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", Some(&placement));
        let annotations = pod
            .metadata
            .annotations
            .expect("restore Pod carries annotations");
        assert_eq!(
            annotations
                .get(ANNOTATION_PREFER_NO_EVICTION)
                .map(String::as_str),
            Some(ANNOTATION_PREFER_NO_EVICTION_VALUE),
            "restore Pod must also opt out of descheduler eviction (#498)"
        );
        assert_eq!(
            annotations
                .get(ANNOTATION_RESTORE_IMAGE_PATH)
                .map(String::as_str),
            Some("/var/lib/sandbawks/staging/sbx-evict-restore"),
            "restore Pod must keep the shim's restore-image-path annotation"
        );
    }

    /// Epic #484: the restore Pod carries NO init container. The wait for the
    /// node to finish staging lives in the restore shim (it gates the root
    /// container's `runsc restore` on the `.staged` marker), not in a Pod init
    /// container — gVisor whole-sandbox restore rejects any container absent
    /// from the checkpoint. The restore Pod stays topologically identical to a
    /// cold-start Pod (the restore-image-path annotation the shim reads is
    /// asserted separately in `restore_pod_keeps_restore_image_path_annotation`).
    #[test]
    fn restore_pod_has_no_init_container() {
        let sandbox = minimal_sandbox("sbx-gate");
        let placement = RestorePlacement {
            staging_path: "/var/lib/sandbawks/staging/sbx-gate/restore".into(),
        };
        let spec = PodAdapter::pod_spec_for(&sandbox, "img:latest", Some(&placement))
            .spec
            .expect("pod spec");

        assert!(
            spec.init_containers.is_none(),
            "restore Pod must carry no init container (the staging gate is shim-side)"
        );
        // Exactly the single envd container, identical to a cold-start Pod.
        assert_eq!(spec.containers.len(), 1, "only the envd container");
        assert_eq!(spec.containers[0].name, "envd");
    }

    /// A cold-start Pod (`restore == None`) likewise carries no init container,
    /// so the cold-start and restore shapes are topologically identical.
    #[test]
    fn cold_start_pod_has_no_init_container() {
        let sandbox = minimal_sandbox("sbx-cold-init");
        let spec = PodAdapter::pod_spec_for(&sandbox, "img:latest", None)
            .spec
            .expect("pod spec");
        assert!(
            spec.init_containers.is_none(),
            "cold-start Pod must have no init containers"
        );
    }

    /// Assert the capture-in-checkpoint surface (epic #466, issues #468/#469) on
    /// a built Pod: the relocate env is set, and — crucially — the target is a
    /// *rootfs path*, NOT a separate volume/mount. A `medium: Memory` emptyDir
    /// would present to the sandbox as a gofer-backed `bind` (not captured in
    /// `pages.img`) and a writable one would trip the node's #237 mount-set gate,
    /// so the only Pod volume/mount is the read-only seed. Shared by the
    /// cold-start and restore tests so the two shapes are held to identical
    /// topology.
    fn assert_envd_relocate_surface(pod: &Pod, label: &str) {
        let spec = pod.spec.as_ref().expect("pod spec");

        // No emptyDir anywhere: the relocate target lives in the captured root
        // overlay, not a separate volume. A `medium: Memory` emptyDir here would
        // be a gofer-backed bind — uncaptured and a #237 violation.
        assert!(
            spec.volumes
                .as_ref()
                .is_none_or(|vs| vs.iter().all(|v| v.empty_dir.is_none())),
            "{label}: no emptyDir volume may be present (relocate target is a rootfs path)"
        );
        // The only volume is the read-only node-staged envd seed.
        assert!(
            spec.volumes
                .as_ref()
                .is_some_and(|vs| vs.iter().any(|v| v.name == ENVD_BIN_VOLUME)),
            "{label}: the node-staged envd seed volume must remain"
        );

        let container = &spec.containers[0];
        let mounts = container
            .volume_mounts
            .as_ref()
            .unwrap_or_else(|| panic!("{label}: envd container has no volume mounts"));
        // No mount at the relocate dir — it is a plain rootfs directory.
        assert!(
            mounts
                .iter()
                .all(|m| m.mount_path != sandbawks_types::ENVD_RUN_DIR),
            "{label}: there must be no volume mount at {} (rootfs path, not a volume)",
            sandbawks_types::ENVD_RUN_DIR
        );
        // The read-only seed mount must remain, and be the only mount.
        assert!(
            mounts
                .iter()
                .any(|m| m.name == ENVD_BIN_VOLUME && m.read_only == Some(true)),
            "{label}: the read-only envd seed mount must remain"
        );
        assert_eq!(
            mounts.len(),
            1,
            "{label}: the seed mount must be the only volume mount"
        );

        // The relocate env, set to the in-rootfs target.
        let env = container.env.as_ref().expect("container env");
        let relocate = env
            .iter()
            .find(|e| e.name == ENVD_RELOCATE_TARGET_ENV)
            .unwrap_or_else(|| panic!("{label}: missing {ENVD_RELOCATE_TARGET_ENV}"));
        assert_eq!(
            relocate.value.as_deref(),
            Some(ENVD_RELOCATE_TARGET),
            "{label}: relocate target must point at the captured rootfs path"
        );

        // The entrypoint must still be the node-staged seed binary, unchanged.
        assert_eq!(
            container.command.as_deref(),
            Some([ENVD_BIN_PATH.to_owned()].as_slice()),
            "{label}: entrypoint must remain the staged seed binary"
        );
    }

    /// Epic #466: a cold-start Pod carries the relocate env pointing at a rootfs
    /// path (no emptyDir / no relocate-dir mount), keeping the seed mount and
    /// entrypoint intact.
    #[test]
    fn cold_start_pod_carries_envd_relocate_surface() {
        let sandbox = minimal_sandbox("sbx-relocate-cold");
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        assert_envd_relocate_surface(&pod, "cold-start");
    }

    /// Epic #466: a restore Pod carries the identical relocate surface, so its
    /// mount topology matches the checkpoint (the relocate step does not re-run
    /// on a restore Pod, but the mount set must still match — #237 invariant).
    #[test]
    fn restore_pod_carries_envd_relocate_surface() {
        let sandbox = minimal_sandbox("sbx-relocate-restore");
        let placement = RestorePlacement {
            staging_path: "/var/lib/sandbawks/staging/sbx-relocate-restore/restore".into(),
        };
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", Some(&placement));
        assert_envd_relocate_surface(&pod, "restore");
    }

    /// Epic #466: the relocate budget is added to BOTH the memory request and
    /// limit so the envd copy in the overlay-tmpfs doesn't eat workload headroom.
    /// `minimal_sandbox` requests `1Gi`; the rendered Pod must request
    /// `1Gi + ENVD_RELOCATE_BUDGET_BYTES` bytes.
    #[test]
    fn envd_relocate_budget_added_to_memory() {
        let sandbox = minimal_sandbox("sbx-relocate-mem");
        let spec = PodAdapter::pod_spec_for(&sandbox, "img:latest", None)
            .spec
            .expect("pod spec");
        let resources = spec.containers[0]
            .resources
            .as_ref()
            .expect("container resources");
        let expected = (1024i128 * 1024 * 1024 + ENVD_RELOCATE_BUDGET_BYTES as i128).to_string();
        for (kind, map) in [
            ("requests", resources.requests.as_ref()),
            ("limits", resources.limits.as_ref()),
        ] {
            let memory = map
                .and_then(|m| m.get("memory"))
                .unwrap_or_else(|| panic!("{kind} must carry a memory entry"));
            assert_eq!(
                memory.0, expected,
                "{kind} memory must be the customer request plus the relocate budget"
            );
        }
    }

    /// #594: a non-gVisor (Kata) sandbox must NOT carry the relocate env or its
    /// memory budget. The relocation is a gVisor checkpoint mechanism (Kata's
    /// hypervisor snapshots the whole guest RAM, so it gains nothing), and the
    /// needless self-copy would only waste the relocate memory budget. The seed
    /// mount and entrypoint stay intact — envd just execs the staged seed in
    /// place. (Since #656 the relocate target nests under sticky-`1777`
    /// `/var/tmp/envd`, so omitting it is an optimization, not a crash-avoidance.)
    #[test]
    fn kata_sandbox_omits_relocate_env_and_budget() {
        let mut sandbox = minimal_sandbox("sbx-kata");
        sandbox.spec.runtime_class_name = Some("kata-vm-isolation".into());
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        let spec = pod.spec.as_ref().expect("pod spec");
        let container = &spec.containers[0];

        // No relocate marker env.
        assert!(
            container
                .env
                .as_ref()
                .is_none_or(|env| env.iter().all(|e| e.name != ENVD_RELOCATE_TARGET_ENV)),
            "kata cold-start Pod must not carry {ENVD_RELOCATE_TARGET_ENV}"
        );

        // Bare memory request/limit — the customer's quantity verbatim, with no
        // relocate budget (the gVisor path adds the budget as a byte count; the
        // non-gVisor path passes the request through unchanged).
        let resources = container.resources.as_ref().expect("container resources");
        let expected = "1Gi";
        for (kind, map) in [
            ("requests", resources.requests.as_ref()),
            ("limits", resources.limits.as_ref()),
        ] {
            let memory = map
                .and_then(|m| m.get("memory"))
                .unwrap_or_else(|| panic!("{kind} must carry a memory entry"));
            assert_eq!(
                memory.0, expected,
                "{kind} memory must be the bare request (no relocate budget) for a non-gVisor runtime"
            );
        }

        // Entrypoint is still the staged seed binary — envd execs it in place.
        assert_eq!(
            container.command.as_deref(),
            Some([ENVD_BIN_PATH.to_owned()].as_slice()),
            "entrypoint must remain the staged seed binary"
        );
    }

    /// `quantity_to_bytes` decodes the IEC/SI suffixes Kubernetes accepts and
    /// rejects an unrecognized unit (so the caller falls back to verbatim).
    #[test]
    fn quantity_to_bytes_decodes_known_units() {
        assert_eq!(quantity_to_bytes("512"), Some(512.0));
        assert_eq!(quantity_to_bytes("1Ki"), Some(1024.0));
        assert_eq!(quantity_to_bytes("256Mi"), Some(256.0 * 1024.0 * 1024.0));
        assert_eq!(quantity_to_bytes("1Gi"), Some(1024.0 * 1024.0 * 1024.0));
        assert_eq!(quantity_to_bytes("512M"), Some(512e6));
        // Exponent notation reads `e` as an unknown suffix → None (verbatim
        // fallback), never a wrong byte count.
        assert_eq!(quantity_to_bytes("1e3"), None);
        assert_eq!(quantity_to_bytes("12Zi"), None);
    }

    /// `memory_with_relocate_budget` returns the base verbatim when it can't
    /// parse the unit, so an exotic quantity is never reshaped into a wrong value.
    #[test]
    fn memory_budget_falls_back_on_unparseable_unit() {
        // `12Zi` is shape-valid (`ResourceQuantity` only checks digits + a
        // letter suffix) but `Zi` is not a unit `quantity_to_bytes` knows, so
        // the budget step returns it verbatim rather than emit a wrong count.
        let exotic: sandbawks_types::ResourceQuantity = "12Zi".parse().expect("shape-valid");
        assert_eq!(memory_with_relocate_budget(&exotic).0, "12Zi");
    }

    /// Pull the single node-affinity term's match expressions out of a Pod built
    /// by `pod_spec_for`, for the #272 / #545 scheduling-gate assertions.
    fn node_affinity_requirements(pod: &Pod) -> Vec<NodeSelectorRequirement> {
        pod.spec
            .as_ref()
            .and_then(|s| s.affinity.as_ref())
            .and_then(|a| a.node_affinity.as_ref())
            .and_then(|na| {
                na.required_during_scheduling_ignored_during_execution
                    .as_ref()
            })
            .and_then(|ns| ns.node_selector_terms.first())
            .and_then(|term| term.match_expressions.clone())
            .unwrap_or_default()
    }

    /// Build a [`minimal_sandbox`] whose status records the runsc version its
    /// snapshot was checkpointed against (#545, Capability B).
    fn sandbox_with_snapshot_runsc_version(name: &str, version: Option<&str>) -> Sandbox {
        use sandbawks_types::SandboxStatus;
        let mut sandbox = minimal_sandbox(name);
        sandbox.status = Some(SandboxStatus {
            snapshot_runsc_version: version.map(str::to_owned),
            ..Default::default()
        });
        sandbox
    }

    /// #545 Capability B: a restore Pod for a snapshot taken at runsc version X
    /// is pinned (via `nodeAffinity`) to nodes advertising that exact version, on
    /// top of the existing #272 envd-staged `Exists` gate. A mismatched fleet then
    /// leaves the Pod `Pending` rather than crash-looping `runsc restore`.
    #[test]
    fn restore_pod_gates_on_recorded_runsc_version() {
        let sandbox =
            sandbox_with_snapshot_runsc_version("sbx-runsc-gate", Some("release-20260525.0"));
        let placement = RestorePlacement {
            staging_path: "/var/lib/sandbawks/staging/sbx-runsc-gate".into(),
        };
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", Some(&placement));
        let reqs = node_affinity_requirements(&pod);

        // The #272 envd-staged Exists gate is still present.
        assert!(
            reqs.iter()
                .any(|r| r.key == ENVD_STAGED_LABEL && r.operator == "Exists"),
            "restore Pod must keep the envd-staged Exists gate (#272)"
        );

        // The #545 runsc-version gate is an `In` match on the recorded version.
        let runsc = reqs
            .iter()
            .find(|r| r.key == RUNSC_VERSION_LABEL)
            .expect("restore Pod must carry a runsc-version nodeAffinity (#545)");
        assert_eq!(runsc.operator, "In");
        assert_eq!(
            runsc.values.as_deref(),
            Some(["release-20260525.0".to_owned()].as_slice()),
            "the gate must pin to the snapshot's recorded runsc version"
        );
    }

    /// A cold-start Pod (`restore == None`) must NOT carry the runsc-version gate
    /// even when the Sandbox happens to have a recorded version: a cold start has
    /// no checkpoint to match and must stay schedulable across the whole fleet.
    #[test]
    fn cold_start_pod_has_no_runsc_version_gate() {
        let sandbox =
            sandbox_with_snapshot_runsc_version("sbx-cold-runsc", Some("release-20260525.0"));
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        let reqs = node_affinity_requirements(&pod);

        assert!(
            reqs.iter()
                .any(|r| r.key == ENVD_STAGED_LABEL && r.operator == "Exists"),
            "cold-start Pod keeps the envd-staged gate"
        );
        assert!(
            !reqs.iter().any(|r| r.key == RUNSC_VERSION_LABEL),
            "cold-start Pod must not be gated on a runsc version (no checkpoint to match)"
        );
    }

    /// A restore Pod for a snapshot with NO recorded runsc version (a pre-#545
    /// snapshot, or one a node couldn't stamp) degrades to the envd-staged gate
    /// only — pinning to an empty label value would strand every such restore.
    #[test]
    fn restore_pod_without_recorded_version_degrades_to_envd_only() {
        for version in [None, Some("")] {
            let sandbox = sandbox_with_snapshot_runsc_version("sbx-no-runsc", version);
            let placement = RestorePlacement {
                staging_path: "/var/lib/sandbawks/staging/sbx-no-runsc".into(),
            };
            let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", Some(&placement));
            let reqs = node_affinity_requirements(&pod);

            assert!(
                reqs.iter()
                    .any(|r| r.key == ENVD_STAGED_LABEL && r.operator == "Exists"),
                "restore Pod keeps the envd-staged gate even without a recorded version"
            );
            assert!(
                !reqs.iter().any(|r| r.key == RUNSC_VERSION_LABEL),
                "an absent/empty recorded version must not pin the restore Pod (version={version:?})"
            );
        }
    }

    /// End-to-end through `pod_spec_for`: a customer who pins
    /// `OTEL_RESOURCE_ATTRIBUTES` in `spec.env` does not lose sandbox identity
    /// (issue #435). The built container env carries exactly one
    /// `OTEL_RESOURCE_ATTRIBUTES` entry (no duplicate from the raw spec.env pass-
    /// through) and it contains both the customer's attr and the identity attrs.
    #[test]
    fn pod_spec_customer_pinned_otel_attrs_keep_identity() {
        let mut sandbox = make_sandbox("sbx-pin", "ns", Some("uid-pin"), "tmpl");
        sandbox
            .spec
            .env
            .insert(OTEL_RESOURCE_ATTRIBUTES_ENV.into(), "team=blue".into());

        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        let env = pod
            .spec
            .expect("pod spec")
            .containers
            .into_iter()
            .next()
            .expect("envd container")
            .env
            .expect("container env");

        let attrs: Vec<&EnvVar> = env
            .iter()
            .filter(|e| e.name == OTEL_RESOURCE_ATTRIBUTES_ENV)
            .collect();
        assert_eq!(
            attrs.len(),
            1,
            "exactly one OTEL_RESOURCE_ATTRIBUTES env var (no duplicate): {env:?}"
        );
        let value = attrs[0].value.as_deref().expect("attrs value");
        assert!(
            value.contains("team=blue"),
            "customer attr retained: {value}"
        );
        assert!(
            value.contains("k8s.pod.name=sbx-pin") && value.contains("sandbox.uid=uid-pin"),
            "sandbox identity attrs must survive a customer pin: {value}"
        );
    }

    /// Issue #494: customer `spec.env` values are emitted as
    /// `valueFrom.secretKeyRef` against the per-sandbox env Secret, never as
    /// an inline `value`. The built container env carries a `SECRET` var with
    /// `value: None` and a populated `secret_key_ref`, and **no** `EnvVar` in
    /// the whole Pod carries the literal secret string.
    #[test]
    fn pod_spec_env_value_is_secret_backed_not_inlined() {
        let mut sandbox = make_sandbox("sbx-leak", "ns", Some("uid-leak"), "tmpl");
        sandbox.spec.env.insert("SECRET".into(), "shhh".into());

        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        let env = pod
            .spec
            .expect("pod spec")
            .containers
            .into_iter()
            .next()
            .expect("envd container")
            .env
            .expect("container env");

        let secret_var = env
            .iter()
            .find(|e| e.name == "SECRET")
            .expect("SECRET env var present");
        assert_eq!(
            secret_var.value, None,
            "customer env must not be inlined as a plaintext value: {secret_var:?}"
        );
        let key_ref = secret_var
            .value_from
            .as_ref()
            .and_then(|s| s.secret_key_ref.as_ref())
            .expect("SECRET must resolve via secretKeyRef");
        assert_eq!(
            key_ref.name, "sandbox-sbx-leak-env",
            "secretKeyRef points at the deterministic per-sandbox env Secret"
        );
        assert_eq!(key_ref.key, "SECRET", "secretKeyRef keys off the env name");

        // The literal value must appear nowhere in the Pod's env — not as an
        // inline `value`, not folded into any other var.
        assert!(
            env.iter().all(|e| e.value.as_deref() != Some("shhh")),
            "the cleartext secret value must never appear in the Pod spec: {env:?}"
        );
    }

    /// Issue #494: the generated env Secret carries the `spec.env` payload in
    /// `string_data` and an owner reference back to the Sandbox so it is
    /// garbage-collected with the CR.
    #[test]
    fn env_secret_carries_payload_and_owner_reference() {
        let mut sandbox = make_sandbox("sbx-sec", "ns", Some("uid-sec"), "tmpl");
        sandbox
            .spec
            .env
            .insert("ANTHROPIC_API_KEY".into(), "sk-xyz".into());
        sandbox.spec.env.insert("PLAIN".into(), "value".into());

        let secret = PodAdapter::env_secret_for(&sandbox).expect("non-empty env yields a Secret");

        assert_eq!(
            secret.metadata.name.as_deref(),
            Some("sandbox-sbx-sec-env"),
            "deterministic name derived from the sandbox"
        );
        assert_eq!(secret.metadata.namespace.as_deref(), Some("ns"));

        let data = secret.string_data.expect("string_data carries the payload");
        assert_eq!(
            data.get("ANTHROPIC_API_KEY").map(String::as_str),
            Some("sk-xyz")
        );
        assert_eq!(data.get("PLAIN").map(String::as_str), Some("value"));

        let owners = secret
            .metadata
            .owner_references
            .expect("Secret owner-references the Sandbox when it has a uid");
        assert_eq!(owners.len(), 1, "exactly one owner reference");
        assert_eq!(owners[0].kind, "Sandbox");
        assert_eq!(owners[0].name, "sbx-sec");
        assert_eq!(owners[0].uid, "uid-sec");
        assert_eq!(
            owners[0].controller,
            Some(true),
            "owner ref must be controlling so the GC tears the Secret down with the sandbox"
        );
    }

    /// Issue #494: `OTEL_RESOURCE_ATTRIBUTES` is consumed as an inline merged
    /// var in `pod_spec_for` (#435), never via `secretKeyRef`, so it is
    /// excluded from the Secret — the Secret holds exactly the keys the Pod
    /// references. A sandbox whose *only* `spec.env` entry is that key yields
    /// no Secret at all.
    #[test]
    fn env_secret_excludes_inline_consumed_otel_resource_attributes() {
        let mut sandbox = make_sandbox("sbx-otel", "ns", Some("uid-otel"), "tmpl");
        sandbox
            .spec
            .env
            .insert(OTEL_RESOURCE_ATTRIBUTES_ENV.into(), "team=blue".into());
        sandbox.spec.env.insert("PLAIN".into(), "value".into());

        let secret = PodAdapter::env_secret_for(&sandbox).expect("non-empty env yields a Secret");
        let data = secret.string_data.expect("string_data carries the payload");
        assert_eq!(data.get("PLAIN").map(String::as_str), Some("value"));
        assert!(
            !data.contains_key(OTEL_RESOURCE_ATTRIBUTES_ENV),
            "inline-consumed OTEL_RESOURCE_ATTRIBUTES must not be stored in the Secret: {data:?}"
        );

        // Only the inline-consumed key ⇒ nothing left to store ⇒ no Secret.
        let mut otel_only = make_sandbox("sbx-otel-only", "ns", Some("uid-otel-only"), "tmpl");
        otel_only
            .spec
            .env
            .insert(OTEL_RESOURCE_ATTRIBUTES_ENV.into(), "team=blue".into());
        assert!(
            PodAdapter::env_secret_for(&otel_only).is_none(),
            "a spec.env holding only the inline-consumed key must not create a Secret"
        );
    }

    /// Issue #494: an empty `spec.env` produces no Secret (avoids a dangling
    /// empty Secret) and no secret-backed env vars.
    #[test]
    fn empty_env_produces_no_secret() {
        let sandbox = make_sandbox("sbx-empty", "ns", Some("uid-empty"), "tmpl");
        assert!(
            PodAdapter::env_secret_for(&sandbox).is_none(),
            "empty spec.env must not create a Secret"
        );

        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", None);
        let env = pod
            .spec
            .expect("pod spec")
            .containers
            .into_iter()
            .next()
            .expect("envd container")
            .env
            .expect("container env");
        // No env var should reference the (nonexistent) env Secret.
        assert!(
            env.iter().all(|e| e
                .value_from
                .as_ref()
                .and_then(|s| s.secret_key_ref.as_ref())
                .is_none()),
            "empty spec.env must emit no secret-backed vars: {env:?}"
        );
    }

    /// Issue #494: the secret-ref rewrite applies uniformly to restore Pods,
    /// so cold-start and restore Pod env shapes stay topologically identical.
    #[test]
    fn restore_pod_env_is_also_secret_backed() {
        let mut sandbox = make_sandbox("sbx-restore", "ns", Some("uid-restore"), "tmpl");
        sandbox.spec.env.insert("SECRET".into(), "shhh".into());

        let placement = RestorePlacement {
            staging_path: "/var/lib/sandbawks/restore/sbx-restore".into(),
        };
        let pod = PodAdapter::pod_spec_for(&sandbox, "img:latest", Some(&placement));
        let env = pod
            .spec
            .expect("pod spec")
            .containers
            .into_iter()
            .next()
            .expect("envd container")
            .env
            .expect("container env");

        let secret_var = env
            .iter()
            .find(|e| e.name == "SECRET")
            .expect("SECRET env var present on restore Pod");
        assert_eq!(
            secret_var.value, None,
            "restore Pod must not inline the value"
        );
        assert!(
            secret_var
                .value_from
                .as_ref()
                .and_then(|s| s.secret_key_ref.as_ref())
                .is_some(),
            "restore Pod env must be secret-backed too"
        );
        assert!(
            env.iter().all(|e| e.value.as_deref() != Some("shhh")),
            "the cleartext secret value must never appear in a restore Pod spec: {env:?}"
        );
    }

    // ── pod_termination_cause unit tests (issue #418) ─────────────────────

    /// A live Pod (`phase: Running`, no terminated containers) is not terminal.
    #[test]
    fn live_pod_is_not_terminal() {
        let pod = Pod {
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some("Running".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(pod_termination_cause(&pod), None);
    }

    /// A Pod with `phase: Failed` and no eviction signal surfaces as `Other`.
    #[test]
    fn failed_phase_without_eviction_is_other() {
        let pod = Pod {
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some("Failed".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            pod_termination_cause(&pod),
            Some(PodTerminationCause::Other)
        );
    }

    /// A Pod with `status.reason = "Evicted"` surfaces the kubelet message.
    #[test]
    fn evicted_pod_surfaces_eviction_message() {
        let msg = "Pod ephemeral local storage usage exceeds the total limit of containers 256Mi.";
        let pod = Pod {
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some("Failed".into()),
                reason: Some("Evicted".into()),
                message: Some(msg.into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            pod_termination_cause(&pod),
            Some(PodTerminationCause::Evicted {
                message: msg.into()
            })
        );
    }

    /// An evicted Pod with no `status.message` gets a fallback eviction string.
    #[test]
    fn evicted_pod_without_message_gets_fallback() {
        let pod = Pod {
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some("Failed".into()),
                reason: Some("Evicted".into()),
                message: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        match pod_termination_cause(&pod) {
            Some(PodTerminationCause::Evicted { message }) => {
                assert!(!message.is_empty(), "fallback message must be non-empty");
            }
            other => panic!("expected Evicted, got {other:?}"),
        }
    }

    /// A container with `state.terminated.reason = "OOMKilled"` surfaces the
    /// memory limit when the Pod spec carries it.
    #[test]
    fn oomkilled_container_surfaces_memory_limit() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateTerminated, ContainerStatus, PodSpec,
        };
        use std::collections::BTreeMap;

        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    resources: Some(ResourceRequirements {
                        limits: Some({
                            let mut m = BTreeMap::new();
                            m.insert("memory".into(), Quantity("512Mi".into()));
                            m
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some("Running".into()),
                container_statuses: Some(vec![ContainerStatus {
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            reason: Some("OOMKilled".into()),
                            exit_code: 137,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            pod_termination_cause(&pod),
            Some(PodTerminationCause::OomKilled {
                memory_limit: Some("512Mi".into())
            })
        );
    }

    /// A container with `reason = "OOMKilled"` but no memory limit in spec
    /// still surfaces `OomKilled` with `memory_limit: None`.
    #[test]
    fn oomkilled_without_spec_limit_is_oomkilled_none() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateTerminated, ContainerStatus,
        };

        let pod = Pod {
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                container_statuses: Some(vec![ContainerStatus {
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            reason: Some("OOMKilled".into()),
                            exit_code: 137,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            pod_termination_cause(&pod),
            Some(PodTerminationCause::OomKilled { memory_limit: None })
        );
    }

    /// Eviction signal takes priority over a simultaneously-set OOMKilled
    /// container status.
    #[test]
    fn eviction_takes_priority_over_oomkill() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateTerminated, ContainerStatus,
        };

        let pod = Pod {
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some("Failed".into()),
                reason: Some("Evicted".into()),
                message: Some("memory pressure".into()),
                container_statuses: Some(vec![ContainerStatus {
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            reason: Some("OOMKilled".into()),
                            exit_code: 137,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            pod_termination_cause(&pod),
            Some(PodTerminationCause::Evicted {
                message: "memory pressure".into()
            }),
            "eviction (status.reason) takes priority over OOMKilled container signal"
        );
    }

    // ── per_sandbox_observability_env unit tests (issue #435) ─────────────

    /// Helper: build a minimal Sandbox for per_sandbox_observability_env tests.
    fn make_sandbox(name: &str, namespace: &str, uid: Option<&str>, template: &str) -> Sandbox {
        use sandbawks_types::{ResourceRequests, SandboxSpec};
        let mut sbx = Sandbox::new(
            name,
            SandboxSpec {
                template_ref: template.into(),
                runtime_class_name: None,
                resources: ResourceRequests {
                    cpu: "1".parse().expect("valid cpu"),
                    memory: "1Gi".parse().expect("valid memory"),
                    ephemeral_storage: None,
                },
                timeout_seconds: 60,
                auto_pause: false,
                desired_phase: None,
                env: Default::default(),
                metadata: Default::default(),
                network_policy: None,
                snapshot_retention: None,
                pvc_retention: None,
                tolerations: None,
                node_selector: None,
                security_context: None,
                persistence_mode: None,
                volume_claim_templates: None,
                restore_from: None,
            },
        );
        sbx.metadata.namespace = Some(namespace.into());
        sbx.metadata.uid = uid.map(str::to_owned);
        sbx
    }

    /// All four per-sandbox resource attributes appear in the injected
    /// `OTEL_RESOURCE_ATTRIBUTES` value when the Sandbox has a uid.
    #[test]
    fn per_sandbox_attrs_built_from_cr() {
        let sbx = make_sandbox(
            "sbx-abc123",
            "staging",
            Some("11111111-2222-3333-4444-555555555555"),
            "python-3.11",
        );
        let vars = per_sandbox_observability_env(&sbx, None, |_| None);
        let attrs_var = vars
            .iter()
            .find(|e| e.name == OTEL_RESOURCE_ATTRIBUTES_ENV)
            .expect("OTEL_RESOURCE_ATTRIBUTES must be present");
        let attrs = attrs_var.value.as_deref().expect("value must be Some");
        assert!(
            attrs.contains("k8s.pod.name=sbx-abc123"),
            "semconv pod name missing: {attrs}"
        );
        assert!(
            attrs.contains("k8s.namespace.name=staging"),
            "semconv namespace missing: {attrs}"
        );
        assert!(
            attrs.contains("sandbox.template=python-3.11"),
            "custom template key missing: {attrs}"
        );
        assert!(
            attrs.contains("sandbox.uid=11111111-2222-3333-4444-555555555555"),
            "custom uid key missing: {attrs}"
        );
    }

    /// When the Sandbox CR has no uid (e.g. unit test stubs that have not been
    /// admitted by the apiserver), `sandbox.uid` is absent — the output is
    /// still well-formed and the other three attrs are present.
    #[test]
    fn per_sandbox_attrs_omits_uid_when_absent() {
        let sbx = make_sandbox("sbx-no-uid", "default", None, "base");
        let vars = per_sandbox_observability_env(&sbx, None, |_| None);
        let attrs = vars
            .iter()
            .find(|e| e.name == OTEL_RESOURCE_ATTRIBUTES_ENV)
            .and_then(|e| e.value.as_deref())
            .expect("OTEL_RESOURCE_ATTRIBUTES present");
        assert!(
            !attrs.contains("sandbox.uid"),
            "uid key must not appear: {attrs}"
        );
        assert!(
            attrs.contains("k8s.pod.name=sbx-no-uid"),
            "pod name must still appear: {attrs}"
        );
    }

    /// The controller's cluster-level `OTEL_RESOURCE_ATTRIBUTES` is retained
    /// (merge-not-clobber). Both base and per-sandbox attrs appear in the
    /// output, separated by a comma.
    #[test]
    fn per_sandbox_attrs_merge_not_clobber() {
        let sbx = make_sandbox("sbx-merge", "prod", Some("uid-merge"), "tmpl");
        let vars = per_sandbox_observability_env(&sbx, None, |name| {
            (name == OTEL_RESOURCE_ATTRIBUTES_ENV)
                .then(|| "cluster.region=us-east-1,cluster.name=prod-a".to_owned())
        });
        let attrs = vars
            .iter()
            .find(|e| e.name == OTEL_RESOURCE_ATTRIBUTES_ENV)
            .and_then(|e| e.value.as_deref())
            .expect("OTEL_RESOURCE_ATTRIBUTES present");
        assert!(
            attrs.contains("cluster.region=us-east-1"),
            "base attr must be retained: {attrs}"
        );
        assert!(
            attrs.contains("cluster.name=prod-a"),
            "base attr must be retained: {attrs}"
        );
        assert!(
            attrs.contains("k8s.pod.name=sbx-merge"),
            "per-sandbox attr must be present: {attrs}"
        );
    }

    /// A customer-pinned `OTEL_RESOURCE_ATTRIBUTES` (spec.env) takes the place of
    /// the controller's cluster-level base, but the per-sandbox identity attrs
    /// are still appended after it — a customer cannot null out sandbox identity
    /// (issue #435), only override individual keys via OTel last-wins.
    #[test]
    fn customer_pinned_attrs_keep_identity() {
        let sbx = make_sandbox("sbx-cust", "ns", Some("uid-cust"), "tmpl");
        // Customer pins their own attrs AND the controller has a cluster base;
        // the customer's value wins for the base portion, controller's is dropped.
        let vars =
            per_sandbox_observability_env(&sbx, Some("team=blue,k8s.pod.name=spoof"), |name| {
                (name == OTEL_RESOURCE_ATTRIBUTES_ENV)
                    .then(|| "cluster.region=us-east-1".to_owned())
            });
        let attrs = vars
            .iter()
            .find(|e| e.name == OTEL_RESOURCE_ATTRIBUTES_ENV)
            .and_then(|e| e.value.as_deref())
            .expect("OTEL_RESOURCE_ATTRIBUTES present");
        // Customer base is honoured; controller cluster base is replaced by it.
        assert!(
            attrs.contains("team=blue"),
            "customer attr must be present: {attrs}"
        );
        assert!(
            !attrs.contains("cluster.region"),
            "controller base must be replaced by the customer pin: {attrs}"
        );
        // Identity attrs are present and win — the customer's `k8s.pod.name=spoof`
        // is overridden by the real per-sandbox value appended after it.
        assert!(
            attrs.ends_with(",sandbox.uid=uid-cust") || attrs.contains(",sandbox.uid=uid-cust,"),
            "sandbox.uid identity attr must be appended: {attrs}"
        );
        let spoof_pos = attrs
            .find("k8s.pod.name=spoof")
            .expect("customer key present");
        let real_pos = attrs
            .rfind("k8s.pod.name=sbx-cust")
            .expect("identity key present");
        assert!(
            real_pos > spoof_pos,
            "real k8s.pod.name must appear after the customer's spoof (last-wins): {attrs}"
        );
    }

    /// Per-sandbox attrs are appended last so they win on key collision.
    /// OTel spec: the last definition of a duplicate key takes precedence in
    /// `OTEL_RESOURCE_ATTRIBUTES`.
    #[test]
    fn per_sandbox_key_precedence_last_wins() {
        let sbx = make_sandbox("sbx-win", "ns", Some("uid-win"), "tmpl");
        // Base sets k8s.pod.name to a stale value; per-sandbox must override it.
        let vars = per_sandbox_observability_env(&sbx, None, |name| {
            (name == OTEL_RESOURCE_ATTRIBUTES_ENV).then(|| "k8s.pod.name=stale-name".to_owned())
        });
        let attrs = vars
            .iter()
            .find(|e| e.name == OTEL_RESOURCE_ATTRIBUTES_ENV)
            .and_then(|e| e.value.as_deref())
            .expect("OTEL_RESOURCE_ATTRIBUTES present");
        // The merged string is "k8s.pod.name=stale-name,k8s.pod.name=sbx-win,...".
        // OTel SDK resolves last-wins, so sbx-win is the effective value. Assert
        // ordering directly — per-sandbox must appear after the base entry — which
        // is the precise property the docstring claims.
        let base_pos = attrs.find("k8s.pod.name=stale-name").expect("base present");
        let sandbox_pos = attrs
            .rfind("k8s.pod.name=sbx-win")
            .expect("per-sandbox present");
        assert!(
            sandbox_pos > base_pos,
            "per-sandbox attr must appear after base attr (last-wins semantics): {attrs}"
        );
    }

    /// When no `OTEL_SERVICE_NAME` is set on the controller, the function
    /// injects `OTEL_SERVICE_NAME=sandbawks-envd` so envd spans and inner
    /// Claude Code agent spans land under a named service.
    #[test]
    fn per_sandbox_service_name_default_is_envd() {
        let sbx = make_sandbox("sbx-svc", "ns", None, "tmpl");
        let vars = per_sandbox_observability_env(&sbx, None, |_| None);
        let svc_var = vars
            .iter()
            .find(|e| e.name == OTEL_SERVICE_NAME_ENV)
            .expect("OTEL_SERVICE_NAME must be present when not set by operator");
        assert_eq!(svc_var.value.as_deref(), Some(ENVD_SERVICE_NAME));
        assert!(
            svc_var.value_from.is_none(),
            "must be a literal value, not a downward-API ref"
        );
    }

    /// Even when the operator has set `OTEL_SERVICE_NAME` on the controller,
    /// the function still injects `sandbawks-envd`: the controller's value names
    /// the *controller*, never envd, and the var is not forwarded, so honoring
    /// it would leave envd with the SDK `unknown_service` placeholder. A
    /// customer who wants a different name pins it via `spec.env`, which
    /// `pod_spec_for`'s customer_keys filter applies after this function.
    #[test]
    fn per_sandbox_service_name_always_envd_even_when_operator_set() {
        let sbx = make_sandbox("sbx-op-svc", "ns", None, "tmpl");
        let vars = per_sandbox_observability_env(&sbx, None, |name| {
            (name == OTEL_SERVICE_NAME_ENV).then(|| "my-custom-envd".to_owned())
        });
        let svc_var = vars
            .iter()
            .find(|e| e.name == OTEL_SERVICE_NAME_ENV)
            .expect("OTEL_SERVICE_NAME must be injected regardless of controller-side value");
        assert_eq!(svc_var.value.as_deref(), Some(ENVD_SERVICE_NAME));
    }

    /// `OTEL_RESOURCE_ATTRIBUTES` is no longer in `FORWARDED_OTEL_VARS` — it is
    /// handled entirely by `per_sandbox_observability_env` to enable the
    /// merge-not-clobber semantics. If it crept back in, it would produce a
    /// duplicate key in the env vec (the raw controller value followed by the
    /// merged per-sandbox value), which would make the merge logic inert.
    #[test]
    fn otel_resource_attributes_not_in_forwarded_vars() {
        assert!(
            !FORWARDED_OTEL_VARS.contains(&OTEL_RESOURCE_ATTRIBUTES_ENV),
            "OTEL_RESOURCE_ATTRIBUTES must not be forwarded verbatim; \
             per_sandbox_observability_env owns the merge"
        );
    }

    /// `OTEL_SERVICE_NAME` is not in `FORWARDED_OTEL_VARS` — forwarding the
    /// controller's own service name (`sandbawks-controller`) into envd would
    /// be incorrect. The per_sandbox function injects the right value instead.
    #[test]
    fn otel_service_name_not_in_forwarded_vars() {
        assert!(
            !FORWARDED_OTEL_VARS.contains(&OTEL_SERVICE_NAME_ENV),
            "OTEL_SERVICE_NAME must not be forwarded from the controller (wrong service name); \
             per_sandbox_observability_env owns this injection"
        );
    }

    /// #560/#620: the status-line classifier. Locks down the byte slicing and
    /// the intentional HTTP/2-is-not-serving decision.
    #[test]
    fn classify_serving_response_accepts_only_http1_200() {
        assert!(classify_serving_response(
            b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"
        ));
        assert!(classify_serving_response(b"HTTP/1.0 200 OK"));
        assert!(!classify_serving_response(
            b"HTTP/1.1 503 Service Unavailable"
        ));
        assert!(!classify_serving_response(b"HTTP/1.1 500\r\n"));
        // HTTP/2 reply is deliberately rejected (envd's :49984 is HTTP/1 today).
        assert!(!classify_serving_response(b"HTTP/2 200\r\n"));
        // Truncated before the status code, and empty.
        assert!(!classify_serving_response(b"HTTP/1.1 2"));
        assert!(!classify_serving_response(b""));
    }

    /// #560/#620: the real probe (`probe_envd_serving`) against a local
    /// `TcpListener`,
    /// covering a clean 200, a non-200, a one-byte-at-a-time 200 (exercising the
    /// `head.len() >= 12` accumulation loop), a reply that closes before a full
    /// status line, and a refused connection.
    #[tokio::test]
    async fn probe_envd_serving_classifies_socket_responses() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Bind an ephemeral listener that answers one connection with `response`
        // (optionally one byte at a time), then closes. Returns its address.
        async fn serve_once(response: &'static [u8], drip: bool) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            tokio::spawn(async move {
                if let Ok((mut sock, _)) = listener.accept().await {
                    // Drain the request so the client's write never blocks.
                    let mut req = [0u8; 256];
                    let _ = sock.read(&mut req).await;
                    if drip {
                        for byte in response {
                            if sock.write_all(std::slice::from_ref(byte)).await.is_err() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        }
                    } else {
                        let _ = sock.write_all(response).await;
                    }
                    let _ = sock.shutdown().await;
                }
            });
            addr
        }

        // Clean 200 → serving.
        assert!(
            probe_envd_serving(
                &serve_once(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n", false).await
            )
            .await,
            "a clean HTTP/1.1 200 must classify as serving"
        );
        // Non-200 → not serving.
        assert!(
            !probe_envd_serving(
                &serve_once(b"HTTP/1.1 500 Internal Server Error\r\n\r\n", false).await
            )
            .await,
            "a 500 must classify as not serving"
        );
        // 200 dripped one byte at a time → still serving (exercises the loop).
        assert!(
            probe_envd_serving(&serve_once(b"HTTP/1.1 200 OK\r\n\r\n", true).await).await,
            "a byte-fragmented 200 must still classify as serving"
        );
        // Reply closes before a full status line (< 12 bytes) → not serving.
        assert!(
            !probe_envd_serving(&serve_once(b"HTTP/1.1 2", false).await).await,
            "a reply that closes before the status code must classify as not serving"
        );
        // Nothing listening (connection refused) → not serving.
        assert!(
            !probe_envd_serving("127.0.0.1:1").await,
            "a refused connection must classify as not serving"
        );
    }

    // ---------- summarize_pod_status (issue #625) ----------

    /// A `Pending` Pod with a backing-off image pull surfaces the blocking
    /// container reason and message.
    #[test]
    fn summarize_surfaces_container_waiting_reason() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateWaiting, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            status: Some(PodStatus {
                phase: Some("Pending".into()),
                container_statuses: Some(vec![ContainerStatus {
                    state: Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some("ImagePullBackOff".into()),
                            message: Some("back-off pulling image \"nope\"".into()),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let summary = summarize_pod_status(&pod).expect("pod has status");
        assert_eq!(summary.phase, "Pending");
        assert_eq!(summary.reason.as_deref(), Some("ImagePullBackOff"));
        assert_eq!(
            summary.message.as_deref(),
            Some("back-off pulling image \"nope\"")
        );
    }

    /// An init container's blocking reason wins over a later app container's,
    /// because a blocked init container is what actually holds the Pod Pending.
    #[test]
    fn summarize_prefers_init_container_reason() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateWaiting, ContainerStatus, PodStatus,
        };
        let waiting = |reason: &str| ContainerStatus {
            state: Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some(reason.into()),
                    message: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let pod = Pod {
            status: Some(PodStatus {
                phase: Some("Pending".into()),
                init_container_statuses: Some(vec![waiting("CreateContainerError")]),
                container_statuses: Some(vec![waiting("PodInitializing")]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let summary = summarize_pod_status(&pod).expect("pod has status");
        assert_eq!(summary.reason.as_deref(), Some("CreateContainerError"));
    }

    /// A Pod the scheduler can't place surfaces its `PodScheduled=False`
    /// reason rather than a bare `Pending`.
    #[test]
    fn summarize_surfaces_unschedulable_from_condition() {
        use k8s_openapi::api::core::v1::{PodCondition, PodStatus};
        let pod = Pod {
            status: Some(PodStatus {
                phase: Some("Pending".into()),
                conditions: Some(vec![PodCondition {
                    type_: "PodScheduled".into(),
                    status: "False".into(),
                    reason: Some("Unschedulable".into()),
                    message: Some("0/3 nodes are available".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let summary = summarize_pod_status(&pod).expect("pod has status");
        assert_eq!(summary.reason.as_deref(), Some("Unschedulable"));
    }

    /// A healthy, running Pod projects its phase with no blocking reason.
    #[test]
    fn summarize_running_pod_has_no_reason() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateRunning, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            status: Some(PodStatus {
                phase: Some("Running".into()),
                container_statuses: Some(vec![ContainerStatus {
                    state: Some(ContainerState {
                        running: Some(ContainerStateRunning::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let summary = summarize_pod_status(&pod).expect("pod has status");
        assert_eq!(summary.phase, "Running");
        assert_eq!(summary.reason, None);
    }

    /// A Pod with no `status` at all projects nothing.
    #[test]
    fn summarize_none_when_no_status() {
        assert_eq!(summarize_pod_status(&Pod::default()), None);
    }
}
