//! Warm-pool fast path for `POST /sandboxes` (issue #332).
//!
//! When a [`SandboxWarmPool`] covers the requested `templateRef` and has
//! a `Running` member to spare, the gateway *claims* that member instead
//! of cold-booting a fresh sandbox. The claim is a control-plane
//! reassignment: it removes the pool label (atomically, guarded by the
//! candidate's `resourceVersion`) and patches the requester's overrides
//! onto the *already-`Running`* sandbox — no pod creation, no restore, no
//! resume. The member is live, so the gateway returns it straight away.
//! The claimed sandbox keeps its existing name — Kubernetes names are
//! immutable — and that name becomes the E2B `sandboxID`.
//!
//! The claim is a compare-and-swap against the apiserver: two concurrent
//! requests racing for the same candidate both PATCH with the observed
//! `resourceVersion`; exactly one lands, the loser gets `409 Conflict`
//! and [`claim_for`] retries against the next candidate. No lock CRD, no
//! leader election — the apiserver's optimistic concurrency is the lock.
//!
//! [`WarmClaim`] abstracts the four `kube` calls the flow makes so the
//! orchestration in [`claim_for`] is unit-testable against an in-memory
//! fake; production wires [`KubeWarmClaim`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use kube::api::{ListParams, Patch, PatchParams};
use kube::{Api, ResourceExt};
use sandbawks_types::warm_pool::{CLAIMED_AT_ANNOTATION, WARM_POOL_LABEL};
use sandbawks_types::{NetworkPolicy, Sandbox, SandboxPhase, SandboxSpec, SandboxWarmPool};

use crate::error::AppError;
use crate::warm_claim_metrics::{record_claim, record_handoff, ClaimOutcome};

/// Requester-supplied fields patched onto a claimed warm sandbox so it
/// behaves as if it had been cold-booted for this caller.
///
/// `runtime_class_name` is included because the gateway resolves the
/// template's `defaultRuntimeClass` before the claim; the warm member was
/// created carrying it already, but the requester's resolved class is
/// re-stamped so a later pause/resume keeps working under gVisor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClaimOverrides {
    pub env: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
    pub timeout_seconds: u32,
    pub runtime_class_name: Option<String>,
    /// Egress policy to stamp onto the claimed member's `spec.networkPolicy`
    /// (issue #542). `Some` only when the create request carried a per-create
    /// `egressAllow`, in which case it already holds the **union** of the
    /// template baseline and the per-create destinations (computed up-front by
    /// the create route's `merge_egress_policies`, matching the cold-start
    /// path). `None` leaves the member's template baseline untouched — minted
    /// warm members already carry it, so an override-free claim must not
    /// rewrite it. Not extracted by [`Self::from_spec`]; set explicitly by
    /// [`claim_for`] from its `egress_override` argument.
    pub network_policy: Option<NetworkPolicy>,
    /// Owning `TeamAPIKey` name to stamp as the [`API_KEY_NAME_ANNOTATION`] on
    /// the claimed member, for per-key usage attribution (epic #480 Phase 2,
    /// issue #631). A minted warm member carries no owning key (it is claimed by
    /// whichever team wins the race), so the claiming request's key is stamped
    /// here at claim time — the warm-path analogue of the cold create stamping it
    /// in `routes::sandboxes::create_sandbox`. `None`/empty leaves the annotation
    /// off. Not extracted by [`Self::from_spec`] (it reads the spec, not metadata);
    /// set explicitly by [`claim_for`] from the requester's stamped annotation.
    pub api_key_name: Option<String>,
}

impl ClaimOverrides {
    /// Extract the override-able fields from a freshly-built request spec.
    ///
    /// `network_policy` is intentionally left `None` here: the egress override
    /// is conditional on a per-create `egressAllow` being present, which the
    /// post-merge `spec.networkPolicy` (template baseline ∪ per-create) can't
    /// distinguish from a template-only baseline. [`claim_for`] sets it from
    /// the explicit `egress_override` argument instead.
    pub fn from_spec(spec: &SandboxSpec) -> Self {
        Self {
            env: spec.env.clone(),
            metadata: spec.metadata.clone(),
            timeout_seconds: spec.timeout_seconds,
            runtime_class_name: spec.runtime_class_name.clone(),
            network_policy: None,
            api_key_name: None,
        }
    }
}

/// The `kube` operations [`claim_for`] needs, abstracted for testing.
#[async_trait]
pub trait WarmClaim: Send + Sync {
    /// Name of a [`SandboxWarmPool`] in `namespace` whose
    /// `spec.templateRef` equals `template_ref`, if one exists.
    async fn pool_for_template(
        &self,
        namespace: &str,
        template_ref: &str,
    ) -> Result<Option<String>, AppError>;

    /// `Running` members of `pool_name` (each carrying its
    /// `resourceVersion` so the caller can guard the claim).
    async fn running_candidates(
        &self,
        namespace: &str,
        pool_name: &str,
    ) -> Result<Vec<Sandbox>, AppError>;

    /// Atomically claim `name` by removing [`WARM_POOL_LABEL`], guarded
    /// by `resource_version`.
    ///
    /// Returns `Ok(true)` if the claim landed, `Ok(false)` if the patch
    /// was rejected with `409 Conflict` (another request won the race —
    /// the caller should try the next candidate).
    async fn relabel_claim(
        &self,
        namespace: &str,
        name: &str,
        resource_version: Option<String>,
        claimed_at: &str,
    ) -> Result<bool, AppError>;

    /// Patch the requester's overrides onto the claimed (already-`Running`)
    /// sandbox and return the patched object so the caller can render its
    /// DTO. The member is live, so there is no resume to drive.
    ///
    /// Writes `overrides.timeout_seconds` — the raw idle window (issue #481)
    /// — to `spec.timeoutSeconds`, and stamps `status.lastActivityTimestamp`
    /// to `claimed_at` so the controller measures the idle window from the
    /// *claim* instant rather than the member's (earlier) mint time (#380):
    /// a warm member's `creationTimestamp` is its mint time, so without the
    /// stamp the controller's creation fallback would place the deadline in
    /// the past and insta-pause the member on claim. `timeout_seconds == 0`
    /// is the "idle timeout disabled" sentinel — no window, no stamp.
    async fn apply_overrides(
        &self,
        namespace: &str,
        name: &str,
        overrides: &ClaimOverrides,
        claimed_at: &str,
    ) -> Result<Sandbox, AppError>;
}

/// Try to claim a live (`Running`) warm sandbox for `requested`.
///
/// Returns `Ok(Some(sandbox))` with the claimed sandbox (its name is the
/// `sandboxID` to return), or `Ok(None)` when there is no covering pool
/// or the pool drained out from under the request — in which case
/// `create_sandbox` falls through to its cold-boot path.
///
/// `egress_override` carries the per-create egress policy to stamp onto the
/// claimed member (issue #542): `Some` when the request supplied a non-empty
/// `egressAllow`, already unioned with the template baseline (so the claimed
/// member's egress matches the cold-start path); `None` to leave the member's
/// template baseline untouched.
///
/// # Errors
///
/// Propagates any backing `kube` error other than the expected `409` on
/// a lost claim race (which is folded into the retry loop).
pub async fn claim_for(
    backend: &dyn WarmClaim,
    namespace: &str,
    requested: &Sandbox,
    claimed_at: &str,
    egress_override: Option<&NetworkPolicy>,
) -> Result<Option<Sandbox>, AppError> {
    // Start the handoff clock at entry so the `hit` latency histogram covers the
    // whole claim path (pool lookup, candidate list, CAS, overrides patch) — the
    // sub-second figure the feature advertises (#288).
    let started = Instant::now();
    let template = &requested.spec.template_ref;

    // A request carrying per-create `volumeClaimTemplates` cannot be served by a
    // pre-warmed member: that member was minted from the template alone and has
    // no PVCs bound for this request's claims, and re-binding storage onto a live
    // sandbox is not a control-plane reassignment (epic #603 Phase 4 / issue
    // #607). Fall through to cold-boot, which provisions the PVCs in
    // `PodAdapter::create_pod`. Skip before the pool lookup — the incompatibility
    // is structural, independent of whether a pool exists — and record the
    // terminal `miss_incompatible` outcome so the warm-vs-cold SLI denominator
    // stays complete. `Some(empty)` reads as "explicitly no volumes" and is
    // claimable, matching the `None ⇒ inherit` / `Some(empty) ⇒ none` precedence
    // the controller's resolution uses.
    if requested
        .spec
        .volume_claim_templates
        .as_deref()
        .is_some_and(|vcts| !vcts.is_empty())
    {
        record_claim(None, ClaimOutcome::MissIncompatible);
        tracing::debug!(
            outcome = "miss_incompatible",
            namespace = %namespace,
            template = %template,
            "create requests per-create volumeClaimTemplates; falling through to cold-boot"
        );
        return Ok(None);
    }
    // Every backing-store call below records `outcome="error"` before
    // propagating, so the terminal outcomes partition *all* claim attempts —
    // not just the ones that completed without a kube failure (#288 review).
    // `pool` is `None` until `pool_for_template` resolves it.
    let Some(pool) = backend
        .pool_for_template(namespace, template)
        .await
        .inspect_err(|_| record_claim(None, ClaimOutcome::Error))?
    else {
        // Common path (no pool covers this template); silent in the log so it
        // stays signal-rich, but counted so the warm-vs-cold SLI denominator is
        // complete (#288). `pool=None` → the `none` label marker.
        record_claim(None, ClaimOutcome::MissNoPool);
        return Ok(None);
    };

    let mut overrides = ClaimOverrides::from_spec(&requested.spec);
    // Stamp the per-create egress override (already unioned with the template
    // baseline) onto the claim so `apply_overrides` patches the member's
    // `spec.networkPolicy`; the controller then reconciles the live policy to
    // match (issue #542). `None` leaves the member's baseline untouched.
    overrides.network_policy = egress_override.cloned();
    // Carry the owning `TeamAPIKey` name from the requester (the create handler
    // stamped it on `requested` before this claim) so `apply_overrides` brands the
    // claimed member with it for per-key usage attribution (#631). A minted member
    // has no owning key until it is claimed.
    overrides.api_key_name = requested
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(sandbawks_types::API_KEY_NAME_ANNOTATION))
        .filter(|v| !v.is_empty())
        .cloned();
    let candidates = backend
        .running_candidates(namespace, &pool)
        .await
        .inspect_err(|_| record_claim(Some(&pool), ClaimOutcome::Error))?;
    let mut races_lost = 0u32;
    for candidate in candidates {
        let name = candidate.name_any();
        let rv = candidate.resource_version();
        // Lost the CAS → another request claimed this one; try the next.
        if backend
            .relabel_claim(namespace, &name, rv, claimed_at)
            .await
            .inspect_err(|_| record_claim(Some(&pool), ClaimOutcome::Error))?
        {
            // Anchor the idle-timeout deadline at *this* claim, not at the
            // member's (earlier) mint time. The controller measures the idle
            // window from `status.lastActivityTimestamp`, so `apply_overrides`
            // stamps it to `claimed_at` while writing the raw window to
            // `spec.timeoutSeconds` — landing the deadline at `claim +
            // duration` (#380 / #481). `0` (disabled timeout) is preserved.
            let claimed = backend
                .apply_overrides(namespace, &name, &overrides, claimed_at)
                .await
                .inspect_err(|_| record_claim(Some(&pool), ClaimOutcome::Error))?;
            // The feature's headline event: a warm hit. Record the handoff
            // latency (entry → overrides patch return) and the terminal `hit`
            // outcome (#288) before the log line.
            record_handoff(&pool, started.elapsed());
            record_claim(Some(&pool), ClaimOutcome::Hit);
            // Logged at info with the structured fields an operator needs to
            // reason about pool sizing and contention alongside the metrics.
            tracing::info!(
                outcome = "hit",
                pool = %pool,
                namespace = %namespace,
                template = %template,
                sandbox = %name,
                races_lost,
                "claimed warm sandbox"
            );
            return Ok(Some(claimed));
        }
        // Lost this candidate's CAS — count it as a contention event (#288).
        // This is per-CAS, not terminal: the loop retries the next candidate.
        record_claim(Some(&pool), ClaimOutcome::RaceLost);
        races_lost += 1;
    }
    // Every candidate was claimed by a concurrent request between our list
    // and our patch — fall through to cold-boot rather than spin.
    record_claim(Some(&pool), ClaimOutcome::MissDrained);
    tracing::debug!(
        outcome = "miss_drained",
        pool = %pool,
        namespace = %namespace,
        template = %template,
        races_lost,
        "warm pool drained under contention; falling through to cold-boot"
    );
    Ok(None)
}

/// [`WarmClaim`] backend used by the production gateway. Builds namespaced
/// `Api`s from the tenant's impersonating client so claim/patch operations
/// are apiserver-isolated to the tenant namespace (#442).
pub struct KubeWarmClaim {
    clients: Arc<crate::tenant_client::TenantClients>,
}

impl KubeWarmClaim {
    /// Build a claim backend over the per-namespace impersonating clients.
    pub fn new(clients: Arc<crate::tenant_client::TenantClients>) -> Self {
        Self { clients }
    }

    fn sandboxes(&self, namespace: &str) -> Result<Api<Sandbox>, AppError> {
        Ok(Api::namespaced(
            self.clients.for_namespace(namespace)?,
            namespace,
        ))
    }

    fn pools(&self, namespace: &str) -> Result<Api<SandboxWarmPool>, AppError> {
        Ok(Api::namespaced(
            self.clients.for_namespace(namespace)?,
            namespace,
        ))
    }
}

#[async_trait]
impl WarmClaim for KubeWarmClaim {
    #[tracing::instrument(
        skip_all,
        name = "kube:api::pool_for_template",
        err,
        fields(otel.kind = "client", template = %template_ref, namespace = %namespace)
    )]
    async fn pool_for_template(
        &self,
        namespace: &str,
        template_ref: &str,
    ) -> Result<Option<String>, AppError> {
        // Metric counterpart of the `kube:api::pool_for_template` span (#373).
        crate::kube_op_metrics::timed("warmpool", "pool_for_template", async move {
            let pools = self.pools(namespace)?.list(&ListParams::default()).await?;
            Ok(pools
                .into_iter()
                .find(|p| p.spec.template_ref == template_ref)
                .map(|p| p.name_any()))
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::running_candidates",
        err,
        fields(otel.kind = "client", pool = %pool_name, namespace = %namespace)
    )]
    async fn running_candidates(
        &self,
        namespace: &str,
        pool_name: &str,
    ) -> Result<Vec<Sandbox>, AppError> {
        // Metric counterpart of the `kube:api::running_candidates` span (#373).
        crate::kube_op_metrics::timed("sandbox", "running_candidates", async move {
            let lp = ListParams::default().labels(&format!("{WARM_POOL_LABEL}={pool_name}"));
            Ok(self
                .sandboxes(namespace)?
                .list(&lp)
                .await?
                .into_iter()
                .filter(|s| s.status.as_ref().map(|st| st.phase) == Some(SandboxPhase::Running))
                .collect())
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::relabel_claim",
        err,
        fields(otel.kind = "client", sandbox = %name, namespace = %namespace)
    )]
    async fn relabel_claim(
        &self,
        namespace: &str,
        name: &str,
        resource_version: Option<String>,
        claimed_at: &str,
    ) -> Result<bool, AppError> {
        // Metric counterpart of the `kube:api::relabel_claim` span (#373). A
        // 409/404 folds to `Ok(false)` below, so a lost CAS records `success`
        // here — the kube call itself succeeded; the lost race is the
        // orchestration layer's `warm.claims_total{outcome="race_lost"}`.
        crate::kube_op_metrics::timed("sandbox", "relabel_claim", async move {
            // JSON merge patch (RFC 7396): a `null` value deletes the key, so
            // setting the label to null removes the sandbox from the pool.
            // Including `metadata.resourceVersion` makes the apiserver reject
            // the patch with 409 if our observed version is stale — that is
            // the compare-and-swap that serializes concurrent claimers.
            let patch = serde_json::json!({
                "metadata": {
                    "resourceVersion": resource_version,
                    "labels": { WARM_POOL_LABEL: serde_json::Value::Null },
                    "annotations": { CLAIMED_AT_ANNOTATION: claimed_at },
                }
            });
            match self
                .sandboxes(namespace)?
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => Ok(true),
                // 409 = stale `resourceVersion` (another claimer won the CAS).
                // 404 = the candidate was deleted/consumed between our list and
                // this patch (another replica claimed it, a trim removed it, or
                // it was killed). Both are "lost the candidate" — fold to
                // `Ok(false)` so the loop tries the next one rather than
                // surfacing a 5xx to a caller who'd happily cold-boot.
                Err(kube::Error::Api(ae)) if ae.code == 409 || ae.code == 404 => Ok(false),
                Err(e) => Err(AppError::from(e)),
            }
        })
        .await
    }

    #[tracing::instrument(
        skip_all,
        name = "kube:api::apply_overrides",
        err,
        fields(otel.kind = "client", sandbox = %name, namespace = %namespace)
    )]
    async fn apply_overrides(
        &self,
        namespace: &str,
        name: &str,
        overrides: &ClaimOverrides,
        claimed_at: &str,
    ) -> Result<Sandbox, AppError> {
        // Metric counterpart of the `kube:api::apply_overrides` span (#373).
        crate::kube_op_metrics::timed("sandbox", "apply_overrides", async move {
            // Stamp the active trace context into the overrides patch so the
            // controller's reconcile of this claim joins the request's trace
            // (OTel Phase 5). This is the warm-path analogue of the cold create's
            // annotation in `routes::sandboxes::create_sandbox`; it refreshes any
            // traceparent the warm member carried from when the pool created it,
            // so the reconcile follows *this* claim rather than the speculative
            // top-up. Merging an empty map (tracing disabled) is inert under RFC
            // 7396, so the key is always safe to include.
            let mut annotations = BTreeMap::new();
            sandbawks_observability::inject_into_annotations(&mut annotations);
            // Stamp the configured idle window so the claimed member carries the
            // same `TIMEOUT_DURATION_ANNOTATION` a cold-create does (a
            // human-facing record mirroring `spec.timeoutSeconds`, issue #481).
            // `0` is the "disabled" sentinel — leave the annotation off to match
            // the annotation-free shape of an unclaimed warm member.
            if overrides.timeout_seconds > 0 {
                annotations.insert(
                    crate::routes::sandboxes::TIMEOUT_DURATION_ANNOTATION.to_owned(),
                    overrides.timeout_seconds.to_string(),
                );
            }
            // Brand the claimed member with the owning `TeamAPIKey` name for
            // per-key usage attribution (#631); rides the same metadata.annotations
            // merge patch below. Omitted when absent so an un-stamped claim leaves
            // the member annotation-free (matching a keyless / operator-applied CR).
            if let Some(key_name) = overrides.api_key_name.as_deref().filter(|v| !v.is_empty()) {
                annotations.insert(
                    sandbawks_types::API_KEY_NAME_ANNOTATION.to_owned(),
                    key_name.to_owned(),
                );
            }
            // Anchor the idle clock at the claim instant (#380 / #481) by
            // stamping `status.lastActivityTimestamp` *before* the spec
            // patch that activates the timeout. ORDERING IS LOAD-BEARING: the
            // spec patch below flips `timeoutSeconds` 0 -> N, and that is the
            // write that turns the idle timeout on. The controller watches it
            // and reconciles; if the activity stamp has not yet landed when it
            // does, `activity_baseline` falls back to the member's stale
            // mint-time `creationTimestamp`, the deadline is already in the
            // past, and the member insta-pauses on claim (a hard regression —
            // every claimed warm member with `autoPause` + a non-zero timeout
            // died the instant it was handed out). Stamping first, while the
            // timeout is still the disabled `0` sentinel (deadline `None`, so
            // no pause can fire), guarantees the baseline is already present
            // the moment the timeout goes active — closing the race rather
            // than racing the controller to land the second write.
            //
            // Skipped for the disabled sentinel — there is no window to anchor.
            // `patch_status` targets the status subresource (a plain `.status`
            // merge on the main resource is dropped when a status subresource
            // exists).
            if overrides.timeout_seconds > 0 {
                let status_patch = serde_json::json!({
                    "status": { "lastActivityTimestamp": claimed_at },
                });
                self.sandboxes(namespace)?
                    .patch_status(name, &PatchParams::default(), &Patch::Merge(&status_patch))
                    .await?;
            }
            // `PatchParams::default()` + `Patch::Merge` — same merge-patch
            // discipline as the pause/timeout routes so repeated patches from
            // the gateway's field manager all land. The member is already
            // `Running`, so we do *not* touch `desiredPhase`: a claim is a
            // reassignment, not a resume.
            //
            // `timeoutSeconds` is the raw idle window (issue #481). The deadline
            // is anchored at the claim instant via the `status.lastActivityTimestamp`
            // stamp above, not by growing `timeoutSeconds` — a warm member's
            // `creationTimestamp` (its mint time) is irrelevant to the window.
            let mut spec_patch = serde_json::json!({
                "env": overrides.env,
                "metadata": overrides.metadata,
                "timeoutSeconds": overrides.timeout_seconds,
                "runtimeClassName": overrides.runtime_class_name,
            });
            // Per-create egress override (issue #542). Included only when the
            // request carried a non-empty `egressAllow` — already unioned with
            // the template baseline — so the claimed member's `spec.networkPolicy`
            // matches the cold-start path. Omitting the key under RFC 7396 leaves
            // a minted member's template baseline untouched (acceptance criterion
            // for an empty override). The controller reconciles the live
            // `NetworkPolicy` to this spec on the next pass
            // (`RuntimeAdapter::ensure_egress_policy`).
            if let Some(network_policy) = &overrides.network_policy {
                spec_patch["networkPolicy"] = serde_json::json!(network_policy);
            }
            let patch = serde_json::json!({
                "metadata": {
                    "annotations": annotations,
                },
                "spec": spec_patch,
            });
            // This spec patch is issued last and its response reflects the
            // status subresource as of now, so the returned `Sandbox` carries
            // the just-written `lastActivityTimestamp` — the gateway renders
            // `endAt` from the claim instant, not the stale mint-time
            // `creationTimestamp` fallback.
            let patched = self
                .sandboxes(namespace)?
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await?;
            Ok(patched)
        })
        .await
    }
}

/// In-memory [`WarmClaim`] for tests.
///
/// Kept always-public (not `#[cfg(test)]`-gated), mirroring
/// [`crate::routes::sandboxes::FakeSandboxOps`] and
/// [`crate::auth::FakeKeyStore`], so the api integration tests in
/// `tests/warm_claim.rs` can drive the router's claim path without a kube
/// apiserver. The default is an empty pool — that is what
/// [`crate::AppState::for_test`] wires, so every existing test falls
/// straight through to cold-boot.
#[derive(Default)]
pub struct FakeWarmClaim {
    /// `(template_ref, pool_name)` of the single pool this fake models.
    pool: Option<(String, String)>,
    /// Running candidates the pool hands out, in priority order.
    candidates: Vec<Sandbox>,
    /// Candidate name whose first claim is rejected with a simulated
    /// `409` (a concurrent request won the CAS) so the retry loop runs.
    lose_race: Option<String>,
    /// Names the claim path applied overrides to (resume side effects).
    applied: std::sync::Mutex<Vec<String>>,
}

impl FakeWarmClaim {
    /// A fake with no covering pool — every claim falls through.
    pub fn empty() -> Self {
        Self::default()
    }

    /// A fake whose pool `pool_name` covers `template_ref` and offers
    /// `candidates` (each should be a `Running` sandbox).
    pub fn with_pool(template_ref: &str, pool_name: &str, candidates: Vec<Sandbox>) -> Self {
        Self {
            pool: Some((template_ref.to_owned(), pool_name.to_owned())),
            candidates,
            ..Self::default()
        }
    }

    /// Make `name`'s first claim attempt lose the CAS race.
    pub fn losing_race_on(mut self, name: &str) -> Self {
        self.lose_race = Some(name.to_owned());
        self
    }

    /// Names whose overrides were applied (i.e. that were claimed).
    pub fn applied(&self) -> Vec<String> {
        self.applied.lock().expect("poisoned").clone()
    }
}

#[async_trait]
impl WarmClaim for FakeWarmClaim {
    async fn pool_for_template(
        &self,
        _ns: &str,
        template_ref: &str,
    ) -> Result<Option<String>, AppError> {
        Ok(self
            .pool
            .as_ref()
            .filter(|(t, _)| t == template_ref)
            .map(|(_, name)| name.clone()))
    }

    async fn running_candidates(&self, _ns: &str, _pool: &str) -> Result<Vec<Sandbox>, AppError> {
        Ok(self.candidates.clone())
    }

    async fn relabel_claim(
        &self,
        _ns: &str,
        name: &str,
        _rv: Option<String>,
        _claimed_at: &str,
    ) -> Result<bool, AppError> {
        Ok(self.lose_race.as_deref() != Some(name))
    }

    async fn apply_overrides(
        &self,
        _ns: &str,
        name: &str,
        overrides: &ClaimOverrides,
        claimed_at: &str,
    ) -> Result<Sandbox, AppError> {
        self.applied.lock().expect("poisoned").push(name.to_owned());
        // Echo the claimed name back with the overrides folded in. The
        // member was already `Running` (a claim is a reassignment, not a
        // resume), so its phase is left at `Running`, mirroring what the
        // real overrides patch produces.
        let mut s = self
            .candidates
            .iter()
            .find(|c| c.name_any() == name)
            .cloned()
            .unwrap_or_else(|| Sandbox::new(name, blank_spec()));
        s.spec.env = overrides.env.clone();
        s.spec.metadata = overrides.metadata.clone();
        // The raw idle window (issue #481) — mirroring
        // `KubeWarmClaim::apply_overrides`; the deadline anchors at the claim
        // instant via the `lastActivityTimestamp` stamp below, not by growing
        // `timeoutSeconds`.
        s.spec.timeout_seconds = overrides.timeout_seconds;
        s.spec.runtime_class_name = overrides.runtime_class_name.clone();
        // Per-create egress override (issue #542): mirror the real patch, which
        // only sets `spec.networkPolicy` when the override is present. `None`
        // leaves the member's (template-baseline) policy untouched.
        if let Some(network_policy) = &overrides.network_policy {
            s.spec.network_policy = Some(network_policy.clone());
        }
        let mut status = sandbawks_types::SandboxStatus {
            phase: SandboxPhase::Running,
            ..Default::default()
        };
        // Mirror the real patch's annotation + activity stamp so tests
        // exercise the same claimed-member shape. `0` is the disabled
        // sentinel — no window record, no idle baseline.
        if overrides.timeout_seconds > 0 {
            s.metadata
                .annotations
                .get_or_insert_with(BTreeMap::new)
                .insert(
                    crate::routes::sandboxes::TIMEOUT_DURATION_ANNOTATION.to_owned(),
                    overrides.timeout_seconds.to_string(),
                );
            status.last_activity_timestamp = Some(claimed_at.to_owned());
        }
        // Mirror the real patch branding the member with the owning key (#631).
        if let Some(key_name) = overrides.api_key_name.as_deref().filter(|v| !v.is_empty()) {
            s.metadata
                .annotations
                .get_or_insert_with(BTreeMap::new)
                .insert(
                    sandbawks_types::API_KEY_NAME_ANNOTATION.to_owned(),
                    key_name.to_owned(),
                );
        }
        s.status = Some(status);
        Ok(s)
    }
}

/// Minimal valid `SandboxSpec` for fixtures that synthesize a sandbox.
fn blank_spec() -> SandboxSpec {
    use sandbawks_types::ResourceRequests;
    SandboxSpec {
        template_ref: "tpl".into(),
        runtime_class_name: None,
        resources: ResourceRequests {
            cpu: "1".parse().expect("valid cpu"),
            memory: "1Gi".parse().expect("valid memory"),
            ephemeral_storage: None,
        },
        timeout_seconds: 0,
        auto_pause: true,
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // `Client` is no longer a production import (the backend holds a
    // `TenantClients` cache); the mock-client tests still need it.
    use kube::Client;
    use sandbawks_types::SandboxStatus;

    fn candidate(name: &str) -> Sandbox {
        let mut s = Sandbox::new(name, blank_spec());
        s.spec.template_ref = "py".into();
        s.status = Some(SandboxStatus {
            phase: SandboxPhase::Running,
            ..SandboxStatus::default()
        });
        s
    }

    fn requested() -> Sandbox {
        candidate("requested")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_pool_falls_through() {
        let backend = FakeWarmClaim::empty();
        let got = claim_for(&backend, "ns", &requested(), "now", None)
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claims_first_running_candidate() {
        let backend = FakeWarmClaim::with_pool(
            "py",
            "py-pool",
            vec![candidate("sbx-warm-1"), candidate("sbx-warm-2")],
        );
        let got = claim_for(&backend, "ns", &requested(), "now", None)
            .await
            .unwrap()
            .expect("claim succeeds");
        assert_eq!(got.name_any(), "sbx-warm-1");
        assert_eq!(backend.applied(), vec!["sbx-warm-1"]);
    }

    /// Epic #480 Phase 2 / issue #631: the owning `TeamAPIKey` the create handler
    /// stamped on the requester must brand the claimed warm member, so the
    /// controller can attribute the member's running-seconds to the right key.
    #[tokio::test(flavor = "current_thread")]
    async fn claim_brands_member_with_owning_api_key() {
        use std::collections::BTreeMap;

        let backend = FakeWarmClaim::with_pool("py", "py-pool", vec![candidate("sbx-warm-1")]);
        let mut req = requested();
        req.metadata.annotations = Some(BTreeMap::from([(
            sandbawks_types::API_KEY_NAME_ANNOTATION.to_owned(),
            "team-key-7".to_owned(),
        )]));

        let got = claim_for(&backend, "ns", &req, "now", None)
            .await
            .unwrap()
            .expect("claim succeeds");
        assert_eq!(
            got.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(sandbawks_types::API_KEY_NAME_ANNOTATION))
                .map(String::as_str),
            Some("team-key-7"),
            "claimed member must carry the owning TeamAPIKey for per-key billing (#631)"
        );
    }

    /// Epic #603 Phase 4 / issue #607: a request carrying per-create
    /// `volumeClaimTemplates` must fall through to cold-boot even when a pool
    /// with live candidates exists — a pre-warmed member cannot satisfy the
    /// request's PVCs. The skip happens before any candidate is examined, so the
    /// backend's claim/apply path is never touched.
    #[tokio::test(flavor = "current_thread")]
    async fn per_create_volume_claim_templates_force_cold_boot() {
        use sandbawks_types::{VolumeClaimSpec, VolumeClaimTemplate, VolumeClaimTemplateMetadata};

        let backend = FakeWarmClaim::with_pool(
            "py",
            "py-pool",
            vec![candidate("sbx-warm-1"), candidate("sbx-warm-2")],
        );
        let mut req = requested();
        req.spec.volume_claim_templates = Some(vec![VolumeClaimTemplate {
            metadata: VolumeClaimTemplateMetadata {
                name: "workspace".into(),
                ..Default::default()
            },
            spec: VolumeClaimSpec {
                storage: "10Gi".parse().expect("valid storage quantity"),
                access_modes: vec!["ReadWriteOnce".into()],
                storage_class_name: None,
                volume_mode: None,
            },
        }]);

        let got = claim_for(&backend, "ns", &req, "now", None).await.unwrap();
        assert!(
            got.is_none(),
            "per-create volumeClaimTemplates must fall through to cold-boot"
        );
        assert!(
            backend.applied().is_empty(),
            "no warm member may be claimed when the request carries PVCs"
        );
    }

    /// An *empty* `volumeClaimTemplates` reads as "explicitly no volumes" (the
    /// `Some(empty)` ⇒ none precedence), so it stays claimable — the guard keys
    /// on a non-empty list, not mere presence of the field.
    #[tokio::test(flavor = "current_thread")]
    async fn empty_volume_claim_templates_still_claims() {
        let backend = FakeWarmClaim::with_pool("py", "py-pool", vec![candidate("sbx-warm-1")]);
        let mut req = requested();
        req.spec.volume_claim_templates = Some(Vec::new());

        let got = claim_for(&backend, "ns", &req, "now", None)
            .await
            .unwrap()
            .expect("empty VCT list is claimable");
        assert_eq!(got.name_any(), "sbx-warm-1");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retries_past_lost_race() {
        // The first candidate loses the CAS (409) → claim_for must skip
        // it and land the second, never applying overrides to the first.
        let backend = FakeWarmClaim::with_pool(
            "py",
            "py-pool",
            vec![candidate("sbx-warm-1"), candidate("sbx-warm-2")],
        )
        .losing_race_on("sbx-warm-1");
        let got = claim_for(&backend, "ns", &requested(), "now", None)
            .await
            .unwrap()
            .expect("claim succeeds on retry");
        assert_eq!(got.name_any(), "sbx-warm-2");
        assert_eq!(backend.applied(), vec!["sbx-warm-2"]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn all_candidates_lost_falls_through() {
        let backend = FakeWarmClaim::with_pool("py", "py-pool", vec![candidate("sbx-warm-1")])
            .losing_race_on("sbx-warm-1");
        let got = claim_for(&backend, "ns", &requested(), "now", None)
            .await
            .unwrap();
        assert!(got.is_none(), "drained pool falls through to cold-boot");
    }

    /// A warm member minted 10 minutes ago and claimed now with a 60s idle
    /// window must get its deadline anchored at the *claim* instant, not the
    /// mint instant. Under the idle model (#481) the controller measures from
    /// `status.lastActivityTimestamp`, so the claim stamps that to `claimed_at`
    /// and writes the *raw* 60s window to `spec.timeoutSeconds` — no growing
    /// from the (stale) mint-time `creationTimestamp` (#380).
    #[tokio::test(flavor = "current_thread")]
    async fn claim_anchors_idle_timeout_at_claim_not_mint() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

        let mut minted = candidate("sbx-warm-old");
        minted.metadata.creation_timestamp =
            Some(Time(chrono::Utc::now() - chrono::Duration::minutes(10)));

        let mut req = requested();
        req.spec.timeout_seconds = 60;

        let backend = FakeWarmClaim::with_pool("py", "py-pool", vec![minted]);
        let claimed = claim_for(&backend, "ns", &req, "claimed-at-now", None)
            .await
            .unwrap()
            .expect("claim succeeds");

        // The raw 60s window, not a grown offset.
        assert_eq!(
            claimed.spec.timeout_seconds, 60,
            "claim writes the raw idle window, got {}",
            claimed.spec.timeout_seconds,
        );
        // The idle clock is anchored at the claim instant via the activity
        // stamp — this keeps an old member from insta-pausing, AND the
        // *returned* object must carry the stamp so the gateway renders `endAt`
        // from the claim instant rather than falling back to the stale
        // mint-time `creationTimestamp`. The real `KubeWarmClaim` returns its
        // `patch_status` response (not the earlier spec patch) for exactly this
        // reason; this assertion is the prod/fake contract that guards it.
        assert_eq!(
            claimed
                .status
                .as_ref()
                .and_then(|s| s.last_activity_timestamp.as_deref()),
            Some("claimed-at-now"),
            "claim must anchor the idle baseline at claimed_at on the returned object",
        );
        // The duration annotation mirrors the raw window.
        assert_eq!(
            claimed
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(crate::routes::sandboxes::TIMEOUT_DURATION_ANNOTATION))
                .map(String::as_str),
            Some("60"),
        );
    }

    /// The "idle timeout disabled" sentinel (`0`) must survive a claim even on
    /// an old member — anchoring it would resurrect the auto-pause the sentinel
    /// disables (#380). No window, no activity stamp.
    #[tokio::test(flavor = "current_thread")]
    async fn claim_keeps_disabled_timeout_disabled() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

        let mut minted = candidate("sbx-warm-old");
        minted.metadata.creation_timestamp =
            Some(Time(chrono::Utc::now() - chrono::Duration::minutes(10)));

        // `requested()` carries the disabled sentinel (`timeout_seconds: 0`).
        let backend = FakeWarmClaim::with_pool("py", "py-pool", vec![minted]);
        let claimed = claim_for(&backend, "ns", &requested(), "claimed-at-now", None)
            .await
            .unwrap()
            .expect("claim succeeds");

        assert_eq!(
            claimed.spec.timeout_seconds, 0,
            "disabled timeout must stay disabled",
        );
        assert!(
            claimed
                .status
                .as_ref()
                .and_then(|s| s.last_activity_timestamp.as_ref())
                .is_none(),
            "a disabled timeout has no idle baseline to anchor",
        );
        assert!(
            claimed
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(crate::routes::sandboxes::TIMEOUT_DURATION_ANNOTATION))
                .is_none(),
            "disabled timeout must not gain a duration annotation",
        );
    }

    /// Hard-down regression (idle-timeout activation race, #481 follow-up):
    /// `KubeWarmClaim::apply_overrides` MUST stamp
    /// `status.lastActivityTimestamp` *before* the spec patch that
    /// flips `timeoutSeconds` `0 -> N`. The spec patch is the write that
    /// activates the idle timeout; the controller reconciles it, and if the
    /// activity stamp has not yet landed `activity_baseline` falls back to
    /// the member's stale mint-time `creationTimestamp`, the deadline is
    /// already in the past, and the member is paused the instant it is
    /// handed out. The `FakeWarmClaim` can't catch a two-write ordering bug
    /// (it builds one object), so this drives the real backend over a
    /// recording `kube::Client` and asserts the status-subresource patch
    /// precedes the bare-resource patch.
    #[tokio::test(flavor = "current_thread")]
    async fn apply_overrides_stamps_activity_before_activating_timeout() {
        use std::sync::{Arc, Mutex};

        // Records the path of every request the backend issues, in order.
        // `apply_overrides` awaits each patch fully before the next, so the
        // recorded order is the wire order.
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        // Every patch deserializes its response into a `Sandbox`, so the
        // mock must return a valid serialized object for both calls.
        let body = serde_json::to_vec(&Sandbox::new("sbx-warm-x", blank_spec()))
            .expect("sandbox serializes");
        let recorder = calls.clone();
        let service = tower::service_fn(move |req: axum::http::Request<kube::client::Body>| {
            recorder
                .lock()
                .expect("poisoned")
                .push(req.uri().path().to_owned());
            let body = body.clone();
            async move {
                Ok::<_, std::convert::Infallible>(
                    axum::http::Response::builder()
                        .status(200)
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(kube::client::Body::from(body))
                        .expect("response is well-formed"),
                )
            }
        });
        let backend = KubeWarmClaim::new(Arc::new(crate::tenant_client::TenantClients::fixed(
            Client::new(service, "ns"),
        )));

        let overrides = ClaimOverrides {
            env: Default::default(),
            metadata: Default::default(),
            timeout_seconds: 300,
            runtime_class_name: None,
            network_policy: None,
            api_key_name: None,
        };
        backend
            .apply_overrides("ns", "sbx-warm-x", &overrides, "2026-06-12T00:02:04Z")
            .await
            .expect("apply_overrides succeeds");

        let paths = calls.lock().expect("poisoned").clone();
        assert_eq!(
            paths,
            vec![
                "/apis/sandbawks.dev/v1alpha1/namespaces/ns/sandboxes/sbx-warm-x/status".to_owned(),
                "/apis/sandbawks.dev/v1alpha1/namespaces/ns/sandboxes/sbx-warm-x".to_owned(),
            ],
            "the activity stamp (/status) must be written before the timeout-activating spec patch",
        );
    }

    /// Issue #542: a per-create `egressAllow` (already unioned with the template
    /// baseline by the create route) handed to `claim_for` must be stamped onto
    /// the claimed member's `spec.networkPolicy`, so a warm-served request gets
    /// the same egress a cold boot would.
    #[tokio::test(flavor = "current_thread")]
    async fn claim_stamps_per_create_egress_override() {
        let backend = FakeWarmClaim::with_pool("py", "py-pool", vec![candidate("sbx-warm-1")]);
        // The union the create route computed: template baseline ∪ per-create.
        let union = NetworkPolicy {
            egress_allow: vec!["git.example.com".into(), "10.0.0.0/8".into()],
        };
        let claimed = claim_for(&backend, "ns", &requested(), "now", Some(&union))
            .await
            .unwrap()
            .expect("claim succeeds");

        assert_eq!(
            claimed
                .spec
                .network_policy
                .as_ref()
                .map(|np| np.egress_allow.as_slice()),
            Some(union.egress_allow.as_slice()),
            "the claimed member must carry the per-create egress union",
        );
    }

    /// Issue #542 (acceptance criterion 2): an absent per-create override
    /// (`None`) must leave the claimed member's template-baseline
    /// `spec.networkPolicy` untouched rather than blanking or rewriting it.
    #[tokio::test(flavor = "current_thread")]
    async fn claim_without_override_leaves_baseline_untouched() {
        // Mint a member already carrying a template egress baseline.
        let mut minted = candidate("sbx-warm-1");
        minted.spec.network_policy = Some(NetworkPolicy {
            egress_allow: vec!["baseline.example".into()],
        });

        let backend = FakeWarmClaim::with_pool("py", "py-pool", vec![minted]);
        let claimed = claim_for(&backend, "ns", &requested(), "now", None)
            .await
            .unwrap()
            .expect("claim succeeds");

        assert_eq!(
            claimed
                .spec
                .network_policy
                .as_ref()
                .map(|np| np.egress_allow.as_slice()),
            Some(["baseline.example".to_owned()].as_slice()),
            "an override-free claim must preserve the member's template baseline",
        );
    }

    /// Issue #542: the real `KubeWarmClaim::apply_overrides` must include
    /// `spec.networkPolicy` in its spec patch when (and only when) an egress
    /// override is present. Drives the recording `kube::Client` and asserts the
    /// serialized patch body carries the union.
    #[tokio::test(flavor = "current_thread")]
    async fn apply_overrides_patches_network_policy_when_present() {
        use std::sync::{Arc, Mutex};

        // Capture the body of the bare-resource (spec) patch — the last call.
        let bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let resp = serde_json::to_vec(&Sandbox::new("sbx-warm-x", blank_spec()))
            .expect("sandbox serializes");
        let recorder = bodies.clone();
        let service = tower::service_fn(move |req: axum::http::Request<kube::client::Body>| {
            let is_spec_patch = !req.uri().path().ends_with("/status");
            let recorder = recorder.clone();
            let resp = resp.clone();
            async move {
                if is_spec_patch {
                    let bytes = req
                        .into_body()
                        .collect_bytes()
                        .await
                        .expect("collect patch body");
                    let json: serde_json::Value =
                        serde_json::from_slice(&bytes).expect("patch body is JSON");
                    recorder.lock().expect("poisoned").push(json);
                }
                Ok::<_, std::convert::Infallible>(
                    axum::http::Response::builder()
                        .status(200)
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(kube::client::Body::from(resp))
                        .expect("response is well-formed"),
                )
            }
        });
        let backend = KubeWarmClaim::new(Arc::new(crate::tenant_client::TenantClients::fixed(
            Client::new(service, "ns"),
        )));

        let overrides = ClaimOverrides {
            env: Default::default(),
            metadata: Default::default(),
            timeout_seconds: 0,
            runtime_class_name: None,
            network_policy: Some(NetworkPolicy {
                egress_allow: vec!["git.example.com".into(), "10.0.0.0/8".into()],
            }),
            api_key_name: None,
        };
        backend
            .apply_overrides("ns", "sbx-warm-x", &overrides, "2026-06-12T00:02:04Z")
            .await
            .expect("apply_overrides succeeds");

        let bodies = bodies.lock().expect("poisoned");
        let spec_patch = bodies.last().expect("a spec patch was issued");
        assert_eq!(
            spec_patch["spec"]["networkPolicy"]["egressAllow"],
            serde_json::json!(["git.example.com", "10.0.0.0/8"]),
            "the spec patch must carry the per-create egress union, got {spec_patch}",
        );
    }

    /// Issue #542: with no egress override, `apply_overrides` must omit
    /// `spec.networkPolicy` from the patch entirely (RFC 7396: omitting the key
    /// leaves the member's existing policy untouched).
    #[tokio::test(flavor = "current_thread")]
    async fn apply_overrides_omits_network_policy_when_absent() {
        use std::sync::{Arc, Mutex};

        let bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let resp = serde_json::to_vec(&Sandbox::new("sbx-warm-x", blank_spec()))
            .expect("sandbox serializes");
        let recorder = bodies.clone();
        let service = tower::service_fn(move |req: axum::http::Request<kube::client::Body>| {
            let is_spec_patch = !req.uri().path().ends_with("/status");
            let recorder = recorder.clone();
            let resp = resp.clone();
            async move {
                if is_spec_patch {
                    let bytes = req
                        .into_body()
                        .collect_bytes()
                        .await
                        .expect("collect patch body");
                    let json: serde_json::Value =
                        serde_json::from_slice(&bytes).expect("patch body is JSON");
                    recorder.lock().expect("poisoned").push(json);
                }
                Ok::<_, std::convert::Infallible>(
                    axum::http::Response::builder()
                        .status(200)
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(kube::client::Body::from(resp))
                        .expect("response is well-formed"),
                )
            }
        });
        let backend = KubeWarmClaim::new(Arc::new(crate::tenant_client::TenantClients::fixed(
            Client::new(service, "ns"),
        )));

        let overrides = ClaimOverrides {
            env: Default::default(),
            metadata: Default::default(),
            timeout_seconds: 0,
            runtime_class_name: None,
            network_policy: None,
            api_key_name: None,
        };
        backend
            .apply_overrides("ns", "sbx-warm-x", &overrides, "2026-06-12T00:02:04Z")
            .await
            .expect("apply_overrides succeeds");

        let bodies = bodies.lock().expect("poisoned");
        let spec_patch = bodies.last().expect("a spec patch was issued");
        assert!(
            spec_patch["spec"].get("networkPolicy").is_none(),
            "no override ⇒ the spec patch must not mention networkPolicy, got {spec_patch}",
        );
    }
}
