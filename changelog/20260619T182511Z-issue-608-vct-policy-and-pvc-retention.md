# Issue #608 — Filesystem persistence: VCT policy + PVC retention (epic #603 Phase 5)

_Date: 2026-06-19_

The optional hardening on top of working `filesystem`-mode persistence (epic
#603). Two independent features, both opt-in and both leaving the default
deployment unchanged:

1. **`volumeClaimTemplatesPolicy`** — a platform-owner control over whether a
   tenant-authored `Sandbox` may bring its own `volumeClaimTemplates` on top of
   the ones its `SandboxTemplate` defines.
2. **PVC retention GC** — reclaims a paused `filesystem`-mode sandbox (and
   cascades its retained PVCs) once it has sat past a retention window, the
   `filesystem`-mode analog of the existing snapshot-retention sweep.

## Why

`filesystem` mode (delete-Pod pause / cold-boot resume over Sandbox-owned PVCs)
is fully functional after Phases 1–4. This phase closes two operability gaps it
opened:

- A tenant who can `kubectl apply` a `Sandbox` could previously attach arbitrary
  PVCs (its explicit `volumeClaimTemplates` won wholesale over the template's).
  In a shared namespace (ties into multi-namespace tenancy, #442) the platform
  owner needs to gate that.
- Unlike a snapshot-mode pause (state is an S3 object under
  `snapshotStore.retention`), a `filesystem`-mode pause keeps a **live PVC** that
  bills until something reclaims it. There was no retention accounting for it.

## What changed

### Policy (`volumeClaimTemplatesPolicy`)

- `sandbawks-types`: new `VolumeClaimTemplatesPolicy` enum (`Disallowed`
  default | `Allowed` | `Overrides`), `volume_claim_templates_policy` field on
  `SandboxTemplateSpec`, and a pure, never-failing
  `merge_volume_claim_templates(template, tenant, policy) -> VctMergeOutcome`
  (effective claims + suppressed claims). Mirrors
  `kubernetes-sigs/agent-sandbox`.
  - `Disallowed` (default): effective set is exactly the template's; tenant
    claims are suppressed.
  - `Allowed`: tenant may add new-named claims; a name collision with a template
    claim is suppressed (template wins).
  - `Overrides`: tenant may add and override by name; nothing suppressed.
  - Deterministic ordering; duplicate tenant names collapse to one volume.
- `sandbawks-controller`: `resolve_pod_inputs` — the single pod-creation edge all
  paths (cold-start, warm-pool, hand-applied CR) flow through — now resolves the
  effective PVC set through the policy instead of taking tenant claims wholesale.
  Suppressed claims are surfaced as a `VolumeClaimTemplatesPolicy` status
  condition (`status=False`, reason `ClaimsSuppressed`) — **suppressed, never a
  hard failure**, so an over-reaching tenant runs on the template baseline.

> **Behavior note:** the default `Disallowed` means a `Sandbox` that sets its own
> `volumeClaimTemplates` now gets only the template's. The E2B API never sets
> Sandbox-level VCTs, so this affects only directly-applied `Sandbox` CRs — the
> intended platform-owner control. Opt a template into tenant storage with
> `Allowed` / `Overrides`.

### Retention / GC

- `sandbawks-types`: new `pvc_retention` field on `SandboxSpec` (ISO-8601), the
  `filesystem`-mode analog of `snapshot_retention`.
- `sandbawks-controller`:
  - `PvcRetentionConfig { enabled, default_duration }` on `ReconcileContext`
    (disabled by default), wired from `SANDBAWKS_PVC_RETENTION_ENABLED` /
    `SANDBAWKS_PVC_RETENTION_DEFAULT_DURATION` (default `P7D`).
  - `SandboxStore::delete` (background propagation; 404 ⇒ `Ok`) — deleting the
    Sandbox cascades its retained PVCs via the controller ownerReferences stamped
    in Phase 2.
  - The reconcile loop, for a `Paused` sandbox carrying a `Suspended=True`
    condition (the filesystem-suspend marker, which doubles as the retention
    anchor), deletes the Sandbox once `now ≥ suspended_at + window`. Gated on
    `phase == Paused` and the opt-in switch; resolution order
    `spec.pvcRetention → default_duration`; an unparseable `spec.pvcRetention`
    fails safe (skips GC). Emits a `PvcRetentionReclaimed` Kubernetes Event.
  - `paused_requeue_interval` tightens the `Paused` heartbeat toward the deadline
    (mirrors `running_requeue_interval`).
  - **Observability** (PR #613 review): a `controller.pvc_retention.reclaimed_total`
    counter (`pvc_retention_metrics`, OnceLock no-op when OTel is disabled)
    increments on each reclamation, so this destructive auto-deletion is
    trendable/alertable in aggregate — not just per-object via the Event.

### Schema / chart

- Regenerated CRDs (`sandboxes`, `sandboxtemplates`).
- Helm: `controller.pvcRetention.{enabled,defaultDuration}` (default disabled) →
  controller env; bumped `Chart.yaml` `version` 0.1.23 → 0.1.24.

## Tests

- `sandbawks-types`: `merge_volume_claim_templates` across all three policies,
  empty-tenant no-op, duplicate-name collapse, default-is-`Disallowed`.
- `sandbawks-controller`:
  - pure GC decision helpers (anchor read, window boundary, explicit-window
    override, disabled/non-filesystem skip, fail-safe on bad duration, requeue
    tightening);
  - `resolve_pod_inputs` policy enforcement (`Disallowed` suppresses + reports,
    `Allowed` admits new names);
  - reconcile integration: paused filesystem sandbox past window is deleted (and
    `delete` called once), within window is kept, and disabled never collects.

## Gates

`just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`, `helm lint`
+ `helm template` (env present only when enabled).

## Deploy note

CRD schema changed — **`kubectl apply -f deploy/crds`** after deploy (Helm does
not manage CRDs; a stale CRD silently prunes `pvcRetention` /
`volumeClaimTemplatesPolicy`).
