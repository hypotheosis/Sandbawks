# Issue #542 — Apply per-create egress override on the warm-claim path

**Date:** 2026-06-16T23:28:51Z
**Branch:** `feat-issue-542-warm-claim-egress-override`
**Labels:** area/api, area/controller, kind/feat, prio/p2

## Problem

The per-create egress override (`CreateSandboxBody.egressAllow`, design option 2
from #537) was wired on the **cold-start** create path only
(`crates/sandbawks-api/src/routes/sandboxes.rs` — `body_to_sandbox` +
`merge_egress_policies`). When a `POST /sandboxes` was instead served by a
**warm-pool claim**, the per-create `egressAllow` was silently dropped: the
claimed member kept the egress baseline it was minted with from the template
(#540), so a caller adding a destination at create time got different results
depending on whether the request hit a warm member or a cold boot.

The root constraint: the per-sandbox egress `NetworkPolicy` is materialized
**once**, at `PodAdapter::create_pod` (create-or-409, *never* reconciled), and a
warm claim never runs `create_pod`. So honoring the override required
propagating it both to the CR's `spec.networkPolicy` *and* to the already-live
`NetworkPolicy` — which is why it was deferred from #540.

## Approach (controller-reconciles, fully correct)

A two-part change so a warm-claimed member ends up with exactly the egress a
cold-booted member of the same spec would enforce:

1. **API stamps the spec** — the warm-claim path patches the claimed member's
   `spec.networkPolicy` to the union of the template baseline and the per-create
   destinations (the same value the cold-start path computes via
   `merge_egress_policies`), and **only** when the request carried a non-empty
   `egressAllow`. An empty override leaves the member's template baseline
   untouched (acceptance criterion 2).

2. **Controller reconciles enforcement** — a new idempotent
   `RuntimeAdapter::ensure_egress_policy` hook (default no-op) re-materializes
   the live `NetworkPolicy` (and, on an FQDN-opted cluster, its companion Cilium
   `toFQDNs` policy) from `spec.networkPolicy`. The reconcile loop calls it for a
   `Running` sandbox that carries a `spec.networkPolicy`, so the claim's spec
   patch is propagated to enforcement on the next reconcile.

Reusing the existing `network_policy_for` / `cilium_network_policy_for` builders
keeps the policy shape (podSelector, policyTypes, infra OTel/DNS egress rules,
FQDN handling) identical to the cold-start path — no logic is duplicated in the
API crate.

## Changes

### `crates/sandbawks-types`
- `runtime.rs`: new `RuntimeAdapter::ensure_egress_policy(&self, &Sandbox)`
  trait method with a default no-op impl.

### `crates/sandbawks-controller`
- `pod_adapter.rs`:
  - `PodAdapter::ensure_egress_policy` — compare-then-patch: GET the live
    baseline `NetworkPolicy`, and JSON-merge-patch its `spec.egress` only when
    it differs from the spec-derived desired set (so a quiescent Running pass
    costs at most one GET and never churns `resourceVersion`); self-heals a
    missing policy by creating it. Then ensures the Cilium FQDN companion.
  - `PodAdapter::ensure_cilium_network_policy` — create-or-**update** helper for
    the FQDN policy (the existing create-or-ack path can't widen an existing
    one); a `404` on the patch delegates to the create-or-ack path, preserving
    its missing-CRD graceful-degrade.
  - Imported `Patch` / `PatchParams`.
- `reconcile.rs`: call `ctx.adapter.ensure_egress_policy` for a `Running`
  sandbox whose `spec.networkPolicy.is_some()`, best-effort (a transient sync
  failure is logged and retried on the next requeue rather than aborting the
  lifecycle reconcile).
- `fake_adapter.rs`: `FakeRuntimeAdapter` records `ensure_egress_policy` calls
  (`ensure_egress_calls()`) so the reconcile wiring is unit-testable.

### `crates/sandbawks-api`
- `warm_claim.rs`:
  - `ClaimOverrides` gains a `network_policy: Option<NetworkPolicy>` field
    (left `None` by `from_spec`; set explicitly by `claim_for`).
  - `claim_for` takes an `egress_override: Option<&NetworkPolicy>` argument and
    stamps it onto the overrides.
  - `KubeWarmClaim::apply_overrides` includes `spec.networkPolicy` in its merge
    patch only when the override is present (RFC 7396: omitting the key leaves
    the member's baseline untouched).
  - `FakeWarmClaim::apply_overrides` mirrors that behavior.
- `routes/sandboxes.rs`: `create_sandbox` captures whether the request carried a
  per-create `egressAllow` *before* the template-baseline union, and passes the
  resulting union (or `None`) into `claim_for`.

## Tests

- `warm_claim.rs` unit tests: per-create egress unions onto the claimed member;
  an absent override leaves the baseline untouched; the real `apply_overrides`
  patch body includes `spec.networkPolicy` when present and omits it when
  absent.
- `tests/pod_adapter.rs`: `ensure_egress_policy` PATCHes the live policy when the
  egress differs (asserting the CIDR rule lands), and is a no-op (GET only) when
  the live policy already matches.
- `tests/reconcile.rs`: a `Running` sandbox with a `spec.networkPolicy` calls the
  sync hook; one without it, and a `Paused` one, do not.

## Acceptance criteria

- [x] A `POST /sandboxes` with a non-empty `egressAllow` served by a warm claim
      yields a member whose egress is the **union** of the template baseline and
      the per-create destinations — matching the cold-start path.
- [x] An empty `egressAllow` leaves the claimed member's template baseline
      untouched.

## Notes / follow-ups

- `create_pod` keeps its create-or-409 materialization; `ensure_egress_policy`
  is the update path the original `#443` comment anticipated ("if mutable egress
  policy is ever supported, the 409 branch must become an update/patch path").
- The egress sync is gated on `Running` + `spec.networkPolicy.is_some()` because
  egress is only ever widened (set-once baseline + claim-time union), never
  removed.
