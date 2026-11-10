# Issue #443 — Materialize default-deny egress NetworkPolicy per sandbox

**Date:** 2026-06-13T23:17:29Z
**Branch:** `sandbawks/sandbawks-443`
**Issue:** #443 (area/controller, area/deploy, kind/security, prio/p1)

## Problem

MVP spec §9 mandates a default-deny egress posture on every sandbox Pod, with
`Sandbox.spec.networkPolicy.egressAllow` opening specific destinations. The CRD
surface (`NetworkPolicy { egress_allow: Vec<String> }`) existed, but **no code
ever created a `networking.v1.NetworkPolicy`** — sandbox Pods ran with
unrestricted egress. For a product that executes arbitrary tenant code, that
default-deny posture is the trust boundary, and it was absent.

## Change

Mirrors the existing per-sandbox env-Secret pattern: the controller's
`PodAdapter` now materializes an owner-referenced `networking.v1.NetworkPolicy`
for every sandbox, created inside `create_pod` **before** the Pod (so the
egress boundary exists the instant the Pod is admitted). Covers both
cold-start and restore paths.

### `crates/sandbawks-controller/src/pod_adapter.rs`

- `PodAdapter::network_policy_for(&Sandbox) -> NetworkPolicy` — pure builder:
  - `podSelector` targets the **per-sandbox** identity label
    `sandbawks.dev/sandbox=<name>` (promoted to the `SANDBOX_NAME_LABEL_KEY`
    constant, shared with the Pod label so the two can't drift), not the shared
    `app.kubernetes.io/name` label.
  - `policyTypes: [Egress]` (egress only — ingress between sandbox pods is out
    of scope per the spec).
  - Owner-references the Sandbox CR (when `metadata.uid` is set) so kube GCs it
    with the sandbox.
- `PodAdapter::network_policy_name` — deterministic `sandbox-<name>-egress`.
- `egress_entry_as_cidr` — classifies each `egressAllow` entry.
- `create_pod` creates the policy with the same create-or-409 idempotency as
  the env Secret / Pod.

### Egress semantics (the documented decision)

A standard `networking.v1` NetworkPolicy egress rule can only name an L3
destination as an `ipBlock` CIDR — it has **no FQDN/DNS-name field**. So:

| `egressAllow` | Result |
|---|---|
| empty / `None` | `egress: None` ⇒ **deny all egress** (DNS included) |
| IP / CIDR entry | exact `ipBlock` egress rule (bare IP widened to `/32`/`/128`) |
| DNS-name entry | opens **DNS egress** (UDP+TCP 53) so the name resolves; L3 enforcement of the resolved destination is delegated to the cluster's name-aware CNI layer (Cilium FQDN policy) |

Per the issue's "decide … call it out" clause, this PR takes the **"allow DNS +
rely on name-based controllers"** option for FQDN entries. Encoding the FQDN
destination itself — a `CiliumNetworkPolicy` `toFQDNs` rule, or reconcile-time
name→CIDR resolution — is deliberate follow-up, not this baseline.

### `crates/sandbawks-types/src/sandbox.rs`

- Corrected the now-accurate doc-comments on `SandboxSpec.network_policy` and
  the `NetworkPolicy` struct (the field previously *claimed* a default-deny
  policy was attached — now it actually is).

### `deploy/`

- CRDs regenerated (`crdgen`) — description-only diff from the doc-comment
  edits; no schema change.
- Controller RBAC already grants `networking.k8s.io/networkpolicies`
  (get/list/watch/create/update/patch/delete) — no RBAC change required.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — see PR.
- New unit tests in `crates/sandbawks-controller/tests/pod_adapter.rs`:
  default-deny baseline (empty + `None`), CIDR→ipBlock, DNS-name→DNS egress,
  owner-ref presence/absence; the `create_pod` mock test now asserts the
  NetworkPolicy POST precedes the Pod POST.

## CNI dependency

A NetworkPolicy is inert unless the CNI enforces it. The deployment targets
Cilium, which does; on a CNI without NetworkPolicy support the policy is
silently ignored and egress stays open.

## Out of scope

- Ingress policy between sandbox pods.
- FQDN (`toFQDNs`) / reconcile-time name resolution for DNS-name `egressAllow`
  entries.
