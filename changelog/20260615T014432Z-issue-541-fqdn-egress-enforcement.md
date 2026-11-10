# Issue #541 — FQDN egress enforcement via Cilium `toFQDNs`

**Date:** 2026-06-15T01:44:32Z
**Branch:** `feat-issue-541-fqdn-egress`
**Issue:** #541 (follow-up to #537 / PR #540; relates to #443 / #520 / #536)

## Summary

A DNS-*name* entry in a sandbox's `egressAllow` previously only opened DNS
(port 53) under the baseline `networking.v1.NetworkPolicy` — that policy type
has no FQDN field, so it could not authorize the *resolved* L3 connection.
Reaching an external / CDN-fronted host therefore required brittle CIDR entries.

This change backs each DNS-name `egressAllow` entry with a per-sandbox Cilium
`toFQDNs` `CiliumNetworkPolicy`, materialized **alongside** the baseline
NetworkPolicy. Cilium merges the two additively, so the name's resolved address
is permitted while the default-deny posture (#443) holds for everything else.

The feature is gated behind an explicit Helm flag
(`controller.fqdnEgress.enabled`, default `false`). On a cluster that has not
opted in — or where the `CiliumNetworkPolicy` CRD is absent — behavior degrades
to today's DNS-port-only / CIDR-only path with no regression.

## Design decisions

- **Explicit flag over runtime CNI capability detection.** A flag is
  deterministic, unit-testable without an apiserver, and avoids a discovery
  race. The operator enables it only on a Cilium cluster whose CRD is installed.
- **DNS-visibility rule is mandatory.** Cilium's `toFQDNs` only learns a
  name→IP mapping from DNS answers its DNS proxy observes, so the materialized
  policy includes a `toPorts` rule on port 53 with `rules.dns: matchPattern "*"`
  pointed at the cluster resolver.
- **Resolver location is configurable (no distro hardcode).** The DNS resolver
  namespace and pod label are read from `SANDBAWKS_FQDN_DNS_NAMESPACE` /
  `SANDBAWKS_FQDN_DNS_POD_LABEL`, defaulting to the stock CoreDNS placement
  (`kube-system`, `k8s-app=kube-dns`). A malformed (no `=`) label override falls
  back to the default rather than emitting a broken selector.
- **Graceful CRD-absent handling.** If the flag is set but the CRD is missing,
  the per-sandbox create gets a `404`; the controller logs a warning and
  continues (DNS-port-only fallback) rather than bricking sandbox creation. A
  `409` is treated as idempotent success, matching the baseline policy.
- **`CiliumNetworkPolicy` is addressed dynamically.** It is a Cilium-owned CRD
  with no `k8s-openapi` type, so it is built as a `DynamicObject` and POSTed via
  an `ApiResource` describing the `cilium.io/v2` GVK.

## Changes

### `crates/sandbawks-controller/src/pod_adapter.rs`
- New env-var constants (`FQDN_EGRESS_ENABLED_ENV`, `FQDN_DNS_NAMESPACE_ENV`,
  `FQDN_DNS_POD_LABEL_ENV`) with CoreDNS defaults, and `cilium.io/v2` GVK
  constants.
- `PodAdapter::cilium_network_policy_for` — pure builder returning
  `Option<DynamicObject>`: `None` unless the flag is `"true"` and at least one
  `egressAllow` entry is a DNS name; otherwise a CiliumNetworkPolicy scoped to
  the sandbox's identity label with a DNS-visibility rule and one `toFQDNs`
  `matchName` per name. Reuses `egress_entry_as_cidr` to separate names from
  CIDRs (CIDRs stay enforced by the baseline policy).
- `cilium_network_policies` / `cilium_network_policy_api_resource` /
  `cilium_network_policy_name` helpers; `create_pod` now materializes the CNP
  after the baseline NetworkPolicy and before the Pod, with 409→idempotent /
  404→warn-and-continue handling.
- Refactor: extracted the duplicated owner-reference block (Pod, env Secret,
  NetworkPolicy, and now CiliumNetworkPolicy) into a shared
  `sandbox_owner_references` helper (DRY).

### `crates/sandbawks-controller/tests/pod_adapter.rs`
- Six unit tests for `cilium_network_policy_for`: flag-off → none; CIDR-only →
  none; full shape (GVK, name, endpointSelector, DNS-visibility, toFQDNs);
  mixed-list filtering; configurable resolver + malformed-label fallback; owner
  reference with/without uid.

### Helm (`deploy/helm/sandbawks/`)
- `values.yaml`: new `controller.fqdnEgress` block (`enabled`, `dnsNamespace`,
  `dnsPodLabel`), disabled by default.
- `templates/controller-deployment.yaml`: gated env block exporting the three
  `SANDBAWKS_FQDN_*` vars.
- `templates/rbac.yaml`: gated `cilium.io/ciliumnetworkpolicies` write rule
  (only rendered when the flag is on, so non-Cilium clusters reference no CRD).
- `Chart.yaml`: version `0.1.11` → `0.1.12`.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all workspace tests pass (incl. 6 new FQDN unit tests).
- `helm template` rendered with and without `controller.fqdnEgress.enabled`:
  disabled emits no FQDN env / no `cilium.io` RBAC; enabled emits both with the
  configured resolver values.

## Not covered here (requires live cluster)

The issue's e2e acceptance criterion — an actual connection to an FQDN host
from inside a gVisor sandbox on a Cilium cluster — needs an e2e Cilium + gVisor
cluster and is out of scope for unit/local validation (the reason this work was
carved out of #540). The locally-verifiable surface (policy shape, gating,
graceful degradation, RBAC/Helm wiring) is fully covered above.
