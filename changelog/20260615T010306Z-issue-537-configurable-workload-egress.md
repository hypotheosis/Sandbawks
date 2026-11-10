# Issue #537 — Configurable workload egress (template baseline + per-create override)

**Date:** 2026-06-15T01:03:06Z
**Issue:** #537 (`feat`, prio/p2; area/api, area/controller, area/types)
**Branch:** `sandbawks/sandbawks-537`

## Problem

`Sandbox.spec.networkPolicy.egressAllow` existed (issue #443 / PR #520) and was
consumed by the NetworkPolicy builder, but **no user-facing surface wired it
through**. Every `SandboxSpec` construction site hardcoded `network_policy:
None`, so the only way to widen a sandbox's egress beyond the default-deny
baseline was hand-editing the live `Sandbox` CR — impossible for the
warm-pool-backed flow, where members are minted and claimed automatically.

## What changed

Implemented the issue's recommended keystone (option 1) plus the per-create
override (option 2), with **union** precedence (`template baseline ∪ per-create
override`). FQDN enforcement (option 3) is deferred — see below.

### `sandbawks-types`

- **`SandboxTemplateSpec.network_policy: Option<NetworkPolicy>`** — a
  template-level egress baseline. Optional, `skip_serializing_if = "None"`, so
  existing templates are unaffected and the CRD field is non-required.
- **`merge_egress_policies(base, overlay) -> Option<NetworkPolicy>`** — unions
  two egress allow-lists, baseline-first, order-preserving, de-duplicated, and
  normalizes an empty union back to `None`. Re-exported from the crate root.
  Allocation-free on the at-most-one-side-present fast path; the membership
  check stays O(n·m) on the tiny allow-lists involved rather than allocating a
  `HashSet` (which would also drop insertion order).

### `sandbawks-controller` (warm pool)

- **`WarmSandboxDefaults.network_policy`** — resolved from the template by
  `from_template`.
- **`warm_sandbox` mint** — replaced the hardcoded `network_policy: None`
  (`warm_pool.rs:422`) with `defaults.network_policy.clone()`, so warm-pool
  members inherit the template's egress baseline. This is the half that closes
  the warm-pool gap.

### `sandbawks-api` (cold-start create route)

- **`CreateSandboxBody.egress_allow: Vec<String>`** — a per-create override
  (sandbawks extension; `serde(default)`, so E2B SDKs that don't send it are
  unaffected).
- **`body_to_sandbox`** captures the override onto `spec.networkPolicy`.
- **`create_sandbox`** unions the resolved template's baseline over the
  per-create override via `merge_egress_policies` (template first). A template
  miss keeps just the override.

### CRDs

- Regenerated `deploy/crds/sandboxtemplates.sandbawks.dev.yaml`
  (`cargo run -p sandbawks-types --bin crdgen -- deploy/crds`). New optional
  `spec.networkPolicy.egressAllow` field. **Apply out-of-band**
  (`kubectl apply -f deploy/crds`) — Helm ships with `--skip-crds`.

## Deferred: option 3 (FQDN enforcement)

A DNS-name entry in `egressAllow` still only opens port 53; it does not
authorize the L3 connection. Real hostname enforcement needs a Cilium
`toFQDNs` `CiliumNetworkPolicy` (or equivalent), is CNI-specific, must be gated
behind a capability/flag, and requires an e2e Cilium + gVisor cluster to verify
— out of scope for this PR and not locally testable. Hostname entries continue
to degrade to DNS-port-only under the standard NetworkPolicy path, unchanged
from #443.

Also deferred: the **warm-claim** path does not apply a per-create egress
override onto an already-minted member (the member carries the template
baseline it was minted with). Per-create override is wired on the cold-start
path only; warm members get the template baseline.

## Tests

- `sandbawks-types`: `merge_egress_policies` unit tests (both-none, single-side
  pass-through, empty-normalizes-to-none, baseline-first union + dedup,
  override-cannot-revoke); `SandboxTemplateSpec` egress YAML round-trip +
  omit-when-none; doctest on `merge_egress_policies`.
- `sandbawks-controller`: `warm_sandbox` inherits the template egress baseline;
  no egress when the template sets none.
- `sandbawks-api`: in-module `body_to_sandbox` tests (captures per-create
  egress; `None` when absent).

## Validation

- `just fmt`
- `cargo clippy --all-targets -- -D warnings`
- `just test`

## Review follow-up (PR #540, review #263)

Addressed the actionable test-coverage and clarity gaps from the automated
review (82/100, comment-only):

- **CRD schema guard** — added `network_policy_is_optional_in_crd_schema`
  (mirrors `envd_image_is_optional_in_crd_schema`) so the field can't silently
  flip to required.
- **Controller propagation** — added in-module unit tests for the (private)
  `WarmSandboxDefaults::from_template`: extracts the egress baseline / resolves
  to `None` when the template sets none.
- **Cold-start-only clarity (issue #1 in the review)** — documented on
  `CreateSandboxBody.egress_allow` that the override is applied on the
  cold-start path only today; a warm-claim ignores it (tracked in #542).

Not done — **handler-level integration test for the resolve-template-then-merge
branch in `create_sandbox`**: the API test harness (`AppState::for_test`) backs
the template lookup with a mock `kube::Client` that returns 204, so the
`Ok(template)` arm (where `merge_egress_policies` runs) is unreachable without
standing up apiserver mocking — disproportionate for a 3-line glue branch whose
constituent parts (`merge_egress_policies`, `body_to_sandbox` capture) are both
unit-tested. Tracked as a note on the PR rather than built out here.

Deferred items tracked as follow-up issues: **#541** (FQDN enforcement, option
3) and **#542** (warm-claim per-create override).
