# Issue #605 — PR #610 review: volumeClaimTemplate name validation + hardening

_2026-06-19 16:15 UTC — addressing review #307._

## Context

Review of PR #610 raised four points; the load-bearing one is the unvalidated
`pvc_name` concatenation (`{template}-{sandbox}`), a class of bug that has bitten
prior PVC work.

## Verified failure mode (correcting the review)

A `create_pod` error does **not** retry indefinitely: it folds the Sandbox to
terminal `phase=Failed` with `FailureReason::ActionFailure`
(`reconcile.rs:838-848`). So an invalid composed name produces a dead sandbox
carrying a buried apiserver "Invalid value … must be a DNS-1123 subdomain"
string — opaque, not a hot loop. Two facets the review's DNS-1123 framing
missed: (1) the template name is also used verbatim as the **Pod volume name**,
which must be a DNS-1123 *label* (≤63, no dots) — *stricter* than a PVC name; and
(2) the `-` join is actually collision-safe across sandboxes because the sandbox
half is always a fixed-format `sbx-<random>` suffix, so the real exposure is
*invalid* or *duplicate* names, not cross-sandbox aliasing.

## Changes

- **CRD admission validation** (`sandbawks-types`,
  `VolumeClaimTemplateMetadata.name`): `#[schemars(regex(pattern = …),
  length(max = 63))]` emits a DNS-1123-label `pattern` + `maxLength: 63` into
  both `sandboxes`/`sandboxtemplates` CRD schemas, so a malformed name is
  rejected at `kubectl apply`. CRDs regenerated.
- **Controller-side guard** (`pod_adapter.rs`):
  `validate_volume_claim_template_names` (+ a hand-rolled `is_dns1123_label`, no
  `regex` dep) runs at the top of `ensure_pvcs`, before any apiserver call. It
  rejects an invalid or duplicate template name with a clear, offender-naming
  message instead of an opaque apiserver rejection. Defense-in-depth for an
  older CRD with the validation pruned or a hand-patched object. (Addresses
  review #15905 + #15904.)
- **Foreign-PVC integration test**
  (`create_pod_refuses_foreign_owned_pvc_and_skips_pod`): drives 409 → GET →
  foreign-owner → error against the mock kube client and asserts `create_pod`
  fails closed and never POSTs the Pod — locking the security-load-bearing
  branch directly, not just via `pvc_owned_by` units. (Addresses review #15903.)
- **Doc** (`ensure_pvcs`): note that a `404` on the post-409 GET means a racing
  teardown and the wrapped retry is correct. (Addresses review #15902.)

## Notes

- List-uniqueness is enforced controller-side rather than via a CRD CEL rule: an
  `x-kubernetes-validations` list-uniqueness expression is disproportionate next
  to the O(n) controller check over a handful of short names, which also yields a
  better error message. Per-name validity (the part that must be caught at
  admission) is at the CRD layer.
- **Deploy:** Helm skips CRDs — `kubectl apply -f deploy/crds` after deploy or
  the new `pattern`/`maxLength` are silently pruned. No chart change → no
  `Chart.yaml` bump.

## Gates

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — green (2 new unit/integration tests; whole suite passing)
