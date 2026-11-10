# Filesystem persistence: runtime + API guardrails (epic #603 Phase 4)

**Date:** 2026-06-19T17:14:08Z
**Issue:** #607
**Branch:** `feat-issue-607-filesystem-runtime-api-guardrails`

## Why

Epic #603 added a runtime-agnostic `filesystem` persistence mode so Kata — whose
VMM-level snapshot the `containerd-shim-kata` cannot drive (see #594) — can be
paused via the Pod lifecycle. Phase 1 (#609) added the `PersistenceMode` enum and
`volumeClaimTemplates`; Phase 2 (#610) provisions/mounts the PVCs; Phase 3 (#611)
wired the suspend behavior. This phase adds the **guardrails** that keep the new
mode safe at the runtime and API edges:

1. `snapshot` (live-memory checkpoint) is unsupported on Kata — the only Kata
   snapshot path is the out-of-band Cloud-Hypervisor one that *destroys the VM*
   (removed in #602). Rather than let that pairing fail-closed deep in the node's
   pause path, reject it up front with an actionable error so the operator knows
   to opt into `filesystem`.
2. The E2B pause/resume endpoints are mode-agnostic on the wire (they only flip
   `desiredPhase`), but what survives a pause differs by mode — that contract
   needed documenting.
3. A request carrying per-create `volumeClaimTemplates` cannot be served by a
   pre-warmed pool member (it has no PVCs bound for those claims), so the warm
   claim must explicitly fall through to cold-boot.

## What changed

### Shared validation (`crates/sandbawks-types/src/sandbox.rs`)

- `is_kata_runtime_class(&str) -> bool` — classifies a `RuntimeClass` name as
  Kata by the shared `kata` handler family (exact `kata`, or any `kata-` prefix:
  covers `kata`, `kata-fc`, and the AKS pool's `kata-vm-isolation`). Keyed on the
  family prefix rather than a baked-in environment-specific name, and normalized
  case/whitespace-insensitively for hand-applied CRs.
- `UnsupportedPersistenceMode` (thiserror) — `SnapshotOnKata { runtime_class }`,
  whose message names the offending class and the `filesystem` fix.
- `PersistenceMode::validate_for_runtime_class(self, Option<&str>)` — the single
  rule: `snapshot` on a Kata class is rejected; every other pairing (including
  the default `snapshot` on gVisor / the cluster default, and `filesystem`
  anywhere) is permitted. Callers pass the values *after* template-default
  resolution, since the default mode is `snapshot` — a Kata template that omits
  `persistenceMode` resolves to `snapshot` and is correctly rejected.
- Exported from `lib.rs`; unit-tested (Kata-family detection, the rejected and
  permitted pairings).

### Controller guard (`crates/sandbawks-controller/src/reconcile.rs`)

- `resolve_pod_inputs` — the single edge where both the persistence mode and the
  runtime class are fully resolved (template defaults filled) and through which
  every pod-materializing path flows (gateway cold-start, warm-pool mint, and a
  hand-applied `kubectl apply` CR) — now validates the resolved pairing and
  returns a `DispatchError` on `snapshot`-on-Kata. This is the defense-in-depth
  guard that catches a direct CR the gateway never sees; a rejection surfaces as
  a clear Sandbox-status failure instead of a node-side fail-closed pause.
- Unit test (`#[tokio::test]`) over a `FakeSandboxTemplateStore`: default
  `snapshot` on a Kata template is rejected with an actionable message; explicit
  `filesystem` on the same class resolves cleanly.

### API admission + docs (`crates/sandbawks-api/src/routes/sandboxes.rs`)

- `admit_persistence_mode(per_create, template_default, runtime_class)` — pure,
  unit-testable helper that resolves the effective mode (per-create override →
  template default → `snapshot` default) and returns
  `AppError::bad_request("unsupported_persistence_mode", ..)` on an unsupported
  pairing. `create_sandbox` calls it once the template read has filled the
  runtime class, so the caller gets a synchronous 400 rather than an async
  Failed.
- `pause_sandbox` / `connect_sandbox` doc comments now spell out the per-mode
  semantics: `snapshot` preserves live memory (restored in place; gVisor-only),
  `filesystem` preserves only PVC-mounted files (cold-boot re-mount; runtime-
  agnostic, Kata's only mechanism).
- Unit tests for the helper (rejects `snapshot`-on-Kata explicit and via the
  template default; accepts `filesystem`-on-Kata and any mode on gVisor/default).

### Warm-pool fallthrough (`crates/sandbawks-api/src/warm_claim.rs`, `warm_claim_metrics.rs`)

- `claim_for` now returns `Ok(None)` (cold-boot fallthrough) before any candidate
  is examined when the request carries a non-empty `volumeClaimTemplates` — a
  structural incompatibility independent of whether a pool exists. An *empty*
  list reads as "explicitly no volumes" and stays claimable, matching the
  `None ⇒ inherit` / `Some(empty) ⇒ none` precedence the controller uses.
- New terminal `ClaimOutcome::MissIncompatible` (`miss_incompatible` label) keeps
  the warm-vs-cold SLI denominator complete for the skipped claims.
- Unit tests: a per-create VCT request falls through (no member claimed) even
  with live candidates; an empty VCT list still claims.

## What did NOT change

- The E2B pause/resume **wire** contract — endpoints still only flip
  `desiredPhase`; the mode difference is in what the controller drives, not the
  request/response.
- The CRD schema — this is pure validation/admission logic, so no
  `deploy/crds` regen and no Helm chart change.
- `snapshot` behavior on gVisor and the cluster default — unchanged; the default
  pairing is explicitly permitted.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — green (one pre-existing timing flake in `crates/envd`'s
  `quiesce_drains_grpc_connections_and_rebinds`, unrelated to this change, passes
  on isolated re-run).
- Adjusted the pre-existing #289 test
  `create_pod_explicit_runtime_class_overrides_template_default`, which
  arbitrarily used `kata-fc` to prove runtime-class override: it now pairs that
  Kata class with `filesystem` (its only valid mode under the new guard) so it
  exercises the override without tripping the unrelated rejection.

## Follow-ups (out of scope, tracked elsewhere)

- E2E on the AKS Kata pool: `filesystem`-mode Kata sandbox, PVC at `/workspace`,
  write → pause (Pod deleted, PVC retained) → resume (fresh VM, file present).
- PVC policy (Disallowed/Allowed/Overrides) + paused-PVC retention — #603 Phase 5
  (#608).
- Warm pool *for* `filesystem` mode (pre-binding generic PVCs into warm members)
  is deferred; v1 cold-boots any VCT-carrying request.
