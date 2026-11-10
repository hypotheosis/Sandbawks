# Default persistenceMode to `filesystem` when `volumeClaimTemplates` is set

**Date:** 2026-06-20T16:00:00Z
**Issue:** #616
**Branch:** `feat-issue-616-infer-filesystem-from-vcts`

## Why

`volumeClaimTemplates` (VCTs) and `persistenceMode` are independent fields, and
`persistenceMode` defaults to `snapshot`. So a template/Sandbox that declares
`volumeClaimTemplates` but leaves `persistenceMode` unset resolved to `snapshot`
— even though VCTs are **only** meaningful in `filesystem` mode (in `snapshot`
mode a declared PVC is provisioned but never part of the runtime checkpoint).
On Kata that resolved-`snapshot` was then rejected by the #607 guard; on gVisor
it silently provisioned PVCs the snapshot pause path never used.

The 2026-06-19 incident (epic #603): a `compound-sandbox` Kata template had VCTs
set but `persistenceMode` unset, resolved to the `snapshot` default, the #607
guard rejected `snapshot`-on-Kata at `resolve_pod_inputs`, and the warm pool
re-minted into a `Failed`-storm. The operator's intent — they declared a
workspace PVC — was unambiguous, but they also had to know to set a second,
separate field. This inference removes that footgun: declaring
`volumeClaimTemplates` is itself the signal for `filesystem` mode. (The
warm-pool *amplification* is tracked separately in #615.)

## What changed

### Shared helper (`crates/sandbawks-types/src/sandbox.rs`)

- `resolve_effective_persistence_mode(sandbox_mode, template_mode,
  has_volume_claims) -> PersistenceMode` — the single rule, a pure function so
  the two callers can't drift. Resolution order: explicit Sandbox mode →
  explicit template mode → (`filesystem` if `has_volume_claims` else `snapshot`).
  Inference fills *only* the otherwise-`snapshot` default slot; an explicit mode
  (including an explicit `snapshot` alongside VCTs) is honored unchanged.
- Exported from `lib.rs`. Unit-tested for the resolution matrix: explicit-wins
  (Sandbox and template), Sandbox-over-template precedence, inference-on-VCTs,
  and no-VCTs-default.

### Controller (`crates/sandbawks-controller/src/reconcile.rs`)

- `resolve_pod_inputs` computes `has_volume_claims` from the *effective* (merged)
  VCT set, calls the shared helper, and stamps the resolved mode onto the
  effective Sandbox before the #607 `validate_for_runtime_class` guard — so a
  storage-declaring Kata template now resolves to `filesystem` and passes the
  guard. Logs at debug when the inference fires (unset mode → `filesystem` from
  non-empty VCTs), alongside the existing runtime-class resolution trace. This
  covers cold-start, warm-pool mint, and hand-applied CRs (all flow through here).
- The pause/resume resolver (the module-local async
  `resolve_effective_persistence_mode`, #606) now applies the same inference,
  computing the effective VCT set the same way, so a running sandbox's pause
  mechanism agrees with the mode its Pod was materialized under rather than
  diverging back to the bare `snapshot` default.

### Gateway (`crates/sandbawks-api/src/routes/sandboxes.rs`)

- `admit_persistence_mode` gains a `has_volume_claims` parameter and delegates to
  the shared helper, so admission and the controller's resolution agree for every
  combination. The E2B create body carries no per-create VCTs, so the template's
  `volumeClaimTemplates` non-emptiness *is* the effective set at this edge.

## Backward compatibility

- **Explicit always wins** — an explicit `persistenceMode` (including `snapshot`)
  is untouched; on Kata explicit `snapshot` is still rejected.
- **No VCTs ⇒ no change** — every template/Sandbox without `volumeClaimTemplates`
  keeps resolving to `snapshot` exactly as before.
- **Runtime-agnostic** — the inference applies on every runtime; on Kata it
  additionally means a storage-declaring template "just works" past the #607 guard.

## Validation

`just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
New unit tests on the pure helper plus controller (`resolve_pod_inputs` infers
`filesystem` from VCTs / still rejects no-VCT snapshot on Kata) and gateway
(`admit_persistence_mode` infers `filesystem` from VCTs on Kata) coverage.
