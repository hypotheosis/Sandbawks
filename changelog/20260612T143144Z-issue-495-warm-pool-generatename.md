# Pool-derived `generateName` for warm-pool sandboxes

**Issue:** #495 — *warm sandboxes use a fixed `sbx-warm-` generateName regardless of pool name*
**Date:** 2026-06-12T14:31:44Z
**Area:** `area/controller` · `kind/fix`

## Problem

Every warm-pool member `Sandbox` was minted with a hardcoded
`metadata.generateName` of `sbx-warm-`, so apiserver-assigned names
(`sbx-warm-7x9qd`) carried no trace of their owning `SandboxWarmPool`. With
more than one pool in a namespace the names were indistinguishable at a glance
(`kubectl get sandboxes`, logs, traces); only the `sandbawks.dev/warm-pool`
label told them apart.

## Change

`crates/sandbawks-controller/src/warm_pool.rs`:

- Added `warm_generate_name(pool_name)` which builds a `sbx-<pool>-` prefix,
  folding the pool name in so an assigned member name self-identifies its pool.
- Added the `MAX_GENERATE_NAME_PREFIX = 58` constant documenting the
  apiserver's prefix budget (it reserves 5 of a 63-char window for the random
  suffix and silently truncates a longer `generateName`). The helper truncates
  the pool segment to fit and trims any hyphen exposed at the cut boundary so
  the prefix always ends in exactly one clean `-` (never `sbx-foo--<suffix>`).
- Truncation is done on a UTF-8 char boundary (`char_indices().nth`) to avoid a
  panic on a multibyte name, even though K8s names are ASCII in practice.
- `warm_sandbox()` now computes the prefix once (before `pool_name` is moved
  into the `OwnerReference`) and passes it to `generate_name`.
- Updated the `warm_sandbox` doc comment to describe the pool-derived form and
  reaffirm that `WARM_POOL_LABEL` — not the name prefix — remains the source of
  truth for claim/trim matching.

## Behavioral scope

- Inert for pool bookkeeping: claim (`warm_claim.rs`) and trim (`plan_pool`)
  match members by the `sandbawks.dev/warm-pool` label, never by the name
  prefix. Grep confirmed no logic keys off the `sbx-warm-` literal.
- Only affects members minted **after** this change; existing generated names
  are immutable and unchanged.
- Cold-boot (non-warm) sandbox naming is untouched.

## Tests

`crates/sandbawks-controller/tests/warm_pool.rs`:

- Updated `warm_sandbox_carries_pool_label_and_owner_ref` to assert the
  `sbx-python-pool-` prefix for the `python-pool` fixture.
- Added `warm_sandbox_truncates_overlong_pool_name_in_generate_name`: a
  73-char pool name with a hyphen on the 53-char cut boundary asserts the
  *exact* resulting prefix (`sbx-<52 a's>-`, 57 chars). This pins the
  trailing-hyphen trim unambiguously — without the trim the prefix would be the
  58-char `sbx-<52 a's>--`, so the exact-value assertion catches any regression
  that drops the trim.

## Validation

`just check` — `cargo fmt --all -- --check`, `cargo clippy --workspace
--all-targets -- -D warnings`, `cargo test --workspace --all-targets` — all
green (exit 0, no warnings).
