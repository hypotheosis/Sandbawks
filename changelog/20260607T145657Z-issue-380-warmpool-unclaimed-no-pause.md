# Unclaimed warm-pool members must not auto-pause; anchor idle-timeout at claim

**Issue:** #380
**Branch:** `fix-issue-380-warmpool-unclaimed-no-pause`

## Problem

A warm pool exists so a pre-booted sandbox sits `Running`, ready to be handed
out the instant a request arrives. Pausing an **unclaimed** member defeats that
purpose — a paused member must be resumed before it can serve, which is exactly
the cold-start latency the pool is meant to eliminate.

The code *claimed* this was already the behavior: `warm_sandbox()` was commented
"Warm members are held `Running` until claimed — the reconciler never drives
them to `Paused`", and `reconcile::timeout_deadline` documented that "warm-pool
members are minted with `timeout_seconds: 0`". But that invariant only held when
`timeout_seconds == 0`, and **no real template uses 0**:

- `warm_pool::warm_sandbox` minted every member with
  `timeout_seconds = template.spec.default_timeout_seconds`.
- Real templates set this positive: `60` in `store.rs` and
  `deploy/kind/fixtures/sandbox-smoke.yaml`, `600` in the kind-smoke workflow.
- `timeout_deadline()` returns `creationTimestamp + timeout_seconds`, anchored
  at **mint time**.
- `timeout_trigger()` then fires `Trigger::Timeout { auto_pause: true }` once
  that deadline passes; the `(Running, Timeout)` edge drives the member to
  `Pausing`.

Net effect: an unclaimed warm member auto-paused ~`defaultTimeoutSeconds` after
it was minted, while still sitting in the pool. `PoolMembers::bucket` then routed
the `Paused`/`Pausing` member to `stale` and the reconciler deleted and re-minted
it — churn, not a warm pool. (Consistent with the field reports of "every
warm-pool pause Fails".)

## Fix

Two coupled changes — #1 alone still leaves a *claimed* member's deadline
anchored to mint time.

### 1. Mint warm members with the disabled-timeout sentinel (controller)

`warm_pool::warm_sandbox` now mints `timeout_seconds: 0` instead of inheriting
the template default. `WarmSandboxDefaults` (and `from_template`) drop the
now-unused `timeout_seconds` field — the template's `defaultTimeoutSeconds` is
deliberately not carried onto an unclaimed member. This makes
`timeout_deadline`'s long-standing doc comment ("warm members are minted with
`timeout_seconds: 0`") *true*, so `reconcile.rs` needed no logic change: a
sentinel-`0` member has no deadline and the reconciler never pauses it.

### 2. Anchor the post-claim deadline at the claim instant (gateway)

The whole system is `creationTimestamp`-anchored: the controller derives the
deadline as `creationTimestamp + spec.timeoutSeconds`, and `sandbox_to_dto`
renders `endAt` from the same formula. A warm member's `creationTimestamp` is its
(earlier) *mint* time, so stamping the raw caller duration at claim would place
the deadline a timeout after *mint* — typically already in the past for a member
that sat in the pool, insta-pausing it on claim.

`warm_claim::claim_for` now grows the stamped `spec.timeoutSeconds` to
`elapsed_since_creation + duration`, reusing the existing
`routes::sandboxes::timeout_offset_from_now` helper (now `pub(crate)`) that the
`/refresh` and `/resume` paths already use. The deadline — and the `endAt` the
gateway advertises — therefore land at `claim_instant + duration`, while
`TIMEOUT_DURATION_ANNOTATION` keeps the raw duration for later refresh/resume
re-anchoring. The `apply_overrides` trait method gained a `timeout_offset_seconds`
parameter for this; the `0` (disabled) sentinel is passed through unchanged,
never grown into a bare `elapsed`.

This was the change the prior author anticipated — the `apply_overrides` comment
already noted the annotation stays correct "even if `apply_overrides` is later
changed to write a grown `timeoutSeconds`."

## Tests

- `tests/warm_pool.rs`: `warm_sandbox` now asserts the minted member carries
  `timeout_seconds == 0` (was `300`); `WarmSandboxDefaults` fixtures drop the
  removed field.
- `warm_claim.rs` unit tests:
  - `claim_anchors_idle_timeout_at_claim_not_mint` — a member minted 10 min ago
    claimed with a 60s timeout gets a grown `~660s` offset and a `"60"` duration
    annotation.
  - `claim_keeps_disabled_timeout_disabled` — a `0` (disabled) timeout survives a
    claim on an old member without growing into `elapsed`.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — green (the new tests pass; existing claim/warm-pool tests
  unchanged). Note: `sandbawks-node`'s `dispatching_reconcile_emits_the_task_span`
  is a pre-existing intermittently-flaky tracing-span test in a crate this PR
  does not touch; it passes on isolation/re-run.

## Rollout (transient, self-healing)

Warm members that were minted by the **pre-fix** controller carry
`timeout_seconds = defaultTimeoutSeconds`, so each one will still auto-pause
**once** after this rolls out — the deadline was already stamped at their mint
time. This is operator-visible noise, not a correctness problem: the new
controller's `PoolMembers::bucket` routes a `Paused`/`Pausing` member to `stale`
and the reconciler deletes and re-mints it (now with `timeout_seconds: 0`), so
the pool drains the legacy members and refills with non-expiring ones within a
reconcile cycle. A kind-smoke run rolling from an old image will briefly show a
`Paused → deleted → replaced` cycle on pre-existing members during the deploy
window; freshly minted members never pause.

## Out of scope

- Lazy/eager resume tuning and pause-bundle streaming.
- The runsc `--root` mismatch pause failure (#377 lineage) — orthogonal; this
  change reduces how often warm-pool pause is attempted but does not fix that
  path.
- Per-pool/per-template configurable idle policy beyond "don't pause until
  claimed."
