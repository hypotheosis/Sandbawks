# Fix: warm-claim insta-pause race (idle-timeout activation ordering)

**Date:** 2026-06-12T00:14:46Z
**Regression introduced by:** #482 ("treat the sandbox timeout as an idle timeout")
**Crate:** `sandbawks-api` (`src/warm_claim.rs`)

## Symptom

Hard-down: after #482 merged and deployed, every claimed warm-pool member was
auto-paused the instant it was handed out. The investigated sandbox
`sbx-warm-m7jtx` transitioned `Running -> Pausing` 117 ms after its claim:

| time | event |
|------|-------|
| `23:53:57` | minted as warm member, `timeoutSeconds: 0` (disabled) |
| `23:56:29` | Creating -> Running (sat in pool) |
| `00:02:04.220` | claimed (`claimed-at` annotation, `timeoutSeconds` -> `300`) |
| `00:02:04.337` | reconcile on *object updated* -> Running -> Pausing |
| `00:02:09` | Pausing -> Paused |

The live CR showed `creationTimestamp: 23:53:57`, `spec.timeoutSeconds: 300`,
and **no `status.lastActivityTimestamp`**.

## Root cause

`KubeWarmClaim::apply_overrides` performed two **non-atomic** writes in the
wrong order:

1. **spec patch first** — flipped `timeoutSeconds` `0 -> 300`. This is the
   write that *activates* the idle timeout.
2. **`patch_status` second** — stamped `status.lastActivityTimestamp =
   claimed_at`, the baseline the controller measures the idle window from.

The controller watches the spec patch and reconciles it. When it did so
*before* the status stamp was visible in its reflector cache, the new
`activity_baseline()` fell back to the member's **stale mint-time
`creationTimestamp`** (`23:53:57`). Deadline = `creationTimestamp + 300 =
23:58:57`, already minutes in the past at claim -> `Trigger::Timeout {
auto_pause: true }` -> immediate pause.

Ruled out during investigation: RBAC (`sandboxes/status` rule **is** present in
the deployed `sandbawks-api` ClusterRole), version skew (both api and
controller pods were running #482 code), and a stray `0` timeout value
(`timeoutSeconds` was correctly `300`).

## Fix

Reverse the two writes: stamp `status.lastActivityTimestamp` **before** the
spec patch that flips `timeoutSeconds` `0 -> N`. While the timeout is still the
disabled `0` sentinel the deadline is `None`, so no pause can fire; by the time
the spec patch activates the timeout, the baseline is already present and the
deadline is `claimed_at + N` (in the future). This closes the race
deterministically instead of relying on the gateway's second write winning a
race against the controller. The spec patch is now issued last, so its response
still carries the freshly written stamp — the returned `Sandbox` renders
`endAt` from the claim instant, unchanged from before.

## Tests

- New: `warm_claim::tests::apply_overrides_stamps_activity_before_activating_timeout`
  drives the real `KubeWarmClaim` over a recording `kube::Client` and asserts
  the `/status` subresource patch precedes the bare-resource spec patch. The
  `FakeWarmClaim` cannot catch a two-write ordering bug (it builds a single
  object), so this guards the wire order directly.
- `cargo test -p sandbawks-api` — all pass.
- `cargo clippy -p sandbawks-api --all-targets -- -D warnings` — clean.
- `cargo fmt -p sandbawks-api -- --check` — clean.
