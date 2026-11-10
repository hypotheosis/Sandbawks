# Issue #247 — Resume crash-recovery: don't promote `Running` on a deleted Pod

**Date:** 2026-06-01T14:50:50Z
**Branch:** `fix-issue-247-resume-stale-podref-promote`
**Crate:** `sandbawks-controller`

## Problem

`Action::CreateRestorePod` (`reconcile.rs`) performs, in order:

1. `delete_pod(stale pre-pause Pod)` — a real cluster side effect,
2. scrub `pod_ref` / `pod_uid` / `envd_address` on the **in-memory** `new_status`,
3. `create_pod(restore)` and set `pod_ref`,
4. return → `reconcile` patches the post-dispatch status.

A controller crash between steps 1 and 4 loses the in-memory scrub. The
durable `status` still carries the pre-pause `envd_address` while
`phase == Resuming` and `snapshot_job_ref == None`. On the next reconcile:

- the pre-dispatch probe was gated on `!has_addr`, so a recorded (stale)
  address skipped re-validation;
- `derive_trigger` read the stale `envd_address` as readiness and emitted
  `Trigger::PodReady`;
- `(Resuming, PodReady) → (Running, Action::Noop)` promoted the Sandbox to
  `Running` while its `pod_ref` named the Pod deleted in step 1.

Latent before PR #246; that PR (which actually deletes the stale Pod
rather than relying on `create_pod`'s 409-as-success) made it observable.

## Fix (option b — authoritative readiness probe)

The restore Pod reuses the Sandbox's name (`PodAdapter::pod_spec_for`), so
the durable `pod_ref` keeps naming the correct Pod across the crash. The
pre-dispatch readiness probe in `reconcile()` is now **authoritative for
`Resuming`**:

- It re-validates against the apiserver even when `envd_address` is already
  set (cold-start `Creating` keeps the original `!has_addr` short-circuit —
  its address is only ever written by a live probe and is never stale).
- The probe is skipped while a Stage job is in flight (`snapshot_job_ref`
  set), since the SnapshotJob observer short-circuits the reconcile there
  anyway — staging-phase status is left untouched.
- When the probed Pod is **not Ready** (gone, still booting, or unreachable
  — `Ok(None)` or a transient `Err`) and a stale address is recorded, the
  probe **suppresses `envd_address` + `pod_uid` in memory for that pass
  only** (no `patch_status`) and keeps `pod_ref` untouched in durable
  status.

`Err` is treated like `Ok(None)` (not allowed to fall through to a
promotion) because the failure can be *partial* — the Pod GET fails while
the Sandbox PATCH would still succeed — and promoting on an address we
could not re-validate is exactly the bug this fix closes. The suppression
is in-memory rather than a durable wipe to avoid a status-write storm under
apiserver flakiness (a PATCH per requeue per Resuming Sandbox); it is
self-healing because the next requeue re-probes from scratch. This mirrors
the `Ok(Some)` arm's asymmetry: a positive Ready answer is authoritative
(its partial fields are kept via `.or(...)`), a non-answer is not.

Net effect: promotion to `Running` now *always* requires a successful live
probe of the Pod named by `pod_ref`, so a Sandbox can never be `Running`
while `pod_ref` names a deleted Pod.

### Recovery behaviour

- **Restore Pod was (re)created** (same name): the next probe writes the
  fresh address and promotes correctly — automatic recovery.
- **Restore Pod never created** (narrow window after `delete_pod`, before
  `create_pod`): the Sandbox stays safely stuck at `Resuming` (fail-stuck,
  never fail-wrong). Re-driving `CreateRestorePod` automatically is *not*
  done because `Ok(None)` cannot distinguish "Pod gone" from "Pod booting",
  and re-driving in the latter case would destroy a live restore Pod.

No changes to the state machine, `derive_trigger`, or the `Action::Noop`
dispatch arm — the fix is localized to the pre-dispatch probe.

## Tests

- `resuming_with_stale_envd_address_and_no_live_pod_does_not_promote`
  (`tests/reconcile.rs`): seeds the crash-window durable state (stale
  `envd_address`/`pod_uid`/`pod_ref` at `Resuming`, no `snapshot_job_ref`)
  with a `NoReadyPodAdapter` (`pod_ready → Ok(None)`); asserts the Sandbox
  stays `Resuming` (not promoted) and that no durable write occurred (the
  seeded status is left intact — the safety comes from not promoting, not
  from scrubbing).
- `resuming_with_stale_envd_address_and_probe_error_does_not_promote`
  (`tests/reconcile.rs`): same seed with a `ProbeErrorPodAdapter`
  (`pod_ready → Err(Transient)`), locking the `Err(_)` arm against future
  refactors.
- Existing resume tests
  (`paused_with_desired_running_stages_then_creates_restore_pod`,
  `create_restore_pod_deletes_the_stale_paused_pod`) continue to pass: the
  new `!staging_in_flight` gate preserves the prior behaviour exactly
  (skip while staging; probe once the ref clears).

## Refs

- PR #246 review thread on `reconcile.rs` (the stale-Pod delete).
- Plan 07 epic #204.
