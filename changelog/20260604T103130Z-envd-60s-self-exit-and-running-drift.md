# envd 60 s self-exit + Sandbox `Running`/Pod `Completed` drift

**Date:** 2026-06-04T10:31:30Z
**Branch:** `fix/envd-60s-self-exit-and-running-drift`
**Crates touched:** `envd`, `sandbawks-types`, `sandbawks-controller`

## Symptom

Sandboxes reported `status.phase: Running` while their backing Pods were
`Completed` (phase `Succeeded`, envd `exitCode: 0`). Every sandbox Pod —
whether created via the E2B SDK or directly with `kubectl` — died at
**exactly 60 seconds** after boot, and the controller never noticed.

## Diagnosis (two stacked bugs)

### A — envd self-terminates 60 s after boot (root cause)

`crates/envd/src/bin/envd.rs` wrapped the gRPC serve task in
`tokio::time::timeout(DRAIN_TIMEOUT, serve_task)` **without first waiting
for a shutdown signal**. `tokio::time::timeout` starts its clock the moment
it is polled, so the 30 s cap bounded the *serving lifetime*, not the
post-signal drain. With no signal ever sent, the gRPC timeout elapsed at
`t+30s` and the HTTP-listener timeout at `t+60s`, after which envd flushed
its snapshot and exited `0` → Pod `Completed`. No SIGTERM, no `Killing`
event, no `SnapshotJob` — purely internal. Latent since the envd entrypoint
was written (#91); only bites now that sandboxes are expected to outlive
60 s.

Confirmed live: a `kubectl`-created Sandbox (no api-gateway involvement,
byte-identical Pod spec) died identically at `+60.0s`, isolating the bug to
envd itself.

### B — controller leaves the Sandbox `Running` over a dead Pod

`reconcile::derive_trigger` had no edge out of `Running` for a Pod that
terminates on its own — the only exits were `desiredPhase=Paused`
(`PauseRequested`) and `deletionTimestamp` (`KillRequested`). A Pod going
`Succeeded`/`Failed` produced no trigger, so the Sandbox stayed `Running`
forever, masking bug A.

## Fix

### A — gate the drain cap on the shutdown signal

Added `join_with_drain_cap(task, gate, cap)`: it awaits a shutdown-broadcast
`gate` **before** applying `tokio::time::timeout(cap, task)`. Until a signal
fires the gate blocks, so both servers serve unbounded; once it fires, `cap`
bounds only the drain — the original intent. Two dedicated gate subscribers
(`grpc_drain_gate`, `http_drain_gate`) are taken on the existing broadcast
channel. ~3 lines of behavior change plus the extracted, unit-testable
helper.

### B — `Running` liveness probe → `Failed`

- New `RuntimeAdapter::pod_terminated(&PodRef) -> Result<bool>` (default
  `false`, so existing fakes are unaffected). `PodAdapter` overrides it:
  `true` when `status.phase` is `Succeeded`/`Failed` or the Pod is gone
  (404); transient GET errors propagate so the probe retries rather than
  declaring a live Sandbox failed.
- `reconcile()` runs `running_liveness_trigger` when the Sandbox is
  `Running` and no spec/SnapshotJob trigger applies; a terminated Pod yields
  `Trigger::Failure`, which the existing `(Running, Failure) → Failed` edge
  in `state_machine` folds into `SandboxPhase::Failed`. No CRD change
  (`Failed` already exists).
- Detection rides the existing 30 s `Running` requeue heartbeat (now
  load-bearing, documented in `reconcile_adapter`); prompter detection would
  need an owned-Pod watch, deliberately deferred.

## Tests

- `envd`: `drain_cap_does_not_start_until_shutdown_fires` (regression for the
  60 s self-exit) and `drain_cap_applies_after_shutdown_fires`, using
  `#[tokio::test(start_paused = true)]` virtual time (added `test-util` to
  envd dev-deps).
- `sandbawks-controller`: `running_with_terminated_pod_transitions_to_failed`
  and `running_with_live_pod_stays_running` in `tests/reconcile.rs`;
  `FakeRuntimeAdapter::with_pod_terminated` toggles the probe.

## Verification

- `cargo fmt --check` clean
- `cargo clippy -p envd -p sandbawks-controller -p sandbawks-types --all-targets -- -D warnings` clean
- `cargo test` for all three crates green (envd bin 6, controller reconcile 17, full suites pass)
- `cargo build --workspace` clean (node + api still satisfy the trait)

## Not addressed (follow-ups)

- `Trigger::Timeout` (`spec.timeoutSeconds` auto-pause) is handled in
  `state_machine` but never emitted by `derive_trigger` — the auto-pause /
  auto-terminate-on-timeout path is dead. Separate latent gap.
- Existing Sandboxes already stuck `Running` over dead Pods will fold to
  `Failed` on their next 30 s heartbeat once this controller ships; no
  manual cleanup needed.
