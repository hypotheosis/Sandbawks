# Issue #282 — periodic `envd-staged` node-label reconciler

Date: 2026-06-02T21:31:26Z
Branch: `feat-issue-282-periodic-envd-staged-label`

Follow-up to #272 / #281. Relates to #270.

## Problem

PR #281 (issue #272) added a scheduling gate: `sandbawks-node` stamps the
`sandbawks.io/envd-staged` label on its Node once at boot, after verifying
the staged binary is present, and the controller stamps a matching
`Exists` nodeAffinity onto sandbox Pods so they never schedule onto a
not-yet-staged node.

But `advertise_envd_staged` was a **one-shot at boot**. Once set, nothing
ever cleared or re-asserted the label:

- If the staged binary later disappears (ephemeral hostPath that doesn't
  survive a node reboot, operator cleanup, corruption) while the node
  binary keeps running, the label is stale. Pods keep scheduling, mount
  the now-empty dir, and CrashLoop on the missing binary — i.e. #272
  recurs.
- A stale label can also survive across a reboot long enough to schedule a
  CrashLooping sandbox.

A node-binary crash/restart already self-heals (it re-runs the boot
advertise), so the unhandled cases are *binary-disappears-while-running*
and *stale-label-across-reboot*.

## Resolution

Added a small periodic reconciler in `crates/sandbawks-node/src/node_label.rs`
that makes the label track on-disk reality for the life of the process:

- `staged_label_removal_patch()` — a merge patch that sets the label to
  JSON `null` (RFC 7386 key deletion), touching nothing else.
- `patch_node_labels()` — shared helper so the add and remove paths share
  one field-manager / patch-type choice.
- `reconcile_envd_staged()` — one tick: binary present → patch the label
  present (`AdvertiseOutcome::Advertised`); binary missing → patch the
  label removed (`AdvertiseOutcome::Withdrawn`). A missing binary is *not*
  an error here (unlike the fail-closed boot advertise); it is the
  condition the reconciler exists to react to. Only a failed patch
  returns `Err`.
- `run_envd_staged_reconciler()` — ticks every `interval`
  (`DEFAULT_RECONCILE_INTERVAL = 30s`), logging a structured `result=` field
  per tick (`ok` / `missing` / `patch_failed`). Patch failures are logged
  and retried next tick rather than crashing the DaemonSet. The immediate
  first interval tick is skipped because the boot advertise already fired.

The binary (`src/bin/sandbawks-node.rs`) spawns the reconciler as a
detached task right after the fail-closed boot advertise, with a new
`--envd-staged-reconcile-secs` flag (env
`SANDBAWKS_ENVD_STAGED_RECONCILE_SECS`, default 30).

### Observability note

The issue proposed a `sandbawks_node_envd_advertised_total{result=...}`
counter "once this becomes periodic." The workspace has no metrics
exporter wired in yet, so adding a Prometheus dependency would be scope
creep. Instead each tick emits a structured `tracing` event carrying the
same `result` label (`ok` / `missing` / `patch_failed`), which is the
idiomatic observability surface for this codebase and is trivially
promotable to a counter when a metrics exporter lands.

## Acceptance

- The `envd-staged` label is removed within one tick (default 30s) of the
  staged binary becoming unavailable — covered by
  `reconcile_withdraws_label_when_binary_missing` (asserts the merge patch
  nulls the label).
- The label is re-added within one tick of restoration — covered by
  `reconcile_readvertises_label_when_binary_present`.
- A node on non-persistent staging storage no longer strands a stale label
  across a reboot: the periodic loop withdraws it within a tick of the
  binary going missing, independent of any node-binary restart.

## Tests

- Unit: `staged_label_removal_patch_nulls_only_the_staged_label`.
- Wire (mocked kube transport): `reconcile_withdraws_label_when_binary_missing`,
  `reconcile_readvertises_label_when_binary_present`.
- Loop (`start_paused` + auto-advance): `run_reconciler_skips_first_tick_then_tracks_binary`
  drives `run_envd_staged_reconciler` itself, asserting the first PATCH
  lands one full interval in (skip-first-tick) and that the loop tracks the
  binary present→missing across ticks.

## Review-response changes (PR #285 review #115)

Addressed seven self-review comments:

1. **Reject `--envd-staged-reconcile-secs 0`.** A zero interval panics
   `tokio::time::interval`; the CLI now uses `value_parser = clap::value_parser!(u64).range(1..)`
   so the typo is rejected at startup instead of orphaning a dead
   reconciler (a stale-label fail-open).
2. **`MissedTickBehavior::Delay`.** Set on the ticker to keep spacing
   stable after a missed tick rather than burst-firing, matching the
   controller's retention sweep.
3. **Supervised, not detached.** The binary keeps the reconciler's
   `JoinHandle` and `tokio::select!`s it against the main reconcile loop;
   if the reconciler ever resolves (panic/cancel) the process exits
   non-zero so kubelet restarts the Pod rather than running on with a dead
   gate.
4. **Comment accuracy.** Reworded the spawn-site comment that implied the
   *task* (rather than the handle) is dropped at shutdown.
5. **Steady-state observability.** Added a one-shot `tracing::info!` at
   reconciler entry (`interval`, `node`, `path`) so "is the loop alive?"
   is answerable at the default `info` level despite per-tick `Advertised`
   logging at `debug`.
6. **Loop test.** Added the `start_paused` test above.
7. **Counter follow-up.** Filed #286 to promote the `result=` tracing
   events to the `sandbawks_node_envd_advertised_total` counter when a
   metrics exporter lands; referenced from the code.

## Validation

- `just fmt` — clean.
- `just clippy` (`-D warnings`) — clean.
- `just test` (`cargo test --workspace --all-targets`) — all pass.
- `sandbawks-node --help` — confirms `--envd-staged-reconcile-secs`
  (default 30, env `SANDBAWKS_ENVD_STAGED_RECONCILE_SECS`).
