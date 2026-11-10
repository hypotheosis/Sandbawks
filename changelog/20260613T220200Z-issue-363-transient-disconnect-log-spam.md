# Tame transient apiserver-disconnect ERROR spam (escalation + recovery signal)

**Issue:** #363 — `fix(node): transient apiserver-disconnect logs alarming ERROR spam (reconcilers already self-heal)`
**Area:** node, observability
**Date:** 2026-06-13

## Problem

When a `sandbawks-node` DaemonSet pod briefly lost its connection to the kube
apiserver (a transient HTTP/2 `SendRequest` failure — apiserver restart, LB
drain, network blip), the envd-staged label reconciler emitted an `ERROR` line
**every 30s tick** for the whole window, with no escalation threshold and no
"recovered" signal. A self-healing ~4-minute blip therefore read as an incident
in the logs even though nothing was lost.

Recovery was already automatic and correct: the kube-rs `Client` (hyper
connection pool) re-dials on the next request, the `node_label`
`tokio::time::interval` loop retries every tick, and `Controller::run` keeps
yielding across a transient watcher break. This change is **observability
hygiene only** — no change to retry/requeue timing or the kube client.

## What changed

### `crates/sandbawks-node/src/node_label.rs`

- New pure state machine `FailureStreak` (no I/O) tracking consecutive failed
  reconcile ticks, plus a `FailureLogLevel { Warn, Error }` decision and a
  `FAILURE_ESCALATION_THRESHOLD = 3` constant (~90s at the 30s tick interval).
- The reconcile loop now:
  - logs the **first** failures of a streak at `WARN` (transient blip), and
    escalates to `ERROR` only once the streak reaches the threshold — so a
    single-tick blip never hits `ERROR`;
  - emits one `INFO` "envd-staged label reconcile recovered after N failed
    tick(s)" line on the first successful tick after a streak, then resets;
  - keeps the per-tick `record_envd_staged_tick(ReconcileResult::PatchFailed)`
    counter on **every** failed tick regardless of log severity, so alerting
    keys off the `envd_advertised_total{result="patch_failed"}` rate, not log
    lines.
- Both failure log lines now carry a `consecutive_failures` structured field.

### `crates/sandbawks-node/src/reconcile.rs`

- Added a doc note at the `reconcile SnapshotJob error` WARN site clarifying
  that `event queue error` is kube-rs's transient watcher reconnect (the
  watcher re-lists on reconnect — no SnapshotJob/pause is lost), not a dropped
  job. Left at `WARN` (one line per reconnect is not spammy the way a per-tick
  `ERROR` is), so no escalation gating was added there.

## Tests

- `failure_streak_escalates_then_recovers` — drives fail×2 (WARN), fail×N
  (ERROR, sustained), success (recovery reports streak length once), success
  (silent), fail (fresh streak back at WARN).
- `failure_streak_escalates_exactly_at_threshold` — asserts the WARN→ERROR
  boundary lands exactly at `FAILURE_ESCALATION_THRESHOLD`.
- Both assert the pure state machine directly rather than capturing log output.
- Existing `envd_advertised_metrics` tests stay green (`patch_failed` counter
  semantics unchanged).

## Out of scope (unchanged)

- The runsc-root `--root` WARN (#366 / fixed by #367).
- OTel `PeriodicReader.ExportFailed` export errors (#356 / #350).
- Underlying apiserver/network reliability (infrastructure).
- Reconciler retry/requeue cadence and the kube client connection pool —
  recovery is already automatic.

## Validation

- `just fmt`
- `cargo clippy --all-targets -- -D warnings`
- `just test`
