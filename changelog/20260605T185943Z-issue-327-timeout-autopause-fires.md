# Issue #327 — Fix: timeout-based auto-pause/auto-terminate never fired

**Date:** 2026-06-05T18:59:43Z
**Branch:** `fix-issue-327-timeout-autopause-fires`
**Area:** `area/controller`

## Root cause

The `Sandbox` state machine has the right edges:

```rust
// crates/sandbawks-controller/src/state_machine.rs
(Running, Timeout { auto_pause: true })  => Some((Pausing, Action::Pause)),
(Running, Timeout { auto_pause: false }) => Some((Terminating, Action::Terminate)),
```

…but `reconcile::derive_trigger` never produced a `Trigger::Timeout`. Its match
arms only emitted `PodCreate`, `PodReady`, `PauseRequested`, `ResumeRequested`,
and `KillRequested` (plus `Failure` from the `Running`/`Creating` liveness
probes). Nothing tracked elapsed time against `spec.timeoutSeconds`, so the two
`(Running, Timeout)` edges were dead code: a healthy, long-running sandbox was
never auto-paused or auto-terminated — `spec.timeoutSeconds` / `spec.autoPause`
were effectively no-ops for the idle-timeout path, a resource/cost leak.

## Design decision: wall-clock since `creationTimestamp`, no CRD change

The issue listed three open questions. They are all resolved by an invariant the
**gateway already maintains** (`crates/sandbawks-api/src/routes/sandboxes.rs`):

- `sandbox_to_dto` renders the SDK's `endAt` as `creationTimestamp +
  timeoutSeconds`.
- `set_timeout` / keepalive (`refresh_sandbox_timeout`) rewrites
  `timeoutSeconds = elapsed_since_creation + keep_alive_seconds`, holding the
  invariant `creationTimestamp + timeoutSeconds == requested expiry`.

So:

1. **Idle vs. wall-clock** → the deadline is **wall-clock since
   `creationTimestamp`**. "Activity resets the clock" is handled *upstream* by
   the gateway bumping `timeoutSeconds`; the controller only honors the
   deadline. No envd activity reporting needed.
2. **Timestamp source** → `metadata.creationTimestamp`. **No new
   `SandboxStatus` field, no CRD change, no regen.**
3. **Requeue cadence** → the `Running` heartbeat is tightened to fire near the
   deadline (below).

`timeout_seconds == 0` is treated as the **disabled sentinel**: warm-pool
members are minted with `timeout_seconds: 0` and only get a real value when a
user claims them, so treating `0` as "expire now" would insta-pause the whole
warm pool.

## The fix

All in `crates/sandbawks-controller/`:

- **`reconcile.rs`**
  - `timeout_deadline(&Sandbox) -> Option<DateTime<Utc>>` — `creationTimestamp +
    timeoutSeconds`, returning `None` for the disabled sentinel, a missing
    timestamp, or arithmetic overflow.
  - `timeout_trigger(&Sandbox, now) -> Option<Trigger>` — pure, clock-injected;
    emits `Trigger::Timeout { auto_pause }` once `now >= deadline`.
  - Wired into the `Running` fallback chain **after** the liveness probe: a
    *dead* Pod still wins (→ `Failed`, since pausing would snapshot a corpse); a
    *live but timed-out* Pod gets `Timeout`.
  - `running_requeue_interval(&Sandbox, now, heartbeat) -> Duration` (pub) —
    clamps the remaining-time-to-deadline into `[1s, heartbeat]`; a past
    deadline floors at 1s; a disabled/uncomputable deadline keeps the unchanged
    `heartbeat`.
- **`bin/sandbawks-controller.rs`** — the `Running` requeue arm now calls
  `running_requeue_interval(..)` instead of a flat 30s, so the timeout fires
  near `endAt` rather than up to a heartbeat late. Far-off/disabled deadlines
  keep the 30s cadence, so the liveness-probe cadence is unchanged in the common
  case.

## Tests

- **`reconcile.rs` unit + property tests** (`mod tests`): `timeout_trigger`
  fires at/after the deadline with the right `auto_pause`, not before; `0`
  disabled sentinel; missing `creationTimestamp`; `running_requeue_interval`
  caps/tracks/floors. Property tests: trigger fires **iff** `now >= deadline`
  (mirroring `auto_pause`), and the interval always stays within `[1s,
  heartbeat]`.
- **`tests/reconcile.rs` integration**: an hour-old `Running` Sandbox with
  `autoPause: true` + a live Pod → `Pausing` + one Pause `SnapshotJob`; with
  `autoPause: false` → `Terminating` (no SnapshotJob); inside its window → stays
  `Running`; `timeout_seconds == 0` → stays `Running`.

## Validation

- `just fmt` — clean.
- `just clippy` (`cargo clippy --workspace --all-targets -- -D warnings`) —
  clean.
- `just test` (`cargo test --workspace --all-targets`) — all 59 test binaries
  pass, zero failures.

No Docker/Helm/CI artifact is added by this change; the runnable artifact is the
controller's reconcile path, exercised end-to-end against the in-memory fakes by
the integration tests above.

## Known interactions (resume after a long pause)

Wiring up the timeout surfaces a latent interaction on the **resume** path that
the missing trigger previously masked. `connect_sandbox`
(`crates/sandbawks-api/src/routes/sandboxes.rs`) patches
`spec.desiredPhase = Running` but does **not** bump `spec.timeoutSeconds`. So a
sandbox paused for longer than its `timeoutSeconds` window comes back with
`now >= creationTimestamp + timeoutSeconds` already true: the first `Running`
reconcile after resume immediately fires `Trigger::Timeout`, re-pausing it (with
`autoPause: true`, the SDK default) or terminating it right after the user
reconnected.

The fix in this PR is correct in isolation — the deadline math is right; it is
the resume path that fails to extend the deadline. Until `connect_sandbox` is
taught to extend the timeout on resume (tracked in #358 — extend to
`elapsed_since_creation + spec.timeoutSeconds`, the same shape as
`refresh_sandbox_timeout`), SDK consumers resuming a long-paused sandbox should
call `Sandbox.set_timeout` immediately after `Sandbox.connect()` to push the
deadline back into the future.
