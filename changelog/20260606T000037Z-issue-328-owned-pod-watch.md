# Issue #328 — Feat: watch owned Pods so Pod termination is detected promptly

**Date:** 2026-06-06T00:00:37Z
**Branch:** `feat-issue-328-owned-pod-watch`
**Area:** `area/controller`

## Background

#325 added a `Running`-phase liveness probe (`running_liveness_trigger`) that
folds a terminated backing Pod into `SandboxPhase::Failed`. A sandbox Pod's
terminal transition (envd's snapshot-and-exit, OOM-kill, eviction) fires **no
`Sandbox` watch event** — the watched object is the `Sandbox`, not the `Pod` —
so the probe relied on the controller's 30s `Running` requeue heartbeat to run.
Detection therefore landed within ~30s of the Pod dying. Correct, but a
deliberate latency compromise the `reconcile_adapter` requeue comment called
out.

## Change

Wire an owned-Pod watch onto the sandbox `Controller`. Sandbox Pods already
carry a controller `ownerReference` to their `Sandbox`, so `.owns::<Pod>` maps
a Pod event back to its owner with no custom mapper. A Pod reaching a terminal
phase now requeues its owner `Sandbox` within ~1s instead of waiting on the
heartbeat.

- `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`
  - Added `.owns(sandbox_pods, watcher::Config::default().labels(&pod_selector))`
    to the Sandbox `Controller` builder.
  - The watch is scoped with the label selector
    `app.kubernetes.io/name=sandbawks-sandbox` so node/api Pod churn doesn't
    drive needless reconciles.
  - Updated the `reconcile_adapter` doc comment: the heartbeat is no longer the
    *only* thing that surfaces a self-exited Pod — it is now the safety net
    behind the prompt owned-Pod event.

- `crates/sandbawks-controller/src/pod_adapter.rs`
  - Promoted the hard-coded `app.kubernetes.io/name` / `sandbawks-sandbox` pair
    to shared public constants `SANDBOX_POD_LABEL_KEY` /
    `SANDBOX_POD_LABEL_VALUE` (DRY). `pod_spec_for` stamps the label from these
    constants, and the controller builds its watch selector from the same
    source, so the two can never silently drift.
  - Added `sandbox_pod_carries_watch_selector_label`: asserts the literal
    values are stable (changing them would orphan the watch over
    already-running Pods) and that `pod_spec_for` actually stamps the label the
    selector matches.

- `crates/sandbawks-controller/src/lib.rs`
  - Re-exported the two new constants.

## Design notes

- This is purely a detection-latency optimization. The liveness probe from #325
  remains the actual decision logic, and the 30s `Running` heartbeat stays as
  the safety net — detection still lands within ~30s even if a Pod event is
  missed.
- The selector uses a `KEY=VALUE` equality match (not a key-existence match
  like the warm-pool controller's `.labels(WARM_POOL_LABEL)`), because
  `app.kubernetes.io/name` is a standard key other Pods set to other values.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all green, including the new
  `sandbox_pod_carries_watch_selector_label` unit test.

The cluster-level latency claim (terminal Pod requeues its owner within ~1s)
requires a live kind+gVisor cluster with a running sandbox and is not exercised
by the unit suite; the unit test covers the local invariant the watch depends
on (selector matches the stamped label). See the PR's manual test plan.
