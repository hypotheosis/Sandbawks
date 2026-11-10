# Issue #436: Controller Leader Election (HA)

**Date:** 2026-06-09T06:00:00Z
**Branch:** `feat-issue-436-leader-election-ha`
**Issue:** #436

## Summary

Implements leader election for the `sandbawks-controller` so it can run at
`replicas > 1` without double-reconciling or double-sweeping snapshots. The
elected leader runs both Controller loops (Sandbox + WarmPool) and the
snapshot-retention sweeper; standby replicas idle in an acquire loop and take
over within one Lease TTL (~15 s) if the leader's Pod is evicted or its node
fails.

## Changes

### `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`

Added manual leader election using the Kubernetes `coordination.k8s.io/v1`
Lease API (already available via `k8s-openapi` with the `v1_31` feature):

- **`LeaderElectionConfig`** — reads `SANDBAWKS_LEADER_ELECTION_ENABLED`,
  `SANDBAWKS_LEADER_LEASE_NAME`, `POD_NAME`, `SANDBAWKS_LEADER_LEASE_DURATION_SECS`,
  `SANDBAWKS_LEADER_RENEW_DEADLINE_SECS`, and `SANDBAWKS_LEADER_RETRY_PERIOD_SECS`
  from the environment.
- **`make_lease()`** — constructs a typed `Lease` object.
- **`try_acquire_or_renew()`** — attempts to create or renew the Lease using
  optimistic concurrency (`resourceVersion`): two concurrent standbys
  observing an expired Lease race via PUT; only one wins (HTTP 409 for the
  loser).
- **`main()` gate** — when enabled, polls `try_acquire_or_renew()` until
  acquired, then spawns a background renewal task. The renewal task calls
  `std::process::exit(1)` if the Lease is lost or the renewal deadline is
  exceeded, causing Kubernetes to restart the Pod and allowing a standby to
  take over.

The gate is placed **before** both Controller loops and the retention sweeper
so all controller work (including snapshot GC) runs only on the leader.

### `deploy/helm/sandbawks/templates/rbac.yaml`

Added `coordination.k8s.io/leases` to the controller ClusterRole:

```yaml
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

### `deploy/helm/sandbawks/values.yaml`

Added `controller.leaderElection` block:

```yaml
controller:
  leaderElection:
    enabled: false          # set true alongside replicas > 1
    leaseName: sandbawks-controller-leader
    leaseDurationSeconds: 15
    renewDeadlineSeconds: 10
    retryPeriodSeconds: 2
```

### `deploy/helm/sandbawks/templates/controller-deployment.yaml`

When `controller.leaderElection.enabled` is true, the chart now injects:
- `POD_NAME` (downward API `metadata.name`) — used as the Lease holder identity
- `SANDBAWKS_LEADER_ELECTION_ENABLED=true`
- `SANDBAWKS_LEADER_LEASE_NAME`
- `SANDBAWKS_LEADER_LEASE_DURATION_SECS`
- `SANDBAWKS_LEADER_RENEW_DEADLINE_SECS`
- `SANDBAWKS_LEADER_RETRY_PERIOD_SECS`

## Design decisions

- **No new Rust dependencies.** `k8s-openapi` (already in the workspace with
  the `v1_31` feature) provides `coordination::v1::Lease` and `LeaseSpec`.
  The `kube-leader-election` crate (0.43.0) was evaluated but requires
  `kube ^3`, incompatible with the workspace's `kube 0.97`.
- **`process::exit(1)` on lease loss.** The simplest fail-fast model:
  Kubernetes restarts the Pod, which then enters the standby acquire loop. An
  in-place "re-acquire after loss" state machine was rejected as more complex
  with the same mean-time-to-recovery.
- **Optimistic concurrency on PUT.** The `replace` call carries the Lease's
  current `resourceVersion`; a concurrent standby gets HTTP 409 and returns
  `false` rather than silently winning a split-brain.
- **Default disabled.** `leaderElection.enabled: false` preserves the current
  single-replica behavior with zero startup overhead.

## Validation

- `just fmt` ✅
- `cargo clippy -p sandbawks-controller --all-targets -- -D warnings` ✅
- `cargo test -p sandbawks-controller` ✅ (22 tests pass)
- Workspace `just test` / `just clippy` blocked by pre-existing `envd` protoc
  build failure (no `protoc` in this environment); not introduced by this PR.

## Acceptance criteria from issue

| Criterion | Status |
|---|---|
| `replicas: 1` works unchanged | ✅ Leader election is opt-in (`enabled: false` default) |
| Retention sweeper runs only on leader | ✅ Gate precedes `build_retention_runner()` |
| `coordination.k8s.io/leases` RBAC | ✅ Added to controller ClusterRole |
| Standby takeover within one Lease TTL | ✅ Renewal task exits on loss; default TTL 15 s |
| `replicas > 1` without leader election unsupported | ✅ Documented in values.yaml |
