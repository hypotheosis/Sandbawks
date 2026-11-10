---
title: Events
description: Reference for the Kubernetes Events the Sandbawks controller records on Sandbox and SandboxWarmPool resources — reasons and types.
slug: reference/events
head:
  - tag: title
    content: Events · Sandbawks docs
tableOfContents: true
---

The controller records Kubernetes Events on `Sandbox` and `SandboxWarmPool`
resources. View them with `kubectl describe` or `kubectl get events`:

```sh
kubectl get events -n team-acme --field-selector involvedObject.kind=Sandbox
```

## Sandbox events

| Reason | Type | Recorded when |
|--------|------|---------------|
| `PodCreated` | Normal | The sandbox's Pod was submitted for scheduling. |
| `PodReady` | Normal | The Pod became ready and the sandbox is `Running`. |
| `PauseRequested` | Normal | A pause was requested. |
| `AutoPause` | Normal | The idle timeout was reached and the sandbox is being paused. |
| `Timeout` | Normal | The idle timeout was reached and the sandbox is being terminated, because `autoPause` is `false`. |
| `Paused` | Normal | The snapshot was validated and committed; the sandbox is `Paused`. |
| `Suspended` | Normal | A `Filesystem`-mode pause finished: the Pod was deleted and the PVCs kept. |
| `ResumeRequested` | Normal | A resume was requested; the restore Pod is being scheduled. |
| `NodeAssigned` | Normal | The restore Pod was scheduled; the snapshot is being staged on its node. |
| `SnapshotStaged` | Normal | The snapshot was staged on the node; the restore can start. |
| `ResumeSucceeded` | Normal | The sandbox is `Running` after a restore. |
| `RestoreFromRequested` | Normal | The sandbox is being created from another sandbox (`restoreFrom`). |
| `TerminateRequested` | Normal | Termination was requested. |
| `RestorePodLost` | Warning | The restore Pod was lost or stuck and is being recreated. |
| `PauseRetry` | Warning | A pause checkpoint failed and is being retried. |
| `RecoverySucceeded` | Normal | After a pause failure, the workload was healthy and the sandbox returned to `Running`. |
| `OrphanedPodGC` | Warning | After a pause failure, the workload was unreachable and its Pod was deleted. |
| `EvictionRecovery` | Warning | The Pod was evicted or killed, and the sandbox is being restored from its snapshot or started fresh. |
| `PvcRetentionReclaimed` | Normal | A paused `Filesystem`-mode sandbox passed its PVC retention window and was deleted. |
| `Failed` | Warning | The sandbox moved to `Failed`. The message gives the cause. |

## SandboxWarmPool events

| Reason | Type | Recorded when |
|--------|------|---------------|
| `ToppedUp` | Normal | New members were created to reach `minReady`. |
| `TrimmedExcess` | Normal | Ready members above `maxReady` were deleted. |
| `ReapedFailed` | Warning | Members in `Failed` or `Terminating` were deleted. |
| `ReclaimedStale` | Normal | Members left `Paused` or `Pausing` by older Sandbawks versions were deleted. |
| `EvictedVersionStale` | Normal | Members created from an older version of the template were replaced. |
| `TemplateNotFound` | Warning | The pool's `templateRef` doesn't match a `SandboxTemplate`. |
| `WarmPoolBackoff` | Warning | The mint-failure circuit breaker opened; the pool stops creating members until the backoff ends. |
| `WarmPoolRecovered` | Normal | The circuit breaker closed; the pool resumes creating members. |

## See also

- [Sandbox reference](/reference/custom-resources/sandbox/) — conditions, which
  keep the important parts of this history on the resource.
- [SandboxWarmPool reference](/reference/custom-resources/sandboxwarmpool/) — the
  circuit breaker.
