---
title: The sandbox lifecycle
description: How a sandbox moves between Creating, Running, Paused, and Failed — the idle timeout, how the gateway answers calls to a sandbox that isn't ready, and how the controller recovers from failed pauses, lost restores, and evictions.
slug: explanation/sandbox-lifecycle
head:
  - tag: title
    content: The sandbox lifecycle · Sandbawks docs
tableOfContents: true
---

A sandbox spends most of its life in one of a few phases: `Creating`, `Running`,
`Pausing`, `Paused`, `Resuming`, and finally `Terminating` or `Failed`. This page
explains what moves a sandbox between them, what a client sees while a sandbox
isn't ready, and how Sandbawks recovers when something goes wrong partway.

## Idle, not elapsed, time

The E2B `timeout` you pass when you create a sandbox becomes
`spec.timeoutSeconds`, and Sandbawks treats it as an idle timeout. The API
gateway records activity each time it forwards a request to the sandbox's `envd`
(file operations, commands, terminals) in `status.lastActivityTimestamp`. A
sandbox is paused only after it has gone a full timeout with no activity, not
when a fixed time since creation has passed. An agent that keeps working keeps its
sandbox, however long the session.

Creating a sandbox, setting its timeout, connecting to it, and claiming it from a
warm pool all count as activity too. That matters most for warm-pool members,
which may have been waiting in the pool for hours. Their timeout is `0`, meaning
disabled, until a client claims them, and the clock starts at the claim.

Sandboxes created through the E2B API have `autoPause` set, so the idle timeout
pauses them rather than terminating them. A sandbox paused this way stays paused
until a client connects to it again; the controller doesn't resume it on its own.

## What a client sees while a sandbox isn't ready

A sandbox can be unable to serve requests for many reasons: its Pod is still
pulling an image or waiting for a node, a resume is restoring a snapshot, or the
Pod is stuck in `ImagePullBackOff`. The controller summarizes the Pod's state in
`status.podStatus`, so the gateway, which can't read Pods itself, can tell a client
what's happening.

The gateway's goal is that a client, often an LLM driving an agent, gets a clear
"retry shortly" or "this won't work" rather than a hang or an opaque timeout:

- **Connecting returns quickly.** Connecting to a paused sandbox starts the resume
  and waits up to five seconds. If the sandbox isn't ready by then, the gateway
  returns its handle anyway; the client's next call does the waiting. This keeps a
  slow resume from exceeding the SDK's own request timeout.
- **A call to a sandbox that's still starting waits briefly.** A command waits
  silently up to `api.envdSilentParkMaxSecs` (15 seconds by default). If the
  sandbox becomes ready, the command runs; otherwise the client gets a notice
  saying the sandbox isn't ready yet and why. File operations fail with a
  retryable error that uses the `EAGAIN` ("resource temporarily unavailable")
  wording.
- **A leading `sleep` is honored.** Agents often handle "not ready" by sleeping
  and retrying. If a command starts with `sleep <seconds>`, the gateway waits that
  long, up to `api.envdSleepHonorMaxSecs` (300 seconds by default), sending
  keepalives so that proxies don't cut the connection. It then ends the call with
  exit code 100 and a message to resend the command if the sandbox is ready, or
  exit code 75 if it's still waiting. The command itself isn't run by that call.
- **A stuck sandbox says so.** If the Pod is in a state that won't recover on its
  own, such as an image that can't be pulled, the notice says that it won't
  become ready, rather than asking the client to retry.

## Bounded recovery

Several steps in a sandbox's life can fail halfway. In each case, Sandbawks retries
a bounded number of times and then stops in a state you can inspect, rather than
retrying forever or destroying a working sandbox.

- **A failed pause leaves the workload running.** If checkpointing a sandbox
  fails, the controller keeps its Pod running and retries the checkpoint up to
  `controller.pauseRecovery.maxRetries` times. If every attempt fails, the sandbox
  moves to `Failed` with `failureReason` `pause_checkpoint_failed`, but that
  failure is recoverable: if the workload is still healthy, the controller moves
  the sandbox back to `Running`. If the Pod is unreachable for longer than
  `controller.pauseRecovery.gcGraceSeconds`, the controller deletes it to free the
  node.
- **A lost restore is recreated.** If a resume's restore Pod can't be scheduled,
  or is scheduled but never becomes ready, the controller recreates it. After
  `controller.restore.maxRestarts` attempts, the sandbox moves to `Failed` with
  `failureReason` `resume_exhausted`.
- **An evicted sandbox is restored or replaced.** If the kubelet evicts a running
  sandbox's Pod or the kernel kills it for exceeding its memory, the controller
  restores it from its last snapshot if it has one; otherwise it starts a fresh
  sandbox. The `StateRestored` or `StateLost` condition records which happened, so
  you can tell whether files were lost.

Sandbawks also avoids causing evictions: sandbox Pods carry the
`descheduler.alpha.kubernetes.io/prefer-no-eviction` annotation, so a descheduler
configured to honor it won't evict an idle-looking sandbox to rebalance nodes.

## Following a sandbox's history

Each phase change produces a Kubernetes Event on the `Sandbox`, and the warm-pool
reconciler records Events on the `SandboxWarmPool`. `kubectl describe sandbox`
shows the recent history, and the
[Events reference](/reference/events/) lists every reason. Conditions on the
`Sandbox` keep the parts of that history that matter later, such as failures and
eviction recoveries.

## See also

- [Sandbox reference](/reference/custom-resources/sandbox/) — phases, status
  fields, failure reasons, and conditions.
- [Helm chart reference](/reference/helm-chart/) — the `controller.restore`,
  `controller.pauseRecovery`, and `api.envd*` values.
