# Issue #625 — "Sandbox not ready yet" notice on tool calls racing a not-yet-serving sandbox

_Date: 2026-06-20_

When a harness issues a per-sandbox tool call (`commands.run`, `files.write`,
…) against a sandbox that is assigned but **not yet serving envd**, the gateway
used to either block for the full cold-start budget (300 s) or surface an
opaque `503 unavailable` / generic timeout. Neither tells the LLM driving the
harness "this is fine, just not ready yet — retry shortly" or, conversely,
"this will never work — give up."

This change turns a not-ready sandbox into an **actionable signal**, and —
per the reporter's follow-up — does so for **any** non-serving state, not just
a gVisor `runsc restore`: a cold start still pulling its image, a Pod stuck
`Pending` waiting for a schedulable node, a restore in flight, or a backing Pod
wedged in `ImagePullBackOff` / `CrashLoopBackOff`.

## What changed

### Types (`sandbawks-types`)

- New `PodStatusSummary { phase, reason, message }` and
  `SandboxStatus::pod_status: Option<PodStatusSummary>`. This is the controller's
  distilled projection of the backing Pod's observed phase + dominant blocking
  container reason. The gateway holds **no pod-read RBAC** (#442), so it relies
  on this controller-written field rather than reading the Pod itself.
- New `RuntimeAdapter::pod_status_summary(pod_ref)` trait method (default
  `Ok(None)`), overridden by `PodAdapter` with a real `status.phase` +
  container-status read.

### Controller (`sandbawks-controller`)

- `summarize_pod_status(pod)` distills `Pod.status` into a `PodStatusSummary`:
  the coarse phase plus the most significant blocking container reason
  (init containers before app containers; `waiting.reason` before a bare
  `terminated.reason`), falling back to a `PodScheduled=False` condition
  (`Unschedulable`) so a Pod stuck for want of a node carries a concrete cause.
- The reconcile loop projects this into `status.pod_status` while a sandbox is
  `Creating` / `Resuming` and not yet serving, and clears it once an
  `envd_address` is recorded. Patched **only on a change** so an in-flight or
  stuck Pod does not generate a status write per requeue.

### Gateway (`sandbawks-api`)

- `CachedSandbox` now projects `status.phase`, `status.pod_status`, and
  `status.failure_reason` (in addition to `desired_phase` + `envd_address`), and
  exposes a `readiness()` classification: `Ready` / `Paused` / `NotReady`, where
  `NotReady` carries whether the state is **terminal** (a `Failed` /
  `Terminating` sandbox, or a Pod stuck on a `TERMINAL_POD_REASONS` reason such
  as `ImagePullBackOff` / `CrashLoopBackOff`).
- `resolve_sandbox_outcome` returns `Ready` or `NotReady`:
  - **Terminal** → fail fast, no parking.
  - **Transient** → park for the address, capped by phase: the full
    `ENVD_READY_TIMEOUT` (300 s) for a cold start (SDK transparency preserved),
    the shorter `ENVD_RESUME_NOTICE_TIMEOUT` (15 s) for a resume/restore in
    flight, so a slow or wedged resume (#560) surfaces the notice promptly
    instead of hanging.
- **Streaming** process calls (`process.Start` / `process.Connect`) emit a
  synthetic NOTICE on the **stderr** channel (`synthetic_notice_response`) and
  end the stream cleanly, so the model sees a human-readable hint in-band
  (`NOTICE: sandbox is still restoring its state (Pod is Pending); retry
  shortly`).
- **Unary** calls get a distinct, documented code via a new
  `AppError::FailedPrecondition` variant: `sandbox_not_ready` (transient,
  Connect `unavailable` — retry) or `sandbox_failed` (terminal, Connect
  `failed_precondition` — give up), both distinguishable from `sandbox_paused`
  and from a generic envd failure.
- `/health` maps a not-ready sandbox to a 502 so `Sandbox.is_running()` reports
  `False`, with no idle-timeout activity stamp.

### CRD

- `deploy/crds/sandboxes.sandbawks.dev.yaml` regenerated for the new
  `status.podStatus` field.

## Why this shape

- **Controller-projected, not gateway pod-reads.** Giving the gateway
  cluster-wide pod-read RBAC would widen the surface #442 deliberately kept
  minimal. The controller already observes the Pod every reconcile, so it is the
  natural place to distill the one-line signal the gateway needs.
- **Cold start keeps the long park.** The E2B SDKs fire calls the instant
  `Sandbox.create` returns and only retry on `RemoteProtocolError`, not on
  application envelopes — so eagerly returning "not ready" on a cold start would
  surface as an immediate `TimeoutException`. A genuinely stuck cold start is
  caught by the terminal-pod-reason fast-fail instead of by shortening the wait.
- **Resume in flight is capped short.** A resume can be slow or wedge entirely;
  parking it for the full cold-start budget is exactly the "hangs then surfaces
  an opaque error" failure this issue fixes.

## Tests

- `sandbawks-types` / `sandbawks-controller`: `summarize_pod_status` over a
  backing-off image pull, an init-container-wins case, an `Unschedulable`
  condition, a healthy running Pod, and a status-less Pod.
- `sandbawks-api` `envd_cache`: `readiness()` classification (ready / paused /
  transient resume / terminal `Failed` / terminal stuck-Pod / progressing
  `ContainerCreating`) and `wait_until_ready` terminal-vs-timeout outcomes.
- `sandbawks-api` `envd_proxy`: not-ready message formatting (transient +
  terminal), distinct error-code mapping, phase-based wait cap, and decoding the
  synthetic stderr notice stream.

## Wire-contract change

On the **per-sandbox proxy route only**, a not-ready sandbox no longer returns
the E2B code `envd_not_ready` (502). It now returns one of:

- `sandbox_not_ready` — transient, 502 / Connect `unavailable` (retry shortly).
- `sandbox_failed` — terminal, 412 / Connect `failed_precondition` (give up).

The HTTP status for the transient case is unchanged (502), so
`Sandbox.is_running()` and the SDK's transport-level retry behavior are
preserved. `envd_not_ready` is still emitted unchanged by the other routes
(`routes::exec`, `routes::sandboxes`, `routes::files`), so the two codes now
coexist by design. Any external SDK or consumer that branches on the literal
`envd_not_ready` string *for the proxy route* should update to the new codes.

The synthetic streaming notice also carries a distinguishing process exit code
so a harness keying off the exit code alone (not just the in-band stderr line)
can tell a not-ready signal apart from a real command failure: `75`
(`EX_TEMPFAIL`, retry) for transient, `69` (`EX_UNAVAILABLE`, give up) for
terminal.

## Operator note

`status.podStatus` is a new CRD field. Helm does **not** manage CRDs — apply the
regenerated CRD out-of-band, or a stale CRD will silently prune the field
(patch 200, field dropped):

```
kubectl apply -f deploy/crds
```

## Not done here / follow-ups

- End-to-end validation against the real E2B SDK in `tests/e2b-sdk-compat/`
  requires a running gateway + kind cluster (`just cluster-up`), which is not
  available in this change's environment. The acceptance-criterion SDK check is
  a manual step in the PR's test plan; the gate logic is covered by the unit
  tests above.
- Fixing the resume *wedge* itself (a restored envd that accepts connections but
  serves nothing) remains #560's domain — this change surfaces it as a retryable
  notice rather than resolving it.
