# Sandbox connection UX contract

How the **sandbawks gateway** (`crates/sandbawks-api`) signals a *not-yet-serving*
sandbox to a client, and how a client (the e2b SDKs, or a harness like **zidane**)
is expected to interpret those signals. This is the shared contract behind issues
#625 / #627 (not-ready notice), #641 / #646 (sleep-aware re-signal), and #649
(immediate notice for an unscheduled Pod).

The server half lives in **sandbawks**. The client half — surfacing the NOTICE
text, distinguishing transient vs terminal, and using a cold-start-aware probe
budget — lives in **zidane** and is tracked there. Keep both in sync with this
document.

## 1. The problem

The official e2b SDKs fire `files.write` + `commands.run` (and more) in parallel
immediately after `Sandbox.create`, *before* the controller has reported
`status.envdAddress`. A tool call therefore routinely races a sandbox that is
assigned but not yet serving envd:

- a **cold start** still pulling its image or creating its container,
- a **resume** whose `runsc restore` is in flight,
- a Pod stuck in `ImagePullBackOff` / `CrashLoopBackOff` (will not recover),
- a Pod that is **unscheduled / `Pending`** because the node pool is at its
  autoscaler max and the warm pool is exhausted (a node may free up — retriable,
  but slow).

Surfacing an opaque `503` / transport error for all of these is useless to an LLM
driving a harness. The gateway instead classifies the sandbox and emits an
**actionable, in-band** signal.

## 2. The signal: synthetic NOTICE frames

The gateway holds no pod-read RBAC (#442). It classifies a sandbox purely from the
controller-written projection on the `Sandbox` CR:

- `status.phase` — the lifecycle phase (`Creating` / `Resuming` / `Failed` / …).
- `status.podStatus` (`PodStatusSummary`) — the backing Pod's `phase` plus the
  dominant blocking `reason` (`ImagePullBackOff`, `ContainerCreating`,
  `Unschedulable`, …) and `message`. `None` once envd is serving.
- `status.failureReason` — why a `Failed` sandbox went terminal.

For a **streaming** `process.Start` / `process.Connect` call against a not-ready
sandbox, the gateway does **not** proxy to an envd that is not up. It synthesizes
the exact frame sequence envd emits for a real short-lived process:

```
start{pid: 0}  →  data{stderr: "<LEAD>: <message>\n"}  →  end{exitCode}  →  end-stream
```

so the SDK decodes it on its existing path — `commands.run` returns the stderr and
the exit code with **no special handling**. `pid: 0` is a synthetic placeholder
(no process was started). The first-frame-must-be-`start` invariant is preserved
(both e2b SDKs require it; JS throws otherwise, Python reads `pid = 0`).

### Lead word and exit code

| State | Lead | Exit | Meaning to the client |
|-------|------|------|-----------------------|
| Transient not-ready | `NOTICE` | `75` | Keep retrying; the sandbox is still starting. |
| Terminal not-ready | `ERROR` | `69` | Give up; this will not recover without intervention. |
| Ready after a `sleep` park | `NOTICE` | `100` | Resend your command (the leading `sleep` was consumed). |

The exit codes are the machine-readable contract: a client branches on the **exit
code alone** (`75` keep-waiting, `69` fail-fast, `100` resend), and may surface the
stderr text to a human / LLM verbatim.

## 3. Unary calls

A unary (non-streaming) per-sandbox RPC against a not-ready sandbox returns a
distinct error code rather than a generic `503`:

- `sandbox_not_ready` (`unavailable`) — transient, retry shortly.
- `sandbox_failed` (`failed_precondition`) — terminal, give up.
- `sandbox_paused` (`failed_precondition`) — resume the sandbox first.

## 4. `process.Start` / `process.Connect` park modes

The streaming `process.Start` handler picks one of three modes from the
classification and the command. `process.Connect` (re-attach) and the unary path
share Mode A's silent-park behavior via `resolve_sandbox_outcome`.

### Mode A — plain command, scheduled-and-booting Pod

Parks **silently** (no keepalive — there is no `start` frame yet) up to
`silent_park_max` (`SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS`, default **15 s**), then
**proxies the command** if the sandbox readied within the window, or emits the
transient/terminal NOTICE. This keeps a `commands.run` racing a sandbox that is
seconds from ready **transparent** — the caller never sees a retry bounce.

### Mode B — leading `sleep <x>` (issues #641 / #646)

A command that begins with a parseable `sleep <x>` enters a **fully synthetic**
keepalive-protected park up to `min(<x>, sleep_honor_max())`
(`SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS`, default cap **300 s**). It emits a
synthetic `start{pid:0}` immediately, keepalives during the wait, and ends with a
**non-zero re-signal**: `exit 100` ("resend your command") on readiness, or
`exit 75` ("still waiting") on timeout. The real command is **never proxied** from
this call — the agent retries once the sandbox is available.

### Mode C — plain command, unscheduled/`Pending` Pod (issue #649) — *shipped*

A **transient** not-ready sandbox whose backing Pod **cannot plausibly serve within
the silent window** emits the transient NOTICE (`exit 75`,
`NOTICE: sandbox is still starting up (Pod is Pending); retry shortly`)
**immediately** — with **no** silent park.

"Cannot plausibly serve within the silent window" means the projected Pod is
`Pending` with **no progressing reason**:

- `reason: None` — the Pod is accepted but **not yet scheduled** (cold start
  waiting for a node), or
- `reason: "Unschedulable"` (and similar scheduling-failure reasons projected from
  a `PodScheduled=False` condition) — the node pool is at capacity.

The complement — a Pod that is **scheduled and actively booting**
(`ContainerCreating` / `PodInitializing`, or any non-`Pending` phase) — stays on
**Mode A**: it p95-readies within seconds, and the transparent proxy is worth the
short park. A **mid-restore** sandbox (`Resuming` / `Paused` / `Pausing`) likewise
stays on Mode A even when its restore Pod is briefly `Pending` — this preserves the
transparent fast-resume the SDK relies on (#646 must not regress).

**Mode C is transient, not terminal.** A capacity-`Pending` Pod genuinely might
schedule once a node frees up, so the client is told to **keep retrying** (`exit
75`), distinct from the terminal fast-fail for `ImagePullBackOff` /
`CrashLoopBackOff` (`exit 69`, give up — unchanged).

**Why immediate matters.** The zidane harness gates readiness with
`commands.run('true', { timeoutMs: 5000 })` retried for a 60 s budget. On AKS a
`compound-sandbox` create hit the kata node pool at its autoscaler max
(`NotTriggerScaleUp: max node group size reached`) with the warm pool exhausted, so
its Pod stayed `Pending`. Under Mode A, every 5 s probe aborted *before* the 15 s
silent park emitted the NOTICE, so the harness never saw "Pod is Pending; retry" —
it failed with an opaque `did not become ready within 60s` and killed the sandbox.
Mode C delivers the signal in under a second, well inside a 5 s probe.

A leading `sleep` (Mode B) still wins over Mode C: it is the SDK's deliberate
keepalive park and re-signal, and is honored even for a `Pending` Pod.

## 5. Client contract (zidane half — tracked in zidane)

A conforming client:

1. **Surfaces the NOTICE / ERROR stderr text** instead of swallowing it.
2. **Branches on the exit code:** `75` → keep waiting (within a cold-start-aware
   budget), `69` → fail fast, `100` → resend the command, `0` → success.
3. **Uses a cold-start-aware probe timeout / budget** — a per-call readiness probe
   timeout must be long enough to receive a Mode A silent-park NOTICE (≥
   `silent_park_max`) *or* rely on Mode C delivering the transient NOTICE
   immediately and retry on `exit 75` until an overall cold-start budget elapses.

## 6. Scope boundary

The **server (sandbawks) half** is the classification + the three park modes above.
The **client (zidane) half** — §5 — lives in the zidane repo.

**Out of scope:** warm-pool sizing / kata node-pool `maxSize` (the capacity lever
behind the long `Pending` wait) — operational, separate.

## 7. References

- Code: `crates/sandbawks-api/src/routes/envd_proxy.rs` — `proc_start_mode`,
  `ProcStartMode`, `resolve_sandbox_outcome`, `not_ready_message`, the
  `NOTICE_EXIT_*` constants.
- Code: `crates/sandbawks-api/src/envd_cache.rs` — `CachedSandbox::readiness`,
  `NotReady::can_serve_within_silent_window`, `is_terminal`,
  `TERMINAL_POD_REASONS`, `PROGRESSING_POD_REASONS`.
- Projection: `crates/sandbawks-controller/src/pod_adapter.rs` —
  `summarize_pod_status`, `scheduling_block_reason`.
- Issues: #625 / #627, #641 / #646, #649.
