# Design: not-ready park + re-signal (revises PR #644 / issue #641)

Date: 2026-06-23
Status: proposed (awaiting review)
Supersedes the keepalive-extended-park and the strip/rewrite approaches explored
in the PR #644 review thread.

## Problem

A sandbox can take a long time to go from *requested* to *realized* — a
`Creating` Pod stuck `Pending` while the cluster autoscaler brings up a node can
be 3+ minutes, and a large restore (`Resuming`) is also slow. During that window
a streaming `process.Start` (`commands.run(...)`) has nowhere to go: there is no
envd serving yet.

For a **headless agentic harness** the failure mode that matters is *ambiguous
silence*. Today the gateway parks quietly waiting for readiness (up to
`ENVD_READY_TIMEOUT` = 300s for cold start; 15s for resume), emitting nothing.
Streaming process routes are exempt from the global 60s `HTTP_REQUEST_TIMEOUT`
(middleware.rs:161), so nothing bounds that silent park. A quiet park long enough
to outlive a ~60s ingress/LB idle timeout gets the connection reset, and the
agent sees an opaque transport error and concludes the system is broken — when in
fact everything on the wire was fine and the sandbox was simply still coming up.

The goal: the harness should **always receive a clean, in-band signal** — "the
wire is fine, the sandbox just isn't up yet; here's what to do" — and **never** a
quiet timeout that reads as a malfunction. Issue #625 introduced that signal (a
synthetic stderr `NOTICE` + non-zero exit). PR #644 (issue #641) tried to make
the wait productive by honoring a leading `sleep <x>`. This spec reworks how that
honoring is done.

## The dominant constraint: the SDK handshake (do not re-litigate)

Every `commands.run` in the e2b SDK (verified against `e2b` js 2.30.5 / py
2.29.5, the versions CI pins) begins by reading the **first** Connect envelope
and requiring it to be a `start` event:

- **JS** (`handleProcessStartEvent`): throws `"Expected start event"` if the first
  frame is anything else.
- **Python** (`_start`): reads `start_event.event.start.pid`; a non-`start` first
  frame silently yields `pid = 0`.

Consequences that bound every option here:

- We **cannot** emit a keepalive (or any non-`start` frame) before a real `start`.
  So we cannot keep a connection "warm with keepalives" while waiting for a
  sandbox that hasn't produced a process yet — there is no `start` to ride on.
- We **cannot** synthesize a `start` and later splice the real command's stream
  to keep one call alive across readiness, because the synthetic `start` carries
  `pid = 0`; the real pid arrives too late (the SDK ignores the second `start`),
  silently breaking `kill` / `send_stdin` / background handles. The proxy cannot
  tell foreground from background (it is purely client-side), so the breakage
  would be unconditional.

Therefore: **a single call can never both wait for an unrealized sandbox and then
run the real command.** Waiting and running must be *different calls*. That fact
drives the design.

## Design: two modes, both ending in a clean frame

`parse_leading_sleep` (from PR #644, **kept as-is**) is used only to *detect* a
leading `sleep` and read its duration — never to rewrite or strip the command.

### Mode A — plain not-ready command (no leading sleep)

Unchanged in shape from #625, with one fix: the silent park is **bounded**.

1. Classify the sandbox (cache lookup, seed-if-absent preserves the 404).
2. `Ready` → proxy to envd normally. `Paused` → 403. Terminal not-ready →
   immediate notice.
3. Transient not-ready → park **silently** up to `ENVD_SILENT_PARK_MAX` (default
   15s, applied to **all** phases including cold start — this is the fix; cold
   start no longer parks 300s). If it becomes ready inside that short window,
   proxy normally (catches the common fast resume with no retry). Otherwise emit
   the existing transient `NOTICE` (stderr + exit `75`/`EX_TEMPFAIL`).

The bound must stay under the idle floor; the silent park has no keepalive (it
can't — no `start` yet), so its only safety is being short.

### Mode B — intercepted leading sleep (the productive wait)

When a not-ready, transient sandbox receives a `process.Start` whose command
begins with `sleep <x>`, treat `<x>` as a **keepalive-protected park budget** and
end with a re-signal. **The command is never proxied from this call.** The entire
response is synthetic:

1. Open the streaming response immediately and send a synthetic `start(pid = 0)`
   — this is the existing #625 frame shape, and because the call is *fully
   synthetic* (we never attach a real process to it), `pid = 0` is harmless.
2. Park for readiness up to `min(<x>, ENVD_SLEEP_HONOR_MAX)` (default cap 300s),
   emitting a keepalive frame every `PROC_KEEPALIVE_INTERVAL` (15s). The
   connection stays demonstrably alive the whole time — positive visibility, on
   any ingress (real frames, not transport PING).
3. Terminate with a **non-zero** synthetic notice:
   - **Became ready** (at elapsed `T < <x>`) → stderr *"pod is now available; the
     leading `sleep` was consumed while waiting — resend your command"* + exit
     `NOTICE_EXIT_READY_RESEND`. Returned the instant we're ready ("as soon as
     we're able"), not at the end of `<x>`.
   - **Window elapsed / still not ready** → the existing transient `NOTICE` +
     exit `75`. Returned a hair *under* the parked duration so our signal beats
     the SDK's own call timeout rather than racing it.
4. The agent resends its real command on the next turn; it runs verbatim against a
   now-ready sandbox with a real `start` and real pid — normal semantics.

Non-zero exit is load-bearing: for a pipelined `sleep 5; real_work`, an exit-0
would make the agent believe `real_work` *ran*. It did not. Non-zero + the stderr
hint tell the agent the real work has not executed and to resend it.

Bare vs pipelined sleeps are handled **identically** (we never run either), so no
shell surgery is needed. We may cheaply tailor the message by whether any
trailing content follows the sleep (bare → "proceed with your command";
pipelined → "resend without the leading `sleep`") — a presence check, not a
parse/strip.

`proc_connect` (the `.wait()` re-attach) carries no fresh command to inspect, so
it gets Mode A's bounded silent park only — which still fixes its 300s cold-start
silent park.

### Why this shape

- **No command rewriting.** We detect a leading sleep and read its duration;
  nothing else. The fragile shell-stripping / duration-rewriting problem is gone.
- **No pid corruption, no splice.** The intercepted-sleep call is synthetic
  end-to-end; the real command only runs on a separate retry against a ready
  sandbox. `pid = 0` never matters.
- **No timing-intent ambiguity.** We never guess whether the agent's sleep meant
  "wait for the sandbox" vs "wall-clock" vs "in-sandbox warm-up" — the real
  command runs as written on retry.
- **Robust to a naive agent.** If the agent resends the whole `sleep 5; real_work`
  pipeline on the now-ready sandbox, it runs correctly (a real 5s sleep, then
  work) — wasteful by seconds, never wrong.

Cost: **one extra round trip** (the sleep turn returns "resend," then the real
command runs). Cheap and legible given the clean-signal retry loop is already the
model.

## Components (`crates/sandbawks-api/src/routes/envd_proxy.rs`)

- `parse_leading_sleep` / `parse_duration_token` / `sum_sleep_operands` — kept;
  detection + duration only.
- Config readers (`OnceLock<Duration>`, matching the existing `std::env::var` +
  `OnceLock` idiom; invalid/empty → default, logged via `tracing::warn!`):
  - `ENVD_SILENT_PARK_MAX` (default 15s) — Mode A silent-park ceiling, all phases.
  - `ENVD_SLEEP_HONOR_MAX` (default 300s) — Mode B keepalive-park ceiling.
- `not_ready_cap` → returns `ENVD_SILENT_PARK_MAX` for all transient phases
  (was the 15s/300s split). Pure: take the ceiling as a parameter; the resolve
  fn reads the knob and passes it (keeps it unit-testable without env).
- **Retire** `honored_park_cap` and `ENVD_SLEEP_AWARE_MAX_WAIT` (Mode B no longer
  extends a proxied park; it runs a synthetic stream instead). Audit
  `ENVD_READY_TIMEOUT`; remove if unused after cold start stops using it.
- New `sleep_park_response(state, sandbox_id, sleep, cap) -> Response`: opens the
  channel, spawns a producer that sends `start(pid=0)`, parks for readiness with
  keepalives up to `min(sleep, cap)`, then emits the ready-resend or
  timeout notice. Reuses the keepalive frame + notice-frame builders. No envd
  connection, no generic stream type — purely synthetic, so it is small and
  directly unit-testable (abstract the readiness future like `forward_event_stream`
  abstracts its stream).
- Factor the notice frame sequence out of `synthetic_notice_response` into a
  shared `synthetic_notice_frames(...)` used by both the immediate notice and the
  Mode B terminal frames (DRY).
- Exit codes: keep `NOTICE_EXIT_TRANSIENT` (75) and `NOTICE_EXIT_TERMINAL` (69);
  add a **distinct** `NOTICE_EXIT_READY_RESEND` (documented `EX_TEMPFAIL`-family
  sentinel, non-zero, distinct from 69/75) so a harness can distinguish "ready, go
  now" from "still waiting" by exit code alone. The stderr message remains
  authoritative; the code exists so a harness can branch without parsing text.
- `proc_start`: decode body (existing reorder), `parse_leading_sleep`. If a sleep
  is present **and** the sandbox is transiently not-ready → `sleep_park_response`.
  Otherwise Mode A. A `Ready` sandbox always proxies the command verbatim (a sleep
  is only intercepted while not-ready).

## Configuration & deploy

- Code defaults are authoritative; the feature works with no env set.
- Both knobs **are wired in this PR** into the api Helm values + deployment env
  (`deploy/helm/sandbawks/`), and `Chart.yaml` `version:` is bumped (project rule).

## Contracts & caveats

- **Client timeout is the harness's job.** An agent that sends `sleep 300` is
  assumed to set its `commands.run(timeout=...)` to match (the e2b default is
  60s; `0` = unlimited). No server behavior overrides the client's own deadline;
  we only ensure our signal arrives before it.
- **Idle floor.** `ENVD_SILENT_PARK_MAX` must stay under the smallest expected
  ingress idle timeout (Mode A has no keepalive). Mode B is keepalive-protected
  and may run long.
- **One extra round trip** per honored sleep, by design.

## Testing

- `parse_leading_sleep` tests — unchanged.
- Config parsing tests for both knobs (valid / invalid → default / empty).
- `not_ready_cap` test — returns the ceiling for all transient phases.
- `sleep_park_response` producer tests (with an abstracted readiness future):
  (a) emits `start(pid=0)` first; (b) emits a keepalive per interval during a
  silent park; (c) ready → ready-resend notice with `NOTICE_EXIT_READY_RESEND`;
  (d) timeout → transient notice with exit 75.
- Mode A bounded-park-to-notice stays out of unit scope (matches the existing
  `tests/envd_proxy.rs` boundary); covered by the manual live-cluster plan.

## Documentation

- Module/doc comments: record the SDK-handshake constraint and *why* keepalive-
  before-a-real-`start` and splice-across-readiness are impossible, so no one
  re-attempts them. Document the two modes, the knobs, and the
  ready-resend / still-waiting notice contract (messages + exit codes).
- Replace PR #644's dated `changelog/` entry with one describing this approach and
  the SDK-handshake finding. Update the PR #644 description.

## Retired alternatives (so they are not rediscovered)

- **Keepalive frames during the wait, then splice the real command** — breaks the
  SDK handshake (JS throws / Python pid 0); pid corruption is unconditional.
- **Synthetic `start` + pid virtualization** (gateway owns a synthetic→real pid
  ledger) — turns the stateless gateway into a stateful process broker with a
  shared, lifecycle-managed registry mirroring envd; disproportionate to the need.
- **Transport HTTP/2 PING** — SDK-correct but only helps if the api↔ingress hop is
  HTTP/2 and that ingress resets idle on PING; not assumable. May be added later
  as a non-guaranteed bonus.
- **Strip the leading sleep and proxy the remainder** — requires shell rewriting
  (separator semantics: `&&`/`;` strip-safe, `||`/`|`/`&` not; duration suffixes;
  quoting); a mis-strip corrupts the agent's real command. Superseded by
  notify-and-retry, which runs the command verbatim on retry.
- **Rewrite the leading sleep to the remaining duration** — delays the real work
  to the original deadline (the opposite of "as soon as we're able"), and the
  remainder math is wrong for the in-sandbox-warm-up intent (the in-sandbox clock
  starts only at readiness). Superseded.
- **Long bounded silent park (no signal) for cold start** — the original 300s
  quiet park; the exact ambiguous-timeout failure this spec removes.

## Resolved decisions

1. **Defaults** — `ENVD_SILENT_PARK_MAX` = 15s, `ENVD_SLEEP_HONOR_MAX` = 300s. ✓
2. **Exit code** — `NOTICE_EXIT_READY_RESEND` is a **distinct** non-zero code
   (not a reuse of 75). ✓
3. **Helm** — both env vars wired in this PR; `Chart.yaml` version bumped. ✓
