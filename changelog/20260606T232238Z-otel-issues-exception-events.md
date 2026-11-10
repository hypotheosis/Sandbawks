# otel: surface server faults in the APM "Issues" view via exception events

**Branch:** `fix-api-traces-http-semconv` (PR #370 — global OTel span-shape cleanup).

## Problem

traceway's **Issues** view was empty. Probing the live instance showed Issues
are populated **only** by an OTel `exception` *span event* on a span in a
rendered trace (Endpoint or Task, including child spans). Span `status=ERROR`
and HTTP 5xx alone do **not** create an Issue, and error *logs* do not either.

Our error paths emit `tracing::error!(error = ?e, "some message")`. Because
tracing-opentelemetry only rewrites an error event into an `exception` event
when the event has **no message** and an `error` field (defaults all on in
0.33), our messaged error events only set span status — never an exception
event — so nothing reached Issues.

## Change

Emit exception events at the two fault chokepoints, attached to the now-rendered
Endpoint/Task spans (this PR's other commits):

- **`sandbawks-api/src/error.rs`** — `AppError::Internal` (the 5xx arm) now emits
  `tracing::error!(error = ?err)` with **no message**, which tracing-opentelemetry
  rewrites into an `exception` event on the current `http.request` (Endpoint)
  span. One chokepoint covers every handler's 5xx. 4xx client errors are left
  alone — they are not faults and must not create Issues. (Replaces the prior
  messaged log line; the error's Debug becomes `exception.message`.)
- **controller/node reconcile spans** — added `err` to the three top-level
  reconcile `#[instrument]`s (`reconcile`, `reconcile_pool`, node `reconcile`).
  On `Err`, the macro emits the no-message error event → exception on the Task
  span → Issue. Sandbox-level failures folded into the `Failed` phase return
  `Ok`, so only genuine infrastructure errors trip this.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- Mechanism proven against the live traceway: an `exception` span event on a
  rendered Endpoint/Task/child created an Issue; `status=ERROR`/5xx without an
  exception event did not; error logs did not.
- Post-deploy: confirm a 5xx api response and a failing reconcile each create an
  Issue (the exception event must land on the rendered Endpoint/Task span — the
  api path relies on the request span being current during `IntoResponse`).

## Notes

- Companion to the api Endpoint + controller/node Task span shaping in this PR;
  Issues require those rendered traces to exist, which they now do.
- The exception carries `exception.message` (from the error's Debug); enriching
  with `exception.type`/`exception.stacktrace` is a possible follow-up.
