# Process-exec stream keepalive (fix idle HTTP/2 `RST_STREAM`)

**Date:** 2026-06-04T20:09:49Z
**Area:** `crates/sandbawks-api/src/routes/envd_proxy.rs`

## Symptom

Running a long, output-silent command through the E2B SDK
(`sbx.commands.run(...)`) against `https://sandbawks.example.com` fails partway
through with:

```
httpcore.RemoteProtocolError: <StreamReset stream_id:N, error_code:2, remote_reset:True>
```

`error_code:2` is HTTP/2 `INTERNAL_ERROR`; `remote_reset:True` means the
**server side** sent the `RST_STREAM`. A fast, chatty command (`git clone`,
which prints continuous progress) succeeds, while a silent one
(`claude --print` thinking, a build that prints nothing, even a bare
`sleep 90`) is reset. A bare `sleep 90` reproduces it deterministically.

## Root cause

The Connect-RPC process-streaming proxy (`forward_event_stream`) backing
`process.Process/Start` and `.../Connect` was a pure pass-through: it wrote a
frame to the client **only** when envd emitted a `ProcessEvent`. During a
silent command no frames flow, so the HTTP/2 stream goes idle and the
ingress / cloud LB in front of the gateway resets it at its ~60s idle timeout.

This timeout is already documented in the codebase: the SSE event stream
(`routes/events.rs`) and file-watch stream (`routes/files.rs`) deliberately
emit a keepalive every 15s "tuned below the typical 60s idle-timeout enforced
by k8s Ingress / cloud LBs." The process-exec streaming path had no such
keepalive, and its subrouter is intentionally merged outside the 30s unary
timeout layer, so nothing kept a silent stream warm.

Ruled out: OOM / pod death (no `OOMKilled` events; sibling warm pods healthy
at the same `1Gi` limit; pods are `restartPolicy: Never`, so an OOM would
surface a `Failed` pod, not a clean stream reset) and sandbox `timeout_seconds`
auto-pause (would tear down the whole connection, not a single stream).

## Fix

`forward_event_stream` now drives a `tokio::time::interval`
(`PROC_KEEPALIVE_INTERVAL = 15s`) alongside the source stream via
`tokio::select!`. After each interval of genuine silence it injects a
synthetic `keepalive` `ProcessEvent`. The timer is reset whenever real output
is forwarded, so keepalives only fill true gaps.

The keepalive frame is byte-for-byte identical to an envd-emitted keepalive
(`{"event":{"keepalive":{}}}`) — the wire shape was already wired end-to-end
(`ProcessEventDto.keepalive`, `process_event::Event::Keepalive`). The E2B
SDK's `_handle_events` decodes it, finds neither `data` nor `end` set, and
silently discards it, so no client change is required.

To make the timing testable, `forward_event_stream` is now generic over
`S: futures::Stream<Item = Result<T, tonic::Status>> + Unpin` instead of a
concrete `tonic::Streaming<T>`; `tonic::Streaming` satisfies the bound, so the
two call sites are unchanged (turbofish dropped; both type params inferred).

## Tests

- `keepalive_envelope_is_ignorable_event` — asserts the synthesized frame is a
  zero-flag, correctly length-prefixed envelope whose JSON body is exactly
  `{"event":{"keepalive":{}}}`.
- `silent_stream_emits_keepalive_within_interval` — under `start_paused`
  virtual time, a source stream that never yields produces a keepalive within
  two idle intervals, proving the timer fires without any real output.

`cargo fmt --check`, `cargo clippy --features test-util --tests` (no
warnings), and `cargo test -p sandbawks-api --features test-util` all pass.

## Follow-up

Default sandbox memory limit is `1Gi`; `claude --print` agentic loops are
memory-hungry and may still warrant a larger limit independent of this fix,
but that is not the cause of the stream reset observed here.
