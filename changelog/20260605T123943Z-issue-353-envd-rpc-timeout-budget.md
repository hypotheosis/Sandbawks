# Tie envd unary per-RPC deadline to the HTTP budget (hygiene; does NOT fix the resume hang)

**Date:** 2026-06-05T12:39:43Z
**Branch:** `fix-issue-353-envd-rpc-timeout-budget`
**Refs:** #353, #349; real resume hang tracked by #350

## What this is (and is not)

This is a **hygiene** change. It enforces the invariant *inner per-RPC tonic
deadline < outer axum HTTP budget* so the two timeout knobs can never drift
apart again. It is **not** a fix for the pause/resume smoke failure — see
"Why the smoke still fails" below. The PR is intentionally narrowed to the
invariant + test; `Refs #353` (not `Closes`).

## Background

The pause/resume smoke (Plan 05 T14) intermittently 500s at the post-resume
`GET /files/download` step:

```
handler:files::download: tonic error → internal
  status=Status { code: Cancelled, message: "Timeout expired",
                  source: Some(tonic::transport::Error(Transport, TimeoutExpired(()))) }
```

#349 bumped the **outer** axum per-request `TimeoutLayer` 30s → 60s but left the
**inner** per-RPC tonic deadline (`ENVD_REQUEST_TIMEOUT`) hard-coded at **30s**,
so the inner deadline silently undercut the outer budget and was the binding
constraint. Independent of the resume bug, that is a latent footgun: a stuck
unary call would always die at the inner 30s and render as a 500 rather than the
outer layer's 408, and any future bump to one number could keep undercutting the
other.

## Change

All edits are in `crates/sandbawks-api`:

- `middleware.rs`: hoisted the outer HTTP budget into named constants —
  `HTTP_REQUEST_TIMEOUT_SECS: u64 = 60` and its `Duration` companion
  `HTTP_REQUEST_TIMEOUT`. `timeout_layer` now takes a `Duration` instead of a
  bare `u64` so the canonical constant flows through unambiguously.
- `bin/sandbawks-api.rs`: the timed router now layers
  `timeout_layer(HTTP_REQUEST_TIMEOUT)` instead of `timeout_layer(60)`.
- `envd_client.rs`: `ENVD_REQUEST_TIMEOUT` is now **derived** as
  `HTTP_REQUEST_TIMEOUT_SECS - ENVD_REQUEST_TIMEOUT_MARGIN_SECS` (60 − 5 = 55s)
  rather than a hard-coded 30s. Deriving it from the same constant makes the
  invariant *inner per-RPC deadline < outer HTTP budget* hold **by
  construction**. A 5s margin keeps the inner tonic deadline strictly first, so
  a genuinely stuck call surfaces a clean, deterministic gRPC status instead of
  racing the outer layer's raw 408.
- `envd_client.rs`: `channel_from` now takes an explicit
  `request_timeout: Option<Duration>` instead of a `with_request_timeout: bool`.
  The unary flavour passes `EnvdPool::UNARY_TIMEOUT` (`Some(ENVD_REQUEST_TIMEOUT)`)
  and the streaming flavour passes `EnvdPool::STREAMING_TIMEOUT` (`None`). Naming
  the streaming deadline makes "streaming stays deadline-free" a single testable
  symbol rather than an inline `false`.
- A `const _: () = assert!(...)` compile-time guard rejects any future edit that
  lets the margin reach zero or exceed the HTTP budget.

## Tests

`envd_client::tests`:

- `unary_deadline_stays_below_http_budget` — asserts
  `ENVD_REQUEST_TIMEOUT < HTTP_REQUEST_TIMEOUT` and that it kept real headroom
  over the prior 30s floor.
- `streaming_channel_is_deadline_free` — asserts `EnvdPool::STREAMING_TIMEOUT`
  is `None`, guarding the out-of-scope streaming surface.
- `unary_channel_carries_the_bounded_deadline` — asserts the unary flavour does
  carry `Some(ENVD_REQUEST_TIMEOUT)`.

## Why the smoke still fails (the real bug — #350)

With this change deployed, the smoke run on 2026-06-05 failed **again**, now at
`latency=55006 ms` — i.e. the download hung the *full* new 55s deadline and was
killed by the inner tonic timeout. That is the decisive datum:

- It is a **hard hang, not a slow-start.** A 21-byte file cannot take 55s; it
  either returns in milliseconds or blocks forever. No timeout value fixes a call
  that never completes. (The issue predicted this: "Lengthening the timeout alone
  only enlarges the window.")
- The RPC failed with tonic `TimeoutExpired`, **not** `envd_unavailable` (the
  5s connect-timeout path). So the TCP/h2 connect to the resumed `:49983`
  succeeded and envd **accepted the stream** — then its handler returned nothing
  for 55s.
- Per `crates/envd/src/bin/envd.rs`, the resumed envd **boots fresh** and
  re-seeds from the state snapshot (it is not a memory-restore of the envd
  process): it binds `:49984` (`/__state`), then spawns the gRPC server on
  `:49983`. Since the api got a *connected* channel, that listener is up and
  accepting — so this is very likely **not** the "transport not serving yet"
  race; a fresh tokio server that has already accepted the stream is serving.
- That points at the **handler blocking on the restored filesystem**:
  `Filesystem.download` reads `/tmp/sentinel.txt` (written pre-pause) out of the
  gVisor-restored rootfs, and that read appears to hang — a gofer / filesystem-
  restore problem, not a gRPC-readiness or timeout problem.
- The resumed Pod has **no readiness probe**, so "container Ready" ≠ "envd
  serving" — exactly the gap #350 names.

**Not yet isolated:** the api↔envd boundary is localized (envd received the call
and did not answer), but envd's *internal* boundaries are not — "gRPC dispatch
never ran" vs. "filesystem syscall hung". The CI kind cluster is ephemeral and
gone, so this could not be probed live. The decisive next diagnostic (owned by
**#350**): immediately post-resume, hit `:49984/__state` and fire a
**non-filesystem** unary RPC (e.g. `Process.List`). If `Process.List` returns
fast but `Filesystem.download` hangs → restored rootfs/gofer; if all gRPC hangs
→ serve-readiness after all. The smoke currently cannot tell these apart.
