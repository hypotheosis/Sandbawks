# Issue #560 — resumed envd wedges when checkpointed with a live external gRPC connection

**Date:** 2026-06-16
**Branch:** `fix-issue-560-checkpoint-conn-drain`
**Scope:** Two-part fix in `sandbawks-types` + `sandbawks-controller` (recovery
gate) and `envd` (connection drain). PR #562.

## Symptom

A sandbox that resumed "successfully" (`phase: Running`, `ResumeSucceeded`,
filesystem + process restored) was **unreachable via the E2B protocol**. The
restored `envd` accepted TCP connections on both `:49983` (gRPC) and `:49984`
(HTTP `/__state`) but answered **nothing** on either — listeners bound (LISTEN),
fds present, process idle. The 300 s idle-timeout autopause then could not
checkpoint the wedged envd (`checkpoint failed: envd /__state timed out after
10s`), so the sandbox was driven to terminal `phase: Failed` and the Pod
deleted. Resume was effectively dead-on-arrival.

## Root cause

Confirmed by a self-contained A/B test (external gRPC client from a sibling pod
holding an in-flight `process.Start` stream across pause/resume):

- **Connected at checkpoint → wedges.** **Disconnected before checkpoint →
  resumes clean.** The tracked child process was held constant, so the trigger
  is the **live external-peer gRPC/HTTP-2 connection itself**.

When envd is `runsc checkpoint`ed while a client connection is ESTABLISHED, the
checkpoint captures that socket. On `runsc restore` the peer is gone (it was
never part of the sandbox), and the restored half-open socket / hyper-tonic
connection state wedges envd's serving runtime — it keeps accepting new
connections but services none.

Several earlier hypotheses were investigated and **refuted** en route: a generic
tokio-reactor-loss; a runsc checkpoint↔restore version drift; OTLP/telemetry;
the tokio signal/SIGCHLD (child-process) machinery; cold-vs-restore;
multi-generation restore. A standalone matched-version `runsc` C/R harness
reproduced none of them — only the *external* client connection did.

## Fix

### Part 1 — `envd`: drain client connections before checkpoint (the cure)

The node already drives `POST /__quiesce` just before `runsc checkpoint`.
Extended that handler to also drain envd's gRPC client connections, so the
checkpoint captures a fresh, connection-free `:49983` listener.

tonic spawns one **detached** task per connection, so there is no way to close
live connections while keeping the listener — and a graceful shutdown waits for
in-flight RPCs (a held stream would block it). So gRPC now serves on a
**dedicated Tokio runtime** (`run_grpc_loop`, on its own OS thread). `/__quiesce`
bumps a `watch` epoch; the supervisor tears that runtime down
(`shutdown_timeout` aborts the serve task *and* every connection task, closing
all sockets), rebinds a fresh listener, and acks the epoch **only after the new
listener is accepting** (so there is never a window where `:49983` is unbound at
checkpoint). `/__quiesce` rides the independent `:49984` listener, so it can
drive and await the `:49983` drain without a chicken-and-egg. The gateway
reconnects after resume as normal.

### Part 2 — `sandbawks-controller`: serve-level resume readiness gate (recovery)

Kubelet `Ready` on `:49983` only means the listener *accepts* — not that envd
*answers*. Added `RuntimeAdapter::envd_serving` (a real `GET /__state` 200 probe;
dependency-free `tokio` TCP in `PodAdapter`) and gated the `Resuming → Running`
promotion on it. A restore Pod that is kubelet-Ready but not serving has its
promotion-gating `envd_address` withheld, so the existing bounded resume
readiness deadline recreates the restore Pod (`RestorePodLost`) and recovers via
the #407/#518 `StateLost` path instead of declaring a dead sandbox `Running`.

## Validation

- `just fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `just test` — all green.
- New `sandbawks-controller` test `lifecycle::resume_withholds_running_until_envd_serves`:
  kubelet-Ready but non-serving envd stays `Resuming`, promotes only once
  `/__state` answers `200`.
- New `envd` test `quiesce_drains_grpc_connections_and_rebinds`: drives
  `run_grpc_loop` and asserts at the TCP layer that a drain force-closes a held
  connection and a fresh connection still succeeds.
- The `envd` `binary_smoke` integration test (real envd launch + gRPC + `/__state`
  + SIGTERM drain) passes against the new dedicated-runtime serving path.
- End-to-end gVisor checkpoint/restore efficacy of Part 1 is validated by the
  A/B harness on a deployed image (`runsc` C/R cannot run under `cargo test`);
  the "disconnect-before-pause → resumes clean" A/B result already proves the
  principle.
