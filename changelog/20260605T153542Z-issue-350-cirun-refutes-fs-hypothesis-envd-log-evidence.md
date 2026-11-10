# Issue #350 — CI run refutes the filesystem hypothesis; pivot to root-cause (restored-envd log evidence)

**Date:** 2026-06-05T15:35:42Z
**Branch:** `fix-issue-350-resume-readiness-gate-49983` (PR #355)
**Prior entry:** `20260605T150932Z-issue-350-resume-readiness-gate-49983.md`

## What the first CI run proved

The self-classifying probe added to `pause-resume-smoke.sh` ran in CI and produced a
decisive result that **refutes the issue's leading hypothesis** (a filesystem-restore /
gofer hang):

| Signal | Observation |
|---|---|
| `Process.List` (non-filesystem unary RPC, via api `GET /health` → same `:49983`) | HTTP 500 after **55,025 ms**, `tonic Cancelled / TimeoutExpired` |
| `/files/download` (filesystem RPC) | HTTP 500 after **55,029 ms**, identical `TimeoutExpired` |
| `:49984 /__state` (HTTP, probed in-Pod) | **serving** (163 bytes) |
| TCP `:49983` readinessProbe (this PR) | **passed** — Pod `Ready: True`, `tcp-socket :49983 #success=1` |

Conclusions:

1. **Not the filesystem / gofer.** A trivial non-fs `Process.List` hangs identically to
   the download. The restored-rootfs-read hypothesis is wrong.
2. **The envd process is alive** — its axum HTTP server on `:49984` answers in-Pod.
3. **The tonic gRPC server on `:49983` accepts the TCP connection but never serves any
   RPC** (api error is `TimeoutExpired` *after* connect, not `envd_unavailable`). A
   post-restore **gRPC-serve wedge**, not a connect/transport-readiness race.
4. **The TCP readinessProbe is too shallow to catch it** — it passes (TCP accept → Ready)
   while every gRPC RPC hangs. So that gate does not fix this failure mode (it remains a
   correct, cheap defense for the *cold-start* "process up but port not listening" case).

Structural smoking gun: `:49984` (works) is a bare axum router; `:49983` (wedged) is the
only path carrying `GrpcTraceLayer` + `GrpcMetricsLayer`, HTTP/2, and the service
registries (`crates/envd/src/bin/envd.rs:503-528`). The snapshot bundle's `memory.zst`
confirms this is a `runsc` memory checkpoint/restore, so the whole process (tokio runtime,
tonic task, OTel SDK background tasks) is frozen/thawed.

## This iteration: gather the missing boundary evidence (no fix yet)

Per systematic debugging (instrument boundaries, run once, find WHERE it breaks before
fixing), the one un-instrumented boundary is the **restored envd container's own logs** —
the smoke captured api/controller/node logs on failure but never the sandbox Pod's.

`deploy/kind/fixtures/pause-resume-smoke.sh`:

- **Classify phase:** after the two probes, dump the restored envd container's logs inline
  (`kubectl logs <pod> -c envd --timestamps --tail=120`), so the next run's output is
  self-contained.
- **Failure diagnostics (`cleanup`):** dump the sandbox Pod's envd logs (current +
  `--previous`) alongside the existing api/controller/node dumps.

These logs discriminate the two open questions in one shot:

- **Fresh boot vs memory restore:** a fresh boot re-emits the
  `starting envd ... listen=[::]:49983 http_listen=[::]:49984` banner (and
  `loaded pause/resume seed`); a memory restore thaws a quiescent server and emits nothing
  new.
- **Serve-task state:** did the gRPC serve task error / panic / drain, or is it silent
  while `:49984` keeps serving?

No fix is proposed in this iteration — the next CI run's envd logs decide the single
hypothesis to test (leading candidate: the OTel meter / `GrpcMetricsLayer`, the only
component on the wedged `:49983` path that `:49984` does not share).

## Validation

- `bash -n` + `shellcheck -S warning` on `pause-resume-smoke.sh` — clean.
- No Rust changes in this iteration (the readinessProbe + its unit test from the prior
  entry are unchanged; clippy/test already green there).
