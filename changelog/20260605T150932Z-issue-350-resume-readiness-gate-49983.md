# Issue #350 — Resume readiness: gate on the envd gRPC port (:49983) + self-classifying smoke probe

**Date:** 2026-06-05T15:09:32Z
**Branch:** `fix-issue-350-resume-readiness-gate-49983`
**Issue:** #350 (labels: `area/controller`, `area/envd`, `kind/fix`, `prio/p2`, `runtime/gvisor`)

## Background

After `POST /sandboxes/{id}/connect` resumes a sandbox onto a *fresh* Pod, there is
a window where the Pod/Sandbox is reported `Running` + `Ready` but the surface the
filesystem routes actually use (the `:49983` gRPC server) isn't serving yet. PR #349
(30s → 60s api request timeout) and PR #354 (per-RPC tonic deadline) widened the
margin but did not close the race; the post-resume `GET /files/download` still hung
the full budget.

The issue body proposes gating "envd is ready" on the gRPC port `:49983` rather than
(only) the `:49984` `/__state` HTTP probe. The issue author's 2026-06-05 comment adds
new evidence: the resumed `:49983` listener *accepts the stream* and then the handler
returns nothing for the full budget — leading hypothesis is a **filesystem-restore /
gofer** hang, not gRPC serve-readiness. Root cause **not yet isolated**. The author
recommends a diagnostic-first probe "worth adding regardless".

Per the user's decision, this change implements **both** levers.

## Changes

### 1. Controller — `:49983` readinessProbe (`crates/sandbawks-controller/src/pod_adapter.rs`)

`pod_spec_for` now attaches a **TCP-socket `readinessProbe` on `:49983`** to the envd
container, for cold-start *and* restore Pods.

- Previously the sandbox Pod carried **no** readinessProbe, so "container Ready" meant
  only "envd process started". The controller's `pod_ready` gate keys off Pod condition
  `Ready=True` before it stamps `status.envdAddress` and lets `derive_trigger` promote
  the Sandbox to `Running` — so it could promote before `:49983` was accepting.
- The TCP probe makes "container Ready" mean "the gRPC port is accepting connections",
  which transitively tightens the existing `pod_ready` gate without adding a gRPC client
  to the controller.
- **TCP, not a gRPC/httpGet probe:** envd serves only `Filesystem` + `Process` and
  registers no `grpc.health.v1.Health` service, so kubelet's native `grpc` probe would
  fail permanently. TCP is the faithful "is `:49983` listening" signal.
- Cadence: `initialDelaySeconds=0`, `periodSeconds=1`, `timeoutSeconds=1`,
  `successThreshold=1`, `failureThreshold=3` — fast Ready flip on a slow GiB restore,
  conventional flap-tolerance once Ready.
- A readinessProbe is kubelet-side metadata, **not** part of the `runsc` sandbox, so the
  cold-start and restore Pods stay topologically identical to the checkpoint.

**Known limitation (documented in code):** a TCP probe confirms the listener *accepts*
a connection, not that a unary RPC *returns*. So it does **not** by itself close the
case where the listener is up but a handler hangs on the gVisor-restored rootfs (the
author's leading hypothesis). That regime is what the smoke probe below classifies.

New unit test: `pod_spec_for_gates_readiness_on_envd_grpc_port` (cold-start + restore)
asserts the probe targets `:49983` (not `:49984`) and is a plain TCP probe.

### 2. Smoke — self-classifying post-resume probe (`deploy/kind/fixtures/pause-resume-smoke.sh`)

New `resume-readiness-classify` phase, run after the resume reaches `Running`/Ready and
before the sentinel download:

- Fires a **non-filesystem unary RPC** (`Process.List`) through the api's synthesized
  `GET /health` (resolves the sandbox by `E2b-Sandbox-Id`, dials the **same** `:49983`
  gRPC server the download uses). Records HTTP status + latency.
- Probes envd's `:49984` `/__state` HTTP listener in-Pod via busybox `wget`
  (best-effort, non-fatal) as a baseline.

The download step now captures the HTTP status/latency (instead of `-fsS` aborting) and,
on failure, emits an explicit classification:

- `Process.List` fast 200 **but** download fails → **FILESYSTEM-RESTORE / gofer** is the
  culprit; the `:49983` readiness gate will not fix it.
- `Process.List` also non-200 → genuine **gRPC SERVE-READINESS**; the `:49983` gate is the
  right lever.

No `--max-time` on the download itself, so the large-overlay leg
(`PAUSE_RESUME_SMOKE_LARGE_MIB`) can still stream GiB; the api's own `TimeoutLayer` (408)
bounds a real hang.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — all pass, including the new `pod_spec_for_gates_readiness_on_envd_grpc_port`.
- `bash -n` + `shellcheck -S warning` on the smoke script — clean.

**Not run here:** the end-to-end `pause-resume-smoke.sh` against a
`SANDBAWKS_KIND_GVISOR=1` cluster — no kind/gVisor cluster is reachable from this
environment (the CI kind cluster is ephemeral). The acceptance criteria (deterministic
large-payload resume) can only be confirmed in CI; the diagnostic probe is designed so
that if it still fails, the failure self-classifies which lever to pull next.
