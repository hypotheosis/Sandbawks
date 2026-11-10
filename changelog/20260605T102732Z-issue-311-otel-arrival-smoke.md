# OTel end-to-end collector-arrival smoke (issue #311)

**Date:** 2026-06-05T10:27:32Z
**Branch:** `test-issue-311-otel-arrival-smoke`
**Issue:** #311 (OTel observability, Phase 2 follow-up to PR #310 / epic #294)

## Problem

PR #310 (OTel Phase 2) wired `init_telemetry` into all four binaries and
proved, locally, both the disabled path (stderr `fmt`, no exporter) and the
active path (full provider stack, clean flush). What it never exercised is the
rollout's actual acceptance criterion: spans / logs **arriving** at a live OTLP
collector stamped with the right `service.name`. No collector existed in the
PR's build environment, so end-to-end arrival had never been demonstrated.

The collector fixture (`deploy/kind/fixtures/otel-collector.yaml`, issue #301)
and the chart's `observability` block already existed, but the
"telemetry-demonstrably-reaches-a-collector" check was a manual,
read-the-`kubectl-logs`-by-eye procedure documented in `docs/observability.md`.
This change automates that check and gates CI on it.

## Change

A self-contained arrival assertion plus its CI wiring:

- **`deploy/kind/fixtures/otel-arrival-smoke.sh`** — applies the collector
  fixture (idempotent), then polls the `debug` exporter's stdout until at least
  one received signal (span, metric, or log record) carries the expected
  `service.name` for each of `sandbawks-api`, `sandbawks-controller`,
  `sandbawks-node`, and `envd`. On timeout it fails listing exactly which
  names did and did not arrive. The needle is the detailed-debug resource
  attribute `service.name: Str(<name>)`, fixed-string matched with the trailing
  `)` so `sandbawks-node` never matches `sandbawks-node-foo`.
  - Tunable via env: `OTEL_SMOKE_EXPECTED_SERVICES`, `OTEL_SMOKE_NAMESPACE`,
    `OTEL_SMOKE_DEPLOY`, `OTEL_SMOKE_TIMEOUT_SECS`, `OTEL_SMOKE_POLL_SECS`,
    `OTEL_SMOKE_APPLY_COLLECTOR`.
  - `OTEL_SMOKE_LOG_FILE` is an offline/unit-test seam: the parser runs once
    against a captured log file, no cluster required.
- **`just otel-arrival-smoke`** — thin wrapper recipe.
- **`.forgejo/workflows/kind-smoke.yaml`** — new `KIND_SMOKE_INCLUDE_OTEL`
  gate. When on (and the gVisor + pause/resume path is on):
  - a step installs the collector fixture *before* the helm install so the
    OTLP endpoint resolves at workload boot;
  - the gVisor-mode helm install gains
    `--set observability.enabled=true --set observability.endpoint=…` (the
    non-OTel path is byte-for-byte unchanged);
  - an `OTel arrival smoke` step runs after the pause/resume + warm-pool
    smokes (so every service has emitted) and asserts arrival;
  - the failure-diagnostics step now dumps the collector Deployment + logs.
- **`docs/observability.md`** — documents `just otel-arrival-smoke` and the new
  CI step alongside the existing manual single-`trace_id` procedure.

## Why this shape

- node + envd only emit telemetry for a real gVisor sandbox, so the assertion
  is sequenced after the pause/resume smoke (which creates one) and gated on
  `KIND_SMOKE_INCLUDE_PAUSE_RESUME`. The expected-service set is configurable
  so a lighter workload can assert just api + controller.
- The collector is applied before the helm install, not after, so the first
  batch export lands rather than being dropped against a missing Service.
- The assertion reads `kubectl logs`, not the api gateway, so it needs no
  port-forward.

## Validation

- `bash -n` + `shellcheck`: clean.
- Parser exercised offline via `OTEL_SMOKE_LOG_FILE` against a synthetic
  detailed-debug sample: all-four-present passes; dropping `envd` fails with
  the missing name reported; a narrowed `OTEL_SMOKE_EXPECTED_SERVICES` passes;
  `sandbawks-node-foo` does NOT satisfy `sandbawks-node` (exact-match guard).
- `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`: pass (no Rust
  changed; workspace gate confirms the change is shell/CI/docs only).
- The in-cluster path runs in CI on PRs via the `kind-smoke` job; a local kind
  cluster was not stood up for this change (the offline seam covers the
  parser; the cluster path is what CI exists to exercise).
