# CI: regression guard for the rendered Vector telemetry-hop config

**Date:** 2026-06-17T10:28:00Z
**Area:** `scripts/vector-hop-roundtrip-smoke.sh`, `.forgejo/workflows/ci.yml`, `justfile`
**Relates to:** #516; guards the #496 / PR #515 fix class (regression from #499)

## Why

PR #515 fixed a bug (#496) where the Vector telemetry hop dropped 100% of
traces and logs: the `opentelemetry` source decoded OTLP into Vector's native
model while the `opentelemetry` sink (`codec: otlp`) required the raw OTLP
envelope, so every event failed to *serialize* and was silently dropped.

The bug's defining property: **`vector validate` passed the whole time.** It is
a *semantic* source/sink shape mismatch at serialize time, not a config syntax
error, so nothing in CI caught it — and nothing would catch a regression of the
same class (a future Vector image bump, a `use_otlp_decoding` change, a stamp
edit, or a `build_otlp_metrics` change could re-break the hop and ship green).

The existing `otel-arrival-smoke` step targets the dev stdout debug fixture
(`deploy/kind/fixtures/otel-collector.yaml`), not the production Vector hop
(`deploy/helm/sandbawks/templates/otel-collector-configmap.yaml`), so it never
exercised this path.

## What

A new self-contained, cluster-free harness reproducing PR #515's local repro in
CI, plus the wiring to run it.

### `scripts/vector-hop-roundtrip-smoke.sh`

Two layers, smallest first:

1. **`vector validate`** on the rendered chart config — fast topology / VRL
   compile / bad-inputs preflight. (Does NOT catch the serialize-time mismatch;
   that passed validate. Layer 2 is the real guard.)
2. **Live OTLP round-trip** through the rendered hop:
   - renders `data["vector.yaml"]` from the chart with the collector + the
     prometheus surface enabled, pointing the Traceway exporter at a stand-in
     receiver;
   - runs that rendered config as the **sender**, a second Vector
     (`opentelemetry` source, full `use_otlp_decoding`, `console` sink) as a
     stand-in Traceway **receiver**;
   - drives `telemetrygen` to push real OTLP protobuf (traces + logs +
     gauge/sum/histogram metrics) carrying a spoofed
     `service.namespace="EVIL-TENANT"`;
   - asserts:
     - **0** `does not contain OTLP top-level field` / `Failed serializing`
       drops in the sender (the #496 class);
     - all three signals decode at the receiver;
     - the spoofed `service.namespace` is overwritten with the trusted
       namespace on traces, logs **and** metrics (also regression-guards the
       metrics-stamp security fix);
     - the OTLP histogram invariant `bucketCounts == explicitBounds + 1`;
     - `:9090/metrics` exposes the metric series with the stamped tenant labels
       and never the spoofed one.

Implementation notes:

- **Same pinned Vector image as the chart.** The default Vector image is read
  straight from `observability.collector.image.{repository,tag}` in
  `values.yaml` (via `yq`), so CI and prod can't drift — satisfying the issue's
  "uses the same pinned Vector image" criterion structurally, not by a copied
  constant.
- **`telemetrygen` pinned to `v0.135.0`** — the first tag whose `metrics`
  subcommand supports `--metric-type Histogram` (0.123.0–0.130.0 only emit
  Gauge/Sum). Pulled through the ghcr.io pull-through proxy
  (a ghcr.io pull-through proxy), so the guard needs no direct ghcr.io egress.
- **Config shipped via `docker cp`, not a `-v` bind mount.** Under the Forgejo
  `runs-on: docker` runner the daemon is a separate dind sidecar (reached via
  `DOCKER_HOST`), so a bind mount of a path on the runner-container filesystem
  would resolve to the wrong path on the daemon host. `docker cp` streams the
  file over the Docker API, working identically against a shared local daemon
  and a dind one — and sidesteps the SELinux `:Z` relabel a bind mount needs on
  the Fedora CI host.
- All cross-container traffic flows between sibling containers on a
  run-scoped user-defined network; the orchestrator never needs to reach a
  container directly, so the harness is dind-agnostic.
- Fully tunable via `VHOP_*` env (image overrides, namespaces, prom port,
  signal count, timeouts, `VHOP_KEEP=1` to leave containers up).

### CI job `vector-hop` (`.forgejo/workflows/ci.yml`)

`runs-on: docker` (the dind-backed runner) on the existing `sandbawks-ci-runner`
image (already ships docker + helm + jq; the job adds only a pinned `yq`).
Gated on the `helm` path filter (any chart change) OR the `scripts` filter (the
harness or the workflow file), so it runs exactly when the rendered config or
the guard itself could change.

### `justfile` recipe `vector-hop-smoke`

Local entry point: `just vector-hop-smoke`. Requires Docker, helm, yq; no
cluster.

## Verification

- Harness passes locally against the current chart (`vector 0.56.0-debian`):
  validate OK; 0 serialize drops; traces/logs/metrics all decoded; tenant
  boundary holds on all three signals; histogram invariant holds; prometheus
  surface exposed with stamped labels.
- **Negative control:** reverting the #496 fix in the chart (setting
  `use_otlp_decoding.{logs,traces}=false`) makes the harness FAIL — `vector
  validate` still passes (confirming why layer 2 is required) while the arrival
  assertion catches the dropped traces/logs. Reverted after the check.
- `shellcheck` + `bash -n` clean (also enforced by the `scripts` CI gate).

## Acceptance criteria (issue #516)

- [x] CI fails if the rendered Vector hop config fails `vector validate`.
- [x] CI fails if a serialize-time shape mismatch drops a signal (the #496 class).
- [x] CI fails if the metrics tenant stamp stops overwriting a spoofed `service.namespace`.
- [x] Uses the same pinned Vector image as the chart (read from `values.yaml`).

## Registry access

All three images are pulled through the private registry pull-through proxies — Vector
and busybox via `dockerio`, `telemetrygen` via `ghcrio` — so the job needs no
direct egress to Docker Hub or ghcr.io. The kind-smoke already pulls from
the private registry on the same dind daemon, so this path is known reachable.
