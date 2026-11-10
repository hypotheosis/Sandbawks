#!/usr/bin/env bash
# Local OTLP emission smoke for the #335 SnapshotJob metrics.
#
# Stands up a throwaway `opentelemetry-collector-contrib` in Docker (the same
# image/config as the kind fixture, but no cluster needed), runs the
# `emit_snapshotjob_metrics` example against it through the *real* OTel SDK, and
# asserts the collector's debug exporter received both new instruments —
# `snapshotjob.claims_total` (counter, node+mode) and `snapshotjob.watch_info`
# (gauge, node+namespace). This is the local complement to `just
# otel-arrival-smoke`, which needs a full gVisor cluster: it proves the
# *emission* path of the node metrics without one.
#
# Usage:  just otel-emit-smoke   (or: bash scripts/otel-emit-smoke.sh)
#
# Overrides (env): COLLECTOR_IMAGE, OTLP_HTTP_PORT (default 4318),
# OTLP_GRPC_PORT (default 4317).
set -euo pipefail

COLLECTOR_IMAGE="${COLLECTOR_IMAGE:-docker.io/otel/opentelemetry-collector-contrib:0.123.0}"
OTLP_HTTP_PORT="${OTLP_HTTP_PORT:-4318}"
OTLP_GRPC_PORT="${OTLP_GRPC_PORT:-4317}"
CONTAINER="sandbawks-otel-emit-smoke"

# Workspace root is this script's parent dir, so the recipe works from anywhere.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$(mktemp /tmp/otelcol-emit-smoke.XXXXXX.yaml)"

cleanup() {
    docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
    rm -f "${CONFIG}"
}
trap cleanup EXIT

# Minimal pipeline: accept OTLP on both transports, dump every signal to stdout
# via the debug exporter so we can grep the received metrics back out.
cat >"${CONFIG}" <<'YAML'
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [debug]
YAML
# World-readable + SELinux relabel (`:z`): the collector runs as non-root and,
# on an SELinux host (Fedora), can't otherwise read a bind-mounted /tmp file.
chmod 644 "${CONFIG}"

echo "==> starting collector (${COLLECTOR_IMAGE})"
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" \
    -p "${OTLP_GRPC_PORT}:4317" -p "${OTLP_HTTP_PORT}:4318" \
    -v "${CONFIG}:/etc/otelcol/config.yaml:ro,z" \
    "${COLLECTOR_IMAGE}" --config /etc/otelcol/config.yaml >/dev/null

# Wait for the receiver to be listening before emitting.
for _ in $(seq 1 30); do
    if docker logs "${CONTAINER}" 2>&1 | grep -q "Everything is ready"; then
        break
    fi
    sleep 0.5
done

echo "==> emitting metrics via the real SDK"
OTEL_SDK_DISABLED=0 \
OTEL_SERVICE_NAME=sandbawks-node \
OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:${OTLP_HTTP_PORT}" \
    cargo run -q --manifest-path "${ROOT}/Cargo.toml" \
        -p sandbawks-node --example emit_snapshotjob_metrics

# Give the collector a moment to print the last received batch.
sleep 2
LOGS="$(docker logs "${CONTAINER}" 2>&1)"

echo "==> asserting both instruments arrived"
fail=0
for needle in \
    "Name: snapshotjob.claims_total" \
    "Name: snapshotjob.watch_info" \
    "node: Str(emit-smoke-node)" \
    "mode: Str(pause)" \
    "mode: Str(resume)" \
    "mode: Str(stage)" \
    "namespace: Str(sandbawks-system)"; do
    if grep -qF "${needle}" <<<"${LOGS}"; then
        echo "  ok: ${needle}"
    else
        echo "  MISSING: ${needle}"
        fail=1
    fi
done

if [[ "${fail}" -ne 0 ]]; then
    echo "FAIL: expected SnapshotJob metrics did not reach the collector" >&2
    exit 1
fi
echo "PASS: snapshotjob.claims_total and snapshotjob.watch_info reached the collector"
