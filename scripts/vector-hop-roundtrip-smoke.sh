#!/usr/bin/env bash
# Regression guard for the rendered production Vector telemetry hop (issue #516,
# guarding the #496 / PR #515 fix class).
#
# The bug PR #515 fixed had one nasty property: `vector validate` passed the
# whole time. The `opentelemetry` source decoded OTLP into Vector's native model
# while the `opentelemetry` sink (`codec: otlp`) required the raw OTLP envelope,
# so every event failed to *serialize* at flush time and was silently dropped —
# a semantic source/sink shape mismatch, not a config syntax error. Nothing in
# CI exercised the serialize path, so the hop could (and did) ship 100%-dropping
# and green.
#
# This harness closes that gap with two layers, smallest first:
#
#   1. `vector validate` on the rendered chart config (catches topology / VRL
#      compile / bad-inputs-wiring errors — but NOT the serialize-time mismatch,
#      which passed validate; that is exactly why layer 2 exists).
#   2. A live round-trip through the rendered hop: the production config runs as
#      the SENDER, a second Vector (`opentelemetry` source, full
#      `use_otlp_decoding`) stands in as the Traceway intake RECEIVER,
#      `telemetrygen` pushes real OTLP protobuf (traces + logs + gauge/sum/
#      histogram metrics) carrying a SPOOFED `service.namespace`, and we assert:
#        - 0 `does not contain OTLP top-level field` / `Failed serializing`
#          errors in the sender logs (the #496 class);
#        - all three signals decode at the receiver;
#        - the tenant boundary holds — the spoofed `service.namespace` is
#          overwritten on traces, logs AND metrics (regression-guards the
#          metrics-stamp security fix too);
#        - the OTLP histogram invariant `bucketCounts == explicitBounds + 1`;
#        - the `:<prom-port>/metrics` scrape surface exposes the metric series
#          with the stamped tenant labels.
#
# Everything runs in Docker against the SAME pinned Vector image the chart uses
# (`observability.collector.image` in values.yaml), so CI and prod can't drift.
#
# Preconditions: `docker`, `helm`, `yq`, and `jq` on PATH, and a reachable
# Docker daemon. The chart's Vector image comes from Docker Hub and the
# `telemetrygen` image from ghcr.io; override either (below) to use a mirror.
#
# Config is shipped into the containers with `docker cp`, NOT a `-v` bind mount.
# That is deliberate: when the daemon is remote (a dind sidecar reached via
# `DOCKER_HOST`), a bind mount of a path on THIS host's filesystem would resolve
# to the wrong — empty — path on the daemon host. `docker cp` streams the file
# over the Docker API, so it works identically against a local daemon and a
# remote/dind one (and sidesteps the SELinux `:Z` relabel a bind mount needs).
#
# Tunables (all env, sensible defaults baked in):
#   VHOP_VECTOR_IMAGE        Vector image for both sender and receiver. Default:
#                            read from the chart's values.yaml so this can never
#                            silently drift from what prod runs.
#   VHOP_TELEMETRYGEN_IMAGE  OTLP load generator image. Default: pinned
#                            telemetrygen v0.135.0 (first tag whose `metrics`
#                            subcommand supports `--metric-type Histogram`),
#                            from ghcr.io.
#   VHOP_NAMESPACE           the trusted namespace the hop stamps onto every
#                            signal (stands in for the downward-API
#                            `${SANDBAWKS_NAMESPACE}`). Default: sandbawks-prod.
#   VHOP_SPOOF_NAMESPACE     the tenant the load generator tries to spoof; must
#                            be overwritten everywhere. Default: EVIL-TENANT.
#   VHOP_PROM_PORT           prometheus_exporter scrape port to render + probe.
#                            Default: 9090.
#   VHOP_SIGNAL_COUNT        number of each signal telemetrygen emits. Default: 5.
#   VHOP_READY_TIMEOUT_SECS  per-container startup budget. Default: 60.
#   VHOP_ARRIVAL_TIMEOUT_SECS  budget for all three signals to reach the
#                            receiver after the load generator exits. Default: 30.
#   VHOP_KEEP                if 1, leave the containers/network up on exit (for
#                            manual poking). Default: 0 (always torn down).
set -euo pipefail

# Resolve the repo root from this script's location (scripts/ -> one parent up)
# so `helm template` finds the chart regardless of the caller's working dir.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${REPO_ROOT}/deploy/helm/sandbawks"
VALUES_FILE="${CHART_DIR}/values.yaml"

NAMESPACE="${VHOP_NAMESPACE:-sandbawks-prod}"
SPOOF_NAMESPACE="${VHOP_SPOOF_NAMESPACE:-EVIL-TENANT}"
PROM_PORT="${VHOP_PROM_PORT:-9090}"
SIGNAL_COUNT="${VHOP_SIGNAL_COUNT:-5}"
READY_TIMEOUT_SECS="${VHOP_READY_TIMEOUT_SECS:-60}"
ARRIVAL_TIMEOUT_SECS="${VHOP_ARRIVAL_TIMEOUT_SECS:-30}"
KEEP="${VHOP_KEEP:-0}"
TELEMETRYGEN_IMAGE="${VHOP_TELEMETRYGEN_IMAGE:-ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.135.0}"
# Tiny image with `wget`, used only to scrape the sender's prometheus surface.
# Pulled through the dockerio proxy like every other Docker Hub image here.
BUSYBOX_IMAGE="${VHOP_BUSYBOX_IMAGE:-docker.io/library/busybox:1.36}"

# Default the Vector image to whatever the chart pins, so this guard exercises
# the exact build prod runs (issue #516 acceptance: "uses the same pinned Vector
# image as the chart"). `yq` reads the repository:tag straight out of values.
default_vector_image() {
    local repo tag
    repo="$(yq -r '.observability.collector.image.repository' "${VALUES_FILE}")"
    tag="$(yq -r '.observability.collector.image.tag' "${VALUES_FILE}")"
    printf '%s:%s' "${repo}" "${tag}"
}
VECTOR_IMAGE="${VHOP_VECTOR_IMAGE:-$(default_vector_image)}"

# Unique, run-scoped names so concurrent runs / leftover containers never
# collide ($$ is the shell PID). The receiver/sender network ALIASES are fixed
# (`receiver`/`sender`) because the rendered sender config hard-codes the
# receiver hostname via the exporter endpoint we pass to `helm template`.
RUN_ID="$$"
NET="vhop-net-${RUN_ID}"
RECV="vhop-recv-${RUN_ID}"
SEND="vhop-send-${RUN_ID}"
VALIDATE="vhop-validate-${RUN_ID}"
WORKDIR="$(mktemp -d)"

log()  { printf 'vector-hop-smoke: %s\n' "$*"; }
fail() { printf 'vector-hop-smoke: FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
    if [[ "${KEEP}" == "1" ]]; then
        log "VHOP_KEEP=1; leaving ${RECV}, ${SEND}, network ${NET} up"
        return
    fi
    docker rm -f "${RECV}" "${SEND}" "${VALIDATE}" >/dev/null 2>&1 || true
    docker network rm "${NET}" >/dev/null 2>&1 || true
    rm -rf "${WORKDIR}"
}
trap cleanup EXIT

# Render the production Vector config (data["vector.yaml"]) with the collector
# and the prometheus surface enabled, pointing the Traceway exporter at our
# stand-in receiver. `--show-only` emits just the ConfigMap; `yq` pulls the
# embedded config string out of it.
render_sender_config() {
    helm template vhop "${CHART_DIR}" \
        --set observability.enabled=true \
        --set observability.collector.enabled=true \
        --set "observability.collector.exporter.endpoint=http://${RECV}:4318" \
        --set observability.collector.exporter.token=dummy-token \
        --set observability.collector.prometheus.enabled=true \
        --set "observability.collector.prometheus.port=${PROM_PORT}" \
        --show-only templates/otel-collector-configmap.yaml \
        | yq -r '.data["vector.yaml"]' > "${WORKDIR}/sender.yaml"
    [[ -s "${WORKDIR}/sender.yaml" ]] || fail "rendered sender config is empty"
}

# A minimal stand-in Traceway intake: an opentelemetry source with FULL OTLP
# decoding (so every signal is rebuilt into its raw envelope) dumped to stdout
# as JSON. `kubectl logs`-style capture of this container is the assertion
# surface for arrival, tenant boundary, and the histogram invariant.
write_receiver_config() {
    cat > "${WORKDIR}/receiver.yaml" <<'EOF'
data_dir: /vector-data-dir
api:
  enabled: false
sources:
  in:
    type: opentelemetry
    use_otlp_decoding:
      logs: true
      traces: true
      metrics: true
    grpc:
      address: "[::]:4317"
    http:
      address: "[::]:4318"
sinks:
  out:
    type: console
    inputs: ["in.logs", "in.traces", "in.metrics"]
    encoding:
      codec: json
EOF
}

# Block until `docker logs <container>` contains <needle>, or the readiness
# budget expires. `SECONDS` is bash's monotonic since-shell-start counter.
wait_for_log() {
    local container="$1" needle="$2" deadline=$(( SECONDS + READY_TIMEOUT_SECS ))
    while :; do
        if docker logs "${container}" 2>&1 | grep -qF "${needle}"; then
            return 0
        fi
        if ! docker inspect -f '{{.State.Running}}' "${container}" 2>/dev/null | grep -q true; then
            docker logs "${container}" 2>&1 | tail -20 >&2
            fail "${container} exited before becoming ready (waiting for '${needle}')"
        fi
        (( SECONDS < deadline )) || { docker logs "${container}" 2>&1 | tail -20 >&2; fail "${container} not ready within ${READY_TIMEOUT_SECS}s"; }
        sleep 1
    done
}

# Start one Vector container (sender or receiver) with its config copied in via
# `docker cp` (see the header for why not a bind mount). `--user 0:0` + a tmpfs
# data_dir sidestep the image's non-root `vector` user not owning the scratch
# dir. The config lands at `/vector.yaml` (root always exists, unlike
# `/etc/vector`, which `docker cp` would refuse to create). Args after the first
# three are extra `docker create` flags (env vars). Create -> cp -> start so the
# config is in place before Vector reads it.
run_vector() {
    local name="$1" alias="$2" cfg="$3"; shift 3
    docker create --name "${name}" --network "${NET}" --network-alias "${alias}" \
        --user 0:0 --tmpfs /vector-data-dir \
        "$@" \
        "${VECTOR_IMAGE}" --config /vector.yaml >/dev/null
    docker cp "${cfg}" "${name}:/vector.yaml"
    docker start "${name}" >/dev/null
}

# Fire telemetrygen for one signal, spoofing service.namespace so the tenant
# re-stamp is exercised. $1 is the subcommand (traces/logs/metrics), the rest
# are extra flags (e.g. --metric-type Histogram). All target the SENDER's OTLP
# HTTP port; the count flag is `--<signal> N` for traces/logs/metrics alike.
emit() {
    local signal="$1"; shift
    docker run --rm --network "${NET}" "${TELEMETRYGEN_IMAGE}" "${signal}" \
        --otlp-http --otlp-insecure --otlp-endpoint "${SEND}:4318" \
        --otlp-attributes "service.namespace=\"${SPOOF_NAMESPACE}\"" \
        "--${signal}" "${SIGNAL_COUNT}" "$@" >/dev/null 2>&1
}

# Poll the receiver's captured JSON until all three OTLP envelope types have
# arrived, or the arrival budget expires. Writes the captured lines to $1.
wait_for_arrival() {
    local out="$1" deadline=$(( SECONDS + ARRIVAL_TIMEOUT_SECS ))
    while :; do
        docker logs "${RECV}" 2>/dev/null | grep -E '^\{' > "${out}" || true
        if grep -q resourceSpans "${out}" && grep -q resourceLogs "${out}" && grep -q resourceMetrics "${out}"; then
            return 0
        fi
        if (( SECONDS >= deadline )); then
            log "signals present so far: spans=$(grep -c resourceSpans "${out}" || true) logs=$(grep -c resourceLogs "${out}" || true) metrics=$(grep -c resourceMetrics "${out}" || true)"
            return 1
        fi
        sleep 2
    done
}

main() {
    log "vector image:       ${VECTOR_IMAGE}"
    log "telemetrygen image: ${TELEMETRYGEN_IMAGE}"

    render_sender_config
    write_receiver_config

    # --- Layer 1: vector validate on the rendered config -------------------
    # `--no-environment` skips healthchecks (the Traceway endpoint is
    # unreachable in CI); the tenant/token env vars must still be set because
    # Vector errors on an undefined `${VAR}` interpolation at load time. Same
    # create -> cp -> start-and-attach dance as run_vector so it works under a
    # dind daemon; `-a` streams validate's output and propagates its exit code.
    log "layer 1: vector validate on the rendered hop config"
    docker create --name "${VALIDATE}" --user 0:0 --tmpfs /vector-data-dir \
        -e "SANDBAWKS_NAMESPACE=${NAMESPACE}" -e TRACEWAY_OTLP_TOKEN=dummy-token \
        "${VECTOR_IMAGE}" validate --no-environment /vector.yaml >/dev/null
    docker cp "${WORKDIR}/sender.yaml" "${VALIDATE}:/vector.yaml"
    docker start -a "${VALIDATE}" || true
    local validate_code
    validate_code="$(docker inspect -f '{{.State.ExitCode}}' "${VALIDATE}")"
    docker rm -f "${VALIDATE}" >/dev/null 2>&1 || true
    (( validate_code == 0 )) || fail "vector validate rejected the rendered hop config (exit ${validate_code})"

    # --- Layer 2: live round-trip through the rendered hop -----------------
    log "layer 2: round-trip (sender = rendered hop, receiver = stand-in intake)"
    # A prior run killed before its EXIT trap fired (OOM, timeout, runner
    # teardown) can leak the network/containers under this same PID-derived
    # name — container PIDs are low and recycled, so `docker network create`
    # then fails "network ... already exists". Remove same-named leftovers
    # first so the guard self-heals on a shared runner. Same-named only, so a
    # concurrent run's resources are never touched.
    docker rm -f "${RECV}" "${SEND}" "${VALIDATE}" >/dev/null 2>&1 || true
    docker network rm "${NET}" >/dev/null 2>&1 || true
    docker network create "${NET}" >/dev/null

    run_vector "${RECV}" receiver "${WORKDIR}/receiver.yaml"
    run_vector "${SEND}" sender   "${WORKDIR}/sender.yaml" \
        -e "SANDBAWKS_NAMESPACE=${NAMESPACE}" -e TRACEWAY_OTLP_TOKEN=dummy-token

    # The receiver must be listening before the sender healthchecks its sinks
    # against it, and both must be up before telemetrygen fires.
    wait_for_log "${RECV}" "Vector has started"
    wait_for_log "${SEND}" "Vector has started"
    wait_for_log "${SEND}" "Healthcheck passed"

    log "emitting ${SIGNAL_COUNT}x traces, logs, gauge, sum, histogram (spoofing service.namespace=${SPOOF_NAMESPACE})"
    emit traces
    emit logs
    emit metrics --metric-type Gauge
    emit metrics --metric-type Sum
    emit metrics --metric-type Histogram

    # Assertion 1 (the #496 class): the sender must not have logged a single
    # serialize-time drop. This is the regression that shipped green before.
    local serialize_errs
    serialize_errs="$(docker logs "${SEND}" 2>&1 | grep -cE 'does not contain OTLP top-level|Failed serializing' || true)"
    if (( serialize_errs != 0 )); then
        docker logs "${SEND}" 2>&1 | grep -E 'does not contain OTLP top-level|Failed serializing' | head -5 >&2
        fail "${serialize_errs} serialize-time drop(s) in sender logs (the #496 source/sink shape mismatch regressed)"
    fi
    log "PASS: 0 serialize-time drops in the sender (the #496 class)"

    # Assertion 2: all three signals decoded at the receiver.
    local recv="${WORKDIR}/recv.jsonl"
    if ! wait_for_arrival "${recv}"; then
        fail "not all signals reached the receiver within ${ARRIVAL_TIMEOUT_SECS}s (a signal was dropped in the hop)"
    fi
    log "PASS: traces, logs and metrics all decoded at the receiver"

    # Assertion 3: tenant boundary. The spoofed namespace must appear NOWHERE in
    # the received envelopes, and our trusted namespace must be stamped on each
    # of the three signal types.
    if grep -qF "${SPOOF_NAMESPACE}" "${recv}"; then
        grep -F "${SPOOF_NAMESPACE}" "${recv}" | head -2 >&2
        fail "spoofed namespace '${SPOOF_NAMESPACE}' leaked through the hop (tenant boundary open)"
    fi
    local sig
    for sig in resourceSpans resourceLogs resourceMetrics; do
        grep "${sig}" "${recv}" | grep -qF "${NAMESPACE}" \
            || fail "trusted namespace '${NAMESPACE}' not stamped on ${sig} (tenant re-stamp missing for this signal)"
    done
    log "PASS: tenant boundary holds — '${SPOOF_NAMESPACE}' overwritten with '${NAMESPACE}' on traces, logs AND metrics"

    # Assertion 4: OTLP histogram invariant — bucketCounts has exactly one more
    # entry than explicitBounds (the implicit +inf bucket). This guards the
    # hand-built build_otlp_metrics envelope against a Vector +inf-representation
    # change.
    local hist_points bad_points
    hist_points="$(jq -c 'recurse | objects | select(has("histogram")) | .histogram.dataPoints[]' "${recv}" 2>/dev/null | wc -l)"
    (( hist_points > 0 )) || fail "no histogram data points reached the receiver (the metrics Traceway leg dropped them)"
    bad_points="$(jq -c 'recurse | objects | select(has("histogram")) | .histogram.dataPoints[] | select((.bucketCounts|length) != (.explicitBounds|length) + 1)' "${recv}" 2>/dev/null | wc -l)"
    (( bad_points == 0 )) || fail "${bad_points} histogram data point(s) violate bucketCounts == explicitBounds + 1"
    log "PASS: histogram invariant holds on ${hist_points} data point(s) (bucketCounts == explicitBounds + 1)"

    # Assertion 5: the prometheus_exporter scrape surface exposes the metric
    # series, carrying the stamped tenant labels and never the spoofed one.
    local prom
    prom="$(docker run --rm --network "${NET}" "${BUSYBOX_IMAGE}" \
            wget -qO- "http://${SEND}:${PROM_PORT}/metrics" 2>/dev/null || true)"
    grep -qE '^gen' <<<"${prom}" || fail "prometheus :${PROM_PORT}/metrics did not expose the 'gen' metric series"
    grep -q "${NAMESPACE}" <<<"${prom}" || fail "prometheus series missing the stamped namespace label '${NAMESPACE}'"
    if grep -q "${SPOOF_NAMESPACE}" <<<"${prom}"; then
        fail "prometheus series carry the spoofed namespace '${SPOOF_NAMESPACE}' (metrics tenant stamp regressed)"
    fi
    log "PASS: prometheus :${PROM_PORT}/metrics exposes the series with stamped tenant labels"

    # Assertion 6: Prometheus naming conformance. Prometheus forbids `.` in metric
    # and label names; the opentelemetry source surfaces OTLP resource attributes
    # as dotted tag keys (`resource.service.namespace`), so the hop must sanitize
    # them to `_` before the prometheus_exporter (issue #516 comment). Check the
    # name structure only: drop `# HELP/# TYPE` comments, strip quoted label
    # VALUES (which legitimately contain dots, e.g. a float or a DNS name), then
    # strip the trailing ` <value> <timestamp>` — any `.` left is in a metric or
    # label NAME and is non-conformant.
    # Keep only sample lines (drop `# HELP/# TYPE` and blanks). A Prometheus
    # metric name is `[a-zA-Z_:][a-zA-Z0-9_:]*`, so a sample line may legally
    # begin with `_` or `:` as well as a letter — match all three so a
    # `_`-prefixed dotted name can't slip past as a false negative.
    local dotted
    dotted="$(grep -vE '^#' <<<"${prom}" | grep -E '^[a-zA-Z_:]' \
        | sed 's/"[^"]*"//g; s/ .*$//' | grep -E '\.' || true)"
    if [[ -n "${dotted}" ]]; then
        printf '%s\n' "${dotted}" | head -3 >&2
        fail "prometheus exposition has '.' in a metric/label name (forbidden; sanitize_prometheus_names regressed)"
    fi
    log "PASS: prometheus exposition has no '.' in any metric or label name"

    log "ALL CHECKS PASSED"
}

main "$@"
