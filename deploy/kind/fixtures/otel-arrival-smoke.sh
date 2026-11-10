#!/usr/bin/env bash
# OTel end-to-end "collector arrival" smoke (issue #311 — OTel Phase 2
# follow-up to PR #310).
#
# PR #310 proved, locally, that the SDK is *wired up*: the disabled path logs
# to stderr and the active path builds the full provider stack. What it never
# exercised is the rollout's actual acceptance criterion — telemetry
# *arriving* at a live OTLP collector stamped with the right `service.name`.
# This script closes that gap.
#
# What it asserts: for each expected `service.name`, at least one signal (a
# span, metric, or log record) bearing that name has reached the in-cluster
# debug collector (deploy/kind/fixtures/otel-collector.yaml). That collector's
# `debug` exporter prints every received signal's resource attributes to its
# own stdout as `service.name: Str(<name>)`, so `kubectl logs` is the whole
# assertion surface — no APM, no query language, no token required.
#
# Preconditions (the kind-smoke workflow's gVisor + pause/resume path arranges
# all three; `just otel-arrival-smoke` assumes the same cluster shape):
#   1. The otel-collector fixture is applied — this script applies and waits on
#      it when OTEL_SMOKE_APPLY_COLLECTOR=1 (the default), so a standalone run
#      is self-contained.
#   2. The chart was installed with observability.enabled=true and
#      observability.endpoint pointing at the collector, so all four workloads
#      export there (the helm `sandbawks.observabilityEnv` helper wires
#      api/controller/node; the controller forwards the same config into the
#      envd container per issue #345).
#   3. A workload has been driven that exercises every expected service. api +
#      controller emit on any sandbox lifecycle action; node + envd only emit
#      for a real gVisor sandbox, so the pause/resume smoke must have run for
#      the default four-service assertion to pass. Narrow the set via
#      OTEL_SMOKE_EXPECTED_SERVICES when driving a lighter workload.
#
# Tunables (all env, sensible defaults baked in):
#   OTEL_SMOKE_EXPECTED_SERVICES  space-separated service.names to require
#                                 (default: the four rollout services). Set to
#                                 the empty string to assert attributes only.
#   OTEL_SMOKE_EXPECTED_ATTRS     space-separated span/resource attributes to
#                                 *additionally* require (issue #426). Each is
#                                 either `key=value` (exact match, e.g.
#                                 `gen_ai.operation.name=execute_tool`) or a bare
#                                 `key` (presence of any value, e.g.
#                                 `gen_ai.tool.call.arguments`). Empty by default.
#   OTEL_SMOKE_NAMESPACE          collector namespace (default: observability).
#   OTEL_SMOKE_DEPLOY             collector Deployment (default: otel-collector).
#   OTEL_SMOKE_TIMEOUT_SECS       arrival poll budget in seconds (default: 120).
#                                 Generous on purpose: the OTLP batch processors
#                                 flush on an interval, so the first signals lag
#                                 the workload by a few seconds.
#   OTEL_SMOKE_POLL_SECS          seconds between collector-log polls (default: 5).
#   OTEL_SMOKE_APPLY_COLLECTOR    apply + await the fixture if 1 (default: 1).
#   OTEL_SMOKE_LOG_FILE           read collector output from this file instead
#                                 of `kubectl logs` — an offline / unit-test
#                                 seam that exercises the parser without a
#                                 cluster (single pass, no apply, no polling).
set -euo pipefail

# Resolve the repo root from this script's location (deploy/kind/fixtures/ ->
# three parents up) so the collector-fixture apply works regardless of the
# caller's working directory.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COLLECTOR_FIXTURE="${REPO_ROOT}/deploy/kind/fixtures/otel-collector.yaml"

# Default to the four services the Phase 2 rollout promises (OBSERVABILITY.md
# §"service.name" table). `read -r -a` splits the (possibly overridden)
# space-separated list into an array; an empty override yields a 0-length array
# (assert attributes only).
# `-` (not `:-`): an explicitly-empty `OTEL_SMOKE_EXPECTED_SERVICES=""` means
# "assert attributes only" and must be honored, whereas *unset* falls back to
# the four rollout services.
read -r -a EXPECTED_SERVICES <<<"${OTEL_SMOKE_EXPECTED_SERVICES-sandbawks-api sandbawks-controller sandbawks-node sandbawks-envd}"
# Additional arbitrary attributes to require, space-separated `key=value`
# (e.g. `gen_ai.operation.name=execute_tool`). A service is just the attribute
# `service.name=<svc>`, so both fold into one matcher below. Empty by default.
read -r -a EXPECTED_ATTRS <<<"${OTEL_SMOKE_EXPECTED_ATTRS:-}"
NAMESPACE="${OTEL_SMOKE_NAMESPACE:-observability}"
DEPLOY="${OTEL_SMOKE_DEPLOY:-otel-collector}"
TIMEOUT_SECS="${OTEL_SMOKE_TIMEOUT_SECS:-120}"
POLL_SECS="${OTEL_SMOKE_POLL_SECS:-5}"
APPLY_COLLECTOR="${OTEL_SMOKE_APPLY_COLLECTOR:-1}"
LOG_FILE="${OTEL_SMOKE_LOG_FILE:-}"

# Parallel arrays of (label, needle): a human label for reporting and the exact
# fixed-string to grep in the debug exporter's `verbosity: detailed` dump. Every
# attribute renders there as `     -> <key>: Str(<value>)`; the trailing `)`
# anchors the match to the *exact* value (so `sandbawks-node` never matches a
# hypothetical `sandbawks-node-foo`), and a fixed-string (`-F`) grep keeps the
# parens literal.
TARGET_LABELS=()
TARGET_NEEDLES=()

# Register one (label, needle) target. `$1` label, `$2` needle.
register_target() {
    TARGET_LABELS+=("$1")
    TARGET_NEEDLES+=("$2")
}

# A service `foo` is asserted via its `service.name` resource attribute.
for svc in "${EXPECTED_SERVICES[@]}"; do
    register_target "service.name=${svc}" "service.name: Str(${svc})"
done
# An attribute is asserted either by exact value (`key=value`, split on the
# FIRST `=` so the value may itself contain `=`) or, when written as a bare
# `key` with no `=`, by *presence* of any value (`key: Str(` — useful when the
# value is non-deterministic, e.g. a redacted command line whose exact text
# depends on how the SDK shell-wraps the command).
for attr in "${EXPECTED_ATTRS[@]}"; do
    if [[ "${attr}" == *=* ]]; then
        register_target "${attr}" "${attr%%=*}: Str(${attr#*=})"
    else
        register_target "${attr} (any value)" "${attr}: Str("
    fi
done

if (( ${#TARGET_NEEDLES[@]} == 0 )); then
    echo "otel-arrival-smoke: nothing to assert (both services and attrs empty)" >&2
    exit 2
fi

# Echo the collector's accumulated stdout to its first argument (a file path).
# Reads from OTEL_SMOKE_LOG_FILE when set (offline/test seam), else the full
# (`--tail=-1`) Deployment logs. A transient `kubectl logs` failure — e.g. the
# pod restarting mid-poll — is swallowed so the poll loop simply retries on the
# next tick rather than aborting the whole smoke.
collector_logs_into() {
    local out="$1"
    if [[ -n "${LOG_FILE}" ]]; then
        cat "${LOG_FILE}" >"${out}"
        return 0
    fi
    kubectl logs --tail=-1 "deploy/${DEPLOY}" -n "${NAMESPACE}" >"${out}" 2>/dev/null || : >"${out}"
}

# Print, one per line, the LABEL of every target whose needle is absent from the
# collector output `$1`.
missing_targets() {
    local logfile="$1" i
    for i in "${!TARGET_NEEDLES[@]}"; do
        grep -qF "${TARGET_NEEDLES[$i]}" "${logfile}" || printf '%s\n' "${TARGET_LABELS[$i]}"
    done
}

# Dump every distinct attribute value the collector saw that we care about
# (service.name plus any gen_ai.* / error.type) — the single most useful thing
# to look at when an expected target is missing (typo vs. genuinely absent vs.
# wrong endpoint all look different here).
report_seen() {
    local logfile="$1" seen
    seen="$(grep -oE '(service\.name|gen_ai\.[a-z_.]+|error\.type): Str\([^)]*\)' "${logfile}" \
        | sort -u || true)"
    if [[ -n "${seen}" ]]; then
        echo "otel-arrival-smoke: attribute values seen in collector output:" >&2
        printf '%s\n' "${seen}" >&2
    else
        echo "otel-arrival-smoke: NO matching attributes seen in collector output at all." >&2
        echo "  -> the collector received nothing; check observability.enabled/endpoint and that a workload ran." >&2
    fi
}

main() {
    echo "otel-arrival-smoke: asserting collector arrival for: ${TARGET_LABELS[*]}"

    # Stand up (or refresh) the collector unless told not to, and unless we are
    # asserting against a captured log file. The fixture is idempotent —
    # `kubectl apply` upserts and `rollout status` is a wait, not a mutation.
    if [[ -z "${LOG_FILE}" && "${APPLY_COLLECTOR}" == "1" ]]; then
        echo "otel-arrival-smoke: applying collector fixture ${COLLECTOR_FIXTURE}"
        kubectl apply -f "${COLLECTOR_FIXTURE}"
        kubectl -n "${NAMESPACE}" rollout status "deploy/${DEPLOY}" --timeout=120s
    fi

    local logfile
    logfile="$(mktemp)"
    # shellcheck disable=SC2064  # expand logfile now, at trap-set time.
    trap "rm -f '${logfile}'" EXIT

    # File mode is a single deterministic pass — there is nothing to wait for.
    if [[ -n "${LOG_FILE}" ]]; then
        collector_logs_into "${logfile}"
        local missing
        missing="$(missing_targets "${logfile}")"
        if [[ -z "${missing}" ]]; then
            echo "otel-arrival-smoke: all ${#TARGET_NEEDLES[@]} target(s) present: ${TARGET_LABELS[*]}"
            return 0
        fi
        echo "otel-arrival-smoke: FAILED; missing target(s): ${missing//$'\n'/ }" >&2
        report_seen "${logfile}"
        return 1
    fi

    # Cluster mode: poll the collector logs until every expected service.name
    # has arrived or the budget expires. `SECONDS` is bash's monotonic
    # seconds-since-shell-start counter — a dependency-free deadline.
    local deadline=$(( SECONDS + TIMEOUT_SECS ))
    while :; do
        collector_logs_into "${logfile}"
        local missing
        missing="$(missing_targets "${logfile}")"
        if [[ -z "${missing}" ]]; then
            echo "otel-arrival-smoke: all ${#TARGET_NEEDLES[@]} target(s) arrived: ${TARGET_LABELS[*]}"
            return 0
        fi
        if (( SECONDS >= deadline )); then
            echo "otel-arrival-smoke: TIMEOUT after ${TIMEOUT_SECS}s; missing target(s): ${missing//$'\n'/ }" >&2
            report_seen "${logfile}"
            return 1
        fi
        echo "otel-arrival-smoke: waiting (${missing//$'\n'/ } not yet arrived); retrying in ${POLL_SECS}s"
        sleep "${POLL_SECS}"
    done
}

main "$@"
