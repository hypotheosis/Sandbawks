#!/usr/bin/env bash
# Smoke-test assertion script for the kind-based deployment test
# (issue #72 / epic #66).
#
# Assumes `just cluster-up` already created the `sandbawks-smoke` kind
# cluster, loaded the controller image, and `helm install`-ed the chart.
# Walks one Sandbox from `Creating` to `Running` against the fake
# adapter and exits non-zero if the controller never reconciles.
#
# The jsonpath-form `kubectl wait` here requires kubectl >= 1.31, which
# matches the KIND_NODE_IMAGE pin in the justfile.
set -euo pipefail

FIXTURE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/deploy/kind/fixtures"
FIXTURE="${FIXTURE_DIR}/sandbox-smoke.yaml"

echo "smoke: waiting for sandbawks-controller deployment to become Available"
kubectl wait deploy/sandbawks-controller \
    --for=condition=Available --timeout=120s

echo "smoke: applying ${FIXTURE}"
kubectl apply -f "${FIXTURE}"

echo "smoke: waiting for sandbox/sbx-smoke to reach phase=Running"
kubectl wait sandbox/sbx-smoke \
    --for=jsonpath='{.status.phase}'=Running \
    --timeout=60s

echo "smoke: controller reconciled sbx-smoke to Running"
