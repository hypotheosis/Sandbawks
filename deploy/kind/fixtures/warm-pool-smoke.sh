#!/usr/bin/env bash
# Plan 08 A8 — SandboxWarmPool claim smoke, driven from the host against a
# `just cluster-up` kind cluster.
#
# Proves the warm-pool fast path end-to-end on real gVisor pods + S3:
#
#   1. Apply a SandboxTemplate + TeamAPIKey + SandboxWarmPool(minReady=1).
#   2. Wait for `status.ready == 1` — the controller cold-boots one
#      sandbox and holds it `Running` (issue #332: members are kept live,
#      not pre-paused), carrying the `sandbawks.dev/warm-pool` label.
#   3. Record that warm sandbox's name.
#   4. POST /sandboxes with the pool's templateID.
#   5. Assert the returned `sandboxID` EQUALS the warm sandbox's name —
#      i.e. the gateway claimed (reassigned) the warm sandbox rather than
#      minting a fresh `sbx-<random>` cold-boot id.
#   6. Confirm the claimed sandbox is `Running` (a claim is a
#      reassignment, so it is live immediately — no resume).
#   7. Pause it and wait for `Paused`, then kill it; delete the pool.
#      The pause runs a real `runsc checkpoint` → S3 under gVisor, so it
#      both exercises the non-root snapshot-write path (PR #324) and
#      proves a claimed member inherited a working `runtimeClassName`.
#
# Depends on the same S3 (garage) + gVisor infra the pause/resume smoke
# uses, so CI runs it only when KIND_SMOKE_INCLUDE_PAUSE_RESUME=1. Gated
# behind WARM_POOL_SMOKE (default 1) so a flaky run can be disabled
# without editing the workflow, mirroring the pause/resume toggle.
#
# Requires: bash, curl, kubectl, jq, openssl. The kubectl context must
# point at the smoke cluster and the api must be reachable at
# http://localhost:8080 (the workflow's background port-forward).

set -euo pipefail

WARM_POOL_SMOKE="${WARM_POOL_SMOKE:-1}"
if [[ "$WARM_POOL_SMOKE" != "1" ]]; then
    echo "warm-pool-smoke: disabled (WARM_POOL_SMOKE=${WARM_POOL_SMOKE}); skipping"
    exit 0
fi

API_BASE="${SANDBAWKS_API_BASE:-http://localhost:8080}"
NAMESPACE="${SANDBAWKS_API_NAMESPACE:-default}"
# How long to wait for the pool to warm one sandbox: a cold boot (image
# pull on a fresh node) until it is held Running. Generous so a slow CI
# node doesn't flake; the happy path is far quicker.
READY_TIMEOUT="${WARM_POOL_READY_TIMEOUT:-240}"
# How long to wait for the claimed sandbox to settle at Running.
RESUME_TIMEOUT="${WARM_POOL_RESUME_TIMEOUT:-90}"
# How long to wait for the post-claim pause (`runsc checkpoint` → S3).
PAUSE_TIMEOUT="${WARM_POOL_PAUSE_TIMEOUT:-120}"

# Known API key for the smoke — NOT A SECRET, published in plain text.
# The matching keyHash/keyPrefix are derived so apply-side and
# request-side stay in sync.
SMOKE_API_KEY="sbk_warmpool_warmpool_warmpool_x"
SMOKE_KEY_PREFIX="$(printf '%s' "$SMOKE_API_KEY" | cut -c1-8)"
SMOKE_KEY_HASH="sha256:$(printf '%s' "$SMOKE_API_KEY" | openssl dgst -sha256 -r | awk '{print $1}')"

TEMPLATE_NAME="warm-pool-template"
TEAM_KEY_NAME="warm-pool-team-key"
POOL_NAME="warm-pool-smoke"
POOL_LABEL="sandbawks.dev/warm-pool=${POOL_NAME}"

CLAIMED_ID=""

cleanup() {
    local rc=$?
    # Delete the pool first so its ownerReference cascade reaps any warm
    # members we didn't explicitly kill, then the claimed sandbox.
    kubectl -n "$NAMESPACE" delete sandboxwarmpool "$POOL_NAME" \
        --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [[ -n "$CLAIMED_ID" ]]; then
        kubectl -n "$NAMESPACE" delete sandbox "$CLAIMED_ID" \
            --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
    # Sweep any warm members left behind by a mid-run failure.
    kubectl -n "$NAMESPACE" delete sandbox -l "$POOL_LABEL" \
        --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if (( rc != 0 )); then
        echo "warm-pool-smoke: FAILED (exit $rc) — diagnostics follow" >&2
        kubectl -n "$NAMESPACE" get sandboxwarmpool -o wide >&2 || true
        kubectl -n "$NAMESPACE" get sandbox -l "$POOL_LABEL" -o wide >&2 || true
        kubectl -n "$NAMESPACE" get sandbox -o wide >&2 || true
        kubectl -n "$NAMESPACE" logs deploy/sandbawks-controller --tail=120 >&2 || true
        kubectl -n "$NAMESPACE" logs deploy/sandbawks-api --tail=80 >&2 || true
    fi
    exit "$rc"
}
trap cleanup EXIT

echo "warm-pool-smoke: pre-flight checks"
for bin in curl kubectl jq openssl; do
    command -v "$bin" >/dev/null || { echo "warm-pool-smoke: $bin missing" >&2; exit 1; }
done

echo "warm-pool-smoke: waiting for sandbawks-api Deployment to be Available"
kubectl -n "$NAMESPACE" wait deploy/sandbawks-api \
    --for=condition=Available --timeout=120s

echo "warm-pool-smoke: probing $API_BASE/readyz"
for attempt in $(seq 1 30); do
    if curl -fsS -o /dev/null "$API_BASE/readyz"; then
        echo "warm-pool-smoke: api reachable (attempt $attempt)"
        break
    fi
    if (( attempt == 30 )); then
        echo "warm-pool-smoke: api never became reachable at $API_BASE" >&2
        exit 1
    fi
    sleep 1
done

echo "warm-pool-smoke: applying SandboxTemplate + TeamAPIKey + SandboxWarmPool"
kubectl apply -f - <<EOF
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: ${TEMPLATE_NAME}
  namespace: ${NAMESPACE}
spec:
  image: docker.io/library/debian:12-slim
  defaultRuntimeClass: gvisor
  defaultResources:
    cpu: "100m"
    memory: "64Mi"
    ephemeralStorage: "256Mi"
  envdImage: ghcr.io/hypotheosis/envd:dev
  defaultTimeoutSeconds: 300
---
apiVersion: sandbawks.dev/v1alpha1
kind: TeamAPIKey
metadata:
  # Post-#583 the TeamAPIKey lives in the SYSTEM namespace — the api lists
  # keys in SANDBAWKS_SYSTEM_NAMESPACE (its own pod namespace), not
  # cluster-wide. This smoke installs the chart into the kubectl-default
  # namespace and runs the tenant in the same namespace, so the system
  # namespace and the tenant namespace are both \${NAMESPACE} here.
  name: ${TEAM_KEY_NAME}
  namespace: ${NAMESPACE}
spec:
  # Tenant namespace this key authorizes (#583). Identity comes from
  # spec.namespace, NOT metadata.namespace; the CRD now requires it.
  namespace: "${NAMESPACE}"
  keyPrefix: "${SMOKE_KEY_PREFIX}"
  keyHash: "${SMOKE_KEY_HASH}"
  scopes:
    - "sandboxes:*"
---
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxWarmPool
metadata:
  name: ${POOL_NAME}
  namespace: ${NAMESPACE}
spec:
  templateRef: ${TEMPLATE_NAME}
  minReady: 1
  maxReady: 2
EOF

# Reaching status.ready==1 proves the controller cold-booted a member and
# is holding it Running and claimable (issue #332). The non-root
# snapshot-write regression (PR #324) is exercised later, when we pause the
# claimed sandbox — see step 7.
echo "warm-pool-smoke: waiting up to ${READY_TIMEOUT}s for status.ready==1"
WARM_NAME=""
for attempt in $(seq 1 "$READY_TIMEOUT"); do
    READY="$(kubectl -n "$NAMESPACE" get sandboxwarmpool "$POOL_NAME" \
        -o jsonpath='{.status.ready}' 2>/dev/null || true)"
    if [[ "$READY" == "1" ]]; then
        # Grab the Running member's name — that's what a claim must return.
        WARM_NAME="$(kubectl -n "$NAMESPACE" get sandbox -l "$POOL_LABEL" \
            -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' \
            2>/dev/null | awk '{print $1}')"
        if [[ -n "$WARM_NAME" ]]; then
            echo "warm-pool-smoke: pool ready, warm sandbox=${WARM_NAME} (after ${attempt}s)"
            break
        fi
    fi
    sleep 1
done
if [[ -z "$WARM_NAME" ]]; then
    echo "warm-pool-smoke: pool never reported a ready Running member within ${READY_TIMEOUT}s" >&2
    kubectl -n "$NAMESPACE" get sandboxwarmpool "$POOL_NAME" -o yaml >&2 || true
    kubectl -n "$NAMESPACE" get sandbox -l "$POOL_LABEL" -o yaml >&2 || true
    exit 1
fi

# Guard the runtime-class regression directly: a warm sandbox MUST inherit
# the template's gVisor runtimeClassName, or a later pause's `runsc
# checkpoint` runs against a runc container and fails. The post-claim pause
# in step 7 would surface that, but assert it up front for a clear signal.
WARM_RTC="$(kubectl -n "$NAMESPACE" get sandbox "$WARM_NAME" \
    -o jsonpath='{.spec.runtimeClassName}' 2>/dev/null || true)"
if [[ "$WARM_RTC" != "gvisor" ]]; then
    echo "warm-pool-smoke: warm sandbox runtimeClassName='${WARM_RTC}', expected 'gvisor'" >&2
    echo "warm-pool-smoke: warm sandboxes must inherit the template's defaultRuntimeClass" >&2
    exit 1
fi
echo "warm-pool-smoke: warm sandbox runtimeClassName=gvisor (runtime-class inherited)"

# Phase 3 (#442): the claim POST below impersonates the per-namespace
# `sandbawks-tenant` SA, which the controller's tenant_rbac reconciler
# provisions asynchronously after observing the TeamAPIKey. Gate on the
# effective permission (cluster-admin CI kubeconfig may impersonate) so the
# claim doesn't 502 on a not-yet-propagated RBAC race.
TENANT_SA="system:serviceaccount:${NAMESPACE}:sandbawks-tenant"
echo "warm-pool-smoke: waiting up to 60s for tenant RBAC (${TENANT_SA}) from the controller"
rbac_ready=""
for attempt in $(seq 1 60); do
    if kubectl auth can-i create sandboxes.sandbawks.dev \
        -n "${NAMESPACE}" --as="${TENANT_SA}" >/dev/null 2>&1; then
        rbac_ready=1
        echo "warm-pool-smoke: tenant RBAC ready (attempt ${attempt})"
        break
    fi
    sleep 1
done
if [ -z "${rbac_ready}" ]; then
    echo "warm-pool-smoke: FAILED — tenant RBAC for ${TENANT_SA} not provisioned within 60s (#442)" >&2
    exit 1
fi

echo "warm-pool-smoke: POST /sandboxes (templateID=${TEMPLATE_NAME}) — expect a claim"
CREATE_RESP="$(curl -fsS -X POST "$API_BASE/sandboxes" \
    -H "X-API-Key: $SMOKE_API_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"templateID\":\"${TEMPLATE_NAME}\",\"timeout\":300}")"
CLAIMED_ID="$(printf '%s' "$CREATE_RESP" | jq -er '.sandboxID')"
echo "warm-pool-smoke: create returned sandboxID=${CLAIMED_ID}"

if [[ "$CLAIMED_ID" != "$WARM_NAME" ]]; then
    echo "warm-pool-smoke: claim FAILED — sandboxID '${CLAIMED_ID}' != warm sandbox '${WARM_NAME}'" >&2
    echo "warm-pool-smoke: a cold-boot id means the warm-pool fast path did not fire" >&2
    exit 1
fi
echo "warm-pool-smoke: claim CONFIRMED — gateway adopted the warm sandbox's name"

# A claim is a reassignment, not a resume: the member was already Running,
# so it must report Running essentially immediately. Poll briefly to absorb
# the overrides patch's reconcile rather than asserting on the first read.
echo "warm-pool-smoke: confirming ${CLAIMED_ID} is Running (claimed live, no resume)"
PHASE=""
for attempt in $(seq 1 "$RESUME_TIMEOUT"); do
    PHASE="$(kubectl -n "$NAMESPACE" get sandbox "$CLAIMED_ID" \
        -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "$PHASE" == "Running" ]]; then
        echo "warm-pool-smoke: claimed sandbox Running (after ${attempt}s)"
        break
    fi
    sleep 1
done
if [[ "$PHASE" != "Running" ]]; then
    echo "warm-pool-smoke: claimed sandbox stuck in phase '${PHASE}' after ${RESUME_TIMEOUT}s" >&2
    kubectl -n "$NAMESPACE" get sandbox "$CLAIMED_ID" -o yaml >&2 || true
    exit 1
fi

# Pause the claimed sandbox to exercise the real `runsc checkpoint` → S3
# under gVisor. This is the non-root snapshot-write guard (PR #324): when
# envd's `--state-file` default lived under root-owned `/var/lib/envd`, a
# workload image running envd as a non-root `USER` hit `EACCES`, envd
# exited non-zero, and the sandbox never reached `Paused`. It also proves
# end-to-end that the claimed member inherited a working gVisor
# runtimeClassName — the reason that field is load-bearing now that members
# are no longer pre-paused. (`cargo test`'s tempdir cannot cover either.)
echo "warm-pool-smoke: POST /sandboxes/${CLAIMED_ID}/pause"
PAUSE_STATUS="$(curl -fsS -o /dev/null -w '%{http_code}' \
    -X POST "$API_BASE/sandboxes/${CLAIMED_ID}/pause" \
    -H "X-API-Key: $SMOKE_API_KEY")"
if [[ "$PAUSE_STATUS" != "200" ]]; then
    echo "warm-pool-smoke: pause returned HTTP ${PAUSE_STATUS}, expected 200" >&2
    exit 1
fi

echo "warm-pool-smoke: waiting up to ${PAUSE_TIMEOUT}s for ${CLAIMED_ID} to reach Paused"
PHASE=""
for attempt in $(seq 1 "$PAUSE_TIMEOUT"); do
    PHASE="$(kubectl -n "$NAMESPACE" get sandbox "$CLAIMED_ID" \
        -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "$PHASE" == "Paused" ]]; then
        echo "warm-pool-smoke: claimed sandbox Paused — checkpoint wrote to S3 (after ${attempt}s)"
        break
    fi
    sleep 1
done
if [[ "$PHASE" != "Paused" ]]; then
    echo "warm-pool-smoke: claimed sandbox stuck in phase '${PHASE}' after ${PAUSE_TIMEOUT}s" >&2
    echo "warm-pool-smoke: a non-Paused result here can mean the non-root snapshot-write regression (PR #324) returned" >&2
    kubectl -n "$NAMESPACE" get sandbox "$CLAIMED_ID" -o yaml >&2 || true
    exit 1
fi

echo "warm-pool-smoke: POST /sandboxes/${CLAIMED_ID}/kill"
KILL_STATUS="$(curl -fsS -o /dev/null -w '%{http_code}' \
    -X POST "$API_BASE/sandboxes/${CLAIMED_ID}/kill" \
    -H "X-API-Key: $SMOKE_API_KEY")"
if [[ "$KILL_STATUS" != "204" ]]; then
    echo "warm-pool-smoke: kill returned HTTP ${KILL_STATUS}, expected 204" >&2
    exit 1
fi
CLAIMED_ID=""

echo "warm-pool-smoke: SUCCESS — pool warmed (Running), claim returned the warm sandbox, pause green"
