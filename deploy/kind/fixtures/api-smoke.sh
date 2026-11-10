#!/usr/bin/env bash
# Plan 04 T15 — API gateway smoke test driven from the host against a
# `just cluster-up` kind cluster.
#
# Walks the full E2B-compatible API lifecycle against real Pods + real
# envd on the cluster's default containerd runtime:
#
#   1. Apply a `SandboxTemplate` + `TeamAPIKey` CR keyed to a known key.
#   2. POST /sandboxes              -> 201, parse sandboxID
#   3. wait for status.envdAddress  -> non-empty (60s budget)
#   4. POST /sandboxes/{id}/pause   -> 200, spec.desiredPhase=Paused
#   5. POST /sandboxes/{id}/connect -> 200, parse envdURL
#   6. POST /files/mint (PUT)       -> mint upload URL
#      PUT to that URL               -> 204
#   7. GET /files/{id}/list?path=/tmp -> uploaded file listed
#   8. POST /files/mint (GET)       -> mint download URL
#      GET that URL                  -> body matches local file
#   9. POST /sandboxes/{id}/kill    -> 204
#
# The script is idempotent / re-runnable: any leftover CRs from a
# previous run are deleted before the new run starts.
#
# Requires: bash, curl, kubectl, jq, openssl. The kubectl context must
# already point at the `kind-sandbawks-smoke` cluster brought up by
# `just cluster-up`, and the api Deployment must be reachable at
# http://localhost:8080 (the `kubectl port-forward` step in cluster-up
# arranges this).

set -euo pipefail

API_BASE="${SANDBAWKS_API_BASE:-http://localhost:8080}"
NAMESPACE="${SANDBAWKS_API_NAMESPACE:-default}"

# Known API key for the smoke. The matching `keyHash` / `keyPrefix` are
# computed below from this literal so the apply-side and the request-side
# stay in sync. NOT A SECRET — published in plain text in this repo.
SMOKE_API_KEY="sbk_smoke_smoke_smoke_smoke_smoke"
SMOKE_KEY_PREFIX="$(printf '%s' "$SMOKE_API_KEY" | cut -c1-8)"
SMOKE_KEY_HASH="sha256:$(printf '%s' "$SMOKE_API_KEY" | openssl dgst -sha256 -r | awk '{print $1}')"

TEMPLATE_NAME="api-smoke-template"
TEAM_KEY_NAME="api-smoke-team-key"

# Tenant-namespace case (issue #13): a second key whose spec.namespace is a
# namespace other than the one the chart runs in. Exercises the envd proxy
# and the signed /files URLs for a sandbox outside api.defaultNamespace.
TENANT_NAMESPACE="${SANDBAWKS_API_TENANT_NAMESPACE:-api-smoke-tenant}"
TENANT_API_KEY="sbk_tenant_tenant_tenant_tenant_t"
TENANT_KEY_PREFIX="$(printf '%s' "$TENANT_API_KEY" | cut -c1-8)"
TENANT_KEY_HASH="sha256:$(printf '%s' "$TENANT_API_KEY" | openssl dgst -sha256 -r | awk '{print $1}')"

LOCAL_PAYLOAD="$(mktemp)"
DOWNLOADED_PAYLOAD="$(mktemp)"
SANDBOX_ID=""
TENANT_SANDBOX_ID=""

cleanup() {
    local rc=$?
    if [[ -n "$SANDBOX_ID" ]]; then
        kubectl -n "$NAMESPACE" delete sandbox "$SANDBOX_ID" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
    if [[ -n "$TENANT_SANDBOX_ID" ]]; then
        kubectl -n "$TENANT_NAMESPACE" delete sandbox "$TENANT_SANDBOX_ID" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
    rm -f "$LOCAL_PAYLOAD" "$DOWNLOADED_PAYLOAD"
    if (( rc != 0 )); then
        echo "api-smoke: FAILED (exit $rc) — diagnostics follow" >&2
        kubectl -n "$NAMESPACE" get sandbox -o wide >&2 || true
        kubectl -n "$NAMESPACE" get pods -o wide >&2 || true
        kubectl -n "$TENANT_NAMESPACE" get sandbox,pods -o wide >&2 || true
        kubectl -n "$NAMESPACE" logs deploy/sandbawks-api --tail=80 >&2 || true
        kubectl -n "$NAMESPACE" logs deploy/sandbawks-controller --tail=80 >&2 || true
    fi
    exit "$rc"
}
trap cleanup EXIT

echo "api-smoke: pre-flight checks"
command -v curl >/dev/null   || { echo "api-smoke: curl missing" >&2; exit 1; }
command -v kubectl >/dev/null || { echo "api-smoke: kubectl missing" >&2; exit 1; }
command -v jq >/dev/null     || { echo "api-smoke: jq missing" >&2; exit 1; }
command -v openssl >/dev/null || { echo "api-smoke: openssl missing" >&2; exit 1; }

echo "api-smoke: waiting for sandbawks-api Deployment to be Available"
kubectl -n "$NAMESPACE" wait deploy/sandbawks-api \
    --for=condition=Available --timeout=120s

echo "api-smoke: probing $API_BASE/readyz"
for attempt in $(seq 1 30); do
    if curl -fsS -o /dev/null "$API_BASE/readyz"; then
        echo "api-smoke: api reachable (attempt $attempt)"
        break
    fi
    if (( attempt == 30 )); then
        echo "api-smoke: api never became reachable at $API_BASE" >&2
        exit 1
    fi
    sleep 1
done

echo "api-smoke: applying SandboxTemplate + TeamAPIKey fixtures"
# Apply both in one heredoc so a re-run picks up edits without leaving
# orphan resources behind. `kubectl apply` is idempotent on the names.
#
# The heredoc delimiter is UNQUOTED (<<EOF, not <<'EOF') on purpose: the
# body relies on ${TEMPLATE_NAME}/${NAMESPACE}/${SMOKE_KEY_*} expansion.
# A consequence is that backticks and $(...) in the body are evaluated as
# command substitutions by the shell — so any literal backtick in the YAML
# (even inside a comment) MUST be backslash-escaped (\`) or it runs as a
# command and leaks "/bin/bash: -c: option requires an argument" to stderr.
kubectl apply -f - <<EOF
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: ${TEMPLATE_NAME}
  namespace: ${NAMESPACE}
spec:
  # The e2b SDK's commands.run wraps every command as \`/bin/bash -l -c\`,
  # so the workload image MUST provide /bin/bash (issue #265 — cold start
  # now honors spec.image; busybox has only /bin/sh and would 500 on the
  # SDK-compat exec tests). envd itself is node-staged and mounted in
  # read-only, so it is NOT part of this image. debian:12-slim is the
  # minimal Debian base that ships bash + coreutils.
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
EOF

# Phase 3 (#442): the api impersonates the per-namespace `sandbawks-tenant`
# ServiceAccount for tenant ops, and that SA + Role + RoleBinding are
# provisioned asynchronously by the controller's tenant_rbac reconciler when it
# observes the TeamAPIKey above. Until that RBAC exists AND propagates, the
# impersonated calls (template GET, warm-pool list, sandbox create) 403 and the
# gateway returns 502. Gate the POST on the effective permission via
# `kubectl auth can-i --as=<tenant-SA>` (the CI kubeconfig is cluster-admin, so
# it may impersonate).
TENANT_SA="system:serviceaccount:${NAMESPACE}:sandbawks-tenant"
echo "api-smoke: waiting up to 60s for tenant RBAC (${TENANT_SA}) from the controller"
rbac_ready=""
for attempt in $(seq 1 60); do
    if kubectl auth can-i create sandboxes.sandbawks.dev \
        -n "${NAMESPACE}" --as="${TENANT_SA}" >/dev/null 2>&1; then
        rbac_ready=1
        echo "api-smoke: tenant RBAC ready (attempt ${attempt})"
        break
    fi
    sleep 1
done
if [ -z "${rbac_ready}" ]; then
    echo "api-smoke: FAILED — tenant RBAC for ${TENANT_SA} not provisioned within 60s (controller tenant_rbac reconciler, #442)" >&2
    exit 1
fi

echo "api-smoke: POST /sandboxes (templateID=${TEMPLATE_NAME})"
CREATE_RESP="$(curl -fsS -X POST "$API_BASE/sandboxes" \
    -H "X-API-Key: $SMOKE_API_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"templateID\":\"${TEMPLATE_NAME}\",\"timeout\":300}")"
SANDBOX_ID="$(printf '%s' "$CREATE_RESP" | jq -er '.sandboxID')"
echo "api-smoke: created sandbox ${SANDBOX_ID}"

echo "api-smoke: waiting up to 60s for status.envdAddress"
ENVD_ADDR=""
for attempt in $(seq 1 60); do
    ENVD_ADDR="$(kubectl -n "$NAMESPACE" get sandbox "$SANDBOX_ID" \
        -o jsonpath='{.status.envdAddress}' 2>/dev/null || true)"
    if [[ -n "$ENVD_ADDR" ]]; then
        echo "api-smoke: envdAddress=${ENVD_ADDR} (after ${attempt}s)"
        break
    fi
    sleep 1
done
if [[ -z "$ENVD_ADDR" ]]; then
    echo "api-smoke: envdAddress never populated within 60s" >&2
    kubectl -n "$NAMESPACE" get sandbox "$SANDBOX_ID" -o yaml >&2 || true
    exit 1
fi

# NOTE: pause + connect intentionally moved out of the api-smoke since
# Plan 05 T15 wired SandboxTemplate.defaultRuntimeClass into the
# created Sandbox. The fixture's template sets `gvisor`, so a real
# `pause` now triggers a `runsc checkpoint` that exits the container
# (default runsc behavior) — and the file upload steps below would
# then 502 against a Completed Pod. The full pause/resume round-trip
# (real `runsc checkpoint` → S3 → delete Pod → restore → file
# round-trip) lives in `pause-resume-smoke.sh`. The api-smoke now
# focuses on the steady-state HTTP surface: lifecycle create,
# signed-URL file upload/list/download, kill.

# Generate a payload with non-trivial bytes so a silent truncation in
# either direction fails the diff. 64 KiB pulled from /dev/urandom keeps
# the smoke fast while still covering the multi-chunk-read code path.
dd if=/dev/urandom of="$LOCAL_PAYLOAD" bs=1024 count=64 status=none

echo "api-smoke: POST /files/mint (PUT) → upload URL"
MINT_PUT_RESP="$(curl -fsS -X POST "$API_BASE/files/mint" \
    -H "X-API-Key: $SMOKE_API_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"sandbox_id\":\"${SANDBOX_ID}\",\"path\":\"/tmp/smoke.bin\",\"method\":\"PUT\",\"ttl\":300}")"
UPLOAD_URL="$(printf '%s' "$MINT_PUT_RESP" | jq -er '.url')"
echo "api-smoke: PUT ${API_BASE}${UPLOAD_URL}"
UPLOAD_STATUS="$(curl -fsS -o /dev/null -w '%{http_code}' \
    -X PUT --data-binary "@${LOCAL_PAYLOAD}" \
    "${API_BASE}${UPLOAD_URL}")"
if [[ "$UPLOAD_STATUS" != "204" ]]; then
    echo "api-smoke: upload returned HTTP ${UPLOAD_STATUS}, expected 204" >&2
    exit 1
fi

echo "api-smoke: GET /files/${SANDBOX_ID}/list?path=/tmp"
LIST_RESP="$(curl -fsS "$API_BASE/files/${SANDBOX_ID}/list?path=/tmp" \
    -H "X-API-Key: $SMOKE_API_KEY")"
if ! printf '%s' "$LIST_RESP" | jq -e '.entries[] | select(.name=="smoke.bin")' >/dev/null; then
    echo "api-smoke: uploaded file 'smoke.bin' not listed in /tmp" >&2
    echo "api-smoke: list response was:" >&2
    printf '%s\n' "$LIST_RESP" >&2
    exit 1
fi

echo "api-smoke: POST /files/mint (GET) → download URL"
MINT_GET_RESP="$(curl -fsS -X POST "$API_BASE/files/mint" \
    -H "X-API-Key: $SMOKE_API_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"sandbox_id\":\"${SANDBOX_ID}\",\"path\":\"/tmp/smoke.bin\",\"method\":\"GET\",\"ttl\":300}")"
DOWNLOAD_URL="$(printf '%s' "$MINT_GET_RESP" | jq -er '.url')"
echo "api-smoke: GET ${API_BASE}${DOWNLOAD_URL}"
curl -fsS -o "$DOWNLOADED_PAYLOAD" "${API_BASE}${DOWNLOAD_URL}"
if ! cmp -s "$LOCAL_PAYLOAD" "$DOWNLOADED_PAYLOAD"; then
    echo "api-smoke: downloaded payload differs from local source" >&2
    ls -l "$LOCAL_PAYLOAD" "$DOWNLOADED_PAYLOAD" >&2 || true
    exit 1
fi
echo "api-smoke: file round-trip checksum matches"

echo "api-smoke: POST /sandboxes/${SANDBOX_ID}/kill"
KILL_STATUS="$(curl -fsS -o /dev/null -w '%{http_code}' \
    -X POST "$API_BASE/sandboxes/${SANDBOX_ID}/kill" \
    -H "X-API-Key: $SMOKE_API_KEY")"
if [[ "$KILL_STATUS" != "204" ]]; then
    echo "api-smoke: kill returned HTTP ${KILL_STATUS}, expected 204" >&2
    exit 1
fi
# Don't double-delete in cleanup() — the CR is gone (or background-deleting).
SANDBOX_ID=""

# ---------------------------------------------------------------------------
# Tenant namespace (issue #13). The key lives in the system namespace and
# names TENANT_NAMESPACE in spec.namespace, so its sandboxes live there, not in
# the gateway's default namespace. Before #13 every envd-proxied call and
# signed /files URL for such a sandbox answered sandbox_not_found.
# ---------------------------------------------------------------------------
echo "api-smoke: tenant namespace ${TENANT_NAMESPACE}: applying fixtures"
kubectl create namespace "$TENANT_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - <<EOF
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: ${TEMPLATE_NAME}
  namespace: ${TENANT_NAMESPACE}
spec:
  image: docker.io/library/debian:12-slim
  defaultRuntimeClass: gvisor
  defaultResources:
    cpu: "100m"
    memory: "64Mi"
    ephemeralStorage: "256Mi"
  defaultTimeoutSeconds: 300
---
apiVersion: sandbawks.dev/v1alpha1
kind: TeamAPIKey
metadata:
  name: api-smoke-tenant-key
  namespace: ${NAMESPACE}
spec:
  namespace: "${TENANT_NAMESPACE}"
  keyPrefix: "${TENANT_KEY_PREFIX}"
  keyHash: "${TENANT_KEY_HASH}"
  scopes:
    - "sandboxes:*"
EOF

TENANT_SA="system:serviceaccount:${TENANT_NAMESPACE}:sandbawks-tenant"
echo "api-smoke: waiting up to 60s for tenant RBAC (${TENANT_SA})"
rbac_ready=""
for attempt in $(seq 1 60); do
    if kubectl auth can-i create sandboxes.sandbawks.dev \
        -n "${TENANT_NAMESPACE}" --as="${TENANT_SA}" >/dev/null 2>&1; then
        rbac_ready=1
        break
    fi
    sleep 1
done
if [ -z "${rbac_ready}" ]; then
    echo "api-smoke: FAILED — tenant RBAC for ${TENANT_SA} not provisioned within 60s" >&2
    exit 1
fi

echo "api-smoke: tenant POST /sandboxes"
TENANT_CREATE_RESP="$(curl -fsS -X POST "$API_BASE/sandboxes" \
    -H "X-API-Key: $TENANT_API_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"templateID\":\"${TEMPLATE_NAME}\",\"timeout\":300}")"
TENANT_SANDBOX_ID="$(printf '%s' "$TENANT_CREATE_RESP" | jq -er '.sandboxID')"
TENANT_TOKEN="$(printf '%s' "$TENANT_CREATE_RESP" | jq -er '.envdAccessToken')"
echo "api-smoke: created tenant sandbox ${TENANT_SANDBOX_ID}"
if [[ "$TENANT_TOKEN" != "${TENANT_NAMESPACE}."* ]]; then
    echo "api-smoke: envdAccessToken is not bound to ${TENANT_NAMESPACE}" >&2
    exit 1
fi

echo "api-smoke: waiting up to 60s for the tenant sandbox's status.envdAddress"
ENVD_ADDR=""
for attempt in $(seq 1 60); do
    ENVD_ADDR="$(kubectl -n "$TENANT_NAMESPACE" get sandbox "$TENANT_SANDBOX_ID" \
        -o jsonpath='{.status.envdAddress}' 2>/dev/null || true)"
    [[ -n "$ENVD_ADDR" ]] && break
    sleep 1
done
if [[ -z "$ENVD_ADDR" ]]; then
    echo "api-smoke: tenant envdAddress never populated within 60s" >&2
    exit 1
fi

echo "api-smoke: tenant GET /health through the envd proxy"
HEALTH_STATUS="$(curl -sS -o /dev/null -w '%{http_code}' "$API_BASE/health" \
    -H "E2b-Sandbox-Id: ${TENANT_SANDBOX_ID}" \
    -H "X-Access-Token: ${TENANT_TOKEN}")"
if [[ "$HEALTH_STATUS" != "200" ]]; then
    echo "api-smoke: tenant envd /health returned HTTP ${HEALTH_STATUS}, expected 200" >&2
    exit 1
fi

echo "api-smoke: tenant signed-URL file round-trip"
MINT_PUT_RESP="$(curl -fsS -X POST "$API_BASE/files/mint" \
    -H "X-API-Key: $TENANT_API_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"sandbox_id\":\"${TENANT_SANDBOX_ID}\",\"path\":\"/tmp/tenant.bin\",\"method\":\"PUT\",\"ttl\":300}")"
UPLOAD_STATUS="$(curl -fsS -o /dev/null -w '%{http_code}' \
    -X PUT --data-binary "@${LOCAL_PAYLOAD}" \
    "${API_BASE}$(printf '%s' "$MINT_PUT_RESP" | jq -er '.url')")"
if [[ "$UPLOAD_STATUS" != "204" ]]; then
    echo "api-smoke: tenant upload returned HTTP ${UPLOAD_STATUS}, expected 204" >&2
    exit 1
fi
MINT_GET_RESP="$(curl -fsS -X POST "$API_BASE/files/mint" \
    -H "X-API-Key: $TENANT_API_KEY" \
    -H 'Content-Type: application/json' \
    -d "{\"sandbox_id\":\"${TENANT_SANDBOX_ID}\",\"path\":\"/tmp/tenant.bin\",\"method\":\"GET\",\"ttl\":300}")"
curl -fsS -o "$DOWNLOADED_PAYLOAD" "${API_BASE}$(printf '%s' "$MINT_GET_RESP" | jq -er '.url')"
if ! cmp -s "$LOCAL_PAYLOAD" "$DOWNLOADED_PAYLOAD"; then
    echo "api-smoke: tenant downloaded payload differs from local source" >&2
    exit 1
fi

echo "api-smoke: tenant POST /sandboxes/${TENANT_SANDBOX_ID}/kill"
KILL_STATUS="$(curl -fsS -o /dev/null -w '%{http_code}' \
    -X POST "$API_BASE/sandboxes/${TENANT_SANDBOX_ID}/kill" \
    -H "X-API-Key: $TENANT_API_KEY")"
if [[ "$KILL_STATUS" != "204" ]]; then
    echo "api-smoke: tenant kill returned HTTP ${KILL_STATUS}, expected 204" >&2
    exit 1
fi
TENANT_SANDBOX_ID=""

echo "api-smoke: SUCCESS — full lifecycle + file round-trip green (default and tenant namespace)"
