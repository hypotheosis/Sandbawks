#!/usr/bin/env bash
# Point the kind smoke / bench cluster at the *central* garage cluster as
# the S3 snapshot backend instead of standing up a single-node garage
# in-cluster (see `install-garage.sh`). This exists to A/B the slow
# pause/resume timings: a local single-node garage vs. the real
# multi-node central cluster, so we can tell whether the slowness is an
# artifact of the local fixture (issue: slow pause/resume investigation).
#
# It reuses the SAME S3 backend the build cache already targets — the
# `SCCACHE_*` connection settings + the `AWS_*` credentials the
# Dockerfile cooker stage uses — but writes snapshots into a SEPARATE
# bucket named by `SNAPSHOT_BUCKET`, so build-cache objects and snapshot
# objects never collide in one bucket.
#
# Drop-in alternative to `install-garage.sh` on the smoke/bench path:
# both write the `sandbawks-snapshot-store` Secret the chart wires into
# the node DaemonSet via `envFrom`, and both emit the resolved
# endpoint/bucket/region to `$GITHUB_ENV` so the helm `--set
# snapshotStore.*` flags are identical regardless of which backend is in
# play. Unlike `install-garage.sh` this provisions NOTHING in-cluster
# (no garage Deployment/Service) — the central cluster already exists.
#
# Required environment (CI provides these as repository variables/secrets):
#   SNAPSHOT_BUCKET        — the central bucket dedicated to snapshots.
#   SCCACHE_ENDPOINT       — bare host:port of the central garage S3 API
#                            (no scheme; scheme is derived below).
#   AWS_ACCESS_KEY_ID      — S3 access key for the central cluster.
#   AWS_SECRET_ACCESS_KEY  — S3 secret key for the central cluster.
# Optional:
#   SCCACHE_REGION         — S3 region (default us-east-1, matching the
#                            chart's `snapshotStore.region` default).
#   SCCACHE_S3_USE_SSL     — "true" -> https:// endpoint; anything else
#                            (incl. empty) -> http:// (garage default).
#   SANDBAWKS_GARAGE_NAMESPACE / SANDBAWKS_SNAPSHOT_SECRET — Secret
#                            namespace + name, defaulted to match the
#                            chart + `install-garage.sh`.
#
# Requires: bash, kubectl.

set -euo pipefail

NAMESPACE="${SANDBAWKS_GARAGE_NAMESPACE:-default}"
SECRET_NAME="${SANDBAWKS_SNAPSHOT_SECRET:-sandbawks-snapshot-store}"
BUCKET="${SNAPSHOT_BUCKET:-}"
ENDPOINT_HOST="${SCCACHE_ENDPOINT:-}"
REGION="${SCCACHE_REGION:-us-east-1}"
ACCESS_KEY="${AWS_ACCESS_KEY_ID:-}"
SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-}"

echo "use-central-snapshot-store: pre-flight checks"
command -v kubectl >/dev/null || { echo "use-central-snapshot-store: kubectl missing" >&2; exit 1; }

# Fail fast with an actionable message rather than letting an empty
# endpoint/bucket/credential silently produce a snapshot store the node
# DaemonSet can't talk to (which would surface much later as an opaque
# pause timeout).
missing=()
[[ -n "$BUCKET" ]]        || missing+=("SNAPSHOT_BUCKET")
[[ -n "$ENDPOINT_HOST" ]] || missing+=("SCCACHE_ENDPOINT")
[[ -n "$ACCESS_KEY" ]]    || missing+=("AWS_ACCESS_KEY_ID")
[[ -n "$SECRET_KEY" ]]    || missing+=("AWS_SECRET_ACCESS_KEY")
if (( ${#missing[@]} > 0 )); then
    echo "use-central-snapshot-store: missing required env: ${missing[*]}" >&2
    echo "use-central-snapshot-store: set these (repository variables/secrets) or run install-garage.sh for a local garage instead" >&2
    exit 1
fi

# Derive the endpoint scheme. The central garage S3 API speaks plain
# HTTP by default (same as the in-cluster fixture); `SCCACHE_S3_USE_SSL`
# is the existing knob that flips the build cache to https, so honour it
# here too for parity. `SANDBAWKS_S3_ENDPOINT` (consumed by the node
# binary) expects a full URL, whereas `SCCACHE_ENDPOINT` is bare
# host:port — so we prepend the scheme.
if [[ "${SCCACHE_S3_USE_SSL:-}" == "true" ]]; then
    SCHEME="https"
else
    SCHEME="http"
fi
ENDPOINT_URL="${SCHEME}://${ENDPOINT_HOST}"

echo "use-central-snapshot-store: writing Secret/${SECRET_NAME} (namespace=${NAMESPACE})"
# `helm.sh/resource-policy: keep` mirrors the chart-managed snapshot-store
# Secret's annotation (and `install-garage.sh`'s) so a chart `helm
# uninstall` doesn't nuke this fixture-managed Secret. `stringData` lets
# kubectl base64-encode the raw credentials for us.
kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SECRET_NAME}
  namespace: ${NAMESPACE}
  annotations:
    helm.sh/resource-policy: keep
type: Opaque
stringData:
  SANDBAWKS_S3_ACCESS_KEY: "${ACCESS_KEY}"
  SANDBAWKS_S3_SECRET_KEY: "${SECRET_KEY}"
EOF

# Emit the resolved settings to $GITHUB_ENV so the downstream helm
# install + pause/resume smoke steps consume them uniformly with the
# local-garage path. When run outside Actions (no $GITHUB_ENV), fall back
# to stdout so a developer can eval/inspect them.
emit() {
    if [[ -n "${GITHUB_ENV:-}" ]]; then
        printf '%s\n' "$@" >>"$GITHUB_ENV"
    else
        printf '%s\n' "$@"
    fi
}
emit \
    "SNAPSHOT_STORE_ENDPOINT=${ENDPOINT_URL}" \
    "SNAPSHOT_STORE_BUCKET=${BUCKET}" \
    "SNAPSHOT_STORE_REGION=${REGION}"

echo "use-central-snapshot-store: done"
echo "  endpoint: ${ENDPOINT_URL}"
echo "  bucket:   ${BUCKET}"
echo "  region:   ${REGION}"
echo "  secret:   ${SECRET_NAME} (namespace ${NAMESPACE})"
