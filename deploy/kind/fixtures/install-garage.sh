#!/usr/bin/env bash
# Plan 05 — install Garage (https://garagehq.deuxfleurs.fr) into the
# kind smoke cluster as the S3 backend the `sandbawks-node` DaemonSet
# streams snapshots into. Replaces an earlier MinIO-via-bitnami fixture
# that was repeatedly flaking on Docker Hub pulls (and that MinIO itself
# is now an unmaintained codebase + bitnami chart trust evaporated post
# Broadcom).
#
# Garage is a single ~30 MiB static binary, written in Rust, with both
# server and CLI in the same executable — so `kubectl exec garage` does
# all the bootstrap work below without a separate client image.
#
# Image source: upstream `docker.io/dxflrs/garage:v1.0.1`. Set
# SANDBAWKS_GARAGE_IMAGE to pull from a mirror instead.
#
# Invoked by `SANDBAWKS_KIND_GVISOR=1 just cluster-up` and by
# `.github/workflows/kind-smoke.yaml` after the kind cluster is up + the
# CRDs are applied + before `helm install sandbawks`. Idempotent — safe
# to re-run against an existing cluster:
#   - `kubectl apply` upserts the ConfigMap / Deployment / Service.
#   - `garage layout assign` is a no-op when the layout already has the
#     node assigned at the same capacity; `garage layout apply --version`
#     errors only on stale version mismatches, which we tolerate.
#   - `garage bucket create` errors with `BucketAlreadyExists` on re-run,
#     which is treated as success.
#   - `garage key create` is re-runnable when given an existing name;
#     we re-read the existing key's ID+Secret via `garage key info`.
#   - `kubectl apply` on the Secret overwrites it with whatever the
#     current key creds are, so the DaemonSet stays in sync.
#
# Requires: bash, openssl, kubectl. All three are on the CI runners and
# on any developer workstation that can already run `just cluster-up`.

set -euo pipefail

NAMESPACE="${SANDBAWKS_GARAGE_NAMESPACE:-default}"
BUCKET="${SANDBAWKS_S3_BUCKET:-sandbawks-snapshots}"
SECRET_NAME="${SANDBAWKS_SNAPSHOT_SECRET:-sandbawks-snapshot-store}"
KEY_NAME="${SANDBAWKS_GARAGE_KEY_NAME:-sandbawks-key}"

# Pinned garage image. Bump deliberately when validated.
GARAGE_IMAGE="${SANDBAWKS_GARAGE_IMAGE:-docker.io/dxflrs/garage:v1.0.1}"

# Random tokens. The rpc_secret protects the cluster's internal RPC
# (irrelevant in a single-node setup but garage refuses to start without
# it); admin_token gates the /admin HTTP API (which we don't expose
# here, but garage requires the token to be set when [admin] is
# configured). 32 hex chars = 128 bits, well above garage's minimums.
RPC_SECRET="${SANDBAWKS_GARAGE_RPC_SECRET:-$(openssl rand -hex 32)}"
ADMIN_TOKEN="${SANDBAWKS_GARAGE_ADMIN_TOKEN:-$(openssl rand -hex 32)}"

echo "install-garage: pre-flight checks"
command -v kubectl >/dev/null || { echo "install-garage: kubectl missing" >&2; exit 1; }
command -v openssl >/dev/null || { echo "install-garage: openssl missing" >&2; exit 1; }

echo "install-garage: applying ConfigMap/garage-config (namespace=${NAMESPACE})"
# Single-node config: replication_factor=1 + empty bootstrap_peers so
# garage doesn't try to reach other nodes. `db_engine = "lmdb"` is the
# smallest-footprint backend and the upstream default for v1+.
# `s3_region = "us-east-1"` matches the chart's default
# `snapshotStore.region` so rust-s3 signs requests with the same region
# the server expects.
kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: garage-config
data:
  garage.toml: |
    metadata_dir = "/var/lib/garage/meta"
    data_dir = "/var/lib/garage/data"
    db_engine = "lmdb"
    replication_factor = 1
    rpc_bind_addr = "[::]:3901"
    rpc_secret = "${RPC_SECRET}"
    bootstrap_peers = []
    [s3_api]
    s3_region = "us-east-1"
    api_bind_addr = "[::]:3900"
    root_domain = ".s3.garage"
    [admin]
    api_bind_addr = "[::]:3903"
    admin_token = "${ADMIN_TOKEN}"
EOF

echo "install-garage: applying Deployment/garage (image=${GARAGE_IMAGE})"
# `emptyDir` for data: CI is ephemeral and a kind cluster gets deleted
# between runs, so persisting garage state across pod restarts in the
# same cluster is enough. For local dev where `just cluster-up` survives
# restarts, the same emptyDir still survives Pod restarts inside the
# same cluster lifetime — losing it on a `cluster-down` is fine.
#
# `command: ["/garage"]` + `args: ["server"]` is the upstream-recommended
# invocation. The binary auto-detects the config at `/etc/garage.toml`
# (the default), which is where the ConfigMap is mounted via `subPath`.
kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: garage
  labels:
    app.kubernetes.io/name: garage
    app.kubernetes.io/component: snapshot-store
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: garage
  template:
    metadata:
      labels:
        app.kubernetes.io/name: garage
    spec:
      containers:
        - name: garage
          image: ${GARAGE_IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["/garage"]
          args: ["server"]
          ports:
            - { name: s3,    containerPort: 3900 }
            - { name: rpc,   containerPort: 3901 }
            - { name: admin, containerPort: 3903 }
          readinessProbe:
            tcpSocket: { port: 3900 }
            initialDelaySeconds: 2
            periodSeconds: 2
          volumeMounts:
            - name: config
              mountPath: /etc/garage.toml
              subPath: garage.toml
            - name: data
              mountPath: /var/lib/garage
          resources:
            # CPU bumped (perf investigation): the GiB pause/resume bench showed
            # garage processes bytes at a fixed ~43 MB/s regardless of client
            # concurrency (N) or chunk size — the signature of byte-proportional
            # work (content-addressed block hashing + LMDB) pinned to a single
            # core. The old 1-core *limit* was this fixture's, so it throttled
            # the local A/B. limit 8 ≥ the default client PARALLELISM (8) so each
            # in-flight PUT can get a core (CI build host has 32).
            #
            # request 4 (not 1): the *request* is the CFS share, i.e. garage's
            # *guaranteed* slice under contention — and on this node garage
            # competes with our zstd/hashing + the gVisor sandbox during each
            # pause. request 1 would only guarantee ~1 core if the node briefly
            # saturates, capping garage below its 8 limit and muddying the
            # signal. 4 guarantees a strong share; raise to 8 (== limit, i.e.
            # Guaranteed QoS) for zero contention variability. (Limits set the
            # hard CFS quota; requests only set shares — they don't themselves
            # throttle on an idle node.)
            requests: { cpu: "4",   memory: "256Mi" }
            limits:   { cpu: "8",   memory: "2Gi" }
      volumes:
        - name: config
          configMap:
            name: garage-config
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: garage
  labels:
    app.kubernetes.io/name: garage
spec:
  selector:
    app.kubernetes.io/name: garage
  ports:
    - { name: s3,    port: 3900, targetPort: 3900 }
    - { name: rpc,   port: 3901, targetPort: 3901 }
    - { name: admin, port: 3903, targetPort: 3903 }
EOF

echo "install-garage: waiting for Deployment/garage to be Available"
kubectl -n "${NAMESPACE}" rollout status deploy/garage --timeout=2m

# Re-read the Pod name AFTER rollout — a config change earlier in the
# run would have replaced the previous Pod.
GARAGE_POD="$(kubectl -n "${NAMESPACE}" get pod \
    -l app.kubernetes.io/name=garage \
    -o jsonpath='{.items[0].metadata.name}')"
echo "install-garage: bootstrapping via pod/${GARAGE_POD}"

# `garage node id` emits the full `<short-id>@<rpc_addr>` form on stdout
# along with a friendly banner on stderr; `-q` keeps stderr quiet but
# stdout still has the full form. Strip the `@host:port` suffix because
# `garage layout assign` accepts only the short hex.
NODE_ID="$(kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
    /garage node id -q | cut -d@ -f1 | tr -d '[:space:]')"
if [[ -z "${NODE_ID}" ]]; then
    echo "install-garage: could not read garage node id" >&2
    exit 1
fi
echo "install-garage: node_id=${NODE_ID}"

# Layout assign + apply. `assign` is idempotent on identical params;
# `apply --version N` requires the *next* layout version, so we read
# the current version + 1. On a fresh cluster the layout starts at
# version 0 and we apply version 1; on a re-run the layout is already
# at version >=1 and assign-with-same-params is a no-op (so we skip the
# `apply` if `garage layout show` reports the staged layout matches the
# applied one).
echo "install-garage: layout assign"
kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
    /garage layout assign -z dc1 -c 1G "${NODE_ID}" >/dev/null 2>&1 || true

# `layout apply --version` will fail on re-run because there's nothing
# new to apply; suppress that specific error path. Read the current
# version, increment, try to apply, swallow "no changes" failures.
NEXT_VERSION="$(kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
    /garage layout show 2>/dev/null | awk '/Current cluster layout version:/{v=$NF} END{if (v!="") print v + 1}')"
NEXT_VERSION="${NEXT_VERSION:-1}"
echo "install-garage: layout apply --version ${NEXT_VERSION}"
kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
    /garage layout apply --version "${NEXT_VERSION}" >/dev/null 2>&1 || true

echo "install-garage: bucket create ${BUCKET}"
kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
    /garage bucket create "${BUCKET}" >/dev/null 2>&1 || true

echo "install-garage: key create ${KEY_NAME}"
# `garage key create` errors with "key already exists" on re-run; in
# that case we re-read the existing key's secret via `key info -s`
# (the `-s` flag prints the secret in machine-readable form alongside
# the key id; without it the secret is masked).
KEY_OUTPUT="$(kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
    /garage key create "${KEY_NAME}" 2>&1 || true)"
if ! printf '%s\n' "${KEY_OUTPUT}" | grep -q 'Secret key:'; then
    KEY_OUTPUT="$(kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
        /garage key info "${KEY_NAME}" -s)"
fi
ACCESS_KEY="$(printf '%s\n' "${KEY_OUTPUT}" | awk -F': *' '/^Key ID:/{print $2; exit}' | tr -d '[:space:]')"
SECRET_KEY="$(printf '%s\n' "${KEY_OUTPUT}" | awk -F': *' '/^Secret key:/{print $2; exit}' | tr -d '[:space:]')"
if [[ -z "${ACCESS_KEY}" || -z "${SECRET_KEY}" ]]; then
    echo "install-garage: could not parse Key ID / Secret key from garage output" >&2
    printf '%s\n' "${KEY_OUTPUT}" >&2
    exit 1
fi

echo "install-garage: bucket allow ${KEY_NAME} on ${BUCKET}"
kubectl -n "${NAMESPACE}" exec "${GARAGE_POD}" -- \
    /garage bucket allow --read --write --owner "${BUCKET}" --key "${KEY_NAME}" \
    >/dev/null

echo "install-garage: writing Secret/${SECRET_NAME}"
# `helm.sh/resource-policy: keep` mirrors the chart-managed
# snapshot-store Secret's annotation so a chart `helm uninstall` doesn't
# nuke this fixture-managed Secret.
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

GARAGE_ENDPOINT="http://garage.${NAMESPACE}.svc.cluster.local:3900"

echo "install-garage: done"
echo "  endpoint: ${GARAGE_ENDPOINT}"
echo "  bucket:   ${BUCKET}"
echo "  secret:   ${SECRET_NAME} (namespace ${NAMESPACE})"
