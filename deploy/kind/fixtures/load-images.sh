#!/usr/bin/env bash
# Pull commit-scoped images from the registry and load them into a named kind
# cluster. Each image is retried up to MAX_ATTEMPTS times so a transient
# docker-daemon store race or a one-off registry hiccup doesn't abort a
# long bench or smoke run.
#
# After every `docker pull` the script asserts that the tag is immediately
# visible to `docker image inspect` before handing it to `kind load`. This
# catches the containerd image-store eventual-consistency window that caused
# the "not present locally" flake in issue #368 (the tag existed in the
# registry layer but the local content-store hadn't indexed it yet).
#
# Using the immutable `:${IMAGE_TAG}` (sha-scoped) tag rather than a shared
# `:dev` prevents cross-job races: smoke and bench run in parallel on the
# same docker daemon; a mutable `:dev` tag could be untagged by the other
# job's `docker rmi -f` between this job's `kind load` resolving the tag
# and its internal `docker save`. The sha-scoped tag is never mutated.
#
# Required environment (exported by the calling workflow step):
#   IMAGE_TAG    — immutable commit-scoped tag (e.g. sha-abc1234deadbeef)
#   CLUSTER_NAME — name of the kind cluster to load into
#
# Optional:
#   MAX_ATTEMPTS — per-image attempt limit (default: 3)
#   RETRY_DELAY  — seconds to wait between retries (default: 2)
#
# Requires: bash, docker, kind.

set -euo pipefail

: "${IMAGE_TAG:?IMAGE_TAG must be set}"
: "${CLUSTER_NAME:?CLUSTER_NAME must be set}"

MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"
RETRY_DELAY="${RETRY_DELAY:-2}"

# Images that both smoke and bench load into kind. The script is fully
# synchronous — all four are loaded before it returns — so order only affects
# which image surfaces a failure first on a bad pull.
IMAGES=(sandbawks-controller sandbawks-api envd sandbawks-node)

load_image() {
    local img="$1"
    local ref="ghcr.io/hypotheosis/${img}:${IMAGE_TAG}"
    local attempt

    for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
        echo "[load-images] ${img}: attempt ${attempt}/${MAX_ATTEMPTS}"

        # Pull the image. On failure, fall through to the retry path rather
        # than exiting immediately — the registry can transiently 503.
        if ! docker pull "${ref}"; then
            echo "[load-images] ${img}: pull failed on attempt ${attempt}" >&2
        # Assert the tag is visible before handing it to kind load. The
        # containerd image store can lag: `docker pull` returns 0 but the
        # content-store hasn't indexed the new tag yet, so `kind load`'s
        # internal `docker save` fails with "not present locally".
        elif ! docker image inspect "${ref}" >/dev/null 2>&1; then
            echo "[load-images] ${img}: inspect failed after pull on attempt ${attempt}" >&2
        elif kind load docker-image --name "${CLUSTER_NAME}" "${ref}"; then
            return 0
        else
            echo "[load-images] ${img}: kind load failed on attempt ${attempt}" >&2
        fi

        if [ "${attempt}" -lt "${MAX_ATTEMPTS}" ]; then
            echo "[load-images] ${img}: retrying in ${RETRY_DELAY}s..." >&2
            sleep "${RETRY_DELAY}"
        fi
    done

    echo "[load-images] ${img}: all ${MAX_ATTEMPTS} attempts failed" >&2
    return 1
}

for img in "${IMAGES[@]}"; do
    load_image "${img}"
done

echo "[load-images] all images loaded into cluster ${CLUSTER_NAME}"
