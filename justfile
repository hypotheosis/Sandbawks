# sandbawks workspace task runner.
#
# Recipes here are the canonical entry points used by CI and by the
# issue-to-PR workflow. Keep them thin wrappers over cargo so
# the actual toolchain config stays in Cargo.toml / .cargo/config.toml.

default:
    @just --list

# Format the entire workspace in-place.
fmt:
    cargo fmt --all

# Verify formatting without modifying files (CI mode).
fmt-check:
    cargo fmt --all -- --check

# Lint the entire workspace; warnings are errors.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Run all workspace tests.
test:
    cargo test --workspace --all-targets

# Full pre-push gate: format check, lint, test.
#
# Intentionally Rust-only: the Go shim subtree has a separate toolchain
# and its own gate (`just shim-check`). CI runs the two as independent,
# path-filtered jobs (`rust` and `go` in .github/workflows/ci.yml), so a
# Rust-only contributor without `go` installed can still run `just check`.
# If you touched shim/, run `just shim-check` too before pushing.
check: fmt-check clippy test

# Build the containerd-shim-sandbawks-v1 Go binary (Plan 07 T1, issue
# #205). This is the one Go binary in an otherwise all-Rust repo — see
# shim/README.md for why. It is NOT part of the Cargo workspace, so it has
# its own toolchain (`go`) and dependency manifest (`shim/go.mod`); `cargo`
# never sees it. The binary is emitted under the shared `target/` tree
# (already gitignored) so it sits beside cargo's build output and is
# cleaned by the same `rm -rf target`. Plan 07 T5 bakes it into the
# gVisor kind-node image.
shim-build:
    cd shim && go build -o ../target/shim/containerd-shim-sandbawks-v1 \
        ./cmd/containerd-shim-sandbawks-v1

# Go pre-push gate for the shim/ subtree — the analogue of `check` for the
# one Go binary in the repo. Mirrors the `go` CI job (.github/workflows/
# ci.yml): gofmt enforcement, vet, test, build. `go vet` does not check
# formatting, so the explicit gofmt step is what keeps the subtree honest.
# Run this before pushing any change under shim/; it is deliberately not
# folded into `check` (see the comment there).
shim-check:
    cd shim && test -z "$(gofmt -l .)" || \
        { echo "gofmt needed:"; gofmt -l .; exit 1; }
    cd shim && go vet ./...
    cd shim && go test ./...
    cd shim && go build ./...

# Build the sandbawks-controller container image locally.
# Tag matches what `kind load docker-image` (T5) and the publish workflow
# (T4) expect; the `:dev` tag marks it as an unpublished local build.
controller-image-build:
    docker build -t ghcr.io/hypotheosis/sandbawks-controller:dev \
        -f crates/sandbawks-controller/Dockerfile .

# Smoke-test the freshly built image by invoking the binary's `--help`.
# Used as a cheap sanity check that the image is runnable; not a substitute
# for the kind-based deployment smoke test in T5.
controller-image-run:
    docker run --rm ghcr.io/hypotheosis/sandbawks-controller:dev --help

# Build the `envd` carrier image locally. This is NOT the reference
# template image: it carries only the static envd binary, which the node
# DaemonSet's `stage-envd` init container copies onto a hostPath for
# sandbox Pods to exec (injection model, issue #265). A `SandboxTemplate`
# points its `envdImage` (not `image`) at this carrier; its `image` is a
# separate, envd-free rootfs (e.g. compound-sandbox).
envd-image-build:
    docker build -t ghcr.io/hypotheosis/envd:dev \
        -f crates/envd/Dockerfile .

# Smoke-test the freshly built envd image. `--help` is enough to prove
# the static binary loads on `scratch` (issue #268 — no userland) and
# that the entrypoint is wired correctly; the gRPC surface itself is
# exercised by the integration test in crates/envd/tests/.
envd-image-run:
    docker run --rm ghcr.io/hypotheosis/envd:dev --help

# Build the sandbawks-api container image locally. Tag matches what
# the `build-images` matrix in .github/workflows/kind-smoke.yaml emits
# (re-tagged from `:sha-<sha>` to `:dev` inside the smoke job); the
# `:dev` tag marks it as an unpublished local build.
api-image-build:
    docker build -t ghcr.io/hypotheosis/sandbawks-api:dev \
        -f crates/sandbawks-api/Dockerfile .

# Smoke-test the freshly built api image. `--help` proves the static
# binary loads on distroless and that the entrypoint is wired correctly.
api-image-run:
    docker run --rm ghcr.io/hypotheosis/sandbawks-api:dev --help

# Build the sandbawks-node container image locally. Tag matches what
# the `build-images` matrix in .github/workflows/kind-smoke.yaml emits
# (re-tagged from `:sha-<sha>` to `:dev` once Plan 05 T13 wires kind
# load); the `:dev` tag marks it as an unpublished local build.
node-image-build:
    docker build -t ghcr.io/hypotheosis/sandbawks-node:dev \
        -f crates/sandbawks-node/Dockerfile .

# Smoke-test the freshly built node image. `--help` proves the static
# binary loads on distroless cc-debian12 and that the entrypoint is
# wired correctly. The bundled `runsc` is exercised via
# `docker run --rm ghcr.io/hypotheosis/sandbawks-node:dev --version` ad-hoc
# during Plan 05 T11/T12 integration.
node-image-run:
    docker run --rm ghcr.io/hypotheosis/sandbawks-node:dev --help

# Build the sandbawks-stage-gate init-gate image locally (epic #484,
# Phase A). Carries only the static file-poll binary on `scratch` — the
# no-creds init container that blocks a restore Pod until the node
# DaemonSet stages its marker. Tag matches what the `build-images` matrix
# in .github/workflows/kind-smoke.yaml emits; the `:dev` tag marks it as
# an unpublished local build.
stage-gate-image-build:
    docker build -t ghcr.io/hypotheosis/sandbawks-stage-gate:dev \
        -f crates/sandbawks-stage-gate/Dockerfile .

# Smoke-test the freshly built stage-gate image. `--help` proves the
# static binary loads on `scratch` (no userland) and that the entrypoint
# is wired correctly.
stage-gate-image-run:
    docker run --rm ghcr.io/hypotheosis/sandbawks-stage-gate:dev --help

# Build the custom kind node image locally (issue #147). Layers `runsc`
# + a containerd runtime handler config atop `kindest/node:v1.31.0` so
# the smoke cluster has a backing handler for the `gvisor` RuntimeClass
# (T12). Plan 05 T13 will flip `cluster-up` below to consume this `:dev`
# tag in place of the vanilla `kindest/node` pin.
kind-node-image-build:
    docker build -t sandbawks-kind-node:dev \
        -f ci/kind-node/Dockerfile .

# kind node image pin. The smoke cluster's per-node `image:` field in
# `deploy/kind/cluster.yaml` is the source of truth — this constant is
# only used by tooling that needs to know which image to pre-build or
# pre-pull, and by `scripts/smoke-assert.sh`'s sanity check. Both
# control-plane and worker nodes run `sandbawks-kind-node:latest`
# (issue #147), which layers runsc + a containerd runtime handler for
# `io.containerd.runsc.v1` atop `kindest/node:v1.31.0`. The k8s minor
# version (1.31) is aligned with the workspace `k8s-openapi` feature
# flag (`v1_31`) so the API surface the controller compiles against
# matches the apiserver the smoke cluster runs.
KIND_NODE_IMAGE := "ghcr.io/hypotheosis/sandbawks-kind-node:latest"

# `pullPolicy=Never` is critical here: kind has the `:dev` image loaded
# in-cluster via `kind load docker-image`; we never want kubelet to
# attempt a registry pull for an unpublished tag and crash-loop on
# ErrImagePull.
#
# Brings up a two-node kind smoke cluster (control-plane + worker) with
# controller + api + envd loaded. The api Deployment is gated behind
# helm `api.enabled`; we flip it on here (Plan 04 T15) so
# `deploy/kind/fixtures/api-smoke.sh` can drive the full HTTP/WS surface
# against real envd Pods.
#
# Branches on `SANDBAWKS_KIND_GVISOR` (Plan 05 T13). Default in CI,
# opt-in locally:
#   - SANDBAWKS_KIND_GVISOR=1: builds the `sandbawks-kind-node:dev` and
#     `sandbawks-node:dev` images, retags the kind-node image as
#     `:latest` so cluster.yaml's per-node pin resolves to the local
#     build, runs `deploy/kind/fixtures/install-garage.sh` to seed the
#     S3 bucket + snapshot-store Secret, and installs the chart with
#     `gvisor.enabled=true` + `node.enabled=true` so the node DaemonSet
#     lands on the worker and stamps `runtimeClassName: gvisor` on
#     sandbox Pods.
#   - default mode: expects `sandbawks-kind-node:latest` to already be
#     present locally (pre-built via `just kind-node-image-build &&
#     docker tag sandbawks-kind-node:dev ghcr.io/hypotheosis/sandbawks-kind-node:latest`,
#     or pulled from ghcr.io). Installs the chart with `node.enabled=false`
#     so the node DaemonSet stays out — its DaemonSet template is
#     wired to the snapshot-store Secret, which only the gvisor path
#     populates.
#
# The trailing `kubectl port-forward` is best-effort and runs in the
# background so the host process can reach `svc/sandbawks-api`; the
# api-smoke script does its own `wait deploy/sandbawks-api` and
# `curl /readyz` retry loop, so a delayed port-forward attach is harmless.
cluster-up: controller-image-build envd-image-build api-image-build
    #!/usr/bin/env bash
    set -euo pipefail

    GVISOR_MODE="${SANDBAWKS_KIND_GVISOR:-0}"

    if [[ "${GVISOR_MODE}" == "1" ]]; then
        echo "cluster-up: SANDBAWKS_KIND_GVISOR=1 — building kind-node + node images"
        # Build the custom kind node image (issue #147) and retag it as
        # `:latest` so `deploy/kind/cluster.yaml`'s per-node image pin
        # resolves to the freshly-built local image instead of attempting
        # a pull from ghcr.io. The retag is unconditional so a stale local
        # `:latest` from a previous run can never shadow the current build.
        just kind-node-image-build
        docker tag sandbawks-kind-node:dev ghcr.io/hypotheosis/sandbawks-kind-node:latest
        # Build the in-cluster node DaemonSet image. Loaded into the
        # cluster after creation alongside the controller/api/envd images.
        just node-image-build
    else
        # Default mode: cluster.yaml pins the kind-node image to
        # `ghcr.io/hypotheosis/sandbawks-kind-node:latest`. If that tag isn't
        # locally present, `kind create cluster` will try to pull it from
        # ghcr.io (which needs a `docker login ghcr.io` while the package is
        # private). Fail loudly with a hint instead of letting kind's error surface.
        if ! docker image inspect ghcr.io/hypotheosis/sandbawks-kind-node:latest >/dev/null 2>&1; then
            echo "cluster-up: ghcr.io/hypotheosis/sandbawks-kind-node:latest not found locally." >&2
            echo "  Either:" >&2
            echo "    - SANDBAWKS_KIND_GVISOR=1 just cluster-up   (build locally)" >&2
            echo "    - docker pull ghcr.io/hypotheosis/sandbawks-kind-node:latest" >&2
            exit 1
        fi
    fi

    # cluster.yaml pins the kind node image per-node, so no `--image`
    # flag is needed; passing one would be silently overridden by the
    # YAML's per-node value.
    #
    # Skip `kind create` if the cluster already exists — kind errors out
    # with "node(s) already exist for a cluster with the name X" on
    # re-creation, which would block a cluster-up that died mid-flight
    # (e.g. after the cluster came up but before `helm install` finished)
    # from being re-run to pick up where it stopped. Every step after
    # `kind create` is independently idempotent (`kind load` overwrites,
    # `kubectl apply` upserts, `helm upgrade --install` patches in
    # place), so re-running is safe as long as we don't trip over the
    # create.
    if kind get clusters 2>/dev/null | grep -qx 'sandbawks-smoke'; then
        echo "cluster-up: kind cluster 'sandbawks-smoke' already exists — skipping create"
    else
        kind create cluster --name sandbawks-smoke --config deploy/kind/cluster.yaml
    fi
    kind load docker-image --name sandbawks-smoke ghcr.io/hypotheosis/sandbawks-controller:dev
    kind load docker-image --name sandbawks-smoke ghcr.io/hypotheosis/envd:dev
    kind load docker-image --name sandbawks-smoke ghcr.io/hypotheosis/sandbawks-api:dev

    # CRDs are not bundled in the chart (Helm 3 doesn't upgrade chart-managed
    # CRDs anyway, so committing them under the chart was misleading and
    # drift-prone). Apply them out-of-band before `helm install --skip-crds`.
    kubectl apply -f deploy/crds

    if [[ "${GVISOR_MODE}" == "1" ]]; then
        kind load docker-image --name sandbawks-smoke ghcr.io/hypotheosis/sandbawks-node:dev

        # Seed Garage (S3 backend) + the snapshot-store Secret before
        # the chart install so the node DaemonSet's `envFrom` reference
        # resolves on first rollout — otherwise Pods stay in
        # CreateContainerConfigError. Garage replaces an earlier MinIO
        # fixture that was repeatedly flaking on Docker Hub pulls (and
        # that MinIO itself is now unmaintained + bitnami chart trust
        # evaporated post Broadcom).
        bash deploy/kind/fixtures/install-garage.sh

        helm upgrade --install sandbawks deploy/helm/sandbawks \
            --skip-crds \
            --set api.enabled=true \
            --set node.enabled=true \
            --set gvisor.enabled=true \
            --set snapshotStore.existingSecret=sandbawks-snapshot-store \
            --set snapshotStore.endpoint=http://garage.default.svc.cluster.local:3900 \
            --set snapshotStore.bucket=sandbawks-snapshots \
            --set images.api.repository=ghcr.io/hypotheosis/sandbawks-api \
            --set images.api.tag=dev \
            --set images.api.pullPolicy=Never \
            --set images.controller.repository=ghcr.io/hypotheosis/sandbawks-controller \
            --set images.controller.tag=dev \
            --set images.controller.pullPolicy=Never \
            --set images.node.repository=ghcr.io/hypotheosis/sandbawks-node \
            --set images.node.tag=dev \
            --set images.node.pullPolicy=Never \
            --set envd.image=ghcr.io/hypotheosis/envd \
            --set envd.tag=dev \
            --set envd.pullPolicy=Never \
            --wait --timeout 300s
    else
        # `helm upgrade --install` instead of `helm install` so a re-run
        # against an existing `sandbawks` release patches in place
        # rather than erroring with "cannot re-use a name that is still
        # in use".
        helm upgrade --install sandbawks deploy/helm/sandbawks \
            --skip-crds \
            --set api.enabled=true \
            --set node.enabled=false \
            --set images.api.repository=ghcr.io/hypotheosis/sandbawks-api \
            --set images.api.tag=dev \
            --set images.api.pullPolicy=Never \
            --set images.controller.repository=ghcr.io/hypotheosis/sandbawks-controller \
            --set images.controller.tag=dev \
            --set images.controller.pullPolicy=Never \
            --set envd.image=ghcr.io/hypotheosis/envd \
            --set envd.tag=dev \
            --set envd.pullPolicy=Never \
            --wait --timeout 180s
    fi

    # Background `kubectl port-forward` so the host can hit the api at
    # http://localhost:8080. `pkill -f` first to make the recipe idempotent
    # when re-run without a `cluster-down` in between.
    pkill -f 'kubectl port-forward svc/sandbawks-api 8080:8080' 2>/dev/null || true
    nohup kubectl port-forward svc/sandbawks-api 8080:8080 \
        >/tmp/sandbawks-api-port-forward.log 2>&1 &

# `kind delete cluster` is idempotent — a no-op if the named cluster
# doesn't exist, so this recipe is safe to run from a clean machine.
#
# Tears down the kind smoke cluster and the background port-forward
# `cluster-up` started.
cluster-down:
    -pkill -f 'kubectl port-forward svc/sandbawks-api 8080:8080' 2>/dev/null || true
    kind delete cluster --name sandbawks-smoke

# `controller-image-build` is a transitive dep via `cluster-up`, so the
# fresh image is rebuilt and loaded into the new cluster automatically.
#
# Recycles the smoke cluster: tear down, then bring back up with a fresh image.
cluster-reload: cluster-down cluster-up

# Reads the `sandbawks.dev/runsc-version` Node label the `sandbawks-node`
# DaemonSet stamps every reconcile tick (#546); a node with no label (host runsc
# unreadable, or `node.runscReconcile.enabled=false`) shows `<none>`/UNKNOWN.
# `desired` defaults to the version baked into the deployed DaemonSet (its
# `SANDBAWKS_RUNSC_VERSION` env, from `node.runscVersion`); pass an explicit one to
# compare against a different target, e.g. `just runsc-versions 20260525.0`. The
# label is the full `release-<ver>` token, so both sides are normalised to the bare
# version. Exits non-zero if a known desired is set and any node has drifted, so it
# doubles as a CI/cron drift gate.
#
# Show each node's host runsc version and flag any drift off the pinned version.
runsc-versions desired='':
    #!/usr/bin/env bash
    set -euo pipefail
    desired='{{ desired }}'
    if [ -z "$desired" ]; then
        # Auto-detect the pinned version from the deployed DaemonSet (any namespace).
        desired="$(kubectl get daemonset -A -o jsonpath='{.items[?(@.metadata.name=="sandbawks-node")].spec.template.spec.containers[0].env[?(@.name=="SANDBAWKS_RUNSC_VERSION")].value}' 2>/dev/null || true)"
    fi
    norm() { printf '%s' "${1#release-}"; }   # strip the release- prefix for comparison
    want="$(norm "$desired")"
    if [ -n "$want" ]; then
        echo "desired runsc version: ${want}"
    else
        echo "desired runsc version: (unknown — pass one as an argument)"
    fi
    printf '%-52s %-24s %s\n' NODE RUNSC-VERSION STATUS
    nodes="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.sandbawks\.dev/runsc-version}{"\n"}{end}')"
    drift=0
    while IFS=$'\t' read -r node ver; do
        [ -z "$node" ] && continue
        have="$(norm "${ver:-}")"
        if [ -z "$have" ]; then
            status='UNKNOWN (no label)'
        elif [ -z "$want" ]; then
            status='-'
        elif [ "$have" = "$want" ]; then
            status='ok'
        else
            status='DRIFT'; drift=1
        fi
        printf '%-52s %-24s %s\n' "$node" "${ver:-<none>}" "$status"
    done <<< "$nodes"
    [ "$drift" -eq 0 ] || { echo "drift detected: one or more nodes are not at ${want}" >&2; exit 1; }

# Apply the smoke fixture and assert the controller advances the
# Sandbox to `Running`. Assumes the cluster from `cluster-up` is live;
# CI chains this directly after `cluster-up` in the smoke job.
smoke:
    bash scripts/smoke-assert.sh

# Plan 04 T15 — drive the full E2B-compatible API lifecycle against the
# kind cluster brought up by `cluster-up`. Hits `localhost:8080` via the
# background port-forward; the script handles its own waiting for the
# api Deployment to be Available.
api-smoke:
    bash deploy/kind/fixtures/api-smoke.sh

# Plan 05 T14 — entry point for the pause/resume snapshot smoke
# (issue #150). Walks the full S3-backed snapshot round-trip
# (pause → mc ls → delete Pod → resume → file round-trip → kill)
# against the cluster brought up by `SANDBAWKS_KIND_GVISOR=1 just
# cluster-up`. Hits `localhost:8080` via the background port-forward
# `cluster-up` arranges; the script handles its own waiting for the
# api Deployment to be Available.
pause-resume-smoke:
    bash deploy/kind/fixtures/pause-resume-smoke.sh

# Phase 7 (issue #301) — stand up the local OTLP collector fixture in the
# `observability` namespace of the kind cluster. The collector accepts OTLP on
# :4317 (gRPC) / :4318 (HTTP) and dumps every received signal to its stdout.
# Pair with a chart install that targets it:
#   helm upgrade --install sandbawks deploy/helm/sandbawks \
#     --set observability.enabled=true \
#     --set observability.endpoint=http://otel-collector.observability.svc:4318
# then drive `just smoke` / `just pause-resume-smoke` and watch the spans land
# via `just otel-collector-logs`. The single-trace_id check: spans from
# sandbawks-api, sandbawks-controller, sandbawks-node, and envd share one
# trace_id. See docs/observability.md.
otel-collector-up:
    kubectl apply -f deploy/kind/fixtures/otel-collector.yaml
    # `rollout restart` after apply so an edit to the embedded `config.yaml`
    # ConfigMap actually reaches the running pod: `kubectl apply` updates the
    # ConfigMap but leaves the Deployment's pod spec unchanged, so without
    # this the collector keeps its old config (the raw manifest has no
    # Helm-style `checksum/config` annotation to roll on). Harmless no-op on a
    # first install.
    kubectl -n observability rollout restart deploy/otel-collector
    kubectl -n observability rollout status deploy/otel-collector --timeout=120s

# Follow the collector's stdout — the `debug` exporter prints every received
# trace / metric / log, so this is where the exported signals show up.
otel-collector-logs:
    kubectl -n observability logs -f deploy/otel-collector

# Tear the collector fixture (and its namespace) back down.
otel-collector-down:
    kubectl delete -f deploy/kind/fixtures/otel-collector.yaml --ignore-not-found

# Issue #335 — local OTLP emission smoke for the node SnapshotJob metrics.
# Stands up a throwaway collector in Docker (no cluster needed), runs the
# `emit_snapshotjob_metrics` example through the real OTel SDK, and asserts the
# `snapshotjob.claims_total` / `snapshotjob.watch_info` series reach it with the
# right attributes. The local complement to `otel-arrival-smoke`, which needs a
# full gVisor cluster. Requires Docker.
otel-emit-smoke:
    bash scripts/otel-emit-smoke.sh

# Issue #311 — end-to-end OTel "collector arrival" smoke. Asserts that at
# least one received span / log record carries the expected `service.name`
# for each of sandbawks-api, sandbawks-controller, sandbawks-node, and
# sandbawks-envd — turning "the SDK is wired up" into "telemetry demonstrably reaches a
# collector with correct resource attributes". Applies the collector fixture
# itself, then polls its debug-exporter stdout. Assumes a gVisor cluster
# brought up with `observability.enabled=true` pointing at the collector AND
# that a workload exercising every service has run (e.g. `just
# pause-resume-smoke`); narrow `OTEL_SMOKE_EXPECTED_SERVICES` for a lighter
# workload. See docs/observability.md.
otel-arrival-smoke:
    bash deploy/kind/fixtures/otel-arrival-smoke.sh

# Issue #516 — regression guard for the rendered production Vector telemetry
# hop. Renders the chart's otel-collector ConfigMap, runs `vector validate` on
# it, then drives a live OTLP round-trip (the rendered hop as sender, a second
# Vector as a stand-in Traceway intake, `telemetrygen` as the load generator)
# and asserts no serialize-time drops (the #496 class), all three signals
# arrive, the spoofed `service.namespace` is re-stamped on traces/logs/metrics,
# the OTLP histogram bucket invariant holds, and the prometheus scrape surface
# is exposed. Uses the same pinned Vector image the chart deploys. Requires
# Docker, helm, and yq; no cluster needed.
vector-hop-smoke:
    bash scripts/vector-hop-roundtrip-smoke.sh

# Plan 06 T1 (issue #183) — drive the official `e2b` Python SDK
# against the kind cluster's gateway. Assumes `SANDBAWKS_KIND_GVISOR=1
# just cluster-up` has already run: that recipe (a) seeds the
# `api-smoke-template` SandboxTemplate + matching TeamAPIKey via
# `deploy/kind/fixtures/api-smoke.sh` and (b) backgrounds the
# `kubectl port-forward svc/sandbawks-api 8080:8080` the harness's
# default `E2B_API_URL` targets. The Python project lives outside the
# Cargo workspace (sibling to `deploy/kind/fixtures/`), `uv`-managed.
# Override `E2B_API_URL` / `E2B_API_KEY` to point at a non-kind gateway.
# Extra arguments are forwarded to pytest, so CI can pass
# `--junitxml=…` for artifact upload (Plan 06 T2, issue #184).
sdk-smoke *PYTEST_ARGS:
    cd tests/e2b-sdk-compat && uv sync && uv run pytest -v {{PYTEST_ARGS}}

# Plan 06 T5 (issue #187) — drive the official `@e2b/code-interpreter`
# JS SDK against the same kind cluster's gateway, parallel to
# `sdk-smoke` (Python). Same assumptions: `SANDBAWKS_KIND_GVISOR=1
# just cluster-up` has already run and the api-smoke fixtures are
# applied. The JS project lives outside the Cargo workspace (sibling
# to `tests/e2b-sdk-compat/`), npm-managed.
#
# `npm install` is run before the test invocation so a fresh checkout
# works without a manual install step; subsequent runs are no-ops if
# `node_modules/` is up-to-date. Extra arguments are forwarded to
# vitest, so CI can pass options like `--reporter=junit` via
# `JUNIT_OUTPUT_FILE` (see vitest.config.ts) for artifact upload.
sdk-smoke-js *VITEST_ARGS:
    cd tests/e2b-sdk-compat-js && npm install && npm test -- {{VITEST_ARGS}}

# Clean-room E2B JS SDK smoke against a LIVE gateway in native HOSTNAME
# mode (`{port}-{id}.{domain}`) — the way an unmodified E2B app addresses
# a real deployment, proving 100% API compatibility. Unlike `sdk-smoke-js`
# (kind cluster, header-mode via `E2B_SANDBOX_URL`), this targets a
# deployed wildcard domain such as `*.e2b.example.com` and creates a real
# sandbox: it runs `echo`/`hostname`/`uname -a`, a file round-trip, and a
# streaming command (printing per-chunk arrival times to distinguish live
# streaming from upstream buffering), then kills the sandbox.
#
# Requires `E2B_API_KEY` in the environment (a team key). `E2B_SANDBOX_URL`
# / `E2B_API_URL` are deliberately unset inside the recipe so the SDK uses
# its native hostname routing; an ambient export of either would defeat the
# point of the test. `bun` is used to match the validated local run.
#
#   E2B_API_KEY=sbk_... just e2b-node-smoke e2b.example.com
#   E2B_API_KEY=sbk_... just e2b-node-smoke staging.example.com base
e2b-node-smoke domain template="compound-sandbox":
    cd tests/e2b-node-smoke && bun install && \
        unset E2B_SANDBOX_URL E2B_API_URL && \
        E2B_DOMAIN={{domain}} E2B_TEMPLATE={{template}} bun run smoke.ts

# Plan 06 T3 (issue #185) — pause/resume perf bench harness. Assumes
# the kind cluster (`cluster-up`, gVisor mode strongly recommended) is
# already live, the api gateway is reachable at `localhost:8080`, and
# the `api-smoke-template` SandboxTemplate + matching TeamAPIKey have
# been applied (which `api-smoke.sh` / the kind-smoke workflow's API
# smoke step already does). The bench lives outside the Cargo
# workspace (sibling to `tests/e2b-sdk-compat/`) so a perf regression
# never manifests as a workspace `cargo test` / clippy failure — see
# `tests/bench/Cargo.toml` for the standalone `[workspace]` rationale.
#
# Tunables (all optional, sensible defaults baked in — see
# `tests/bench/src/main.rs`):
#
#   - BENCH_ITERATIONS=20            (issue #185 default)
#   - BENCH_PAYLOAD_MIB=1024         (~1 GiB sandbox)
#   - BENCH_INCLUDE_RESUME=0         (resume gate; opt-in default. The
#                                     in-cluster runsc-restore path is now
#                                     green (Plan 07, epic #204), so CI
#                                     sets this to 1; set it locally to
#                                     measure resume too)
#   - BENCH_PAUSE_P95_MS=8000        (spec §13 ceiling)
#   - BENCH_RESUME_P95_MS=2000       (spec §13 ceiling)
#   - BENCH_RESULTS_PATH=bench-results.json   (relative path —
#                                              see below for where
#                                              this actually resolves)
#
# `cargo run --release` so the bench is comparable to what CI builds.
# The JSON report writes to the *bench binary's* working directory:
# because this recipe `cd`s into `tests/bench/` before invoking
# cargo, the default `BENCH_RESULTS_PATH=bench-results.json` lands
# at `tests/bench/bench-results.json`, NOT at the repo root. If you
# want the file at the repo root (matching the CI workflow's
# upload-artifact step), set `BENCH_RESULTS_PATH=$PWD/bench-results.json`
# before running, or override with any other absolute path.
bench *EXTRA_ARGS:
    cd tests/bench && cargo run --release -- {{EXTRA_ARGS}}

# Internal helper: read the workspace version from Cargo.toml and create +
# push a single `vX.Y.Z` tag. Idempotent against the local tag (skips if it
# already exists) so re-running a release recipe doesn't blow up.
tag-workspace-version:
  #!/usr/bin/env bash
  set -euo pipefail
  version="$(grep '^version = ' Cargo.toml | head -1 | cut -d'"' -f2)"
  tag="v${version}"
  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
    echo "tag ${tag} already exists locally, skipping create"
  else
    git tag -a "${tag}" -m "Release ${tag}"
  fi
  git push origin "${tag}"

release-patch:
  #!/usr/bin/env bash
  set -euo pipefail
  git pull origin main 
  current="$(grep '^version = ' Cargo.toml | head -1 | cut -d'"' -f2)"
  IFS='.' read -r major minor patch_ver <<< "$current"
  next_version="${major}.${minor}.$((patch_ver + 1))"
  sed -i "s/^appVersion: .*/appVersion: \"${next_version}\"/" deploy/helm/sandbawks/Chart.yaml
  git add deploy/helm/sandbawks/Chart.yaml
  git commit -m "chore: bump Chart.yaml appVersion to ${next_version}" || true
  cargo release --no-publish --no-verify patch --execute
  just tag-workspace-version
  cat deploy/helm/sandbawks/Chart.yaml |grep ersi


release-minor:
  #!/usr/bin/env bash
  set -euo pipefail
  git pull origin main
  current="$(grep '^version = ' Cargo.toml | head -1 | cut -d'"' -f2)"
  IFS='.' read -r major minor patch_ver <<< "$current"
  next_version="${major}.$((minor + 1)).0"
  sed -i "s/^appVersion: .*/appVersion: \"${next_version}\"/" deploy/helm/sandbawks/Chart.yaml
  git add deploy/helm/sandbawks/Chart.yaml
  git commit -m "chore: bump Chart.yaml appVersion to ${next_version}"
  cargo release --no-publish --no-verify minor --execute
  just tag-workspace-version
  cat deploy/helm/sandbawks/Chart.yaml |grep ersi

release-major:
  #!/usr/bin/env bash
  set -euo pipefail
  git pull origin main
  current="$(grep '^version = ' Cargo.toml | head -1 | cut -d'"' -f2)"
  IFS='.' read -r major minor patch_ver <<< "$current"
  next_version="$((major + 1)).0.0"
  sed -i "s/^appVersion: .*/appVersion: \"${next_version}\"/" deploy/helm/sandbawks/Chart.yaml
  git add deploy/helm/sandbawks/Chart.yaml
  git commit -m "chore: bump Chart.yaml appVersion to ${next_version}"
  cargo release --no-publish --no-verify major --execute
  just tag-workspace-version
  cat deploy/helm/sandbawks/Chart.yaml |grep ersi
