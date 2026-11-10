# CI smoke T8 — kind-smoke workflow caching

**Issue:** #74
**Epic:** #66 — CI: kind-based deployment smoke test
**Date:** 2026-05-23

## What landed

Three caching layers wired into `.forgejo/workflows/kind-smoke.yaml`,
in the order they fire on a warm run:

1. `actions/cache@v4` over `~/.cargo/registry`, `~/.cargo/git`, and the
   workspace `target/` directory, keyed by `${{ runner.os
   }}-cargo-controller-${{ hashFiles('Cargo.lock') }}` with a
   `restore-keys` fallback on the Cargo.lock hash prefix.
2. `actions/cache@v4` over `/tmp/kind-node-image.tar` keyed by
   `kind-node-v1.31.0`, paired with a `Load or pull kind node image`
   step that `docker load`s the cached tarball when present and
   otherwise `docker pull`s and `docker save`s it.
3. `docker/setup-buildx-action@v3` plus `docker/build-push-action@v6`
   replacing the inline `docker build` step. The action uses
   `cache-from: type=gha` / `cache-to: type=gha,mode=max` against the
   Forgejo runner's GHA-compatible cache backend, and `load: true` so
   the resulting image lands in the dind sidecar's image store for
   `kind load docker-image`.

## Why these choices

### Cargo cache is kept despite limited direct payoff

The current build path runs `cargo build` inside the controller
Dockerfile's `rust:1.88-bookworm` builder stage, so the host's
`~/.cargo` and `target/` directories are not touched on this workflow.
The cache step is still included because (a) the cost on a miss is a
single tarball restore, (b) the spec in issue #74 calls for it
explicitly, and (c) it future-proofs any host-side cargo invocation
that gets added to this workflow later (e.g. a pre-build linter).
Caching the cargo state across docker builds would require either a
`cargo-chef`-style refactor of the Dockerfile or a BuildKit
`RUN --mount=type=cache` directive — both out of scope for T8.

### Kind node image cache adapted away from `~/.kind/images`

The issue's suggested path of `~/.kind/images` is populated by
`helm/kind-action@v1`, which this workflow deliberately does not use
(`uses:` references to action-style helpers like `helm/kind-action`,
`azure/setup-helm`, and `extractions/setup-just` rely on either GitHub
API auth or the `/opt/hostedtoolcache` convention, neither of which
exists on the Forgejo `docker` runner; the original workflow comment
at the top of `kind-smoke.yaml` explains the constraint in detail).

The adaptation caches a docker image tarball at
`/tmp/kind-node-image.tar` instead. The `Load or pull kind node image`
step branches: cache hit means `docker load -i /tmp/kind-node-image.tar`
(seeds the dind sidecar's image store with `kindest/node:v1.31.0`);
cache miss means `docker pull` followed by `docker save`. Either way,
the `kind create cluster --image kindest/node:v1.31.0` step that runs
next finds the image already present in the docker daemon and skips
the upstream registry round-trip.

### BuildKit GHA cache is the load-bearing speedup

The controller Dockerfile is a two-stage musl-static build. The early
layers — `apt-get install musl-tools protobuf-compiler` and `rustup
target add x86_64-unknown-linux-musl` — are the most expensive parts
that *don't* depend on source code, so a layer cache pays back well.
`type=gha,mode=max` exports all intermediate layers (not just the
final image's layers) into the runner's cache so they're available on
the next run. The actual `cargo build` layer still re-runs whenever
any file under the build context changes, because `COPY . .` happens
before it — that's a known limitation called out in the workflow
comment.

`docker/setup-buildx-action@v3` is required before
`docker/build-push-action@v6` because the GHA cache backend is a
buildx feature, not a feature of the legacy `docker build` builder.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — workspace tests all green.
- `act -W .forgejo/workflows/kind-smoke.yaml -l` — workflow parses and
  the `kind-smoke` job is discoverable. (Not a runtime check; `act`'s
  ubuntu-latest runner image and lack of a dind sidecar make a full
  local execution unrepresentative of the Forgejo runner.)
- Python `yaml.safe_load` — YAML structurally valid.

The acceptance criterion in #74 (cold < 4 min, warm < 90 s) can only
be measured by running the workflow twice in succession on the real
Forgejo Actions runner with the cache backend live. That happens
implicitly when this PR's CI runs and re-runs. If the warm run does
not land under 90 s, the most likely culprits in priority order are:

1. The cargo build layer inside the Dockerfile keeps re-running on
   source changes — moving to a `cargo-chef` or
   `RUN --mount=type=cache` Dockerfile would be the next lever.
2. The kind cluster create itself (`--wait 120s`) — not addressed by
   caching at all; lives in the kubelet/etcd cold-start budget.
3. The runner provisioning + tool-install step
   (`Install docker CLI, kind, kubectl, helm`) — a custom runner image
   with these pre-baked would shave it, at the cost of maintaining a
   runner image.

## Out of scope

- Buildjet / remote BuildKit clusters — deferred per the issue.
- Pulling the pre-built controller image from Harbor as a cache source
  — would create an ordering dependency on the `controller-image.yaml`
  workflow (T4); deferred per the issue.
- Refactoring the Dockerfile to cache cargo state across docker builds
  (cargo-chef or `RUN --mount=type=cache`) — separate concern; this PR
  is purely workflow-side.
