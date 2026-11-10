# 2026-05-31 — CI: force-remove orphan kind node containers before create (#238 follow-up)

The `bench` leg of `.forgejo/workflows/kind-smoke.yaml` failed at cluster
creation:

```
ERROR: failed to create cluster: node(s) already exist for a cluster with the name "sandbawks-bench"
```

## Why

The Forgejo runner shares a single dind sidecar across runs. A run that
is killed mid-`kind create` (cancelled, OOM, timeout) can leave the
cluster's node container behind. `kind delete cluster --name X` keys on
cluster *state* and does not always reap such a half-created orphan — but
`kind create cluster` still collides on the container *name* and aborts
with "node(s) already exist". The pre-create `kind delete … 2>/dev/null ||
true` therefore silently no-ops and the create fails.

## Fix

Add a backstop before `kind create` in both the `bench` and `kind-smoke`
cluster-creation steps: after `kind delete cluster`, force-remove any
container still carrying kind's own cluster label —

```bash
kind delete cluster --name <cluster> 2>/dev/null || true
docker rm -f $(docker ps -aq --filter "label=io.x-k8s.kind.cluster=<cluster>") 2>/dev/null || true
kind create cluster --name <cluster> …
```

`docker` targets the same dind daemon kind uses (the steps already
`docker pull`/`docker load` against it). An empty filter match makes
`docker rm -f` error with "requires at least 1 argument", which the
trailing `|| true` swallows. The `kind-smoke` step's previously-unmasked
`kind delete` is also softened to `2>/dev/null || true` for consistency,
since the `docker rm` backstop now does the load-bearing cleanup.

This does not change the intentional smoke‖bench parallelism (decoupled
by distinct cluster names + apiserver ports 6443/6444); it only makes
each job's create resilient to leftovers from a dead prior run on the
shared dind.

## Validation

- YAML parses (`python3 -c 'yaml.safe_load_all(...)'`).
- Workspace gate unaffected (no Rust/Helm change): not re-run for this
  CI-only edit.
