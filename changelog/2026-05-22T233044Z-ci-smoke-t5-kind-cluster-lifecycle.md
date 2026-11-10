# CI smoke T5 — kind cluster config + justfile lifecycle targets

**Issue:** #71
**Epic:** #66 — CI: kind-based deployment smoke test
**Date:** 2026-05-22

## What landed

- `deploy/kind/cluster.yaml` — minimal kind v1alpha4 cluster spec for a
  single control-plane node named `sandbawks-smoke`. **No
  `kubeadmConfigPatches` block** — see *Deviations* below.
- `justfile` additions:
  - `KIND_NODE_IMAGE := "kindest/node:v1.31.0"` — pinned at the
    justfile layer (not inside `cluster.yaml`) so the cluster manifest
    stays minimal and the version bump is a one-line diff.
  - `cluster-up` (depends on `controller-image-build`) — `kind create
    cluster`, `kind load docker-image`, then `helm install` with
    `api.enabled=false`, `node.enabled=false`, the local image repo +
    `:dev` tag, and `pullPolicy=Never`. `--wait --timeout 120s` so the
    recipe blocks until the controller Deployment reaches Available.
  - `cluster-down` — `kind delete cluster`. Idempotent (kind makes it
    a no-op when the cluster is absent), so it's safe to invoke from a
    clean machine.
  - `cluster-reload` — `cluster-down` then `cluster-up`. The
    `controller-image-build` dependency rides along via `cluster-up`,
    so this rebuilds the image every recycle (which is the whole
    reason you'd reload).

## Why these choices

### `kindest/node:v1.31.0`

The workspace `Cargo.toml` enables the `v1_31` feature on
`k8s-openapi`, which fixes the Kubernetes API surface the controller is
generated against. Running the smoke cluster on the matching minor
guarantees that any field the controller expects to be present in
`PATCH` / `LIST` responses actually is. Bumping `k8s-openapi`'s feature
flag in the future should be paired with bumping this image tag.

### `pullPolicy=Never` for the controller image

`ghcr.io/hypotheosis/sandbawks-controller:dev` is an unpublished local tag.
Without `pullPolicy=Never`, kubelet's default policy for non-`:latest`
tags is `IfNotPresent`, which would normally be fine — kind has the
image loaded — but any cluster-internal pull retry (image GC, restart
after eviction, etc.) would resolve `<registry>/...` against the actual
Harbor registry, find the unpublished `:dev` tag is absent, and
crash-loop on `ErrImagePull`. `Never` short-circuits the resolution
entirely.

### `--wait --timeout 120s`

The validation criterion in the issue is that the controller
Deployment reaches `Available=True` within 60 s. The `helm install
--wait` flag blocks until that condition is satisfied (or the timeout
fires), making the recipe's exit code load-bearing: a green
`just cluster-up` is sufficient evidence the controller actually
started. The 120 s timeout doubles the criterion to leave headroom for
the image-load step on slower machines.

### `cluster-reload` as a dependency-chained recipe

Originally drafted as `just cluster-down && just cluster-up` inside the
recipe body (per the issue spec). Rewrote to use just's dependency
mechanism (`cluster-reload: cluster-down cluster-up`) for two reasons:

1. The dependency form prints the same `kind delete` → `kind create` →
   `helm install` sequence in the standard `just` execution log without
   the extra layer of `just`-invoking-`just` indirection.
2. If `cluster-down` fails (unlikely but possible), the dependency
   form aborts before `cluster-up`, where the original `&&` chain
   would have done the same thing — but the dependency form is the
   idiomatic just expression.

## Deviations from the issue spec (and why)

The issue body proposed this `kubeadmConfigPatches` block to re-apply
the control-plane node label:

```yaml
kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "node-role.kubernetes.io/control-plane="
```

Verifying against `kind v0.31.0` + `kindest/node:v1.31.0` surfaced two
problems with this:

1. **The map form of `kubeletExtraArgs` is rejected by
   `kubeadm.k8s.io/v1beta4`.** kubeadm switched to v1beta4 as the
   default config version in k8s 1.31, and v1beta4 changed
   `kubeletExtraArgs` from `map[string]string` to a list of
   `{name, value}` entries. The map form causes `kubeadm init` to
   fail at the `wait-control-plane` phase (the apiserver / kubelet
   never come up), which manifests as a kind create that hangs for
   ~5 minutes before erroring out with a generic "could not
   initialize a Kubernetes cluster" trace.
2. **The patch is redundant.** kind already applies
   `node-role.kubernetes.io/control-plane=` to control-plane nodes
   by default. Verified by creating a kind cluster with no config
   patches at all and observing `kubectl get nodes --show-labels`
   returning that label.

So the patch was dropped entirely. `cluster.yaml` is now just the
single-node `nodes: [{role: control-plane}]` minimum, and the
control-plane label still resolves correctly. A leading comment block
in `cluster.yaml` records this so a future reader doesn't reintroduce
the patch.

(If a future Kubernetes version stops applying that label by default
for kind, the correct fix would be to add the patch back in v1beta4
list form, not the v1beta3 map form.)

## Validation

Repo-level:

- `just fmt` — no diff.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — 100% green across all workspace crates.
- `just --list` — confirms the three new recipes (`cluster-up`,
  `cluster-down`, `cluster-reload`) appear with sensible one-line
  descriptions.

Chart-level:

- `helm install --dry-run --debug sandbawks deploy/helm/sandbawks
  --set api.enabled=false --set node.enabled=false --set
  images.controller.repository=ghcr.io/hypotheosis/sandbawks-controller
  --set images.controller.tag=dev --set images.controller.pullPolicy=Never`
  — renders successfully. The resulting manifests contain exactly
  **one** `Deployment` (`sandbawks-controller`), with `image:
  "ghcr.io/hypotheosis/sandbawks-controller:dev"`. No `sandbawks-api`
  Deployment, no `sandbawks-node` DaemonSet — matches the issue's
  two `kubectl ... 2>&1 | grep 'not found'` assertions.

Live cluster validation (against `kind v0.31.0` + Docker 29.4.2 +
helm v4.1.4 + kubectl v1.36 on Fedora 44, kernel 6.19, cgroups v2):

- `just cluster-up` from a clean machine: exits 0 in **~30 s** (target
  was < 90 s).
- `kubectl get deploy -n default sandbawks-controller -o
  jsonpath='{.status.conditions[?(@.type=="Available")].status}'` →
  `True` (in fact the `helm install --wait` in `cluster-up`
  already enforces this — the assertion is a belt-and-braces check).
- `kubectl get deploy -n default sandbawks-api 2>&1 | grep 'not found'`
  → `Error from server (NotFound): deployments.apps "sandbawks-api"
  not found`.
- `kubectl get ds -n default sandbawks-node 2>&1 | grep 'not found'`
  → `Error from server (NotFound): daemonsets.apps "sandbawks-node"
  not found`.
- `just cluster-down`: exits 0 in ~2 s, cluster removed (`kind get
  clusters` returns empty).
- `just cluster-down` second invocation against a clean machine:
  exits 0 (kind prints `Deleting cluster ...` then returns success
  even when no such cluster exists — idempotent as required).
- `just cluster-reload` from clean: exits 0 in ~36 s; controller image
  rebuilt and re-loaded into the new cluster.

## Out of scope (per the issue)

- Multi-node clusters.
- Loading a remote registry image — `:dev` is local-only; the
  Harbor-published image is exercised by T7's CI workflow, not by
  `just cluster-up`.

## Dependencies

- Builds on **T2** (chart `enabled` toggles, #68) —
  required for `--set api.enabled=false --set node.enabled=false` to
  actually suppress those workloads. Verified by the helm dry-run above.
- Builds on **T3** (`controller-image-build` recipe + Dockerfile,
  #69) — `cluster-up` invokes it
  transitively.
