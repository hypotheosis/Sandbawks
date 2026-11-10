---
title: Run sandboxes on a dedicated node pool
description: Pin sandbox Pods to a labeled, tainted node pool with template tolerations and nodeSelector, and let the node DaemonSet run on that pool.
slug: how-to/run-sandboxes-on-a-dedicated-node-pool
head:
  - tag: title
    content: Run sandboxes on a dedicated node pool · Sandbawks docs
tableOfContents: true
---

This guide shows you how to run a template's sandboxes on their own node pool,
for example to keep untrusted workloads off your general nodes or to use a pool
with a different runtime. You label and taint the pool, let the Sandbawks node
DaemonSet run there, and give the template a matching `nodeSelector` and
`tolerations`.

## Before you begin

- Sandbawks installed with Helm.
- A node pool you can label and taint. This guide uses the label and taint key
  `sandbawks.example.com/pool` with the value `sandboxes`.

## Label and taint the pool

Label the pool's nodes so sandboxes can select them, and taint them so other Pods
stay off:

```sh
kubectl label nodes <node-name> sandbawks.example.com/pool=sandboxes
kubectl taint nodes <node-name> sandbawks.example.com/pool=sandboxes:NoSchedule
```

On a managed Kubernetes service, set the label and taint on the node pool itself
so that new nodes get them too.

## Let the node DaemonSet run on the pool

Sandbox Pods schedule only onto nodes with the `sandbawks.dev/envd-staged` label.
The `sandbawks-node` DaemonSet adds that label after it stages `envd` on a node,
so the DaemonSet must run on the new pool. It doesn't tolerate custom taints by
default. Add a toleration in your Helm values:

```yaml
node:
  tolerations:
    - key: sandbawks.example.com/pool
      operator: Equal
      value: sandboxes
      effect: NoSchedule
```

```sh
helm upgrade sandbawks oci://ghcr.io/hypotheosis/charts/sandbawks \
  --version 0.1.29 --namespace sandbawks --reuse-values -f node-pool-values.yaml
```

Check that the pool's nodes are ready for sandboxes:

```sh
kubectl get nodes -l sandbawks.example.com/pool=sandboxes -L sandbawks.dev/envd-staged
```

Each node shows `true` in the `ENVD-STAGED` column once the DaemonSet has staged
`envd` there. A node that never gets the label is missing the DaemonSet's
toleration, or doesn't report the runtime handler that the sandboxes' RuntimeClass
uses.

## Pin the template's sandboxes to the pool

Add `nodeSelector` and `tolerations` to the `SandboxTemplate`:

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-base
  namespace: team-acme
spec:
  image: registry.example.com/sandboxes/python:3.12
  defaultRuntimeClass: gvisor
  defaultTimeoutSeconds: 1800
  defaultResources:
    cpu: "1"
    memory: 1Gi
  nodeSelector:
    sandbawks.example.com/pool: sandboxes
  tolerations:
    - key: sandbawks.example.com/pool
      operator: Equal
      value: sandboxes
      effect: NoSchedule
```

```sh
kubectl apply -f python-base.yaml
```

The controller adds these to each sandbox Pod when it creates the Pod, including
for warm-pool members and for `Sandbox` resources you apply directly. Pods that
already exist keep their placement until they are recreated, for example on the
next resume.

## Override the placement for one sandbox

A `Sandbox` that sets `spec.nodeSelector` or `spec.tolerations` uses its own value
instead of the template's. The values replace the template's; they aren't merged.
Only a `Sandbox` you apply with `kubectl` can set them, because the E2B create
request has no fields for them.

## Check where sandboxes run

The `Node` column of `kubectl get sandboxes` shows each sandbox's node:

```sh
kubectl get sandboxes -n team-acme
```

A sandbox Pod that stays `Pending` usually means that no node on the pool both
matches the selector and has the `sandbawks.dev/envd-staged` label. Check the
Pod's events:

```sh
kubectl describe pod -n team-acme -l sandbawks.dev/sandbox=<sandbox-id>
```

:::note
On resume, a `Snapshot`-mode sandbox is placed only on a node whose
`sandbawks.dev/runsc-version` label matches the `runsc` release the snapshot was
taken with. This applies in addition to the template's `nodeSelector`, so keep the
`runsc` release the same across the pool. The `node.runscVersion` Helm value pins it.
:::

## See also

- [SandboxTemplate reference](/reference/custom-resources/sandboxtemplate/) —
  `tolerations` and `nodeSelector`.
- [Helm chart reference](/reference/helm-chart/) — `node.tolerations`,
  `node.nodeSelector`, and `node.runscVersion`.
