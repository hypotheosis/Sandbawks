---
title: Run sandboxes on Kata Containers
description: Run sandboxes in Kata Containers VMs instead of gVisor by configuring the node DaemonSet for Kata and giving templates a Kata RuntimeClass with the Filesystem persistence mode.
slug: how-to/run-sandboxes-on-kata-containers
head:
  - tag: title
    content: Run sandboxes on Kata Containers · Sandbawks docs
tableOfContents: true
---

This guide shows you how to run sandboxes in [Kata Containers](https://katacontainers.io/)
virtual machines instead of gVisor, for example on an AKS node pool with Pod
Sandboxing. Kata sandboxes can pause and resume only in the `Filesystem`
persistence mode: pausing keeps the sandbox's PVCs and deletes its Pod, and
resuming starts a new VM. Memory and running processes aren't preserved.

:::caution
The Helm chart renders a single node DaemonSet, and `node.runtime` applies to all
of it. A cluster can serve either gVisor or Kata sandboxes, not both
([#10](https://github.com/hypotheosis/Sandbawks/issues/10)). This guide assumes
every node that hosts sandboxes runs Kata.
:::

## Before you begin

- Kata Containers installed on the sandbox nodes, with a RuntimeClass for it.
  Your platform may provide one; for example, AKS Pod Sandboxing provides
  `kata-vm-isolation`.
- A StorageClass that provisions volumes dynamically, for the sandboxes' PVCs.

## Find the Kata RuntimeClass and handler

List the RuntimeClasses and their CRI handlers:

```sh
kubectl get runtimeclass -o custom-columns=NAME:.metadata.name,HANDLER:.handler
```

Note the name of the Kata RuntimeClass and its handler. Check that the Kata nodes
report the same handler:

```sh
kubectl get node <kata-node> -o jsonpath='{.status.runtimeHandlers[*].name}'
```

Sandbawks treats a RuntimeClass named `kata`, or starting with `kata-`, as Kata.

## Configure the node DaemonSet for Kata

Set `node.runtime` to `kata` in your Helm values. If the Kata nodes are tainted,
add a matching toleration so the DaemonSet runs on them:

```yaml
node:
  runtime: kata
  # Only if the handler isn't `kata`, for example `kata-cc` for confidential Kata.
  # runtimeHandler: kata-cc
  tolerations:
    - key: kata
      operator: Equal
      value: "true"
      effect: NoSchedule
```

```sh
helm upgrade sandbawks oci://ghcr.io/hypotheosis/charts/sandbawks \
  --version 0.1.29 --namespace sandbawks --reuse-values -f kata-values.yaml
```

With `node.runtime: kata`, the node waits for the `kata` handler, or the
`node.runtimeHandler` you set, before it marks a node ready for sandboxes. Check
that the Kata nodes have the label:

```sh
kubectl get nodes -L sandbawks.dev/envd-staged
```

## Create a Kata template

Create a `SandboxTemplate` that uses the Kata RuntimeClass and the `Filesystem`
persistence mode:

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-kata
  namespace: team-acme
spec:
  image: registry.example.com/sandboxes/python:3.12
  defaultRuntimeClass: kata-vm-isolation
  defaultTimeoutSeconds: 1800
  defaultResources:
    cpu: "1"
    memory: 1Gi
  persistenceMode: Filesystem
  securityContext:
    fsGroup: 1000
  tolerations:
    - key: kata
      operator: Equal
      value: "true"
      effect: NoSchedule
  volumeClaimTemplates:
    - metadata:
        name: workspace
        annotations:
          sandbawks.dev/mount-path: /home/user/workspace
      spec:
        storage: 10Gi
```

```sh
kubectl apply -f python-kata.yaml
```

- `defaultRuntimeClass`: the Kata RuntimeClass you found earlier.
- `persistenceMode: Filesystem`: required. Without it, or volume claim templates
  that imply it, the effective mode is `Snapshot`, which Kata doesn't support. The
  API gateway then rejects create requests with the error code
  `unsupported_persistence_mode`.
- `volumeClaimTemplates`: the data that survives a pause. Leave this out if your
  sandboxes don't need to keep files; they can still pause, but they resume empty.
- `tolerations`: needed if the Kata nodes are tainted and the RuntimeClass doesn't
  add the toleration itself.

## Create and pause a Kata sandbox

Create a sandbox from the template with your E2B SDK:

```python
from e2b import Sandbox

sbx = Sandbox.create(template="python-kata", timeout=600)
sbx.files.write("/home/user/workspace/notes.txt", "kept across pauses")
sbx.pause()

resumed = Sandbox.connect(sbx.sandbox_id)
print(resumed.files.read("/home/user/workspace/notes.txt"))
```

Check that the sandbox runs under Kata:

```sh
kubectl get sandboxes -n team-acme
```

The `Runtime` column shows the Kata RuntimeClass.

## See also

- [Keep sandbox files on persistent volumes](/how-to/keep-sandbox-files-on-persistent-volumes/)
  — volume claim templates and PVC retention.
- [Run sandboxes on a dedicated node pool](/how-to/run-sandboxes-on-a-dedicated-node-pool/)
  — `node.tolerations` and template placement.
- [Helm chart reference](/reference/helm-chart/) — `node.runtime` and
  `node.runtimeHandler`.
