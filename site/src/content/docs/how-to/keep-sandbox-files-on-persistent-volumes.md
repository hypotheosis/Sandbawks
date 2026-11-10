---
title: Keep sandbox files on persistent volumes
description: Give sandboxes a PersistentVolumeClaim that survives pause and resume by using the Filesystem persistence mode, and control how long paused volumes are kept.
slug: how-to/keep-sandbox-files-on-persistent-volumes
head:
  - tag: title
    content: Keep sandbox files on persistent volumes · Sandbawks docs
tableOfContents: true
---

This guide shows you how to give sandboxes a PersistentVolumeClaim (PVC) whose
files survive a pause and resume. In the `Filesystem` persistence mode, pausing a
sandbox deletes its Pod but keeps its PVCs, and resuming starts a fresh Pod that
mounts the same PVCs. Use it when you want a workspace that outlives the Pod, or
when you run sandboxes on Kata, which supports only this mode for pausing.

:::note
Only data on the PVCs survives a `Filesystem`-mode pause. Memory, running
processes, and files elsewhere in the sandbox, such as `/tmp`, are lost. The
default `Snapshot` mode preserves all of these, but it requires gVisor.
:::

## Before you begin

- Sandbawks 0.1.29 or later, with the matching CRDs applied
  (`kubectl apply -f deploy/crds`). The Helm chart grants the controller the
  PersistentVolumeClaim permissions it needs.
- A StorageClass that provisions volumes dynamically: either the cluster's
  default StorageClass or one you name in the claim.
- A `SandboxTemplate` you can edit. This guide uses `python-base` in the
  `team-acme` namespace.

## Add a volume claim template to the template

Add `volumeClaimTemplates` to the `SandboxTemplate`. Each entry becomes one PVC per
sandbox. When a template has volume claim templates and doesn't set
`persistenceMode`, its sandboxes use the `Filesystem` mode.

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
  securityContext:
    fsGroup: 1000
  volumeClaimTemplates:
    - metadata:
        name: workspace
        annotations:
          sandbawks.dev/mount-path: /home/user/workspace
      spec:
        storage: 10Gi
        accessModes:
          - ReadWriteOnce
```

Here's what the fields do:

- `metadata.name`: names the claim. It must be a DNS-1123 label, such as
  `workspace`. Each sandbox's PVC is named `<claim-name>-<sandbox-name>`.
- `sandbawks.dev/mount-path`: sets where the volume is mounted. Without the
  annotation, it is mounted at `/<claim-name>`, for example `/workspace`.
- `spec.storageClassName`: add this field to provision from a StorageClass other
  than the cluster default.
- `securityContext.fsGroup`: without it, the new volume is owned by `root`, and a
  workload that runs as a non-root user can't write to it. Set it to a group of
  the user your image runs as.

To make the mode explicit rather than inferred, also set
`persistenceMode: Filesystem`.

Apply the template:

```sh
kubectl apply -f python-base.yaml
```

## Create a sandbox and write to the volume

Create a sandbox from the template with your E2B SDK, and write a file to the
mounted volume:

```python
from e2b import Sandbox

sbx = Sandbox.create(template="python-base", timeout=600)
sbx.files.write("/home/user/workspace/notes.txt", "kept across pauses")
sandbox_id = sbx.sandbox_id
print(sandbox_id)
```

Check that the controller created the PVC:

```sh
kubectl get pvc -n team-acme
```

The output lists a PVC named `workspace-<sandbox-id>` with status `Bound`.

## Pause and resume the sandbox

Pause the sandbox, then reconnect to resume it:

```python
sbx.pause()

resumed = Sandbox.connect(sandbox_id)
print(resumed.files.read("/home/user/workspace/notes.txt"))
```

The file is still there. While the sandbox is paused, its Pod is gone but its PVC
remains:

```sh
kubectl get sandbox <sandbox-id> -n team-acme
kubectl get pods -n team-acme -l sandbawks.dev/sandbox=<sandbox-id>
kubectl get pvc workspace-<sandbox-id> -n team-acme
```

A paused sandbox shows the phase `Paused` and has a `Suspended` condition with
status `True` and reason `PodReleased`:

```sh
kubectl get sandbox <sandbox-id> -n team-acme \
  -o jsonpath='{.status.conditions[?(@.type=="Suspended")]}'
```

Sandboxes created through the E2B API also pause automatically after their idle
timeout, the `timeout` you pass to `Sandbox.create`, and keep their PVCs the same
way.

## Control how long paused volumes are kept

By default, the controller deletes a `Filesystem`-mode sandbox, and its PVCs with
it, after the sandbox has been paused for seven days.

:::caution
Deleting a paused sandbox deletes the data on its PVCs.
:::

- To change the default for all sandboxes, set
  `controller.pvcRetention.defaultDuration` in your Helm values, for example
  `P30D`.
- To keep paused PVCs until you delete them yourself, set
  `controller.pvcRetention.enabled` to `false`.
- To set a window for one sandbox, set `spec.pvcRetention` on its `Sandbox`, for
  example `P1D`.

The window starts when the pause finishes. Resuming and pausing again starts it
over.

## Let a sandbox add its own volumes

By default, a `Sandbox` can't add volume claim templates of its own; only the
template's claims are created. To let sandboxes add claims, set
`volumeClaimTemplatesPolicy` on the template:

- `Allowed`: a `Sandbox` can add claims under new names.
- `Overrides`: a `Sandbox` can also replace a template claim of the same name.

The E2B create request has no field for volume claims, so only a `Sandbox` you
apply with `kubectl` can add its own. For example:

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: Sandbox
metadata:
  name: data-job
  namespace: team-acme
spec:
  templateRef: python-base
  timeoutSeconds: 1800
  autoPause: true
  resources:
    cpu: "1"
    memory: 1Gi
  volumeClaimTemplates:
    - metadata:
        name: cache
      spec:
        storage: 5Gi
```

If the template's policy doesn't allow a claim, the controller drops it rather
than failing the sandbox, and it adds a `VolumeClaimTemplatesPolicy` condition
with reason `ClaimsSuppressed` that names the dropped claim.

## Use persistent volumes on Kata

Kata sandboxes can be paused only in the `Filesystem` mode. Sandbawks treats a
RuntimeClass named `kata`, or starting with `kata-`, as Kata. If the effective
mode for a Kata sandbox is `Snapshot`, the API gateway rejects the create request
with the error code `unsupported_persistence_mode`, and the controller rejects a
`Sandbox` you apply directly. Set `persistenceMode: Filesystem` on Kata
templates, or give them volume claim templates.

## See also

- [Sandbox reference](/reference/custom-resources/sandbox/) — `persistenceMode`,
  `volumeClaimTemplates`, `pvcRetention`, and the conditions they set.
- [SandboxTemplate reference](/reference/custom-resources/sandboxtemplate/) —
  `volumeClaimTemplatesPolicy` and how template values reach a sandbox.
- [Helm chart reference](/reference/helm-chart/) — the `controller.pvcRetention`
  values.
