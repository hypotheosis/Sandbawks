---
title: Clone a sandbox
description: Create a new sandbox from another sandbox's saved state with restoreFrom, through the API or a Sandbox resource, and check that the clone succeeded.
slug: how-to/clone-a-sandbox
head:
  - tag: title
    content: Clone a sandbox · Sandbawks docs
tableOfContents: true
---

This guide shows you how to start a new sandbox from the saved state of an
existing one, for example to fork an agent's environment at a checkpoint. The new
sandbox sets `restoreFrom` to the source sandbox's name. How the state is copied
depends on the clone's persistence mode:

- `Snapshot` mode: the clone restores the source's most recent snapshot, which
  includes its memory, processes, and files.
- `Filesystem` mode: each of the clone's PVCs is cloned from the source's PVC of
  the same claim name, and the clone starts a fresh Pod on the copies. Only data
  on the PVCs carries over.

## Before you begin

- The source sandbox is in the same namespace as the clone.
- The clone uses the same runtime class as the source. In `Snapshot` mode it also
  uses the same template image, usually because both use the same template.
- In `Snapshot` mode, the source has been paused at least once, so its
  `status.snapshotRef` is set. The source can be in any phase, including `Failed`,
  as long as its `Sandbox` still exists.
- In `Filesystem` mode, the source is paused, and your StorageClass's CSI driver
  supports volume cloning. The clone must use the same claim names as the source,
  which it does when both use the same template. See
  [Keep sandbox files on persistent volumes](/how-to/keep-sandbox-files-on-persistent-volumes/).

## Pause the source sandbox

Pause the source with your E2B SDK, for example `sbx.pause()` in Python. Then
check that it has saved state.

For a `Snapshot`-mode source, check for a snapshot reference:

```sh
kubectl get sandbox <source-id> -n team-acme -o jsonpath='{.status.snapshotRef}'
```

For a `Filesystem`-mode source, check that the `Suspended` condition is `True`:

```sh
kubectl get sandbox <source-id> -n team-acme \
  -o jsonpath='{.status.conditions[?(@.type=="Suspended")].status}'
```

## Create the clone through the API

Send a create request with `restoreFrom` set to the source sandbox's ID. The API
also accepts the field as `sourceSandboxID`. The E2B SDKs don't send this field,
so call the API directly:

```sh
curl -s -X POST "$E2B_API_URL/sandboxes" \
  -H "X-API-Key: $E2B_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"templateID": "python-base", "timeout": 600, "restoreFrom": "<source-id>"}'
```

The response contains the clone's `sandboxID`. Connect to it with your SDK as you
would to any sandbox, for example `Sandbox.connect(clone_id)` in Python.

:::caution
If the template has a `SandboxWarmPool` with a ready member, the API currently
hands out that warm member and ignores `restoreFrom`, so you get a fresh sandbox
instead of a clone ([#6](https://github.com/hypotheosis/Sandbawks/issues/6)). For
templates with a warm pool, create the clone as a `Sandbox` resource instead.
:::

## Create the clone as a Sandbox resource

You can also apply a `Sandbox` that sets `spec.restoreFrom`:

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: Sandbox
metadata:
  name: agent-fork-1
  namespace: team-acme
spec:
  templateRef: python-base
  timeoutSeconds: 600
  autoPause: true
  resources:
    cpu: "1"
    memory: 1Gi
  restoreFrom: <source-id>
```

```sh
kubectl apply -f agent-fork-1.yaml
```

## Check the clone

Watch the clone's phase:

```sh
kubectl get sandbox agent-fork-1 -n team-acme -w
```

A `Snapshot`-mode clone moves from `Creating` through `Resuming` to `Running`; a
`Filesystem`-mode clone moves from `Creating` to `Running`. In `Filesystem` mode,
the clone's PVCs appear as `<claim-name>-agent-fork-1`.

If the phase becomes `Failed`, read the reason from the `Failed` condition:

```sh
kubectl get sandbox agent-fork-1 -n team-acme \
  -o jsonpath='{.status.conditions[?(@.type=="Failed")].message}'
```

| Message mentions | What to do |
|------------------|------------|
| `not found in namespace` | Use the name of a source sandbox in the clone's namespace. |
| `has no snapshot recorded` | Pause the source in `Snapshot` mode first. |
| `is not Suspended` | Pause the `Filesystem`-mode source first. |
| `runtime class ... does not match` | Give the clone the source's runtime class. |
| `template image ... does not match` | Use a template with the source's image. |
| `raw s3:// snapshot URLs are not yet supported` | Pass the source sandbox's name instead of a snapshot URL. |

:::caution
A `Snapshot`-mode clone keeps the source's identity inside the sandbox: the
kernel hostname (`uname -n`), the network interface's MAC and IP address, and any
`/etc/machine-id`. Sandbawks doesn't reset these yet. If your workload registers
itself by hostname or machine ID, give each clone its own identity yourself, or
use `Filesystem` mode, whose clones start with a fresh identity.
:::

## See also

- [Sandbox reference](/reference/custom-resources/sandbox/) — `restoreFrom` and
  `persistenceMode`.
- [Keep sandbox files on persistent volumes](/how-to/keep-sandbox-files-on-persistent-volumes/)
  — set up `Filesystem` mode.
