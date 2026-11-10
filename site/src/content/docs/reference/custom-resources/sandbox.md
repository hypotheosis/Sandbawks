---
title: Sandbox
description: Reference for the sandbawks.dev Sandbox custom resource — spec fields, status fields, lifecycle phases, and printer columns.
slug: reference/custom-resources/sandbox
head:
  - tag: title
    content: Sandbox · Sandbawks docs
tableOfContents: true
---

A `Sandbox` represents a single sandbox: one E2B sandbox, backed by one Pod
while running. The controller drives the Pod and snapshot lifecycle from the
`spec` and records observed state in `status`.

## Resource identity

| Property | Value |
|----------|-------|
| API group | `sandbawks.dev` |
| Version | `v1alpha1` |
| Kind | `Sandbox` |
| List kind | `SandboxList` |
| Plural | `sandboxes` |
| Singular | `sandbox` |
| Short name | `sbx` |
| Scope | Namespaced |
| Status subresource | Yes |

## Spec fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `templateRef` | string | Yes | — | Name of a `SandboxTemplate` in the same namespace. |
| `resources` | object | Yes | — | Per-sandbox resource budget. See [Resources](#resources). |
| `timeoutSeconds` | integer (`uint32`) | Yes | — | Idle window in seconds: how long the sandbox can go with no client activity before it is auto-paused (if `autoPause` is `true`) or terminated (if `false`). `0` disables the idle timeout. See [Idle timeout](#idle-timeout). |
| `autoPause` | boolean | No | `false` | If `true`, the idle timeout transitions the sandbox to `Paused`. If `false`, it transitions to `Terminating`. |
| `desiredPhase` | string (enum) | No | null | Phase the API gateway wants the controller to drive toward. `Paused` requests a pause; `Running` requests a resume. Enum values match [Phases](#phases). |
| `runtimeClassName` | string | No | null | Overrides the template's default RuntimeClass. |
| `env` | map[string]string | No | — | Environment variables injected into the sandbox process. The controller stores the values in a per-sandbox Secret and references them from the Pod, so they don't appear in the Pod object. |
| `metadata` | map[string]string | No | — | Opaque key/value passed through from the E2B API. Not interpreted by the controller; surfaced back via `GET /sandboxes`. |
| `networkPolicy` | object | No | null | Optional egress policy. When omitted, egress is deny-all by default. See [Network policy](#network-policy). |
| `snapshotRetention` | string (ISO-8601 duration) | No | null | How long to retain this sandbox's snapshot after the controller writes it, for example `P7D`, `PT24H`, or `PT30M`. See [Snapshot retention](#snapshot-retention). |
| `pvcRetention` | string (ISO-8601 duration) | No | null | How long a paused `Filesystem`-mode sandbox can keep its PVCs before the controller deletes the sandbox and its PVCs. See [PVC retention](#pvc-retention). |
| `tolerations` | array[object] | No | null | Tolerations added to the sandbox Pod. When null, the template's `tolerations` apply. See [Scheduling](#scheduling). |
| `nodeSelector` | map[string]string | No | null | Node-selector labels added to the sandbox Pod. When null, the template's `nodeSelector` applies. See [Scheduling](#scheduling). |
| `securityContext` | object | No | null | Pod-level security context for the sandbox Pod. When null, the template's `securityContext` applies. See [Security context](#security-context). |
| `persistenceMode` | string (enum) | No | null | How the sandbox's writable state survives a pause. When null, the template's value applies, then the inferred default. See [Persistence](#persistence). |
| `volumeClaimTemplates` | array[object] | No | null | PersistentVolumeClaims the sandbox owns. Merged with the template's claims under the template's `volumeClaimTemplatesPolicy`. See [Volume claim templates](#volume-claim-templates). |
| `restoreFrom` | string | No | null | Creates this sandbox from the saved state of another sandbox instead of cold-booting. See [Restore from](#restore-from). |

### Resources

`spec.resources` defines the per-sandbox resource budget. `cpu` and `memory` map
onto the Pod container's `resources.limits`; `ephemeralStorage` maps onto the
Pod's ephemeral-storage limit.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cpu` | string | Yes | CPU limit, as a Kubernetes quantity. |
| `memory` | string | Yes | Memory limit, as a Kubernetes quantity. |
| `ephemeralStorage` | string | No | Ephemeral-storage limit, as a Kubernetes quantity. |

### Idle timeout

`spec.timeoutSeconds` is an idle window, not an absolute deadline. The
controller measures it from `status.lastActivityTimestamp`, which the API
gateway updates on every request it proxies to the sandbox (filesystem,
process, and exec or PTY streams). Until the first activity is recorded, the
window is measured from `metadata.creationTimestamp`. Creating a sandbox,
setting its timeout, and claiming it from a warm pool also restart the window.

A value of `0` disables the idle timeout. Warm-pool members are created with `0`,
so they never time out while unclaimed.

### Network policy

The controller creates a `networking.k8s.io/v1` NetworkPolicy for every sandbox
that denies all egress by default. `spec.networkPolicy.egressAllow` opens
specific destinations on top of that baseline.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `egressAllow` | array[string] | `[]` | Egress destinations to permit. An empty list permits no egress. |

Each `egressAllow` entry is an IP address, a CIDR block, or a DNS name:

- IP and CIDR entries (for example `10.0.0.0/8` or `2001:db8::1`) become
  `ipBlock` egress rules on the baseline NetworkPolicy.
- A DNS-name entry opens DNS egress (port 53) on the baseline NetworkPolicy. A
  standard NetworkPolicy can't express a DNS-name destination, so the connection
  to the resolved address is permitted only when FQDN egress is enabled
  (`controller.fqdnEgress.enabled` Helm value). The controller then also creates a
  Cilium `CiliumNetworkPolicy` with a `toFQDNs` rule for each DNS name. FQDN
  egress requires the Cilium CNI.

Enforcing any NetworkPolicy requires a NetworkPolicy-aware CNI.

When a sandbox is created through the E2B API, the API gateway sets
`spec.networkPolicy` to the union of the template's `networkPolicy` and any
destinations in the create request. Warm-pool members get the template's
`networkPolicy` when they are created. A `Sandbox` you apply directly with
`kubectl` doesn't inherit the template's `networkPolicy`.

### Snapshot retention

`spec.snapshotRetention` is an ISO-8601 duration string. When set, it takes
precedence over the per-namespace annotation `sandbawks.dev/snapshot-retention`
and the controller-wide default. The resolution order at garbage-collection time
is:

1. `spec.snapshotRetention`
2. The namespace annotation `sandbawks.dev/snapshot-retention`
3. The controller-wide default (`snapshotStore.retention.defaultDuration` Helm value)

When the field is null, resolution falls through to the namespace or controller
default.

### PVC retention

`spec.pvcRetention` is an ISO-8601 duration string that applies to sandboxes
whose effective `persistenceMode` is `Filesystem`. While such a sandbox is
paused, its PVCs keep their storage. Once the sandbox has been paused for longer
than the retention window, the controller deletes the `Sandbox`, and Kubernetes
garbage-collects its PVCs through their owner references. The window is measured
from when the sandbox finished suspending (the transition time of its
`Suspended` condition), so resuming and pausing again restarts it.

The resolution order is:

1. `spec.pvcRetention`
2. The controller-wide default (`controller.pvcRetention.defaultDuration` Helm
   value, `P7D` by default)

The controller collects paused sandboxes only when PVC retention is enabled
controller-wide (`controller.pvcRetention.enabled` Helm value). It is enabled by
default.

:::caution
PVC retention deletes the paused `Sandbox` and the data on its PVCs. To keep paused
PVCs until you delete them yourself, set `controller.pvcRetention.enabled` to
`false`, or set a longer `spec.pvcRetention`.
:::

### Scheduling

`spec.tolerations` and `spec.nodeSelector` are added to the sandbox Pod, so it
can run on a tainted or labeled node pool, such as a dedicated Kata node pool.
When a field is null, the controller uses the template's value. A value set on
the `Sandbox` replaces the template's value; the two aren't merged. The controller
applies these when it creates the Pod, so cold-started sandboxes and warm-pool
members both get them.

Each entry of `spec.tolerations` has the shape of a Kubernetes Pod toleration.

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Taint key the toleration applies to. Empty, with `operator: Exists`, matches all keys. |
| `operator` | string | `Exists` or `Equal`. Kubernetes defaults to `Equal`. |
| `value` | string | Taint value to match. Empty with `operator: Exists`. |
| `effect` | string | Taint effect to match: `NoSchedule`, `PreferNoSchedule`, or `NoExecute`. Empty matches all effects. |
| `tolerationSeconds` | integer (`int64`) | For a `NoExecute` toleration, how long the Pod stays bound after the taint is added. Unset means forever. |

### Security context

`spec.securityContext` is set as the sandbox Pod's `spec.securityContext`. When it
is null, the template's value applies; a value on the `Sandbox` replaces the
template's value. The most common use is `fsGroup`, which lets a non-root
workload write to a PVC created from `volumeClaimTemplates`. Without it, the
volume is owned by `root:root`.

| Field | Type | Description |
|-------|------|-------------|
| `fsGroup` | integer (`int64`) | Supplemental group that owns the Pod's volumes. The kubelet changes each volume's ownership to this group, with the setgid bit. |
| `fsGroupChangePolicy` | string | When the kubelet applies `fsGroup` ownership: `Always` or `OnRootMismatch`. |
| `runAsUser` | integer (`int64`) | UID that every container in the Pod runs as. |
| `runAsGroup` | integer (`int64`) | Primary GID that every container in the Pod runs as. |
| `runAsNonRoot` | boolean | If `true`, the kubelet refuses to start a container that would run as UID 0. |
| `supplementalGroups` | array[integer] | Extra GIDs added to every container's processes. |

### Persistence

`spec.persistenceMode` selects how the sandbox's writable state survives a pause.

| Value | Meaning |
|-------|---------|
| `Snapshot` | Pause checkpoints memory and filesystem through the runtime (gVisor `runsc checkpoint`) to the snapshot store, and resume restores the checkpoint. |
| `Filesystem` | Pause deletes the Pod and keeps the sandbox's PVCs. Resume starts a fresh Pod that mounts the same PVCs. Memory isn't preserved; only data on the PVCs survives. |

The effective mode is the first of these that applies:

1. `spec.persistenceMode` on the `Sandbox`
2. `persistenceMode` on the `SandboxTemplate`
3. `Filesystem`, if the effective set of volume claim templates is not empty
4. `Snapshot`

`Snapshot` is rejected for a Kata runtime class: a RuntimeClass named `kata` or
starting with `kata-`. To pause Kata sandboxes, use `Filesystem`. The API gateway
rejects the request at create time, and the controller rejects a `Sandbox`
applied directly.

### Volume claim templates

Each entry of `spec.volumeClaimTemplates` produces one PersistentVolumeClaim that
the sandbox owns. The PVC is named `<claim-name>-<sandbox-name>`, so it is
garbage-collected with the `Sandbox`, and it outlives the Pod across a
`Filesystem`-mode pause.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `metadata.name` | string | Yes | Claim name, unique within the sandbox. Must be a DNS-1123 label: lowercase alphanumerics and `-`, starting and ending with an alphanumeric, at most 63 characters. Also used as the Pod volume name. |
| `metadata.labels` | map[string]string | No | Labels added to the PVC. |
| `metadata.annotations` | map[string]string | No | Annotations added to the PVC. The `sandbawks.dev/mount-path` annotation sets where the volume is mounted in the sandbox. |
| `spec.storage` | string | Yes | Requested size, as a Kubernetes quantity, for example `10Gi`. Becomes the PVC's `resources.requests.storage`. |
| `spec.accessModes` | array[string] | No | Access modes, for example `ReadWriteOnce`. Empty uses the provisioner's default. |
| `spec.storageClassName` | string | No | StorageClass to provision from. Unset uses the cluster's default StorageClass. |
| `spec.volumeMode` | string | No | `Filesystem` or `Block`. Unset uses the Kubernetes default, `Filesystem`. |

The volume is mounted at the path in the `sandbawks.dev/mount-path` annotation.
Without the annotation, it is mounted at `/<claim-name>`; for example, a claim
named `workspace` is mounted at `/workspace`.

The effective set of claims combines the template's claims with the
`Sandbox`'s claims under the template's `volumeClaimTemplatesPolicy`:

- When `spec.volumeClaimTemplates` is null, the template's claims apply.
- When it is set, the template's policy decides which of the `Sandbox`'s claims
  are kept. See [SandboxTemplate](/reference/custom-resources/sandboxtemplate/).

Claims the policy drops don't fail the sandbox. The controller records them in a
`VolumeClaimTemplatesPolicy` condition with status `False` and reason
`ClaimsSuppressed`.

### Restore from

`spec.restoreFrom` creates a new sandbox from the saved state of another sandbox.
The value is one of these:

- The name of a source `Sandbox` in the same namespace. The source can be in any
  phase, including `Failed`, as long as the `Sandbox` still exists and has the
  saved state the effective persistence mode needs.
- An `s3://` URL of a snapshot manifest. The controller recognizes this form but
  doesn't act on it yet: it moves the sandbox to `Failed` with a message asking for
  the source sandbox's name instead.

How the controller restores depends on the new sandbox's effective
`persistenceMode`:

- `Snapshot`: the controller copies the source's snapshot reference and digests to
  this sandbox's status, then restores it the same way it resumes a paused
  sandbox.
- `Filesystem`: each PVC the new sandbox creates is cloned from the source's
  PVC of the same claim name, through the PVC `dataSource`, and a fresh Pod
  starts on the cloned volumes. The source must be suspended (paused in
  `Filesystem` mode).

Before restoring, the controller checks the source against the new sandbox's
runtime class and template image. On a mismatch, the sandbox moves to `Failed`
with a condition. For snapshots written before Sandbawks recorded the template
image, the image check only logs a warning.

:::caution
A `Snapshot`-mode clone restores the source's guest kernel, so it keeps the
source's identity: the kernel hostname (`uname -n`), the network interface's MAC
and IP address inside the sandbox, and any `/etc/machine-id`. `/etc/hostname`
reflects the new Pod, so the two disagree. Sandbawks doesn't reset this identity
yet. Don't rely on hostname or machine ID to tell a clone from its source. A
`Filesystem`-mode clone starts a fresh Pod and gets a new identity.
:::

## Status fields

`status` is written by the controller.

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string (enum) | Lifecycle phase. Defaults to `Creating`. See [Phases](#phases). |
| `assignedNode` | string | Node currently hosting the sandbox Pod. Null when `Paused`, or `Creating` before scheduling. |
| `podRef` | string | Reference to the backing Pod, if any. |
| `podUid` | string | UID of the Pod, mirrored from `Pod.metadata.uid` once the controller observes the Pod. |
| `envdAddress` | string | `host:port` of the in-sandbox `envd` agent when running. |
| `lastTransitionTime` | string (RFC 3339) | Timestamp of the last phase transition. |
| `conditions` | array[object] | Kubernetes-style status conditions. See [Conditions](#conditions). |
| `snapshotRef` | string | Storage URL (for example `s3://...`) of the snapshot when `Paused`. |
| `snapshotSha256` | string | Aggregate SHA-256 of the snapshot referenced by `snapshotRef`. |
| `snapshotEnvdSha256` | string | SHA-256 of the node-staged `envd` binary the snapshot was checkpointed against. |
| `snapshotRunscVersion` | string | gVisor `runsc` release the snapshot was checkpointed with, for example `release-20260525.0`. On resume, the controller places the restore Pod only on nodes that run the same release. Null for older snapshots and when the node couldn't read its `runsc` version. |
| `snapshotJobRef` | string | Name of the in-flight `SnapshotJob` driving the current pause or resume. Cleared when the job reaches a terminal phase. |
| `restoreStagingPath` | string | Local filesystem path on the staging node where a `Stage` `SnapshotJob` materialized the snapshot's parts, used by a start-time restore. |
| `resumeRestartCount` | integer (`uint32`) | Number of times the controller has recreated the restore Pod during the current resume. Reset to `0` on each new resume. When it exceeds the limit (`controller.restore.maxRestarts` Helm value), the sandbox moves to `Failed` with `failureReason` `resume_exhausted`. |
| `resumeLastAttempt` | string (RFC 3339) | Timestamp of the most recent restore-Pod creation during the current resume. |
| `pauseRestartCount` | integer (`uint32`) | Number of times the controller has retried the pause checkpoint during the current pause. Reset to `0` on each new pause. When it reaches the limit (`controller.pauseRecovery.maxRetries` Helm value), the sandbox moves to `Failed` with `failureReason` `pause_checkpoint_failed`. |
| `pauseLastAttempt` | string (RFC 3339) | Timestamp of the most recent pause-checkpoint attempt during the current pause. |
| `failureReason` | string | Why the sandbox most recently moved to `Failed`. See [Failure reasons](#failure-reasons). Null if it has never failed. |
| `failedAt` | string (RFC 3339) | Timestamp the sandbox most recently moved to `Failed`. Cleared when a recoverable failure is recovered back to `Running`. |
| `lastActivityTimestamp` | string (RFC 3339) | Timestamp of the last client activity the API gateway recorded. The idle timeout is measured from it. See [Idle timeout](#idle-timeout). |
| `podStatus` | object | Summary of the backing Pod while the sandbox isn't yet serving requests. Null once `envd` is serving. See [Pod status](#pod-status). |

### Failure reasons

`status.failureReason` holds one of these values:

| Value | Meaning |
|-------|---------|
| `pod_vanished_creating` | The Pod disappeared while the sandbox was being created. |
| `pod_terminated_running` | The Pod terminated while the sandbox was running. |
| `pod_evicted` | The Pod was evicted. |
| `snapshot_job_failed` | A `SnapshotJob` for the sandbox failed. |
| `action_failure` | A controller action failed. |
| `resume_exhausted` | The resume ran out of restore-Pod retries. |
| `pause_checkpoint_failed` | The pause checkpoint ran out of retries. The workload Pod may still be running, so the controller keeps trying to recover the sandbox. |

### Pod status

`status.podStatus` lets the API gateway tell a client why a sandbox isn't ready
yet, for example whether it is still starting or stuck on an image pull.

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | The Pod's `status.phase`: `Pending`, `Running`, `Succeeded`, `Failed`, or `Unknown`. |
| `reason` | string | The most significant container waiting or terminated reason that is blocking the Pod, for example `ImagePullBackOff` or `CrashLoopBackOff`. Null while the Pod is still progressing without an error. |
| `message` | string | The kubelet's message for `reason`, when it has one. |

### Conditions

Each entry of `status.conditions` is a conventional Kubernetes status condition.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Condition type. |
| `status` | string | Yes | Condition status. |
| `reason` | string | No | Machine-readable reason. |
| `message` | string | No | Human-readable message. |
| `lastTransitionTime` | string | No | Timestamp of the last transition. |

The controller sets these condition types:

| Type | Set when |
|------|----------|
| `Failed` | The sandbox moves to `Failed`. The `reason` is `ActionFailure`. A new entry is added each time. |
| `Suspended` | A `Filesystem`-mode pause is in progress (status `False`, reason `Suspending`) or complete (status `True`, reason `PodReleased`). |
| `VolumeClaimTemplatesPolicy` | The template's policy dropped one or more of the sandbox's volume claim templates (status `False`, reason `ClaimsSuppressed`). |
| `StateRestored` | The Pod was evicted and the controller is restoring the sandbox from its last snapshot (reason `EvictedRestoredFromSnapshot`). |
| `StateLost` | The Pod was evicted with no snapshot to restore, so the controller is starting a fresh sandbox (reason `EvictedNoSnapshot`). |
| `OrphanReclaimed` | After an unrecoverable pause-checkpoint failure, the controller deleted the orphaned workload Pod (reason `PauseCheckpointOrphanGC`). |

## Phases

`status.phase` and `spec.desiredPhase` use the same set of values:

| Value | Meaning |
|-------|---------|
| `Creating` | The sandbox is being created and scheduled. |
| `Running` | The sandbox Pod is running and ready. |
| `Pausing` | A pause is in progress. |
| `Paused` | The sandbox is paused; its state is held in a snapshot. |
| `Resuming` | A resume is in progress. |
| `Terminating` | The sandbox is being torn down. |
| `Failed` | The sandbox entered a failure state. |

## Additional printer columns

These columns appear in `kubectl get sandboxes` output.

| Column | Source |
|--------|--------|
| `Phase` | `.status.phase` |
| `Claimed` | `.metadata.annotations.sandbawks\.dev/claimed-at` — when a warm-pool member was claimed. Empty for an unclaimed member. |
| `Template` | `.spec.templateRef` |
| `Runtime` | `.spec.runtimeClassName` |
| `Node` | `.status.assignedNode` |
| `Age` | `.metadata.creationTimestamp` |

## Example

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: Sandbox
metadata:
  name: example
  namespace: team-acme
spec:
  templateRef: python-base
  timeoutSeconds: 3600
  autoPause: true
  resources:
    cpu: "1"
    memory: 1Gi
    ephemeralStorage: 4Gi
  env:
    LOG_LEVEL: debug
  networkPolicy:
    egressAllow:
      - pypi.org
  snapshotRetention: P7D
```

A sandbox that keeps its files on a PVC across pauses:

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: Sandbox
metadata:
  name: example-workspace
  namespace: team-acme
spec:
  templateRef: python-base
  timeoutSeconds: 1800
  autoPause: true
  resources:
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
  pvcRetention: P7D
```

Because `volumeClaimTemplates` is set and `persistenceMode` isn't, the effective
mode is `Filesystem`. The template's `volumeClaimTemplatesPolicy` must allow
the `Sandbox`'s own claims.

## See also

- [SandboxTemplate](/reference/custom-resources/sandboxtemplate/) — the template `templateRef` points to.
- [SnapshotJob](/reference/custom-resources/snapshotjob/) — the job that drives a pause or resume.
