---
title: SnapshotJob
description: Reference for the sandbawks.dev SnapshotJob custom resource — spec fields, status fields, modes, phases, the snapshot manifest, and printer columns.
slug: reference/custom-resources/snapshotjob
head:
  - tag: title
    content: SnapshotJob · Sandbawks docs
tableOfContents: true
---

A `SnapshotJob` is the desired and observed state of a single snapshot operation
on one node. One `SnapshotJob` corresponds to exactly one container on one node.
The controller creates these resources and sets `spec.nodeName` and
`spec.containerId`; the per-node worker reconciles them and writes `status`.

## Resource identity

| Property | Value |
|----------|-------|
| API group | `sandbawks.dev` |
| Version | `v1alpha1` |
| Kind | `SnapshotJob` |
| List kind | `SnapshotJobList` |
| Plural | `snapshotjobs` |
| Singular | `snapshotjob` |
| Short name | `sbxsj` |
| Scope | Namespaced |
| Status subresource | Yes |
| Selectable field | `.spec.nodeName` |

## Spec fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mode` | string (enum) | Yes | Direction of the operation. See [Modes](#modes). |
| `nodeName` | string | Yes | `Node.metadata.name` the per-node worker must run on. The node-side reconciler filters its watch by this field. |
| `sandboxName` | string | Yes | Name of the `Sandbox` this job operates on, in the same namespace. |
| `containerId` | string | Yes | CRI container ID for the sandbox's primary container, populated by the controller at job-creation time. |
| `snapshotRef` | string | No | For `Resume`, the snapshot URL to restore from (typically `s3://...`). Null for `Pause` jobs. |
| `expectedSha256` | string | No | For `Resume`, the aggregate SHA-256 the controller expects the fetched manifest to match. Null for `Pause` jobs and legacy callers. |
| `expectedEnvdSha256` | string | No | For `Resume` and `Stage`, the `envd`-binary SHA-256 the controller observed at pause. Null for `Pause` jobs and manifests that recorded no `envd` digest. |
| `targetPath` | string | No | For `Stage`, the local path on the node to stage the snapshot into. The controller sets it so the restore Pod can be created, with this path, before staging finishes. Null for `Pause` jobs and for `Stage` jobs that let the node choose the path. |

### Modes

`spec.mode` selects the direction of the operation.

| Value | Meaning |
|-------|---------|
| `Pause` | Checkpoint a running container to a snapshot. |
| `Resume` | Restore a container from a snapshot. |
| `Stage` | Materialize a snapshot's parts on a node's local filesystem for a start-time restore. |

## Status fields

`status` is written by the per-node worker.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `phase` | string (enum) | `Pending` | Current phase. See [Phases](#phases). |
| `startedAt` | string (RFC 3339) | — | Timestamp the worker first picked the job up. |
| `completedAt` | string (RFC 3339) | — | Timestamp the worker entered a terminal phase (`Complete` or `Failed`). |
| `message` | string | — | Human-readable diagnostic message. Populated on `Failed` with the underlying error, or to surface progress on long-running phases. |
| `manifest` | object | — | On `Complete` for a `Pause` job, the snapshot manifest. Null while in flight and for `Resume` jobs. See [Manifest](#manifest). |
| `localPath` | string | — | On `Complete` for a `Stage` job, the local filesystem path where the snapshot's parts were materialized. Null while in flight and for `Pause` and `Resume` jobs. |

### Phases

| Value | Meaning |
|-------|---------|
| `Pending` | The job has not been picked up yet. |
| `Checkpointing` | The worker is checkpointing the container. |
| `Uploading` | The worker is uploading snapshot parts. |
| `Validating` | The worker is validating snapshot parts. |
| `Restoring` | The worker is restoring the container. |
| `Complete` | The operation finished successfully. |
| `Failed` | The operation failed. See `status.message`. |

### Manifest

`status.manifest` describes a completed `Pause` snapshot.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `createdAt` | string (RFC 3339) | Yes | — | Timestamp the manifest was created. |
| `sandboxId` | string | Yes | — | The `Sandbox` the snapshot belongs to (the resource `metadata.name`). |
| `runtimeClass` | string | Yes | — | Runtime that produced the snapshot, for example `gvisor`, `kata-fc`, or `runc-criu`. |
| `sha256` | string | Yes | — | Aggregate SHA-256 over the sorted part hashes. Empty when `parts` is empty. |
| `snapshotRef` | string | No | `""` | Storage URL of the manifest, for example `s3://bucket/key`. |
| `envdSha256` | string | No | `""` | SHA-256 of the node-staged `envd` binary the paused sandbox was executing. Empty for older manifests or when the staged binary could not be read. |
| `runscVersion` | string | No | `""` | gVisor `runsc` release the checkpoint was taken with, for example `release-20260525.0`. A restore needs the same release, so the controller places the restore Pod on a node that runs it. Empty for older manifests or when the node couldn't read its `runsc` version. |
| `envdCaptured` | boolean | No | `false` | `true` when `envd` ran from a copy inside the sandbox's root filesystem at pause time, so its code is part of the checkpoint. The restore then skips the check that the node's `envd` binary matches `envdSha256`. |
| `templateRef` | string | No | `""` | Reserved for validating clones restored from a snapshot URL. Currently always empty. |
| `templateImage` | string | No | `""` | Reserved for validating clones restored from a snapshot URL. Currently always empty. |
| `parts` | array[object] | No | `[]` | Individual parts of the snapshot bundle. See [Manifest parts](#manifest-parts). |

#### Manifest parts

Each entry of `manifest.parts` is one uploaded chunk of a snapshot. The pair
`(name, sha256)` uniquely identifies a part.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Filename of the part inside the snapshot bundle, for example `memory.zst`. |
| `sha256` | string | Yes | Lower-case-hex SHA-256 of the part's bytes. |
| `sizeBytes` | integer (`uint64`) | Yes | On-the-wire (compressed) byte length the sink reported writing. |
| `uncompressedBytes` | integer (`uint64`) | No | Uncompressed payload size in bytes, when the producer knows it. Used by the resume-path memory-headroom gate. |

## Additional printer columns

These columns appear in `kubectl get snapshotjobs` output.

| Column | Source |
|--------|--------|
| `State` | `.status.phase` |
| `Mode` | `.spec.mode` |
| `Node` | `.spec.nodeName` |
| `Sandbox` | `.spec.sandboxName` |
| `Age` | `.metadata.creationTimestamp` |

## Example

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SnapshotJob
metadata:
  name: example-pause
  namespace: team-acme
spec:
  mode: Pause
  nodeName: node-3
  sandboxName: example
  containerId: containerd://a1b2c3d4
```

## See also

- [Sandbox](/reference/custom-resources/sandbox/) — `status.snapshotJobRef` names the in-flight job.
