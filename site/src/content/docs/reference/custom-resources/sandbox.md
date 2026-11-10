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
| `timeoutSeconds` | integer (`uint32`) | Yes | — | Seconds before the sandbox is auto-paused (if `autoPause` is `true`) or terminated (if `false`). |
| `autoPause` | boolean | No | `false` | If `true`, the timeout transitions the sandbox to `Paused`. If `false`, the timeout transitions to `Terminating`. |
| `desiredPhase` | string (enum) | No | null | Phase the API gateway wants the controller to drive toward. `Paused` requests a pause; `Running` requests a resume. Enum values match [Phases](#phases). |
| `runtimeClassName` | string | No | null | Overrides the template's default RuntimeClass. |
| `env` | map[string]string | No | — | Environment variables injected into the sandbox process. |
| `metadata` | map[string]string | No | — | Opaque key/value passed through from the E2B API. Not interpreted by the controller; surfaced back via `GET /sandboxes`. |
| `networkPolicy` | object | No | null | Optional egress policy. When omitted, egress is deny-all by default. See [Network policy](#network-policy). |
| `snapshotRetention` | string (ISO-8601 duration) | No | null | How long to retain this sandbox's snapshot after the controller writes it, for example `P7D`, `PT24H`, or `PT30M`. See [Snapshot retention](#snapshot-retention). |

### Resources

`spec.resources` defines the per-sandbox resource budget. `cpu` and `memory` map
onto the Pod container's `resources.limits`; `ephemeralStorage` maps onto the
Pod's ephemeral-storage limit.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cpu` | string | Yes | CPU limit, as a Kubernetes quantity. |
| `memory` | string | Yes | Memory limit, as a Kubernetes quantity. |
| `ephemeralStorage` | string | No | Ephemeral-storage limit, as a Kubernetes quantity. |

### Network policy

`spec.networkPolicy` is optional. When omitted, the controller attaches a
default-deny NetworkPolicy.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `egressAllow` | array[string] | `[]` | Egress destinations (DNS names) to permit. An empty list permits no egress. |

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
| `snapshotJobRef` | string | Name of the in-flight `SnapshotJob` driving the current pause or resume. Cleared when the job reaches a terminal phase. |
| `restoreStagingPath` | string | Local filesystem path on the staging node where a `Stage` `SnapshotJob` materialized the snapshot's parts, used by a start-time restore. |

### Conditions

Each entry of `status.conditions` is a conventional Kubernetes status condition.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Condition type. |
| `status` | string | Yes | Condition status. |
| `reason` | string | No | Machine-readable reason. |
| `message` | string | No | Human-readable message. |
| `lastTransitionTime` | string | No | Timestamp of the last transition. |

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

## See also

- [SandboxTemplate](/reference/custom-resources/sandboxtemplate/) — the template `templateRef` points to.
- [SnapshotJob](/reference/custom-resources/snapshotjob/) — the job that drives a pause or resume.
