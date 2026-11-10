---
title: SandboxWarmPool
description: Reference for the sandbawks.dev SandboxWarmPool custom resource — spec fields, status fields, and printer columns.
slug: reference/custom-resources/sandboxwarmpool
head:
  - tag: title
    content: SandboxWarmPool · Sandbawks docs
tableOfContents: true
---

A `SandboxWarmPool` maintains a pool of `Running`, ready-to-claim sandboxes for
instant cold-start handoff. The controller keeps `status.ready` between
`spec.minReady` and `spec.maxReady` by booting fresh sandboxes from
`spec.templateRef` and holding them `Running`. A claim is a control-plane
reassignment, not a Pod creation or restore.

## Resource identity

| Property | Value |
|----------|-------|
| API group | `sandbawks.dev` |
| Version | `v1alpha1` |
| Kind | `SandboxWarmPool` |
| List kind | `SandboxWarmPoolList` |
| Plural | `sandboxwarmpools` |
| Singular | `sandboxwarmpool` |
| Short name | `sbxwp` |
| Scope | Namespaced |
| Status subresource | Yes |

## Spec fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `templateRef` | string | Yes | Name of a `SandboxTemplate` in the same namespace whose image, resources, and `envd` build define the pool members. |
| `minReady` | integer (`uint32`) | Yes | Lower bound on `status.ready`. The controller creates new sandboxes until the warm count reaches this floor. |
| `maxReady` | integer (`uint32`) | Yes | Upper bound on `status.ready`. The controller does not warm additional sandboxes once the count reaches this ceiling. |

## Status fields

`status` is written by the controller.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ready` | integer (`uint32`) | `0` | Number of sandboxes in the pool currently `Running` and ready for handoff. |
| `failed` | integer (`uint32`) | `0` | Members seen in `Failed` or `Terminating` during the last reconcile. The controller deletes these on every pass, so a value that stays above zero means members keep failing after they're created. |
| `total` | integer (`uint32`) | `0` | All members seen during the last reconcile, in any phase. `maxReady` bounds only `ready`, so `total` can exceed `maxReady`; a large `total` points to leaked members. |
| `members` | array[string] | — | Names of the `Running` members currently ready to claim. |
| `consecutiveMintFailures` | integer (`uint32`) | `0` | Number of consecutive reconciles that found failed members. Reset to `0` by a reconcile that finds none. |
| `degraded` | boolean | `false` | `true` while the mint-failure circuit breaker is open and the pool creates no new members. See [Mint-failure circuit breaker](#mint-failure-circuit-breaker). |
| `degradedReason` | string | — | The main failure message from the failed members when the breaker opened. Null while the breaker is closed. |
| `backoffUntil` | string (RFC 3339) | — | Time until which the breaker blocks new members. After it passes, the pool creates one probe member. Null while the breaker is closed. |
| `backoffCount` | integer (`uint32`) | `0` | Number of failed probes since the breaker opened. Each one doubles the backoff. Reset to `0` when the breaker closes. |

### Mint-failure circuit breaker

When members keep failing right after they are created, for example because an
admission webhook rejects their Pods, the pool stops creating new members instead
of replacing them in a loop:

1. When `consecutiveMintFailures` reaches 5, the breaker opens: `degraded` becomes
   `true` and the pool creates no members until `backoffUntil`.
2. After the backoff, the pool creates one probe member. If the probe fails, the
   breaker reopens and `backoffCount` increases.
3. The first backoff is 30 seconds, and it doubles with each failed probe, up to
   15 minutes.
4. A reconcile that finds no failed members closes the breaker and resets the
   counters.

These values are fixed in the controller; they aren't Helm values.

## Additional printer columns

These columns appear in `kubectl get sandboxwarmpools` output.

| Column | Source |
|--------|--------|
| `Template` | `.spec.templateRef` |
| `Min` | `.spec.minReady` |
| `Max` | `.spec.maxReady` |
| `Ready` | `.status.ready` |
| `Failed` | `.status.failed` |
| `Total` | `.status.total` |

## Example

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxWarmPool
metadata:
  name: python-pool
  namespace: team-acme
spec:
  templateRef: python-base
  minReady: 2
  maxReady: 10
```

## See also

- [SandboxTemplate](/reference/custom-resources/sandboxtemplate/) — defines the pool members.
- [Sandbox](/reference/custom-resources/sandbox/) — the resource the pool holds ready to claim. Warm-pool members are named `sbx-<pool-name>-<suffix>`, and their `Claimed` printer column shows when each was claimed.
