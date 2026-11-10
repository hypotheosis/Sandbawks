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
| `members` | array[string] | — | Names of the `Running` members currently ready to claim. |

## Additional printer columns

These columns appear in `kubectl get sandboxwarmpools` output.

| Column | Source |
|--------|--------|
| `Template` | `.spec.templateRef` |
| `Min` | `.spec.minReady` |
| `Max` | `.spec.maxReady` |
| `Ready` | `.status.ready` |

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
- [Sandbox](/reference/custom-resources/sandbox/) — the resource the pool holds ready to claim.
