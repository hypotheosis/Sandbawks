---
title: TeamAPIKey
description: Reference for the sandbawks.dev TeamAPIKey custom resource — spec fields and printer columns.
slug: reference/custom-resources/teamapikey
head:
  - tag: title
    content: TeamAPIKey · Sandbawks docs
tableOfContents: true
---

A `TeamAPIKey` authenticates a team — one Kubernetes namespace — at the
`sandbawks-api` ingress. The resource stores only the hash of the key, never the
key itself. The full key is shown once at issue time and is not recoverable.

The 8-character `keyPrefix` is indexed so a lookup is constant time; the
remainder of the key is then constant-time-compared against `keyHash`.

## Resource identity

| Property | Value |
|----------|-------|
| API group | `sandbawks.dev` |
| Version | `v1alpha1` |
| Kind | `TeamAPIKey` |
| List kind | `TeamAPIKeyList` |
| Plural | `teamapikeys` |
| Singular | `teamapikey` |
| Short name | `tak` |
| Scope | Namespaced |
| Status subresource | No |

## Spec fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `keyHash` | string | Yes | SHA-256 hex digest of the full key, prefixed `sha256:`. |
| `keyPrefix` | string | Yes | First 8 characters of the issued key, for example `sbk_a1b2`. |
| `scopes` | array[string] | No | Scope strings. Only `sandboxes:*` is honored; other scopes are reserved. |

## Additional printer columns

This column appears in `kubectl get teamapikeys` output.

| Column | Source |
|--------|--------|
| `Prefix` | `.spec.keyPrefix` |

## Example

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: TeamAPIKey
metadata:
  name: ci-runner
  namespace: team-acme
spec:
  keyPrefix: sbk_a1b2
  keyHash: sha256:3f786850e387550fdab836ed7e6dc881de23001b...
  scopes:
    - sandboxes:*
```
