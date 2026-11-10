---
title: SandboxTemplate
description: Reference for the sandbawks.dev SandboxTemplate custom resource — spec fields, defaults, and printer columns.
slug: reference/custom-resources/sandboxtemplate
head:
  - tag: title
    content: SandboxTemplate · Sandbawks docs
tableOfContents: true
---

A `SandboxTemplate` is a reusable definition for a class of sandboxes. It
captures the OCI image and default resource, runtime, and timeout values. A
`Sandbox` references a template by name; per-`Sandbox` spec values override the
template defaults.

## Resource identity

| Property | Value |
|----------|-------|
| API group | `sandbawks.dev` |
| Version | `v1alpha1` |
| Kind | `SandboxTemplate` |
| List kind | `SandboxTemplateList` |
| Plural | `sandboxtemplates` |
| Singular | `sandboxtemplate` |
| Short name | `sbxtpl` |
| Scope | Namespaced |
| Status subresource | No |

## Spec fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `image` | string | Yes | OCI image reference for the sandbox's rootfs. The `envd` agent does not need to be baked into this image; the node DaemonSet stages it and the controller mounts it read-only. |
| `defaultResources` | object | Yes | Resource defaults applied when the `Sandbox` does not specify them. See [Default resources](#default-resources). |
| `defaultRuntimeClass` | string | Yes | RuntimeClass name to use unless the `Sandbox` overrides it. |
| `defaultTimeoutSeconds` | integer (`uint32`) | Yes | Default timeout in seconds. |
| `envdImage` | string | No | `envd` binary carrier image. Not consumed on the cold-start and restore paths under node-staged `envd` delivery; setting it is currently a no-op. |

### Default resources

`spec.defaultResources` has the same shape as a `Sandbox`'s `spec.resources`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cpu` | string | Yes | Default CPU limit, as a Kubernetes quantity. |
| `memory` | string | Yes | Default memory limit, as a Kubernetes quantity. |
| `ephemeralStorage` | string | No | Default ephemeral-storage limit, as a Kubernetes quantity. |

## Additional printer columns

These columns appear in `kubectl get sandboxtemplates` output.

| Column | Source |
|--------|--------|
| `Image` | `.spec.image` |
| `Runtime` | `.spec.defaultRuntimeClass` |

## Example

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-base
  namespace: team-acme
spec:
  image: registry.example.com/sandboxes/python:3.12
  defaultRuntimeClass: gvisor
  defaultTimeoutSeconds: 3600
  defaultResources:
    cpu: "1"
    memory: 1Gi
    ephemeralStorage: 4Gi
```

## See also

- [Sandbox](/reference/custom-resources/sandbox/) — references a template through `spec.templateRef`.
- [SandboxWarmPool](/reference/custom-resources/sandboxwarmpool/) — pre-warms sandboxes from a template.
