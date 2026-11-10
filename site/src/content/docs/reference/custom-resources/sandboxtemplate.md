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
| `defaultTimeoutSeconds` | integer (`uint32`) | Yes | Default idle timeout in seconds. |
| `envdImage` | string | No | `envd` binary carrier image. Not consumed on the cold-start and restore paths under node-staged `envd` delivery; setting it is currently a no-op. |
| `networkPolicy` | object | No | Egress baseline for sandboxes created from this template. Same shape as `Sandbox.spec.networkPolicy`. See [Network policy](#network-policy). |
| `tolerations` | array[object] | No | Tolerations for the Pods of sandboxes created from this template. Same shape as `Sandbox.spec.tolerations`. |
| `nodeSelector` | map[string]string | No | Node-selector labels for the Pods of sandboxes created from this template. |
| `securityContext` | object | No | Pod-level security context for sandboxes created from this template, for example `fsGroup`. Same shape as `Sandbox.spec.securityContext`. |
| `persistenceMode` | string (enum) | No | `Snapshot` or `Filesystem`. Applies to sandboxes that don't set their own. See [Persistence](#persistence). |
| `volumeClaimTemplates` | array[object] | No | PersistentVolumeClaims each sandbox created from this template owns. Same shape as `Sandbox.spec.volumeClaimTemplates`. |
| `volumeClaimTemplatesPolicy` | string (enum) | No | Whether a `Sandbox` can add or override volume claim templates. Defaults to `Disallowed`. See [Volume claim templates policy](#volume-claim-templates-policy). |

The field shapes of `tolerations`, `securityContext`, and `volumeClaimTemplates`
are described in the [Sandbox reference](/reference/custom-resources/sandbox/).

### Default resources

`spec.defaultResources` has the same shape as a `Sandbox`'s `spec.resources`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cpu` | string | Yes | Default CPU limit, as a Kubernetes quantity. |
| `memory` | string | Yes | Default memory limit, as a Kubernetes quantity. |
| `ephemeralStorage` | string | No | Default ephemeral-storage limit, as a Kubernetes quantity. |

## How template values reach a sandbox

| Field | How it applies |
|-------|----------------|
| `defaultResources` | The API gateway copies it into the `Sandbox` it creates. |
| `defaultRuntimeClass`, `tolerations`, `nodeSelector`, `securityContext` | The controller uses the template's value when the `Sandbox` doesn't set the field (`runtimeClassName` for the runtime class). A value on the `Sandbox` replaces the template's value. |
| `persistenceMode` | Used when the `Sandbox` doesn't set it. See [Persistence](#persistence). |
| `volumeClaimTemplates` | Combined with the `Sandbox`'s claims under `volumeClaimTemplatesPolicy`. |
| `networkPolicy` | Combined with the create request. See [Network policy](#network-policy). |

The controller applies the runtime class, `tolerations`, `nodeSelector`, `securityContext`,
`persistenceMode`, and `volumeClaimTemplates` when it creates the sandbox Pod, so
they apply to cold-started sandboxes, warm-pool members, and `Sandbox` resources
you apply directly.

### Network policy

`spec.networkPolicy.egressAllow` is a standing allow-list. When the API gateway
creates a sandbox, it sets the sandbox's `networkPolicy` to the union of the
template's destinations and any destinations in the create request, with the
template's destinations first and duplicates removed. Neither side can remove the
other's destinations. Warm-pool members get the template's `networkPolicy` when
they are created; when one is claimed with a create request that adds
destinations, the claimed sandbox gets the union.

A `Sandbox` you apply directly with `kubectl` doesn't inherit the template's
`networkPolicy`.

### Persistence

The effective persistence mode of a sandbox is the first of these that applies:
the `Sandbox`'s `persistenceMode`, the template's `persistenceMode`, `Filesystem`
if the effective set of volume claim templates is not empty, and otherwise
`Snapshot`. `Snapshot` is rejected on Kata runtime classes (`kata`, or any name
starting with `kata-`), so a Kata template must use `Filesystem` for its
sandboxes to be pausable.

### Volume claim templates policy

`spec.volumeClaimTemplatesPolicy` controls whether a `Sandbox` can contribute its
own `volumeClaimTemplates` on top of the template's.

| Value | Meaning |
|-------|---------|
| `Disallowed` | Default. The `Sandbox`'s claims are ignored; only the template's claims are created. |
| `Allowed` | The `Sandbox` can add claims with names the template doesn't use. A claim whose name matches a template claim is ignored, and the template's claim is kept. |
| `Overrides` | The `Sandbox` can add claims and replace template claims of the same name. |

The effective claims are the template's claims in template order, followed by the
`Sandbox`'s added claims in their order. A replaced claim keeps the template
claim's position. When the policy drops a claim, the sandbox doesn't fail: the
controller adds a `VolumeClaimTemplatesPolicy` condition, with status `False` and
reason `ClaimsSuppressed`, that names the dropped claims.

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

A template whose sandboxes keep a workspace volume across pauses, run on a
dedicated node pool, and can reach a package index:

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-workspace
  namespace: team-acme
spec:
  image: registry.example.com/sandboxes/python:3.12
  defaultRuntimeClass: gvisor
  defaultTimeoutSeconds: 1800
  defaultResources:
    cpu: "1"
    memory: 1Gi
  networkPolicy:
    egressAllow:
      - pypi.org
  nodeSelector:
    sandbawks.example.com/pool: sandboxes
  tolerations:
    - key: sandbawks.example.com/pool
      operator: Equal
      value: sandboxes
      effect: NoSchedule
  securityContext:
    fsGroup: 1000
  persistenceMode: Filesystem
  volumeClaimTemplates:
    - metadata:
        name: workspace
      spec:
        storage: 10Gi
        accessModes:
          - ReadWriteOnce
  volumeClaimTemplatesPolicy: Allowed
```

## See also

- [Sandbox](/reference/custom-resources/sandbox/) — references a template through `spec.templateRef`.
- [SandboxWarmPool](/reference/custom-resources/sandboxwarmpool/) — pre-warms sandboxes from a template.
