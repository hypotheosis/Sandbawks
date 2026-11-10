---
title: Custom resources
description: The sandbawks.dev custom resource definitions — Sandbox, SandboxTemplate, SandboxWarmPool, SnapshotJob, and TeamAPIKey — their API identities, scope, and roles.
slug: reference/custom-resources
head:
  - tag: title
    content: Custom resources · Sandbawks docs
tableOfContents: true
---

Sandbawks installs five custom resource definitions (CRDs) in the `sandbawks.dev`
API group. All five are namespaced and served at version `v1alpha1`.

## Resources

| Kind | Plural | Short name | Status subresource | Page |
|------|--------|------------|--------------------|------|
| `Sandbox` | `sandboxes` | `sbx` | Yes | [Sandbox](/reference/custom-resources/sandbox/) |
| `SandboxTemplate` | `sandboxtemplates` | `sbxtpl` | No | [SandboxTemplate](/reference/custom-resources/sandboxtemplate/) |
| `SandboxWarmPool` | `sandboxwarmpools` | `sbxwp` | Yes | [SandboxWarmPool](/reference/custom-resources/sandboxwarmpool/) |
| `SnapshotJob` | `snapshotjobs` | `sbxsj` | Yes | [SnapshotJob](/reference/custom-resources/snapshotjob/) |
| `TeamAPIKey` | `teamapikeys` | `tak` | No | [TeamAPIKey](/reference/custom-resources/teamapikey/) |

## API group

| Property | Value |
|----------|-------|
| Group | `sandbawks.dev` |
| Served version | `v1alpha1` |
| Scope | Namespaced |
| `apiVersion` value | `sandbawks.dev/v1alpha1` |

## Ownership

`Sandbox`, `SandboxTemplate`, `SandboxWarmPool`, and `TeamAPIKey` are the
declarative surface of the control plane. `SnapshotJob` is created and consumed
by the controller and its per-node workers; it carries the desired and observed
state of a single pause, resume, or stage operation on one node.

The `status` subresource on `Sandbox`, `SandboxWarmPool`, and `SnapshotJob` is
written by the controller. `SandboxTemplate` and `TeamAPIKey` have no `status`
subresource.
