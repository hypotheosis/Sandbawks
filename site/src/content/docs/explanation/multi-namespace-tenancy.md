---
title: Multi-namespace tenancy
description: How one Sandbawks installation serves many teams, each in its own namespace — where API keys live, the per-tenant ServiceAccount the gateway impersonates, and what the API server enforces.
slug: explanation/multi-namespace-tenancy
head:
  - tag: title
    content: Multi-namespace tenancy · Sandbawks docs
tableOfContents: true
---

Sandbawks equates a team with a Kubernetes namespace. One installation, with one
controller, one node DaemonSet, and one API gateway, serves every team's namespace.
This page explains how a request ends up in the right namespace, and why it
can't reach any other.

## One control plane, many namespaces

The controller and the node DaemonSet watch Sandbawks resources in every
namespace. A `Sandbox`, `SandboxTemplate`, or `SandboxWarmPool` created in any
namespace is reconciled by the same installation. Templates and warm pools are
per namespace, so each team has its own catalog of templates and its own warm
capacity.

The installation's own namespace still matters for a few things: it holds the
leader-election Lease, the API keys, and the gateway's default namespace.

## API keys live outside tenant reach

A [`TeamAPIKey`](/reference/custom-resources/teamapikey/) maps an API key to a
team. The key objects live in the namespace Sandbawks is installed in, not in the
tenants' namespaces, and `spec.namespace` names the tenant namespace the key
grants access to.

Keeping keys out of tenant namespaces closes an escalation path. If a key's
tenant were taken from the namespace the key object lives in, anyone who could
create objects in a namespace could mint a key for it. With the keys in the system
namespace, only whoever administers Sandbawks can issue them, and a tenant's own
permissions don't extend to its credentials.

## The gateway acts as the tenant

For each tenant namespace that has a key, the controller creates a
`sandbawks-tenant` ServiceAccount, a `sandbawks-tenant` Role, and a RoleBinding
between them. The Role allows only what a tenant needs:

- `sandboxes`: full access.
- `sandboxes/status`: `get` and `patch`, to record activity when a warm sandbox is
  claimed.
- `sandboxtemplates` and `sandboxwarmpools`: read-only.

When a request arrives, the gateway resolves its key to a namespace, then makes
its Kubernetes calls for that request while impersonating that namespace's
`sandbawks-tenant` ServiceAccount. The gateway's own ServiceAccount can't create,
change, or delete sandboxes, templates, or warm pools. It can read sandboxes and
their status, update status, read keys and Events, and impersonate.

So the Kubernetes API server, not just the gateway's code, enforces the boundary.
Even if the gateway mis-resolved a namespace, a request impersonating team A's
ServiceAccount is refused when it touches team B's namespace, because team A's
Role exists only in team A's namespace. A bug in the gateway becomes a failed
request instead of a cross-tenant leak.

A few calls carry no team identity and run under the gateway's own
ServiceAccount, with narrow, mostly read-only permissions: recording client
activity for the idle timeout, reading Events for the event stream, and watching
sandboxes to route `envd` traffic.

The controller doesn't delete a namespace's tenant ServiceAccount, Role, and
RoleBinding when the last key for that namespace is removed.

## How envd traffic finds its sandbox

Calls that reach a sandbox's `envd`, such as file operations, commands, and
terminals, carry no API key, so the gateway can't take the namespace from a team.
It takes it from the sandbox's access token instead. The `envdAccessToken` the
gateway returns when a sandbox is created or connected is bound to the sandbox's
namespace, and it names that namespace in front of its signature. The gateway
checks the signature, which covers both the namespace and the sandbox ID, before
it looks anything up, then finds the sandbox in that namespace.

Sandbox names are only unique within a namespace, so this binding matters: a
token for a sandbox in one namespace can never reach a sandbox with the same name
in another. Signed file URLs work the same way. A URL minted with
`/files/mint` carries the minting team's namespace, so a team can only mint URLs
for its own sandboxes.

Tokens issued before Sandbawks bound them to a namespace still work, but only for
sandboxes in the gateway's default namespace, which is all they could reach
before.

## See also

- [TeamAPIKey reference](/reference/custom-resources/teamapikey/) — `spec.namespace`.
- [How Sandbawks maps E2B onto Kubernetes](/explanation/how-sandbawks-maps-e2b-onto-kubernetes/)
  — where tenancy fits in the overall design.
