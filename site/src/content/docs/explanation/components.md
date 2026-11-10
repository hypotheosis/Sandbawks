---
title: The Sandbawks components
description: What actually runs in a Sandbawks deployment — the API gateway, the controller, the per-node DaemonSet, and the envd agent — where each one lives, and why the work is split across them.
slug: explanation/components
head:
  - tag: title
    content: The Sandbawks components · Sandbawks docs
tableOfContents: true
---

The companion page on [how Sandbawks maps E2B onto Kubernetes](/explanation/how-sandbawks-maps-e2b-onto-kubernetes/)
is about the *objects* — the custom resources that hold a sandbox's intent and
observed state. This page is about the *processes* that run those objects: what
actually runs in the cluster, where each piece lives, and why the work is divided
the way it is. It is background reading, not a deployment guide; when you need
exact images, flags, or field names, the reference pages and the source repo are
the place to look.

A Sandbawks deployment is four cooperating pieces. Two of them are cluster-wide
control-plane processes, one runs on every node, and the last one runs *inside*
each sandbox:

```
        control plane (cluster-wide)              every node
   ┌───────────────┐   ┌──────────────┐    ┌──────────────────────────┐
   │ sandbawks-api │   │  controller  │    │      sandbawks-node      │
   │  (Deployment) │   │ (Deployment) │    │        (DaemonSet)       │
   └───────┬───────┘   └──────┬───────┘    └────────────┬─────────────┘
           │ writes spec      │ creates Pods            │ stages envd,
           ▼                  ▼ & SnapshotJobs          │ runs checkpoints
       Sandbox / SnapshotJob / … (CRDs)                 ▼
           ▲                                     ┌───────────────┐
   E2B SDK ┘ reads status                        │  sandbox Pod  │
                                                 │   └─ envd ◀────┘ mounted in
                                                 └───────────────┘
```

The split is not arbitrary. Each boundary follows a real difference in what the
work needs: how it scales, where it has to run, and what it is allowed to touch.

## The API gateway: the edge that speaks E2B

`sandbawks-api` is the front door. It runs as an ordinary Deployment — stateless,
and replicated for availability and throughput — and it speaks the E2B HTTP API
so that the SDK your agent already uses talks to it unchanged.

What it deliberately does *not* do is run sandboxes. When a client asks it to
create, pause, resume, or time out a sandbox, the gateway translates that request
into a write on a Kubernetes object and returns; to answer a status query it
reads back from the object's `status`. Keeping the gateway stateless is what lets
it be replicated freely: any replica can serve any request, because the real
state lives in the cluster, not in the process. This is the request path, and
it is discussed in more depth on the [mapping
page](/explanation/how-sandbawks-maps-e2b-onto-kubernetes/#two-halves-the-request-path-and-the-reconcile-path).

## The controller: the brain that reconciles

`sandbawks-controller` is the other half of the control plane. It watches the
objects the gateway writes and does the slow, stateful work of making reality
match them: creating and deleting Pods, attaching network policy, deciding when a
sandbox must be paused or resumed, keeping warm pools at their target size, and
cleaning up afterwards. Whatever it observes, it writes back into each object's
`status`.

The controller is a single logical reconcile loop — one brain converging the
cluster on its declared intent — rather than a replicated request handler. That
is the right shape for a control loop: you want one authority deciding what
should happen, not several racing to decide it. Crucially, the controller
*decides* but does not reach into nodes to do node-local work itself. When a
sandbox needs to be checkpointed, the controller does not perform the checkpoint;
it records that intent as a [`SnapshotJob`](/reference/custom-resources/snapshotjob/)
pinned to the right node and lets the node-local worker carry it out. That
hand-off is the seam between the control plane and the next component.

## The node DaemonSet: the hands on each node

`sandbawks-node` is the piece the other documentation only mentioned in passing,
and it is worth treating properly, because it is where several constraints
converge. It runs as a **DaemonSet**: exactly one instance on every node, placed
and replaced automatically as nodes join and leave the cluster. That shape is not
incidental — it is forced by the nature of the work.

The node worker has two jobs.

**It stages the `envd` agent onto the node.** Before it does anything else, the
node worker installs a single, cluster-wide build of the `envd` binary onto the
node's local disk, where it can later be mounted read-only into every sandbox on
that node. This is why `envd` does not have to be baked into customer images (see
[below](#envd-the-agent-inside-the-sandbox)), and doing it once per node — rather
than once per sandbox — is what makes the agent cheap to ship and update.

**It carries out node-local checkpoint and restore work.** This is the larger
job. The node worker watches for `SnapshotJob`s assigned to *its own* node and
executes them against the local container runtime: capturing a running sandbox's
memory and state to a snapshot (`Pause`), reconstructing a sandbox from a snapshot
(`Resume`), or materializing a snapshot's parts onto local disk ahead of time so a
sandbox can be *born restored* (`Stage`). It moves snapshot data between the node
and the shared object store, and it manages the node's local snapshot and staging
caches — including keeping enough free disk headroom that this housekeeping never
fills the node.

Why must this be a per-node process at all, instead of something the controller
does remotely? Because a checkpoint has to run *where the container lives*. To
freeze and capture a live process you need direct, privileged access to that
node's container runtime and to the process itself — access the controller, sitting
elsewhere in the cluster, simply does not have. The node worker therefore runs
privileged and close to the runtime, which is exactly the kind of powerful,
node-scoped capability you want confined to a single well-defined component rather
than handed to the control plane at large. The DaemonSet model then guarantees
the obvious invariant: every node that can host a sandbox has exactly one worker
able to checkpoint it, with no manual placement.

This is the same division of labour described for `SnapshotJob`s on the [mapping
page](/explanation/how-sandbawks-maps-e2b-onto-kubernetes/#snapshots-and-snapshotjobs-per-node-work-made-into-an-object):
the controller *decides* a sandbox should pause; the node worker *does* the
pausing. Modelling the hand-off as an object, rather than a direct call, is what
lets the two run, fail, and restart independently.

## envd: the agent inside the sandbox

The fourth piece is not a cluster workload at all — it runs *inside* every
sandbox. `envd` is a small agent that the E2B API ultimately talks to in order to
run commands and move files; a sandbox's `status.envdAddress` is how the rest of
the system reaches it.

Rather than ship `envd` in every customer image, Sandbawks treats it as a single
cluster-wide binary that the node DaemonSet stages onto each node and mounts
read-only into the sandbox. That keeps customer images focused on their own
tooling and lets the agent be versioned and updated independently of any image.
It also matters for correctness across a pause: a memory checkpoint is only valid
against the exact binary it was taken from, which is why the snapshot machinery
records a hash of the `envd` build a sandbox was running. The reasoning behind
that integrity check is spelled out on the [mapping
page](/explanation/how-sandbawks-maps-e2b-onto-kubernetes/#snapshots-and-snapshotjobs-per-node-work-made-into-an-object).

## Reading the split two ways

Once the four pieces are on the table, two axes make sense of why they are
separate.

The first is the **request path versus the reconcile path**. The gateway is the
synchronous edge: it turns an E2B call into intent and returns immediately. The
controller and node worker are the asynchronous interior that converges on that
intent over time. Decoupling them is what lets a slow operation — pausing a large
sandbox, say — proceed without holding an HTTP request open.

The second is the **control plane versus the data plane**. The gateway and
controller are cluster-wide and node-agnostic: they reason about objects and never
touch a container runtime. The node DaemonSet is the data plane: node-scoped,
privileged, and concerned with the messy local reality of freezing and restoring
real processes. Keeping that powerful node-local capability in one dedicated
component — instead of spreading it across the control plane — is the same
instinct that runs through the rest of the design: give each concern exactly the
authority it needs, and no more.

## See also

- [How Sandbawks maps E2B onto Kubernetes](/explanation/how-sandbawks-maps-e2b-onto-kubernetes/) — the objects these components act on, and the request/reconcile split in more depth.
- [Custom resources reference](/reference/custom-resources/) — the exact fields, types, and defaults for each resource named here.
- [Point your agent at Sandbawks](/tutorials/point-your-agent-at-sandbawks/) — drive a real sandbox end to end with the standard E2B SDK.
