---
title: How Sandbawks maps E2B onto Kubernetes
description: How the sandbawks.dev custom resources, the API gateway, and the controller fit together to turn E2B API calls into running, pausable sandboxes on Kubernetes.
slug: explanation/how-sandbawks-maps-e2b-onto-kubernetes
head:
  - tag: title
    content: How Sandbawks maps E2B onto Kubernetes · Sandbawks docs
tableOfContents: true
---

Sandbawks lets you point an existing E2B client at your own Kubernetes cluster.
The E2B API was designed around a hosted service, not around Kubernetes objects,
so something has to translate between the two worlds. That translation is the
heart of Sandbawks, and the [custom
resources](/reference/custom-resources/) are where it happens. This page is about
how those pieces relate — what each resource is *for*, and why the system is
split up the way it is. It is background reading, not a set of instructions; when
you need exact field names and types, the reference pages are the place to look.

## Two halves: the request path and the reconcile path

It helps to picture Sandbawks as two cooperating halves.

The first half is the **API gateway** (`sandbawks-api`). It speaks the E2B HTTP
API, so the SDK your agent already uses talks to it unchanged. But the gateway
does not run sandboxes itself. When a client asks it to create, pause, resume, or
time out a sandbox, the gateway translates that request into a change on a
Kubernetes object — it writes intent into the cluster and returns.

The second half is the **controller**. It watches those objects and does the
slow, stateful work of making reality match them: creating Pods, attaching
network policy, driving checkpoints and restores, cleaning up snapshots. The
controller writes what it observes back into each object's `status`.

```
E2B SDK ──HTTP──▶ API gateway ──writes spec──▶  Sandbox (CRD)  ◀──reconciles── Controller
                                                     │ status                      │
                                                     └──────────read by gateway◀───┘ drives Pods,
                                                                                     snapshots, etc.
```

This separation is the usual Kubernetes pattern — a declarative object as the
contract, a control loop converging on it — and it is why everything downstream
of the gateway is expressed as a custom resource rather than an RPC. The object
is durable, observable with `kubectl`, and survives a restart of either half.

## One sandbox is one object is one Pod

The central resource is the [`Sandbox`](/reference/custom-resources/sandbox/).
One `Sandbox` corresponds to one E2B sandbox, and — while it is running — one
Pod. That one-to-one-to-one mapping is deliberate. It means a sandbox's lifecycle
*is* a Kubernetes object's lifecycle: you can list sandboxes with `kubectl get
sandboxes`, watch their phase, and let Kubernetes' own garbage collection and
ownership machinery do work that would otherwise need bespoke bookkeeping.

The qualifier "while it is running" matters, though, and it is the reason
Sandbawks is more than a thin Pod wrapper. A sandbox is not always backed by a
Pod. When it is paused, the Pod is gone and the sandbox's state lives in a
snapshot in object storage. The `Sandbox` object persists across that gap; the
Pod is just the form the sandbox takes when active.

## Templates: the shape a sandbox is poured into

Most of what defines a sandbox — the image, how much CPU and memory it gets, what
runtime isolates it, how long it lives by default — is not unique to a single
sandbox. It is shared by a whole class of them. The
[`SandboxTemplate`](/reference/custom-resources/sandboxtemplate/) captures that
shared shape once, and each `Sandbox` references a template by name and overrides
only what it needs to.

This is the ordinary trade between defaults and per-instance control, and the
reason to keep templates separate is operational: an operator can publish a small
set of vetted templates — a particular image, a particular RuntimeClass — and
callers get those guarantees without restating them, or being able to drift from
them, on every create.

## The lifecycle: creating, running, pausing, resuming

A `Sandbox` moves through a handful of phases, and the
[full set is enumerated in the reference](/reference/custom-resources/sandbox/#phases).
The interesting transitions are not the obvious `Creating → Running` one but the
pause and resume edges, because that is where Sandbawks does something a plain Pod
cannot.

The gateway expresses what it wants through `spec.desiredPhase`: asking for
`Paused` requests a pause, asking for `Running` requests a resume. The controller
treats that as a goal and drives toward it. There is also a time-based path: every
sandbox has a timeout, and `spec.autoPause` decides what reaching it means. With
auto-pause on, the timeout pauses the sandbox and preserves its state; with it
off, the timeout tears the sandbox down. The same deadline therefore expresses
two quite different intentions — "park this when idle" versus "this is
disposable" — depending on a single boolean.

Pausing is the step that justifies the whole snapshot machinery. To pause a
sandbox the controller has to checkpoint a live process — its memory, its open
state — to durable storage, so it can be brought back faithfully later. Resuming
reverses that. Neither is instantaneous, and crucially, neither can be done by the
controller alone: a checkpoint has to run *on the node where the container lives*,
with access to the container runtime. That constraint is what produces the next
resource.

## Snapshots and SnapshotJobs: per-node work made into an object

When the controller decides a sandbox must be paused or resumed, it does not
perform the checkpoint itself. It creates a
[`SnapshotJob`](/reference/custom-resources/snapshotjob/) — one job, one
container, one node — and pins it to the node hosting the sandbox. A per-node
worker watches only for jobs assigned to its own node, does the actual checkpoint
or restore against the local runtime, and reports progress back through the job's
`status`.

Modelling this hand-off as its own resource, rather than an internal queue inside
the controller, buys the same things the `Sandbox` object buys: the work is
durable, inspectable, and survives a worker restart. It also draws a clean line
between *deciding* a sandbox should pause (a cluster-wide concern, the
controller's job) and *carrying out* the checkpoint (a node-local concern, the
worker's job). A `SnapshotJob` has three [modes](/reference/custom-resources/snapshotjob/#modes):
`Pause` and `Resume` are the two directions you would expect, and `Stage`
materializes a snapshot's parts onto a node's disk ahead of time so a sandbox can
be *born restored* at start-up rather than booting and then restoring.

A completed pause produces a **manifest**: a record of the snapshot's parts, each
with its own hash, plus an aggregate hash over them. This is more than
bookkeeping. A snapshot written on one node may be restored on another, possibly
days later, after passing through object storage that the cluster does not fully
trust. The hashes let the restoring node verify it is reconstructing exactly what
was captured. The manifest even records a hash of the `envd` agent binary the
sandbox was running, because a memory checkpoint is only valid against the exact
binary it was taken from — restore it against a drifted binary and the process's
code pages no longer match the file behind them. The integrity fields you see in
the reference exist to close precisely that window between pause and resume.

## envd: the agent inside every sandbox

Inside a running sandbox is a small agent called `envd`. It is what the E2B API
ultimately talks to in order to run commands and move files; a sandbox's
`status.envdAddress` is how the rest of the system reaches it. Rather than bake
`envd` into every customer image, Sandbawks stages a single cluster-wide binary
onto each node and mounts it read-only into the sandbox. That keeps customer
images focused on their own tooling and lets the agent be updated independently —
and, as noted above, it is the reason the snapshot machinery cares so much about
which exact `envd` binary a checkpoint was taken against.

## Warm pools: trading idle capacity for start-up latency

Creating a sandbox from cold — scheduling a Pod, pulling an image, booting — takes
time that an interactive agent would rather not spend. A
[`SandboxWarmPool`](/reference/custom-resources/sandboxwarmpool/) is the answer to
that latency: the controller keeps a configurable number of sandboxes already
`Running` and idle, and a "claim" simply reassigns one of these live members to a
caller. Because the member is already up, claiming it involves no Pod creation and
no restore — the cost has been paid in advance.

The trade is explicit and worth naming: a warm pool spends real, idle CPU and
memory to buy low start-up latency. The pool's floor and ceiling are how you tune
that trade — enough warm capacity to absorb your bursts, not so much that you are
paying to keep idle sandboxes alive for no one. It is the same bargain as a
thread pool or a pre-forked server, applied to whole sandboxes.

## Tenancy: API keys and namespaces

Finally, multi-tenancy. Sandbawks equates a **team** with a **Kubernetes
namespace**, and a [`TeamAPIKey`](/reference/custom-resources/teamapikey/) is what
ties an incoming API key to one. When a request arrives at the gateway, the key it
carries selects the namespace its sandboxes live in, and the namespace boundary
then does the heavy lifting of isolation that Kubernetes already knows how to
enforce.

A detail worth dwelling on: the `TeamAPIKey` stores only a hash of the key, never
the key itself, and the full key is shown exactly once when it is issued. This is
the standard posture for credentials at rest — a leaked object yields a hash, not a
usable key — and it is why a lost key cannot be recovered, only replaced. Leaning
on namespaces for the tenancy boundary, rather than inventing a parallel
permission system, is the same instinct that runs through the rest of the design:
where Kubernetes already has a concept that fits, Sandbawks borrows it instead of
building beside it.

## How it all comes back to E2B

Step back and the shape is consistent. An E2B call lands on the gateway; the
gateway records intent on a `Sandbox`, shaped by a `SandboxTemplate`, scoped to a
namespace chosen by a `TeamAPIKey`; the controller reconciles that object,
leaning on `SnapshotJob`s for node-local checkpoint work and on `SandboxWarmPool`s
to hide start-up latency; and the sandbox's observed state flows back through
`status` to the gateway, which renders it as the E2B response the client expects.

The E2B API is the contract at the edge. Kubernetes objects are the contract
inside. Sandbawks is the translation between them — and the custom resources are
that translation written down.

## See also

- [Custom resources reference](/reference/custom-resources/) — the exact fields, types, and defaults for each resource discussed here.
- [Point your agent at Sandbawks](/tutorials/point-your-agent-at-sandbawks/) — drive a real sandbox end to end with the standard E2B SDK.
