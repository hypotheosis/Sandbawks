# Single `node.runtime` knob selects the pause/resume adapter (epic #594)

**Date:** 2026-06-19T10:36:15Z
**Issue:** #594 (follow-up to #595)
**Branch:** `fix/chart-runtime-adapter-selection`

## Symptom

On the AKS Kata cluster, every pause of a Running sandbox failed:

```
status.conditions: Failed — pause checkpoint failed 3 times:
  checkpoint failed: envd /__state timed out after 10s
```

`/__state` is a **GvisorAdapter** step (`fetch_state` before `runsc checkpoint`);
the `KataAdapter` never calls it. So the node was running the gVisor pause flow
against a Kata sandbox.

## Root cause

#595 added the node binary's `--runtime-adapter` flag
(`SANDBAWKS_RUNTIME_ADAPTER`, default `gvisor`) but **never plumbed a Helm value
to set it** — only `node.runtimeHandler` got chart wiring. So the node DaemonSet
ran with `SANDBAWKS_GVISOR_RUNTIME_HANDLER=kata` (gate matched) but no adapter
env, defaulted to `gvisor`, and built `GvisorAdapter` on the Kata pool. The
KataAdapter was unreachable via Helm.

## Fix — one knob, not two

Rather than just add a second `node.runtimeAdapter` value that has to be kept in
sync with `node.runtimeHandler` (the exact mismatch that caused this), the chart
now exposes a single primary knob:

- **`node.runtime`** (`""`=gvisor | `gvisor` | `kata` | `kata-fc`): selects the
  pause/resume adapter (`SANDBAWKS_RUNTIME_ADAPTER`) **and** defaults the
  staging-gate CRI handler (`SANDBAWKS_GVISOR_RUNTIME_HANDLER`). `kata` → handler
  defaults to `kata`. A Kata pool needs only `node.runtime: kata`.
- **`node.runtimeHandler`** is demoted to an **optional override** of just the
  handler, for cases where the registered CRI handler differs from the default —
  confidential Kata registers `kata-cc`, or an operator renamed the RuntimeClass
  handler.

The adapter and handler remain *distinct binary flags* (the handler is the exact
registered CRI string — renameable, with variants — while the adapter is the
mechanism family); the chart just collapses the common case into one value.

`deploy/helm/sandbawks/templates/node-daemonset.yaml`: derive `$handler` from
`node.runtimeHandler` else the `node.runtime` default; render both envs.
`values.yaml`: `node.runtime` added, `runtimeHandler` reworded as an override
(the unreleased `node.runtimeAdapter` from this branch is removed).

## Render matrix (verified with `helm template`)

| values | `SANDBAWKS_RUNTIME_ADAPTER` | `SANDBAWKS_GVISOR_RUNTIME_HANDLER` |
|---|---|---|
| (default) | *(unset → gvisor)* | *(unset → runsc)* |
| `runtime: kata` | `kata` | `kata` |
| `runtime: kata` + `runtimeHandler: kata-cc` | `kata` | `kata-cc` |

`helm lint` clean. Chart `version` `0.1.22` → `0.1.23`.

## Honest scope

This makes the **KataAdapter actually run** for pause/resume on a Kata pool — it
fixes "the wrong adapter is selected." It does **not** by itself prove Kata
pause works end to end: the KataAdapter's Cloud Hypervisor socket round-trip and
pod-UID → Kata-sandbox-id resolution remain the unvalidated Phase C live seam
(#594). Expect the *next* failure to surface there once `node.runtime: kata` is
deployed — which is the point: it lets us finally exercise the real CH path on a
live node.
