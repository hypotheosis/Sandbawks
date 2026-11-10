# Pause-failure CR condition names the unreachable host

- **Date:** 2026-06-14T22:56:04Z
- **Issue:** #528
- **Crate:** `sandbawks-controller`

## Summary

A pause failed three times and folded the Sandbox to `Failed` with a CR
condition that read, in part:

> `pause checkpoint failed 3 times without success (most recent: checkpoint
> failed: connect: Host is unreachable (os error 113)); leaving the workload
> Pod running`

Reading that with `kubectl describe sandbox`, an operator could not answer the
two obvious questions — **which host was unreachable** and **from where** —
because the message names neither the probe target nor the node.

## The constraint that shaped the fix

The naive fix is "put the pod IP back into the node's probe error." That is
exactly what **PR #434** deliberately *removed*: the node's `HttpEnvdProbe`
error is the string Traceway hashes on, so a per-sandbox token in it splits one
logical failure class into N one-off exception groups. The referenced Traceway
exception (`ef8a009b8053cf76`) only groups because the address is absent.

So the node error and the CR condition are two sinks of one string today, with
opposite requirements: Traceway wants it address-free; the operator wants the
address. The fix **decouples them** — it leaves the node error (and #434's
grouping) completely untouched and enriches only the controller-built CR
condition, using `SandboxStatus` fields the controller already holds.

## What landed

`crates/sandbawks-controller/src/reconcile.rs`:

- New `pause_failure_location(status: &SandboxStatus) -> String` — derives a
  `"; sandbox envd was at <host:port> on node <node>"` suffix from
  `status.envd_address` and `status.assigned_node`. Returns `""` when neither
  is known, so the condition degrades to its original un-located form rather
  than dangling a half-formed clause.
- `pause_recovery_trigger` weaves that suffix into the terminal
  `PauseCheckpointFailed` condition message. The `tracing::warn!` line is left
  unchanged (its static message + low-cardinality `fail_msg` field keep their
  grouping).

Because the `Trigger::Failure` message is shared, the location also flows
through `phase_transition_event` into the `Failed` Kubernetes Event note
(`reason: message`), so it surfaces in `kubectl describe event` as well as the
CR condition — a strictly-additive diagnostic win on both operator-facing
surfaces. It does **not** reach the node's Traceway-grouped probe error, which
is the surface #434 protects.

No node changes, no `RuntimeError` / Traceway message changes, and no CRD schema
change — `envd_address` and `assigned_node` are existing `SandboxStatus` fields,
so nothing new needs to be persisted or `kubectl apply`-ed.

## Resulting condition

> `pause checkpoint failed 3 times without success (most recent: checkpoint
> failed: connect: Host is unreachable (os error 113)); sandbox envd was at
> 10.42.3.17:49983 on node worker-02; leaving the workload Pod running`

## Tests

- `pause_exhaustion_message_locates_the_unreachable_host` — asserts the envd
  host and node both reach the condition message and the trailing clause is
  preserved.
- `pause_failure_location_degrades_without_status_fields` — empty suffix on an
  empty status; node-only status yields a node-only clause.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — green
