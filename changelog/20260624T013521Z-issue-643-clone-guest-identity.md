# Clone guest-identity reconciliation: decision + first contract

**Date:** 2026-06-24T01:35:21Z
**Issue:** #643
**Branch:** `feat-issue-643-clone-guest-identity`

## Why

A gVisor `snapshot`-mode clone (`spec.restoreFrom`, #416 / PR #642) rebuilds the
entire Sentry from the *source's* checkpoint, so it inherits the source's
in-kernel identity — `uname -n`, captured `/etc/machine-id` / boot-id, and the
netstack MAC/IP. Meanwhile the kubelet re-materialises `/etc/hostname` & friends
from the *destination* Pod, so a clone runs with a split hostname and a
machine-id duplicated across the source and every clone of it. It does not fail
the restore, but a workload that keys on hostname / machine-id (service
registration, clustering, licensing) gets the source's identity instead of a
unique one.

## What changed

### Design decision (`docs/superpowers/specs/2026-06-24-clone-guest-identity-reconcile-design.md`)

Records the strategy (the primary #643 acceptance criterion): a **hybrid** —
rewrite the UTS hostname and regenerate `/etc/machine-id` & friends in-guest via
`envd` on a clone's first boot, while **accepting** the inherited netstack MAC/IP
(the host veth + CNI address carry the real L2/L3 identity; the in-guest MAC is
cosmetic and never on the wire). Filesystem-mode clones cold-boot fresh identity,
so the reconciliation is snapshot-mode only.

### First load-bearing contract (`sandbawks-types`)

- `clone_needs_guest_identity_reset(restore_from, effective_mode)` — pure
  predicate, `true` iff `restore_from` is set **and** the resolved persistence
  mode is `Snapshot`. The single source of truth the controller (and future
  callers) branch on to decide whether a clone needs a guest-identity reset.
- `CLONE_IDENTITY_RESET_ANNOTATION` (`sandbawks.dev/clone-identity-reset`) +
  `CLONE_IDENTITY_RESET_VALUE` — the controller→`envd` seam. The controller will
  stamp this on a restore Pod when the predicate holds; `envd` reads it on startup
  to run the one-shot reset. An annotation, not a label (nothing selects on it).
- Narrowed the "Known limitation" doc block on `SandboxSpec::restore_from` to
  point at the spec and the new predicate.

## What is deliberately *not* here

The **runtime wiring** — controller stamping the annotation at pod
materialization, and `envd` performing the in-guest `sethostname(2)` +
machine-id regeneration — is left to a follow-up. Its acceptance test is
intrinsically a live-cluster e2e (`kubectl exec` against a running clone, per
#643's verification block) that cannot be exercised in a headless/no-cluster
environment. Landing the contract now fixes the seam the controller and `envd`
changes review against, and keeps this change's claims limited to what its unit
tests actually prove.

## Validation

- `just fmt` ✅
- `cargo clippy --all-targets -- -D warnings` ✅
- `just test` ✅ (includes the new `clone_identity_reset_needed_only_for_snapshot_clones`
  unit test and the doctest on `clone_needs_guest_identity_reset`)
