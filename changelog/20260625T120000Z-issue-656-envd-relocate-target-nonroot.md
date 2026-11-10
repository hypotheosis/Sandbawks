# Move the envd capture-relocate target off root-owned `/sandbawks/run` (issue #656)

**Date:** 2026-06-25T12:00:00Z
**Issue:** #656 — envd capture-in-checkpoint relocate (`mkdir /sandbawks/run`) crash-loops cold start for non-root images under gVisor
**Crates:** `sandbawks-types` (`crates/sandbawks-types/src/runtime.rs`), with literal/comment follow-through in `envd`, `sandbawks-controller`, `sandbawks-node`

## Summary

The capture-in-checkpoint relocation step (epic #466) crash-looped envd cold start
for any workload image that runs envd as a **non-root user under gVisor**. On
startup envd `mkdir -p`s the relocate dir and re-execs itself from there so its code
pages land in the gVisor overlay that `runsc checkpoint` captures. The target was a
top-level `/sandbawks/run` — and `/sandbawks` is auto-created by the kubelet as the
**root-owned** parent of the read-only `/sandbawks/bin` envd-seed mount. A non-root
process (e.g. `compound-sandbox`'s `uid=1000(dev)`) cannot create `/sandbawks/run`
under it, so the `mkdir` fails `EACCES (os error 13)`. envd exits non-zero, the
container goes terminal, the controller folds the member to `Failed`
(`pod_vanished_creating`), and warm pools re-mint endlessly (~30 s churn, `ready=0`);
standalone sandboxes never leave `Creating`.

Observed on the on-prem k3s cluster after upgrading 0.1.18 → 0.1.28: the pre-upgrade
envd had no relocate step, so warm members booted fine; the upgrade exposed it.

## Root cause

The relocate target `/sandbawks/run` reintroduced the exact bug class already fixed
for envd's `--state-file` (placed under `/var/tmp` sticky-`1777` instead of
root-owned `/var/lib`). envd is *designed* to run as the image's `USER` — routinely
non-root: it does not `setuid`, its rootfs-fingerprint walk tolerates `EACCES`, and
the state-file lives under `/var/tmp` for exactly this reason. The relocate target
must therefore also be non-root-writable.

## Fix

Move the relocate target to `/var/tmp/envd/run/envd` (parent `/var/tmp/envd/run`),
nested alongside the `--state-file` under sticky-`1777` `/var/tmp`, which a non-root
envd can create. It stays a plain rootfs path (no volume mounted), so the
`runsc checkpoint` capture property is unchanged. Single source of truth:
`ENVD_RUN_DIR` / `ENVD_RELOCATE_TARGET` in `sandbawks-types::runtime`.

- `crates/sandbawks-types/src/runtime.rs` — `ENVD_RUN_DIR` → `/var/tmp/envd/run`,
  `ENVD_RELOCATE_TARGET` → `/var/tmp/envd/run/envd`; doc comments updated; new
  regression test `relocate_target_is_non_root_writable` pins the invariant (target
  under `/var/tmp`, never `/sandbawks`).
- `crates/sandbawks-controller/src/pod_adapter.rs` — comments updated; the gVisor
  gating of the relocate env/memory-budget is now framed as an optimization (Kata
  gains nothing from a self-copy) rather than crash-avoidance.
- `crates/envd/src/bin/envd.rs`, `crates/sandbawks-node/src/gvisor.rs`,
  `crates/sandbawks-{controller,node}/tests/*` — test fixtures/assertions updated to
  the new path (the no-mount assertion now tracks `ENVD_RUN_DIR`).
- `deploy/crds/snapshotjobs.sandbawks.dev.yaml` — regenerated (`envdCaptured`
  description text only; no schema change).

The node only reads the relocate env's **presence** as the `envd_captured`
capture-mode marker (`container_relocate_target(...).is_some()`), never the path
value, so the change is safe end-to-end.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all pass.
- New regression test + the updated envd relocate-decision and node OCI-parse tests pass.
- Full gVisor checkpoint/restore proof (`gvisor_capture_e2e.rs`) requires a real
  runsc environment; real-cluster validation is deploying the rebuilt envd and
  watching the warm pool reach `ready`.
