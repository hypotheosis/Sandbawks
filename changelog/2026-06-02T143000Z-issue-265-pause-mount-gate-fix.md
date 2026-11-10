> **SUPERSEDED** — this fixed the pause mount-set gate for the per-Pod
> injection model, which was then abandoned (it broke gVisor resume). The
> shipped design is the **node-staged read-only hostPath** model in
> [`2026-06-02T153000Z-issue-265-node-staged-envd.md`](2026-06-02T153000Z-issue-265-node-staged-envd.md).
> Kept as a record of the design journey; it does **not** describe the
> current code.

# Issue #265 follow-up — injection volumes must pass the #237 pause mount-set gate

**Date:** 2026-06-02T14:30:00Z
**Branch:** `fix-issue-265-coldstart-template-image`
**Issue:** #265 / PR #266

## Symptom

After the envd-injection change, cold start reached `Running` but **pause**
failed immediately (`Pausing -> Failed`); the pause/resume smoke and the
resume bench both went red:

```
phase transition committed sandbox=sbx-4867ec84fb4d from=Pausing to=Failed
Error: status.phase for sbx-486fcc1f70c8 never reached Paused within 60s (last="Failed")
```

## Root cause

The node's pause path runs the #237 fail-closed mount-set assertion
(`assert_writable_mounts_overlay_backed` in `crates/sandbawks-node/src/gvisor.rs`)
*before* `runsc checkpoint`: it rejects any **writable** mount that isn't
captured by the memory checkpoint (overlay / tmpfs / pseudo-fs) or a
known runtime-managed furniture bind.

The injection model (#266) added two **default-medium `emptyDir`** mounts
to the workload container — `/sandbawks/bin` and `/sandbawks/run`. A
default-medium emptyDir is a gofer-backed (host-passthrough) bind: writable
and invisible to the memory checkpoint. The gate correctly flagged them and
failed the pause. (Pre-injection, envd ran from the overlay rootfs, which
*is* captured, so pause worked.)

## Fix

Shape the injection volumes so they are legitimately checkpoint-safe, and
teach the gate about them as defense-in-depth:

- `crates/sandbawks-controller/src/pod_adapter.rs`:
  - `/sandbawks/run` is now a `medium: Memory` emptyDir — a gVisor tmpfs,
    MemoryFile-backed, so envd's state file rides in `pages.img` and
    survives a memory-only restore (matching the pre-injection property
    that the state file lived on the captured overlay rootfs).
  - `/sandbawks/bin` is mounted **read-only** on the workload container
    (the `envd-inject` init container still writes it rw). A read-only
    mount is exempt from the gate, and keeping it a host-backed bind keeps
    the tens-of-MB binary out of the memory checkpoint (it's re-injected
    on the restored Pod instead).
- `crates/sandbawks-node/src/gvisor.rs`: new `SANDBAWKS_MANAGED_MOUNT_DESTS`
  exemption (mirrors `RUNTIME_MANAGED_BIND_DESTS`) so the gate accepts
  `/sandbawks/bin` + `/sandbawks/run` even on a containerd build that
  surfaces them as writable binds — both are controller-managed
  (re-injected / captured elsewhere), not workload state lost on restore.

## Tests

- `sandbawks-node`: `sandbawks_injection_mounts_are_accepted` — the gate
  no longer flags the injection mounts.
- `sandbawks-controller`: `injection_volumes_survive_the_pause_mount_gate`
  — `/sandbawks/run` is `medium: Memory`, workload `/sandbawks/bin` is
  read-only, init-container `/sandbawks/bin` is writable.

## Still cluster-only (issue #271)

This resolves the identified pre-checkpoint gate failure. Whether
`runsc checkpoint`/`restore` itself tolerates the read-only gofer bind for
`/sandbawks/bin` (binary re-injected on restore) is the kind+gVisor smoke
tracked in #271 — not runnable without a cluster.

## Validation

- `just fmt` / `cargo clippy --all-targets -- -D warnings` / `just test` — all green.
