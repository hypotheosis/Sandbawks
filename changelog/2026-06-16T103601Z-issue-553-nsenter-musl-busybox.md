# Issue #553 — bundle the musl (static) busybox for the #550 upgrader's nsenter

**Date:** 2026-06-16
**Branch:** `fix-issue-553-nsenter-glibc-musl`
**Scope:** One-line Dockerfile fix to a load-time failure in the #550 privileged
fleet-upgrader (PR #552, already merged). Follow-up bug, not part of #550's branch.

## Symptom

Every privileged runsc fleet-upgrade tick failed on the cluster:

```
WARN runsc fleet-upgrade tick failed; retrying next interval (#550)
  error=host installer launch failed (status exit status: 1):
    stderr="/usr/local/bin/nsenter: /lib/x86_64-linux-gnu/libc.so.6:
            version `GLIBC_2.38' not found (required by /usr/local/bin/nsenter)"
```

## Root cause

`crates/sandbawks-node/Dockerfile` bundled the **default** `busybox` image to
provide `nsenter`. That default is the **glibc** variant — dynamically linked,
requiring **GLIBC_2.38** (`readelf` confirms). The node runtime is
`gcr.io/distroless/cc-debian12` (bookworm, **glibc 2.36**), so the dynamic loader
can't resolve `nsenter` and it dies before it can `setns` into PID 1.

The #552 pre-merge `--help` smoke passed only because it ran inside the busybox
*base* image (newer glibc), never the distroless runtime — the mismatch went
unnoticed until the upgrader ran on a real node.

This is **non-fatal** to the node (the tick catches it, `WARN`s, bumps
`runsc_upgrade_errors_total{step="launch"}`, retries), but the Capability C
in-place upgrade is entirely inoperative until fixed.

## Fix

Bundle **`busybox:musl`** instead of the default. It is **static-PIE** — no
`INTERP` segment, no libc dependency at all — so it runs on the distroless runtime
regardless of glibc, and still ships the `nsenter` applet (verified by the existing
build-time applet check). The Dockerfile comment now documents why musl is
required, so a future edit doesn't regress to glibc.

## Validation

- `docker build --target busybox` + `file`/`readelf -l` on the staged
  `/out/nsenter` → `static-pie linked`, **no `INTERP`** (runs on glibc 2.36).
- The build-time `busybox … | grep -qw nsenter` applet check passes against the
  musl build (the image builds).
- `cargo fmt --check`, `cargo clippy -p sandbawks-node --all-targets -D warnings`,
  `cargo test -p sandbawks-node` (incl. the `runsc_upgrade` suite) — green.
- Dockerfile-only change; no chart/CRD/Rust-logic change, so no Chart.yaml bump.

The live nsenter/k3s-restart path still needs an on-node run once the rebuilt
image is published and the DaemonSet rolled.
