# Issue #470 — capture-in-checkpoint envd: live gVisor capture/restore acceptance test

**Date:** 2026-06-18T10:51:06Z
**Area:** node / envd (tests)
**Parent epic:** #466 (make snapshots survive envd upgrades)
**Depends on:** #468 (pod relocate surface) and #469 (`envd_captured` manifest flag + guard branch)

## Summary

The make-or-break end-to-end proof for the capture-in-checkpoint epic: that an
envd which has self-relocated onto the checkpoint-captured overlay
(`/sandbawks/run/envd`) survives a real `runsc checkpoint` / `runsc restore`
round-trip **serving its code pages from `pages.img`**, and that the
node-staged host binary is genuinely irrelevant at restore. Until now the whole
epic rested on an unverified gVisor assumption; this commit verifies it against
live `runsc`.

## Changes

`crates/envd/src/bin/envd.rs`:

- New compile-time build identifier `BUILD_ID`, defaulting to
  `CARGO_PKG_VERSION` and overridable at compile time via
  `SANDBAWKS_ENVD_BUILD_ID` (read with `option_env!`). Baked into the binary's
  pages, so it reflects whichever code is actually executing.
- New `GET /__version` route on the layer-free `:49984` HTTP listener, returning
  `BUILD_ID` as a `text/plain` body. This is the observable provenance seam:
  after `runsc restore`, the *captured* id means the process is serving from
  `pages.img`; the *host* id would mean it re-faulted a host-swapped binary.
- Unit test `version_route_returns_build_id` asserts the route is wired and
  returns the crate version when no build id is stamped.

`crates/envd/build.rs`:

- Emits `cargo:rerun-if-env-changed=SANDBAWKS_ENVD_BUILD_ID` so toggling the id
  between two `cargo build` invocations recompiles the binary instead of reusing
  a cached one that bakes the stale id. Load-bearing for the acceptance test,
  which builds two envd binaries with distinct ids from one target directory.

`crates/sandbawks-node/tests/gvisor_capture_e2e.rs` (new):

- Opt-in, self-skipping live-gVisor acceptance test. No-ops (printing why)
  unless `SANDBAWKS_GVISOR_E2E` is set **and** every host prerequisite is
  present (`runsc`, `ip`, `busybox`, and root or passwordless `sudo`), so it
  never runs on an ordinary `cargo test` / CI host — mirroring how the repo's
  other live-gVisor coverage is exercised out-of-band.
- Flow: build OLD + NEW envd (distinct `BUILD_ID`s; overridable via
  `SANDBAWKS_ENVD_OLD_BIN` / `SANDBAWKS_ENVD_NEW_BIN`) → assemble a minimal OCI
  bundle (seed envd at `/usr/local/bin/envd`, busybox, the seed's `ldd` deps, a
  netns with loopback up) → cold-start under `runsc` so envd self-relocates onto
  `/sandbawks/run/envd` → assert it serves the **OLD** id over `/__version`
  **and** that PID 1's `/proc/1/exe` resolves under the relocate dir (so a
  silent relocation no-op is caught at cold start, not just via the restore
  round-trip) → `runsc checkpoint` → **replace the on-disk seed binary with
  NEW** (the
  standalone analogue of swapping `/var/lib/sandbawks/envd/envd` on the node) →
  `runsc restore` → assert it **still** serves the **OLD** id.
- A `Drop` guard tears down the sandbox + netns even on a failed assertion, so a
  panic never leaks a live sandbox or a network namespace.

## gVisor configuration notes (discovered while building this)

- The relocate target lives in the workload rootfs and the test runs `runsc`
  with `--overlay2=root:memory`, so the relocated binary rides the captured
  overlay-tmpfs (matching the production `root:self` capture surface from #469).
- Probing is done from **inside the sandbox's own network namespace** via
  `runsc exec ... busybox wget`, sidestepping host↔sandbox networking. A
  direct-`runsc` sandbox has no routable loopback unless a netns with `lo` up is
  supplied (what containerd/CNI normally provides); the test supplies one.
- `OTEL_SDK_DISABLED=1` is set on the sandbox envd so no OTLP exporter is built —
  avoiding the post-restore blocking-pool starvation tracked in #356/#560.
- The relocate target's parent and the state-file dir are world-writable because
  the spec process runs without `CAP_DAC_OVERRIDE` against host-uid-owned dirs.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — green (the new live test self-skips without the opt-in)
- Live acceptance run (`SANDBAWKS_GVISOR_E2E=1 cargo test -p sandbawks-node
  --test gvisor_capture_e2e`) against host `runsc release-20260525.0`: the
  restored sandbox reports the OLD build id after the on-disk seed was swapped
  to NEW between checkpoint and restore — proving the captured code pages
  survived the host-binary swap.

## Out of scope

- Production rollout / enabling the relocate env by default (#468 landing).
- Wiring this opt-in test into automated CI (no live-gVisor CI runner today).
