# Make the runsc state `--root` configurable (fix k3s pause)

**Date:** 2026-06-05T00:41:10Z
**Issue:** #343
**Branch:** `fix-issue-343-runsc-root-config`

## Summary

Every pause on the real k3s libvirt cluster failed at the `runsc
checkpoint` step with `FetchSpec failed: loading container: file does not
exist`, leaving the Sandbox CR `Failed` while the backing Pod kept
`Running` (pause is fail-closed). The container ID resolution was correct;
the problem was the `--root` passed to `runsc`.

`GvisorAdapter`'s `runsc_root` was hardcoded to `DEFAULT_RUNSC_ROOT`
(`/run/containerd/runsc/k8s.io`) and the node binary never called the
existing `with_runsc_root` builder, so there was no env/Helm override. On
k3s/RKE2 the `io.containerd.runsc.v1` shim keeps its per-container state
under a different subtree, so `runsc` read an empty directory and failed
per-pause forever (`autoPause: true` sandboxes re-failed every
`timeoutSeconds`).

This change makes the runsc `--root` configurable end to end: a node
binary flag/env, threaded into the adapter, surfaced as a Helm value, plus
a loud boot-time warning when the resolved root is absent so the
misconfiguration is visible at deploy rather than silently per-pause.

## Changes

### `crates/sandbawks-node/src/bin/sandbawks-node.rs`
- Added a `--runsc-root` clap arg (env `SANDBAWKS_RUNSC_ROOT`, default
  `gvisor::DEFAULT_RUNSC_ROOT`) and a `RUNSC_ROOT_ENV` constant.
- Threaded the value through `build_gvisor_adapter`, which now calls the
  previously-unused `GvisorAdapter::with_runsc_root` before
  `with_headroom_policy`.
- Added `warn_if_runsc_root_missing`, called at boot: it emits a
  `tracing::warn!` (naming the env var and the k3s/RKE2 case) when the
  resolved root is not a directory, but does **not** fail the process —
  the shim creates the root lazily on first gVisor container start, so a
  freshly-booted node legitimately has no such dir yet. A hard failure
  would crash-loop a healthy node.
- Logged `runsc_root` in the boot `info!` line.
- New `#[cfg(test)]` module: default value, flag override, and both
  branches of `warn_if_runsc_root_missing` (existing dir vs absent path).

### `deploy/helm/sandbawks/values.yaml`
- Added `node.runscRoot` (default `/run/containerd/runsc/k8s.io`) with
  docs explaining the k3s/RKE2 override and how to find the real root with
  `find /run -maxdepth 4 -name '<container-id>' -type d`.

### `deploy/helm/sandbawks/templates/node-daemonset.yaml`
- Render `SANDBAWKS_RUNSC_ROOT` from `node.runscRoot`.
- Refreshed the `containerd-run` volumeMount comment, which previously
  asserted the now-configurable hardcoded `--root`.

## Validation

- `just fmt`
- `cargo clippy --all-targets -- -D warnings`
- `just test`
- `helm template` of the node DaemonSet renders the new env var.

## Notes / follow-ups

The exact k3s runsc-state subpath was not reproducible from this
environment (no affected node / cluster access here), so the *default*
`node.runscRoot` is unchanged and stock-containerd clusters are
unaffected. The fix gives operators the override knob and a loud
deploy-time signal; the issue's verification snippet
(`find /run -maxdepth 4 -name "$cid" -type d`) determines the correct
value to set on k3s.
