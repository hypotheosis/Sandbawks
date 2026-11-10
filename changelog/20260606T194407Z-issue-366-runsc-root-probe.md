# Probe per-environment runsc `--root` candidates (fix recurring k3s pause)

**Issue:** #366
**Branch:** `fix-issue-366-runsc-root-probe-k3s`

## Problem

Every pause on the k3s-libvirt cluster failed at `runsc checkpoint` with the
opaque

```
checkpoint failed: runsc checkpoint exit exit status: 128: FetchSpec failed: loading container: file does not exist
```

even though the resolved container ID was correct. Pause is fail-closed (the
error is raised before any source teardown), so the backing Pod stayed
`Running` and warm sandboxes re-failed every 600s forever — snapshots were 100%
broken on the cluster.

### Root cause (verified against the live cluster + node logs)

`#343` made the runsc `--root` configurable (`SANDBAWKS_RUNSC_ROOT` /
`node.runscRoot`) but kept it a single static value. On k3s/RKE2 the
embedded-containerd *socket* lives under `/run/k3s/containerd`
(`hostPathContainerdRun`), but the gVisor `io.containerd.runsc.v1` shim is
registered with no explicit `root`, so it keeps its per-container runsc state at
runsc's compiled-in default (`/run/containerd/runsc/k8s.io`) — a *different host
directory* the node container never mounted. The boot warning confirmed it:

```
WARN configured runsc --root is not a directory ... the root is wrong for this distro (e.g. k3s/RKE2) runsc_root=/run/containerd/runsc/k8s.io
```

A single static `--root` is a per-distro footgun; this exact failure has now
bitten on two separate clusters.

## Change

Make the node **probe** an ordered, per-environment list of runsc `--root`
candidates and auto-select whichever one actually holds the target container —
with **no distro-specific path baked into the binary**, so the same image works
across stock containerd, k3s, RKE2, etc. given the right config.

### `crates/sandbawks-node/src/gvisor.rs`

- New `extra_runsc_roots: Vec<PathBuf>` field on `GvisorAdapter` +
  `with_extra_runsc_roots` builder. Empty by default → unchanged single-root
  behavior.
- `candidate_runsc_roots()` — ordered, de-duplicated `[primary, ...fallbacks]`.
- `resolve_runsc_root(container_id)` — returns the first candidate whose
  `<root>/<container_id>` state dir exists; on no match, falls back to the
  configured primary and logs the candidates tried (so `runsc` still emits its
  own diagnostic — never worse than before, but now actionable).
- Both the pause checkpoint and resume restore calls now use the resolved root
  instead of the static `runsc_root`.
- `RUNSC_ROOT_FALLBACKS_SEPARATOR` constant (`:`) for the env list.

### `crates/sandbawks-node/src/bin/sandbawks-node.rs`

- New `--runsc-root-fallbacks` arg (`SANDBAWKS_RUNSC_ROOT_FALLBACKS`,
  colon-separated `Vec<PathBuf>`), threaded into the adapter via a new
  `RunscRootConfig` struct (keeps `build_gvisor_adapter` under the
  `too_many_arguments` threshold).
- Boot log now records `runsc_root_fallbacks`; the missing-root warning points
  at both env knobs.

### `deploy/helm/sandbawks` (per-environment configurable, no k3s defaults)

- `node.runscRootFallbacks: []` → rendered as `SANDBAWKS_RUNSC_ROOT_FALLBACKS`
  (colon-joined) only when non-empty.
- `node.extraHostPathMounts: []` → generic `{name, hostPath, mountPath,
  readOnly?, type?}` mounts so operators expose the runsc-state subtree their
  shim uses. RW by default (resume writes back into the root); `type` defaults
  to `DirectoryOrCreate`.
- `values.yaml` documents the k3s/RKE2 recipe as a commented example.

Defaults render no extra env/mounts, so stock containerd and kind are
unaffected.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- New unit tests:
  - `candidate_runsc_roots_lists_primary_then_fallbacks_deduped`
  - `resolve_runsc_root_prefers_primary_when_it_holds_the_container`
  - `resolve_runsc_root_selects_fallback_when_primary_is_empty`
  - `resolve_runsc_root_falls_back_to_primary_when_no_candidate_matches`
  - `runsc_root_fallbacks_default_to_empty`, `runsc_root_fallbacks_split_on_colon`
- `helm template` rendered the chart with defaults (no extra env/mounts) and
  with the k3s-style override (correct `SANDBAWKS_RUNSC_ROOT_FALLBACKS` env, RW
  `host-runsc` volumeMount, and `DirectoryOrCreate` hostPath volume).

## Notes / follow-ups

- The exact host path the k3s gVisor shim uses was inferred from the shim's
  default + the boot warning, not yet confirmed by `ls` on a node (no node-exec
  available from here). The probe design is robust to this: it auto-selects
  whichever configured candidate is correct, and ops confirm the path with
  `find /run /var/lib -maxdepth 5 -name '<container-id>' -type d`.
- Gap #2 from the issue (node refusing to *claim* SnapshotJobs when no root is
  resolvable, rather than claim-then-fail) is intentionally out of scope: it
  needs node-capability signaling and risks regressing fresh-node behavior. The
  probe + self-diagnosing log already converts the opaque infinite-fail loop
  into an actionable failure.
