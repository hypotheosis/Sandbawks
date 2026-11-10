# Generic, distro-agnostic runsc `--root` discovery (fix recurring k3s pause)

**Issue:** #377
**Branch:** `fix-issue-377-runsc-root-generic-discovery`

## Problem

After #366/#367 shipped the per-environment runsc `--root` *probe*, every pause
on the k3s-libvirt cluster still failed at `runsc checkpoint` with:

```
checkpoint failed: runsc checkpoint exit exit status: 128: FetchSpec failed: loading container: file does not exist
```

Traceway exception group `fbc0baad3239af2a`: 18 occurrences in 85 minutes (one
per ~5-min warm-pool auto-pause) — snapshots 100% broken on the cluster again.

### Root cause (verified via traceway logs)

The #367 probe *ran* but had nothing to probe. Its no-match WARN logged
`candidates = ["/run/containerd/runsc/k8s.io"]` — a single entry. The probe only
selects among paths an operator hand-lists in `node.runscRootFallbacks` (empty
by default), so on a cluster where nobody enumerated the exact
`runsc/<namespace>` suffix, the candidate list was just the (wrong-for-k3s)
primary and the probe fell straight through to the failing checkpoint.

The design constraint is **don't assume one distro** — sandbawks must run on many
Kubernetes cluster types. #367 avoided baking k3s paths into the binary, but only
worked if each operator already knew their distro's runsc layout and wired it in;
that relocated the per-distro coupling from the image to the operator rather than
removing it.

### Second root cause (found via node `find`/`ls` while validating)

Inspecting a live k3s node showed runsc keeps per-container state as **flat
files** directly under the root — `<root>/<cid>_sandbox:<sandbox-id>.state` (+
`.lock`, plus `runsc-<sandbox-id>.sock`) at `/run/containerd/runsc/k8s.io/` —
**not** a `<root>/<container-id>/` subdirectory. Both #366's probe and this PR's
first cut checked `root.join(cid).is_dir()`, which real runsc never satisfies;
the unit tests had fabricated `<root>/<cid>` dirs, so the wrong assumption went
unnoticed. On kind it was masked because the configured primary was already
correct and the code fell through to it. The detection had to be rewritten to
match runsc's actual on-disk layout.

(The verifying host path: k3s relocates containerd's state dir to
`/run/k3s/containerd`, but the runc/runsc shims keep their runtime root at the
stock host `/run/containerd/{runc,runsc}/k8s.io` — which the node DS never
mounted.)

## Change

Add **generic, distro-agnostic discovery**: instead of selecting from an
operator-supplied candidate list, the node *searches* an operator-declared,
neutral containerd run/state root and finds the runsc root holding this
container's state wherever the shim placed it — matching runsc's real flat-file
layout (`<root>/<cid>.state` or `<root>/<cid>_sandbox:<sandbox-id>.state`), with
a legacy `<root>/<cid>/` directory accepted defensively. The operator declares
one neutral fact (and mounts it); the node discovers the rest. No distro path in
the binary; the same image works on stock containerd, k3s, RKE2, etc.

Resolution order in `resolve_runsc_root` is now:
1. **Explicit candidates** (`runscRoot` + `runscRootFallbacks`) — cheapest, an
   operator override always wins. (Unchanged #366 behavior.)
2. **Generic discovery** (#377) — bounded, symlink-free recursive search of the
   configured run/state root(s).
3. **Fallback + warn** — configured primary, with a now-richer diagnostic.

### `crates/sandbawks-node/src/gvisor.rs`

- New `runsc_root_search_dirs: Vec<PathBuf>` field + `with_runsc_root_search_dirs`
  builder. Empty by default → discovery off, identical to prior behavior.
- `root_holds_container(root, cid)` — detects runsc's real flat-file state
  layout (`<root>/<cid>.state` or `<root>/<cid>_sandbox:<sandbox-id>.state`, via
  `name_is_runsc_state_for`) and, defensively, a legacy `<root>/<cid>/` dir.
  **Replaces the `is_dir()` check** in the #366 explicit-candidate path, which
  real runsc never satisfied.
- `discover_runsc_root(container_id)` — first search dir that yields a match wins.
- `find_runsc_root(base, container_id, max_depth)` — iterative (non-recursive)
  DFS, depth-bounded (`RUNSC_ROOT_SEARCH_MAX_DEPTH = 6`), never follows symlinks
  (no cycles / no escape). Returns the directory holding the container's state
  **only** under a `runsc` path component, so the `runc` root and containerd's
  identically-keyed task bundle (`io.containerd.runtime.v2.task/.../<cid>`) are
  never false positives. (`runsc` is the runtime's own name, not a distro path.)
- Both pause checkpoint and resume restore inherit discovery via the shared
  `resolve_runsc_root`. The no-match WARN now names `search_dirs` and points at
  `node.runscRootSearchDirs` as the preferred fix.

### `crates/sandbawks-node/src/bin/sandbawks-node.rs`

- New `--runsc-root-search-dirs` arg (`SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS`,
  colon-separated `Vec<PathBuf>`), threaded through `RunscRootConfig`.
- `warn_if_runsc_root_missing` now treats a present search dir as "configured"
  too, so a discovery-configured node is not nagged at boot.
- Boot log records `runsc_root_search_dirs`.

### `deploy/helm/sandbawks`

- `node.runscRootSearchDirs: []` → rendered as `SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS`
  (colon-joined) only when non-empty. Documented as the **preferred**,
  distro-agnostic alternative to enumerating `runscRootFallbacks`, with a k3s/RKE2
  example that reuses the generic `extraHostPathMounts`.

Defaults render no extra env/mounts, so stock containerd and kind are unaffected.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — all green.
- New unit tests (`gvisor.rs`):
  - `discovery_matches_real_k3s_layout_skipping_runc_and_task_bundle` (real
    flat-file runsc state + runc/task-bundle siblings)
  - `resolve_runsc_root_detects_file_based_state_in_explicit_candidate`
  - `resolve_runsc_root_discovers_state_dir_under_search_dir`
  - `discovery_ignores_containerd_task_bundle_false_positive`
  - `discovery_picks_runsc_tree_when_both_trees_present`
  - `explicit_candidate_wins_over_discovery`
  - `find_runsc_root_respects_depth_bound`
- New unit tests (`sandbawks-node.rs`):
  - `warn_if_runsc_root_missing_returns_true_when_a_search_dir_exists`
  - `runsc_root_search_dirs_default_to_empty`, `runsc_root_search_dirs_split_on_colon`
  - existing `warn_if_runsc_root_missing_*` updated for the new 3-arg signature.
- `helm template` with defaults (no extra env/mounts) and with a search-dirs
  override (correct `SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS` env + mounted volume).

## Notes / follow-ups

- The exact host path the k3s gVisor shim uses is still unverified by `ls` on a
  node (no node-exec from here), but discovery is robust to it: point
  `runscRootSearchDirs` at the cluster's containerd run root and the node finds
  the subdir. Ops confirm with `find /run /var/lib -maxdepth 5 -name '<cid>' -type d`.
- **Mount constraint:** discovery still cannot read a host subtree that isn't
  mounted into the node container — the unavoidable, but *generic*, piece of
  config. On k3s-libvirt the stock host `/run/containerd` is not currently
  mounted; the immediate cluster unblock is to mount it and set
  `runscRootSearchDirs` accordingly (issue #377 step C).
- **Deferred (issue #377 Part B):** the fail-fast / stop-claiming-doomed-jobs
  gate (originally #366 Gap #2) is a cross-component behavior change (node
  capability signaling + controller/warm-pool claim logic) and is intentionally
  out of scope here; tracked as follow-up on #377.
