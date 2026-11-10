# Issue #483 — Staging sweeper: reclaim orphaned staging dirs left by resume node-move retries

## Summary

The node-local staging sweeper (`run_staging_sweeper` /
`sweep_staging_dirs` in `crates/sandbawks-node/src/gvisor.rs`) decided whether
to reclaim a `<stagingRoot>/<sandbox-id>` subtree purely from the owning
`Sandbox`'s **phase**: it kept the bundle while the sandbox was
`Running`/`Resuming`/`Pausing`.

Now that resume is scheduler-placed (the restore Pod is created unpinned and a
recovery retry can land it on a **different** node, re-staging the snapshot
there), the abandoned node's `<stagingRoot>/<sandbox-id>` is orphaned while the
sandbox stays `Running`/`Resuming` on the new node. The phase-only gate kept
the stale, multi-GiB bundle until the sandbox terminated, leaking disk on every
node a resume retry passed through.

## Change

Added a **locality gate** alongside the existing phase gate. A node now keeps a
staging subtree only when the owning sandbox both (a) is in a phase that still
reads the bundle, **and** (b) has its live Pod on *this* node — or its host node
is not yet known (`status.assigned_node == None`), in which case the bundle is
kept rather than reclaimed on incomplete information.

### `crates/sandbawks-node/src/gvisor.rs`

- New `SandboxStagingOwner { phase, assigned_node }` — the owner state the
  sweep needs (phase + hosting node).
- New pure predicate `staging_bundle_retained_on_node(owner, this_node)`,
  composing `staging_bundle_needed` (phase gate) with the node-locality gate.
- Renamed the lookup abstraction to reflect its widened return:
  `SandboxPhaseLookup` → `SandboxOwnerLookup`, `phase_of` → `owner_of`
  (now returns `Option<SandboxStagingOwner>`), and the production impl
  `KubeSandboxPhaseLookup` → `KubeSandboxOwnerLookup` (reads
  `status.assigned_node` in addition to `status.phase`).
- `sweep_staging_dirs` and `run_staging_sweeper` take a `this_node: &str` /
  `String` and feed it to the new predicate. Reclaim log line now records the
  owner state and `this_node`.

### `crates/sandbawks-node/src/bin/sandbawks-node.rs`

- Wired the existing `NODE_NAME` (downward-API) value into the sweeper task.

### `crates/sandbawks-node/tests/staging_gc.rs`

- Reworked the fake lookup to carry owner state (phase + node).
- New tests: `retention_predicate_combines_phase_and_node_locality`,
  `sweep_reclaims_running_owner_whose_pod_moved_to_another_node` (#483 core),
  and `sweep_keeps_running_owner_with_unknown_host_node` (incomplete-info
  safety). Existing phase-lifecycle and best-effort-error tests retained.

## Safety note

The sweep now trusts `status.assigned_node` to reflect the live Pod's host, the
same trust the phase gate already places in `status.phase`. The `None` host
case keeps (never reclaim on incomplete information), covering the pre-bind
window of a freshly-scheduled resume restore Pod.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — all workspace tests pass (9/9 in `staging_gc.rs`, including the
  two new #483 cases)
