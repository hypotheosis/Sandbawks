# Cold-start scheduling gate for node-staged envd (#272)

**Date:** 2026-06-02T19:37:54Z
**Issue:** #272 — Cold-start race: sandbox Pod can schedule before node DaemonSet stages envd
**Branch:** `fix-issue-272-envd-staging-gate`

## Problem

Sandbox Pods exec the node-staged `envd` binary (`/sandbawks/bin/envd`) as
their entrypoint, mounted read-only from a `hostPath` of type
`DirectoryOrCreate`. The binary is placed by the `sandbawks-node`
DaemonSet's `stage-envd` init container. Nothing tied a sandbox Pod's
*scheduling* to the DaemonSet having finished staging on the target node,
so a Pod landing on a not-yet-staged node mounted an **empty** directory
and CrashLooped with `exec: /sandbawks/bin/envd: no such file or
directory`. Observable on cluster bootstrap, node cordon/uncordon, node
scale-up, and the initial DaemonSet roll on a node.

(Subsequent DaemonSet image rolls are already safe from the *missing*-binary
CrashLoop: `envd self-install` renames the binary into place atomically, so
a Pod scheduling mid-roll execs a complete binary. Matching the exact
post-roll content is #270; clearing a stale label if the binary later
disappears is the #282 follow-up.)

## Fix: node-label + nodeAffinity gate

The idiomatic option from the issue. Two cooperating sides plus a shared
contract:

1. **Shared contract — `sandbawks-types::labels`** (new module). Defines
   `ENVD_STAGED_LABEL = "sandbawks.io/envd-staged"` and
   `ENVD_STAGED_LABEL_VALUE = "true"`, re-exported from the crate root.
   Keeping the key in the schema-only crate gives the controller and the
   node a single source of truth — a drift would silently break
   scheduling.

2. **Node side — `sandbawks-node::node_label`** (new module). The node
   binary, after building its kube client and **verifying the staged
   binary is actually present** at `gvisor::DEFAULT_ENVD_BIN_PATH`,
   patches `ENVD_STAGED_LABEL` onto its own `Node`. The `stage-envd` init
   container has necessarily completed before the main container starts,
   and the binary-presence check is the explicit fail-closed assertion
   that the node is truly ready. A missing binary (`BinaryMissing`) is a
   hard error so kubelet restarts the Pod and re-runs `stage-envd` rather
   than advertising a false-ready node.

3. **Controller side — `pod_adapter::pod_spec_for`**. Every sandbox Pod
   now carries a `requiredDuringSchedulingIgnoredDuringExecution`
   nodeAffinity with an `Exists` match on `ENVD_STAGED_LABEL`. The
   scheduler only places sandboxes onto nodes that have advertised a
   staged binary. Applied to cold-start and restore Pods alike; a restore
   Pod pins `spec.nodeName` (bypassing the scheduler), so the affinity is
   inert there but the Pod shape stays uniform.

4. **RBAC.** The `sandbawks-node` ClusterRole gains `patch` on the core
   `nodes` resource (labels live in `metadata`, not the status
   subresource). Split out from the shared `pods`/`nodes` get/list/watch
   rule.

## Why `Exists` and not a value match

The value is informational today; the gate only requires the key to
exist. Per-digest staging (#270) will replace the value with the staged
binary's content hash so a sandbox can additionally require the exact
`envd` build its checkpoint was taken against. The `Exists` gate is the
minimal correct fix for the CrashLoop and leaves the digest hook for #270.

## Files changed

- `crates/sandbawks-types/src/labels.rs` (new) + `lib.rs` (module + re-export)
- `crates/sandbawks-node/src/node_label.rs` (new) + `lib.rs` (module + doc)
- `crates/sandbawks-node/src/bin/sandbawks-node.rs` (call `advertise_envd_staged` at boot)
- `crates/sandbawks-controller/src/pod_adapter.rs` (nodeAffinity + resolved doc)
- `crates/sandbawks-controller/tests/pod_adapter.rs` (gate assertion, both branches)
- `deploy/helm/sandbawks/templates/rbac.yaml` (nodes/patch for node SA)
- `deploy/helm/sandbawks/templates/node-daemonset.yaml` (doc note)

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all crates pass; new tests:
  - `pod_spec_for_gates_scheduling_on_envd_staged_node_label` (controller)
  - `staged_label_patch_sets_only_the_staged_label`,
    `ensure_binary_present_ok_when_file_exists`,
    `ensure_binary_present_errors_when_file_missing` (node)
- `helm lint deploy/helm/sandbawks` — 0 failed; rendered `sandbawks-node`
  ClusterRole carries `resources: ["nodes"], verbs: [... "patch"]`.
