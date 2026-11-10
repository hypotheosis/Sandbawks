# 2026-05-26 — Issue #171: fresh-pod resume orchestration

Closes forgejo issue #171.
Plan 05 T15's smoke deliberately deletes the original Pod after pause
and then calls `/connect`, validating that the snapshot bundle in S3 is
the authoritative record. Before this change the controller still
dispatched the Resume `SnapshotJob` against the pre-pause Pod's UID, so
`resolve_pod` failed with `no pod with uid <stale> on node …` and the
sandbox stayed in `Resuming` forever. This PR makes resume actually
work against a fresh Pod (same node or different node).

## Design

After brainstorming options (a) pause-then-restore-over-running-envd,
(b) restore-mode envd shim, and (c) admission-webhook CRI interception,
we picked **(b)**: the controller injects `SANDBAWKS_RESTORE_MODE=1`
into the fresh resume Pod's envd container, envd boots into an idle
hold without binding any listener or writing the state file, and the
node DaemonSet then runs `runsc pause` + `runsc restore` over the idle
sandbox. No race window between envd binding ports and `runsc restore`
overlaying it, and no new admission infrastructure.

State-machine shape: kept `Resuming` as a single phase. The internal
sub-state (recreate Pod → wait Ready → dispatch SnapshotJob → wait
Restore → Running) lives in the existing status fields
(`pod_uid`/`envd_address`/`snapshot_job_ref`), not in new CRD enum
variants — keeps the schema unchanged one plan after Plan 05.

## What landed

### envd: restore-mode idle hold

* `crates/envd/src/bin/envd.rs` — `SANDBAWKS_RESTORE_MODE` env var. When
  set, `run()` skips listener binding and snapshot-file I/O, logs a
  single "restore-mode hold" line, and `wait_for_signal()`s until
  SIGTERM/SIGINT.
* `crates/envd/tests/binary_smoke.rs::envd_restore_mode_skips_listeners_and_state_file`
  — spawns the binary with the env var, asserts neither gRPC nor HTTP
  ports are bound after 500 ms, asserts the state file is never created
  (pre- or post-SIGTERM), and asserts clean SIGTERM exit. Pins the
  contract the node DaemonSet relies on.

### Controller: fresh-Pod resume workflow

* `crates/sandbawks-controller/src/state_machine.rs` — new
  `Action::RecreatePodForResume`; `(Paused, ResumeRequested)` now lands
  on it instead of `Action::Resume`. Added `(Resuming, PodReady) →
  Resuming + Action::Resume` self-loop for the second sub-step.
* `crates/sandbawks-controller/src/reconcile.rs` —
  * Pre-trigger pod-readiness probe extended from `Creating` to
    `Creating | Resuming` so it rehydrates the fresh Pod's
    `pod_uid`/`envd_address`/`assigned_node`.
  * `derive_trigger` recognises `(Resuming, pod_ready=true,
    snapshot_job_ref=None) → PodReady`.
  * `Action::RecreatePodForResume`: best-effort `delete_pod` for the
    stale Pod, then clears `pod_ref`/`pod_uid`/`envd_address`/
    `assigned_node`, then `create_pod(restore_mode=true)`.
  * `Action::Resume`: defensive guard that the rehydrated `pod_uid` is
    non-empty before dispatching the SnapshotJob — caught at the
    controller boundary rather than producing a zero-UID
    `container_id` for the node worker.

### Trait + adapter: restore-mode parameter

* `crates/sandbawks-types/src/runtime.rs` — `RuntimeAdapter::create_pod`
  grows a `restore_mode: bool` parameter. The default impl returns a
  synthetic `pod_uid` from `pod_ready` so the `Action::Resume` guard
  doesn't trip on test fakes.
* `crates/sandbawks-controller/src/pod_adapter.rs` — `PodAdapter` injects
  `SANDBAWKS_RESTORE_MODE=1` on the envd container only when
  `restore_mode=true`; a user-supplied `SANDBAWKS_RESTORE_MODE` in
  `Sandbox.spec.env` is filtered out so the controller's intent wins.

### Node DaemonSet: pause-before-restore

* `crates/sandbawks-node/src/gvisor.rs` — `GvisorAdapter::resume` now
  invokes `runsc pause <cid>` before `runsc restore --image-path
  <work_dir> <cid>`. The pause is best-effort: a non-zero exit (e.g.
  the legacy same-pod path where the sandbox is already stopped) is
  logged at debug and the restore proceeds — `runsc restore` tolerates
  an already-stopped sandbox. New test
  `resume_pauses_then_restores_in_that_order` pins the invocation
  order using a recording stub.

### Smoke: resume half stays gated behind `PAUSE_RESUME_SMOKE_FULL=1`

* `deploy/kind/fixtures/pause-resume-smoke.sh` — the orchestration
  scaffolding lands but `PAUSE_RESUME_SMOKE_FULL` default stays at
  `0` because the runtime call itself doesn't yet succeed against
  kubelet-managed containers. See "What still doesn't work" below.

## Tests added

| Test | What it pins |
| --- | --- |
| `envd_restore_mode_skips_listeners_and_state_file` | restore-mode envd: no gRPC/HTTP bind, no state file, clean SIGTERM exit |
| `pod_spec_for_injects_restore_mode_env_when_requested` | env injection happens iff `restore_mode=true` |
| `pod_spec_for_user_cannot_force_restore_mode` | user-supplied `SANDBAWKS_RESTORE_MODE` is filtered; controller intent wins |
| `paused_resume_requested_to_resuming_recreate_pod` | state machine: `(Paused, ResumeRequested) → RecreatePodForResume` |
| `resuming_pod_ready_dispatches_resume_snapshot_job` | state machine: `(Resuming, PodReady) → Resume` |
| `paused_with_desired_running_recreates_pod_and_then_dispatches_resume` | reconcile loop: two-pass workflow against the fakes |
| `resume_pauses_then_restores_in_that_order` | node DaemonSet: argv ordering and per-call container UID |
| `no_self_loops` proptest amended | `(Resuming, PodReady)` self-loop is the second explicit exception |

## Validation

* `just fmt` ✓
* `cargo clippy --workspace --all-targets -- -D warnings` ✓
* `just test` ✓ — full workspace.

## Follow-up commits on this PR

### Manifest sha256 cross-check (hardening)

Plan 05 T9 had introduced a placeholder-empty-`sha256` pattern in the
node reconciler because `SnapshotJobSpec` didn't carry a manifest. The
adapter's manifest-level cross-check became vacuous, and an attacker
with S3 write access could swap both a snapshot's parts *and* its
manifest's recorded part-hashes without being detected at the
manifest-aggregate level. Per-part hashing still defended against
naive part-only tampering, but the aggregate gate was open.

This PR re-closes that gate by threading the aggregate through:

* `SandboxStatus.snapshot_sha256: Option<String>` — new field. The
  controller's `observe_snapshot_job` folds `manifest.sha256` into it
  on Pause completion, alongside the existing `snapshot_ref` fold.
* `SnapshotJobSpec.expected_sha256: Option<String>` — new field. The
  controller's `build_snapshot_job` stamps it on Resume dispatch from
  `Sandbox.status.snapshot_sha256`.
* The node reconciler fills `placeholder.sha256` from
  `job.spec.expected_sha256` rather than always-empty; the adapter's
  manifest-mismatch check now fires whenever the controller commits a
  value.
* `Option::is_none`-fallback retained for legacy callers (direct
  adapter use in unit tests, or Pause completions that surface an
  empty aggregate), with per-part sha256 verification still as the
  integrity gate in that path.

New tests pin the round trip:

| Test | What it pins |
| --- | --- |
| `snapshot_job_complete_advances_pausing_to_paused_and_folds_snapshot_ref` (extended) | Pause completion folds `manifest.sha256` into `Sandbox.status.snapshot_sha256` |
| `paused_with_desired_running_recreates_pod_and_then_dispatches_resume` (extended) | Resume dispatch stamps `expected_sha256` from the seeded `snapshot_sha256` |
| `full_lifecycle_creating_to_terminating` (extended) | End-to-end: pause manifest sha256 → status → resume `SnapshotJobSpec.expected_sha256` |
| `resume_rejects_storage_sha256_mismatch_when_committed` | Adapter bails before any restore work when controller commitment diverges from storage; `apply_rootfs_delta` is never called |
| `resume_tolerates_empty_controller_sha256` | Legacy / no-commitment path still works (per-part verification carries the integrity gate) |

CRDs regenerated via `cargo run -p sandbawks-types --bin crdgen`.

## What still doesn't work — follow-up #176

The cluster smoke ran every scaffolding piece end-to-end and proved it
works:

* Controller flow: `Paused → Resuming → RecreatePodForResume →
  PodReady → Resume → Failed` (the Failed-at-the-end is the gap below;
  every step before it succeeded).
* Fresh Pod boots into `SANDBAWKS_RESTORE_MODE=1`, envd skips listener
  bind and state-file writes.
* Snapshot sha256 binding: `snapshotSha256: dfefe35a9e9d…` lands in
  `Sandbox.status` and travels forward to
  `SnapshotJobSpec.expected_sha256`.
* Node DaemonSet picks up the Resume job, fetches the manifest from
  S3, cross-checks the aggregate against the controller's commitment,
  verifies per-part sha256s, applies the rootfs delta.

The runtime call itself fails:

```
restore failed: runsc restore exit exit status: 128:
  starting container: cannot restore container "…" in state running
```

`runsc restore` accepts only `stopped` or `created` state. Getting a
kubelet-managed running container to `stopped` requires `runsc kill`,
which tears down the `containerd-shim-runsc-v1` shim and breaks
kubelet's view of the Pod (Pod transitions to Failed, CNI tears down
the netns). `runsc restore` was designed for standalone OCI use, not
kubelet-managed containers under a shim. The proper integration is
**containerd CRI checkpoint/restore** (Kubernetes [KEP-2008]; alpha
in 1.30+; the matching containerd 1.7+ Task Service RPCs), where the
shim itself mediates restore on container-create so kubelet stays in
sync.

That's substantially bigger scope than the orchestration #171 was
filed to land — tracked separately as **forgejo #176**. The smoke's
`PAUSE_RESUME_SMOKE_FULL` default stays at `0` until #176 closes; set
it to `1` locally to reproduce the runtime failure verbatim.

[KEP-2008]: https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2008-forensic-container-checkpointing

## What's deliberately *not* here

* Cross-node restore — the snapshot is already in S3 since Plan 05 T6,
  so this is scheduler-side work that lives in Plan 06/07.
* Lazy / page-fault restore — Plan 07.
* Multi-container Pods.
* The runtime-side restore call itself — see #176 above.

## Out-of-tree validation (cluster smoke)

This branch can't run kind+gvisor end-to-end from the agent's
workstation, so the cluster-level proof falls to CI: `kind-smoke.yaml`
runs `pause-resume-smoke.sh` with `KIND_SMOKE_INCLUDE_PAUSE_RESUME=1`
on every PR, which (now that the smoke default is `=1`) exercises
delete-pod → connect → fresh-Pod restore-mode boot → `runsc pause` +
`runsc restore` → file round-trip → kill → GC.
