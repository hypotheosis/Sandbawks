# Auto-pause no longer races a stopped container; checkpoint errors group in Traceway

**Issues:** #382, #384
**Branch:** `fix-issue-382-384-stopped-container-checkpoint`

Two coupled defects surfaced by the same Traceway exceptions
(`1a623b71522a7c56`, `0c53689cc06524b1`):

```
checkpoint failed: runsc checkpoint exit exit status: 128:
cannot checkpoint container "<id>" in state stopped
```

## Problem

### #382 — auto-pause checkpoints an already-exited container

A sandbox Pod runs a single `envd` container with `restartPolicy: Never`. When
envd exits, the gVisor sandbox process stops **immediately** — `runsc` flips the
container to `stopped` at once — but `pod.status.phase` does not become
`Succeeded`/`Failed` until the kubelet's next sync. In that lag window:

- `controller::running_liveness_trigger` is already ordered *ahead* of the
  idle-timeout (`reconcile.rs` lines 364–367, its comment: "a *dead* Pod wins
  over the idle-timeout below: pausing would try to snapshot a corpse"). The
  bug was not trigger *ordering* — it was that the trigger delegated to
  `PodAdapter::pod_terminated`, which keyed terminality **solely on
  `status.phase`** (or a 404). During the phase-update lag it **returned
  `false`** even though the container was already `stopped`, so
  `running_liveness_trigger` yielded `None` and execution fell through to
  `timeout_trigger`.
- `timeout_trigger` (newly active for `Running` sandboxes as of #357) then
  fired `Trigger::Timeout`, the `(Running, Timeout)` edge dispatched a pause
  SnapshotJob, and the node ran `runsc checkpoint` against the stopped
  container → exit 128, `in state stopped`, SnapshotJob `Failed`.

### #384 — the error message fragments Traceway grouping

The grouped signal Traceway hashes is the `RuntimeError` `Display` recorded by
the `pause`/`resume` `#[instrument(err)]` spans. Both runsc sites
(`gvisor.rs` checkpoint and restore) embedded runsc's stderr verbatim, and that
stderr echoes the **container id** we passed. The id is high-cardinality, so
every sandbox produced a *new* exception group (`count: 1`) instead of one
growing group — which is exactly why #382's single bug appeared as two distinct
hashes.

## Fix

### 1. Treat a terminated container as terminal (controller, #382 root)

`crates/sandbawks-controller/src/pod_adapter.rs`: extracted a pure
`pod_is_terminal(&Pod)` helper and call it from `pod_terminated`. It is terminal
when **either** `status.phase` is `Succeeded`/`Failed` **or** any
`status.containerStatuses[].state.terminated` is set. The kubelet writes
`state.terminated` the moment it observes the exit — well before the phase flips
— so `running_liveness_trigger` now returns a trigger (rather than `None`) in
the lag window and folds the Sandbox into `Failed` (via the existing
`(Running, Failure)` edge) instead of falling through to the idle-timeout and
auto-pausing a corpse.

**Knock-on:** `creating_liveness_trigger` also calls `pod_terminated`, so the
widened definition changes it too — a `Creating` Pod whose `envd` crashes before
its phase flips now folds to `Failed` from the container-terminated signal,
rather than only via a 404 or a terminal phase. This is the desired behavior
(the existing `creating_with_vanished_pod_transitions_to_failed` test already
pins the `pod_terminated → true ⇒ Failed` edge for `Creating`; the new signal is
unit-tested in `pod_is_terminal`).

### 2. Scrub the container id from grouped error messages (node, #384)

`crates/sandbawks-node/src/gvisor.rs`: added `scrub_container_id(text, id)`,
which replaces the id with the fixed placeholder `<container-id>`. Applied to
both the checkpoint and restore error paths. The reason text (`in state
stopped`, etc.) is preserved, so a kind of failure still groups *and* stays
self-explanatory. Two stderrs differing only by id now scrub byte-identical, so
Traceway collapses them into one exception class whose count climbs.

The raw id remains queryable per-occurrence: `container_id` is now a span field
(`tracing::field::Empty` in the `pause`/`resume` instrument macros, recorded
right after `resolve_pod`), so it rides in the occurrence attributes rather than
the group key.

Together: #382 removes the dominant *source* of these failures; #384 ensures any
residual race-window or unrelated runsc failure groups cleanly rather than
flooding the issues view.

## Tests

- `pod_adapter.rs`: `terminal_phase_is_terminal`,
  `running_pod_with_live_container_is_not_terminal`,
  `running_phase_with_terminated_container_is_terminal` (the #382 case).
- `gvisor.rs`: `scrub_container_id_replaces_id_with_placeholder`,
  `scrub_container_id_makes_distinct_ids_group` (the grouping property),
  `scrub_container_id_covers_full_id_form`, `scrub_container_id_empty_is_noop`.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — all green (node lib 76, controller lib 36)

## Out of scope

- A general/centralized error-message scrubbing layer — the targeted helper at
  the runsc sites is sufficient.
- A fully-typed "skip pause for an already-dead container" SnapshotJob outcome:
  the residual micro-window between runsc marking the container `stopped` and
  the kubelet writing `state.terminated` still yields a fail-closed pause error,
  but it now groups as a single low-noise exception rather than fragmenting.
