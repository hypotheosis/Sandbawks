# Plan 08 B — gVisor lazy / page-fault restore (issue #262)

Date: 2026-06-02

Workstream B of Plan 08 (epic #260). Flips gVisor restore from eager to
lazy / page-fault, and ties the staged restore bundle's lifetime to the
resumed sandbox so it survives the on-demand page reads lazy restore
performs after `Start` returns.

## B1 — enable lazy / page-fault restore

- `shim/internal/wrapper/service.go`: `RestoreConfig.Background` flipped
  `false -> true`. `runsc restore` now returns from `Start` before every
  page is faulted in; gVisor reads the remaining pages from the
  `--image-path` bundle on demand for the resumed container's lifetime
  (spec §6 / §13). The comment now cross-references the B2 retention
  requirement.
- `shim/internal/wrapper/annotations_test.go`: the restore-dispatch test
  now asserts the dispatched `RestoreConfig.Background == true`.

## B2 — retain the staged bundle for the sandbox lifetime

With lazy restore the staged `--image-path` bundle must outlive container
start. Reclaim is therefore keyed to the owning `Sandbox`'s phase, not to
`SnapshotJob` deletion.

- `crates/sandbawks-node/src/gvisor.rs`:
  - `staging_bundle_needed(phase) -> bool` — pure retention predicate.
    Keep on `Running`/`Resuming`/`Pausing` (a live container may fault a
    page); drop on `Creating`/`Paused`/`Failed`/`Terminating` and on the
    CR-absent (`None`) case. Exhaustive match — a future `SandboxPhase`
    variant forces a conscious keep/drop decision.
  - `SandboxPhaseLookup` trait + `KubeSandboxPhaseLookup` (over
    `kube::Api<Sandbox>`) — resolves a sandbox's phase, distinguishing
    absent (`Ok(None)`) from a transport error.
  - `sweep_staging_dirs(staging_root, lookup)` — reclaims each
    `<sandbox-id>` subtree whose owner no longer needs it; best-effort
    per-subtree removal so one stuck dir can't strand the rest; a missing
    root is an empty sweep, not an error.
  - `run_staging_sweeper(...)` — fixed-interval loop, supervised by the
    binary like the #282 envd-staged reconciler.
- `crates/sandbawks-node/src/bin/sandbawks-node.rs`: spawns and supervises
  the sweeper; adds `--staging-sweep-secs` / `SANDBAWKS_STAGING_SWEEP_SECS`
  (default 60 s).
- `crates/sandbawks-node/tests/staging_gc.rs`: predicate coverage for every
  phase + CR-absent; sweep fixtures (keep `Running`, reclaim absent;
  lifecycle `Running` -> kept then `Paused` -> reclaimed; missing root).

Controller side: confirmed no change needed. `SnapshotJobStore` exposes
only `get`/`create` — the controller never deletes a `SnapshotJob`, so it
cannot trigger a mid-`Running` reclaim. SnapshotJob CRs are GC'd by
Kubernetes via `OwnerRef` only when the `Sandbox` is deleted, and the
state machine flips `Paused -> Resuming` *before* `StageSnapshot` runs, so
a freshly-staged bundle never coexists with `Paused` (delete-on-`Paused`
is race-free).

## B3 — tighten the resume p95 CI gate

- `.forgejo/workflows/kind-smoke.yaml`: `BENCH_RESUME_P95_MS` tightened
  `6500 -> 6000`. At the job's 16 MiB payload the memory image is tiny, so
  resume is still dominated by fixed pod-lifecycle overhead that lazy
  restore does not touch; lazy restore can only lower the restore step, so
  6000 ms keeps ~800 ms headroom over the eager-measured ~5.2 s baseline.
  The spec §13 2 s target needs a GiB-scale re-measurement (where the
  page-fault win dominates), tracked as a follow-up. `DEFAULT_RESUME_P95_MS`
  stays 2000 as the documented goal.
- `tests/bench/src/main.rs`: unchanged — the resume arm measures wall-clock
  resume time, which already exercises the lazy path through the flipped
  shim.

## Review follow-ups (PR #290 review)

- `sweep_staging_dirs`: `phase_of` and `entry.file_type()` errors are now
  log-and-`continue` (per-subtree best-effort) instead of `?`-aborting the
  whole pass, matching the doc — one flaky kube `Get` or bad inode no longer
  starves other reclaims for a tick. New `tests/staging_gc.rs` case
  (`sweep_skips_lookup_error_but_reclaims_others`) pins it; doc/`# Errors`
  updated.
- `DEFAULT_STAGING_ROOT`: docstring now warns that the adapter and the node
  binary's sweeper read it in lockstep — a future `SANDBAWKS_STAGING_ROOT`
  override must thread into both or they desync.
- `KubeSandboxPhaseLookup`: documents the load-bearing single-namespace
  assumption (a cross-namespace sandbox reads as `Ok(None)` → reclaimed while
  `Running` = silent data loss).
- `shim/internal/wrapper/service.go`: notes that only the resume Pod's *root*
  container drives Restore (sub-containers pass through, per
  `TestCreateSubContainerWithAnnotationPassesThrough`), tying the Go-side
  "bundle outlives container start" invariant to the root sandbox's phase.
- `kind-smoke.yaml`: records the provenance of the ~5.2 s baseline (an
  *inherited* Plan 07 eager figure, #229 / PR #234, not a fresh lazy
  measurement) and links the B3 re-measure follow-up #291.
