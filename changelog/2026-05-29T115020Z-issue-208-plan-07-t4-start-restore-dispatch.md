# Plan 07 T4 — Start-time dispatch to Restore for restored containers

- **Issue:** #208 (epic #204, Plan 07 Task 4)
- **Date:** 2026-05-29T11:50:20Z
- **Area:** `shim/` — `containerd-shim-sandbawks-v1`

## Summary

Completed the resume seam in the gVisor task-service decorator. Task 3 (#207)
taught `Service.Create` to redirect a restore-annotated container to gVisor's
`CreateWithFSRestore` and remember its image path in a per-container-ID
`sync.Map`. This task teaches `Service.Start` the mirror move: when a container
ID carries a recorded restore intent, consume it and dispatch gVisor's `Restore`
extension RPC with the remembered image path; otherwise pass straight through to
the wrapped service's `Start`.

With Create and Start both intercepting, an annotated CRI create/start pair now
flows through `CreateWithFSRestore` + `Restore` end to end — the runtime-side
gap #204 set out to close.

## Changes

- `shim/internal/wrapper/service.go`
  - `Service.Start` now intercepts container-level starts. It guards on an empty
    `ExecID` (an exec process inside a running container never restores, matching
    `Delete`'s guard), consumes the intent with `LoadAndDelete` (atomic
    read-and-clear, so a stop/start cycle restores exactly once), and dispatches
    `extension.RestoreRequest{Start, Conf: {ImagePath, Direct: false,
    Background: false}}`.
  - `RestoreConfig.Background` is held `false` with a one-line comment marking it
    as gVisor's lazy / page-fault restore seam — Plan 08 flips it to `true`.

- `shim/internal/wrapper/service_test.go`
  - `recordingService` gained a `lastRestoreReq` field and its `Restore` fake now
    captures the request, so dispatch tests can assert on the built
    `RestoreConfig`, not merely that `Restore` fired.

- `shim/internal/wrapper/annotations_test.go`
  - Added the three T4 smoke cases plus an ExecID guard test:
    1. no intent → `Start` passes through;
    2. intent → `Restore` with the remembered path, `Direct`/`Background` false,
       intent consumed;
    3. two containers with distinct paths don't cross-contaminate;
    - exec-process `Start` passes through and leaves the container intent intact.

## Validation

Go subtree gate (`just shim-check`) — gofmt, `go vet`, `go test ./...`,
`go build ./...` — all green. The Rust workspace gate (`just check`) is not
applicable: this change touches only the Go `shim/` subtree, which the justfile
and CI run as an independent, path-filtered job.

## Notes

- `RestoreConfig.Background = true` (lazy / page-fault resume) is explicitly out
  of scope here — Plan 08.

## Review response (PR #221)

- **Dispatch-error axis (service.go).** Documented why the intent is cleared
  before `Restore` dispatches: a failed restore surfaces as the `Restore` error
  and degrades to a plain boot on a later `Start` rather than silently retrying
  behind a stale intent — recovery (delete + recreate) is the orchestrator's job.
  Behavior unchanged; the docstring now closes the loop on the error path.
- **Cross-contamination test robustness (service_test.go / annotations_test.go).**
  Replaced the fake's single `lastRestoreReq` with a `restoreReqs` slice
  mirroring `calls`, and pinned each dispatch in the two-container test to its
  own indexed request, so reordering or batching the Start calls can no longer
  mask a cross-contamination regression.
- **`intent.(string)` assertion.** Confirmed left as-is: the unchecked assertion
  is the correct failure mode for a write-contract violation, as the invariant
  comment documents.
