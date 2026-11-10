# Plan 07 T3 — Create-time annotation dispatch to `CreateWithFSRestore` (issue #207)

**Date:** 2026-05-29T02:00:00Z
**Branch:** `feat-issue-207-create-annotation-dispatch`
**Issue:** #207 (epic #204)
**Plan:** `docs/superpowers/plans/2026-05-28-sandbawks-07-shim-wrapper-resume.md`, Task 3

## Summary

Turns the T2 pass-through `Create` into the decorator's primary interception
point. A container whose OCI bundle carries the
`io.sandbawks/restore-image-path` annotation is now redirected to gVisor's
`CreateWithFSRestore` extension RPC; every other container still forwards
straight through to the wrapped service's `Create`. The restore intent is
recorded per container ID so Task 4's `Start` override can route the matching
`Start` to gVisor's `Restore` RPC, and that state is cleared on `Delete`/
`Shutdown`.

## What landed

- **`shim/internal/wrapper/annotations.go`** (new) — `parseRestoreAnnotation(bundleDir)`
  reads `<bundleDir>/config.json`, decodes only the `annotations` map (a narrow
  `ociConfig` struct rather than the full runtime-spec `Spec`), and returns the
  restore image path. Return contract:
  - `("", nil)` — no restore: config.json absent, or annotation missing/empty.
  - `(path, nil)` — restore: annotation present, value absolute and existing.
  - `("", error)` — annotation present but value relative or non-existent, or
    config.json present-but-unreadable/malformed.
  No env-var fallback. The absolute-and-exists checks are a defense against a
  crafted annotation escaping the staging root, and run *before* any dispatch.
- **`shim/internal/wrapper/service.go`**
  - `Service` gains a `restoring sync.Map` (container ID → restore image path).
    `sync.Map` fits the access pattern: write-once on `Create`, read on `Start`
    (T4), delete-once on teardown, all from concurrent RPC goroutines.
  - `Create` parses the annotation, dispatches to `CreateWithFSRestore`
    (`Conf: FSRestoreConfig{ImagePath: path, Direct: false}`) when present and
    records intent **after** a successful dispatch (so a failed restore leaves
    no stale entry), or passes through to `Create` when absent. Invalid
    annotations error before any RPC fires.
  - `Delete` clears the intent for the container ID, but only on a
    container-level delete (empty `ExecID`) — an exec-process delete preserves
    it. `Shutdown` drops every recorded intent.
- **`shim/internal/wrapper/service_test.go`** — `recordingService` gains a
  `lastRestore` field so dispatch tests can assert on the `FSRestoreConfig`
  built (image path + `Direct` flag), not merely that the RPC fired.
- **`shim/internal/wrapper/annotations_test.go`** (new) — the three smoke
  cases plus three guards: relative-path rejection, `Delete` clears only on
  container-level delete, and `Shutdown` clears all intent.

## Design notes

- **Missing `config.json` is not an error.** It returns `("", nil)` so the
  pass-through path stays viable for the generic forwarding table test (which
  drives `Create` with an empty request and therefore no bundle). A
  present-but-broken config.json *is* an error, since it signals a genuinely
  malformed bundle rather than "nothing to restore".
- **Intent recorded post-dispatch.** Storing only after `CreateWithFSRestore`
  succeeds avoids a stale entry when the restore itself fails.
- **`ExecID`-aware `Delete`.** A `DeleteRequest` with a non-empty `ExecID`
  targets an exec process, not the container, so it must not drop the
  container's restore intent.

## Validation

- `just shim-check` — `gofmt` clean, `go vet ./...` clean, `go test ./...`
  green, `go build ./...` ok.
- `go test -race ./...` — green (exercises the `sync.Map` under the race
  detector).

Note: this is the Go shim subtree, gated by `just shim-check` (its documented
analogue of the Rust `just check`), not `just fmt` / `cargo clippy` / `just
test` — there is no `Cargo.toml` under `shim/`.
