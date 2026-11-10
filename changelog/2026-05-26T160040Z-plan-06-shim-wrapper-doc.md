# 2026-05-26 — Plan 06 doc: `containerd-shim-sandbawks-v1` wrapper for kubelet-mediated resume

Doc-only change. Lands the written Plan 06 design at
[`docs/superpowers/plans/2026-05-26-sandbawks-06-shim-wrapper-resume.md`](../docs/superpowers/plans/2026-05-26-sandbawks-06-shim-wrapper-resume.md);
no code or deploy changes in this PR. Closes part of issue
#176 by committing the
design; the implementation tasks (T1–T12 in the plan) ship as separate
PRs under the same issue umbrella, following the Plan 05 pattern.

## Why this plan, why now

Plan 05 + issues #170/#171/#173/#174 landed the entire controller-side
scaffold for fresh-Pod resume: Pod recreate, `SANDBAWKS_RESTORE_MODE=1`
envd idle hold, sha256 binding pause→`Sandbox.status`→
`SnapshotJobSpec.expected_sha256`→node cross-check, live `/__state` HTTP
probe. The smoke confirms the orchestration is correct end-to-end. The
runtime call itself fails because `runsc restore` rejects
kubelet-managed containers — they are in state `running` (or `paused`)
and `runsc restore` accepts only `stopped`/`created`. Issue #176 frames
this as "switch to containerd CRI Checkpoint/Restore RPCs" but
verification of the upstream code shows that framing is partly wrong:

- gVisor's containerd shim returns `errdefs.ErrNotImplemented` for the
  standard Task `Checkpoint` RPC, so the pause side doesn't benefit from
  going through containerd.
- KEP-2008 (the only upstream KEP touching this area) explicitly puts
  *restore* out of scope for kubelet. There is no upstream-supported
  "CRI Restore" RPC to switch to.
- However, gVisor's shim already exposes `CreateWithFSRestore` and
  `Restore` as *extension* RPCs in
  [`pkg/shim/v1/extension/extension.go`](https://github.com/google/gvisor/blob/master/pkg/shim/v1/extension/extension.go),
  implemented by
  [`pkg/shim/v1/runsc/service.go`](https://github.com/google/gvisor/blob/master/pkg/shim/v1/runsc/service.go).
  Containerd doesn't call them (they're outside the standard Task
  service), but a small custom shim that wraps the existing gVisor shim
  packages can translate annotated CRI Create/Start into them.

Plan 06 ships exactly that wrapper. The result is the standard kubelet →
containerd → shim path *unchanged*; the shim transparently substitutes
restore semantics when it sees the sandbawks annotation. Operators
install one extra binary (drop-in next to vanilla `runsc`) and one
RuntimeClass YAML — same operational pattern as installing gVisor itself.

## What this PR contains

- `docs/superpowers/plans/2026-05-26-sandbawks-06-shim-wrapper-resume.md`
  — full plan, 12 tasks with file-level scope, verification gates, and
  commit-message templates per task.
- This changelog entry.

## What this PR does not contain

No Rust code, no Go code, no Helm template, no Dockerfile, no smoke
script changes. Those land in subsequent PRs corresponding to Tasks
T1–T12 in the plan.

## Decisions locked here (do not relitigate in T1–T12 PRs)

1. **Custom shim binary that wraps gVisor's `pkg/shim/v1`** — rejected
   alternatives: NRI/CRI plugin (containerd restart burden), forking
   gVisor's `cmd/containerd-shim-runsc-v1` source (couples sandbawks to
   gVisor's source tree), waiting for upstream KEP support (no such KEP
   exists).
2. **Annotation-driven dispatch, not RuntimeClass-per-restore** — Pods
   that need restore set `io.sandbawks/restore-image-path` in
   annotations; RuntimeClass stays singular per node.
3. **Additive RuntimeClass `sandbawks-runsc`**, vanilla `gvisor` from
   Plan 05 stays.
4. **Pause path stays direct `runsc checkpoint` shell-out**, since the
   shim's standard `Checkpoint` is `ErrNotImplemented` and pause doesn't
   have the lifecycle problem restore does.
5. **Snapshot staging via new `SnapshotJobMode::Stage`** — controller
   stages the bundle to a local path on the target node *before*
   creating the resume Pod; the shim reads a local path, not S3.
6. **`SANDBAWKS_RESTORE_MODE` envd shim is deprecated**, kept for one
   release (Plan 06 ships both paths), removed in Plan 07.
7. **Go subtree at `shim/`** — first Go code in the repo; rationale
   documented in the plan and a `shim/README.md`.

## Plan 06 ↔ Plan 05 numbering note

Plan 05's "Next plan" preview slot named Plan 06 = SDK compat +
benchmarks + retention. That work is renumbered to Plan 07+. Plan 06 is
now the runtime gap, because shipping a working resume gates the SDK
compat work (SDK tests need a working `connect` after `pause` to be
meaningful).

## Acceptance for this PR

- The plan doc exists and renders correctly in Forgejo.
- The plan's architectural decisions are explicit enough that T1–T12 PR
  authors do not need to re-derive them from upstream source.
- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test
  --workspace --all-features` all stay green (this is doc-only, so this
  is a regression check, not a forward-progress check).
