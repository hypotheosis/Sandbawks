# Issue #193 — Snapshot bundle was essentially empty on gVisor

## Summary

`GvisorAdapter::pause` was bundling a near-empty `memory.zst` on every
pause (18 B: the framing of an empty zstd stream) regardless of how
much state the sandbox had accumulated. The bench harness PR #192
surfaced this on a kind cluster: four iterations writing 256 × 1 MiB
of incompressible bytes per pause all produced ~18 B `memory.zst` +
~270 B `rootfs-delta.tar.zst`.

## Root cause

Two compounding facts, neither documented in our adapter:

1. **gVisor's `runsc checkpoint --image-path` writes `*.img` files,
   not `memory*` files.** For gVisor release `20240603.0` (the
   version baked into `sandbawks-kind-node`), checkpoint produces:
     - `checkpoint.img` — saved sentry state.
     - `pages.img` — MemoryFile pages (this is where the sandbox-
       internal overlay-tmpfs upper layer lives).
     - `pages_meta.img` — MemoryFile metadata.
   The pre-fix glob in `tar_zstd_memory_files_blocking` matched
   `memory*`, which catches **none** of these. The resulting tar was
   empty; zstd-of-empty is the 18 B we observed.

2. **gVisor's default `--overlay2=root:self` keeps every rootfs write
   in a sandbox-internal overlay-tmpfs.** That tmpfs lives in sandbox
   memory and is invisible to containerd's snapshotter Diff API — so
   `rootfs-delta.tar.zst` is empty by design on gVisor. The data IS
   captured by `pages.img` (via runsc checkpoint), but only if we
   actually pull `pages.img` into our bundle.

So the bug isn't *where* the workload writes (`/tmp` vs `/workspace`
vs anywhere else) — it's that our pause-side bundler was looking for
files runsc never produces.

## Fix

Change the glob in `crates/sandbawks-node/src/gvisor.rs` from
`memory*` to `*.img` (file extension match, case-insensitive). The
function is renamed `tar_zstd_memory_files_blocking` →
`tar_zstd_checkpoint_files_blocking` for accuracy. The wire-format
part name stays `memory.zst` for manifest v2 compatibility; doc
comments in `crates/sandbawks-types/src/snapshot.rs` are updated to
reflect that the part now carries the runtime's checkpoint state
files, not "memory pages" alone.

The resume side already calls `tar::Archive::unpack` over the same
zstd stream — no behavioural change needed; it now writes
`checkpoint.img` / `pages.img` / `pages_meta.img` back into the work
dir before `runsc restore --image-path`, which is exactly what runsc
expects.

## Test changes

- `crates/sandbawks-node/src/gvisor.rs` — unit test renamed and
  updated to write `checkpoint.img`, `pages.img`, `pages_meta.img`,
  plus a `memory-debug.log` decoy that *would* have matched the old
  `memory*` glob but must not match `*.img`.
- `crates/sandbawks-node/tests/gvisor.rs` — fixture constants
  renamed `MEMORY_FILE_1` → `CHECKPOINT_FILE_1` (and similarly for
  `_2`), with the values changed to the real runsc filenames
  (`checkpoint.img`, `pages.img`). Pause stub + resume test both pick
  up the rename uniformly.

## Bench docstring

`tests/bench/src/main.rs` had a long "Current limitations" preamble
calling out #193 as one of two unfixed gaps. Updated to mark #193
fixed (and to leave #194's iteration-count ceiling in place). No CI
default changes in this PR — that's a separate workflow bump that can
follow once we have a real measurement to set the threshold against.

## Validation

- `just fmt` — pass.
- `cargo clippy --all-targets -- -D warnings` — pass.
- `just test` — pass.
- In-cluster verification deferred to the bench's next CI run after
  merge; reproducing on a local kind cluster is the recommended manual
  step in the PR body.

## Out of scope

- Raising `BENCH_PAYLOAD_MIB` / `BENCH_ITERATIONS` in
  `.forgejo/workflows/bench.yaml`. Those bumps should land in a
  follow-up once a few green CI runs confirm the gate measures what
  it claims.
- Forgejo #194 (envd self-exits after ~3 pauses). Untouched here.

## Follow-up commit: bytes_saved + workload-floor gate

Review noted that the bench's JSON output didn't show per-iteration
snapshot bundle sizes — meaning the original #193 symptom would
silently pass the duration-only gate even after the glob fix shipped
(a 256 MiB workload yielding an 18 B bundle trivially meets the
spec §13 latency target). Follow-up commit on this branch extends
the bench to:

- After each pause, read `SnapshotJob.status.manifest.parts[*].sizeBytes`
  via `kubectl get snapshotjobs -o json`; record `bytes_bundle` and
  per-part sizes in each pause Sample.
- After each resume (when `BENCH_INCLUDE_RESUME=1`), re-fetch the
  same Pause manifest; record `bytes_bundle` on the resume Sample
  and assert the byte sum hasn't drifted from the pause-side value.
  The node-side `runsc restore` already sha256-verifies every part
  it downloads — this is defense-in-depth surfaced in the bench
  output rather than only the node DaemonSet log.
- Add a workload-floor gate keyed on `BENCH_BUNDLE_FLOOR_RATIO`
  (default 0.1): any pause iteration whose `bytes_bundle <
  payload_mib * 1 MiB * floor_ratio` fails the gate. The default
  ratio is intentionally loose (incompressible payload bytes
  round-trip ≈ 1:1 through tar+zstd, but gVisor adds some pages-
  image overhead). Set to `0` to disable when running degenerate
  workloads on purpose.
- Six new unit tests pin the floor-gate logic (zero-payload
  short-circuit, zero-ratio disable, above-floor pass, the explicit
  issue-#193 regression case, resume-phase skip, None-bytes skip)
  + one test pins the JSON shape so `bytes_bundle` / `parts` get
  elided when unset.
