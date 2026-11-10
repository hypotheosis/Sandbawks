# Plan 07 resume: switch to memory-only `runsc restore`; drop the shim's `CreateWithFSRestore` graft

- **Issue:** #232 (closes the #230 fs-checkpoint seam). Part of epic #204 (Plan 07).
  Decision from #230/#231; gating spill test in #233; surfaced by #229
  (`PAUSE_RESUME_SMOKE_FULL=1`).
- **Date:** 2026-05-31T12:00:00Z
- **Status:** core fix landed and the directly-attached fail-closed hardening
  (checkpoint postconditions) landed, both fully unit/integration-tested. The
  remaining `#232` hardening items and the kind-smoke re-confirmation are
  carried forward as tracked follow-up (see "Deferred" below) — they require a
  live node/cluster to be meaningful and verifiable.

## What changed

### Core fix — memory-only restore (the #230 blocker)

The decision validated in #231 and stress-tested in #233: under sandbawks'
default `--overlay2=root:self` the workload's rootfs writes live in the
sandbox-internal overlay-tmpfs, which `runsc checkpoint` captures in
`pages.img`. A plain `Create` followed by a memory `Restore` therefore
reconstructs memory *and* filesystem on its own — the `CreateWithFSRestore` /
`--fs-restore-image-path` graft was both redundant and broken (it
double-restored the same overlay-tmpfs and failed every restore with
`containerManager.Restore: EOF`).

- **`shim/internal/wrapper/service.go`** — `Create` now always dispatches a
  plain `s.next.Create`; if the bundle carries the restore annotation it
  additionally records the staged image path so `Start` can drive the memory
  `Restore`. The `CreateWithFSRestore` forwarding method is kept *only* to
  satisfy `extension.TaskServiceExt` (the compile-time assertion stays
  meaningful); nothing in the decorator dispatches to it anymore. Package /
  method docs rewritten to explain the memory-only model.
- **`shim/internal/wrapper/annotations.go`** — comments rewritten: the
  `io.sandbawks/restore-image-path` annotation now points at the staged `*.img`
  checkpoint directory for the memory `Restore`. The root-vs-subcontainer guard
  is **kept** — it still applies: a gVisor memory checkpoint is a *whole-sandbox*
  image, so the root container's `Restore` brings every container back, and a
  second `Restore` on a sub-container would re-restore the sandbox. (Reframed
  from "gVisor rejects FSRestoreImagePath on a sub-container" to "the
  whole-sandbox restore already brought it back".)
- **Shim tests** (`service_test.go`, `annotations_test.go`) — the two
  annotation tests now assert a plain `Create` plus recorded restore intent
  (renamed `…RecordsRestoreIntent`); the dead `recordingService.lastRestore`
  field was removed. All other forward/guard tests unchanged and green.
- **`shim/cmd/containerd-shim-sandbawks-v1/main.go`** and **`shim/README.md`** —
  stale "Create → CreateWithFSRestore" descriptions updated to the memory-only
  flow. Also fixed a pre-existing README error: the runtime handler's
  `runtime_type` is `io.containerd.sandbawks.v1` (not
  `io.containerd.sandbawks-runsc.v1`), matching `main.go` and the containerd
  config fragment.
- **`crates/sandbawks-node/src/gvisor.rs`** / **`tests/gvisor.rs`** — comments
  that referenced `CreateWithFSRestore` updated to the memory `Restore`. The
  `rootfs-delta.tar.zst` part is still produced and staged (cheap; the only
  carrier for committed lower-layer writes) but is **not** consumed by the
  memory restore — documented inline with links to #231/#233. (The deprecated
  in-process `resume()` fallback still calls `apply_rootfs_delta`, now a
  documented no-op for the overlay-tmpfs model; the fallback is slated for
  removal in Plan 08.)

### Fail-closed checkpoint postconditions (#233-motivated hardening)

`runsc checkpoint` exiting 0 is necessary but not sufficient: a
partial/truncated checkpoint can leave a missing or empty image that fails
`runsc restore` with an opaque EOF *at resume time*, after the source sandbox
is gone. New `verify_checkpoint_postconditions` (in `gvisor.rs`) runs in
`pause()` immediately after the exit-0 check and before any staging/upload:
each of `checkpoint.img` / `pages.img` / `pages_meta.img` must exist, be a
regular file, and be non-empty — otherwise the pause fails
`CheckpointFailed` and nothing is staged. `pages.img` and `pages_meta.img`
are written as a pair, so the non-empty-on-both requirement catches the
asymmetric-truncation shape directly. Covered by three lib unit tests plus a
new `pause_fails_closed_when_checkpoint_image_missing` integration test (a
stub that exits 0 but omits `pages_meta.img` → pause fails, store stays
empty). The pause stub fixture now writes all three images.

### Smoke (`deploy/kind/fixtures/pause-resume-smoke.sh`)

- **`#232` large-overlay re-confirm**: new `PAUSE_RESUME_SMOKE_LARGE_MIB`
  (default `0`). When positive, the sentinel becomes that many MiB of
  incompressible `/dev/urandom`, exercising the pause→resume round-trip at scale
  (set `=1024` for the ≥1 GiB gating leg). The integrity gate is the existing
  byte-for-byte `cmp` (now sha256-logged and large-file-safe — never dumps a
  multi-MiB payload to the log); a spilled upper would surface as a mismatch.
  Default 0 keeps the fast CI smoke unchanged. Bundle object sizes are logged
  for the large leg per the "record memory.zst size vs bytes written" criterion.
- **Multi-container resume assertion**: after the shim-path check, assert the
  restored Pod's container reports Ready — the signal that the whole gVisor
  sandbox (pod-sandbox/infra container + envd application container) restored in
  one shot. The envd-served file round-trip is the end-to-end corroboration that
  envd's own state restored, not just the pod.
- **JUnit timing telemetry**: the smoke now records the wall-clock duration of
  each labelled phase (`pre-flight`, `create-sandbox`, `write-sentinel`,
  `pause`, `verify-snapshot-bundle`, `delete-pod`, `resume`, `assert-shim-path`,
  `sentinel-roundtrip`, `kill-and-gc`) and, when `PAUSE_RESUME_SMOKE_JUNIT` names
  a path, writes a JUnit XML report there via the `cleanup()` EXIT trap — on
  success *and* failure (the in-flight phase is marked `<failure>` on a non-zero
  exit). Same shape as the SDK conformance suite. The kind-smoke workflow sets
  that env var and uploads the result as a `pause-resume-smoke-results` artifact
  (pinned `actions/upload-artifact@v3`, `if: always()`), so pause/resume phase
  timings land in the same dashboards as the SDK junit. Disabled by default
  (empty env var) so local runs are unchanged.

## CI: JUnit results → PR comment

A new `ci/junit-pr-comment.mjs` renders all three JUnit suites
(`pause-resume-smoke-results.xml`, `sdk-smoke-results.xml`,
`sdk-smoke-js-results.xml`) into a single, upserted pull-request comment — an
overall pass/fail/skip headline, a per-suite table (tests / pass / fail / error
/ skip / time + totals), and a collapsible list of failed/errored cases with
their messages. Added as a `Comment JUnit results on PR` step at the end of the
`kind-smoke` job (`if: always() && github.event_name == 'pull_request'`).

Self-contained on purpose rather than a third-party reporter action:
- The ci-runner image is `node:20-bookworm-slim` (no python3), so the script is
  dependency-free Node (regex JUnit parse + global `fetch`).
- Forgejo's act-runner has no GitHub **Check Runs** API, which the popular
  reporter actions (`mikepenz/action-junit-report`,
  `EnricoMi/publish-unit-test-result-action`, `dorny/test-reporter`) publish
  through — so they would no-op/error here. Forgejo *does* support issue/PR
  comments via its Gitea-style API, which the script posts to with the run's
  `GITHUB_TOKEN`. Same class of constraint that already pins
  `actions/upload-artifact@v3` and fully-qualified `uses:` URLs.

The comment is idempotent (an HTML marker lets it edit its own prior comment
instead of spamming on every push) and the script is best-effort — it always
exits 0, so a comment/parse hiccup never fails the gate, and on a non-`pull_request`
event it just prints the summary to the job log. Parsing + rendering verified
locally with Node against passing, failing (with XML-escaped messages), and
missing-file inputs.

## Deferred (tracked follow-up, not in this PR)

These `#232` acceptance items are real but each needs a live node/cluster to be
meaningful and verifiable, so shipping them as unit-only stubs would be tech
debt. They are called out here and should become scoped sub-issues:

- **Kind-smoke green for the baseline + multi-container + ≥1 GiB re-confirm** —
  the script changes above are in place, but the actual cluster runs (incl. the
  `PAUSE_RESUME_SMOKE_LARGE_MIB=1024` leg) must execute in CI on a real node's
  disk + snapshotter. Cannot be run from this change set.
- **Envd rootfs fingerprint** — hash the writable rootfs into `envd-state.json`
  at pause and re-verify after restore. Cross-cutting (envd fs-walk + node
  verify); its oracle is the live restore.
- **Mount-set assertion at pause** — assert every writable mount is
  overlay-backed. Needs a container OCI-mount seam the node does not currently
  obtain.
- **Resource-headroom policy** — gate pause/restore on node disk + restore-time
  memory vs the committed upper. Needs node disk/mem introspection (a new
  dependency) and is only verifiable under real headroom pressure.
- **Never tear down the source until a restore is proven** — a controller
  state-machine change (throwaway restore + envd health check before GC).

The checkpoint-postcondition gate above is the cheap, always-on structural floor
those items build on; the byte-for-byte large-overlay `cmp` is the strongest
integrity proof available without them.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all suites pass (incl. new node lib + integration tests).
- `just shim-check` (`gofmt`, `go vet`, `go test`) and `just shim-build` — clean.
- `bash -n` on the smoke script — clean.

The kind smoke (`SANDBAWKS_KIND_GVISOR=1 just cluster-up && just
pause-resume-smoke`, optionally with `PAUSE_RESUME_SMOKE_LARGE_MIB=1024`)
cannot be run from this change set and must be exercised in CI on a real node.
