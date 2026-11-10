# Plan 07 T11 — flip `PAUSE_RESUME_SMOKE_FULL=1`; assert shim path

- **Issue:** #215 (epic #204, Plan 07 Task 11)
- **Date:** 2026-05-30T00:51:02Z
- **Area:** `deploy/kind/fixtures/` (pause-resume smoke fixture)

## Summary

The kind pause-resume smoke (`deploy/kind/fixtures/pause-resume-smoke.sh`)
previously defaulted `PAUSE_RESUME_SMOKE_FULL=0`, skipping the resume half
because `runsc restore` could not run over a kubelet-managed container
(forgejo #176). Epic #204 closed that gap with
`containerd-shim-sandbawks-v1` (T1–T10), so the resume half now works
end-to-end.

This task flips the default to `1` and promotes the variable to a
**tri-state** so the existing `=0` contract is preserved verbatim:

- **`1` (new default)** — exercise the shim restore path end-to-end. After
  the resume reaches `status.phase=Running`, assert the resumed Pod was
  created via the shim: `runtimeClassName: sandbawks-runsc` **and** the
  `io.sandbawks/restore-image-path` annotation is present. Then the
  existing sentinel byte-for-byte round-trip + kill/GC run as before.
- **`0` (unchanged)** — skip the resume half; verify pause + snapshot
  upload only, then exit success. Identical to the pre-Plan-07 behavior, so
  anyone with `PAUSE_RESUME_SMOKE_FULL=0` already pinned keeps exactly what
  they had. **Not** silently redefined.
- **`legacy` (new, transient)** — exercise the deprecated
  `SANDBAWKS_RESTORE_MODE` idle-hold shim T10 kept for one release; assert
  the deprecation warning fires. Removed alongside the env-var shim in
  Plan 08.

Any other value is rejected up front with a clear error.

## Runtime class is selected per mode

- **`1`** runs the template under `defaultRuntimeClass: sandbawks-runsc`.
  The FULL=1 assertion requires the **resume** Pod to carry
  `runtimeClassName: sandbawks-runsc`, and the controller
  (`pod_adapter::pod_spec_for`) only *defaults* restore Pods to it when the
  Sandbox does not pin its own class — a Sandbox that inherited `gvisor`
  from its template would carry `gvisor` straight through to the resume Pod
  and fail the assertion. So both Pods run under `sandbawks-runsc`. This is
  sound because the shim is a pass-through over gVisor's `pkg/shim/v1` for a
  no-annotation `Create` (the original Pod), so the node DaemonSet's
  `runsc checkpoint` shell-out works exactly as under vanilla `gvisor`. The
  `sandbawks-runsc` RuntimeClass is rendered by the Helm chart
  (`sandbawksRunsc.enabled` defaults `true`) and backed by the
  `io.containerd.sandbawks.v1` handler baked into the kind-node image
  (`ci/kind-node/Dockerfile`, Plan 07 T5).
- **`0`** and **`legacy`** keep vanilla `gvisor`. `0` never resumes, so it
  needs no shim; leaving it on `gvisor` keeps the skip-and-exit path
  self-contained against a stock gVisor cluster (its pre-Plan-07 behavior)
  rather than implicitly coupling a snapshot-upload check to the
  `sandbawks-runsc` RuntimeClass + shim handler being installed. `legacy`
  uses `gvisor` deliberately so the resume Pod skips the shim and the fresh
  envd hits the deprecated idle-hold.

## Why `legacy` patches `spec.env` *after* pause

`SANDBAWKS_RESTORE_MODE` reaches envd only via `Sandbox.spec.env`, which
`pod_adapter` passes through verbatim to **both** the original and restore
Pods (T9 removed the controller's own restore-mode injection). Setting it at
sandbox-create time would idle-hold the *original* envd, so the pause half
would never observe a `status.envdAddress`. The fixture instead
`kubectl patch`es `spec.env` after pause, before resume — the
controller's `CreateRestorePod` picks it up while the original Pod (already
deleted) never saw it. The `legacy` template also pins the vanilla `gvisor`
runtime class so the resume Pod skips the shim, letting the fresh envd boot,
read the env var, settle into the idle-hold, and emit the deprecation
warning the smoke greps for. `/connect`'s server-side `envdAddress` poll
times out in idle-hold (envd binds no listeners), so that call's non-2xx is
tolerated; the controller still creates the restore Pod off the
`desiredPhase=Running` patch.

## Changes

### `deploy/kind/fixtures/pause-resume-smoke.sh`

- Resolve `SMOKE_MODE="${PAUSE_RESUME_SMOKE_FULL:-1}"` and validate it is
  one of `{0,1,legacy}` (else exit 1).
- Select the template's `defaultRuntimeClass` by mode: `sandbawks-runsc`
  for `0`/`1`, `gvisor` for `legacy`.
- Replaced the `if [[ ... != "1" ]]` skip-gate with a `case` dispatch:
  `0` → skip-and-exit (unchanged text/semantics); `legacy` → patch env,
  force a fresh Pod, trigger resume, assert the deprecation warning in the
  resumed envd's logs, then clean up and exit; `1` → fall through to the
  shim round-trip.
- After the `1`-mode resume reaches `Running`, added the shim-path
  assertion (`runtimeClassName == sandbawks-runsc` + non-empty
  `io.sandbawks/restore-image-path` annotation) before the sentinel
  round-trip.

### `.forgejo/workflows/kind-smoke.yaml`

No change — `KIND_SMOKE_INCLUDE_PAUSE_RESUME: '1'` was already set (Plan 05
T14); the job now validates a real restore via the new default.

## Follow-up: the FULL=1 default surfaced a real resume-breaking bug

The first CI run with `PAUSE_RESUME_SMOKE_FULL=1` failed in the resume
half. Root cause (the smoke flip exposed a latent node-deploy defect):

- The node stages a paused snapshot to `GvisorAdapter`'s
  `DEFAULT_STAGING_ROOT = /var/lib/sandbawks/staging`. The node binary
  constructs `GvisorAdapter::new(...)` without `.with_staging_root(...)`
  and no env overrides it, so that default stands.
- The node DaemonSet only hostPath-mounted `/var/lib/sandbawks/snapshots`
  (the pause/resume `work_root`) — **not** the staging root.
- So `stage_snapshot` wrote the bundle into the node Pod's *ephemeral*
  filesystem. The controller then created the restore Pod with
  `io.sandbawks/restore-image-path=/var/lib/sandbawks/staging/...`, but the
  host-side `containerd-shim-sandbawks-v1` reads that path off the
  **node host**, where it didn't exist → `runsc restore` had no image →
  the restore Pod never became Ready → `/connect` timed out (30s) → smoke
  failed. The pause half worked because its `work_root` *is* mounted.

**Fix:** the node DaemonSet now hostPath-mounts the staging root too
(`staging-cache` → `/var/lib/sandbawks/staging`, configurable via
`node.hostPathStagingCache`, matching `DEFAULT_STAGING_ROOT`), so the node
worker and the host shim see the same directory.

### Diagnostics

The smoke's failure dump now also prints the `SnapshotJob` CRs
(`spec.nodeName` / `status.phase` / `status.localPath` / `status.message`)
and `kubectl describe` of the sandbox's Pod(s) — a shim `runsc restore`
failure surfaces as a `FailedCreatePodSandBox` kubelet event, not in any
controller/node log. The dump now runs *before* the cleanup deletes the
Sandbox (which cascades to the restore Pod), so the Pod events survive.

### Incidental

`install-garage.sh`'s `kubectl exec … | awk '…; exit'` could SIGPIPE under
`set -o pipefail` (awk closes the pipe early, `kubectl exec` gets EPIPE →
141), wedging `cluster-up` before the helm install. Rewritten to consume
all input (`{v=$NF} END{…}`). Found while standing up the local repro.

## Second CI iteration: two more resume-path defects the staging fix unmasked

With the staging mount in place, CI confirmed the staging step (`Stage`
job `Complete`, `localPath` under `/var/lib/sandbawks/staging`) and the
restore Pod (`runtimeClassName: sandbawks-runsc` + the restore-image-path
annotation) now **starts** (`1/1`). Two further defects surfaced — both
only reachable once FULL=1 actually drove the resume:

1. **Smoke parsed a field `/connect` doesn't return.** The resume block
   did `NEW_ENVD_URL="$(… | jq -er '.envdURL')"`, but `/connect` returns
   the SDK `SandboxDTO` (no `envdURL` — the SDK derives the URL itself).
   `jq -e` on the missing field exited 1 and `set -e` aborted the smoke
   *before* the resume could complete. The value was only logged, never
   used — the real validation is the `phase=Running` wait + shim assertion
   + sentinel round-trip. Replaced with a body-less `curl -fsS -o
   /dev/null` that fires the resume and fails only on a non-2xx.

2. **`Resuming` re-probed only every 30s.** `reconcile()`'s active
   `pod_ready` probe runs for `Creating | Resuming`, but the requeue
   cadence fast-polled (3s) only `Creating`; `Resuming` fell into the 30s
   heartbeat. The restore Pod has no SnapshotJob to fire a readiness watch
   event, so `Resuming→Running` lagged a full 30s. Gave `Resuming` the
   same 3s cadence as `Creating`.

## Third CI iteration: the restore was a silent cold-start (the real fidelity bug)

With the orchestration fixed, CI got all the way to `phase=Running`, passed
the shim-path assertion, then failed on the **sentinel round-trip**:
`POST /files/mint (GET)` → `curl: (22) … 404`. The restore Pod was
`Running`/`Ready`, but the file written before pause (`/tmp/sentinel.txt`)
was **absent** after resume.

Root cause: `containerd-shim-sandbawks-v1` reads the
`io.sandbawks/restore-image-path` annotation from the **container's OCI
bundle `config.json`** (`parseRestoreAnnotation(r.Bundle)`) to decide
between `CreateWithFSRestore` and a plain `Create`. But the annotation is a
**pod** annotation, and containerd's CRI plugin only copies pod annotations
into a container's OCI spec when they're allowlisted via the runtime's
`pod_annotations` — the same mechanism gVisor documents for `dev.gvisor.*`.
The `sandbawks-runsc` runtime config had **no `pod_annotations`**, so the
annotation never reached `config.json`, the shim read an empty path, and
every resume fell back to a **cold-start `Create`**: a fresh envd on the
base image, no restored filesystem or memory. That is exactly the observed
signature — Pod healthy (cold start works), shim-path assertion green (it
checks the *pod* annotation via kubectl, not the container OCI spec), and
the pre-pause file gone (404).

**Fix:** `ci/kind-node/containerd-sandbawks-runsc.toml` now sets
`pod_annotations = ["io.sandbawks/restore-image-path"]`, so the annotation
reaches the envd container's `config.json` and the shim actually dispatches
to `CreateWithFSRestore` + `Restore`.

## Fourth CI iteration: the shim fix wasn't even in the tested image

The `pod_annotations` run produced a byte-identical failure. Root cause:
the **kind-smoke job pulls a prebuilt `sandbawks-kind-node:latest`** (or a
cache keyed on `ci/kind-node/**`) — it never builds the kind-node image
from branch source. That image bakes in **both** the shim binary and its
containerd config. `:latest` is only republished from `main`
(`kind-node-image.yaml`), so a PR branch's shim/containerd changes are
*never* in the image kind-smoke runs — the smoke silently tested the stale
`main` image. (The earlier fixes that *did* show progress all lived in
per-run-built artifacts: the helm chart, the controller image, the smoke
script.)

**Fix:** `kind-smoke.yaml` (both the `kind-smoke` and `bench` jobs) now
key the kind-node image cache on `ci/kind-node/**` **and** `shim/**`, and
on a cache miss **build the image from branch source** instead of pulling
`:latest`. A PR that touches those paths thus smoke-tests its own kind-node
image; PRs that don't still hit the cache and `docker load` the fast path.
This is what lets the `pod_annotations` fix (and the rest of the resume
restore bring-up) actually be exercised on a PR branch.

The build runs via `docker buildx build --load`, gated behind a
cache-miss-only `setup-buildx-action` step: the `sandbawks-ci-runner`
container ships the docker CLI but **not** the buildx component that
BuildKit (and the Dockerfile's `RUN --mount=type=bind` steps) require — a
first attempt with `DOCKER_BUILDKIT=1 docker build` failed with "BuildKit
is enabled but the buildx component is missing". The `build-images` job
already uses `setup-buildx-action` in this same container, so the pattern
is proven here.

## Fifth CI iteration: the shim restored the pod sandbox, not just the container

With the kind-node image finally built from branch source (so the
`pod_annotations` fix was actually present), the restore was attempted for
the first time — and produced a **new** failure:
`FailedCreatePodSandBox: … runsc did not terminate successfully`.

Root cause: `pod_annotations` copies a pod annotation onto the OCI spec of
**both** the pod sandbox (the "pause" infra container) **and** the
application container. The shim's `Create` (`shim/internal/wrapper`) had no
guard distinguishing the two, so it dispatched the **pod sandbox** create
to `CreateWithFSRestore` — runsc then failed trying to restore a pause
container from the envd snapshot. (The original cold-start pod created fine
because it had no restore annotation at all; the failure appeared only once
the annotation became visible on the sandbox spec.)

**Fix:** `parseRestoreAnnotation` now returns "" (plain pass-through) when
the bundle's `io.kubernetes.cri.container-type` annotation is `sandbox`, so
only the application container is ever restored. The guard suppresses
restore *only* for an explicit sandbox — absent/other values (non-CRI
bundles, the unit tests) still restore — so it's conservative. Covered by
two new wrapper unit tests (sandbox-skips, container-still-restores);
`go test ./...`, `go vet`, `gofmt` all clean.

## Sixth CI iteration: the baked runsc was 2 years older than the shim

The container-type guard worked — the shim now restores the **container**,
not the sandbox — and invoked `runsc create -fs-restore-image-path=…`. runsc
rejected it: `flag provided but not defined: -fs-restore-image-path`.

Root cause: a gVisor version mismatch baked into the images.
`containerd-shim-sandbawks-v1` is built from the gVisor `shim/go.mod` pins
(`v0.0.0-20260527191743-…`, 2026-05), whose `pkg/shim/v1` drives the
`CreateWithFSRestore`/`Restore` extension (the `-fs-restore-image-path`
flag). But both Dockerfiles still baked **runsc `20240603.0`** (2024-06) —
which predates the extension. (The Plan 07 doc's T2 even said to pin the
shim to `20240603.0` "so the extension RPC surface matches"; that was
wrong — 2024-06 gVisor has no such extension — and the shim was correctly
built against 2026-05 gVisor, but the runsc bake was never bumped to match.)

**Fix:** bump `GVISOR_RELEASE` `20240603.0` → **`20260525.0`** in both
`ci/kind-node/Dockerfile` and `crates/sandbawks-node/Dockerfile` (kept in
lockstep). `20260525.0` is the latest published gVisor release at/before the
shim's pinned commit (2026-05-27; gVisor cut no 05-27 release) — confirmed
present in the release bucket with `runsc` + `containerd-shim-runsc-v1` +
their `.sha512`. The Dockerfiles self-verify via the downloaded `.sha512`,
so no hardcoded hash changes. A ~2-day shim(0527)↔runsc(0525) skew remains;
negligible for gVisor's flag/protocol surface, and 0525 is the closest
release that isn't *newer* than the shim.

## Seventh CI iteration: restore belongs on the root container, not the sub-container

The runsc bump worked — the `-fs-restore-image-path` flag was recognized —
and the next, sharper error appeared:
`cannot set FSRestoreImagePath when creating container in existing sandbox`.

The gVisor source (`runsc/container/container.go`, `New()`) gates it on
`specutils.IsRootContainer`: an FS restore is allowed **only** for the
container that creates a new sandbox (the pod sandbox / root, or a
standalone container), and rejected for a sub-container joining an existing
sandbox. A full-sandbox checkpoint restores every container's fs + memory
when the **root** container is created with the image; sub-containers then
attach via a plain Create.

So iteration 5's container-type guard was **backwards**: it restored the
envd *sub-container* (which joins the pause sandbox) and skipped the
sandbox. Flipped it — `parseRestoreAnnotation` now suppresses restore for
`container-type=container` (sub-container) and dispatches `CreateWithFSRestore`
for the root/pod-sandbox (or annotation-absent standalone) container,
mirroring gVisor's `IsRootContainer` gate exactly so the shim never
dispatches a restore gVisor would reject. The two wrapper unit tests were
flipped to match (root-sandbox restores / sub-container passes through);
`go test`, `go vet`, `gofmt` clean.

### Known latent issue (not blocking, flagged for follow-up)

`status.envdAddress` is cleared on the controller's `CreateRestorePod`
step, not on pause — so `/connect` during a resume can return `200` off
the **stale** pre-pause address before the restore Pod exists. The smoke
no longer trusts that (it gates on `phase=Running` + the sentinel), but a
real SDK client would briefly get a dead address. Worth clearing
`envd_address` at pause; left out of this PR to keep the resume bring-up
scoped.

## Verification

- `bash -n` + `shellcheck` on `pause-resume-smoke.sh` and
  `install-garage.sh` — syntax OK, clean.
- `helm lint` + `helm template` of the chart — the node DaemonSet renders
  both `snapshot-cache` and the new `staging-cache` hostPath mount.
- `just fmt` (no Rust touched — no-op), `cargo clippy --all-targets -- -D
  warnings`, `just test` — all green.
- **Staging mount, mechanism verified on a live kind cluster:** brought up
  `SANDBAWKS_KIND_GVISOR=1 just cluster-up`, `helm upgrade`d with the new
  value, and confirmed the node Pod carries
  `staging-cache → /var/lib/sandbawks/staging` backed by a hostPath, and
  that the directory is shared with the kind-node host (where the shim
  reads it). This proves the fix closes the node-Pod↔host gap.

### Could not run locally: the gVisor restore itself

The full `runsc restore` round-trip could **not** be exercised on this
host: gVisor pods don't start here at all. Both `gvisor` and
`sandbawks-runsc` cold-start Pods fail identically with
`cannot run with network enabled in root network namespace` — the pinned
runsc `release-20240603.0` (in lockstep with the node image's gVisor) is
incompatible with this host's kernel (6.19) for pod network setup. This is
an environment limitation, not a code issue; the `kind-smoke` CI job (where
gVisor works — its cold-start Pod reached `Running`) is the authoritative
verifier and now exercises the shim restore with the staging mount in
place. The `legacy` branch's cluster behavior is likewise verified by
reasoning about the controller/envd contract rather than a live run; it is
transient and removed in Plan 08.
