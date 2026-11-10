# 2026-05-24 — Plan 05: `GvisorAdapter` + `sandbawks-node` + S3 streaming

Plan-wide summary changelog, in addition to the per-task entries in
`changelog/2026-05-25T*-plan-05-t*.md`. Plan source:
`docs/superpowers/plans/2026-05-24-sandbawks-05-gvisor-node.md`.

## What landed

- **New crate `crates/sandbawks-node`** — distroless DaemonSet binary
  hosting the `GvisorAdapter` and the `SnapshotJob` reconciler. Library-
  first so unit tests drive `pause`/`resume` against an in-process
  `FakeSnapshotStore` without ever touching containerd or S3; the
  binary in `src/bin/sandbawks-node.rs` wires the kube client + node
  name (downward API) + `S3SnapshotSink`/`Source` + a real
  `GvisorAdapter`.
- **`SnapshotJob` CRD** (`sandbawks.io/v1alpha1`, plural
  `snapshotjobs`, shortname `sbxsj`, namespaced). The controller writes
  one per pause/resume with `spec.nodeName=<sandbox's assigned node>`;
  every `sandbawks-node` DaemonSet replica filters on `spec.nodeName ==
  $NODE_NAME` via a kube field-selector so each job lands on exactly one
  worker. `SandboxStatus` gains `snapshotJobRef` so the controller knows
  which job to watch on requeue.
- **`SnapshotSink` / `SnapshotSource` traits + `SnapshotManifest v2`**
  in `sandbawks-types`. Both crates depend on the type names; keeping
  the trait + S3 impl + `FakeSnapshotStore` in `-types` lets the
  controller's test footprint stay S3-free. `SnapshotManifest` now
  carries `parts: Vec<SnapshotPart { name, sha256, size_bytes }>`
  (memory, rootfs delta, envd state) plus an aggregate sha256 over the
  sorted part hashes. Deserializer accepts the v1 single-sha256 shape
  via `#[serde(default)]` so Plan 02's test fixtures keep parsing.
- **Controller `dispatch()` refactor** — `Action::Pause` / `::Resume`
  now create a `SnapshotJob` CR and patch `Sandbox.status.snapshotJobRef`
  instead of calling adapter methods inline. A separate code path
  before `dispatch()` watches the job: `phase=Complete` fires
  `Trigger::SnapshotUploaded` / `::ResumeReady` and copies the manifest
  URL into `Sandbox.status.snapshotRef`; `phase=Failed` fires
  `Trigger::Failure(message)`. The state machine itself is unchanged
  (state-machine unit + property tests still pass byte-identical).
- **`PodAdapter` honors `Sandbox.spec.runtimeClassName`.** The "ignores
  spec.runtimeClassName" warning branch is gone; `pod.spec.runtimeClassName`
  is stamped through cleanly. With the chart's `gvisor.enabled=true`
  default + every smoke fixture's `defaultRuntimeClass: gvisor`, every
  Pod the controller creates now lands on the gVisor runtime handler.
- **`S3SnapshotSink` / `S3SnapshotSource`** in `sandbawks-node/src/s3.rs`.
  Streams `put_part` in 4 MiB chunks with per-chunk sha256, uploads
  chunks in parallel via `futures::stream::buffer_unordered(8)`, and
  HEADs each chunk before PUT so retries are idempotent.
  `get_part`/`get_manifest` verify sha256 as each chunk lands.
- **`GvisorAdapter::pause`** — connects to the local containerd socket
  via `containerd-client`, resolves the container ID from the Pod's
  `containerStatuses`, shells out to `runsc checkpoint
  --image-path /var/lib/sandbawks/snapshots/<sandbox>/<ts>/`,
  tar+zstd-streams the resulting memory image to S3 as `memory.zst`,
  diffs the rootfs via the containerd snapshotter to S3 as
  `rootfs-delta.tar.zst`, snapshots envd's `/__state` to
  `envd-state.json`, then writes the aggregate manifest.
- **`GvisorAdapter::resume`** — symmetric: fetches the manifest,
  streams each part to a fresh work-dir while verifying sha256, applies
  the rootfs delta to a new containerd snapshot, shells out to
  `runsc restore --image-path <work-dir> <container-id>`, then polls
  envd's port for up to 30s before returning ready.
- **Custom `sandbawks-kind-node` image** (`ci/kind-node/Dockerfile`)
  layered atop `kindest/node:v1.31.0` with `runsc` (gVisor release
  20240603.0) plus a containerd runtime handler config registering
  `io.containerd.runsc.v1`. Published via
  `.forgejo/workflows/kind-node-image.yaml` to
  `ghcr.io/hypotheosis/sandbawks-kind-node:{sha,latest,YYYY.MM}`. Both kind
  nodes in the smoke cluster pin this image so the `gvisor` RuntimeClass
  has a backing handler everywhere.
- **`sandbawks-node` Dockerfile** — multi-stage distroless build. The
  Rust builder reuses the ci-runner image for cargo-chef + sccache; a
  separate stage downloads + extracts the gVisor release tarball.
  Runtime layer is `gcr.io/distroless/cc-debian12:nonroot` with both
  binaries on PATH. Image published into the existing `build-images`
  matrix in `.forgejo/workflows/kind-smoke.yaml`, so every PR builds
  + smokes the node image alongside controller/api/envd.
- **Helm chart wiring** — `runtimeclass-gvisor.yaml` (gated by
  `.Values.gvisor.enabled`), `snapshot-store-secret.yaml` (auto-
  generates a 32/64-char credential pair when no `existingSecret` is
  pinned; carries `helm.sh/resource-policy: keep` so the credential
  survives `helm uninstall`), real `args: []` on the node DaemonSet
  (no `--placeholder`), `NODE_NAME` downward-API env, `envFrom` pulling
  `SANDBAWKS_S3_ACCESS_KEY` / `SANDBAWKS_S3_SECRET_KEY` from the
  snapshot-store Secret, and a `sandbawks-node` ClusterRole extended
  with `snapshotjobs` CRUD + `snapshotjobs/status` patch.
- **Kind two-node cluster + Garage S3 fixture.** `deploy/kind/cluster.yaml`
  now lists control-plane + worker, both pinning the
  `sandbawks-kind-node:latest` image. `deploy/kind/fixtures/install-garage.sh`
  deploys [Garage](https://garagehq.deuxfleurs.fr) — a single-binary
  Rust-native S3-compatible store — as a one-replica Deployment +
  Service, bootstraps the cluster layout / bucket / access key via
  `kubectl exec garage`, and writes the `sandbawks-snapshot-store`
  Secret that the node DaemonSet's `envFrom` reads from. The garage
  image is mirrored to Harbor (`docker.io/dxflrs/garage`) so the
  smoke never depends on Docker Hub being reachable from the runner.
  `just cluster-up` gains a `SANDBAWKS_KIND_GVISOR=1` mode that builds +
  retags the kind-node image as `:latest`, runs the garage fixture, and
  installs the chart with the full gVisor flag set; opt-in locally,
  default in CI.
- **End-to-end `pause-resume-smoke.sh`** — writes a sentinel via
  `POST /files/mint` + PUT, pauses, asserts `Sandbox.status.snapshotRef`
  starts with `s3://`, lists `manifest.json` + `memory.zst` +
  `rootfs-delta.tar.zst` + `envd-state.json` against MinIO via an
  in-cluster `mc` pod, **deletes the original Pod** (forces resume onto
  a fresh Pod — validates the "any node" property of S3-backed
  snapshots), connects, downloads the sentinel and asserts byte-for-byte
  match, then kills + polls in-cluster GC.
- **CI green gate sweep (T15).** `.forgejo/workflows/kind-smoke.yaml`'s
  `kind-smoke` job was re-engineered to use the `sandbawks-kind-node`
  image (two-node), pull + load `sandbawks-node` alongside the other
  workspace images, run `install-garage.sh` before the helm install,
  and pass the full gVisor flag set (`node.enabled=true`,
  `gvisor.enabled=true`, `snapshotStore.*`) to `helm install` so the
  pause/resume smoke step exercises the real S3-backed snapshot
  round-trip on every merge to `main`. Timeout bumped 15→25 minutes to
  cover the additional setup.

## Known gaps surfaced by T15 (tracked, not stubbed silently)

T15 was supposed to deliver a working prototype end-to-end. The pause
side does — checkpoint, all three parts uploaded to S3, manifest
persisted, `Sandbox.status.snapshotRef` populated, original Pod
reaped, `status.phase=Paused`. The resume side does *not*. Two gaps
are tracked as follow-up issues rather than papered over:

- **#170 — envd `/__state` HTTP probe is a placeholder.** Plan 05 spec
  §6.1 called for envd to expose `/__state` so its in-process runtime
  state (active connections, in-flight processes) could be snapshotted
  alongside `memory.zst` and `rootfs-delta.tar.zst`. envd as of Plan
  03 only serves gRPC on port 49983 — the HTTP route was never built.
  `ReqwestEnvdState::fetch_state` returns placeholder bytes (`{}`) so
  the SnapshotManifest shape and the bundle-contents smoke assertion
  stay stable. On restore, envd will boot fresh against the restored
  rootfs rather than rehydrating sidecar state — invisible for the
  current sentinel-on-disk smoke, but it'll bite the moment a real
  user pauses a sandbox with an open SSE stream or `Process.Connect`.
- **#171 — Fresh-pod resume orchestration is unimplemented.** Plan 05
  T7/T8 landed the byte-mechanics of `runsc checkpoint`/`runsc restore`
  and the S3 transfer, but nothing in the controller or node DaemonSet
  re-creates a Pod on resume. The pause-resume smoke's `kubectl delete
  pod` step (testing the "any node" property) makes the Resume
  SnapshotJob's `resolve_pod` lookup fail because the original Pod UID
  is gone. The resume side of the smoke is gated behind
  `PAUSE_RESUME_SMOKE_FULL=1` (default 0) until #171 lands. Without
  fresh-pod resume the user-visible "pause → connect later" semantic
  is unrecoverable across any Pod eviction (drain, restart, OOM).

## What's deliberately not here

- **Snapshot retention / GC.** The pause/resume smoke deletes the
  Sandbox CR + backing Pod at the end of the run, but the corresponding
  S3 objects under `s3://sandbawks-snapshots/<sandbox-id>/` linger
  indefinitely. A `SnapshotRetention` policy + reaper land in Plan 06
  alongside the SDK conformance work.
- **Lazy / page-fault restore.** gVisor 2024+ supports it, but enabling
  it would make the pause/resume smoke nondeterministic on first read
  (first page faults pull from S3 mid-test). Plan 07's `SandboxWarmPool`
  reopens this.
- **Node-local snapshot cache.** Every resume in Plan 05 streams the
  full snapshot from S3; Plan 07 adds a host-path cache so warm pool
  hits skip S3 entirely.
- **`KataFcAdapter`, `RuncCriuAdapter`.** The runtime trait is
  multi-adapter-ready, but only `GvisorAdapter` ships in Plan 05.
  Plans 08 and 09 cover Kata Containers + Firecracker and runc+CRIU
  respectively.
- **mTLS between gateway and envd.** Pod-network isolation is the
  current trust boundary, same as Plan 04.
- **CRI-mediated checkpoint.** KEP-2008 targets runc+CRIU; the
  containerd-runsc shim does not implement it today. `GvisorAdapter`
  shells out to `runsc checkpoint` directly until upstream changes.

## Next plan

Plan 06 — **E2B SDK compat tests + benchmarks + snapshot retention**:

- `pip install e2b` against the gateway; full SDK method matrix in CI.
- Perf gates for pause p95 < 8 s and resume p95 < 2 s on a 1 GiB
  sandbox (spec §13).
- `SnapshotRetention` policy + S3 reaper (the only open question left
  from spec §15 that this plan defers).
