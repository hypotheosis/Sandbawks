# sandbawks Plan 05 — `GvisorAdapter` in `sandbawks-node` + S3-streamed snapshots + end-to-end pause/resume on `kind`

> **Scope.** Fifth of ~9 sub-plans decomposing the sandbawks MVP. After Plan 04
> the controller materializes real Pods but `PodAdapter.pause` is a no-op — the
> recorded state of a sandbox is *not* preserved across a `pause → connect`
> cycle. Plan 05 lands the real mechanic: a `GvisorAdapter` that
> snapshots/restores via `runsc checkpoint`, S3-streamed snapshots with chunked
> + sha256-verified uploads, and `PodAdapter` honoring
> `Sandbox.spec.runtimeClassName`.
>
> See [`docs/superpowers/specs/2026-05-21-sandbawks-mvp-design.md`](../specs/2026-05-21-sandbawks-mvp-design.md)
> §6 (pause/resume mechanics), §14 sub-plan 5 (scope), §15 (open questions —
> snapshot retention is the only one this plan defers).

## Predecessor plans

- [Plan 01 — types + CRDs](2026-05-21-sandbawks-01-types-and-crds.md)
- [Plan 02 — controller skeleton](2026-05-22-sandbawks-02-controller-skeleton.md)
- [Plan 03 — `envd-rs` + reference template image](2026-05-23-sandbawks-03-envd.md)
- [Plan 04 — sandbawks-api E2B-compatible HTTP/WS gateway](2026-05-23-sandbawks-04-api-gateway.md)

## Goal

Ship `crates/sandbawks-node` (DaemonSet hosting `GvisorAdapter`) plus a
`SnapshotJob` CRD that bridges the controller to the right node so that, after
Plan 05, an operator running `helm install sandbawks` on a kind cluster with
the new `sandbawks-kind-node-runtime` image gets:

- `POST /sandboxes/<id>/pause` → real `runsc checkpoint` → multi-part snapshot
  streamed to S3 (MinIO in tests, AWS S3 in prod) with per-chunk sha256.
- The sandbox's Pod is reaped; `Sandbox.status.phase=Paused`,
  `status.snapshotRef=s3://...`.
- `POST /sandboxes/<id>/connect` → fresh Pod scheduled on any node with the
  same RuntimeClass → snapshot downloaded → `runsc restore` → byte-identical
  process + memory + filesystem state.
- `Sandbox.spec.runtimeClassName` is honored end-to-end:
  `pod.spec.runtimeClassName` is set; kubelet routes to the gVisor runtime
  handler; the kind cluster ships with gVisor installed.

## Architectural decisions locked in this plan

These were resolved during brainstorming; **implementers should not relitigate
them**.

### 1. `SnapshotJob` CRD is the dispatch mechanism

The controller writes a `SnapshotJob` CR with
`spec.nodeName=<pod's assigned node>`; the `sandbawks-node` DaemonSet on that
node reconciles only `SnapshotJob`s whose `spec.nodeName` matches its own
`$NODE_NAME` (from the downward API).

- **Why:** Kubernetes-native, durable across controller and node restarts,
  observable via `kubectl get snapshotjobs -A`, leverages the existing
  `kube::runtime::Controller` infra in both crates.
- **Rejected:** Per-node headless gRPC service (bespoke transport, harder to
  retry, no obvious observability story) and Pod-annotation-as-queue (conflates
  request state with Pod metadata, surprising during debugging).

### 2. Custom `kindest/sandbawks-node-runtime` kind node image

Built from `kindest/node:v1.31.0` + `runsc` + a containerd runtime handler
config that registers `runsc` as a runtime class. Published to Harbor like
`ci-runner`.

- **Why:** Reproducible across CI runs (no per-run gVisor download), no manual
  bootstrap, parallels the existing `sandbawks-ci-runner` image pattern.
- **Rejected:** Bootstrapping gVisor inside each kind run (slow, fragile,
  cache-unfriendly) and skipping gVisor on kind entirely (drops the spec's
  "kind cluster with gVisor RuntimeClass + MinIO" requirement from §12).

### 3. `runsc checkpoint` is invoked directly via subprocess

KEP-2008 (`CheckpointContainer` CRI RPC) targets runc + CRIU, **not** gVisor —
the containerd-runsc-shim does not implement the CRI checkpoint API.
`sandbawks-node` resolves the sandbox's container ID via the local containerd
gRPC socket (using `containerd-client`), then shells out to
`runsc checkpoint --image-path /var/lib/sandbawks/snapshots/<job> <container-id>`.
Resume is symmetric (`runsc restore`).

- **Why:** Reality of the current gVisor + containerd stack; there is no
  CRI-mediated path.
- **Future:** When the runsc-shim grows CRI checkpoint support, the
  `GvisorAdapter` is the single point of contact to swap.

### 4. MinIO is a kind fixture, not a Helm sub-chart

Production users bring their own S3 (real AWS S3, their own MinIO cluster, or
a managed offering). The Helm chart only takes endpoint + bucket + credentials;
the kind fixture (`deploy/kind/fixtures/install-minio.sh`) installs MinIO via
the upstream Bitnami chart and seeds a bucket.

- **Why:** Keeps the production chart focused on sandbawks's own components.

### 5. All kind smoke tests share the gVisor-enabled kind node image

Plan 04's `api-smoke.sh` already templates `defaultRuntimeClass: gvisor`; once
`PodAdapter` honors it (Task 5), every smoke test runs under gVisor.

- **Why:** Catches RuntimeClass-dispatch regressions on every PR, not just
  Plan 05's bespoke test. Same kind cluster shape across all CI smoke jobs
  simplifies caching.

### 6. `SnapshotManifest` evolves to multi-part with per-part sha256

The Plan 02 placeholder shape (single `sha256` string) is extended to
`parts: Vec<SnapshotPart { name, sha256, size_bytes }>` covering `memory.zst`,
`rootfs-delta.tar.zst`, `envd-state.json`. The existing single `sha256` field
is repurposed to mean "aggregate sha256 over the sorted part hashes" — useful
for fast integrity checks without parsing all parts.

- **Why:** The spec's S3 layout (§6.1) requires multi-part; the v1 shape was a
  temporary placeholder.
- **Compat:** The deserializer accepts the v1 single-sha256 shape and treats
  `parts` as empty so existing test fixtures keep parsing.
- **Retired:** The `inline://manifest/{sha256}` scheme used by `dispatch()` in
  Plan 02 — `snapshot_ref` becomes an `s3://...` URL exclusively.

### 7. `SnapshotSink` / `SnapshotSource` traits live in `sandbawks-types`

Both `sandbawks-node` (writes) and the controller (records URLs, may read
manifests during reconcile validation) need the type names; the trait + S3
impl + a fake-in-memory impl all live in `sandbawks-types` so the controller
crate keeps its no-S3-dependency test footprint.

- **Why:** Without this, either the controller pulls in `rust-s3` (heavy) or
  we duplicate the trait. Keeping abstractions in `-types` matches the existing
  `RuntimeAdapter` pattern.

## File map

```
crates/
  sandbawks-types/
    Cargo.toml                              # adds nothing new (sha2 already present)
    src/
      lib.rs                                # +pub mod snapshot_job; +pub mod snapshot;
      snapshot.rs                           # NEW — SnapshotSink/Source traits, SnapshotManifest v2, SnapshotPart
      snapshot_job.rs                       # NEW — SnapshotJob CRD (CustomResource derive)
      sandbox.rs                            # +SandboxStatus::snapshot_job_ref field
      runtime.rs                            # extended RuntimeError variants; pause/resume gain SnapshotSink/Source params
      testing.rs                            # NEW — FakeSnapshotStore
      bin/crdgen.rs                         # +write_crd(snapshotjobs.sandbawks.io.yaml, &SnapshotJob::crd())
    tests/
      crdgen.rs                             # EXPECTED_FILES: 4 → 5

  sandbawks-controller/
    Cargo.toml                              # +sandbawks-types/testing feature on dev-deps
    src/
      reconcile.rs                          # dispatch() refactor: SnapshotJob CR creation instead of inline adapter calls
      pod_adapter.rs                        # PodSpec.runtime_class_name = sandbox.spec.runtime_class_name; drop warning branch
      lib.rs                                # SnapshotJob watcher wired into Controller::new
    tests/
      reconcile.rs                          # 4 tests refactored to mock SnapshotJob status
      lifecycle.rs                          # full lifecycle now via SnapshotJob
      pod_adapter.rs                        # +RuntimeClass=gvisor and =None cases

  sandbawks-node/                           # NEW CRATE
    Cargo.toml
    Dockerfile                              # distroless multi-stage; bakes in runsc
    src/
      lib.rs
      gvisor.rs                             # GvisorAdapter impl
      s3.rs                                 # S3SnapshotSink / S3SnapshotSource (rust-s3-backed)
      reconcile.rs                          # SnapshotJob reconciler (field-selector on spec.nodeName)
      bin/sandbawks-node.rs                 # binary entry; reads NODE_NAME from env

ci/
  kind-node/                                # NEW
    Dockerfile                              # kindest/node:v1.31.0 + runsc + containerd runsc handler config
    containerd-runsc.toml                   # /etc/containerd/conf.d/runsc.toml

deploy/
  helm/sandbawks/templates/
    runtimeclass-gvisor.yaml                # NEW — gated by .Values.gvisor.enabled
    snapshot-store-secret.yaml              # NEW — mirrors api-signing-secret pattern
    node-daemonset.yaml                     # --placeholder → real binary; NODE_NAME env; envFrom S3 Secret
    rbac.yaml                               # +snapshotjobs CRUD + /status update on sandbawks-node SA
  helm/sandbawks/values.yaml                # +gvisor.enabled=true; +snapshotStore.existingSecret default unset
  kind/
    cluster.yaml                            # 1 control-plane + 1 worker, both ghcr.io/hypotheosis/sandbawks-kind-node:latest
    fixtures/
      install-minio.sh                      # NEW — installs MinIO via Bitnami chart, seeds bucket + Secret
      pause-resume-smoke.sh                 # NEW — end-to-end integration test

.forgejo/workflows/
  kind-node-image.yaml                      # NEW — mirrors ci-runner-image.yaml; publishes sandbawks-kind-node
  kind-smoke.yaml                           # +sandbawks-node in build-images matrix; KIND_SMOKE_INCLUDE_PAUSE_RESUME=1

changelog/
  2026-05-24-plan-05-sandbawks-node.md      # NEW

justfile                                    # +node-image-build, +node-image-run, +kind-node-image-build, +pause-resume-smoke
```

New workspace deps (added in `Cargo.toml` `[workspace.dependencies]`):

```toml
# S3-compatible object storage (Plan 05)
rust-s3 = { version = "0.35", default-features = false, features = ["tokio-rustls-tls"] }

# Compression + archive (Plan 05)
zstd = "0.13"
tar = "0.4"

# containerd client (Plan 05)
containerd-client = "0.8"
```

`sha2`, `bytes`, `tokio`, `futures`, `tracing`, `thiserror`, `anyhow`,
`async-trait`, `chrono`, `serde`, `serde_json` are all already in
`[workspace.dependencies]` (Plans 01–04).

---

## Task 1: `crates/sandbawks-node` scaffold

**Files touched**

- `Cargo.toml` (workspace): add `crates/sandbawks-node` to `members`; add
  `rust-s3`, `zstd`, `tar`, `containerd-client` to `[workspace.dependencies]`.
- `crates/sandbawks-node/Cargo.toml`: new crate manifest.
- `crates/sandbawks-node/src/lib.rs`: module map (`pub mod gvisor; pub mod s3;
  pub mod reconcile;`); empty stubs.
- `crates/sandbawks-node/src/bin/sandbawks-node.rs`: `clap`-parsed binary stub
  that accepts `--placeholder` (so the existing Helm chart can run it
  unchanged until Task 12) and `--help`.

**Smoke**

- `cargo build -p sandbawks-node` succeeds.
- `cargo run -p sandbawks-node -- --help` prints usage.
- Workspace gates (`fmt-check`, `clippy -- -D warnings`, `test`) stay green.

**Commit**: `feat(node): workspace + crates/sandbawks-node scaffold (Plan 05 T1)`

---

## Task 2: `SnapshotJob` CRD + `SandboxStatus::snapshot_job_ref`

**Files touched**

- `crates/sandbawks-types/src/snapshot_job.rs` (new) — `SnapshotJobSpec`,
  `SnapshotJobStatus`, `SnapshotJobMode { Pause, Resume }`,
  `SnapshotJobPhase { Pending, Checkpointing, Uploading, Restoring, Complete, Failed }`.
  Derive `CustomResource`; group `sandbawks.io`, version `v1alpha1`, plural
  `snapshotjobs`, shortname `sbxsj`, namespaced. Printcolumns: `State`
  (`.status.phase`), `Node` (`.spec.nodeName`), `Sandbox` (`.spec.sandboxName`),
  `Age`.
- `crates/sandbawks-types/src/lib.rs`: `pub mod snapshot_job;` +
  `pub use snapshot_job::*;`.
- `crates/sandbawks-types/src/sandbox.rs`: add
  `pub snapshot_job_ref: Option<String>` to `SandboxStatus` (camelCase
  `snapshotJobRef`).
- `crates/sandbawks-types/src/bin/crdgen.rs`: add
  `write_crd(out_dir, "snapshotjobs.sandbawks.io.yaml", &SnapshotJob::crd())?`.
- `crates/sandbawks-types/tests/crdgen.rs`: bump `EXPECTED_FILES` array from
  4 → 5; add `"snapshotjobs.sandbawks.io.yaml"`.
- `deploy/crds/snapshotjobs.sandbawks.io.yaml`: regenerated artifact.

**Verification**

- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` produces 5 files,
  idempotent on second run.
- `cargo test -p sandbawks-types` green.

**Commit**: `feat(types): SnapshotJob CRD + Sandbox.status.snapshotJobRef (Plan 05 T2)`

---

## Task 3: `SnapshotSink`/`SnapshotSource` traits + `SnapshotManifest` v2

**Files touched**

- `crates/sandbawks-types/src/snapshot.rs` (new) — `SnapshotPart { name,
  sha256, size_bytes }`, `SnapshotManifest` v2 (with `parts: Vec<SnapshotPart>`
  and aggregate `sha256` over sorted part hashes), `SnapshotSink` /
  `SnapshotSource` traits.
- `crates/sandbawks-types/src/runtime.rs`: extend `SnapshotManifest` to the new
  shape (keep `sandbox_id`, `runtime_class`, `created_at`; add `parts`,
  `snapshot_ref` URL field). Deserializer accepts the v1 single-sha256 shape
  and treats `parts` as empty (`#[serde(default)]`).
- `crates/sandbawks-types/src/runtime.rs`: extend `RuntimeError` with
  `SnapshotUpload(String)`, `SnapshotDownload(String)`,
  `CheckpointFailed(String)`, `RestoreFailed(String)`. Extend
  `RuntimeAdapter::pause` signature to `(pod_uid: &str, sink: &dyn
  SnapshotSink)`. Extend `RuntimeAdapter::resume` signature to
  `(manifest: &SnapshotManifest, pod_uid: &str, source: &dyn SnapshotSource)`.
  Default impls become no-ops that ignore the new params (so `FakeRuntimeAdapter`
  continues to compile unchanged in Task 4 before its imminent deletion).
- `crates/sandbawks-types/src/testing.rs` (new) — `FakeSnapshotStore`: in-memory
  `Arc<Mutex<HashMap<String, Vec<u8>>>>`; implements both `SnapshotSink` and
  `SnapshotSource`. Used by the controller's unit tests.
- `crates/sandbawks-types/Cargo.toml`: add a `testing` feature gating
  `pub mod testing;`.

**Verification**

- `cargo test -p sandbawks-types --all-features` green.
- Hegel property test: round-trip an arbitrary `Vec<SnapshotPart>` through
  `FakeSnapshotStore` and assert byte-identity + sha256 match.

**Commit**: `feat(types): SnapshotSink/Source traits + SnapshotManifest v2 (Plan 05 T3)`

---

## Task 4: Controller `dispatch()` refactor — `SnapshotJob` creation

**Files touched**

- `crates/sandbawks-controller/src/reconcile.rs`:
  - Replace lines 359–388 (the `Action::Pause` and `Action::Resume` arms in
    `dispatch()`) with logic that:
    1. Creates a `SnapshotJob` CR named
       `<sandbox-name>-<mode>-<epoch-millis>` with `spec.nodeName =
       sandbox.status.assigned_node`, `spec.sandboxName = name`,
       `spec.mode = Pause|Resume`, `spec.containerId =
       sandbox.status.pod_uid` (or look up by Pod ref), and for resume
       `spec.snapshotRef = sandbox.status.snapshot_ref`.
    2. Patches `Sandbox.status.snapshot_job_ref` with the new CR name.
    3. **Does not** advance phase — `Pausing` stays `Pausing`, `Resuming` stays
       `Resuming`.
  - Add a separate code path that runs *before* `dispatch()` — when
    `status.snapshot_job_ref.is_some()`, look up the `SnapshotJob`:
    - `status.phase=Complete` → fire `Trigger::SnapshotUploaded` (if mode=Pause)
      or `Trigger::ResumeReady` (if mode=Resume); copy
      `SnapshotJob.status.manifest.snapshot_ref` into
      `Sandbox.status.snapshot_ref`; clear `status.snapshot_job_ref`.
    - `status.phase=Failed` → fire `Trigger::Failure(message)`; clear
      `status.snapshot_job_ref`.
    - Otherwise → leave alone (requeue with backoff).
- `crates/sandbawks-controller/src/lib.rs`: add a `SnapshotJob` watcher to the
  `Controller::new(...)` setup so updates to `SnapshotJob.status` requeue the
  parent Sandbox.
- `crates/sandbawks-controller/src/fake_adapter.rs`: delete `pause` and
  `resume` (no longer called by `dispatch()`); keep `create_pod`/`delete_pod`/
  `pod_ready` and `runtime_class`. Delete `FakeAdapterCall::Pause` and
  `::Resume` variants. `FakeRuntimeAdapter` becomes a Pod-lifecycle fake only.
- `crates/sandbawks-controller/tests/reconcile.rs`: rework these 4 tests to
  mock `SnapshotJob` status instead of adapter returns:
  - `running_with_desired_paused_pauses_via_adapter` → renamed
    `running_with_desired_paused_creates_snapshot_job`; assert a `SnapshotJob`
    is created with `mode=Pause` and `Sandbox.status.snapshot_job_ref` is set.
  - `pause_adapter_failure_transitions_to_failed` → renamed
    `pause_snapshot_job_failure_transitions_to_failed`; pre-create a
    `SnapshotJob` with `status.phase=Failed`; assert the Sandbox transitions
    to `Failed`.
  - `paused_with_desired_running_resumes` → renamed
    `paused_with_desired_running_creates_resume_snapshot_job`; symmetric.
  - `reconcile_is_idempotent` → verify no duplicate `SnapshotJob` is created
    on a second reconcile pass.
- `crates/sandbawks-controller/tests/lifecycle.rs`: full Creating → Running →
  Pausing → Paused → Resuming → Running flow now driven by `SnapshotJob`
  status transitions (mocked via `KubeSandboxStore`'s injectable child-store).

**Verification**

- `cargo test -p sandbawks-controller --all-features` green.
- State-machine unit tests in `tests/state_machine_unit.rs` and
  `tests/state_machine_properties.rs` are **unchanged** (the state machine
  itself is unaffected).

**Commit**: `feat(controller): dispatch via SnapshotJob CRD (Plan 05 T4)`

---

## Task 5: `PodAdapter` RuntimeClass dispatch

**Files touched**

- `crates/sandbawks-controller/src/pod_adapter.rs`:
  - Line 146: change `..Default::default()` for the `PodSpec` to:
    ```rust
    PodSpec {
        runtime_class_name: sandbox.spec.runtime_class_name.clone(),
        restart_policy: Some("Never".into()),
        containers: vec![...],
        ..Default::default()
    }
    ```
  - Lines 164–169: delete the `tracing::warn!` block that previously logged
    "PodAdapter ignores spec.runtimeClassName"; the field is now honored.
- `crates/sandbawks-controller/tests/pod_adapter.rs`: add two cases:
  - `pod_runtime_class_set_when_sandbox_specifies_gvisor` — Sandbox with
    `spec.runtime_class_name = Some("gvisor")`, assert
    `Pod.spec.runtime_class_name == Some("gvisor")`.
  - `pod_runtime_class_absent_when_sandbox_unset` — Sandbox with
    `runtime_class_name = None`, assert `Pod.spec.runtime_class_name.is_none()`.

**Verification**

- `cargo test -p sandbawks-controller --all-features` green.
- `helm template test deploy/helm/sandbawks --set gvisor.enabled=true` still
  renders the chart (no template changes here yet — that's Task 12).

**Commit**: `feat(controller): PodAdapter honors spec.runtimeClassName (Plan 05 T5)`

---

## Task 6: `S3SnapshotSink` + `S3SnapshotSource` impls

**Files touched**

- `crates/sandbawks-node/Cargo.toml`: add `rust-s3`, `bytes`, `futures`,
  `tokio`, `tracing`, `thiserror`, `async-trait`, `sha2`, `sandbawks-types`,
  `hegel` (dev-dep).
- `crates/sandbawks-node/src/s3.rs` (new) — `S3SnapshotSink` and
  `S3SnapshotSource` structs each wrapping `s3::Bucket`:
  - `put_part`: streams the input `Stream<Item = Bytes>` in 4 MiB chunks,
    computes a per-chunk sha256, uploads chunks in parallel via
    `futures::stream::buffer_unordered(8)`. Before each chunk PUT, HEAD the
    object to check whether a chunk with the same sha256 already exists
    (idempotent retry).
  - `put_manifest`: small JSON PUT to `<key-prefix>/manifest.json`.
  - `get_part`, `get_manifest`: symmetric reads with sha256 verification as
    each chunk lands.
- Hegel property test in `crates/sandbawks-node/tests/s3.rs` with an
  `InMemoryS3` test double (just a `HashMap<String, Vec<u8>>`) that exercises
  the round-trip on arbitrary byte vectors of size up to 32 MiB.

**Verification**

- `cargo test -p sandbawks-node --all-features` green.

**Commit**: `feat(node): S3SnapshotSink/Source — chunked, parallel, idempotent (Plan 05 T6)`

---

## Task 7: `GvisorAdapter::pause` — `runsc checkpoint` + stream to S3

**Files touched**

- `crates/sandbawks-node/Cargo.toml`: add `containerd-client`, `zstd`, `tar`,
  `tokio` (process), `reqwest` (for envd `/__state` probe).
- `crates/sandbawks-node/src/gvisor.rs` (new) — `GvisorAdapter` struct.
  `pause` sequence:
  1. Connect to containerd via the local Unix socket
     `/run/containerd/containerd.sock` (mounted into the DaemonSet's container
     in Plan 04 already).
  2. Look up the container by ID derived from
     `pod.status.containerStatuses[0].containerID` (strip the `containerd://`
     prefix; pod info is fetched via kube API).
  3. Allocate a fresh work-dir `/var/lib/sandbawks/snapshots/<sandbox-id>/<ts>/`.
  4. `tokio::process::Command::new("runsc").args(["checkpoint",
     "--image-path", &work_dir, container_id]).status().await` — return
     `RuntimeError::CheckpointFailed` on non-zero exit, including stderr.
  5. Tar + zstd `<work-dir>/memory` (and any sibling files runsc emitted)
     into a streaming `Stream<Item = Bytes>` → `SnapshotSink::put_part("memory.zst")`.
  6. Compute rootfs delta via the containerd snapshotter Diff API → tar + zstd
     → `SnapshotSink::put_part("rootfs-delta.tar.zst")`.
  7. HTTP GET envd's `/__state` endpoint at `pod.status.podIP:49983` →
     `SnapshotSink::put_part("envd-state.json")`.
  8. Build `SnapshotManifest` with all three parts + aggregate sha256 →
     `SnapshotSink::put_manifest(...)`.
  9. Return the manifest.

**Verification**

- A unit test using `FakeSnapshotStore` plus a stubbed `runsc` binary
  (`tempfile::tempdir` + a small shell script on `PATH`) that writes a known
  memory file and exits 0. Assert all three parts and the manifest land in the
  store with correct sha256.
- Integration validation is in Task 14.

**Commit**: `feat(node): GvisorAdapter::pause via runsc checkpoint + S3 streaming (Plan 05 T7)`

---

## Task 8: `GvisorAdapter::resume` — download + verify + `runsc restore`

**Files touched**

- `crates/sandbawks-node/src/gvisor.rs` (extend) — `resume` sequence:
  1. Fetch manifest via `SnapshotSource::get_manifest`.
  2. Stream each part to a temporary work-dir, verifying per-part sha256 as
     bytes land; bail with `RuntimeError::SnapshotDownload` on mismatch.
  3. Apply `rootfs-delta.tar.zst` to a fresh containerd snapshot.
  4. Write `envd-state.json` to the work-dir.
  5. `tokio::process::Command::new("runsc").args(["restore", "--image-path",
     &work_dir, container_id]).status().await` — return
     `RuntimeError::RestoreFailed` on non-zero exit.
  6. Wait for the restored process to bind its envd port (poll
     `pod.status.podIP:49983` with a 30s budget, 500 ms intervals).
  7. Return `Ok(())`.

**Verification**

- Unit test mirrors Task 7's test: pre-populate `FakeSnapshotStore` with a
  manifest + three parts, stub `runsc restore` to exit 0, assert work-dir
  contents.
- Integration validation is in Task 14.

**Commit**: `feat(node): GvisorAdapter::resume via runsc restore (Plan 05 T8)`

---

## Task 9: `sandbawks-node` reconciler binary

**Files touched**

- `crates/sandbawks-node/src/reconcile.rs` (new) — `kube::runtime::Controller`
  over `SnapshotJob` with a field-selector
  (`Config::default().fields(&format!("spec.nodeName={}", node_name))`) so each
  DaemonSet replica only sees jobs for its own node. Reconcile function
  dispatches to `GvisorAdapter::pause` or `::resume` based on `spec.mode`,
  then patches `SnapshotJob.status.phase` to `Complete` or `Failed`.
- `crates/sandbawks-node/src/bin/sandbawks-node.rs`:
  - Read `NODE_NAME` from env (set via downward API in Task 12).
  - Read `SANDBAWKS_S3_ENDPOINT`, `SANDBAWKS_S3_BUCKET`,
    `SANDBAWKS_S3_REGION`, `SANDBAWKS_S3_ACCESS_KEY`,
    `SANDBAWKS_S3_SECRET_KEY` from env (Helm wires them in Task 12).
  - Construct `S3SnapshotSink` + `S3SnapshotSource` + `GvisorAdapter`.
  - Spawn `reconcile::run(...)`.
- `crates/sandbawks-node/tests/reconcile.rs` — exercise the reconcile function
  with a `FakeSnapshotStore` and a fake `RuntimeAdapter` that records
  pause/resume calls. Assert idempotency on retry.

**Verification**

- `cargo test -p sandbawks-node --all-features` green.
- `cargo run -p sandbawks-node --release` against `KUBECONFIG=$KIND_CONFIG`
  starts up, watches `SnapshotJob`s, logs `node_name = sandbawks-worker`.

**Commit**: `feat(node): SnapshotJob reconciler + binary entry (Plan 05 T9)`

---

## Task 10: `sandbawks-node` Dockerfile + Forgejo Actions image build

**Files touched**

- `crates/sandbawks-node/Dockerfile` (new) — distroless multi-stage:
  - Builder: `ghcr.io/hypotheosis/sandbawks-ci-runner:latest` (already has Rust
    toolchain + sccache + cargo-chef). Build with `--release --bin sandbawks-node`.
  - `runsc` builder: separate stage that downloads the gVisor release tarball
    and extracts `runsc` for the target architecture.
  - Runtime: `gcr.io/distroless/cc-debian12:nonroot`. COPY `sandbawks-node` +
    `runsc` from the respective builders. `ENTRYPOINT ["/usr/local/bin/sandbawks-node"]`.
- `.forgejo/workflows/kind-smoke.yaml`: extend the `build-images` matrix with:
  ```yaml
  - image: node
    repo: ghcr.io/hypotheosis/sandbawks-node
    dockerfile: crates/sandbawks-node/Dockerfile
  ```
- `justfile`: add recipes:
  ```just
  node-image-build:
      docker build -t sandbawks-node:dev -f crates/sandbawks-node/Dockerfile .

  node-image-run:
      docker run --rm sandbawks-node:dev --help
  ```

**Verification**

- `just node-image-build` succeeds locally.
- `just node-image-run` prints help.
- On `main`, the `kind-smoke` workflow's `build-images` matrix publishes
  `ghcr.io/hypotheosis/sandbawks-node:{sha-<sha>,latest}`.

**Commit**: `feat(node): Dockerfile + matrix entry in kind-smoke workflow (Plan 05 T10)`

---

## Task 11: Custom `sandbawks-kind-node` image (gVisor pre-baked)

**Files touched**

- `ci/kind-node/Dockerfile` (new):
  ```dockerfile
  FROM kindest/node:v1.31.0

  ARG GVISOR_RELEASE=20240603.0
  RUN set -eux; \
      arch="$(dpkg --print-architecture)"; \
      curl -fsSL -o /usr/local/bin/runsc \
        "https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE}/${arch}/runsc"; \
      chmod 0755 /usr/local/bin/runsc

  COPY ci/kind-node/containerd-runsc.toml /etc/containerd/conf.d/runsc.toml
  ```
- `ci/kind-node/containerd-runsc.toml`:
  ```toml
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
    runtime_type = "io.containerd.runsc.v1"
  ```
- `.forgejo/workflows/kind-node-image.yaml` (new) — mirrors
  `ci-runner-image.yaml`. Triggers: `workflow_dispatch` + `push` on `main`
  with path filter `ci/kind-node/**`. Publishes
  `ghcr.io/hypotheosis/sandbawks-kind-node:{sha-<sha>,latest,YYYY.MM}`.
- `justfile`:
  ```just
  kind-node-image-build:
      docker build -t sandbawks-kind-node:dev -f ci/kind-node/Dockerfile .
  ```

**Verification**

- `just kind-node-image-build` succeeds.
- `kind create cluster --image sandbawks-kind-node:dev` succeeds; `kubectl
  exec -n kube-system <containerd-pod> -- crictl info` shows `runsc` runtime
  registered.

**Commit**: `feat(ci): sandbawks-kind-node image with runsc pre-baked (Plan 05 T11)`

---

## Task 12: Helm chart — RuntimeClass, S3 secret, real node binary, RBAC

**Files touched**

- `deploy/helm/sandbawks/templates/runtimeclass-gvisor.yaml` (new):
  ```yaml
  {{- if .Values.gvisor.enabled }}
  apiVersion: node.k8s.io/v1
  kind: RuntimeClass
  metadata:
    name: gvisor
    labels: {{- include "sandbawks.labels" . | nindent 4 }}
  handler: runsc
  {{- end }}
  ```
- `deploy/helm/sandbawks/templates/snapshot-store-secret.yaml` (new) — mirrors
  the api signing-secret pattern: only render when
  `snapshotStore.existingSecret` is unset; auto-generate a 32-char access key
  + 64-char secret key via `randAlphaNum`. Secret is named
  `{{ include "sandbawks.fullname" . }}-snapshot-store`.
- `deploy/helm/sandbawks/templates/node-daemonset.yaml`:
  - Replace `args: ["--placeholder"]` with `args: []`.
  - Add `env`:
    ```yaml
    - name: NODE_NAME
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
    - name: SANDBAWKS_S3_ENDPOINT
      value: {{ .Values.snapshotStore.endpoint | quote }}
    - name: SANDBAWKS_S3_BUCKET
      value: {{ .Values.snapshotStore.bucket | quote }}
    - name: SANDBAWKS_S3_REGION
      value: {{ .Values.snapshotStore.region | quote }}
    ```
  - Add `envFrom` pulling `SANDBAWKS_S3_ACCESS_KEY` + `SANDBAWKS_S3_SECRET_KEY`
    from the snapshot-store Secret.
- `deploy/helm/sandbawks/templates/rbac.yaml`: extend the `sandbawks-node`
  ClusterRole with:
  - `apiGroups: ["sandbawks.io"]`, `resources: ["snapshotjobs"]`,
    verbs: `["get", "list", "watch", "create", "update", "patch", "delete"]`.
  - `apiGroups: ["sandbawks.io"]`, `resources: ["snapshotjobs/status"]`,
    verbs: `["get", "update", "patch"]`.
- `deploy/helm/sandbawks/values.yaml`: add:
  ```yaml
  gvisor:
    enabled: true
  ```
  and ensure `snapshotStore.existingSecret: ""` (unset → auto-generate).

**Verification**

- `helm lint deploy/helm/sandbawks` clean.
- `helm template test deploy/helm/sandbawks --set gvisor.enabled=true` renders
  the RuntimeClass, the snapshot-store Secret, the node DaemonSet with real
  args (no `--placeholder`) + `NODE_NAME` env + `envFrom` snapshot-store, and
  the extended `sandbawks-node` ClusterRole with `snapshotjobs` permissions.
- `helm template test deploy/helm/sandbawks --set
  snapshotStore.existingSecret=my-creds` does **not** render the Secret;
  DaemonSet's `envFrom` references `my-creds`.

**Commit**: `chore(deploy): Helm wiring — RuntimeClass, S3 secret, real node binary, RBAC (Plan 05 T12)`

---

## Task 13: Kind cluster config + MinIO fixture + `cluster-up` integration

**Files touched**

- `deploy/kind/cluster.yaml`:
  ```yaml
  kind: Cluster
  apiVersion: kind.x-k8s.io/v1alpha4
  name: sandbawks-smoke
  nodes:
    - role: control-plane
      image: ghcr.io/hypotheosis/sandbawks-kind-node:latest
    - role: worker
      image: ghcr.io/hypotheosis/sandbawks-kind-node:latest
  ```
- `deploy/kind/fixtures/install-minio.sh` (new) — installs MinIO via the
  Bitnami chart, waits for ready, uses `mc` to create the
  `sandbawks-snapshots` bucket, writes a `sandbawks-snapshot-store` Secret in
  the `default` namespace with the MinIO root creds + endpoint.
- `justfile`:
  - `cluster-up` learns `SANDBAWKS_KIND_GVISOR=1` (default in CI; opt-in
    locally) which (a) builds/loads the kind-node image if not present,
    (b) installs MinIO via the fixture, (c) sets `--set gvisor.enabled=true
    --set snapshotStore.existingSecret=sandbawks-snapshot-store
    --set snapshotStore.endpoint=http://minio.default.svc.cluster.local:9000
    --set snapshotStore.bucket=sandbawks-snapshots
    --set node.enabled=true` on the helm install.
  - New recipe `pause-resume-smoke`:
    ```just
    pause-resume-smoke:
        bash deploy/kind/fixtures/pause-resume-smoke.sh
    ```

**Verification**

- `SANDBAWKS_KIND_GVISOR=1 just cluster-up` completes; `kubectl get
  runtimeclass gvisor` exists; `kubectl get pods -n default minio-*` is Ready;
  `kubectl get secret sandbawks-snapshot-store` exists.

**Commit**: `chore(deploy): kind cluster + MinIO fixture + cluster-up gvisor mode (Plan 05 T13)`

---

## Task 14: `pause-resume-smoke.sh` — end-to-end integration test

**Files touched**

- `deploy/kind/fixtures/pause-resume-smoke.sh` (new). Mirrors `api-smoke.sh`'s
  idiom (kubectl wait, curl + jq, retry loops with explicit attempt ceiling,
  cleanup trap). Sequence:

  1. Apply `SandboxTemplate` with `defaultRuntimeClass: gvisor` and
     `TeamAPIKey` fixtures.
  2. `POST /sandboxes` → poll `Sandbox.status.envdAddress` (60s budget).
  3. `POST /files/upload` writing `/workspace/sentinel.txt` with content
     `phase-5-sentinel-${RANDOM}`. Stash the content.
  4. `POST /sandboxes/<id>/pause` → poll `Sandbox.status.phase=Paused` (60s).
     Assert `Sandbox.status.snapshotRef` starts with `s3://`. Run `mc ls
     minio/sandbawks-snapshots/<sandbox-id>/` and assert `manifest.json`,
     `memory.zst`, `rootfs-delta.tar.zst`, `envd-state.json` are present.
  5. `kubectl delete pod` for the (now-paused) sandbox's Pod. Forces resume
     to land on a fresh Pod — validates the "any node" property of S3-backed
     snapshots.
  6. `POST /sandboxes/<id>/connect` → poll `Sandbox.status.phase=Running` (60s);
     stash new `envdAddress`.
  7. `GET /files/download?path=/workspace/sentinel.txt` → assert content
     matches stash from step 3 byte-for-byte.
  8. `POST /sandboxes/<id>/kill` → poll until Sandbox + Pod + S3 objects are
     GC'd (30s). (S3 GC of orphans is Plan 06; this step only asserts the
     in-cluster cleanup.)

- `.forgejo/workflows/kind-smoke.yaml`: add a step in the smoke job:
  ```yaml
  - name: Pause/resume smoke
    if: env.KIND_SMOKE_INCLUDE_PAUSE_RESUME == '1'
    run: bash deploy/kind/fixtures/pause-resume-smoke.sh
  ```
  Set `KIND_SMOKE_INCLUDE_PAUSE_RESUME: '1'` at the job level.

**Verification**

- Locally: `SANDBAWKS_KIND_GVISOR=1 just cluster-up && just pause-resume-smoke
  && just cluster-down` exits 0.
- CI: kind-smoke workflow on `main` runs both `api-smoke.sh` and
  `pause-resume-smoke.sh` to green.

**Commit**: `test(node): pause/resume smoke — full S3 round-trip on gVisor (Plan 05 T14)`

---

## Task 15: Workspace green gate + changelog

**Files touched**

- `changelog/2026-05-24-plan-05-sandbawks-node.md` (new) — matches Plan 04
  changelog shape: Summary, What landed (with subsection per task),
  What's deliberately not here (snapshot GC, lazy restore, node-local cache,
  Kata/CRIU adapters), Next plan preview (Plan 06 SDK compat + benchmarks +
  retention/GC).

**Verification gates**

All of these must pass before T15 closes:

- `cargo test --workspace --all-features` green.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  silent.
- `cargo fmt --all -- --check` silent.
- `cargo doc --workspace --no-deps` warning-free.
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` produces exactly
  5 files; running it twice produces no diff.
- `helm lint deploy/helm/sandbawks` clean.
- `helm template test deploy/helm/sandbawks --set gvisor.enabled=true` renders
  the RuntimeClass, the `sandbawks-node` DaemonSet without `--placeholder`,
  the S3 credentials secret reference, and the `snapshotjobs.sandbawks.io`
  CRD.
- The Forgejo Actions `kind-node-image.yaml` workflow runs green on `main`
  and publishes `ghcr.io/hypotheosis/sandbawks-kind-node:{sha,latest}` to Harbor.
- The `kind-smoke` workflow on `main` runs green with the pause/resume smoke
  included; full lifecycle goes through a real gVisor sandbox, real
  `runsc checkpoint`, real S3 upload to MinIO, real restore.

**Commit**: `docs: Plan 05 green-gate sweep + changelog (Plan 05 T15)`

---

# 2026-05-24 — Plan 05: `GvisorAdapter` + `sandbawks-node` + S3 streaming

## What landed

(To be filled in during Task 15 when the plan closes; placeholder shape:)

- `crates/sandbawks-node` — new crate hosting `GvisorAdapter` + S3 streaming
  + `SnapshotJob` reconciler.
- `SnapshotJob` CRD — Kubernetes-native dispatch from controller to per-node
  workers.
- `PodAdapter` now honors `Sandbox.spec.runtimeClassName`; cluster ships
  gVisor by default.
- Custom `sandbawks-kind-node` image with `runsc` pre-baked.
- MinIO kind fixture + S3-streamed snapshots (parallel chunks, sha256,
  idempotent retries).
- End-to-end `pause-resume-smoke.sh` that writes a sentinel, pauses, deletes
  the Pod, resumes on a fresh Pod, and reads the sentinel back.

## What's deliberately not here

- **Snapshot GC** — orphaned S3 objects after sandbox termination linger
  until Plan 06 (which has snapshot retention as a tracked open question
  alongside SDK compat).
- **Lazy/page-fault restore** — supported by gVisor 2024+ but skipped here
  to keep the integration test deterministic. Plan 07's warm pool reopens
  this.
- **Node-local snapshot cache** — Plan 07's warm pool needs it; Plan 05
  always pulls from S3.
- **`KataFcAdapter`, `RuncCriuAdapter`** — Plans 08 and 09 (or 09 deferred).

## Next plan

**Plan 06 — E2B SDK compat tests + benchmarks**:
- `pip install e2b` against the gateway; full SDK method matrix in CI.
- Perf gates for pause p95 < 8 s and resume p95 < 2 s on a 1 GiB sandbox
  (spec §13).
- Snapshot retention policy (open question §15) lands alongside SDK work.

## Definition of done

All Task 15 gates above are green.
