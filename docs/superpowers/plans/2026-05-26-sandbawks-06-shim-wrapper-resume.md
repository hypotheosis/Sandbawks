# sandbawks Plan 06 — `containerd-shim-sandbawks-v1`: gVisor shim wrapper for kubelet-mediated resume

> **Scope.** Sixth of the sandbawks sub-plans. After Plan 05 + issues #170,
> #171, #173, #174, the controller scaffolding for fresh-Pod resume works
> end-to-end (Pod recreate, `SANDBAWKS_RESTORE_MODE=1` envd shim, sha256
> binding, `/__state` live wiring). The *runtime call itself* still fails
> because `runsc restore` rejects kubelet-managed containers — they are in
> state `running` (or `paused`) and never in the `stopped`/`created` state
> `runsc restore` requires. Plan 06 closes that gap by shipping a custom
> containerd shim that drives gVisor's extension `CreateWithFSRestore` +
> `Restore` RPCs at container-create time, so the container is born in the
> restored state and the standard kubelet → containerd → shim flow never has
> to be inverted.
>
> See issue #176 for the
> originating bug report.

## Predecessor plans + issues

- [Plan 05 — gVisor adapter, S3 streaming, end-to-end pause/resume on kind](2026-05-24-sandbawks-05-gvisor-node.md)
- Issue #170 — envd `/__state` HTTP probe
- Issue #171 — fresh-Pod resume orchestration
- Issue #173 — real `/__state` HTTP probe; revert T15 stub
- Issue #174 — live state wiring in envd `/__state`

## Numbering note

Plan 05's "Next plan" section preview named SDK compat + benchmarks +
retention as Plan 06. That work is renumbered to Plan 07+. Plan 06 is now
the runtime gap surfaced by #176, because shipping pause/resume that *works*
gates everything else (SDK compat tests need a working resume to be
meaningful). Plans 07 (lazy restore), 08 (Kata-fc), 09 (runc-criu) are
unchanged.

## Goal

Replace the failing direct `runsc restore` shell-out in
`GvisorAdapter::resume` with a kubelet-mediated path that uses gVisor's
existing — but un-exposed-to-containerd — `CreateWithFSRestore` and
`Restore` task-service extension RPCs. After Plan 06, a sandbawks operator
who runs `helm install sandbawks` on a kind cluster with the
`sandbawks-kind-node` image gets:

- `POST /sandboxes/<id>/pause` — unchanged from Plan 05 (`runsc checkpoint`
  shell-out, S3 upload).
- `POST /sandboxes/<id>/connect` on a paused sandbox — controller stages
  the snapshot bundle to a local path on a node (via a `Stage` SnapshotJob),
  then creates a fresh Pod that carries `runtimeClassName:
  sandbawks-runsc` + a `io.sandbawks/restore-image-path=<staged-path>`
  annotation. kubelet calls the standard CRI Create+Start; the
  `containerd-shim-sandbawks-v1` wrapper translates those into
  `CreateWithFSRestore` + `Restore`; the container is born already
  restored. No `runsc restore` shell-out runs on the path of the
  kubelet-managed container.
- The `pause-resume-smoke.sh` smoke flips `PAUSE_RESUME_SMOKE_FULL=1` back
  to the default (issue #176's deferred final acceptance).

## Architectural decisions locked in this plan

These were validated by reading the upstream `google/gvisor` source for
[`pkg/shim/v1/extension/extension.go`](https://github.com/google/gvisor/blob/master/pkg/shim/v1/extension/extension.go)
and [`pkg/shim/v1/runsc/service.go`](https://github.com/google/gvisor/blob/master/pkg/shim/v1/runsc/service.go).
**Implementers should not relitigate them.**

### 1. Custom containerd shim binary wraps gVisor's `pkg/shim/v1`

We ship `containerd-shim-sandbawks-v1`, a Go binary that links
`github.com/google/gvisor/pkg/shim/v1/...` as a library and wraps the
underlying runsc TaskService with a thin decorator. The decorator inspects
the OCI spec's annotations at `Create` and `Start` time and, if the
sandbawks restore annotation is present, dispatches to gVisor's
`CreateWithFSRestore` and `Restore` extension RPCs instead of the standard
`Create`/`Start`.

- **Why:** The two extension RPCs *already exist* in upstream gVisor and
  do exactly what we need — `CreateWithFSRestore` creates a container whose
  filesystem comes from a restore image path, and `Restore` loads the
  process state into the just-created container. Containerd itself does not
  call them (CRI has no Restore concept), so a small adapter is the
  minimum glue.
- **Rejected — NRI / CRI plugin:** Requires enabling NRI in
  `/etc/containerd/config.toml` + a containerd restart on every node. The
  restart touches every workload on the host, not just sandbawks, which is
  a meaningful operator burden.
- **Rejected — fork gVisor's `cmd/containerd-shim-runsc-v1`:** Couples
  sandbawks to upstream gVisor's source tree and requires merge work on
  every gVisor release. Linking the `pkg/shim/v1` packages as a Go module
  dependency is cheaper and survives upstream refactors as long as the
  exported interface holds.
- **Rejected — upstream kubelet/CRI annotation for restore:** No KEP exists
  (verified against the `kubernetes/enhancements/keps/sig-node/` directory;
  KEP-2008 explicitly puts restore *out of scope* for kubelet). Waiting on
  upstream is open-ended.

### 2. Annotation-driven dispatch, not a separate RuntimeClass per restore

The shim wrapper always installs the same RuntimeClass
(`sandbawks-runsc`); restore intent is signaled per-Pod via the
`io.sandbawks/restore-image-path` annotation. Pods without the annotation
get standard fresh-start behavior (pass-through to vanilla
`CreateContainer` → runsc).

- **Why:** RuntimeClass is a coarse, cluster-scoped switch — a sandbox's
  "this is a restore" state changes on every resume. Annotations are
  per-Pod and trivially set by the existing `PodAdapter`.

### 3. Vanilla `gvisor` RuntimeClass coexists with `sandbawks-runsc`

The Plan 05 `gvisor` RuntimeClass stays; sandbawks Pods that need restore
support migrate to `sandbawks-runsc`. Operators who want vanilla gVisor for
non-sandbawks workloads on the same cluster are unaffected.

- **Why:** Don't replace global infrastructure when an additive
  RuntimeClass works.
- **Sandbawks Pods default to `sandbawks-runsc`** — `PodAdapter` swaps the
  default. A `Sandbox.spec.runtimeClassName` set explicitly by the user
  still wins.

### 4. Pause path stays direct (`runsc checkpoint` shell-out)

The gVisor shim's standard Task `Checkpoint` RPC is
`errdefs.ErrNotImplemented`
([source](https://github.com/google/gvisor/blob/master/pkg/shim/v1/runsc/service.go)).
We do **not** plumb pause through the shim. Plan 05 Task 7's direct
`runsc checkpoint` invocation in `GvisorAdapter::pause` remains.

- **Why:** The shim doesn't help on the pause side because the upstream
  Checkpoint RPC is unimplemented, and the lifecycle problem the wrapper
  solves (kubelet + CRI + CNI tear-down on container state transitions)
  only applies to *restore*. Pause leaves the running container undisturbed.

### 5. Snapshot staging via a new `SnapshotJob` mode `Stage`

Before the resume Pod is created, the controller dispatches a
`SnapshotJob` with `spec.mode: Stage` to the target node. The node
DaemonSet downloads the snapshot bundle from S3 to
`/var/lib/sandbawks/staging/<sandbox-id>/<job-uuid>/`, verifies sha256,
and patches `SnapshotJob.status.localPath` + `status.phase=Complete`. The
controller waits for `Complete`, then creates the resume Pod targeted
to the same node (`spec.nodeName`) with the annotation pointing at the
staged path. No volume mount is needed — the shim is a host-spawned
daemon and reads the path against the host FS directly; see T9.

- **Why:** Decouples snapshot fetch from Pod scheduling — the shim only
  needs a path, not S3 credentials, and the Stage phase reuses the
  existing `S3SnapshotSource` machinery from Plan 05 T6.
- **Cleanup:** Staging directories are GC'd by the node DaemonSet when the
  parent `SnapshotJob` is deleted (already gated by an `OwnerRef`).
- **Rejected — shim downloads from S3:** Puts S3 credentials in the shim
  config, makes the shim depend on network during container create
  (kubelet's `CreateContainer` timeout is 4 min, but flaky S3 latency on
  the create path is risky), and would need the shim to know about S3
  endpoints which differs per-deployment.

### 6. `SANDBAWKS_RESTORE_MODE=1` envd shim is deprecated, removed in Plan 07

Issue #171's envd idle-shim exists because Plan 05 had no way to keep a
fresh Pod from racing `runsc restore`. The shim wrapper makes that
unnecessary — `CreateWithFSRestore` creates the container *already
restored*; the fresh-entrypoint never runs. We keep the env-var support
code in envd for one release (Plan 06 ships both paths working — if the
shim isn't installed, the controller falls back to `RESTORE_MODE=1` +
`runsc restore` direct, with a deprecation warning logged), then delete
it in Plan 07.

- **Why:** Don't break #171's contract in the same release that fixes the
  underlying issue.

### 7. Go subtree at repo root: `shim/`

The shim is a Go binary because gVisor's reusable shim packages are Go.
First Go code in the repo. Lives at `shim/`, with its own `go.mod`,
`go.sum`, Dockerfile, and Bazel-free build via plain `go build`.

- **Why:** A Rust port of gVisor's shim packages is enormously more code
  than a thin Go wrapper that imports them; Go interop is acceptable for
  one binary that talks ttrpc, not a multi-language polyglot. The Rust
  side of sandbawks doesn't depend on the Go subtree — the shim is shipped
  as a standalone binary in the node image.

## File map

```
shim/                                       # NEW Go subtree
  README.md                                 # one-Go-binary-in-a-Rust-repo rationale
  go.mod                                    # github.com/hypotheosis/sandbawks/shim
  go.sum
  cmd/containerd-shim-sandbawks-v1/
    main.go                                 # shim entry: registers manager + service factory
  internal/wrapper/
    service.go                              # decorator over gVisor's TaskServiceExt
    service_test.go                         # table tests for annotation dispatch
    annotations.go                          # OCI spec.annotations parsing helpers

crates/
  sandbawks-types/
    src/
      snapshot_job.rs                       # +SnapshotJobMode::Stage variant; SnapshotJobStatus.localPath: Option<String>
      runtime.rs                            # RuntimeAdapter::stage_snapshot(sandbox_id, manifest, sink) → local_path
    bin/crdgen.rs                           # regen — schema bump for new enum + status field

  sandbawks-controller/
    src/
      state_machine.rs                      # (Resuming, StageComplete) → Resuming + Action::CreateRestorePod
      reconcile.rs                          # split Action::Resume: Stage SnapshotJob first; then create Pod with restore annotation; remove RESTORE_MODE injection
      pod_adapter.rs                        # default runtimeClassName=sandbawks-runsc when restore_mode; annotations.insert("io.sandbawks/restore-image-path", staged_path); spec.nodeName pins to staging node
    tests/
      reconcile.rs                          # Stage → CreateRestorePod → PodReady → Running fixture
      pod_adapter.rs                        # restore-mode Pod has sandbawks-runsc class + annotation + spec.nodeName pin

  sandbawks-node/
    src/
      gvisor.rs                             # GvisorAdapter::stage_snapshot impl (download + verify, write local path); resume() is now a fallback-only path (deprecated)
      reconcile.rs                          # SnapshotJob reconciler dispatches Stage → adapter.stage_snapshot
    tests/
      gvisor.rs                             # stage_snapshot writes expected files; resume() still works for backwards-compat smoke

  envd/
    src/bin/envd.rs                         # SANDBAWKS_RESTORE_MODE handling logs "deprecated, will be removed in Plan 07" on entry

ci/
  kind-node/
    Dockerfile                              # +stage that builds containerd-shim-sandbawks-v1 from shim/ and COPYs to /usr/local/bin/
    containerd-sandbawks-runsc.toml         # NEW — runtime handler "sandbawks-runsc" → runtime_type "io.containerd.sandbawks-runsc.v1"

deploy/
  helm/sandbawks/templates/
    runtimeclass-sandbawks-runsc.yaml       # NEW — gated by .Values.sandbawksRunsc.enabled (default true)
  helm/sandbawks/values.yaml                # +sandbawksRunsc.enabled=true
  kind/fixtures/
    pause-resume-smoke.sh                   # PAUSE_RESUME_SMOKE_FULL defaults back to 1

changelog/
  2026-05-26T160040Z-plan-06-shim-wrapper-doc.md     # THIS PR — plan doc only
  2026-...-plan-06-t<N>-...md                        # one per task as they land

justfile                                    # +shim-build, +shim-image-build
```

New workspace deps: none on the Rust side; the shim subtree manages its
own Go deps via `go.mod` (pinned to a tagged gVisor release).

---

## Task 1: `shim/` Go subtree scaffold

**Files touched**

- `shim/README.md` — one paragraph explaining why a single Go binary lives
  in a Rust repo (gVisor's shim packages are Go-only; cheaper to wrap than
  reimplement); link to this plan.
- `shim/go.mod` — module `github.com/hypotheosis/sandbawks/shim`, Go 1.23.
- `shim/cmd/containerd-shim-sandbawks-v1/main.go` — stub that prints
  `containerd-shim-sandbawks-v1 (scaffold)` and exits 0.
- `justfile` — recipe `shim-build`: `cd shim && go build -o
  ../target/shim/containerd-shim-sandbawks-v1 ./cmd/containerd-shim-sandbawks-v1`.
- `.gitignore` — `shim/vendor/` if we end up vendoring.

**Smoke**

- `just shim-build` produces a binary that runs and exits 0.
- `cargo test --workspace` unaffected (no Rust changes).

**Commit:** `feat(shim): Go subtree scaffold for containerd-shim-sandbawks-v1 (Plan 06 T1)`

---

## Task 2: Wrap gVisor's runsc TaskService — pass-through decorator

**Files touched**

- `shim/go.mod` — add `gvisor.dev/gvisor v0.0.0-<pinned-rev>` (latest
  tagged release; pin sha for reproducibility).
- `shim/cmd/containerd-shim-sandbawks-v1/main.go` — hands off lifecycle
  to gVisor's shim harness via the `Manager` constructor exported from
  [`pkg/shim/v1/manager.go`](https://github.com/google/gvisor/blob/master/pkg/shim/v1/manager.go)
  (exact constructor symbol TBD at implementation time — `manager.go`'s
  `Manager` interface exposes `Name`/`Start`/`Stop`/`Info`, and the
  binary's `main.go` is a thin wrapper that registers our wrapped
  service factory in place of the default runsc service from
  [`pkg/shim/v1/runsc/service.go`](https://github.com/google/gvisor/blob/master/pkg/shim/v1/runsc/service.go)).
  The wrapped service factory lives at `internal/wrapper.NewService(...)`.
- `shim/internal/wrapper/service.go` — `Service` struct wrapping
  `extension.TaskServiceExt`. Implements *every* method of
  `task.TaskService` plus the two extensions. Default behavior: forward
  every call straight to the wrapped service. No annotation parsing yet.
- `shim/internal/wrapper/service_test.go` — table test: for each of the
  ~17 standard task RPCs + the 2 extensions, assert the wrapper calls
  through to a recording fake.

**Smoke**

- `cd shim && go test ./...` green.
- A locally-built shim binary registers with kind's containerd
  (`crictl info` shows `sandbawks-runsc` under `runtimes`) — deferred to
  T6 where the image lands; for T2, manual `runsc --help`-style invocation
  is enough.

**Commit:** `feat(shim): pass-through TaskService decorator over gVisor v1 (Plan 06 T2)`

---

## Task 3: Annotation-aware `Create` → `CreateWithFSRestore`

**Files touched**

- `shim/internal/wrapper/annotations.go` — `parseRestoreAnnotation(bundleDir
  string) (imagePath string, ok bool, err error)`. Reads
  `<bundleDir>/config.json`, extracts
  `annotations["io.sandbawks/restore-image-path"]` from the OCI runtime
  spec, returns the local path. No env-var fallback — Decision §2 makes
  the annotation the sole contract surface. Errors on a relative or
  non-existent path (defense against escaping the staging root).
- `shim/internal/wrapper/service.go` — override `Create`:
  ```go
  func (s *Service) Create(ctx context.Context, r *task.CreateTaskRequest) (*task.CreateTaskResponse, error) {
      path, ok, err := parseRestoreAnnotation(r.Bundle)
      if err != nil { return nil, err }
      if !ok {
          return s.inner.Create(ctx, r)
      }
      return s.inner.CreateWithFSRestore(ctx, &extension.CreateWithFSRestoreRequest{
          Create: r,
          Conf:   extension.FSRestoreConfig{ImagePath: path, Direct: false},
      })
  }
  ```
- Persist the restore intent for the matching `Start` call: a small
  in-shim `sync.Map` keyed by container ID, set when we dispatch
  `CreateWithFSRestore`. Cleared on `Delete` or `Shutdown`.
- `shim/internal/wrapper/service_test.go` — three cases:
  1. No annotation → falls through to `Create`, no state stored.
  2. Annotation present, path exists → `CreateWithFSRestore` dispatched
     with that path; state stored.
  3. Annotation present, path missing → error before any dispatch.

**Smoke**

- `cd shim && go test ./...` green.

**Commit:** `feat(shim): Create-time annotation dispatch to CreateWithFSRestore (Plan 06 T3)`

---

## Task 4: Annotation-aware `Start` → `Restore`

**Files touched**

- `shim/internal/wrapper/service.go` — override `Start`:
  ```go
  func (s *Service) Start(ctx context.Context, r *task.StartRequest) (*task.StartResponse, error) {
      if _, isRestore := s.restoreCIDs.Load(r.ID); !isRestore {
          return s.inner.Start(ctx, r)
      }
      s.restoreCIDs.Delete(r.ID)
      return s.inner.Restore(ctx, &extension.RestoreRequest{
          Start: r,
          Conf:  extension.RestoreConfig{ImagePath: s.imagePathFor(r.ID), Direct: false, Background: false},
      })
  }
  ```
- Track image paths alongside CIDs in the `sync.Map` from T3.
- `shim/internal/wrapper/service_test.go` — extend:
  1. `Create` without annotation → `Start` passes through.
  2. `Create` with annotation → `Start` dispatches `Restore` with the
     remembered image path.
  3. Two containers with different annotations on the same shim instance
     don't cross-contaminate.

**Smoke**

- `cd shim && go test ./...` green.

**Commit:** `feat(shim): Start-time dispatch to Restore for restored containers (Plan 06 T4)`

---

## Task 5: Shim binary in the kind-node image

**Files touched**

- `ci/kind-node/Dockerfile`:
  - New stage `shim-builder`: `FROM golang:1.23-alpine`, copies `shim/`,
    runs `go build`, emits the binary.
  - Final stage: `COPY --from=shim-builder /out/containerd-shim-sandbawks-v1
    /usr/local/bin/`.
- `ci/kind-node/containerd-sandbawks-runsc.toml` (new):
  ```toml
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.sandbawks-runsc]
    runtime_type = "io.containerd.sandbawks-runsc.v1"
  ```
  Containerd discovers the shim by name pattern: a runtime_type of
  `io.containerd.<name>.v1` is looked up on `$PATH` as
  `containerd-shim-<name>-v1`. The binary we ship at
  `/usr/local/bin/containerd-shim-sandbawks-v1` satisfies that.
- Existing `ci/kind-node/Dockerfile` (lines 81-84) currently `COPY`s
  `containerd-runsc.toml` to `/tmp/` and then `cat`s it onto the end of
  `/etc/containerd/config.toml` — kind's containerd config has no
  `imports = [...]` key, so `conf.d/` drop-ins are silently ignored (the
  existing Dockerfile comment is explicit on this). T5 mirrors that
  pattern: a second `COPY ci/kind-node/containerd-sandbawks-runsc.toml
  /tmp/` followed by a second `cat … >> /etc/containerd/config.toml; rm`
  in the same `RUN` block. Do **not** use `conf.d/`.

**Verification**

- `just kind-node-image-build` succeeds.
- `kind create cluster --image sandbawks-kind-node:dev` succeeds;
  `kubectl exec -n kube-system <containerd-pod> -- crictl info` shows both
  `runsc` and `sandbawks-runsc` runtimes registered.

**Commit:** `feat(ci): bake containerd-shim-sandbawks-v1 into kind-node image (Plan 06 T5)`

---

## Task 6: Helm `RuntimeClass: sandbawks-runsc`

**Files touched**

- `deploy/helm/sandbawks/templates/runtimeclass-sandbawks-runsc.yaml`:
  ```yaml
  {{- if .Values.sandbawksRunsc.enabled }}
  apiVersion: node.k8s.io/v1
  kind: RuntimeClass
  metadata:
    name: sandbawks-runsc
    labels: {{- include "sandbawks.labels" . | nindent 4 }}
  handler: sandbawks-runsc
  {{- end }}
  ```
- `deploy/helm/sandbawks/values.yaml`:
  ```yaml
  sandbawksRunsc:
    enabled: true
  ```

**Verification**

- `helm lint` clean.
- `helm template test deploy/helm/sandbawks --set sandbawksRunsc.enabled=true`
  renders the RuntimeClass with handler `sandbawks-runsc`.

**Commit:** `chore(deploy): RuntimeClass sandbawks-runsc Helm template (Plan 06 T6)`

---

## Task 7: `SnapshotJobMode::Stage` + `SnapshotJobStatus.localPath`

**Files touched**

- `crates/sandbawks-types/src/snapshot_job.rs`:
  - Extend `SnapshotJobMode` with `Stage`. JSON tag `"Stage"`
    (PascalCase, matches the existing `Pause`/`Resume` variants — the
    enum has no `#[serde(rename_all = ...)]`, so serde defaults to the
    Rust variant name). Do **not** add a `rename_all` attribute in this
    task; that would silently break already-deployed `SnapshotJob`
    resources whose `mode` field is `"Pause"`/`"Resume"`.
  - Extend `SnapshotJobStatus` with
    `pub local_path: Option<String>` (camelCase `localPath`,
    `#[serde(default, skip_serializing_if = "Option::is_none")]`).
- `crates/sandbawks-types/src/runtime.rs`:
  - Add `RuntimeAdapter::stage_snapshot(&self, sandbox_id: &str,
    manifest: &SnapshotManifest, source: &dyn SnapshotSource) ->
    Result<String, RuntimeError>` — returns the local path the bundle was
    staged to. Default impl returns
    `RuntimeError::Unsupported("stage_snapshot")`.
- `crates/sandbawks-types/bin/crdgen.rs` — no code change, but the
  regenerated `snapshotjobs.sandbawks.io.yaml` schema picks up the new
  enum variant + status field. Check that the existing crdgen golden test
  still passes after regen.

**Verification**

- `cargo test -p sandbawks-types --all-features` green.
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` regenerates 5
  files; running it twice produces no diff. The new variant + field appear
  in the schema.

**Commit:** `feat(types): SnapshotJobMode::Stage + status.localPath (Plan 06 T7)`

---

## Task 8: `GvisorAdapter::stage_snapshot` — S3 → local path with sha256 verify

**Files touched**

- `crates/sandbawks-node/src/gvisor.rs`:
  - Implement `stage_snapshot`: allocate
    `/var/lib/sandbawks/staging/<sandbox-id>/<job-uuid>/`, stream each
    `SnapshotPart` from the `SnapshotSource` with sha256 verification as
    bytes land, lay out the files as `CreateWithFSRestore` expects
    (`memory`, `rootfs-delta/`, `envd-state.json` — confirm exact layout
    against gVisor's `runsc restore --image-path` docs during
    implementation), return the directory path.
  - `GvisorAdapter::resume` keeps its current implementation as a
    *fallback* code path used only when the
    `io.sandbawks/restore-via-shim` env-var is unset on the node binary.
    A `tracing::warn!` notes the fallback was taken.
- `crates/sandbawks-node/src/reconcile.rs`:
  - `SnapshotJob::Stage` dispatches to `stage_snapshot`; on success, patch
    `status.localPath` + `status.phase=Complete`. On failure, patch
    `status.phase=Failed` with `status.message`.
- `crates/sandbawks-node/tests/gvisor.rs`:
  - New test: pre-populate `FakeSnapshotStore` with a manifest + three
    parts, run `stage_snapshot`, assert the returned path exists, each
    expected file is present, and per-file sha256 matches the manifest.

**Verification**

- `cargo test -p sandbawks-node --all-features` green.

**Commit:** `feat(node): GvisorAdapter::stage_snapshot — S3 download + verify (Plan 06 T8)`

---

## Task 9: Controller — split `Action::Resume` into Stage + CreateRestorePod

**Files touched**

- `crates/sandbawks-controller/src/state_machine.rs`:
  - Add `Action::StageSnapshot` and `Action::CreateRestorePod`.
  - `(Paused, ResumeRequested) → Resuming + Action::StageSnapshot`.
  - `(Resuming, StageComplete) → Resuming + Action::CreateRestorePod`.
  - `(Resuming, PodReady, snapshot_job_ref=None) → Running + Action::None`
    (the existing terminal transition; renamed for clarity).
- `crates/sandbawks-controller/src/reconcile.rs`:
  - `Action::StageSnapshot` creates a `SnapshotJob` with `mode: Stage`,
    `spec.nodeName = <picked target node>` (re-use the existing pick-a-node
    logic from `Action::RecreatePodForResume`), `spec.sandboxName`,
    `spec.snapshotRef = sandbox.status.snapshot_ref`. Patches
    `Sandbox.status.snapshot_job_ref`.
  - Pre-trigger probe: when `status.snapshot_job_ref.is_some()` and that
    job's mode is `Stage` and `status.phase=Complete`, copy
    `SnapshotJob.status.localPath` into a new
    `Sandbox.status.restoreStagingPath` field, clear
    `status.snapshot_job_ref`, fire `Trigger::StageComplete`.
  - `Action::CreateRestorePod` calls `create_pod` (no longer with
    `restore_mode=true` env-injection); the `restore_staging_path` is
    threaded through to `PodAdapter`.
- `crates/sandbawks-controller/src/pod_adapter.rs`:
  - When `restore_staging_path` is set:
    - Default `runtime_class_name` to `Some("sandbawks-runsc")` if
      sandbox didn't set one (sandbawks's choice; user spec still wins).
    - Insert `Pod.metadata.annotations["io.sandbawks/restore-image-path"]
      = staging_path`.
    - Set `Pod.spec.nodeName` to the staging node — the resume Pod must
      land on the same node where T8 staged the bundle, since the
      annotation carries a node-local filesystem path.
    - Do **not** add a `hostPath` volume or any volume mount referencing
      the staging path. The containerd shim runs as a host-spawned
      daemon process and resolves `ImagePath` against the host FS
      directly; the container is in its own mount namespace and never
      needs to read the staging dir (after `CreateWithFSRestore` the
      container's filesystem is materialized by the runtime from
      checkpoint + rootfs-delta — the staging dir is source-only).
    - Do **not** inject `SANDBAWKS_RESTORE_MODE=1` anymore.
- `crates/sandbawks-controller/tests/reconcile.rs`:
  - New fixture: `(Paused, Connect) → StageSnapshot job created → mock
    Stage job complete → CreateRestorePod with annotation + RuntimeClass
    + staging path → PodReady → Running`.

**Verification**

- `cargo test -p sandbawks-controller --all-features` green.
- State-machine unit + property tests pass (they should — only new
  states/actions added).

**Commit:** `feat(controller): split Resume into Stage + CreateRestorePod (Plan 06 T9)`

---

## Task 10: Deprecate `SANDBAWKS_RESTORE_MODE` envd shim

**Files touched**

- `crates/envd/src/bin/envd.rs`:
  - When `SANDBAWKS_RESTORE_MODE=1` is set on entry, log
    `tracing::warn!("SANDBAWKS_RESTORE_MODE is deprecated and will be
    removed in Plan 07; falling back to idle hold")` and proceed as
    today. No behavior change.
- `crates/envd/tests/binary_smoke.rs::envd_restore_mode_skips_listeners_and_state_file`:
  - Add an assertion that the deprecation warning appears in the binary's
    stderr.
- `changelog/<ts>-plan-06-t10-deprecate-restore-mode.md` documents the
  removal target (Plan 07) and links #171 for context.

**Verification**

- `cargo test -p envd --all-features` green.

**Commit:** `chore(envd): deprecate SANDBAWKS_RESTORE_MODE; warn on entry (Plan 06 T10)`

---

## Task 11: Flip `PAUSE_RESUME_SMOKE_FULL=1` default; smoke includes shim path

**Files touched**

- `deploy/kind/fixtures/pause-resume-smoke.sh`:
  - Default `PAUSE_RESUME_SMOKE_FULL=${PAUSE_RESUME_SMOKE_FULL:-1}`
    (currently `:-0`).
  - Promote the variable to tri-state so the existing `=0` semantics
    are preserved (anyone with `PAUSE_RESUME_SMOKE_FULL=0` already set
    in CI configs or local scripts keeps the skip-and-exit behavior
    they expect):
    - `=1` (new default): exercise the shim path end-to-end. After the
      resume step, assert that the resumed container was created via
      the shim path: query the `sandbawks-runsc` RuntimeClass on the
      Pod and assert the `io.sandbawks/restore-image-path` annotation
      is set.
    - `=0` (unchanged): skip the resume half entirely; print "skipping
      resume round-trip" and exit success. Same as today (script lines
      335-342).
    - `=legacy` (new, transient): exercise the deprecated `RESTORE_MODE`
      path that T10 kept in for one release. Removed alongside T10's
      removal in Plan 07. Useful for verifying the deprecation warning
      fires correctly during the transition window.
  - Do **not** silently redefine `=0` to mean "take the legacy path".
- `.forgejo/workflows/kind-smoke.yaml`:
  - `KIND_SMOKE_INCLUDE_PAUSE_RESUME: '1'` (already so per Plan 05 T14);
    no change needed, but the test now actually validates a real restore.

**Verification**

- Locally: `SANDBAWKS_KIND_GVISOR=1 just cluster-up && just
  pause-resume-smoke && just cluster-down` exits 0 with the resumed
  sentinel matching.
- CI: `kind-smoke` job on `main` runs green; the resume step now exercises
  the shim path end-to-end.

**Commit:** `test(node): pause-resume-smoke FULL=1 default; assert shim path (Plan 06 T11)`

---

## Task 12: Workspace green gate + changelog

**Files touched**

- `changelog/<ts>-plan-06-summary.md` — matches Plan 05's shape: Summary,
  What landed (per task), What's deliberately not here (lazy restore,
  Kata-fc, runc-criu, SDK compat — now Plan 07+), Next plan preview.

**Verification gates**

All must pass before T12 closes:

- `cargo test --workspace --all-features` green.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  silent.
- `cargo fmt --all -- --check` silent.
- `cargo doc --workspace --no-deps` warning-free.
- `cd shim && go test ./...` green.
- `cd shim && go vet ./...` silent.
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` idempotent.
- `helm lint deploy/helm/sandbawks` clean.
- `helm template test deploy/helm/sandbawks --set sandbawksRunsc.enabled=true`
  renders the new RuntimeClass.
- `just kind-node-image-build` produces an image containing the shim
  binary at `/usr/local/bin/containerd-shim-sandbawks-v1`.
- The `kind-smoke` workflow on `main` runs green with
  `PAUSE_RESUME_SMOKE_FULL=1`; the resumed container was created via the
  shim path (asserted in T11).

**Commit:** `docs: Plan 06 green-gate sweep + changelog (Plan 06 T12)`

---

# 2026-05-26 — Plan 06: `containerd-shim-sandbawks-v1` for kubelet-mediated resume

## What landed

(To be filled in during Task 12 when the plan closes; placeholder shape:)

- `shim/` — Go subtree shipping `containerd-shim-sandbawks-v1`, a thin
  decorator over gVisor's `pkg/shim/v1` that translates annotated
  Create+Start into the existing `CreateWithFSRestore`+`Restore`
  extension RPCs.
- `RuntimeClass: sandbawks-runsc` — additive to the existing `gvisor`
  RuntimeClass; sandbawks Pods default to it.
- `SnapshotJobMode::Stage` — controller dispatches snapshot download to
  the target node before Pod create; node DaemonSet writes a local
  staging directory and reports it back via status.
- Controller `Action::Resume` split into `StageSnapshot` +
  `CreateRestorePod`; the restore Pod is pinned (`spec.nodeName`) to
  the staging node and carries the `io.sandbawks/restore-image-path`
  annotation the shim reads at container-create time. No volume mount
  is needed — the shim resolves the path against the host FS directly.
- `pause-resume-smoke.sh` defaults `PAUSE_RESUME_SMOKE_FULL=1`; resume
  now actually works against a kubelet-managed container.
- `SANDBAWKS_RESTORE_MODE` envd shim is deprecated, scheduled for removal
  in Plan 07.

## What's deliberately not here

- **`runsc checkpoint` replacement** — the shim's standard Task
  `Checkpoint` RPC is `errdefs.ErrNotImplemented` upstream. Pause stays on
  the Plan 05 T7 direct shell-out. Revisit only if upstream gVisor wires
  Checkpoint through the shim.
- **Lazy / page-fault restore** — Plan 07.
- **Kata-fc** — Plan 08.
- **runc-criu** — Plan 09.
- **SDK compat tests + perf gates** — renumbered to Plan 07 (was Plan
  06's preview slot in Plan 05's "Next plan" section).

## Next plan

**Plan 07 — lazy/page-fault restore + warm pool**:
- Use gVisor's `runsc restore --background` / lazy-page mode to start the
  container before all memory pages are paged in.
- Warm-pool of pre-restored sandboxes for sub-second connect latency.
- Removal of the deprecated `SANDBAWKS_RESTORE_MODE` envd shim.

## Definition of done

All Task 12 gates above are green.
