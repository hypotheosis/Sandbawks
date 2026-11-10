# Plan 05 T9 — sandbawks-node SnapshotJob reconciler

Issue: #145 (Plan 05 epic
#136)

## Summary

Wire `GvisorAdapter` into a real reconciler. The new
`sandbawks-node::reconcile` module builds a `kube::runtime::Controller`
that watches `SnapshotJob` CRs filtered to the local node via the
field-selector `spec.nodeName=$NODE_NAME`, dispatches each observed job
to `adapter.pause` or `adapter.resume` based on `spec.mode`, and patches
`SnapshotJob.status` (phase, manifest, message, timestamps) back to the
apiserver. The `sandbawks-node` binary entry now reads `NODE_NAME` plus
the `SANDBAWKS_S3_*` env family, constructs an `S3SnapshotSink` +
`S3SnapshotSource` over a shared `rust-s3` `Bucket` handle, connects a
`GvisorAdapter` over a containerd UDS, and spawns the reconcile loop —
with the legacy `--placeholder` flag still in place so the Plan 04 Helm
chart keeps Pods Running until T12 cuts over.

## Surface area

- `crates/sandbawks-node/src/reconcile.rs`
  - `pub async fn run(client, namespace, ctx)` — boots
    `kube::runtime::Controller<SnapshotJob>` with
    `watcher::Config::default().fields("spec.nodeName=$NODE_NAME")`,
    `shutdown_on_signal`, a 30 s requeue heartbeat, and a 5 s
    error-policy backoff.
  - `pub async fn reconcile(job, ctx)` — single-pass state machine,
    exposed so unit tests can drive the dispatch + idempotency + failure
    paths without standing up a `Controller`.
  - `pub trait SnapshotJobStatusStore` (+ production
    `KubeSnapshotJobStatusStore` and `NodeReconcileError`) — write-side
    abstraction for the `status` subresource, mirroring the controller-
    crate `SandboxStore::patch_status` pattern; production uses
    server-side apply via `PatchParams::apply("sandbawks-node")` +
    `Patch::Merge`.
  - `pub struct ReconcileContext { node_name, adapter, sink, source,
    status_store }` — dependency bundle the `Controller` clones across
    requeues.
- `crates/sandbawks-node/src/bin/sandbawks-node.rs`
  - Production main: builds an in-cluster `kube::Client`,
    `S3SnapshotSink` / `S3SnapshotSource` (path-style addressing for
    MinIO compatibility), `GvisorAdapter` over `containerd_client::Client::from_path`
    + `ReqwestEnvdState::with_default_client`, then blocks on
    `reconcile::run`.
  - Env surface: `NODE_NAME` (required), `SANDBAWKS_NAMESPACE`
    (defaults `default`), `SANDBAWKS_S3_{ENDPOINT,BUCKET,REGION,
    ACCESS_KEY,SECRET_KEY,KEY_PREFIX}`, `SANDBAWKS_WORK_ROOT`,
    `SANDBAWKS_CONTAINERD_SOCKET`. The required vars surface a
    `Context`-wrapped error pointing at the missing var name; defaults
    cover work-root, containerd socket, region, and key prefix.
  - `--placeholder` remains: kubelet keeps the Pod in `Running` until
    T12 swaps the chart's `args:` over to the production set.
- `crates/sandbawks-node/tests/reconcile.rs` (new)
  - Drives `reconcile()` against in-memory fakes: a recording
    `FakeRuntimeAdapter` + `FakeSnapshotJobStatusStore` + the existing
    `sandbawks_types::testing::FakeSnapshotStore`.
  - Six tests covering: Pause dispatch, Resume dispatch, terminal-phase
    idempotency, Pause failure → `phase=Failed` with non-empty
    `message`, Resume failure → `phase=Failed` with non-empty `message`,
    Resume missing `spec.snapshotRef` → propagated `MissingSnapshotRef`
    error after Failed-phase patch lands.
- `crates/sandbawks-node/Cargo.toml`
  - Added the `kube` workspace dep (already configured with the
    `client` + `derive` + `runtime` + `rustls-tls` features at the
    workspace level — no extra knobs needed).

## State machine

Per observed `SnapshotJob`:

1. `status.phase ∈ {Complete, Failed}` → idempotent no-op. This is what
   guards against a re-emitted watch event running `runsc checkpoint`
   twice on the same container.
2. Otherwise patch the in-flight phase (`Checkpointing` for Pause,
   `Restoring` for Resume), preserving the original `started_at` across
   retries so the controller observes a stable "first picked up"
   timestamp.
3. Call `adapter.pause(container_id, &sink)` or
   `adapter.resume(&placeholder_manifest, container_id, &source)`.
4. Patch terminal phase: `Complete` (carrying the manifest on Pause;
   empty on Resume) on success, `Failed` with `message =
   err.to_string()` on failure. `completed_at` stamps the terminal
   patch.

The `Resume` path's manifest argument is a placeholder synthesized from
`spec.snapshotRef` until a later plan task threads the full manifest
through `SnapshotJobSpec`; `GvisorAdapter::resume` re-fetches the
authoritative manifest from `source` anyway. The empty-`sha256`
placeholder matches the v1-compatible empty-snapshot shape so the T8
cross-check stays sound under T9's wiring.

## Validation

- `just fmt` — pass.
- `cargo clippy --workspace --all-targets -- -D warnings` — pass.
- `just test` — full workspace pass; the new `tests/reconcile.rs`
  contributes 6 tests.
- `cargo run -p sandbawks-node -- --help` — binary launches; CLI still
  exposes `--placeholder` per the Helm chart contract.

## Out of scope (covered by later tasks)

- Dockerfile + CI image build → T10 (#146).
- DaemonSet wiring (downward-API `NODE_NAME`, real args, env-from-Secret
  for the S3 credentials) → T12 (#148).
- Live integration test against a real kind + MinIO + gVisor stack →
  T14 (#150).
- Threading the full `SnapshotManifest` through `SnapshotJobSpec` so the
  T8 `manifest.sha256` cross-check becomes a real integrity gate rather
  than a no-op against the empty placeholder.
