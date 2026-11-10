# sandbawks MVP — Design Spec

- **Status:** Draft
- **Date:** 2026-05-21
- **Authors:** Peter Grace
- **Scope:** v0.1 MVP

## 1. Context & Motivation

E2B (e2b.dev) is the de facto code-execution sandbox for AI agents. It exposes a
simple REST API: create a sandbox, run code in it, pause it (preserving full
filesystem + memory state), resume it later, kill it. Their hosted offering
uses Firecracker microVMs on bare-metal hosts and reports ~150ms cold start
(via a warm pool), ~1s resume, ~4s/GiB pause.

Several Kubernetes-native projects target the same use case but each has gaps:

- `kubernetes-sigs/agent-sandbox` (SIG Apps, Google-backed, 2.3k stars): has a
  `Sandbox` CRD but pause/resume/snapshot is on the 2026–2027 roadmap, and it
  does not expose an E2B-compatible API.
- `openkruise/agents` (CNCF, Alibaba-driven, ~180 stars): ships pause/resume/
  checkpoint via gVisor + Kata and exposes an E2B-compatible protocol, but has
  a smaller ecosystem and is tightly coupled to OpenKruise.
- E2B's own infra is open-source but assumes bare-metal Firecracker hosts and
  is not a Kubernetes controller.

**sandbawks** is a Kubernetes-native, E2B-API-compatible sandbox controller
written in Rust. It targets a generic Kubernetes cluster (no node-prep required
for the default profile), abstracts the underlying sandbox runtime behind a
pluggable `RuntimeAdapter` trait (gVisor by default, Kata-Firecracker on
capable nodes, runc+CRIU as a low-isolation fallback), and ships full E2B REST
API parity in the MVP so that the existing E2B Python and JS SDKs work against
sandbawks with only an `E2B_API_URL` change.

**Intended outcome:** an operator can `helm install sandbawks` on any
conformant Kubernetes cluster, point an existing E2B-SDK-based agent at it,
and get sandbox create / exec / pause / resume / kill working end-to-end with
performance within a small multiple of hosted E2B.

## 2. Goals & Non-Goals

### Goals (MVP)

- Full E2B REST API parity for sandbox lifecycle, exec, file ops, and events.
- Pluggable runtime via Kubernetes RuntimeClass: gVisor, Kata-Firecracker,
  runc+CRIU.
- Pause to S3-compatible object storage; resume can land on any node.
- Warm pool (`SandboxWarmPool` CRD) for sub-second cold start.
- Runs on a generic Kubernetes cluster (no bare-metal requirement for the
  default gVisor profile).
- Multi-tenant via Kubernetes Namespaces + `TeamAPIKey` CRD.
- Snapshot integrity (sha256, idempotent upload, partial-upload recovery).

### Non-Goals (deferred to v0.2+)

- Admin / tenant UI.
- Template build pipeline (Dockerfile → microVM image). MVP consumes pre-built
  OCI images.
- Multi-region snapshot replication.
- Billing / usage metering.
- GPU sandboxes.
- Live migration (vs. pause/resume).

## 3. Architecture

Three control-plane components plus an in-sandbox agent:

```
┌─────────────────────────────────────────────────────────────────┐
│   E2B SDK (Python/JS)  ──HTTPS──>  sandbawks-api (axum)         │
│                                       │                          │
│                              creates  ▼                          │
│                                ┌──── Sandbox CRD ────┐           │
│                                │                     │           │
│                                ▼ watches             │           │
│                       sandbawks-controller (kube-rs) │           │
│                                │                     │           │
│                       creates  ▼                     │           │
│                              Pod (RuntimeClass=gvisor│kata-fc)   │
│                                │                     │           │
│                  per-node ───  ▼                     │           │
│                       sandbawks-node (DaemonSet) ────┘           │
│                                │                                 │
│                       pauses/restores via CRI checkpoint         │
│                                │                                 │
│                                ▼                                 │
│                     ┌─── S3-compatible store ───┐                │
│                     │ memory.zst + rootfs.zst   │                │
│                     │ + manifest.json           │                │
│                     └───────────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

### 3.1 Components

| Component | Process | Responsibility |
|---|---|---|
| `sandbawks-api` | axum Deployment | E2B-compatible REST API. Translates HTTP into CRD operations. Stateless, horizontally scalable. Authenticates via `X-API-Key`. |
| `sandbawks-controller` | `kube-rs` Deployment (leader-elected) | Watches `Sandbox`, `SandboxTemplate`, `SandboxWarmPool`. Materializes Pods. Drives the Sandbox state machine. |
| `sandbawks-node` | DaemonSet | Owns pause/resume mechanics via `RuntimeAdapter` impls. Streams snapshots ↔ S3 with parallel chunked transfer. |
| `envd-rs` | Process inside sandbox Pod | gRPC server on port 49983 (E2B-compatible). Handles exec, file ops, PTY. Baked into template images. |

### 3.2 Crate / repo layout

```
sandbawks/
  Cargo.toml               # workspace
  crates/
    sandbawks-types/       # shared CRDs, error types, API DTOs
    sandbawks-api/         # axum HTTP server
    sandbawks-controller/  # kube-rs controller
    sandbawks-node/        # DaemonSet + RuntimeAdapter impls
    envd/                  # in-sandbox agent
  deploy/
    crds/                  # CustomResourceDefinitions
    helm/sandbawks/        # Helm chart
  docs/
    superpowers/specs/
  changelog/
```

## 4. Custom Resources

### 4.1 `Sandbox`

```yaml
apiVersion: sandbawks.io/v1alpha1
kind: Sandbox
metadata:
  name: sbx-abc123              # equals E2B sandboxID
  namespace: team-acme
spec:
  templateRef: python-data-sci
  runtimeClassName: gvisor      # override SandboxTemplate.defaultRuntimeClass
  resources:
    cpu: "2"
    memory: 2Gi
    ephemeralStorage: 10Gi      # mapped to Pod ephemeral-storage limit
  timeout: 3600s
  autoPause: true               # pause-on-timeout vs kill
  env:
    OPENAI_API_KEY: ...
  metadata:                     # opaque K/V from E2B API
    user: "alice"
  networkPolicy:
    egressAllow:
      - api.openai.com
status:
  phase: Running                # Creating|Running|Pausing|Paused|Resuming|Terminating|Failed
  assignedNode: node-7
  podRef: sbx-abc123-pod
  envdAddress: 10.244.3.27:49983
  snapshotRef: s3://snapshots/team-acme/sbx-abc123/2026-05-21T18-22-04Z
  conditions: [...]
  lastTransitionTime: 2026-05-21T18:22:04Z
```

### 4.2 `SandboxTemplate`

```yaml
apiVersion: sandbawks.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-data-sci
  namespace: team-acme
spec:
  image: ghcr.io/acme/py-data-sci:1.4
  defaultRuntimeClass: gvisor
  defaultResources:
    cpu: "1"
    memory: 1Gi
    ephemeralStorage: 5Gi
  envdImage: ghcr.io/sandbawks/envd:0.1
  defaultTimeout: 3600s
```

### 4.3 `SandboxWarmPool`

```yaml
apiVersion: sandbawks.io/v1alpha1
kind: SandboxWarmPool
metadata:
  name: python-pool
  namespace: team-acme
spec:
  templateRef: python-data-sci
  minReady: 5
  maxReady: 20
status:
  ready: 5
  warmSnapshots:
    - sbx-warm-001
    - sbx-warm-002
```

### 4.4 `TeamAPIKey` (auth)

```yaml
apiVersion: sandbawks.io/v1alpha1
kind: TeamAPIKey
metadata:
  name: ci-bot
  namespace: team-acme
spec:
  keyPrefix: "sbk_a1b2"         # first 8 chars, indexed for lookup
  keyHash: "sha256:..."         # SHA-256 of the full key; never the key itself
  scopes: ["sandboxes:*"]
```

## 5. State machine

The `Sandbox` state machine is driven by `sandbawks-controller`:

```
              POST /sandboxes
                    │
                    ▼
              ┌──────────┐
       ┌─────>│ Creating │────────────┐
       │      └──────────┘            │ Pod Ready + envd reachable
       │                              ▼
       │                          ┌─────────┐
       │ POST /connect            │ Running │
       │     (warm pool hit)      └─────────┘
       │      ┌──────────────────────┬─────────────┐
       │      │ POST /pause          │ POST /kill  │ timeout + autoPause
       │      ▼                      │             ▼
       │  ┌─────────┐                │         ┌─────────┐
       │  │ Pausing │                │         │ Pausing │
       │  └─────────┘                │         └─────────┘
       │      │ snapshot uploaded    │             │
       │      ▼                      │             ▼
       │  ┌────────┐                 │         ┌────────┐
       └──│ Paused │                 │         │ Paused │
          └────────┘                 │         └────────┘
              │ POST /connect        │
              ▼                      ▼
          ┌──────────┐         ┌─────────────┐
          │ Resuming │         │ Terminating │
          └──────────┘         └─────────────┘
                                      │
                                      ▼
                                  (deleted)
```

Every transition is **durable in `status.phase` before side effects**, so the
controller can crash and resume safely. The DaemonSet treats `phase=Pausing`
and `phase=Resuming` as its work queue.

## 6. Pause / Resume mechanics

`sandbawks-node` exposes `RuntimeAdapter` impls behind a trait:

```rust
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    /// Quiesce the running container, capture memory + rootfs delta,
    /// stream to the sink, return a manifest. Idempotent.
    async fn pause(
        &self,
        pod_uid: &str,
        sink: &SnapshotSink,
    ) -> Result<SnapshotManifest, RuntimeError>;

    /// Restore a sandbox from the manifest into the target Pod's
    /// sandbox slot. Idempotent on retry.
    async fn resume(
        &self,
        manifest: &SnapshotManifest,
        target_pod: &Pod,
    ) -> Result<(), RuntimeError>;

    fn runtime_class(&self) -> &str;
}
```

Three impls in MVP:

| Adapter | Mechanism | Target pause | Target resume | Node req |
|---|---|---|---|---|
| `GvisorAdapter` | containerd `Checkpoint` API + `runsc checkpoint` with `--exclude-committed-zero-pages` | ~3–5 s for 1–2 GiB | ~1–2 s | any Linux node |
| `KataFcAdapter` | Kata snapshot RPC + Firecracker MAP_PRIVATE memory snapshot | ~1–2 s for 1–2 GiB | ~100–500 ms | KVM-capable node (advertised via NFD) |
| `RuncCriuAdapter` | CRI checkpoint API (KEP-2008 alpha) + CRIU | ~5–8 s | ~3–5 s | runc + CRIU on node |

### 6.1 Snapshot layout in S3

```
s3://<bucket>/<namespace>/<sandboxID>/<ISO-8601-ts>/
  manifest.json            # { runtime, version, files[], sha256, sandbox_meta }
  memory.zst               # streamed in 4 MiB chunks, parallel upload
  rootfs-delta.tar.zst     # delta vs the template OCI image
  envd-state.json          # any envd-internal state
```

### 6.2 Resume can land on a different node

Snapshots are object-store-backed, so resume scheduling is free to choose any
node whose RuntimeClass matches. This is critical for spreading load and
surviving node failures. Trade-off: pulling a multi-GiB snapshot from S3
across the cluster network is slower than node-local restore. Mitigations:

- Parallel chunk download (8–16 concurrent streams).
- Lazy/page-fault restore where the runtime supports it (gVisor 2024+,
  Firecracker).
- Optional node-local snapshot cache (CSI generic-ephemeral or hostPath) for
  hot snapshots.

## 7. E2B API surface (full parity, MVP)

| E2B endpoint | Handler | Action |
|---|---|---|
| `POST /sandboxes` | `create_sandbox` | Create `Sandbox`. If `SandboxWarmPool` for the template has capacity, claim a warm snapshot and short-circuit cold boot. |
| `GET /sandboxes` | `list_sandboxes` | List Sandboxes in the API-key's namespace. |
| `POST /sandboxes/{id}/pause` | `pause_sandbox` | Set `spec.phase=Paused` (declarative). Controller + node DaemonSet drive snapshot upload. |
| `POST /sandboxes/{id}/connect` | `resume_sandbox` | If `Paused`, set `spec.phase=Running`; resume from `status.snapshotRef`. Return envd address. |
| `POST /sandboxes/{id}/kill` | `kill_sandbox` | Delete the `Sandbox`. Controller GCs Pod + snapshot per retention policy. |
| `GET /events/sandboxes` (SSE) | `events_stream` | Stream from k8s Events API, filtered to Sandboxes in namespace. |
| `GET /events/sandboxes/{id}` (SSE) | `events_stream_one` | As above, for a single sandbox. |
| Exec / code-run (WebSocket) | `exec_proxy` | API proxies WS → envd gRPC. |
| File upload (presigned URL) | `file_upload_mint` + `file_upload_proxy` | API mints a short-lived signed URL on its own domain (HMAC-signed token, not S3). Client PUTs to that URL; gateway authorizes the token and streams the body to envd. Keeps S3 out of the data path so file ops work with no object store. |
| File download (presigned URL) | `file_download_mint` + `file_download_proxy` | Same pattern, reverse direction. |
| `files.list` | `files_list` | RPC to envd `Files.List`. |

**SDK compatibility test:** `pip install e2b` + set `E2B_API_URL` →
all SDK methods exercised in CI.

## 8. Warm pool

`sandbawks-controller` reconciles `SandboxWarmPool` toward `spec.minReady`:

1. Observe `ready < minReady`. Create cold Sandboxes from `templateRef`, let
   envd boot fully and idle.
2. Immediately pause each one, upload snapshot, set `phase=Paused`. Cache the
   snapshot on a fraction of nodes (hostPath warm cache) for lowest restore
   latency.
3. On `POST /sandboxes` with a matching `templateRef`, controller claims a
   warm snapshot, resumes it, hands the sandbox to the requester.

Target cold start (warm pool hit): **~150–300 ms on gVisor, ~50–100 ms on
Kata-FC**, matching hosted E2B order of magnitude.

Cold start without warm pool (first-time template): **~1–3 s on gVisor,
~300–800 ms on Kata-FC**.

## 9. Auth & multi-tenancy

- One E2B "team" = one Kubernetes Namespace.
- `TeamAPIKey` CRDs hold `keyPrefix` (indexed, first 8 chars of `sbk_*`) and
  `keyHash` (sha256). Gateway looks up by prefix, constant-time-compares the
  rest of the key against the hash.
- All Sandbox / SandboxTemplate operations are namespace-scoped via standard
  Kubernetes RBAC; the API gateway impersonates a per-namespace ServiceAccount.
- Default NetworkPolicy on every sandbox Pod: deny all egress. Per-Sandbox
  `spec.networkPolicy.egressAllow` opens specific destinations.

## 10. Error handling

- `thiserror` per crate; `anyhow` only at top-level main / HTTP handler
  boundaries.
- API returns E2B-shaped JSON errors:
  `{ "code": "sandbox_not_found", "message": "...", "request_id": "..." }`.
- Controller uses `kube::runtime::controller` with default exponential-backoff
  requeue on transient failures.
- Every state transition is durable in `status.phase` before side effects
  begin; controller restart is safe.
- Snapshot upload is idempotent + checksummed: every chunk is sha256'd, the
  manifest is the last write, partial uploads on the next attempt detect
  existing chunks and skip.

## 11. Observability

- `tracing` everywhere; `tracing-subscriber` with `env-filter` (already in
  `Cargo.toml`).
- Standard Kubernetes Events on every Sandbox transition.
- Prometheus metrics from each component:
  - `sandbawks_sandbox_phase_transitions_total{from, to, runtime}`
  - `sandbawks_sandbox_pause_duration_seconds{runtime}` (histogram)
  - `sandbawks_sandbox_resume_duration_seconds{runtime, cache_hit}` (histogram)
  - `sandbawks_warmpool_ready{template}` (gauge)
  - `sandbawks_snapshot_bytes{direction}` (counter)

## 12. Testing

- **Unit:** state machine transitions, CRD validation, E2B API serialization,
  RuntimeAdapter mocks.
- **Property** (via `hegel`): pause+resume preserves filesystem byte-for-byte;
  pause+resume preserves env vars; round-trip API serialization.
- **Integration:** `kind` cluster with gVisor RuntimeClass + MinIO; full
  lifecycle: create → exec → pause → resume → exec → kill.
- **E2E:** real E2B Python SDK with `E2B_API_URL=https://sandbawks.local`;
  smoke-test all SDK methods.
- **Perf benchmarks** in CI:
  - Cold start (no warm pool).
  - Cold start (warm pool hit).
  - Pause latency vs sandbox memory size.
  - Resume latency vs sandbox memory size, same-node vs cross-node.

## 13. Verification (how we'll know MVP is done)

1. `helm install sandbawks` on a fresh `kind` cluster succeeds.
2. The E2B Python SDK's `examples/` (pointed at sandbawks) all pass.
3. `cargo test` and the integration suite are green.
4. Perf benchmark CI run shows: cold-start-with-warm-pool p95 < 500 ms on
   gVisor; resume p95 < 2 s for a 1 GiB sandbox; pause p95 < 8 s for a 1 GiB
   sandbox.
5. `cargo clippy -- -D warnings` and `cargo fmt --check` pass.

## 14. Decomposition note for planning

This spec is intentionally larger than a single implementation plan. The
`writing-plans` step should decompose into ordered sub-plans roughly along
these seams; each sub-plan should be independently testable:

1. **`sandbawks-types` + CRDs + Helm scaffold** — schema work, no behavior.
2. **`sandbawks-controller` skeleton** — reconcile loop driving the state
   machine against a fake `RuntimeAdapter` that no-ops; full unit + property
   tests of the state machine.
3. **`envd-rs` + reference template image** — in-sandbox agent, gRPC surface,
   container image build. The vendored E2B `filesystem.proto` is metadata-only
   (`Stat`, `MakeDir`, `Move`, `ListDir`, `Remove`, `WatchDir` + watcher trio)
   and is pinned byte-identical to upstream; **file *content* (read/write of
   bytes) is not exposed over gRPC** and is deferred to sub-plan 4 below.
4. **`sandbawks-api` (lifecycle endpoints + file-content transfer)** — create /
   list / kill / pause / connect translated to CRD operations; SSE events;
   **and the file upload / download path** (`file_upload_mint` +
   `file_upload_proxy`, `file_download_mint` + `file_download_proxy` per §7).
   The gateway mints short-lived HMAC-signed URLs on its own domain and
   streams bodies into / out of envd's filesystem on the server side, since
   envd itself does not carry content RPCs over gRPC. `files.list` and the
   rest of the metadata surface remain pure gRPC pass-through to envd.
5. **`GvisorAdapter` in `sandbawks-node`** — first real runtime adapter, S3
   streaming, end-to-end pause/resume on `kind`.
6. **E2B SDK compat tests + benchmarks** — wire real E2B Python SDK against
   sandbawks; perf gates land in CI.
7. **`SandboxWarmPool` reconciler** — warm-snapshot cache + claim path.
8. **`KataFcAdapter`** — second runtime adapter on KVM-capable nodes.
9. **File ops + `RuncCriuAdapter`** (or defer the latter past v0.1).

## 15. Open questions (to resolve during planning)

- Exact protocol between `sandbawks-api` and `envd-rs` for exec — proxy over
  WebSocket (matches E2B SDK expectation) vs direct gRPC streaming. Lean
  toward WebSocket-on-the-wire, gRPC-internally.
- Snapshot retention policy: per-namespace default (7d?), per-Sandbox
  override?
- Whether to ship the runc+CRIU adapter in v0.1 or defer; KEP-2008 is alpha
  and may not be enabled on most clusters.
- Whether warm pool's hostPath snapshot cache requires PodSecurityPolicy /
  PSA exceptions.

## 16. Out of scope (explicit)

- UI / admin dashboard.
- Template build pipeline (Dockerfile → image).
- Multi-region snapshot replication.
- Billing / usage metering.
- GPU sandboxes.
- Live migration.
