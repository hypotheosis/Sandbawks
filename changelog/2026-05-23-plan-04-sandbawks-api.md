# 2026-05-23 — Plan 04: `sandbawks-api` E2B-compatible HTTP/WS gateway

Plan-wide summary changelog, in addition to the per-task entries in
`changelog/2026-05-2{3,4}T*-plan-04-t*.md`. Plan source:
`docs/superpowers/plans/2026-05-23-sandbawks-04-api-gateway.md`.

## What landed

- **New crate `crates/sandbawks-api`** — axum-based HTTP/WS gateway
  speaking the public E2B REST surface. Library-first so integration
  tests can drive the in-process router without binding a TCP port;
  the binary in `src/bin/sandbawks-api.rs` is a thin entrypoint that
  wires the kube client + signing key + envd pool.
- **`TeamAPIKey` auth middleware + `TeamContext` extractor.** Every
  request resolves `X-API-Key` via the prefix-indexed
  `TeamAPIKey` CRD lookup described in spec §9; the matching CR's
  namespace becomes the team's namespace for the duration of the
  request. Constant-time hash compare lives in `sandbawks-types::auth`.
- **Lifecycle endpoints.** `POST /sandboxes`, `GET /sandboxes`,
  `POST /sandboxes/:id/{kill,pause,connect}`. `pause` patches
  `spec.desiredPhase=Paused`; `connect` patches
  `spec.desiredPhase=Running` and polls `status.envdAddress` with a
  30s deadline before surfacing `envd_not_ready`.
- **SSE event streams** at `/events/sandboxes` (team-wide) and
  `/events/sandboxes/:id` (single-sandbox). Each kube Event is
  projected into a stable E2B-shaped DTO with a 15s SSE keepalive
  comment to survive idle proxies.
- **Exec WebSocket bridge** at `/sandboxes/:id/exec`. The gateway
  upgrades the public WS connection and bridges frame-by-frame to
  envd's `Process.Connect` / `Pty.Connect` bidirectional gRPC streams
  (WS-public, gRPC-internal — resolves the spec §15 open question).
- **File upload/download via HMAC-signed URLs.** `POST /files/mint`
  issues short-lived tokens; `PUT /files/upload?token=…` and
  `GET /files/download?token=…` verify the token (the only auth on
  those routes) before relaying the body into envd's `WriteAll` /
  `ReadAll` RPCs.
- **Filesystem metadata pass-through.**
  `GET /files/:sandbox_id/{list,stat,watch}`,
  `DELETE /files/:sandbox_id/remove`,
  `POST /files/:sandbox_id/{move,makedir}` — each a thin projection
  of the matching envd `Filesystem.*` RPC into camelCase JSON.
- **`PodAdapter`** — the first real `RuntimeAdapter` impl. Materializes
  a Pod that runs the envd image on the cluster's default containerd
  runtime; `spec.runtimeClassName` is accepted-but-ignored until
  Plan 05's `GvisorAdapter`. Hooks into the controller's reconcile
  loop via `--runtime pod` (default in chart).
- **`spec.desiredPhase`** typed field on `Sandbox` replaces the legacy
  `sandbawks.io/desired-phase` annotation. The annotation reader stays
  one plan for backward compatibility; Plan 05 removes it.
- **Helm chart additions.** `api-deployment.yaml`, `api-service.yaml`,
  `api-signing-secret.yaml`, RBAC for the `sandbawks-api`
  ServiceAccount (read `teamapikeys`, read/write `sandboxes`, watch
  `events`), and the `api.signingSecret.existingSecret` override.
- **Dockerfile + Forgejo Actions workflow** publishing
  `ghcr.io/hypotheosis/sandbawks-api:{sha,latest}` on every push to `main`.
- **Kind integration test** — `deploy/kind/fixtures/api-smoke.sh`
  walks the full E2B-compatible HTTP lifecycle against real Pods +
  real envd on a `just cluster-up` cluster. Wired into the
  `kind-smoke` workflow behind `KIND_SMOKE_INCLUDE_API=1`.

## What's deliberately not here

- **`GvisorAdapter` and real pause/resume state preservation.**
  Plan 05. The current `PodAdapter` no-ops pause/resume — the gateway
  patches `spec.desiredPhase` faithfully, but the Pod itself isn't
  snapshotted/restored.
- **E2B SDK compat tests + perf benchmarks.** Plan 06. The wire format
  is documented and unit-tested here; SDK-driven smoke arrives next.
- **`SandboxWarmPool` reconciler.** Plan 07.
- **mTLS between gateway and envd.** Pod-network isolation is the
  current trust boundary; mTLS is revisited in Plan 06 alongside the
  SDK conformance work.
- **`RuntimeClass`-driven dispatch.** Plan 05, alongside `GvisorAdapter`.

## Next plan

Plan 05 — `GvisorAdapter` in `sandbawks-node`: first real pause/resume
adapter, S3 snapshot streaming, lazy-restore optimizations.
