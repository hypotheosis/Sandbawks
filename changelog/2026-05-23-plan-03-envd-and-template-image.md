# 2026-05-23 — Plan 03: `envd-rs` in-sandbox agent + reference template image

## What landed

- New crate `crates/envd`: the in-sandbox gRPC agent that future sandbox
  Pods will run as a sidecar / PID-1. Listens on port 49983 (per spec §4.2)
  and implements the three E2B-compatible services described below.
- Vendored upstream protos under `crates/envd/proto/{filesystem,process}/`
  pinned byte-identical to E2B's `envd` repo (commit hash and licence
  notice in `proto/NOTICE`). `tonic-build` generates the Rust bindings at
  build time; we never edit the proto files in-tree.
- `FilesystemService` — metadata-only `Stat`, `MakeDir`, `Move`,
  `ListDir`, `Remove` plus the `WatchDir` / event-stream trio. Every
  request path is canonicalised through a single `resolve()` guard that
  rejects `../etc/passwd`-style escapes regardless of intermediate
  symlinks. File *content* is deliberately not on gRPC — it ships in
  Plan 04 via `sandbawks-api`'s presigned-URL proxy.
- `ProcessService` — non-PTY exec surface (`Start`, `Connect`,
  `SendInput`, `SendSignal`, `StreamInput`, `CloseStdin`, `List`,
  `Update`). Uses a `tokio::sync::broadcast` channel per child to
  fan-out stdout/stderr, with a small post-exit retention window so
  late `Connect` calls still observe `EndEvent`. Race-freedom argument
  written down in the module docs.
- `PtyService` — PTY allocation via `portable-pty`, resize, signal,
  and bidirectional stream over the same `Connect` shape `ProcessService`
  uses. Keeps the SDK surface uniform.
- `state` module — deterministic `EnvdSnapshot` (BTreeMap-keyed env, ordered
  process list) so the future runtime checkpoint/restore path (Plan 05)
  has a stable serialisation contract to round-trip across pause/resume.
- `crates/envd/src/main.rs` — binary entrypoint that wires the three
  services into a `tonic` server, honours `ENVD_LISTEN`, and shuts down
  cleanly on `SIGINT`/`SIGTERM`.
- Full-lifecycle integration test (`crates/envd/tests/lifecycle.rs`):
  launches the binary, drives filesystem + process + PTY RPCs against
  the live gRPC surface, asserts the resulting state snapshot.
- `crates/envd/Dockerfile` — distroless multi-stage build producing a
  ~static image small enough for the reference template; corresponding
  `just envd-image-build` / `envd-image-run` recipes.
- `.forgejo/workflows/envd-image.yml` — CI workflow building and
  pushing `ghcr.io/hypotheosis/envd:{sha,latest}` to Harbor on `main`.
- `deploy/helm/sandbawks/values.yaml` — `images.envd` block defaulted
  to the published Harbor tag so consumers of the chart pick up envd
  without further config.
- `cargo doc --workspace --no-deps` is now warning-free across envd's
  module docs (fixed in this PR as part of the green-gate sweep).

## What's deliberately not here

- Controller-driven pod materialisation — `sandbawks-controller` still
  drives the state machine against a fake `RuntimeAdapter`. Pod creation,
  the envd sidecar wiring, and pod-readiness watcher land alongside the
  first real adapter (Plan 05 / `GvisorAdapter`).
- The `sandbawks-api` HTTP/WS gateway — Plan 04. Until that lands there
  is no externally-reachable surface; envd only listens inside the
  sandbox network namespace.
- Real `RuntimeAdapter` implementations (`GvisorAdapter`, `KataFcAdapter`,
  `RuncCriuAdapter`) — Plans 05, 08, 09.
- File-content read/write — deferred to Plan 04's presigned-URL proxy,
  per spec §14 sub-plan 4. envd intentionally does not carry bytes over
  gRPC.
- `SandboxWarmPool` reconciler and snapshot-cache hostPath — Plan 07.
- Snapshot upload / download to S3 — part of `GvisorAdapter` in Plan 05.
- TLS on the envd listener — the listener stays plain h2 on the loopback
  / pod network because `sandbawks-api` (Plan 04) terminates TLS at the
  edge and proxies internally. Revisit if we ever expose envd directly.

## Next plan

`Plan 04 — sandbawks-api (E2B-compatible HTTP/WS gateway)` — see
`docs/superpowers/specs/2026-05-21-sandbawks-mvp-design.md` §14 sub-plan 4
for scope: lifecycle endpoints (create / list / kill / pause / connect)
translated to CRD operations, SSE events, and the file upload / download
HMAC-signed-URL proxy that streams bytes into envd's filesystem on the
server side.
