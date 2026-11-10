# Plan 04 T12 — Filesystem metadata pass-through

**Issue:** #111
**Plan reference:** `docs/superpowers/plans/2026-05-23-sandbawks-04-api-gateway.md` § Task 12
**Spec reference:** §7 (filesystem)

## What landed

Six new HTTP routes under `/files/:sandbox_id/...`, each mapped to one
envd `Filesystem` RPC and gated by `auth_middleware`. The content-byte
routes (`/files/mint`, `/files/upload`, `/files/download`) shipped in
T11 and are unchanged here.

| HTTP                                                       | Envd RPC                |
|------------------------------------------------------------|-------------------------|
| `GET    /files/:sandbox_id/list?path=&depth=`              | `Filesystem.ListDir`    |
| `GET    /files/:sandbox_id/stat?path=`                     | `Filesystem.Stat`       |
| `DELETE /files/:sandbox_id/remove?path=`                   | `Filesystem.Remove`     |
| `POST   /files/:sandbox_id/move {from, to}`                | `Filesystem.Move`       |
| `POST   /files/:sandbox_id/makedir {path}`                 | `Filesystem.MakeDir`    |
| `GET    /files/:sandbox_id/watch?path=&recursive=` (SSE)   | `Filesystem.WatchDir`   |

### sandbawks-api: handlers + DTOs + router wiring

- `crates/sandbawks-api/src/routes/files.rs` — six new handlers
  (`list_dir`, `stat`, `remove`, `move_path`, `make_dir`, `watch`)
  following a single shape: extract `TeamContext` + `:sandbox_id` +
  params, resolve the envd `host:port`, fetch a pooled
  `FilesystemClient`, single tonic call, project into a DTO. Errors
  fall through `From<tonic::Status> for AppError` so gRPC status codes
  map to the right HTTP envelope (NotFound → 404, InvalidArgument →
  400, Unavailable → 502).
- `entry_to_dto` projects envd's `EntryInfo` into a stable E2B-shaped
  `EntryDto` (camelCase wire fields, `Timestamp` → RFC 3339,
  `symlinkTarget` omitted when absent). `file_type_str` /
  `event_type_str` map the proto enums (transported as `i32`) onto the
  lowercase strings the E2B SDK consumes.
- The `watch` endpoint relays envd's server-streaming `WatchDir` as
  SSE: each `FilesystemEvent` becomes one `data: <json>\n\n` frame;
  envd's internal `StartEvent` / `KeepAlive` variants are filtered
  out. The SSE stream carries a 15s `KeepAlive` comment so it survives
  the same 60s idle-timeout `routes::events` is tuned for.
- `envd_addr_for` now takes the namespace as a parameter — T11's
  token-authed `upload` / `download` keep passing
  `state.default_namespace` (no `TeamContext` available on those
  paths), and the new T12 handlers pass `team.namespace` from the
  authenticated context.
- `router(state)` mounts all six new routes alongside `/files/mint`
  under the same `auth_middleware` layer.

### sandbawks-api: deps

- `crates/sandbawks-api/Cargo.toml` — adds an explicit dependency on
  `prost-types`. The generated envd code references `Timestamp` via
  `::prost_types::Timestamp`; the new
  `timestamp_to_rfc3339(Option<prost_types::Timestamp>)` helper needs
  to name that type in its signature, which only works when
  `prost-types` is in this crate's own dependency graph.

### sandbawks-api: tests

- `crates/sandbawks-api/tests/files.rs` (new) — seven auth-gate smoke
  tests covering every authed route (`list`, `stat`, `remove`, `move`,
  `makedir`, `watch`, `mint`). Each confirms an unauthenticated
  request returns 401, guarding against a copy/paste regression that
  drops a route off `auth_middleware`. End-to-end round-trips against
  a real envd are out of scope here — they land in T15.

## Why minimal tests

The issue spec called for "lightweight tests covering auth
requirement; round-trip against real envd is in T15". The auth-gate
shape mirrors `tests/events.rs` (also a router-only smoke pass) so the
patterns stay consistent and the test crate doesn't grow a tonic-mock
dependency that T15 is explicitly going to obsolete.

## Verification

- `just fmt` — applied, no diff.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace passes; the new `tests/files.rs` suite
  reports `7 passed; 0 failed`.

## What's left for later tasks

- T13: Helm template wiring (unaffected by T12).
- T15: integration test driving every metadata RPC + the watch SSE
  stream against a live kind cluster.
