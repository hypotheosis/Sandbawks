# Issue #401 — envd OTel re-arm: `/__quiesce` + `/__rearm` HTTP endpoints

**Date:** 2026-06-08T08:53:04Z
**Issue:** #401 — envd OTel re-arm: `/__quiesce` + `/__rearm` endpoints in envd (parent: #356)
**Type:** fix (`kind/fix`, `area/envd`, `area/observability`, `runtime/gvisor`)

## Background

Issue #400 gave `sandbawks-observability` a `RearmHandle` that hot-swaps the
OpenTelemetry provider stack in a live process: `quiesce()` tears the stale
exporters down without blocking on a dead collector, `rearm()` rebuilds them
from the current environment. That handle was already constructed in envd's
`main` (issue #400 wiring) but bound as `_rearm` and never driven.

This issue exposes the handle to the node over HTTP so the node can drive the
quiesce/re-arm transitions around `runsc checkpoint`/`restore`.

## What changed

### `crates/sandbawks-observability/src/rearm.rs`

- `RearmHandle::disabled()` promoted from `pub(crate)` to `pub`. It is the
  natural counterpart to the already-public `is_disabled()`, and lets
  downstream binaries build a no-op handle for wiring tests without standing
  up a live SDK (or mutating the one-shot global subscriber). Documented as
  such.

### `crates/envd/src/bin/envd.rs`

- Two new `POST` routes on the **layer-free `:49984` HTTP listener** — the
  same `Router` as `/__state`, deliberately free of the gRPC trace/metrics
  `tower` layers so the rearm transitions keep working even when the gRPC
  surface is mid-trouble:
  - `POST /__quiesce` → `RearmHandle::quiesce(QUIESCE_SHUTDOWN_CAP)` → always
    `200 OK`. `quiesce` is infallible and panic-free (a hung exporter is
    *abandoned* after the cap, never joined), so the route has no `500` path.
  - `POST /__rearm` → `RearmHandle::rearm()` → `200 OK` on success, `500` on
    failure (the handle is left safely quiesced so a retry can re-arm cleanly).
- New `AppState { snapshot, rearm }` with `FromRef<AppState>` impls for
  `SnapshotHandle` and `RearmHandle`, so each handler extracts only the slice
  it needs (`State<SnapshotHandle>` / `State<RearmHandle>`). `GET /__state`
  stays a side-effect-free `GET`; quiesce is a separate `POST` so the
  destructive transition is never coupled to state capture or its retries.
- Router construction extracted into `http_router(snapshot, rearm)` so tests
  exercise the exact route table production serves (no drift).
- `run` now takes the `RearmHandle`; `main` passes the live handle through
  instead of dropping it as `_rearm`. `_telemetry` stays bound so the guard
  flushes on drop.
- New `QUIESCE_SHUTDOWN_CAP = 5s`: a deliberately short bound on the
  pre-checkpoint quiesce's stale-provider shutdown, so the node never stalls
  on a dead collector. (`DRAIN_TIMEOUT`'s 30s governs the SIGTERM RPC drain,
  a different concern.)
- Module + route doc comments updated to describe the new surface.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace green.
- New `#[tokio::test]` wiring tests in `envd.rs` drive the production router
  (built via `http_router`, wired to `RearmHandle::disabled()`) with
  `tower::ServiceExt::oneshot`:
  - `quiesce_route_returns_200` — `POST /__quiesce` → `200`.
  - `rearm_route_returns_200_on_success` — `POST /__rearm` → `200`.
  - `quiesce_route_rejects_get` — `GET /__quiesce` → `405`, proving the
    destructive transition is `POST`-only.

The exporter-state *flip* itself (quiesce stops emission, rearm restores it)
is proven against an in-memory exporter in `sandbawks-observability`'s `rearm`
tests (#400); this change owns the HTTP surface that drives those transitions.

## Out of scope (per the issue)

- gRPC-metrics recorder instrument rebuild (#356 Phase 3).
- Node-side orchestration that actually calls these routes (#356 Phase 4).
