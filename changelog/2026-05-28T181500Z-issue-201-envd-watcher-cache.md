# Issue #201 — replace `resolve_sandbox` apiserver polling with a Sandbox watcher

Surfaced in review of PR #199 (issue #191). The envd reverse proxy's
`resolve_sandbox` waited for `status.envdAddress` to populate by polling
the apiserver with a `get` every 500ms for up to 30s. The official E2B
SDKs fire `files.write` + `commands.run` (and more) in parallel
immediately after `Sandbox.create`, before the controller has reported
the address — so a single cold-start sandbox could generate up to
`N_concurrent * 60` apiserver `get`s before envd was ready.

## What changed

### New module: `crates/sandbawks-api/src/envd_cache.rs`

`EnvdCache` — a lock-free `sandbox_id -> CachedSandbox` map (`DashMap`,
matching the pattern already used by `EnvdPool`) plus a shared
`tokio::sync::Notify`, fed by a single `kube::runtime::watcher` over the
`Sandbox` resource scoped to the gateway's `default_namespace`.

- `CachedSandbox` is a tiny projection — only `desiredPhase` (for the
  pause gate) and `envdAddress` (the value callers wait for) — so
  read-path clones are cheap.
- `EnvdCache::spawn(api)` starts the background watcher task;
  `EnvdCache::new()` builds an empty, watcher-less cache for tests.
- `wait_until_ready(id, deadline)` parks waiters on the shared `Notify`.
  It registers interest via `Notified::enable()` *before* reading the
  map, closing the race where an `apply()` landing between the read and
  the await would otherwise leave a waiter asleep until the deadline.
- The watcher loop handles the full `Init` / `InitApply` / `InitDone`
  envelope: on each (re)connect it atomically reconciles the cache to
  the freshly-listed set (`replace_all`), dropping entries for sandboxes
  deleted while the watch was down so the map can't grow unbounded
  across reconnects. `Delete` events drop single entries; watcher errors
  are logged and skipped (the watcher reconnects internally).

### `resolve_sandbox` rewrite (`routes/envd_proxy.rs`)

The poll loop is gone. Resolution is now:

1. **Fast path** — cache hit: serve the cached projection with zero
   apiserver traffic.
2. **Cache miss** — issue exactly one `get`. This preserves the
   immediate `sandbox_not_found` 404 (the cache cannot distinguish
   "never created" from "watcher not yet synced") and seeds the cache so
   the wait observes the sandbox.
3. **Pending** — park on the cache's `Notify` until the next reconcile
   fills the address, with the same 30s `ENVD_READY_TIMEOUT` backstop.

All error semantics are preserved: `missing_sandbox_id` (400),
`sandbox_not_found` (404), `sandbox_paused` (403), `envd_not_ready`
(503). The pause gate (`spec.desiredPhase == Paused`) and the
`E2b-Sandbox-Port` mismatch warning were factored into a shared
`check_paused_then_address` helper used by both branches. The now-unused
`ENVD_READY_POLL_INTERVAL` constant was removed.

`AppState` gained an `envd_cache: EnvdCache` field — spawned in
`from_kube` (namespaced to `default_namespace`), empty in `for_test`.

## Apiserver-load impact

- Warm sandbox: **0** `get`s (was up to 60 per call).
- Cold-start sandbox, `N` concurrent SDK calls: **N** `get`s total then
  `N` waiters collapse onto one watch stream (was `N * 60`).

## Scope notes

- `routes::sandboxes::connect_sandbox` has the same poll shape and the
  issue flagged it as a candidate to share the cache. It was **left
  unchanged**: `connect_sandbox` is authenticated and operates in
  arbitrary per-team namespaces resolved from the caller's API key, while
  the cache is scoped to `default_namespace`. Folding it in would require
  a cluster-wide watch and broader RBAC, which is out of scope here (the
  issue marks the sharing as non-blocking).
- No RBAC change needed — the gateway ServiceAccount already holds
  `watch` on `sandboxes` (used by the existing events watcher).

## Validation

- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test --workspace --all-targets` — all clean (240 passed).
- New unit tests cover apply/lookup/remove, `replace_all` stale-entry
  eviction, and the four `wait_until_ready` outcomes including the
  park-then-wake race (`start_paused` time control keeps them instant).
- The watcher-against-real-apiserver path is exercised end-to-end by the
  existing SDK compat suite, which stands up a kind cluster.
