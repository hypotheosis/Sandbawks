# Plan 04 T9 — EnvdPool: pooled tonic clients keyed by `host:port`

Date: 2026-05-24T15:58:48Z
Issue: gfd/sandbawks#108 (Plan 04 epic #99)

## Scope

Introduces a shared pool of `tonic::transport::Channel`s on the API
gateway so the upcoming exec / file / metadata routes (T10–T12) can
talk to each sandbox's envd through a single multiplexed h2 connection
per `status.envdAddress`, rather than re-handshaking on every request.

## What changed

- `Cargo.toml` (workspace) — added `dashmap = "6"` under
  `[workspace.dependencies]` with a comment pointing back to T9.
- `crates/sandbawks-api/Cargo.toml` — pulled `dashmap` in via
  `{ workspace = true }`.
- `crates/sandbawks-api/src/envd_client.rs` — replaced the
  placeholder module with the real `EnvdPool`:
  - `EnvdPool { inner: Arc<DashMap<String, Channel>> }`,
    `#[derive(Clone, Default)]` so it's cheap to embed in `AppState`.
  - `EnvdPool::new()` — named constructor that mirrors the other
    `AppState`-field initializers.
  - `channel(&self, addr) -> Result<Channel, AppError>` — fetch-or-open
    with a fast cached path. Uses `Endpoint::from_shared("http://{addr}")`
    with a 5-second `connect_timeout` and a 30-second per-RPC
    `timeout`. Both transport-creation and connect failures map to
    `AppError::UpstreamUnavailable { code: "envd_unavailable", … }`
    (the same code the existing `From<tonic::Status>` impl uses for
    `Unavailable` so the SDK sees a single error class for "envd's
    not there"). Cold-start races are collapsed by inserting into the
    `DashMap` only after the handshake completes, then returning a
    clone of the (possibly winning) entry — channels are
    `Clone`-cheap so the bounded extra handshake during contention is
    not worth gating behind a per-addr mutex.
  - `filesystem(addr) -> FilesystemClient<Channel>` and
    `process(addr) -> ProcessClient<Channel>` convenience
    constructors that just wrap `channel(addr)` so route handlers
    don't have to spell the client constructor.
- `crates/sandbawks-api/src/state.rs` — added an
  `envd_pool: EnvdPool` field on `AppState`. Both `from_kube` and
  `for_test` initialize it via `EnvdPool::new()`.
- `crates/sandbawks-api/tests/envd_client.rs` — new integration
  test crate:
  - `channel_against_dead_port_yields_upstream_error` — asks the
    pool to connect to `127.0.0.1:1`, asserts the returned error is
    `AppError::UpstreamUnavailable { code: "envd_unavailable", … }`.
    Real round-trip coverage is deferred to T15's kind integration
    suite per the issue.

## Why `http://` (no TLS)

envd lives inside the sandbox Pod and the gateway talks to it across
the cluster's CNI. Mint-per-sandbox TLS would need controller
plumbing the project doesn't have today; the issue explicitly defers
that to Plan 06 if mTLS becomes part of the SDK-compat surface.

## Why insert-after-connect

`DashMap::entry().or_insert(channel)` is called only after the
handshake completes, so we never hold a shard write-lock across an
`await` point. The cost is a bounded number of redundant connects
during cold-start contention on the same address (clones are
discarded as soon as the loser sees the winner's entry), which is
strictly preferable to either:

1. Holding a shard guard across `.connect().await` — would deadlock
   `DashMap`.
2. Keying connections behind a `Mutex<HashMap<_, Channel>>` — would
   serialize every cold-start across all addresses.

## Validation

```
just fmt
cargo clippy --workspace --all-targets -- -D warnings
just test
```

All green. New test:

- Integration (`tests/envd_client.rs`):
  - `channel_against_dead_port_yields_upstream_error`
