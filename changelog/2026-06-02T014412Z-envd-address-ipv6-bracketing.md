# Make envd host:port formatting IPv6-safe (controller + node)

## Symptom

The E2B Python SDK failed connecting to sandboxes on IPv6 clusters with a
misleading timeout:

```
e2b.exceptions.TimeoutException: invalid envd address
"fdcd:4a83:f883:5a::70ac:49983": invalid URI: This error is likely due to
sandbox timeout.
```

## Root cause

Multiple call sites joined a bare Pod IP to a port with `format!("{ip}:{port}")`.
That is correct for IPv4 but emits an unbracketed authority for IPv6 —
`fdcd:4a83:f883:5a::70ac:49983`. An unbracketed IPv6 literal is not a valid URI
authority (RFC 3986 requires `[addr]:port`), so it is rejected by URI parsers,
`SocketAddr`/`ToSocketAddrs`, and HTTP `Host` headers.

The first reported failure path: the controller stamps
`Sandbox.status.envdAddress`, the gateway dials it via
`Endpoint::from_shared(format!("http://{addr}"))`
(`crates/sandbawks-api/src/envd_client.rs`), the URI parse fails, we return
`envd_unavailable`, and the E2B SDK rewraps the persistent upstream failure as
a `TimeoutException`.

This only began surfacing after #255 (`bind IPv6 wildcard [::] by default`)
made sandbox Pods reachable on IPv6; pre-#255 clusters were IPv4-only so the
bare `host:port` form always parsed.

## Fix

Added a single shared helper `sandbawks_types::host_port` (in
`crates/sandbawks-types/src/net.rs`) that brackets IPv6 literals, generic over
the port's `Display` so both `u16` and `ContainerPort`'s `i32` callers use it
without a cast:

```rust
pub fn host_port<P: Display>(host: &str, port: P) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}
```

Every site passes a bare IP literal (never a scheme-bearing or already-bracketed
host), so a colon unambiguously identifies the IPv6 case.

Call sites routed through the helper:

- `sandbawks-controller` `PodAdapter::pod_ready` — `status.envdAddress`
  (the reported failure). Replaced a crate-local copy of the helper.
- `sandbawks-node` `HttpEnvdProbe::fetch_state` — the `runsc checkpoint`
  `/__state` probe. The bracketed authority is now used for both
  `TcpStream::connect` and the HTTP `Host` header, so `fetch_state_inner` no
  longer takes a separate `host_header` argument.
- `sandbawks-node` `wait_for_tcp_bind` — the post-`runsc restore`
  TCP-readiness poll.

### Consumer audit (`status.envdAddress`)

Bracketing at the source is safe for every consumer:

- `envd_client.rs` `http://{addr}` — the failing path; now valid.
- `envd_proxy.rs` `rsplit(':').next()` for the port — still returns the port
  from `[v6]:port`.
- `exec.rs`, `files.rs` — forward straight to `EnvdPool::channel`.
- `reconcile.rs`, `envd_cache.rs`, `sandboxes.rs`, `store.rs` — store / clone /
  compare opaquely; no host parsing.

No consumer extracts the host via `split(':').next()`.

## Tests

`sandbawks_types::net::tests` covers IPv4, IPv6 (reproducing the reported
authority byte-for-byte), and the signed-port-without-cast path, plus a doctest.

Validation: `cargo test -p sandbawks-types -p sandbawks-controller -p
sandbawks-node` (all pass), `cargo fmt --check`, and `cargo clippy -p
sandbawks-types -p sandbawks-controller -p sandbawks-node --lib -- -D warnings`
(clean).
