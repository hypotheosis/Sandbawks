# 2026-05-26 — Issue #170: real envd `/__state` probe in envd + node

## Scope

Reverses the T15 stub commit `1d18716` and builds the feature the way
the Plan 05 spec §6.1 intended:

1. envd serves `GET /__state` on a dedicated HTTP listener (default
   `0.0.0.0:49984`). The route returns the live `EnvdState` JSON
   (processes / ptys / watches registries + schema version). The gRPC
   listener stays on `:49983` — second-listener over h2c sniffing per
   the issue's design call ("the chart already mounts ports per
   container").
2. `sandbawks_node::gvisor::ReqwestEnvdState::fetch_state` is back to a
   real `reqwest` GET; the placeholder `b"{}"` return and the
   `#[allow(dead_code)]` on the inner client are both gone. The probe
   targets the new HTTP port (49984), not the gRPC port (49983).
3. `PodAdapter` declares both `containerPort`s on the envd container
   (`envd` 49983 + `envd-state` 49984) so kubelet sees the new
   surface.
4. `GvisorAdapter`'s resume-side TCP-bind wait now defaults to the
   gRPC port (`ENVD_GRPC_PORT`) — that's "envd's primary RPC surface
   is accepting again", which is the readiness condition the
   controller cares about. The old (misnamed) `ENVD_HTTP_PORT` is now
   what its name claims: the HTTP probe port.

## Files touched

| File | Change |
|------|--------|
| `crates/envd/Cargo.toml` | Add `axum` dep + `tokio/net` feature for the HTTP listener |
| `crates/envd/src/bin/envd.rs` | New `--http-listen` flag, `axum` server for `GET /__state`, broadcast-channel fanout so SIGTERM drains both servers |
| `crates/envd/tests/binary_smoke.rs` | Pass `--http-listen`; raw-TCP `GET /__state` probe + parse-back-into-EnvdState assertion |
| `crates/envd/Dockerfile` | `EXPOSE 49984` alongside `49983` |
| `crates/sandbawks-node/src/gvisor.rs` | Revert stub; rename `ENVD_HTTP_PORT=49983` → split `ENVD_GRPC_PORT=49983` + `ENVD_HTTP_PORT=49984`; drop `#[allow(dead_code)]` |
| `crates/sandbawks-controller/src/pod_adapter.rs` | Second `containerPort` for `envd-state` (49984) |
| `crates/sandbawks-controller/tests/pod_adapter.rs` | Assert both ports are declared |

## What we did *not* do (deferred)

The issue lists four pieces of work; this PR covers (1) and (2) end to
end. Two remain as follow-ups, both because they're scoped to the
runtime-orchestration side of the boundary rather than envd itself:

- **Point 3** — staging `envd-state.json` from the snapshot bundle
  into the resumed Pod's filesystem at a path the freshly-booting
  envd reads on startup. The envd binary already accepts
  `--state-file` (added in Plan 03 T7), so the binary side is done;
  what's missing is the gVisor adapter's resume-side write to a
  host-path that's projected back into the resumed Pod. `runsc
  restore` restores the *process* from memory anyway, so this is
  belt-and-braces for the future case where envd restarts fresh post
  resume — file a follow-up if/when that matters for a real user
  flow.

- **Point 4** — a smoke that proves envd-side state survives pause /
  resume (e.g. open a long-lived `Process.Connect` stream, pause,
  resume, assert the stream is still alive). This is gated by point
  3 and by the Plan 05 wiring that bridges the live `ProcessService`
  registry into `SnapshotState` (currently a Plan 05 TODO — services
  start fresh on every boot, so even with a live `/__state` route
  the body is mostly empty today). Worth filing a follow-up issue
  once the registries are wired live.

## Validation

- `just fmt` — clean
- `cargo clippy --workspace --all-targets -- -D warnings` — clean
- `just test` — full workspace pass (200+ tests)
- `envd_boots_handles_sigterm_and_writes_snapshot` (binary smoke) now
  also exercises the HTTP `/__state` route: spawn envd, hit both
  ports, parse `EnvdState` back from the body, then drain on
  SIGTERM. Confirmed running locally.

### Follow-up fix: gVisor checkpoint vs. probe TCP state machine

The first push of this PR ran into a CI regression in the
pause-resume smoke. `runsc checkpoint` refuses to save a sandbox
whose TCP stack has any endpoint in a non-CLOSED state ("endpoint
cannot be saved in connected state"), and our new HTTP probe was
leaving the per-connection endpoint behind on envd's side. Pre-#173
the probe was stubbed (no network I/O), so this never surfaced.

#### The TCP-state-machine trap

Two iterations were needed:

1. **First attempt** — make the node the active TCP closer
   (`pool_max_idle_per_host(0)` on reqwest + `Connection: close`
   header). CI failure: the sandbox-side endpoint ended up in
   **TIME_WAIT** instead of ESTABLISHED. `Connection: close` makes
   the *server* (envd, inside the sandbox) the active closer, and
   gVisor's TIME_WAIT lasts 2 × MSL ≈ 60 s — well past the
   checkpoint window.
2. **Root cause** — *any* FIN-based close leaves one side in
   TIME_WAIT for ~60 s. There's no permutation of HTTP/1.1 headers
   that puts both endpoints straight into CLOSED via FIN.

#### The fix that works: TCP RST

The only way to leave the sandbox-side endpoint in `CLOSED`
immediately is to **RST** the connection from the node side
(SO_LINGER(0) → close() sends RST → server endpoint goes straight
to CLOSED, no TIME_WAIT).

Since neither `reqwest` nor `hyper`'s default `TcpConnector` expose
`SO_LINGER`, the probe is now a hand-rolled HTTP/1.1 client over
`tokio::net::TcpStream`:

- New struct `HttpEnvdProbe` (replaces `ReqwestEnvdState`). Issues a
  one-shot `GET /__state HTTP/1.1` directly over a TCP stream;
  parses the response by hand (status line + Content-Length + body);
  drops the stream to trigger the RST close.
- `socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))`
  is the load-bearing call. std's `set_linger` is still unstable
  (rust-lang/rust#88494) and tokio's is deprecated, so socket2 is
  the only stable path. Added as a workspace dependency.
- `reqwest` is gone from `sandbawks-node`'s `Cargo.toml`.
- `GvisorAdapter::pause` keeps a 250 ms post-probe wait
  (`POST_PROBE_TCP_DRAIN`, down from 500 ms) to cover the RST
  in-flight window plus scheduling jitter on a CPU-saturated CI
  runner. Overridable via `with_post_probe_drain(Duration::ZERO)`
  for unit tests that use `FakeEnvdState` (no real TCP).
- Binary entrypoint updated: `HttpEnvdProbe::new()` replaces the
  prior `ReqwestEnvdState::with_default_client()`.

The longer-term cleaner design (envd writes state to a file in its
own filesystem; node reads it via containerd's overlayfs upperdir,
no network at all) is noted in the type-level docs but deferred —
that's a Plan 06+ scope change.

## Why the body is still mostly empty today

The `SnapshotState` the route serves is populated only at boot
(loaded from `--state-file`). The live `FilesystemService` /
`ProcessService` don't currently mutate it during operation — that
wiring is a Plan 05 TODO and Plan 03 T6 explicitly defers it. So a
freshly-booted Pod's `/__state` returns `{"version":1,"processes":[],
"ptys":[],"watches":[]}`. That's still strictly better than the
placeholder `b"{}"` it replaces:

- It's version-stamped, so a future schema bump fails loudly on
  restore instead of silently parsing as defaults.
- The shape is the canonical `EnvdState` — same struct that
  round-trips through `--state-file`, same struct the restore-side
  decoder parses.
- It picks up live state automatically the moment the service-side
  wiring lands; no second code change needed in the node DaemonSet.

## Why a second listener and not h2c sniffing

The issue called the choice ("Second listener is simpler"); confirmed
in implementation:

- One axum `Router::with_state` + one `tokio::net::TcpListener::bind`
  is ~20 lines, including shutdown coordination. h2c sniffing would
  require wrapping the gRPC listener with `tower::ServiceExt`
  branching and is more invasive in `tonic::Server`'s builder.
- Pod-local: the new port only matters to the per-node DaemonSet,
  which talks pod-IP-direct. No Service surface change needed.
- Cleanly separates the "this is a casual `curl` debugging surface"
  from the "this is tonic with HTTP/2 framing" interfaces.
