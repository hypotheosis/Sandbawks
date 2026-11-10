# IPv6-capable bind defaults (`0.0.0.0` -> `[::]`)

**Date:** 2026-06-02T00:36:53Z

## Summary

Switched every server listen default in the repo from the IPv4 wildcard
`0.0.0.0` to the IPv6 wildcard `[::]` so all of our software is IPv6
capable out of the box. On Linux, where `net.ipv6.bindv6only=0` is the
default, `[::]` accepts both native IPv6 connections and IPv4-mapped
connections, so a single bind serves dual-stack, IPv6-primary, and
IPv4-only clients alike. `0.0.0.0` answers only IPv4.

## Motivation

On a dual-stack / IPv6-primary Kubernetes cluster the Pod IP that kubelet
uses for liveness/readiness probes is IPv6. `sandbawks-api` defaulted to
`0.0.0.0:8080` (IPv4 only), so the probe to
`http://[<pod-ipv6>]:8080/healthz` got `connection refused`, failing both
probes and driving the `sandbawks-api` Deployment into
`CrashLoopBackOff`. Binding `[::]` resolves this and future-proofs the
other listeners against the same class of failure.

## Changes

| File | Was | Now |
| --- | --- | --- |
| `crates/sandbawks-api/src/bin/sandbawks-api.rs` | `default_value = "0.0.0.0:8080"` | `default_value = "[::]:8080"` |
| `crates/envd/src/bin/envd.rs` (`DEFAULT_LISTEN`, gRPC) | `"0.0.0.0:49983"` | `"[::]:49983"` |
| `crates/envd/src/bin/envd.rs` (`DEFAULT_HTTP_LISTEN`, `/__state`) | `"0.0.0.0:49984"` | `"[::]:49984"` |
| `deploy/helm/sandbawks/templates/api-deployment.yaml` (`SANDBAWKS_API_LISTEN`) | `"0.0.0.0:8080"` | `"[::]:8080"` |

Doc comments and the module-level `//!` doc on `envd` were updated to
describe the `[::]` dual-stack semantics. The `clap` `--help` text for
`envd --listen` and `sandbawks-api --listen` now documents the IPv6
default and points loopback test usage at `[::1]` / `127.0.0.1`.

## Out of scope (intentionally unchanged)

- **Client connect targets** (e.g. `curl http://localhost:8080/readyz`
  in CI port-forward checks): loopback on the client side is correct.
- **Kind cluster `apiServerAddress: "0.0.0.0"`** in
  `.forgejo/workflows/kind-smoke.yaml`: that is the kind/Kubernetes API
  server's own bind for dind reachability, not our application.
- **Test scaffolding binds** to `127.0.0.1:0` and mock `pod_ip:
  "0.0.0.0"` fixtures in `sandbawks-node` tests: ephemeral-port test
  harness and inert mock data, not production listeners.

## Verification

- `cargo fmt --check` — clean.
- `cargo clippy -p envd -p sandbawks-api --bins -- -D warnings` — clean.
- The three new default strings parse as valid IPv6 `SocketAddr`s
  (`[::]:8080`, `[::]:49983`, `[::]:49984`; all `is_ipv6() == true`).
- `cargo test -p envd` — all suites pass (includes socket-binding
  binary-smoke tests).

## Bundled fix: `sandbawks-api` test compile

A pre-existing, independent breakage surfaced while validating the gate:
`cargo test -p sandbawks-api` failed to compile because the
`envd_cache.rs` TTL tests use `#[tokio::test(start_paused = true)]`,
which needs tokio's `test-util` feature. The workspace tokio omits that
feature (production builds stay lean), and `sandbawks-api` had no tokio
entry in `[dev-dependencies]` to add it.

Fixed by adding `tokio = { workspace = true, features = ["test-util"] }`
to `crates/sandbawks-api/Cargo.toml` `[dev-dependencies]`. Cargo unions
features, so `test-util` is enabled for test builds only; the production
dependency surface is unchanged. `cargo test -p sandbawks-api` now
passes (35 lib tests + integration suites, 0 failures).

## Operator note

Existing clusters already running the old image can recover without a
rebuild by setting `SANDBAWKS_API_LISTEN=[::]:8080` on the Deployment;
the updated Helm chart now ships that value by default.
