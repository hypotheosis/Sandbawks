# Plan 04 T6 — lifecycle CRUD endpoints

Date: 2026-05-24T15:03:50Z
Issue: gfd/sandbawks#105 (Plan 04 epic #99)

## Scope

Wires the first three E2B lifecycle endpoints into the gateway:

| Method | Path                    | Response                      |
|--------|-------------------------|-------------------------------|
| POST   | `/sandboxes`            | `201 Created` + `SandboxDTO`  |
| GET    | `/sandboxes`            | `200 OK` + `[SandboxDTO]`     |
| POST   | `/sandboxes/:id/kill`   | `204 No Content`              |

Pause + connect are reserved for T7 — their DTO types (`PauseResponse`,
`ConnectResponse`) ship here so the module's wire-shape surface lands
in one commit.

## What changed

- `crates/sandbawks-api/src/routes/sandboxes.rs` — replaced the
  placeholder with the full implementation: DTOs, a `SandboxOps` trait
  abstraction, a `KubeSandboxOps` impl backed by `kube::Api<Sandbox>`,
  a `FakeSandboxOps` impl backed by a `tokio::sync::Mutex<Vec<Sandbox>>`,
  helpers `body_to_sandbox` / `sandbox_to_dto` / `random_suffix`, three
  handlers, and the `router(state)` constructor.
- `crates/sandbawks-api/src/state.rs` — `AppState` now carries
  `key_store: Arc<dyn KeyStore>` and `sandbox_ops: Arc<dyn SandboxOps>`.
  Added `from_kube(client, namespace)` for the binary and `for_test`
  for integration tests (kept always-public to match the existing
  `FakeKeyStore` pattern and avoid the `--features test-util` dance
  for workspace-wide `cargo test`).
- `crates/sandbawks-api/Cargo.toml` — added the `test-util` feature
  flag (no-op for now, kept for forward-compat as the plan specifies).
- `crates/sandbawks-api/src/bin/sandbawks-api.rs` — merges the new
  sandboxes router and stacks the request-id / trace / timeout
  middleware on top. `/healthz` and `/readyz` stay outside the auth
  middleware so kubelet probes remain anonymous.
- `crates/sandbawks-api/tests/sandboxes.rs` — three integration tests
  covering happy-path create / list / kill against `FakeSandboxOps`.

## Wire-shape notes

E2B spells acronyms uppercase (`templateID`, `sandboxID`, `clientID`)
and serde's `rename_all = "camelCase"` produces lowercase-ID variants
(`templateId`, ...). Each affected field gets an explicit
`#[serde(rename = "...")]` rather than wrestling with a custom
rename strategy.

## Validation

```
just fmt
cargo clippy --workspace --all-targets -- -D warnings
just test
cargo test -p sandbawks-api --features test-util
cargo clippy -p sandbawks-api --all-features -- -D warnings
```

All green. New integration tests: `create_sandbox_returns_201_with_e2b_shape`,
`list_sandboxes_returns_array`, `kill_returns_204`.
