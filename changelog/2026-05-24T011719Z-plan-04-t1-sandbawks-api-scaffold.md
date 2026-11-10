# Plan 04 T1 — `sandbawks-api` crate scaffold

**Date:** 2026-05-24T01:17:19Z
**Branch:** `feat-issue-100-api-scaffold`
**Issue:** #100
**Plan:** `docs/superpowers/plans/2026-05-23-sandbawks-04-api-gateway.md` § Task 1

## What changed

Landed the `crates/sandbawks-api/` workspace member — a compiling-but-mostly-empty axum gateway crate that serves `GET /healthz` and `GET /readyz` returning `"ok"`. Every later task in Plan 04 fills in one slice of this scaffold (auth in T3, error type + middleware in T2, per-route modules in T6–T12, envd client pool in T9, signing in T11).

The workspace root `Cargo.toml` picks up nine new shared dependencies that the gateway and any future HTTP-serving crate share: `axum 0.7` (with `ws` + `macros`), `tower 0.5`, `tower-http 0.6` (with `trace`/`request-id`/`timeout`/`cors`/`util`), `hmac 0.12`, `sha2 0.10`, `subtle 2.6`, `base64 0.22`, `mime 0.3`, and `chrono 0.4` (with `serde` + `clock`, `default-features = false`).

### Files created

- `crates/sandbawks-api/Cargo.toml` — workspace-pinned manifest. Path-deps on `sandbawks-types` and `envd`; pulls in axum, tower, tower-http, kube, k8s-openapi, tonic, prost, hmac, sha2, subtle, base64, mime, chrono, clap, and the standard tokio/serde/tracing/anyhow/thiserror stack. `[[bin]] sandbawks-api → src/bin/sandbawks-api.rs`.
- `crates/sandbawks-api/src/lib.rs` — module map (`auth`, `envd_client`, `error`, `middleware`, `routes`, `signing`, `state`), public re-exports of `AppError` / `AppResult` / `AppState`, and the `CRATE_NAME` constant the smoke test keys off.
- `crates/sandbawks-api/src/auth.rs` — one-line placeholder; T3 lands the real `TeamAPIKey` middleware.
- `crates/sandbawks-api/src/envd_client.rs` — one-line placeholder; T9 lands the envd client pool.
- `crates/sandbawks-api/src/error.rs` — temporary `pub type AppError = anyhow::Error;` + `AppResult<T>` alias so other modules + the binary can reference it. T2 replaces it with the real enum + `IntoResponse`.
- `crates/sandbawks-api/src/middleware.rs` — one-line placeholder; T2 lands the request-id / trace / timeout factory functions.
- `crates/sandbawks-api/src/routes/mod.rs` — declares `events`, `exec`, `files`, `sandboxes` submodules.
- `crates/sandbawks-api/src/routes/{events,exec,files,sandboxes}.rs` — one-line placeholders; T6–T12 fill them in.
- `crates/sandbawks-api/src/signing.rs` — one-line placeholder; T11 lands HMAC URL signing.
- `crates/sandbawks-api/src/state.rs` — minimal `AppState { kube: Client, default_namespace: Arc<str> }`. Per the plan, T9 grows it with `envd_clients` and T11 with `signing_key`.
- `crates/sandbawks-api/src/bin/sandbawks-api.rs` — tokio main that initialises tracing, builds an in-cluster kube client, reads `SANDBAWKS_DEFAULT_NAMESPACE` and `SANDBAWKS_API_LISTEN`, wires the two health routes onto `AppState`, and serves via `axum::serve` with SIGINT graceful shutdown.
- `crates/sandbawks-api/tests/smoke.rs` — single-assertion link test against `CRATE_NAME`, mirroring `crates/envd/tests/smoke.rs` and `crates/sandbawks-controller/tests/smoke.rs`.

### Files modified

- `Cargo.toml` — appends `"crates/sandbawks-api"` to `members` and adds the nine workspace deps listed above under a `# HTTP / WS gateway (Plan 04)` / `# Crypto for TeamAPIKey hashing + HMAC signed URLs (Plan 04)` / `# Misc (Plan 04)` block.

## Out of scope (covered by later tasks)

- Real `AppError` enum + `IntoResponse` → T2
- Request-id / trace / timeout middleware → T2
- TeamAPIKey auth middleware + `TeamContext` extractor → T3
- Pod materialization (`PodAdapter`) → T5
- Lifecycle / SSE events / exec WS / file routes → T6–T12
- Helm wiring → T13
- Dockerfile + image-build CI workflow → T14

## Validation

| Step | Status | Notes |
| --- | --- | --- |
| `cargo build -p sandbawks-api` | pass | clean build, no warnings |
| `cargo test -p sandbawks-api --tests` | pass | 1 test (`smoke::crate_name_is_correct`) |
| `cargo clippy -p sandbawks-api --all-targets -- -D warnings` | pass | no findings |
| `just fmt` | pass | `cargo fmt --all` clean |
| `just test` | pass | full workspace test suite green; no regressions in `sandbawks-types`, `sandbawks-controller`, or `envd` |
| `cargo clippy --workspace --all-targets -- -D warnings` | pass | full workspace lint green |

The binary is **not** runnable outside a cluster — `Client::try_default()` fails without a kubeconfig or in-cluster service account token. T15's kind smoke runs it for real once the Helm wiring (T13) and the image build (T14) land.

## Next steps

Task 2 lands `AppError` + the request-id / tracing / timeout middleware factories, replacing the `pub type AppError = anyhow::Error;` stand-in.
