# Plan 04 T11 — File upload/download via HMAC-signed URLs

**Issue:** #110
**Plan reference:** `docs/superpowers/plans/2026-05-23-sandbawks-04-api-gateway.md` § Task 11
**Spec reference:** §7 (file upload/download)

## What landed

### envd: two additive `Filesystem` RPCs

The vendored E2B `filesystem.proto` previously carried metadata RPCs
only — file content was deliberately out of scope (see Plan 03). T11
adds two single-message RPCs on the same service so the gateway can
move bytes server-side after verifying a signed URL:

```proto
rpc WriteAll(WriteAllRequest) returns (WriteAllResponse);
rpc ReadAll(ReadAllRequest)   returns (ReadAllResponse);
```

- `crates/envd/proto/filesystem/filesystem.proto` — extension RPCs +
  messages, with a comment marking them as sandbawks-local.
- `crates/envd/proto/SOURCE` — the divergence is recorded per
  Apache-2.0 §4(b); upstream RPCs and field tags are unchanged, so
  the published E2B SDKs remain wire-compatible with the metadata
  surface.
- `crates/envd/proto/NOTICE` — updated to call out the deviation in
  the redistribution notice.
- `crates/envd/src/filesystem.rs` — `write_all` / `read_all` handlers
  routed through the existing `resolve()` chroot guard, so signed-URL
  callers can't `../` out of the sandbox root.
- `crates/envd/tests/filesystem.rs` — round-trip + path-traversal +
  missing-path tests.

### sandbawks-api: HMAC signing + three handlers

- `crates/sandbawks-api/src/signing.rs` (replacing the placeholder):
  `SigningKey`, `SignedUrlPayload`, `mint`, `verify`. HMAC-SHA256 over
  the JSON payload, base64url-no-pad token shape, constant-time tag
  compare via `subtle::ConstantTimeEq`, expiry against
  `chrono::Utc::now().timestamp()`. Verify distinguishes
  `AppError::BadRequest` (malformed token) from
  `AppError::Unauthorized` (bad signature or expired) so the gateway
  returns the right HTTP status.
- `crates/sandbawks-api/src/routes/files.rs` (replacing the
  placeholder): `POST /files/mint` (auth-gated, mints upload-or-
  download URLs with a TTL clamped to 1h), `PUT /files/upload?token=`,
  `GET /files/download?token=` (both authenticated solely by the
  signed token). Bodies are buffered into memory because the new envd
  RPCs are single-message — sufficient for MVP file sizes; chunking
  defers to Plan 07/09.
- `crates/sandbawks-api/src/state.rs` — `AppState` gains
  `signing_key: Arc<SigningKey>`, loaded from
  `SANDBAWKS_SIGNING_SECRET` in `from_kube` (panics if unset — Helm
  wires this in T13) and to a deterministic value in `for_test`.
- `crates/sandbawks-api/src/bin/sandbawks-api.rs` — merges
  `routes::files::router` into the main app.
- `crates/sandbawks-api/tests/signing.rs` — round-trip, expired,
  tampered-payload, tampered-tag, and malformed-token tests.

## How tokens flow

```
client                       gateway                      envd
  |    POST /files/mint        |                            |
  |  + X-API-Key, body={sbx,p,m,ttl}                        |
  | -------------------------> |                            |
  |  201 { url: "/files/upload?token=<b64body>.<b64tag>" }  |
  | <------------------------- |                            |
  |                            |                            |
  |    PUT /files/upload?token=...  (body = file bytes)     |
  | -------------------------> |   verify token, lookup     |
  |                            |   sandbox -> envd_address  |
  |                            | -------- WriteAll --------> |
  |                            | <-------- 204 -------------- |
  |  204                       |                            |
  | <------------------------- |                            |
```

## Verification

- `just fmt-check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` (workspace) — all suites pass, including the new five
  signing tests and the three envd `WriteAll` / `ReadAll` tests.

## What's left for later tasks

- T12: filesystem metadata pass-through routes (stat / remove / move /
  makedir / watch).
- T13: Helm template for the `SANDBAWKS_SIGNING_SECRET` Secret +
  Deployment env wiring.
- T15: integration test against a live kind cluster.
