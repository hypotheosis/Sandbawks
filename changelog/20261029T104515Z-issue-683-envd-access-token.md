# Issue #683: secure every sandbox with an envd access token

## Problem

Sandboxes were created unsecured. The create answer had no
`envdAccessToken`, and the envd proxy (`routes::envd_proxy`) accepted a bare
sandbox id as the whole credential. An unauthenticated
`POST https://49983-<id>.<domain>/files?path=...` wrote the file (CLM-0064),
and the SDK refused expiring download URLs because there was no token to sign
with (CLM-0065). Sandbox ids are not secrets: they show up in logs and
hostnames and are partly time-ordered.

## Change

- New `sandbawks_api::envd_auth`:
  - `envd_access_token(key, sandbox_id)`: `HMAC-SHA256(SANDBAWKS_SIGNING_SECRET,
    "sandbawks/envd-access-token/v1\0" || id)`, base64url. It is derived, not
    stored, so there is no CRD change and no apiserver round-trip to verify,
    and it survives pause/resume, warm claims and gateway restarts.
  - `verify_file_signature`: the E2B v1 `/files` signature
    (`v1_` + base64(sha256(`path:op:user:token[:exp]`)), see the SDK's
    `e2b/sandbox/signature.py`), with the expiration enforced.
- `SandboxDTO.envdAccessToken`: set on create (cold and warm claim), connect
  (v1 and v2) and get. Left off list entries, as on hosted E2B.
- The envd proxy authorises every route (`authorize`, called from
  `classify_sandbox`) **before** the sandbox lookup. Every Connect-RPC route
  (unary and streaming `Start` / `Connect`) and `/health` need
  `X-Access-Token`. `/files` accepts the header or a valid
  `signature` / `signature_expiration`. If neither is valid, or the signature
  was stripped, has expired, or covers a different operation, the answer is
  `401` (`invalid_access_token` / `invalid_signature`).
- `pause-resume-smoke.sh` reads `envdAccessToken` from the create answer and
  sends it on its raw `/health` probe.

## Compatibility

The stock Python and JS SDKs already send `X-Access-Token` on every envd call
when the create / connect answer carries the token, so SDK clients need no
change. Raw HTTP clients of the proxy must now send the header. Rotating
`SANDBAWKS_SIGNING_SECRET` rotates every token, and clients then have to
`connect` again.

## Tests

- `envd_auth` unit tests check the signature against a copy of the SDK
  algorithm, with and without expiration and with an empty user, and check
  that it is bound to the sandbox, the operation and the expiration.
- `tests/envd_proxy.rs`: `401` with no token, with a wrong token, for an
  unsigned upload (the CLM-0064 repro), for a stripped or expired signature,
  for a read signature used to upload, and on streaming `process.Start`. A
  signed download passes auth.
- `tests/sandboxes.rs`: create, get and connect return the derived token, and
  list omits it.
