# Issue #364 — E2B hostname-based sandbox selection

**Date:** 2026-06-10 12:30:16 UTC
**Branch:** `feat-issue-364-hostname-sandbox-selection`
**Type:** feat (`kind/feat`, `area/api`, `area/deploy`)

## Summary

The official E2B SDKs encode the target sandbox in the request **hostname**:
`{port}-{sandbox_id}.{team_domain}` resolves via wildcard DNS to a team-isolated
edge. The sandbawks gateway previously only resolved a sandbox from the
`E2b-Sandbox-Id` **header**, so any SDK pointed at a real wildcard domain could
not reach the right sandbox without the client manually injecting that header —
which is not E2B-SDK-compliant.

This change teaches the gateway to resolve the sandbox (and intended envd port)
from the request `Host` when a wildcard base domain is configured, falling back
to the existing header path otherwise. With no domain configured the behavior is
byte-for-byte unchanged.

## Behavior

- **Golden path (wildcard domain):** `Host: {port}-{sandbox_id}.{domain}`
  resolves the sandbox with no `E2b-Sandbox-Id` header required.
- **Header fallback:** a `Host` that is not a subdomain of the configured
  domain (e.g. `localhost:8080`) falls through to the `E2b-Sandbox-Id` header.
- **Precedence:** a parseable matching `Host` wins; otherwise the header. If
  neither yields an id, the existing `missing_sandbox_id` 400 is returned (its
  message now mentions both mechanisms).
- **Config-gated:** parsing only activates when `SANDBAWKS_SANDBOX_DOMAIN`
  (Helm `api.sandboxDomain`) is set; unset → header-only, no behavior change.
- **Port:** the `{port}` host label feeds the existing
  `warn_on_port_mismatch` diagnostic (non-authoritative, matching today's
  `E2b-Sandbox-Port` header handling). Per-port routing remains out of scope.

## Parsing rules (`parse_sandbox_authority`)

1. Strip any `:port` on the `Host` value itself.
2. Require the host to be a strict subdomain of the configured base domain
   (strip the `.{domain}` suffix; a bare apex is rejected).
3. Split the remaining label on the **first** `-` into `{port}` / `{sandbox_id}`
   — so sandbox ids that contain dashes survive intact.
4. Reject when the port segment is absent/non-numeric or the id is empty
   (→ fall through to the header).

## Changes

- `crates/sandbawks-api/src/routes/envd_proxy.rs`
  - New `resolve_target` (dual-source resolver) and pure
    `parse_sandbox_authority` parser.
  - `resolve_sandbox` now resolves via `resolve_target`; `check_paused_then_address`
    and `warn_on_port_mismatch` take a resolved `requested_port: Option<&str>`
    instead of re-reading the header.
  - Unit tests for the parser (golden path, host `:port` stripping, dashed ids,
    wrong-suffix, bare apex, malformed/non-numeric port, no-dash).
- `crates/sandbawks-api/src/state.rs` — new `sandbox_domain: Option<Arc<str>>`
  field + `with_sandbox_domain` builder; both constructors default to `None`.
- `crates/sandbawks-api/src/bin/sandbawks-api.rs` — new `SANDBAWKS_SANDBOX_DOMAIN`
  clap arg, threaded into `AppState`.
- `crates/sandbawks-api/tests/envd_proxy.rs` — integration tests: hostname
  resolves with no header (→404 lookup miss), non-matching host falls back to
  header, domain-set-but-no-identifier still returns 400.
- `deploy/helm/sandbawks/templates/api-deployment.yaml` — wire
  `SANDBAWKS_SANDBOX_DOMAIN` (only when `api.sandboxDomain` is set).
- `deploy/helm/sandbawks/values.yaml` — new `api.sandboxDomain`; documented
  `*.{domain}` Ingress host + wildcard-TLS note.

## Out of scope

- Per-port routing (the `{port}` label stays non-authoritative).
- Authn/authz changes (still cluster-internal network isolation + bare id).
- Wildcard TLS certificate issuance (operator/deploy concern).

## Validation

- `just fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `just test`.
- `helm template` of the chart with and without `api.sandboxDomain` set.

## Review follow-up (PR #461, review #201)

Addressed five reviewer comments (all "follow-up, non-blocking" sharp edges):

- **Case-insensitive + FQDN-robust parsing** (`parse_sandbox_authority`): the
  authority is now lowercased and a trailing FQDN dot trimmed before the
  domain-suffix match, so `3000-SBX.E2B.Example.com.` resolves. The extracted
  id is lowercased to stay aligned with the (always-lowercase) k8s CR name. The
  IPv6 comment was reworded to say the bracketed-literal case is *handled*
  (splits to `host = "["`, which can't match a real domain), not impossible.
- **Empty-domain hardening**: `parse_sandbox_authority` returns `None` for an
  empty `domain`, and the binary maps an empty/whitespace
  `SANDBAWKS_SANDBOX_DOMAIN` to `None` (matching the Helm `{{- with }}` check).
- **Conflict diagnostic** (`resolve_target`): when the Host parses but a
  disagreeing `E2b-Sandbox-Id` header is also present, a `tracing::warn!`
  surfaces it (Host still wins) — same spirit as `warn_on_port_mismatch`.
- **Precedence regression guard**: new `resolve_target_host_wins_over_disagreeing_header`
  unit test (at the resolver, since the integration 404 path can't distinguish
  which id was used without seeding a sandbox), plus parser unit tests for the
  case-insensitive, trailing-dot, and empty-domain cases.
