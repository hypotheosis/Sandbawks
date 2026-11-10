# envd traffic reaches sandboxes in tenant namespaces (issue #13)

Date: 2026-11-10T21:09:30Z

## Problem

Since multi-namespace tenancy, a `TeamAPIKey` puts its team's sandboxes in
`spec.namespace`, but the gateway's per-sandbox data plane only looked in
`api.defaultNamespace`:

- the envd proxy's cache watched the default namespace, and a cache miss fell
  back to a `get` there, so files, commands and PTY for a sandbox in any other
  namespace answered `sandbox_not_found`;
- signed `/files/upload` and `/files/download` URLs resolved in the default
  namespace, and `/files/mint` took no team, so any key could mint a URL for
  any default-namespace sandbox id;
- `note_activity` stamped `lastActivityTimestamp` in the default namespace,
  so the idle timeout would pause a busy tenant sandbox (the exec route had the
  same bug).

## Fix

Sandbox names are only unique within a namespace, so the namespace is now
part of every per-sandbox credential and lookup.

- **envd access token** (`envd_auth`): now `<namespace>.<mac>`, with the MAC
  over `(namespace, id)` under a new `v2` domain. `verify_access_token`
  returns the namespace the token authorises before any lookup, so the
  "401 before lookup" property holds. A relabelled namespace fails the MAC.
  Legacy namespace-less tokens are still accepted, but only for the default
  namespace, which is all they could reach before. That keeps already-connected
  clients working without letting an old token reach another namespace.
- **envd cache** (`envd_cache`): watches `Api::all`, keyed
  `id -> namespace -> entry`. `lookup`, `remove` and `wait_until_ready` take
  the namespace, and `namespaces_of(id)` lists candidates.
- **proxy** (`routes/envd_proxy`): `authorize` returns the namespace, and the
  resolved `SandboxRef { namespace, id }` drives the lookup, park, activity
  stamp and stream activity. A `/files` signature, which carries no
  namespace, is checked against each cached namespace's token and then the
  legacy default-namespace token.
- **signed file URLs** (`signing`, `routes/files`): `SignedUrlPayload` gains
  an optional `namespace`. `/files/mint` sets it to the caller's team
  namespace, and upload/download resolve there. Tokens minted before the change
  fall back to the default namespace and expire within the mint TTL.
- **activity** (`activity`, `routes/exec`): `note_activity(namespace, id)`,
  with the throttle keyed by `namespace/id`.
- **DTO** (`routes/sandboxes`): `envdAccessToken` is issued for the sandbox's
  `metadata.namespace` (falling back to the team's).

No RBAC or chart change: the gateway's ClusterRole already grants
cluster-wide `sandboxes: get/list/watch` and `sandboxes/status: patch`.

## Tests

- New unit tests: namespaced tokens (stable, per-namespace,
  relabel-proof, unambiguous boundary), legacy-token scoping, and the cache
  keeping same-named sandboxes in two namespaces apart.
- New integration tests: a tenant token resolves in its namespace, a token
  never reaches the same id elsewhere, relabelled token → 401, a legacy token
  is default-namespace only, a signed URL resolves its namespace from the
  cache, `/files/mint` binds the caller's namespace, and signing-payload
  round-trip and back-compat.
- Existing signature tests and the real-SDK vectors now use the legacy token
  explicitly. The SDK treats the token as opaque, so the vectors still pin
  the signature format.
