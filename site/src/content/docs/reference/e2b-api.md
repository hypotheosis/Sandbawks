---
title: E2B API
description: Reference for the HTTP API the Sandbawks gateway serves — authentication, the sandbox lifecycle routes including the v2 routes, the create request and sandbox response, envd routing and access tokens, file routes, and event streams.
slug: reference/e2b-api
head:
  - tag: title
    content: E2B API · Sandbawks docs
tableOfContents: true
---

The `sandbawks-api` gateway serves the E2B API that the official E2B SDKs use,
plus a few Sandbawks extensions. Point the SDKs at it with `E2B_API_URL` and
`E2B_API_KEY`. This page lists the routes and the fields Sandbawks reads and
returns.

## Authentication

Lifecycle, file helper, and event routes require an API key in the `X-API-Key`
header. The key must match a [`TeamAPIKey`](/reference/custom-resources/teamapikey/);
it selects the team namespace the request acts in. A request without a valid key
gets `401`. Creating a sandbox requires the `sandboxes:create` scope.

Routes that reach a sandbox's `envd` use a per-sandbox access token instead. See
[envd routes](#envd-routes).

## Sandbox lifecycle routes

| Method and path | SDK operation | Description |
|-----------------|---------------|-------------|
| `POST /v2/sandboxes` | `Sandbox.create` (E2B SDK 2.61 and later) | Create a sandbox. See [Create request](#create-request). |
| `POST /sandboxes` | `Sandbox.create` (older SDKs) | Same as `POST /v2/sandboxes`. |
| `GET /v2/sandboxes`, `GET /sandboxes` | `Sandbox.list` | List the team's sandboxes. |
| `GET /sandboxes/{id}` | `get_info`, `is_running` | Get one sandbox. |
| `DELETE /sandboxes/{id}` | `kill` | Terminate a sandbox. |
| `POST /sandboxes/{id}/kill` | — | Same as `DELETE /sandboxes/{id}`, kept for older clients. |
| `POST /sandboxes/{id}/pause` | `pause` | Pause a sandbox. |
| `POST /v2/sandboxes/{id}/connect` | `Sandbox.connect` (E2B SDK 2.61 and later) | Resume a paused sandbox, or return a running one. The request body is ignored. |
| `POST /sandboxes/{id}/connect` | `Sandbox.connect` (older SDKs) | Same as the v2 route. |
| `POST /sandboxes/{id}/timeout` | `set_timeout` | Set the sandbox's idle timeout and restart its idle window. |
| `POST /sandboxes/{id}/refreshes` | — | Same as `/timeout`, kept for older clients. |

`connect` waits up to five seconds for the sandbox to become ready. If it isn't
ready by then, the response contains the sandbox's handle anyway, and the first
`envd` call waits for it. See [The sandbox lifecycle](/explanation/sandbox-lifecycle/).

### Create request

| Field | Type | Description |
|-------|------|-------------|
| `templateID` | string | Name of a `SandboxTemplate` in the team's namespace. |
| `timeout` | integer | Idle timeout in seconds. Becomes `spec.timeoutSeconds`. |
| `metadata` | map[string]string | Opaque key/value pairs, returned on reads. |
| `envVars` | map[string]string | Environment variables for the sandbox. |
| `egressAllow` | array[string] | Sandbawks extension. Egress destinations to add to the template's allow-list. See [Allow sandbox egress](/how-to/allow-sandbox-egress/). |
| `restoreFrom` | string | Sandbawks extension, also accepted as `sourceSandboxID`. Creates the sandbox from another sandbox's saved state. See [Clone a sandbox](/how-to/clone-a-sandbox/). |

Sandboxes created through the API have `autoPause` set, so the idle timeout
pauses them. Their resources come from the template's `defaultResources`. If the
template has a `SandboxWarmPool` with a ready member, the gateway hands out that
member instead of creating a new sandbox. It currently does so even when the request
sets `restoreFrom` ([#6](https://github.com/hypotheosis/Sandbawks/issues/6)).

A create request is rejected with `400` and the code
`unsupported_persistence_mode` if the template resolves to the `Snapshot`
persistence mode on a Kata runtime class.

### Sandbox response

| Field | Type | Description |
|-------|------|-------------|
| `sandboxID` | string | The sandbox's ID: the `Sandbox` resource's name. |
| `templateID` | string | The template name. |
| `clientID` | string | Client identifier, for E2B SDK compatibility. |
| `alias` | string | Always null. |
| `metadata` | map[string]string | The metadata from the create request. |
| `startedAt` | string (RFC 3339) | When the sandbox was created. |
| `endAt` | string (RFC 3339) | When the sandbox reaches its idle timeout if it stays idle. |
| `envdVersion` | string | Version of the sandbox's `envd`. |
| `envdAccessToken` | string | Access token for `envd` routes. Returned by create, connect, and get, and omitted from list entries. |
| `state` | string | `running` or `paused`. |

## envd routes

The SDKs reach a sandbox's `envd` through the gateway, with Connect RPC
(`/filesystem.Filesystem/*`, `/process.Process/*`) and the `/files` route for file
content. The gateway selects the sandbox in one of two ways:

- **By host name**, when `api.sandboxDomain` is set: a request to
  `<port>-<sandbox-id>.<domain>` goes to that sandbox. This is the scheme the
  official SDKs use for hosted E2B.
- **By header**: the `E2b-Sandbox-Id` header names the sandbox. Use this when
  there's no wildcard domain, for example with `localhost` or a port-forward.

Every `envd` request must carry the sandbox's `envdAccessToken` in the
`X-Access-Token` header, or, for file URLs, a signature made with it. The SDKs do
this automatically. A request without a valid token is refused.

| Path | Description |
|------|-------------|
| `/filesystem.Filesystem/{ListDir, Stat, MakeDir, Move, Remove}` | Filesystem operations. |
| `/process.Process/{Start, Connect}` | Start a process or reattach to one, as a stream. |
| `/process.Process/{List, SendSignal, SendInput, Update, CloseStdin}` | Manage processes. |
| `GET /files`, `POST /files` | Download or upload file content. Uploads are limited to 64 MiB. |
| `GET /health` | `envd` health. |

Long-running process streams aren't subject to the gateway's request timeout. For
how these routes behave while a sandbox is still starting, see
[The sandbox lifecycle](/explanation/sandbox-lifecycle/).

## File helper routes

Sandbawks also serves file routes that use the team API key instead of an `envd`
token.

| Method and path | Description |
|-----------------|-------------|
| `POST /files/mint` | Mint a short-lived signed URL for uploading or downloading one file. The body is `{sandbox_id, path, method, ttl}`. |
| `PUT /files/upload?token=...` | Upload file content with a minted token. No API key needed. |
| `GET /files/download?token=...` | Download file content with a minted token. No API key needed. |
| `GET /files/{sandbox_id}/list?path=&depth=` | List a directory. |
| `GET /files/{sandbox_id}/stat?path=` | Get file information. |
| `DELETE /files/{sandbox_id}/remove?path=` | Remove a file or directory. |
| `POST /files/{sandbox_id}/move` | Move a file. The body is `{from, to}`. |
| `POST /files/{sandbox_id}/makedir` | Create a directory. The body is `{path}`. |
| `GET /files/{sandbox_id}/watch?path=&recursive=` | Stream filesystem changes as server-sent events. |

## Event streams

| Method and path | Description |
|-----------------|-------------|
| `GET /events/sandboxes` | Stream Kubernetes Events for all of the team's sandboxes, as server-sent events. |
| `GET /events/sandboxes/{id}` | Stream Events for one sandbox. |

Each event is a `data:` frame with the JSON object
`{type, reason, message, sandboxID, timestamp}`. The streams send a keepalive every
15 seconds. The reasons are listed in the [Events reference](/reference/events/).

## Health

`GET /healthz` and `GET /readyz` are the gateway's liveness and readiness probes.
They don't require authentication.

## See also

- [Point your agent at Sandbawks](/tutorials/point-your-agent-at-sandbawks/) —
  use the E2B SDK against Sandbawks.
- [Helm chart reference](/reference/helm-chart/) — `api.sandboxDomain` and the
  Ingress values.
