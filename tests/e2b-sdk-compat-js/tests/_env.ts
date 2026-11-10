// Shared environment + fixtures for the sandbawks E2B JS SDK
// compatibility harness. Mirrors `tests/e2b-sdk-compat/conftest.py`.
//
// The harness expects two pieces of environment, both defaulted so a
// fresh `SANDBAWKS_KIND_GVISOR=1 just cluster-up` works without any
// exports:
//
// * `E2B_API_URL` — the gateway base URL. Defaults to
//   `http://localhost:8080` because the workspace `cluster-up` recipe
//   arranges a background `kubectl port-forward svc/sandbawks-api
//   8080:8080`.
// * `E2B_API_KEY` / `SMOKE_API_KEY` — the team API key the gateway
//   authenticates against. Defaults to the literal
//   `sbk_smoke_smoke_smoke_smoke_smoke` used by
//   `deploy/kind/fixtures/api-smoke.sh`; the matching `TeamAPIKey` CR
//   is applied by that fixture, so the two stay in lockstep.
//
// Both values are written into `process.env` at module load — the
// JS SDK reads them at `Sandbox.create` / `ConnectionConfig`
// construction time, same eager-snapshot semantics as the Python SDK.

// Matches deploy/kind/fixtures/api-smoke.sh. NOT A SECRET — published
// in plain text in this repo; the matching TeamAPIKey CR is seeded by
// the api-smoke fixture.
export const DEFAULT_API_URL = "http://localhost:8080";
export const DEFAULT_API_KEY = "sbk_smoke_smoke_smoke_smoke_smoke";
export const DEFAULT_TEMPLATE_ID = "api-smoke-template";

// Populate env vars the SDK reads at construction time. Idempotent —
// each test file imports this module; the first import wins and
// subsequent imports leave any caller-provided overrides alone.
const apiUrl = process.env.E2B_API_URL ?? DEFAULT_API_URL;
const apiKey =
    process.env.E2B_API_KEY ??
    process.env.SMOKE_API_KEY ??
    DEFAULT_API_KEY;
process.env.E2B_API_URL = apiUrl;
process.env.E2B_API_KEY = apiKey;
// Issue #191: route per-sandbox envd RPCs through the gateway so the
// reverse proxy in `routes::envd_proxy` can demux on `E2b-Sandbox-Id`.
// Without this override the JS SDK builds an unreachable
// `https://49983-sbx-….e2b.app` URL for every `files.*` / `commands.*`
// call.
process.env.E2B_SANDBOX_URL = process.env.E2B_SANDBOX_URL ?? apiUrl;

export const gatewayUrl: string = apiUrl;
export const apiKeyValue: string = apiKey;
export const templateId: string =
    process.env.SANDBAWKS_TEMPLATE_ID ?? DEFAULT_TEMPLATE_ID;
