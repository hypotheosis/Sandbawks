# E2B API reference

Date: 2026-11-10T20:53:50Z

Added `site/src/content/docs/reference/e2b-api.md` and its sidebar entry. It documents:

- `X-API-Key` authentication and the `sandboxes:create` scope;
- every lifecycle route, including the v2 create/list/connect routes and the
  back-compat aliases;
- the create request (with the `egressAllow` and `restoreFrom`/`sourceSandboxID`
  extensions) and the sandbox response;
- envd routing by host name or `E2b-Sandbox-Id`, with `X-Access-Token`;
- the file helper routes, the SSE event streams and the health probes.
