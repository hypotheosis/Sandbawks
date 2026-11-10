# E2B JS SDK live smoke (native hostname mode)

A clean-room smoke that drives the official `@e2b/code-interpreter` SDK
against a **live, deployed** sandbawks gateway using the way an unmodified
E2B application addresses a real deployment: **native hostname routing**
(`https://{port}-{sandbox_id}.{domain}`), configured with only
`E2B_API_KEY` + `E2B_DOMAIN`.

This is the production complement to `tests/e2b-sdk-compat-js`, which runs
against the local kind cluster in *header mode* (`E2B_SANDBOX_URL` pointed
at the gateway base). This harness deliberately does **not** set
`E2B_SANDBOX_URL` / `E2B_API_URL`, so it exercises the wildcard Ingress +
hostname-based sandbox selection (issue #364) end to end — the real proof
of 100% E2B API compatibility.

## What it does

Against a freshly created sandbox it runs, timing each step:

- `commands.run` for `echo hello`, `hostname`, `uname -a`
- a `files.write` + `files.read` round-trip
- a streaming command (`echo a; sleep 2; echo b; sleep 2; echo c`),
  printing per-chunk arrival times so live streaming is distinguishable
  from upstream buffering

then kills the sandbox and prints `passed=N failed=N`.

## Run

```bash
# from the repo root
E2B_API_KEY=<team key> just e2b-node-smoke
# override domain / template:
E2B_API_KEY=<team key> just e2b-node-smoke staging.example.com base
```

Or directly:

```bash
cd tests/e2b-node-smoke
bun install   # or: npm install
E2B_API_KEY=<team key> E2B_DOMAIN=e2b.example.com E2B_TEMPLATE=compound-sandbox \
  bun run smoke.ts
```

## Env

| var | required | default | note |
|-----|----------|---------|------|
| `E2B_API_KEY` | yes | — | team API key the gateway authenticates |
| `E2B_DOMAIN` | yes | — | wildcard base domain of the deployment |
| `E2B_TEMPLATE` | no | `base` | SandboxTemplate id to create from |
| `CMD_TIMEOUT_MS` | no | `15000` | per-command exec timeout |
| `REQUEST_TIMEOUT_MS` | no | `20000` | per-HTTP-request timeout |
| `KEEP` | no | — | `1` leaves the sandbox running (skips kill) |

Ensure `E2B_SANDBOX_URL` / `E2B_API_URL` are **not** exported in your
shell — either one overrides the SDK's native hostname routing and
defeats the purpose of this smoke. The `just e2b-node-smoke` recipe
unsets both for you.
