# sandbawks — E2B JavaScript SDK compatibility harness

An npm-managed [vitest](https://vitest.dev) project that exercises
the official [`@e2b/code-interpreter`](https://www.npmjs.com/package/@e2b/code-interpreter)
JS SDK against a sandbawks gateway reachable via `E2B_API_URL`.
Implements **Plan 06 T5** of the sandbawks roadmap (`#187`, parent
epic `#182`).

The harness lives outside the Rust workspace on purpose — it is a
sibling test rig like `tests/e2b-sdk-compat/` (the Python harness)
and `tests/bench/` (the perf bench), not a Cargo workspace member.
Keeping it out of `Cargo.toml`'s `members` means `cargo test
--workspace` stays Rust-only and the Node.js toolchain does not need
to be present for the Rust gate to pass.

## Layout

```
tests/e2b-sdk-compat-js/
  package.json           # npm dependencies: @e2b/code-interpreter, vitest, ...
  tsconfig.json          # TS config (no emit; vitest handles transpilation)
  vitest.config.ts       # sequential, single-fork, junit-on-demand
  tests/
    _env.ts              # populates E2B_API_URL / E2B_API_KEY before tests
    lifecycle.test.ts    # create / list / pause / connect / kill
    commands.test.ts     # commands.run + streaming
    files.test.ts        # write / read / list / stat / remove / rename / makeDir
    events.test.ts       # SSE event subscription
    skipped.test.ts      # explicit skips for warm-pool / template-build deps
```

## Install

```bash
cd tests/e2b-sdk-compat-js
npm install
```

`npm install` materializes a hermetic `node_modules/` inside this
directory. Both `node_modules/` and the harness's local caches are
gitignored at the repo root.

## Run

The harness assumes a running sandbawks gateway. The supported
local setup is the gVisor-enabled kind cluster brought up by the
workspace `cluster-up` recipe:

```bash
SANDBAWKS_KIND_GVISOR=1 just cluster-up
```

That recipe also seeds the matching `SandboxTemplate` and
`TeamAPIKey` fixtures the harness expects (via
`deploy/kind/fixtures/api-smoke.sh`) and starts a background
`kubectl port-forward svc/sandbawks-api 8080:8080`, which the
harness's default `E2B_API_URL` (`http://localhost:8080`) targets.

Then, from the repository root:

```bash
just sdk-smoke-js
```

That recipe runs `npm install` (idempotent) and `npm test` inside
this directory. Set `E2B_API_URL` to point at a non-kind gateway:

```bash
E2B_API_URL=https://sandbawks.example.com \
E2B_API_KEY=sbk_yourkey \
just sdk-smoke-js
```

## CI

The harness is exercised on every PR by the `kind-smoke` job in
`.github/workflows/kind-smoke.yaml`, immediately after the Python
`SDK compat smoke` step on the same cluster — so a single failure
pattern (regression in the gateway's E2B-compat surface) shows up
against both SDKs in CI without bringing up a second cluster.

The flow:

1. The job's existing pre-steps bring up the kind cluster, install
   the helm chart with `api.enabled=true` + `gvisor.enabled=true`,
   run the API smoke, the pause/resume smoke, then the Python SDK
   compat smoke. Those steps prove the gateway is healthy before
   the JS suite layers on top.
2. The `SDK compat smoke JS (Plan 06 T5)` step installs Node, runs
   `npm ci` against the harness's `package.json`, opens its own
   `kubectl port-forward svc/sandbawks-api 8080:8080` (scoped to the
   step so it doesn't collide with earlier port-forwards), and runs
   `npm test` with `JUNIT_OUTPUT_FILE` set so vitest emits a junit
   XML the workflow can publish as an artifact.
3. The `Upload SDK smoke (JS) junit results` step uploads
   `sdk-smoke-js-results.xml` as a workflow artifact named
   `sdk-smoke-js-results`, visible on the GitHub Actions run page.

The step is gated by `KIND_SMOKE_INCLUDE_SDK_JS` (defaulted to `'1'`
in the workflow's `env:` block). Flip it to anything else on a debug
branch to keep the rest of the smoke green while iterating on the
harness.

This task is filed as `prio/p2` — the §13 verification gate only
enumerates the Python `examples/`, so a JS-side regression should be
loud (visible junit failure) but does **not** block the v0.1.0 MVP
milestone if Node.js toolchain issues surface.

## Discovery-only validation

Vitest's `list` mode does not require a live gateway and is the
cheap smoke that the harness loads cleanly:

```bash
cd tests/e2b-sdk-compat-js
npm install
npm run test:collect
```

Local contributors should run this to confirm the harness imports
on their machine before bringing up the cluster.

## What's covered

Every endpoint listed in spec §7 that the official
`@e2b/code-interpreter` SDK actually drives:

| Spec endpoint | Test module | Notes |
|---|---|---|
| `POST /v2/sandboxes`, `GET /v2/sandboxes`, `GET /sandboxes/{id}`, `POST /sandboxes/{id}/timeout`, `GET /health`, `POST /sandboxes/{id}/kill`, `POST /sandboxes/{id}/pause`, `POST /v2/sandboxes/{id}/connect` | `lifecycle.test.ts` | covers `isRunning` / `getInfo` / `setTimeout` (landed by #189) plus the cold and warm lifecycle paths |
| Exec / code-run WebSocket | `commands.test.ts` | `commands.run`, `commands.run({ background: true })` streaming |
| `files.list`, `files.getInfo`, `files.remove`, `files.rename`, `files.makeDir`, `files.read`, `files.write` | `files.test.ts` | drives both metadata gRPC pass-through and the signed-URL upload/download path. `files.watchDir` deferred — see `skipped.test.ts` |
| `GET /events/sandboxes` (SSE) | `events.test.ts` | smoke that the SSE stream produces a phase transition |
| `pause → resume → exec`, `write → list → read → remove` | compound flows in `lifecycle.test.ts` / `files.test.ts` | exercise spec §13.2 acceptance |

## What's skipped (with TODO)

`skipped.test.ts` and module-level `describe.skip` carry the same
deferral matrix as the Python harness:

* `Sandbox.createSnapshot` / `Sandbox.deleteSnapshot` / `Sandbox.listSnapshots`
  — warm-pool / explicit snapshot management; **Plan 07**.
* `Template.build` / template build pipeline — out of scope for v0.1;
  tracked as a future plan.
* `Sandbox.getMetrics` — Prometheus surface lands with spec §11.
* `files.watchDir` — SDK contract (callback vs async iterable)
  needs observation against a live gateway.
Each skip carries a one-line reason and a reference to the relevant
plan/epic so reviewers can grep for them once the underlying feature
ships.

## Out of scope (this task)

* Adding the JS test results to the §13 verification gate — that
  remains Python-only per spec.
* Wiring the JS suite into the perf bench from T3 — separate
  concern; the JS bench would be its own task if ever needed.
* Other JS SDK variants — `@e2b/code-interpreter` covers the MVP
  surface; older `@e2b/sdk` is not in scope.
