# sandbawks — E2B Python SDK compatibility harness

A `uv`-managed pytest project that exercises the official
[`e2b`](https://pypi.org/project/e2b/) Python SDK against a sandbawks
gateway reachable via `E2B_API_URL`. Implements **Plan 06 T1** of the
sandbawks roadmap (`#183`, parent epic `#182`).

The harness lives outside the Rust workspace on purpose — it is a
sibling test rig like `deploy/kind/fixtures/`, not a Cargo workspace
member. Keeping it out of `Cargo.toml`'s `members` means
`cargo test --workspace` stays Rust-only and the Python toolchain
does not need to be present for the Rust gate to pass.

## Layout

```
tests/e2b-sdk-compat/
  pyproject.toml         # uv-managed dependencies: e2b, pytest, ...
  conftest.py            # autouse fixture populating E2B_API_URL / E2B_API_KEY
  tests/
    test_lifecycle.py    # create / pause / connect / kill
    test_commands.py     # commands.run + streaming
    test_files.py        # write / read / list / stat / remove / move / makedir
    test_events.py       # SSE event subscription
    test_skipped.py      # explicit skips for warm-pool / template-build deps
```

## Install

```bash
cd tests/e2b-sdk-compat
uv sync
```

`uv sync` materializes a hermetic `.venv/` inside this directory.
Both `.venv/` and `.uv/` are gitignored at the repo root.

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
just sdk-smoke
```

That recipe runs `uv sync` and `uv run pytest -v` inside this
directory. Set `E2B_API_URL` to point at a non-kind gateway:

```bash
E2B_API_URL=https://sandbawks.example.com \
E2B_API_KEY=sbk_yourkey \
just sdk-smoke
```

## CI

The harness is exercised on every PR by the `kind-smoke` job in
`.github/workflows/kind-smoke.yaml`. The flow:

1. The job's existing pre-steps bring up the kind cluster, install the
   helm chart with `api.enabled=true` + `gvisor.enabled=true`, run the
   API smoke, then run the pause/resume smoke. Those steps prove the
   gateway and the S3-backed snapshot path are healthy before the SDK
   suite layers on top.
2. The `SDK compat smoke (Plan 06 T2)` step opens its own
   `kubectl port-forward svc/sandbawks-api 8080:8080` (scoped to the
   step so it doesn't collide with the earlier port-forwards), then
   runs `just sdk-smoke --junitxml=…`.
3. The `Upload SDK smoke junit results` step uploads
   `sdk-smoke-results.xml` as a workflow artifact named
   `sdk-smoke-results`, visible on the GitHub Actions run page.

The step is gated by `KIND_SMOKE_INCLUDE_SDK` (defaulted to `'1'` in
the workflow's `env:` block). Flip it to anything else on a debug
branch to keep the rest of the smoke green while iterating on the
harness.

## Discovery-only validation

Pytest discovery does not require a live gateway and is the cheap
smoke that the harness loads cleanly:

```bash
cd tests/e2b-sdk-compat
uv sync
uv run pytest -v --collect-only
```

This is what CI in **T2** will run before the gateway is up, and
what local contributors should run to confirm the harness imports
on their machine.

## What's covered

Every endpoint listed in spec §7 that the official `e2b` SDK
actually drives:

| Spec endpoint | Test module | Notes |
|---|---|---|
| `POST /v2/sandboxes`, `GET /v2/sandboxes`, `GET /sandboxes/{id}`, `POST /sandboxes/{id}/timeout`, `GET /health`, `POST /sandboxes/{id}/kill`, `POST /sandboxes/{id}/pause`, `POST /v2/sandboxes/{id}/connect` | `test_lifecycle.py` | covers `is_running` / `get_info` / `set_timeout` (landed by #189) plus the cold and warm lifecycle paths |
| Exec / code-run WebSocket | `test_commands.py` | `commands.run`, `commands.run(background=True)` streaming |
| `files.list`, `files.stat`, `files.remove`, `files.move`, `files.make_dir`, `files.read`, `files.write` | `test_files.py` | drives both metadata gRPC pass-through and the signed-URL upload/download path. `files.watch_dir` deferred to T2 — see `test_skipped.py` |
| `GET /events/sandboxes` (SSE) | `test_events.py` | smoke that the SSE stream produces a phase transition |
| `pause → resume → exec`, `write → list → read → remove` | compound flows in `test_lifecycle.py` / `test_files.py` | exercise spec §13.2 acceptance |

## What's skipped (with TODO)

`test_skipped.py` carries `pytest.skip` markers for SDK methods
whose backing features are not yet implemented in sandbawks:

* `Sandbox.create_snapshot` / `Sandbox.delete_snapshot` / `Sandbox.list_snapshots`
  — warm-pool / explicit snapshot management; **Plan 07**.
* `Template.build` / template build pipeline — out of scope for v0.1;
  tracked as a future plan.
* `Sandbox.get_metrics` — Prometheus surface lands with spec §11.
* `files.watch_dir` — SDK contract (callback vs generator) needs
  observation against a live gateway; rewrite in **T2** (#184).

Each skip carries a one-line reason and a reference to the relevant
plan/epic so reviewers can grep for them once the underlying feature
ships.

## Out of scope (this task)

* CI wiring — see Plan 06 **T2**.
* JS SDK parity — see Plan 06 **T5**.
* Perf gates / benchmark harness — see Plan 06 **T3**.
* Snapshot retention — see Plan 06 **T4**.
