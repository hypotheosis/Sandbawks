# Plan 06 T1 — E2B Python SDK compatibility harness

**Issue:** #183 (parent epic #182)
**Branch:** `feat-issue-183-e2b-sdk-compat-harness`
**Date:** 2026-05-26

## Summary

Adds `tests/e2b-sdk-compat/`, a `uv`-managed Python pytest project
that drives the official [`e2b`](https://pypi.org/project/e2b/) SDK
against a sandbawks gateway reachable via `E2B_API_URL`. Wires a new
workspace `just sdk-smoke` recipe so the harness has a one-shot
local entry point that mirrors the pattern already established by
`just api-smoke` and `just pause-resume-smoke`. CI integration is
explicitly deferred to **T2**, per the issue's scope.

The harness lives outside the Cargo workspace on purpose — it is a
sibling test rig like `deploy/kind/fixtures/`, not a workspace
member. The Rust toolchain does not need Python to be present, and
`cargo test --workspace` stays Rust-only.

## Test matrix

Every spec §7 endpoint the e2b SDK actually drives is covered by at
least one test:

| Endpoint / surface | Module | Notes |
|---|---|---|
| `POST /sandboxes`, `GET /sandboxes`, `POST /sandboxes/{id}/pause`, `POST /sandboxes/{id}/connect`, `POST /sandboxes/{id}/kill` | `tests/test_lifecycle.py` | covers cold create / list / kill / pause-resume-exec compound. `is_running` / `get_info` / `set_timeout` deferred to `test_skipped.py` — backing routes (`GET /sandboxes/:id`, `POST /sandboxes/:id/refreshes`) tracked in #189 |
| Exec / code-run WebSocket | `tests/test_commands.py` | blocking + background + streaming + envs + `list`/`kill` |
| `files.list/stat/remove/move/makedir/read/write` | `tests/test_files.py` | drives both gRPC metadata pass-through and signed-URL upload/download. `files.watch_dir` deferred to T2 — SDK contract needs live-gateway observation |
| `GET /events/sandboxes` (SSE), `GET /events/sandboxes/{id}` | `tests/test_events.py` | wire-level via `httpx`; SDK has no public events client yet |
| `pause → resume → exec` compound flow | `tests/test_lifecycle.py::test_pause_resume_exec_roundtrip` | spec §13 acceptance #2 |
| `write → list → read → remove` compound flow | `tests/test_files.py::test_write_list_read_remove_roundtrip` | spec §13 acceptance #2 |

`tests/test_skipped.py` carries `pytest.skip` markers (with TODO
references) for SDK methods whose backing features are not yet
implemented:

- `Sandbox.create_snapshot` / `delete_snapshot` / `list_snapshots` → **Plan 07**
- `Template.build` → post-MVP template build pipeline
- `Sandbox.get_metrics` → spec §11 observability surface
- `Sandbox.create(volume_mounts=...)` → post-MVP Volume CRD
- `files.watch_dir` → SDK contract needs live-gateway observation, deferred to **T2** (#184)
- `Sandbox.is_running` / `get_info` / `set_timeout` → backing routes (`GET /sandboxes/:id`, `POST /sandboxes/:id/refreshes`) not yet implemented; tracked in **#189**

Discovery enumerates **28 tests across 5 modules**.

## Validation

- `cd tests/e2b-sdk-compat && uv sync && uv run pytest -v --collect-only`
  → 28 tests collected in 0.02s, no import errors. Run on the
  development host with no live gateway, proving the harness loads
  hermetically.
- `cargo fmt --all` → no diff.
- `cargo clippy --all-targets -- -D warnings` → clean.
- `just test` → 48 result lines, all `ok`, no failures.
- **NOT YET RUN on this host:**
  `SANDBAWKS_KIND_GVISOR=1 just cluster-up && just sdk-smoke`
  end-to-end requires a kind cluster with the `sandbawks-kind-node`
  image (which itself layers `runsc` atop `kindest/node`). That's a
  multi-minute, heavyweight operation; it is the contract T2 will
  enforce in CI. The harness's discovery-only smoke (above) is the
  cheap local guard that the harness loads.

## File-level breakdown

- `tests/e2b-sdk-compat/pyproject.toml` — uv project metadata; deps
  pinned to compatible ranges (`e2b>=2.21,<3`, `pytest>=8,<9`,
  `pytest-asyncio>=0.23,<1`, `httpx>=0.27,<1`).
- `tests/e2b-sdk-compat/conftest.py` — session-scoped autouse fixture
  that populates `E2B_API_URL` / `E2B_API_KEY` from the environment
  (defaults match `deploy/kind/fixtures/api-smoke.sh`), plus a
  per-test `sandbox` fixture that creates / kills.
- `tests/e2b-sdk-compat/tests/{test_lifecycle,test_commands,test_files,test_events,test_skipped}.py`
  — the test matrix.
- `tests/e2b-sdk-compat/README.md` — install / run / discovery /
  scope-of-work documentation.
- `justfile` — adds the `sdk-smoke` recipe.
- `.gitignore` — adds `tests/e2b-sdk-compat/.venv/`, `.uv/`,
  `.pytest_cache/`, and `__pycache__/`.

## Out of scope (per the issue)

- CI wiring → **T2**.
- JS SDK parity → **T5**.
- Perf gates / benchmark harness → **T3**.
- Snapshot retention → **T4**.
