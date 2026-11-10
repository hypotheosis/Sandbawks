# CI smoke T9 — kind-smoke docs + workspace green gate

**Issue:** #75
**Epic:** #66 — CI: kind-based deployment smoke test
**Date:** 2026-05-23

## What landed

- `docs/kind-smoke.md` — user-facing runbook for the kind smoke test.
  Covers the three-command local repro
  (`just cluster-up && just smoke && just cluster-down`), the
  prereq matrix (Docker, kind >= 0.24, kubectl >= 1.31, helm >= 3.16,
  just), an explicit "what this proves / what it does not prove"
  contract, a flake-debug runbook keyed off the three failure modes
  the workflow actually produces (`ErrImagePull`, `kubectl wait`
  timeout on `condition=Available`, `kubectl wait` timeout on
  Sandbox `status.phase`), and a "how to extend" section with
  copy-paste diffs for Plan 03 envd, Plan 04 api, and Plan 05 node.
- This changelog entry. Together with #67–#74, this closes out epic
  #66.

## What the epic delivered (T1 through T9)

The whole arc, for posterity:

| Task | Issue | Landed |
| ---- | ----- | ------ |
| T1 — Forgejo workflows path migration                 | #67 | `.github/workflows/` -> `.forgejo/workflows/`, no logic changes. |
| T2 — Chart `api.enabled` / `node.enabled` toggles     | #68 | Helm conditionals on the api Deployment and node DaemonSet. |
| T3 — Controller Dockerfile + image build target       | #69 | Two-stage musl-static build into distroless; `just controller-image-build` recipe. |
| T4 — Controller image publish workflow                | #70 | `.forgejo/workflows/controller-image.yaml` -> Harbor on push to `main`. |
| T5 — kind cluster config + `just cluster-{up,down,reload}` | #71 | Single-control-plane kind cluster pinned at `kindest/node:v1.31.0`. |
| T6 — Sandbox smoke fixture + `smoke-assert.sh`        | #72 | `deploy/kind/fixtures/sandbox-smoke.yaml` + jsonpath-form `kubectl wait`. |
| T7 — Forgejo Actions kind-smoke workflow              | #73 | `.forgejo/workflows/kind-smoke.yaml`, dind-aware kubeconfig rewriting. |
| T8 — Caching for sub-90s warm runs                    | #74 | Cargo + kind node image + BuildKit GHA layer caches. |
| T9 — Docs + workspace green gate                      | #75 | This PR. |

## Workspace green gate (run locally before merge)

The full list, in the order they should run on a clean checkout:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
cargo doc --workspace --no-deps
helm lint deploy/helm/sandbawks
helm template t deploy/helm/sandbawks > /dev/null
just cluster-up && just smoke && just cluster-down
```

See the PR body for the live results from this branch.

## What's deliberately not here

- **api smoke.** Gated off via `--set api.enabled=false`; lands with
  Plan 04 (#52).
- **node smoke.** Gated off via `--set node.enabled=false`; lands with
  Plan 05 (#54). Will also require a worker node in
  `deploy/kind/cluster.yaml`.
- **Runtime-adapter smoke.** The current smoke uses the fake adapter
  (`SandboxRuntime` impl driven by an annotation). The
  containerd/gVisor adapter is Plan 05.
- **envd smoke.** Fixture references `ghcr.io/hypotheosis/envd:dev` for
  schema compliance only; the image does not exist yet. Plan 03 (#53)
  will publish it and this changelog's "how to extend" section in
  `docs/kind-smoke.md` describes the fixture diff at that point.
- **Performance benchmarks.** The < 90 s warm-run target from T8 is
  the only quantitative gate; no broader benchmark suite exists yet.
- **Multi-arch images.** Controller image is `linux/amd64` only.

## Next plan

Plan 03 (#53) — envd —
lands next. It will publish `ghcr.io/hypotheosis/envd` and extend the smoke
fixture with the real published tag per the diff in `docs/kind-smoke.md`.

## Dependencies

- Blocked by T8 (#74). Confirmed merged on `main` at `22b136a` before
  starting this work.
