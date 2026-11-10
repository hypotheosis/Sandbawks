# Issue #134 — Unify image build + kind-smoke into one workflow with a matrix build job

## Summary

Collapsed the four CI workflows that touched the three workspace image
trees (`controller`, `api`, `envd`) into a single
`.forgejo/workflows/kind-smoke.yaml` with two jobs:

1. `build-images` — strategy matrix over `[controller, api, envd]`,
   `fail-fast: false`, each leg builds + pushes its Dockerfile against
   Harbor's per-image `:buildcache` registry tag.
2. `kind-smoke` — `needs: build-images`, pulls the commit-scoped image
   tags, `kind load`s them into the cluster, runs the existing chart
   smoke or the api smoke (gated by `KIND_SMOKE_INCLUDE_API` for
   bisect parity).

Deleted in the same commit:

- `.forgejo/workflows/controller-image.yaml`
- `.forgejo/workflows/api-image.yaml`
- `.forgejo/workflows/envd-image.yaml`

Each of those workflows previously rebuilt the same three images on
every push to `main` after the kind-smoke gate had already built them
for the pre-merge PR — six builds per merged PR for the same source
tree. The new shape is three matrix-parallel builds per commit, full
stop.

## Tag scheme

Implemented per the issue's "use both" default:

| Trigger          | Tags emitted (per image)                                 |
|------------------|----------------------------------------------------------|
| Any commit       | `:sha-<full-github-sha>` *(what `kind-smoke` pulls)*     |
| `pull_request`   | additionally `:pr-<pr-number>` *(moving)*                |
| Push to `main`   | additionally `:latest`                                   |
| Push to `v*` tag | additionally `<semver>` (e.g. `1.2.3`)                   |

The smoke job pulls `:sha-<github.sha>` and re-tags to `:dev` locally
so the helm chart's default `pullPolicy: Never` + `tag: dev` values
keep working without per-call overrides for the commit tag.

## Fork PR handling

`build-images` detects fork PRs via
`github.event.pull_request.head.repo.full_name != github.repository`
and runs the build with `push: false` (the Dockerfile is still
exercised). `kind-smoke` is skipped entirely on fork PRs — there's
nothing pushed for it to pull. v1 keeps it at that; an
`actions/upload-artifact` passthrough that ships the built tarball
to the smoke job is a follow-up if fork contributors become a
regular blocker.

No PR-comment-from-workflow in v1 — the fork-PR build prints a
workflow warning via `::warning::` instead. The Forgejo Actions
`secrets.GITHUB_TOKEN` is also blocked on fork PRs, so posting a
proper comment would require additional plumbing that is out of
scope for this refactor.

## Harbor retention

The new commit-scoped `:sha-*` and `:pr-*` tags will accumulate
quickly — one of each per commit on every PR — so Harbor retention
needs to be tuned out-of-band on each of:

- `ghcr.io/hypotheosis/sandbawks-controller`
- `ghcr.io/hypotheosis/sandbawks-api`
- `ghcr.io/hypotheosis/envd`

Recommended policy (configured in the Harbor UI; we don't yet have a
terraform/pulumi story for Harbor in-repo):

- **Retain** `:latest`, semver tags (`v*`), `:buildcache`, and any
  tag with no prefix (defensive — covers manual pushes).
- **Retain** the most recent 5 `:sha-*` tags per repo (covers
  immediate revert + bisect needs).
- **Expire** `:sha-*` tags older than 14 days.
- **Expire** `:pr-*` tags 14 days after the PR closes (Harbor doesn't
  natively know about PR state, so the simplest approximation is the
  same 14-day window — `:pr-N` is moving, so its `last_pushed`
  timestamp tracks the latest commit on that PR).

## Files touched

- `.forgejo/workflows/kind-smoke.yaml` — rewritten.
- `.forgejo/workflows/controller-image.yaml` — **deleted**.
- `.forgejo/workflows/api-image.yaml` — **deleted**.
- `.forgejo/workflows/envd-image.yaml` — **deleted**.
- `.forgejo/workflows/ci-runner-image.yaml` — comment refresh; runs-on
  / triggers / image unchanged. (Explicitly out of scope per the
  issue's "Workflows to leave alone" list, but the header comment
  referenced two now-deleted files.)
- `crates/sandbawks-controller/Dockerfile` — comment refresh on the
  `BUILDER_IMAGE` ARG (named the deleted publish workflows).
- `justfile` — comment refresh on `api-image-build` recipe (named
  `api-image.yaml`).

No Dockerfile build steps changed; the three workspace Dockerfiles
already accept `BUILDER_IMAGE` + `SCCACHE_*` build-args and the
per-RUN guard from issue #130 handles missing creds gracefully, so
the build behaviour is byte-identical to the deleted workflows'.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean
  (no Rust code touched).
- `just test` — passes (no Rust code touched).
- `helm template deploy/helm/sandbawks` — chart unchanged.

Wall-clock + sccache-hit-rate validation moves to the first actual
CI run of the new workflow on the resulting PR; those numbers are
the gate the issue's acceptance criteria call out.

## Out of scope

- Shared cacher layer across the three images via a common
  `:sandbawks-cacher:buildcache` tag — issue explicitly defers this.
- Path filters on the matrix (today all three images always rebuild).
- Migrating `ci-runner-image.yaml` into the unified workflow.
- Harbor retention as code.
