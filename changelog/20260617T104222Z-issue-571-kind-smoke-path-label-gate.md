# Path-filter + label-gate the kind-smoke/bench workflow (#571)

**Date:** 2026-06-17T10:42:22Z
**Issue:** #571
**File touched:** `.forgejo/workflows/kind-smoke.yaml`

## Problem

`kind-smoke.yaml` built five container images (controller/api/envd/node +
the cached kind-node) and then ran the cluster smoke and the GiB-scale perf
bench on **every** `pull_request`, regardless of what the PR changed. A docs-
or chart-only PR paid the full multi-image BuildKit + kind-cluster cost for
nothing, and the `bench` job (`timeout-minutes: 75`) ran on every same-repo PR
even though it is rarely needed.

`ci.yml` already path-filters its Rust/Go/scripts gates via a `detect-changes`
job (`dorny/paths-filter@v3`); the heavy cluster workflow did not.

## Change

Added a `detect-changes` job to `kind-smoke.yaml` mirroring `ci.yml`'s — same
tool, same taxonomy — emitting a single `images` output that is `true` only
when a file that can change image *contents* or *runtime* behavior is touched:

- Rust compilation inputs (`**/*.rs`, `**/Cargo.toml`, `Cargo.lock`,
  `**/*.proto`, `rust-toolchain*`, `.cargo/**`, `deploy/crds/**`)
- per-crate Dockerfiles (`crates/*/Dockerfile`)
- the Go shim (`shim/**`)
- the runsc-baked kind-node image inputs (`ci/kind-node/**`)
- this workflow file itself

Gating wired into the three heavy jobs:

| Job            | `needs`                          | runs when |
|----------------|----------------------------------|-----------|
| `build-images` | `[branch-up-to-date, detect-changes]` | non-PR event, **or** `images == true`, **or** the PR has the `benchmark` label |
| `kind-smoke`   | `[build-images, detect-changes]` | same-repo PR **and** `images == true` |
| `bench`        | `build-images`                   | same-repo PR **and** PR has the `benchmark` label |

- Non-PR events (push to `main`, `v*` tags) always build so `:latest` / semver
  tags stay current; `detect-changes` runs on those events too but its output
  is ignored via the `github.event_name != 'pull_request'` clause.
- `build-images` is also forced by the `benchmark` label so a labelled
  docs-only PR can still bench (the bench job `needs:` the freshly-built
  `sha-<sha>` images).
- `branch-up-to-date` still runs unconditionally as the fail-fast gate;
  everything chains off it, so a behind-base PR cascade-skips the workflow.
- Skipped-via-`if` jobs report as success (not stuck-pending), so
  branch-protection required checks still resolve — the same pattern `ci.yml`
  uses for its rust/go/scripts gates.

## Note on the helm chart

The issue's *prose* lists the helm chart among the gate triggers, but its
*acceptance criteria* require a chart-only PR to **skip** build/smoke/bench
(chart coverage lives in `ci.yml`'s helm lint/template/version-bump jobs). The
acceptance criteria are the contract, so `deploy/helm/sandbawks/**` is
intentionally **excluded** from the `images` filter.

## Validation

- `python3 -c "import yaml; yaml.safe_load(...)"` — workflow parses; folded
  `if:` scalars collapse to the intended single-line expressions.
- Logic traced against all five acceptance criteria (docs-only skip,
  helm-only skip, code/Dockerfile/shim/kind-node trigger, bench label gate,
  required-check resolution).
- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test` — no
  Rust changed; run to confirm the workspace stays green.
