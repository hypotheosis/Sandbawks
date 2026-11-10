# 2026-05-22 — Plan 01 Task 12: GitHub Actions CI

Closes #13.

## What landed

`.github/workflows/ci.yml` with the three jobs the issue calls for:

1. **rust** — `cargo fmt --all -- --check`, `cargo clippy --workspace
   --all-targets -- -D warnings`, `cargo test --workspace --all-targets`,
   and `cargo doc --workspace --no-deps`. The job uses
   `dtolnay/rust-toolchain@stable` with `rustfmt` and `clippy`
   components, and `actions/cache@v4` keyed on `Cargo.lock` to keep
   the registry / `target/` warm between runs.

2. **crd-yaml-up-to-date** — re-runs `cargo run -p sandbawks-types
   --bin crdgen -- /tmp/crds` and `diff -ru deploy/crds /tmp/crds`.
   This is the drift gate: if anyone edits a CRD type in the
   `sandbawks-types` crate without regenerating `deploy/crds/`, this
   job fails loudly instead of letting stale YAML reach the cluster.

3. **helm** — installs Helm 3.16.0 via `azure/setup-helm@v4`, then
   runs `helm lint deploy/helm/sandbawks`, a `helm template test`
   smoke render piped to `/dev/null`, and `diff -ru deploy/crds
   deploy/helm/sandbawks/crds` to guarantee the chart's vendored CRD
   copies stay in lockstep with the canonical `deploy/crds/`.

## Why `.github/` rather than `.forgejo/`

The repo's authoritative remote is Forgejo (`github.com/hypotheosis/sandbawks`),
but the issue and the parent plan call for `.github/workflows/`.
Forgejo Actions (and Gitea Actions) read workflows from
`.github/workflows/` as a compatibility default and the workflow
syntax used here is the GitHub Actions subset both runtimes
implement. Migrating to `.forgejo/workflows/` later is a follow-up
that needs no source changes — just a `git mv`.

## Validation

- `just fmt` — silent (no formatting changes).
- `cargo clippy --workspace --all-targets -- -D warnings` — silent.
- `just test` — 20 workspace tests pass, unchanged from T11.

This change is workflow-only, so the validation steps confirm that
adding the CI file did not perturb the existing build. The workflow
itself will be exercised the first time it runs on a PR or push to
`main`; if any of the three jobs fails, the fix is to land it in a
follow-up rather than retrofit the workflow.

## What's next

Plan 01 is functionally complete with this CI gate in place. The
self-review checklist at the bottom of the plan doc can now be
walked end-to-end on `main` after this PR merges, and Plan 02
(`sandbawks-controller` skeleton) becomes unblocked.
