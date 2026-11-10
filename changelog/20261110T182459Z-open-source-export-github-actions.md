# Open-source export: scrubbed tree + GitHub Actions CI

Date: 2026-11-10T18:24:59Z

## What changed

This repository is a fresh-history export of the private sandbawks tree,
prepared for publication on GitHub.

### Deployment-specific identifiers removed

- Image references to the private registry now point at
  `ghcr.io/hypotheosis/*`; pull-through-proxy references resolve to their
  upstream registries (`docker.io/...`, `ghcr.io/...`).
- Links to the private issue tracker are reduced to bare `#N` references
  (see "Project history" in README.md for what those numbers mean).
- Live hostnames, node names, and local filesystem paths in docs, changelog
  entries and test fixtures are replaced with `example.com` hosts or neutral
  placeholders. `tests/e2b-node-smoke` (and `just e2b-node-smoke`) now
  require `E2B_DOMAIN` instead of defaulting to a deployment's domain.
- The Go shim module path is `github.com/hypotheosis/sandbawks/shim`.
- `Cargo.toml` `repository`, the chart's `home`/`sources`, a `LICENSE`
  (Apache-2.0, matching the declared crate license) and a README history
  note were added or corrected.
- Helm chart `version` 0.1.28 -> 0.1.29 (default image registry changed);
  the `envd` default tag is now the `0.1.29` release tag instead of a
  commit SHA from the old history.
- `claims.json` is carried over unmodified: its records are sealed, and
  its evidence locators still name the old `.forgejo/` workflow paths.

### CI ported from Forgejo Actions to GitHub Actions

| Forgejo workflow | GitHub replacement |
|---|---|
| `ci.yml` | `.github/workflows/ci.yml` — same jobs; `sudo` for apt, artifact v4, explicit token permissions, vector-hop guard on the hosted runner's Docker |
| rustdocs step in `ci.yml` (S3 bucket) | `.github/workflows/docs.yml` — GitHub Pages, skipped while the repo is private |
| `kind-smoke.yaml` | `.github/workflows/kind-smoke.yaml` — images to ghcr.io with `GITHUB_TOKEN`, GHA layer cache instead of registry cache + S3 sccache, cluster jobs on the runner VM (no dind kubeconfig rewrite, no orphan sweeps) |
| `chart-publish.yaml` (Harbor OCI) | `.github/workflows/chart-publish.yaml` — `oci://ghcr.io/hypotheosis/charts` |
| `shim-publish.yaml` (S3 bucket) | `.github/workflows/shim-publish.yaml` — GitHub Releases `shim-<version>` + moving `shim-latest` |
| `kind-node-image.yaml` | `.github/workflows/kind-node-image.yaml` — ghcr.io, GHA cache |
| `ci-runner-image.yaml` + `ci/runner/` | removed — hosted runners provide Docker; kind/kubectl/helm pins moved to `.github/actions/setup-cluster-tools` |

`scripts/install-runsc-k3s.sh` now downloads the shim from
`<base>/shim-<version>/containerd-shim-sandbawks-v1` with
`base = https://github.com/hypotheosis/sandbawks/releases/download`.

The `ci/*-pr-comment.mjs` reporters already used `GITHUB_API_URL`; they now
paginate with `per_page`.

## Validation

- `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`,
  `cargo test --workspace`
- `go build/vet/test ./...` and `gofmt -l` in `shim/`
- `bash -n` + `shellcheck` on every `scripts/*.sh`
- `helm lint` / `helm template` on the chart
- every workflow and composite action parses as YAML

The GitHub Actions workflows themselves are validated by the first push to
the (private) GitHub repository.

## Known follow-ups

- While the repository is private, `install-runsc-k3s.sh` cannot fetch the
  shim anonymously; release assets become public with the repository.
- ghcr.io packages are created private; make them public alongside the
  repository.
- No `v0.1.29` tag exists yet in this history, so the chart's default
  `0.1.29` image tags are not yet published.
