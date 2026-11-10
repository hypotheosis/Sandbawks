# Publish the Helm chart to Harbor + chart release guard-rails (issue #460)

Date: 2026-06-10

## Context

The deployment story was "clone the repo and `helm upgrade --install sandbawks
deploy/helm/sandbawks ...`" — the chart at `deploy/helm/sandbawks/` was never
packaged or published, so every install read the chart out of whatever revision
happened to be checked out. The four app images already ship to Harbor
(`ghcr.io/hypotheosis/...`); the chart now ships the same way, with a few guard-rails
to keep the chart version, image tags, and Cargo version from drifting apart.

## Changes

### 1. Publish the chart as an OCI artifact (`.forgejo/workflows/chart-publish.yaml`, new)

- On push to `main` and on `v*` tag pushes, `helm package` the chart and
  `helm push` it to `oci://ghcr.io/hypotheosis/charts` (Harbor 2.x stores Helm charts as OCI
  artifacts under a project, exactly like container images).
- Reuses the existing `secrets.HARBOR_USERNAME` / `secrets.HARBOR_PASSWORD`
  org secrets (the same creds the image build in `kind-smoke.yaml` uses); login
  is `--password-stdin` to keep the token out of the process table and logs.
- No `pull_request` trigger (the chart is only ever published post-merge / on a
  tag, so fork PRs never reach the Harbor credentials) and deliberately **no
  `paths:` filter**: on a tag push, `paths` is evaluated against the tagged
  commit's diff, so a release tag landing on a commit that didn't touch the
  chart would be silently skipped. The publish is a few seconds on a tiny tgz,
  so always-running on `main` + tags is simpler and footgun-free.

### 2. Semver-tagged images on tag push (already in place — verified)

`kind-smoke.yaml` already triggers on `push: tags: ['v*']` and emits
`type=semver,pattern={{version}}`, so `git tag v1.2.3 && git push --tags`
produces `ghcr.io/hypotheosis/<image>:1.2.3`. The new chart-publish workflow shares
the same `v*` trigger, so one tag now yields a coherent image + chart set.

### 3. Chart-version-bump guard (`.forgejo/workflows/ci.yml`)

- Added a `helm` path filter (`deploy/helm/sandbawks/**`) to the existing
  `detect-changes` job and a new `helm` output.
- New `chart-version-bump` job: on a PR that touches the chart, fail unless
  `Chart.yaml`'s `version` differs from the PR base. Base copy is read from
  `FETCH_HEAD` after an explicit shallow fetch of the base ref, so it works
  regardless of the runner's remote-tracking setup.

### 4. appVersion tied to the Cargo workspace version

- The publish workflow injects `--app-version "$(cargo workspace version)"` at
  package time, so the published chart always names the code revision it
  deploys.
- The existing `helm` job in `ci.yml` additionally asserts the committed
  `Chart.yaml` `appVersion` equals the `[workspace.package] version` in
  `Cargo.toml`, so the source file fails fast in a PR if it drifts rather than
  only being corrected at publish time.
- Both versions are read with `sed` (not yq/tomlq) to avoid adding a runner
  dependency; the Cargo read is scoped to the `[workspace.package]` section
  because `Cargo.toml` has many other `version = ` lines.

## Validation

- `helm lint deploy/helm/sandbawks` — clean.
- `helm template test deploy/helm/sandbawks` — renders.
- `helm package deploy/helm/sandbawks --app-version <v>` — confirmed the flag
  injects the appVersion into the packaged tgz while leaving the on-disk
  `Chart.yaml` untouched.
- Version-extraction `sed`/`grep` expressions and the version-bump guard's
  comparison logic exercised locally for both pass and fail cases.
- Both workflow YAML files parse with `yq`.
- Workspace gates (`just fmt-check`, `just clippy`, `just test`) green — this
  change touches only CI YAML + chart metadata, no Rust.

## Out of scope

- Reconciling the inconsistent default image registries in `values.yaml`
  (`controller`/`node` default to `ghcr.io/pete-grace/...` while `api`/`envd`
  default to `ghcr.io/hypotheosis/...`).
- Chart signing/attestation (cosign) and a public chart index for anonymous
  pulls.
- Cutting an actual `v0.1.0` release tag — this builds the machinery, not the
  first release.
