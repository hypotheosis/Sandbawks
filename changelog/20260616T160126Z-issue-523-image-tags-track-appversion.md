# Issue #523 — Helm image tags now track `appVersion`

**Date:** 2026-06-16T16:01:26Z
**Branch:** `fix-issue-523-image-tags-track-appversion`
**Type:** bug fix (Helm chart)

## Problem

A freshly pulled chart rendered stale, hardcoded image tags that no longer
matched the chart it shipped in:

```
$ helm pull oci://ghcr.io/hypotheosis/sandbawks --version 0.1.9
$ helm template sandbawks/ | grep image:
        image: "ghcr.io/hypotheosis/envd:2e1e237"
        image: "ghcr.io/pete-grace/sandbawks-node:0.1.0"
        image: "ghcr.io/hypotheosis/sandbawks-api:0"
        image: "ghcr.io/pete-grace/sandbawks-controller:0.1.0"
```

The publish workflow (`chart-publish.yaml`) injects `appVersion` from the Cargo
workspace version at package time, but it never bumped the placeholder image
tags in `values.yaml` (`0.1.0` for controller/node, `0000000` for api — the
latter rendering as `:0`). So the chart's `appVersion` advanced while the image
tags it referenced stayed frozen at their initial placeholders, pointing at
images that may not exist.

## Fix

Default each first-party image tag to the chart's `appVersion`:

- `templates/api-deployment.yaml`, `templates/controller-deployment.yaml`,
  `templates/node-daemonset.yaml`: the image reference now reads
  `{{ .Values.images.<svc>.tag | default .Chart.AppVersion }}`.
- `values.yaml`: the three placeholder tags are now blank (`tag: ""`) so the
  template default takes effect, with a comment explaining the fallback.

This is coherent with how images are published: a release `v*` tag pushes the
images at a matching `type=semver` tag (`kind-smoke.yaml`) *and* publishes the
chart with the same injected `appVersion`, so a clean `helm template` of the
pulled chart references `:<appVersion>` — images that actually exist.

`envd` is deliberately left at its content-addressed pin (`2e1e237`, issue
#270 restore determinism) — it is a separate binary with its own immutability
requirement, not tied to the workspace version.

### Registry fix

The `controller` and `node` repository defaults also pointed at the wrong
registry — `ghcr.io/pete-grace/sandbawks-{controller,node}` — while the images
are actually built and published to `ghcr.io/hypotheosis/sandbawks-{controller,node}`
(`kind-smoke.yaml`). CI papered over this by overriding
`images.{controller,node}.repository` on every `helm install`. Both defaults are
now `ghcr.io/hypotheosis/...`, matching `api`/`envd`, so a clean `helm template`
references images that exist without an override.

CI (`kind-smoke`) and production deployments continue to pin an explicit
immutable tag via `--set images.<svc>.tag=<sha>`; the `default` only supplies a
sane value when no override is given.

`Chart.yaml` `version` bumped `0.1.15` → `0.1.16` (every chart change ships a
distinct OCI release; `ci.yml` enforces the bump).

## Validation

- `helm lint deploy/helm/sandbawks` — passes.
- `helm template t deploy/helm/sandbawks | grep image:` — now renders
  `ghcr.io/hypotheosis/sandbawks-{api,controller,node}:0.1.11` (the current
  `appVersion`, all on the correct registry); `envd` unchanged at
  `ghcr.io/hypotheosis/envd:2e1e237`. No `ghcr.io/pete-grace` references remain.
- `helm template t deploy/helm/sandbawks --set images.controller.tag=sha-deadbeef`
  — still renders the explicit override (`...sandbawks-controller:sha-deadbeef`).
- No Rust touched, so the workspace gate (`cargo clippy` / `just test`) is
  unaffected; `just fmt-check` is clean.
