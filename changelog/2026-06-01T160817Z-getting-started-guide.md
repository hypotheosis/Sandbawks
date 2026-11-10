# Getting-started guide

**Date:** 2026-06-01T16:08:17Z

## Summary

Added `docs/getting-started.md`, a comprehensive onboarding guide that
takes an operator from a bare machine to a deployed sandbawks system and
a verified end-to-end check using the official Python E2B SDK as the
probe.

## Why

The repo had a local smoke runbook (`docs/kind-smoke.md`) and an SDK
harness README (`tests/e2b-sdk-compat/README.md`), but no single
front-door document. New operators had no clear, prerequisite-first path,
and the most common deploy failures (gVisor not installed, cluster older
than 1.31, snapshot store unconfigured) were undocumented as a
pre-deploy checklist.

## What the guide covers

- **Component overview** — gateway / controller / node / envd and the
  five backing CRDs.
- **Prerequisites, validated up front** — host tooling version table
  (Docker, kind 0.24, kubectl/k8s 1.31, helm 3.16, just, Rust 1.88, uv,
  openssl) and the four cluster prerequisites that actually bite (K8s
  1.31, gVisor `runsc` RuntimeClass on every node, S3 store for
  pause/resume, privileged node DaemonSet). Includes a copy-paste
  validation block that checks tooling presence, cluster reachability,
  server version >= 1.31, and the `gvisor` RuntimeClass.
- **Local quickstart** — `SANDBAWKS_KIND_GVISOR=1 just cluster-up` +
  `just api-smoke`.
- **Production deploy** — out-of-band CRDs + `helm install --skip-crds`,
  with hardening notes on the rotating signing secret and snapshot-store
  config, plus a `SandboxTemplate` + `TeamAPIKey` registration example.
- **Verify-the-tooling step (Python E2B SDK)** — recommended path
  (`just sdk-smoke`), the cheap `--collect-only` import check, and a
  standalone create→exec→kill script. Documents the localhost routing
  caveat: the per-sandbox envd proxy demuxes on the `E2b-Sandbox-Id`
  header, which the stock SDK omits on `connect()`, so localhost
  verification should prefer the harness (which patches it via
  `conftest.py`) or deploy behind wildcard DNS.
- **Troubleshooting table** mapping common symptoms to fixes.

## Verification

All commands, recipe names, env vars, prerequisite versions, the dev API
key (`sbk_smoke_smoke_smoke_smoke_smoke`), template name
(`api-smoke-template`), and the localhost header-injection caveat were
verified against the live tree: `justfile`, `docs/kind-smoke.md`,
`deploy/kind/fixtures/api-smoke.sh`, `tests/e2b-sdk-compat/conftest.py`,
`tests/e2b-sdk-compat/README.md`, `tests/e2b-sdk-compat/pyproject.toml`,
and `deploy/crds/`.

## Files

- Added: `docs/getting-started.md`
- Added: `changelog/2026-06-01T160817Z-getting-started-guide.md`
</content>
