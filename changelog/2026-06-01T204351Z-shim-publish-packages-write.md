# shim-publish: grant the Actions token `packages: write`

## Problem

The first run of `.forgejo/workflows/shim-publish.yaml` (commit `58169d0b`)
failed when uploading to the Forgejo generic package registry:

```
upload failed (401): https://<forgejo-host>/api/packages/<org>/generic/sandbawks-shim/sha-58169d0b/containerd-shim-sandbawks-v1
```

## Root cause

When a Forgejo Actions workflow declares no `permissions:` block, the imported
run token (`secrets.GITHUB_TOKEN`) is handed a restricted, repo-read scope.
Package-registry writes therefore have no `write:package` authority and the
registry refuses the credential outright with **401** (a scope-only failure
would surface as 403; 401 means the credential itself was rejected).

## Fix

Added a job-level `permissions:` block to `build-and-publish`:

```yaml
permissions:
  contents: read
  packages: write
```

- `packages: write` lifts the ephemeral run token to package-write on the
  Forgejo org so the `PUT .../api/packages/<org>/generic/...` uploads are accepted.
- `contents: read` is re-added because declaring any `permissions:` block
  resets every unlisted scope to `none`, which would otherwise break the
  `actions/checkout` step.

No new secret is required. If a future instance disables Actions package
writes entirely, the publish step's comment documents the fallback: swap
`REG_TOKEN` for a `PACKAGES_TOKEN` access token (with `write:package`) and set
`REG_USER` to its owner.

## Follow-up: parameterized manual dispatch

Added two `workflow_dispatch` inputs so manual runs are parameterizable:

- `version` (string, default empty) — publish under an explicit immutable tag
  (e.g. `v1.2.3`) instead of the default `sha-<short>`. Empty falls back to
  the commit sha tag.
- `update_latest` (boolean, default `true`) — whether to also move the mutable
  `latest` pointer. Untick to publish a one-off `version` without disturbing
  `latest`.

Both are read via `github.event.inputs.*`, which is null on `push` events, so
push runs keep the prior behaviour (sha tag + update `latest`). The publish
script gates the `latest` DELETE/PUT and final log line on these values.

## Files touched

- `.forgejo/workflows/shim-publish.yaml`
