# shim-publish: use a `PACKAGES_TOKEN` (auto-token can't write packages)

## Symptom

Every run of `.forgejo/workflows/shim-publish.yaml` failed uploading to the
Forgejo generic package registry with `401`:

```
upload failed (401): https://<forgejo-host>/api/packages/<org>/generic/sandbawks-shim/sha-<short>/containerd-shim-sandbawks-v1
```

An earlier attempt to fix this by adding `permissions: packages: write` to the
job (#251) did **not** help — the 401 persisted.

## Investigation

Forgejo's package middleware returns an identical `401 reqPackageAccess` for
*no auth*, *bad token*, and *authenticated-but-no-package-scope*, so the status
code alone could not identify the cause. A temporary diagnostic step was added
on `diag/shim-auth-probe` to instrument the CI→registry boundary and run once.

Evidence from that run:

| Observation | Conclusion |
| --- | --- |
| `GITHUB_TOKEN len = 40`, `GITEA_TOKEN len = 40` | Auto-token is populated (not empty) |
| `GET /api/v1/user` with the auto-token -> `200` | Auto-token authenticates correctly |
| Every package `PUT` (basic + bearer, both tokens) -> `401 reqPackageAccess` | Rejected at the package-permission check, not auth |
| `PUT` with a bogus basic-auth username also reached `reqPackageAccess` | Gitea authenticates by the token and **ignores** the basic-auth username |

## Root cause

The Forgejo automatic Actions token (`GITHUB_TOKEN` / `GITEA_TOKEN`) is a valid,
authenticating token but carries **no `write:package` scope**, and Forgejo does
**not** grant that scope through the GitHub-compat `permissions: packages: write`
block. The token's scope is the wall — independent of the acting user's org
permissions, the basic-auth username, or the auth scheme. The auto-token can
therefore never publish to the package registry on this instance.

## Fix

- `REG_TOKEN` now reads a `PACKAGES_TOKEN` secret instead of
  `secrets.GITHUB_TOKEN`. `PACKAGES_TOKEN` must be a Forgejo access token with
  `write:package` (and `delete:package` for the `latest` DELETE) scope, owned by
  a member of the Forgejo org with package-write access.
- Removed the proven-ineffective `permissions:` block (reverts the job to its
  pre-#251 default token scope, which checkout already worked under).
- `REG_USER` stays `github.actor`; since Gitea authenticates by the token and
  ignores the username, no separate username secret is required.
- Added a fail-fast guard that exits with an actionable message if
  `PACKAGES_TOKEN` is empty, instead of surfacing another opaque 401.

## Operator action required

Create the secret before this can succeed: a Forgejo access token with
`write:package` scope, added as repo (or org) secret `PACKAGES_TOKEN`.

## Files touched

- `.forgejo/workflows/shim-publish.yaml`

## Follow-up cleanup

- Delete the temporary `diag/shim-auth-probe` branch and its diagnostic step.
