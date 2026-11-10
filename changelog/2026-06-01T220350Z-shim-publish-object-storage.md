# shim-publish: ship to object storage instead of the Forgejo registry

## Why

The Forgejo generic package registry route was abandoned. Publishing required a
`write:package` PAT (the auto Actions token has no package scope — see
`2026-06-01T213051Z-shim-publish-pat-auth.md`), and on top of that the
**download** side was a dead end: the Forgejo host runs with
`REQUIRE_SIGNIN_VIEW = true`, so package objects can't be fetched anonymously
and there is no per-object "make public" toggle in Forgejo. k3s nodes had no
clean way to pull the shim during provisioning.

## What changed

Pivoted publishing to the public, web-enabled `sandbawks-releases` bucket on the
same S3-compatible endpoint sccache uses — mirroring ci.yml's existing
`sandbawks-rustdocs` publish. Nodes now download anonymously over HTTPS at
`https://github.com/hypotheosis/sandbawks/releases`.

### `.forgejo/workflows/shim-publish.yaml`

- Replaced the Forgejo package-registry step with an `aws s3 cp` upload to
  `s3://sandbawks-releases/sandbawks-shim/<version>/containerd-shim-sandbawks-v1[.sha512]`.
- Auth: `secrets.AWS_ACCESS_KEY_ID` / `secrets.AWS_SECRET_ACCESS_KEY` plus the
  `vars.SCCACHE_{REGION,ENDPOINT,S3_USE_SSL,S3_ENABLE_VIRTUAL_HOST_STYLE}`
  connection vars — identical to the rustdocs publish. Endpoint normalization
  and path/virtual addressing are copied from that step.
- Kept the `version` / `update_latest` `workflow_dispatch` inputs and the
  `sha-<short>` + `latest` dual-publish.
- Dropped the Forgejo-only machinery: PAT/basic-auth, the 409 immutability
  tolerance, and DELETE-before-PUT for `latest` (S3 `cp` overwrites in place,
  so re-publishing is idempotent). No `--acl` — public-read is a bucket-level
  setting, like `sandbawks-rustdocs`.
- Gated the publish step to push-on-`main` and `workflow_dispatch` (the trusted
  contexts that carry the AWS secrets), mirroring ci.yml.

### `scripts/install-runsc-k3s.sh`

- Default `SANDBAWKS_SHIM_BASE_URL` changed from the Forgejo package URL to
  `https://github.com/hypotheosis/sandbawks/releases`. The `<base>/<version>/<file>`
  layout is unchanged, so `fetch_shim()` is untouched.
- Reworded the Forgejo/registry references (help text, env docs, fetch comment,
  failure hint). `SANDBAWKS_SHIM_TOKEN` is retained but now documented as an
  optional bearer token for a private mirror; the default public bucket needs
  none.

## Verification

- `shim-publish.yaml` parses; the publish step's `env`/`if`/script are correct.
- Embedded publish script passes `bash -n`; version/latest + endpoint
  normalization simulated for push and dispatch inputs.
- `install-runsc-k3s.sh` passes `bash -n` and `shellcheck -S warning` clean.
- Not exercised end-to-end here (needs the AWS secrets + the web-enabled
  bucket); first push to `main` will perform the real upload.

## Operator note

`sandbawks-releases` must be web-enabled (public-read) and routed to
`https://github.com/hypotheosis/sandbawks/releases`, as the user confirmed.

## Follow-up: also publish the install script

The provisioning script `scripts/install-runsc-k3s.sh` is now published in the
same build to a stable top-level key, so a script change is immediately
available to newly-bootstrapping nodes:

  https://raw.githubusercontent.com/hypotheosis/sandbawks/main/scripts/install-runsc-k3s.sh

- Added `scripts/install-runsc-k3s.sh` to the workflow's `push.paths` trigger —
  without this, script-only changes would never fire a publish.
- The script upload is gated with `latest` (`update_latest=true`): both are
  moving pointers, so a pinned one-off shim release does not disturb the
  bootstrap URL. Uploaded with `--content-type text/x-shellscript`.

## Follow-up: overridable k3s restart unit

The final step restarts k3s so its embedded containerd reloads the new handler
config. The unit name was only auto-detected (`k3s.service` vs
`k3s-agent.service`), which is wrong on nodes that ship k3s under a different
unit. Added `SANDBAWKS_K3S_UNIT` to override it; the auto-detection remains the
default. Also fixed the `--help` printer, which truncated at a hard-coded line
number and no longer showed the later env vars, and corrected a stale "Forgejo
package registry" reference in the script header (now the object-storage
bucket).

## Files touched

- `.forgejo/workflows/shim-publish.yaml`
- `scripts/install-runsc-k3s.sh`

## Supersedes

- PR #252 (`PACKAGES_TOKEN` Forgejo auth) — now moot; close it.
