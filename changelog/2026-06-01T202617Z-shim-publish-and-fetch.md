# Publish `containerd-shim-sandbawks-v1` as a curl-able artifact; fetch it in the k3s installer

**Date:** 2026-06-01T20:26:17Z

## Problem

`scripts/install-runsc-k3s.sh` (added earlier today) required the sandbawks
shim to already be on the node's PATH, but nothing put it there: the shim is
baked only into the **CI kind node image** (`ci/kind-node/Dockerfile`), not the
production `sandbawks-node` DaemonSet image. On a real k3s cluster the first
install failed with `--require-sandbawks-runsc`.

An interim idea — having the installer extract the shim from the node image
*already pulled on the node* — was rejected as racy: the node image lands only
after the DaemonSet schedules a pod there, which has no ordering guarantee vs.
a node-provisioning script. Runtime install must be self-contained and depend
on nothing already running on the node. (The DaemonSet pod itself runs as a
plain runc pod with no `runtimeClassName`, so installing the runtime before it
exists is the correct order — only the later sandbox Pods need the handlers.)

## Change

Treat the shim as a published, versioned artifact fetched by URL + sha512 —
mirroring how the node/kind Dockerfiles already fetch `runsc` from Google
storage.

### `.forgejo/workflows/shim-publish.yaml` (new)

- Triggers: `workflow_dispatch` + push to `main` touching `shim/**` or the
  workflow file.
- Builds the static shim with `setup-go@v5` (pinned via `go-version-file:
  shim/go.mod`) and `CGO_ENABLED=0 go build -trimpath -ldflags='-s -w'` —
  identical toolchain/flags to `ci/kind-node/Dockerfile`'s shim-builder stage.
- Publishes the binary + `.sha512` to the Forgejo generic package registry
  under both `sha-<short>` (immutable; pin this in production) and `latest`
  (mutable; DELETE-before-PUT since the registry is immutable per file).

### `scripts/install-runsc-k3s.sh`

- New `fetch_shim`: downloads the shim from the package registry (default
  `https://<forgejo-host>/api/packages/<org>/generic/sandbawks-shim`), verifies it
  against the sibling `.sha512` by direct hash comparison (filename-agnostic),
  and `install -m 0755` to `/usr/local/bin`.
- New flags/env: `--shim-version`, `--shim-url`, `--skip-sandbawks-runsc`
  (gVisor only), and `SANDBAWKS_SHIM_{BASE_URL,VERSION,TOKEN}` (the token is
  sent as `Authorization: token …` for a private registry). Kept
  `--require-sandbawks-runsc`, now meaning "a failed fetch is fatal".
- Default behavior: best-effort fetch; an already-installed shim is kept when
  the caller didn't pin a version. Self-contained — no dependency on the
  DaemonSet image.

## Notes

- Token scope: the publish step uses the Forgejo-provided `GITHUB_TOKEN`. If
  the instance restricts its package-write scope, swap in a `PACKAGES_TOKEN`
  secret (noted inline in the workflow).
- Verified locally: `go build` produces a static x86-64 ELF; `bash -n` +
  `shellcheck` clean on the script; the workflow YAML and every embedded shell
  block parse.
- Not addressed here: baking the shim into the production node image is no
  longer required for host install, but may still be wanted if a future
  in-cluster path needs it.
