# Plan 04 T14 — sandbawks-api Dockerfile + Forgejo Actions workflow

Forgejo issue: #113 (part of Plan 04 epic #99).

## Summary

Packages the `sandbawks-api` binary as an OCI image, publishes it to Harbor on
every push to `main`, and exposes local build + smoke recipes via `just`.
Mirrors the existing controller / envd image pipelines but consumes the
pre-baked `sandbawks-ci-runner` image (PR #98 / issue #93) as the workflow
container, dropping the manual docker-CLI install step the older two flows
carry.

## Changes

### `crates/sandbawks-api/Dockerfile` (new)

Multi-stage build modelled on `crates/sandbawks-controller/Dockerfile`:

- **Builder:** `rust:1.88-bookworm` with `musl-tools` + `protobuf-compiler` +
  `libprotobuf-dev`. The `libprotobuf-dev` package is required because
  `sandbawks-api` depends transitively on the `envd` crate, whose `build.rs`
  runs `tonic-build` on `proto/filesystem/filesystem.proto`; that schema
  imports `google/protobuf/timestamp.proto`, which Debian ships only in the
  `-dev` package.
- **Cross-compile flags:** `CC_x86_64_unknown_linux_musl=musl-gcc` so `ring`
  picks the right linker, and `RUSTFLAGS="-C target-feature=+crt-static -C
  relocation-model=static"` to defeat Rust 1.71+'s static-PIE default — without
  it the binary embeds the musl loader as PT_INTERP and distroless `static-
  debian12` doesn't ship that loader.
- **Runtime:** `gcr.io/distroless/static-debian12:nonroot`, `ENTRYPOINT
  ["/usr/local/bin/sandbawks-api"]`, `USER nonroot`.

Final image: **21.7 MiB** (docker reports MB but the underlying value is MiB).

### `.forgejo/workflows/api-image.yaml` (new)

- Triggers on `push` to `main` and on `v*` tags, with a `paths:` filter so
  unrelated commits don't burn a publish run. Path filter covers
  `crates/sandbawks-api/**`, `crates/sandbawks-types/**`, `crates/envd/proto/**`,
  `Cargo.toml`, `Cargo.lock`, and the workflow file itself.
- `runs-on: ubuntu-latest` with `container: ghcr.io/hypotheosis/sandbawks-ci-runner:
  latest`. The runner image already ships the docker CLI, so no inline
  download step is needed (unlike `controller-image.yaml` / `envd-image.yaml`).
  `:latest` is acceptable here because this is an image publish, not a CI
  gate — a runner-image rebuild cannot red-x a PR through this job.
- Uses `docker/setup-buildx-action@v3`, `docker/login-action@v3`,
  `docker/metadata-action@v5`, `docker/build-push-action@v6`, all with
  fully-qualified `https://github.com/...` `uses:` URLs per the
  `feedback_forgejo_actions_uses` memory (bare `owner/repo@ref` 404s against
  `data.forgejo.org`).
- Tag scheme matches the other two image publish flows: `type=sha`,
  `type=raw,value=latest,enable={{is_default_branch}}`, `type=semver`.
- Registry cache (`type=registry,ref=...:buildcache,mode=max`) layered on
  top, also matching the controller/envd pattern.

### `justfile`

Added two recipes:

- `api-image-build` — `docker build -t ghcr.io/hypotheosis/sandbawks-api:dev -f
  crates/sandbawks-api/Dockerfile .`
- `api-image-run` — `docker run --rm ghcr.io/hypotheosis/sandbawks-api:dev --help`

### `crates/sandbawks-api/src/bin/sandbawks-api.rs`

The issue's verification step (`just api-image-run`) requires `--help` to print
clap usage, but the bin previously read its two config values straight out of
`std::env::var(...)` with no clap parser. Added a minimal `clap::Parser`
struct (`clap` was already in the crate's `Cargo.toml`, unused) that exposes:

- `--listen <ADDR>` (env: `SANDBAWKS_API_LISTEN`, default `0.0.0.0:8080`)
- `--default-namespace <NAMESPACE>` (env: `SANDBAWKS_DEFAULT_NAMESPACE`,
  default `default`)

Both flags fall through to the same environment variables the Helm chart
already sets (`deploy/helm/sandbawks/templates/api-deployment.yaml`), so
deployments are unchanged — only `--help` / `--version` are new on the CLI
surface.

## Validation

```text
just fmt                                       — clean
cargo clippy --workspace --all-targets -D ...  — clean
just test                                      — all suites green
just api-image-build                           — image built (21.7 MiB)
just api-image-run                             — clap usage printed
```

Sample `--help` output:

```text
E2B-compatible HTTP/WS API gateway for the sandbawks Kubernetes sandbox controller.

Usage: sandbawks-api [OPTIONS]

Options:
      --listen <ADDR>                  Socket the HTTP server binds. Matches the `containerPort` the Helm chart's api Deployment exposes [env: SANDBAWKS_API_LISTEN=] [default: 0.0.0.0:8080]
      --default-namespace <NAMESPACE>  Kubernetes namespace the gateway places Sandboxes in when an inbound request doesn't pin one [env: SANDBAWKS_DEFAULT_NAMESPACE=] [default: default]
  -h, --help                           Print help
  -V, --version                        Print version
```

## Notes

The workflow itself can't be exercised locally without a Forgejo runner +
Harbor credentials, but the `:latest` runner-image pull plus a `docker build`
inside a node20-based container both have well-understood failure modes; the
local `just api-image-build` (which exercises the same Dockerfile end-to-end)
is the proximate validation that the build succeeds.
