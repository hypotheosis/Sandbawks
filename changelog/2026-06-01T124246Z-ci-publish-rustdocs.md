# CI: publish workspace rustdocs to `sandbawks-rustdocs`

**Issue:** #156 — Publish
workspace rustdocs from CI to an S3-backed static site.

## Summary

The `rust` job in `.forgejo/workflows/ci.yml` already ran
`cargo doc --workspace --no-deps`, but the generated HTML in `target/doc/`
was discarded at the end of the job. This change adds a `Publish rustdocs`
step that mirrors `target/doc/` to the `latest/` prefix of the dedicated
`sandbawks-rustdocs` object-store bucket on every push to `main`, giving
contributors and reviewers a way to skim the contract surface without
cloning and building.

## Changes

- **`.forgejo/workflows/ci.yml`** — new `Publish rustdocs` step after the
  existing `docs` step in the `rust` job:
  - Gated on `github.event_name == 'push' && github.ref == 'refs/heads/main'`
    so fork PRs (which never receive the AWS secrets) skip it, mirroring the
    `is_fork` gating used by the `build-images` job in `kind-smoke.yaml`.
  - Installs `awscli` via apt only if not already on the runner image.
  - Synthesizes a top-level `target/doc/index.html` linking each workspace
    crate. `cargo doc --workspace` produces no landing page on stable — the
    `--enable-index-page` flag is nightly-only — so the bucket root would
    otherwise 404. The generator enumerates `target/doc/*/index.html`, which
    matches exactly the per-crate landing pages (shared asset dirs such as
    `static.files/` carry no `index.html` and are skipped).
  - Runs `aws s3 sync target/doc s3://sandbawks-rustdocs/latest/ --delete`
    against the same S3-compatible endpoint sccache uses
    (`vars.SCCACHE_ENDPOINT`). `--delete` keeps `latest/` an exact mirror,
    pruning files for crates or items that no longer exist.
  - Normalizes the endpoint to a scheme-qualified URL (sccache's
    `SCCACHE_ENDPOINT` may be a bare `host[:port]` with TLS toggled by
    `SCCACHE_S3_USE_SSL`) and matches sccache's path/virtual-host addressing
    mode so the shared endpoint resolves the bucket identically.

- **`README.md`** — new "Documentation" section describing the published
  docs. The concrete public URL is intentionally left as a `TODO(#156)`
  until the bucket's static-site hostname is finalized (operator decision).

## Design notes

- **Dedicated bucket, hardcoded name.** The bucket name `sandbawks-rustdocs`
  is written literally rather than read from a new `vars.DOCS_BUCKET` org
  variable. The endpoint is reused from sccache, and the bucket is a fixed,
  known value, so hardcoding removes an operator prerequisite. A dedicated
  bucket (vs. a `docs/` prefix on the sccache bucket) keeps the public-read
  ACL off the private sccache bucket.

## Validation

- `just fmt` — pass (no Rust touched; no-op).
- `cargo clippy --workspace --all-targets -- -D warnings` — pass.
- `just test` — pass.
- `cargo doc --workspace --no-deps` locally, then ran the exact
  index-generation snippet against the real `target/doc/`: it emitted links
  for all six workspace crates (`crdgen`, `envd`, `sandbawks_api`,
  `sandbawks_controller`, `sandbawks_node`, `sandbawks_types`) and every
  linked `index.html` was confirmed present.
- Exercised the endpoint-normalization and addressing-style shell logic
  standalone across bare-host/schemed/SSL-on/SSL-off cases.
- `.forgejo/workflows/ci.yml` re-validated as well-formed YAML.

### Not exercised locally

- The live `aws s3 sync` upload — no AWS credentials or object-store
  endpoint are available in the dev environment, and the publish step is
  gated to `push`/`main` so it cannot run from the PR. It is only exercised
  post-merge on the first `main` push.

## Operator prerequisites (not code in this PR)

- Provision the `sandbawks-rustdocs` bucket with static-site hosting +
  public-read on `latest/**`.
- Point a DNS record / static-site hostname at it, then replace the
  `TODO(#156)` in `README.md` with the live URL.

## Out of scope (per issue #156)

- Per-branch / per-PR doc previews (`branch/<branch>/`).
- Per-tag immutable snapshots (`tag/<tag>/`).
- Switching doc generators or public API stability guarantees.
