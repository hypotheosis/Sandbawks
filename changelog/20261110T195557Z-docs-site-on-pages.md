# User docs site moved into the repository and published with the rustdocs

Date: 2026-11-10T19:55:57Z

The Astro Starlight user documentation (Diátaxis: tutorials, how-to guides,
reference, explanation) now lives in `site/`. Previously it was a separate
private repository, last updated 2026-06-09.

- `.github/workflows/docs.yml` (renamed `rustdocs` → `docs`) builds the
  site with Node 24 and the workspace rustdocs, and publishes one GitHub
  Pages artifact: the docs at `https://hypotheosis.github.io/Sandbawks/`
  and the rustdocs under `/Sandbawks/api/` (previously at the site root).
- `site/astro.config.mjs` sets `site` and `base: '/Sandbawks'`.
  `site/src/plugins/remark-base-links.mjs` prefixes root-relative Markdown
  links with the base, so pages keep writing `/reference/...` links.
- Image and install instructions now use the public packages:
  `ghcr.io/hypotheosis/{sandbawks-api,sandbawks-controller,sandbawks-node,envd}`,
  the OCI chart `oci://ghcr.io/hypotheosis/charts/sandbawks` (0.1.29), CRDs
  from a `v0.1.29` clone of this repository, and the shim from the
  `shim-latest` / `shim-sha-<sha>` GitHub releases. The Helm reference now
  documents the empty image `tag` default (resolves to the chart's
  `appVersion`) and envd's `0.1.29` tag.
- Private forge, registry and release hosts were removed from the pages.

The docs still trail the code: features added since 2026-06-09 (persistence
modes, `restoreFrom`, FQDN egress, scheduling fields, Kata, Azure snapshot
store, usage metering, multi-namespace tenancy, leader election, the Vector
collector, the idle timeout, the v2 routes) aren't documented yet.
