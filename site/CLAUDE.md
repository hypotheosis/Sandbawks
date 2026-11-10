# Sandbawks documentation site

`site/` holds ONLY the user documentation for Sandbawks. The product source is
the rest of this repository. Nothing in `site/` changes product behavior.

When documenting behavior (API routes, CRD fields, Helm values, defaults),
verify it against the source in this repository (`crates/`, `deploy/`,
`scripts/`). If a detail can't be confirmed, flag it as uncertain instead of
guessing. Never invent parameter names, return values, or features.

## Documentation framework
This site follows Diátaxis. Every page is one of four types — tutorial,
how-to, reference, or explanation — and types are not mixed on a single page.
Before writing or editing, identify which type the page is and keep it within
that mode. New pages go in the matching section:
  - src/content/docs/tutorials
  - src/content/docs/how-to
  - src/content/docs/reference
  - src/content/docs/explanation

## Stack & tooling
- Docs framework: Astro Starlight, served from the `/Sandbawks` base path on
  GitHub Pages (rustdocs share the Pages site under `/Sandbawks/api/`).
- Package manager: npm
- Runtime version: node 24

## Common commands (run from `site/`)
- Install:          npm ci
- Live preview:     npm run dev
- Build site:       npm run build
- Prose/style lint: npx eslint
Run the linter before proposing a PR.

## Layout
- `src/content/docs/` — Markdown/MDX content (what you'll usually edit)
- `src/assets/`, `public/` — images and static files
- `src/plugins/remark-base-links.mjs` — adds the base path to root-relative links

## Writing conventions
- Style guide: Google developer docs style
- Voice: second person ("you"), active voice, present tense.
  - Exception: tutorials may use collaborative "we" ("together we'll create a
    sandbox") to build the teacher–learner partnership, per the Diátaxis tutorial
    convention. All other page types (how-to, reference, explanation) stay in
    second person.
- Headings: sentence case.
- Use the framework's admonition syntax (`:::note`, `:::warning`) for callouts,
  not a bold "Note:".
- Write internal links as site-absolute paths without the base
  (`/reference/helm-chart/`); the remark plugin adds `/Sandbawks`.
- Image references use the public packages under `ghcr.io/hypotheosis/`.

## Frontmatter
Every page requires:
  - title
  - description
  - slug
  - head
  - tableOfContents

## What not to do
- Don't edit auto-generated files. Edit the source or template instead.
- Don't hand-edit version numbers or release notes.
- Don't add or reorder top-level nav sections without flagging it for review.
- Don't document features that aren't confirmed to exist.
- Don't name private hosts, registries or forges; this repository is public.
