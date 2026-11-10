# Sandbawks documentation site

The user documentation for Sandbawks, built with
[Astro Starlight](https://starlight.astro.build) and published to
<https://hypotheosis.github.io/Sandbawks/>. The workspace rustdocs are published
alongside it at `/Sandbawks/api/`; `.github/workflows/docs.yml` builds both into
one GitHub Pages site on every push to `main`.

The pages follow [Diátaxis](https://diataxis.fr/): every page is a tutorial,
how-to guide, reference or explanation, and lives in the matching folder under
`src/content/docs/`.

## Working on the docs

Use Node 24. From this directory:

| Command | What it does |
|---------|--------------|
| `npm ci` | Install dependencies. |
| `npm run dev` | Serve the site at `http://localhost:4321/Sandbawks/` with live reload. |
| `npm run build` | Build the static site to `dist/`. |
| `npm run preview` | Serve the built site locally. |
| `npx eslint` | Lint the site. Run it before opening a pull request. |

The site is served from the `/Sandbawks` base path. Write internal links as
site-absolute paths without the base (`[Sandbox](/reference/custom-resources/sandbox/)`);
`src/plugins/remark-base-links.mjs` adds the base at build time.
