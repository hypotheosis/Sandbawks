// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import remarkBaseLinks from './src/plugins/remark-base-links.mjs';

// GitHub Pages serves the repository's site at /Sandbawks/. The docs workflow
// (.github/workflows/docs.yml) builds this site and puts the rustdocs under
// /Sandbawks/api/ in the same Pages artifact.
const base = '/Sandbawks';

// https://astro.build/config
export default defineConfig({
	site: 'https://hypotheosis.github.io',
	base,
	markdown: { remarkPlugins: [[remarkBaseLinks, { base }]] },
	integrations: [
		starlight({
			title: 'Sandbawks',
			logo: { src: './src/assets/sandbawks.png', alt: 'Sandbawks' },
			favicon: '/favicon-32.png',
			head: [
				{ tag: 'link', attrs: { rel: 'apple-touch-icon', sizes: '180x180', href: `${base}/apple-touch-icon.png` } },
			],
			social: [{ icon: 'github', label: 'Source', href: 'https://github.com/hypotheosis/sandbawks' }],
			sidebar: [
				{
					label: 'Tutorials',
					items: [
						{ label: 'Point your agent at Sandbawks', slug: 'tutorials/point-your-agent-at-sandbawks' },
					],
				},
				{
					label: 'How-to guides',
					items: [
						{ label: 'Install on Kubernetes', slug: 'how-to/install-on-kubernetes' },
						{ label: 'Keep sandbox files on persistent volumes', slug: 'how-to/keep-sandbox-files-on-persistent-volumes' },
						{ label: 'Clone a sandbox', slug: 'how-to/clone-a-sandbox' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Helm chart', slug: 'reference/helm-chart' },
						{
							label: 'Custom resources',
							items: [
								{ label: 'Overview', slug: 'reference/custom-resources' },
								{ label: 'Sandbox', slug: 'reference/custom-resources/sandbox' },
								{ label: 'SandboxTemplate', slug: 'reference/custom-resources/sandboxtemplate' },
								{ label: 'SandboxWarmPool', slug: 'reference/custom-resources/sandboxwarmpool' },
								{ label: 'SnapshotJob', slug: 'reference/custom-resources/snapshotjob' },
								{ label: 'TeamAPIKey', slug: 'reference/custom-resources/teamapikey' },
							],
						},
					],
				},
				{
					label: 'Explanation',
					items: [
						{ label: 'The Sandbawks components', slug: 'explanation/components' },
						{ label: 'How Sandbawks maps E2B onto Kubernetes', slug: 'explanation/how-sandbawks-maps-e2b-onto-kubernetes' },
					],
				},
			],
		}),
	],
});
