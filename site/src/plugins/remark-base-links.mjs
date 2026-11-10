// Prefix root-relative Markdown links (`[text](/reference/...)`) with Astro's
// `base`, so pages can keep writing site-absolute links while the site is
// served from a sub-path (GitHub Pages serves it under /Sandbawks/). Starlight
// already handles the sidebar and its own components; this covers links
// written in page bodies.
export default function remarkBaseLinks({ base }) {
	const prefix = base.replace(/\/+$/, '');
	const rewrite = (url) =>
		prefix && url.startsWith('/') && !url.startsWith('//') && !url.startsWith(`${prefix}/`)
			? `${prefix}${url}`
			: url;
	const walk = (node) => {
		if ((node.type === 'link' || node.type === 'definition') && typeof node.url === 'string') {
			node.url = rewrite(node.url);
		}
		node.children?.forEach(walk);
	};
	return (tree) => walk(tree);
}
