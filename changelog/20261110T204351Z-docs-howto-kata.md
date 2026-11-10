# How-to: run sandboxes on Kata Containers

Date: 2026-11-10T20:43:51Z

Added `site/src/content/docs/how-to/run-sandboxes-on-kata-containers.md`, its sidebar entry
and a link from the AKS section of the install guide. It covers:

- finding the Kata RuntimeClass and CRI handler;
- `node.runtime: kata` with tolerations;
- a Kata template with `persistenceMode: Filesystem`;
- pausing and resuming through the E2B SDK.

It documents that the chart serves only one runtime per cluster (#10).
