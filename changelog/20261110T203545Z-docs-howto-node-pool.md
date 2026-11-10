# How-to: run sandboxes on a dedicated node pool

Date: 2026-11-10T20:35:45Z

Added `site/src/content/docs/how-to/run-sandboxes-on-a-dedicated-node-pool.md` and its
sidebar entry. It covers:

- labeling and tainting a pool;
- `node.tolerations`, so the node DaemonSet stages envd and sets
  `sandbawks.dev/envd-staged` there;
- template `nodeSelector` and `tolerations`, and per-Sandbox overrides
  (which replace the template values);
- troubleshooting Pending sandbox Pods, and the runsc-version placement
  gate on resume.
