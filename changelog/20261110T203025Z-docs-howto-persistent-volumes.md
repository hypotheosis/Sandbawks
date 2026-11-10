# How-to: keep sandbox files on persistent volumes

Date: 2026-11-10T20:30:25Z

Added `site/src/content/docs/how-to/keep-sandbox-files-on-persistent-volumes.md`
and its sidebar entry. It covers the `Filesystem` persistence mode:

- volume claim templates on a SandboxTemplate, with `fsGroup` and the
  `sandbawks.dev/mount-path` annotation;
- pausing and resuming through the E2B SDK, and checking the PVC and the
  `Suspended` condition;
- PVC retention (enabled by default, seven days);
- `volumeClaimTemplatesPolicy` for sandbox-supplied claims;
- the Kata requirement for `Filesystem` mode.
