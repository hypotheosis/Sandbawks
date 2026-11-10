# Sandbox lifecycle explanation and Events reference

Date: 2026-11-10T20:51:30Z

Added `site/src/content/docs/explanation/sandbox-lifecycle.md` and
`site/src/content/docs/reference/events.md`, with sidebar entries.

- The explanation covers the idle timeout and its activity sources,
  auto-pause, the not-ready handling (connect settle, silent park, the
  leading-`sleep` keepalive park with exit 100/75, EAGAIN errors), bounded
  pause/restore/eviction recovery, and the descheduler opt-out.
- The reference lists every Event reason the controller records on
  Sandbox and SandboxWarmPool.
