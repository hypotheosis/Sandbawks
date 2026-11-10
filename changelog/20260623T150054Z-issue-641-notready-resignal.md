# Not-ready park + re-signal (issue #641)

## What changed

The streaming `process.Start` not-ready path was reworked into two modes:

- **Plain not-ready command** parks **silently** at most
  `SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS` (default 15s, all phases including cold
  start — previously cold start parked 300s silently), then proxies if it
  readied or emits the existing #625 retry notice.
- **A leading `sleep <x>`** on a transiently not-ready sandbox is honored as a
  keepalive-protected park up to `min(<x>, SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS)`
  (default cap 300s). The call is fully synthetic — `start(pid=0)` + keepalives
  — and ends with a **non-zero** signal: "sandbox now available; resend your
  command" (exit 100) on readiness, or the transient "still waiting" notice
  (exit 75) on timeout. The command is **never proxied** from this call; the
  agent resends it verbatim against the now-ready sandbox.

## Why

Both e2b SDKs (js 2.30.5 / py 2.29.5) require the first Connect frame to be a
real `start` (JS throws; Python silently reads pid 0), so a keepalive cannot
precede a real process and a synthetic start cannot splice the real command
without corrupting pid. A single call therefore cannot both wait for an
unrealized sandbox and run the real command. Bounding the silent park keeps a
long quiet wait from being cut by an ingress idle timeout (which a headless
agent reads as a malfunction); the keepalive sleep-park gives positive
liveness, and the non-zero re-signal keeps the agent in a clean retry loop
instead of an opaque timeout.

## Config

- `SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS` (default 15) — Helm `api.envdSilentParkMaxSecs`.
- `SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS` (default 300) — Helm `api.envdSleepHonorMaxSecs`.
- An agent honoring a long `sleep` must set its own `commands.run(timeout=...)`
  to match (the e2b default is 60s).
