# Purge the legacy gVisor apt channel so a host `apt-get upgrade` can't re-drift runsc

**Issue:** #547 — `chore(node): purge legacy gVisor apt channel in install-runsc-k3s.sh`
**Related:** #455 (the original runsc pin this follows up), #545 (per-node runsc reporting + placement gate + in-place fleet upgrade), #546 (the durable in-cluster reconcile)
**Area:** node, deploy
**Date:** 2026-06-15

## Problem

#522 pinned the host runsc via a checksummed binary fetch to `/usr/local/bin`,
but it only closed the **install-time** drift vector. Every node provisioned by a
pre-#522 version of `install-runsc-k3s.sh` still carries that script's residue:

- `/etc/apt/sources.list.d/gvisor.list` pointing at the **rolling** gVisor
  `release main` channel (and the matching keyring), and
- the apt-managed `runsc` package at `/usr/bin/runsc`.

gVisor's `release` suite always resolves the `runsc` package name to the *latest*
date-stamped build. So any host `apt-get upgrade` — the `apt-daily-upgrade.timer`,
a broadened `unattended-upgrades`, cloud-init `package_upgrade`, or a manual ops
run — silently bumps `/usr/bin/runsc` off the pin (observed: nodes drifting to
`release-20260608.0` while the pin is `20260525.0`), with no review.

Two reasons the existing pin can't catch this:

1. The installer is invoked from cloud-init `runcmd`, which runs **once at first
   boot** — already-provisioned nodes never re-run it.
2. Even on a re-run, #522 installs to `/usr/local/bin/runsc` but left the apt
   source and package in place, so the rolling channel stayed armed. Worse, the
   version check (`runsc --version`) reports the `/usr/local/bin` copy that
   normally shadows `/usr/bin/runsc` on PATH, so it logs "already pinned" while
   the stale apt package still sits underneath.

This is exactly the gap between #455's two proposed options: we shipped option 2
(checksummed binary) but skipped the package-hygiene half of option 1 (hold/remove
the apt package).

## What changed

### `scripts/install-runsc-k3s.sh`

- Added `purge_legacy_apt_runsc()`: removes `/etc/apt/sources.list.d/gvisor.list`
  and `/usr/share/keyrings/gvisor-archive-keyring.gpg`, and `apt-get purge`s the
  `runsc` package when `dpkg-query` reports it installed.
- Called it unconditionally at the **top of `install_runsc`**, *before* the
  "already at pinned version" early-return — because the stale `/usr/bin/runsc`
  is typically invisible to the version check (shadowed by `/usr/local/bin`).
- Guarded on `command -v dpkg-query` so the step is a clean no-op on non-dpkg
  hosts instead of failing under `set -euo pipefail`; `apt-get purge` failures
  are tolerated (`|| true`) so cleanup never blocks the pinned install.
- Updated the header idempotency note to document the apt cleanup.

No change to the pinned binary fetch, the containerd handler registration, or the
k3s restart flow.

## Why this is the right layer (and what it is *not*)

This makes the **one-shot installer** self-healing for the apt residue whenever it
runs. It does **not** make a once-per-boot `runcmd` converge the fleet — that
fundamentally needs an in-cluster reconcile. The durable fix (a `sandbawks-node`
DaemonSet that re-asserts the pinned binary, removes the apt residue, and reports
the version) is tracked under #545; this change is the cheap, correct
node-provisioning half that also gives that reconcile a clean script to call.

## Validation

- `bash -n scripts/install-runsc-k3s.sh` — syntax OK.
- `shellcheck scripts/install-runsc-k3s.sh` — clean.
- `./scripts/install-runsc-k3s.sh --help` — renders.

## On-node confirmation (post-deploy)

```sh
dpkg -l | grep -E 'runsc|gvisor'                 # expect: no apt-managed runsc
cat /etc/apt/sources.list.d/gvisor.list          # expect: file absent
command -v runsc; runsc --version                # expect: /usr/local/bin, release-20260525.0
grep -i runsc /var/log/apt/history.log*          # the prior apt upgrade that caused drift
```
