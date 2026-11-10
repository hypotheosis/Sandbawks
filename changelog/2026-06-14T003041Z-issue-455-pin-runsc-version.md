# Pin gVisor runsc version in install-runsc-k3s.sh

Issue: #455

## Problem

`scripts/install-runsc-k3s.sh` used the unpinned gVisor apt `release` channel,
so each node got whichever runsc version was current at provisioning time.
That silently drifted the runsc version across the fleet and undermined the
cgroupfs support matrix documented in #306 and the `--systemd-cgroup`
enforcement fix from #448, both of which are only valid for the tested runsc
release.

## Change

The installer now fetches a pinned gVisor release directly from GCS with
sha512 verification, matching the pattern already used in
`crates/sandbawks-node/Dockerfile` and `ci/kind-node/Dockerfile`.

- Default pin: `20260525.0` (kept in lockstep with the Dockerfile pins).
- Overridable via `--runsc-version <ver>` or the `SANDBAWKS_RUNSC_VERSION`
  environment variable.
- Base URL is configurable with `SANDBAWKS_RUNSC_BASE_URL`
  (default: `https://storage.googleapis.com/gvisor/releases/release`).
- Downloads and verifies both `runsc` and `containerd-shim-runsc-v1`.
- Idempotent: an installed runsc that already reports the pinned release is
  left untouched; otherwise it is replaced with the pinned version.

## Bumping the pin

To move to a newer gVisor release, change the default value of
`SANDBAWKS_RUNSC_VERSION` near the top of `scripts/install-runsc-k3s.sh` and
update the same pin in `crates/sandbawks-node/Dockerfile` and
`ci/kind-node/Dockerfile` to keep host, DaemonSet, and kind-node runsc versions
in lockstep.
