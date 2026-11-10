# `scripts/install-runsc-k3s.sh` — per-node gVisor/runsc installer for k3s

**Date:** 2026-06-01T18:36:24Z

## Why

First real-cluster install is on k3s. The Helm chart ships two RuntimeClasses:

| RuntimeClass     | containerd handler | backed by                         |
|------------------|--------------------|-----------------------------------|
| `gvisor`         | `runsc`            | gVisor package (`io.containerd.runsc.v1`) |
| `sandbawks-runsc`| `sandbawks-runsc`  | `containerd-shim-sandbawks-v1` (`io.containerd.sandbawks.v1`) |

k3s's embedded containerd knows neither handler until a runtime is installed
**on the host** and registered in containerd's config. The `sandbawks-node`
DaemonSet can't do this — it runs inside a container. So sandbox Pods fail
with "RuntimeHandler not supported" until an operator runs a per-node install.
This script makes that step repeatable across nodes.

## What it does

1. Installs `runsc` (+ `containerd-shim-runsc-v1`) from the official gVisor
   APT repo. Skips if already present. Refuses non-x86_64/aarch64 nodes.
2. Detects the containerd CRI plugin key from the live rendered config
   (`io.containerd.grpc.v1.cri` for config v2, `io.containerd.cri.v1.runtime`
   for config v3 / containerd 2.x) so the appended blocks match the version.
3. Writes `/var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl` using
   k3s's `{{ template "base" . }}` include, then restarts k3s
   (`k3s.service` or `k3s-agent.service`, auto-detected).
4. **runsc registration is auto-detect-first.** Modern k3s (v1.30+)
   auto-registers an installed runsc binary; emitting our own `runsc` TOML
   table on top of that would be a duplicate-key parse error. So the first
   pass omits the explicit `runsc` block, restarts, and checks
   `k3s crictl info`. Only if the handler is absent does it append an explicit
   block and restart once more — robust across k3s versions, no duplicates.
5. Registers `sandbawks-runsc` only when `containerd-shim-sandbawks-v1` is on
   PATH (handler name `sandbawks-runsc`, `runtime_type =
   io.containerd.sandbawks.v1`, `pod_annotations =
   ["io.sandbawks/restore-image-path"]`). `--require-sandbawks-runsc` makes a
   missing shim a hard failure instead of a skip-with-warning.
6. Verifies each handler is live via `k3s crictl info` (grep, no jq dep).

Idempotent: apt upserts the package, the template is rewritten each run, k3s
is restarted only as needed, and re-running is a no-op once both handlers are
registered.

## Notes

- The runtime_type/handler-name mapping mirrors the kind CI fragments
  (`ci/kind-node/containerd-runsc.toml`,
  `ci/kind-node/containerd-sandbawks-runsc.toml`) so dev/CI and k3s stay
  consistent.
- `runsc install` is intentionally NOT called: it edits the host's
  `/etc/containerd/config.toml`, which k3s does not use.
- Verified locally: `bash -n` + `shellcheck` clean; template rendering keeps
  the Go directive literal and produces valid TOML; `--help` prints usage.

## Usage

```bash
sudo ./scripts/install-runsc-k3s.sh                        # runsc (+ shim if present)
sudo ./scripts/install-runsc-k3s.sh --require-sandbawks-runsc
```
