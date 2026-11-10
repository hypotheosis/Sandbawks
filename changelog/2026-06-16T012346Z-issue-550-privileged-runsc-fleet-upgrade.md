# Issue #550 — privileged in-place runsc fleet upgrade (nsenter + k3s restart), Capability C of #545

**Date:** 2026-06-16
**Branch:** `sandbawks/sandbawks-550`
**Scope:** The remaining, genuinely **privileged** slice of the #545 fleet-upgrade
epic. Capability A (per-node `runsc-version` label + drift metric) and the
restart-**free** part of Capability C (atomic `hostPath` runsc swap) landed via
#546; Capability B (restore-placement gate) landed via #545's PR. This issue adds
the opt-in, controlled, **restart-bearing** in-place upgrade.

## Why

#546's restart-free convergence swaps the host runsc *binary* via a `hostPath`
write — sufficient when only the binary version drifts, because containerd execs
`runsc` fresh per sandbox. But some convergence scenarios require re-running the
full `scripts/install-runsc-k3s.sh`, which **rewrites the containerd CRI handler
tables** (`config.toml.tmpl` + `runsc.toml`) and **restarts k3s/containerd** —
handler tables are only re-read on a k3s (re)start. That restart kills running
sandboxes on the node, so it is intentionally excluded from #546's restart-free
DaemonSet step. This change lets the DaemonSet drive that full installer on the
host without manual SSH, behind hard safety rails.

## What changed

### New crate module — `crates/sandbawks-node/src/runsc_upgrade.rs`

A long-lived reconciler (`run_runsc_upgrader`), supervised in
`bin/sandbawks-node.rs` alongside the other node loops. Its tick:

1. Reads the host `runsc --version` (reusing #546's `RunscVersionReader`); if the
   host is already at `node.runscVersion`, it **releases the fleet lease if held**
   (the post-restart cleanup path) and stops — `AlreadyConverged`.
2. If `node.runscAutoUpgrade` is off → `Disabled`.
3. If the node hosts live sandboxes (#546's `LiveSandboxCounter`, reused verbatim)
   → `LiveSandboxes(n)`: a k3s restart would destroy them; defer until drained.
4. Otherwise acquires the cluster-wide upgrade **Lease**; `Contended` if another
   node holds it, `InProgress` if our own install is still in flight (don't
   relaunch), `Launched` on a fresh acquisition.
5. On `Launched`, runs the host installer **detached** so it survives the Pod the
   restart kills.

The three non-negotiable safety rails:

- **One node at a time.** A single namespaced `coordination.k8s.io/v1` Lease
  (`KubeLeaseUpgradeCoordinator`, holder = node name) serializes the rollout, so
  there is never a simultaneous fleet-wide k3s restart. The pure `decide_lock`
  mirrors the controller's leader-election decision, specialized to a mutex with a
  generous TTL: a node that bricks mid-upgrade stalls the rollout only for that TTL
  (default 600 s), and a stalled-past-TTL own-lease auto-retries.
- **Drained-node only.** Upgrade only with zero live sandboxes — composes with the
  #545 Capability B restore-placement gate that keeps restores off not-yet-upgraded
  nodes mid-rollout.
- **Survives the Pod's own death.** `NsenterUpgradeRunner` stages the bundled
  installer onto the host, then `nsenter -t 1 -m -u -i -n -p -- systemd-run
  --collect --unit … install-runsc-k3s.sh --runsc-version <ver>` — a transient unit
  owned by the host's systemd, detached from the Pod's cgroup, so the install keeps
  running across the k3s/containerd bounce that kills the DaemonSet Pod. On restart
  the new Pod sees the host converged and releases the lease so the next node
  proceeds without waiting out the TTL.

The host-exec and lease I/O are behind the `HostUpgradeRunner` / `UpgradeCoordinator`
traits so the whole tick is unit-tested with fakes; production impls do the real
`nsenter`/`systemd-run` and Lease GET→CREATE/DELETE.

### New metrics — `crates/sandbawks-node/src/runsc_upgrade_metrics.rs`

`runsc_upgrade_total{decision}` (disabled | already_converged | live_sandboxes |
contended | in_progress | launched) and `runsc_upgrade_errors_total{step}` make the
rollout + the privileged failure modes observable without scraping logs.

### Dockerfile — `crates/sandbawks-node/Dockerfile`

Bundles, for the upgrader, into the distroless runtime image:
- a static **busybox** (new `busybox` stage) exposed as `nsenter` (busybox
  dispatches by `argv[0]` basename, so a copy named `nsenter` IS the applet — no
  symlink, no util-linux, no extra libc); the applet's existence is verified at
  build time.
- `scripts/install-runsc-k3s.sh` at `/usr/local/bin/install-runsc-k3s.sh`, so the
  host-side install is self-contained.

### Helm — `deploy/helm/sandbawks/`

- `values.yaml`: `node.runscAutoUpgrade` (default false) + a
  `node.runscUpgrade.{enabled,leaseName,leaseDurationSeconds,intervalSeconds,hostStagingDir}`
  block (master `enabled` default false — the privileged surface is opt-in).
- `templates/node-daemonset.yaml`: `SANDBAWKS_RUNSC_UPGRADE_*` env; a RW host
  installer-staging `hostPath` mount (`/opt/sandbawks` → `/host/opt/sandbawks`); the
  shared host `/usr/local/bin` mount now renders for the reconciler **or** the
  upgrader. Two render-time `fail` guards: `runscAutoReconcile` ⊕ `runscAutoUpgrade`
  are mutually exclusive, and `runscAutoUpgrade` requires `runscUpgrade.enabled`.
- `templates/rbac.yaml`: the node ClusterRole gains, **only when**
  `runscUpgrade.enabled`, `coordination.k8s.io/leases: get,create,update,delete`
  (the minimized, gated privileged surface).
- `Chart.yaml`: `0.1.13` → `0.1.14`.

> **Apply-CRDs reminder:** no CRD field changed (acceptance criterion 3 —
> different-version snapshots staying unrestorable — is satisfied by the already-
> landed Capability B placement gate), so `deploy/crds` is unaffected; no
> `kubectl apply -f deploy/crds` needed.

## Privilege scope (minimized + documented)

The DaemonSet already runs `privileged` + `hostPID` (CRI checkpoint). This adds no
new container-security setting; it adds, gated on `runscUpgrade.enabled`: the host
installer-staging mount, the bundled `nsenter`/installer, and the four `leases`
verbs. There is no general host-exec — the only host command path is the fixed
`nsenter … systemd-run … install-runsc-k3s.sh`.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` — all pass, including the new `runsc_upgrade` unit tests
  (`decide_lock` ordering/TTL, tick orchestration across every decision with fakes,
  installer staging) and the wire-level `tests/runsc_upgrade.rs` (Lease
  GET→CREATE acquire, GET→DELETE release, no-delete when another node holds it).
- `helm lint` + `helm template` (default, `runscUpgrade.enabled=true`,
  reconcile-off + upgrade-on, both-on no-dup-env, and both mutual-exclusion guards)
  — render/​fail correctly.
- `docker build --target busybox` + `docker run --entrypoint /out/nsenter … --help`
  — confirms the bundled busybox dispatches to the `nsenter` applet.

The full node image build (in-container Rust compile) and the **live** privileged
path — the real `nsenter`/`systemd-run` host install, the k3s restart, and
surviving the Pod death — require a k3s node and are covered by the PR's manual
test plan, not run here.
