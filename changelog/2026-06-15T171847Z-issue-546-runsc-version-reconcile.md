# Issue #546 — DaemonSet restart-free runsc binary reconcile + legacy apt cleanup + version reporting

**Date:** 2026-06-15
**Branch:** `feat-issue-546-runsc-reconcile-version-report`
**Scope:** Sub-issue of the #545 fleet-upgrade epic — the **safe, restart-free,
`nsenter`-free** slice (Capability A + the non-restarting part of Capability C),
plus the apt-residue cleanup the #455 drift investigation surfaced.

## Why

Despite #455 pinning runsc, every node provisioned before #522 still carries the
old installer's residue: a **rolling** gVisor apt channel
(`/etc/apt/sources.list.d/gvisor.list` → `release main`) plus an apt-managed
`/usr/bin/runsc`. Any host `apt-get upgrade` (apt-daily timer, unattended
upgrades, ops) bumps runsc off its pin. The installer now purges that residue,
but it only runs **once at first boot** (cloud-init), so it can neither converge
already-drifted nodes nor *keep* them converged. That needs an in-cluster
reconcile — the job of the `sandbawks-node` DaemonSet.

The key insight that bounds scope: **CRI handler registration needs a k3s
restart; binary version convergence does not.** containerd execs `runsc` fresh
per sandbox, so swapping `/usr/local/bin/runsc` is a plain `hostPath` file write —
new sandboxes pick up the new binary with no restart and no `nsenter`. So this
change keeps the DaemonSet's privilege surface to `hostPath` writes only.

## What changed

### New crate module — `crates/sandbawks-node/src/runsc_reconcile.rs`

A long-lived reconciler (`run_runsc_reconciler`), supervised in
`bin/sandbawks-node.rs` alongside the other node loops, whose tick does three
things:

1. **Report (read-only, always)** — executes the host `runsc --version`, parses
   the `release-<ver>` token (`parse_runsc_release`), stamps it on the Node as
   `sandbawks.dev/runsc-version` and exports it via the `runsc_version{node,release}`
   info gauge. A missing host binary is a quiet skip.
2. **Disarm the rolling channel (always, idempotent)** — removes
   `/etc/apt/sources.list.d/gvisor.list` (and, opt-in, its keyring). With no
   source, a host `apt-get upgrade` has no candidate to promote `/usr/bin/runsc`
   to, so drift stops even if the stale binary lingers. The `apt-get purge runsc`
   that would clean dpkg state needs host exec (`nsenter`), so the DaemonSet
   deliberately skips it to stay `nsenter`-free — removing the source is
   sufficient to stop drift.
3. **Converge (opt-in, drained-node only)** — when `node.runscAutoReconcile=true`,
   the host release differs from `node.runscVersion`, **and** the node has no
   live sandboxes, atomically installs the image-**bundled** runsc +
   `containerd-shim-runsc-v1` onto the host `/usr/local/bin` (temp file +
   `rename`, mode 0755). Bundling (not curl) guarantees the host-side runsc and
   the DaemonSet's checkpoint-side runsc are the same build — the gVisor C/R
   version-match requirement. The decision (`decide_converge`) is gated in
   priority order; a Dockerfile-pin vs Helm-value mismatch (`BundledMismatch`)
   refuses to converge rather than thrash.

The convergence "no live sandboxes" gate composes with the #545 Capability B
placement gate: swapping the binary makes existing-version snapshots
unrestorable on that node mid-rollout.

### New label + metric

- `sandbawks_types::RUNSC_VERSION_LABEL` (`sandbawks.dev/runsc-version`) — the
  cross-crate contract the #545 restore-placement gate will key off.
- `crates/sandbawks-node/src/runsc_version_metrics.rs` — the `runsc.version` info
  gauge (always `1`, release on a label), exported as `runsc_version{node,release}`.

### Dockerfile — `crates/sandbawks-node/Dockerfile`

The runsc downloader stage now also fetches + sha512-verifies
`containerd-shim-runsc-v1`, and the runtime image bundles both binaries, so the
runsc + shim pair converged onto the host can never desync.

### Helm — `deploy/helm/sandbawks/`

- `values.yaml`: `node.runscVersion`, `node.runscAutoReconcile`, and a
  `node.runscReconcile.{enabled,hostBinDir,aptSourceDir,aptKeyringPath,intervalSeconds}`
  block.
- `templates/node-daemonset.yaml`: `SANDBAWKS_RUNSC_*` env, plus two RW
  `hostPath` mounts — host `/usr/local/bin` at `/host/usr/local/bin` (so it never
  shadows the image's own bundled runsc) and host `/etc/apt/sources.list.d` at
  `/host/etc/apt/sources.list.d`. No `hostPID`/`nsenter`/privileged-restart
  surface is added beyond these writes.
- `Chart.yaml`: version `0.1.12` → `0.1.13`.

> **Apply-CRDs reminder:** this change adds a Node *label* and a metric, not a CRD
> field, so `deploy/crds` is unaffected — no `kubectl apply -f deploy/crds`
> needed. The label is written at runtime by the DaemonSet.

## Validation

- `just fmt` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `just test` (`cargo test --workspace --all-targets`) — all pass, including the
  new `runsc_reconcile` unit tests (parse, converge-gate ordering, apt-disarm
  idempotence, atomic install perms, terminal-phase classification) and the
  wire-level `tests/runsc_reconcile.rs` (Node version-label PATCH + live-sandbox
  pod LIST/selectors).
- `helm lint` + `helm template` (default, `runscAutoReconcile=true`,
  `runscReconcile.enabled=false`, and `aptKeyringPath` set) — render correctly;
  disabling drops both mounts and all env.

The host-binary execution / real apt removal / real binary swap require a live
node and so are covered by the chart-rendered manual test plan in the PR, not run
here.
