# Issue #512 — Relocatable restore staging root

Date: 2026-06-13T16:57:09Z
Branch: `feat-issue-512-relocatable-staging-root`

## Problem

PR #509 made the controller's restore staging root configurable via
`SANDBAWKS_RESTORE_STAGING_ROOT` (sourced from `node.hostPathStagingCache`), but
the coupling was only half-wired. Three locations must agree on the staging
root, yet only the controller annotation tracked it:

- Controller annotation — `SANDBAWKS_RESTORE_STAGING_ROOT` (configurable).
- Node DaemonSet `mountPath` — hardcoded `/var/lib/sandbawks/staging`.
- Node binary staging root — compile-time `DEFAULT_STAGING_ROOT`, no env
  override; `ensure_under_staging_root` *rejects* any target outside it.

So overriding `node.hostPathStagingCache` shifted the host bind and the
controller annotation but not the node's in-container write path or the
sweeper root — the node refused to stage and restore broke. The default was the
only supported value, guarded by documentation alone.

## Change

### Node binary (`crates/sandbawks-node/src/bin/sandbawks-node.rs`)

- New `--staging-root` flag / `SANDBAWKS_STAGING_ROOT` env, defaulting to
  `DEFAULT_STAGING_ROOT`. One resolved value is threaded into **both** the
  `GvisorAdapter` (via `with_staging_root`) and `run_staging_sweeper`, so the
  stage site and the sweep site can never desync.
- `build_gvisor_adapter` gained a `staging_root` parameter wired to
  `with_staging_root`.
- Startup fails fast (`anyhow::bail!`) on a relative staging root: the host-side
  shim reads the staged path verbatim off the node filesystem, so a non-absolute
  root cannot resolve across the worker and the shim.
- The resolved staging root is logged at boot alongside the runsc roots.

### Node library (`crates/sandbawks-node/src/gvisor.rs`)

- Updated the `DEFAULT_STAGING_ROOT` doc comment: it is now only a default, with
  one flag feeding the adapter and the sweeper (the prior comment claimed the
  binary deliberately exposed no override).

### Helm chart (`deploy/helm/sandbawks/`)

- New `sandbawks.stagingRoot` template helper (`_helpers.tpl`) — the single
  source of truth, sourced from `node.hostPathStagingCache`. It is the
  **render-time guard**: `fail`s the render if the value is unset or not an
  absolute path.
- All three consumers now resolve through the helper, so they agree by
  construction:
  - `controller-deployment.yaml` — `SANDBAWKS_RESTORE_STAGING_ROOT`.
  - `node-daemonset.yaml` — the `staging-cache` `mountPath`, its `hostPath`,
    and the new `SANDBAWKS_STAGING_ROOT` env.
- `values.yaml` doc rewritten: `node.hostPathStagingCache` now relocates all
  three at once; stale "only supported value" notes removed from the controller
  deployment comment.
- `Chart.yaml` version bumped `0.1.6` -> `0.1.7`.

## Acceptance mapping

- ✅ Node binary reads the staging root from env, defaulting to the const;
  `GvisorAdapter` / sweeper / `ensure_under_staging_root` all honor it (the last
  two through the single threaded value).
- ✅ Node DaemonSet renders `mountPath` from `node.hostPathStagingCache`
  (`mountPath == hostPath`) and sets the node binary's staging-root env from the
  same value.
- ✅ Render-time Helm guard fails fast when the staging root is unset/relative;
  because all three sites derive from one helper, an operator override cannot
  silently diverge them. A complementary node startup assertion rejects a
  relative root for standalone runs.

## Validation

- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`.
- `helm template test deploy/helm/sandbawks` renders; `--set
  node.hostPathStagingCache=relative` and `=""` both fail the render with the
  guard message; `--set node.hostPathStagingCache=/mnt/fast/staging` relocates
  the controller env, the DaemonSet mountPath/hostPath, and the node env
  together.

## PR #513 review follow-ups

- **Stale doc comment** — `crates/sandbawks-controller/src/reconcile.rs` still
  documented `RESTORE_STAGING_ROOT_ENV` as "not a freely relocatable root" with
  "the default is the supported value". Rewritten to describe the relocatable,
  helper-driven behavior this change introduces.
- **Validation asymmetry (defense-in-depth)** — the relative-root rejection
  lived only in the node binary and the Helm guard. Added
  `reconcile::ensure_restore_staging_root_absolute()`, called at controller
  startup, mirroring the node binary's `bail!`: a direct/standalone controller
  invocation (bypassing Helm) with a relative `SANDBAWKS_RESTORE_STAGING_ROOT`
  now fails fast instead of silently recording an unresolvable
  `io.sandbawks/restore-image-path` annotation. The pure predicate
  `ensure_staging_root_absolute` carries a unit test (absolute ok; relative and
  empty rejected). The validated root is logged at controller boot.
