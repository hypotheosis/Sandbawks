# Plan 03 T11 — envd image default wired into Helm chart

Forgejo issue #64. Part of
epic #53.

## What landed

- `deploy/helm/sandbawks/values.yaml`: new `envd:` block with `image`,
  `tag`, `pullPolicy`. The tag is pinned to `2e1e237` — the short SHA
  of the most recent `main` commit touching `crates/envd/**` or the
  `envd-image` publish workflow (commit `2e1e2373…`, the T10 commit
  that added the workflow itself).
- `deploy/helm/sandbawks/templates/controller-deployment.yaml`: two new
  env vars on the controller container — `SANDBAWKS_DEFAULT_ENVD_IMAGE`
  (rendered as `<image>:<tag>`) and `SANDBAWKS_DEFAULT_ENVD_PULL_POLICY`.

## Design choice

The issue offered two wiring locations:

1. A ConfigMap consumed by the controller.
2. A `--default-envd-image` arg on the controller Deployment.

We picked a third, closer-to-existing-wiring option: an env var on the
controller Deployment, sourced from chart values. Rationale:

- The Plan 02 controller binary
  (`crates/sandbawks-controller/src/bin/sandbawks-controller.rs`)
  already reads `SANDBAWKS_NAMESPACE` and `RUST_LOG` from env — no new
  flag-parsing dependency, no new mount.
- The Plan 02 controller does not yet consume the default (Plan 05
  lands pod creation and the envd sidecar injection). Staging the
  value as an env var means Plan 05 picks it up with a single
  `std::env::var(...)` call.
- A ConfigMap mount would require an extra template + volume + reload
  semantics for zero current benefit. Easy to switch to later if an
  operator actually needs to change the default without restarting the
  controller.

Per-template overrides via `SandboxTemplate.spec.envdImage` continue to
win — the CRD still marks that field `required`, so the chart-wide
default is only the *fallback* once Plan 05's pod-creation path lands.

## Validation

- `helm lint deploy/helm/sandbawks` — passes (one INFO about a missing
  icon, pre-existing).
- `helm template test deploy/helm/sandbawks | grep 'ghcr.io/hypotheosis/envd'`
  — hits line `value: "ghcr.io/hypotheosis/envd:2e1e237"`.
- `helm template test deploy/helm/sandbawks --set envd.tag=foo | grep
  'ghcr.io/hypotheosis/envd:'` — hits `value: "ghcr.io/hypotheosis/envd:foo"`, so
  the override flows through.
- Re-rendered with the kind-smoke flag set
  (`--set api.enabled=false --set node.enabled=false --set
  images.controller.repository=… --set images.controller.tag=dev --set
  images.controller.pullPolicy=Never`) — the controller Deployment
  still renders cleanly with the new env vars present.
- `just fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `just test` — all green.

## Out of scope / follow-ups

- Optional `helm template … | grep 'ghcr.io/hypotheosis/envd'` assertion in
  `scripts/smoke-assert.sh` (nice-to-have per the issue). Deferred to a
  follow-up so this PR stays scoped.
- imagePullSecret plumbing for Harbor. Assumes the cluster has a
  Harbor credential set as a default pull secret; if a future deploy
  finds that assumption wrong, follow up with a `pullSecrets:` block.
- The controller binary itself reading `SANDBAWKS_DEFAULT_ENVD_IMAGE`
  and injecting it as a sidecar — Plan 05 work.
