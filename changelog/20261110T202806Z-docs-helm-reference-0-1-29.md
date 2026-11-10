# Helm chart reference brought up to date with 0.1.29

Date: 2026-11-10T20:28:06Z

`site/src/content/docs/reference/helm-chart.md` now documents every leaf key
in `deploy/helm/sandbawks/values.yaml` (153). Each documented default is
checked against the values file by script.

New coverage:

- api: `sandboxDomain` (E2B hostname routing), `envdSilentParkMaxSecs` and
  `envdSleepHonorMaxSecs`.
- controller: leader election, restore recovery, pause recovery, PVC
  retention (enabled by default) and FQDN egress.
- node: `tolerations`, `nodeSelector`, `runtime` and `runtimeHandler` (Kata),
  runsc version management (reconcile and privileged upgrade), memory
  pressure and sandbox pressure telemetry.
- snapshotStore: `provider` and the Azure Blob Storage keys.
- usage: the whole metering, rollup and spill block.
- observability: `captureGenaiContent` and the Vector collector, including
  its Prometheus endpoint and the not-yet-supported split-pipeline keys.

The api, controller and node sections note the chart's render-time guards
(e.g. `replicas > 1` without leader election).
