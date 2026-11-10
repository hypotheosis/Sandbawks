---
title: Helm chart
description: Reference for the Sandbawks Helm chart — chart identity and every value that can be set, with defaults, across images, the api, controller, node DaemonSet, envd, runtime classes, snapshot store, usage metering, and observability.
slug: reference/helm-chart
head:
  - tag: title
    content: Helm chart · Sandbawks docs
tableOfContents: true
---

The Sandbawks Helm chart deploys the `sandbawks-api` gateway, the
`sandbawks-controller`, and the `sandbawks-node` DaemonSet, plus the
`RuntimeClass` resources and Secrets they need. This page documents every value
the chart accepts. For an install walkthrough, see
[Install Sandbawks on Kubernetes](/how-to/install-on-kubernetes).

## Chart identity

| Property | Value |
|----------|-------|
| Chart name | `sandbawks` |
| API version | `v2` |
| Type | `application` |
| Chart version | `0.1.29` |
| App version | `0.1.29` |
| Published at | `oci://ghcr.io/hypotheosis/charts/sandbawks` |
| Description | E2B-compatible Kubernetes sandbox controller |

## Images

Image references for the three server workloads. The images are public
packages on the GitHub Container Registry. An empty `tag` resolves to the
chart's `appVersion`, so a chart version pulls the images it was released with.
Set `tag` only to override that, for example to pin an immutable build.

| Key | Default | Description |
|-----|---------|-------------|
| `images.api.repository` | `ghcr.io/hypotheosis/sandbawks-api` | API gateway image repository. |
| `images.api.tag` | `""` | API gateway image tag. Empty means the chart's `appVersion`. |
| `images.api.pullPolicy` | `IfNotPresent` | API gateway image pull policy. |
| `images.controller.repository` | `ghcr.io/hypotheosis/sandbawks-controller` | Controller image repository. |
| `images.controller.tag` | `""` | Controller image tag. Empty means the chart's `appVersion`. |
| `images.controller.pullPolicy` | `IfNotPresent` | Controller image pull policy. |
| `images.node.repository` | `ghcr.io/hypotheosis/sandbawks-node` | Node DaemonSet image repository. |
| `images.node.tag` | `""` | Node DaemonSet image tag. Empty means the chart's `appVersion`. |
| `images.node.pullPolicy` | `IfNotPresent` | Node DaemonSet image pull policy. |

## api

The `sandbawks-api` gateway Deployment and its Service, RBAC, and optional
Ingress.

| Key | Default | Description |
|-----|---------|-------------|
| `api.enabled` | `true` | Render the api Deployment, Service, and RBAC. |
| `api.replicas` | `2` | Number of gateway replicas. |
| `api.defaultNamespace` | `""` | Default namespace the gateway reads and writes `Sandbox` resources in when a request doesn't pin one (`SANDBAWKS_DEFAULT_NAMESPACE`). Empty uses the release namespace. |
| `api.sandboxDomain` | `""` | Wildcard base domain for E2B hostname-based sandbox selection (`SANDBAWKS_SANDBOX_DOMAIN`), for example `e2b.example.com`. When set, the gateway reads the target sandbox from a `<port>-<sandbox-id>.<domain>` request host, which is the scheme the official E2B SDKs use, and falls back to the `E2b-Sandbox-Id` header. When `api.ingress.enabled` is also `true`, the chart adds a `*.<domain>` Ingress host. Empty means header-only routing. |
| `api.envdSilentParkMaxSecs` | `15` | Maximum seconds the gateway holds a plain `process.Start` call to a sandbox that isn't ready yet before it returns a retry notice (`SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS`). The connection is silent while it waits, so keep this below your ingress or load balancer idle timeout. |
| `api.envdSleepHonorMaxSecs` | `300` | Maximum seconds the gateway honors a leading `sleep <seconds>` in a command sent to a sandbox that isn't ready yet, holding the call with keepalives before signaling the client to resend (`SANDBAWKS_ENVD_SLEEP_HONOR_MAX_SECS`). Because of the keepalives, this can exceed the idle timeout. |
| `api.signingSecret.existingSecret` | `""` | Name of a pre-existing Secret, with a `signing-secret` key, to use instead of the chart-managed one. When blank, the chart generates a secret that changes on every upgrade, which is not suitable for production. |
| `api.service.type` | `ClusterIP` | Service type for the gateway. |
| `api.service.port` | `8080` | Service port for the gateway. |
| `api.ingress.enabled` | `false` | Render an Ingress for the gateway. |
| `api.ingress.className` | `""` | IngressClass to bind, for example `nginx` or `traefik`. Blank uses the cluster default. |
| `api.ingress.annotations` | `{}` | Controller-specific Ingress annotations, for example a TLS issuer, body size, or rewrites. |
| `api.ingress.hosts` | `[{host: sandbawks.example.com, paths: [{path: /, pathType: Prefix}]}]` | Hosts and paths routed to the gateway Service. You don't need to add the `api.sandboxDomain` wildcard host; the chart adds it. |
| `api.ingress.tls` | `[]` | TLS blocks passed verbatim to `Ingress.spec.tls`. Terminating TLS for the `*.<domain>` host needs a wildcard certificate. |
| `api.resources.requests` | `{cpu: 100m, memory: 128Mi}` | Gateway resource requests. |
| `api.resources.limits` | `{cpu: "1", memory: 512Mi}` | Gateway resource limits. |

## controller

The `sandbawks-controller` Deployment and its RBAC.

| Key | Default | Description |
|-----|---------|-------------|
| `controller.enabled` | `true` | Render the controller Deployment and RBAC. |
| `controller.replicas` | `1` | Controller replicas. More than one requires `controller.leaderElection.enabled`; the chart refuses to render otherwise. |
| `controller.logLevel` | `info` | Controller log level. |
| `controller.runtime` | `pod` | Runtime adapter for materializing `Sandbox` resources: `pod` (creates Pods through the API server) or `fake` (in-memory, for tests and development only). |
| `controller.resources.requests` | `{cpu: 100m, memory: 128Mi}` | Controller resource requests. |
| `controller.resources.limits` | `{cpu: "1", memory: 512Mi}` | Controller resource limits. |

### Leader election

With leader election enabled, controller replicas coordinate through a
`coordination.k8s.io/v1` Lease. Only the leader runs the reconcile loops and the
snapshot retention sweep; a standby takes over after the leader stops renewing.
A graceful shutdown doesn't release the Lease, so a takeover can take up to one
`leaseDurationSeconds`. The controller refuses to start unless
`0 < retryPeriodSeconds < renewDeadlineSeconds < leaseDurationSeconds`.

| Key | Default | Description |
|-----|---------|-------------|
| `controller.leaderElection.enabled` | `false` | Enable leader election. Required for `controller.replicas` greater than 1. A single replica works with either setting. |
| `controller.leaderElection.leaseName` | `sandbawks-controller-leader` | Name of the Lease. |
| `controller.leaderElection.leaseDurationSeconds` | `15` | Lease TTL: how long standbys wait before taking over from a silent leader. |
| `controller.leaderElection.renewDeadlineSeconds` | `10` | How long the leader tries to renew before it gives up and exits. |
| `controller.leaderElection.retryPeriodSeconds` | `2` | How often standbys poll and the leader renews. |

### Restore recovery

Bounds for the loop that recovers a resume whose restore Pod is stuck. These
don't control how long the restore shim waits for the node to finish staging;
the shim reads `SANDBAWKS_STAGE_GATE_TIMEOUT_SECS` from its own environment and
defaults to 300 seconds.

| Key | Default | Description |
|-----|---------|-------------|
| `controller.restore.scheduleDeadlineSeconds` | `120` | How long a restore Pod can stay `Pending` (unscheduled) before the controller recreates it without a node pin (`SANDBAWKS_RESUME_SCHEDULE_DEADLINE_SECS`). |
| `controller.restore.readyDeadlineSeconds` | `120` | How long a scheduled restore Pod can stay not ready before the controller treats the restore as stuck and recreates the Pod (`SANDBAWKS_RESUME_READY_DEADLINE_SECS`). |
| `controller.restore.maxRestarts` | `5` | Maximum restore-Pod recreations before the sandbox moves to `Failed` with `failureReason` `resume_exhausted` (`SANDBAWKS_RESUME_MAX_RESTARTS`). |

### Pause recovery

When a pause checkpoint fails, the controller leaves the workload Pod running
and retries. These values bound the retry and clean-up.

| Key | Default | Description |
|-----|---------|-------------|
| `controller.pauseRecovery.maxRetries` | `3` | How many times a failed pause checkpoint is retried, with backoff, before the sandbox moves to `Failed` with `failureReason` `pause_checkpoint_failed` (`SANDBAWKS_PAUSE_MAX_RETRIES`). |
| `controller.pauseRecovery.gcGraceSeconds` | `300` | How long a failed sandbox's Pod can stay unreachable before the controller deletes it to free the node (`SANDBAWKS_FAILED_GC_GRACE_SECS`). A Pod whose `envd` still responds is recovered to `Running` instead. |

### PVC retention

Garbage collection for paused sandboxes whose persistence mode is `Filesystem`.
See [Sandbox](/reference/custom-resources/sandbox/).

| Key | Default | Description |
|-----|---------|-------------|
| `controller.pvcRetention.enabled` | `true` | Delete a paused `Filesystem`-mode sandbox, and with it its PVCs, once it has been paused longer than its retention window (`SANDBAWKS_PVC_RETENTION_ENABLED`). Set to `false` to keep paused PVCs until you delete them. |
| `controller.pvcRetention.defaultDuration` | `P7D` | Retention window (ISO-8601 duration) for a sandbox that doesn't set `spec.pvcRetention` (`SANDBAWKS_PVC_RETENTION_DEFAULT_DURATION`). |

:::caution
PVC retention is enabled by default and deletes the data on a paused sandbox's
PVCs after seven days.
:::

### FQDN egress

A DNS-name entry in a sandbox's `egressAllow` only opens DNS (port 53) on the
sandbox's baseline NetworkPolicy. With FQDN egress enabled, the controller also
creates a Cilium `CiliumNetworkPolicy` with a `toFQDNs` rule for each name, so
connections to the resolved addresses are allowed. Enable it only on a Cilium
cluster with the `CiliumNetworkPolicy` CRD installed; enabling it also grants the
controller write access to `ciliumnetworkpolicies`.

| Key | Default | Description |
|-----|---------|-------------|
| `controller.fqdnEgress.enabled` | `false` | Create a per-sandbox `CiliumNetworkPolicy` for DNS-name egress entries (`SANDBAWKS_FQDN_EGRESS_ENABLED`). |
| `controller.fqdnEgress.dnsNamespace` | `kube-system` | Namespace the cluster DNS resolver runs in. The policy routes the sandbox's DNS through Cilium's DNS proxy to this resolver (`SANDBAWKS_FQDN_DNS_NAMESPACE`). |
| `controller.fqdnEgress.dnsPodLabel` | `k8s-app=kube-dns` | `key=value` Pod label that selects the cluster DNS resolver (`SANDBAWKS_FQDN_DNS_POD_LABEL`). A value without `=` falls back to the default. |

## node

The `sandbawks-node` DaemonSet, which stages `envd`, drives pause and resume, and
transfers snapshots on each node. The containerd and `runsc` path values are the
ones you adjust per platform — see
[Install Sandbawks on Kubernetes](/how-to/install-on-kubernetes) for what to set
on EKS, GKE, AKS, and k3s.

| Key | Default | Description |
|-----|---------|-------------|
| `node.enabled` | `true` | Render the node DaemonSet and its RBAC. |
| `node.resources.requests` | `{cpu: 100m, memory: 128Mi}` | Node resource requests. |
| `node.resources.limits` | `{cpu: "2", memory: 1Gi}` | Node resource limits. |
| `node.tolerations` | `[]` | Tolerations for the DaemonSet. The DaemonSet already tolerates the standard node-condition taints, but not custom pool taints. To run on a tainted pool, such as an AKS Kata pool tainted `kata=true:NoSchedule`, add a matching toleration; otherwise the node never stages `envd` and sandbox Pods for that pool stay `Pending`. |
| `node.nodeSelector` | `{}` | Node selector for the DaemonSet. |
| `node.runtime` | `""` | Sandbox runtime the node pool serves: `""` or `gvisor` (gVisor `runsc` checkpoint and restore), `kata`, or `kata-fc`. With a Kata value, the node drives pause and resume through the Kata VM and checks for the `kata` CRI handler. The value applies to the whole DaemonSet, so a cluster with both gVisor and Kata pools needs one node deployment per pool. |
| `node.runtimeHandler` | `""` | Overrides the CRI handler name the node looks for in `status.runtimeHandlers` before it marks itself ready for sandboxes. Normally derived from `node.runtime` (`runsc` for gVisor, `kata` for Kata). Set it only when the handler differs, for example `kata-cc` for confidential Kata. Must match the `handler` of the sandboxes' RuntimeClass. |
| `node.hostPathContainerdRun` | `/run/containerd` | Host containerd **run** root, bind-mounted at the fixed in-container `/run/containerd`. On k3s and RKE2 set `/run/k3s/containerd`. |
| `node.hostPathContainerdData` | `/var/lib/containerd` | Host containerd **data** root, bind-mounted at `mountPath == hostPath`. Must be a non-empty absolute path. On k3s set `/var/lib/rancher/k3s/agent/containerd`; on RKE2, `/var/lib/rancher/rke2/agent/containerd`. |
| `node.runscRoot` | `/run/containerd/runsc/k8s.io` | In-container `--root` passed to every `runsc checkpoint` and `restore` — where the `runsc` shim keeps container state (`SANDBAWKS_RUNSC_ROOT`). |
| `node.runscRootFallbacks` | `[]` | Fallback `runsc --root` candidates, probed in order after `runscRoot` (`SANDBAWKS_RUNSC_ROOT_FALLBACKS`, colon-joined). |
| `node.runscRootSearchDirs` | `[]` | Containerd run or state roots the node searches to *discover* the `runsc` state root when neither `runscRoot` nor `runscRootFallbacks` holds the container (`SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS`). The preferred, distribution-independent option for k3s and RKE2. |
| `node.extraHostPathMounts` | `[]` | Extra hostPath mounts (`{name, hostPath, mountPath, readOnly?, type?}`; the first three are required) that expose the `runsc` state directories referenced above. Mounted read-write; `type` defaults to `DirectoryOrCreate`. |
| `node.s3Parallelism` | `""` | In-flight chunk concurrency for snapshot transfer (`SANDBAWKS_S3_PARALLELISM`). Empty uses the binary default (8). Higher values raise the DaemonSet's peak memory. |
| `node.s3ChunkMib` | `""` | Per-chunk object size in MiB for snapshot transfer (`SANDBAWKS_S3_CHUNK_MIB`). Empty uses the binary default (4). |
| `node.snapshotValidateLevel` | `""` | Pause restorability validation: `off`, `head`, `sha`, or `full` (`SANDBAWKS_S3_VALIDATE_LEVEL`). Empty uses the binary default, `head`. |
| `node.hostPathSnapshotCache` | `/var/lib/sandbawks/snapshots` | Host path for the snapshot cache. |
| `node.hostPathStagingCache` | `/var/lib/sandbawks/staging` | Host path for the staging root, where a paused sandbox's bundle is materialized before restore. Also sets the node's `SANDBAWKS_STAGING_ROOT` and the controller's `SANDBAWKS_RESTORE_STAGING_ROOT`. Must be an absolute host path the shim can read. |
| `node.hostPathEnvd` | `/var/lib/sandbawks/envd` | Host path the staged static `envd` binary is written to and that every sandbox Pod mounts read-only. **Must survive a node reboot**; otherwise sandbox Pods crash-loop. |
| `node.headroomFraction` | `0.8` | Maximum fraction of the staging filesystem's free disk (and, on the in-process restore path, memory) a snapshot bundle can use (`SANDBAWKS_HEADROOM_FRACTION`). |
| `node.headroomMinFreeBytes` | `1073741824` | Minimum free bytes a bundle must leave (`SANDBAWKS_HEADROOM_MIN_FREE_BYTES`). Default 1 GiB. |

### runsc version management

The node reports each node's host `runsc` release as the
`sandbawks.dev/runsc-version` Node label and a `runsc_version` metric. A resume is
placed only on a node with the `runsc` release the snapshot was taken with.

| Key | Default | Description |
|-----|---------|-------------|
| `node.runscVersion` | `"20260525.0"` | Pinned `runsc` release the node reports drift against and, with `runscAutoReconcile` or `runscAutoUpgrade`, converges to. Must match the release bundled in the node image and the pin in `scripts/install-runsc-k3s.sh`. Accepts `20260525.0` or `release-20260525.0`. Must be quoted; the chart refuses to render an unquoted number. |
| `node.runscAutoReconcile` | `false` | On a node with no running sandboxes whose host `runsc` differs from `runscVersion`, replace `/usr/local/bin/runsc` and `containerd-shim-runsc-v1` with the image's bundled build, without restarting the node's container runtime. Mutually exclusive with `runscAutoUpgrade`. |
| `node.runscReconcile.enabled` | `true` | Enable version reporting, the removal of the gVisor apt source (so the host doesn't follow the rolling channel), and the two host mounts they need. |
| `node.runscReconcile.hostBinDir` | `/usr/local/bin` | Host directory holding the pinned `runsc`, mounted read-write. |
| `node.runscReconcile.aptSourceDir` | `/etc/apt/sources.list.d` | Host apt source directory, mounted read-write. The node removes `gvisor.list` from it. |
| `node.runscReconcile.aptKeyringPath` | `""` | In-container path of a gVisor apt keyring to remove as well. Empty disables keyring removal. Requires mounting the keyring's host directory through `extraHostPathMounts`. |
| `node.runscReconcile.intervalSeconds` | `300` | Reconcile interval in seconds (`SANDBAWKS_RUNSC_RECONCILE_SECS`). |
| `node.runscAutoUpgrade` | `false` | Privileged in-place upgrade: on a node with no running sandboxes that holds the cluster-wide upgrade Lease, run the full host installer through `nsenter`, which rewrites containerd's runtime handler configuration and **restarts k3s**. Requires `runscUpgrade.enabled`. Mutually exclusive with `runscAutoReconcile`. Use `runscAutoReconcile` for a plain binary update. |
| `node.runscUpgrade.enabled` | `false` | Enable the privileged upgrader: the upgrade Lease RBAC, the host installer staging mount, and the upgrade loop. With `runscAutoUpgrade` off, the loop only reports and releases a stale Lease. |
| `node.runscUpgrade.leaseName` | `sandbawks-runsc-upgrade` | Name of the cluster-wide Lease that limits the upgrade to one node at a time (`SANDBAWKS_RUNSC_UPGRADE_LEASE_NAME`). |
| `node.runscUpgrade.leaseDurationSeconds` | `600` | Upgrade Lease TTL, in seconds: the longest the rollout stalls if a node fails mid-upgrade. Must exceed one installer run (`SANDBAWKS_RUNSC_UPGRADE_LEASE_DURATION_SECS`). |
| `node.runscUpgrade.intervalSeconds` | `300` | Upgrade loop interval in seconds (`SANDBAWKS_RUNSC_UPGRADE_SECS`). |
| `node.runscUpgrade.hostStagingDir` | `/opt/sandbawks` | Host directory the node stages the installer into and runs it from. |

### Memory pressure

When enabled, the node watches each sandbox's Pod cgroup. When a sandbox's memory
use reaches `highWaterFraction` of its limit, the node emits a metric and a
warning and saves the sandbox's `envd` state to the snapshot store before the
kernel can OOM-kill it. Enabling it mounts the host `/sys/fs/cgroup` read-only.

| Key | Default | Description |
|-----|---------|-------------|
| `node.memoryPressure.enabled` | `false` | Enable the memory-pressure watcher. |
| `node.memoryPressure.cgroupRoot` | `/sys/fs/cgroup` | Host cgroup v2 root, mounted read-only at the same path. Must match `sandboxPressure.cgroupRoot` when both are enabled. |
| `node.memoryPressure.highWaterFraction` | `0.9` | Fraction of the memory limit, in `(0.0, 1.0]`, at which the watcher reacts. |
| `node.memoryPressure.intervalSeconds` | `5` | Poll interval in seconds. |

### Sandbox pressure telemetry

Per-sandbox resource-pressure logs and OpenTelemetry gauges (CPU, memory, disk,
I/O, network), for debugging slow or failing sandboxes. Off by default; enabling
it restarts the DaemonSet.

| Key | Default | Description |
|-----|---------|-------------|
| `node.sandboxPressure.enabled` | `false` | Enable sandbox pressure telemetry. |
| `node.sandboxPressure.cgroupRoot` | `/sys/fs/cgroup` | Host cgroup v2 root, mounted read-only. Must match `memoryPressure.cgroupRoot` when both are enabled. |
| `node.sandboxPressure.procRoot` | `/proc` | Host `/proc` root, used for per-sandbox network counters. |
| `node.sandboxPressure.checkIntervalMs` | `1000` | Sample interval in milliseconds. |
| `node.sandboxPressure.reportIntervalMs` | `1000` | How often a log line is emitted while a sandbox is under pressure, in milliseconds. Raised to at least `checkIntervalMs`. |
| `node.sandboxPressure.walkIntervalSeconds` | `15` | How often, in seconds, the node fully re-measures each sandbox's disk use. Between measurements, disk use is estimated from written bytes. |
| `node.sandboxPressure.cpuWarnFraction` | `0.85` | CPU usage fraction, in `(0.0, 1.0]`, that triggers a warning. |
| `node.sandboxPressure.memWarnFraction` | `0.85` | Memory usage fraction that triggers a warning. |
| `node.sandboxPressure.diskWarnFraction` | `0.80` | Disk usage fraction that triggers a warning. |
| `node.sandboxPressure.psiWarnPercent` | `25` | Pressure stall information (`some avg10`) percentage, in `(0.0, 100.0]`, that triggers a warning. |
| `node.sandboxPressure.clearRatio` | `0.7` | A warning clears once the signal drops below its threshold times this ratio, in `(0.0, 1.0)`. |

## envd

The default envd-rs agent image staged onto every node and execed by every
sandbox Pod. With node-staged delivery this cluster-wide value is authoritative;
per-template `SandboxTemplate.spec.envdImage` is a no-op.

| Key | Default | Description |
|-----|---------|-------------|
| `envd.image` | `ghcr.io/hypotheosis/envd` | envd image. Prefer a digest pin (`ghcr.io/hypotheosis/envd@sha256:<digest>`) for registry-enforced immutability. |
| `envd.tag` | `0.1.29` | envd release tag (immutable by publisher convention; ignored when the image carries a digest). |
| `envd.pullPolicy` | `IfNotPresent` | envd image pull policy. |

:::note
A paused sandbox's memory image is backed by the staged envd binary, so the
restore node must stage a byte-identical envd. The controller records the
binary's sha256 in the snapshot manifest and fails the restore closed if it
drifts — so a moving tag turns "resume" into "fail and retry", never silent
corruption. Prefer an immutable digest pin.
:::

## RuntimeClasses

| Key | Default | Description |
|-----|---------|-------------|
| `gvisor.enabled` | `true` | Render the cluster-scoped `RuntimeClass: gvisor` (handler `runsc`). The RuntimeClass doesn't install `runsc`; the node DaemonSet marks a node ready for sandboxes only when the node reports the `runsc` handler. Disable where the cluster already defines a `gvisor` RuntimeClass (for example, GKE Sandbox). |
| `sandbawksRunsc.enabled` | `true` | Render the cluster-scoped `RuntimeClass: sandbawks-runsc` (handler `sandbawks-runsc`), backed by `containerd-shim-sandbawks-v1` for filesystem snapshot/restore. Disable where the shim isn't installed on every node. |

## snapshotStore

Object store for snapshots, plus retention GC. Both the node DaemonSet and the
controller read it.

| Key | Default | Description |
|-----|---------|-------------|
| `snapshotStore.provider` | `s3` | Storage backend: `s3` (AWS S3 or any S3-compatible store, configured by the `endpoint`, `bucket`, and `region` keys) or `azure` (Azure Blob Storage, configured by the `azure` keys). |
| `snapshotStore.endpoint` | `""` | S3 endpoint. Blank so a fresh install doesn't talk to a real bucket. |
| `snapshotStore.bucket` | `sandbawks-snapshots` | Snapshot bucket. |
| `snapshotStore.region` | `us-east-1` | Bucket region. |
| `snapshotStore.existingSecret` | `""` | Name of a pre-existing Secret (with `SANDBAWKS_S3_ACCESS_KEY` and `SANDBAWKS_S3_SECRET_KEY` keys) to use instead of the chart-managed one. Blank generates random credentials on first install and preserves them across upgrades. Production should use an externally managed Secret. |
| `snapshotStore.azure.accountName` | `""` | Azure storage account name. |
| `snapshotStore.azure.accountKey` | `""` | Storage account access key. Supply it at install time, for example with `--set-string snapshotStore.azure.accountKey=...`, or use `azure.existingSecret`. Don't commit a real value. |
| `snapshotStore.azure.container` | `sandbawks-snapshots` | Blob container that holds snapshots. |
| `snapshotStore.azure.endpoint` | `""` | Blob endpoint override, for Azurite or sovereign clouds, for example `http://azurite:10000/devstoreaccount1`. Empty uses public Azure. |
| `snapshotStore.azure.existingSecret` | `""` | Name of a pre-existing Secret with a `SANDBAWKS_AZURE_ACCOUNT_KEY` key. Blank renders a Secret from `azure.accountKey`. |
| `snapshotStore.retention.enabled` | `false` | Master switch for the controller's snapshot retention GC. `false` means objects accumulate until you opt in. |
| `snapshotStore.retention.defaultDuration` | `P7D` | Chart-wide default retention window (ISO-8601 duration) when the Sandbox and namespace don't specify one. |
| `snapshotStore.retention.scanInterval` | `PT1H` | How often the controller runs a retention sweep (ISO-8601 duration). |

## usage

Per-API-key usage metering. Usage data goes to the `snapshotStore` bucket, under
separate key prefixes; enabling it gives the controller the snapshot-store
credentials. Usage metering supports S3-compatible stores only; it doesn't support
`snapshotStore.provider: azure`.

| Key | Default | Description |
|-----|---------|-------------|
| `usage.metering.enabled` | `false` | Write one NDJSON usage event, with resource-weighted seconds per team and API key, for each completed `Running` interval of a sandbox. |
| `usage.rollup.enabled` | `false` | Periodically roll the events up into per-team, per-key, per-hour usage records. The rollup is idempotent. Enable `usage.metering.enabled` too; otherwise there are no events to roll up. |
| `usage.rollup.interval` | `PT1H` | How often the rollup runs (ISO-8601 duration). Records are bucketed by hour regardless of this interval. |
| `usage.eventPrefix` | `usage` | Key prefix the usage events are written under and read from. |
| `usage.ledgerPrefix` | `ledger` | Key prefix the rolled-up usage records are written under. |
| `usage.spill.enabled` | `false` | When an event still can't be written after its retries, save it to a PVC and replay it later, instead of dropping it. Without the spill, events that close during a long object-store outage are dropped and counted in the `controller.usage.events_total{outcome="dropped"}` metric. |
| `usage.spill.mountPath` | `/var/lib/sandbawks/usage-spill` | Mount path of the spill PVC in the controller container (`SANDBAWKS_USAGE_SPILL_DIR`). |
| `usage.spill.size` | `1Gi` | Spill PVC size. |
| `usage.spill.storageClassName` | `""` | StorageClass for the spill PVC. Empty uses the cluster default. |
| `usage.spill.accessModes` | `[ReadWriteOnce]` | Spill PVC access modes. With more than one controller replica, include `ReadWriteMany`; the chart refuses to render otherwise. |
| `usage.spill.drainInterval` | `PT1M` | How often saved events are replayed (ISO-8601 duration). |

## observability

OpenTelemetry export for the api Deployment, controller Deployment, and node
DaemonSet, plus each sandbox's `envd`, which the controller configures. When
disabled (the default), each binary logs to stderr only and never attempts an
export.

| Key | Default | Description |
|-----|---------|-------------|
| `observability.enabled` | `false` | Enable OTLP export. When off, the chart sets `OTEL_SDK_DISABLED=true` on every workload. |
| `observability.endpoint` | `""` | OTLP base URL. Must be a plaintext target; terminate TLS in a collector. With `observability.collector.enabled`, leave it blank: workloads default to the in-namespace collector. |
| `observability.protocol` | `http/protobuf` | Wire protocol: `http/protobuf` or `grpc`. |
| `observability.sampler` | `parentbased_always_on` | Sampling strategy. |
| `observability.samplerArg` | `"1.0"` | Sampling ratio (used only by `*_traceidratio` strategies). |
| `observability.resourceAttributes` | `""` | Extra resource attributes, comma-separated `key=value`. |
| `observability.timeoutMs` | `""` | Per-export timeout in ms. Blank leaves the SDK default (about 10 seconds). |
| `observability.captureGenaiContent` | `false` | Record the redacted command line of each command run in a sandbox as the `gen_ai.tool.call.arguments` attribute on `envd`'s `execute_tool` spans. The redaction masks known secret patterns but isn't guaranteed to catch every secret. Environment variable values are never recorded. |
| `observability.existingHeadersSecret` | `""` | Name of a Secret with an `otlp-headers` key (the preferred way to supply OTLP auth headers). Leave blank with the collector enabled. |
| `observability.headers` | `""` | Inline OTLP auth headers, comma-separated `key=value`. Lands in the rendered manifest and release history; don't use for real secrets. Leave blank with the collector enabled. |

### Collector

An in-namespace [Vector](https://vector.dev/) Deployment and Service that receive
plaintext OTLP from every workload, including each sandbox's `envd`, and forward
it to your OTLP backend with a bearer token that only the collector holds.
`envd` runs in customer-controlled Pods and is never given the token.

The collector Service listens on ports 4317 and 4318 and accepts traffic from any
Pod in the release namespace, including sandbox Pods.

| Key | Default | Description |
|-----|---------|-------------|
| `observability.collector.enabled` | `false` | Deploy the collector. |
| `observability.collector.image.repository` | `docker.io/timberio/vector` | Collector image repository. |
| `observability.collector.image.tag` | `"0.56.0-debian"` | Collector image tag. |
| `observability.collector.image.pullPolicy` | `IfNotPresent` | Collector image pull policy. |
| `observability.collector.replicas` | `1` | Collector replicas. |
| `observability.collector.logLevel` | `info` | Vector log level. Not passed to the rendered configuration; set it through the image if you need to. |
| `observability.collector.bufferMaxEvents` | `1000` | In-memory buffer size, in events, for each of the three forwarding sinks (traces, metrics, logs). When a buffer is full, for example while the backend is unreachable, the newest events are dropped. |
| `observability.collector.resources.requests` | `{cpu: 50m, memory: 64Mi}` | Collector resource requests. |
| `observability.collector.resources.limits` | `{cpu: "1", memory: 256Mi}` | Collector resource limits. |
| `observability.collector.exporter.endpoint` | `""` | OTLP/HTTP base URL of the backend. The collector appends `/v1/traces`, `/v1/metrics`, and `/v1/logs`. |
| `observability.collector.exporter.existingTokenSecret` | `""` | Name of a Secret with a `token` key: the bearer token the collector sends to the backend. Preferred over `token`. |
| `observability.collector.exporter.token` | `""` | Inline bearer token. Lands in the rendered release; use only for development. Required when the collector is enabled and `existingTokenSecret` is blank. |
| `observability.collector.prometheus.enabled` | `false` | Also expose the received metrics for Prometheus to scrape, at `:<port>/metrics`, with a Service port and `prometheus.io/scrape` Pod annotations. |
| `observability.collector.prometheus.port` | `9090` | Port of the Prometheus scrape endpoint. |
| `observability.collector.splitPipelines.enabled` | `false` | Separate pipelines for sandbox and internal telemetry. Not yet supported with the Vector collector: the chart refuses to render when it is `true`. |
| `observability.collector.splitPipelines.sandboxServiceNamePrefix` | `"sandbox/"` | Prefix for the `service.name` of sandbox telemetry in the split pipeline. |
| `observability.collector.k8sattributes.enabled` | `false` | Add Kubernetes Pod attributes to sandbox telemetry. Not yet supported with the Vector collector: the chart refuses to render when it is `true`. |
| `observability.collector.networkPolicy.enabled` | `false` | Restrict ingress to the collector Service to sandbox Pods and Sandbawks Pods. |

## installCrds

| Key | Default | Description |
|-----|---------|-------------|
| `installCrds` | `true` | Documented intent: install CRDs as part of the chart. |

:::caution
This value is currently **not consumed by any template** — the chart ships no
CRD resources. Apply the CRDs out-of-band before installing the chart, as shown
in [Install Sandbawks on Kubernetes](/how-to/install-on-kubernetes#1-apply-the-crds):

```sh
kubectl apply -f deploy/crds
```
:::
