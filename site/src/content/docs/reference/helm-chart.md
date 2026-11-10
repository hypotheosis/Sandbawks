---
title: Helm chart
description: Reference for the Sandbawks Helm chart — chart identity and every value that can be set, with defaults, across images, the api, controller, node DaemonSet, envd, runtime classes, snapshot store, and observability.
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
| `api.defaultNamespace` | `""` | Default namespace the gateway reads/writes `Sandbox` CRs in when a request doesn't pin one (`SANDBAWKS_DEFAULT_NAMESPACE`). Empty tracks the release namespace. |
| `api.signingSecret.existingSecret` | `""` | Name of a pre-existing Secret (with a `signing-secret` key) to use instead of the chart-managed one. Blank lets the chart generate one, which rotates on every upgrade — fine for MVP, not production. |
| `api.service.type` | `ClusterIP` | Service type for the gateway. |
| `api.service.port` | `8080` | Service port for the gateway. |
| `api.ingress.enabled` | `false` | Render an Ingress for the gateway. |
| `api.ingress.className` | `""` | IngressClass to bind (e.g. `nginx`, `traefik`). Blank uses the cluster default. |
| `api.ingress.annotations` | `{}` | Controller-specific Ingress annotations (TLS issuer, body size, rewrites). |
| `api.ingress.hosts` | `[{host: sandbawks.example.com, paths: [{path: /, pathType: Prefix}]}]` | Hosts and paths routed to the gateway Service. |
| `api.ingress.tls` | `[]` | TLS blocks passed verbatim to `Ingress.spec.tls`. |
| `api.resources.requests` | `{cpu: 100m, memory: 128Mi}` | Gateway resource requests. |
| `api.resources.limits` | `{cpu: "1", memory: 512Mi}` | Gateway resource limits. |

## controller

The `sandbawks-controller` Deployment and its RBAC.

| Key | Default | Description |
|-----|---------|-------------|
| `controller.enabled` | `true` | Render the controller Deployment and RBAC. |
| `controller.replicas` | `1` | Controller replicas. Leader-elected; can scale for HA. |
| `controller.logLevel` | `info` | Controller log level. |
| `controller.runtime` | `pod` | Runtime adapter for materializing `Sandbox` CRs: `pod` (real, issues Pods against the API server) or `fake` (in-memory, test/dev only). |
| `controller.resources.requests` | `{cpu: 100m, memory: 128Mi}` | Controller resource requests. |
| `controller.resources.limits` | `{cpu: "1", memory: 512Mi}` | Controller resource limits. |

## node

The `sandbawks-node` DaemonSet, which drives `runsc checkpoint`/`restore` and
snapshot transfer on each node. The containerd and `runsc` path values are the
ones you adjust per platform — see
[Install Sandbawks on Kubernetes](/how-to/install-on-kubernetes) for what to set
on EKS, GKE, AKS, and k3s.

| Key | Default | Description |
|-----|---------|-------------|
| `node.enabled` | `true` | Render the node DaemonSet and its RBAC. |
| `node.resources.requests` | `{cpu: 100m, memory: 128Mi}` | Node resource requests. |
| `node.resources.limits` | `{cpu: "2", memory: 1Gi}` | Node resource limits. |
| `node.hostPathContainerdRun` | `/run/containerd` | Host containerd **run** root, bind-mounted at the fixed in-container `/run/containerd`. On k3s/RKE2 set `/run/k3s/containerd`. |
| `node.hostPathContainerdData` | `/var/lib/containerd` | Host containerd **data** root, bind-mounted at `mountPath == hostPath`. Must be a non-empty absolute path. On k3s set `/var/lib/rancher/k3s/agent/containerd`. |
| `node.runscRoot` | `/run/containerd/runsc/k8s.io` | In-container `--root` passed to every `runsc checkpoint`/`restore` — where the `runsc` shim keeps container state (`SANDBAWKS_RUNSC_ROOT`). |
| `node.runscRootFallbacks` | `[]` | Fallback `runsc --root` candidates, probed in order after `runscRoot` (`SANDBAWKS_RUNSC_ROOT_FALLBACKS`, colon-joined). |
| `node.runscRootSearchDirs` | `[]` | Containerd run/state roots the node searches to *discover* the `runsc` state root when neither `runscRoot` nor `runscRootFallbacks` holds the container (`SANDBAWKS_RUNSC_ROOT_SEARCH_DIRS`). The preferred, distro-agnostic option for k3s/RKE2. |
| `node.extraHostPathMounts` | `[]` | Extra hostPath mounts (`{name, hostPath, mountPath, readOnly?, type?}`; first three required) to expose the runsc-state subtrees referenced above. Mounted RW; `type` defaults to `DirectoryOrCreate`. |
| `node.s3Parallelism` | `""` | In-flight chunk concurrency for snapshot transfer (`SANDBAWKS_S3_PARALLELISM`). Empty → binary default (8). |
| `node.s3ChunkMib` | `""` | Per-chunk object size in MiB for snapshot transfer (`SANDBAWKS_S3_CHUNK_MIB`). Empty → binary default (4). |
| `node.snapshotValidateLevel` | `""` | Pause restorability validation: `off` \| `head` \| `sha` \| `full` (`SANDBAWKS_S3_VALIDATE_LEVEL`). Empty → binary default `head`. |
| `node.hostPathSnapshotCache` | `/var/lib/sandbawks/snapshots` | Host path for the snapshot cache. |
| `node.hostPathStagingCache` | `/var/lib/sandbawks/staging` | Host path for the staging root where a paused sandbox's bundle is materialized before restore. Must be a hostPath the shim can read. |
| `node.hostPathEnvd` | `/var/lib/sandbawks/envd` | Host path the staged static envd binary is written to and every sandbox Pod mounts read-only. **Must survive node reboot** (otherwise sandbox Pods CrashLoop). |
| `node.headroomFraction` | `0.8` | Max fraction of staging-filesystem disk (and, on the in-process restore path, memory) a snapshot bundle may consume (`SANDBAWKS_HEADROOM_FRACTION`). |
| `node.headroomMinFreeBytes` | `1073741824` | Minimum free bytes a bundle must leave (`SANDBAWKS_HEADROOM_MIN_FREE_BYTES`). Default 1 GiB. |

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
| `gvisor.enabled` | `true` | Render the cluster-scoped `RuntimeClass: gvisor` (handler `runsc`). Disable where `runsc` isn't installed on every node, or where the cluster already defines a `gvisor` RuntimeClass (e.g. GKE Sandbox). |
| `sandbawksRunsc.enabled` | `true` | Render the cluster-scoped `RuntimeClass: sandbawks-runsc` (handler `sandbawks-runsc`), backed by `containerd-shim-sandbawks-v1` for filesystem snapshot/restore. Disable where the shim isn't installed on every node. |

## snapshotStore

S3-compatible object store for snapshots, plus retention GC.

| Key | Default | Description |
|-----|---------|-------------|
| `snapshotStore.endpoint` | `""` | S3 endpoint. Blank so a fresh install doesn't talk to a real bucket. |
| `snapshotStore.bucket` | `sandbawks-snapshots` | Snapshot bucket. |
| `snapshotStore.region` | `us-east-1` | Bucket region. |
| `snapshotStore.existingSecret` | `""` | Name of a pre-existing Secret (with `SANDBAWKS_S3_ACCESS_KEY` and `SANDBAWKS_S3_SECRET_KEY` keys) to use instead of the chart-managed one. Blank generates random credentials on first install and preserves them across upgrades. Production should pin an externally-managed Secret. |
| `snapshotStore.retention.enabled` | `false` | Master switch for the controller's snapshot retention GC. `false` means objects accumulate until you opt in. |
| `snapshotStore.retention.defaultDuration` | `P7D` | Chart-wide default retention window (ISO-8601 duration) when the Sandbox/namespace don't specify one. |
| `snapshotStore.retention.scanInterval` | `PT1H` | How often the controller runs a retention sweep (ISO-8601 duration). |

## observability

OpenTelemetry export for the api Deployment, controller Deployment, and node
DaemonSet. When disabled (the default), each binary short-circuits to stderr
logging and never attempts an export.

| Key | Default | Description |
|-----|---------|-------------|
| `observability.enabled` | `false` | Enable OTLP export. When off, the chart sets `OTEL_SDK_DISABLED=true` on every workload. |
| `observability.endpoint` | `""` | OTLP base URL. Must be a plaintext target (route TLS via an in-cluster collector). |
| `observability.protocol` | `http/protobuf` | Wire protocol: `http/protobuf` or `grpc`. |
| `observability.sampler` | `parentbased_always_on` | Sampling strategy. |
| `observability.samplerArg` | `"1.0"` | Sampling ratio (used only by `*_traceidratio` strategies). |
| `observability.resourceAttributes` | `""` | Extra resource attributes, comma-separated `key=value`. |
| `observability.timeoutMs` | `""` | Per-export timeout in ms. Blank leaves the SDK default (~10s). |
| `observability.existingHeadersSecret` | `""` | Name of a Secret with an `otlp-headers` key (preferred way to supply OTLP auth headers). |
| `observability.headers` | `""` | Inline OTLP auth headers, comma-separated `key=value`. Lands in the rendered manifest and release history; don't use for real secrets. |

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
