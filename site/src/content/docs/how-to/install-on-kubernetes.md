---
title: Install Sandbawks on Kubernetes
description: Install the Sandbawks Helm chart on Amazon EKS, Google GKE, Azure AKS, k3s, or kind — prerequisites per platform, applying the CRDs, and running the chart.
slug: how-to/install-on-kubernetes
head:
  - tag: title
    content: Install Sandbawks on Kubernetes · Sandbawks docs
tableOfContents: true
---

This guide installs Sandbawks into an existing Kubernetes cluster with the Helm
chart. It covers the prerequisites that every cluster needs, the extra
per-platform setup for Amazon EKS, Google GKE, Azure AKS, and k3s, and the
install steps themselves.

It assumes you operate the target cluster and can install node-level software
(a container runtime handler) on it. For the full list of chart settings, see
the [Helm chart reference](/reference/helm-chart).

## Before you begin

Sandbawks runs three server workloads — the `sandbawks-api` gateway and the
`sandbawks-controller` as Deployments, and `sandbawks-node` as a DaemonSet on
every node that runs sandboxes (see [the components](/explanation/components)).
The DaemonSet talks to the node's container runtime directly, so the cluster has
requirements beyond "can schedule a Pod".

Confirm each of these before you install:

- **A containerd-based cluster.** The `sandbawks-node` DaemonSet runs with
  `hostPID` and bind-mounts the node's containerd run and data roots to drive
  `runsc checkpoint` / `runsc restore`. Other CRI runtimes are not supported.
- **gVisor (`runsc`) installed on every sandbox node.** Sandboxes run under
  gVisor for isolation. The chart renders a cluster-scoped `RuntimeClass` named
  `gvisor` (handler `runsc`) and the controller stamps `runtimeClassName: gvisor`
  onto sandbox Pods. **Installing `runsc` on your nodes is your responsibility —
  the chart does not do it.** Without `runsc` on a node, sandbox Pods stay
  `Pending` with `RuntimeClassNotFound`.
- **The `sandbawks-runsc` shim on every sandbox node, if you want pause/resume.**
  Filesystem snapshot and restore route through the `containerd-shim-sandbawks-v1`
  shim, which registers a `sandbawks-runsc` handler. The chart renders a matching
  `sandbawks-runsc` RuntimeClass. If the shim is not installed cluster-wide, set
  `sandbawksRunsc.enabled=false`.
- **Helm 3 and `kubectl`,** both configured against the target cluster.
- **Persistent, node-local storage** for the DaemonSet's host paths: the staged
  envd binary (`node.hostPathEnvd`, default `/var/lib/sandbawks/envd`), the
  snapshot cache (`node.hostPathSnapshotCache`), and the staging cache
  (`node.hostPathStagingCache`).

  :::caution
  `node.hostPathEnvd` **must survive a node reboot.** A node advertises
  readiness through a label stored in etcd that persists across reboots; if the
  envd binary lives on ephemeral storage (tmpfs, instance-store) it vanishes on
  reboot while the label lingers, and sandbox Pods scheduled there CrashLoop. Use
  node-local persistent storage.
  :::
- **An S3-compatible object store** (endpoint, bucket, credentials) for
  snapshots, if you use pause/resume. You configure it through `snapshotStore.*`
  at install time.

### Find your node's containerd paths

Most per-platform setup comes down to one question: where does *this* cluster's
containerd keep its run root, data root, and gVisor (`runsc`) state? The chart
defaults match stock containerd; managed distributions relocate these paths.

On a node, find the containerd socket (its directory is the run root):

```sh
ls -l /run/*/containerd/containerd.sock
```

To find where the gVisor shim writes `runsc` state for a running sandbox, take a
sandbox container's ID and search for its state directory:

```sh
find /run /var/lib -maxdepth 5 -name '<container-id>' -type d
```

You set the matching `node.*` values (`hostPathContainerdRun`,
`hostPathContainerdData`, and the `runscRoot*` keys) from what these commands
report. The platform sections below tell you what to expect.

## Amazon EKS

EKS managed node groups run containerd. On the stock Amazon Linux node layout the
containerd run and data roots match the chart defaults
(`/run/containerd`, `/var/lib/containerd`), so the default `node.*` paths
generally apply.

:::caution
EKS does **not** ship gVisor. You must install `runsc` on every sandbox node
yourself (for example through a custom AMI or a node bootstrap step) before
sandbox Pods can schedule. Verify the containerd and `runsc` state paths on a
real node with the [discovery commands above](#find-your-nodes-containerd-paths)
rather than assuming the defaults — the layout can change between AMI versions
and was not verified against EKS for this guide.
:::

## Google GKE

GKE offers [GKE Sandbox](https://cloud.google.com/kubernetes-engine/docs/concepts/sandbox-pods),
a managed gVisor that you enable per node pool. When it is enabled, GKE already
defines a `gvisor` RuntimeClass on the cluster.

:::caution
Because GKE may already provide a `gvisor` RuntimeClass, the chart-rendered one
can collide with it. If you rely on GKE Sandbox, consider installing with
`gvisor.enabled=false` so the chart does not manage that RuntimeClass.

GKE nodes (Container-Optimized OS) place containerd's run and data roots, and the
`runsc` state root, at paths that were not verified against GKE for this guide.
Confirm them on a node with the
[discovery commands above](#find-your-nodes-containerd-paths) and set the
`node.*` values accordingly. The `sandbawks-runsc` shim for pause/resume is
separate from GKE Sandbox's gVisor and is not provided by GKE.
:::

## Azure AKS

AKS node pools run containerd.

:::caution
AKS does not provide a managed gVisor, so you must install `runsc` (and, for
pause/resume, the `sandbawks-runsc` shim) on every sandbox node yourself. The
containerd run/data roots and the `runsc` state path on AKS nodes were not
verified for this guide — confirm them with the
[discovery commands above](#find-your-nodes-containerd-paths) and set the
`node.*` values to match.
:::

## k3s

k3s embeds its own containerd, so several paths differ from stock containerd.
The settings below are derived from a k3s node verified on 2026-06-07.

On k3s:

- The containerd CRI socket and task state live under **`/run/k3s/containerd`**,
  and the data root under **`/var/lib/rancher/k3s/agent/containerd`**.
- The runc/`runsc` shims, however, keep their OCI runtime root at the **stock
  `/run/containerd`**. The gVisor shim writes `runsc` state as flat files at
  `/run/containerd/runsc/k8s.io/<cid>_sandbox:<sandbox-id>.state` — a path the
  k3s run-root mount does not cover.

Because the `runsc` state lives outside the k3s run root, point the DaemonSet at
the k3s containerd roots **and** expose the stock `/run/containerd` so the node
can discover the `runsc` state. Add this to your values file:

```yaml
node:
  # k3s relocates containerd's CRI socket and data root.
  hostPathContainerdRun: /run/k3s/containerd
  hostPathContainerdData: /var/lib/rancher/k3s/agent/containerd
  # The gVisor shim still writes runsc state under stock /run/containerd, which
  # the run-root mount above does not cover. Expose it and let the node discover
  # the runsc state root wherever the shim placed it.
  runscRootSearchDirs:
    - /run/host-containerd
  extraHostPathMounts:
    - name: host-containerd
      hostPath: /run/containerd        # stock containerd run root (NOT /run/k3s/containerd)
      mountPath: /run/host-containerd  # a runscRootSearchDirs entry
```

`runscRootSearchDirs` makes the node recursively find the
`<root>/<container-id>` state directory under the mounted path, so you don't have
to hard-code the exact `runsc/<namespace>` suffix. Confirm the real host path on
a node if pause fails:

```sh
find /run /var/lib -maxdepth 5 -name '<container-id>' -type d
```

:::note
The same `runscRootSearchDirs` + `extraHostPathMounts` pattern applies to RKE2
and other embedded-containerd distributions; only the host source paths change.
:::

## kind

[kind](https://kind.sigs.k8s.io/) (Kubernetes IN Docker) runs each cluster node
as a Docker container, so enabling gVisor and pause/resume takes three steps that
managed clusters skip: making the runtime binaries available *inside* the node
containers, configuring containerd to register the runtime handlers, and pointing
the Helm chart at those handlers.

### Install runtime binaries on the Docker host

The kind node containers mount binaries from the host, so install all three
runtime binaries on the Docker host before creating the cluster.

**gVisor** (`runsc` and its containerd shim) — download from the
[gVisor releases page](https://gvisor.dev/docs/user_guide/install/):

```sh
ARCH=$(uname -m)
GVISOR_URL="https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}"
cd /tmp
curl -fsSL "${GVISOR_URL}/runsc" -o runsc
curl -fsSL "${GVISOR_URL}/runsc.sha512" -o runsc.sha512
sha512sum -c runsc.sha512
curl -fsSL "${GVISOR_URL}/containerd-shim-runsc-v1" -o containerd-shim-runsc-v1
curl -fsSL "${GVISOR_URL}/containerd-shim-runsc-v1.sha512" -o containerd-shim-runsc-v1.sha512
sha512sum -c containerd-shim-runsc-v1.sha512
sudo install -o root -g root -m 0755 runsc /usr/local/bin/runsc
sudo install -o root -g root -m 0755 containerd-shim-runsc-v1 /usr/local/bin/containerd-shim-runsc-v1
```

On Debian or Ubuntu you can use the gVisor apt repository instead — but note that
the apt package places binaries at `/usr/bin/`, not `/usr/local/bin/`, so adjust
the `extraMounts` `hostPath` values in the cluster config below to match.

**Sandbawks pause/resume shim** (`containerd-shim-sandbawks-v1`) — download it
from the project's GitHub releases:

```sh
SHIM_URL="https://github.com/hypotheosis/sandbawks/releases/download/shim-latest"
cd /tmp
curl -fsSL "${SHIM_URL}/containerd-shim-sandbawks-v1" -o containerd-shim-sandbawks-v1
curl -fsSL "${SHIM_URL}/containerd-shim-sandbawks-v1.sha512" -o containerd-shim-sandbawks-v1.sha512
sha512sum -c containerd-shim-sandbawks-v1.sha512
sudo install -o root -g root -m 0755 containerd-shim-sandbawks-v1 /usr/local/bin/containerd-shim-sandbawks-v1
```

For production clusters, replace `shim-latest` in the URL with a specific
release tag (for example `shim-sha-e9707301`) to pin the binary version. The
[releases page](https://github.com/hypotheosis/sandbawks/releases) lists them.

### Create a gVisor-capable kind cluster

Save the following as `kind-sandbawks.yaml`. It mounts all three runtime binaries
into every node and uses `containerdConfigPatches` to register both the `runsc`
and `sandbawks-runsc` handlers in every node's containerd config at cluster-create
time — no post-create restarts needed:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
    runtime_type = "io.containerd.runsc.v1"
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.sandbawks-runsc]
    runtime_type = "io.containerd.sandbawks.v1"
nodes:
- role: control-plane
  extraMounts:
  - hostPath: /usr/local/bin/runsc
    containerPath: /usr/local/bin/runsc
    readOnly: true
  - hostPath: /usr/local/bin/containerd-shim-runsc-v1
    containerPath: /usr/local/bin/containerd-shim-runsc-v1
    readOnly: true
  - hostPath: /usr/local/bin/containerd-shim-sandbawks-v1
    containerPath: /usr/local/bin/containerd-shim-sandbawks-v1
    readOnly: true
- role: worker
  extraMounts:
  - hostPath: /usr/local/bin/runsc
    containerPath: /usr/local/bin/runsc
    readOnly: true
  - hostPath: /usr/local/bin/containerd-shim-runsc-v1
    containerPath: /usr/local/bin/containerd-shim-runsc-v1
    readOnly: true
  - hostPath: /usr/local/bin/containerd-shim-sandbawks-v1
    containerPath: /usr/local/bin/containerd-shim-sandbawks-v1
    readOnly: true
```

Create the cluster:

```sh
kind create cluster --name sandbawks-dev --config kind-sandbawks.yaml
```

Add worker nodes to the `nodes` list if you want more than one sandbox node.

### Verify gVisor

Before installing Sandbawks, confirm the `runsc` handler works:

```sh
kubectl apply -f - <<'EOF'
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor-smoke
handler: runsc
---
apiVersion: v1
kind: Pod
metadata:
  name: gvisor-smoke
spec:
  runtimeClassName: gvisor-smoke
  containers:
  - name: uname
    image: busybox
    command: ["uname", "-r"]
  restartPolicy: Never
EOF

kubectl wait --for=condition=Succeeded pod/gvisor-smoke --timeout=60s
kubectl logs gvisor-smoke
kubectl delete pod gvisor-smoke
kubectl delete runtimeclass gvisor-smoke
```

The pod log shows a gVisor kernel version string (for example `4.4.0`) instead of
your host kernel. If the pod stays `Pending` with `RuntimeClassNotFound`, the
`runsc` handler was not registered — re-check that both binaries are present on
the host path and that the cluster was created with the config above.

### Helm values for kind

kind nodes use the stock containerd socket and data root
(`/run/containerd`, `/var/lib/containerd`), so the `node.*` path overrides
required for k3s and other embedded-containerd distributions are not needed.

The only common adjustments are the API service type (kind clusters have no
cloud load-balancer controller, so a `LoadBalancer` Service stays `Pending`) and
enabling the Sandbawks pause/resume shim you installed above:

```yaml
api:
  service:
    type: NodePort
sandbawksRunsc:
  enabled: true
```

The cluster is now gVisor-capable. Continue with [Install Sandbawks](#install-sandbawks) below.

## Install Sandbawks

The chart is published as an OCI artifact at
`oci://ghcr.io/hypotheosis/charts/sandbawks`, and its images are public packages
under `ghcr.io/hypotheosis`. You don't need `helm repo add` or registry
credentials. The CRDs ship separately, in the source repository. Clone it at the
release tag that matches the chart version:

```sh
git clone --branch v0.1.29 --depth 1 https://github.com/hypotheosis/sandbawks
cd sandbawks
```

The CRDs live at `deploy/crds`.

### 1. Apply the CRDs

The chart does **not** install CRDs (Helm 3 does not upgrade chart-managed CRDs,
so they are kept out of the chart). Apply the five `sandbawks.dev` CRDs
out-of-band first:

```sh
kubectl apply -f deploy/crds
```

This installs `Sandbox`, `SandboxTemplate`, `SandboxWarmPool`, `SnapshotJob`, and
`TeamAPIKey`. See the [custom resource reference](/reference/custom-resources/)
for what each one is.

### 2. Write a values file

The chart's image defaults already point at the public packages, and an empty
tag resolves to the chart's `appVersion`. These are the images a chart version
pulls:

| Workload | Image |
|----------|-------|
| API gateway | `ghcr.io/hypotheosis/sandbawks-api:<chart appVersion>` |
| Controller | `ghcr.io/hypotheosis/sandbawks-controller:<chart appVersion>` |
| Node DaemonSet | `ghcr.io/hypotheosis/sandbawks-node:<chart appVersion>` |
| envd agent | `ghcr.io/hypotheosis/envd:0.1.29` (set by `envd.tag`) |

Override `images.*` only if you mirror the images into your own registry. At
minimum, configure the snapshot store and add the platform overrides from the
section above. A starting point:

```yaml
# Only if you mirror the images; otherwise leave images.* at the defaults.
# images:
#   api:        { repository: <your-registry>/sandbawks-api }
#   controller: { repository: <your-registry>/sandbawks-controller }
#   node:       { repository: <your-registry>/sandbawks-node }

# Snapshot store (required for pause/resume).
snapshotStore:
  endpoint: https://<your-s3-endpoint>
  bucket: sandbawks-snapshots
  region: us-east-1

# Plus any node.* overrides for your platform (see the sections above).
```

See the [Helm chart reference](/reference/helm-chart) for every setting,
including ingress, observability, snapshot retention, and resource limits.

### 3. Install the chart

```sh
helm upgrade --install sandbawks oci://ghcr.io/hypotheosis/charts/sandbawks \
  --version 0.1.29 \
  --namespace sandbawks --create-namespace \
  --skip-crds \
  --values my-values.yaml
```

`--skip-crds` keeps Helm from touching CRDs, which you applied in step 1.
`upgrade --install` makes re-runs idempotent.

### 4. Verify the install

```sh
kubectl get pods -n sandbawks
```

Expect the `sandbawks-api` and `sandbawks-controller` Deployments running, and
one `sandbawks-node` DaemonSet Pod per sandbox node. If a sandbox you create
later stays `Pending` with `RuntimeClassNotFound`, `runsc` is missing on that
node; if pauses fail with a `runsc checkpoint ... file does not exist` error,
the `runsc` state path is wrong — revisit your `node.runscRoot*` values with the
[discovery commands](#find-your-nodes-containerd-paths).

## Next steps

- [Point your agent at Sandbawks](/tutorials/point-your-agent-at-sandbawks) to
  make a first request against the gateway.
- [Helm chart reference](/reference/helm-chart) for the full list of values.
