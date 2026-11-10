# Getting started with sandbawks

`sandbawks` is an E2B-compatible sandbox control plane for Kubernetes. It
lets the official [E2B SDKs](https://e2b.dev) (Python / JS) drive
short-lived, gVisor-isolated sandboxes on your own cluster instead of
e2b.dev. This guide takes you from a bare machine to a deployed system
and a green end-to-end check using the **Python E2B SDK** as the probe.

> Read the prerequisites section in full and run the validation block
> **before** you attempt any deployment. Most failed installs trace back
> to a missing gVisor runtime class, a cluster older than 1.31, or a
> snapshot store that was never configured. Validating up front turns a
> 20-minute debugging session into a 30-second check.

## What gets deployed

| Component | Crate | Role |
| --- | --- | --- |
| API gateway | `sandbawks-api` | E2B-compatible HTTP/WS gateway; listens on `:8080`. The SDK talks to this. |
| Controller | `sandbawks-controller` | Reconciles `Sandbox` / `SandboxTemplate` CRs into Pods. |
| Node agent | `sandbawks-node` | Per-node DaemonSet; drives gVisor checkpoint/restore and S3 snapshot streaming (pause/resume). |
| envd | `envd` | In-sandbox agent (gRPC on `49983`) the gateway proxies to for exec / filesystem. |

Five CRDs back the system (`deploy/crds/`): `Sandbox`,
`SandboxTemplate`, `SandboxWarmPool`, `SnapshotJob`, `TeamAPIKey`.

---

## 1. Prerequisites

### 1a. Host tooling

You need these on the machine you deploy *from*. Versions below are the
hard minimums enforced by the smoke gate (`docs/kind-smoke.md`).

| Tool | Minimum | Why |
| --- | --- | --- |
| Docker | engine running | Builds component images; kind needs the daemon. |
| [kind](https://kind.sigs.k8s.io/) | 0.24 | First release with `kindest/node:v1.31.0`. (local cluster only) |
| kubectl | 1.31 | The smoke scripts use the jsonpath form of `kubectl wait`. |
| [helm](https://helm.sh/) | 3.16 | Stable `--wait --timeout` semantics used by the install. |
| [just](https://github.com/casey/just) | any | Recipe runner for every build/deploy task. |
| Rust toolchain | 1.88 | Builds the workspace + container images. |
| [uv](https://docs.astral.sh/uv/) | any | Runs the Python E2B SDK verification harness. |
| `openssl` | any | Hashes the API key when seeding a `TeamAPIKey`. |

### 1b. Cluster prerequisites (the ones that bite)

These are **non-negotiable** and are the usual cause of a deploy that
installs but never produces a running sandbox:

1. **Kubernetes 1.31.** The workspace pins `k8s-openapi` to the `v1_31`
   feature; older API servers will reject or mishandle the CRDs.
2. **gVisor (`runsc`) on every node** that runs sandboxes, exposed as a
   `RuntimeClass`. The chart installs a `gvisor` RuntimeClass whose
   handler is `runsc`; if the nodes' containerd has no
   `io.containerd.runsc.v1` runtime configured, sandbox Pods will fail
   with `RunContainerError`. For local work, the bundled custom kind node
   image bakes `runsc` in (`ci/kind-node/Dockerfile`).
3. **Pause/resume needs S3-compatible object storage** (AWS S3, MinIO,
   Garage, ...). If you only need create/exec/kill you can skip this, but
   `pause()` / `connect()` and the node DaemonSet require a reachable
   endpoint, bucket, and credentials. The node DaemonSet stays in
   `CreateContainerConfigError` until the snapshot-store Secret exists.
4. **Node DaemonSet runs privileged** (`privileged: true`, `hostPID:
   true`) to reach the CRI checkpoint API and the local containerd
   socket. Confirm your PodSecurity admission policy permits this in the
   target namespace before installing.

### 1c. Validate prerequisites (run this first)

Copy-paste this block. Every line should print an OK; a FAIL tells you
exactly what to fix before deploying.

```bash
# --- host tooling ---
for t in docker kind kubectl helm just cargo uv openssl; do
  if command -v "$t" >/dev/null 2>&1; then echo "OK   $t"; else echo "FAIL $t (missing)"; fi
done

# --- cluster reachable + version >= 1.31 ---
kubectl version -o json 2>/dev/null \
  | python3 -c 'import sys,json; v=json.load(sys.stdin)["serverVersion"]; \
maj,minr=int(v["major"]),int(v["minor"].rstrip("+")); \
print("OK   k8s", v["gitVersion"]) if (maj,minr) >= (1,31) \
else print("FAIL k8s", v["gitVersion"], "(need >= 1.31)")' \
  || echo "FAIL kubectl cannot reach a cluster"

# --- gVisor RuntimeClass present (after install, or pre-provisioned) ---
kubectl get runtimeclass gvisor >/dev/null 2>&1 \
  && echo "OK   runtimeclass/gvisor" \
  || echo "WARN runtimeclass/gvisor not found yet (chart installs it; nodes still need runsc)"
```

> The `gvisor` RuntimeClass existing is necessary but not sufficient: the
> node's containerd must actually have the `runsc` handler. The only
> reliable proof is the verification in section 4 producing a running
> sandbox.

---

## 2. Quickstart: local kind cluster

This is the fastest way to a working system and the supported local
path. It builds the component images, creates a kind cluster with a
gVisor-capable node, applies CRDs, installs the chart, seeds a dev
`SandboxTemplate` + `TeamAPIKey`, and port-forwards the gateway to
`http://localhost:8080`.

```bash
# Build images + kind cluster + chart + gVisor + snapshot store (Garage)
SANDBAWKS_KIND_GVISOR=1 just cluster-up

# Seed the dev template + API key and prove the HTTP lifecycle works
just api-smoke
```

`just cluster-up` is idempotent and re-runnable. When it finishes the
gateway is reachable at `http://localhost:8080` via a background
`kubectl port-forward`. Tear everything down with `just cluster-down`.

The dev API key seeded by the fixture is
`sbk_smoke_smoke_smoke_smoke_smoke` (template `api-smoke-template`). It is
**not a secret** — it is published in the repo for local smoke testing
only. Never reuse it in production.

---

## 3. Deploying to a real cluster

The chart deliberately does **not** ship CRDs (Helm 3 will not upgrade
chart-managed CRDs, which causes drift). Apply them out-of-band, then
install with `--skip-crds`.

```bash
# 1. CRDs first (regenerate from source with:
#    cargo run -p sandbawks-types --bin crdgen -- deploy/crds)
kubectl apply -f deploy/crds

# 2. Install the chart. Point the snapshot store at your S3 endpoint and
#    supply a pre-created signing secret + snapshot-store secret in prod.
helm install sandbawks deploy/helm/sandbawks --skip-crds \
  --set api.enabled=true \
  --set controller.enabled=true \
  --set node.enabled=true \
  --set gvisor.enabled=true \
  --set images.api.repository=<registry>/sandbawks-api        --set images.api.tag=<tag> \
  --set images.controller.repository=<registry>/sandbawks-controller --set images.controller.tag=<tag> \
  --set images.node.repository=<registry>/sandbawks-node      --set images.node.tag=<tag> \
  --set envd.image=<registry>/envd                            --set envd.tag=<tag> \
  --set snapshotStore.existingSecret=<your-s3-secret> \
  --set snapshotStore.endpoint=https://s3.example.com \
  --set snapshotStore.bucket=sandbawks-snapshots

# 3. Wait for rollout
kubectl rollout status deploy/sandbawks-controller
kubectl rollout status deploy/sandbawks-api
kubectl rollout status daemonset/sandbawks-node   # only if node.enabled=true
```

Production hardening notes:

- **API signing secret.** If `api.signingSecret.existingSecret` is
  blank, the chart auto-generates one and **rotates it on every `helm
  upgrade`** — fine for dev, breaks live tokens in prod. Provide a stable
  pre-created Secret.
- **Snapshot store.** `snapshotStore.endpoint` defaults to empty
  (pause/resume disabled). Set endpoint + bucket + credentials Secret to
  enable it.
- **Each team is a Kubernetes namespace.** The `clientID` returned by the
  API is the namespace name; create a `TeamAPIKey` CR per team
  (`spec.keyPrefix` = first 8 chars, `spec.keyHash` = `sha256:<digest>`).

### Register a template + API key

A sandbox cannot be created until a `SandboxTemplate` exists. Minimal
example (mirrors `deploy/kind/fixtures/api-smoke.sh`):

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: my-template
  namespace: default
spec:
  image: docker.io/library/busybox:1.36
  defaultRuntimeClass: gvisor
  defaultResources:
    cpu: "100m"
    memory: "64Mi"
    ephemeralStorage: "256Mi"
  envdImage: <registry>/envd:<tag>
  defaultTimeoutSeconds: 300
---
apiVersion: sandbawks.dev/v1alpha1
kind: TeamAPIKey
metadata:
  # The key lives in the system namespace (where sandbawks is installed),
  # out of tenant reach. Tenant identity comes from spec.namespace below.
  name: my-team-key
  namespace: default
spec:
  namespace: "default"             # tenant namespace this key authorizes (required)
  keyPrefix: "sbk_team"            # first 8 chars of the full key
  keyHash: "sha256:<digest>"       # printf '%s' "$KEY" | openssl dgst -sha256 -r | awk '{print $1}'
  scopes: ["sandboxes:*"]
```

---

## 4. Verify the tooling works (Python E2B SDK)

This is the step that proves the whole chain end to end: gateway auth,
controller reconciliation, gVisor sandbox boot, and the envd exec/file
proxy. We use the **official Python E2B SDK** as the probe.

### 4a. Recommended: run the SDK compatibility harness

The repo ships a `uv`-managed pytest harness (`tests/e2b-sdk-compat/`)
that drives the real `e2b` SDK (`e2b>=2.21,<3`) against the gateway. It
is the canonical "is my tooling working" check because it handles a
localhost-specific routing quirk for you (see the caveat below).

```bash
# Assumes section 2's `SANDBAWKS_KIND_GVISOR=1 just cluster-up` has run
# (it seeds the template/key and port-forwards :8080). For a remote
# gateway, export E2B_API_URL / E2B_API_KEY instead.
just sdk-smoke
```

A green run exercises create / list / kill, `commands.run` with
streaming, the full file API (`write` / `read` / `list` / `stat` /
`remove` / `move` / `make_dir`), and SSE phase events.

Even cheaper — confirm the harness imports without a live gateway:

```bash
cd tests/e2b-sdk-compat && uv sync && uv run pytest -v --collect-only
```

### 4b. Standalone smoke script

If you want a hand-rolled check, point the SDK at your gateway via env
vars. The SDK reads them at `Sandbox` construction time:

```bash
pip install "e2b>=2.21,<3"     # or: uv pip install
export E2B_API_URL=http://localhost:8080       # your gateway base URL
export E2B_SANDBOX_URL=$E2B_API_URL            # see caveat below
export E2B_API_KEY=sbk_smoke_smoke_smoke_smoke_smoke   # dev key; use a real key in prod
```

```python
from e2b import Sandbox

# Boots a gVisor sandbox from the registered template.
sbx = Sandbox.create(template="api-smoke-template", timeout=300)
print("created:", sbx.sandbox_id)

# Exercises the envd exec proxy.
result = sbx.commands.run("echo hello-from-sandbawks")
print("stdout:", result.stdout.strip())

# Exercises the envd filesystem proxy.
sbx.files.write("/tmp/probe.txt", "ok")
print("readback:", sbx.files.read("/tmp/probe.txt"))

sbx.kill()
print("killed:", sbx.sandbox_id)
```

> **Localhost routing caveat (important).** Against a production wildcard
> domain (`{port}-{sandbox_id}.{team_domain}`), the SDK encodes the
> target sandbox in the hostname and everything just works. Against a
> plain `http://localhost:8080` (kind / port-forward), the gateway has no
> hostname to demux on and instead reads the `E2b-Sandbox-Id` header. The
> stock SDK injects that header on `Sandbox.create()` but **not** on
> `Sandbox.connect()` — so a connect-then-exec flow against localhost
> will fail unless you add the header yourself. The `just sdk-smoke`
> harness patches this for you (`tests/e2b-sdk-compat/conftest.py`).
> Bottom line: for localhost verification prefer 4a; for a standalone
> script, stick to the create→exec→kill flow above, or deploy behind
> wildcard DNS.

### What a green check proves

- Gateway auth accepted your `TeamAPIKey`.
- Controller reconciled the `Sandbox` CR to `Running` under gVisor.
- The node's `runsc` handler actually exists and booted the sandbox.
- The gateway successfully proxied exec + filesystem RPCs to envd.

---

## 5. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `401 unauthorized` from the SDK | `X-API-Key` not recognized | Confirm a `TeamAPIKey` CR with matching `keyPrefix` + `sha256` hash exists in the target namespace. |
| Sandbox stuck `Pending` / `RunContainerError` | No `runsc` handler on the node | Verify gVisor is installed in the node's containerd; check the `gvisor` RuntimeClass handler. |
| `commands.run` / file calls hang or 502 on localhost | Missing `E2b-Sandbox-Id` routing header | Use `just sdk-smoke` (4a), or deploy behind wildcard DNS. |
| Node DaemonSet `CreateContainerConfigError` | Snapshot-store Secret missing | Create the secret referenced by `snapshotStore.existingSecret` before install. |
| `pause()` fails | Pause/resume not configured | Set `snapshotStore.endpoint` / `bucket` / credentials; ensure node DaemonSet is healthy. |
| CRDs rejected on apply | Cluster older than 1.31 | Upgrade the API server, or run the section 1c validation first. |

## Reference

- Local smoke runbook: `docs/kind-smoke.md`
- SDK compatibility harness: `tests/e2b-sdk-compat/README.md`
- Helm values: `deploy/helm/sandbawks/values.yaml`
- CRDs: `deploy/crds/` (regenerate with `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`)
- Workspace API docs: `cargo doc --workspace --no-deps`
</content>
</invoke>
