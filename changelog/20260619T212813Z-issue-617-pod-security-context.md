# Issue #617 — Pod securityContext (fsGroup/...) on SandboxTemplate

_Date: 2026-06-19_

Expose a pod-level `securityContext` on the `SandboxTemplate`/`Sandbox` so a
non-root container can write to a dynamically provisioned `volumeClaimTemplates`
PVC — which otherwise mounts `root:root` and is unwritable (epic #603 filesystem
mode).

## Why

A `persistenceMode: filesystem` sandbox with a non-root image gets a PVC mounted
at the configured path but owned by root, so the workload can't write to its own
workspace. The standard Kubernetes remedy is `pod.spec.securityContext.fsGroup`
(the kubelet chowns the volume to that GID with setgid and adds it to the
container's supplemental groups), but sandbawks set no `securityContext` at all.

## What changed

- **`sandbawks-types`**: new mirrored-subset `PodSecurityContext`
  (`fsGroup`, `runAsUser`, `runAsGroup`, `runAsNonRoot`, `fsGroupChangePolicy`,
  `supplementalGroups`) — mirrored, not reused, for the same reason as
  `Toleration`/`VolumeClaimTemplate` (k8s-openapi types don't derive
  `JsonSchema`). Added `security_context: Option<PodSecurityContext>` to
  `SandboxTemplateSpec` and `SandboxSpec`.
- **`sandbawks-controller`**:
  - `resolve_pod_inputs` fills an empty Sandbox `security_context` slot from the
    template (same empty-slot precedence as `tolerations`/`nodeSelector`;
    explicit CR value wins; `None` ⇒ unchanged).
  - `pod_adapter::pod_spec_for` converts the resolved mirror into
    `k8s_openapi::PodSecurityContext` and sets `PodSpec.securityContext`. Empty
    `supplementalGroups` maps to the upstream `None`.
- Regenerated CRDs (`sandboxes`, `sandboxtemplates`).

Pod-level scope (`PodSpec.securityContext`) is what `fsGroup` needs and applies
to all containers; per-container `SecurityContext` is left for a follow-up.

## Backward compatibility

`None` ⇒ the Pod is built with no `securityContext`, exactly as before. The
field is `Option`, `skip_serializing_if = None`, and absent from the CRD
`required` set, so old CRs stay valid (additive schema change). Runtime-agnostic:
`fsGroup` is applied by the kubelet/CSI before the container (gVisor `runsc` or
Kata guest) starts.

## Usage

```yaml
spec:
  securityContext:
    fsGroup: 1000        # GID the image's user belongs to
    runAsUser: 1000      # optional
    runAsGroup: 1000
  volumeClaimTemplates:
    - metadata:
        name: workspace
        annotations: { sandbawks.dev/mount-path: /workspace }
      spec: { storage: 10Gi, accessModes: ["ReadWriteOnce"] }
```

## Tests

- `sandbawks-types`: `PodSecurityContext` camelCase serialize/omit-empty +
  full round-trip.
- `sandbawks-controller`: `pod_spec_for` sets `PodSpec.securityContext` when
  present (and leaves it `None` when absent; empty `supplementalGroups` → `None`);
  `resolve_pod_inputs` inherits an empty slot from the template and honors an
  explicit Sandbox override.

## Gates

`just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`.

## Deploy note

CRD schema changed — **`kubectl apply -f deploy/crds`** after deploy (Helm does
not manage CRDs; a stale CRD silently prunes `securityContext`). No Helm chart
change in this PR (the field is a CR field, not a chart value), so no chart
version bump.
