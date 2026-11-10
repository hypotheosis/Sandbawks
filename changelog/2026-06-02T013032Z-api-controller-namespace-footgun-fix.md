# Fix: API/controller Sandbox-namespace mismatch footgun

## Summary

Made the api gateway's default Sandbox-CR namespace track the Helm release
namespace instead of the static literal `default`. This removes a footgun
where installing the chart into any namespace other than `default` left the
api and controller operating in different namespaces, so Sandboxes were
created but never reconciled into Pods.

## Symptom

A freshly provisioned sandbox never came online. The e2b client failed with:

```
ConnectException: sandbox sbx-... has no envdAddress yet (waited 30s)
TimeoutException: ... This error is likely due to sandbox timeout.
```

## Root cause

Two components resolved the Sandbox-CR namespace independently and disagreed:

| Component | Namespace | Source |
|---|---|---|
| `sandbawks-api` | `default` | `SANDBAWKS_DEFAULT_NAMESPACE` <- `api.defaultNamespace` (chart default `default`) |
| `sandbawks-controller` | release ns (e.g. `sandbawks`) | `SANDBAWKS_NAMESPACE` <- `fieldRef: metadata.namespace` |

The api created the `Sandbox` CR in `default`; the controller watches only its
own pod namespace (`Api::namespaced`, `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`).
The CR was never reconciled (empty `phase`, no scheduled node), so no Pod, no
envd, no `envdAddress`, and the client timed out.

Not RBAC (both ship cluster-wide ClusterRoles) and not auth (the api lists
`TeamAPIKey`s cluster-wide, `crates/sandbawks-api/src/auth.rs`), which is why
the create succeeded while reconciliation silently did nothing.

## Change

- `deploy/helm/sandbawks/templates/api-deployment.yaml`: render
  `SANDBAWKS_DEFAULT_NAMESPACE` as
  `{{ .Values.api.defaultNamespace | default .Release.Namespace | quote }}`.
- `deploy/helm/sandbawks/values.yaml`: change `api.defaultNamespace` default
  from `default` to `""` (empty), so the fallback engages out of the box.

An explicit `api.defaultNamespace` still wins, for deployments that
deliberately split the api and controller across namespaces.

## Verification

```
$ helm template sandbawks deploy/helm/sandbawks --namespace sandbawks \
    --set api.enabled=true --show-only templates/api-deployment.yaml
            - name: SANDBAWKS_DEFAULT_NAMESPACE
              value: "sandbawks"          # tracks release namespace

$ helm template ... --set api.defaultNamespace=tenant-x ...
            - name: SANDBAWKS_DEFAULT_NAMESPACE
              value: "tenant-x"           # explicit override still wins
```
