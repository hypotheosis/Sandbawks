# Plan 04 T13 — Helm chart wiring (api Deployment + RBAC + signing secret)

Forgejo issue: #112 (part of Plan 04 epic #99).

## Summary

Replaces the `--placeholder` api Deployment stub with the real `sandbawks-api`
binary, provisions an HMAC signing secret, narrows the api ServiceAccount's
cluster-scoped RBAC to what the gateway actually consumes, and wires the
controller's `--runtime` flag through the chart so production installs default
to the real `PodAdapter` rather than the `fake` test shim.

## Changes

### `deploy/helm/sandbawks/templates/api-deployment.yaml`
- Dropped `--placeholder` args.
- Mounted the three env vars the api binary reads at startup:
  - `SANDBAWKS_DEFAULT_NAMESPACE` ← `.Values.api.defaultNamespace`
  - `SANDBAWKS_API_LISTEN=0.0.0.0:8080`
  - `SANDBAWKS_SIGNING_SECRET` ← `secretKeyRef` against the secret resolved by
    the new `sandbawks.signingSecretName` helper.
- Added `httpGet` readiness probe on `/readyz` (initialDelay 2s, period 5s)
  and liveness probe on `/healthz` (initialDelay 10s, period 10s), reusing
  the `http` named port so a future port-number change only edits one place.

### `deploy/helm/sandbawks/templates/api-signing-secret.yaml` (new)
- Chart-managed `Secret` named `sandbawks-api-signing` containing a 64-char
  `randAlphaNum` value (`b64enc`'d into `data.signing-secret`). Rotated on
  every `helm upgrade --install` — acceptable for MVP since unsigned tokens
  have a 24h TTL ceiling.
- Skipped entirely when `.Values.api.signingSecret.existingSecret` is set, so
  production deployments can pin a stable, externally-rotated key.

### `deploy/helm/sandbawks/templates/rbac.yaml`
- Replaced the over-broad api `ClusterRole` (which had write verbs on
  `sandboxtemplates` and `teamapikeys` the gateway never touches) with the
  spec-mandated split:
  - `get/list/watch` on `sandbawks.io/teamapikeys`
  - `get/list/watch/create/delete/patch/update` on `sandbawks.io/sandboxes`
  - `get/list/watch` on core `events` (powers SSE event stream — Plan 04 T8).

### `deploy/helm/sandbawks/templates/_helpers.tpl`
- New `sandbawks.signingSecretName` helper. Returns `.Values.api.signingSecret.existingSecret`
  when set, otherwise the chart-default `sandbawks-api-signing`. Used by the
  Deployment's `secretKeyRef` so the existingSecret override flows through
  without editing the Deployment template.

### `deploy/helm/sandbawks/templates/controller-deployment.yaml`
- Wired `args: [--runtime, "{{ .Values.controller.runtime | default "pod" }}"]`
  so production installs use the real `PodAdapter`. Without this, the
  controller's clap default (`Fake`) would silently win.

### `deploy/helm/sandbawks/values.yaml`
- `api.defaultNamespace: default` and `api.signingSecret.existingSecret: ""`
  (defaults that match the auto-generated-secret happy path).
- `images.api.repository` → `ghcr.io/hypotheosis/sandbawks-api` and `tag` → `0000000`
  (commit-sha placeholder; T14's publish workflow rewrites it on every build).
- `controller.runtime: pod` with a comment explaining the `pod` vs `fake`
  trade-off for operators who reach for it.

## Verification

```
just fmt              # ok
cargo clippy ...      # ok (no warnings)
just test             # all suites pass
helm lint deploy/helm/sandbawks                                          # 0 failed
helm template test deploy/helm/sandbawks | grep -A2 SANDBAWKS_SIGNING_SECRET  # renders secretKeyRef
helm template test deploy/helm/sandbawks | grep -- '--runtime'           # `- --runtime` + `- "pod"`
helm template ... --set api.signingSecret.existingSecret=my-own-secret   # secretKeyRef → my-own-secret; auto-secret omitted
```

## Notes / future work

- The auto-generated signing secret rotates per `helm upgrade --install`, so
  any tokens issued before the upgrade will fail HMAC verification afterward.
  This is fine for MVP (24h TTL ceiling, single-replica dev installs) but
  operators running multi-replica or rolling upgrades should pin
  `api.signingSecret.existingSecret` to a Secret managed out-of-band.
- The image tag placeholder (`0000000`) intentionally won't resolve against
  the registry — that surfaces immediately if T14's publish workflow hasn't
  run yet rather than silently pulling a stale tag.
