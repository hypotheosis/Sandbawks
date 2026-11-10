# Node SnapshotJob watcher defaulted to `default` namespace → warm pool stuck in `Pausing`

**Date:** 2026-06-04T12:12:53Z
**Area:** `sandbawks-node` binary, Helm node DaemonSet
**Severity:** High — warm pool delivered zero ready members; continuous churn.

## Symptom

`compound-pool` (min=2) reported `READY=0` indefinitely. Warm-pool members were
created, booted to `Running`, transitioned to `Pausing`, and then hung there
forever. Pods diverged — one observed `Completed` (envd exited), one still
`Running` — both with `Sandbox.status.phase=Pausing`. Every `SnapshotJob` in the
namespace (including 10–11h-old orphans whose sandboxes no longer existed)
carried an **empty `.status`**.

## Root cause

The `sandbawks-node` DaemonSet's `SnapshotJob` reconciler logged at boot:

```
sandbawks-node SnapshotJob controller starting  namespace=default
```

The node binary resolves its watch namespace from `SANDBAWKS_NAMESPACE`, falling
back to the literal string `"default"`. The **node DaemonSet manifest never set
`SANDBAWKS_NAMESPACE`** (unlike the controller deployment, which injects it via
the downward API), so the node watched `default` while the controller created
all `SnapshotJob` CRs in `sandbawks`. The node therefore never claimed any pause
job, never drove the gVisor checkpoint, and never wrote job status. The
controller, which advances `Pausing → Paused` only on a `SnapshotJob` `Complete`
status, waited forever.

Two independent layers were at fault:
1. **Manifest gap** (proximate cause): node DS missing the `SANDBAWKS_NAMESPACE`
   downward-API env that the controller deployment already has.
2. **Fragile code default**: a bare `"default"` fallback silently sent the
   watcher to the wrong namespace instead of failing loud or self-correcting.

## Blast radius (wider than the visible symptom)

The mis-resolved `namespace` is consumed by three k8s-namespaced clients in the
node binary, not just the SnapshotJob watcher:
- `reconcile::run(.., namespace, ..)` — the SnapshotJob watcher (the visible
  symptom: pauses never claimed).
- `KubeSandboxPhaseLookup` (Plan 08 B2 staging sweeper) — looks up owner phase
  via `Api::namespaced` (`gvisor.rs:350`); pointed at `default` it would never
  match a `sandbawks` sandbox, so staging-dir reclamation was mis-targeted too.
- `build_gvisor_adapter(.., &namespace, ..)` → `ContainerdClient` Pod lookups via
  `Api::namespaced` (`gvisor.rs:2609`).

The cluster never reached the staging/restore path (everything wedged in
`Pausing`), so only the watcher symptom was observed — but the single fix
repairs all three consumers. Surfaced in PR #334 review.

## Fix

- **`crates/sandbawks-node/src/bin/sandbawks-node.rs`**: resolve the namespace
  *after* the kube client is built and fall back to
  `kube_client.default_namespace()` (the in-cluster service-account namespace at
  `/var/run/secrets/kubernetes.io/serviceaccount/namespace`) instead of the
  literal `"default"`. An explicit `SANDBAWKS_NAMESPACE` override still wins. The
  node, controller, and `SnapshotJob` CRs are always co-located, so the Pod's own
  namespace is the correct default.
- **`deploy/helm/sandbawks/templates/node-daemonset.yaml`**: inject
  `SANDBAWKS_NAMESPACE` from `fieldRef: metadata.namespace`, mirroring the
  controller deployment, so the manifest is explicit and correct even on older
  node images.

Belt-and-suspenders: either change alone fixes the live cluster after redeploy;
together they also prevent recurrence on a future DaemonSet that forgets the env.

## Verification

- `cargo check -p sandbawks-node` — clean.
- `cargo clippy -p sandbawks-node` — no warnings.
- `cargo fmt -p sandbawks-node --check` — clean.
- `helm template deploy/helm/sandbawks` renders `SANDBAWKS_NAMESPACE` via
  `fieldRef: metadata.namespace` in the node DaemonSet.

## Notes / follow-ups (not addressed here)

- The `Completed` pod's envd exited at exactly 60s via two back-to-back 30s
  drain timeouts (`server drain timed out` / `HTTP /__state drain timed out`).
  envd began draining at startup, *before* the node DS even booted, so this is
  independent of the namespace bug. envd is injected by sandbawks (not shipped in
  customer images), so the stale-binary theory is wrong; the 60s drain-on-startup
  behavior on the injected envd warrants a separate investigation vs. #325.
- `rootfs fingerprint capture failed; Permission denied (os error 13)` on `/` —
  snapshots would carry no resume oracle even once pauses complete.
