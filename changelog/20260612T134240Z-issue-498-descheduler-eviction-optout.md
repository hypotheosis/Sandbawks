# Protect actively utilized sandboxes from descheduler eviction (#498)

**Date:** 2026-06-12

## Problem

The cluster's `sigs.k8s.io/descheduler` evicted warm sandbox `sbx-warm-zqv22`
on 2026-06-11 with reason `LowNodeUtilization`. A warm (pre-started, awaiting a
claim) or momentarily-idle claimed sandbox registers as low CPU/memory
utilization and looks indistinguishable from a wasted pod to the descheduler's
balancing heuristics — so it is evicted to rebalance the node, destroying the
live (or warm) gVisor session. No controller live-migrates a sandbox Pod, so
this surfaces to the user as an unexpected mid-session failure.

## Investigation

The descheduler is **not** configured in this repo — it is deployed
out-of-band (ArgoCD/Helm) in `kube-system`. Inspecting the live cluster:

- Image `registry.k8s.io/descheduler/descheduler:v0.35.0`.
- `DefaultEvictor` policy runs with `noEvictionPolicy: Mandatory`.

The `noEvictionPolicy: Mandatory` setting is the decisive signal: it exists
solely to make the `descheduler.alpha.kubernetes.io/prefer-no-eviction: "true"`
pod annotation a **hard** skip across every descheduler plugin (including
`LowNodeUtilization`). The operator had already opted in to honoring that
annotation; sandbox Pods simply never set it.

### Why not `descheduler.alpha.kubernetes.io/evict: "false"`

The original issue floated this annotation. In descheduler v0.35.0 the
`evict` annotation is **presence-checked** and means *opt-in* — it forces a
normally-protected Pod to become evictable. Setting `evict: "false"` would
therefore have the catastrophic opposite effect of force-evicting **every**
sandbox. The correct opt-*out* signal is `prefer-no-eviction`, paired with the
deployed `noEvictionPolicy`.

## Change

`crates/sandbawks-controller/src/pod_adapter.rs`:

- Added the `ANNOTATION_PREFER_NO_EVICTION`
  (`descheduler.alpha.kubernetes.io/prefer-no-eviction`) /
  `ANNOTATION_PREFER_NO_EVICTION_VALUE` (`true`) constants, with a doc comment
  recording the descheduler version, the `noEvictionPolicy: Mandatory`
  contract, and the `evict`-annotation footgun.
- `PodAdapter::pod_spec_for` now stamps the opt-out annotation on **every**
  sandbox Pod (cold-start and restore alike), merging it with the existing
  restore-image-path annotation instead of replacing it.

## Lifecycle / scope

The protection is keyed to **Pod existence**, which maps cleanly onto sandbox
state: a paused sandbox is checkpointed to object storage and has no Pod, so
there is nothing to evict and nothing to annotate; a warm/claimed/running
sandbox has a Pod and is protected. No new reconcile path or transition-time
patching is required, so there is no annotate/de-annotate race.

The annotation suppresses **only** the descheduler. Involuntary disruptions
(node failure, `NoExecute` taint eviction by the node controller) and
`kubectl drain` (which calls the Eviction API directly and ignores this
annotation) are unaffected — legitimate node-maintenance eviction still works,
satisfying the issue's second acceptance criterion.

## Tests

- `cold_start_pod_opts_out_of_descheduler_eviction` — cold-start Pod carries
  `prefer-no-eviction=true` and no restore-image-path annotation.
- `restore_pod_keeps_both_eviction_and_restore_annotations` — restore Pod
  carries both annotations; the opt-out does not clobber the staging path.

## Validation

- `just fmt`
- `cargo clippy --all-targets -- -D warnings`
- `just test`
