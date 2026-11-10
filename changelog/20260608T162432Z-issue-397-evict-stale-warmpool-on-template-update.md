# Evict stale warm-pool members when their SandboxTemplate is updated

**Issue:** #397
**Date:** 2026-06-08T16:24:32Z
**Area:** controller / warm pool

## Problem

When a `SandboxTemplate` was edited (new image, different `defaultRuntimeClass`,
changed `defaultResources`), existing unclaimed warm-pool members kept running
against the old spec. The warm-pool controller never watched `SandboxTemplate`
objects, so a template change triggered no reconcile and stale members sat in
`Running` until claimed — handing a requester a sandbox built from a superseded
template.

## Change

A warm member now records the template `resourceVersion` it was minted against,
and each reconcile evicts any unclaimed member whose recorded version no longer
matches the template's current version. A `SandboxTemplate` watch requeues every
referencing pool within ~1s of an edit instead of waiting for the 10s heartbeat.

### `crates/sandbawks-types/src/warm_pool.rs`

- Added `TEMPLATE_RV_ANNOTATION = "sandbawks.dev/warm-pool-template-rv"`, the
  annotation recording the template `resourceVersion` a member was minted
  against. Exported from the crate root and pinned by the stability test.

### `crates/sandbawks-controller/src/warm_pool.rs`

- `WarmSandboxDefaults` gains `template_resource_version: Option<String>`,
  populated from `template.meta().resource_version` (`None` on a lookup miss).
- `warm_sandbox()` stamps `TEMPLATE_RV_ANNOTATION` on each member when the
  version is known; a lookup miss leaves the member unannotated rather than
  stamping a placeholder.
- `PoolMembers` gains a `version_stale` bucket. `bucket()` takes a
  `current_rv: Option<&str>`: when `Some`, a `Running` member whose stamped
  annotation differs — or is absent (minted before this feature) — is routed to
  `version_stale` instead of `ready`. When `None` (template-lookup miss) the
  version is unknown and no member is evicted, the same lenient-miss posture the
  resource/runtime-class defaults take (avoids churning the whole pool on a
  transient miss).
- `reconcile_pool()` resolves the template defaults once, up front and
  unconditionally (the current `resourceVersion` is needed to bucket members),
  passes `current_rv` to `bucket()`, and deletes `version_stale` members in the
  same loop as the legacy-`Paused` reclaim. The freed slots are restored by
  top-up. The reconcile log line gains an `evict` field.

### `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`

- The warm-pool `Controller` now exposes its own `SandboxWarmPool` reflector
  store (`Controller::store()`), shared into a new `.watches::<SandboxTemplate>`
  mapper. A template edit fans out to one `ObjectRef<SandboxWarmPool>` per
  same-namespace pool whose `spec.templateRef` names the template, requeuing
  them immediately.

## Behaviour preserved

- **Claimed members are never touched.** A claim removes `WARM_POOL_LABEL`
  atomically, so the label-selected `list_members` never returns claimed
  members — a template edit cannot evict an in-use sandbox.
- **Lenient miss.** A momentarily-missing template yields `current_rv = None`,
  so no member is evicted against an unknown version.

## Operational notes

- **One-shot eviction wave on first rollout.** Every warm member that predates
  this change carries no `TEMPLATE_RV_ANNOTATION`, so the first reconcile after
  this lands reads each pre-existing member as version-stale (unannotated, and
  the current version is now known) and evicts the whole pool, briefly dropping
  `status.ready` to 0 per pool until top-up re-mints fresh members. This is the
  intended behaviour of `is_version_stale`, but operators rolling this out to
  clusters with large warm pools — the pools exist precisely to mask cold-boot
  latency — should expect the one-time dip. It is self-healing: each evicted
  slot is refilled on subsequent reconciles, and from then on members carry the
  annotation and only churn on an actual template edit.
- **Brief `max_ready` overshoot is possible.** Top-up runs before the
  version-stale deletes in the same reconcile pass, and version-stale members
  are not counted toward `total` in `plan_pool`. When `min_ready` members are
  version-stale the cluster transiently holds up to `2*min_ready` warm members
  for the few seconds until the deletes land, which can momentarily exceed
  `max_ready` where `max_ready < 2*min_ready`.

## Tests

`crates/sandbawks-types/tests/warm_pool.rs`
- Stability assertion for `TEMPLATE_RV_ANNOTATION`.

`crates/sandbawks-controller/tests/warm_pool.rs`
- `warm_sandbox` stamps the RV annotation; a lookup miss leaves it unset.
- `bucket()` routes version-mismatched / unannotated `Running` members to
  `version_stale`, and evicts nothing when the version is unknown.
- `reconcile_pool` evicts a version-stale member and re-mints a fresh one
  carrying the current RV; leaves a current-version member untouched; never
  touches a claimed (label-less) member on a template update.

## Validation

- `just fmt` — clean
- `cargo clippy --all-targets -- -D warnings` — clean
- `just test` — full workspace green (controller warm_pool suite: 21 passed)

## Out of scope

- Per-template envd staging (#270).
- Draining in-flight (`Creating`/`Resuming`) members on template update.
- Surfacing a "pool is refreshing" signal in `SandboxWarmPoolStatus`.
