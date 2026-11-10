# sandbawks Plan 08 — Warm pool + lazy/page-fault restore + `SANDBAWKS_RESTORE_MODE` removal

> **For agentic workers:** REQUIRED SUB-SKILL: use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task.
>
> **Scope.** Eighth of the sandbawks sub-plans. Plan 07 landed a working
> end-to-end resume: `POST /sandboxes/<id>/connect` stages the snapshot to
> a node, creates a fresh Pod under `runtimeClassName: sandbawks-runsc` with
> the `io.sandbawks/restore-image-path` annotation, and the
> `containerd-shim-sandbawks-v1` wrapper drives `CreateWithFSRestore` +
> `Restore` so the container is *born restored*. Plan 08 delivers the three
> things Plan 07's "Next plan preview" explicitly deferred:
>
> 1. **`SandboxWarmPool` reconciler + full claim path** — keep paused
>    sandboxes warm to `spec.minReady`, and have `sandbawks-api`
>    `POST /sandboxes` atomically claim a warm sandbox instead of cold-booting
>    when a matching `SandboxWarmPool` has spare capacity.
> 2. **gVisor lazy / page-fault restore** — flip
>    `RestoreConfig.Background` to `true` in the shim (the seam Plan 07 T4
>    left), keep the staged image alive for the resumed container's lifetime,
>    and tighten the resume p95 CI gate from its Plan 07 tripwire toward the
>    spec §13 target.
> 3. **Remove the deprecated `SANDBAWKS_RESTORE_MODE` envd shim** — delete
>    the idle-hold branch, the `restore.rs` constants module, and the
>    `=legacy` smoke tri-state Plan 07 T10/T11 kept for one release.

**Goal:** Sub-second warm starts through the public E2B API, lazy-restore
latency wins, and removal of the last fresh-pod-resume vestige — all on the
working Plan 07 resume path.

**Architecture:** A new `SandboxWarmPool` reconciler (additive watcher on the
existing controller) maintains a labelled fleet of pre-paused sandboxes;
`sandbawks-api` claims one atomically via optimistic-concurrency relabel and
resumes it. Lazy restore is a one-line shim flip plus staging-dir lifetime
extension. The `SANDBAWKS_RESTORE_MODE` removal is pure deletion.

**Tech stack:** Rust (`kube-rs`, `axum`, `thiserror`, `tokio`), Go (gVisor
`pkg/shim/v1`), Helm, kind + gVisor smoke, `tests/bench` standalone crate.

## Issue tracking

- Epic: #260
- Workstream A (warm pool, tasks A1–A8): #261
- Workstream B (lazy restore, tasks B1–B3): #262
- Workstream C (`SANDBAWKS_RESTORE_MODE` removal, tasks C1–C3): #263

All three sub-issues are mutually independent and each blocks the epic.

## Numbering and predecessors

- [Plan 05 — gVisor adapter, S3 streaming, pause/resume on kind](2026-05-24-sandbawks-05-gvisor-node.md)
- Plan 06 (executed) — SDK compat + benchmarks + retention, epic #182
- [Plan 07 — `containerd-shim-sandbawks-v1`, kubelet-mediated resume](2026-05-28-sandbawks-07-shim-wrapper-resume.md)
- Spec: [`2026-05-21-sandbawks-mvp-design.md`](../specs/2026-05-21-sandbawks-mvp-design.md) §8 (warm pool), §6 (lazy restore), §15 (open questions).

## Scope decisions locked in this plan

Ratified during brainstorming (2026-06-02). **Implementers should not
relitigate these.**

### 1. Warm pool uses S3-backed snapshots claimed + staged on demand — no node-local hostPath cache

Spec §8 floats an optional hostPath warm-snapshot cache on a fraction of
nodes. **Deferred.** Plan 08's warm pool is a fleet of `Paused` sandboxes
whose snapshots live in S3; a claim resumes one through the existing Plan 07
`SnapshotJob::Stage` → restore path. Lazy restore (Workstream B) already
recovers most of the latency the hostPath cache would have. The cache,
its PodSecurity (PSA) exceptions, placement, and eviction (spec §15) stay
out of scope; revisit only if benchmarks demand it.

### 2. Full claim path, including `sandbawks-api`

`POST /sandboxes` with a `templateRef` matching a `SandboxWarmPool` that has
a ready sandbox claims that sandbox atomically and resumes it, returning the
claimed sandbox's existing name as the E2B `sandboxID`. Kubernetes object
names are immutable, so we **do not rename** the warm sandbox — we adopt its
name as the new sandbox identity, patch the requester's overrides
(`env`, `metadata`, `timeoutSeconds`) onto its spec, and drive it to
`Running`. On no warm capacity, `create_sandbox` falls through to today's
cold-boot path unchanged.

### 3. Atomic claim via optimistic-concurrency relabel

Warm sandboxes carry the label `sandbawks.io/warm-pool=<pool-name>`. A claim
is a `kube` patch that (a) removes that label and (b) sets
`sandbawks.io/claimed-at`, guarded by the candidate's `resourceVersion`. Two
concurrent `POST`s racing for the same candidate: one wins, the loser gets a
409 Conflict and retries against the next candidate. No new lock CRD, no
leader coordination — the API server's compare-and-swap is the lock.

### 4. Warm sandboxes are ordinary `Sandbox` objects, distinguished only by a label

A warm sandbox is a normal `Sandbox` created by the warm-pool reconciler with
`metadata.labels["sandbawks.io/warm-pool"]=<pool>` and
`metadata.ownerReferences` → the `SandboxWarmPool`. It boots, the reconciler
pauses it, and it sits in `Paused`. The `Sandbox` state machine
(`state_machine.rs`) is **unchanged** — warming reuses the existing
`PauseRequested`/`ResumeRequested` triggers. All warm logic lives in the new
`warm_pool` reconciler module so the subsystem reads in one place.

### 5. Lazy restore keeps the staged image alive for the container's lifetime

`RestoreConfig.Background=true` makes `runsc restore` return from `Start`
before every page is faulted in; pages are read lazily from the `--image-path`
bundle afterward. The staged bundle therefore **must outlive container start**
— today it is GC'd when the `Stage` `SnapshotJob` is deleted. Plan 08 ties the
staging directory's lifetime to the *resumed sandbox*, not the stage job:
the node retains the staging dir until the owning `Sandbox` leaves
`Running`/`Resuming` (paused again or killed). This is the only non-trivial
part of the lazy-restore flip.

### 6. Lazy restore is unconditional on `sandbawks-runsc` (no per-Pod opt-in)

Lazy restore is strictly better than eager when the runtime supports it; the
pinned gVisor release (`20240603.0`, per Plan 07 T2 / #193) does. We flip the
shim's `Background` to `true` for all sandbawks restores rather than adding an
`io.sandbawks/lazy-restore` annotation. (If a future runtime regression needs
an escape hatch, the annotation is a one-task follow-up — not built now,
YAGNI.)

## File map

```
crates/
  sandbawks-types/
    src/
      warm_pool.rs            # +label/annotation const helpers (WARM_POOL_LABEL, CLAIMED_AT_ANNOTATION)
      lib.rs                  # re-export the new consts
  sandbawks-controller/
    src/
      warm_pool.rs            # NEW — SandboxWarmPool reconciler (top-up, trim, auto-pause)
      claim.rs                # NEW — atomic claim primitive (shared by api via a thin trait, tested here)
      lib.rs                  # +pub mod warm_pool; +pub mod claim;
      bin/sandbawks-controller.rs  # +SandboxWarmPool Controller wired alongside the Sandbox controller
    tests/
      warm_pool.rs            # NEW — reconcile fixtures: ready<min -> create; ready>max -> trim; Running -> pause
      claim.rs                # NEW — optimistic-concurrency: one winner, loser retries
  sandbawks-api/
    src/
      routes/sandboxes.rs     # create_sandbox: try claim_warm() before cold-boot create()
      warm_claim.rs           # NEW — kube-backed claim impl used by the gateway
      state.rs                # +AppState handle for the claim client (reuse state.kube)
    tests/
      warm_claim.rs           # NEW — claim hits return warm sandbox name; miss falls through to cold-boot
  sandbawks-node/
    src/
      gvisor.rs               # staging-dir GC keyed to sandbox lifetime, not stage-job deletion
  envd/
    src/
      bin/envd.rs             # DELETE the SANDBAWKS_RESTORE_MODE idle-hold branch
      restore.rs              # DELETE file
      lib.rs                  # remove `pub mod restore;`
    tests/
      binary_smoke.rs         # delete the restore-mode/deprecation-warning test

shim/
  internal/wrapper/service.go # RestoreConfig.Background: false -> true
  internal/wrapper/service_test.go # assert Background=true on dispatched RestoreRequest

deploy/
  helm/sandbawks/templates/
    rbac-controller.yaml      # +sandboxwarmpools verbs; controller may create/delete Sandboxes
    rbac-api.yaml             # +sandboxes patch (claim relabel) if not already present
  kind/fixtures/
    pause-resume-smoke.sh     # remove `=legacy` tri-state; SMOKE_MODE back to {0,1}

.forgejo/workflows/
  kind-smoke.yaml             # BENCH_RESUME_P95_MS tightened; +warm-pool smoke step

tests/bench/
  src/main.rs                 # +optional warm-start bench arm (BENCH_INCLUDE_WARM_START)

changelog/
  2026-06-02T...-plan-08-t<N>-...md   # one per task as they land
```

New deps: none on either side.

---

# Workstream A — `SandboxWarmPool` reconciler + claim path

> Maps to **sub-issue 1** of the Plan 08 epic. Depends on nothing else in the
> plan; the largest workstream. Build A1→A8 in order.

## Task A1: Warm-pool label + annotation constants

**Files**

- `crates/sandbawks-types/src/warm_pool.rs` — append:

```rust
/// Label stamped on every `Sandbox` the warm-pool reconciler manages.
/// Its value is the owning `SandboxWarmPool` name. The claim path removes
/// this label (atomically, guarded by `resourceVersion`) to take a sandbox
/// out of the pool; the reconciler counts `Paused` sandboxes carrying it
/// to compute `status.ready`.
pub const WARM_POOL_LABEL: &str = "sandbawks.io/warm-pool";

/// Annotation set on a sandbox at the instant it is claimed out of a pool.
/// Value is an RFC3339 timestamp. Purely observational — the *atomic* act
/// is removing [`WARM_POOL_LABEL`]; this records when it happened.
pub const CLAIMED_AT_ANNOTATION: &str = "sandbawks.io/claimed-at";
```

- `crates/sandbawks-types/src/lib.rs` — ensure `warm_pool` is `pub` and the
  two consts are reachable (the module is already `pub mod warm_pool;`).

**Test (TDD):** `crates/sandbawks-types/tests/warm_pool.rs` — add:

```rust
#[test]
fn warm_pool_label_and_annotation_consts_are_stable() {
    // These strings are an external contract (kube labels survive across
    // controller/api versions); pin them so a rename is a conscious break.
    assert_eq!(sandbawks_types::warm_pool::WARM_POOL_LABEL, "sandbawks.io/warm-pool");
    assert_eq!(sandbawks_types::warm_pool::CLAIMED_AT_ANNOTATION, "sandbawks.io/claimed-at");
}
```

**Verify**

- `cargo test -p sandbawks-types --all-features` green.

**Commit:** `feat(types): warm-pool label + claim annotation constants (Plan 08 A1)`

---

## Task A2: Warm-pool reconciler skeleton — observe `status.ready`

**Files**

- `crates/sandbawks-controller/src/warm_pool.rs` (NEW). Pure observation:
  list `Sandbox`es in the pool's namespace carrying
  `WARM_POOL_LABEL=<pool>`, count those in `Paused`, write `status.ready`
  + `status.warm_snapshots`. No creation/deletion yet.

```rust
//! `SandboxWarmPool` reconciler.
//!
//! Keeps a labelled fleet of pre-paused `Sandbox`es between
//! `spec.minReady` and `spec.maxReady`. All warm-specific logic lives
//! here; the `Sandbox` state machine is untouched (warming reuses the
//! ordinary Pause/Resume triggers). See Plan 08 Workstream A.

use std::sync::Arc;

use kube::api::{Api, ListParams};
use kube::ResourceExt;
use sandbawks_types::warm_pool::{SandboxWarmPool, SandboxWarmPoolStatus, WARM_POOL_LABEL};
use sandbawks_types::{Sandbox, SandboxPhase};

use crate::error::ControllerError; // existing crate error type

/// Snapshot of the pool's observed members, derived once per reconcile so
/// the top-up/trim logic (A3) operates on a consistent view.
pub struct PoolMembers {
    pub paused: Vec<Sandbox>,
    pub pending: Vec<Sandbox>, // Creating/Running/Pausing — in flight toward Paused
}

/// List the sandboxes labelled for `pool` and bucket them by readiness.
pub async fn observe_members(
    sandboxes: &Api<Sandbox>,
    pool_name: &str,
) -> Result<PoolMembers, ControllerError> {
    let lp = ListParams::default().labels(&format!("{WARM_POOL_LABEL}={pool_name}"));
    let all = sandboxes.list(&lp).await?;
    let mut paused = Vec::new();
    let mut pending = Vec::new();
    for s in all {
        match s.status.as_ref().map(|st| st.phase) {
            Some(SandboxPhase::Paused) => paused.push(s),
            Some(SandboxPhase::Failed) | Some(SandboxPhase::Terminating) => {} // drop
            _ => pending.push(s),
        }
    }
    Ok(PoolMembers { paused, pending })
}

/// Compute the status the reconciler should write for the observed members.
pub fn status_for(members: &PoolMembers) -> SandboxWarmPoolStatus {
    SandboxWarmPoolStatus {
        ready: members.paused.len() as u32,
        warm_snapshots: members.paused.iter().map(|s| s.name_any()).collect(),
    }
}
```

> Confirm `crate::error::ControllerError` / the `Sandbox.status.phase` field
> path against `state_machine.rs` and the existing reconcile module before
> writing; mirror the import style already used in `reconcile.rs`.

- `crates/sandbawks-controller/src/lib.rs` — `pub mod warm_pool;`.

**Test (TDD):** `crates/sandbawks-controller/tests/warm_pool.rs`:

```rust
// status_for buckets Paused vs in-flight and reports the Paused names.
#[test]
fn status_counts_only_paused_members() {
    let members = PoolMembers {
        paused: vec![sandbox_in_phase("sbx-warm-a", SandboxPhase::Paused),
                     sandbox_in_phase("sbx-warm-b", SandboxPhase::Paused)],
        pending: vec![sandbox_in_phase("sbx-warm-c", SandboxPhase::Creating)],
    };
    let st = status_for(&members);
    assert_eq!(st.ready, 2);
    assert_eq!(st.warm_snapshots, vec!["sbx-warm-a".to_string(), "sbx-warm-b".to_string()]);
}
```

(`sandbox_in_phase` is a one-line test helper building a `Sandbox` with a
`name` and `status.phase`; define it in the test module.)

**Verify**

- `cargo test -p sandbawks-controller --test warm_pool` green.

**Commit:** `feat(controller): SandboxWarmPool observe + status (Plan 08 A2)`

---

## Task A3: Top-up toward `minReady`, trim above `maxReady`

**Files**

- `crates/sandbawks-controller/src/warm_pool.rs` — add the reconcile body:

```rust
/// Decide the create/delete deltas for a pool given its spec + members.
/// Pure — returns names to delete and the count to create, so it is
/// exhaustively unit-testable without a cluster.
pub struct PoolPlan {
    pub create: u32,
    pub delete: Vec<String>,
}

pub fn plan_pool(
    min_ready: u32,
    max_ready: u32,
    members: &PoolMembers,
) -> PoolPlan {
    let warm = members.paused.len() as u32;
    let in_flight = members.pending.len() as u32;
    let total = warm + in_flight;
    // Create only up to the ceiling, counting in-flight so we don't
    // over-provision while sandboxes are still booting.
    let create = min_ready.saturating_sub(total).min(max_ready.saturating_sub(total));
    // Trim only *Paused* excess (never kill an in-flight boot); oldest
    // first by name sort for determinism.
    let mut delete = Vec::new();
    if warm > max_ready {
        let mut names: Vec<String> = members.paused.iter().map(|s| s.name_any()).collect();
        names.sort();
        delete = names.into_iter().take((warm - max_ready) as usize).collect();
    }
    PoolPlan { create, delete }
}
```

Then a `reconcile_pool(pool, sandboxes) -> Result<Action>` that calls
`observe_members`, `plan_pool`, applies the plan (create N warm sandboxes via
A4's constructor, delete the trim set), and patches `status_for(...)`.

**Test (TDD):** extend `tests/warm_pool.rs`:

```rust
#[test]
fn plan_creates_up_to_min_counting_in_flight() {
    let m = PoolMembers { paused: paused_n(1), pending: pending_n(1) }; // total 2
    let plan = plan_pool(/*min*/ 5, /*max*/ 10, &m);
    assert_eq!(plan.create, 3);          // 5 - 2
    assert!(plan.delete.is_empty());
}

#[test]
fn plan_never_exceeds_max() {
    let m = PoolMembers { paused: paused_n(0), pending: pending_n(0) };
    let plan = plan_pool(/*min*/ 20, /*max*/ 3, &m);
    assert_eq!(plan.create, 3);          // clamped to max
}

#[test]
fn plan_trims_paused_excess_oldest_first() {
    let m = PoolMembers { paused: named_paused(&["sbx-c", "sbx-a", "sbx-b"]), pending: vec![] };
    let plan = plan_pool(/*min*/ 1, /*max*/ 1, &m);
    assert_eq!(plan.create, 0);
    assert_eq!(plan.delete, vec!["sbx-a".to_string(), "sbx-b".to_string()]);
}
```

**Verify**

- `cargo test -p sandbawks-controller --test warm_pool` green.

**Commit:** `feat(controller): warm-pool top-up + trim planning (Plan 08 A3)`

---

## Task A4: Warm `Sandbox` constructor + auto-pause on Running

**Files**

- `crates/sandbawks-controller/src/warm_pool.rs`:
  - `fn warm_sandbox(pool: &SandboxWarmPool, template_ref: &str) -> Sandbox`
    — builds a `Sandbox` with a generated name (`sbx-warm-<rand-suffix>`;
    reuse the existing id-gen helper the api/controller already uses —
    confirm its path, do not invent a new one), `spec.template_ref =
    pool.spec.template_ref`, `metadata.labels[WARM_POOL_LABEL] = pool name`,
    and an `ownerReference` to the pool so deleting the pool GCs its fleet.
  - In `reconcile_pool`, after the create/trim apply: for each member in
    `members.pending` whose `status.phase == Running`, patch
    `spec.desired_phase = Paused` (the same annotation/field the
    `PauseRequested` trigger reads — confirm the exact field used by
    `pause_sandbox` in the api and reuse it). This is what drives a freshly
    booted warm sandbox down to `Paused`.

**Test (TDD):** extend `tests/warm_pool.rs`:

```rust
#[test]
fn warm_sandbox_carries_pool_label_and_owner_ref() {
    let pool = test_pool("python-pool", "python-data-sci", 5, 20);
    let sb = warm_sandbox(&pool, &pool.spec.template_ref);
    assert_eq!(sb.labels().get(WARM_POOL_LABEL).map(String::as_str), Some("python-pool"));
    assert_eq!(sb.spec.template_ref, "python-data-sci");
    assert!(sb.owner_references().iter().any(|o| o.kind == "SandboxWarmPool"));
}
```

Plus a fixture asserting a `Running` warm member yields a pause patch (use the
existing fake `kube` client harness from `tests/reconcile.rs`).

**Verify**

- `cargo test -p sandbawks-controller --test warm_pool` green.

**Commit:** `feat(controller): warm sandbox constructor + auto-pause on Running (Plan 08 A4)`

---

## Task A5: Wire the `SandboxWarmPool` Controller

**Files**

- `crates/sandbawks-controller/src/bin/sandbawks-controller.rs` — add a
  second `Controller` for `SandboxWarmPool` that also watches `Sandbox`
  (so a member reaching `Running`/`Paused` requeues its pool via an
  `ownerReference`-derived `ObjectRef`), running concurrently with the
  existing Sandbox controller in the same `tokio::select!`. Mirror the
  existing `Controller::new(...).watches(...).run(...)` shape already in the
  file (shown around lines 108–135).

```rust
let warm_pools: Api<SandboxWarmPool> = Api::all(client.clone());
let warm_controller = Controller::new(warm_pools, watcher::Config::default())
    .watches(
        sandboxes_for_warm, // Api<Sandbox>, all namespaces
        watcher::Config::default().labels(WARM_POOL_LABEL), // only labelled members
        |s: Sandbox| {
            s.owner_references()
                .iter()
                .filter(|o| o.kind == "SandboxWarmPool")
                .filter_map(|o| {
                    s.namespace().map(|ns| {
                        kube::runtime::reflector::ObjectRef::new(&o.name).within(&ns)
                    })
                })
                .collect::<Vec<_>>()
        },
    )
    .shutdown_on_signal()
    .run(reconcile_warm_pool_adapter, warm_error_policy, ctx.clone())
    .for_each(|res| async move {
        match res {
            Ok((obj, _)) => tracing::debug!(?obj, "warm-pool reconciled"),
            Err(e) => tracing::warn!(error = %e, "warm-pool reconcile error"),
        }
    });

tokio::select! {
    _ = controller => {},
    _ = warm_controller => {},
    _ = signal::ctrl_c() => tracing::info!("SIGINT received, shutting down"),
}
```

`reconcile_warm_pool_adapter` / `warm_error_policy` are thin wrappers around
`warm_pool::reconcile_pool` matching the `kube::runtime::Controller` reconciler
signature (mirror `reconcile_adapter`/`error_policy` already in the binary).

**Verify**

- `cargo build -p sandbawks-controller` clean.
- `cargo test -p sandbawks-controller --all-features` green.
- Manual: with a `SandboxWarmPool` applied to a kind cluster (deferred to the
  smoke in A8), `kubectl get sbxwp` shows `Ready` climbing toward `minReady`.

**Commit:** `feat(controller): run SandboxWarmPool controller alongside Sandbox (Plan 08 A5)`

---

## Task A6: Atomic claim primitive

**Files**

- `crates/sandbawks-controller/src/claim.rs` (NEW) — the relabel-with-
  `resourceVersion`-precondition claim, written so both the controller tests
  and the api gateway exercise the same logic.

```rust
//! Atomic warm-sandbox claim.
//!
//! Claiming = removing `WARM_POOL_LABEL` from a `Paused` sandbox, guarded
//! by the candidate's `resourceVersion`. The API server's compare-and-swap
//! is the lock: concurrent claimers race, exactly one PATCH lands, the
//! losers get 409 Conflict and try the next candidate. No lock CRD.

use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::core::params::Preconditions; // resourceVersion guard
use sandbawks_types::warm_pool::{CLAIMED_AT_ANNOTATION, WARM_POOL_LABEL};
use sandbawks_types::Sandbox;

/// Outcome of an attempt to claim from a pool.
pub enum ClaimOutcome {
    /// Adopted this sandbox name; resume it and return it as the sandboxID.
    Claimed(String),
    /// No ready warm sandbox — caller should cold-boot.
    Empty,
}

/// Try to claim one `Paused` sandbox out of `pool_name` in `namespace`.
/// `now_rfc3339` is injected (testability; the crate forbids `Date::now`).
pub async fn try_claim(
    sandboxes: &Api<Sandbox>,
    pool_name: &str,
    now_rfc3339: &str,
) -> Result<ClaimOutcome, kube::Error> {
    let lp = ListParams::default()
        .labels(&format!("{WARM_POOL_LABEL}={pool_name}"));
    let candidates = sandboxes.list(&lp).await?;
    for c in candidates.into_iter().filter(is_paused) {
        let name = c.metadata.name.clone().expect("listed object has a name");
        let rv = c.metadata.resource_version.clone();
        // JSON-merge patch: drop the pool label, stamp claimed-at.
        let patch = serde_json::json!({
            "metadata": {
                "labels": { WARM_POOL_LABEL: serde_json::Value::Null },
                "annotations": { CLAIMED_AT_ANNOTATION: now_rfc3339 },
            }
        });
        let pp = PatchParams {
            // Guard on the observed resourceVersion: lose the race -> 409.
            ..PatchParams::default()
        };
        match sandboxes
            .patch_with_preconditions(&name, &Preconditions { resource_version: rv, uid: None }, &pp, &Patch::Merge(&patch))
            .await
        {
            Ok(_) => return Ok(ClaimOutcome::Claimed(name)),
            Err(kube::Error::Api(ae)) if ae.code == 409 => continue, // lost race; next
            Err(e) => return Err(e),
        }
    }
    Ok(ClaimOutcome::Empty)
}

fn is_paused(s: &Sandbox) -> bool {
    matches!(s.status.as_ref().map(|st| st.phase), Some(sandbawks_types::SandboxPhase::Paused))
}
```

> Verify the exact `kube` API for a resourceVersion-guarded patch in the
> pinned `kube` version (`patch_with_preconditions` vs. encoding the
> precondition into `PatchParams`); adjust the call to whatever the version
> exposes. The *semantics* — patch fails with 409 on a stale
> `resourceVersion` — are the contract; the method name may differ.

- `crates/sandbawks-controller/src/lib.rs` — `pub mod claim;`.

**Test (TDD):** `crates/sandbawks-controller/tests/claim.rs` — using the fake
`kube` harness, seed two `Paused` warm sandboxes, fire two `try_claim` calls
concurrently, assert exactly one `Claimed` per distinct name and zero
double-claims; seed zero and assert `Empty`.

**Verify**

- `cargo test -p sandbawks-controller --test claim` green.

**Commit:** `feat(controller): atomic warm-sandbox claim primitive (Plan 08 A6)`

---

## Task A7: `sandbawks-api` claims before cold-boot

**Files**

- `crates/sandbawks-api/src/warm_claim.rs` (NEW) — a gateway-side wrapper
  that: (1) looks up a `SandboxWarmPool` whose `spec.template_ref ==
  sandbox.spec.template_ref` in the team namespace; (2) if found, calls
  `try_claim`; (3) on `Claimed(name)`, patches the requester's overrides
  (`env`, `metadata`, `timeoutSeconds`, `runtimeClassName`) onto the claimed
  sandbox and writes `spec.desired_phase = Running` to drive the resume;
  returns the claimed `Sandbox`. On `Empty` / no pool, returns `None`.
- `crates/sandbawks-api/src/routes/sandboxes.rs` — in `create_sandbox`,
  after resolving the template's `default_runtime_class` (line ~603) and
  **before** `state.sandbox_ops().create(...)` (line 617):

```rust
    // Warm-pool fast path: if a SandboxWarmPool covers this template and has
    // a ready sandbox, claim+resume it instead of cold-booting. A miss (no
    // pool, or pool empty) falls through to the cold-boot create below.
    if let Some(warm) = warm_claim::try_claim_for(&state, &team.namespace, &sandbox).await? {
        let dto = sandbox_to_dto(&warm, &team.namespace);
        return Ok((StatusCode::CREATED, Json(dto)));
    }
    let created = state.sandbox_ops().create(&team.namespace, sandbox).await?;
```

- `crates/sandbawks-api/src/state.rs` — no new field needed; `warm_claim`
  uses `state.kube` (already present, used by the template lookup above).

**Test (TDD):** `crates/sandbawks-api/tests/warm_claim.rs` — two cases against
the api's mock kube client: (a) a ready pool → response carries the *warm
sandbox's* name and the request overrides are patched on; (b) no pool / empty
pool → falls through to the cold-boot `create` mock (asserted by the mock
recording a `create` call). Reuse the existing `routes/sandboxes.rs` test
harness (`SandboxStore` trait impls around lines 223–355).

**Verify**

- `cargo test -p sandbawks-api --all-features` green.

**Commit:** `feat(api): claim warm sandbox on POST /sandboxes before cold-boot (Plan 08 A7)`

---

## Task A8: RBAC + warm-pool kind smoke

**Files**

- `deploy/helm/sandbawks/templates/rbac-controller.yaml` — add
  `sandboxwarmpools` (+`/status`) to the controller ClusterRole verbs
  (`get,list,watch,patch,update`), and ensure the controller can
  `create`/`delete` `sandboxes` (warming creates them, trim deletes them).
- `deploy/helm/sandbawks/templates/rbac-api.yaml` — ensure the api
  ServiceAccount may `patch` `sandboxes` (claim relabel) and `list`
  `sandboxwarmpools`. Add only what's missing.
- `deploy/kind/fixtures/` — new `warm-pool-smoke.sh` (or a step appended to
  the existing api-smoke fixture): apply a `SandboxWarmPool` with
  `minReady: 1`, wait for `status.ready==1`, `POST /sandboxes` with the
  matching `templateRef`, assert the returned `sandboxID` equals the warm
  sandbox's name (claim happened, not cold-boot) and the sandbox reaches
  `Running`. Gate behind a `WARM_POOL_SMOKE=${WARM_POOL_SMOKE:-1}` toggle so
  CI can disable it if flaky, mirroring the `PAUSE_RESUME_SMOKE_FULL` pattern.
- `.forgejo/workflows/kind-smoke.yaml` — invoke the warm-pool smoke step
  after the pause-resume smoke (it depends on resume working).

**Verify**

- `helm lint deploy/helm/sandbawks` clean; `helm template ...` renders the
  new RBAC rules.
- Locally: `SANDBAWKS_KIND_GVISOR=1 just cluster-up && <warm-pool-smoke> &&
  just cluster-down` exits 0; `kubectl get sbxwp` shows `Ready: 1` before the
  claim and the claimed sandbox's name matches the API response.

**Commit:** `test(node): warm-pool RBAC + kind smoke (claim returns warm sandbox) (Plan 08 A8)`

---

# Workstream B — gVisor lazy / page-fault restore

> Maps to **sub-issue 2**. Independent of Workstream A. The shim flip is
> trivial; the staging-dir lifetime (B2) is the real work.

## Task B1: Flip `RestoreConfig.Background` to `true`

**Files**

- `shim/internal/wrapper/service.go` (around line 174–184) — change
  `Background: false` to `Background: true` and update the comment to note
  lazy restore is now the default (Plan 08), pages fault in after `Start`
  returns, and that the `--image-path` bundle must outlive container start
  (cross-reference B2):

```go
return s.next.Restore(ctx, &extension.RestoreRequest{
    Start: r,
    Conf: extension.RestoreConfig{
        ImagePath: imagePath,
        Direct:    false,
        // Plan 08: lazy / page-fault restore. Start returns before all
        // pages are faulted; gVisor reads them from ImagePath on demand.
        // The staged bundle MUST outlive container start — see Plan 08 B2
        // (staging-dir GC keyed to sandbox lifetime).
        Background: true,
    },
})
```

- `shim/internal/wrapper/service_test.go` — extend the restore-dispatch test
  (around line 29) to assert the captured `RestoreRequest.Conf.Background ==
  true`.

**Verify**

- `cd shim && go test ./... && go vet ./...` green/silent.

**Commit:** `feat(shim): enable gVisor lazy/page-fault restore (Background=true) (Plan 08 B1)`

---

## Task B2: Keep the staged image alive for the resumed container's lifetime

**Problem.** Today the node DaemonSet GCs a staging directory when its `Stage`
`SnapshotJob` is deleted (Plan 07 §5, `OwnerRef`). With eager restore that was
safe — the bundle is fully read before `Start` returns. With lazy restore,
gVisor reads pages from `ImagePath` *after* `Start`, for as long as the
container runs. Deleting the staging dir mid-run would crash the sandbox on
the next page fault.

**Fix.** Tie staging-dir retention to the *resumed sandbox*, not the stage
job. The node retains `/var/lib/sandbawks/staging/<sandbox-id>/<job-uuid>/`
until the owning `Sandbox` leaves `Running`/`Resuming` (re-paused or killed).

**Files**

- `crates/sandbawks-node/src/gvisor.rs` — the staging GC currently keyed to
  `SnapshotJob` deletion must instead key to sandbox phase. Add a sweep that
  deletes a staging dir only when its `Sandbox` is absent or in
  `Paused`/`Failed`/`Terminating` (i.e. no longer needs lazy page reads).
  Factor the "is this staging dir still needed" predicate into a pure helper
  for unit testing.
- If the `Stage` `SnapshotJob` has an `OwnerRef` to the `Sandbox` (Plan 07),
  ensure the controller does **not** delete that job (and thus does not
  trigger GC) while the sandbox is `Running` — confirm in
  `crates/sandbawks-controller/src/reconcile.rs` that the stage job is kept
  until pause/kill, and adjust if it is deleted on `(Resuming, PodReady) →
  Running`.

**Test (TDD):** `crates/sandbawks-node/tests/gvisor.rs` — unit-test the
retention predicate: `Running`/`Resuming` → keep; `Paused`/`Failed`/
`Terminating`/absent → delete. Plus a fixture: stage a bundle, mark the
sandbox `Running`, run the sweep, assert the dir still exists; flip to
`Paused`, sweep again, assert it's gone.

**Verify**

- `cargo test -p sandbawks-node --all-features` green.

**Commit:** `fix(node): retain lazy-restore staging dir for the sandbox lifetime (Plan 08 B2)`

---

## Task B3: Re-measure resume p95 and tighten the CI gate

**Files**

- `.forgejo/workflows/kind-smoke.yaml` (around line 824) — lower
  `BENCH_RESUME_P95_MS` from `6500` to a value justified by the measured
  lazy-restore p95 on the CI runner. Update the comment block (lines ~812–824)
  to state lazy restore landed (Plan 08 B1), drop the "Tighten back to 2 s
  when Plan 08 lands" note, and record the new observed p95 + the headroom the
  gate keeps. Target the spec §13 ceiling (`DEFAULT_RESUME_P95_MS = 2000`);
  if the constrained CI runner can't hold 2000 ms even with lazy restore,
  set the gate to observed-p95 + headroom and open a follow-up issue to chase
  the spec target (do not silently leave it at 6500).
- `tests/bench/src/main.rs` — no change to `DEFAULT_RESUME_P95_MS` (already
  the §13 goal); confirm the resume arm still measures the lazy path.

**Verify**

- The `kind-smoke` bench job runs green on `main` with the new gate;
  `bench-results.json` shows resume p95 under the new ceiling.

**Commit:** `test(ci): tighten resume p95 gate after lazy restore (Plan 08 B3)`

---

# Workstream C — Remove `SANDBAWKS_RESTORE_MODE`

> Maps to **sub-issue 3**. Independent of A and B. Pure deletion of the
> Plan 07 T10 deprecation Plan 07 promised to remove here.

## Task C1: Delete the envd idle-hold branch + `restore.rs`

**Files**

- `crates/envd/src/bin/envd.rs` — delete the entire restore-mode block
  (lines ~310–338: the `if std::env::var(RESTORE_MODE_ENV)...` branch through
  its `return Ok(())`), and the `use ...restore::{RESTORE_MODE_ENV,
  RESTORE_MODE_DEPRECATION_WARNING}` import (line ~83). envd now always binds
  its listeners — the container is born restored by the shim, so there is no
  pre-restore idle hold to perform.
- `crates/envd/src/restore.rs` — **delete the file.**
- `crates/envd/src/lib.rs` — remove `pub mod restore;`.
- `crates/envd/tests/binary_smoke.rs` — delete
  `envd_restore_mode_skips_listeners_and_state_file` and the deprecation-
  warning assertion (they test behavior that no longer exists).

**Verify**

- `cargo build -p envd` clean (no dangling references to the deleted consts).
- `cargo test -p envd --all-features` green.
- `grep -rn "RESTORE_MODE" crates/` returns nothing.

**Commit:** `refactor(envd): remove deprecated SANDBAWKS_RESTORE_MODE idle hold (Plan 08 C1)`

---

## Task C2: Drop the `=legacy` smoke tri-state

**Files**

- `deploy/kind/fixtures/pause-resume-smoke.sh` — remove every `legacy`
  branch and the env-injection it performs:
  - The header comments (lines ~87–88, ~180–217) describing the tri-state —
    rewrite to binary `{0,1}`.
  - `case "$SMOKE_MODE" in 0 | 1 | legacy)` (line ~205) → `0 | 1)`; fix the
    error message (line ~207) to `{0,1}`.
  - The whole `legacy)` case in the resume dispatch (lines ~547–609) — delete.
  - Any `legacy` mention in the `SENTINEL_LARGE_MIB`/runtime-class comments
    (lines ~213–240, ~333) — drop the `legacy` clause.
- `grep -n "legacy\|RESTORE_MODE" deploy/kind/fixtures/pause-resume-smoke.sh`
  returns nothing after the edit.

**Verify**

- `bash -n deploy/kind/fixtures/pause-resume-smoke.sh` (syntax) clean.
- `SMOKE_MODE` accepts only `0`/`1`; an invalid value still fails fast.
- Locally: `PAUSE_RESUME_SMOKE_FULL=1 <smoke>` still passes end-to-end.

**Commit:** `test(node): drop SANDBAWKS_RESTORE_MODE legacy smoke path (Plan 08 C2)`

---

## Task C3: Changelog + repo-wide grep clean

**Files**

- `changelog/2026-06-02T...-plan-08-c-remove-restore-mode.md` — note the
  removal, link #171 (the original fresh-pod resume design the env-var shim
  served) and the Plan 07 T10 deprecation changelog.
- Final sweep: `grep -rn "SANDBAWKS_RESTORE_MODE\|RESTORE_MODE_ENV\|
  RESTORE_MODE_DEPRECATION" -- crates/ deploy/ ci/ shim/ justfile` returns
  only the historical plan docs (05/06/07) and changelog entries, never live
  code or fixtures.

**Verify**

- The grep above is clean of live references.

**Commit:** `docs(changelog): record SANDBAWKS_RESTORE_MODE removal (Plan 08 C3)`

---

# Task Z: Workspace green-gate sweep + Plan 08 summary

**Files**

- `changelog/2026-06-02T...-plan-08-summary.md` — Summary; What landed
  (per workstream A/B/C); What's deliberately not here (hostPath warm cache,
  Kata-fc = Plan 09, runc-criu); Next plan preview.

**Verification gates — all must pass before Plan 08 closes:**

- `cargo test --workspace --all-features` green (incl. the standalone
  `tests/bench` crate: `cd tests/bench && cargo test`).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` silent.
- `cargo fmt --all -- --check` silent.
- `cargo doc --workspace --no-deps` warning-free.
- `cd shim && go test ./... && go vet ./...` green/silent.
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` idempotent
  (no schema change this plan, but the gate stays).
- `helm lint deploy/helm/sandbawks` clean; `helm template ...` renders the
  new RBAC.
- `kind-smoke` on `main`: pause-resume smoke green (lazy restore path),
  warm-pool smoke green (claim returns the warm sandbox name), bench resume
  p95 under the tightened gate.
- `grep -rn "RESTORE_MODE" crates/ deploy/ ci/ shim/` is clean of live code.

**Commit:** `docs: Plan 08 green-gate sweep + changelog (Plan 08 Z)`

---

## What's deliberately not in Plan 08

- **Node-local hostPath warm-snapshot cache** (spec §8, §15) — deferred;
  lazy restore covers most of the latency. Revisit if warm-start benchmarks
  demand sub-stage-download latency.
- **`io.sandbawks/lazy-restore` per-Pod opt-out annotation** — YAGNI; lazy is
  unconditional on `sandbawks-runsc`. One-task follow-up if a runtime
  regression ever needs eager fallback.
- **`KataFcAdapter`** — Plan 09. **`RuncCriuAdapter`** — Plan 10 (or deferred
  past v0.1, per spec §15).

## Next plan preview

**Plan 09 — `KataFcAdapter`**: second `RuntimeAdapter` on KVM-capable nodes
(advertised via NFD), Firecracker `MAP_PRIVATE` memory snapshot, targeting the
spec's ~50–100 ms warm-start / ~300–800 ms cold-start envelope. The warm pool
and claim path Plan 08 delivers are runtime-agnostic and light up for free on
Kata once the adapter lands.

## Definition of done

All Task Z gates green; `kubectl get sbxwp` maintains `minReady`; a
`POST /sandboxes` against a populated pool returns a warm sandbox (observably
faster than cold boot) and the sandbox reaches `Running`; resume bench p95 is
under the tightened gate with lazy restore enabled; and `SANDBAWKS_RESTORE_MODE`
is gone from all live code and fixtures.
