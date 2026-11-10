# Issue #261 — `SandboxWarmPool` reconciler + claim path (Plan 08 A)

Date: 2026-06-02T22:43:20Z
Branch: `feat-issue-261-warmpool-reconciler-claim`

Workstream A of Plan 08 (epic #260). The `SandboxWarmPool` CRD already
existed but had **zero reconciler logic**; this change builds the whole
subsystem — warm-pool top-up/trim/auto-pause, a second controller, the
atomic claim primitive, the `sandbawks-api` integration, RBAC, and a kind
smoke. Independent of Workstreams B (#262) and C (#263).

## What landed

### A1 — warm-pool label + claim annotation constants (`area/types`)
`crates/sandbawks-types/src/warm_pool.rs` gains `WARM_POOL_LABEL`
(`sandbawks.io/warm-pool`) and `CLAIMED_AT_ANNOTATION`
(`sandbawks.io/claimed-at`), re-exported from the crate root and pinned by
a stability test (the label is an external contract that survives across
controller/api versions).

### A2–A4 — reconciler module (`crates/sandbawks-controller/src/warm_pool.rs`, NEW)
Layered the same way the Sandbox reconcile loop is, so the decisions are
unit-testable without a cluster:

- `PoolMembers::bucket` — buckets listed members into `paused` vs
  in-flight `pending`, dropping `Failed`/`Terminating`.
- `status_for` — `status.ready` = `Paused` count; `warm_snapshots` = their
  names.
- `plan_pool` — pure create/trim planner: tops up toward `minReady`
  counting in-flight (so a reconcile burst doesn't over-provision), clamps
  the create count to `maxReady` (so a `min > max` misconfig can't run
  away), and trims only `Paused` excess above `maxReady`, oldest-by-name
  first.
- `warm_sandbox` — builds a warm `Sandbox` with the pool label + an
  `ownerReference` back to the pool (so deleting the pool GCs its fleet);
  the name is left to the apiserver via `generateName: sbx-warm-`, keeping
  the constructor a pure, clock-free function.
- `reconcile_pool` — glue over a `WarmPoolStore` trait (Kube + Fake impls):
  observe → plan → create/trim → auto-pause any `Running` member via
  `spec.desiredPhase = Paused` → write `status`.

### A5 — controller wiring (`bin/sandbawks-controller.rs`)
A second `kube::runtime::Controller` for `SandboxWarmPool`, namespaced to
the controller's namespace, watching its member `Sandbox`es (selected by
`WARM_POOL_LABEL`) and requeuing the owning pool via the `ownerReference`.
Runs concurrently with the existing Sandbox controller in the same
`tokio::select!`.

### A6–A7 — `sandbawks-api` claim path (`crates/sandbawks-api/src/warm_claim.rs`, NEW)
`create_sandbox` now tries a warm claim before cold-booting. `claim_for`
finds a `SandboxWarmPool` covering the requested `templateRef`, then claims
a `Paused` member via an optimistic-concurrency relabel: a JSON merge patch
that removes `WARM_POOL_LABEL` and includes the candidate's
`resourceVersion` as a precondition. Concurrent claimers race on the
apiserver's compare-and-swap — exactly one PATCH lands, losers get `409`
and `claim_for` retries the next candidate. The claimed sandbox keeps its
name (k8s names are immutable), inherits the requester's overrides
(`env`/`metadata`/`timeoutSeconds`/`runtimeClassName`), and is driven to
`Running`; its name is returned as the E2B `sandboxID`. A miss (no pool, or
the pool drained) falls through to today's cold-boot create unchanged.

The four `kube` calls are behind a `WarmClaim` trait so the orchestration
is unit-testable; `AppState` gains a `warm_claim` field (default
no-pool fake in `for_test`, swappable via `with_warm_claim`).

### A8 — RBAC + kind smoke
- `deploy/helm/sandbawks/templates/rbac.yaml`: the api ClusterRole gains
  `sandboxwarmpools` `get/list/watch` (the claim lookup). The controller
  role already carried `sandboxwarmpools` (+`/status`) and `sandboxes`
  create/delete from when the CRD was added.
- `deploy/kind/fixtures/warm-pool-smoke.sh` (NEW): applies a
  `SandboxWarmPool(minReady=1)`, waits for `status.ready==1`, asserts a
  `POST /sandboxes` returns the warm sandbox's name (a claim, not a
  cold-boot) and that the claimed sandbox resumes to `Running`. Gated
  behind `WARM_POOL_SMOKE` (default 1) and wired into `kind-smoke.yaml`
  behind `KIND_SMOKE_INCLUDE_WARM_POOL` (it reuses the pause/resume smoke's
  gVisor + S3 wiring, so it runs only when API + PAUSE_RESUME are on).

## Deviation from the plan's file map

The plan placed the claim primitive in `crates/sandbawks-controller/src/claim.rs`
and had `sandbawks-api` call into it. But `sandbawks-api` does **not**
depend on `sandbawks-controller` (and shouldn't — the controller pulls in
`rust-s3` and the whole reconcile stack, and api→controller is a layering
inversion). The claim is only ever performed by the gateway, so it lives in
`sandbawks-api/src/warm_claim.rs` where it is used — one implementation, in
the crate that needs it, rather than a dead module in the controller. The
*semantics* (atomic optimistic-concurrency relabel, 409-retry) and the
unit tests (winner/loser race, drained-pool fall-through) are exactly as
A6 specified.

## Validation

- `cargo fmt --all -- --check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace --all-targets` — green (new: 11 controller
  warm-pool tests, 4 api claim unit tests, 2 api router claim tests, 1
  types stability test).
- `helm lint deploy/helm/sandbawks` — clean; `helm template` renders the
  new api `sandboxwarmpools` rule.
- `bash -n` + `shellcheck` on `warm-pool-smoke.sh` — clean.

The kind warm-pool smoke (`status.ready==1` → claim → resume on a real
gVisor + S3 cluster) is **not** run in this session — it needs the
`just cluster-up` kind+gVisor cluster the CI `kind-smoke` job stands up.
It runs in CI behind `KIND_SMOKE_INCLUDE_WARM_POOL=1`.

## Post-CI fix — warm sandboxes must inherit the template's runtime class

The first CI run of the warm-pool smoke surfaced a real bug: warm
sandboxes were created with `runtimeClassName: None`, so their Pods ran
under the cluster's **default** runtime (runc) instead of gVisor. The
reconciler's auto-pause then fired a `runsc checkpoint` against a non-runsc
container, which failed — every warm sandbox went
`Creating → Running → Pausing → Failed`. Because `Failed` members are
dropped from both the ready and in-flight buckets, the reconciler treated
the pool as empty and cold-booted replacements **forever**, so
`status.ready` never reached `minReady` and the smoke timed out.

Fix: `reconcile_pool` now resolves the pool's `SandboxTemplate` (new
`WarmPoolStore::template_defaults`) and threads its `defaultRuntimeClass`
+ `defaultTimeoutSeconds` into `warm_sandbox` via `WarmSandboxDefaults` —
exactly the way the gateway's cold-boot path stamps the runtime class onto
a fresh `Sandbox`. A warm sandbox now boots under gVisor, the auto-pause
checkpoint succeeds, it reaches `Paused`, `status.ready` climbs to
`minReady`, and the create loop stops. Covered by two new controller tests
(`warm_sandbox` carries the runtime class; `reconcile_pool` stamps it onto
every created warm sandbox). A missing template resolves to empty defaults
with a `WARN`, matching the gateway's lenient cold-boot lookup.

## Review hardening (PR #287 review #116)

Addressed the actionable review findings:

- **Claim 404 ≡ 409** (`warm_claim.rs`): `relabel_claim` now folds `404`
  (candidate deleted/consumed between list and patch) to `Ok(false)` like
  the `409` CAS loss, so the loop tries the next candidate instead of
  surfacing a 5xx to a caller who'd happily cold-boot.
- **OwnerReference UID guard** (`warm_pool.rs`): `reconcile_pool` refuses
  to create warm sandboxes when the pool has no apiserver UID (an
  `Invariant` error, consistent with the existing namespace guard) — an
  empty owner UID would silently defeat kube GC and orphan the fleet.
- **Watch mapper filters `controller: true`** (binary): a member is
  requeued only for its *controlling* pool.
- **Trim by `creationTimestamp`, name-tiebroken** (`plan_pool`): oldest
  (most snapshot-stale) member trimmed first, robust to apiserver
  `generateName` suffix changes.
- **Structured reconcile + claim logs**: `reconcile_pool` emits one
  `info` line per pass (pool, namespace, ready, pending, min/max, create,
  trim, pause) so a pool stuck below its floor is visible at the default
  level; `claim_for` logs `outcome="hit"` (info, with pool + races_lost)
  and `outcome="miss_drained"` (debug).
- **Smoke asserts `runtimeClassName=gvisor`** on the warm member, so the
  runtime-class regression fails with a clear message rather than an
  opaque 240s timeout.

Deferred (with reasoning, replied on-thread):

- **Prometheus counters / histograms / gauges** for claim-rate, claim
  latency, and warm-pool depth: there is **no metrics pipeline anywhere in
  the project** today (no `metrics` crate, exporter, or `/metrics`
  endpoint — only the `middleware.rs` note pointing at future Prometheus
  work). Emitting `counter!`/`histogram!` macros with no installed recorder
  is a no-op; this SLI surface belongs with that planned cross-cutting
  work, not bolted onto one feature. Structured logs added now as the
  interim signal.
- **`join_all` for the create/trim/pause batches**: kept sequential
  deliberately — a cold `minReady=20` pool firing 20 concurrent CREATEs
  risks apiserver priority-and-fairness throttling, whereas sequential
  writes are naturally paced and the per-reconcile burst only happens once
  (subsequent reconciles create 0 because in-flight members are counted).

Closes #261.
