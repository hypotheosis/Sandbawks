//! sandbawks-controller binary entrypoint.
//!
//! Boots tracing, builds an in-cluster kube client, wires
//! `KubeSandboxStore` + `FakeRuntimeAdapter`, and runs the kube-rs
//! `Controller` over `Sandbox` CRs. Plan 05 swaps the fake adapter
//! out for the real `GvisorAdapter`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use k8s_openapi::api::{coordination::v1::Lease, core::v1::Pod};
use kube::{
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
    Api, Client, ResourceExt,
};
use sandbawks_controller::{
    leader_election::{try_acquire_or_renew, LeaderElectionConfig, LeaseOutcome},
    reconcile, reconcile_metrics, reconcile_pool, run_retention_pass,
    s3_usage::{S3UsageLedger, S3UsageSink, S3UsageSource},
    usage::{NoopUsageSink, UsageEventSink},
    usage_ledger::run_usage_rollup_pass,
    usage_spill::{FsUsageSpillQueue, NoopUsageSpillQueue, UsageSpillQueue},
    AzureRetentionStore, ControllerError, FakeRuntimeAdapter, KubeOpenIntervalSource,
    KubeSandboxStore, KubeSandboxTemplateStore, KubeSnapshotJobStore, KubeWarmPoolStore,
    PodAdapter, PvcRetentionConfig, ReconcileContext, RetentionConfig, S3RetentionStore,
    SnapshotRetentionStore, WarmPoolStore, SANDBOX_POD_LABEL_KEY, SANDBOX_POD_LABEL_VALUE,
};
use sandbawks_types::warm_pool::WARM_POOL_LABEL;
use sandbawks_types::{
    parse_iso8601_duration, RuntimeAdapter, Sandbox, SandboxPhase, SandboxTemplate,
    SandboxWarmPool, SnapshotJob, SnapshotProvider,
};
use tokio::signal;

/// Which `RuntimeAdapter` impl the controller binds at startup.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum RuntimeChoice {
    /// No-op adapter — does not create real Pods. Default; matches the
    /// Plan 02 lifecycle tests and CI smoke runs against `kind` without
    /// envd images loaded.
    Fake,
    /// Real Pod-materializing adapter (Plan 04 T5). Issues `POST /pods`
    /// against the kube API; required for the gateway integration test.
    Pod,
}

/// CLI surface for the controller binary.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Which runtime adapter to use.
    #[arg(
        long,
        value_enum,
        default_value_t = RuntimeChoice::Fake,
        env = "SANDBAWKS_RUNTIME"
    )]
    runtime: RuntimeChoice,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI before initialising telemetry so `--help` / `--version` /
    // arg-parse errors short-circuit (clap calls `process::exit`, which does
    // not run the guard's `Drop`) *before* any OTLP exporter is built — and
    // so the four binaries share one ordering (`sandbawks-api` does the same).
    let cli = Cli::parse();

    // Initialise the shared OpenTelemetry bootstrap (logs, metrics, traces over
    // OTLP) and hold the guard for the whole of `main`. The guard MUST stay
    // bound to a named variable: the batch exporters flush asynchronously, so
    // dropping it early — or with `let _ = ...`, which drops immediately —
    // exports nothing. It flushes all three pipelines when `main` returns.
    //
    // With `OTEL_SDK_DISABLED` set (the local default) this installs only the
    // stderr fmt subscriber and builds no exporter, so `cargo run` against a
    // kind cluster needs no collector. Root spans for the reconcile loop come
    // from `#[instrument]` (Plan 04 Phase 4), not a `TraceLayer`; this phase
    // only establishes the provider + subscriber.
    //
    // Ordering note (mirrors `sandbawks-api`): the first OTLP exporter is built
    // here, *before* `Client::try_default()` below resolves in-cluster
    // credentials, which can take non-trivial time. If you set
    // `OTEL_EXPORTER_OTLP_TIMEOUT`, keep it generous enough to cover that kube
    // startup or the earliest exports can spuriously time out during boot.
    // The re-arm handle is only used by checkpoint/restore-capable binaries
    // (envd); the controller never pauses, so it ignores it.
    let (_telemetry, _rearm) =
        sandbawks_observability::init_telemetry("sandbawks-controller", env!("CARGO_PKG_VERSION"))
            .context("initialising OpenTelemetry SDK")?;

    // Fail fast on a relative restore staging root (issue #512). Helm feeds this
    // and the node's staging root from one guarded helper so the deployed path is
    // safe; this guards a direct/standalone invocation that bypasses Helm from
    // silently recording an unresolvable io.sandbawks/restore-image-path.
    let restore_staging_root = reconcile::ensure_restore_staging_root_absolute()?;

    let client = Client::try_default()
        .await
        .context("building in-cluster kube client")?;
    // `SANDBAWKS_NAMESPACE` no longer scopes the watches — those are
    // cluster-wide (`Api::all`) so one controller reconciles every namespace
    // (#582). It is retained solely for the leader-election Lease (which lives
    // in the controller's own namespace) and telemetry resource attributes.
    let namespace = std::env::var("SANDBAWKS_NAMESPACE").unwrap_or_else(|_| "default".into());

    let sandboxes: Api<Sandbox> = Api::all(client.clone());
    let snapshot_jobs_api: Api<SnapshotJob> = Api::all(client.clone());
    let store = Arc::new(KubeSandboxStore::new(client.clone()));
    let adapter: Arc<dyn RuntimeAdapter> = match cli.runtime {
        RuntimeChoice::Fake => Arc::new(FakeRuntimeAdapter::new("fake")),
        RuntimeChoice::Pod => Arc::new(PodAdapter::new(client.clone())),
    };
    let snapshot_jobs = Arc::new(KubeSnapshotJobStore::new(client.clone()));
    let templates = Arc::new(KubeSandboxTemplateStore::new(client.clone()));
    let pvc_retention = pvc_retention_config_from_env().context("building PVC retention config")?;
    if pvc_retention.enabled {
        tracing::info!(
            default_duration = ?pvc_retention.default_duration,
            "PVC-retention GC enabled: paused filesystem sandboxes past their window will be \
             deleted (cascading their PVCs)"
        );
    }
    let (usage_sink, usage_sink_real) = build_usage_sink();
    let usage_spill = build_usage_spill_queue();
    let ctx = Arc::new(ReconcileContext {
        store,
        adapter,
        snapshot_jobs,
        templates,
        client: Some(client.clone()),
        pvc_retention,
        usage_sink: Arc::clone(&usage_sink),
        usage_spill: Arc::clone(&usage_spill),
    });
    // Replay parked usage events once the store recovers (#636). Only meaningful
    // with a real sink to replay to and a spill directory to drain.
    let usage_spill_drainer =
        match build_usage_spill_drainer(usage_spill, usage_sink, usage_sink_real) {
            Ok(handle) => handle,
            Err(e) => return Err(e).context("building usage spill drainer"),
        };

    tracing::info!(
        namespace = %namespace,
        runtime = ?cli.runtime,
        restore_staging_root = %restore_staging_root,
        "starting sandbawks-controller"
    );

    // Leader election gate (issue #436). When enabled, the controller polls
    // until it wins the `coordination.k8s.io/v1` Lease, then starts a
    // background renewal task. If the lease is ever lost (e.g. the leader
    // Pod stalls) the renewal task calls `process::exit(1)` so Kubernetes
    // restarts this Pod and a standby replica can claim the Lease within one
    // TTL (~15 s). When disabled (the default `replicas: 1` path) this block
    // is skipped and the binary behaves exactly as before.
    //
    // Because this gate precedes both the retention sweeper and both
    // Controller loops, ALL controller work — including snapshot GC — runs
    // only on the elected leader. Standby replicas spin in the acquire loop
    // and never start the sweeper, satisfying the "sweeper must only run on
    // the leader" requirement.
    if let Some(le_config) = LeaderElectionConfig::from_env()
        .context("reading leader election config from environment")?
    {
        let lease_api: Api<Lease> = Api::namespaced(client.clone(), &namespace);

        tracing::info!(
            lease  = %le_config.lease_name(),
            holder = %le_config.holder_identity(),
            "leader election enabled — waiting for lease"
        );

        // Acquire loop: spin until we hold the Lease. `Standby` (another
        // replica holds a valid Lease) and `Conflict` (we lost a create/PUT
        // race) both just mean "keep waiting"; only `Held` breaks out.
        loop {
            match tokio::time::timeout(
                le_config.renew_deadline(),
                try_acquire_or_renew(&lease_api, &le_config),
            )
            .await
            {
                Ok(Ok(LeaseOutcome::Held)) => {
                    tracing::info!(
                        lease  = %le_config.lease_name(),
                        holder = %le_config.holder_identity(),
                        "acquired leader lease — starting controller work"
                    );
                    break;
                }
                Ok(Ok(LeaseOutcome::Standby | LeaseOutcome::Conflict)) => {
                    tracing::debug!(
                        lease = %le_config.lease_name(),
                        "standby: another replica holds the lease"
                    );
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "lease acquire error — retrying");
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        deadline = ?le_config.renew_deadline(),
                        "lease acquire timed out — retrying"
                    );
                }
            }
            tokio::time::sleep(le_config.retry_period()).await;
        }

        // Background renewal: keep the Lease alive while we run. The task
        // tracks the time since the last *successful* renewal and exits the
        // process when that exceeds `renew_deadline` — NOT merely when a single
        // call times out. A steady stream of fast-failing renew calls (an
        // apiserver blip, throttling, a partial network partition) would
        // otherwise keep this Pod believing it is still the leader — running
        // both Controller loops and the retention sweeper — while the Lease
        // expires at the apiserver and a standby legitimately takes over. That
        // is the exact double-reconcile / double-sweep this gate exists to
        // prevent (issue #436 review). On exit, Kubernetes restarts the Pod and
        // it re-enters the standby acquire loop above.
        let renewal_api: Api<Lease> = Api::namespaced(client.clone(), &namespace);
        let renewal_cfg = le_config.clone();
        tokio::spawn(async move {
            let renew_deadline = renewal_cfg.renew_deadline();
            let mut ticker = tokio::time::interval(renewal_cfg.retry_period());
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Seeded at acquisition: we hold the Lease as the loop begins.
            let mut last_success = Instant::now();
            loop {
                ticker.tick().await;
                if last_success.elapsed() > renew_deadline {
                    // NOTE: `process::exit` skips the OTel `_telemetry` guard's
                    // Drop, so the final renew-failure traces/metrics may be
                    // dropped. Accepted tradeoff: stopping the double-reconcile
                    // outweighs a clean telemetry flush on leader loss.
                    tracing::error!(
                        lease    = %renewal_cfg.lease_name(),
                        deadline = ?renew_deadline,
                        "renew deadline exceeded since last successful renewal — \
                         exiting for restart"
                    );
                    std::process::exit(1);
                }
                match tokio::time::timeout(
                    renew_deadline,
                    try_acquire_or_renew(&renewal_api, &renewal_cfg),
                )
                .await
                {
                    Ok(Ok(LeaseOutcome::Held)) => {
                        last_success = Instant::now();
                        tracing::debug!(lease = %renewal_cfg.lease_name(), "leader lease renewed");
                    }
                    Ok(Ok(LeaseOutcome::Standby)) => {
                        // Confirmed loss: a GET saw another replica holding a
                        // valid Lease. Exit immediately so the restart re-enters
                        // the standby loop. (Same telemetry-flush caveat as the
                        // deadline branch above.)
                        tracing::error!(
                            lease = %renewal_cfg.lease_name(),
                            "lost leader lease to another replica — exiting for restart"
                        );
                        std::process::exit(1);
                    }
                    Ok(Ok(LeaseOutcome::Conflict)) => {
                        // HTTP 409: the Lease changed under us. This fires both
                        // on a real takeover and on a benign third-party write
                        // (kubectl edit, an admission webhook) — so do NOT exit
                        // on it. The next tick re-reads: if we genuinely lost
                        // the Lease, that GET returns `Standby` and we exit
                        // there; otherwise we renew. The `last_success` deadline
                        // above bounds how long we tolerate persistent conflicts.
                        tracing::warn!(
                            lease = %renewal_cfg.lease_name(),
                            "lease renewal conflicted (409) — re-checking next tick"
                        );
                    }
                    Ok(Err(e)) => {
                        // Transient API error — log and retry. The deadline
                        // check at the top of the loop handles persistence.
                        tracing::warn!(error = %e, "lease renewal error (transient)");
                    }
                    Err(_elapsed) => {
                        // A single call exceeded `renew_deadline`. Don't exit on
                        // one slow call; the deadline check handles a sustained
                        // inability to renew.
                        tracing::warn!(
                            lease    = %renewal_cfg.lease_name(),
                            deadline = ?renew_deadline,
                            "lease renewal call timed out — re-checking next tick"
                        );
                    }
                }
            }
        });
    }

    // Snapshot retention GC (issue #186). Constructed early so the
    // ticker future joins the `tokio::select!` below alongside the
    // kube `Controller` — both shut down on SIGINT together.
    let retention_handle = match build_retention_runner() {
        Ok(Some(handle)) => Some(handle),
        Ok(None) => {
            // Either the operator never opted in (default-off) or
            // they opted in without credentials. The latter case is
            // already logged at WARN inside `build_retention_runner`;
            // this INFO line covers the silent default-off case.
            tracing::info!("snapshot retention sweeper not started");
            None
        }
        Err(e) => {
            // Boot-time misconfig (e.g. unparseable duration) is a
            // controller-startup failure: better to surface it now
            // than silently disable GC and let snapshots accumulate.
            return Err(e).context("building retention runner");
        }
    };

    // Phase 3 usage-ledger rollup ticker (epic #480, issue #632). Independent of
    // the retention sweeper; same opt-in + same `tokio::select!`-joined lifetime.
    let usage_rollup_handle = match build_usage_rollup_runner(client.clone()) {
        Ok(Some(handle)) => Some(handle),
        Ok(None) => {
            tracing::info!("usage-ledger rollup not started");
            None
        }
        Err(e) => return Err(e).context("building usage rollup runner"),
    };

    // Owned-Pod watch (issue #328): sandbox Pods carry a controller
    // `ownerReference` to their `Sandbox`, so `.owns::<Pod>` maps a Pod
    // event back to its owner with no custom mapper. A Pod reaching a
    // terminal phase (envd's snapshot-and-exit, OOM-kill, eviction) thus
    // requeues its owner within ~1s instead of waiting on the 30s
    // `Running` heartbeat. The watch is scoped to sandbox Pods via the
    // `app.kubernetes.io/name=sandbawks-sandbox` selector so node/api Pod
    // churn doesn't drive needless reconciles. The liveness probe from
    // #325 (`running_liveness_trigger`) remains the decision logic and the
    // heartbeat stays as the safety net; this only tightens detection
    // latency.
    let sandbox_pods: Api<Pod> = Api::all(client.clone());
    let pod_selector = format!("{SANDBOX_POD_LABEL_KEY}={SANDBOX_POD_LABEL_VALUE}");

    // Watch SnapshotJob CRs and requeue the parent Sandbox whenever a
    // job's `status` changes. The mapping function reads
    // `spec.sandboxName` (Plan 05 T2) — every job is tied to exactly
    // one Sandbox by name.
    let controller = Controller::new(sandboxes, watcher::Config::default())
        .owns(
            sandbox_pods,
            watcher::Config::default().labels(&pod_selector),
        )
        .watches(
            snapshot_jobs_api,
            watcher::Config::default(),
            |job: SnapshotJob| {
                let mut obj_ref = kube::runtime::reflector::ObjectRef::new(&job.spec.sandbox_name);
                if let Some(ns) = job.namespace() {
                    obj_ref = obj_ref.within(&ns);
                }
                std::iter::once(obj_ref)
            },
        )
        .shutdown_on_signal()
        .run(reconcile_adapter, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "reconcile error"),
            }
        });

    // Second controller: keep each `SandboxWarmPool` topped up to
    // `spec.minReady`. It watches its member `Sandbox`es (the ones
    // carrying `WARM_POOL_LABEL`) so a member reaching `Paused`/`Running`
    // requeues its owning pool immediately via the ownerReference, rather
    // than waiting for the 10s heartbeat. Runs concurrently with the
    // Sandbox controller in the same `tokio::select!`.
    let warm_pools: Api<SandboxWarmPool> = Api::all(client.clone());
    let warm_members: Api<Sandbox> = Api::all(client.clone());
    let warm_templates: Api<SandboxTemplate> = Api::all(client.clone());
    let warm_store = Arc::new(KubeWarmPoolStore::new(client.clone()));
    // Build the pool controller in two steps so we can grab a reader over its
    // own `SandboxWarmPool` reflector store (issue #397). The `SandboxTemplate`
    // watch mapper below uses it to fan a template edit out to every pool that
    // references it; the controller populates this store as it runs, so the
    // mapper sees the live set of pools without a second reflector.
    let warm_pool_controller = Controller::new(warm_pools, watcher::Config::default());
    let pool_store = warm_pool_controller.store();
    let warm_controller = warm_pool_controller
        .watches(
            warm_members,
            watcher::Config::default().labels(WARM_POOL_LABEL),
            |s: Sandbox| {
                let namespace = s.namespace();
                s.owner_references()
                    .iter()
                    // Only the controlling pool owns this member; a stray
                    // non-controller SandboxWarmPool owner ref (shouldn't
                    // happen, but guard it) must not requeue a foreign pool.
                    .filter(|o| o.kind == "SandboxWarmPool" && o.controller == Some(true))
                    .map(|o| {
                        let mut obj_ref = kube::runtime::reflector::ObjectRef::new(&o.name);
                        if let Some(ns) = namespace.clone() {
                            obj_ref = obj_ref.within(&ns);
                        }
                        obj_ref
                    })
                    .collect::<Vec<_>>()
            },
        )
        // Watch `SandboxTemplate` so editing a template (new image, runtime
        // class, or resources) requeues every pool that references it within
        // ~1s instead of waiting for the 10s heartbeat (issue #397). The
        // pool's reconcile then evicts unclaimed members minted against the
        // old `resourceVersion` and re-mints from the updated template.
        .watches(
            warm_templates,
            watcher::Config::default(),
            move |t: SandboxTemplate| {
                let template_name = t.name_any();
                let template_namespace = t.namespace();
                // `pool_store.state()` is empty until the pool reflector's
                // initial list completes, so a template event that fires inside
                // the controller's boot window fans out to zero pools. That is
                // still safe: the pool's own first reconcile resolves the
                // template's current `resourceVersion` via `template_defaults`
                // and evicts any stale members itself. It just means the "~1s
                // requeue on template edit" path doesn't apply for edits made
                // during a restart — don't assume this mapper is the only thing
                // that catches them.
                pool_store
                    .state()
                    .into_iter()
                    // Only same-namespace pools that name this template; a
                    // SandboxTemplate is namespaced, and a pool's templateRef
                    // resolves within the pool's own namespace. Require both
                    // namespaces to be present and equal via an explicit
                    // `Some` match: both objects are namespaced so each side is
                    // always `Some`, but matching on `Some` keeps a
                    // hypothetical double-`None` from comparing equal and
                    // spuriously passing the filter.
                    .filter(|p| {
                        p.spec.template_ref == template_name
                            && matches!(
                                (p.namespace(), template_namespace.as_deref()),
                                (Some(pool_ns), Some(tpl_ns)) if pool_ns == tpl_ns
                            )
                    })
                    .map(|p| {
                        let mut obj_ref = kube::runtime::reflector::ObjectRef::new(&p.name_any());
                        if let Some(ns) = p.namespace() {
                            obj_ref = obj_ref.within(&ns);
                        }
                        obj_ref
                    })
                    .collect::<Vec<_>>()
            },
        )
        .shutdown_on_signal()
        .run(
            reconcile_warm_pool_adapter,
            warm_pool_error_policy,
            warm_store,
        )
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "warm-pool reconciled"),
                Err(e) => tracing::warn!(error = %e, "warm-pool reconcile error"),
            }
        });

    // Provisions per-tenant RBAC (SA + Role + RoleBinding) from each
    // TeamAPIKey's spec.namespace so the api can impersonate the tenant SA
    // (#583). Watches TeamAPIKey cluster-wide; runs alongside the Sandbox and
    // warm-pool controllers under the same client + leader-election gate.
    let tenant_rbac_controller = sandbawks_controller::tenant_rbac::controller(client.clone());

    tokio::select! {
        _ = controller => {},
        _ = warm_controller => {},
        _ = tenant_rbac_controller => {},
        _ = signal::ctrl_c() => tracing::info!("SIGINT received, shutting down"),
    }

    if let Some(handle) = retention_handle {
        handle.abort();
    }
    if let Some(handle) = usage_rollup_handle {
        handle.abort();
    }
    if let Some(handle) = usage_spill_drainer {
        handle.abort();
    }

    Ok(())
}

/// Build the PVC-retention GC config from env (epic #603 Phase 5 / issue #608).
///
/// `SANDBAWKS_PVC_RETENTION_ENABLED` is the master switch — default OFF, the
/// same fail-safe stance as the snapshot retention sweeper, so a chart upgrade
/// that adds the var without the operator flipping it on never auto-deletes a
/// sandbox. `SANDBAWKS_PVC_RETENTION_DEFAULT_DURATION` (ISO-8601, default `P7D`)
/// is the window applied to a paused filesystem sandbox that does not set its
/// own `spec.pvcRetention`. The default duration is parsed even when disabled so
/// a typo surfaces at startup rather than the day the operator opts in.
/// Build the authoritative usage-event sink (epic #480 Phase 2, issue #631).
///
/// Master switch `SANDBAWKS_USAGE_METERING_ENABLED` (default OFF) gates it, so a
/// chart upgrade that surfaces the var does not start writing usage objects until
/// the operator opts in. When enabled it reuses the **same** S3 connection env vars
/// as snapshot retention (`SANDBAWKS_S3_*`), writing one NDJSON event per closed
/// `Running` interval under `SANDBAWKS_USAGE_PREFIX` (default `usage`). The Phase 3
/// rollup (#632) reads beneath that prefix.
///
/// Never fails startup: usage metering is an opt-in p2 signal and must not
/// destabilize the core control loop. Any incomplete/unsupported configuration logs
/// a WARN and falls back to the [`NoopUsageSink`] (metering effectively off) rather
/// than crashing the controller. Azure is not yet wired here — S3-compatible object
/// stores only (a follow-up adds the Azure sink, mirroring `AzureRetentionStore`).
/// Returns the usage sink and whether it is a **real** (object-store-backed) sink
/// — `false` for the `NoopUsageSink` fallback. The flag gates the spill drainer:
/// there is nothing to replay parked events to without a real sink.
fn build_usage_sink() -> (Arc<dyn UsageEventSink>, bool) {
    let enabled = std::env::var("SANDBAWKS_USAGE_METERING_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    if !enabled {
        tracing::info!("usage metering disabled (SANDBAWKS_USAGE_METERING_ENABLED unset)");
        return (Arc::new(NoopUsageSink), false);
    }

    // S3-compatible backend only for now. An `azure` provider (or any unknown
    // value) falls back to Noop with a warning rather than failing startup.
    let provider = std::env::var("SANDBAWKS_SNAPSHOT_PROVIDER").unwrap_or_else(|_| "s3".into());
    if provider != "s3" {
        tracing::warn!(
            provider = %provider,
            "usage metering enabled but only the s3 provider is supported today; \
             metering disabled (Noop sink)"
        );
        return (Arc::new(NoopUsageSink), false);
    }

    let endpoint = std::env::var("SANDBAWKS_S3_ENDPOINT").unwrap_or_default();
    let bucket = std::env::var("SANDBAWKS_S3_BUCKET").unwrap_or_default();
    let access_key = std::env::var("SANDBAWKS_S3_ACCESS_KEY").unwrap_or_default();
    let secret_key = std::env::var("SANDBAWKS_S3_SECRET_KEY").unwrap_or_default();
    if endpoint.is_empty() || bucket.is_empty() || access_key.is_empty() || secret_key.is_empty() {
        tracing::warn!(
            "SANDBAWKS_USAGE_METERING_ENABLED=true but the S3 connection \
             (SANDBAWKS_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY) is incomplete; \
             usage metering disabled (Noop sink)"
        );
        return (Arc::new(NoopUsageSink), false);
    }
    let region = std::env::var("SANDBAWKS_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
    let prefix = std::env::var("SANDBAWKS_USAGE_PREFIX").unwrap_or_else(|_| "usage".into());

    match S3UsageSink::new(
        endpoint,
        bucket,
        region,
        access_key,
        secret_key,
        prefix.clone(),
    ) {
        Ok(sink) => {
            tracing::info!(
                prefix = %prefix,
                "usage metering enabled: authoritative usage events → S3 (epic #480 Phase 2)"
            );
            (Arc::new(sink), true)
        }
        Err(e) => {
            tracing::warn!(error = %e, "building S3 usage sink failed; usage metering disabled");
            (Arc::new(NoopUsageSink), false)
        }
    }
}

/// The configured usage-spill directory (the controller's spill PVC mount), or
/// `None` when `SANDBAWKS_USAGE_SPILL_DIR` is unset/blank (spilling disabled).
fn usage_spill_dir() -> Option<String> {
    std::env::var("SANDBAWKS_USAGE_SPILL_DIR")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

/// Build the durable usage-spill queue (#636). A configured spill directory yields
/// a PVC-backed [`FsUsageSpillQueue`]; otherwise the [`NoopUsageSpillQueue`] (a
/// failed emission falls back to the pre-spill drop behaviour).
fn build_usage_spill_queue() -> Arc<dyn UsageSpillQueue> {
    match usage_spill_dir() {
        Some(dir) => {
            tracing::info!(
                dir = %dir,
                "usage spill queue enabled: emissions that fail after retries are parked for replay (#636)"
            );
            Arc::new(FsUsageSpillQueue::new(dir))
        }
        None => Arc::new(NoopUsageSpillQueue),
    }
}

/// Compose the spill-drainer ticker (#636): periodically replay parked usage events
/// through `sink` once the object store recovers.
///
/// Spawns only when a spill directory is configured AND the sink is real (parked
/// events can't be replayed to a Noop sink — draining there would silently delete
/// them). `Err` only on an unparseable interval.
fn build_usage_spill_drainer(
    spill: Arc<dyn UsageSpillQueue>,
    sink: Arc<dyn UsageEventSink>,
    sink_is_real: bool,
) -> Result<Option<tokio::task::JoinHandle<()>>> {
    let Some(dir) = usage_spill_dir() else {
        return Ok(None);
    };
    if !sink_is_real {
        tracing::warn!(
            "SANDBAWKS_USAGE_SPILL_DIR is set but the usage sink is not configured; \
             spill drainer not started (parked events can't be replayed without a real sink)"
        );
        return Ok(None);
    }
    let interval_raw =
        std::env::var("SANDBAWKS_USAGE_SPILL_DRAIN_INTERVAL").unwrap_or_else(|_| "PT1M".into());
    let interval = parse_iso8601_duration(&interval_raw)
        .with_context(|| format!("parsing SANDBAWKS_USAGE_SPILL_DRAIN_INTERVAL={interval_raw}"))?;

    tracing::info!(
        interval_secs = interval.as_secs(),
        dir = %dir,
        "usage spill drainer enabled (#636)"
    );
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match spill.drain(sink.as_ref()).await {
                Ok(report) => {
                    if report.replayed > 0 || report.failed > 0 {
                        tracing::info!(
                            replayed = report.replayed,
                            failed = report.failed,
                            "usage spill drain complete"
                        );
                    } else {
                        tracing::debug!("usage spill drain: nothing parked to replay");
                    }
                }
                Err(e) => tracing::error!(error = %e, "usage spill drain failed; will retry"),
            }
        }
    });
    Ok(Some(handle))
}

/// Compose the usage-ledger rollup ticker (epic #480 Phase 3, issue #632), if env
/// vars indicate it should run.
///
/// Master switch `SANDBAWKS_USAGE_ROLLUP_ENABLED` (default OFF). When on, it reads
/// the authoritative Phase 2 events under `SANDBAWKS_USAGE_PREFIX` (default `usage`)
/// and writes rolled `UsageRecord`s under `SANDBAWKS_USAGE_LEDGER_PREFIX` (default
/// `ledger`), both in the same S3 bucket as snapshot retention, on a
/// `SANDBAWKS_USAGE_ROLLUP_INTERVAL` cadence (ISO-8601, default `PT1H`).
///
/// Returns `Ok(None)` for the opt-in-but-not-yet-configured cases (default off, the
/// non-`s3` provider, or incomplete S3 credentials — the latter two warn), so a
/// staged rollout never crashes the controller; `Err` only on a genuinely
/// unparseable interval, surfaced as a startup failure.
fn build_usage_rollup_runner(client: kube::Client) -> Result<Option<tokio::task::JoinHandle<()>>> {
    let enabled = std::env::var("SANDBAWKS_USAGE_ROLLUP_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    if !enabled {
        return Ok(None);
    }

    let provider = std::env::var("SANDBAWKS_SNAPSHOT_PROVIDER").unwrap_or_else(|_| "s3".into());
    if provider != "s3" {
        tracing::warn!(
            provider = %provider,
            "usage rollup enabled but only the s3 provider is supported today; rollup not started"
        );
        return Ok(None);
    }

    let interval_raw =
        std::env::var("SANDBAWKS_USAGE_ROLLUP_INTERVAL").unwrap_or_else(|_| "PT1H".into());
    let interval = parse_iso8601_duration(&interval_raw)
        .with_context(|| format!("parsing SANDBAWKS_USAGE_ROLLUP_INTERVAL={interval_raw}"))?;

    let endpoint = std::env::var("SANDBAWKS_S3_ENDPOINT").unwrap_or_default();
    let bucket = std::env::var("SANDBAWKS_S3_BUCKET").unwrap_or_default();
    let access_key = std::env::var("SANDBAWKS_S3_ACCESS_KEY").unwrap_or_default();
    let secret_key = std::env::var("SANDBAWKS_S3_SECRET_KEY").unwrap_or_default();
    if endpoint.is_empty() || bucket.is_empty() || access_key.is_empty() || secret_key.is_empty() {
        tracing::warn!(
            "SANDBAWKS_USAGE_ROLLUP_ENABLED=true but the S3 connection \
             (SANDBAWKS_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY) is incomplete; \
             usage rollup not started"
        );
        return Ok(None);
    }
    let region = std::env::var("SANDBAWKS_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
    let event_prefix = std::env::var("SANDBAWKS_USAGE_PREFIX").unwrap_or_else(|_| "usage".into());
    let ledger_prefix =
        std::env::var("SANDBAWKS_USAGE_LEDGER_PREFIX").unwrap_or_else(|_| "ledger".into());

    let source = S3UsageSource::new(
        endpoint.clone(),
        bucket.clone(),
        region.clone(),
        access_key.clone(),
        secret_key.clone(),
        event_prefix.clone(),
    )
    .context("building S3 usage source")?;
    let ledger = S3UsageLedger::new(
        endpoint,
        bucket,
        region,
        access_key,
        secret_key,
        ledger_prefix.clone(),
    )
    .context("building S3 usage ledger")?;
    // In-progress usage for still-running sandboxes (#637): the rollup also accounts
    // each `Running` sandbox's open interval up to `now`, so long-lived sandboxes
    // accrue billable usage every pass rather than only at close.
    let open_source = KubeOpenIntervalSource::new(client);

    tracing::info!(
        interval_secs = interval.as_secs(),
        event_prefix = %event_prefix,
        ledger_prefix = %ledger_prefix,
        "usage-ledger rollup enabled (epic #480 Phase 3; open-interval accounting #637)"
    );

    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Keep spacing stable when a rollup runs long (large bucket); don't
        // burst-fire missed ticks onto an in-flight pass.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match run_usage_rollup_pass(&source, &open_source, &ledger, chrono::Utc::now()).await {
                Ok(report) => {
                    if report.records_written > 0 || report.failed > 0 {
                        tracing::info!(
                            events_read = report.events_read,
                            open_intervals = report.open_intervals,
                            records_written = report.records_written,
                            failed = report.failed,
                            "usage rollup complete"
                        );
                    } else {
                        tracing::debug!(
                            events_read = report.events_read,
                            open_intervals = report.open_intervals,
                            "usage rollup complete (no records)"
                        );
                    }
                }
                Err(e) => tracing::error!(error = %e, "usage rollup pass failed; will retry"),
            }
        }
    });
    Ok(Some(handle))
}

fn pvc_retention_config_from_env() -> Result<PvcRetentionConfig> {
    let enabled = std::env::var("SANDBAWKS_PVC_RETENTION_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let default_duration_raw =
        std::env::var("SANDBAWKS_PVC_RETENTION_DEFAULT_DURATION").unwrap_or_else(|_| "P7D".into());
    let default_duration = parse_iso8601_duration(&default_duration_raw).with_context(|| {
        format!("parsing SANDBAWKS_PVC_RETENTION_DEFAULT_DURATION={default_duration_raw}")
    })?;
    Ok(PvcRetentionConfig {
        enabled,
        default_duration,
    })
}

/// Compose the retention ticker future, if env vars indicate the
/// retention reconciler is meant to run.
///
/// The backend is selected by `SANDBAWKS_SNAPSHOT_PROVIDER` (default
/// `s3`); `azure` builds an [`AzureRetentionStore`] instead.
///
/// Returns:
/// - `Ok(Some(handle))` when retention is enabled AND the selected
///   provider is fully configured — the handle is a spawned task.
/// - `Ok(None)` for the S3 staged-rollout case where retention is
///   enabled but `SANDBAWKS_S3_ENDPOINT` is empty (the operator hasn't
///   pointed the controller at a bucket yet). Logged at WARN.
/// - `Err(_)` on misconfig (unparseable duration, missing required
///   provider credentials, etc.) — surfaces as a controller startup
///   failure.
fn build_retention_runner() -> Result<Option<tokio::task::JoinHandle<()>>> {
    // `_RETENTION_ENABLED` is the master switch. Default OFF so a
    // chart upgrade that adds the env var (but operator hasn't
    // explicitly flipped it on) doesn't start deleting things.
    let enabled = std::env::var("SANDBAWKS_RETENTION_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    if !enabled {
        return Ok(None);
    }

    let default_duration_raw =
        std::env::var("SANDBAWKS_RETENTION_DEFAULT_DURATION").unwrap_or_else(|_| "P7D".into());
    let default_duration = parse_iso8601_duration(&default_duration_raw).with_context(|| {
        format!("parsing SANDBAWKS_RETENTION_DEFAULT_DURATION={default_duration_raw}")
    })?;
    let scan_interval_raw =
        std::env::var("SANDBAWKS_RETENTION_SCAN_INTERVAL").unwrap_or_else(|_| "PT1H".into());
    let scan_interval = parse_iso8601_duration(&scan_interval_raw).with_context(|| {
        format!("parsing SANDBAWKS_RETENTION_SCAN_INTERVAL={scan_interval_raw}")
    })?;

    // `S3SnapshotSink` nests each sandbox under `<key_prefix>/<sandbox-id>/`,
    // so the controller LISTs the whole bucket (delimiter-less, recursing
    // into the per-sandbox subtrees) — no namespace scoping in this loop
    // yet. Per-namespace iteration will live in the caller (which will
    // enumerate namespaces and call `run_retention_pass` per-ns with
    // `format!("{ns}/")`), not inside this builder. Until then the prefix is
    // empty and the log line below carries `SANDBAWKS_NAMESPACE` purely for
    // operator orientation, not because it scopes the sweep.
    let cfg = RetentionConfig {
        enabled: true,
        default_duration,
        scan_interval,
        prefix: String::new(),
    };

    // Provider selection mirrors the node's `build_snapshot_pair`
    // dispatch (Task 5). Default S3 so an existing chart that hasn't
    // set the var keeps its current backend.
    let provider = std::env::var("SANDBAWKS_SNAPSHOT_PROVIDER")
        .ok()
        .map(|v| v.parse::<SnapshotProvider>())
        .transpose()
        .context("parsing SANDBAWKS_SNAPSHOT_PROVIDER")?
        .unwrap_or_default();

    let store: Arc<dyn SnapshotRetentionStore> = match provider {
        SnapshotProvider::S3 => {
            // Without an endpoint there's nothing to GC against.
            // Returning `None` rather than an error lets the operator
            // stage the rollout: flip `enabled` first, point at a
            // bucket in a follow-up change. But the
            // `enabled=true && endpoint=""` state is operator-visible
            // misconfiguration — they explicitly opted in and forgot
            // the credentials — so warn loudly here. Pure-default
            // (`enabled=false`) stays silent; the caller logs the
            // disabled case at INFO.
            let endpoint = std::env::var("SANDBAWKS_S3_ENDPOINT").unwrap_or_default();
            if endpoint.is_empty() {
                tracing::warn!(
                    "SANDBAWKS_RETENTION_ENABLED=true but SANDBAWKS_S3_ENDPOINT is empty; \
                     retention sweeper will not run"
                );
                return Ok(None);
            }
            let bucket_name = std::env::var("SANDBAWKS_S3_BUCKET")
                .context("SANDBAWKS_S3_BUCKET required when retention enabled")?;
            let region =
                std::env::var("SANDBAWKS_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
            let access_key = std::env::var("SANDBAWKS_S3_ACCESS_KEY")
                .context("SANDBAWKS_S3_ACCESS_KEY required when retention enabled")?;
            let secret_key = std::env::var("SANDBAWKS_S3_SECRET_KEY")
                .context("SANDBAWKS_S3_SECRET_KEY required when retention enabled")?;
            let store =
                S3RetentionStore::new(endpoint, bucket_name, region, access_key, secret_key)
                    .context("building S3RetentionStore")?;
            Arc::new(store)
        }
        SnapshotProvider::Azure => {
            // Azure addresses the account by name (no endpoint URL for
            // public cloud); `SANDBAWKS_AZURE_ENDPOINT` is optional and
            // only set for Azurite / sovereign clouds.
            let account = std::env::var("SANDBAWKS_AZURE_ACCOUNT_NAME")
                .context("SANDBAWKS_AZURE_ACCOUNT_NAME required when retention enabled")?;
            let access_key = std::env::var("SANDBAWKS_AZURE_ACCOUNT_KEY")
                .context("SANDBAWKS_AZURE_ACCOUNT_KEY required when retention enabled")?;
            let container = std::env::var("SANDBAWKS_AZURE_CONTAINER")
                .context("SANDBAWKS_AZURE_CONTAINER required when retention enabled")?;
            let endpoint = std::env::var("SANDBAWKS_AZURE_ENDPOINT")
                .ok()
                .filter(|v| !v.is_empty());
            let store = AzureRetentionStore::new(account, access_key, container, endpoint)
                .context("building AzureRetentionStore")?;
            Arc::new(store)
        }
    };

    tracing::info!(
        controller_namespace = %std::env::var("SANDBAWKS_NAMESPACE").unwrap_or_else(|_| "default".into()),
        default_duration_secs = cfg.default_duration.as_secs(),
        scan_interval_secs = cfg.scan_interval.as_secs(),
        "snapshot retention enabled"
    );

    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cfg.scan_interval);
        // `MissedTickBehavior::Delay` keeps the loop spacing stable
        // when a sweep takes longer than the interval (e.g. a large
        // bucket); without this, missed ticks would burst-fire and
        // overlap with the in-flight sweep.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // For now the controller uses the chart default as the
            // effective retention for every object. Per-Sandbox and
            // per-namespace resolution (via
            // `sandbawks_controller::effective_retention`) will be
            // wired once per-namespace S3 prefixing lands — see PR
            // body. The math + resolution code itself is already
            // tested in `crates/sandbawks-controller/src/retention.rs`.
            match run_retention_pass(
                store.as_ref(),
                &cfg,
                cfg.default_duration,
                std::time::SystemTime::now(),
            )
            .await
            {
                Ok(report) => {
                    if report.skipped {
                        tracing::debug!("retention pass skipped (config disabled)");
                    } else if report.deleted > 0 || report.failed > 0 {
                        // Surface at INFO when the sweeper actually
                        // did work (or tried to and hit per-object
                        // errors) — these ticks are what an operator
                        // wants to see in the controller log.
                        tracing::info!(
                            deleted = report.deleted,
                            kept = report.kept,
                            failed = report.failed,
                            "retention pass complete"
                        );
                    } else {
                        // Steady-state ticks (`deleted=0, failed=0`)
                        // are pure noise once a bucket has stabilized
                        // — log them at DEBUG so the default-INFO
                        // operator view stays signal-rich.
                        tracing::debug!(
                            deleted = report.deleted,
                            kept = report.kept,
                            failed = report.failed,
                            "retention pass complete"
                        );
                    }
                }
                Err(e) => tracing::warn!(error = %e, "retention pass failed"),
            }
        }
    });
    Ok(Some(handle))
}

/// Adapter between kube-rs `Controller`'s expected signature and our
/// `reconcile()`. kube-rs calls this with the observed Sandbox + the
/// shared context; we extract the name and delegate.
///
/// The requeue cadence is phase-aware: while the Sandbox is `Creating`
/// **or** `Resuming` — the two phases where `reconcile()`'s active
/// `pod_ready` probe runs (it gates `Creating→Running` and the restore
/// Pod's `Resuming→Running`) — we requeue every 3 seconds so a
/// freshly-scheduled Pod is observed Ready promptly. The restore Pod has
/// no SnapshotJob to fire a watch event on readiness, so without the fast
/// `Resuming` cadence its `Running` transition would lag a full 30s.
/// Other phases drop back to a 30-second heartbeat. For `Running` that
/// heartbeat is load-bearing, not just a safety net: it drives
/// `reconcile()`'s liveness probe (`running_liveness_trigger`), which is
/// how a Sandbox whose backing Pod exited on its own (envd's
/// snapshot-and-exit) is observed and folded into `Failed`. The Pod's
/// terminal transition fires no *Sandbox* watch event, but the owned-Pod
/// watch wired up in `main` (issue #328) maps it back to the owner via the
/// controller `ownerReference`, so the probe normally fires within ~1s of
/// the Pod dying. The same watch likewise accelerates the `Creating`-phase
/// probe (`creating_liveness_trigger`, #337): a `Creating` Sandbox whose
/// Pod is GC'd or evicted requeues within ~1s rather than waiting for the
/// 3s `Creating` repoll. This heartbeat stays as the safety net: detection
/// still lands within ~30s even if the Pod event is missed.
async fn reconcile_adapter(
    sandbox: Arc<Sandbox>,
    ctx: Arc<ReconcileContext>,
) -> Result<Action, ControllerError> {
    let name = sandbox
        .metadata
        .name
        .clone()
        .ok_or(ControllerError::Invariant("missing metadata.name"))?;
    // Source the namespace from the watched object itself: a cluster-wide
    // watch delivers Sandboxes from every namespace, so the reconcile target
    // namespace is the object's own, not a fixed install namespace (#582).
    let namespace = sandbox
        .metadata
        .namespace
        .clone()
        .ok_or(ControllerError::Invariant("missing metadata.namespace"))?;
    let phase = sandbox
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(SandboxPhase::Creating);
    // Capture the PVC-retention config (cheap: two scalars) before `ctx` is
    // moved into `reconcile`, so the `Paused` requeue arm below can size its
    // interval against the retention deadline (#608).
    let pvc_retention = ctx.pvc_retention.clone();
    // Time the reconcile and record its outcome (#300). Hold the `Result` past
    // the metric call rather than `?`-ing it inline so a failed reconcile is
    // counted before its error propagates to the kube-rs error policy.
    let start = Instant::now();
    let result = reconcile(&namespace, &name, ctx).await;
    reconcile_metrics::record_reconcile("Sandbox", &result, start.elapsed());
    result?;
    let interval = match phase {
        SandboxPhase::Creating | SandboxPhase::Resuming => Duration::from_secs(3),
        // Issue #327: tighten the `Running` heartbeat so the idle-timeout
        // fires near `creationTimestamp + spec.timeoutSeconds` rather than up
        // to a full 30s late. `running_requeue_interval` caps at the 30s
        // heartbeat for far-off (or disabled) deadlines, so the liveness
        // probe's cadence is unchanged in the common case.
        SandboxPhase::Running => reconcile::running_requeue_interval(
            &sandbox,
            chrono::Utc::now(),
            Duration::from_secs(30),
        ),
        // Issue #608: tighten the `Paused` heartbeat so a paused filesystem
        // sandbox's PVC-retention GC fires near its deadline rather than up to
        // 30s late. `paused_requeue_interval` caps at the 30s heartbeat when
        // retention is disabled or the deadline is far off, so the common case
        // (snapshot-mode pause, or retention off) keeps the plain heartbeat.
        SandboxPhase::Paused => reconcile::paused_requeue_interval(
            &sandbox,
            chrono::Utc::now(),
            &pvc_retention,
            Duration::from_secs(30),
        ),
        _ => Duration::from_secs(30),
    };
    Ok(Action::requeue(interval))
}

fn error_policy(
    _sandbox: Arc<Sandbox>,
    _err: &ControllerError,
    _ctx: Arc<ReconcileContext>,
) -> Action {
    reconcile_metrics::record_requeue("Sandbox");
    Action::requeue(Duration::from_secs(5))
}

/// Adapter between kube-rs `Controller`'s expected signature and
/// `reconcile_pool()`. The context is the `KubeWarmPoolStore`; the
/// requeue cadence lives inside `reconcile_pool` (the warm-pool heartbeat
/// is uniform, unlike the Sandbox loop's phase-aware cadence).
async fn reconcile_warm_pool_adapter(
    pool: Arc<SandboxWarmPool>,
    store: Arc<KubeWarmPoolStore>,
) -> Result<Action, ControllerError> {
    // Time the warm-pool reconcile and record its outcome under the same
    // instruments as the Sandbox loop, distinguished by `kind` (#300).
    let start = Instant::now();
    let result = reconcile_pool(&pool, store.as_ref() as &dyn WarmPoolStore).await;
    reconcile_metrics::record_reconcile("SandboxWarmPool", &result, start.elapsed());
    result
}

fn warm_pool_error_policy(
    _pool: Arc<SandboxWarmPool>,
    _err: &ControllerError,
    _store: Arc<KubeWarmPoolStore>,
) -> Action {
    reconcile_metrics::record_requeue("SandboxWarmPool");
    Action::requeue(Duration::from_secs(5))
}
