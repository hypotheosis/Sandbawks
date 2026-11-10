//! sandbawks-api binary entrypoint.
//!
//! Tasks 2–12 grow this into the full router; this initial commit
//! only serves `GET /healthz` + `GET /readyz` so Plan 04 T13's Helm
//! chart probes pass against a real running binary.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use clap::Parser;
use kube::Client;
use tokio::signal;

/// HTTP gateway that fronts the sandbawks controller's `Sandbox` CRD
/// and the in-sandbox `envd` agents. Flags also accept their values
/// from the matching environment variables so that the Helm chart
/// (deploy/helm/sandbawks/templates/api-deployment.yaml) can drive
/// configuration via `env:` entries without changing the container
/// command line.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Socket the HTTP server binds. Matches the `containerPort` the
    /// Helm chart's api Deployment exposes. Defaults to the IPv6 wildcard
    /// `[::]`, which serves both IPv6 and (on Linux, via IPv4-mapped
    /// addresses) IPv4 clients — so kubelet liveness/readiness probes
    /// reach the server on dual-stack and IPv6-primary clusters where the
    /// Pod IP is IPv6 and an IPv4-only `0.0.0.0` bind would refuse them.
    #[arg(
        long,
        env = "SANDBAWKS_API_LISTEN",
        default_value = "[::]:8080",
        value_name = "ADDR"
    )]
    listen: SocketAddr,

    /// Kubernetes namespace the gateway places Sandboxes in when an
    /// inbound request doesn't pin one.
    #[arg(
        long,
        env = "SANDBAWKS_DEFAULT_NAMESPACE",
        default_value = "default",
        value_name = "NAMESPACE"
    )]
    default_namespace: String,

    /// Wildcard base domain the gateway is fronted by (e.g.
    /// `e2b.example.com`). When set, the per-sandbox proxy resolves the
    /// target sandbox from a `{port}-{sandbox_id}.{domain}` request
    /// `Host` — the scheme the official E2B SDKs use (issue #364) —
    /// falling back to the `E2b-Sandbox-Id` header. Leave unset for
    /// localhost / kind / port-forward, where there is no hostname to
    /// demux on and the header path is used exclusively. The Helm
    /// chart's api Deployment wires this from `api.sandboxDomain`.
    #[arg(long, env = "SANDBAWKS_SANDBOX_DOMAIN", value_name = "DOMAIN")]
    sandbox_domain: Option<String>,

    /// Namespace the gateway lists `TeamAPIKey`s in — the system namespace
    /// where keys live, out of tenant reach (#583). The Helm chart wires
    /// this from the api Pod's own namespace via the downward API. Unset
    /// (local dev) falls back to `--default-namespace`.
    #[arg(long, env = "SANDBAWKS_SYSTEM_NAMESPACE", value_name = "NAMESPACE")]
    system_namespace: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialise the shared OpenTelemetry bootstrap (logs, metrics, traces over
    // OTLP) and hold the guard for the whole of `main`. The guard MUST stay
    // bound to a named variable: the batch exporters flush asynchronously, so
    // dropping it early — or with `let _ = ...`, which drops immediately —
    // exports nothing. It flushes all three pipelines when `main` returns.
    //
    // With `OTEL_SDK_DISABLED` set (the local default) this installs only the
    // stderr fmt subscriber and builds no exporter, so `cargo run` against a
    // kind cluster needs no collector. See `OBSERVABILITY.md` / `TRACING.md`.
    //
    // Ordering note: the first OTLP exporter is constructed inside this call,
    // *before* `Client::try_default()` below resolves in-cluster credentials
    // (which can take non-trivial time). If you set `OTEL_EXPORTER_OTLP_TIMEOUT`
    // keep it generous enough to cover that kube startup, or the earliest
    // exports can spuriously time out during boot.
    // The re-arm handle is only used by checkpoint/restore-capable binaries
    // (envd); the api never pauses, so it ignores it.
    let (_telemetry, _rearm) =
        sandbawks_observability::init_telemetry("sandbawks-api", env!("CARGO_PKG_VERSION"))
            .context("initialising OpenTelemetry SDK")?;

    // Build the base `Config` explicitly (rather than `Client::try_default`,
    // which discards it) so the SAME validated config seeds both the
    // gateway's own client and the per-namespace impersonation cache (#442).
    // `Config::infer()` tries `$KUBECONFIG` / `~/.kube/config` first and falls
    // back to the in-cluster service-account token, so this works for a
    // developer running `cargo run` against kind and for the production Pod.
    let base_config = kube::Config::infer()
        .await
        .context("inferring kube config (kubeconfig or in-cluster service account)")?;
    let kube = Client::try_from(base_config.clone())
        .context("building kube client from inferred config")?;
    let default_namespace: std::sync::Arc<str> = args.default_namespace.into();
    // Normalize an empty / whitespace-only `SANDBAWKS_SANDBOX_DOMAIN` to
    // `None`: clap captures an exported `SANDBAWKS_SANDBOX_DOMAIN=""` as
    // `Some("")`, which would otherwise enable hostname routing with a
    // degenerate empty domain. Treating empty as unset matches the Helm
    // template's `{{- with .Values.api.sandboxDomain }}` truthiness check.
    let sandbox_domain: Option<std::sync::Arc<str>> = args
        .sandbox_domain
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .map(Into::into);
    if let Some(domain) = sandbox_domain.as_deref() {
        tracing::info!(domain, "hostname-based sandbox selection enabled");
    }
    // The system namespace defaults to the gateway's default namespace when
    // unset (local dev); in-cluster the Helm chart injects the api Pod's own
    // namespace via the downward API.
    let system_namespace: std::sync::Arc<str> = args
        .system_namespace
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .map(Into::into)
        .unwrap_or_else(|| default_namespace.clone());
    let state =
        sandbawks_api::AppState::from_kube(kube, base_config, default_namespace, system_namespace)
            .with_sandbox_domain(sandbox_domain);

    // `/healthz` and `/readyz` are intentionally outside the
    // `sandboxes` router so they sit outside `auth_middleware` and
    // stay anonymous-pollable for kubelet liveness/readiness probes.
    //
    // Router structure: a "timed" subrouter carries the global 30s
    // request timeout and is then merged with the server-streaming
    // envd-proxy surface, which must *not* inherit that timeout — a
    // background `commands.run(...).wait()` whose stream lives for
    // minutes would otherwise be amputated (see #191 review). Layers
    // attached to a subrouter *before* it is merged stay scoped to
    // those routes; the outer `request_id_layer` / `trace_layer`
    // applies to every request regardless of subset.
    //
    // NOTE: this axum-layer split is only *half* the streaming-timeout
    // fix. The matching half lives in `EnvdPool::streaming_channel`,
    // which dials envd without tonic's per-RPC `grpc-timeout` — without
    // that the stream would still be killed at 30s by the transport
    // even though the axum layer no longer applies. Both are required.
    let timed = Router::new()
        .merge(sandbawks_api::routes::sandboxes::router(state.clone()))
        .merge(sandbawks_api::routes::events::router(state.clone()))
        .merge(sandbawks_api::routes::files::router(state.clone()))
        .merge(sandbawks_api::routes::envd_proxy::unauthenticated_unary_router(state.clone()))
        // The outer HTTP budget for non-streaming requests. Sized for the
        // post-resume serve-readiness window (see `HTTP_REQUEST_TIMEOUT_SECS`
        // for the rationale, issue #353). The inner per-RPC tonic deadline in
        // `envd_client` derives itself from this same constant so it can never
        // undercut this layer again. Streaming routes (exec WS, file
        // up/download bodies) are exempt via the layer split above and set
        // their own longer bounds.
        .layer(sandbawks_api::middleware::timeout_layer(
            sandbawks_api::middleware::HTTP_REQUEST_TIMEOUT,
        ));

    let app = Router::new()
        .merge(timed)
        .merge(sandbawks_api::routes::exec::router(state.clone()))
        .merge(sandbawks_api::routes::envd_proxy::unauthenticated_streaming_router(state.clone()))
        .layer(sandbawks_api::middleware::request_id_layer())
        .layer(sandbawks_api::middleware::trace_layer())
        // OTel HTTP-server metrics (OBSERVABILITY.md §8.1). Outermost so it
        // times the whole request; `Router::layer` applies it per matched
        // route, so the `MatchedPath` extension is populated when it runs —
        // that is what bounds the `http.route` label (#300).
        .layer(axum::middleware::from_fn(
            sandbawks_api::http_metrics::track_metrics,
        ))
        // Kubelet liveness/readiness probes are merged *after* the
        // trace/metrics layers so they stay anonymous AND untraced/unmetered:
        // layers attached before a merge stay scoped to the routes they were
        // attached to (see the router-structure note above). Without this every
        // probe would render as an `http.route=/healthz` Endpoint in the APM,
        // drowning real endpoints and inflating trace export volume (#370 review).
        .merge(
            Router::new()
                .route("/healthz", get(|| async { "ok" }))
                .route("/readyz", get(|| async { "ok" })),
        );

    let listen = args.listen;
    tracing::info!(addr = %listen, "sandbawks-api listening");
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding {listen}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = signal::ctrl_c().await;
            tracing::info!("SIGINT received, shutting down");
        })
        .await
        .context("axum::serve")?;

    Ok(())
}
