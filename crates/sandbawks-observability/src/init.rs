//! The single OpenTelemetry initialization entry point.
//!
//! [`init_telemetry`] wires the whole SDK — tracer, meter, and logger
//! providers, their OTLP exporters, the resource, the sampler, the global
//! propagator, and the layered `tracing` subscriber that bridges the program
//! into OpenTelemetry — behind one call. A binary invokes it once near the top
//! of `main` and holds the returned [`TelemetryGuard`] for the process
//! lifetime.
//!
//! The keystone is `tracing_opentelemetry::layer()` in [`build_reload_layers`]:
//! application code only ever touches the `tracing` API (`info!`,
//! `#[instrument]`, span macros), and that one layer turns `tracing` spans
//! into OpenTelemetry spans. Without it you get logs but **zero traces** — the
//! single most common way an OTel setup "looks done" yet emits nothing (see
//! `TRACING.md`).
//!
//! # Hot-swapping providers (re-arm)
//!
//! A memory-restored envd (gVisor checkpoint/restore, issue #356) cannot re-run
//! `init_telemetry` — the global subscriber and OTel globals can only be set
//! once per process. So the OTel span layer and the log bridge are installed
//! behind a [`tracing_subscriber::reload`] layer, and `init_telemetry` hands
//! back a [`RearmHandle`](crate::RearmHandle) alongside the guard. The handle's
//! `quiesce`/`rearm` pair swaps both layers (and the global meter/tracer) to a
//! fresh provider stack in the live process. The fmt layer and the registry
//! `EnvFilter` stay static — only the OTel-exporting layers move.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _; // brings `.tracer()` into scope
use opentelemetry::{Key, KeyValue, Value};
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
// The trace and log `BatchConfigBuilder` types are distinct (one per signal),
// so they are imported under disambiguating aliases rather than a shared name.
use opentelemetry_sdk::logs::BatchConfigBuilder as LogBatchConfigBuilder;
use opentelemetry_sdk::trace::BatchConfigBuilder as TraceBatchConfigBuilder;
use opentelemetry_sdk::Resource;
use tracing::Subscriber;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{reload, EnvFilter, Layer, Registry};

use crate::export_runtime::ExportRuntime;
use crate::guard::{Providers, SharedProviders, TelemetryGuard};
use crate::log_bridge::TraceContextLogBridge;
use crate::rearm::{ProviderFactory, RearmHandle};

/// The static base of the subscriber stack the reload layer sits on:
/// the bare `Registry` gated by the global `EnvFilter`.
///
/// Spelling it out (rather than letting it stay an anonymous `Layered<...>`)
/// is what lets [`ReloadHandle`] name a single, stable subscriber type for the
/// reloadable layers — the OTel span layer and the log bridge are added on top
/// of *this* type, so swapping them never changes the type the handle is bound
/// to. The fmt layer is added *after* the reload layer, so it does not appear
/// here.
type BaseSubscriber = Layered<EnvFilter, Registry>;

/// The reloadable layer payload: the OTel span layer and the log bridge, type-
/// erased into trait objects so one `Vec` can hold both heterogeneous layers
/// under a single concrete type.
///
/// An empty `Vec` is the no-op (quiesced) state; a two-element `Vec` is the
/// armed state. Boxing keeps the payload one nameable type across reloads —
/// crucial, since [`reload::Handle::reload`] can only swap a value for another
/// of the *same* type. Per-layer filtering is applied *outside* the reload
/// (see [`build_reloadable_subscriber`]), never inside the payload: a
/// per-layer `Filtered` registers its `FilterId` during the one-shot
/// `Layer::on_layer` pass, which a post-init `reload` never re-runs, so a
/// freshly-built `Filtered` swapped in here would filter against an
/// unregistered id.
type ReloadLayers = Vec<Box<dyn Layer<BaseSubscriber> + Send + Sync + 'static>>;

/// Clonable handle that swaps the reloadable OTel layers in the live
/// subscriber. Held by [`RearmHandle`](crate::RearmHandle); `reload(Vec::new())`
/// quiesces, `reload(build_reload_layers(...))` re-arms.
pub(crate) type ReloadHandle = reload::Handle<ReloadLayers, BaseSubscriber>;

/// `service.name` resource-attribute key (OTel semantic convention).
const SERVICE_NAME: &str = "service.name";
/// `service.version` resource-attribute key (OTel semantic convention).
const SERVICE_VERSION: &str = "service.version";
/// `service.instance.id` resource-attribute key (OTel semantic convention) —
/// the per-replica discriminator that keeps multiple Pods sharing one
/// `service.name` apart in dashboards and alerts.
const SERVICE_INSTANCE_ID: &str = "service.instance.id";
/// `vcs.revision` resource-attribute key (OTel VCS semantic convention) —
/// the git commit the binary was built from, stamped onto every trace, metric,
/// and log so an operator can tell which build produced a given signal (e.g.
/// "was this error already fixed on a newer commit?"). See [`vcs_revision`].
const VCS_REVISION: &str = "vcs.revision";
/// Git SHA captured at build time by `build.rs` (CI/Docker `--build-arg
/// GIT_SHA`, else `git rev-parse HEAD`, else the sentinel below).
const BUILD_GIT_SHA: Option<&str> = option_env!("SANDBAWKS_GIT_SHA");
/// Sentinel `vcs.revision` value when no SHA could be resolved at build time.
const UNKNOWN_REVISION: &str = "unknown";
/// K8s downward-API environment variable conventionally carrying the Pod name,
/// used as the stable `service.instance.id` in-cluster.
const POD_NAME_ENV: &str = "POD_NAME";
/// Prefix the SDK uses for the placeholder `service.name` it injects when no
/// real value is set (`unknown_service` or `unknown_service:<exe>`).
const UNKNOWN_SERVICE_PREFIX: &str = "unknown_service";
/// Default OTLP endpoint the SDK uses when `OTEL_EXPORTER_OTLP_ENDPOINT` is
/// unset — surfaced verbatim in the startup line so an operator can see it.
const DEFAULT_OTLP_ENDPOINT: &str = "<default http://localhost:4318>";
/// Hard cap on how long a single batch export may block, applied to the trace
/// and log batch processors and the metric reader.
///
/// The SDK default is 30s. After a gVisor checkpoint/restore the OTLP collector
/// is frequently unreachable — or accepts the TCP connection but never replies —
/// so a 30s export bound means `provider.shutdown()`'s final flush blocks far
/// past the resume path's quiesce deadline. The shutdown thread is then
/// abandoned while the underlying Tokio export task survives `runsc checkpoint`
/// and starves the restored runtime (issue #533, Bug 1). Capping the bound makes
/// every export self-cancel quickly, so `RearmHandle::quiesce`'s bounded
/// shutdown completes in time and the background task actually exits *before* the
/// checkpoint. See [`export_flush_timeout`] for the operator override.
const DEFAULT_EXPORT_FLUSH_TIMEOUT: Duration = Duration::from_secs(3);
/// Non-zero floor for the export flush bound (issue #533 review).
///
/// `OTEL_EXPORTER_OTLP_TIMEOUT=0` parses as a *valid* zero `Duration`, which as a
/// `max_export_timeout` would make every export time out instantly and ship no
/// telemetry at all. Clamp up to this floor so a degenerate (or fat-fingered)
/// operator value still leaves a usable export window.
const MIN_EXPORT_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

/// Initialise the OpenTelemetry SDK and the `tracing` → OTel bridge.
///
/// `service_name` becomes the compile-time-fallback `service.name` resource
/// attribute (an operator's `OTEL_SERVICE_NAME` overrides it); `version`
/// likewise seeds `service.version`. Call this once, inside `#[tokio::main]`
/// (the batch exporters need the Tokio runtime to exist), and bind the result
/// to a **named** variable held for the whole of `main`.
///
/// # Returns
///
/// A `(TelemetryGuard, RearmHandle)` pair:
/// - The [`TelemetryGuard`] whose `Drop` flushes and shuts down all three
///   signal pipelines. Dropping it early — or with `let _ = ...`, which drops
///   *immediately* — exports nothing.
/// - The clonable, `'static` [`RearmHandle`](crate::RearmHandle) that swaps the
///   whole provider stack in the live process across a pause/resume
///   (`quiesce` then `rearm`). When `OTEL_SDK_DISABLED` is set, its methods are
///   no-ops.
///
/// # Errors
///
/// Returns an error if `OTEL_EXPORTER_OTLP_PROTOCOL` names an unsupported
/// protocol, if any OTLP exporter fails to build, or if the global `tracing`
/// subscriber cannot be installed (e.g. one is already set).
///
/// # Examples
///
/// ```no_run
/// # fn main() -> anyhow::Result<()> {
/// let (_telemetry, _rearm) =
///     sandbawks_observability::init_telemetry("sandbawks-api", env!("CARGO_PKG_VERSION"))?;
/// // ... run the app ...
/// # Ok(())
/// # }
/// ```
pub fn init_telemetry(
    service_name: &'static str,
    version: &'static str,
) -> Result<(TelemetryGuard, RearmHandle)> {
    // 1. Short-circuit when the operator disabled the SDK. Install only the
    //    fmt + EnvFilter subscriber so local runs and `cargo test` still see
    //    stderr logs, and return a disabled guard plus a no-op re-arm handle —
    //    no exporter is ever built, so no collector connection is attempted.
    //    This is the local default.
    if otel_sdk_disabled() {
        install_fmt_only_subscriber()?;
        // Echo `service_version` and an explicit `endpoint=disabled` so this
        // line is shape-comparable with the active-path startup line below —
        // making the local-vs-prod distinction obvious at a glance in stderr.
        tracing::info!(
            service_name,
            service_version = version,
            endpoint = "disabled",
            "OTEL_SDK_DISABLED set; OpenTelemetry export disabled, fmt-only logging active"
        );
        return Ok((TelemetryGuard::disabled(), RearmHandle::disabled()));
    }

    // 2. Resolve the wire protocol (http/protobuf default, or grpc).
    let protocol = resolve_protocol()?;
    // 3. Build the resource (env wins, `unknown_service` placeholder stripped).
    let resource = build_resource(service_name, version);
    // 4. Build the sampler, deferring any parse warnings until the subscriber
    //    that would surface them exists (step 8).
    let mut warnings = Vec::new();
    let sampler = build_sampler(&mut warnings);
    // Per-export timeout (`OTEL_EXPORTER_OTLP_TIMEOUT`, milliseconds) applied to
    // every exporter; `None` leaves the SDK default (~10s) in place.
    let timeout = exporter_timeout(&mut warnings);

    // 5. Spin up the dedicated export runtime (issue #539) and build the three
    //    providers *inside its context*, so every batch-processor/reader
    //    background task — and the blocking DNS the OTLP client runs — spawns on
    //    that isolated runtime, never on the serving runtime. A black-holed
    //    collector can then at most saturate the export runtime's own pools; the
    //    gRPC `:49983` and HTTP `:49984` listeners stay responsive.
    let export_rt = ExportRuntime::new()?;
    let Providers {
        tracer: tracer_provider,
        meter: meter_provider,
        logger: logger_provider,
    } = build_providers_on(export_rt.handle(), protocol, &resource, sampler, timeout)?;

    // 6. Register globals so `opentelemetry::global::meter(...)` and any
    //    propagation helper reaching for the global propagator see this stack.
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // 7. Install the layered subscriber, with the OTel span layer and log
    //    bridge behind a reload layer so `rearm` can hot-swap them later. The
    //    returned handle is the only post-init seam into those two layers.
    let initial_layers = build_reload_layers(&tracer_provider, &logger_provider, service_name);
    let (subscriber, reload_handle) = build_reloadable_subscriber(initial_layers);
    subscriber
        .try_init()
        .context("installing the global tracing subscriber")?;

    // Replay any config-resolution warnings now that a subscriber exists.
    // Emit structured fields (not a `format!`'d string) so each diagnostic
    // survives JSON log routing and stays greppable by `env_var`.
    for warning in &warnings {
        tracing::warn!(
            env_var = warning.env_var,
            value = %warning.value,
            "{}",
            warning.message
        );
    }

    // 8. Emit a one-shot startup line so a missing-attribute problem is
    //    diagnosable from a single log line.
    emit_startup_line(version, protocol, &resource);

    // 9. Share the live providers between the guard (flush on drop) and the
    //    re-arm handle (tear down on quiesce, replace on rearm), and hand both
    //    back. The factory rebuilds the stack from the *current* environment on
    //    each rearm, so a resumed process picks up any collector/endpoint
    //    changes its restored env carries.
    let shared: SharedProviders = Arc::new(Mutex::new(Some(Providers::new(
        tracer_provider,
        meter_provider,
        logger_provider,
    ))));
    // The factory rebuilds on the same isolated export runtime, so a resumed
    // process re-spawns its fresh export tasks there too. The guard holds a
    // clone so the runtime outlives its flush-on-drop (the final export needs a
    // live runtime); the factory holds the other, keeping it alive as long as
    // any re-arm handle does.
    let factory = make_provider_factory(service_name, version, export_rt.clone());
    let guard = TelemetryGuard::new(shared.clone(), Some(export_rt));
    let rearm = RearmHandle::new(service_name, reload_handle, factory, shared);
    Ok((guard, rearm))
}

/// Build the `tracing` → OTel reload payload: the OTel span layer (turning
/// `tracing` spans into OTel spans) and the trace-correlated log bridge.
///
/// Both are type-erased with [`Layer::boxed`] so the heterogeneous pair fits a
/// single [`ReloadLayers`] `Vec`. Called once at init for the initial stack and
/// again on every `rearm` to wire freshly-built providers in — the tracer/
/// logger are read from the *concrete* providers here, never from the OTel
/// globals (which would be a no-op if read before the global is set).
pub(crate) fn build_reload_layers(
    tracer_provider: &SdkTracerProvider,
    logger_provider: &SdkLoggerProvider,
    service_name: &'static str,
) -> ReloadLayers {
    // Feed the layer the tracer from the *concrete* provider we just built —
    // `global::tracer(...)` would be a no-op if read before the global is set.
    let tracer = tracer_provider.tracer(service_name);
    let span_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // The trace-correlated `tracing` → OTel log-record appender. It wraps the
    // upstream `OpenTelemetryTracingBridge`, attaching the active span's OTel
    // context for the duration of each event so the emitted record carries the
    // span's `trace_id` / `span_id` — making the APM's "logs for this trace"
    // view work (OBSERVABILITY.md §7.2; see `log_bridge` for the mechanism).
    // Without it these records ship with no trace IDs even though the same
    // events look correlated in stderr via the fmt layer.
    let log_bridge = TraceContextLogBridge::new(logger_provider);

    vec![span_layer.boxed(), log_bridge.boxed()]
}

/// Build the layered subscriber and the [`ReloadHandle`] for its OTel layers,
/// *without* installing it — the caller chooses `try_init` (global, for a
/// binary) or `set_default` (scoped, for a test).
///
/// Layer stack, bottom to top:
/// 1. the registry, gated by the global `EnvFilter` (`RUST_LOG`, else `info`) —
///    this is the static [`BaseSubscriber`];
/// 2. the **reload** layer holding the OTel span layer + log bridge, wrapped in
///    the OTel transport filter (`h2`/`hyper`/`tonic`/… quieted to `warn`) so
///    the OTLP signals stay focused on application work. The filter lives
///    *outside* the reload so its `FilterId` is registered once and survives
///    every swap (see [`ReloadLayers`]);
/// 3. the static `fmt` layer writing to stderr.
///
/// `pub(crate)` so the re-arm tests can install the *same* stack thread-scoped
/// (`set_default`) instead of globally, side-stepping the one-global-subscriber
/// constraint while exercising the real reload handle.
pub(crate) fn build_reloadable_subscriber(
    initial_layers: ReloadLayers,
) -> (impl Subscriber + Send + Sync + 'static, ReloadHandle) {
    let (reload_layer, handle) = reload::Layer::new(initial_layers);
    let subscriber = tracing_subscriber::registry()
        .with(global_env_filter()) // RUST_LOG gates everything below
        .with(reload_layer.with_filter(build_otel_layer_filter()))
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr));
    (subscriber, handle)
}

/// Build the closure `rearm` calls to reconstruct the three providers from the
/// *current* environment.
///
/// Capturing only the `'static` service name and version (not the resolved
/// providers) keeps the [`ProviderFactory`] `'static` and lets a resumed
/// process re-resolve protocol/endpoint/resource/sampler/timeout afresh — so a
/// checkpoint/restore that lands in a differently-configured environment picks
/// up the new settings. Config-resolution warnings are surfaced inline since a
/// subscriber already exists by re-arm time.
///
/// The captured [`ExportRuntime`] handle is what keeps re-armed providers
/// isolated: each rebuild goes through [`build_providers_on`], so the fresh
/// export tasks spawn on the same dedicated runtime rather than the serving one
/// (issue #539). Capturing the `Arc` also keeps that runtime alive for as long
/// as any re-arm handle lives.
pub(crate) fn make_provider_factory(
    service_name: &'static str,
    version: &'static str,
    export_rt: Arc<ExportRuntime>,
) -> ProviderFactory {
    Arc::new(move || {
        let protocol = resolve_protocol()?;
        let resource = build_resource(service_name, version);
        let mut warnings = Vec::new();
        let sampler = build_sampler(&mut warnings);
        let timeout = exporter_timeout(&mut warnings);
        let providers =
            build_providers_on(export_rt.handle(), protocol, &resource, sampler, timeout)?;
        for warning in &warnings {
            tracing::warn!(
                env_var = warning.env_var,
                value = %warning.value,
                "{}",
                warning.message
            );
        }
        Ok(providers)
    })
}

/// Build the three signal providers with their batch processors/reader bound to
/// the dedicated export runtime (issue #539).
///
/// The async-runtime processors spawn their background export tasks via a bare
/// [`tokio::spawn`] onto *whatever runtime is current when they are built*
/// ([`opentelemetry_sdk::runtime::Tokio`]). Holding `handle`'s
/// [`enter`](tokio::runtime::Handle::enter) guard across the three builds makes
/// that "current runtime" the export runtime, so the tasks — and every blocking
/// `getaddrinfo` the OTLP HTTP client runs on the now-current `spawn_blocking`
/// pool — land there, never on the caller's serving runtime. This is the seam
/// that decouples telemetry export from request serving.
fn build_providers_on(
    handle: &tokio::runtime::Handle,
    protocol: OtlpProtocol,
    resource: &Resource,
    sampler: Sampler,
    timeout: Option<Duration>,
) -> Result<Providers> {
    // The guard sets the thread-local current-runtime for the duration of the
    // three builds; it is dropped (restoring the caller's context) on return.
    let _enter = handle.enter();
    Ok(Providers::new(
        build_tracer_provider(protocol, resource, sampler, timeout)?,
        build_meter_provider(protocol, resource, timeout)?,
        build_logger_provider(protocol, resource, timeout)?,
    ))
}

/// Whether an environment variable is set to a truthy value (`true`/`1`,
/// case-insensitive), per the OTel boolean environment-variable convention.
fn env_truthy(var: &str) -> bool {
    std::env::var(var)
        .map(|raw| {
            let value = raw.trim().to_ascii_lowercase();
            value == "true" || value == "1"
        })
        .unwrap_or(false)
}

/// Whether `OTEL_SDK_DISABLED` is set, short-circuiting to fmt-only logging.
fn otel_sdk_disabled() -> bool {
    env_truthy("OTEL_SDK_DISABLED")
}

/// The OTLP wire protocol, selected at runtime rather than compile time.
///
/// Supporting both arms is what makes `OTEL_EXPORTER_OTLP_PROTOCOL` a runtime
/// switch instead of a recompile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OtlpProtocol {
    /// OTLP over HTTP with protobuf payloads (the spec default).
    HttpProtobuf,
    /// OTLP over gRPC (tonic transport).
    Grpc,
}

impl OtlpProtocol {
    /// Spec name, for the startup line.
    fn as_str(self) -> &'static str {
        match self {
            Self::HttpProtobuf => "http/protobuf",
            Self::Grpc => "grpc",
        }
    }
}

/// Resolve the OTLP protocol from `OTEL_EXPORTER_OTLP_PROTOCOL`.
///
/// Defaults to `http/protobuf`. `grpc` selects the tonic transport. Anything
/// else is a hard error — silently falling back would hide a misconfiguration.
fn resolve_protocol() -> Result<OtlpProtocol> {
    match std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL") {
        Ok(raw) => match raw.trim() {
            "http/protobuf" => Ok(OtlpProtocol::HttpProtobuf),
            "grpc" => Ok(OtlpProtocol::Grpc),
            other => anyhow::bail!(
                "unsupported OTEL_EXPORTER_OTLP_PROTOCOL={other:?} \
                 (expected `http/protobuf` or `grpc`)"
            ),
        },
        Err(_) => Ok(OtlpProtocol::HttpProtobuf),
    }
}

/// Build the resource with env-wins precedence and the `unknown_service`
/// placeholder stripped (see `OBSERVABILITY.md` §10).
///
/// `Resource::builder()` runs the SDK, env, and telemetry detectors — so an
/// operator's `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` are already
/// present. Two subtleties this function gets right:
///
/// 1. **Env wins over the compile-time fallback.** The detected attributes are
///    merged *on top of* the `service_name` / `version` fallback, so an
///    env-set name overrides the binary default rather than the other way
///    round.
/// 2. **The placeholder never wins.** When no real `service.name` is set the
///    SDK injects `unknown_service[:<exe>]`. That single attribute is filtered
///    out of the detected set before merging, so the compile-time fallback
///    survives while a genuine env-set name still takes precedence.
fn build_resource(service_name: &str, version: &str) -> Resource {
    let detected = Resource::builder().build();

    // Detected attributes, minus the `unknown_service` placeholder so it can't
    // clobber the compile-time fallback below.
    let detected_attributes: Vec<KeyValue> = detected
        .iter()
        .filter(|(key, value)| {
            !(key.as_str() == SERVICE_NAME && is_unknown_service_placeholder(value))
        })
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect();

    // `with_attributes` merges argument-wins, so the detected (env) attributes
    // take precedence over the compile-time fallbacks seeded first. Seeding
    // `service.instance.id` here (before the merge) means an operator's
    // `OTEL_RESOURCE_ATTRIBUTES=service.instance.id=...` still wins.
    Resource::builder_empty()
        .with_service_name(service_name.to_string())
        .with_attribute(KeyValue::new(SERVICE_VERSION, version.to_string()))
        .with_attribute(KeyValue::new(SERVICE_INSTANCE_ID, service_instance_id()))
        .with_attribute(KeyValue::new(VCS_REVISION, vcs_revision()))
        .with_attributes(detected_attributes)
        .build()
}

/// The git SHA stamped onto telemetry as `vcs.revision`.
///
/// Resolves the build-time [`BUILD_GIT_SHA`] (set by `build.rs`), falling back
/// to [`UNKNOWN_REVISION`] when it is absent or blank. Seeded as a compile-time
/// fallback before the detected-attribute merge, so an operator's
/// `OTEL_RESOURCE_ATTRIBUTES=vcs.revision=...` still wins (mirrors
/// `service.instance.id` — see [`build_resource`]).
fn vcs_revision() -> &'static str {
    BUILD_GIT_SHA
        .map(str::trim)
        .filter(|sha| !sha.is_empty())
        .unwrap_or(UNKNOWN_REVISION)
}

/// Resolve a per-replica `service.instance.id`.
///
/// Prefers the K8s downward-API [`POD_NAME_ENV`] (stable across a Pod's life
/// and matching what operators see in `kubectl`); falls back to a per-process
/// random UUID so replicas stay distinguishable even off Kubernetes. An
/// operator-supplied `OTEL_RESOURCE_ATTRIBUTES` value still overrides this —
/// see [`build_resource`].
fn service_instance_id() -> String {
    std::env::var(POD_NAME_ENV)
        .ok()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

/// Whether a `service.name` value is the SDK's `unknown_service[:<exe>]`
/// placeholder rather than a real, operator-supplied name.
fn is_unknown_service_placeholder(value: &Value) -> bool {
    matches!(value, Value::String(name) if name.as_str().starts_with(UNKNOWN_SERVICE_PREFIX))
}

/// Trace-sampling strategy parsed from `OTEL_TRACES_SAMPLER`.
///
/// Separated from [`Sampler`] (which holds a `Box<dyn ShouldSample>` and is
/// therefore not comparable) so the env-name parsing is a pure, unit-testable
/// mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SamplerStrategy {
    AlwaysOn,
    AlwaysOff,
    TraceIdRatio,
    ParentBasedAlwaysOn,
    ParentBasedAlwaysOff,
    ParentBasedTraceIdRatio,
}

impl SamplerStrategy {
    /// The OTel spec default when `OTEL_TRACES_SAMPLER` is unset or unknown.
    const DEFAULT: Self = Self::ParentBasedAlwaysOn;

    /// Parse a spec sampler name; `None` for an unrecognized name.
    fn parse(name: &str) -> Option<Self> {
        match name {
            "always_on" => Some(Self::AlwaysOn),
            "always_off" => Some(Self::AlwaysOff),
            "traceidratio" => Some(Self::TraceIdRatio),
            "parentbased_always_on" => Some(Self::ParentBasedAlwaysOn),
            "parentbased_always_off" => Some(Self::ParentBasedAlwaysOff),
            "parentbased_traceidratio" => Some(Self::ParentBasedTraceIdRatio),
            _ => None,
        }
    }

    /// Build the concrete [`Sampler`]; `ratio` is used only by the
    /// `*traceidratio` strategies.
    fn into_sampler(self, ratio: f64) -> Sampler {
        match self {
            Self::AlwaysOn => Sampler::AlwaysOn,
            Self::AlwaysOff => Sampler::AlwaysOff,
            Self::TraceIdRatio => Sampler::TraceIdRatioBased(ratio),
            // `Box::new` because `ParentBased` owns its delegate as a trait
            // object — the parent strategy wraps a root-span fallback.
            Self::ParentBasedAlwaysOn => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
            Self::ParentBasedAlwaysOff => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
            Self::ParentBasedTraceIdRatio => {
                Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
            }
        }
    }
}

/// A deferred config-resolution diagnostic, collected during env parsing and
/// replayed once the `tracing` subscriber exists (the subscriber is what would
/// surface it). Carrying the offending `env_var` and `value` as discrete fields
/// — rather than baking them into a `format!`'d string — keeps the warning
/// structured end-to-end, so it survives JSON log routing and stays greppable.
struct ConfigWarning {
    /// The environment variable whose value could not be honored as written.
    env_var: &'static str,
    /// The raw (or coerced) value that triggered the warning.
    value: String,
    /// Human-readable description of what was done about it.
    message: &'static str,
}

/// Build the sampler from `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG`.
///
/// An unrecognized strategy or out-of-range ratio pushes a [`ConfigWarning`]
/// onto `warnings` (replayed once the subscriber exists) and falls back to the
/// spec default rather than failing startup.
fn build_sampler(warnings: &mut Vec<ConfigWarning>) -> Sampler {
    let ratio = sampler_ratio(warnings);
    let strategy = match std::env::var("OTEL_TRACES_SAMPLER") {
        Ok(raw) => {
            let name = raw.trim();
            SamplerStrategy::parse(name).unwrap_or_else(|| {
                warnings.push(ConfigWarning {
                    env_var: "OTEL_TRACES_SAMPLER",
                    value: name.to_string(),
                    message: "unknown sampler; falling back to parentbased_always_on",
                });
                SamplerStrategy::DEFAULT
            })
        }
        Err(_) => SamplerStrategy::DEFAULT,
    };
    strategy.into_sampler(ratio)
}

/// Parse the sampler ratio from `OTEL_TRACES_SAMPLER_ARG`, defaulting to `1.0`
/// and clamping to `0.0..=1.0`.
fn sampler_ratio(warnings: &mut Vec<ConfigWarning>) -> f64 {
    match std::env::var("OTEL_TRACES_SAMPLER_ARG") {
        Ok(raw) => match raw.trim().parse::<f64>() {
            Ok(ratio) if (0.0..=1.0).contains(&ratio) => ratio,
            Ok(ratio) => {
                warnings.push(ConfigWarning {
                    env_var: "OTEL_TRACES_SAMPLER_ARG",
                    value: ratio.to_string(),
                    message: "value out of range [0.0, 1.0]; clamping",
                });
                ratio.clamp(0.0, 1.0)
            }
            Err(_) => {
                warnings.push(ConfigWarning {
                    env_var: "OTEL_TRACES_SAMPLER_ARG",
                    value: raw,
                    message: "unparseable value; falling back to 1.0",
                });
                1.0
            }
        },
        Err(_) => 1.0,
    }
}

/// Parse the per-export timeout from `OTEL_EXPORTER_OTLP_TIMEOUT`.
///
/// The OTel spec defines the value in **milliseconds**. `None` (unset, empty,
/// or unparseable) leaves the SDK's built-in default (~10s) in place. An
/// unparseable value pushes a [`ConfigWarning`] rather than failing startup.
fn exporter_timeout(warnings: &mut Vec<ConfigWarning>) -> Option<Duration> {
    let raw = std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT").ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.parse::<u64>() {
        Ok(millis) => Some(Duration::from_millis(millis)),
        Err(_) => {
            warnings.push(ConfigWarning {
                env_var: "OTEL_EXPORTER_OTLP_TIMEOUT",
                value: raw,
                message: "not a millisecond integer; using the SDK default timeout",
            });
            None
        }
    }
}

/// The effective per-export *flush* bound for the batch processors and the
/// metric reader (issue #533, Bug 1).
///
/// Returns [`DEFAULT_EXPORT_FLUSH_TIMEOUT`], or the operator's
/// `OTEL_EXPORTER_OTLP_TIMEOUT` clamped to
/// `[MIN_EXPORT_FLUSH_TIMEOUT, DEFAULT_EXPORT_FLUSH_TIMEOUT]`. An operator may
/// tighten the bound but not extend it past the default (a longer bound would
/// reintroduce the resume-blocking hang this cap exists to prevent) nor collapse
/// it to a useless near-zero window (issue #533 review). This governs how long a
/// single export (including `shutdown`'s final flush) may block on an unreachable
/// collector, independent of the OTLP client's own per-request timeout applied by
/// [`apply_timeout`].
fn export_flush_timeout(timeout: Option<Duration>) -> Duration {
    match timeout {
        // `clamp` is valid here because MIN <= DEFAULT by construction.
        Some(operator) => operator.clamp(MIN_EXPORT_FLUSH_TIMEOUT, DEFAULT_EXPORT_FLUSH_TIMEOUT),
        None => DEFAULT_EXPORT_FLUSH_TIMEOUT,
    }
}

/// Apply an optional per-export `timeout` to an OTLP exporter builder.
///
/// The HTTP and gRPC builders are different concrete types, but both implement
/// [`WithExportConfig`], so one generic helper covers both protocol arms
/// without duplicating the `match timeout` in each `build_*_exporter`.
fn apply_timeout<B: WithExportConfig>(builder: B, timeout: Option<Duration>) -> B {
    match timeout {
        Some(timeout) => builder.with_timeout(timeout),
        None => builder,
    }
}

/// Build the OTLP span exporter for the chosen protocol.
fn build_span_exporter(protocol: OtlpProtocol, timeout: Option<Duration>) -> Result<SpanExporter> {
    let builder = SpanExporter::builder();
    match protocol {
        OtlpProtocol::HttpProtobuf => apply_timeout(builder.with_http(), timeout).build(),
        OtlpProtocol::Grpc => apply_timeout(builder.with_tonic(), timeout).build(),
    }
    .context("building the OTLP span exporter")
}

/// Build the OTLP metric exporter for the chosen protocol.
fn build_metric_exporter(
    protocol: OtlpProtocol,
    timeout: Option<Duration>,
) -> Result<MetricExporter> {
    let builder = MetricExporter::builder();
    match protocol {
        OtlpProtocol::HttpProtobuf => apply_timeout(builder.with_http(), timeout).build(),
        OtlpProtocol::Grpc => apply_timeout(builder.with_tonic(), timeout).build(),
    }
    .context("building the OTLP metric exporter")
}

/// Build the OTLP log exporter for the chosen protocol.
fn build_log_exporter(protocol: OtlpProtocol, timeout: Option<Duration>) -> Result<LogExporter> {
    let builder = LogExporter::builder();
    match protocol {
        OtlpProtocol::HttpProtobuf => apply_timeout(builder.with_http(), timeout).build(),
        OtlpProtocol::Grpc => apply_timeout(builder.with_tonic(), timeout).build(),
    }
    .context("building the OTLP log exporter")
}

/// Build the tracer provider: batch processor (Tokio-driven) + sampler +
/// resource.
fn build_tracer_provider(
    protocol: OtlpProtocol,
    resource: &Resource,
    sampler: Sampler,
    timeout: Option<Duration>,
) -> Result<SdkTracerProvider> {
    let exporter = build_span_exporter(protocol, timeout)?;
    // The async-runtime batch processor drives the async OTLP client on the
    // Tokio runtime; the default (dedicated-thread) processor's
    // `futures_executor::block_on` cannot — see the Cargo.toml feature note.
    //
    // Cap `max_export_timeout` (issue #533): the SDK default is 30s, so without
    // this a `shutdown` final-flush to an unreachable collector would outlast the
    // resume quiesce deadline and leave a live Tokio export task to be captured
    // by `runsc checkpoint`.
    let batch_config = TraceBatchConfigBuilder::default()
        .with_max_export_timeout(export_flush_timeout(timeout))
        .build();
    let processor = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(batch_config)
        .build();
    let mut builder = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_sampler(sampler)
        .with_resource(resource.clone());

    // `OTEL_DEBUG_STDOUT`: attach a *second*, synchronous stdout span processor
    // (OBSERVABILITY.md §5.2) so spans are pretty-printed the instant they end.
    // The OTLP batch processor above is untouched, so wire throughput is
    // unaffected — this is purely a local "is the collector dropping it?" aid.
    if env_truthy("OTEL_DEBUG_STDOUT") {
        builder = builder.with_simple_exporter(opentelemetry_stdout::SpanExporter::default());
    }

    Ok(builder.build())
}

/// Build the meter provider: periodic reader (Tokio-driven) + resource.
fn build_meter_provider(
    protocol: OtlpProtocol,
    resource: &Resource,
    timeout: Option<Duration>,
) -> Result<SdkMeterProvider> {
    let exporter = build_metric_exporter(protocol, timeout)?;
    // Cap the reader's export timeout for the same reason as the trace/log batch
    // processors (issue #533): a hung metric export must not outlast the resume
    // quiesce deadline.
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_timeout(export_flush_timeout(timeout))
        .build();
    Ok(SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource.clone())
        .build())
}

/// Build the logger provider: batch processor (Tokio-driven) + resource.
fn build_logger_provider(
    protocol: OtlpProtocol,
    resource: &Resource,
    timeout: Option<Duration>,
) -> Result<SdkLoggerProvider> {
    let exporter = build_log_exporter(protocol, timeout)?;
    // Cap `max_export_timeout` for the same reason as the trace batch processor
    // (issue #533): keep `shutdown`'s final flush from outlasting the resume
    // quiesce deadline and stranding a live Tokio export task across checkpoint.
    let batch_config = LogBatchConfigBuilder::default()
        .with_max_export_timeout(export_flush_timeout(timeout))
        .build();
    let processor = BatchLogProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(batch_config)
        .build();
    Ok(SdkLoggerProvider::builder()
        .with_log_processor(processor)
        .with_resource(resource.clone())
        .build())
}

/// The global registry-level filter: `RUST_LOG` if set, else `info`.
fn global_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// The OTel export layer's *own* filter: the global base, then a few noisy
/// transport targets quieted to `warn`.
///
/// This narrows only what reaches the APM as spans — the OTLP exporter's own
/// gRPC/HTTP plumbing — so trace trees stay focused on application work. The
/// registry-level [`global_env_filter`] that gates the fmt layer is untouched,
/// so `RUST_LOG=debug` still shows everything in the terminal.
fn build_otel_layer_filter() -> EnvFilter {
    let mut filter = global_env_filter();
    for directive in [
        "h2=warn",
        "hyper=warn",
        "hyper_util=warn",
        "tonic=warn",
        "tower=warn",
        "reqwest=warn",
        "opentelemetry=warn",
    ] {
        if let Ok(parsed) = directive.parse() {
            filter = filter.add_directive(parsed);
        }
    }
    filter
}

/// Install only the `fmt` + `EnvFilter` subscriber (the `OTEL_SDK_DISABLED`
/// path). Diagnostics go to stderr to match the rest of the workspace.
fn install_fmt_only_subscriber() -> Result<()> {
    tracing_subscriber::registry()
        .with(global_env_filter())
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .try_init()
        .context("installing the fmt-only tracing subscriber")
}

/// Emit the one-shot startup line dumping the resolved service identity, the
/// wire protocol/endpoint, and the raw `OTEL_RESOURCE_ATTRIBUTES` so an
/// operator can diagnose a missing-attribute problem from a single line.
fn emit_startup_line(version: &str, protocol: OtlpProtocol, resource: &Resource) {
    let resolved_service_name = resource
        .get(&Key::new(SERVICE_NAME))
        .map(|value| value.to_string())
        .unwrap_or_default();
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| DEFAULT_OTLP_ENDPOINT.to_string());
    let raw_resource_attributes = std::env::var("OTEL_RESOURCE_ATTRIBUTES").unwrap_or_default();

    tracing::info!(
        service_name = %resolved_service_name,
        service_version = version,
        protocol = protocol.as_str(),
        endpoint = %endpoint,
        otel_resource_attributes = %raw_resource_attributes,
        "OpenTelemetry initialised"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    /// Fetch a resource attribute's value as an owned `String`.
    fn attribute(resource: &Resource, key: &str) -> Option<String> {
        resource
            .get(&Key::new(key.to_string()))
            .map(|v| v.to_string())
    }

    #[test]
    #[serial]
    fn resource_strips_unknown_service_when_env_unset() {
        // Arrange: no operator-supplied identity.
        std::env::remove_var("OTEL_SERVICE_NAME");
        std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");

        // Act
        let resource = build_resource("sandbawks-api", "9.9.9");

        // Assert: the compile-time fallback survives, not the placeholder.
        assert_eq!(
            attribute(&resource, SERVICE_NAME).as_deref(),
            Some("sandbawks-api")
        );
        assert_eq!(
            attribute(&resource, SERVICE_VERSION).as_deref(),
            Some("9.9.9")
        );
    }

    #[test]
    #[serial]
    fn resource_env_service_name_wins_over_fallback() {
        // Arrange: operator pins the service name via env.
        std::env::set_var("OTEL_SERVICE_NAME", "override-name");

        // Act
        let resource = build_resource("sandbawks-api", "9.9.9");

        // Assert: env wins, fallback version still present.
        assert_eq!(
            attribute(&resource, SERVICE_NAME).as_deref(),
            Some("override-name")
        );
        assert_eq!(
            attribute(&resource, SERVICE_VERSION).as_deref(),
            Some("9.9.9")
        );

        // Cleanup so the other serial test sees a clean environment.
        std::env::remove_var("OTEL_SERVICE_NAME");
    }

    #[test]
    #[serial]
    fn resource_carries_vcs_revision() {
        // Arrange: no operator-supplied identity, so the compile-time fallback
        // (the build-time SHA) lands on the resource.
        std::env::remove_var("OTEL_SERVICE_NAME");
        std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");

        // Act
        let resource = build_resource("sandbawks-api", "9.9.9");

        // Assert: `vcs.revision` survived the `with_attribute`/`with_attributes`
        // merge intact. Check its *shape* rather than comparing against the same
        // `vcs_revision()` that produced it (which would be tautological): a
        // lower-hex SHA (optionally `-dirty`), or the `unknown` sentinel.
        let revision = attribute(&resource, VCS_REVISION)
            .expect("vcs.revision must be present on the resource");
        assert!(
            is_well_formed_revision(&revision),
            "vcs.revision {revision:?} is neither a hex SHA nor the `unknown` sentinel"
        );
    }

    /// Whether `value` is a plausible `vcs.revision`: the `unknown` sentinel, or
    /// 7–40 lower-hex characters with an optional `-dirty` suffix (matching the
    /// shape `build.rs` can emit).
    fn is_well_formed_revision(value: &str) -> bool {
        if value == UNKNOWN_REVISION {
            return true;
        }
        let sha = value.strip_suffix("-dirty").unwrap_or(value);
        (7..=40).contains(&sha.len())
            && sha
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    #[test]
    fn vcs_revision_falls_back_to_unknown_sentinel() {
        // The resolved revision is never blank: a missing/empty build-time SHA
        // collapses to the `unknown` sentinel rather than an empty attribute.
        assert!(!vcs_revision().is_empty());
        match BUILD_GIT_SHA.map(str::trim).filter(|sha| !sha.is_empty()) {
            Some(sha) => assert_eq!(vcs_revision(), sha),
            None => assert_eq!(vcs_revision(), UNKNOWN_REVISION),
        }
    }

    #[test]
    fn sampler_strategy_parses_each_known_name() {
        assert_eq!(
            SamplerStrategy::parse("always_on"),
            Some(SamplerStrategy::AlwaysOn)
        );
        assert_eq!(
            SamplerStrategy::parse("always_off"),
            Some(SamplerStrategy::AlwaysOff)
        );
        assert_eq!(
            SamplerStrategy::parse("traceidratio"),
            Some(SamplerStrategy::TraceIdRatio)
        );
        assert_eq!(
            SamplerStrategy::parse("parentbased_always_on"),
            Some(SamplerStrategy::ParentBasedAlwaysOn)
        );
        assert_eq!(
            SamplerStrategy::parse("parentbased_always_off"),
            Some(SamplerStrategy::ParentBasedAlwaysOff)
        );
        assert_eq!(
            SamplerStrategy::parse("parentbased_traceidratio"),
            Some(SamplerStrategy::ParentBasedTraceIdRatio)
        );
    }

    #[test]
    fn sampler_strategy_unknown_name_is_none() {
        assert_eq!(SamplerStrategy::parse("nonsense"), None);
        assert_eq!(SamplerStrategy::parse(""), None);
        // The default is applied by the caller when parsing yields `None`.
        assert_eq!(
            SamplerStrategy::DEFAULT,
            SamplerStrategy::ParentBasedAlwaysOn
        );
    }

    #[test]
    fn sampler_strategy_builds_expected_variants() {
        assert!(matches!(
            SamplerStrategy::AlwaysOn.into_sampler(1.0),
            Sampler::AlwaysOn
        ));
        assert!(matches!(
            SamplerStrategy::AlwaysOff.into_sampler(1.0),
            Sampler::AlwaysOff
        ));
        assert!(matches!(
            SamplerStrategy::TraceIdRatio.into_sampler(0.25),
            Sampler::TraceIdRatioBased(ratio) if (ratio - 0.25).abs() < f64::EPSILON
        ));
        assert!(matches!(
            SamplerStrategy::ParentBasedAlwaysOn.into_sampler(1.0),
            Sampler::ParentBased(_)
        ));
        assert!(matches!(
            SamplerStrategy::ParentBasedTraceIdRatio.into_sampler(0.5),
            Sampler::ParentBased(_)
        ));
    }

    #[test]
    fn export_flush_timeout_clamps_to_floor_and_default() {
        // Unset: the bounded default that keeps `shutdown` under the resume
        // quiesce deadline (issue #533).
        assert_eq!(export_flush_timeout(None), DEFAULT_EXPORT_FLUSH_TIMEOUT);
        // A smaller-but-sane operator timeout wins (an operator may tighten the
        // bound within [floor, default]).
        let tighter = Duration::from_millis(1500);
        assert_eq!(export_flush_timeout(Some(tighter)), tighter);
        // A larger operator timeout is capped back to the default — extending it
        // would reintroduce the resume-blocking hang the cap prevents.
        assert_eq!(
            export_flush_timeout(Some(Duration::from_secs(60))),
            DEFAULT_EXPORT_FLUSH_TIMEOUT
        );
        // A degenerate zero (`OTEL_EXPORTER_OTLP_TIMEOUT=0`) is raised to the
        // floor rather than collapsing every export to an instant timeout.
        assert_eq!(
            export_flush_timeout(Some(Duration::ZERO)),
            MIN_EXPORT_FLUSH_TIMEOUT
        );
        // Any sub-floor value is likewise raised to the floor.
        assert_eq!(
            export_flush_timeout(Some(Duration::from_millis(50))),
            MIN_EXPORT_FLUSH_TIMEOUT
        );
    }

    #[test]
    #[serial]
    fn protocol_defaults_to_http_protobuf() {
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
        assert_eq!(resolve_protocol().unwrap(), OtlpProtocol::HttpProtobuf);
    }

    #[test]
    #[serial]
    fn protocol_grpc_is_recognised_and_unknown_errors() {
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
        assert_eq!(resolve_protocol().unwrap(), OtlpProtocol::Grpc);

        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "carrier-pigeon");
        assert!(resolve_protocol().is_err());

        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    }

    #[test]
    #[serial]
    fn sampler_ratio_defaults_clamps_and_warns() {
        let mut warnings = Vec::new();

        std::env::remove_var("OTEL_TRACES_SAMPLER_ARG");
        assert_eq!(sampler_ratio(&mut warnings), 1.0);
        assert!(warnings.is_empty());

        std::env::set_var("OTEL_TRACES_SAMPLER_ARG", "0.3");
        assert!((sampler_ratio(&mut warnings) - 0.3).abs() < f64::EPSILON);
        assert!(warnings.is_empty());

        std::env::set_var("OTEL_TRACES_SAMPLER_ARG", "5.0");
        assert_eq!(sampler_ratio(&mut warnings), 1.0);
        assert_eq!(warnings.len(), 1, "out-of-range ratio should warn");

        std::env::set_var("OTEL_TRACES_SAMPLER_ARG", "not-a-number");
        assert_eq!(sampler_ratio(&mut warnings), 1.0);
        assert_eq!(warnings.len(), 2, "unparsable ratio should warn");

        std::env::remove_var("OTEL_TRACES_SAMPLER_ARG");
    }

    /// The re-arm seam (issue #539): `make_provider_factory` must rebuild the
    /// providers *on the export runtime*, not the caller's runtime — this is the
    /// path `RearmHandle::rearm` drives after a restore, and the `rearm.rs` tests
    /// deliberately bypass it with an in-memory factory.
    ///
    /// The proof is structural and needs no collector: the async-runtime batch
    /// processors call a bare `tokio::spawn` *at build time*
    /// ([`opentelemetry_sdk::runtime::Tokio`]), which panics outside any runtime.
    /// This test runs the factory from a thread with **no ambient Tokio
    /// runtime**, so it can only succeed if `build_providers_on` entered the
    /// export runtime's context — exactly the isolation the re-arm path relies
    /// on. Were the `handle.enter()` removed, this would panic instead of
    /// returning `Ok`.
    #[test]
    #[serial]
    fn provider_factory_rebuilds_on_the_export_runtime() {
        // Hermetic env: defaults only, so the factory resolves the standard
        // http/protobuf exporter without an operator override leaking in.
        std::env::remove_var("OTEL_SDK_DISABLED");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

        let export_rt = ExportRuntime::new().expect("build export runtime");
        let factory = make_provider_factory("test-service", "9.9.9", export_rt);

        // No `#[tokio::main]` / `block_on` here: this thread has no current
        // runtime. A successful build can only mean the processors' build-time
        // `tokio::spawn` ran inside the export runtime `factory` captured.
        let providers = factory().expect("factory rebuilds providers off the export runtime");

        // Tear down on this (runtime-less) thread: dropping `Providers` runs each
        // provider's shutdown. The exporters never connected, so the bounded
        // final flush over empty batches returns promptly.
        drop(providers);
    }
}
