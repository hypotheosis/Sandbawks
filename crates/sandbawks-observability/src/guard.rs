//! RAII shutdown guard for the OpenTelemetry SDK.
//!
//! [`init_telemetry`](crate::init_telemetry) returns a [`TelemetryGuard`].
//! Holding it until the end of `main` is the single most important footgun to
//! avoid: the trace and log pipelines use *batch* processors that flush
//! asynchronously, so a short-lived process that drops the guard early (or
//! never binds it to a name — `let _ = ...` drops it *immediately* in Rust)
//! tears the processors down before they flush and exports nothing.
//!
//! Bind it to a named variable that lives for the whole process:
//!
//! ```no_run
//! # fn main() -> anyhow::Result<()> {
//! let (_telemetry, _rearm) = sandbawks_observability::init_telemetry(
//!     "sandbawks-api",
//!     env!("CARGO_PKG_VERSION"),
//! )?;
//! // ... run the app; `_telemetry` drops here, flushing all three pipelines.
//! # Ok(())
//! # }
//! ```
//!
//! # Sharing with [`RearmHandle`](crate::RearmHandle)
//!
//! The guard and the re-arm handle both need to reach the three live
//! providers — the guard to flush them on drop, the handle to swap them across
//! a pause/resume (`RearmHandle::quiesce` tears the old ones down,
//! `RearmHandle::rearm` installs fresh ones). They therefore share a single
//! `Arc<Mutex<Option<Providers>>>` cell: whoever shuts the providers down
//! `take`s them out, so the other side sees `None` and the underlying provider
//! shutdowns run at most once. After a successful re-arm the cell holds the
//! *new* providers, so a later guard drop flushes whatever is current.

use std::sync::{Arc, Mutex};

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;

use crate::export_runtime::ExportRuntime;

/// The three signal providers built by `init_telemetry`, held together so a
/// single place can shut them all down.
///
/// `pub(crate)` (not private) because the re-arm path in
/// [`rearm`](crate::rearm) builds a fresh set on resume and tears the stale set
/// down; the fields are read there to wire the new providers into the reload
/// layers and the global meter/tracer. Callers outside the crate only ever
/// touch the providers through the `tracing` macros and the global meter.
#[derive(Debug)]
pub(crate) struct Providers {
    pub(crate) tracer: SdkTracerProvider,
    pub(crate) meter: SdkMeterProvider,
    pub(crate) logger: SdkLoggerProvider,
}

impl Providers {
    /// Bundle the three freshly-built providers.
    pub(crate) fn new(
        tracer: SdkTracerProvider,
        meter: SdkMeterProvider,
        logger: SdkLoggerProvider,
    ) -> Self {
        Self {
            tracer,
            meter,
            logger,
        }
    }
}

/// The provider cell shared between [`TelemetryGuard`] and
/// [`RearmHandle`](crate::RearmHandle).
///
/// `Some` while providers are live; `None` once they have been shut down (by
/// either side) or for a disabled guard. The `Mutex` serialises the
/// `take`/replace between a guard drop and a concurrent quiesce/rearm.
pub(crate) type SharedProviders = Arc<Mutex<Option<Providers>>>;

/// RAII guard whose `Drop` flushes and shuts down the OpenTelemetry SDK.
///
/// Two shapes:
/// - **active** — shares the live-providers cell; `Drop` (or an explicit
///   [`shutdown`](Self::shutdown)) flushes and tears down whatever providers
///   the cell currently holds.
/// - **disabled** — owns an empty cell; produced by
///   [`TelemetryGuard::disabled`] when `OTEL_SDK_DISABLED` short-circuits init.
///   Dropping it is a no-op.
///
/// Shutdown is idempotent: it is driven through [`Option::take`] on the shared
/// cell, so calling [`shutdown`](Self::shutdown) and then dropping the guard
/// (or a concurrent [`RearmHandle::quiesce`](crate::RearmHandle::quiesce)) runs
/// the underlying provider shutdowns at most once.
#[derive(Debug)]
pub struct TelemetryGuard {
    /// Shared with the re-arm handle; `None` once shut down or for a disabled
    /// guard, `Some` while active.
    providers: SharedProviders,
    /// The dedicated export runtime (issue #539), held so it outlives the
    /// providers it drives: the guard's flush-on-drop runs a final export that
    /// needs a live runtime, so the explicit [`shutdown`](Self::shutdown) in
    /// `Drop` flushes the providers *before* this field drops and the last
    /// `Arc` tears the runtime down. `None` for a disabled guard, which never
    /// built a runtime. Shared via [`Arc`] with the re-arm factory.
    ///
    /// The leading underscore marks it RAII-only: it is never read, existing
    /// purely to extend the runtime's lifetime and run `ExportRuntime::drop`
    /// when released. The `_` exempts it from the dead-code lint without an
    /// explicit suppression.
    _export_rt: Option<Arc<ExportRuntime>>,
}

impl TelemetryGuard {
    /// Build an active guard over the shared providers cell, holding the export
    /// runtime alive for the guard's lifetime.
    pub(crate) fn new(providers: SharedProviders, export_rt: Option<Arc<ExportRuntime>>) -> Self {
        Self {
            providers,
            _export_rt: export_rt,
        }
    }

    /// Build a disabled guard that owns an empty (never-populated) cell.
    ///
    /// Returned from `init_telemetry` when `OTEL_SDK_DISABLED` is set: the
    /// fmt-only subscriber is still installed, but no exporter is ever
    /// constructed, so there is nothing to flush on drop and no export runtime
    /// was built.
    pub(crate) fn disabled() -> Self {
        Self {
            providers: Arc::new(Mutex::new(None)),
            _export_rt: None,
        }
    }

    /// Whether this guard currently has no providers to flush (disabled, or
    /// already shut down / quiesced).
    pub fn is_disabled(&self) -> bool {
        lock_recover(&self.providers).is_none()
    }

    /// Flush and shut down all three providers, idempotently.
    ///
    /// `Drop` calls this, but it is exposed so a binary can flush
    /// deterministically before the end of `main` if it wants to. The
    /// `Option::take` on the shared cell makes a second call (and the
    /// subsequent drop, or a racing quiesce) a no-op, so the underlying
    /// provider shutdowns run at most once. Per-provider shutdown errors are
    /// logged at `warn` rather than propagated — by the time the process is
    /// tearing down there is nothing actionable to do with them.
    pub fn shutdown(&mut self) {
        let Some(providers) = lock_recover(&self.providers).take() else {
            return;
        };
        // Order is not significant — the three pipelines are independent — but
        // shutting all three down even if an earlier one errors is, so each is
        // logged independently rather than short-circuited with `?`.
        log_shutdown("tracer", providers.tracer.shutdown());
        log_shutdown("meter", providers.meter.shutdown());
        log_shutdown("logger", providers.logger.shutdown());
    }
}

/// Lock a [`Mutex`], recovering the guard if a previous holder panicked.
///
/// A poisoned telemetry-provider lock is not worth panicking over: the data
/// behind it is still structurally valid, and the re-arm path is contractually
/// "never panic". Recovering the inner guard keeps shutdown/flush working
/// instead of cascading a second panic during teardown.
pub(crate) fn lock_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Log a single provider's shutdown result at `warn` on failure.
///
/// `pub(crate)` so the re-arm path's off-thread, deadline-bounded shutdown
/// (`rearm::quiesce`) reports stale-provider teardown failures the same way.
pub(crate) fn log_shutdown(provider: &str, result: OTelSdkResult) {
    if let Err(error) = result {
        // `%error` uses the `Display` impl; the OTel SDK error types are
        // `std::error::Error` but we only need the human-readable message here.
        tracing::warn!(provider, %error, "OpenTelemetry provider shutdown failed");
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an active guard from providers with no exporters attached. These
    /// build and shut down entirely in-process (no network), so they exercise
    /// the guard's lifecycle without a collector.
    fn active_guard() -> TelemetryGuard {
        let providers = Providers::new(
            SdkTracerProvider::builder().build(),
            SdkMeterProvider::builder().build(),
            SdkLoggerProvider::builder().build(),
        );
        // No export runtime: these providers carry no async-runtime processor,
        // so they build and flush entirely in-process.
        TelemetryGuard::new(Arc::new(Mutex::new(Some(providers))), None)
    }

    #[test]
    fn disabled_guard_reports_disabled_and_drops_cleanly() {
        let guard = TelemetryGuard::disabled();
        assert!(guard.is_disabled());
        // Dropping a disabled guard must not panic (no providers to flush).
        drop(guard);
    }

    #[test]
    fn active_guard_reports_enabled() {
        let guard = active_guard();
        assert!(!guard.is_disabled());
    }

    #[test]
    fn shutdown_is_idempotent() {
        let mut guard = active_guard();
        assert!(!guard.is_disabled());

        // First explicit shutdown drains the providers...
        guard.shutdown();
        assert!(
            guard.is_disabled(),
            "guard should report disabled once providers are taken"
        );

        // ...and a second shutdown — plus the implicit drop below — is a no-op
        // rather than a double-free / double-shutdown panic.
        guard.shutdown();
        assert!(guard.is_disabled());
        drop(guard);
    }

    #[test]
    fn shutdown_through_one_clone_disables_the_other() {
        // Two guards (here, two handles to the same cell — the guard/rearm
        // sharing the SDK uses) must not both run the provider shutdown.
        let cell: SharedProviders = Arc::new(Mutex::new(Some(Providers::new(
            SdkTracerProvider::builder().build(),
            SdkMeterProvider::builder().build(),
            SdkLoggerProvider::builder().build(),
        ))));
        let mut first = TelemetryGuard::new(cell.clone(), None);
        let second = TelemetryGuard::new(cell, None);

        first.shutdown();
        assert!(first.is_disabled());
        // The second guard sees the emptied cell and has nothing left to flush.
        assert!(second.is_disabled());
    }
}
