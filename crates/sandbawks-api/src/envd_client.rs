//! Pooled `tonic` channels keyed by `host:port`, shared across every
//! envd-bound route.
//!
//! Each sandbox Pod exposes a single envd process at a stable
//! `status.envdAddress` (e.g. `10.42.7.13:49982`). The exec / file /
//! metadata routes (Plan 04 T10–T12) all need to talk to that envd —
//! and they get called many times per sandbox lifecycle, often
//! concurrently from the same axum worker pool. Opening a fresh
//! `tonic::transport::Channel` per request would re-handshake h2 and
//! restart connection pooling per call, which the upstream E2B SDKs
//! never do.
//!
//! [`EnvdPool`] solves that by caching one `Channel` per `host:port`
//! string in a [`DashMap`]. `Channel` is already
//! `Clone`-cheap (an `Arc<Inner>` internally), so handing each handler
//! its own clone keeps the underlying h2 connection multiplexed across
//! every concurrent RPC. The map itself is wrapped in `Arc` so the
//! pool can be cloned freely as part of [`crate::AppState`] without
//! duplicating the channel cache.
//!
//! # No TLS — yet
//!
//! envd runs in the same Pod-network as the API gateway. We
//! deliberately use `http://` rather than `https://` here: the only
//! traffic on this connection is from the gateway to envd over the
//! cluster's internal CNI, and adding TLS would require provisioning
//! per-sandbox certificates the controller doesn't currently mint.
//! Plan 06 may revisit this if the SDK-compat surface grows to expose
//! mTLS.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use envd::proto::filesystem::filesystem_client::FilesystemClient;
use envd::proto::process::process_client::ProcessClient;
use sandbawks_observability::Injector;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};

use crate::error::AppError;
use crate::middleware::HTTP_REQUEST_TIMEOUT_SECS;

/// A tonic [`Interceptor`] that injects the active span's W3C `traceparent`
/// into every outbound RPC's metadata.
///
/// Installed on every client this pool hands out (see [`EnvdPool::filesystem`]
/// et al.), it is the *upstream* half of the api -> envd trace hop (issue
/// #298): envd's [`GrpcTraceLayer`](envd::trace::GrpcTraceLayer) reads the
/// header back and parents its per-RPC span to the api span, so both share one
/// `trace_id`. Outside any span the injected context is empty and no header is
/// written — an un-traced call path stays valid.
#[derive(Clone, Copy, Default)]
pub struct TraceparentInjector;

impl Interceptor for TraceparentInjector {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        sandbawks_observability::inject_current(&mut MetadataInjector(request.metadata_mut()));
        Ok(request)
    }
}

/// [`Injector`] adapter over tonic's [`MetadataMap`], so the W3C propagator can
/// write `traceparent` into outbound gRPC metadata. tonic owns `MetadataMap`,
/// so this adapter lives here rather than in `sandbawks-observability` (which
/// stays tonic-free).
struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    /// Set `key: value`, silently skipping a pair that is not a valid ASCII
    /// metadata entry (the W3C keys always are — this only guards a future
    /// non-conforming propagator). Binary (`-bin`) keys never arise here.
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(key), Ok(value)) = (
            MetadataKey::from_bytes(key.as_bytes()),
            MetadataValue::try_from(value),
        ) {
            self.0.insert(key, value);
        }
    }
}

/// A pooled envd [`Channel`] wrapped with the [`TraceparentInjector`].
///
/// Named so the generated client types below are nameable in `EnvdPool`'s
/// method signatures (and in callers that annotate the client type, e.g.
/// `exec.rs`) without spelling out the `InterceptedService` generics.
pub type TracedChannel = InterceptedService<Channel, TraceparentInjector>;

/// `FilesystemClient` over a trace-propagating pooled channel.
pub type EnvdFilesystemClient = FilesystemClient<TracedChannel>;

/// `ProcessClient` over a trace-propagating pooled channel.
pub type EnvdProcessClient = ProcessClient<TracedChannel>;

/// Time we'll wait for the initial TCP/h2 handshake to envd before
/// declaring the upstream unavailable. Kept short — envd lives in the
/// same Pod network, so anything over a few seconds means the sandbox
/// Pod is gone or networking is broken, not that envd is "slow".
const ENVD_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Headroom (in seconds) subtracted from the outer HTTP budget to derive
/// [`ENVD_REQUEST_TIMEOUT`]. Keeping the inner per-RPC deadline strictly
/// *below* the outer [`timeout_layer`](crate::middleware::timeout_layer)
/// budget guarantees the tonic deadline trips first and surfaces a clean,
/// deterministic gRPC status rather than racing — and losing to — the outer
/// axum layer's raw 408 (issue #353).
const ENVD_REQUEST_TIMEOUT_MARGIN_SECS: u64 = 5;

/// Per-RPC timeout applied by tonic on every *unary* call made through
/// a channel from this pool. Bounded high enough that a unary RPC can
/// run without tripping it, but low enough that a wedged envd doesn't
/// leak handler tasks indefinitely.
///
/// Derived from [`HTTP_REQUEST_TIMEOUT_SECS`] minus
/// [`ENVD_REQUEST_TIMEOUT_MARGIN_SECS`] so the invariant *inner per-RPC
/// deadline < outer HTTP budget* holds **by construction**. Before #353 this
/// was a hard-coded 30s that silently undercut #349's 60s HTTP budget, so the
/// first unary filesystem RPC after a resume (`GET /files/download`) was
/// killed at 30s — inside the resumed-envd serve-readiness window — and
/// rendered as a 500, even though the sandbox was healthy. Tying the two
/// constants together prevents that drift from recurring.
///
/// **Not applied to the streaming channel** — see [`EnvdPool::streaming_channel`].
/// tonic translates `Endpoint::timeout` into a per-RPC `grpc-timeout`
/// header that bounds the *whole* call, including server-streaming
/// responses. A finite cap there would amputate any
/// `commands.run(...).wait()` (or `process.Connect` re-attach) that
/// outlives it, which is common in real SDK use (dependency installs,
/// long test suites). The streaming surface therefore dials a separate
/// channel with no per-RPC deadline and relies on the client closing
/// the stream (or h2 keepalive detecting a dead peer) for cleanup.
const ENVD_REQUEST_TIMEOUT: Duration =
    Duration::from_secs(HTTP_REQUEST_TIMEOUT_SECS - ENVD_REQUEST_TIMEOUT_MARGIN_SECS);

/// Compile-time guard for the #353 invariant: the inner per-RPC tonic
/// deadline must stay strictly below the outer HTTP budget, otherwise a
/// stuck unary call races the axum [`timeout_layer`](crate::middleware::timeout_layer)
/// and the binding constraint becomes non-deterministic. A non-zero margin
/// also rejects the degenerate "equal budgets" case.
const _: () = assert!(
    ENVD_REQUEST_TIMEOUT_MARGIN_SECS > 0
        && ENVD_REQUEST_TIMEOUT_MARGIN_SECS < HTTP_REQUEST_TIMEOUT_SECS,
    "ENVD_REQUEST_TIMEOUT must be > 0 and strictly below the HTTP budget"
);

/// Shared cache of `tonic::transport::Channel`s keyed by
/// `host:port`.
///
/// `Clone` is cheap — every clone shares the same inner [`DashMap`]
/// via `Arc`, so any route that gets a clone via `AppState` sees the
/// same cached channels as every other route.
///
/// Two independent caches are kept per address:
///
/// - `inner` — channels carrying the [`ENVD_REQUEST_TIMEOUT`] per-RPC
///   deadline, used by every unary RPC.
/// - `streaming_inner` — channels with *no* per-RPC deadline, used by
///   the server-streaming RPCs (`Process.Start`, `Process.Connect`).
///
/// They cannot share a channel because the deadline is baked into the
/// `Endpoint` at connect time, not chosen per call.
///
/// # Python analogue
///
/// Conceptually similar to `httpx.AsyncClient(pool_limits=...)`
/// shared as a module-level singleton: the first call to a given host
/// performs the handshake, every subsequent call reuses the pooled
/// connection.
#[derive(Clone, Default)]
pub struct EnvdPool {
    /// `Arc<DashMap<_, _>>` rather than `DashMap<_, _>` so cloning the
    /// pool (which `AppState` does on every request) doesn't duplicate
    /// the cache — only the `Arc` refcount bumps.
    inner: Arc<DashMap<String, Channel>>,

    /// Companion cache of deadline-free channels for server-streaming
    /// RPCs. Keyed by the same `host:port` string as [`Self::inner`].
    streaming_inner: Arc<DashMap<String, Channel>>,
}

impl EnvdPool {
    /// Per-RPC deadline carried by the *unary* channel flavour
    /// ([`Self::channel`]). `Some(_)` — every unary call is bounded by
    /// [`ENVD_REQUEST_TIMEOUT`].
    const UNARY_TIMEOUT: Option<Duration> = Some(ENVD_REQUEST_TIMEOUT);

    /// Per-RPC deadline carried by the *streaming* channel flavour
    /// ([`Self::streaming_channel`]). `None` — server-streaming RPCs must
    /// outlive any finite per-RPC deadline, so this flavour is dialled with
    /// no `grpc-timeout`. Named (rather than an inline `None`) so the
    /// "streaming stays deadline-free" contract is a single testable symbol.
    const STREAMING_TIMEOUT: Option<Duration> = None;

    /// Build an empty pool. Identical to [`EnvdPool::default`]; kept
    /// as a named constructor so call-sites read as `EnvdPool::new()`
    /// alongside the other `AppState` field initializers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch the channel for `addr` (`host:port`), opening a fresh one
    /// if this is the first call for that address.
    ///
    /// The returned [`Channel`] is the same instance for every caller
    /// that passes the same `addr`, so the underlying h2 connection
    /// is multiplexed across every concurrent RPC.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::UpstreamUnavailable`] (with code
    /// `envd_unavailable`) if `addr` doesn't parse as a URI or if the
    /// initial TCP/h2 handshake can't complete within the connect
    /// timeout (see `ENVD_CONNECT_TIMEOUT`).
    pub async fn channel(&self, addr: &str) -> Result<Channel, AppError> {
        self.channel_from(&self.inner, addr, Self::UNARY_TIMEOUT)
            .await
    }

    /// Fetch a *deadline-free* channel for `addr`, for server-streaming
    /// RPCs that must outlive [`ENVD_REQUEST_TIMEOUT`].
    ///
    /// Identical to [`channel`](Self::channel) except the underlying
    /// `Endpoint` is built without `.timeout(...)`, so tonic emits no
    /// `grpc-timeout` header and the stream runs until the client
    /// closes it or h2 keepalive trips. Cached separately in
    /// `streaming_inner` so it never collides with the timed unary
    /// channel for the same address.
    ///
    /// # Errors
    ///
    /// Same as [`channel`](Self::channel).
    pub async fn streaming_channel(&self, addr: &str) -> Result<Channel, AppError> {
        self.channel_from(&self.streaming_inner, addr, Self::STREAMING_TIMEOUT)
            .await
    }

    /// Shared cache-or-connect path for both channel flavours.
    ///
    /// `request_timeout` is the per-RPC `.timeout(...)` applied to the
    /// endpoint: `Some(_)` for the unary flavour, `None` for the
    /// deadline-free streaming flavour. `cache` selects which of the two
    /// `DashMap`s to read / populate.
    ///
    /// Deliberately *not* `#[instrument]`ed: this is on the hot path of every
    /// envd RPC and the overwhelming majority of calls hit the cache and clone
    /// a `DashMap` entry in sub-microseconds. A span here would fire per RPC —
    /// the "span per item in a hot loop" the instrumentation guide warns
    /// against. The cold connect (the one edge worth a client span) is split
    /// into [`connect_channel`](Self::connect_channel) below.
    async fn channel_from(
        &self,
        cache: &DashMap<String, Channel>,
        addr: &str,
        request_timeout: Option<Duration>,
    ) -> Result<Channel, AppError> {
        // Fast path: already cached. `DashMap::get` returns a guard
        // that derefs to `&Channel`; we clone the channel (cheap) and
        // drop the guard before the await point below so we never
        // hold a shard lock across `.connect().await`.
        if let Some(existing) = cache.get(addr) {
            return Ok(existing.clone());
        }

        let channel = self.connect_channel(addr, request_timeout).await?;

        // `entry().or_insert()` collapses the race where two tasks miss
        // the cache for the same `addr` concurrently: the first to grab
        // the shard write-lock wins, the second's freshly-built channel
        // is dropped and replaced with the cached one. Channels are
        // `Clone`-cheap, so the wasted handshake is bounded and only
        // happens during cold-start contention — not on the
        // steady-state path.
        Ok(cache.entry(addr.to_owned()).or_insert(channel).clone())
    }

    /// Build and connect a fresh h2 channel to `addr` — the cold-cache edge
    /// of [`channel_from`](Self::channel_from).
    ///
    /// This is the only part of the channel path that does real I/O (the
    /// TCP/h2 handshake), so it carries the client span; the cache-hit path
    /// above stays span-free so steady-state RPCs don't each emit one.
    #[tracing::instrument(
        skip_all,
        name = "client:api::envd_connect",
        err,
        fields(otel.kind = "client", network.peer.address = %addr)
    )]
    async fn connect_channel(
        &self,
        addr: &str,
        request_timeout: Option<Duration>,
    ) -> Result<Channel, AppError> {
        let mut endpoint = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|err| {
                AppError::upstream(
                    "envd_unavailable",
                    format!("invalid envd address {addr:?}: {err}"),
                )
            })?
            .connect_timeout(ENVD_CONNECT_TIMEOUT);
        if let Some(timeout) = request_timeout {
            endpoint = endpoint.timeout(timeout);
        }

        endpoint.connect().await.map_err(|err| {
            tracing::warn!(addr, error = %err, "envd connect failed");
            AppError::upstream(
                "envd_unavailable",
                format!("envd connect to {addr} failed: {err}"),
            )
        })
    }

    /// Convenience constructor for the generated `FilesystemClient`,
    /// pinned to the pooled channel for `addr`.
    ///
    /// Equivalent to `FilesystemClient::new(pool.channel(addr).await?)`,
    /// but lifts the channel boilerplate out of every filesystem
    /// route handler.
    pub async fn filesystem(&self, addr: &str) -> Result<EnvdFilesystemClient, AppError> {
        Ok(FilesystemClient::with_interceptor(
            self.channel(addr).await?,
            TraceparentInjector,
        ))
    }

    /// Convenience constructor for the generated `ProcessClient`,
    /// pinned to the pooled (timed) channel for `addr`. For unary
    /// process RPCs (`List`, `SendSignal`, `SendInput`, `Update`,
    /// `CloseStdin`).
    pub async fn process(&self, addr: &str) -> Result<EnvdProcessClient, AppError> {
        Ok(ProcessClient::with_interceptor(
            self.channel(addr).await?,
            TraceparentInjector,
        ))
    }

    /// `ProcessClient` pinned to the deadline-free [`streaming_channel`]
    /// for `addr`. For the server-streaming process RPCs (`Start`,
    /// `Connect`) that must not be cut off by the unary 30s deadline.
    ///
    /// [`streaming_channel`]: Self::streaming_channel
    pub async fn process_streaming(&self, addr: &str) -> Result<EnvdProcessClient, AppError> {
        Ok(ProcessClient::with_interceptor(
            self.streaming_channel(addr).await?,
            TraceparentInjector,
        ))
    }

    /// Drop the cached channel for `addr` so a subsequent
    /// [`channel`](Self::channel) call rebuilds the underlying h2
    /// connection from scratch.
    ///
    /// Called by `pause_sandbox` before patching `spec.desiredPhase`:
    /// gVisor's `runsc checkpoint` rejects sandboxes that have any
    /// connected TCP endpoints (`endpoint cannot be saved in connected
    /// state: local … remote …`), and the api gateway's keep-alive
    /// gRPC pool to envd is the most common offender. Evicting here
    /// releases the last reference the gateway holds; the kernel
    /// drains and closes the socket on the next runtime tick.
    ///
    /// Per-Pod eviction only — with multiple api replicas, the other
    /// replicas may still hold open channels. The deploy/helm chart
    /// pins `api.replicas: 1` in the smoke path for that reason.
    ///
    /// Evicts from *both* the unary and streaming caches: a live
    /// streaming channel holds a connected TCP endpoint too, so
    /// leaving it in place would defeat the checkpoint-readiness goal.
    pub fn evict(&self, addr: &str) {
        let unary = self.inner.remove(addr).is_some();
        let streaming = self.streaming_inner.remove(addr).is_some();
        if unary || streaming {
            tracing::debug!(addr, unary, streaming, "evicted envd channel(s) from pool");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::HTTP_REQUEST_TIMEOUT;

    /// The #353 invariant: the inner per-RPC tonic deadline must stay strictly
    /// below the outer axum HTTP budget. If they ever cross (or meet), a stuck
    /// unary call races the two timers and the binding constraint becomes
    /// non-deterministic — exactly the regression where a 30s per-RPC deadline
    /// undercut #349's 60s budget and killed post-resume `GET /files/download`.
    #[test]
    fn unary_deadline_stays_below_http_budget() {
        assert!(
            ENVD_REQUEST_TIMEOUT < HTTP_REQUEST_TIMEOUT,
            "inner per-RPC deadline {ENVD_REQUEST_TIMEOUT:?} must be < outer HTTP budget {HTTP_REQUEST_TIMEOUT:?}"
        );
        // A degenerate near-zero deadline would technically satisfy `<` while
        // breaking the post-resume path; assert it kept real headroom over the
        // old 30s ceiling the issue raised it from.
        assert!(
            ENVD_REQUEST_TIMEOUT >= Duration::from_secs(30),
            "inner per-RPC deadline {ENVD_REQUEST_TIMEOUT:?} regressed below the prior 30s floor"
        );
    }

    /// The streaming flavour must carry *no* per-RPC deadline, or a
    /// `commands.run(...).wait()` / `Process.Connect` re-attach that outlives
    /// the unary budget would be amputated mid-stream. This is the out-of-scope
    /// guard the issue called out explicitly.
    #[test]
    fn streaming_channel_is_deadline_free() {
        assert!(
            EnvdPool::STREAMING_TIMEOUT.is_none(),
            "streaming channel must remain deadline-free"
        );
    }

    /// The unary flavour is the one that *does* carry the bounded deadline.
    #[test]
    fn unary_channel_carries_the_bounded_deadline() {
        assert_eq!(EnvdPool::UNARY_TIMEOUT, Some(ENVD_REQUEST_TIMEOUT));
    }
}
