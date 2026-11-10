//! In-memory `sandbox_id -> envd address` cache fed by a single
//! `kube::runtime::watcher` (issue #201).
//!
//! # Why this exists
//!
//! The official E2B SDKs fire `files.write` + `commands.run` (and more)
//! in parallel immediately after `Sandbox.create`, before the controller
//! has reported `status.envdAddress`. The original
//! [`crate::routes::envd_proxy::resolve_sandbox`] handled that by polling
//! the apiserver with a `get` every 500ms for up to 30s — so a single
//! cold-start sandbox could generate up to `N_concurrent * 60` apiserver
//! `get`s before envd became ready.
//!
//! This module replaces that fan-out of polls with one shared watch
//! stream. A background task ([`EnvdCache::spawn`]) consumes
//! `kube::runtime::watcher` events for the `Sandbox` resource in a single
//! namespace and maintains an in-memory map of each sandbox's
//! reconcile-relevant projection ([`CachedSandbox`]). Readers take the
//! fast path (a lock-free [`DashMap`] read) when the address is already
//! known, or park on a shared [`Notify`] and wake on the next reconcile
//! that fills the address — collapsing `N` concurrent waiters per
//! sandbox onto one watch stream with zero polling.
//!
//! # Namespace scope
//!
//! The watcher is scoped to a single namespace (the gateway's
//! `default_namespace`). That matches `resolve_sandbox`, which looks
//! sandboxes up in `default_namespace` because the per-sandbox envd
//! calls carry no team identity. It deliberately does *not* back
//! `routes::sandboxes::connect_sandbox`, which operates in arbitrary
//! per-team namespaces resolved from the caller's API key — folding that
//! in would require a cluster-wide watch and broader RBAC.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use futures::StreamExt;
use kube::runtime::watcher;
use kube::{Api, ResourceExt};
use sandbawks_types::{PodStatusSummary, Sandbox, SandboxPhase};
use tokio::sync::Notify;
use tokio::time::Instant;

/// Container `state.waiting` / `state.terminated` reasons that mean a Pod
/// will **not** become ready without intervention — a bad image, a config
/// error, or a binary that crash-loops on startup (issue #625). The gateway
/// treats a sandbox whose backing Pod is stuck on one of these as a *terminal*
/// not-ready state (tell the caller to give up) rather than a transient one
/// (retry shortly). `RunContainerError` / `CreateContainerError` are usually
/// permanent (a bad command, a missing mount); they are still recoverable by
/// fixing the template, which is exactly the "won't resolve on its own" signal
/// the LLM driving a harness needs.
///
/// Note `CrashLoopBackOff` is *defensive* coverage today: sandbox Pods run with
/// `restartPolicy: Never` (see `pod_adapter::pod_spec_for`), so a crashed
/// container settles terminal without the kubelet ever entering a back-off
/// *loop*, and this reason should not surface for our Pods. It is kept so the
/// classification stays correct if that policy ever changes — but a future
/// restart-policy flip should revisit whether a single early restart ought to
/// be treated as immediately terminal.
const TERMINAL_POD_REASONS: &[&str] = &[
    "ImagePullBackOff",
    "ErrImagePull",
    "ErrImageNeverPull",
    "InvalidImageName",
    "CrashLoopBackOff",
    "CreateContainerError",
    "CreateContainerConfigError",
    "RunContainerError",
];

/// Container *waiting* reasons that mean a Pod **is scheduled and actively
/// booting** — the kubelet has a node, has accepted the Pod, and is pulling
/// the image / creating the container / running init steps. These are the
/// states that *plausibly serve within the bounded silent park*
/// (`SANDBAWKS_ENVD_SILENT_PARK_MAX_SECS`, default 15s), so a `process.Start`
/// racing one should still take the short silent park and proxy transparently
/// once envd comes up — rather than bounce the caller with a retry NOTICE
/// (issue #649).
///
/// The complement — a `Pending` Pod with *no* progressing reason (not yet
/// scheduled: `status.podStatus.reason` is `None`, or it carries a
/// scheduling-failure reason like `Unschedulable` projected from a
/// `PodScheduled=False` condition) — cannot plausibly serve within that
/// window: it is waiting on the scheduler / a node / capacity, which can take
/// far longer than 15s during a cold start when the node pool is at its
/// autoscaler max. Those get the **immediate** transient NOTICE instead of the
/// silent park, so a client polling readiness with a sub-15s per-call timeout
/// actually receives the "still starting (Pod is Pending); retry shortly"
/// signal before it aborts (issue #649).
const PROGRESSING_POD_REASONS: &[&str] = &["ContainerCreating", "PodInitializing"];

/// Reconcile-relevant projection of one `Sandbox`, cached by id.
///
/// We store only the fields the proxy's resolve path branches on rather
/// than the whole CR: the desired phase (to gate paused sandboxes), the
/// envd address (the value callers wait for), and — for the "still
/// starting / not ready yet" notice (issue #625) — the observed lifecycle
/// `phase`, the controller-projected backing-Pod summary, and the failure
/// reason. Keeping the cached value small makes clones on the read path
/// cheap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedSandbox {
    /// `spec.desiredPhase`. `Some(Paused)` means the proxy must reject
    /// per-sandbox RPCs with `sandbox_paused`.
    pub desired_phase: Option<SandboxPhase>,

    /// `status.phase` — the observed lifecycle phase. Drives the
    /// not-ready notice: `Resuming` / `Creating` is "still getting ready",
    /// `Failed` / `Terminating` is terminal (issue #625).
    pub phase: SandboxPhase,

    /// `status.envdAddress` — the `host:port` the gateway dials. `None`
    /// until the controller reports it.
    pub envd_address: Option<String>,

    /// `status.podStatus` — the controller's projection of the backing
    /// Pod's phase + dominant blocking reason, used to enrich the
    /// not-ready notice and to detect a stuck Pod (`ImagePullBackOff` /
    /// `CrashLoopBackOff`). `None` once the sandbox is serving, and for
    /// CRs written by a controller predating the field (issue #625).
    pub pod_status: Option<PodStatusSummary>,

    /// `status.failureReason` — the stable label of the fold that drove a
    /// `Failed` sandbox terminal, surfaced in the terminal notice so the
    /// caller sees *why* it will not become ready.
    pub failure_reason: Option<String>,
}

impl CachedSandbox {
    /// Project a live `Sandbox` CR into its cached form.
    fn from_sandbox(sandbox: &Sandbox) -> Self {
        let status = sandbox.status.as_ref();
        Self {
            // `SandboxPhase` is `Copy`, so this is a plain copy of the
            // `Option`, not a heap clone.
            desired_phase: sandbox.spec.desired_phase,
            phase: status.map(|s| s.phase).unwrap_or_default(),
            envd_address: status.and_then(|s| s.envd_address.clone()),
            pod_status: status.and_then(|s| s.pod_status.clone()),
            failure_reason: status.and_then(|s| s.failure_reason.clone()),
        }
    }

    /// Classify the sandbox for the proxy's resolve path.
    ///
    /// Ordering mirrors the historical fast path: a `desiredPhase=Paused`
    /// sandbox is rejected (`sandbox_paused`) before anything else, then a
    /// present `envdAddress` is served, and only an address-less sandbox is
    /// reported [`Readiness::NotReady`] with the detail the gateway turns
    /// into an actionable notice.
    pub fn readiness(&self) -> Readiness {
        if matches!(self.desired_phase, Some(SandboxPhase::Paused)) {
            return Readiness::Paused;
        }
        if let Some(addr) = self.envd_address.clone() {
            return Readiness::Ready(addr);
        }
        Readiness::NotReady(self.not_ready())
    }

    /// Build the [`NotReady`] detail for an address-less sandbox.
    fn not_ready(&self) -> NotReady {
        NotReady {
            sandbox_phase: self.phase,
            pod: self.pod_status.clone(),
            failure_reason: self.failure_reason.clone(),
            terminal: self.is_terminal(),
        }
    }

    /// Whether the sandbox will not become ready without intervention:
    /// either its lifecycle phase is terminal (`Failed` / `Terminating`),
    /// or its backing Pod is stuck on a [`TERMINAL_POD_REASONS`] reason.
    fn is_terminal(&self) -> bool {
        matches!(self.phase, SandboxPhase::Failed | SandboxPhase::Terminating)
            || self
                .pod_status
                .as_ref()
                .and_then(|p| p.reason.as_deref())
                .is_some_and(|r| TERMINAL_POD_REASONS.contains(&r))
    }
}

/// Why a sandbox is not (yet) proxyable, carried into the gateway's
/// not-ready notice (issue #625).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotReady {
    /// The observed lifecycle phase (`Creating` / `Resuming` / ...).
    pub sandbox_phase: SandboxPhase,

    /// The controller-projected backing-Pod summary, when one is recorded.
    pub pod: Option<PodStatusSummary>,

    /// `status.failureReason`, for a `Failed` sandbox's terminal notice.
    pub failure_reason: Option<String>,

    /// `true` when this state will not recover without intervention — the
    /// gateway tells the caller to give up rather than retry.
    pub terminal: bool,
}

impl NotReady {
    /// Whether this **transient** not-ready sandbox can plausibly start
    /// serving envd within the bounded silent-park window
    /// (`silent_park_max()`, default 15s) — i.e. whether the gateway should
    /// take the short silent park (and proxy transparently if it readies) or
    /// emit the transient NOTICE **immediately** (issue #649).
    ///
    /// Returns `true` (worth the short park) when the sandbox is mid-restore
    /// (`Resuming` / `Paused` / `Pausing` — p95 a few seconds, the transparent
    /// fast-resume the SDK relies on) **or** its backing Pod is scheduled and
    /// actively booting (a [`PROGRESSING_POD_REASONS`] waiting reason, or a
    /// non-`Pending` phase such as `Running` where envd is moments from up).
    ///
    /// Returns `false` (emit the notice now) when the backing Pod is `Pending`
    /// with no progressing reason — not yet scheduled (cold start waiting for a
    /// node) or stuck `Unschedulable` (node pool at capacity). Such a Pod
    /// genuinely *might* schedule once a node frees up, so the caller is told
    /// to keep retrying (`exit 75`, transient) — it just gets that signal
    /// immediately instead of after a silent 15s the short-timeout poller never
    /// waits out.
    ///
    /// Only meaningful for a transient (`!terminal`) detail; a terminal detail
    /// is handled by the fast-fail path before this is consulted.
    pub fn can_serve_within_silent_window(&self) -> bool {
        // Mid-restore phases ready fast and must stay transparent (#646) — keep
        // the short park regardless of the (often `Pending`) Pod projection.
        if matches!(
            self.sandbox_phase,
            SandboxPhase::Resuming | SandboxPhase::Paused | SandboxPhase::Pausing
        ) {
            return true;
        }
        match self.pod.as_ref() {
            // No projected Pod summary at all — preserve the historical short
            // park (a controller predating the field, or a sandbox whose Pod
            // hasn't been observed yet). Conservative: never *more* eager to
            // bounce the caller than before #649.
            None => true,
            Some(pod) => {
                // A non-`Pending` Pod (e.g. `Running`, container coming up) is
                // moments from serving — short park.
                if pod.phase != "Pending" {
                    return true;
                }
                // A `Pending` Pod serves within the window only when it is
                // scheduled and actively booting. No reason (not yet
                // scheduled) or a scheduling-failure reason (`Unschedulable`)
                // → emit the notice immediately.
                pod.reason
                    .as_deref()
                    .is_some_and(|r| PROGRESSING_POD_REASONS.contains(&r))
            }
        }
    }
}

/// Classification of a cached sandbox for the proxy's resolve path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readiness {
    /// `status.envdAddress` is populated — proxy to this address.
    Ready(String),

    /// `spec.desiredPhase == Paused` — reject with `sandbox_paused`.
    Paused,

    /// No address yet — the sandbox is still getting ready (or terminal).
    NotReady(NotReady),
}

/// Outcome of [`EnvdCache::wait_until_ready`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The sandbox is not paused and `status.envdAddress` is populated.
    Ready(String),

    /// The sandbox's `spec.desiredPhase` is `Paused`; the proxy must
    /// reject the call rather than wait for an address.
    Paused,

    /// A terminal not-ready state was observed mid-wait (a stuck Pod or a
    /// `Failed` sandbox) — there is no point waiting out the deadline.
    Terminal(NotReady),

    /// The deadline elapsed while the sandbox was still transiently
    /// not-ready; the detail describes the last-observed phase.
    TimedOut(NotReady),
}

/// Shared inner state. Held behind an `Arc` so [`EnvdCache`] clones (one
/// per request via `AppState`) all share the same map and notifier.
struct Inner {
    /// `sandbox_id -> namespace -> CachedSandbox`. [`DashMap`] mirrors the
    /// lock-free sharded map already used by [`crate::envd_client::EnvdPool`].
    ///
    /// Keyed by name first because a request names its sandbox by id; the
    /// inner map (almost always one entry — ids are generated) holds every
    /// namespace with a sandbox of that name, since names are only unique
    /// within a namespace (issue #13). [`EnvdCache::namespaces_of`] reads it
    /// for the signature-only `/files` path, which carries no namespace.
    map: DashMap<String, HashMap<String, CachedSandbox>>,

    /// Woken on every map mutation. Waiters re-read the map on each
    /// wake; a single global `Notify` (rather than one per sandbox)
    /// keeps the registration cost flat — most wakes find their entry
    /// unchanged and immediately re-park.
    notify: Notify,
}

/// Lock-free cache of per-sandbox envd addresses, fed by a `Sandbox`
/// watcher. Cheap to clone — every clone shares the same backing map and
/// notifier via `Arc`.
#[derive(Clone)]
pub struct EnvdCache {
    inner: Arc<Inner>,
}

impl EnvdCache {
    /// Build an empty cache with no backing watcher.
    ///
    /// Used by [`crate::AppState::for_test`]: tests drive the map
    /// directly via [`EnvdCache::apply`] instead of standing up a real
    /// apiserver watch.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                map: DashMap::new(),
                notify: Notify::new(),
            }),
        }
    }

    /// Build a cache and spawn the background watcher task that keeps it
    /// in sync with the `Sandbox` resource exposed by `api`.
    ///
    /// Must be called from within a Tokio runtime (the production
    /// constructor [`crate::AppState::from_kube`] runs under
    /// `#[tokio::main]`). The returned cache is immediately usable;
    /// reads simply miss until the watcher's initial list completes.
    pub fn spawn(api: Api<Sandbox>) -> Self {
        let cache = Self::new();
        tokio::spawn(run_watcher(cache.clone(), api));
        cache
    }

    /// Look up the cached projection of the sandbox `sandbox_id` in
    /// `namespace`. Returns `None` when the watcher has not (yet) seen it —
    /// the caller disambiguates "not synced" from "does not exist" with a
    /// single apiserver `get`.
    pub fn lookup(&self, namespace: &str, sandbox_id: &str) -> Option<CachedSandbox> {
        self.inner
            .map
            .get(sandbox_id)
            .and_then(|by_ns| by_ns.get(namespace).cloned())
    }

    /// Every namespace the watcher has seen a sandbox named `sandbox_id` in.
    ///
    /// Used where a request names a sandbox but carries no namespace (a
    /// signed `/files` URL): each candidate's credential is checked in turn.
    pub fn namespaces_of(&self, sandbox_id: &str) -> Vec<String> {
        self.inner
            .map
            .get(sandbox_id)
            .map(|by_ns| by_ns.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Upsert a sandbox's projection and wake any waiters.
    ///
    /// Called by the watcher on `Apply` events and by tests. The
    /// `resolve_sandbox` seed path uses [`EnvdCache::seed_if_absent`]
    /// instead, which must not clobber a fresher watcher write.
    pub fn apply(&self, sandbox: &Sandbox) {
        let Some((namespace, id)) = cache_key(sandbox) else {
            return;
        };
        self.inner
            .map
            .entry(id)
            .or_default()
            .insert(namespace, CachedSandbox::from_sandbox(sandbox));
        // Wake every parked waiter; each re-reads the map and either
        // resolves or re-parks. Cheap relative to the apiserver round
        // trips this replaces.
        self.inner.notify.notify_waiters();
    }

    /// Seed a sandbox's projection *only if* the watcher has not already
    /// cached one, leaving any existing entry untouched.
    ///
    /// `resolve_sandbox` calls this after its disambiguating `get`.
    /// Unlike [`EnvdCache::apply`], it must not overwrite: a watcher
    /// `Apply` carrying a newer projection (e.g. one that already has
    /// `envd_address`) can land while the `get` round-trip is in flight,
    /// and clobbering it with the older `get` response would strand the
    /// caller in [`EnvdCache::wait_until_ready`] for the full deadline
    /// even though the address was already known. A watch event is never
    /// staler than a point-in-time `get`, so keeping the existing entry
    /// is always at least as fresh.
    ///
    /// No `notify_waiters`: this either inserts an address-less entry
    /// (nothing for a waiter to act on) or leaves a fresher entry in
    /// place (whose insertion already woke waiters).
    pub fn seed_if_absent(&self, sandbox: &Sandbox) {
        let Some((namespace, id)) = cache_key(sandbox) else {
            return;
        };
        self.inner
            .map
            .entry(id)
            .or_default()
            .entry(namespace)
            .or_insert_with(|| CachedSandbox::from_sandbox(sandbox));
    }

    /// Drop a sandbox's entry (watcher `Delete` events).
    pub fn remove(&self, namespace: &str, sandbox_id: &str) {
        // Drop the namespace entry, then the id entry once no namespace is
        // left. `remove_if` re-checks emptiness under the shard lock, so a
        // concurrent `apply` for another namespace is never lost.
        if let Some(mut by_ns) = self.inner.map.get_mut(sandbox_id) {
            by_ns.remove(namespace);
        }
        self.inner
            .map
            .remove_if(sandbox_id, |_, by_ns| by_ns.is_empty());
    }

    /// Reconcile the cache to exactly the set in `sandboxes`
    /// (insert-then-retain; not transactional — a concurrent reader
    /// between the two passes may briefly observe a stale entry that is
    /// about to be evicted).
    ///
    /// Called on the watcher's `InitDone`: the watch stream re-lists on
    /// every (re)connect, and any sandbox that was deleted while the
    /// watch was down won't appear in the fresh list. Inserting the new
    /// set and then retaining only its keys drops those stale entries,
    /// preventing unbounded growth across reconnects. The transient
    /// overlap is harmless here — the worst case is one extra cache hit
    /// for a sandbox that's seconds from eviction.
    fn replace_all(&self, sandboxes: Vec<Sandbox>) {
        let mut fresh: HashMap<String, HashMap<String, CachedSandbox>> = HashMap::new();
        for sandbox in &sandboxes {
            if let Some((namespace, id)) = cache_key(sandbox) {
                fresh
                    .entry(id)
                    .or_default()
                    .insert(namespace, CachedSandbox::from_sandbox(sandbox));
            }
        }
        for (id, by_ns) in &fresh {
            self.inner.map.insert(id.clone(), by_ns.clone());
        }
        self.inner.map.retain(|id, _| fresh.contains_key(id));
        self.inner.notify.notify_waiters();
    }

    /// Park until the sandbox is resolvable or `deadline` elapses.
    ///
    /// Re-reads the cache on every wake and returns as soon as the
    /// sandbox is paused or its address appears. Registers interest on
    /// the shared [`Notify`] *before* reading the map (via
    /// [`tokio::sync::futures::Notified::enable`]) so a concurrent
    /// [`EnvdCache::apply`] landing between the read and the await cannot
    /// be missed — without that ordering a waiter could sleep the full
    /// deadline despite the address already being present.
    pub async fn wait_until_ready(
        &self,
        namespace: &str,
        sandbox_id: &str,
        deadline: Instant,
    ) -> WaitOutcome {
        // Remember the last-observed not-ready detail so a deadline expiry can
        // report *why* the sandbox never became ready, rather than a bare
        // timeout. Seeded so a sandbox that vanishes from the cache mid-wait
        // still yields a sensible (non-terminal) detail.
        let mut last = NotReady {
            sandbox_phase: SandboxPhase::default(),
            pod: None,
            failure_reason: None,
            terminal: false,
        };
        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            // Register on the notifier now, before the map read below.
            notified.as_mut().enable();

            if let Some(cached) = self.lookup(namespace, sandbox_id) {
                match cached.readiness() {
                    Readiness::Ready(addr) => return WaitOutcome::Ready(addr),
                    Readiness::Paused => return WaitOutcome::Paused,
                    // A terminal state won't clear by waiting — return now so
                    // the caller fails fast instead of parking to the deadline.
                    Readiness::NotReady(nr) if nr.terminal => return WaitOutcome::Terminal(nr),
                    Readiness::NotReady(nr) => last = nr,
                }
            }

            tokio::select! {
                _ = notified.as_mut() => continue,
                _ = tokio::time::sleep_until(deadline) => {
                    return WaitOutcome::TimedOut(last)
                }
            }
        }
    }
}

/// The `(namespace, name)` a sandbox is cached under, or `None` for an
/// unnamed object. A sandbox from a namespaced watch always carries its
/// namespace; one without (a hand-built test fixture) is cached under `""`.
fn cache_key(sandbox: &Sandbox) -> Option<(String, String)> {
    let id = sandbox.name_any();
    (!id.is_empty()).then(|| (sandbox.namespace().unwrap_or_default(), id))
}

impl Default for EnvdCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Drive a `Sandbox` watch stream into `cache` until the process exits.
///
/// The stream re-lists and replays on every (re)connect, so this loop
/// also handles the `Init` / `InitApply` / `InitDone` envelope to drop
/// entries for sandboxes deleted while the watch was down. Watcher
/// errors are logged and skipped — `kube::runtime::watcher` reconnects
/// internally, so a transient apiserver hiccup must not tear the cache
/// down (it would silently revert every reader to the apiserver-polling
/// fallback).
async fn run_watcher(cache: EnvdCache, api: Api<Sandbox>) {
    use watcher::Event;

    let mut stream = watcher(api, watcher::Config::default()).boxed();
    // Tracks the last successful watch event so the error arm can report
    // how long the cache has been staleness-degrading. A single `warn!`
    // per reconnect failure carries no signal that the stream is stuck in
    // a tight error loop (e.g. RBAC was revoked and every reconnect
    // 403s); the elapsed-since-last-event field lets an operator see the
    // staleness growing without waiting for the once-only `error!` on
    // permanent stream end.
    let mut last_event = Instant::now();
    // Buffers `InitApply` objects between `Init` and `InitDone` so the
    // post-reconnect set can be applied as a unit via `replace_all`
    // (insert-then-retain; see that method's note on the transient
    // overlap a concurrent reader may observe).
    let mut init_buf: Vec<Sandbox> = Vec::new();

    while let Some(event) = stream.next().await {
        match event {
            Ok(Event::Init) => init_buf.clear(),
            Ok(Event::InitApply(sandbox)) => init_buf.push(sandbox),
            Ok(Event::InitDone) => cache.replace_all(std::mem::take(&mut init_buf)),
            Ok(Event::Apply(sandbox)) => cache.apply(&sandbox),
            Ok(Event::Delete(sandbox)) => {
                if let Some((namespace, id)) = cache_key(&sandbox) {
                    cache.remove(&namespace, &id);
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    stale_secs = last_event.elapsed().as_secs(),
                    "sandbox watcher error; auto-restarting (cache staleness-degrading until recovery)"
                );
                // Don't refresh `last_event` on errors — its age is the
                // staleness signal.
                continue;
            }
        }
        last_event = Instant::now();
    }

    // The watcher stream is effectively infinite; reaching here means the
    // apiserver connection is unrecoverable. Surface it — readers fall
    // back to the apiserver `get` path, which is correct but chatty.
    tracing::error!("sandbox watcher stream ended; envd address cache is now stale");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Namespace every fixture sandbox lives in.
    const NS: &str = "team-a";
    use sandbawks_types::{ResourceQuantity, ResourceRequests, SandboxSpec, SandboxStatus};

    /// Build a `Sandbox` fixture with the given id, desired phase, and
    /// optional envd address. `Sandbox::new` stamps `metadata.name` from
    /// `id`, which is what [`EnvdCache::apply`] keys on.
    fn sandbox(id: &str, desired: Option<SandboxPhase>, addr: Option<&str>) -> Sandbox {
        let cpu = "1"
            .parse::<ResourceQuantity>()
            .expect("hardcoded resource quantity is valid");
        let memory = "512Mi"
            .parse::<ResourceQuantity>()
            .expect("hardcoded resource quantity is valid");
        let spec = SandboxSpec {
            template_ref: "tpl".to_owned(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu,
                memory,
                ephemeral_storage: None,
            },
            timeout_seconds: 60,
            auto_pause: false,
            env: Default::default(),
            metadata: Default::default(),
            network_policy: None,
            desired_phase: desired,
            snapshot_retention: None,
            pvc_retention: None,
            tolerations: None,
            node_selector: None,
            security_context: None,
            persistence_mode: None,
            volume_claim_templates: None,
            restore_from: None,
        };
        let mut sb = Sandbox::new(id, spec);
        sb.metadata.namespace = Some(NS.into());
        sb.status = addr.map(|a| SandboxStatus {
            envd_address: Some(a.to_owned()),
            ..Default::default()
        });
        sb
    }

    #[test]
    fn lookup_misses_on_empty_cache() {
        let cache = EnvdCache::new();
        assert_eq!(cache.lookup(NS, "sbx-1"), None);
    }

    #[test]
    fn apply_then_lookup_returns_projection() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox(
            "sbx-1",
            Some(SandboxPhase::Running),
            Some("10.0.0.1:49983"),
        ));
        let cached = cache
            .lookup(NS, "sbx-1")
            .expect("entry present after apply");
        assert_eq!(cached.desired_phase, Some(SandboxPhase::Running));
        assert_eq!(cached.envd_address.as_deref(), Some("10.0.0.1:49983"));
    }

    #[test]
    fn seed_if_absent_inserts_when_missing() {
        let cache = EnvdCache::new();
        cache.seed_if_absent(&sandbox("sbx-1", None, None));
        assert!(cache.lookup(NS, "sbx-1").is_some());
    }

    #[test]
    fn seed_if_absent_preserves_fresher_entry() {
        let cache = EnvdCache::new();
        // Simulate a watcher `Apply` with the address winning the race
        // against the disambiguating `get`.
        cache.apply(&sandbox("sbx-1", None, Some("10.0.0.1:49983")));
        // The stale `get` response (no address) must not clobber it.
        cache.seed_if_absent(&sandbox("sbx-1", None, None));
        assert_eq!(
            cache
                .lookup(NS, "sbx-1")
                .and_then(|c| c.envd_address)
                .as_deref(),
            Some("10.0.0.1:49983")
        );
    }

    #[test]
    fn remove_drops_entry() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox("sbx-1", None, Some("10.0.0.1:49983")));
        cache.remove(NS, "sbx-1");
        assert_eq!(cache.lookup(NS, "sbx-1"), None);
    }

    #[test]
    fn replace_all_drops_stale_entries() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox("sbx-stale", None, Some("10.0.0.9:1")));
        // Re-list after a reconnect that no longer contains `sbx-stale`.
        cache.replace_all(vec![sandbox("sbx-fresh", None, Some("10.0.0.1:2"))]);
        assert_eq!(cache.lookup(NS, "sbx-stale"), None);
        assert_eq!(
            cache
                .lookup(NS, "sbx-fresh")
                .and_then(|c| c.envd_address)
                .as_deref(),
            Some("10.0.0.1:2")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_returns_ready_when_address_already_present() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox("sbx-1", None, Some("10.0.0.1:49983")));
        let deadline = Instant::now() + std::time::Duration::from_secs(300);
        assert_eq!(
            cache.wait_until_ready(NS, "sbx-1", deadline).await,
            WaitOutcome::Ready("10.0.0.1:49983".to_owned())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_returns_paused_for_paused_sandbox() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox("sbx-1", Some(SandboxPhase::Paused), None));
        let deadline = Instant::now() + std::time::Duration::from_secs(300);
        assert_eq!(
            cache.wait_until_ready(NS, "sbx-1", deadline).await,
            WaitOutcome::Paused
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_times_out_when_address_never_arrives() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox("sbx-1", None, None));
        let deadline = Instant::now() + std::time::Duration::from_secs(300);
        // `start_paused` auto-advances time past the deadline once the
        // runtime is otherwise idle, so this resolves without a real wait.
        assert!(matches!(
            cache.wait_until_ready(NS, "sbx-1", deadline).await,
            WaitOutcome::TimedOut(_)
        ));
    }

    /// Build a `Sandbox` carrying a full `SandboxStatus` so tests can drive the
    /// not-ready classification (phase + projected pod status).
    fn sandbox_with_status(
        id: &str,
        desired: Option<SandboxPhase>,
        status: SandboxStatus,
    ) -> Sandbox {
        let mut sb = sandbox(id, desired, None);
        sb.status = Some(status);
        sb
    }

    fn pod(phase: &str, reason: Option<&str>) -> PodStatusSummary {
        PodStatusSummary {
            phase: phase.to_owned(),
            reason: reason.map(str::to_owned),
            message: None,
        }
    }

    #[test]
    fn readiness_ready_when_address_present() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox("sbx-1", None, Some("10.0.0.1:49983")));
        assert_eq!(
            cache.lookup(NS, "sbx-1").unwrap().readiness(),
            Readiness::Ready("10.0.0.1:49983".to_owned())
        );
    }

    #[test]
    fn readiness_paused_takes_priority_over_address() {
        // A paused sandbox is rejected even if a stale address lingers.
        let cache = EnvdCache::new();
        cache.apply(&sandbox(
            "sbx-1",
            Some(SandboxPhase::Paused),
            Some("10.0.0.1:49983"),
        ));
        assert_eq!(
            cache.lookup(NS, "sbx-1").unwrap().readiness(),
            Readiness::Paused
        );
    }

    #[test]
    fn readiness_resuming_is_transient_not_ready() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox_with_status(
            "sbx-1",
            Some(SandboxPhase::Running),
            SandboxStatus {
                phase: SandboxPhase::Resuming,
                pod_status: Some(pod("Pending", None)),
                ..Default::default()
            },
        ));
        let Readiness::NotReady(nr) = cache.lookup(NS, "sbx-1").unwrap().readiness() else {
            panic!("expected NotReady");
        };
        assert!(
            !nr.terminal,
            "a resuming sandbox is transient, not terminal"
        );
        assert_eq!(nr.sandbox_phase, SandboxPhase::Resuming);
        assert_eq!(nr.pod.unwrap().phase, "Pending");
    }

    #[test]
    fn readiness_failed_phase_is_terminal() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox_with_status(
            "sbx-1",
            Some(SandboxPhase::Running),
            SandboxStatus {
                phase: SandboxPhase::Failed,
                failure_reason: Some("resume_exhausted".to_owned()),
                ..Default::default()
            },
        ));
        let Readiness::NotReady(nr) = cache.lookup(NS, "sbx-1").unwrap().readiness() else {
            panic!("expected NotReady");
        };
        assert!(nr.terminal);
        assert_eq!(nr.failure_reason.as_deref(), Some("resume_exhausted"));
    }

    #[test]
    fn readiness_stuck_pod_reason_is_terminal() {
        // A `Creating` sandbox whose Pod is image-pull-backing-off is terminal
        // even though the *sandbox* phase is still the benign `Creating`.
        let cache = EnvdCache::new();
        cache.apply(&sandbox_with_status(
            "sbx-1",
            Some(SandboxPhase::Running),
            SandboxStatus {
                phase: SandboxPhase::Creating,
                pod_status: Some(pod("Pending", Some("ImagePullBackOff"))),
                ..Default::default()
            },
        ));
        let Readiness::NotReady(nr) = cache.lookup(NS, "sbx-1").unwrap().readiness() else {
            panic!("expected NotReady");
        };
        assert!(nr.terminal, "ImagePullBackOff must classify as terminal");
    }

    #[test]
    fn readiness_progressing_pod_reason_is_transient() {
        // `ContainerCreating` is a normal, progressing waiting reason.
        let cache = EnvdCache::new();
        cache.apply(&sandbox_with_status(
            "sbx-1",
            Some(SandboxPhase::Running),
            SandboxStatus {
                phase: SandboxPhase::Creating,
                pod_status: Some(pod("Pending", Some("ContainerCreating"))),
                ..Default::default()
            },
        ));
        let Readiness::NotReady(nr) = cache.lookup(NS, "sbx-1").unwrap().readiness() else {
            panic!("expected NotReady");
        };
        assert!(!nr.terminal, "ContainerCreating is transient, not terminal");
    }

    /// Build a transient [`NotReady`] for the silent-window classification
    /// tests (issue #649).
    fn transient_not_ready(phase: SandboxPhase, pod: Option<PodStatusSummary>) -> NotReady {
        NotReady {
            sandbox_phase: phase,
            pod,
            failure_reason: None,
            terminal: false,
        }
    }

    #[test]
    fn silent_window_pending_pod_no_reason_cannot_serve() {
        // Not yet scheduled (cold start waiting for a node): `Pending`, no
        // projected reason → emit the notice immediately (issue #649).
        let nr = transient_not_ready(SandboxPhase::Creating, Some(pod("Pending", None)));
        assert!(
            !nr.can_serve_within_silent_window(),
            "an unscheduled Pending Pod cannot serve within the silent window"
        );
    }

    #[test]
    fn silent_window_unschedulable_pod_cannot_serve() {
        // Capacity-`Pending` (`PodScheduled=False` → `Unschedulable`) → notice
        // now; still transient (a node may free up), not terminal.
        let nr = transient_not_ready(
            SandboxPhase::Creating,
            Some(pod("Pending", Some("Unschedulable"))),
        );
        assert!(!nr.can_serve_within_silent_window());
    }

    #[test]
    fn silent_window_container_creating_pod_can_serve() {
        // Scheduled and actively booting → p95-readies within the window, keep
        // the short park for transparent fast-start.
        let nr = transient_not_ready(
            SandboxPhase::Creating,
            Some(pod("Pending", Some("ContainerCreating"))),
        );
        assert!(nr.can_serve_within_silent_window());
    }

    #[test]
    fn silent_window_pod_initializing_can_serve() {
        let nr = transient_not_ready(
            SandboxPhase::Creating,
            Some(pod("Pending", Some("PodInitializing"))),
        );
        assert!(nr.can_serve_within_silent_window());
    }

    #[test]
    fn silent_window_running_pod_can_serve() {
        // A non-`Pending` Pod is moments from serving — short park.
        let nr = transient_not_ready(SandboxPhase::Creating, Some(pod("Running", None)));
        assert!(nr.can_serve_within_silent_window());
    }

    #[test]
    fn silent_window_resuming_phase_keeps_park_even_with_pending_pod() {
        // Transparent fast-resume guard: a Resuming sandbox keeps the short
        // park even though its restore Pod is briefly Pending with no reason.
        let nr = transient_not_ready(SandboxPhase::Resuming, Some(pod("Pending", None)));
        assert!(
            nr.can_serve_within_silent_window(),
            "a Resuming sandbox must keep the short park (no fast-resume regression)"
        );
    }

    #[test]
    fn silent_window_missing_pod_summary_keeps_park() {
        // No projected Pod summary at all (legacy controller / unobserved Pod):
        // preserve the historical short park — never more eager to bounce.
        let nr = transient_not_ready(SandboxPhase::Creating, None);
        assert!(nr.can_serve_within_silent_window());
    }

    #[tokio::test(start_paused = true)]
    async fn wait_returns_terminal_for_stuck_pod() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox_with_status(
            "sbx-1",
            Some(SandboxPhase::Running),
            SandboxStatus {
                phase: SandboxPhase::Creating,
                pod_status: Some(pod("Pending", Some("CrashLoopBackOff"))),
                ..Default::default()
            },
        ));
        let deadline = Instant::now() + std::time::Duration::from_secs(300);
        let outcome = cache.wait_until_ready(NS, "sbx-1", deadline).await;
        let WaitOutcome::Terminal(nr) = outcome else {
            panic!("expected Terminal, got {outcome:?}");
        };
        assert!(nr.terminal);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_times_out_carries_last_phase() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox_with_status(
            "sbx-1",
            Some(SandboxPhase::Running),
            SandboxStatus {
                phase: SandboxPhase::Resuming,
                ..Default::default()
            },
        ));
        let deadline = Instant::now() + std::time::Duration::from_secs(15);
        let WaitOutcome::TimedOut(nr) = cache.wait_until_ready(NS, "sbx-1", deadline).await else {
            panic!("expected TimedOut");
        };
        assert_eq!(nr.sandbox_phase, SandboxPhase::Resuming);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_wakes_when_address_arrives_after_parking() {
        let cache = EnvdCache::new();
        cache.apply(&sandbox("sbx-1", None, None));
        let waiter = cache.clone();
        let deadline = Instant::now() + std::time::Duration::from_secs(300);
        let handle =
            tokio::spawn(async move { waiter.wait_until_ready(NS, "sbx-1", deadline).await });

        // Yield so the waiter parks, then deliver the address.
        tokio::task::yield_now().await;
        cache.apply(&sandbox("sbx-1", None, Some("10.0.0.1:49983")));

        assert_eq!(
            handle.await.expect("waiter task joins"),
            WaitOutcome::Ready("10.0.0.1:49983".to_owned())
        );
    }

    /// Sandboxes are only unique per namespace (issue #13): two with the same
    /// name in different namespaces are cached, looked up and removed
    /// independently.
    #[test]
    fn same_name_in_two_namespaces_is_kept_apart() {
        let cache = EnvdCache::new();
        let mut other = sandbox("sbx-1", None, Some("10.0.0.2:49983"));
        other.metadata.namespace = Some("team-b".into());
        cache.apply(&sandbox("sbx-1", None, Some("10.0.0.1:49983")));
        cache.apply(&other);

        let mut namespaces = cache.namespaces_of("sbx-1");
        namespaces.sort();
        assert_eq!(namespaces, vec!["team-a".to_owned(), "team-b".to_owned()]);
        assert_eq!(
            cache.lookup(NS, "sbx-1").unwrap().readiness(),
            Readiness::Ready("10.0.0.1:49983".into())
        );
        assert_eq!(
            cache.lookup("team-b", "sbx-1").unwrap().readiness(),
            Readiness::Ready("10.0.0.2:49983".into())
        );
        assert_eq!(cache.lookup("team-c", "sbx-1"), None);

        cache.remove(NS, "sbx-1");
        assert_eq!(cache.lookup(NS, "sbx-1"), None);
        assert!(cache.lookup("team-b", "sbx-1").is_some());
        assert_eq!(cache.namespaces_of("sbx-1"), vec!["team-b".to_owned()]);

        cache.remove("team-b", "sbx-1");
        assert!(cache.namespaces_of("sbx-1").is_empty());
        assert!(
            cache.inner.map.is_empty(),
            "an id with no namespaces left is dropped"
        );
    }

    #[test]
    fn replace_all_keeps_every_namespace_of_a_name() {
        let cache = EnvdCache::new();
        let mut other = sandbox("sbx-1", None, None);
        other.metadata.namespace = Some("team-b".into());
        cache.replace_all(vec![sandbox("sbx-1", None, None), other]);
        assert_eq!(cache.namespaces_of("sbx-1").len(), 2);
    }
}
