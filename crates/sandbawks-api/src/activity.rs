//! Idle-timeout activity stamping (issue #481).
//!
//! The controller treats `spec.timeoutSeconds` as an *idle* window: a
//! `Running` sandbox is auto-paused / auto-terminated only once it has had
//! no client activity for the window, measured from
//! `status.lastActivityTimestamp`. This module is the gateway side of that
//! contract — it refreshes that stamp as proxied envd requests flow.
//!
//! The gateway is the natural seam: it proxies every per-sandbox call
//! (filesystem, process, exec/pty streams) and so sees every request. Two
//! things keep the stamp from becoming a write storm against the
//! apiserver:
//!
//! 1. **Throttling.** [`ActivityStamper`] records the last stamp instant
//!    per sandbox and suppresses a fresh patch until [`STAMP_THROTTLE`]
//!    has elapsed, so a sandbox hammered with requests still patches at
//!    most once per window.
//! 2. **Fire-and-forget.** The patch runs in a detached task so it never
//!    adds an apiserver round-trip to the request's latency, and a failed
//!    patch is a logged non-event (the next request retries).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use tokio::time::Instant;

use crate::AppState;

/// Minimum wall-clock gap between two `status.lastActivityTimestamp`
/// patches for the same sandbox.
///
/// Sized comfortably below any realistic idle window (the smallest
/// sensible `timeoutSeconds` is tens of seconds) so the stamp is never so
/// stale that the controller pauses a still-active sandbox, yet large
/// enough that a request-saturated sandbox patches at most ~6×/minute. It
/// also sits below the streaming keepalive cadence
/// (`PROC_KEEPALIVE_INTERVAL`, 15s) so a long-running but silent
/// exec/pty stream — which the user has asked be treated as *active* —
/// re-stamps on each keepalive frame and never idles out mid-stream.
///
/// The throttle is per-gateway-pod and in-memory, so with `N` replicas the
/// effective `status.lastActivityTimestamp` write rate is up to `N×` this
/// per-pod cap. That stays bounded and harmless — the writes are
/// last-write-wins of a monotonic timestamp — but it's worth knowing when
/// reasoning about apiserver write load under horizontal scaling.
const STAMP_THROTTLE: Duration = Duration::from_secs(10);

/// How often the background sweeper drops stale throttle entries, and the
/// age past which an entry is considered stale.
///
/// Sandbox ids are ephemeral (`sbx-<random>` per create), so without
/// eviction the throttle map would grow one permanent entry per sandbox the
/// gateway has ever proxied — an unbounded leak over the gateway's lifetime.
/// An entry not touched within this window carries no throttle value (the
/// next stamp for that sandbox is due regardless), so dropping it is always
/// safe; at worst a still-active sandbox re-creates its entry on the next
/// request. 60s keeps the map bounded to "sandboxes active in the last
/// minute" while sweeping cheaply.
const EVICTION_INTERVAL: Duration = Duration::from_secs(60);

/// Per-sandbox throttle for activity stamps. Cloned cheaply with
/// `AppState` (the backing [`DashMap`] is shared behind an `Arc`).
#[derive(Clone, Default)]
pub struct ActivityStamper {
    /// `sandbox_id -> instant of the last stamp we let through`.
    last: Arc<DashMap<String, Instant>>,
}

impl ActivityStamper {
    /// Build an empty stamper.
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn the background sweeper that bounds the throttle map (see
    /// [`EVICTION_INTERVAL`]). Call once at gateway startup; the task holds
    /// only a weak-equivalent `Arc` clone of the backing map and exits when
    /// no other clone remains. Not started in tests (the map stays empty).
    pub fn spawn_eviction(&self) {
        let last = Arc::clone(&self.last);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(EVICTION_INTERVAL);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                // If the gateway dropped its `ActivityStamper`, only this
                // task's clone remains — stop sweeping a map no one reads.
                if Arc::strong_count(&last) == 1 {
                    return;
                }
                Self::evict_stale(&last, Instant::now());
            }
        });
    }

    /// Drop throttle entries older than [`EVICTION_INTERVAL`]. Pure over an
    /// injected `now` so the sweep is unit-testable without the timer task.
    fn evict_stale(last: &DashMap<String, Instant>, now: Instant) {
        last.retain(|_, stamped| now.duration_since(*stamped) < EVICTION_INTERVAL);
    }

    /// Whether a stamp for `sandbox_id` is due, recording `now` as the
    /// latest let-through instant when it returns `true`.
    ///
    /// Atomic per key — the [`DashMap`] entry holds the shard lock across
    /// the read-and-update so two concurrent requests can't both observe a
    /// stale instant and double-patch.
    fn due(&self, sandbox_id: &str, now: Instant) -> bool {
        use dashmap::mapref::entry::Entry;
        match self.last.entry(sandbox_id.to_owned()) {
            Entry::Occupied(mut slot) => {
                if now.duration_since(*slot.get()) < STAMP_THROTTLE {
                    false
                } else {
                    slot.insert(now);
                    true
                }
            }
            Entry::Vacant(slot) => {
                slot.insert(now);
                true
            }
        }
    }
}

impl AppState {
    /// Record client activity against `sandbox_id`'s idle timeout (issue
    /// #481), best-effort.
    ///
    /// Refreshes `status.lastActivityTimestamp` to now so the controller's
    /// idle deadline is measured from this request, *if* the per-sandbox
    /// throttle ([`STAMP_THROTTLE`]) has elapsed since the last stamp.
    /// Never blocks the caller (the patch is detached) and never surfaces an
    /// error, so this is safe to call on the hot proxy path.
    ///
    /// The throttle slot advances when the stamp is *attempted*, not when it
    /// succeeds, so a failed patch is not retried until the next throttle
    /// window elapses (≤ [`STAMP_THROTTLE`]). That bound is far below any
    /// realistic idle window, so a single dropped stamp never causes a
    /// premature pause; the failure is logged at debug and self-heals on the
    /// next due request.
    pub fn note_activity(&self, sandbox_id: &str) {
        if !self.activity.due(sandbox_id, Instant::now()) {
            return;
        }
        let ops = Arc::clone(self.sandbox_ops());
        let namespace = Arc::clone(&self.default_namespace);
        let id = sandbox_id.to_owned();
        tokio::spawn(async move {
            let now = Utc::now().to_rfc3339();
            if let Err(err) = ops.stamp_activity(&namespace, &id, &now).await {
                // Debug, not warn: a transient apiserver hiccup here is
                // self-healing (the next request re-stamps) and would
                // otherwise be a noisy per-sandbox log line.
                tracing::debug!(sandbox = %id, error = %err, "activity stamp patch failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn throttle_suppresses_until_window_elapses() {
        let stamper = ActivityStamper::new();
        let t0 = Instant::now();

        // First stamp for a sandbox always lets through.
        assert!(stamper.due("sbx-1", t0), "first stamp must be due");
        // A second stamp inside the throttle window is suppressed.
        assert!(
            !stamper.due("sbx-1", t0 + STAMP_THROTTLE - Duration::from_millis(1)),
            "a stamp inside the throttle window must be suppressed"
        );
        // Exactly at the window boundary it is due again.
        assert!(
            stamper.due("sbx-1", t0 + STAMP_THROTTLE),
            "a stamp at the window boundary must be due"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_is_per_sandbox() {
        let stamper = ActivityStamper::new();
        let now = Instant::now();
        // Distinct sandboxes don't share a throttle slot.
        assert!(stamper.due("sbx-1", now));
        assert!(
            stamper.due("sbx-2", now),
            "a different sandbox must not be throttled by sbx-1's stamp"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn eviction_drops_only_stale_entries() {
        let stamper = ActivityStamper::new();
        let t0 = Instant::now();
        stamper.due("sbx-old", t0);
        // A fresh entry stamped just under the eviction window from the sweep.
        let now = t0 + EVICTION_INTERVAL;
        stamper.due("sbx-fresh", now - Duration::from_secs(1));

        ActivityStamper::evict_stale(&stamper.last, now);

        // `sbx-old` (aged exactly EVICTION_INTERVAL) is dropped; `sbx-fresh`
        // (aged 1s) survives — bounding the map to recently-active sandboxes.
        assert!(!stamper.last.contains_key("sbx-old"));
        assert!(stamper.last.contains_key("sbx-fresh"));
    }
}
