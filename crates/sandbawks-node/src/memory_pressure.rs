//! Host-side memory-pressure reaction for gVisor sandboxes (#307).
//!
//! After #448 the sandbox's processes nest under the kubelet's *limited*
//! `kubepods.slice` pod cgroup, so `memory.current` / `memory.max` /
//! `memory.events` on that cgroup actually track and bind the sandbox. (Before
//! #448 they didn't: the limit sat on an empty cgroup while the sandbox ran
//! unbounded in `system.slice`, so there was nothing host-side to watch.) The
//! in-sandbox path is a dead end — gVisor's emulated guest cgroupfs is cosmetic
//! and exposes no PSI (#306) — so the only place to "catch" a sandbox
//! approaching its memory cap and save state before the OOM kill is the host,
//! from this DaemonSet.
//!
//! This module watches those pod cgroups and, when a sandbox crosses a
//! high-water fraction of its `memory.max`, (a) emits a metric + a structured
//! warning and (b) captures the sandbox's envd `/__state` to S3 before the
//! kernel reaps it.
//!
//! Design mirrors [`crate::headroom`]: the decision is a pure, unit-testable
//! [`PressurePolicy`] over a [`CgroupMemoryStats`] reading, and the real
//! `/sys/fs/cgroup` reads live behind the [`CgroupMemoryReader`] trait so tests
//! inject fixtures with no real cgroupfs.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::Client;

use crate::gvisor::{Clock, EnvdStateClient};
use crate::s3::BlobStore;

/// Default high-water fraction of `memory.max` at which a sandbox is treated as
/// "under pressure". `0.9` fires when `memory.current` reaches 90% of the cap —
/// late enough to avoid noise, early enough to capture state before the kill.
pub const DEFAULT_HIGH_WATER_FRACTION: f64 = 0.9;

/// Default watcher poll interval, in seconds.
pub const DEFAULT_INTERVAL_SECS: u64 = 5;

/// Host cgroup-v2 root the DaemonSet mounts read-only (the chart default).
pub const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// Label the controller stamps on every sandbox Pod, valued with the `Sandbox`
/// CR name. Used to scope the watcher's Pod list to sandboxes only. Sourced
/// from the shared [`sandbawks_types::SANDBOX_NAME_LABEL`] contract so it
/// cannot drift from the key `PodAdapter::create_pod` stamps.
const SANDBOX_LABEL: &str = sandbawks_types::SANDBOX_NAME_LABEL;

/// A memory reading for one sandbox's pod cgroup, parsed from the cgroup-v2
/// `memory.current` / `memory.max` / `memory.events` files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CgroupMemoryStats {
    /// `memory.current` — bytes currently charged to the cgroup.
    pub current: u64,
    /// `memory.max` — the hard cap, or `None` when the file reads `max`
    /// (unlimited). An unlimited cgroup can't be "approaching a limit", so the
    /// policy treats `None` as never-pressured.
    pub max: Option<u64>,
    /// `memory.events` `high` counter — times usage hit `memory.high`.
    pub events_high: u64,
    /// `memory.events` `max` counter — times an allocation was throttled at
    /// `memory.max` (a precursor to the OOM kill).
    pub events_max: u64,
    /// `memory.events` `oom_kill` counter — processes the cgroup OOM killer has
    /// reaped. Non-zero means a kill already happened.
    pub events_oom_kill: u64,
}

impl CgroupMemoryStats {
    /// `memory.current / memory.max` as a fraction, or `None` when the cgroup is
    /// unlimited (so there's no ratio to report).
    pub fn usage_fraction(&self) -> Option<f64> {
        match self.max {
            Some(max) if max > 0 => Some(self.current as f64 / max as f64),
            _ => None,
        }
    }
}

/// Pure, unit-testable memory-pressure policy.
///
/// A sandbox is "under pressure" when its cgroup has a real cap and either
/// `memory.current` has reached `high_water_fraction` of that cap, or the
/// kernel has already recorded a `max`-throttle or `oom_kill` event on it
/// (which can outrun a slow poll of `memory.current`).
#[derive(Debug, Clone, Copy)]
pub struct PressurePolicy {
    /// Fraction `(0.0, 1.0]` of `memory.max` at which to fire.
    pub high_water_fraction: f64,
}

impl Default for PressurePolicy {
    fn default() -> Self {
        Self {
            high_water_fraction: DEFAULT_HIGH_WATER_FRACTION,
        }
    }
}

impl PressurePolicy {
    /// Build a policy, validating `high_water_fraction ∈ (0.0, 1.0]`.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when the fraction is outside
    /// `(0.0, 1.0]` (or NaN — every comparison with NaN is false, so it fails
    /// the `0.0 <` half).
    pub fn try_new(high_water_fraction: f64) -> Result<Self, String> {
        if !(0.0 < high_water_fraction && high_water_fraction <= 1.0) {
            return Err(format!(
                "high_water_fraction must be in (0.0, 1.0], got {high_water_fraction}"
            ));
        }
        Ok(Self {
            high_water_fraction,
        })
    }

    /// The reason `stats` is "under pressure" — the most severe applicable —
    /// or `None` if it isn't (always `None` for an unlimited cgroup).
    ///
    /// Severity order is `oom_kill` > `events_max` (throttle) > `high_water`, so
    /// the reported `trigger` names the worst thing the kernel has done, not
    /// just that *some* threshold tripped.
    pub fn trigger(&self, stats: &CgroupMemoryStats) -> Option<PressureTrigger> {
        // `None` fraction => unlimited cgroup => never pressured.
        let frac = stats.usage_fraction()?;
        if stats.events_oom_kill > 0 {
            Some(PressureTrigger::OomKill)
        } else if stats.events_max > 0 {
            Some(PressureTrigger::EventsMax)
        } else if frac >= self.high_water_fraction {
            Some(PressureTrigger::HighWater)
        } else {
            None
        }
    }

    /// `true` if `stats` indicates the sandbox is approaching (or has hit) its
    /// memory cap. Always `false` for an unlimited cgroup.
    pub fn is_pressured(&self, stats: &CgroupMemoryStats) -> bool {
        self.trigger(stats).is_some()
    }
}

/// Why a sandbox tripped the pressure check — surfaced as the `trigger`
/// attribute on `memory_pressure.events_total` and in the warning, so a
/// downstream consumer can tell "merely near the cap" from "kernel already
/// throttling" from "OOM kill already happened" without scraping logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureTrigger {
    /// `memory.current` reached the high-water fraction of `memory.max`.
    HighWater,
    /// `memory.events` `max` counter is non-zero (an allocation was throttled
    /// at the hard cap — imminent OOM).
    EventsMax,
    /// `memory.events` `oom_kill` counter is non-zero (a process was reaped).
    OomKill,
}

impl PressureTrigger {
    /// Stable, lower-case metric/label string.
    pub fn as_str(self) -> &'static str {
        match self {
            PressureTrigger::HighWater => "high_water",
            PressureTrigger::EventsMax => "events_max",
            PressureTrigger::OomKill => "oom_kill",
        }
    }
}

/// Reads a sandbox pod cgroup's memory stats, behind a trait so the watcher is
/// unit-testable with fixtures instead of a real `/sys/fs/cgroup`.
pub trait CgroupMemoryReader: Send + Sync {
    /// Read the memory stats for `pod_uid`'s cgroup, or `None` when no cgroup
    /// dir is found for it (the pod isn't a gVisor sandbox nested under
    /// `kubepods.slice`, or it has already gone away).
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when a cgroup dir exists but its files
    /// can't be read/parsed.
    fn read(&self, pod_uid: &str) -> Result<Option<CgroupMemoryStats>, String>;
}

/// Production [`CgroupMemoryReader`]: reads the cgroup-v2 files under a mounted
/// host `/sys/fs/cgroup`.
#[derive(Debug, Clone)]
pub struct SysCgroupMemoryReader {
    cgroup_root: PathBuf,
}

impl SysCgroupMemoryReader {
    /// Build a reader rooted at the mounted host cgroup-v2 filesystem (the
    /// chart mounts `/sys/fs/cgroup`).
    pub fn new(cgroup_root: impl Into<PathBuf>) -> Self {
        Self {
            cgroup_root: cgroup_root.into(),
        }
    }
}

impl CgroupMemoryReader for SysCgroupMemoryReader {
    fn read(&self, pod_uid: &str) -> Result<Option<CgroupMemoryStats>, String> {
        let Some(dir) = pod_cgroup_dir(&self.cgroup_root, pod_uid) else {
            return Ok(None);
        };
        let current = read_u64_file(&dir.join("memory.current"))?;
        let max = read_memory_max(&dir.join("memory.max"))?;
        // memory.events is absent on some kernels/cgroups; treat a missing file
        // as all-zero counters rather than an error.
        let events = std::fs::read_to_string(dir.join("memory.events")).unwrap_or_default();
        let (events_high, events_max, events_oom_kill) = parse_memory_events(&events);
        Ok(Some(CgroupMemoryStats {
            current,
            max,
            events_high,
            events_max,
            events_oom_kill,
        }))
    }
}

/// Resolve the host cgroup-v2 directory for a sandbox pod, probing both kubelet
/// cgroup-driver layouts and returning the first candidate that exists.
///
/// Sandbox Pods are Guaranteed QoS (`requests == limits`, see `pod_adapter.rs`),
/// so the kubelet places them directly under the pods root; the burstable /
/// besteffort sub-trees are probed too so the reader still works if a sandbox is
/// ever non-Guaranteed. Both driver layouts are tried because the layout differs
/// by the kubelet's `--cgroup-driver`:
///
/// - **systemd** (k3s/most managed distros): `kubepods.slice/kubepods-pod<UID>.slice`
///   with UID dashes → underscores and a `.slice` suffix.
/// - **cgroupfs**: `kubepods/pod<UID>` with the raw dashed UID and no suffix.
///
/// Returns `None` when no candidate exists (not a sandbox cgroup, already torn
/// down, or — if *every* sandbox returns `None` — a driver layout this function
/// doesn't recognize; the watcher emits a one-shot diagnostic for that case).
///
/// `pub(crate)` so the sandbox-pressure data layer ([`crate::sandbox_stats`])
/// resolves the same pod cgroup dir without duplicating the dual-driver probe.
pub(crate) fn pod_cgroup_dir(root: &Path, pod_uid: &str) -> Option<PathBuf> {
    let slug = pod_uid.replace('-', "_");
    // systemd driver: kubepods.slice/[<qos>.slice/]kubepods[-<qos>]-pod<slug>.slice
    let systemd = root.join("kubepods.slice");
    // cgroupfs driver: kubepods/[<qos>/]pod<uid>
    let cgroupfs = root.join("kubepods");
    let candidates = [
        systemd.join(format!("kubepods-pod{slug}.slice")),
        systemd
            .join("kubepods-burstable.slice")
            .join(format!("kubepods-burstable-pod{slug}.slice")),
        systemd
            .join("kubepods-besteffort.slice")
            .join(format!("kubepods-besteffort-pod{slug}.slice")),
        cgroupfs.join(format!("pod{pod_uid}")),
        cgroupfs.join("burstable").join(format!("pod{pod_uid}")),
        cgroupfs.join("besteffort").join(format!("pod{pod_uid}")),
    ];
    candidates.into_iter().find(|p| p.is_dir())
}

/// Read a single-integer cgroup file (`memory.current`).
fn read_u64_file(path: &Path) -> Result<u64, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    s.trim()
        .parse()
        .map_err(|e| format!("parse {} ({:?}): {e}", path.display(), s.trim()))
}

/// Parse `memory.max`, mapping the literal `max` (unlimited) to `None`.
fn read_memory_max(path: &Path) -> Result<Option<u64>, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let t = s.trim();
    if t == "max" {
        Ok(None)
    } else {
        t.parse()
            .map(Some)
            .map_err(|e| format!("parse {} ({t:?}): {e}", path.display()))
    }
}

/// Parse the `high` / `max` / `oom_kill` counters out of a `memory.events`
/// body. Lines are `"<key> <count>"`; unknown keys are ignored and missing
/// keys default to `0`.
///
/// Split out as a pure function so it's unit-testable without a real cgroup.
fn parse_memory_events(body: &str) -> (u64, u64, u64) {
    let mut high = 0;
    let mut max = 0;
    let mut oom_kill = 0;
    for line in body.lines() {
        let mut it = line.split_whitespace();
        let (Some(key), Some(val)) = (it.next(), it.next()) else {
            continue;
        };
        let Ok(n) = val.parse::<u64>() else { continue };
        match key {
            "high" => high = n,
            "max" => max = n,
            "oom_kill" => oom_kill = n,
            _ => {}
        }
    }
    (high, max, oom_kill)
}

/// Static configuration for the memory-pressure watcher.
pub struct PressureWatcherConfig {
    /// This node's name — scopes the Pod list to local sandboxes.
    pub node_name: String,
    /// Namespace the sandbox Pods live in.
    pub namespace: String,
    /// Poll interval.
    pub interval: Duration,
    /// Pressure policy.
    pub policy: PressurePolicy,
    /// S3 key prefix under which pressure artifacts are written, as
    /// `<prefix>/memory-pressure/<sandbox>/<rfc3339>.json`.
    pub s3_key_prefix: String,
}

/// One captured sandbox's identity from the Pod list.
struct SandboxPod {
    uid: String,
    /// `Sandbox` CR name (label value, falling back to the Pod name).
    sandbox: String,
    pod_ip: Option<String>,
}

fn sandbox_pods(list: kube::core::ObjectList<Pod>) -> Vec<SandboxPod> {
    list.items
        .into_iter()
        .filter_map(|p| {
            let uid = p.metadata.uid.clone()?;
            // Prefer the Sandbox CR name (label), fall back to the Pod name, and
            // finally to the UID — never an empty string, which would render an
            // S3 key with an empty `//` path component (review #458).
            let sandbox = p
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(SANDBOX_LABEL).cloned())
                .filter(|s| !s.is_empty())
                .or_else(|| p.metadata.name.clone().filter(|s| !s.is_empty()))
                .unwrap_or_else(|| uid.clone());
            let pod_ip = p.status.as_ref().and_then(|s| s.pod_ip.clone());
            Some(SandboxPod {
                uid,
                sandbox,
                pod_ip,
            })
        })
        .collect()
}

/// Long-lived watcher loop. Polls every `interval`: lists the local sandbox
/// Pods, reads each one's pod-cgroup memory, records the node's max usage
/// ratio, and on a pressured sandbox emits a warning + (once per pressure
/// episode) captures its envd `/__state` to S3.
///
/// Best-effort by design: a transient kube/envd/S3 error is logged and the loop
/// continues — it never returns on a recoverable fault, so it stays a
/// never-terminating task like the node's other reconcilers.
pub async fn run_memory_pressure_watcher(
    client: Client,
    reader: Arc<dyn CgroupMemoryReader>,
    envd: Arc<dyn EnvdStateClient>,
    blob: Arc<dyn BlobStore>,
    clock: Arc<dyn Clock>,
    cfg: PressureWatcherConfig,
) -> anyhow::Result<()> {
    let pods: Api<Pod> = Api::namespaced(client, &cfg.namespace);
    // Field-selector narrows to local, Running pods server-side; the label
    // selector narrows to sandboxes.
    let lp = ListParams::default()
        .fields(&format!(
            "spec.nodeName={},status.phase=Running",
            cfg.node_name
        ))
        .labels(SANDBOX_LABEL);

    // Sandboxes currently in a pressure episode. We alert + capture only on the
    // *rising edge* (entering pressure), so a sandbox stuck above the line
    // doesn't spam warns / metrics / S3 uploads every poll. Cleared when its
    // pressure clears or it leaves the node. NB: this is in-process state, so a
    // node-pod restart re-captures each still-pressured sandbox once on the next
    // tick (timestamped keys avoid clobber) — acceptable for the prototype.
    let mut in_pressure: HashSet<String> = HashSet::new();
    // One-shot cgroup-driver diagnostic: if sandbox pods are present but *none*
    // resolve to a cgroup for this many consecutive polls, the kubelet cgroup
    // layout likely isn't one `pod_cgroup_dir` recognizes and the watcher is
    // silently a no-op — warn once so an operator isn't left staring at a flat
    // `node_max_fraction = 0` (review #458).
    const NO_CGROUP_WARN_AFTER_POLLS: u32 = 6;
    let mut polls_without_cgroups: u32 = 0;
    let mut warned_no_cgroups = false;

    tracing::info!(
        node = %cfg.node_name,
        interval_secs = cfg.interval.as_secs(),
        high_water_fraction = cfg.policy.high_water_fraction,
        "memory-pressure watcher started (#307)"
    );

    loop {
        tokio::time::sleep(cfg.interval).await;

        let list = match pods.list(&lp).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "memory-pressure: list sandbox pods failed; will retry");
                continue;
            }
        };
        let present = sandbox_pods(list);
        let mut seen: HashSet<String> = HashSet::with_capacity(present.len());
        let mut max_fraction_seen: f64 = 0.0;
        let mut any_cgroup_resolved = false;

        for pod in &present {
            seen.insert(pod.sandbox.clone());
            let stats = match reader.read(&pod.uid) {
                Ok(Some(s)) => {
                    any_cgroup_resolved = true;
                    s
                }
                Ok(None) => continue, // not a nested sandbox cgroup (or torn down)
                Err(e) => {
                    tracing::warn!(sandbox = %pod.sandbox, error = %e, "memory-pressure: cgroup read failed");
                    continue;
                }
            };
            if let Some(frac) = stats.usage_fraction() {
                max_fraction_seen = max_fraction_seen.max(frac);
            }

            match cfg.policy.trigger(&stats) {
                Some(trigger) => {
                    // Rising edge: first poll of this episode → alert + capture once.
                    if in_pressure.insert(pod.sandbox.clone()) {
                        tracing::warn!(
                            sandbox = %pod.sandbox,
                            trigger = trigger.as_str(),
                            memory_current = stats.current,
                            memory_max = ?stats.max,
                            usage_fraction = ?stats.usage_fraction(),
                            events_max = stats.events_max,
                            events_oom_kill = stats.events_oom_kill,
                            "memory-pressure: sandbox crossed its memory cap high-water line"
                        );
                        crate::memory_pressure_metrics::record_pressure(
                            &cfg.node_name,
                            trigger.as_str(),
                        );
                        capture_state(envd.as_ref(), blob.as_ref(), clock.as_ref(), &cfg, pod)
                            .await;
                    } else {
                        tracing::debug!(
                            sandbox = %pod.sandbox,
                            usage_fraction = ?stats.usage_fraction(),
                            "memory-pressure: sandbox still under pressure"
                        );
                    }
                }
                None => {
                    // Pressure cleared — re-arm for a future episode.
                    in_pressure.remove(&pod.sandbox);
                }
            }
        }

        // Forget sandboxes that have left the node.
        in_pressure.retain(|s| seen.contains(s));
        crate::memory_pressure_metrics::record_node_max_fraction(&cfg.node_name, max_fraction_seen);

        // cgroup-driver mismatch diagnostic (see NO_CGROUP_WARN_AFTER_POLLS).
        if !present.is_empty() && !any_cgroup_resolved {
            polls_without_cgroups = polls_without_cgroups.saturating_add(1);
            if polls_without_cgroups >= NO_CGROUP_WARN_AFTER_POLLS && !warned_no_cgroups {
                warned_no_cgroups = true;
                tracing::warn!(
                    node = %cfg.node_name,
                    sandbox_pods = present.len(),
                    polls = polls_without_cgroups,
                    "memory-pressure: sandbox pods present but no pod cgroup resolved — the kubelet \
                     cgroup driver layout may not be recognized (expected systemd 'kubepods.slice/...' \
                     or cgroupfs 'kubepods/...' under the mounted cgroup root); the watcher is a no-op \
                     until this is corrected"
                );
            }
        } else {
            polls_without_cgroups = 0;
            warned_no_cgroups = false;
        }
    }
}

/// Capture one pressured sandbox's envd `/__state` to S3. Best-effort: every
/// failure is logged + counted, never propagated (the watcher must not die on a
/// capture error).
async fn capture_state(
    envd: &dyn EnvdStateClient,
    blob: &dyn BlobStore,
    clock: &dyn Clock,
    cfg: &PressureWatcherConfig,
    pod: &SandboxPod,
) {
    let Some(pod_ip) = pod.pod_ip.as_deref().filter(|s| !s.is_empty()) else {
        tracing::warn!(sandbox = %pod.sandbox, "memory-pressure: no pod IP; cannot capture envd state");
        crate::memory_pressure_metrics::record_capture(&cfg.node_name, "no_pod_ip");
        return;
    };
    let state = match envd.fetch_state(pod_ip).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(sandbox = %pod.sandbox, error = %e, "memory-pressure: envd /__state fetch failed");
            crate::memory_pressure_metrics::record_capture(&cfg.node_name, "fetch_error");
            return;
        }
    };
    // `:` is awkward in S3 keys/paths; the RFC3339 timestamp uses it, so strip
    // to a filesystem/key-friendly form.
    let ts = clock.now_rfc3339().replace(':', "-");
    let key = format!(
        "{}/memory-pressure/{}/{}.json",
        cfg.s3_key_prefix.trim_end_matches('/'),
        pod.sandbox,
        ts
    );
    match blob.put(&key, state).await {
        Ok(()) => {
            tracing::info!(sandbox = %pod.sandbox, key = %key, "memory-pressure: captured envd state to S3");
            crate::memory_pressure_metrics::record_capture(&cfg.node_name, "ok");
        }
        Err(e) => {
            tracing::warn!(sandbox = %pod.sandbox, key = %key, error = %e, "memory-pressure: S3 upload failed");
            crate::memory_pressure_metrics::record_capture(&cfg.node_name, "upload_error");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    #[test]
    fn policy_rejects_invalid_fraction() {
        assert!(PressurePolicy::try_new(0.9).is_ok());
        assert!(PressurePolicy::try_new(1.0).is_ok());
        assert!(PressurePolicy::try_new(0.0).is_err());
        assert!(PressurePolicy::try_new(1.0001).is_err());
        assert!(PressurePolicy::try_new(-0.1).is_err());
        assert!(PressurePolicy::try_new(f64::NAN).is_err());
    }

    #[test]
    fn unlimited_cgroup_is_never_pressured() {
        let p = PressurePolicy::default();
        let s = CgroupMemoryStats {
            current: 100 * MIB,
            max: None,
            ..Default::default()
        };
        assert!(!p.is_pressured(&s));
        assert_eq!(s.usage_fraction(), None);
    }

    #[test]
    fn below_high_water_is_not_pressured_at_or_above_is() {
        let p = PressurePolicy::try_new(0.9).unwrap();
        // Round MiB multiples so the ratio is exact (no float-truncation slop):
        // a 100-MiB cap makes N MiB == N% of the cap.
        let cap = 100 * MIB;
        // 88% — below the 90% water line.
        let lo = CgroupMemoryStats {
            current: 88 * MIB,
            max: Some(cap),
            ..Default::default()
        };
        assert!(!p.is_pressured(&lo));
        // Exactly 90% — at the line, fires (inclusive).
        let at = CgroupMemoryStats {
            current: 90 * MIB,
            max: Some(cap),
            ..Default::default()
        };
        assert!(p.is_pressured(&at));
        // Above the line, too.
        let hi = CgroupMemoryStats {
            current: 95 * MIB,
            max: Some(cap),
            ..Default::default()
        };
        assert!(p.is_pressured(&hi));
    }

    #[test]
    fn max_throttle_or_oom_event_is_pressured_even_below_water() {
        let p = PressurePolicy::try_new(0.9).unwrap();
        let cap = 256 * MIB;
        let throttled = CgroupMemoryStats {
            current: 10 * MIB, // well below the water line...
            max: Some(cap),
            events_max: 3, // ...but the kernel already throttled at memory.max
            ..Default::default()
        };
        assert!(p.is_pressured(&throttled));
        let oomed = CgroupMemoryStats {
            current: 10 * MIB,
            max: Some(cap),
            events_oom_kill: 1,
            ..Default::default()
        };
        assert!(p.is_pressured(&oomed));
    }

    #[test]
    fn usage_fraction_is_current_over_max() {
        let s = CgroupMemoryStats {
            current: 128 * MIB,
            max: Some(256 * MIB),
            ..Default::default()
        };
        assert_eq!(s.usage_fraction(), Some(0.5));
    }

    #[test]
    fn parse_events_extracts_known_keys_ignores_rest() {
        let body = "low 0\nhigh 4\nmax 2\noom 1\noom_kill 1\noom_group_kill 0\n";
        assert_eq!(parse_memory_events(body), (4, 2, 1));
        // Missing keys default to zero; unknown lines are skipped.
        assert_eq!(parse_memory_events("low 5\n"), (0, 0, 0));
        assert_eq!(parse_memory_events(""), (0, 0, 0));
    }

    #[test]
    fn reader_reads_a_fixture_cgroup_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let uid = "d61481fc-f86e-4c44-8938-316d3d95e482";
        let slug = uid.replace('-', "_");
        let dir = tmp
            .path()
            .join("kubepods.slice")
            .join(format!("kubepods-pod{slug}.slice"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.current"), "240123904\n").unwrap();
        std::fs::write(dir.join("memory.max"), "268435456\n").unwrap();
        std::fs::write(
            dir.join("memory.events"),
            "low 0\nhigh 0\nmax 7\noom 0\noom_kill 0\n",
        )
        .unwrap();

        let reader = SysCgroupMemoryReader::new(tmp.path());
        let stats = reader.read(uid).unwrap().expect("fixture cgroup exists");
        assert_eq!(stats.current, 240_123_904);
        assert_eq!(stats.max, Some(268_435_456));
        assert_eq!(stats.events_max, 7);
        assert!(PressurePolicy::default().is_pressured(&stats));
    }

    #[test]
    fn reader_returns_none_for_unknown_pod() {
        let tmp = tempfile::tempdir().unwrap();
        let reader = SysCgroupMemoryReader::new(tmp.path());
        assert_eq!(
            reader.read("00000000-0000-0000-0000-000000000000").unwrap(),
            None
        );
    }

    #[test]
    fn reader_maps_max_to_unlimited() {
        let tmp = tempfile::tempdir().unwrap();
        let uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let slug = uid.replace('-', "_");
        let dir = tmp
            .path()
            .join("kubepods.slice")
            .join(format!("kubepods-pod{slug}.slice"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.current"), "1048576\n").unwrap();
        std::fs::write(dir.join("memory.max"), "max\n").unwrap();
        let reader = SysCgroupMemoryReader::new(tmp.path());
        let stats = reader.read(uid).unwrap().unwrap();
        assert_eq!(stats.max, None);
        assert!(!PressurePolicy::default().is_pressured(&stats));
    }

    #[test]
    fn trigger_reports_most_severe_reason() {
        let p = PressurePolicy::try_new(0.9).unwrap();
        let cap = 100 * MIB;
        // High-water only.
        let hw = CgroupMemoryStats {
            current: 95 * MIB,
            max: Some(cap),
            ..Default::default()
        };
        assert_eq!(p.trigger(&hw), Some(PressureTrigger::HighWater));
        // events_max outranks high_water.
        let em = CgroupMemoryStats {
            current: 95 * MIB,
            max: Some(cap),
            events_max: 1,
            ..Default::default()
        };
        assert_eq!(p.trigger(&em), Some(PressureTrigger::EventsMax));
        // oom_kill outranks everything.
        let ok = CgroupMemoryStats {
            current: 95 * MIB,
            max: Some(cap),
            events_max: 1,
            events_oom_kill: 1,
            ..Default::default()
        };
        assert_eq!(p.trigger(&ok), Some(PressureTrigger::OomKill));
        // Not pressured → None.
        let lo = CgroupMemoryStats {
            current: 10 * MIB,
            max: Some(cap),
            ..Default::default()
        };
        assert_eq!(p.trigger(&lo), None);
        assert_eq!(PressureTrigger::OomKill.as_str(), "oom_kill");
    }

    #[test]
    fn reader_resolves_cgroupfs_driver_layout() {
        // cgroupfs kubelet driver: kubepods/burstable/pod<UID> with raw dashes.
        let tmp = tempfile::tempdir().unwrap();
        let uid = "11111111-2222-3333-4444-555555555555";
        let dir = tmp
            .path()
            .join("kubepods")
            .join("burstable")
            .join(format!("pod{uid}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.current"), "5242880\n").unwrap();
        std::fs::write(dir.join("memory.max"), "10485760\n").unwrap();
        let reader = SysCgroupMemoryReader::new(tmp.path());
        let stats = reader
            .read(uid)
            .unwrap()
            .expect("cgroupfs-layout cgroup resolves");
        assert_eq!(stats.current, 5_242_880);
        assert_eq!(stats.max, Some(10_485_760));
    }
}
