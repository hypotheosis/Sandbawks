//! Per-sandbox resource-pressure watcher (epic #472, phase 2 / issue #474).
//!
//! The behavioral core of the telemetry feature: an opt-in background loop that
//! polls every sandbox pod on this node, derives CPU / memory / disk / IO
//! pressure signals from the phase-1 readers ([`crate::sandbox_stats`] +
//! [`crate::memory_pressure::CgroupMemoryReader`]), runs a hysteresis episode
//! state machine, and emits structured `warn`/`info` logs. OTel gauges and Helm
//! plumbing are phase 3 (#475).
//!
//! Design (see `docs/superpowers/specs/2026-06-11-sandbox-pressure-telemetry-design.md`):
//! - **Host-side only.** gVisor lies to the guest about its limits, so the node
//!   DaemonSet reading the host pod cgroup is the only truthful vantage point.
//! - **Fail-open.** A missing signal (no CPU quota, no ephemeral limit, PID /
//!   upperdir unresolvable) is skipped for that pod, logged at most once; a
//!   per-pod read error never terminates the loop.
//! - **Hysteresis.** An episode opens when any signal crosses its warn level and
//!   closes only when *all* signals fall below `warn * clear_ratio`, so a value
//!   hovering at the boundary produces one bracketed episode, not a flap storm.
//! - **Pure core.** [`PressureThresholds`], [`EpisodeTracker`], [`FillEstimator`]
//!   and the rate helpers are pure and unit-tested; the loop only does I/O.
//!
//! The existing memory-pressure / OOM-capture watcher (#307,
//! [`crate::memory_pressure`]) is untouched — this is a sibling loop.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::Client;

use crate::memory_pressure::CgroupMemoryReader;
use crate::sandbox_stats::{
    dir_size_bytes, CgroupPressureReader, NetCounters, ProcNetReader, SandboxPidResolver,
    UpperdirResolver,
};

/// Label the controller stamps on every sandbox Pod (value = `Sandbox` CR name).
/// Sourced from the shared [`sandbawks_types::SANDBOX_NAME_LABEL`] contract so
/// it cannot drift from the key `PodAdapter::create_pod` stamps.
const SANDBOX_LABEL: &str = sandbawks_types::SANDBOX_NAME_LABEL;

/// Default check (poll) interval — how often signals are sampled.
pub const DEFAULT_CHECK_INTERVAL_MS: u64 = 1000;
/// Default report interval — how often a `warn` line is emitted while an episode
/// is open. Independent of the check interval (clamped to `>= check`).
pub const DEFAULT_REPORT_INTERVAL_MS: u64 = 1000;
/// Default interval between full upperdir walks that re-baseline the disk-fill
/// estimator.
pub const DEFAULT_WALK_INTERVAL_SECS: u64 = 15;
/// Default warn fraction for CPU utilization (of the cgroup's CPU quota).
pub const DEFAULT_CPU_WARN_FRACTION: f64 = 0.85;
/// Default warn fraction for memory usage (`memory.current / memory.max`).
pub const DEFAULT_MEM_WARN_FRACTION: f64 = 0.85;
/// Default warn fraction for disk fill (estimate / ephemeral-storage limit).
pub const DEFAULT_DISK_WARN_FRACTION: f64 = 0.80;
/// Default warn level for PSI `some avg10` stall percent (CPU / memory / IO).
pub const DEFAULT_PSI_WARN_PERCENT: f64 = 25.0;
/// Default clear ratio: a signal clears at `warn * clear_ratio`.
pub const DEFAULT_CLEAR_RATIO: f64 = 0.7;
/// Default host `/proc` root the network reader resolves PIDs against.
pub const DEFAULT_PROC_ROOT: &str = "/proc";

/// A signal that can open / sustain a pressure episode. Network rate is
/// deliberately *not* here — it is reported as context but never a trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureSignal {
    /// CPU utilization fraction of the cgroup's CPU quota.
    Cpu,
    /// `cpu.pressure` `some avg10`.
    CpuPsi,
    /// Memory usage fraction (`memory.current / memory.max`).
    Memory,
    /// `memory.pressure` `some avg10`.
    MemoryPsi,
    /// `io.pressure` `some avg10`.
    IoPsi,
    /// Disk fill fraction of the pod's ephemeral-storage limit.
    Disk,
}

impl PressureSignal {
    /// Stable, lower-case label for logs.
    pub fn as_str(self) -> &'static str {
        match self {
            PressureSignal::Cpu => "cpu",
            PressureSignal::CpuPsi => "cpu_psi",
            PressureSignal::Memory => "memory",
            PressureSignal::MemoryPsi => "memory_psi",
            PressureSignal::IoPsi => "io_psi",
            PressureSignal::Disk => "disk",
        }
    }
}

/// Render an ordered signal set as a stable comma-joined string for logs.
fn join_signals(sigs: &BTreeSet<PressureSignal>) -> String {
    sigs.iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

/// One tick's computed signal values for a single sandbox. Each is `None` when
/// the signal is unavailable (no quota / no limit / reader returned nothing),
/// which the threshold check treats as "not contributing".
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct PressureSample {
    /// CPU utilization fraction (`Δusage / (Δwall × allowed_cores)`).
    pub cpu_fraction: Option<f64>,
    /// `cpu.pressure` `some avg10`.
    pub cpu_psi: Option<f64>,
    /// Memory usage fraction.
    pub mem_fraction: Option<f64>,
    /// `memory.pressure` `some avg10`.
    pub mem_psi: Option<f64>,
    /// `io.pressure` `some avg10`.
    pub io_psi: Option<f64>,
    /// Disk fill fraction.
    pub disk_fraction: Option<f64>,
    /// Network receive rate, bytes/sec (context only).
    pub net_rx_bps: Option<f64>,
    /// Network transmit rate, bytes/sec (context only).
    pub net_tx_bps: Option<f64>,
}

impl PressureSample {
    /// `(signal, value, warn_level)` for each threshold signal that has a value
    /// this tick. The warn level is the absolute comparison point; the clear
    /// level is derived as `warn_level * clear_ratio` by the caller.
    fn signal_levels(&self, t: &PressureThresholds) -> Vec<(PressureSignal, f64, f64)> {
        let mut out = Vec::with_capacity(6);
        if let Some(v) = self.cpu_fraction {
            out.push((PressureSignal::Cpu, v, t.cpu_warn_fraction));
        }
        if let Some(v) = self.cpu_psi {
            out.push((PressureSignal::CpuPsi, v, t.psi_warn_percent));
        }
        if let Some(v) = self.mem_fraction {
            out.push((PressureSignal::Memory, v, t.mem_warn_fraction));
        }
        if let Some(v) = self.mem_psi {
            out.push((PressureSignal::MemoryPsi, v, t.psi_warn_percent));
        }
        if let Some(v) = self.io_psi {
            out.push((PressureSignal::IoPsi, v, t.psi_warn_percent));
        }
        if let Some(v) = self.disk_fraction {
            out.push((PressureSignal::Disk, v, t.disk_warn_fraction));
        }
        out
    }

    /// Signals at or above their warn level this tick.
    fn warned(&self, t: &PressureThresholds) -> BTreeSet<PressureSignal> {
        self.signal_levels(t)
            .into_iter()
            .filter(|(_, v, warn)| *v >= *warn)
            .map(|(s, _, _)| s)
            .collect()
    }

    /// Signals at or above their *clear* level (`warn × clear_ratio`) this tick.
    /// An episode stays open while this set is non-empty.
    fn above_clear(&self, t: &PressureThresholds) -> BTreeSet<PressureSignal> {
        self.signal_levels(t)
            .into_iter()
            .filter(|(_, v, warn)| *v >= *warn * t.clear_ratio)
            .map(|(s, _, _)| s)
            .collect()
    }
}

/// Pure, validated threshold policy. The clear level for every signal is derived
/// as `warn × clear_ratio` rather than configured separately, which guarantees
/// `clear < warn` (no inverted band) by construction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PressureThresholds {
    /// CPU utilization warn fraction, `(0.0, 1.0]`.
    pub cpu_warn_fraction: f64,
    /// Memory usage warn fraction, `(0.0, 1.0]`.
    pub mem_warn_fraction: f64,
    /// Disk fill warn fraction, `(0.0, 1.0]`.
    pub disk_warn_fraction: f64,
    /// PSI `some avg10` warn percent, `(0.0, 100.0]`.
    pub psi_warn_percent: f64,
    /// Clear ratio, `(0.0, 1.0)` — strictly below 1 so an episode can clear.
    pub clear_ratio: f64,
}

impl Default for PressureThresholds {
    fn default() -> Self {
        Self {
            cpu_warn_fraction: DEFAULT_CPU_WARN_FRACTION,
            mem_warn_fraction: DEFAULT_MEM_WARN_FRACTION,
            disk_warn_fraction: DEFAULT_DISK_WARN_FRACTION,
            psi_warn_percent: DEFAULT_PSI_WARN_PERCENT,
            clear_ratio: DEFAULT_CLEAR_RATIO,
        }
    }
}

impl PressureThresholds {
    /// Validate every bound up front so a misconfiguration fails at boot rather
    /// than silently disabling a signal.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when any fraction is outside `(0.0, 1.0]`,
    /// the PSI percent is outside `(0.0, 100.0]`, or the clear ratio is outside
    /// `(0.0, 1.0)`.
    pub fn try_new(
        cpu_warn_fraction: f64,
        mem_warn_fraction: f64,
        disk_warn_fraction: f64,
        psi_warn_percent: f64,
        clear_ratio: f64,
    ) -> Result<Self, String> {
        for (name, v) in [
            ("cpu_warn_fraction", cpu_warn_fraction),
            ("mem_warn_fraction", mem_warn_fraction),
            ("disk_warn_fraction", disk_warn_fraction),
        ] {
            if !(0.0 < v && v <= 1.0) {
                return Err(format!("{name} must be in (0.0, 1.0], got {v}"));
            }
        }
        if !(0.0 < psi_warn_percent && psi_warn_percent <= 100.0) {
            return Err(format!(
                "psi_warn_percent must be in (0.0, 100.0], got {psi_warn_percent}"
            ));
        }
        // Strictly < 1.0: a clear ratio of 1.0 would make clear == warn, so a
        // signal pinned exactly at warn could never clear.
        if !(0.0 < clear_ratio && clear_ratio < 1.0) {
            return Err(format!(
                "clear_ratio must be in (0.0, 1.0), got {clear_ratio}"
            ));
        }
        Ok(Self {
            cpu_warn_fraction,
            mem_warn_fraction,
            disk_warn_fraction,
            psi_warn_percent,
            clear_ratio,
        })
    }
}

/// Peak value seen for each signal over the life of an episode, for the
/// single summary line emitted when it clears.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Peaks {
    /// Highest CPU utilization fraction.
    pub cpu_fraction: Option<f64>,
    /// Highest `cpu.pressure` `some avg10`.
    pub cpu_psi: Option<f64>,
    /// Highest memory usage fraction.
    pub mem_fraction: Option<f64>,
    /// Highest `memory.pressure` `some avg10`.
    pub mem_psi: Option<f64>,
    /// Highest `io.pressure` `some avg10`.
    pub io_psi: Option<f64>,
    /// Highest disk fill fraction.
    pub disk_fraction: Option<f64>,
    /// Highest network receive rate (bytes/sec).
    pub net_rx_bps: Option<f64>,
    /// Highest network transmit rate (bytes/sec).
    pub net_tx_bps: Option<f64>,
}

/// `a.max(b)` lifting `None` (no value yet) to the other operand.
fn max_opt(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

impl Peaks {
    /// Fold one sample's values into the running peaks.
    fn observe(&mut self, s: &PressureSample) {
        self.cpu_fraction = max_opt(self.cpu_fraction, s.cpu_fraction);
        self.cpu_psi = max_opt(self.cpu_psi, s.cpu_psi);
        self.mem_fraction = max_opt(self.mem_fraction, s.mem_fraction);
        self.mem_psi = max_opt(self.mem_psi, s.mem_psi);
        self.io_psi = max_opt(self.io_psi, s.io_psi);
        self.disk_fraction = max_opt(self.disk_fraction, s.disk_fraction);
        self.net_rx_bps = max_opt(self.net_rx_bps, s.net_rx_bps);
        self.net_tx_bps = max_opt(self.net_tx_bps, s.net_tx_bps);
    }
}

/// What the loop should do with a sandbox this tick, per the hysteresis machine.
#[derive(Debug, Clone, PartialEq)]
pub enum EpisodeAction {
    /// Nothing to emit (no pressure, or an open episode between report ticks).
    None,
    /// Emit a `warn` line. An open episode emits one every report interval for
    /// its whole duration, so a tailing operator sees continuous output.
    Warn {
        /// True on the episode's opening tick.
        first: bool,
        /// Signals currently at/above their warn level. Can be empty while the
        /// episode rides the hysteresis band (above clear, below warn).
        over_warn: BTreeSet<PressureSignal>,
        /// Signals currently at/above their clear level — what keeps the episode
        /// open. Non-empty for the entire episode by construction.
        elevated: BTreeSet<PressureSignal>,
    },
    /// Episode just closed — emit one `info` summary line.
    Clear {
        /// Ticks the episode was open (× check interval ≈ wall duration).
        duration_ticks: u32,
        /// Per-signal peaks over the episode.
        peaks: Peaks,
        /// Union of all signals that warned during the episode.
        triggered: BTreeSet<PressureSignal>,
    },
}

/// Per-sandbox hysteresis state machine. Pure and tick-driven (no clock), so the
/// open → sustain → clear behavior is unit-testable without real time.
#[derive(Debug, Clone)]
pub struct EpisodeTracker {
    thresholds: PressureThresholds,
    /// Emit a `warn` every Nth tick while open (`report_interval / check`).
    report_every: u32,
    open: Option<OpenEpisode>,
}

#[derive(Debug, Clone)]
struct OpenEpisode {
    ticks_open: u32,
    ticks_since_report: u32,
    peaks: Peaks,
    triggered: BTreeSet<PressureSignal>,
}

impl EpisodeTracker {
    /// Build a tracker. `report_every` is clamped to `>= 1` (0 would emit on no
    /// tick / divide-by-zero).
    pub fn new(thresholds: PressureThresholds, report_every: u32) -> Self {
        Self {
            thresholds,
            report_every: report_every.max(1),
            open: None,
        }
    }

    /// Whether an episode is currently open (test/observability helper).
    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// Feed one tick's sample, advancing the state machine and returning the
    /// action the caller should take (log a warn, log a clear, or nothing).
    pub fn observe(&mut self, sample: &PressureSample) -> EpisodeAction {
        let warned = sample.warned(&self.thresholds);
        let elevated = sample.above_clear(&self.thresholds);
        match &mut self.open {
            None => {
                if warned.is_empty() {
                    EpisodeAction::None
                } else {
                    // Rising edge: open the episode and emit the first warn.
                    let mut peaks = Peaks::default();
                    peaks.observe(sample);
                    self.open = Some(OpenEpisode {
                        ticks_open: 0,
                        ticks_since_report: 0,
                        peaks,
                        triggered: warned.clone(),
                    });
                    EpisodeAction::Warn {
                        first: true,
                        over_warn: warned,
                        elevated,
                    }
                }
            }
            Some(ep) => {
                // Stays open while ANY signal is above its clear level.
                if elevated.is_empty() {
                    let closed = self.open.take().expect("open episode present");
                    EpisodeAction::Clear {
                        duration_ticks: closed.ticks_open,
                        peaks: closed.peaks,
                        triggered: closed.triggered,
                    }
                } else {
                    ep.ticks_open = ep.ticks_open.saturating_add(1);
                    ep.peaks.observe(sample);
                    ep.triggered.extend(warned.iter().copied());
                    ep.ticks_since_report = ep.ticks_since_report.saturating_add(1);
                    if ep.ticks_since_report >= self.report_every {
                        ep.ticks_since_report = 0;
                        EpisodeAction::Warn {
                            first: false,
                            over_warn: warned,
                            elevated,
                        }
                    } else {
                        EpisodeAction::None
                    }
                }
            }
        }
    }
}

/// Disk-fill estimator: tracks a cheap per-tick estimate
/// (`last_walk_bytes + Δwbytes_since_walk`) corrected by a real upperdir walk
/// every `walk_every` ticks, and immediately on the first warn crossing.
///
/// The estimate deliberately overestimates (overwrites and deleted temp files
/// still count as written bytes), erring toward a warning — the conservative
/// choice for a telemetry signal whose purpose is catching a sudden balloon.
#[derive(Debug, Clone, Default)]
pub struct FillEstimator {
    last_walk_bytes: u64,
    wbytes_at_walk: u64,
    ticks_since_walk: u32,
    walked_once: bool,
    /// Whether last tick's fraction was at/above warn — latches so a sustained
    /// over-warn estimate doesn't force a confirming walk every tick.
    over_warn: bool,
}

impl FillEstimator {
    /// Current fill estimate given the latest cumulative `io.stat` wbytes.
    pub fn estimate(&self, cur_wbytes: u64) -> u64 {
        self.last_walk_bytes
            .saturating_add(cur_wbytes.saturating_sub(self.wbytes_at_walk))
    }

    /// Whether a real upperdir walk should run this tick: on the first tick,
    /// every `walk_every` ticks, on an io-counter regression (cgroup recreated),
    /// or on the rising edge of the estimate crossing the warn line.
    pub fn should_walk(
        &self,
        cur_wbytes: u64,
        walk_every: u32,
        limit: Option<u64>,
        warn_fraction: f64,
    ) -> bool {
        if !self.walked_once || self.ticks_since_walk >= walk_every {
            return true;
        }
        if cur_wbytes < self.wbytes_at_walk {
            return true; // counter reset — rebaseline
        }
        if !self.over_warn {
            if let Some(lim) = limit {
                if lim > 0 && self.estimate(cur_wbytes) as f64 / lim as f64 >= warn_fraction {
                    return true; // first crossing — confirm with an exact walk
                }
            }
        }
        false
    }

    /// Record a completed walk: `measured_bytes` is the exact upperdir size,
    /// `cur_wbytes` the io.stat reading at walk time (the new delta baseline).
    pub fn record_walk(&mut self, measured_bytes: u64, cur_wbytes: u64) {
        self.last_walk_bytes = measured_bytes;
        self.wbytes_at_walk = cur_wbytes;
        self.ticks_since_walk = 0;
        self.walked_once = true;
    }

    /// Record that a due walk was attempted but could not complete (upperdir
    /// unresolvable). Resets the periodic timer so the next attempt waits a full
    /// `walk_every` instead of hammering containerd every tick; the estimate
    /// keeps running off io.stat from the current baseline (degraded but bounded).
    ///
    /// The `over_warn` latch is deliberately left untouched: if the estimate had
    /// already crossed warn, a permanently-unresolvable upperdir keeps the
    /// first-crossing confirming walk suppressed (the latch's purpose) rather
    /// than re-attempting every tick — the same bounded-degradation contract.
    pub fn note_failed_walk(&mut self) {
        self.ticks_since_walk = 0;
        self.walked_once = true;
    }

    /// Advance one tick without walking.
    pub fn tick(&mut self) {
        self.ticks_since_walk = self.ticks_since_walk.saturating_add(1);
    }

    /// Compute `(fill_bytes, fraction)` for this tick and latch the over-warn
    /// state used to suppress repeated confirming walks. Call once per tick,
    /// after any walk/tick bookkeeping.
    pub fn fraction(
        &mut self,
        cur_wbytes: u64,
        limit: Option<u64>,
        warn_fraction: f64,
    ) -> (u64, Option<f64>) {
        let fill = self.estimate(cur_wbytes);
        let frac = match limit {
            Some(lim) if lim > 0 => Some(fill as f64 / lim as f64),
            _ => None,
        };
        self.over_warn = frac.is_some_and(|f| f >= warn_fraction);
        (fill, frac)
    }
}

/// CPU utilization fraction over an interval: `(Δusage_usec / 1e6) / (Δwall × cores)`.
///
/// Returns `None` on a counter regression (cgroup recreated — `cur < prev`) or a
/// non-positive interval / core count, so the caller resets its baseline and
/// skips the signal for that tick rather than reporting a bogus spike. Not
/// clamped to 1.0: a brief reading above 100% (burst over quota within the
/// accounting window) is reported as-is.
pub fn cpu_utilization(
    prev_usage_usec: u64,
    cur_usage_usec: u64,
    elapsed_secs: f64,
    allowed_cores: f64,
) -> Option<f64> {
    if cur_usage_usec < prev_usage_usec || elapsed_secs <= 0.0 || allowed_cores <= 0.0 {
        return None;
    }
    let used_secs = (cur_usage_usec - prev_usage_usec) as f64 / 1_000_000.0;
    Some(used_secs / (elapsed_secs * allowed_cores))
}

/// Per-second rate of a cumulative counter over an interval. `None` on a
/// regression (`cur < prev`) or a non-positive interval.
pub fn delta_rate(prev: u64, cur: u64, elapsed_secs: f64) -> Option<f64> {
    if cur < prev || elapsed_secs <= 0.0 {
        return None;
    }
    Some((cur - prev) as f64 / elapsed_secs)
}

/// Parse a Kubernetes resource quantity (e.g. `"256Mi"`, `"1Gi"`, `"512M"`,
/// `"1073741824"`) into bytes. Supports binary (`Ki/Mi/Gi/Ti/Pi`) and decimal
/// (`K/M/G/T/P` and SI `k`) suffixes; a bare number is bytes. Returns `None`
/// for an unparseable or negative value.
///
/// Used for the pod's ephemeral-storage limit (a `k8s_openapi` `Quantity`, which
/// is a verbatim string). Millis (`m`) are meaningless for a byte limit and
/// rejected.
pub fn parse_quantity_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s
        .char_indices()
        .find(|(_, c)| !c.is_ascii_digit() && *c != '.')
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: f64 = num.parse().ok()?;
    if value < 0.0 {
        return None;
    }
    let multiplier: f64 = match unit {
        "" => 1.0,
        "Ki" => 1024.0,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "k" | "K" => 1_000.0,
        "M" => 1_000f64.powi(2),
        "G" => 1_000f64.powi(3),
        "T" => 1_000f64.powi(4),
        "P" => 1_000f64.powi(5),
        _ => return None,
    };
    Some((value * multiplier) as u64)
}

/// Static configuration for the sandbox-pressure watcher.
pub struct SandboxPressureConfig {
    /// This node's name — scopes the Pod list and labels the logs.
    pub node_name: String,
    /// Namespace the sandbox Pods live in.
    pub namespace: String,
    /// Poll interval.
    pub check_interval: Duration,
    /// Emit a warn every Nth tick while an episode is open (derived from the
    /// report interval; clamped `>= 1`).
    pub report_every: u32,
    /// Ticks between full upperdir walks (derived from the walk interval;
    /// clamped `>= 1`).
    pub walk_every: u32,
    /// Threshold policy.
    pub thresholds: PressureThresholds,
    /// Pod network interface to read counters for.
    pub net_iface: String,
}

impl SandboxPressureConfig {
    /// Derive `report_every` / `walk_every` tick counts from wall-clock
    /// intervals, clamping the report interval up to the check interval (you
    /// can't report more often than you sample).
    pub fn from_intervals(
        node_name: String,
        namespace: String,
        check_interval: Duration,
        report_interval: Duration,
        walk_interval: Duration,
        thresholds: PressureThresholds,
        net_iface: String,
    ) -> Self {
        let check_ms = check_interval.as_millis().max(1);
        let report_every = (report_interval.as_millis().max(1))
            .div_ceil(check_ms)
            .max(1) as u32;
        let walk_ms = walk_interval.as_millis().max(1);
        let walk_every = walk_ms.div_ceil(check_ms).max(1) as u32;
        Self {
            node_name,
            namespace,
            check_interval,
            report_every,
            walk_every,
            thresholds,
            net_iface,
        }
    }
}

/// One sandbox Pod's identity from the Pod list, with the bits the watcher needs.
struct SandboxPod {
    uid: String,
    sandbox: String,
    container_id: Option<String>,
    /// Pod ephemeral-storage limit in bytes (sum of container limits), if set.
    ephemeral_limit_bytes: Option<u64>,
}

/// Mutable per-sandbox state carried across ticks.
struct PodState {
    sandbox: String,
    last_sample_at: Option<Instant>,
    prev_cpu_usage_usec: Option<u64>,
    prev_net: Option<NetCounters>,
    /// Cached container init PID (stable for a container's life; re-resolved
    /// when the `/proc` entry disappears).
    pid: Option<u32>,
    fill: FillEstimator,
    episode: EpisodeTracker,
    /// Signals for which a resolution failure has already been logged, so the
    /// warning fires once per pod per failure kind, not every tick.
    warned_failures: HashSet<&'static str>,
}

impl PodState {
    fn new(sandbox: String, thresholds: PressureThresholds, report_every: u32) -> Self {
        Self {
            sandbox,
            last_sample_at: None,
            prev_cpu_usage_usec: None,
            prev_net: None,
            pid: None,
            fill: FillEstimator::default(),
            episode: EpisodeTracker::new(thresholds, report_every),
            warned_failures: HashSet::new(),
        }
    }

    /// Log `msg` at most once per `kind` until that kind next succeeds.
    fn warn_once(&mut self, kind: &'static str, msg: &str) {
        if self.warned_failures.insert(kind) {
            tracing::warn!(sandbox = %self.sandbox, "{msg}");
        }
    }

    /// Re-arm `warn_once` for `kind` after a successful read, so a signal that
    /// recovers and then fails again logs the second failure instead of staying
    /// silent for the pod's whole lifetime (#478 review).
    fn clear_failure(&mut self, kind: &'static str) {
        self.warned_failures.remove(kind);
    }
}

/// Build the watcher's view of the local sandbox Pods from a Pod list.
fn sandbox_pods(list: kube::core::ObjectList<Pod>) -> Vec<SandboxPod> {
    list.items
        .into_iter()
        .filter_map(|p| {
            let uid = p.metadata.uid.clone()?;
            let sandbox = p
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(SANDBOX_LABEL).cloned())
                .filter(|s| !s.is_empty())
                .or_else(|| p.metadata.name.clone().filter(|s| !s.is_empty()))
                .unwrap_or_else(|| uid.clone());
            let container_id = p
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_ref())
                .and_then(|cs| cs.first())
                .and_then(|c| c.container_id.clone())
                .map(|id| id.strip_prefix("containerd://").unwrap_or(&id).to_string());
            let ephemeral_limit_bytes = pod_ephemeral_limit_bytes(&p);
            Some(SandboxPod {
                uid,
                sandbox,
                container_id,
                ephemeral_limit_bytes,
            })
        })
        .collect()
}

/// Sum the `ephemeral-storage` limits across a Pod's containers, in bytes.
/// `None` when no container declares one (so disk fill has no denominator and
/// the signal is skipped — fail-open).
fn pod_ephemeral_limit_bytes(pod: &Pod) -> Option<u64> {
    let containers = &pod.spec.as_ref()?.containers;
    let mut total: u64 = 0;
    let mut any = false;
    for c in containers {
        if let Some(lim) = c
            .resources
            .as_ref()
            .and_then(|r| r.limits.as_ref())
            .and_then(|l| l.get("ephemeral-storage"))
            .and_then(|q| parse_quantity_bytes(&q.0))
        {
            total = total.saturating_add(lim);
            any = true;
        }
    }
    any.then_some(total)
}

/// Long-lived watcher loop (epic #472 phase 2). Polls every `check_interval`:
/// lists local sandbox Pods, samples each one's pressure signals, and drives the
/// per-sandbox hysteresis machine, emitting `warn`/`info` logs.
///
/// Best-effort by design: every transient kube / cgroup / containerd / `/proc`
/// error is logged (once per pod for resolution failures) and the loop
/// continues — it never returns on a recoverable fault.
#[allow(clippy::too_many_arguments)]
pub async fn run_sandbox_pressure_watcher(
    client: Client,
    cgroup: Arc<dyn CgroupPressureReader>,
    memory: Arc<dyn CgroupMemoryReader>,
    net: Arc<dyn ProcNetReader>,
    pids: Arc<dyn SandboxPidResolver>,
    upperdirs: Arc<dyn UpperdirResolver>,
    cfg: SandboxPressureConfig,
) -> anyhow::Result<()> {
    let pods: Api<Pod> = Api::namespaced(client, &cfg.namespace);
    let lp = ListParams::default()
        .fields(&format!(
            "spec.nodeName={},status.phase=Running",
            cfg.node_name
        ))
        .labels(SANDBOX_LABEL);

    let mut states: HashMap<String, PodState> = HashMap::new();

    tracing::info!(
        node = %cfg.node_name,
        check_interval_ms = cfg.check_interval.as_millis() as u64,
        report_every_ticks = cfg.report_every,
        walk_every_ticks = cfg.walk_every,
        cpu_warn = cfg.thresholds.cpu_warn_fraction,
        mem_warn = cfg.thresholds.mem_warn_fraction,
        disk_warn = cfg.thresholds.disk_warn_fraction,
        psi_warn = cfg.thresholds.psi_warn_percent,
        clear_ratio = cfg.thresholds.clear_ratio,
        "sandbox-pressure watcher started (#472)"
    );

    loop {
        tokio::time::sleep(cfg.check_interval).await;

        let list = match pods.list(&lp).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "sandbox-pressure: list sandbox pods failed; will retry");
                continue;
            }
        };
        let present = sandbox_pods(list);
        let mut seen: HashSet<String> = HashSet::with_capacity(present.len());

        // Pods are sampled sequentially within a tick: each `sample_pod` awaits
        // its cgroup read + containerd PID/upperdir resolution + blocking walk
        // before the next. Fine at expected per-node sandbox counts; a slow
        // containerd round-trip can push the effective interval past
        // `check_interval`, which the per-pod `last_sample_at` elapsed math
        // already compensates for in the rate calculations (#478 review).
        for pod in &present {
            seen.insert(pod.uid.clone());
            sample_pod(
                &cfg,
                cgroup.as_ref(),
                memory.as_ref(),
                net.as_ref(),
                pids.as_ref(),
                upperdirs.as_ref(),
                pod,
                states.entry(pod.uid.clone()).or_insert_with(|| {
                    PodState::new(pod.sandbox.clone(), cfg.thresholds, cfg.report_every)
                }),
            )
            .await;
        }

        // Forget sandboxes that have left the node.
        states.retain(|uid, _| seen.contains(uid));
    }
}

/// Sample one sandbox for one tick: read every signal (fail-open per signal),
/// build the [`PressureSample`], advance its episode, and emit logs.
///
/// Instrumented so this watcher's per-pod cgroup/containerd/`/proc` I/O is
/// traceable like the rest of the crate (#478 review): a stalled containerd call
/// or a pathological upperdir walk shows up as a long `sample_pod` span.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    name = "sandbox_pressure::sample_pod",
    fields(sandbox = %pod.sandbox)
)]
async fn sample_pod(
    cfg: &SandboxPressureConfig,
    cgroup: &dyn CgroupPressureReader,
    memory: &dyn CgroupMemoryReader,
    net: &dyn ProcNetReader,
    pids: &dyn SandboxPidResolver,
    upperdirs: &dyn UpperdirResolver,
    pod: &SandboxPod,
    state: &mut PodState,
) {
    // Elapsed since this pod's previous sample drives the rate math; fall back
    // to the nominal check interval on the first tick.
    let now = Instant::now();
    let elapsed_secs = state
        .last_sample_at
        .map(|t| now.duration_since(t).as_secs_f64())
        .filter(|s| *s > 0.0)
        .unwrap_or(cfg.check_interval.as_secs_f64());
    state.last_sample_at = Some(now);

    // --- cgroup pressure signals (cpu / psi / io) ---
    let cstats = match cgroup.read(&pod.uid) {
        Ok(Some(s)) => {
            state.clear_failure("cgroup");
            s
        }
        Ok(None) => return, // not a resolvable sandbox cgroup (or torn down)
        Err(e) => {
            state.warn_once(
                "cgroup",
                &format!("sandbox-pressure: cgroup read failed: {e}"),
            );
            return;
        }
    };

    // CPU utilization needs two usage samples and a quota.
    let cpu_fraction = match (state.prev_cpu_usage_usec, cstats.cpu.quota) {
        (Some(prev), Some(quota)) => quota
            .allowed_cores()
            .and_then(|cores| cpu_utilization(prev, cstats.cpu.usage_usec, elapsed_secs, cores)),
        _ => None,
    };
    state.prev_cpu_usage_usec = Some(cstats.cpu.usage_usec);

    // --- memory fraction (existing memory reader) ---
    let mem_fraction = match memory.read(&pod.uid) {
        Ok(Some(m)) => {
            state.clear_failure("memory");
            m.usage_fraction()
        }
        Ok(None) => None,
        Err(e) => {
            state.warn_once(
                "memory",
                &format!("sandbox-pressure: memory read failed: {e}"),
            );
            None
        }
    };

    // --- disk fill estimate ---
    let cur_wbytes = cstats.io.wbytes;
    let (fill_bytes, disk_fraction) =
        compute_disk_fill(cfg, upperdirs, pod, state, cur_wbytes).await;

    // --- network rate (context only) ---
    let (net_rx_bps, net_tx_bps) =
        compute_net_rates(net, pids, pod, state, &cfg.net_iface, elapsed_secs).await;

    let sample = PressureSample {
        cpu_fraction,
        cpu_psi: cstats.cpu_psi.map(|p| p.avg10),
        mem_fraction,
        mem_psi: cstats.memory_psi.map(|p| p.avg10),
        io_psi: cstats.io_psi.map(|p| p.avg10),
        disk_fraction,
        net_rx_bps,
        net_tx_bps,
    };

    // Continuous per-sandbox gauges — the pre-failure baseline the logs can't
    // give (#475). Recorded every tick; a no-op under `OTEL_SDK_DISABLED`.
    crate::sandbox_pressure_metrics::record_sample(
        &cfg.node_name,
        &cfg.namespace,
        &pod.sandbox,
        &sample,
        fill_bytes,
    );

    match state.episode.observe(&sample) {
        EpisodeAction::None => {}
        EpisodeAction::Warn {
            first,
            over_warn,
            elevated,
        } => {
            // The rising edge is `warn` (alert-eligible — exactly one per
            // episode); the sustained lines are `info`, so a sandbox pinned
            // under pressure for minutes is one warn plus a stream of info, not
            // hundreds of warns, while a tailing operator still sees the
            // continuous per-interval output (#478 review). The field set is
            // identical, so a local macro keeps the two levels DRY.
            macro_rules! pressure_event {
                ($level:expr) => {
                    tracing::event!(
                        $level,
                        sandbox = %pod.sandbox,
                        node = %cfg.node_name,
                        namespace = %cfg.namespace,
                        first_tick = first,
                        over_warn = %join_signals(&over_warn),
                        elevated = %join_signals(&elevated),
                        cpu_fraction = ?sample.cpu_fraction,
                        cpu_psi = ?sample.cpu_psi,
                        mem_fraction = ?sample.mem_fraction,
                        mem_psi = ?sample.mem_psi,
                        io_psi = ?sample.io_psi,
                        disk_fraction = ?sample.disk_fraction,
                        net_rx_bps = ?sample.net_rx_bps,
                        net_tx_bps = ?sample.net_tx_bps,
                        "sandbox-pressure: sandbox under resource pressure"
                    )
                };
            }
            if first {
                pressure_event!(tracing::Level::WARN);
                // Count the episode once per signal that opened it (#475). The
                // counter omits `sandbox` to stay bounded — see the metrics module.
                for sig in &over_warn {
                    crate::sandbox_pressure_metrics::record_episode(&cfg.node_name, sig.as_str());
                }
            } else {
                pressure_event!(tracing::Level::INFO);
            }
        }
        EpisodeAction::Clear {
            duration_ticks,
            peaks,
            triggered,
        } => {
            let duration = cfg.check_interval * duration_ticks;
            tracing::info!(
                sandbox = %pod.sandbox,
                node = %cfg.node_name,
                namespace = %cfg.namespace,
                duration_secs = duration.as_secs_f64(),
                triggered = %join_signals(&triggered),
                peak_cpu_fraction = ?peaks.cpu_fraction,
                peak_cpu_psi = ?peaks.cpu_psi,
                peak_mem_fraction = ?peaks.mem_fraction,
                peak_mem_psi = ?peaks.mem_psi,
                peak_io_psi = ?peaks.io_psi,
                peak_disk_fraction = ?peaks.disk_fraction,
                peak_net_rx_bps = ?peaks.net_rx_bps,
                peak_net_tx_bps = ?peaks.net_tx_bps,
                "sandbox-pressure: pressure cleared"
            );
        }
    }
}

/// Run the disk-fill estimator for one tick, performing a real upperdir walk
/// when due (off the reactor via `spawn_blocking`). Returns `(fill_bytes,
/// fill_fraction)` — the byte estimate is always available; the fraction is
/// `None` when the pod has no ephemeral-storage limit to divide by.
async fn compute_disk_fill(
    cfg: &SandboxPressureConfig,
    upperdirs: &dyn UpperdirResolver,
    pod: &SandboxPod,
    state: &mut PodState,
    cur_wbytes: u64,
) -> (u64, Option<f64>) {
    let limit = pod.ephemeral_limit_bytes;
    let warn = cfg.thresholds.disk_warn_fraction;
    if state
        .fill
        .should_walk(cur_wbytes, cfg.walk_every, limit, warn)
    {
        match walk_upperdir(upperdirs, pod).await {
            Some(measured) => {
                state.clear_failure("upperdir");
                state.fill.record_walk(measured, cur_wbytes);
            }
            None => {
                state.warn_once(
                    "upperdir",
                    "sandbox-pressure: upperdir unresolved; disk-fill running off io.stat only",
                );
                state.fill.note_failed_walk();
            }
        }
    } else {
        state.fill.tick();
    }
    state.fill.fraction(cur_wbytes, limit, warn)
}

/// Resolve a container's upperdir and walk it (blocking FS work offloaded).
/// `None` when there is no container id, the upperdir can't be resolved, or the
/// walk's blocking task is cancelled.
#[tracing::instrument(skip_all, name = "sandbox_pressure::walk_upperdir")]
async fn walk_upperdir(upperdirs: &dyn UpperdirResolver, pod: &SandboxPod) -> Option<u64> {
    let cid = pod.container_id.as_deref()?;
    let path = match upperdirs.upperdir(cid).await {
        Ok(Some(p)) => p,
        Ok(None) | Err(_) => return None,
    };
    tokio::task::spawn_blocking(move || dir_size_bytes(&path))
        .await
        .ok()
}

/// Compute network rx/tx rates for one tick (context only). Resolves and caches
/// the container PID; clears the cache when the `/proc` entry has vanished so a
/// restarted container is re-resolved. Returns `(rx_bps, tx_bps)`.
#[tracing::instrument(skip_all, name = "sandbox_pressure::compute_net_rates")]
async fn compute_net_rates(
    net: &dyn ProcNetReader,
    pids: &dyn SandboxPidResolver,
    pod: &SandboxPod,
    state: &mut PodState,
    iface: &str,
    elapsed_secs: f64,
) -> (Option<f64>, Option<f64>) {
    let Some(cid) = pod.container_id.as_deref() else {
        return (None, None);
    };
    // Resolve the PID once and cache it; it's stable for a container's life.
    if state.pid.is_none() {
        match pids.init_pid(cid).await {
            Ok(Some(pid)) => {
                state.clear_failure("pid");
                state.pid = Some(pid);
            }
            Ok(None) => return (None, None), // no running task
            Err(e) => {
                state.warn_once("pid", &format!("sandbox-pressure: PID resolve failed: {e}"));
                return (None, None);
            }
        }
    }
    let Some(pid) = state.pid else {
        return (None, None);
    };
    let counters = match net.read(pid, iface) {
        Ok(Some(c)) => {
            state.clear_failure("net");
            c
        }
        Ok(None) => {
            // /proc entry gone — the task likely exited/restarted; drop the
            // cached PID and the rate baseline so the next tick re-resolves.
            state.pid = None;
            state.prev_net = None;
            return (None, None);
        }
        Err(e) => {
            state.warn_once(
                "net",
                &format!("sandbox-pressure: /proc net read failed: {e}"),
            );
            return (None, None);
        }
    };
    let rates = match state.prev_net {
        Some(prev) => (
            delta_rate(prev.rx_bytes, counters.rx_bytes, elapsed_secs),
            delta_rate(prev.tx_bytes, counters.tx_bytes, elapsed_secs),
        ),
        None => (None, None),
    };
    state.prev_net = Some(counters);
    rates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox_stats::POD_NET_IFACE;

    fn thresholds() -> PressureThresholds {
        PressureThresholds::try_new(0.85, 0.85, 0.80, 25.0, 0.7).unwrap()
    }

    #[test]
    fn thresholds_reject_out_of_range() {
        assert!(PressureThresholds::try_new(0.85, 0.85, 0.80, 25.0, 0.7).is_ok());
        assert!(PressureThresholds::try_new(0.0, 0.85, 0.80, 25.0, 0.7).is_err());
        assert!(PressureThresholds::try_new(1.1, 0.85, 0.80, 25.0, 0.7).is_err());
        assert!(PressureThresholds::try_new(0.85, 0.85, 0.80, 0.0, 0.7).is_err());
        assert!(PressureThresholds::try_new(0.85, 0.85, 0.80, 101.0, 0.7).is_err());
        // clear_ratio must be strictly < 1.0.
        assert!(PressureThresholds::try_new(0.85, 0.85, 0.80, 25.0, 1.0).is_err());
        assert!(PressureThresholds::try_new(0.85, 0.85, 0.80, 25.0, 0.0).is_err());
        assert!(PressureThresholds::try_new(0.85, 0.85, 0.80, 25.0, f64::NAN).is_err());
    }

    #[test]
    fn warned_and_above_clear_select_the_right_signals() {
        let t = thresholds();
        // cpu at 0.90 (>=0.85 warn); mem at 0.62 (>= 0.85*0.7=0.595 clear, < warn);
        // io_psi at 10 (< 25*0.7=17.5 clear).
        let s = PressureSample {
            cpu_fraction: Some(0.90),
            mem_fraction: Some(0.62),
            io_psi: Some(10.0),
            ..Default::default()
        };
        assert_eq!(s.warned(&t), BTreeSet::from([PressureSignal::Cpu]));
        assert_eq!(
            s.above_clear(&t),
            BTreeSet::from([PressureSignal::Cpu, PressureSignal::Memory])
        );
        // None-valued signals never contribute.
        let empty = PressureSample::default();
        assert!(empty.warned(&t).is_empty());
        assert!(empty.above_clear(&t).is_empty());
    }

    fn warn_sample() -> PressureSample {
        PressureSample {
            cpu_fraction: Some(0.95),
            ..Default::default()
        }
    }

    #[test]
    fn episode_opens_sustains_and_clears_without_flapping() {
        let mut ep = EpisodeTracker::new(thresholds(), 1);
        // Rising edge → first warn, cpu over warn.
        match ep.observe(&warn_sample()) {
            EpisodeAction::Warn {
                first,
                over_warn,
                elevated,
            } => {
                assert!(first);
                assert_eq!(over_warn, BTreeSet::from([PressureSignal::Cpu]));
                assert_eq!(elevated, BTreeSet::from([PressureSignal::Cpu]));
            }
            other => panic!("expected first warn, got {other:?}"),
        }
        assert!(ep.is_open());
        // Still above warn, report_every=1 → warn again, not first.
        assert!(matches!(
            ep.observe(&warn_sample()),
            EpisodeAction::Warn { first: false, .. }
        ));
        // Drop into the hysteresis band: 0.62 >= 0.595 clear, < 0.85 warn.
        // Episode STAYS open (no flap) and still emits at cadence, but with an
        // empty over_warn set and cpu still elevated.
        let band = PressureSample {
            cpu_fraction: Some(0.62),
            ..Default::default()
        };
        match ep.observe(&band) {
            EpisodeAction::Warn {
                first,
                over_warn,
                elevated,
            } => {
                assert!(!first);
                assert!(over_warn.is_empty());
                assert_eq!(elevated, BTreeSet::from([PressureSignal::Cpu]));
            }
            other => panic!("expected sustained warn in band, got {other:?}"),
        }
        assert!(ep.is_open());
        // Fall below clear → episode closes with a summary.
        let clear = PressureSample {
            cpu_fraction: Some(0.10),
            ..Default::default()
        };
        match ep.observe(&clear) {
            EpisodeAction::Clear {
                duration_ticks,
                peaks,
                triggered,
            } => {
                assert_eq!(triggered, BTreeSet::from([PressureSignal::Cpu]));
                assert_eq!(peaks.cpu_fraction, Some(0.95));
                // Opened, then 2 sustaining ticks (warn + band) before clear.
                assert_eq!(duration_ticks, 2);
            }
            other => panic!("expected clear, got {other:?}"),
        }
        assert!(!ep.is_open());
        // After clear, a calm sample is a no-op.
        assert_eq!(ep.observe(&clear), EpisodeAction::None);
    }

    #[test]
    fn report_cadence_detailed() {
        let mut ep = EpisodeTracker::new(thresholds(), 3);
        // open → warn(first)
        assert!(matches!(
            ep.observe(&warn_sample()),
            EpisodeAction::Warn { first: true, .. }
        ));
        // +1, +2 → silent
        assert_eq!(ep.observe(&warn_sample()), EpisodeAction::None);
        assert_eq!(ep.observe(&warn_sample()), EpisodeAction::None);
        // +3 → warn again (not first)
        assert!(matches!(
            ep.observe(&warn_sample()),
            EpisodeAction::Warn { first: false, .. }
        ));
    }

    #[test]
    fn cpu_utilization_math_and_regression() {
        // 0.5s of CPU over a 1s wall on 1 core => 50%.
        assert_eq!(cpu_utilization(0, 500_000, 1.0, 1.0), Some(0.5));
        // Same usage but only 0.5 cores allowed => 100%.
        assert_eq!(cpu_utilization(0, 500_000, 1.0, 0.5), Some(1.0));
        // Counter regression (cgroup recreated) => None.
        assert_eq!(cpu_utilization(1_000, 500, 1.0, 1.0), None);
        // Non-positive interval / cores => None.
        assert_eq!(cpu_utilization(0, 100, 0.0, 1.0), None);
        assert_eq!(cpu_utilization(0, 100, 1.0, 0.0), None);
    }

    #[test]
    fn delta_rate_and_regression() {
        assert_eq!(delta_rate(100, 1100, 2.0), Some(500.0));
        assert_eq!(delta_rate(500, 499, 1.0), None); // regression
        assert_eq!(delta_rate(0, 100, 0.0), None); // bad interval
    }

    #[test]
    fn quantity_parses_binary_decimal_and_plain() {
        assert_eq!(parse_quantity_bytes("256Mi"), Some(256 * 1024 * 1024));
        assert_eq!(parse_quantity_bytes("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_quantity_bytes("512M"), Some(512 * 1_000_000));
        assert_eq!(parse_quantity_bytes("1073741824"), Some(1_073_741_824));
        assert_eq!(parse_quantity_bytes("1Ki"), Some(1024));
        // Unparseable / unsupported.
        assert_eq!(parse_quantity_bytes(""), None);
        assert_eq!(parse_quantity_bytes("100m"), None); // millis meaningless for bytes
        assert_eq!(parse_quantity_bytes("abc"), None);
    }

    #[test]
    fn fill_estimator_overestimates_and_corrects_on_walk() {
        let mut e = FillEstimator::default();
        // First tick always walks.
        assert!(e.should_walk(0, 15, Some(1000), 0.8));
        e.record_walk(100, 0); // measured 100 bytes, baseline wbytes=0
                               // 50 bytes written since walk → estimate 150.
        assert_eq!(e.estimate(50), 150);
        e.tick();
        let (fill, frac) = e.fraction(50, Some(1000), 0.8);
        assert_eq!(fill, 150);
        assert_eq!(frac, Some(0.15));
        // No periodic walk yet (ticks_since_walk < 15), not over warn.
        assert!(!e.should_walk(50, 15, Some(1000), 0.8));
    }

    #[test]
    fn fill_estimator_walks_on_first_warn_crossing() {
        let mut e = FillEstimator::default();
        e.record_walk(100, 0);
        // Estimate 900/1000 = 0.9 >= 0.8 warn, not yet latched over_warn → walk.
        assert!(e.should_walk(800, 15, Some(1000), 0.8));
        // After latching over_warn via fraction(), the crossing no longer forces
        // a walk (suppresses per-tick walking while sustained over warn).
        e.fraction(800, Some(1000), 0.8);
        assert!(!e.should_walk(800, 15, Some(1000), 0.8));
    }

    #[test]
    fn fill_estimator_rebaselines_on_counter_reset() {
        let mut e = FillEstimator::default();
        e.record_walk(100, 1000); // baseline wbytes=1000
                                  // Counter regressed (container restart) → walk to rebaseline.
        assert!(e.should_walk(10, 15, Some(1000), 0.8));
    }

    #[test]
    fn fill_estimator_no_limit_has_no_fraction() {
        let mut e = FillEstimator::default();
        e.record_walk(500, 0);
        let (fill, frac) = e.fraction(0, None, 0.8);
        assert_eq!(fill, 500);
        assert_eq!(frac, None);
    }

    #[test]
    fn config_derives_tick_counts() {
        let t = thresholds();
        // check 1s, report 5s, walk 15s → report_every 5, walk_every 15.
        let cfg = SandboxPressureConfig::from_intervals(
            "n".into(),
            "ns".into(),
            Duration::from_millis(1000),
            Duration::from_millis(5000),
            Duration::from_secs(15),
            t,
            POD_NET_IFACE.into(),
        );
        assert_eq!(cfg.report_every, 5);
        assert_eq!(cfg.walk_every, 15);
        // report faster than check is clamped up to 1 tick.
        let cfg2 = SandboxPressureConfig::from_intervals(
            "n".into(),
            "ns".into(),
            Duration::from_millis(2000),
            Duration::from_millis(500),
            Duration::from_millis(2000),
            t,
            POD_NET_IFACE.into(),
        );
        assert_eq!(cfg2.report_every, 1);
        assert_eq!(cfg2.walk_every, 1);
    }

    #[test]
    fn warn_once_rearms_after_recovery() {
        let mut st = PodState::new("sb".into(), thresholds(), 1);
        st.warn_once("cgroup", "boom");
        assert!(st.warned_failures.contains(&"cgroup"));
        // A second failure of the same kind stays suppressed...
        st.warn_once("cgroup", "boom again");
        assert!(st.warned_failures.contains(&"cgroup"));
        // ...until the signal recovers, which re-arms the one-shot warning so a
        // later re-failure is logged again rather than silenced for life.
        st.clear_failure("cgroup");
        assert!(!st.warned_failures.contains(&"cgroup"));
    }

    #[test]
    fn peaks_track_maxima() {
        let mut p = Peaks::default();
        p.observe(&PressureSample {
            cpu_fraction: Some(0.5),
            mem_psi: Some(10.0),
            ..Default::default()
        });
        p.observe(&PressureSample {
            cpu_fraction: Some(0.9),
            mem_psi: Some(3.0),
            ..Default::default()
        });
        assert_eq!(p.cpu_fraction, Some(0.9));
        assert_eq!(p.mem_psi, Some(10.0));
        assert_eq!(p.disk_fraction, None);
    }
}
