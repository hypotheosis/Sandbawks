//! Data layer for the per-sandbox resource-pressure watcher (epic #472,
//! phase 1 / issue #473).
//!
//! This module is *only* the readers and parsers — the part that turns a host
//! source (cgroup-v2 files, `/proc/<pid>/net/dev`, an overlayfs upperdir) into a
//! plain number. The watcher loop, thresholds, hysteresis, logging, and metrics
//! all land in phase 2/3; nothing here is wired into the running DaemonSet yet.
//!
//! Design mirrors [`crate::memory_pressure`]: every parser is a pure,
//! unit-testable function, and the real filesystem / containerd reads sit behind
//! traits ([`CgroupPressureReader`], [`ProcNetReader`], [`SandboxPidResolver`])
//! so phase 2 can inject fixtures with no real cgroupfs, `/proc`, or containerd.
//!
//! Why these signals, and why host-side: gVisor's emulated guest cgroupfs is
//! cosmetic (it reports host RAM, exposes no PSI — see #305/#306), so the only
//! truthful vantage point for a sandbox's CPU/memory/IO pressure is the host pod
//! cgroup the kubelet places it under (after #448 the sandbox actually nests
//! there). Network counters live in the pod network namespace, reached via the
//! sandbox's host PID.
//!
//! Reader return convention (matches [`crate::memory_pressure::CgroupMemoryReader`]):
//! `Ok(Some(stats))` on a successful read, `Ok(None)` when the thing being read
//! does not exist (pod cgroup not found, task not running, `/proc` entry gone) —
//! a normal, expected outcome the watcher skips — and `Err(msg)` only when the
//! source exists but cannot be read or parsed. Phase 2 turns `Ok(None)` into a
//! per-signal skip and `Err` into a one-shot warning, never terminating.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::memory_pressure::pod_cgroup_dir;

/// The pod network interface whose counters the watcher reports. The CNI names
/// the sandbox's veth `eth0` inside the pod netns; `lo` is deliberately excluded
/// so loopback chatter doesn't inflate the rate.
pub const POD_NET_IFACE: &str = "eth0";

/// `cpu.stat` `usage_usec` plus the `cpu.max` quota — the raw inputs from which
/// the watcher derives a CPU-utilization fraction across two samples.
///
/// A fraction needs two readings (it is `Δusage / (Δwall × allowed_cores)`), so
/// this layer only exposes the cumulative counter and the quota; the rate math
/// lives in phase 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CpuStats {
    /// `cpu.stat` `usage_usec` — cumulative CPU time charged to the cgroup, in
    /// microseconds. Monotonic until the cgroup is recreated (a container
    /// restart), which the watcher detects as a counter regression.
    pub usage_usec: u64,
    /// `cpu.max` quota, or `None` when the file reads `max` (no CPU cap). An
    /// uncapped cgroup has no utilization *fraction* to compute, so the watcher
    /// reports PSI only for it.
    pub quota: Option<CpuQuota>,
}

/// A cgroup-v2 `cpu.max` quota: `"<quota_usec> <period_usec>"`.
///
/// The cgroup may use up to `quota_usec` of CPU time every `period_usec`, so the
/// number of whole cores it is allowed is `quota_usec / period_usec` (e.g.
/// `50000 100000` → 0.5 cores; `200000 100000` → 2 cores).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuQuota {
    /// Max CPU microseconds allowed per `period_usec`.
    pub quota_usec: u64,
    /// Accounting window length in microseconds.
    pub period_usec: u64,
}

impl CpuQuota {
    /// Cores this quota permits (`quota_usec / period_usec`). `None` when the
    /// period is zero (a malformed `cpu.max`), since the ratio is undefined.
    pub fn allowed_cores(&self) -> Option<f64> {
        if self.period_usec == 0 {
            None
        } else {
            Some(self.quota_usec as f64 / self.period_usec as f64)
        }
    }
}

/// A single PSI `some avg10` stall figure, as reported by the kernel.
///
/// "some avg10" is the share of the last 10 seconds during which *at least one*
/// task in the cgroup was stalled waiting on the resource — the most direct
/// "this sandbox is lagging on X" signal cgroup-v2 exposes. The kernel emits a
/// percentage normally in `[0.0, 100.0]`, but [`parse_psi_some_avg10`] does not
/// validate or clamp the range — it carries through whatever the file holds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PsiSome {
    /// `some avg10` percentage, verbatim from a `*.pressure` file (unclamped).
    pub avg10: f64,
}

/// `io.stat` read/write byte counters, summed across every backing device.
///
/// `wbytes` is the input to the disk-fill estimator (phase 2): bytes written
/// since the last real upperdir walk are added to that walk's measured size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IoStats {
    /// Cumulative bytes read across all devices.
    pub rbytes: u64,
    /// Cumulative bytes written across all devices.
    pub wbytes: u64,
}

/// Per-interface byte counters parsed from `/proc/<pid>/net/dev`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NetCounters {
    /// Cumulative bytes received on the interface.
    pub rx_bytes: u64,
    /// Cumulative bytes transmitted on the interface.
    pub tx_bytes: u64,
}

/// One full set of cgroup-sourced pressure signals for a sandbox's pod cgroup.
///
/// Memory `current`/`max` is intentionally absent: the existing
/// [`crate::memory_pressure::CgroupMemoryReader`] already provides it, and phase
/// 2 composes the two readers rather than reading `memory.current` twice. This
/// struct carries only the signals that reader does not.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CgroupPressureStats {
    /// CPU usage counter + quota (`cpu.stat` / `cpu.max`).
    pub cpu: CpuStats,
    /// `cpu.pressure` `some avg10`, or `None` when the kernel/cgroup exposes no
    /// PSI for CPU.
    pub cpu_psi: Option<PsiSome>,
    /// `memory.pressure` `some avg10`, or `None` when absent.
    pub memory_psi: Option<PsiSome>,
    /// `io.pressure` `some avg10`, or `None` when absent.
    pub io_psi: Option<PsiSome>,
    /// `io.stat` totals (zeroed when the IO controller is not enabled on the
    /// cgroup, which presents as a missing `io.stat`).
    pub io: IoStats,
}

// ---------------------------------------------------------------------------
// Pure parsers
// ---------------------------------------------------------------------------

/// Parse `cpu.stat`, returning the `usage_usec` counter, or `None` if the key is
/// absent.
///
/// `cpu.stat` is a `"<key> <value>"` table; only `usage_usec` is needed here.
pub fn parse_cpu_stat(body: &str) -> Option<u64> {
    body.lines().find_map(|line| {
        let mut it = line.split_whitespace();
        match (it.next(), it.next()) {
            (Some("usage_usec"), Some(v)) => v.parse().ok(),
            _ => None,
        }
    })
}

/// Parse `cpu.max` (`"<quota> <period>"` or `"max <period>"`), mapping the
/// unlimited `max` quota to `None`.
///
/// Returns `None` for an unlimited or malformed line (missing fields /
/// unparseable numbers) — a CPU cgroup with no usable quota has no utilization
/// fraction to compute.
pub fn parse_cpu_max(s: &str) -> Option<CpuQuota> {
    let mut it = s.split_whitespace();
    let quota_tok = it.next()?;
    let period_usec: u64 = it.next()?.parse().ok()?;
    if quota_tok == "max" {
        return None;
    }
    let quota_usec: u64 = quota_tok.parse().ok()?;
    Some(CpuQuota {
        quota_usec,
        period_usec,
    })
}

/// Parse the `some avg10` field out of a cgroup-v2 PSI (`*.pressure`) body.
///
/// PSI files look like:
/// ```text
/// some avg10=1.23 avg60=0.45 avg300=0.00 total=12345
/// full avg10=0.00 avg60=0.00 avg300=0.00 total=0
/// ```
/// Returns `None` when there is no `some` line or its `avg10=` token is missing
/// or unparseable (so a kernel without PSI yields "no signal", not an error).
pub fn parse_psi_some_avg10(body: &str) -> Option<PsiSome> {
    let some = body.lines().find(|l| {
        // Match the leading "some" token exactly (not a substring of, say, a
        // hypothetical "something" key).
        l.split_whitespace().next() == Some("some")
    })?;
    let avg10 = some.split_whitespace().find_map(|tok| {
        tok.strip_prefix("avg10=")
            .and_then(|v| v.parse::<f64>().ok())
    })?;
    Some(PsiSome { avg10 })
}

/// Parse `io.stat`, summing `rbytes` / `wbytes` across every device line.
///
/// Each line is `"<major>:<minor> rbytes=N wbytes=N rios=N …"`. Unknown keys are
/// ignored; an empty body yields all-zero totals (a valid "no IO yet" reading).
pub fn parse_io_stat(body: &str) -> IoStats {
    let mut stats = IoStats::default();
    for line in body.lines() {
        for tok in line.split_whitespace() {
            if let Some(v) = tok.strip_prefix("rbytes=") {
                stats.rbytes = stats.rbytes.saturating_add(v.parse().unwrap_or(0));
            } else if let Some(v) = tok.strip_prefix("wbytes=") {
                stats.wbytes = stats.wbytes.saturating_add(v.parse().unwrap_or(0));
            }
        }
    }
    stats
}

/// Parse one interface's rx/tx byte counters from a `/proc/<pid>/net/dev` body.
///
/// The file is two header lines then one line per interface:
/// ```text
/// Inter-|   Receive                    |  Transmit
///  face |bytes  packets errs … |bytes  packets errs …
///   eth0: 998877  100   0   …   112233   99    0  …
/// ```
/// The first post-colon column is receive bytes; transmit bytes is the 9th
/// (index 8), after the eight receive columns. Splitting on the first `:`
/// tolerates a counter so large it abuts the interface name (`eth0:998877`).
///
/// Returns `None` when `iface` is not present or its byte columns are missing /
/// unparseable.
pub fn parse_proc_net_dev(body: &str, iface: &str) -> Option<NetCounters> {
    for line in body.lines() {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        if name.trim() != iface {
            continue;
        }
        let cols: Vec<&str> = rest.split_whitespace().collect();
        // Index 0 = receive bytes, index 8 = transmit bytes. A matched-but-
        // malformed row (e.g. a bare `eth0:` with no counters) makes these `?`
        // short-circuit the whole function to `None` — the same value returned
        // when `iface` is absent. Callers read both as "skip this signal", so
        // collapsing the two failure modes is intentional, not a missed case.
        let rx_bytes = cols.first()?.parse().ok()?;
        let tx_bytes = cols.get(8)?.parse().ok()?;
        return Some(NetCounters { rx_bytes, tx_bytes });
    }
    None
}

/// Recursively sum the apparent size (file lengths) of regular files under
/// `root`, the input to the disk-fill estimator's periodic correction (phase 2).
///
/// Best-effort by design (fail-open telemetry): a single unreadable inode never
/// zeroes the whole estimate, so the result is a *lower bound* on actual fill.
/// - A missing `root` (or a subdir that races away mid-walk) contributes
///   nothing — an overlay upperdir is created lazily, so "no upper" means zero
///   fill.
/// - An unreadable directory or entry (e.g. `EACCES` on one inode) is skipped
///   and the walk continues over everything else; the number of skips is logged
///   once at the end.
/// - Symlinks are never followed and contribute no bytes: a symlink loop cannot
///   wedge the walk, and a build tree full of symlinks can't inflate the
///   estimate. Only regular files add bytes; other node types (sockets, fifos,
///   devices) are ignored.
///
/// Iterative (explicit stack) rather than recursive so a pathologically deep
/// tree cannot overflow the call stack — a real risk on the large source trees
/// (e.g. Rust build dirs) this feature exists to watch.
pub fn dir_size_bytes(root: &Path) -> u64 {
    let mut total: u64 = 0;
    let mut skipped: u64 = 0;
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            // A vanished/never-created dir contributes nothing (the top-level
            // `root` and any subdir that races away mid-walk land here).
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            // Any other read error (e.g. EACCES on a locked-down subdir): skip
            // this subtree, keep the rest of the estimate rather than aborting.
            Err(_) => {
                skipped = skipped.saturating_add(1);
                continue;
            }
        };
        for entry in entries {
            let Ok(entry) = entry else {
                skipped = skipped.saturating_add(1);
                continue;
            };
            let path = entry.path();
            // `symlink_metadata` does NOT traverse symlinks, so a symlinked
            // directory is reported as a symlink here and never descended into.
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                skipped = skipped.saturating_add(1);
                continue;
            };
            let ft = meta.file_type();
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                total = total.saturating_add(meta.len());
            }
            // Symlinks and other node types: not followed, contribute no bytes.
        }
    }
    if skipped > 0 {
        // Once per walk, not per inode — a locked-down subdir shouldn't spam.
        tracing::debug!(
            root = %root.display(),
            skipped,
            "dir_size_bytes: skipped unreadable entries; fill estimate is a lower bound"
        );
    }
    total
}

// ---------------------------------------------------------------------------
// Cgroup pressure reader
// ---------------------------------------------------------------------------

/// Reads a sandbox pod cgroup's CPU / PSI / IO stats, behind a trait so the
/// phase-2 watcher is unit-testable with fixtures instead of a real
/// `/sys/fs/cgroup`. Sibling to [`crate::memory_pressure::CgroupMemoryReader`].
pub trait CgroupPressureReader: Send + Sync {
    /// Read the pressure stats for `pod_uid`'s cgroup, or `None` when no cgroup
    /// dir resolves for it (not a nested sandbox, or already torn down).
    ///
    /// # Errors
    ///
    /// Returns a message when a cgroup dir exists but a present file cannot be
    /// read or parsed. Absent optional files (PSI, `io.stat`) are *not* errors —
    /// they degrade to `None` / zero so the watcher stays fail-open.
    fn read(&self, pod_uid: &str) -> Result<Option<CgroupPressureStats>, String>;
}

/// Production [`CgroupPressureReader`]: reads cgroup-v2 files under a mounted
/// host `/sys/fs/cgroup` (the chart mounts it; reuse the same root as the
/// memory-pressure watcher).
#[derive(Debug, Clone)]
pub struct SysCgroupPressureReader {
    cgroup_root: PathBuf,
}

impl SysCgroupPressureReader {
    /// Build a reader rooted at the mounted host cgroup-v2 filesystem.
    pub fn new(cgroup_root: impl Into<PathBuf>) -> Self {
        Self {
            cgroup_root: cgroup_root.into(),
        }
    }
}

impl CgroupPressureReader for SysCgroupPressureReader {
    fn read(&self, pod_uid: &str) -> Result<Option<CgroupPressureStats>, String> {
        let Some(dir) = pod_cgroup_dir(&self.cgroup_root, pod_uid) else {
            return Ok(None);
        };
        // cpu.stat / cpu.max are present whenever the cpu controller is enabled;
        // tolerate absence (degrade to 0 / no-quota) so a cgroup without the
        // controller still yields PSI rather than erroring the whole read.
        let usage_usec = read_to_string_opt(&dir.join("cpu.stat"))?
            .as_deref()
            .and_then(parse_cpu_stat)
            .unwrap_or(0);
        let quota = read_to_string_opt(&dir.join("cpu.max"))?
            .as_deref()
            .and_then(parse_cpu_max);
        let cpu_psi = read_psi(&dir.join("cpu.pressure"))?;
        let memory_psi = read_psi(&dir.join("memory.pressure"))?;
        let io_psi = read_psi(&dir.join("io.pressure"))?;
        let io = read_to_string_opt(&dir.join("io.stat"))?
            .as_deref()
            .map(parse_io_stat)
            .unwrap_or_default();
        Ok(Some(CgroupPressureStats {
            cpu: CpuStats { usage_usec, quota },
            cpu_psi,
            memory_psi,
            io_psi,
            io,
        }))
    }
}

/// Read a file, mapping a missing file (`NotFound`) to `Ok(None)` and any other
/// IO error to `Err`. The "absent file is normal" half of the fail-open policy.
fn read_to_string_opt(path: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

/// Read and parse a PSI file's `some avg10`, treating a missing file as `None`
/// (kernel/controller without PSI) rather than an error.
fn read_psi(path: &Path) -> Result<Option<PsiSome>, String> {
    Ok(read_to_string_opt(path)?
        .as_deref()
        .and_then(parse_psi_some_avg10))
}

// ---------------------------------------------------------------------------
// /proc/<pid>/net/dev reader
// ---------------------------------------------------------------------------

/// Reads a sandbox's pod-netns interface counters from `/proc/<pid>/net/dev`,
/// behind a trait so the watcher is testable with a fixture `/proc` tree.
pub trait ProcNetReader: Send + Sync {
    /// Read `iface`'s rx/tx byte counters from `/proc/<pid>/net/dev`.
    ///
    /// Returns `None` when the `/proc/<pid>` entry is gone (the task exited
    /// between PID resolution and this read) or `iface` is absent from the
    /// table — both normal, skip-this-signal outcomes.
    ///
    /// # Errors
    ///
    /// Returns a message only when the file exists but cannot be read.
    fn read(&self, pid: u32, iface: &str) -> Result<Option<NetCounters>, String>;
}

/// Production [`ProcNetReader`] rooted at a host `/proc`. The root is
/// configurable so the DaemonSet can mount the host `/proc` at a non-default
/// path and tests can point at a fixture tree.
#[derive(Debug, Clone)]
pub struct SysProcNetReader {
    proc_root: PathBuf,
}

impl Default for SysProcNetReader {
    /// Root at the conventional `/proc`.
    fn default() -> Self {
        Self::new("/proc")
    }
}

impl SysProcNetReader {
    /// Build a reader rooted at `proc_root` (production passes the host `/proc`
    /// mount path).
    pub fn new(proc_root: impl Into<PathBuf>) -> Self {
        Self {
            proc_root: proc_root.into(),
        }
    }
}

impl ProcNetReader for SysProcNetReader {
    fn read(&self, pid: u32, iface: &str) -> Result<Option<NetCounters>, String> {
        let path = self.proc_root.join(pid.to_string()).join("net/dev");
        match read_to_string_opt(&path)? {
            Some(body) => Ok(parse_proc_net_dev(&body, iface)),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Sandbox PID resolution (containerd)
// ---------------------------------------------------------------------------

/// Resolves a containerd container ID to its host init PID, the entry point for
/// reading the sandbox's pod-netns counters via [`ProcNetReader`].
///
/// Behind a trait because the production impl needs a live containerd socket;
/// the watcher and its tests use a fake. Implemented for
/// [`crate::gvisor::ContainerdClient`] (which already holds the gRPC channel).
#[async_trait]
pub trait SandboxPidResolver: Send + Sync {
    /// Resolve `container_id`'s host init PID, or `None` when the container has
    /// no running task (so there is no `/proc/<pid>` to read).
    ///
    /// # Errors
    ///
    /// Returns a message when the containerd Tasks query itself fails.
    async fn init_pid(&self, container_id: &str) -> Result<Option<u32>, String>;
}

/// Resolves a container's overlayfs upperdir path on the host — the directory
/// [`dir_size_bytes`] walks for the disk-fill estimator (phase 2).
///
/// Behind a trait for the same reason as [`SandboxPidResolver`]: the production
/// impl needs a live containerd socket, so the watcher and its tests use a fake.
/// Implemented for [`crate::gvisor::ContainerdClient`].
#[async_trait]
pub trait UpperdirResolver: Send + Sync {
    /// Resolve `container_id`'s host upperdir path, or `None` when the container
    /// exposes no writable overlay layer to measure.
    ///
    /// # Errors
    ///
    /// Returns a message when the containerd snapshotter query itself fails.
    async fn upperdir(&self, container_id: &str) -> Result<Option<PathBuf>, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_stat_extracts_usage_usec() {
        let body = "usage_usec 123456\nuser_usec 100000\nsystem_usec 23456\nnr_periods 7\n";
        assert_eq!(parse_cpu_stat(body), Some(123_456));
        // Missing key → None; unrelated keys ignored.
        assert_eq!(parse_cpu_stat("user_usec 5\nsystem_usec 6\n"), None);
        assert_eq!(parse_cpu_stat(""), None);
    }

    #[test]
    fn cpu_max_parses_quota_and_unlimited() {
        let q = parse_cpu_max("50000 100000").expect("explicit quota parses");
        assert_eq!(
            q,
            CpuQuota {
                quota_usec: 50_000,
                period_usec: 100_000
            }
        );
        assert_eq!(q.allowed_cores(), Some(0.5));
        // Two full cores.
        assert_eq!(
            parse_cpu_max("200000 100000").and_then(|q| q.allowed_cores()),
            Some(2.0)
        );
        // "max" → unlimited → None.
        assert_eq!(parse_cpu_max("max 100000"), None);
        // Malformed lines → None.
        assert_eq!(parse_cpu_max("50000"), None);
        assert_eq!(parse_cpu_max(""), None);
        assert_eq!(parse_cpu_max("abc 100000"), None);
    }

    #[test]
    fn cpu_quota_zero_period_has_no_cores() {
        let q = CpuQuota {
            quota_usec: 50_000,
            period_usec: 0,
        };
        assert_eq!(q.allowed_cores(), None);
    }

    #[test]
    fn psi_parses_some_avg10() {
        let body = "some avg10=1.23 avg60=0.45 avg300=0.00 total=12345\n\
                    full avg10=0.50 avg60=0.10 avg300=0.00 total=999\n";
        assert_eq!(parse_psi_some_avg10(body), Some(PsiSome { avg10: 1.23 }));
        // cpu.pressure on older kernels has only the `some` line.
        assert_eq!(
            parse_psi_some_avg10("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"),
            Some(PsiSome { avg10: 0.0 })
        );
        // No `some` line, or empty → None (no signal, not an error).
        assert_eq!(parse_psi_some_avg10("full avg10=1.0 total=1\n"), None);
        assert_eq!(parse_psi_some_avg10(""), None);
    }

    #[test]
    fn io_stat_sums_across_devices() {
        let body = "8:0 rbytes=1024 wbytes=2048 rios=10 wios=20 dbytes=0 dios=0\n\
                    253:0 rbytes=512 wbytes=4096 rios=5 wios=40\n";
        assert_eq!(
            parse_io_stat(body),
            IoStats {
                rbytes: 1536,
                wbytes: 6144
            }
        );
        // Empty / no IO yet → zeroed (valid reading).
        assert_eq!(parse_io_stat(""), IoStats::default());
    }

    #[test]
    fn proc_net_dev_reads_named_iface() {
        let body = "Inter-|   Receive                                                |  Transmit\n\
             face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
            lo:    1234      10    0    0    0     0          0         0     1234      10    0    0    0     0       0          0\n\
          eth0:  998877     100    0    0    0     0          0         0   112233      99    0    0    0     0       0          0\n";
        assert_eq!(
            parse_proc_net_dev(body, "eth0"),
            Some(NetCounters {
                rx_bytes: 998_877,
                tx_bytes: 112_233
            })
        );
        // lo is parseable too (the watcher just doesn't ask for it).
        assert_eq!(
            parse_proc_net_dev(body, "lo"),
            Some(NetCounters {
                rx_bytes: 1234,
                tx_bytes: 1234
            })
        );
        // Absent iface → None.
        assert_eq!(parse_proc_net_dev(body, "eth1"), None);
    }

    #[test]
    fn proc_net_dev_tolerates_counter_abutting_iface_name() {
        // A huge rx counter can run right up against the colon with no space.
        let body = "eth0:18446744073709551615 100 0 0 0 0 0 0 42 9 0 0 0 0 0 0\n";
        assert_eq!(
            parse_proc_net_dev(body, "eth0"),
            Some(NetCounters {
                rx_bytes: u64::MAX,
                tx_bytes: 42
            })
        );
    }

    #[test]
    fn dir_size_sums_regular_files_recursively() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join("a.txt"), b"hello").unwrap(); // 5 bytes
        let sub = root.join("nested/deep");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("b.bin"), vec![0u8; 4096]).unwrap(); // 4096 bytes
        assert_eq!(dir_size_bytes(root), 5 + 4096);
    }

    #[test]
    fn dir_size_missing_root_is_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("no-such-upperdir");
        assert_eq!(dir_size_bytes(&missing), 0);
    }

    #[test]
    fn dir_size_does_not_follow_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // A directory full of bytes, OUTSIDE the walked root.
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("big.bin"), vec![0u8; 1_000_000]).unwrap();
        // Walked tree: one 3-byte regular file plus a symlink to `outside`.
        let walked = root.join("walked");
        std::fs::create_dir_all(&walked).unwrap();
        std::fs::write(walked.join("real.txt"), b"abc").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, walked.join("link")).unwrap();
        // Contract: symlinks contribute exactly zero bytes and are not followed,
        // so the walk sees ONLY the 3-byte regular file. Pinned exactly (not a
        // range) so phase 2's "overestimate-only" property has a firm floor.
        assert_eq!(dir_size_bytes(&walked), 3);
    }

    #[test]
    fn cgroup_pressure_reader_reads_fixture_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let uid = "d61481fc-f86e-4c44-8938-316d3d95e482";
        let slug = uid.replace('-', "_");
        let dir = tmp
            .path()
            .join("kubepods.slice")
            .join(format!("kubepods-pod{slug}.slice"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("cpu.stat"),
            "usage_usec 5000000\nuser_usec 4000000\n",
        )
        .unwrap();
        std::fs::write(dir.join("cpu.max"), "50000 100000\n").unwrap();
        std::fs::write(
            dir.join("cpu.pressure"),
            "some avg10=12.50 avg60=3.00 avg300=0.00 total=1\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("memory.pressure"),
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 total=0\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("io.pressure"),
            "some avg10=7.10 avg60=1.00 avg300=0.00 total=9\nfull avg10=2.0 total=3\n",
        )
        .unwrap();
        std::fs::write(dir.join("io.stat"), "8:0 rbytes=1024 wbytes=8192\n").unwrap();

        let reader = SysCgroupPressureReader::new(tmp.path());
        let stats = reader.read(uid).unwrap().expect("fixture cgroup exists");
        assert_eq!(stats.cpu.usage_usec, 5_000_000);
        assert_eq!(stats.cpu.quota.unwrap().allowed_cores(), Some(0.5));
        assert_eq!(stats.cpu_psi, Some(PsiSome { avg10: 12.5 }));
        assert_eq!(stats.memory_psi, Some(PsiSome { avg10: 0.0 }));
        assert_eq!(stats.io_psi, Some(PsiSome { avg10: 7.1 }));
        assert_eq!(stats.io.wbytes, 8192);
    }

    #[test]
    fn cgroup_pressure_reader_none_for_unknown_pod() {
        let tmp = tempfile::tempdir().unwrap();
        let reader = SysCgroupPressureReader::new(tmp.path());
        assert_eq!(
            reader.read("00000000-0000-0000-0000-000000000000").unwrap(),
            None
        );
    }

    #[test]
    fn cgroup_pressure_reader_tolerates_missing_optional_files() {
        // A cgroup dir with only cpu.stat — no cpu.max, no PSI, no io.stat. The
        // read must succeed with degraded (None / zero) optional signals.
        let tmp = tempfile::tempdir().unwrap();
        let uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let slug = uid.replace('-', "_");
        let dir = tmp
            .path()
            .join("kubepods.slice")
            .join(format!("kubepods-pod{slug}.slice"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("cpu.stat"), "usage_usec 42\n").unwrap();

        let reader = SysCgroupPressureReader::new(tmp.path());
        let stats = reader.read(uid).unwrap().unwrap();
        assert_eq!(stats.cpu.usage_usec, 42);
        assert_eq!(stats.cpu.quota, None);
        assert_eq!(stats.cpu_psi, None);
        assert_eq!(stats.memory_psi, None);
        assert_eq!(stats.io_psi, None);
        assert_eq!(stats.io, IoStats::default());
    }

    #[test]
    fn proc_net_reader_reads_fixture_and_handles_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let pid: u32 = 4242;
        let net_dir = tmp.path().join(pid.to_string()).join("net");
        std::fs::create_dir_all(&net_dir).unwrap();
        std::fs::write(
            net_dir.join("dev"),
            "Inter-|   Receive                                                |  Transmit\n\
             face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
          eth0:  555     1    0    0    0     0          0         0   777     1    0    0    0     0       0          0\n",
        )
        .unwrap();

        let reader = SysProcNetReader::new(tmp.path());
        assert_eq!(
            reader.read(pid, POD_NET_IFACE).unwrap(),
            Some(NetCounters {
                rx_bytes: 555,
                tx_bytes: 777
            })
        );
        // A pid with no /proc entry (task exited) → Ok(None), not Err.
        assert_eq!(reader.read(999_999, POD_NET_IFACE).unwrap(), None);
    }
}
