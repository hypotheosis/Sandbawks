//! Per-node fail-closed resource-headroom policy (issue #238).
//!
//! The committed overlay upper is disk-backed and grows monotonically,
//! and an in-process `runsc restore` faults the saved `pages.img` into
//! RAM. On a busy node, staging a multi-GiB snapshot bundle or restoring
//! it can hit `ENOSPC` (disk) or the OOM killer (memory). #233 measured
//! an 8 GiB `pages.img` restoring in 43.6s — but only because the host
//! had ~88 GiB free.
//!
//! This module is the guard: before [`crate::gvisor::GvisorAdapter`]
//! stages a bundle to disk or restores one into memory, it consults a
//! pure [`HeadroomPolicy`] against a [`NodeStats`] reading and fails the
//! `SnapshotJob` closed — with a typed error naming the shortfall — rather
//! than risk an `ENOSPC`/OOM mid-operation.
//!
//! The policy is deliberately a **pure function** of
//! `(needed_bytes, available_bytes)` so it is unit-testable without any
//! real node introspection; the `statvfs` / `/proc/meminfo` reads live
//! behind the [`NodeStats`] trait so tests inject a fixed reading.

use std::fmt;
use std::path::Path;

/// Max fraction of an available resource a single snapshot may consume,
/// as the conservative default. A snapshot whose bytes exceed
/// `0.8 × available` fails closed — i.e. the node always keeps at least
/// 20% of the resource in reserve. Raise toward `1.0` to pack snapshots
/// more aggressively.
pub const DEFAULT_MAX_FRACTION: f64 = 0.8;

/// Absolute floor of bytes that must remain free *after* an operation,
/// independent of [`DEFAULT_MAX_FRACTION`]. 1 GiB — generous enough to
/// absorb the on-disk expansion of a zstd-compressed `memory.zst` part
/// (the manifest records the compressed on-the-wire size; the bundle
/// decompresses larger on disk) plus assorted runtime scratch.
pub const DEFAULT_MIN_FREE_BYTES: u64 = 1 << 30;

/// The node resource a [`Shortfall`] is reported against — surfaced in
/// the fail-closed error message so an operator can tell a disk-bound
/// failure from a memory-bound one at a glance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadroomResource {
    /// The staging filesystem's available blocks (`statvfs`).
    Disk,
    /// The kernel's `MemAvailable` estimate (`/proc/meminfo`).
    Memory,
}

impl HeadroomResource {
    /// Lower-case label for log/error messages (`"disk"` / `"memory"`).
    pub fn as_str(self) -> &'static str {
        match self {
            HeadroomResource::Disk => "disk",
            HeadroomResource::Memory => "memory",
        }
    }
}

impl fmt::Display for HeadroomResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A failed headroom dimension: the snapshot needs `needed` bytes of
/// `resource`, but the node only has `available`, which the governing
/// policy (`max_fraction`, `min_free_bytes`) deems insufficient.
///
/// Carries the policy parameters so the [`fmt::Display`] message is
/// self-describing — the operator sees both the shortfall and the rule
/// that rejected it without cross-referencing config.
#[derive(Debug, Clone, PartialEq)]
pub struct Shortfall {
    /// Which resource fell short.
    pub resource: HeadroomResource,
    /// Bytes the snapshot would consume of `resource`.
    pub needed: u64,
    /// Bytes currently available of `resource`.
    pub available: u64,
    /// The `max_fraction` ceiling that was applied.
    pub max_fraction: f64,
    /// The `min_free_bytes` floor that was applied.
    pub min_free_bytes: u64,
}

impl fmt::Display for Shortfall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "insufficient node {resource} headroom: snapshot needs {needed} bytes \
             but only {available} available (policy: at most {pct:.0}% of available \
             may be used and at least {floor} bytes must stay free)",
            resource = self.resource,
            needed = self.needed,
            available = self.available,
            pct = self.max_fraction * 100.0,
            floor = self.min_free_bytes,
        )
    }
}

/// Pure, unit-testable resource-headroom policy.
///
/// A given `needed` byte count "fits" within `available` when **both**
/// hold:
///
/// 1. `needed <= max_fraction * available` — the snapshot doesn't claim
///    more than the configured fraction of what's there, and
/// 2. `available - needed >= min_free_bytes` — satisfying it still leaves
///    the absolute floor free.
///
/// The two checks are independent so a tiny snapshot on a nearly-full
/// disk is still rejected by the floor, while a huge snapshot on a roomy
/// disk is rejected by the fraction.
#[derive(Debug, Clone, Copy)]
pub struct HeadroomPolicy {
    /// Max fraction `(0.0, 1.0]` of an available resource a snapshot may
    /// consume. See [`DEFAULT_MAX_FRACTION`].
    pub max_fraction: f64,
    /// Absolute floor of bytes that must remain free afterwards. See
    /// [`DEFAULT_MIN_FREE_BYTES`].
    pub min_free_bytes: u64,
}

impl Default for HeadroomPolicy {
    fn default() -> Self {
        Self {
            max_fraction: DEFAULT_MAX_FRACTION,
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
        }
    }
}

impl HeadroomPolicy {
    /// Build a policy from raw tunables **without validation**.
    ///
    /// `max_fraction` is not checked: a non-positive or `> 1.0` value
    /// simply makes every / no snapshot fit. Prefer [`Self::try_new`] at
    /// any boundary that accepts an operator-supplied value (the node
    /// binary does) so a misconfiguration fails fast rather than silently
    /// disabling the guard; `new` stays for call sites that pass a
    /// known-good literal (tests, [`Self::default`]).
    pub fn new(max_fraction: f64, min_free_bytes: u64) -> Self {
        Self {
            max_fraction,
            min_free_bytes,
        }
    }

    /// Build a policy, validating `max_fraction ∈ (0.0, 1.0]`.
    ///
    /// Keeps the fraction invariant on the type rather than only at the
    /// CLI boundary, so any programmatic caller gets the same guard. NaN
    /// fails the `0.0 < max_fraction` half (every comparison with NaN is
    /// false), so it is rejected too.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when `max_fraction` is outside
    /// `(0.0, 1.0]` (or NaN). `min_free_bytes` needs no validation — any
    /// `u64`, including `0`, is a meaningful floor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use sandbawks_node::headroom::HeadroomPolicy;
    /// assert!(HeadroomPolicy::try_new(0.8, 1 << 30).is_ok());
    /// assert!(HeadroomPolicy::try_new(2.0, 0).is_err());
    /// assert!(HeadroomPolicy::try_new(0.0, 0).is_err());
    /// ```
    pub fn try_new(max_fraction: f64, min_free_bytes: u64) -> Result<Self, String> {
        if !(0.0 < max_fraction && max_fraction <= 1.0) {
            return Err(format!(
                "max_fraction must be in (0.0, 1.0], got {max_fraction}"
            ));
        }
        Ok(Self::new(max_fraction, min_free_bytes))
    }

    /// `true` if `needed` bytes fit within `available` under this policy.
    fn fits(&self, needed: u64, available: u64) -> bool {
        // `max_fraction * available` in f64: a u64 byte count above 2^53
        // loses sub-KiB precision, which is irrelevant to a headroom
        // gate. Floor + saturate back to u64.
        let cap_f = (available as f64 * self.max_fraction).floor();
        let cap = if cap_f >= u64::MAX as f64 {
            u64::MAX
        } else if cap_f < 0.0 {
            0
        } else {
            cap_f as u64
        };
        needed <= cap && available.saturating_sub(needed) >= self.min_free_bytes
    }

    /// Disk-side gate: the snapshot bundle's on-disk footprint vs the
    /// staging filesystem's available bytes.
    ///
    /// # Errors
    ///
    /// Returns a [`Shortfall`] naming [`HeadroomResource::Disk`] when the
    /// bundle would not fit under the policy.
    pub fn check_disk(&self, needed: u64, available: u64) -> Result<(), Shortfall> {
        self.check(HeadroomResource::Disk, needed, available)
    }

    /// Memory-side gate: the saved `pages.img` working set vs the
    /// kernel's available-memory estimate (restore path only).
    ///
    /// # Errors
    ///
    /// Returns a [`Shortfall`] naming [`HeadroomResource::Memory`] when
    /// the working set would not fit under the policy.
    pub fn check_memory(&self, needed: u64, available: u64) -> Result<(), Shortfall> {
        self.check(HeadroomResource::Memory, needed, available)
    }

    fn check(
        &self,
        resource: HeadroomResource,
        needed: u64,
        available: u64,
    ) -> Result<(), Shortfall> {
        if self.fits(needed, available) {
            Ok(())
        } else {
            Err(Shortfall {
                resource,
                needed,
                available,
                max_fraction: self.max_fraction,
                min_free_bytes: self.min_free_bytes,
            })
        }
    }
}

/// Node resource introspection, behind a trait so the headroom gate can
/// be unit-tested with a fixed reading instead of real syscalls.
///
/// The reads are cheap and synchronous (`statvfs` is a single syscall;
/// `/proc/meminfo` is a small pseudo-file), so callers invoke them inline
/// from async code without a `spawn_blocking` hop.
pub trait NodeStats: Send + Sync {
    /// Bytes available to a writer on the filesystem backing `path`
    /// (`statvfs`: blocks-available × fragment-size).
    ///
    /// # Errors
    ///
    /// Returns a human-readable message on a `statvfs` failure; the
    /// caller wraps it into the path-appropriate
    /// [`sandbawks_types::runtime::RuntimeError`] variant and fails closed.
    fn available_disk_bytes(&self, path: &Path) -> Result<u64, String>;

    /// The kernel's `MemAvailable` estimate, in bytes — memory that can
    /// be handed out to a new allocation without swapping.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when `/proc/meminfo` can't be
    /// read or lacks a `MemAvailable:` line.
    fn available_memory_bytes(&self) -> Result<u64, String>;
}

/// Production [`NodeStats`]: `statvfs(2)` for disk, `/proc/meminfo` for
/// memory.
#[derive(Debug, Clone, Copy, Default)]
pub struct SysNodeStats;

impl NodeStats for SysNodeStats {
    fn available_disk_bytes(&self, path: &Path) -> Result<u64, String> {
        // `f_bavail` (blocks available to an unprivileged process) ×
        // `f_frsize` (fragment size) is the bytes a writer can actually
        // claim — distinct from `f_bfree`, which includes root-reserved
        // blocks the node binary may run as root but the policy treats
        // conservatively.
        let stat = nix::sys::statvfs::statvfs(path)
            .map_err(|e| format!("statvfs({}): {e}", path.display()))?;
        Ok(stat.blocks_available() as u64 * stat.fragment_size() as u64)
    }

    fn available_memory_bytes(&self) -> Result<u64, String> {
        let meminfo = std::fs::read_to_string("/proc/meminfo")
            .map_err(|e| format!("read /proc/meminfo: {e}"))?;
        parse_mem_available_bytes(&meminfo)
            .ok_or_else(|| "no MemAvailable: line in /proc/meminfo".to_string())
    }
}

/// Parse the `MemAvailable:` line (reported in kibibytes) out of
/// `/proc/meminfo` contents and return it in bytes.
///
/// Split out as a pure function so the parse logic is unit-testable
/// without a real `/proc`.
///
/// # Examples
///
/// ```
/// # use sandbawks_node::headroom::parse_mem_available_bytes;
/// let meminfo = "MemTotal:       1630712 kB\nMemAvailable:    8153560 kB\n";
/// assert_eq!(parse_mem_available_bytes(meminfo), Some(8153560 * 1024));
/// ```
pub fn parse_mem_available_bytes(meminfo: &str) -> Option<u64> {
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            // Format: `MemAvailable:    8153560 kB` — the value is the
            // first whitespace-delimited token, always in kB.
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kib.saturating_mul(1024));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn policy() -> HeadroomPolicy {
        // 50% fraction, 1 GiB floor — round numbers for legible asserts.
        HeadroomPolicy::new(0.5, GIB)
    }

    #[test]
    fn ample_headroom_passes_both_dimensions() {
        let p = policy();
        // 1 GiB snapshot, 100 GiB free: well under 50% and leaves >> 1 GiB.
        p.check_disk(GIB, 100 * GIB).expect("disk should fit");
        p.check_memory(GIB, 100 * GIB).expect("memory should fit");
    }

    #[test]
    fn disk_shortfall_is_named_disk() {
        let p = policy();
        // 60 GiB snapshot vs 100 GiB free: exceeds the 50% fraction.
        let err = p
            .check_disk(60 * GIB, 100 * GIB)
            .expect_err("60% of available must be rejected by the 50% fraction");
        assert_eq!(err.resource, HeadroomResource::Disk);
        assert!(err.to_string().contains("disk"));
    }

    #[test]
    fn memory_shortfall_is_named_memory() {
        let p = policy();
        let err = p
            .check_memory(60 * GIB, 100 * GIB)
            .expect_err("60% of available must be rejected by the 50% fraction");
        assert_eq!(err.resource, HeadroomResource::Memory);
        assert!(err.to_string().contains("memory"));
    }

    #[test]
    fn absolute_floor_rejects_small_snapshot_on_full_resource() {
        let p = policy();
        // 700 MiB needed of 1.5 GiB available: under the 50% fraction
        // (cap = 750 MiB), so the fraction check passes — but it leaves
        // only 800 MiB free, below the 1 GiB floor, so the floor trips.
        let available = 1_500 * 1024 * 1024; // 1.5 GiB
        let needed = 700 * 1024 * 1024; // 700 MiB (< 50% cap of 750 MiB)
        let err = p
            .check_disk(needed, available)
            .expect_err("leaving < 1 GiB free must trip the floor");
        assert_eq!(err.resource, HeadroomResource::Disk);
    }

    #[test]
    fn fraction_boundary_is_inclusive() {
        // Exactly at the fraction (50% of 100 GiB = 50 GiB) is allowed,
        // provided the floor is also satisfied (50 GiB free >> 1 GiB).
        let p = policy();
        p.check_disk(50 * GIB, 100 * GIB)
            .expect("exactly max_fraction must be allowed");
        // One byte over the cap is rejected.
        p.check_disk(50 * GIB + 1, 100 * GIB)
            .expect_err("one byte over max_fraction must be rejected");
    }

    #[test]
    fn try_new_accepts_valid_fraction_and_rejects_out_of_range() {
        assert!(HeadroomPolicy::try_new(0.8, GIB).is_ok());
        assert!(HeadroomPolicy::try_new(1.0, 0).is_ok());
        // > 1.0 would silently disable the fraction check.
        assert!(HeadroomPolicy::try_new(1.0001, 0).is_err());
        // <= 0.0 would universally trip the gate.
        assert!(HeadroomPolicy::try_new(0.0, 0).is_err());
        assert!(HeadroomPolicy::try_new(-0.5, 0).is_err());
        // NaN fails the `0.0 < x` half and is rejected.
        assert!(HeadroomPolicy::try_new(f64::NAN, 0).is_err());
    }

    #[test]
    fn zero_available_rejects_any_nonzero_need() {
        let p = HeadroomPolicy::default();
        p.check_disk(1, 0)
            .expect_err("nothing fits when nothing is available");
    }

    #[test]
    fn parse_mem_available_extracts_kib_as_bytes() {
        let meminfo = "\
MemTotal:       1630712 kB
MemFree:         1234567 kB
MemAvailable:    8153560 kB
Buffers:          123456 kB
";
        assert_eq!(parse_mem_available_bytes(meminfo), Some(8_153_560 * 1024));
    }

    #[test]
    fn parse_mem_available_missing_line_is_none() {
        let meminfo = "MemTotal: 16307112 kB\nMemFree: 1234567 kB\n";
        assert_eq!(parse_mem_available_bytes(meminfo), None);
    }
}
