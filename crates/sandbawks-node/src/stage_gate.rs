//! The cross-runtime restore staging gate (epic #484, issue #511).
//!
//! Scheduler-placed restore is *create-then-stage*: the controller creates the
//! restore Pod **before** the snapshot exists on any node, the scheduler places
//! it, and only then does the node stage the bundle onto that node — dropping the
//! [`sandbawks_types::STAGED_MARKER_FILENAME`] marker *last*, after every part is
//! on disk (see [`crate::gvisor::write_staged_marker`], the writer side). A
//! restore that begins before that marker appears would restore from an
//! empty/partial directory.
//!
//! **Where the wait lives is runtime-specific; the marker is the
//! runtime-agnostic contract.** For gVisor the wait lives in the Go
//! `containerd-shim-sandbawks-v1` (`waitForStagedImage` in
//! `shim/internal/wrapper/marker.go`), which blocks the root container's
//! `runsc restore` at `Start` — it can *not* be a Pod init container, because a
//! gVisor memory checkpoint restores as a whole sandbox and runsc rejects any
//! container absent from the checkpoint.
//!
//! The planned Rust [`sandbawks_types::RuntimeAdapter`] impls do **not** use that
//! Go shim, so they would not inherit the wait for free:
//!
//! - `KataFcAdapter` (whole-VM snapshot) needs a shim/path-level wait just like
//!   gVisor — [`wait_for_staged_image`] is exactly that wait, callable from the
//!   adapter's restore-trigger point.
//! - `RuncCriuAdapter` (per-container CRIU restore) *may* instead gate via a Pod
//!   init container — the option gVisor can't take. If it does, the init
//!   container owns the wait and this helper is unused for that adapter; if it
//!   drives restore from the adapter itself, it calls [`wait_for_staged_image`].
//!
//! This module is the Rust counterpart to the Go shim's `waitForStagedImage`, so
//! a new Rust adapter honors the contract by *calling* one tested helper rather
//! than re-deriving it (issue #511). It mirrors the shim's tunables — the
//! [`STAGE_GATE_TIMEOUT_ENV`] override, the [`DEFAULT_STAGE_GATE_TIMEOUT`]
//! ceiling, and the [`MARKER_POLL_INTERVAL`] cadence — so both restore paths
//! behave identically and a cluster tunes them with one knob.

use std::io::ErrorKind;
use std::path::Path;
use std::time::Duration;

use sandbawks_types::runtime::RuntimeError;
use sandbawks_types::STAGED_MARKER_FILENAME;
use tokio::time::Instant;

/// Environment variable that overrides [`DEFAULT_STAGE_GATE_TIMEOUT`].
///
/// Mirrors the Go shim's `SANDBAWKS_STAGE_GATE_TIMEOUT_SECS` so a cluster tunes
/// the gVisor (shim) and Rust-adapter restore paths with a single knob. Read
/// from the process environment (a node DaemonSet / shim env var), not from
/// Helm — keeping the timeout per-environment configurable rather than baked
/// into the binary. An unset, blank, zero, or unparseable value falls back to
/// the default; see [`parse_stage_gate_timeout`].
pub const STAGE_GATE_TIMEOUT_ENV: &str = "SANDBAWKS_STAGE_GATE_TIMEOUT_SECS";

/// Default ceiling on how long [`wait_for_staged_image`] blocks the restore on
/// the marker.
///
/// Sized for a GiB-scale bundle staging onto a cold node. The wait returns the
/// instant the marker appears, so a generous ceiling costs nothing on the happy
/// path — it only bounds the failure case. Matches the Go shim's
/// `defaultStageGateTimeout`.
pub const DEFAULT_STAGE_GATE_TIMEOUT: Duration = Duration::from_secs(300);

/// How often [`wait_for_staged_image`] re-stats the marker.
///
/// A quarter second keeps post-staging restore latency low without
/// busy-spinning. Matches the Go shim's `markerPollInterval`.
pub const MARKER_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Resolve the staging-gate timeout from the process environment.
///
/// Reads [`STAGE_GATE_TIMEOUT_ENV`] and parses it via
/// [`parse_stage_gate_timeout`], falling back to [`DEFAULT_STAGE_GATE_TIMEOUT`]
/// on any unset/blank/zero/unparseable value — a misconfigured env degrades to a
/// sane bound, never to a zero-length (instant-fail) wait.
///
/// # Returns
///
/// The configured timeout, or [`DEFAULT_STAGE_GATE_TIMEOUT`].
pub fn stage_gate_timeout() -> Duration {
    // `std::env::var` returns `Err` for both unset and non-UTF-8 values; either
    // way `.ok()` yields `None`, which `parse_stage_gate_timeout` maps to the
    // default. Borrowing `.as_deref()` avoids cloning the string into the parser.
    parse_stage_gate_timeout(std::env::var(STAGE_GATE_TIMEOUT_ENV).ok().as_deref())
}

/// Pure parser for the staging-gate timeout, split out from
/// [`stage_gate_timeout`] so the fallback logic is unit-testable without
/// mutating the (process-global, test-racy) environment.
///
/// # Arguments
///
/// * `raw` - The raw env value (`None` when the variable is unset).
///
/// # Returns
///
/// `raw` parsed as a whole number of seconds, or [`DEFAULT_STAGE_GATE_TIMEOUT`]
/// when `raw` is `None`, blank, zero, or not a non-negative integer.
fn parse_stage_gate_timeout(raw: Option<&str>) -> Duration {
    match raw.map(str::trim) {
        // `0` is rejected on purpose: a zero-length wait would fail closed
        // instantly, defeating the gate. Treat it like a misconfiguration.
        Some(s) if !s.is_empty() => match s.parse::<u64>() {
            Ok(secs) if secs > 0 => Duration::from_secs(secs),
            _ => DEFAULT_STAGE_GATE_TIMEOUT,
        },
        _ => DEFAULT_STAGE_GATE_TIMEOUT,
    }
}

/// Block until the node has finished staging the restore bundle at `image_path`
/// — signalled by the [`STAGED_MARKER_FILENAME`] marker appearing inside it — or
/// until `timeout` elapses.
///
/// This is the Rust counterpart to the Go shim's `waitForStagedImage`, for
/// [`sandbawks_types::RuntimeAdapter`] impls that drive restore themselves
/// (rather than via the gVisor shim or a Pod init container). Call it at the
/// adapter's restore-trigger point, passing the directory named by the restore
/// Pod's `io.sandbawks/restore-image-path` annotation (equivalently
/// [`sandbawks_types::SnapshotJobSpec::target_path`]), before invoking the
/// runtime's restore.
///
/// The marker is checked **before** the first sleep, so an already-staged image
/// returns immediately. A non-`NotFound` stat error (e.g. a permission problem
/// on the staging mount) is returned rather than retried — it will not
/// self-heal. The per-poll sleep is clamped to the time remaining so the wait
/// never overshoots `timeout`.
///
/// # Arguments
///
/// * `image_path` - Directory the bundle is being staged into.
/// * `timeout` - Upper bound on the wait; see [`stage_gate_timeout`].
/// * `poll` - Re-stat cadence; a non-positive value falls back to
///   [`MARKER_POLL_INTERVAL`].
///
/// # Errors
///
/// Returns [`RuntimeError::RestoreFailed`] when the marker does not appear
/// within `timeout`, or when stat-ing it fails for any reason other than the
/// marker being absent. Surfacing a timed-out/failed wait as a restore-start
/// failure lets the controller's bounded `Resuming` recovery loop recreate the
/// restore Pod (`RestorePodLost`) rather than restore from an incomplete bundle
/// (issue #511).
///
/// # Examples
///
/// ```ignore
/// use std::path::Path;
/// use sandbawks_node::stage_gate::{stage_gate_timeout, wait_for_staged_image, MARKER_POLL_INTERVAL};
///
/// // At an adapter's restore-trigger point, before `criu restore` / VM resume:
/// wait_for_staged_image(Path::new(target_path), stage_gate_timeout(), MARKER_POLL_INTERVAL).await?;
/// // ... marker present: every part is on disk, safe to restore.
/// # Ok::<(), sandbawks_types::runtime::RuntimeError>(())
/// ```
pub async fn wait_for_staged_image(
    image_path: &Path,
    timeout: Duration,
    poll: Duration,
) -> Result<(), RuntimeError> {
    // A non-positive poll would busy-spin (or, at zero, never advance the
    // clock under a paused test runtime); clamp to the sane default.
    let poll = if poll.is_zero() {
        MARKER_POLL_INTERVAL
    } else {
        poll
    };
    let marker = image_path.join(STAGED_MARKER_FILENAME);
    // `tokio::time::Instant` (not `std::time::Instant`) so `start_paused` tests
    // can drive the deadline with `tokio::time::advance`.
    let deadline = Instant::now() + timeout;

    loop {
        match tokio::fs::metadata(&marker).await {
            // Marker present: staging dropped it last, so the bundle is complete.
            Ok(_) => return Ok(()),
            // Not there yet — keep polling until the deadline.
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            // Any other stat error won't self-heal; fail closed immediately.
            Err(e) => {
                return Err(RuntimeError::RestoreFailed(format!(
                    "stat staged marker {}: {e}",
                    marker.display()
                )));
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(RuntimeError::RestoreFailed(format!(
                "staged restore image {} not ready: marker {} did not appear within {}s",
                image_path.display(),
                marker.display(),
                timeout.as_secs(),
            )));
        }
        // Clamp the sleep to the remaining time so the wait honors `timeout`
        // exactly even when `poll` is coarse relative to what's left.
        tokio::time::sleep((deadline - now).min(poll)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `parse_stage_gate_timeout` accepts a valid positive integer.
    #[test]
    fn parse_accepts_positive_seconds() {
        assert_eq!(
            parse_stage_gate_timeout(Some("120")),
            Duration::from_secs(120)
        );
        // Surrounding whitespace is tolerated (mirrors the Go shim's TrimSpace).
        assert_eq!(
            parse_stage_gate_timeout(Some("  90 ")),
            Duration::from_secs(90)
        );
    }

    /// Unset / blank / zero / unparseable all degrade to the default, never to a
    /// zero-length wait.
    #[test]
    fn parse_falls_back_to_default() {
        for raw in [
            None,
            Some(""),
            Some("   "),
            Some("0"),
            Some("-5"),
            Some("abc"),
        ] {
            assert_eq!(
                parse_stage_gate_timeout(raw),
                DEFAULT_STAGE_GATE_TIMEOUT,
                "expected default for {raw:?}"
            );
        }
    }

    /// An already-staged image returns `Ok` without waiting (marker checked
    /// before the first sleep).
    #[tokio::test]
    async fn returns_immediately_when_marker_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        tokio::fs::write(dir.path().join(STAGED_MARKER_FILENAME), b"x")
            .await
            .expect("write marker");

        wait_for_staged_image(
            dir.path(),
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await
        .expect("staged image should return Ok");
    }

    /// The wait returns once the marker appears partway through.
    #[tokio::test]
    async fn returns_after_marker_appears() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join(STAGED_MARKER_FILENAME);

        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tokio::fs::write(&marker, b"x").await.expect("write marker");
        });

        wait_for_staged_image(dir.path(), Duration::from_secs(2), Duration::from_millis(5))
            .await
            .expect("should return Ok after marker appears");
        writer.await.expect("writer task");
    }

    /// A bundle that never stages times out as a restore-start failure.
    #[tokio::test]
    async fn times_out_when_never_staged() {
        let dir = tempfile::tempdir().expect("tempdir");

        let err = wait_for_staged_image(
            dir.path(),
            Duration::from_millis(30),
            Duration::from_millis(5),
        )
        .await
        .expect_err("an unstaged image must time out");

        assert!(
            matches!(err, RuntimeError::RestoreFailed(_)),
            "timeout must surface as RestoreFailed, got {err:?}"
        );
    }
}
