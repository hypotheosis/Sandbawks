//! Storage-agnostic snapshot traits.
//!
//! A snapshot of a paused sandbox is a *content-addressed bundle* of
//! parts: typically a CRIU memory image, a rootfs delta tarball, and
//! an `envd-state.json` describing the userland state.
//! [`SnapshotSink`] is the write-side contract (CRIU/checkpoint code
//! streams parts in) and [`SnapshotSource`] is the read-side contract
//! (restore code streams them back out).
//!
//! Keeping the two traits abstract means the per-runtime `pause` /
//! `resume` code (Plan 05 T7/T8) can be unit-tested against the
//! in-memory `FakeSnapshotStore` (under the `testing` cargo feature)
//! without bringing up a real S3 bucket. Plan 05 T6 ships the
//! production `S3SnapshotSink` / `S3SnapshotSource` impls.
//!
//! ## Part naming convention
//!
//! Part names are opaque to the traits but the Plan 05 spec fixes the
//! production-side convention so the manifest is portable across
//! runtimes:
//!
//! - `memory.zst`          — tar+zstd of the runtime's checkpoint
//!   state files. For gVisor 20240603+ this bundles `checkpoint.img`
//!   (sentry state), `pages.img` (MemoryFile pages — also where the
//!   sandbox-internal overlay-tmpfs upper layer lives), and
//!   `pages_meta.img` (MemoryFile metadata). The name is historical
//!   ("memory image") and kept stable for wire compatibility with the
//!   manifest v2 schema even though the contents are broader than
//!   memory pages alone (see issue #193).
//! - `rootfs-delta.tar.zst` — overlayfs upperdir snapshot. For gVisor
//!   this is typically empty: the default `--overlay2=root:self` keeps
//!   all rootfs writes in the sandbox-internal overlay-tmpfs, which is
//!   captured by `memory.zst` above rather than by containerd's
//!   snapshotter Diff.
//! - `envd-state.json`     — userland process registry
//!
//! Tests are free to use shorter names.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime::{RuntimeError, SnapshotManifest};

/// One uploaded chunk of a snapshot.
///
/// `name` is the part's filename inside the snapshot bundle (e.g.
/// `"memory.zst"`); `sha256` is the lower-case-hex SHA-256 of the
/// *uncompressed* payload bytes; `size_bytes` is the on-the-wire length
/// the sink wrote.
///
/// The pair `(name, sha256)` uniquely identifies a part — two parts
/// with the same content hash are interchangeable, which lets the
/// Plan 05 T6 S3 sink short-circuit re-uploads of identical chunks
/// across pause/resume cycles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPart {
    /// Filename of the part inside the snapshot bundle.
    pub name: String,
    /// Lower-case-hex SHA-256 of the part's bytes.
    pub sha256: String,
    /// On-the-wire (compressed) byte length the sink reported writing.
    pub size_bytes: u64,
    /// Uncompressed payload size in bytes, when the producer knows it.
    ///
    /// `size_bytes` is the *compressed* length. For the `memory.zst`
    /// part that under-counts the RAM working set an in-process `runsc
    /// restore` faults in — zstd ratios of 3-5x on memory pages are
    /// common, so a 2 GiB `memory.zst` can decompress to an ~8 GiB
    /// `pages.img` working set (#243). When the producer knows the
    /// pre-compression total (gVisor `pause` sums the `*.img` file sizes
    /// before zstd), it records it here so the resume-path memory
    /// headroom gate ([`crate::runtime::RuntimeAdapter`] impls'
    /// `enforce_headroom`) can admit on the true working set rather than
    /// the compressed proxy.
    ///
    /// `Option` + `#[serde(default)]` keeps v1/v2 manifests
    /// backward-compatible: absent (the only value an older pauser ever
    /// wrote) → the gate falls back to `size_bytes`, preserving
    /// pre-#243 behavior. `skip_serializing_if` keeps the wire form of
    /// parts that don't carry the figure (rootfs delta, envd state)
    /// byte-identical to the v2 schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncompressed_bytes: Option<u64>,
}

impl SnapshotPart {
    /// Build a [`SnapshotPart`] from a name and a fully-buffered byte
    /// slice.
    ///
    /// The sha256 is computed over `bytes` exactly as supplied — no
    /// canonicalization. Callers that compress or otherwise transform
    /// the payload before storage must hash the *post-transform* bytes
    /// here, since the manifest is the integrity record for what's
    /// actually persisted in the store.
    pub fn from_bytes(name: impl Into<String>, bytes: &[u8]) -> Self {
        Self {
            name: name.into(),
            sha256: sha256_hex(bytes),
            size_bytes: bytes.len() as u64,
            // `from_bytes` only sees the post-transform (e.g. compressed)
            // bytes, so it cannot know the uncompressed size. Producers
            // that have it call `with_uncompressed_bytes` to attach it.
            uncompressed_bytes: None,
        }
    }

    /// Attach the uncompressed payload size, consuming and returning
    /// `self` (builder style).
    ///
    /// Used by the gVisor pauser to record the summed `*.img` byte total
    /// onto the `memory.zst` part returned by [`SnapshotSink::put_part`],
    /// which only knows the compressed length. See
    /// [`SnapshotPart::uncompressed_bytes`].
    #[must_use]
    pub fn with_uncompressed_bytes(mut self, uncompressed_bytes: u64) -> Self {
        self.uncompressed_bytes = Some(uncompressed_bytes);
        self
    }
}

/// Lower-case-hex SHA-256 of an arbitrary byte slice.
///
/// The shared primitive behind [`SnapshotPart::from_bytes`] and the
/// node worker's envd-binary digest (issue #270): both need "the
/// sha256 of these bytes as a 64-char hex string", so the hashing lives
/// in one place rather than being re-spelled at each call site.
///
/// # Examples
///
/// ```
/// use sandbawks_types::snapshot::sha256_hex;
///
/// // The well-known SHA-256 of the empty input.
/// assert_eq!(
///     sha256_hex(b""),
///     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
/// );
/// ```
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

/// Compute the aggregate sha256 of a slice of parts.
///
/// The aggregate is `sha256(sorted_parts.iter().map(|p| p.sha256))` —
/// concatenation of part hashes after sorting by part name. Sorting
/// keeps the aggregate stable across re-orderings of insertions into
/// the manifest, which makes it a usable cache key for "is this exact
/// snapshot already uploaded?".
///
/// Returns the empty string when `parts` is empty, matching the v1
/// `SnapshotManifest` shape so v1→v2 migration is a no-op when there
/// are no parts yet.
pub fn aggregate_sha256(parts: &[SnapshotPart]) -> String {
    if parts.is_empty() {
        return String::new();
    }
    let mut sorted: Vec<&SnapshotPart> = parts.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let mut hasher = Sha256::new();
    for part in &sorted {
        hasher.update(part.sha256.as_bytes());
    }
    hex_lower(&hasher.finalize())
}

/// Errors returned by [`parse_iso8601_duration`].
///
/// Carrying the offending input on the variant makes diagnostic
/// messages on misconfigured `Sandbox.spec.snapshotRetention` or
/// `sandbawks.dev/snapshot-retention` namespace annotations actionable
/// without forcing the caller to thread the raw string through their
/// own error type.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DurationParseError {
    /// The string did not start with `P`, or was otherwise structurally
    /// unparseable as an ISO-8601 duration.
    #[error("not an ISO-8601 duration: {0:?}")]
    Malformed(String),
    /// A unit suffix (`Y`, `M`, `W`, `D`, `H`, `M`, `S`) was missing or
    /// not recognised.
    #[error("unknown unit {unit:?} in duration {input:?}")]
    UnknownUnit { input: String, unit: char },
    /// A numeric segment didn't parse as a non-negative integer.
    #[error("invalid number {number:?} in duration {input:?}")]
    InvalidNumber { input: String, number: String },
    /// `Y` (years) or `M` (calendar months) appeared. Calendar units
    /// have variable lengths and we deliberately refuse them rather
    /// than silently picking an approximation. The error message
    /// names a fixed-length workaround so operators who reach for the
    /// natural "1 month" / "1 year" knob get an actionable next step
    /// without having to read the parser source.
    #[error(
        "calendar units (years / calendar months) are not supported in {0:?} — \
         use a fixed-length alternative (e.g. P30D or P4W for one month, P365D for one year)"
    )]
    CalendarUnit(String),
}

/// Parse an ISO-8601 duration string (`P7D`, `PT24H`, `PT30M`, `P1DT12H`)
/// into a [`std::time::Duration`].
///
/// Only the time-based subset is supported: `D` (days, 86400s), `H`
/// (hours), `M` (minutes — only after the `T` delimiter), and `S`
/// (seconds). `W` (weeks, 7 days) is also accepted for convenience.
/// Calendar years (`Y`) and calendar months (`M` before the `T`) are
/// rejected with [`DurationParseError::CalendarUnit`] because their
/// length is context-dependent and our snapshot retention math
/// expects a fixed offset.
///
/// Returned as `std::time::Duration` rather than `chrono::Duration`
/// so downstream callers don't all have to take a `chrono` dep just
/// to consume the value. Adding the result to a `chrono::DateTime`
/// is a one-liner via `chrono::Duration::from_std`.
///
/// # Examples
///
/// ```
/// use sandbawks_types::snapshot::parse_iso8601_duration;
/// use std::time::Duration;
///
/// assert_eq!(parse_iso8601_duration("P7D").unwrap(), Duration::from_secs(7 * 86_400));
/// assert_eq!(parse_iso8601_duration("PT24H").unwrap(), Duration::from_secs(24 * 3_600));
/// assert_eq!(parse_iso8601_duration("PT1M30S").unwrap(), Duration::from_secs(90));
/// assert!(parse_iso8601_duration("7D").is_err());
/// ```
pub fn parse_iso8601_duration(input: &str) -> Result<std::time::Duration, DurationParseError> {
    let rest = input
        .strip_prefix('P')
        .ok_or_else(|| DurationParseError::Malformed(input.to_owned()))?;
    if rest.is_empty() {
        return Err(DurationParseError::Malformed(input.to_owned()));
    }
    // Split on the `T` delimiter — everything before is days/weeks
    // (date part), everything after is hours/minutes/seconds.
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };

    let mut total_secs: u64 = 0;

    // Date part: only `D` (days) and `W` (weeks) supported. `Y` and `M`
    // are explicitly rejected as calendar units.
    for (count, unit) in iter_segments(date_part, input)? {
        let multiplier = match unit {
            'D' => 86_400,
            'W' => 7 * 86_400,
            'Y' => return Err(DurationParseError::CalendarUnit(input.to_owned())),
            'M' => return Err(DurationParseError::CalendarUnit(input.to_owned())),
            other => {
                return Err(DurationParseError::UnknownUnit {
                    input: input.to_owned(),
                    unit: other,
                })
            }
        };
        total_secs = total_secs.saturating_add(count.saturating_mul(multiplier));
    }

    // Time part: `H`, `M`, `S` — `M` here is minutes, not months.
    if let Some(time) = time_part {
        if time.is_empty() {
            return Err(DurationParseError::Malformed(input.to_owned()));
        }
        for (count, unit) in iter_segments(time, input)? {
            let multiplier = match unit {
                'H' => 3_600,
                'M' => 60,
                'S' => 1,
                other => {
                    return Err(DurationParseError::UnknownUnit {
                        input: input.to_owned(),
                        unit: other,
                    })
                }
            };
            total_secs = total_secs.saturating_add(count.saturating_mul(multiplier));
        }
    }

    Ok(std::time::Duration::from_secs(total_secs))
}

/// Split a segment like `7D` or `1H30M` into `(count, unit)` pairs.
///
/// Internal helper for [`parse_iso8601_duration`]; lives here so the
/// time-part and date-part loops can share the digit-buffer state
/// machine.
fn iter_segments(segment: &str, input: &str) -> Result<Vec<(u64, char)>, DurationParseError> {
    let mut out = Vec::new();
    let mut number = String::new();
    for ch in segment.chars() {
        if ch.is_ascii_digit() {
            number.push(ch);
            continue;
        }
        if number.is_empty() {
            // Hit a unit char with no preceding digits — malformed.
            return Err(DurationParseError::Malformed(input.to_owned()));
        }
        let count = number
            .parse::<u64>()
            .map_err(|_| DurationParseError::InvalidNumber {
                input: input.to_owned(),
                number: number.clone(),
            })?;
        out.push((count, ch));
        number.clear();
    }
    if !number.is_empty() {
        // Trailing digits with no unit — malformed.
        return Err(DurationParseError::Malformed(input.to_owned()));
    }
    Ok(out)
}

/// Lower-case-hex encode a 32-byte SHA-256 digest into a 64-char string.
///
/// Inlined rather than pulling in `hex` / `base16ct` since the rest of
/// the crate already builds a hex string for `TeamAPIKey` the same way.
fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // `write!` into a String is infallible.
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Write-side of a snapshot store.
///
/// Implementations must be idempotent on `name`: re-uploading the same
/// `(name, bytes)` pair MUST succeed without error, because the
/// controller may re-invoke `pause` after a crash. The returned
/// [`SnapshotPart`] is what the runtime adapter folds into the
/// [`SnapshotManifest`] it returns.
#[async_trait]
pub trait SnapshotSink: Send + Sync {
    /// Upload `bytes` under `name`. Returns the recorded
    /// [`SnapshotPart`] (name, hash, size).
    ///
    /// `bytes` is supplied as an owned `Vec<u8>`: fully-buffered, which is
    /// the right shape for the small parts that are already in RAM
    /// (`envd-state.json` in the KB range, an empty gVisor rootfs delta).
    /// For parts whose size scales with the workload — the `memory.zst`
    /// CRIU image, which reaches multiple GiB — callers stage the bundle
    /// to disk and use [`put_part_file`] instead so the part is never
    /// resident in RAM in one piece (#293).
    ///
    /// [`put_part_file`]: SnapshotSink::put_part_file
    async fn put_part(&self, name: &str, bytes: Vec<u8>) -> Result<SnapshotPart, RuntimeError>;

    /// Upload the contents of a local file as part `name`, streaming it
    /// from disk so the sink's peak memory is independent of the part
    /// size (#293).
    ///
    /// The default impl reads `path` fully into a `Vec` and delegates to
    /// [`put_part`], which is fine for the small payloads the in-memory
    /// test doubles handle. The production `S3SnapshotSink` overrides it
    /// to read the file in fixed-size chunks and upload them with bounded
    /// in-flight concurrency, so a multi-GiB `memory.zst` never has to be
    /// resident in the node DaemonSet's RAM at once — the OOM that
    /// motivated #293.
    ///
    /// # Warning
    ///
    /// This default reads the whole file with **synchronous**
    /// `std::fs::read` from an `async` context, so it blocks the calling
    /// executor thread for the duration of the read. That is acceptable
    /// *only* for the test-scale files the in-memory doubles
    /// ([`NoopSnapshotStore`], the `testing` feature's `FakeSnapshotStore`)
    /// ever see. It is deliberately **not** wrapped in `spawn_blocking`:
    /// `sandbawks-types` keeps `tokio` a dev-dependency so the controller
    /// and api release binaries don't link a runtime they don't use.
    /// Any impl that may be handed a real, sizeable on-disk part — i.e.
    /// any production sink — **must override this method** with a
    /// streaming, non-blocking implementation rather than relying on this
    /// fallback.
    ///
    /// [`put_part`]: SnapshotSink::put_part
    async fn put_part_file(
        &self,
        name: &str,
        path: &std::path::Path,
    ) -> Result<SnapshotPart, RuntimeError> {
        // Synchronous read — see the `# Warning` above. Safe here because
        // the only callers of this default hand it KB-scale test files.
        let bytes = std::fs::read(path).map_err(|e| {
            RuntimeError::SnapshotUpload(format!("read part file {}: {e}", path.display()))
        })?;
        self.put_part(name, bytes).await
    }

    /// Persist the manifest itself.
    ///
    /// The manifest is what `SnapshotSource::get_manifest` returns on
    /// the read side, and is the contract the controller stores in
    /// `Sandbox.status.snapshotRef` to find the rest of the bundle.
    async fn put_manifest(&self, manifest: &SnapshotManifest) -> Result<(), RuntimeError>;

    /// Stable URL the controller folds into `Sandbox.status.snapshotRef`.
    ///
    /// Production S3 sinks return `s3://<bucket>/<prefix>/manifest.json`
    /// — the same path `put_manifest` writes to. In-memory test sinks
    /// return a deterministic stub (e.g. `mem://manifest`) so unit
    /// tests can pin the value. The default impl returns the empty
    /// string for adapters that don't preserve state across pause /
    /// resume (e.g. `NoopSnapshotStore`).
    fn manifest_url(&self) -> String {
        String::new()
    }

    /// Return a sink that reads/writes under an additional per-snapshot
    /// `sub_prefix` nested beneath this store's base location.
    ///
    /// The pause path calls this with the sandbox's own id so every sandbox
    /// gets a disjoint keyspace (`<base>/<sandbox-id>/manifest.json`,
    /// `<base>/<sandbox-id>/parts/...`). Without it every sandbox shares one
    /// mutable `manifest.json` / `parts/<name>.json` key, so two sandboxes
    /// snapshotting against the same bucket clobber each other's bundles —
    /// a resume then reassembles a part that doesn't match its manifest hash
    /// (the cross-sandbox corruption this method exists to prevent).
    /// [`manifest_url`] on the returned sink reflects the scoped path, so the
    /// `snapshotRef` the controller records points at the per-sandbox bundle.
    ///
    /// Stores that don't key by prefix (the in-memory test doubles) may
    /// return a handle over the same backing state — the read side scopes
    /// with the same `sub_prefix`, so a single sandbox's pause→resume still
    /// round-trips.
    ///
    /// [`manifest_url`]: SnapshotSink::manifest_url
    fn scoped(&self, sub_prefix: &str) -> std::sync::Arc<dyn SnapshotSink>;
}

/// Read-side of a snapshot store.
///
/// Implementations are expected to return the same bytes that the
/// matching [`SnapshotSink::put_part`] call wrote — no compression,
/// no canonicalization. If `name` is unknown, return
/// [`RuntimeError::SnapshotDownload`].
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    /// Fetch the bytes previously stored under `name`.
    ///
    /// Buffers the whole part in RAM — the right shape for the small parts
    /// (`envd-state.json`, an empty gVisor rootfs delta) and the in-memory
    /// test doubles. For parts whose size scales with the workload — the
    /// multi-GiB `memory.zst` CRIU image — callers stream to disk with
    /// [`get_part_to_file`] instead so the part is never resident in RAM in
    /// one piece (#329).
    ///
    /// [`get_part_to_file`]: SnapshotSource::get_part_to_file
    async fn get_part(&self, name: &str) -> Result<Vec<u8>, RuntimeError>;

    /// Download part `name` straight to the local file `path`, streaming it
    /// from the store so the source's peak memory is independent of the part
    /// size (#329). Returns the [`SnapshotPart`] (name, sha256, length)
    /// observed while writing, so the caller can cross-check it against the
    /// controller-committed manifest entry without re-reading the file.
    ///
    /// This is the read-side mirror of [`SnapshotSink::put_part_file`]: the
    /// pause path streamed the bundle *up* to flat RAM (#293); without the
    /// symmetric streamed *download* the validate / resume path still
    /// buffered the whole part in a `Vec<u8>`, OOM-killing the node
    /// DaemonSet at GiB scale under its 1Gi memory limit (#329).
    ///
    /// The default impl fetches the part with [`get_part`] and writes it in
    /// one shot, which is fine for the small payloads the in-memory test
    /// doubles handle.
    ///
    /// # Partial writes on error
    ///
    /// On a mid-stream failure (e.g. a corrupt chunk) an implementation MAY
    /// return `Err` with a partially-written file left at `path` — the bytes
    /// streamed before the failure. Callers must not treat `path` as valid
    /// unless this method returned `Ok`, and should hand a `path` inside a
    /// scratch directory they clean up on error (as
    /// `GvisorAdapter::fetch_verify_and_lay_out` does for its per-pause
    /// work-dir) rather than a long-lived location.
    ///
    /// # Warning
    ///
    /// This default both buffers the entire part (via [`get_part`]) and
    /// writes it with **synchronous** `std::fs::write` from an `async`
    /// context, blocking the calling executor thread. That is acceptable
    /// *only* for the test-scale files the in-memory doubles
    /// ([`NoopSnapshotStore`], the `testing` feature's `FakeSnapshotStore`)
    /// ever see — and it is deliberately not wrapped in `spawn_blocking`
    /// because `sandbawks-types` keeps `tokio` a dev-dependency so the
    /// controller and api release binaries don't link a runtime they don't
    /// use. Any source that may be handed a real, multi-GiB part — i.e. any
    /// production source — **must override this method** with a streaming,
    /// non-blocking implementation rather than relying on this fallback.
    ///
    /// [`get_part`]: SnapshotSource::get_part
    /// [`put_part_file`]: SnapshotSink::put_part_file
    async fn get_part_to_file(
        &self,
        name: &str,
        path: &std::path::Path,
    ) -> Result<SnapshotPart, RuntimeError> {
        let bytes = self.get_part(name).await?;
        let part = SnapshotPart::from_bytes(name, &bytes);
        // Synchronous write — see the `# Warning` above. Safe here because
        // the only callers of this default hand it KB-scale test parts.
        std::fs::write(path, &bytes).map_err(|e| {
            RuntimeError::SnapshotDownload(format!("write part file {}: {e}", path.display()))
        })?;
        Ok(part)
    }

    /// Fetch the manifest previously stored via [`SnapshotSink::put_manifest`].
    async fn get_manifest(&self) -> Result<SnapshotManifest, RuntimeError>;

    /// Cheaply confirm `part`'s objects are present and correctly sized
    /// **without downloading their bodies** — the work behind the "head+size"
    /// pause-validate level (issue #235).
    ///
    /// The default falls back to a full [`get_part`](SnapshotSource::get_part)
    /// (download + sha256 verify) and discards the bytes, so it's a valid — if
    /// costlier — presence proof for any backend. A backend that can do better
    /// (the S3 source HEADs each chunk and compares content-lengths) overrides
    /// it. Returns [`RuntimeError::SnapshotDownload`] if an object is missing
    /// or the wrong size.
    async fn verify_part_present(&self, part: &SnapshotPart) -> Result<(), RuntimeError> {
        self.get_part(&part.name).await.map(|_| ())
    }

    /// Read-side mirror of [`SnapshotSink::scoped`]: return a source that
    /// reads under the same per-snapshot `sub_prefix` the pause path wrote.
    ///
    /// Resume scopes with the sandbox's own id (the same value pause used),
    /// so the keys it reads always agree with the keys pause wrote without
    /// having to parse the recorded `snapshotRef` URL.
    fn scoped(&self, sub_prefix: &str) -> std::sync::Arc<dyn SnapshotSource>;
}

/// A no-op snapshot store used as a placeholder by adapters that do
/// not preserve sandbox state across pause/resume.
///
/// Both trait impls succeed-with-empty / fail-on-fetch:
///
/// - [`SnapshotSink::put_part`]: returns a [`SnapshotPart`] computed
///   over `bytes` but discards the data.
/// - [`SnapshotSink::put_manifest`]: returns `Ok(())` without storing.
/// - [`SnapshotSource::get_part`] / `get_manifest`: return
///   [`RuntimeError::SnapshotDownload`] since nothing is stored.
///
/// The controller's reconcile loop wires this in by default during
/// Plan 05 T3 (this commit); Plan 05 T4 replaces direct sink/source
/// passing with `SnapshotJob` CR creation and the per-node worker
/// constructs a real `S3SnapshotSink` / `S3SnapshotSource` there.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSnapshotStore;

#[async_trait]
impl SnapshotSink for NoopSnapshotStore {
    async fn put_part(&self, name: &str, bytes: Vec<u8>) -> Result<SnapshotPart, RuntimeError> {
        Ok(SnapshotPart::from_bytes(name, &bytes))
    }

    async fn put_manifest(&self, _manifest: &SnapshotManifest) -> Result<(), RuntimeError> {
        Ok(())
    }

    // A store that preserves nothing has nothing to namespace: the scoped
    // handle is just another `NoopSnapshotStore`.
    fn scoped(&self, _sub_prefix: &str) -> std::sync::Arc<dyn SnapshotSink> {
        std::sync::Arc::new(*self)
    }
}

#[async_trait]
impl SnapshotSource for NoopSnapshotStore {
    async fn get_part(&self, name: &str) -> Result<Vec<u8>, RuntimeError> {
        Err(RuntimeError::SnapshotDownload(format!(
            "NoopSnapshotStore has no part {name}"
        )))
    }

    async fn get_manifest(&self) -> Result<SnapshotManifest, RuntimeError> {
        Err(RuntimeError::SnapshotDownload(
            "NoopSnapshotStore has no manifest".into(),
        ))
    }

    fn scoped(&self, _sub_prefix: &str) -> std::sync::Arc<dyn SnapshotSource> {
        std::sync::Arc::new(*self)
    }
}

/// Which object-storage backend the snapshot layer talks to.
///
/// Cluster-global: chosen once at process startup from
/// `SANDBAWKS_SNAPSHOT_PROVIDER` (see the node/controller binaries), not
/// per-Sandbox. `S3` keeps the long-standing `rust-s3` path; `Azure`
/// selects the native Azure Blob Storage backend (account-key auth).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotProvider {
    /// S3 / S3-compatible (AWS, MinIO, garage) via `rust-s3`. The default
    /// so existing deployments are unaffected by the new env var.
    #[default]
    S3,
    /// Azure Blob Storage via `azure_storage_blobs`.
    Azure,
}

/// Error returned when `SANDBAWKS_SNAPSHOT_PROVIDER` holds an unknown value.
#[derive(Debug, thiserror::Error)]
#[error("unknown snapshot provider {0:?} (expected \"s3\" or \"azure\")")]
pub struct SnapshotProviderParseError(String);

impl std::str::FromStr for SnapshotProvider {
    type Err = SnapshotProviderParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Case- and whitespace-insensitive so a misformatted env var
        // (`Azure\n`, ` S3 `) parses the way an operator expects.
        match s.trim().to_ascii_lowercase().as_str() {
            "s3" => Ok(Self::S3),
            "azure" => Ok(Self::Azure),
            other => Err(SnapshotProviderParseError(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_provider_parses_case_insensitively() {
        assert_eq!(
            "s3".parse::<SnapshotProvider>().unwrap(),
            SnapshotProvider::S3
        );
        assert_eq!(
            "S3".parse::<SnapshotProvider>().unwrap(),
            SnapshotProvider::S3
        );
        assert_eq!(
            "azure".parse::<SnapshotProvider>().unwrap(),
            SnapshotProvider::Azure
        );
        assert_eq!(
            "AZURE".parse::<SnapshotProvider>().unwrap(),
            SnapshotProvider::Azure
        );
        assert_eq!(
            " azure\n".parse::<SnapshotProvider>().unwrap(),
            SnapshotProvider::Azure
        );
    }

    #[test]
    fn snapshot_provider_defaults_to_s3() {
        assert_eq!(SnapshotProvider::default(), SnapshotProvider::S3);
    }

    #[test]
    fn snapshot_provider_rejects_unknown() {
        let err = "gcs".parse::<SnapshotProvider>().unwrap_err();
        assert!(
            err.to_string().contains("gcs"),
            "error names the bad value: {err}"
        );
    }

    #[test]
    fn aggregate_sha256_is_stable_under_reordering() {
        let a = SnapshotPart::from_bytes("a.bin", b"alpha");
        let b = SnapshotPart::from_bytes("b.bin", b"beta");
        let forward = aggregate_sha256(&[a.clone(), b.clone()]);
        let reverse = aggregate_sha256(&[b, a]);
        assert_eq!(forward, reverse);
    }

    #[test]
    fn aggregate_sha256_changes_when_content_changes() {
        let a1 = SnapshotPart::from_bytes("a.bin", b"alpha");
        let a2 = SnapshotPart::from_bytes("a.bin", b"alpha2");
        assert_ne!(aggregate_sha256(&[a1]), aggregate_sha256(&[a2]));
    }

    #[test]
    fn aggregate_sha256_empty_is_empty_string() {
        assert_eq!(aggregate_sha256(&[]), "");
    }

    #[test]
    fn parse_iso8601_duration_handles_examples_from_issue() {
        // The issue body cites `P7D` and `PT24H` as the wire format —
        // pin those exactly so a refactor that breaks them is loud.
        assert_eq!(
            parse_iso8601_duration("P7D").unwrap(),
            std::time::Duration::from_secs(7 * 86_400)
        );
        assert_eq!(
            parse_iso8601_duration("PT24H").unwrap(),
            std::time::Duration::from_secs(24 * 3_600)
        );
    }

    #[test]
    fn parse_iso8601_duration_handles_combined_date_and_time() {
        let got = parse_iso8601_duration("P1DT12H30M").unwrap();
        // 1 day + 12 hours + 30 minutes = 131_400 seconds.
        assert_eq!(got, std::time::Duration::from_secs(131_400));
    }

    #[test]
    fn parse_iso8601_duration_handles_weeks_and_seconds() {
        assert_eq!(
            parse_iso8601_duration("P1W").unwrap(),
            std::time::Duration::from_secs(7 * 86_400)
        );
        assert_eq!(
            parse_iso8601_duration("PT45S").unwrap(),
            std::time::Duration::from_secs(45)
        );
    }

    #[test]
    fn parse_iso8601_duration_rejects_missing_p_prefix() {
        assert!(matches!(
            parse_iso8601_duration("7D"),
            Err(DurationParseError::Malformed(_))
        ));
    }

    #[test]
    fn parse_iso8601_duration_rejects_calendar_units() {
        // `Y` (years) — variable-length, ambiguous for retention math.
        assert!(matches!(
            parse_iso8601_duration("P1Y"),
            Err(DurationParseError::CalendarUnit(_))
        ));
        // `M` *before* the `T` is calendar-months (also rejected). `M`
        // *after* the `T` is minutes (accepted) — guarded by the test
        // below.
        assert!(matches!(
            parse_iso8601_duration("P1M"),
            Err(DurationParseError::CalendarUnit(_))
        ));
    }

    #[test]
    fn parse_iso8601_duration_calendar_error_suggests_fixed_length_alternative() {
        // Pin the operator-facing hint in the error message. Calendar
        // units are a natural reach ("keep snapshots one month"); the
        // message must name a fixed-length workaround so the operator
        // doesn't have to read the parser source to recover.
        let rendered = parse_iso8601_duration("P1M").unwrap_err().to_string();
        assert!(
            rendered.contains("P30D") || rendered.contains("P4W"),
            "calendar-unit error should suggest P30D / P4W; got: {rendered}"
        );
    }

    #[test]
    fn parse_iso8601_duration_minutes_after_t_is_not_a_calendar_unit() {
        // Guard the M-before-T vs M-after-T distinction so a future
        // tweak to the date-part loop doesn't accidentally swallow
        // `PT30M` as a calendar month.
        assert_eq!(
            parse_iso8601_duration("PT30M").unwrap(),
            std::time::Duration::from_secs(30 * 60)
        );
    }

    #[test]
    fn parse_iso8601_duration_rejects_trailing_garbage() {
        // Trailing digits with no unit.
        assert!(matches!(
            parse_iso8601_duration("P7"),
            Err(DurationParseError::Malformed(_))
        ));
        // Empty after the `T`.
        assert!(matches!(
            parse_iso8601_duration("PT"),
            Err(DurationParseError::Malformed(_))
        ));
        // Empty body.
        assert!(matches!(
            parse_iso8601_duration("P"),
            Err(DurationParseError::Malformed(_))
        ));
    }

    #[test]
    fn from_bytes_hashes_match_known_vector() {
        // SHA-256 of "abc" is the canonical NIST test vector.
        let part = SnapshotPart::from_bytes("abc", b"abc");
        assert_eq!(
            part.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(part.size_bytes, 3);
    }

    #[test]
    fn sha256_hex_matches_known_vector_and_from_bytes() {
        // Same NIST "abc" vector, proving the standalone helper (issue
        // #270's envd-binary digest) agrees with `SnapshotPart::from_bytes`
        // — they must, since the latter now delegates to it (DRY).
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            SnapshotPart::from_bytes("x", b"abc").sha256
        );
    }

    #[test]
    fn part_without_uncompressed_field_deserializes_to_none() {
        // A part serialized before #243 has no `uncompressedBytes` key.
        // `#[serde(default)]` must map that to `None` so old manifests
        // load and the headroom gate falls back to the compressed size.
        let v2_json = r#"{"name":"memory.zst","sha256":"ab","sizeBytes":42}"#;
        let part: SnapshotPart = serde_json::from_str(v2_json).expect("v2 part parses");
        assert_eq!(part.uncompressed_bytes, None);
    }

    #[test]
    fn part_uncompressed_field_round_trips_and_is_omitted_when_absent() {
        // Present → serialized under the camelCase key and read back.
        let with = SnapshotPart::from_bytes("memory.zst", b"zzz").with_uncompressed_bytes(9_001);
        let json = serde_json::to_string(&with).expect("serialize");
        assert!(
            json.contains("\"uncompressedBytes\":9001"),
            "expected camelCase uncompressedBytes in {json}"
        );
        let back: SnapshotPart = serde_json::from_str(&json).expect("round-trip");
        assert_eq!(back.uncompressed_bytes, Some(9_001));

        // Absent → `skip_serializing_if` omits the key entirely, keeping
        // the wire form identical to the pre-#243 v2 schema.
        let without = SnapshotPart::from_bytes("envd-state.json", b"{}");
        let json = serde_json::to_string(&without).expect("serialize");
        assert!(
            !json.contains("uncompressedBytes"),
            "absent uncompressed size must not appear on the wire: {json}"
        );
    }
}
