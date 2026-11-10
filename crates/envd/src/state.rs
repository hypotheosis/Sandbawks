//! Pause/resume state snapshot for envd.
//!
//! On pause, envd writes its in-memory bookkeeping — running processes,
//! allocated PTYs, registered filesystem watches — to `envd-state.json`
//! next to `memory.zst` and `rootfs-delta.tar.zst` in the snapshot
//! bundle (spec §6.1). On resume, T7 reads the file and rebuilds those
//! registries so PID listings, watcher ids, and PTY ids stay continuous
//! across the pause boundary.
//!
//! # What lives here vs. the runtime's checkpoint
//!
//! envd only owns the *bookkeeping* about its handle tables. The actual
//! process memory, open kernel fds, and PTY line-discipline state are
//! the gVisor adapter's responsibility (Plan 05). When the adapter
//! restores a process, envd consults this snapshot to re-mint its own
//! entry so that callers see a continuous pid.
//!
//! # Determinism
//!
//! The round-trip property test in `tests/state_properties.rs`
//! requires that serializing the same [`EnvdState`] twice produces
//! byte-identical JSON. That holds because every collection in the
//! type tree is either a `Vec` (preserves insertion order) or a
//! [`BTreeMap`] (orders by key). A `HashMap` here would silently
//! introduce nondeterminism via its hasher's seed.
//!
//! # Forward compatibility
//!
//! Serde's default behavior is to ignore unknown JSON fields on
//! deserialize, so a future schema can grow new optional fields and an
//! older envd will still load the file (it just won't see the new
//! fields). The `forwards_compat_unknown_field_ignored` integration
//! test pins this invariant — do not add `#[serde(deny_unknown_fields)]`
//! to any of the structs below without bumping [`SCHEMA_VERSION`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::fingerprint::RootfsFingerprint;

/// Current schema version of the on-disk `envd-state.json` payload.
///
/// Bumped only when the wire shape changes in a backwards-incompatible
/// way; additive changes (new optional fields with a default) keep the
/// version stable because serde ignores unknown fields by default.
pub const SCHEMA_VERSION: u32 = 1;

/// Top-level pause/resume snapshot of the envd agent.
///
/// Serialized as a single JSON object. Sized in the KB range — there's
/// no need for streaming or compression; the whole file is read and
/// written atomically by [`EnvdState::write_to`] / [`EnvdState::read_from`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvdState {
    /// Schema version. Always [`SCHEMA_VERSION`] for fresh captures;
    /// [`EnvdState::restore`] rejects a mismatch with
    /// [`RestoreError::VersionMismatch`].
    pub version: u32,
    /// Snapshots of running and recently-exited processes from the
    /// Process service's registry.
    pub processes: Vec<ProcessSnapshot>,
    /// Snapshots of allocated PTYs from the PTY service's registry.
    pub ptys: Vec<PtySnapshot>,
    /// Snapshots of filesystem watch subscriptions from the Filesystem
    /// service's watcher registry.
    pub watches: Vec<WatchSnapshot>,
    /// Deterministic fingerprint of the writable rootfs captured at
    /// pause, the resume divergence oracle (#236). `None` for captures
    /// that ran without a rootfs path (the pure [`EnvdState::capture`]
    /// constructor, and snapshots written by an envd predating this
    /// field). On resume the value is recomputed over the restored
    /// filesystem and compared; a mismatch fails the resume.
    ///
    /// Additive: `#[serde(default)]` makes an absent field decode to
    /// `None`, so snapshots written before #236 still load and
    /// [`SCHEMA_VERSION`] is **not** bumped. See the module-level
    /// forward-compatibility note.
    ///
    /// This is an *internal envd field*: it is **not** an input to
    /// `sandbawks_types::snapshot::aggregate_sha256` or the per-part
    /// bundle hashes. The snapshot-integrity gate (#235) treats
    /// `envd-state.json` as opaque payload, so a future reader should not
    /// be surprised that two same-pod re-pauses produce `envd-state.json`
    /// files with *different* SHA-256s — the embedded fingerprint differs
    /// when the rootfs differs, by design.
    #[serde(default)]
    pub rootfs_fingerprint: Option<RootfsFingerprint>,
}

/// Bookkeeping snapshot of one entry in the Process registry.
///
/// Mirrors the public fields the `ProcessInfo` gRPC type carries, plus
/// `started_at` (unix epoch seconds) which is needed so the post-resume
/// process listing keeps the same wall-clock age the pre-pause one had.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSnapshot {
    /// OS pid the process had pre-pause. The runtime's checkpoint
    /// (Plan 05) is responsible for re-establishing this same pid on
    /// resume; envd only stores it so the registry key matches.
    pub pid: u32,
    /// Command (`argv[0]`) the process was started with.
    pub cmd: String,
    /// Arguments (`argv[1..]`) the process was started with.
    pub args: Vec<String>,
    /// Environment variables in lexicographic-by-key order. The
    /// `BTreeMap` (rather than `HashMap`) is load-bearing for the
    /// determinism property — see the module docs.
    pub env: BTreeMap<String, String>,
    /// Working directory at start. `PathBuf` rather than `String` so
    /// downstream code keeps a typed path; non-UTF-8 paths are not
    /// supported by the JSON encoding and would already fail at the
    /// serde boundary.
    pub cwd: PathBuf,
    /// Unix epoch seconds at which the process was spawned.
    pub started_at: i64,
}

/// Bookkeeping snapshot of one entry in the PTY registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PtySnapshot {
    /// Opaque PTY id minted by the PTY service. Unrelated to the OS
    /// pid; carried across pause so existing client-side handles stay
    /// valid post-resume.
    pub id: u64,
    /// OS pid of the PTY-backed process this PTY is attached to.
    pub pid: u32,
    /// Last-known terminal row count.
    pub rows: u16,
    /// Last-known terminal column count.
    pub cols: u16,
    /// Command the PTY's child was started with.
    pub cmd: String,
    /// Arguments the PTY's child was started with.
    pub args: Vec<String>,
}

/// Bookkeeping snapshot of one entry in the Filesystem watcher registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchSnapshot {
    /// Path the watcher was registered against.
    pub path: PathBuf,
    /// Opaque subscriber id the watch was minted under.
    pub subscriber_id: u64,
}

/// Live, snapshot-shaped registry of process bookkeeping.
///
/// Decoupled from the in-flight `ProcessService.entries` map because
/// that map owns runtime-only state (broadcast channels, tokio task
/// handles, ownership of the child) that does not survive a pause.
/// Plan 03 T6 lands the snapshot container; the wiring that walks
/// `entries` to populate it is T7's job.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ProcessRegistry {
    snapshots: Vec<ProcessSnapshot>,
}

impl ProcessRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry from a pre-existing batch of snapshots, in the
    /// caller-provided order. Used by [`EnvdState::restore`] to hand
    /// the freshly-decoded snapshots back without an extra allocation.
    pub fn from_snapshots(snapshots: Vec<ProcessSnapshot>) -> Self {
        Self { snapshots }
    }

    /// Append a snapshot. Order is preserved across capture/restore so
    /// callers that scan the registry get a stable iteration.
    pub fn push(&mut self, snapshot: ProcessSnapshot) {
        self.snapshots.push(snapshot);
    }

    /// View the snapshots without taking ownership.
    pub fn snapshots(&self) -> &[ProcessSnapshot] {
        &self.snapshots
    }

    /// Consume the registry and return the underlying vec. Used by
    /// [`EnvdState::capture`] to fold the snapshots into the top-level
    /// state without a clone.
    pub fn into_inner(self) -> Vec<ProcessSnapshot> {
        self.snapshots
    }
}

/// Live, snapshot-shaped registry of PTY bookkeeping. Sibling of
/// [`ProcessRegistry`] — see that type's docs for the design note.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PtyRegistry {
    snapshots: Vec<PtySnapshot>,
}

impl PtyRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry from a pre-existing batch of snapshots.
    pub fn from_snapshots(snapshots: Vec<PtySnapshot>) -> Self {
        Self { snapshots }
    }

    /// Append a snapshot.
    pub fn push(&mut self, snapshot: PtySnapshot) {
        self.snapshots.push(snapshot);
    }

    /// View the snapshots without taking ownership.
    pub fn snapshots(&self) -> &[PtySnapshot] {
        &self.snapshots
    }

    /// Consume the registry and return the underlying vec.
    pub fn into_inner(self) -> Vec<PtySnapshot> {
        self.snapshots
    }
}

/// Live, snapshot-shaped registry of filesystem-watch bookkeeping.
/// Sibling of [`ProcessRegistry`] — see that type's docs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WatchRegistry {
    snapshots: Vec<WatchSnapshot>,
}

impl WatchRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry from a pre-existing batch of snapshots.
    pub fn from_snapshots(snapshots: Vec<WatchSnapshot>) -> Self {
        Self { snapshots }
    }

    /// Append a snapshot.
    pub fn push(&mut self, snapshot: WatchSnapshot) {
        self.snapshots.push(snapshot);
    }

    /// View the snapshots without taking ownership.
    pub fn snapshots(&self) -> &[WatchSnapshot] {
        &self.snapshots
    }

    /// Consume the registry and return the underlying vec.
    pub fn into_inner(self) -> Vec<WatchSnapshot> {
        self.snapshots
    }
}

/// Output of [`EnvdState::restore`]: the three rehydrated registries.
///
/// Returned as a struct rather than a tuple so callers (T7, the resume
/// handler) can destructure by name and not by position.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RestoreResult {
    /// Rehydrated process bookkeeping.
    pub processes: ProcessRegistry,
    /// Rehydrated PTY bookkeeping.
    pub ptys: PtyRegistry,
    /// Rehydrated filesystem-watch bookkeeping.
    pub watches: WatchRegistry,
}

/// Failure modes for [`EnvdState::restore`] and the file-loading
/// helpers.
///
/// Hand-rolled rather than `thiserror` so envd doesn't grow a new
/// dependency for the single error site this module needs.
#[derive(Debug)]
pub enum RestoreError {
    /// On-disk file's `version` field didn't match [`SCHEMA_VERSION`].
    /// Distinct from generic parse errors so the caller can decide
    /// whether to fall back to a compatibility shim.
    VersionMismatch {
        /// Value read from the file.
        found: u32,
        /// Value envd was built to understand.
        expected: u32,
    },
    /// Couldn't read the snapshot file from disk.
    Io(std::io::Error),
    /// Snapshot file existed but couldn't be parsed as JSON of the
    /// expected shape.
    Parse(serde_json::Error),
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VersionMismatch { found, expected } => write!(
                f,
                "envd-state.json schema version mismatch: found {found}, expected {expected}"
            ),
            Self::Io(e) => write!(f, "reading envd-state.json: {e}"),
            Self::Parse(e) => write!(f, "parsing envd-state.json: {e}"),
        }
    }
}

impl std::error::Error for RestoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::VersionMismatch { .. } => None,
            Self::Io(e) => Some(e),
            Self::Parse(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for RestoreError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for RestoreError {
    fn from(e: serde_json::Error) -> Self {
        Self::Parse(e)
    }
}

impl EnvdState {
    /// Capture an [`EnvdState`] from the three live registries.
    ///
    /// Stamps [`SCHEMA_VERSION`] into `version` and pulls the snapshot
    /// vectors out of each registry. The registries themselves keep
    /// their own ordering, which serde then mirrors in the JSON output.
    pub fn capture(
        processes: &ProcessRegistry,
        ptys: &PtyRegistry,
        watches: &WatchRegistry,
    ) -> Self {
        Self {
            version: SCHEMA_VERSION,
            processes: processes.snapshots.clone(),
            ptys: ptys.snapshots.clone(),
            watches: watches.snapshots.clone(),
            rootfs_fingerprint: None,
        }
    }

    /// Attach a writable-rootfs fingerprint to a captured state.
    ///
    /// Builder-style so the binary's capture path can compute the
    /// fingerprint on a blocking thread (it is I/O-heavy) and fold it in
    /// without re-cloning the registries. Returns `self` for chaining.
    #[must_use]
    pub fn with_rootfs_fingerprint(mut self, fingerprint: RootfsFingerprint) -> Self {
        self.rootfs_fingerprint = Some(fingerprint);
        self
    }

    /// Rehydrate the registries from a previously-captured state.
    ///
    /// Returns [`RestoreError::VersionMismatch`] if `version` differs
    /// from [`SCHEMA_VERSION`] — the caller cannot meaningfully
    /// interpret a foreign schema.
    pub fn restore(self) -> Result<RestoreResult, RestoreError> {
        if self.version != SCHEMA_VERSION {
            return Err(RestoreError::VersionMismatch {
                found: self.version,
                expected: SCHEMA_VERSION,
            });
        }
        Ok(RestoreResult {
            processes: ProcessRegistry::from_snapshots(self.processes),
            ptys: PtyRegistry::from_snapshots(self.ptys),
            watches: WatchRegistry::from_snapshots(self.watches),
        })
    }

    /// Serialize to pretty-printed JSON.
    ///
    /// Pretty output is chosen for human inspectability of the snapshot
    /// bundle. The encoding remains deterministic because every
    /// collection in the type tree is `Vec` or `BTreeMap`.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Parse from a JSON byte slice.
    ///
    /// Does *not* validate `version` — callers that intend to
    /// rehydrate registries should chain [`Self::restore`] after this.
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Write the snapshot to `path`, replacing any existing file.
    ///
    /// Writes via a temp-file + rename dance so a crash mid-write
    /// leaves the previous snapshot intact rather than producing a
    /// half-written file. The temp file lives in the same directory as
    /// `path` so the rename is atomic on POSIX filesystems.
    pub fn write_to(&self, path: &Path) -> Result<(), RestoreError> {
        let parent = path.parent().ok_or_else(|| {
            RestoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "snapshot path has no parent directory",
            ))
        })?;
        let tmp = tempfile_in(parent, path)?;
        let json = self.to_json()?;
        std::fs::write(&tmp, json.as_bytes())?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Read and decode a snapshot from `path`. Pairs with
    /// [`Self::write_to`].
    pub fn read_from(path: &Path) -> Result<Self, RestoreError> {
        let bytes = std::fs::read(path)?;
        let state = Self::from_json(&bytes)?;
        Ok(state)
    }
}

/// Build a sibling temp-file path next to `target`.
///
/// Uses the target's file name plus `.tmp` and the current process pid
/// as a per-process disambiguator. Two concurrent writers would still
/// collide, but envd is single-instance per pod so that's acceptable.
fn tempfile_in(dir: &Path, target: &Path) -> Result<PathBuf, RestoreError> {
    let file_name = target.file_name().ok_or_else(|| {
        RestoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "snapshot path has no file name",
        ))
    })?;
    let mut tmp_name = std::ffi::OsString::from(file_name);
    tmp_name.push(format!(".tmp.{}", std::process::id()));
    Ok(dir.join(tmp_name))
}
