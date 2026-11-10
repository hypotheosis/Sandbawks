//! Deterministic content fingerprint of envd's writable rootfs.
//!
//! Plan 07 hardening (#236): resume needs a single deterministic oracle
//! for *any* rootfs divergence across a pause/resume boundary — spill,
//! truncation, silent corruption, or a missed non-overlay mount. At
//! pause, envd computes a [`RootfsFingerprint`] over its writable rootfs
//! and records it in `envd-state.json`. On the next boot (the resume
//! seed path in [`crate::state`] / the binary's `load_boot_seed`), the
//! fingerprint is recomputed over the restored filesystem and compared;
//! a mismatch fails the resume instead of silently handing back a
//! sandbox with a subtly-wrong filesystem.
//!
//! # What surface is hashed
//!
//! The fingerprint walks the directory tree at envd's `--root` — the
//! writable rootfs as envd sees it *from inside the sandbox* (the merged
//! overlay view). envd cannot address the host-side overlay *upper* dir
//! directly (that is a node/containerd concept; see
//! `sandbawks-node::gvisor`), so the in-sandbox merged view is the
//! closest surface envd can re-verify directly — which is exactly why
//! #236 prefers the envd-side check.
//!
//! Two scoping filters keep that surface *stable* across the
//! pause/resume boundary (without them the oracle fails closed against
//! itself — see [`RootfsFingerprint::compute`] for the detail):
//!
//! - The walk stays within `root`'s own filesystem (`st_dev`), so the
//!   churning kernel pseudo-filesystems (`/proc`, `/sys`, `/dev`,
//!   `/run`) and any bind-mounted volumes are excluded. Asserting that
//!   the *expected* mounts are present is a separate concern — the
//!   mount-set assertion (#237) this oracle is designed to pair with.
//! - An explicit `exclude` list drops envd's own `--state-file`, which
//!   the default deployment writes *inside* `--root` and which embeds
//!   this very fingerprint (so hashing it would be self-referential).
//!
//! # Why full-content hashing
//!
//! Each regular file's *contents* are streamed into the hash, not just
//! its size and mtime. A content-free variant (path + mode + size) is
//! cheaper but cannot detect silent corruption — a same-length byte flip
//! in a restored file would pass. Since #236 explicitly lists corruption
//! as a divergence to catch, full-content SHA-256 is the only oracle
//! that satisfies the requirement.
//!
//! **Cost tradeoff:** computation is `O(bytes under --root)`. For the
//! overlay-tmpfs default (`runsc --overlay2=root:self`) the workload's
//! writes are tmpfs-backed and bounded, so in the common case the cost
//! tracks the workload delta rather than the whole base image. When
//! `--root` is pointed at a large merged image the walk is
//! correspondingly expensive; scoping the walk to the overlay upper once
//! envd is handed that path is a future refinement. The hash runs only
//! at pause and at resume boot — never on a request hot path — and the
//! binary offloads it to a blocking thread so the async reactor is never
//! stalled.
//!
//! # Unreadable entries
//!
//! Under the production default `--root=/` with a non-root envd UID, the
//! walk routinely meets files and directories it cannot read. Rather than
//! abort on the first `EACCES` (which would store no fingerprint at all
//! and silently disable the resume oracle, #526), an unreadable entry is
//! recorded deterministically from whatever metadata *is* available, under
//! a dedicated tag, and an unreadable directory's children are skipped.
//! See [`RootfsFingerprint::compute`] for the guarantees this preserves.
//!
//! # Determinism
//!
//! The fingerprint is order-independent by construction: each filesystem
//! entry is hashed into its own 32-byte digest (over a length-framed
//! encoding of its relative path, mode, device id, and content), the
//! per-entry digests are sorted, and the sorted digests are folded into
//! the final SHA-256. This mirrors the `aggregate_sha256` pattern in
//! `sandbawks-types::snapshot` and makes the result independent of the
//! order `read_dir` happens to yield. The per-entry digests are also
//! mutually independent, so the walk is trivially parallelizable if
//! profiling ever shows it on a hot path.

use std::fs::{self, File, Metadata};
use std::io::{self, Read};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Streaming read buffer size for file-content hashing.
///
/// 64 KiB balances syscall overhead against the transient buffer cost;
/// it is well below the size at which a single read would pressure the
/// in-sandbox memory budget.
const READ_CHUNK: usize = 64 * 1024;

// Domain-separation tags mixed into each entry digest so a regular file
// can never collide with a symlink (or directory) that happens to share
// a path and leading bytes. One byte each; the value is arbitrary but
// stable.
const TAG_FILE: u8 = b'f';
const TAG_DIR: u8 = b'd';
const TAG_SYMLINK: u8 = b'l';
/// Devices, FIFOs, and sockets. Their identity is fully captured by the
/// mode + `rdev` fields hashed for every entry, so they share one tag.
const TAG_SPECIAL: u8 = b's';
/// A regular file or symlink that was stat'd but whose contents/target
/// this UID cannot read (`EACCES`). Its path, `mode`, `rdev`, and size
/// still contribute, so a permission, type, or size divergence is caught;
/// only the unreadable bytes themselves fall outside the oracle. The
/// `mode` field carries the file-type bits, so an unreadable regular file
/// cannot collide with an unreadable symlink under this single tag.
const TAG_UNREADABLE: u8 = b'u';
/// An entry whose name a parent listed but which could not even be stat'd
/// (`EACCES` on `symlink_metadata`), so no `mode`/`rdev`/type is available
/// — only its path is hashed. A distinct tag keeps it from colliding with
/// any stat'd entry (every stat'd entry hashes `mode`/`rdev` between its
/// framed path and its type tag).
const TAG_UNREADABLE_NO_META: u8 = b'U';

/// A deterministic fingerprint of a writable rootfs subtree.
///
/// Serialized as a small JSON object embedded in `envd-state.json`. The
/// `digest` is the load-bearing field; `entries` is carried alongside
/// purely for diagnostics (a log line like "snapshot had 1240 entries,
/// restored fs has 1239" localizes a divergence faster than a bare hash
/// mismatch).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootfsFingerprint {
    /// Lower-case-hex SHA-256 over the sorted per-entry digests. Always
    /// 64 hex characters.
    pub digest: String,
    /// Number of filesystem entries (files, dirs, symlinks, special
    /// files) walked under the root, excluding the root itself.
    pub entries: u64,
}

/// Failure modes for [`RootfsFingerprint::verify`].
///
/// Hand-rolled (rather than `thiserror`) to match the error style of
/// the sibling [`crate::state`] module and avoid a new dependency.
#[derive(Debug)]
pub enum FingerprintError {
    /// The filesystem walk itself failed (root missing, permission
    /// denied, read error). At resume this is fatal — a rootfs that
    /// cannot even be read back is not restorable.
    Io(io::Error),
    /// The recomputed fingerprint did not match the one recorded at
    /// pause. This is the fail-closed signal: the restored rootfs
    /// diverged from the one captured.
    Mismatch {
        /// Fingerprint recorded in `envd-state.json` at pause.
        expected: RootfsFingerprint,
        /// Fingerprint recomputed over the restored rootfs.
        actual: RootfsFingerprint,
    },
}

impl std::fmt::Display for FingerprintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "computing rootfs fingerprint: {e}"),
            Self::Mismatch { expected, actual } => write!(
                f,
                "rootfs fingerprint mismatch after restore: expected {} ({} entries), \
                 got {} ({} entries)",
                expected.digest, expected.entries, actual.digest, actual.entries
            ),
        }
    }
}

impl std::error::Error for FingerprintError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Mismatch { .. } => None,
        }
    }
}

impl RootfsFingerprint {
    /// Compute the fingerprint of the directory tree rooted at `root`.
    ///
    /// Walks the tree without following symlinks (so a symlink that
    /// points outside the tree, or forms a cycle, cannot make the walk
    /// escape or loop), hashes each entry independently, then folds the
    /// sorted per-entry digests into the final SHA-256.
    ///
    /// # Scope of the walk
    ///
    /// Two filters keep the fingerprint stable across a pause/resume
    /// boundary — without them the oracle fails closed against itself:
    ///
    /// - **Single filesystem boundary (unix).** Entries that live on a
    ///   different filesystem than `root` (i.e. mount points) are
    ///   skipped, along with everything under them. With the production
    ///   default `--root=/` this excludes the kernel pseudo-filesystems
    ///   (`/proc`, `/sys`, `/dev`, `/run`) whose contents churn
    ///   microsecond-to-microsecond, and also bind-mounted volumes
    ///   (`emptyDir`s, user data mounts) that legitimately mutate between
    ///   pause and resume. Detecting a *missing* mount is intentionally
    ///   **not** this oracle's job — that is the mount-set assertion
    ///   (#237) this fingerprint is designed to pair with.
    /// - **`exclude` list.** Any path in `exclude` is skipped (no hash,
    ///   no descent). The binary passes envd's `--state-file` here: the
    ///   default deployment writes `state.json` *inside* `--root`, so
    ///   hashing it would make every capture's fingerprint depend on the
    ///   previous capture's fingerprint (the file embeds it), which can
    ///   never re-verify. `exclude` paths must be absolute and expressed
    ///   in the same form as the on-disk layout under `root`.
    ///
    /// # Unreadable entries
    ///
    /// With the production default `--root=/` and a non-root envd UID
    /// (e.g. the `compound-sandbox` image's `USER dev`), the walk routinely
    /// meets root-owned `0600` files and `0700` directories it cannot read
    /// (#526). Aborting on the first such `EACCES` would store no
    /// fingerprint, silently disabling the resume oracle. Instead an
    /// unreadable entry is hashed under a dedicated tag from whatever
    /// metadata *is* available — its path, plus (when it could be stat'd)
    /// its `mode`, `rdev`, and size — and an unreadable directory's children
    /// are skipped. This is deterministic across pause/resume because envd
    /// resumes under the same UID against the restored filesystem, so the
    /// same entries are unreadable on both sides. Should an entry's
    /// readability or permissions diverge, its `mode`/tag changes and the
    /// resume fails closed exactly as a content divergence would; only the
    /// (unreadable) bytes of a still-unreadable file are outside the oracle.
    ///
    /// Unreadability is **not** immutability: the oracle keys on an
    /// unreadable file's `mode` *and* size, so a same-filesystem file envd
    /// cannot read but whose size changes across the pause/resume boundary
    /// (e.g. a log another process appends to) now fails the resume closed,
    /// where before #526 the aborted walk stored no fingerprint and
    /// verification was skipped entirely. A paused sandbox is frozen and the
    /// overlay-tmpfs default bounds this surface, so this is the intended
    /// #236 hardening rather than a regression — but it is a genuinely new
    /// failure mode for any such mutating-but-unreadable file.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`io::Error`] only for failures that make the
    /// whole walk meaningless: a missing or unreadable `root`, or a
    /// non-`EACCES` I/O error on some entry. A *nested* directory that
    /// vanishes mid-walk (a concurrent removal) or that this UID cannot
    /// read is treated as having no children rather than as an error, since
    /// the entry that pointed at it is hashed before the recursion; a
    /// missing or unreadable `root`, by contrast, is a hard error because
    /// there is nothing to fingerprint.
    pub fn compute(root: &Path, exclude: &[&Path]) -> io::Result<Self> {
        // `metadata` (not `symlink_metadata`) so a `--root` given as a
        // symlink to the real rootfs resolves to the filesystem the
        // contents actually live on. Doubles as the existence check that
        // makes a missing root a hard `io::Error`.
        let root_dev = filesystem_id(&fs::metadata(root)?);
        let mut digests = Vec::new();
        collect_entries(root, root, true, root_dev, exclude, &mut digests)?;
        let entries = digests.len() as u64;
        // Order-independence: sort the per-entry digests so the final
        // fold does not depend on the order `read_dir` yielded them.
        digests.sort_unstable();
        let mut hasher = Sha256::new();
        for digest in &digests {
            hasher.update(digest);
        }
        Ok(Self {
            digest: hex_lower(&hasher.finalize()),
            entries,
        })
    }

    /// Recompute the fingerprint over `root` and compare it to `self`.
    ///
    /// `exclude` must match the list passed to the [`Self::compute`] call
    /// that produced `self`, or the recompute walks a different surface
    /// and spuriously mismatches.
    ///
    /// # Errors
    ///
    /// - [`FingerprintError::Io`] if the rootfs cannot be walked.
    /// - [`FingerprintError::Mismatch`] if the recomputed fingerprint
    ///   differs from `self` — the fail-closed signal that the restored
    ///   filesystem diverged from the captured one.
    pub fn verify(&self, root: &Path, exclude: &[&Path]) -> Result<(), FingerprintError> {
        let actual = Self::compute(root, exclude).map_err(FingerprintError::Io)?;
        if &actual == self {
            Ok(())
        } else {
            Err(FingerprintError::Mismatch {
                expected: self.clone(),
                actual,
            })
        }
    }
}

/// Recursively walk `dir`, pushing one 32-byte digest per entry into
/// `out`. Paths are framed relative to `root` so the fingerprint is
/// independent of where the tree is mounted.
///
/// `is_root` distinguishes the top-level call from a recursive descent:
/// a missing or unreadable *root* is a hard error (a restored rootfs that
/// cannot be read at all is not restorable, and must not be papered over
/// as the empty-tree digest), whereas a *nested* directory that vanishes
/// between being stat-ed and being opened (a benign mid-walk race — the
/// entry that referenced it is already hashed), or that this UID cannot
/// read (`EACCES` under `--root=/`), is skipped without aborting the walk.
///
/// Iterative recursion via an explicit stack would also work; the
/// straight recursion mirrors the directory structure and the sandbox
/// rootfs is not deep enough to risk a stack overflow.
fn collect_entries(
    root: &Path,
    dir: &Path,
    is_root: bool,
    root_dev: Option<u64>,
    exclude: &[&Path],
    out: &mut Vec<[u8; 32]>,
) -> io::Result<()> {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        // A *nested* directory that vanished mid-walk (`NotFound`) or that
        // this UID cannot read (`PermissionDenied` — common for root-owned
        // `0700` dirs under `--root=/`) is tolerated: the directory entry
        // itself was already hashed by the caller, so we just skip its
        // children. A missing or unreadable *root* stays fatal.
        Err(e)
            if !is_root
                && matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                ) =>
        {
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    for entry in read_dir {
        let entry = entry?;
        let path = entry.path();
        // Excluded path (e.g. the `--state-file` that lives inside the
        // rootfs): skip it and any subtree under it.
        if exclude.contains(&path.as_path()) {
            continue;
        }
        // `symlink_metadata` does not traverse the final symlink, so a
        // symlink is reported as a symlink rather than its target. The
        // parent listed this name, but stat'ing the entry itself can still
        // hit `EACCES` (e.g. a parent dir readable but not searchable); in
        // that case record a path-only "unreadable" marker so the entry
        // still contributes deterministically, then skip it — without a
        // type we cannot know whether to descend.
        let meta = match fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                let relative = path.strip_prefix(root).unwrap_or(&path);
                out.push(hash_unstattable_entry(relative));
                continue;
            }
            Err(e) => return Err(e),
        };
        // Mount point onto another filesystem: skip it (and, by not
        // recursing, everything under it). See `compute`'s "Scope of the
        // walk" note for why pseudo-fs and bind mounts are out of scope.
        if crosses_filesystem(&meta, root_dev) {
            continue;
        }
        let relative = path.strip_prefix(root).unwrap_or(&path);
        out.push(hash_entry(relative, &path, &meta)?);
        if meta.file_type().is_dir() {
            collect_entries(root, &path, false, root_dev, exclude, out)?;
        }
    }
    Ok(())
}

/// The filesystem id (`st_dev`) of `meta`, used to keep the walk within
/// a single filesystem. `None` on non-unix, where the boundary is not
/// enforced (the sandbox is always Linux; a non-unix dev host running
/// `cargo test` just walks everything under the tempdir, which is one
/// filesystem anyway).
#[cfg(unix)]
fn filesystem_id(meta: &Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(meta.dev())
}

#[cfg(not(unix))]
fn filesystem_id(_meta: &Metadata) -> Option<u64> {
    None
}

/// Whether `meta` lives on a different filesystem than the root, i.e. is
/// a mount point that should be excluded from the single-filesystem
/// walk. Always `false` when the boundary is unknown (`root_dev` is
/// `None`) or on non-unix.
fn crosses_filesystem(meta: &Metadata, root_dev: Option<u64>) -> bool {
    matches!((filesystem_id(meta), root_dev), (Some(dev), Some(root)) if dev != root)
}

/// Hash one filesystem entry into a standalone 32-byte digest.
///
/// The encoding is length-framed (every variable-length field is
/// preceded by its byte length) so that two different `(path, content)`
/// pairs cannot produce the same byte stream by shifting a boundary.
fn hash_entry(relative: &Path, absolute: &Path, meta: &Metadata) -> io::Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    update_framed(&mut hasher, &os_bytes(relative));

    // Mode (full `st_mode`, so file-type bits and permission bits both
    // contribute) and `rdev` (device id, which is what distinguishes one
    // device node — including an overlayfs whiteout char device — from
    // another). Unix-only; the sandbox always runs on Linux. A non-unix
    // build (a developer's macOS host running `cargo test`) produces a
    // weaker type+content fingerprint, which is fine because the
    // fingerprint is only ever compared against one computed on the same
    // host within a single pause/resume cycle.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        hasher.update(meta.mode().to_le_bytes());
        hasher.update(meta.rdev().to_le_bytes());
    }

    let file_type = meta.file_type();
    if file_type.is_file() {
        // Open before tagging: an unreadable file (`EACCES` — the common
        // root-owned `0600` file under `--root=/` with a non-root envd UID,
        // #526) is recorded under TAG_UNREADABLE instead of aborting the
        // walk. The readable encoding (TAG_FILE + size + contents) is
        // byte-for-byte unchanged, so a fully-readable tree fingerprints
        // exactly as before. The size (`meta.len()`) is hashed in both
        // branches, so a truncation of an unreadable file is still caught;
        // only its (unreadable) contents fall outside the oracle. A read
        // error *after* a successful open is a genuine fault and propagates.
        match File::open(absolute) {
            Ok(file) => {
                hasher.update([TAG_FILE]);
                // Streaming the contents below already encodes the length
                // (each entry is finalized separately, so a truncation
                // changes the byte count fed into `update` and thus the
                // digest). The size is hashed anyway as a cheap, explicit
                // framing field — it costs 8 bytes and makes the entry
                // encoding self-describing — not as a truncation defense,
                // which the content stream already provides.
                hasher.update(meta.len().to_le_bytes());
                hash_reader(file, &mut hasher)?;
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                hasher.update([TAG_UNREADABLE]);
                hasher.update(meta.len().to_le_bytes());
            }
            Err(e) => return Err(e),
        }
    } else if file_type.is_symlink() {
        // `read_link` only needs search (`x`) on the parent, which the
        // caller's `read_dir` already exercised, so `EACCES` here is rare;
        // tolerate it symmetrically rather than aborting. `mode` (above)
        // carries the `S_IFLNK` type bit, so an unreadable symlink cannot
        // collide with an unreadable regular file under TAG_UNREADABLE.
        match fs::read_link(absolute) {
            Ok(target) => {
                hasher.update([TAG_SYMLINK]);
                update_framed(&mut hasher, &os_bytes(&target));
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                hasher.update([TAG_UNREADABLE]);
                hasher.update(meta.len().to_le_bytes());
            }
            Err(e) => return Err(e),
        }
    } else if file_type.is_dir() {
        hasher.update([TAG_DIR]);
    } else {
        // Device / FIFO / socket: identity already captured by mode +
        // rdev above; the tag just keeps it distinct from an empty file.
        hasher.update([TAG_SPECIAL]);
    }
    Ok(hasher.finalize().into())
}

/// Stream a reader's bytes into `hasher` in [`READ_CHUNK`] blocks so
/// memory stays bounded regardless of file size. Generic over the reader
/// so the caller opens the file (and handles an unreadable-file `EACCES`)
/// rather than this helper, avoiding a redundant second `open`.
fn hash_reader<R: Read>(mut reader: R, hasher: &mut Sha256) -> io::Result<()> {
    let mut buf = [0u8; READ_CHUNK];
    loop {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(())
}

/// Deterministic digest for an entry a parent listed but which could not
/// be `symlink_metadata`-stat'd (`EACCES`). No mode, type, or content is
/// available, so only the relative path is hashed, under TAG_UNREADABLE_NO_META
/// — a tag that cannot collide with any stat'd entry (every stat'd entry
/// hashes its `mode`/`rdev` between the framed path and its type tag).
fn hash_unstattable_entry(relative: &Path) -> [u8; 32] {
    let mut hasher = Sha256::new();
    update_framed(&mut hasher, &os_bytes(relative));
    hasher.update([TAG_UNREADABLE_NO_META]);
    hasher.finalize().into()
}

/// Feed a length-prefixed byte string into the hasher.
///
/// The `u64` length prefix is what makes the encoding unambiguous: it
/// guarantees that `["ab", "c"]` and `["a", "bc"]` hash differently even
/// though their concatenations are equal.
fn update_framed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

/// Borrow a path's raw bytes.
///
/// On Unix paths are arbitrary byte strings (not necessarily UTF-8), so
/// `OsStrExt::as_bytes` is the exact, lossless encoding. The non-unix
/// fallback maps through `to_string_lossy`, which is **structurally
/// weaker**: two distinct paths that differ only in non-UTF-8 bytes
/// collide to the same encoding, so a real divergence between two such
/// paths would go *undetected*. This is acceptable only because the
/// fingerprint is never compared across hosts — production is always
/// Linux (the lossless path), and the non-unix branch exists solely so a
/// developer's macOS/Windows `cargo test` compiles. The consequence is
/// that local divergence tests on a non-unix host are a slightly blunter
/// signal than the production path; they are not load-bearing.
#[cfg(unix)]
fn os_bytes(path: &Path) -> std::borrow::Cow<'_, [u8]> {
    use std::os::unix::ffi::OsStrExt;
    std::borrow::Cow::Borrowed(path.as_os_str().as_bytes())
}

#[cfg(not(unix))]
fn os_bytes(path: &Path) -> std::borrow::Cow<'_, [u8]> {
    std::borrow::Cow::Owned(path.to_string_lossy().into_owned().into_bytes())
}

/// Lower-case-hex encode a SHA-256 digest into a 64-char string.
///
/// Inlined rather than pulling in `hex` / `base16ct`, matching the
/// convention already used by `sandbawks-types::snapshot`.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // `write!` into a `String` is infallible.
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Build a small tree under a fresh tempdir and return its handle.
    /// The tempdir is returned so the caller keeps it alive — dropping
    /// it deletes the tree.
    fn sample_tree() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        fs::create_dir(root.join("sub")).expect("mkdir sub");
        fs::write(root.join("a.txt"), b"alpha").expect("write a");
        fs::write(root.join("sub/b.txt"), b"bravo").expect("write b");
        dir
    }

    /// Skip the enclosing test when the tempdir filesystem cannot reflect
    /// the file mutations the assertion depends on.
    ///
    /// Returning early marks the test as passed (Rust has no stable "skipped"
    /// status), with a diagnostic line on stderr so the skip is not silent —
    /// matching the issue's guidance that these failures are environmental,
    /// not algorithmic.
    macro_rules! require_fs_reflects_mutations {
        () => {
            if !fs_reflects_mutations() {
                eprintln!(
                    "fingerprint test skipped: tempdir filesystem masks file mutations \
                     (e.g. a gVisor `--overlay2` sandbox); see issue #527"
                );
                return;
            }
        };
    }

    /// Whether the tempdir filesystem promptly reflects file mutations to the
    /// fingerprint walk; probed once per test process.
    ///
    /// Most assertions below verify that a mutation (a content rewrite, an
    /// added/removed file, a chmod, a symlink retarget) moves the computed
    /// fingerprint. That only holds on a filesystem whose `read_dir` and
    /// content reads reflect those mutations within a single process. Some
    /// sandbox filesystems — notably the gVisor `--overlay2` merged view that
    /// issue #527 was filed from — cache directory listings and file contents
    /// so aggressively that a freshly-written file is invisible to a
    /// subsequent walk: the mutation is real on disk but the walker cannot
    /// observe it, so the assertion fails for a reason unrelated to the
    /// production algorithm (which is correct and out of scope to change).
    ///
    /// Rather than sniff the filesystem *type* (brittle, and not the actual
    /// precondition), this probes the *behavior* the tests rely on, mirroring
    /// the behavior-probe convention used elsewhere in the tree (e.g.
    /// `runsc_reconcile`'s root self-detect) instead of a uid/magic-number
    /// dependency.
    fn fs_reflects_mutations() -> bool {
        // Probe once per process: the masking is a deterministic property of
        // the filesystem, so there is no value in re-walking a throwaway tree
        // for each of the gated tests.
        static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHED.get_or_init(probe_fs_reflects_mutations)
    }

    /// One-shot probe backing [`fs_reflects_mutations`]. Builds a throwaway
    /// tree (mirroring [`sample_tree`]) on the tempdir filesystem and checks
    /// the two preconditions every gated assertion shares: the walk sees all
    /// freshly-created entries, and a same-length content rewrite moves the
    /// digest. Any I/O failure conservatively reports the filesystem as
    /// unusable so the test is skipped rather than spuriously failed.
    fn probe_fs_reflects_mutations() -> bool {
        let Ok(dir) = tempfile::tempdir() else {
            return false;
        };
        let root = dir.path();
        if fs::create_dir(root.join("sub")).is_err()
            || fs::write(root.join("a.txt"), b"alpha").is_err()
            || fs::write(root.join("sub/b.txt"), b"bravo").is_err()
        {
            return false;
        }
        let Ok(before) = RootfsFingerprint::compute(root, &[]) else {
            return false;
        };
        // Precondition 1: the walk must see every freshly-created entry. A
        // masking overlay yields a stale `read_dir` listing and this is < 3.
        if before.entries != 3 {
            return false;
        }
        // Precondition 2: a same-length content rewrite must reach the content
        // hash. A masking overlay returns the cached bytes and the digest does
        // not move.
        if fs::write(root.join("a.txt"), b"ALPHA").is_err() {
            return false;
        }
        matches!(
            RootfsFingerprint::compute(root, &[]),
            Ok(after) if after.digest != before.digest
        )
    }

    #[test]
    fn compute_is_deterministic() {
        require_fs_reflects_mutations!();
        let tree = sample_tree();
        let first = RootfsFingerprint::compute(tree.path(), &[]).expect("compute 1");
        let second = RootfsFingerprint::compute(tree.path(), &[]).expect("compute 2");
        assert_eq!(first, second, "same tree must fingerprint identically");
        assert_eq!(first.digest.len(), 64, "digest is 64-char lower-hex");
        // sub/, a.txt, sub/b.txt
        assert_eq!(first.entries, 3);
    }

    #[test]
    fn order_independent_across_creation_order() {
        // Two trees with identical final contents but files created in a
        // different order must fingerprint the same — the sort over
        // per-entry digests erases `read_dir` ordering.
        let one = tempfile::tempdir().expect("tempdir 1");
        fs::write(one.path().join("a.txt"), b"alpha").expect("write a");
        fs::write(one.path().join("b.txt"), b"bravo").expect("write b");

        let two = tempfile::tempdir().expect("tempdir 2");
        fs::write(two.path().join("b.txt"), b"bravo").expect("write b");
        fs::write(two.path().join("a.txt"), b"alpha").expect("write a");

        assert_eq!(
            RootfsFingerprint::compute(one.path(), &[]).expect("fp 1"),
            RootfsFingerprint::compute(two.path(), &[]).expect("fp 2"),
        );
    }

    #[test]
    fn content_change_changes_digest() {
        require_fs_reflects_mutations!();
        let tree = sample_tree();
        let before = RootfsFingerprint::compute(tree.path(), &[]).expect("before");
        // Same byte length, different content — only full-content
        // hashing catches this.
        fs::write(tree.path().join("a.txt"), b"ALPHA").expect("rewrite a");
        let after = RootfsFingerprint::compute(tree.path(), &[]).expect("after");
        assert_ne!(before.digest, after.digest);
        assert_eq!(before.entries, after.entries, "entry count is unchanged");
    }

    #[test]
    fn truncation_changes_digest() {
        require_fs_reflects_mutations!();
        let tree = sample_tree();
        let before = RootfsFingerprint::compute(tree.path(), &[]).expect("before");
        fs::write(tree.path().join("a.txt"), b"al").expect("truncate a");
        let after = RootfsFingerprint::compute(tree.path(), &[]).expect("after");
        assert_ne!(before.digest, after.digest);
    }

    #[test]
    fn added_file_changes_digest_and_count() {
        require_fs_reflects_mutations!();
        let tree = sample_tree();
        let before = RootfsFingerprint::compute(tree.path(), &[]).expect("before");
        fs::write(tree.path().join("c.txt"), b"charlie").expect("write c");
        let after = RootfsFingerprint::compute(tree.path(), &[]).expect("after");
        assert_ne!(before.digest, after.digest);
        assert_eq!(after.entries, before.entries + 1);
    }

    #[test]
    fn removed_file_changes_digest_and_count() {
        require_fs_reflects_mutations!();
        let tree = sample_tree();
        let before = RootfsFingerprint::compute(tree.path(), &[]).expect("before");
        fs::remove_file(tree.path().join("a.txt")).expect("rm a");
        let after = RootfsFingerprint::compute(tree.path(), &[]).expect("after");
        assert_ne!(before.digest, after.digest);
        assert_eq!(after.entries, before.entries - 1);
    }

    #[cfg(unix)]
    #[test]
    fn permission_change_changes_digest() {
        require_fs_reflects_mutations!();
        use std::os::unix::fs::PermissionsExt;
        let tree = sample_tree();
        let before = RootfsFingerprint::compute(tree.path(), &[]).expect("before");
        let path = tree.path().join("a.txt");
        let mut perms = fs::metadata(&path).expect("stat").permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&path, perms).expect("chmod");
        let after = RootfsFingerprint::compute(tree.path(), &[]).expect("after");
        assert_ne!(
            before.digest, after.digest,
            "mode is part of the fingerprint"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_target_change_changes_digest() {
        require_fs_reflects_mutations!();
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("real"), b"x").expect("write real");
        symlink("real", dir.path().join("link")).expect("symlink");
        let before = RootfsFingerprint::compute(dir.path(), &[]).expect("before");

        fs::remove_file(dir.path().join("link")).expect("rm link");
        symlink("other", dir.path().join("link")).expect("relink");
        let after = RootfsFingerprint::compute(dir.path(), &[]).expect("after");
        assert_ne!(before.digest, after.digest, "symlink target is hashed");
    }

    #[test]
    fn empty_dir_is_stable_and_nonempty_digest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fp = RootfsFingerprint::compute(dir.path(), &[]).expect("compute");
        assert_eq!(fp.entries, 0);
        // SHA-256 of the empty input is a fixed, well-known value.
        assert_eq!(
            fp.digest,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn verify_accepts_matching_tree() {
        let tree = sample_tree();
        let fp = RootfsFingerprint::compute(tree.path(), &[]).expect("compute");
        fp.verify(tree.path(), &[])
            .expect("matching tree must verify");
    }

    #[test]
    fn verify_rejects_diverged_tree() {
        require_fs_reflects_mutations!();
        let tree = sample_tree();
        let fp = RootfsFingerprint::compute(tree.path(), &[]).expect("compute");
        fs::write(tree.path().join("a.txt"), b"tampered").expect("tamper");
        match fp.verify(tree.path(), &[]) {
            Err(FingerprintError::Mismatch { expected, actual }) => {
                assert_eq!(expected, fp);
                assert_ne!(actual.digest, fp.digest);
            }
            other => panic!("expected Mismatch, got {other:?}"),
        }
    }

    #[test]
    fn verify_surfaces_io_error_for_missing_root() {
        let fp = RootfsFingerprint {
            digest: "0".repeat(64),
            entries: 0,
        };
        let missing = Path::new("/nonexistent/sandbawks/rootfs/path");
        match fp.verify(missing, &[]) {
            Err(FingerprintError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::NotFound),
            other => panic!("expected Io(NotFound), got {other:?}"),
        }
    }

    #[test]
    fn excluded_state_file_does_not_affect_fingerprint() {
        require_fs_reflects_mutations!();
        // The production default writes `state.json` *inside* `--root`,
        // and the file embeds the very fingerprint being computed. If the
        // walk hashed it, every capture's fingerprint would depend on the
        // previous one and resume could never re-verify. Excluding the
        // path must make mutations to it invisible to the fingerprint.
        let tree = sample_tree();
        let state_file = tree.path().join("state.json");
        fs::write(&state_file, b"fingerprint-N").expect("write state");
        let exclude: &[&Path] = &[state_file.as_path()];

        let before = RootfsFingerprint::compute(tree.path(), exclude).expect("before");
        // Rewrite the excluded file with different content (as a real
        // re-pause would, embedding a new fingerprint).
        fs::write(&state_file, b"fingerprint-N-plus-1").expect("rewrite state");
        let after = RootfsFingerprint::compute(tree.path(), exclude).expect("after");
        assert_eq!(
            before, after,
            "mutating an excluded path must not change the fingerprint"
        );
        // `verify` with the same exclude list round-trips.
        before
            .verify(tree.path(), exclude)
            .expect("excluded-file mutation must still verify");

        // Sanity: *without* the exclusion the same mutation is visible,
        // proving the exclude is what's doing the work (not that the file
        // is inert).
        let without_exclude = RootfsFingerprint::compute(tree.path(), &[]).expect("no-exclude");
        assert_ne!(
            after.digest, without_exclude.digest,
            "without the exclude the state file's bytes must be hashed"
        );
    }

    /// Self-detect a DAC-bypass (root / `CAP_DAC_OVERRIDE`) environment
    /// without a uid dependency, mirroring the probe in
    /// `sandbawks-node::runsc_reconcile`: a `0000` file is readable only by
    /// a caller that bypasses the permission bits. The `EACCES`-tolerance
    /// tests below are vacuous under such a caller (the walk simply reads
    /// everything), so they short-circuit when this returns `true`. Takes a
    /// writable `dir` to host the probe and cleans it up.
    #[cfg(unix)]
    fn dac_bypass(dir: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        let probe = dir.join(".dac-probe");
        fs::write(&probe, b"x").expect("write probe");
        fs::set_permissions(&probe, fs::Permissions::from_mode(0o000)).expect("chmod probe 000");
        let bypassed = fs::read(&probe).is_ok();
        fs::set_permissions(&probe, fs::Permissions::from_mode(0o600)).expect("restore probe");
        fs::remove_file(&probe).expect("rm probe");
        bypassed
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_file_does_not_abort_walk() {
        use std::os::unix::fs::PermissionsExt;
        // Depends on the entry count, so it needs a filesystem whose walk
        // reflects freshly-created entries (#527 / #566).
        require_fs_reflects_mutations!();
        let tree = sample_tree();
        let secret = tree.path().join("secret");
        fs::write(&secret, b"top-secret").expect("write secret");
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o000)).expect("chmod 000");

        if dac_bypass(tree.path()) {
            return;
        }

        // The walk completes instead of aborting on the EACCES (#526), and
        // the unreadable file is still counted.
        let fp =
            RootfsFingerprint::compute(tree.path(), &[]).expect("EACCES must not abort the walk");
        assert_eq!(fp.entries, 4, "sub/, a.txt, sub/b.txt, secret");
        // Deterministic across a recompute, and `verify` round-trips — the
        // same unreadable entry hashes identically on a pause/resume cycle.
        let again = RootfsFingerprint::compute(tree.path(), &[]).expect("recompute");
        assert_eq!(fp, again, "unreadable entry must hash deterministically");
        fp.verify(tree.path(), &[])
            .expect("unreadable-entry tree must re-verify");

        fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).expect("restore");
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_file_becoming_readable_fails_closed() {
        use std::os::unix::fs::PermissionsExt;
        // Depends on a chmod moving the digest, so it needs a filesystem
        // whose walk reflects mutations (#527 / #566).
        require_fs_reflects_mutations!();
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("secret");
        fs::write(&secret, b"top-secret").expect("write");
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o000)).expect("chmod 000");

        if dac_bypass(dir.path()) {
            return;
        }

        let locked = RootfsFingerprint::compute(dir.path(), &[]).expect("locked");
        // Granting read flips the entry from TAG_UNREADABLE to a full
        // content hash and changes its mode, so a readability divergence
        // across pause/resume is caught and fails closed.
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).expect("chmod 600");
        let opened = RootfsFingerprint::compute(dir.path(), &[]).expect("opened");
        assert_ne!(
            locked.digest, opened.digest,
            "readability is part of the oracle"
        );
        assert_eq!(locked.entries, opened.entries, "still one entry either way");
        match locked.verify(dir.path(), &[]) {
            Err(FingerprintError::Mismatch { .. }) => {}
            other => panic!("readability divergence must fail closed, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_directory_skips_children_without_aborting() {
        use std::os::unix::fs::PermissionsExt;
        // Depends on the entry count, so it needs a filesystem whose walk
        // reflects freshly-created entries (#527 / #566).
        require_fs_reflects_mutations!();
        let dir = tempfile::tempdir().expect("tempdir");
        let locked_dir = dir.path().join("locked");
        fs::create_dir(&locked_dir).expect("mkdir locked");
        fs::write(locked_dir.join("hidden.txt"), b"hidden").expect("write hidden");
        fs::write(dir.path().join("visible.txt"), b"visible").expect("write visible");
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o000)).expect("chmod 000");

        if dac_bypass(dir.path()) {
            fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o700)).expect("restore");
            return;
        }

        let fp = RootfsFingerprint::compute(dir.path(), &[]).expect("dir EACCES must not abort");
        let again = RootfsFingerprint::compute(dir.path(), &[]).expect("recompute");
        // Restore before asserting so the TempDir can recurse-delete the
        // subtree regardless of whether an assertion below fails.
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o700)).expect("restore");

        // `locked/` is hashed as a directory entry, but `hidden.txt` under
        // it is not walked because the directory is unreadable; only
        // `visible.txt` joins it.
        assert_eq!(fp.entries, 2, "unreadable dir is counted, its child is not");
        assert_eq!(fp, again, "unreadable dir must hash deterministically");
    }

    #[cfg(unix)]
    #[test]
    fn unstattable_entry_under_unsearchable_parent_is_recorded() {
        use std::os::unix::fs::PermissionsExt;
        // Depends on the entry count, so it needs a filesystem whose walk
        // reflects freshly-created entries (#527 / #566).
        require_fs_reflects_mutations!();
        let dir = tempfile::tempdir().expect("tempdir");
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).expect("mkdir sub");
        fs::write(sub.join("entry"), b"data").expect("write entry");
        // 0600 = readable but not searchable: `read_dir(sub)` still lists
        // "entry" (needs `r`), but `symlink_metadata(sub/entry)` resolves a
        // name *through* `sub` and so needs `x`, hitting EACCES — the
        // `hash_unstattable_entry` path-only-marker branch.
        fs::set_permissions(&sub, fs::Permissions::from_mode(0o600)).expect("chmod 600");

        if dac_bypass(dir.path()) {
            fs::set_permissions(&sub, fs::Permissions::from_mode(0o700)).expect("restore");
            return;
        }

        let fp =
            RootfsFingerprint::compute(dir.path(), &[]).expect("un-stat'able entry must not abort");
        let again = RootfsFingerprint::compute(dir.path(), &[]).expect("recompute");
        // Restore before asserting so the TempDir can recurse-delete `sub`.
        fs::set_permissions(&sub, fs::Permissions::from_mode(0o700)).expect("restore");

        // `sub/` (a normal dir entry) + `sub/entry` (recorded path-only
        // because it could not be stat'd through the non-searchable parent).
        assert_eq!(fp.entries, 2, "un-stat'able entry is counted via its path");
        assert_eq!(fp, again, "un-stat'able entry must hash deterministically");
    }
}
