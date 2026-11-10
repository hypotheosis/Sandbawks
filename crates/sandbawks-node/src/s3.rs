//! `S3SnapshotSink` / `S3SnapshotSource` — chunked, parallel,
//! sha256-verified, idempotent snapshot transfer.
//!
//! This module implements the production [`SnapshotSink`] /
//! [`SnapshotSource`] traits that `sandbawks-types` (Plan 05 T3)
//! defines, backed by `rust-s3` so the same code path works against
//! both AWS S3 and a MinIO endpoint in `kind`.
//!
//! ## Wire layout under `key_prefix`
//!
//! Per-part data is split into fixed-size 4 MiB chunks and stored
//! content-addressed by chunk sha256. That gives us three things at
//! once:
//!
//! - **Idempotent retry.** Re-uploading the same part `HEAD`s each
//!   chunk's content-addressed key first; if the chunk is already
//!   present, the `PUT` is skipped. A pause that crashes mid-upload
//!   restarts cleanly without garbage-collecting partial state.
//! - **Cross-part dedup.** Two parts that share a 4 MiB region share
//!   the underlying chunk object — useful when CRIU memory images
//!   between consecutive snapshots overlap heavily.
//! - **Parallel transfer.** Chunks within a part are uploaded /
//!   downloaded via `futures::stream::buffer_unordered(8)`, capping
//!   per-part concurrency at 8 in-flight chunks.
//!
//! ```text
//! <key_prefix>/
//!   manifest.json                          (SnapshotManifest)
//!   parts/<part_name>.json                 (PartIndex — ordered chunks)
//!   chunks/<chunk_sha256>                  (chunk bodies, deduped)
//! ```
//!
//! ## Storage backend abstraction
//!
//! Both sink and source are written against an internal [`BlobStore`]
//! trait whose production impl wraps `s3::Bucket`. Tests substitute
//! [`InMemoryS3`] — a `HashMap<String, Vec<u8>>` wrapper — to exercise
//! the chunking + hashing logic without standing up a real MinIO
//! container. The live MinIO smoke test lives in Plan 05 T14.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use s3::Bucket;
use sandbawks_types::runtime::{RuntimeError, SnapshotManifest};
use sandbawks_types::snapshot::{SnapshotPart, SnapshotSink, SnapshotSource};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Default chunk size for streaming uploads/downloads.
///
/// 4 MiB is below AWS S3's 5 MiB minimum multipart-part size — we're
/// not using S3 multipart uploads (each chunk is its own object), so
/// the constant only needs to balance per-chunk HTTP overhead against
/// peak per-part memory. The effective value is [`effective_chunk_size`],
/// which lets `SANDBAWKS_S3_CHUNK_MIB` override it.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Effective chunk size in bytes, read once from `SANDBAWKS_S3_CHUNK_MIB`
/// (a whole number of MiB; falling back to [`CHUNK_SIZE`]).
///
/// Each chunk is one S3 object, so bigger chunks mean proportionally fewer
/// PUT/HEAD/GET requests. When the backend's per-write cost is dominated by
/// fixed per-object overhead (metadata commit + fsync) rather than bytes —
/// as the GiB bench suggests for garage — larger chunks cut upload time
/// nearly in proportion to the request-count reduction. The tradeoffs:
/// coarser content-addressed dedup, and higher peak RSS
/// (`~(2 * N + 1) * chunk`, so mind the interaction with
/// [`effective_parallelism`] under the pod memory limit). Invalid / `0` /
/// unset values use the default. Cached in a `OnceLock`.
///
/// Only the write path chunks by this size; the read path uses whatever
/// chunk sizes the part index recorded, so a bundle written at one chunk
/// size still restores correctly after the value changes.
pub fn effective_chunk_size() -> usize {
    static CELL: OnceLock<usize> = OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("SANDBAWKS_S3_CHUNK_MIB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .map(|mib| mib * 1024 * 1024)
            .unwrap_or(CHUNK_SIZE)
    })
}

/// Default maximum number of chunk uploads/downloads in flight per part.
///
/// 8 matches the `buffer_unordered(8)` factor specified by Plan 05 T6;
/// it's deliberately conservative because each in-flight chunk holds
/// 4 MiB of memory. The effective value is [`effective_parallelism`],
/// which lets `SANDBAWKS_S3_PARALLELISM` override it.
pub const PARALLELISM: usize = 8;

/// Effective in-flight chunk concurrency, read once from
/// `SANDBAWKS_S3_PARALLELISM` (falling back to [`PARALLELISM`]).
///
/// The snapshot transfer is per-request-latency-bound against garage (a
/// 4 MiB PUT can take hundreds of ms even on loopback), so throughput
/// scales with concurrency until the backend saturates — this knob lets us
/// tune it (and A/B it in the bench) without a rebuild. Peak resident chunk
/// bodies are `~(2 * N + 1) * CHUNK_SIZE`, so raising `N` raises the node
/// DaemonSet's peak RSS (≈ 68 MiB at 8, ≈ 260 MiB at 32) — keep it well
/// under the pod's memory limit. Invalid / `0` / unset values use the
/// default. Cached in a `OnceLock` so every part sees the same value.
pub fn effective_parallelism() -> usize {
    static CELL: OnceLock<usize> = OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("SANDBAWKS_S3_PARALLELISM")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(PARALLELISM)
    })
}

/// Minimal storage-backend abstraction used by [`S3SnapshotSink`] and
/// [`S3SnapshotSource`].
///
/// The production impl wraps [`s3::Bucket`]; [`InMemoryS3`] provides
/// the test double. Keeping the trait `pub(crate)`-shaped would force
/// the property test in `tests/s3.rs` to live inside the crate as a
/// `#[cfg(test)]` module — we expose it so the integration test can
/// construct an `InMemoryS3` from the outside.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Return `Ok(true)` iff an object exists at `key`.
    async fn head(&self, key: &str) -> Result<bool, RuntimeError>;

    /// Return the object's size in bytes, or `None` if it doesn't exist.
    ///
    /// Like [`head`](BlobStore::head) but surfaces the content-length, for the
    /// head+size validate level (confirm a part's chunks are present and the
    /// right size without downloading their bodies).
    async fn head_size(&self, key: &str) -> Result<Option<u64>, RuntimeError>;

    /// Upload `body` to `key`, overwriting any existing object.
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), RuntimeError>;

    /// Download the object at `key`. Returns
    /// [`RuntimeError::SnapshotDownload`] if the object is missing.
    async fn get(&self, key: &str) -> Result<Vec<u8>, RuntimeError>;
}

/// Production [`BlobStore`] impl backed by `rust-s3`.
///
/// Status-code handling lives here rather than at call sites so the
/// trait surface stays bool/`Vec<u8>` and the chunk loop in
/// [`S3SnapshotSink::put_part`] reads as straight async code.
#[async_trait]
impl BlobStore for Bucket {
    async fn head(&self, key: &str) -> Result<bool, RuntimeError> {
        match self.head_object(key).await {
            Ok((_meta, status)) if (200..300).contains(&status) => Ok(true),
            Ok((_meta, 404)) => Ok(false),
            Ok((_meta, status)) => Err(RuntimeError::SnapshotUpload(format!(
                "S3 HEAD {key}: status {status}"
            ))),
            Err(e) => {
                // rust-s3 maps HTTP 404 to `S3Error::HttpFailWithBody(404, _)`
                // on some code paths. Treat any "not found" textual hint as
                // a missing object so the idempotency check is robust to
                // backend-error-shape drift.
                let msg = e.to_string();
                if msg.contains("404") || msg.to_ascii_lowercase().contains("not found") {
                    Ok(false)
                } else {
                    Err(RuntimeError::SnapshotUpload(format!("S3 HEAD {key}: {e}")))
                }
            }
        }
    }

    async fn head_size(&self, key: &str) -> Result<Option<u64>, RuntimeError> {
        match self.head_object(key).await {
            // `content_length` is `Option<i64>`; clamp a (shouldn't-happen)
            // negative to 0 before the unsigned cast.
            Ok((meta, status)) if (200..300).contains(&status) => {
                Ok(Some(meta.content_length.unwrap_or(0).max(0) as u64))
            }
            Ok((_meta, 404)) => Ok(None),
            Ok((_meta, status)) => Err(RuntimeError::SnapshotUpload(format!(
                "S3 HEAD {key}: status {status}"
            ))),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("404") || msg.to_ascii_lowercase().contains("not found") {
                    Ok(None)
                } else {
                    Err(RuntimeError::SnapshotUpload(format!("S3 HEAD {key}: {e}")))
                }
            }
        }
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), RuntimeError> {
        // NOTE: rust-s3 0.35 SHA-256s `body` again here for the SigV4
        // `x-amz-content-sha256` header — a second hash over bytes we already
        // hashed to derive `key` (content addressing). It is NOT removable via
        // config: rust-s3 always signs the payload for PUT (UNSIGNED-PAYLOAD is
        // wired only for presigned URLs) and exposes no hook to supply a
        // precomputed digest. Eliminating it would mean forking rust-s3 or
        // migrating to aws-sdk-s3 (unsigned-payload). Left as-is: the GiB bench
        // showed upload is backend-bound, not node-CPU-bound, so this redundant
        // hash (~1 CPU-s/GiB on a non-saturated node) is off the critical path.
        let resp = self
            .put_object(key, &body)
            .await
            .map_err(|e| RuntimeError::SnapshotUpload(format!("S3 PUT {key}: {e}")))?;
        let status = resp.status_code();
        if !(200..300).contains(&status) {
            return Err(RuntimeError::SnapshotUpload(format!(
                "S3 PUT {key}: status {status}"
            )));
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, RuntimeError> {
        let resp = self
            .get_object(key)
            .await
            .map_err(|e| RuntimeError::SnapshotDownload(format!("S3 GET {key}: {e}")))?;
        let status = resp.status_code();
        if !(200..300).contains(&status) {
            return Err(RuntimeError::SnapshotDownload(format!(
                "S3 GET {key}: status {status}"
            )));
        }
        Ok(resp.to_vec())
    }
}

/// Per-part index file persisted alongside chunk bodies.
///
/// Listing chunks explicitly keeps the get path from having to `LIST`
/// the bucket — a single point lookup of `parts/<name>.json` is enough
/// to know the ordered set of chunks to fetch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PartIndex {
    /// Ordered list of chunks; concatenating their bodies reproduces the
    /// original part payload byte-for-byte.
    chunks: Vec<ChunkRef>,
    /// Total byte length of the assembled part. Cached so the source can
    /// pre-size its output buffer without summing chunk sizes.
    total_size_bytes: u64,
    /// SHA-256 over the entire assembled part — matches what
    /// `SnapshotPart::from_bytes` returns for this part.
    sha256: String,
}

/// A single content-addressed chunk reference inside a [`PartIndex`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ChunkRef {
    /// Lower-case-hex SHA-256 of the chunk body. Doubles as the chunk's
    /// storage key under `<key_prefix>/chunks/<sha256>`.
    sha256: String,
    /// On-the-wire byte length of the chunk body. Last chunk may be
    /// shorter than [`CHUNK_SIZE`]; all other chunks are exactly
    /// [`CHUNK_SIZE`] bytes.
    size_bytes: u64,
}

/// Lower-case-hex encode a SHA-256 digest.
///
/// Inlined rather than pulling in `hex` since the chunk loop is the
/// only consumer and the helper is three lines. Mirrors the
/// equivalent inline encoder in `sandbawks_types::snapshot`.
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    finalize_hex(hasher)
}

/// Lower-case-hex encode a finalized [`Sha256`] hasher.
///
/// Split out from [`sha256_hex`] so the streaming `put_part_file` path —
/// which feeds chunk bodies into a single running hasher rather than one
/// contiguous slice — can produce the part digest the same way (DRY).
fn finalize_hex(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write as _;
        // `write!` into a `String` is infallible.
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Fill `buf` from `file`, returning how many bytes were read.
///
/// The return is `buf.len()` unless EOF was reached first, in which case
/// it's the (shorter) tail length. `std::io::Read::read` is permitted to
/// return short reads, so we loop until the buffer is full or the file
/// ends — `read_exact` can't be used here because the final chunk of a
/// part is almost always shorter than [`CHUNK_SIZE`].
fn read_full(file: &mut std::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
    use std::io::Read as _;
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            // A signal interrupting the read is retryable, not an error.
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Snapshot sink that streams part bytes to an S3-compatible bucket as
/// chunked, content-addressed objects.
///
/// Wrap a `rust-s3` [`Bucket`] via [`S3SnapshotSink::from_bucket`] for
/// production; tests use [`S3SnapshotSink::new`] to inject an
/// alternative [`BlobStore`] (e.g. [`InMemoryS3`]).
#[derive(Clone)]
pub struct S3SnapshotSink {
    /// Backing object store. Held behind `Arc<dyn ...>` so chunk
    /// uploads in `put_part` can clone the handle into each future
    /// the `buffer_unordered` stream drives.
    bucket: Arc<dyn BlobStore>,
    /// Object-key prefix under which `manifest.json`, `parts/...`, and
    /// `chunks/...` are written. No trailing slash.
    key_prefix: String,
    /// Bucket name (the `<bucket>` in `s3://<bucket>/<prefix>/manifest.json`).
    /// Held alongside the `BlobStore` because the `BlobStore` trait
    /// only exposes put/get/head — it doesn't surface the bucket name.
    /// Used by [`SnapshotSink::manifest_url`].
    bucket_name: String,
    /// URL scheme for [`SnapshotSink::manifest_url`] — `"s3"` for the
    /// rust-s3 backend, `"azure"` when this generic sink wraps an
    /// [`crate::azure_blob::AzureBlobStore`]. Defaults to `"s3"` via the
    /// existing constructors so every prior call site is unaffected.
    url_scheme: String,
}

impl S3SnapshotSink {
    /// Build a sink over an arbitrary [`BlobStore`], emitting `s3://`
    /// manifest URLs. Back-compat shim over [`Self::new_with_scheme`].
    pub fn new(
        bucket: Arc<dyn BlobStore>,
        bucket_name: impl Into<String>,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self::new_with_scheme(bucket, bucket_name, key_prefix, "s3")
    }

    /// Build a sink over an arbitrary [`BlobStore`] with an explicit
    /// manifest-URL scheme (`"s3"` or `"azure"`).
    pub fn new_with_scheme(
        bucket: Arc<dyn BlobStore>,
        bucket_name: impl Into<String>,
        key_prefix: impl Into<String>,
        url_scheme: impl Into<String>,
    ) -> Self {
        Self {
            bucket,
            key_prefix: key_prefix.into(),
            bucket_name: bucket_name.into(),
            url_scheme: url_scheme.into(),
        }
    }

    /// Convenience constructor for the production case: wrap a
    /// `rust-s3` [`Bucket`] directly. `Bucket::new` returns a
    /// `Box<Bucket>`, so the caller unwraps and passes by value.
    pub fn from_bucket(
        bucket: Bucket,
        bucket_name: impl Into<String>,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self::new(Arc::new(bucket), bucket_name, key_prefix)
    }

    /// Write the part index object that ties an ordered set of chunks
    /// back to their part name, and return the [`SnapshotPart`] manifest
    /// entry.
    ///
    /// The index is written *last*, after every chunk it references is
    /// durably stored, so the read side never observes a part pointing at
    /// a chunk that hasn't landed yet. Shared by the buffered
    /// [`SnapshotSink::put_part`] and the streaming
    /// [`SnapshotSink::put_part_file`] paths (DRY).
    async fn write_part_index(
        &self,
        name: &str,
        chunk_refs: Vec<ChunkRef>,
        total_len: u64,
        part_sha: String,
    ) -> Result<SnapshotPart, RuntimeError> {
        let index = PartIndex {
            chunks: chunk_refs,
            total_size_bytes: total_len,
            sha256: part_sha.clone(),
        };
        let index_bytes = serde_json::to_vec(&index).map_err(|e| {
            RuntimeError::SnapshotUpload(format!("part index serialize {name}: {e}"))
        })?;
        self.bucket
            .put(&part_index_key(&self.key_prefix, name), index_bytes)
            .await?;

        Ok(SnapshotPart {
            name: name.to_owned(),
            sha256: part_sha,
            size_bytes: total_len,
            // The sink only ever sees the bytes streamed to it (already
            // compressed for `memory.zst`); the gVisor pauser attaches the
            // uncompressed working-set size to the returned part (#243).
            uncompressed_bytes: None,
        })
    }
}

/// Snapshot source that reads parts written by an [`S3SnapshotSink`].
///
/// Symmetric to the sink; uses the same `(prefix, BlobStore)` pair so
/// the controller can construct paired sink/source from one bucket
/// handle.
#[derive(Clone)]
pub struct S3SnapshotSource {
    bucket: Arc<dyn BlobStore>,
    key_prefix: String,
}

impl S3SnapshotSource {
    /// Build a source over an arbitrary [`BlobStore`].
    pub fn new(bucket: Arc<dyn BlobStore>, key_prefix: impl Into<String>) -> Self {
        Self {
            bucket,
            key_prefix: key_prefix.into(),
        }
    }

    /// Convenience constructor mirroring [`S3SnapshotSink::from_bucket`].
    pub fn from_bucket(bucket: Bucket, key_prefix: impl Into<String>) -> Self {
        Self::new(Arc::new(bucket), key_prefix)
    }
}

fn manifest_key(prefix: &str) -> String {
    format!("{prefix}/manifest.json")
}

fn part_index_key(prefix: &str, name: &str) -> String {
    format!("{prefix}/parts/{name}.json")
}

/// Join a base key prefix with a per-snapshot `sub` prefix, trimming stray
/// slashes so neither an empty base nor a leading/trailing `/` produces a
/// `//` or a leading `/` in the resulting object keys. Used by
/// [`SnapshotSink::scoped`] / [`SnapshotSource::scoped`] to nest each
/// sandbox's bundle under `<base>/<sandbox-id>` so concurrent sandboxes
/// never share the mutable `manifest.json` / `parts/<name>.json` keys.
fn join_prefix(base: &str, sub: &str) -> String {
    let base = base.trim_matches('/');
    let sub = sub.trim_matches('/');
    match (base.is_empty(), sub.is_empty()) {
        (true, true) => String::new(),
        (true, false) => sub.to_owned(),
        (false, true) => base.to_owned(),
        (false, false) => format!("{base}/{sub}"),
    }
}

fn chunk_key(prefix: &str, sha256: &str) -> String {
    format!("{prefix}/chunks/{sha256}")
}

#[async_trait]
impl SnapshotSink for S3SnapshotSink {
    #[tracing::instrument(
        skip_all,
        name = "upload:node::part",
        err,
        fields(part = %name, bytes = bytes.len())
    )]
    async fn put_part(&self, name: &str, bytes: Vec<u8>) -> Result<SnapshotPart, RuntimeError> {
        let total_len = bytes.len() as u64;
        let part_sha = sha256_hex(&bytes);

        // Split into 4 MiB chunks. `chunks(CHUNK_SIZE)` borrows from
        // `bytes`; we materialize each chunk into its own `Vec<u8>` so
        // the futures driving the parallel uploads each own their
        // payload (S3 PUTs are `'static`-flavored).
        let chunk_bodies: Vec<Vec<u8>> = if bytes.is_empty() {
            // A zero-length part still needs a deterministic on-disk
            // shape — the part index will simply hold zero chunks and
            // the empty-part sha256 is the SHA-256 of zero bytes.
            Vec::new()
        } else {
            bytes
                .chunks(effective_chunk_size())
                .map(<[u8]>::to_vec)
                .collect()
        };
        drop(bytes); // free the assembled buffer before parallel uploads run.

        // Pre-compute per-chunk sha256s on the current task; chunk
        // hashing is CPU-bound and dwarfed by the network round-trips
        // anyway, so we don't bother spawning blocking tasks.
        let chunk_refs: Vec<ChunkRef> = chunk_bodies
            .iter()
            .map(|body| ChunkRef {
                sha256: sha256_hex(body),
                size_bytes: body.len() as u64,
            })
            .collect();

        // Drive uploads in parallel, capped at PARALLELISM in-flight.
        // Each future does HEAD-then-PUT so a retry across a partial
        // upload is a no-op for chunks that already landed.
        let upload_items: Vec<(ChunkRef, Vec<u8>)> =
            chunk_refs.iter().cloned().zip(chunk_bodies).collect();

        let bucket = self.bucket.clone();
        let key_prefix = self.key_prefix.clone();
        stream::iter(upload_items.into_iter().map(move |(chunk_ref, body)| {
            let bucket = bucket.clone();
            let key_prefix = key_prefix.clone();
            async move {
                let key = chunk_key(&key_prefix, &chunk_ref.sha256);
                if bucket.head(&key).await? {
                    tracing::debug!(
                        chunk_sha = %chunk_ref.sha256,
                        %key,
                        "chunk already present; skipping PUT",
                    );
                    return Ok::<(), RuntimeError>(());
                }
                bucket.put(&key, body).await
            }
        }))
        .buffer_unordered(effective_parallelism())
        .try_collect::<Vec<()>>()
        .await?;

        // Write the part index last — it's the read-side's entry point
        // and must only be visible once every chunk it references has
        // been durably written.
        self.write_part_index(name, chunk_refs, total_len, part_sha)
            .await
    }

    // `bytes` is declared `Empty` and recorded once the transfer is sized
    // (post-read), giving this streaming span size parity with the buffered
    // `upload:node::part` span (which knows the length up front) so an operator
    // can filter both transfer paths by byte count.
    #[tracing::instrument(
        skip_all,
        name = "stream_upload:node::part",
        err,
        fields(part = %name, bytes = tracing::field::Empty)
    )]
    async fn put_part_file(
        &self,
        name: &str,
        path: &std::path::Path,
    ) -> Result<SnapshotPart, RuntimeError> {
        // Streaming counterpart of `put_part`: the file is read in
        // `CHUNK_SIZE` blocks on a blocking worker and the bodies are
        // handed across a *bounded* channel to the async uploader, so the
        // node DaemonSet's peak RSS is independent of the part size
        // (#293). Resident chunk bodies are bounded by the channel depth
        // plus the in-flight set plus the reader's current block:
        // `(2 * PARALLELISM + 1) * CHUNK_SIZE` ≈ 68 MiB, ~80 MiB once the
        // zstd encoder state and S3 client connection buffers are counted
        // — flat whether the part is 16 MiB or 4 GiB.
        let started = std::time::Instant::now();
        let path = path.to_owned();
        let parallelism = effective_parallelism();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Vec<u8>)>(parallelism);

        // Reader: stream the file, hashing each chunk and the running part
        // digest, and back-pressured by the bounded channel so it never
        // races ahead of the uploader. Returns the ordered chunk refs, the
        // total length, and the whole-part sha256 once the file is drained.
        let reader = tokio::task::spawn_blocking(
            move || -> Result<(Vec<ChunkRef>, u64, String, u64, u64), RuntimeError> {
                let mut file = std::fs::File::open(&path).map_err(|e| {
                    RuntimeError::SnapshotUpload(format!("open part file {}: {e}", path.display()))
                })?;
                let mut part_hasher = Sha256::new();
                let mut chunk_refs: Vec<ChunkRef> = Vec::new();
                let mut total_len: u64 = 0;
                // Split the reader's wall time into BUSY (disk read + sha256)
                // vs BLOCKED (waiting on the bounded channel, i.e. the uploader
                // is the long pole). read_hash dominating ⇒ process/IOPS-bound;
                // send_block dominating ⇒ transfer-bound. (#perf attribution.)
                let mut read_hash_ns: u64 = 0;
                let mut send_block_ns: u64 = 0;
                let chunk_size = effective_chunk_size();
                loop {
                    let t_read = std::time::Instant::now();
                    let mut body = vec![0u8; chunk_size];
                    let n = read_full(&mut file, &mut body).map_err(|e| {
                        RuntimeError::SnapshotUpload(format!(
                            "read part file {}: {e}",
                            path.display()
                        ))
                    })?;
                    if n == 0 {
                        read_hash_ns += t_read.elapsed().as_nanos() as u64;
                        break;
                    }
                    body.truncate(n);
                    let chunk_sha = sha256_hex(&body);
                    part_hasher.update(&body);
                    total_len += n as u64;
                    chunk_refs.push(ChunkRef {
                        sha256: chunk_sha.clone(),
                        size_bytes: n as u64,
                    });
                    read_hash_ns += t_read.elapsed().as_nanos() as u64;
                    // `Err` means the receiver was dropped — an upload
                    // failed and the async side gave up, so stop reading.
                    let t_send = std::time::Instant::now();
                    let send_err = tx.blocking_send((chunk_sha, body)).is_err();
                    send_block_ns += t_send.elapsed().as_nanos() as u64;
                    if send_err {
                        break;
                    }
                }
                Ok((
                    chunk_refs,
                    total_len,
                    finalize_hex(part_hasher),
                    read_hash_ns,
                    send_block_ns,
                ))
            },
        );

        // Uploader: drive HEAD-then-PUT with at most `PARALLELISM` chunks
        // in flight. The `select!` polls the in-flight set and the channel
        // *concurrently*: a plain `loop { rx.recv().await; ... }` would
        // leave `FuturesUnordered` unpolled whenever the channel is empty,
        // so a disk-bound reader (the likely case on the snapshot-cache
        // volume, with fast S3 to a local Garage) would stall every upload
        // behind the next read and serialize what should be parallel
        // transfer. `biased` reaps completions before admitting new work,
        // keeping the resident-body count from overshooting the cap. Each
        // future does HEAD-then-PUT so a retry across a partial upload is a
        // no-op for chunks that already landed (idempotency, as `put_part`).
        //
        // DO NOT drop this HEAD to save the round-trip: it is load-bearing for
        // *cross-pause content-addressed dedup* — a sandbox paused repeatedly
        // (warm pool, #261) only re-uploads chunks whose bytes changed; the
        // HEAD hit skips the rest. The bench logs `skipped=0` / all-404 HEADs
        // only because it pauses a *fresh* sandbox each iteration (nothing to
        // dedup against), which makes the HEAD *look* like pure waste — it
        // isn't in production. The safe way to remove the separate round-trip
        // is a conditional PUT (`If-None-Match: *`), which rust-s3 0.35 does
        // not expose (only `put_object`), or a single prefix `LIST` up front
        // instead of N HEADs.
        // Upload-internal counters (#perf): separate time SHIPPING chunks
        // (HEAD/PUT, network/backend) from the reader's PRODUCING time above,
        // plus per-op latency and the peak concurrency actually reached. `head`
        // /`put` are summed across in-flight ops so they exceed wall time; the
        // signal is `avg_put` (per-chunk PUT latency) and whether `max_inflight`
        // reaches PARALLELISM (else the uploader is starved by the reader).
        let head_ns = Arc::new(AtomicU64::new(0));
        let put_ns = Arc::new(AtomicU64::new(0));
        let head_count = Arc::new(AtomicU64::new(0));
        let put_count = Arc::new(AtomicU64::new(0));
        let skip_count = Arc::new(AtomicU64::new(0));
        let mut max_inflight: usize = 0;
        let mut inflight = futures::stream::FuturesUnordered::new();
        let mut upload_err: Option<RuntimeError> = None;
        let mut reader_done = false;
        while !reader_done || !inflight.is_empty() {
            tokio::select! {
                biased;
                // Reap a finished upload first so memory drains before more
                // work is admitted. Guarded so we never poll an empty set
                // (which would yield `None` and busy-spin the loop).
                Some(res) = inflight.next(), if !inflight.is_empty() => {
                    if let Err(e) = res {
                        upload_err = Some(e);
                        break;
                    }
                }
                // Admit a new chunk only while under the in-flight cap, so
                // peak resident bodies stay bounded by `PARALLELISM`.
                maybe = rx.recv(), if !reader_done && inflight.len() < parallelism => {
                    match maybe {
                        Some((chunk_sha, body)) => {
                            let bucket = self.bucket.clone();
                            let key = chunk_key(&self.key_prefix, &chunk_sha);
                            let head_ns = head_ns.clone();
                            let put_ns = put_ns.clone();
                            let head_count = head_count.clone();
                            let put_count = put_count.clone();
                            let skip_count = skip_count.clone();
                            inflight.push(async move {
                                let t_head = std::time::Instant::now();
                                let exists = bucket.head(&key).await?;
                                head_ns.fetch_add(t_head.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                head_count.fetch_add(1, Ordering::Relaxed);
                                if exists {
                                    skip_count.fetch_add(1, Ordering::Relaxed);
                                    return Ok::<(), RuntimeError>(());
                                }
                                let t_put = std::time::Instant::now();
                                let res = bucket.put(&key, body).await;
                                put_ns.fetch_add(t_put.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                put_count.fetch_add(1, Ordering::Relaxed);
                                res
                            });
                            max_inflight = max_inflight.max(inflight.len());
                        }
                        // Channel closed: the reader finished and dropped `tx`.
                        None => reader_done = true,
                    }
                }
            }
        }
        // Drop the receiver so a reader still blocked in `blocking_send`
        // (only possible on the upload-error path, where we broke out with
        // work still pending) unblocks and the `spawn_blocking` task can
        // finish, then join it.
        drop(rx);
        // If BOTH the reader and an upload failed, the reader's error wins:
        // `??` propagates it here before the `upload_err` check below. That
        // ordering is intentional — a read/hash failure is the root cause,
        // and the upload error is the downstream symptom of the receiver
        // going away, so surfacing the read failure is the more useful
        // diagnostic.
        let (chunk_refs, total_len, part_sha, read_hash_ns, send_block_ns) = reader
            .await
            .map_err(|e| RuntimeError::SnapshotUpload(format!("part reader join {name}: {e}")))??;
        if let Some(e) = upload_err {
            return Err(e);
        }
        // Now that the transfer is sized, fill the span's `bytes` field.
        tracing::Span::current().record("bytes", total_len);

        // Emit a per-part completion record. This PR is specifically about
        // a memory/throughput characteristic the existing
        // `snapshotjob.duration` envelope can't see; a line that separates
        // upload cost (bytes, chunks, elapsed) from checkpoint cost lets the
        // bench's provisional pause ceiling be tightened from data (#293).
        let chunks = chunk_refs.len();
        let part = self
            .write_part_index(name, chunk_refs, total_len, part_sha)
            .await?;
        // Upload-internal attribution (#perf): `read_hash`/`send_block` are the
        // reader's busy vs back-pressured time; `head`/`put` are summed across
        // in-flight ops (can exceed wall time), so `avg_put` (per-chunk PUT
        // latency) is the network/backend signal. read_hash ≈ elapsed ⇒
        // process/IOPS-bound; send_block ≈ elapsed ⇒ transfer-bound.
        let puts = put_count.load(Ordering::Relaxed);
        // `checked_div` yields None when no chunk was PUT (all HEAD-skipped),
        // so avg PUT latency reads 0 rather than dividing by zero.
        let avg_put_ms = put_ns
            .load(Ordering::Relaxed)
            .checked_div(puts)
            .unwrap_or(0)
            / 1_000_000;
        tracing::info!(
            part = %name,
            bytes_compressed = total_len,
            chunks,
            elapsed_ms = started.elapsed().as_millis() as u64,
            read_hash_ms = read_hash_ns / 1_000_000,
            send_block_ms = send_block_ns / 1_000_000,
            head_ms = head_ns.load(Ordering::Relaxed) / 1_000_000,
            put_ms = put_ns.load(Ordering::Relaxed) / 1_000_000,
            heads = head_count.load(Ordering::Relaxed),
            puts,
            skipped = skip_count.load(Ordering::Relaxed),
            parallelism,
            max_inflight,
            avg_put_ms,
            "streamed snapshot part to object store",
        );
        Ok(part)
    }

    #[tracing::instrument(skip_all, name = "upload:node::manifest", err, fields(sandbox = %manifest.sandbox_id))]
    async fn put_manifest(&self, manifest: &SnapshotManifest) -> Result<(), RuntimeError> {
        let bytes = serde_json::to_vec(manifest)
            .map_err(|e| RuntimeError::SnapshotUpload(format!("manifest serialize: {e}")))?;
        self.bucket
            .put(&manifest_key(&self.key_prefix), bytes)
            .await
    }

    fn manifest_url(&self) -> String {
        // Composes the canonical `<scheme>://` URL that maps directly to
        // the bucket+key `put_manifest` writes to. The controller folds
        // this into `Sandbox.status.snapshotRef` so a `kubectl get
        // sandbox` always reveals where its bundle lives. The scheme is
        // `s3` by default and `azure` when wrapping an Azure blob store.
        let scheme = &self.url_scheme;
        if self.key_prefix.is_empty() {
            format!("{scheme}://{}/manifest.json", self.bucket_name)
        } else {
            format!(
                "{scheme}://{}/{}/manifest.json",
                self.bucket_name, self.key_prefix
            )
        }
    }

    fn scoped(&self, sub_prefix: &str) -> Arc<dyn SnapshotSink> {
        // Share the underlying `BlobStore` (and its HTTP pool) but nest the
        // keyspace under `<base>/<sub_prefix>`; `manifest_url` then encodes
        // the scoped path into the recorded `snapshotRef`.
        Arc::new(Self {
            bucket: self.bucket.clone(),
            key_prefix: join_prefix(&self.key_prefix, sub_prefix),
            bucket_name: self.bucket_name.clone(),
            url_scheme: self.url_scheme.clone(),
        })
    }
}

#[async_trait]
impl SnapshotSource for S3SnapshotSource {
    #[tracing::instrument(skip_all, name = "download:node::part", err, fields(part = %name))]
    async fn get_part(&self, name: &str) -> Result<Vec<u8>, RuntimeError> {
        // Look up the part index — single point GET, no LIST.
        let index_bytes = self
            .bucket
            .get(&part_index_key(&self.key_prefix, name))
            .await?;
        let index: PartIndex = serde_json::from_slice(&index_bytes)
            .map_err(|e| RuntimeError::SnapshotDownload(format!("part index parse {name}: {e}")))?;

        // Fetch chunks in parallel; verify each chunk's sha256 as the
        // bytes arrive so a corrupted backend object surfaces at the
        // earliest opportunity rather than only after re-assembly.
        let bucket = self.bucket.clone();
        let key_prefix = self.key_prefix.clone();
        let indexed = index.chunks.clone();
        let fetched: Vec<(usize, Vec<u8>)> = stream::iter(indexed.into_iter().enumerate().map(
            move |(seq, chunk_ref)| {
                let bucket = bucket.clone();
                let key_prefix = key_prefix.clone();
                async move {
                    let key = chunk_key(&key_prefix, &chunk_ref.sha256);
                    let body = bucket.get(&key).await?;
                    let actual = sha256_hex(&body);
                    if actual != chunk_ref.sha256 {
                        return Err(RuntimeError::SnapshotDownload(format!(
                            "chunk {key}: expected sha256 {expected} got {actual}",
                            expected = chunk_ref.sha256,
                        )));
                    }
                    if body.len() as u64 != chunk_ref.size_bytes {
                        return Err(RuntimeError::SnapshotDownload(format!(
                            "chunk {key}: expected {expected} bytes got {actual}",
                            expected = chunk_ref.size_bytes,
                            actual = body.len(),
                        )));
                    }
                    Ok::<(usize, Vec<u8>), RuntimeError>((seq, body))
                }
            },
        ))
        .buffer_unordered(effective_parallelism())
        .try_collect::<Vec<(usize, Vec<u8>)>>()
        .await?;

        // Stitch chunks back in declared order. The unordered stream
        // can complete chunks out of sequence, so we slot them in by
        // index rather than appending.
        let mut ordered: Vec<Option<Vec<u8>>> = (0..index.chunks.len()).map(|_| None).collect();
        for (seq, body) in fetched {
            ordered[seq] = Some(body);
        }
        let mut assembled = Vec::with_capacity(index.total_size_bytes as usize);
        for slot in ordered {
            let body = slot.ok_or_else(|| {
                RuntimeError::SnapshotDownload(format!("part {name}: missing chunk slot"))
            })?;
            assembled.extend_from_slice(&body);
        }

        // Final aggregate sha256 check — guards against an index that
        // pointed at the right chunks but in a wrong order.
        let actual = sha256_hex(&assembled);
        if actual != index.sha256 {
            return Err(RuntimeError::SnapshotDownload(format!(
                "part {name}: aggregate sha256 mismatch (expected {expected} got {actual})",
                expected = index.sha256,
            )));
        }
        if assembled.len() as u64 != index.total_size_bytes {
            return Err(RuntimeError::SnapshotDownload(format!(
                "part {name}: assembled length mismatch (expected {expected} got {actual})",
                expected = index.total_size_bytes,
                actual = assembled.len(),
            )));
        }
        Ok(assembled)
    }

    // `bytes` recorded post-transfer (see `put_part_file`) for size parity
    // with the buffered `download:node::part` span.
    #[tracing::instrument(
        skip_all,
        name = "stream_download:node::part",
        err,
        fields(part = %name, bytes = tracing::field::Empty)
    )]
    async fn get_part_to_file(
        &self,
        name: &str,
        path: &std::path::Path,
    ) -> Result<SnapshotPart, RuntimeError> {
        // Streaming counterpart of `get_part`: chunks are fetched with
        // bounded concurrency and written to `path` *as they arrive*, so the
        // node DaemonSet's peak RSS is independent of the part size (#329).
        // Resident chunk bodies are bounded by the `buffered` look-ahead plus
        // the writer channel plus the writer's current block —
        // `(2 * PARALLELISM + 1) * CHUNK_SIZE` ≈ 68 MiB — flat whether the
        // part is 16 MiB or 4 GiB, mirroring the upload side's `put_part_file`.
        let started = std::time::Instant::now();
        let index_bytes = self
            .bucket
            .get(&part_index_key(&self.key_prefix, name))
            .await?;
        let index: PartIndex = serde_json::from_slice(&index_bytes)
            .map_err(|e| RuntimeError::SnapshotDownload(format!("part index parse {name}: {e}")))?;

        // Writer: a single blocking task drains an ordered, bounded channel
        // into a `BufWriter<File>`, feeding each body into the running part
        // digest. The bounded channel back-pressures the fetcher so it never
        // races ahead and pins more than the cap of chunk bodies in RAM.
        let dest = path.to_owned();
        let parallelism = effective_parallelism();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(parallelism);
        let writer = tokio::task::spawn_blocking(move || -> Result<(u64, String), RuntimeError> {
            use std::io::Write as _;
            let file = std::fs::File::create(&dest).map_err(|e| {
                RuntimeError::SnapshotDownload(format!("create part file {}: {e}", dest.display()))
            })?;
            let mut buf = std::io::BufWriter::new(file);
            let mut hasher = Sha256::new();
            let mut total: u64 = 0;
            while let Some(body) = rx.blocking_recv() {
                hasher.update(&body);
                total += body.len() as u64;
                buf.write_all(&body).map_err(|e| {
                    RuntimeError::SnapshotDownload(format!(
                        "write part file {}: {e}",
                        dest.display()
                    ))
                })?;
            }
            buf.flush().map_err(|e| {
                RuntimeError::SnapshotDownload(format!("flush part file {}: {e}", dest.display()))
            })?;
            Ok((total, finalize_hex(hasher)))
        });

        // Fetcher: ordered, bounded-concurrency chunk GETs. `buffered`
        // (not `buffer_unordered`) yields bodies in the chunks' declared
        // order while still running up to `PARALLELISM` GETs at once, so the
        // writer can stream straight to disk without a full reorder buffer.
        // Each chunk's sha256 + length is verified as it arrives.
        let bucket = self.bucket.clone();
        let key_prefix = self.key_prefix.clone();
        let mut fetched = stream::iter(index.chunks.clone().into_iter().map(move |chunk_ref| {
            let bucket = bucket.clone();
            let key_prefix = key_prefix.clone();
            async move {
                let key = chunk_key(&key_prefix, &chunk_ref.sha256);
                let body = bucket.get(&key).await?;
                let actual = sha256_hex(&body);
                if actual != chunk_ref.sha256 {
                    return Err(RuntimeError::SnapshotDownload(format!(
                        "chunk {key}: expected sha256 {expected} got {actual}",
                        expected = chunk_ref.sha256,
                    )));
                }
                if body.len() as u64 != chunk_ref.size_bytes {
                    return Err(RuntimeError::SnapshotDownload(format!(
                        "chunk {key}: expected {expected} bytes got {actual}",
                        expected = chunk_ref.size_bytes,
                        actual = body.len(),
                    )));
                }
                Ok::<Vec<u8>, RuntimeError>(body)
            }
        }))
        .buffered(parallelism);

        let mut fetch_err: Option<RuntimeError> = None;
        while let Some(res) = fetched.next().await {
            match res {
                // `Err` means the writer task ended early (a write failed and
                // dropped `rx`); stop fetching and let the join below surface
                // the writer's error.
                Ok(body) => {
                    if tx.send(body).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    fetch_err = Some(e);
                    break;
                }
            }
        }
        // Drop the sender so the writer's `blocking_recv` returns `None` and
        // the task finishes, then join it.
        drop(tx);
        let (total, part_sha) = writer.await.map_err(|e| {
            RuntimeError::SnapshotDownload(format!("part writer join {name}: {e}"))
        })??;
        // A fetch failure is the root cause (the writer just sees the channel
        // close), so surface it ahead of any benign writer completion.
        if let Some(e) = fetch_err {
            return Err(e);
        }

        // Aggregate checks, identical to `get_part`: guard against an index
        // that pointed at the right chunks but in the wrong order or count.
        if part_sha != index.sha256 {
            return Err(RuntimeError::SnapshotDownload(format!(
                "part {name}: aggregate sha256 mismatch (expected {expected} got {actual})",
                expected = index.sha256,
                actual = part_sha,
            )));
        }
        if total != index.total_size_bytes {
            return Err(RuntimeError::SnapshotDownload(format!(
                "part {name}: assembled length mismatch (expected {expected} got {actual})",
                expected = index.total_size_bytes,
                actual = total,
            )));
        }
        tracing::Span::current().record("bytes", total);
        tracing::info!(
            part = %name,
            bytes = total,
            chunks = index.chunks.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "streamed snapshot part from object store to disk",
        );
        Ok(SnapshotPart {
            name: name.to_owned(),
            sha256: part_sha,
            size_bytes: total,
            uncompressed_bytes: None,
        })
    }

    #[tracing::instrument(skip_all, name = "download:node::manifest", err)]
    async fn get_manifest(&self) -> Result<SnapshotManifest, RuntimeError> {
        let bytes = self.bucket.get(&manifest_key(&self.key_prefix)).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| RuntimeError::SnapshotDownload(format!("manifest parse: {e}")))
    }

    /// Head+size presence check (overrides the download-everything default):
    /// read the small part index, cross-check it against the manifest part,
    /// then HEAD every chunk and confirm each content-length matches the
    /// recorded size. No chunk bodies are transferred and nothing is
    /// decompressed — so it catches missing/truncated chunks and a crossed
    /// index/manifest, but NOT in-chunk byte corruption (that needs the `sha`
    /// or `full` level).
    #[tracing::instrument(
        skip_all,
        name = "verify:node::part_present",
        err,
        fields(part = %part.name, bytes = part.size_bytes)
    )]
    async fn verify_part_present(&self, part: &SnapshotPart) -> Result<(), RuntimeError> {
        let index_bytes = self
            .bucket
            .get(&part_index_key(&self.key_prefix, &part.name))
            .await?;
        let index: PartIndex = serde_json::from_slice(&index_bytes).map_err(|e| {
            RuntimeError::SnapshotDownload(format!("part index parse {}: {e}", part.name))
        })?;
        // Index vs manifest identity — the cheap guard that caught the
        // cross-sandbox key collision (a crossed index/manifest), kept here so
        // head+size doesn't silently accept a mismatched bundle.
        if index.sha256 != part.sha256 {
            return Err(RuntimeError::SnapshotDownload(format!(
                "part {}: index sha256 {} != manifest {}",
                part.name, index.sha256, part.sha256
            )));
        }
        if index.total_size_bytes != part.size_bytes {
            return Err(RuntimeError::SnapshotDownload(format!(
                "part {}: index size {} != manifest {}",
                part.name, index.total_size_bytes, part.size_bytes
            )));
        }
        // HEAD every chunk in parallel; verify presence + content-length.
        let bucket = self.bucket.clone();
        let key_prefix = self.key_prefix.clone();
        let observed: u64 = stream::iter(index.chunks.iter().cloned().map(move |chunk_ref| {
            let bucket = bucket.clone();
            let key_prefix = key_prefix.clone();
            async move {
                let key = chunk_key(&key_prefix, &chunk_ref.sha256);
                match bucket.head_size(&key).await? {
                    Some(sz) if sz == chunk_ref.size_bytes => Ok::<u64, RuntimeError>(sz),
                    Some(sz) => Err(RuntimeError::SnapshotDownload(format!(
                        "chunk {key}: size {sz} != recorded {}",
                        chunk_ref.size_bytes
                    ))),
                    None => Err(RuntimeError::SnapshotDownload(format!(
                        "chunk {key}: missing"
                    ))),
                }
            }
        }))
        .buffer_unordered(effective_parallelism())
        .try_fold(0u64, |acc, sz| async move { Ok(acc + sz) })
        .await?;
        if observed != index.total_size_bytes {
            return Err(RuntimeError::SnapshotDownload(format!(
                "part {}: chunk sizes sum {observed} != index total {}",
                part.name, index.total_size_bytes
            )));
        }
        Ok(())
    }

    fn scoped(&self, sub_prefix: &str) -> Arc<dyn SnapshotSource> {
        // Mirror of `S3SnapshotSink::scoped`: read under the same
        // `<base>/<sub_prefix>` the pause path wrote, so a sandbox's
        // pause→resume round-trips while staying isolated from others.
        Arc::new(Self {
            bucket: self.bucket.clone(),
            key_prefix: join_prefix(&self.key_prefix, sub_prefix),
        })
    }
}

/// In-memory [`BlobStore`] used by `tests/s3.rs`.
///
/// Backed by a single `Arc<Mutex<HashMap<String, Vec<u8>>>>` so the
/// same store handle can be cloned and passed to both a sink and a
/// matching source — exactly the pause-then-resume pattern the
/// Hegel round-trip test exercises.
///
/// The mutex is `std::sync::Mutex` rather than `tokio::sync::Mutex`
/// because the guard is never held across an `.await`.
#[derive(Debug, Clone, Default)]
pub struct InMemoryS3 {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl InMemoryS3 {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current number of stored keys (chunks + part indexes + the
    /// manifest, if written). Tests assert on this to confirm
    /// idempotent retries didn't write extra objects.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("InMemoryS3 poisoned").len()
    }

    /// `true` iff nothing has been written.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().expect("InMemoryS3 poisoned").is_empty()
    }
}

#[async_trait]
impl BlobStore for InMemoryS3 {
    async fn head(&self, key: &str) -> Result<bool, RuntimeError> {
        Ok(self
            .inner
            .lock()
            .expect("InMemoryS3 poisoned")
            .contains_key(key))
    }

    async fn head_size(&self, key: &str) -> Result<Option<u64>, RuntimeError> {
        Ok(self
            .inner
            .lock()
            .expect("InMemoryS3 poisoned")
            .get(key)
            .map(|v| v.len() as u64))
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), RuntimeError> {
        self.inner
            .lock()
            .expect("InMemoryS3 poisoned")
            .insert(key.to_owned(), body);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, RuntimeError> {
        self.inner
            .lock()
            .expect("InMemoryS3 poisoned")
            .get(key)
            .cloned()
            .ok_or_else(|| RuntimeError::SnapshotDownload(format!("InMemoryS3: no key {key}")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// 4 MiB + 1 byte forces exactly two chunks: the second chunk is a
    /// single byte and exercises the "last chunk smaller than CHUNK_SIZE"
    /// branch.
    const ODD_BOUNDARY_LEN: usize = CHUNK_SIZE + 1;

    /// A [`BlobStore`] double that delays each `put`/`get` and records the
    /// maximum number of overlapping such calls.
    ///
    /// Used to prove the streaming transfer runs chunk uploads/downloads
    /// *concurrently* rather than serializing them — the regression guard
    /// for the `FuturesUnordered`-not-polled-while-awaiting-`rx.recv` bug
    /// on the upload side (#293) and the `buffered(PARALLELISM)` download
    /// side (#329). The delay is real wall-clock time (not paused), so
    /// overlap is observable as a max-in-flight count above 1; a serial
    /// loop caps it at exactly 1.
    #[derive(Clone)]
    struct ConcurrencyProbeStore {
        inner: Arc<InMemoryS3>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl ConcurrencyProbeStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(InMemoryS3::new()),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }

        /// Reset the high-water mark so a later phase (e.g. the download in
        /// `get_part_to_file_fetches_chunks_concurrently`) is measured in
        /// isolation from the concurrent puts that populated the store.
        fn reset_max(&self) {
            self.max_in_flight.store(0, Ordering::SeqCst);
        }

        /// Enter an instrumented critical section: bump the in-flight count,
        /// record the new high-water mark, and hold the slot long enough
        /// that genuinely-concurrent calls overlap in wall-clock time. The
        /// returned guard decrements the count on drop. Shared by `put` and
        /// `get` so both transfer directions use identical accounting (DRY).
        async fn enter(&self) {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        fn leave(&self) {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl BlobStore for ConcurrencyProbeStore {
        async fn head(&self, key: &str) -> Result<bool, RuntimeError> {
            self.inner.head(key).await
        }

        async fn head_size(&self, key: &str) -> Result<Option<u64>, RuntimeError> {
            self.inner.head_size(key).await
        }

        async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), RuntimeError> {
            self.enter().await;
            let res = self.inner.put(key, body).await;
            self.leave();
            res
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, RuntimeError> {
            self.enter().await;
            let res = self.inner.get(key).await;
            self.leave();
            res
        }
    }

    #[tokio::test]
    async fn round_trip_small_part() {
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "sbx-1/2026/01");
        let source = S3SnapshotSource::new(store.clone(), "sbx-1/2026/01");

        let payload = b"hello world".to_vec();
        let part = sink
            .put_part("envd-state.json", payload.clone())
            .await
            .expect("put_part");
        assert_eq!(part.name, "envd-state.json");
        assert_eq!(part.size_bytes, payload.len() as u64);
        assert_eq!(part.sha256, sha256_hex(&payload));

        let got = source.get_part("envd-state.json").await.expect("get_part");
        assert_eq!(got, payload);
    }

    /// Two sandboxes sharing one backing store must NOT clobber each other's
    /// name-keyed objects (`parts/<name>.json`, `manifest.json`). Before
    /// `scoped()`, every sandbox wrote the same keys under one flat prefix,
    /// so a second sandbox's pause overwrote the first's part index and the
    /// first's resume reassembled the wrong bytes — the cross-sandbox bundle
    /// corruption that surfaced once pause/resume ran against a shared,
    /// persistent central bucket. `scoped(sandbox_id)` nests each sandbox's
    /// bundle under `<base>/<sandbox-id>/...` so their keyspaces are disjoint.
    #[tokio::test]
    async fn scoped_sandboxes_do_not_clobber_shared_keys() {
        let store = Arc::new(InMemoryS3::new());
        let base_sink = S3SnapshotSink::new(store.clone(), "test-bucket", "snapshots");
        let base_source = S3SnapshotSource::new(store.clone(), "snapshots");

        // Same part NAME, different content — exactly the `rootfs-delta`
        // shape that crossed in production (two sandboxes, one shared
        // `parts/rootfs-delta.tar.zst.json` key).
        let sink_a = base_sink.scoped("sbx-aaaa");
        let sink_b = base_sink.scoped("sbx-bbbb");
        let body_a = b"sandbox A rootfs delta".to_vec();
        let body_b = b"sandbox B rootfs delta (different length)".to_vec();
        sink_a
            .put_part("rootfs-delta.tar.zst", body_a.clone())
            .await
            .expect("put a");
        sink_b
            .put_part("rootfs-delta.tar.zst", body_b.clone())
            .await
            .expect("put b");

        // Each sandbox reads back ITS OWN bytes. Under the old flat layout
        // `get a` would return `body_b` (B's pause overwrote the shared part
        // index), failing this assertion.
        let source_a = base_source.scoped("sbx-aaaa");
        let source_b = base_source.scoped("sbx-bbbb");
        assert_eq!(
            source_a
                .get_part("rootfs-delta.tar.zst")
                .await
                .expect("get a"),
            body_a,
        );
        assert_eq!(
            source_b
                .get_part("rootfs-delta.tar.zst")
                .await
                .expect("get b"),
            body_b,
        );

        // The recorded snapshotRef is per-sandbox, so the controller folds a
        // distinct ref into each `Sandbox.status.snapshotRef`.
        assert_eq!(
            sink_a.manifest_url(),
            "s3://test-bucket/snapshots/sbx-aaaa/manifest.json",
        );
        assert_eq!(
            sink_b.manifest_url(),
            "s3://test-bucket/snapshots/sbx-bbbb/manifest.json",
        );
    }

    /// The manifest URL scheme is parameterized: the default constructor
    /// emits `s3://` (back-compat) while `new_with_scheme` lets the Azure
    /// path emit `azure://` over the SAME generic chunked sink.
    #[tokio::test]
    async fn manifest_url_uses_configured_scheme() {
        let store = std::sync::Arc::new(InMemoryS3::new());
        let s3_sink = S3SnapshotSink::new(store.clone(), "my-bucket", "snapshots");
        assert_eq!(
            s3_sink.manifest_url(),
            "s3://my-bucket/snapshots/manifest.json"
        );

        let az_sink = S3SnapshotSink::new_with_scheme(store, "my-container", "snapshots", "azure");
        assert_eq!(
            az_sink.manifest_url(),
            "azure://my-container/snapshots/manifest.json"
        );
    }

    /// `verify_part_present` (head+size level) accepts a correctly-written
    /// part without downloading bodies, and rejects a manifest part whose
    /// identity disagrees with the stored index (the crossed-bundle guard).
    #[tokio::test]
    async fn verify_part_present_head_size() {
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "snapshots");
        let source = S3SnapshotSource::new(store.clone(), "snapshots");

        let payload: Vec<u8> = (0..ODD_BOUNDARY_LEN).map(|i| (i % 251) as u8).collect();
        let part = sink
            .put_part("memory.zst", payload)
            .await
            .expect("put_part");

        // Correctly-written part: head+size passes.
        source
            .verify_part_present(&part)
            .await
            .expect("verify_part_present ok");

        // Tampered manifest entry (wrong sha) is rejected by the index/manifest
        // identity guard.
        let mut tampered = part.clone();
        tampered.sha256 = "deadbeef".to_string();
        assert!(
            source.verify_part_present(&tampered).await.is_err(),
            "head+size must reject a part whose sha disagrees with the stored index",
        );
    }

    #[tokio::test]
    async fn round_trip_multi_chunk_part() {
        // Use a deterministic pattern so we can spot any byte-shuffling
        // bug at the assembly site immediately.
        let payload: Vec<u8> = (0..ODD_BOUNDARY_LEN).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "ns/sbx/2026");
        let source = S3SnapshotSource::new(store.clone(), "ns/sbx/2026");

        sink.put_part("memory.zst", payload.clone())
            .await
            .expect("put_part");
        let got = source.get_part("memory.zst").await.expect("get_part");
        assert_eq!(got, payload);
        // Two chunks of 4 MiB + 1 byte: ceil(4MiB+1 / 4MiB) = 2 chunks.
        // Plus the part index, that's 3 objects total.
        assert_eq!(store.len(), 3);
    }

    #[tokio::test]
    async fn put_part_file_matches_buffered_put_part_byte_for_byte() {
        // The streaming `put_part_file` path (#293) must produce the exact
        // same on-the-wire layout — same chunk objects, same part index,
        // same `SnapshotPart` — as the buffered `put_part`. Drive both
        // against identical content and assert the stores are equal.
        let payload: Vec<u8> = (0..ODD_BOUNDARY_LEN).map(|i| (i % 251) as u8).collect();

        let buffered_store = Arc::new(InMemoryS3::new());
        let buffered_sink = S3SnapshotSink::new(buffered_store.clone(), "test-bucket", "p");
        let buffered_part = buffered_sink
            .put_part("memory.zst", payload.clone())
            .await
            .expect("buffered put_part");

        let streamed_store = Arc::new(InMemoryS3::new());
        let streamed_sink = S3SnapshotSink::new(streamed_store.clone(), "test-bucket", "p");
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        std::io::Write::write_all(&mut file, &payload).expect("write payload");
        let streamed_part = streamed_sink
            .put_part_file("memory.zst", file.path())
            .await
            .expect("streamed put_part_file");

        // Identical part manifest entry (name, hash, size).
        assert_eq!(streamed_part, buffered_part);

        // Identical object set: same chunk keys + identical part index.
        let buffered = buffered_store.inner.lock().expect("poisoned").clone();
        let streamed = streamed_store.inner.lock().expect("poisoned").clone();
        assert_eq!(streamed, buffered);

        // And the streamed part reads back to the original bytes.
        let source = S3SnapshotSource::new(streamed_store.clone(), "p");
        let got = source.get_part("memory.zst").await.expect("get_part");
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn put_part_file_handles_empty_file() {
        // A zero-length file must produce the empty-part shape: no chunks,
        // the empty-input sha256, and a part index with zero chunks —
        // matching `empty_part_round_trips` for the buffered path.
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");

        let file = tempfile::NamedTempFile::new().expect("temp file");
        let part = sink
            .put_part_file("empty.bin", file.path())
            .await
            .expect("put_part_file");
        assert_eq!(part.size_bytes, 0);
        assert_eq!(part.sha256, sha256_hex(&[]));

        let got = source.get_part("empty.bin").await.expect("get");
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn put_part_file_uploads_chunks_concurrently() {
        // Five distinct 4 MiB chunks (4 * CHUNK_SIZE + 1 byte → 5 chunks).
        // The pattern's 251-byte period is coprime-ish with CHUNK_SIZE, so
        // every chunk hashes differently and none HEAD-skips → five real
        // `put` calls. With the streaming uploader polling in-flight uploads
        // concurrently (PARALLELISM = 8 ≥ 5), they overlap; the pre-fix loop
        // that left `FuturesUnordered` unpolled while awaiting the next read
        // serialized them, capping max-in-flight at 1.
        let len = 4 * CHUNK_SIZE + 1;
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(ConcurrencyProbeStore::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");

        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        std::io::Write::write_all(&mut file, &payload).expect("write payload");
        let part = sink
            .put_part_file("memory.zst", file.path())
            .await
            .expect("put_part_file");

        assert_eq!(part.size_bytes, len as u64);
        assert!(
            store.max_in_flight() >= 2,
            "expected concurrent chunk uploads; max in-flight was {}",
            store.max_in_flight()
        );
    }

    #[tokio::test]
    async fn put_part_is_idempotent() {
        let payload: Vec<u8> = (0..ODD_BOUNDARY_LEN).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");

        sink.put_part("memory.zst", payload.clone())
            .await
            .expect("first put");
        let after_first = store.len();
        sink.put_part("memory.zst", payload.clone())
            .await
            .expect("second put");
        let after_second = store.len();
        // Re-uploading the same bytes should hit only the HEAD path for
        // chunks (already present) and overwrite only the part index;
        // total object count is unchanged.
        assert_eq!(after_first, after_second);
    }

    #[tokio::test]
    async fn empty_part_round_trips() {
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");

        let part = sink.put_part("empty.bin", Vec::new()).await.expect("put");
        assert_eq!(part.size_bytes, 0);
        assert_eq!(part.sha256, sha256_hex(&[]));

        let got = source.get_part("empty.bin").await.expect("get");
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn manifest_round_trips() {
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");

        let manifest = SnapshotManifest {
            envd_sha256: String::new(),
            runsc_version: String::new(),
            // Non-default so the round-trip proves the capture-mode flag
            // (epic #466) survives the S3 manifest write/read.
            envd_captured: true,
            sandbox_id: "sbx-test".into(),
            runtime_class: "gvisor".into(),
            sha256: "abcd".into(),
            created_at: "2026-05-25T00:00:00Z".into(),
            parts: vec![SnapshotPart {
                name: "memory.zst".into(),
                sha256: "deadbeef".into(),
                size_bytes: 42,
                uncompressed_bytes: None,
            }],
            snapshot_ref: "s3://b/p/manifest.json".into(),
            template_ref: String::new(),
            template_image: String::new(),
        };
        sink.put_manifest(&manifest).await.expect("put_manifest");
        let got = source.get_manifest().await.expect("get_manifest");
        assert_eq!(got, manifest);
    }

    #[tokio::test]
    async fn corrupt_chunk_surfaces_as_download_error() {
        let payload: Vec<u8> = (0..ODD_BOUNDARY_LEN).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");
        sink.put_part("memory.zst", payload.clone())
            .await
            .expect("put");

        // Tamper with a chunk's body directly inside the in-memory map
        // so the on-the-wire sha256 no longer matches the index.
        {
            let mut guard = store.inner.lock().expect("poisoned");
            let chunk_key = guard
                .keys()
                .find(|k| k.contains("/chunks/"))
                .expect("at least one chunk key")
                .clone();
            guard.insert(chunk_key, vec![0xFFu8; CHUNK_SIZE]);
        }
        let err = source
            .get_part("memory.zst")
            .await
            .expect_err("must reject mismatched chunk");
        assert!(
            matches!(err, RuntimeError::SnapshotDownload(_)),
            "expected SnapshotDownload, got {err:?}",
        );
    }

    #[tokio::test]
    async fn get_part_to_file_streams_multi_chunk_part_byte_for_byte() {
        // The streaming `get_part_to_file` path (#329) must reconstruct the
        // exact bytes the buffered `get_part` returns, write them to disk in
        // declared chunk order, and report a `SnapshotPart` whose digest and
        // length match the buffered manifest entry.
        let payload: Vec<u8> = (0..ODD_BOUNDARY_LEN).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");

        let put_part = sink
            .put_part("memory.zst", payload.clone())
            .await
            .expect("put_part");

        let dest = tempfile::NamedTempFile::new().expect("temp file");
        let streamed = source
            .get_part_to_file("memory.zst", dest.path())
            .await
            .expect("get_part_to_file");

        // Same manifest entry (name, hash, size) as the buffered upload —
        // `uncompressed_bytes` is the only field the download side can't know.
        assert_eq!(streamed.name, put_part.name);
        assert_eq!(streamed.sha256, put_part.sha256);
        assert_eq!(streamed.size_bytes, put_part.size_bytes);

        // And the file on disk is byte-for-byte the original payload.
        let on_disk = std::fs::read(dest.path()).expect("read staged file");
        assert_eq!(on_disk, payload);
    }

    #[tokio::test]
    async fn get_part_to_file_handles_empty_part() {
        // A zero-length part must produce an empty staged file and the
        // empty-input sha256 — matching the buffered `get_part` shape.
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");
        sink.put_part("empty.bin", Vec::new()).await.expect("put");

        let dest = tempfile::NamedTempFile::new().expect("temp file");
        let part = source
            .get_part_to_file("empty.bin", dest.path())
            .await
            .expect("get_part_to_file");
        assert_eq!(part.size_bytes, 0);
        assert_eq!(part.sha256, sha256_hex(&[]));
        assert!(std::fs::read(dest.path())
            .expect("read staged file")
            .is_empty());
    }

    #[tokio::test]
    async fn get_part_to_file_rejects_corrupt_chunk() {
        // The streamed download must verify each chunk as it lands, exactly
        // as the buffered path does — a tampered backend object surfaces as
        // `SnapshotDownload` rather than silently writing bad bytes to disk.
        let payload: Vec<u8> = (0..ODD_BOUNDARY_LEN).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");
        sink.put_part("memory.zst", payload.clone())
            .await
            .expect("put");

        {
            let mut guard = store.inner.lock().expect("poisoned");
            let chunk_key = guard
                .keys()
                .find(|k| k.contains("/chunks/"))
                .expect("at least one chunk key")
                .clone();
            guard.insert(chunk_key, vec![0xFFu8; CHUNK_SIZE]);
        }
        let dest = tempfile::NamedTempFile::new().expect("temp file");
        let err = source
            .get_part_to_file("memory.zst", dest.path())
            .await
            .expect_err("must reject mismatched chunk");
        assert!(
            matches!(err, RuntimeError::SnapshotDownload(_)),
            "expected SnapshotDownload, got {err:?}",
        );
    }

    #[tokio::test]
    async fn get_part_to_file_fetches_chunks_concurrently() {
        // Download-side mirror of `put_part_file_uploads_chunks_concurrently`
        // (#329): the streaming reader uses `buffered(PARALLELISM)`, so chunk
        // GETs must overlap rather than serialize. Five distinct 4 MiB chunks
        // (4 * CHUNK_SIZE + 1 byte) with a 251-byte period so none HEAD-skip
        // and all five hash differently → five real `get` calls. With
        // PARALLELISM = 8 ≥ 5 they run concurrently; a serial reader would
        // cap max-in-flight at 1.
        let len = 4 * CHUNK_SIZE + 1;
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(ConcurrencyProbeStore::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "p");
        let source = S3SnapshotSource::new(store.clone(), "p");

        sink.put_part("memory.zst", payload.clone())
            .await
            .expect("put_part");
        // Isolate the download measurement from the concurrent setup puts.
        store.reset_max();

        let dest = tempfile::NamedTempFile::new().expect("temp file");
        let part = source
            .get_part_to_file("memory.zst", dest.path())
            .await
            .expect("get_part_to_file");

        assert_eq!(part.size_bytes, len as u64);
        assert!(
            store.max_in_flight() >= 2,
            "expected concurrent chunk downloads; max in-flight was {}",
            store.max_in_flight()
        );
    }
}
