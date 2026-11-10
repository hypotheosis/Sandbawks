//! `rust-s3`-backed usage I/O for the metering epic (#480).
//!
//! Three thin wrappers over one `s3::Bucket`, all reusing the same client crate and
//! env-driven `(endpoint, bucket, region, access_key, secret_key)` quintuple as
//! `sandbawks-node`'s `S3SnapshotSink` and the controller's
//! [`crate::s3_retention::S3RetentionStore`]:
//!
//! - [`S3UsageSink`] — Phase 2 writer: PUTs one NDJSON [`UsageEvent`] per closed
//!   `Running` interval (issue #631).
//! - [`S3UsageSource`] — Phase 3 reader: LISTs + GETs those event objects (#632).
//! - [`S3UsageLedger`] — Phase 3 writer: PUTs one NDJSON [`UsageRecord`] per
//!   `(team, key_name, hour)` (#632).
//!
//! ## Idempotency
//!
//! Both writers PUT to a deterministic key ([`UsageEvent::object_key`] /
//! [`UsageRecord::object_key`]), so a re-emitted interval or a re-run rollup
//! overwrites byte-for-byte rather than appending a duplicate — the ledger never
//! double-bills.

use async_trait::async_trait;
use s3::creds::Credentials;
use s3::{Bucket, Region};

use crate::usage::{UsageEvent, UsageEventSink, UsageSinkError};
use crate::usage_ledger::{UsageEventSource, UsageLedgerError, UsageLedgerSink, UsageRecord};

/// Build a path-style `rust-s3` [`Bucket`] from raw connection parameters.
///
/// Shared by all three usage wrappers (and shaped like
/// [`crate::s3_retention::S3RetentionStore::new`]); the empty endpoint is rejected.
/// Returns the error as a `String` so each caller can wrap it in its own error type.
fn build_bucket(
    endpoint: String,
    bucket_name: String,
    region: String,
    access_key: String,
    secret_key: String,
) -> Result<Bucket, String> {
    if endpoint.is_empty() {
        return Err("empty S3 endpoint".into());
    }
    let region_obj = Region::Custom { region, endpoint };
    let credentials = Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)
        .map_err(|e| format!("S3 credentials: {e}"))?;
    // `rust-s3` returns a `Box<Bucket>`; unbox once so callers hold a plain
    // `Bucket` by value. Path-style matches the node sink + retention store.
    let bucket_boxed = Bucket::new(&bucket_name, region_obj, credentials)
        .map_err(|e| format!("S3 bucket: {e}"))?
        .with_path_style();
    Ok(*bucket_boxed)
}

/// Production [`UsageEventSink`] backed by `rust-s3` (Phase 2 writer).
#[derive(Clone)]
pub struct S3UsageSink {
    bucket: Bucket,
    /// Key prefix every usage event object is written under (e.g. `usage`).
    prefix: String,
}

impl S3UsageSink {
    /// Build a sink from raw connection parameters and a key `prefix`.
    pub fn new(
        endpoint: impl Into<String>,
        bucket_name: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, UsageSinkError> {
        let bucket = build_bucket(
            endpoint.into(),
            bucket_name.into(),
            region.into(),
            access_key.into(),
            secret_key.into(),
        )
        .map_err(UsageSinkError::Store)?;
        Ok(Self {
            bucket,
            prefix: prefix.into(),
        })
    }
}

#[async_trait]
impl UsageEventSink for S3UsageSink {
    async fn emit(&self, event: &UsageEvent) -> Result<(), UsageSinkError> {
        let key = event.object_key(&self.prefix);
        let body = event.to_ndjson_line()?;
        // A successful PUT to a deterministic key is an idempotent overwrite. Any
        // non-2xx is surfaced as an error so the caller's fail-open path can log +
        // count it (a sink outage must never wedge the lifecycle transition).
        match self.bucket.put_object(&key, body.as_bytes()).await {
            Ok(r) if (200..300).contains(&r.status_code()) => Ok(()),
            Ok(r) => Err(UsageSinkError::Store(format!(
                "S3 PUT {key}: status {}",
                r.status_code()
            ))),
            Err(e) => Err(UsageSinkError::Store(format!("S3 PUT {key}: {e}"))),
        }
    }
}

/// Production [`UsageEventSource`] backed by `rust-s3` (Phase 3 reader).
#[derive(Clone)]
pub struct S3UsageSource {
    bucket: Bucket,
    /// Key prefix the event objects live under — the same prefix [`S3UsageSink`]
    /// writes to.
    prefix: String,
}

impl S3UsageSource {
    /// Build a source over the event `prefix`.
    pub fn new(
        endpoint: impl Into<String>,
        bucket_name: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, UsageLedgerError> {
        let bucket = build_bucket(
            endpoint.into(),
            bucket_name.into(),
            region.into(),
            access_key.into(),
            secret_key.into(),
        )
        .map_err(UsageLedgerError::Store)?;
        Ok(Self {
            bucket,
            prefix: prefix.into(),
        })
    }
}

#[async_trait]
impl UsageEventSource for S3UsageSource {
    async fn list_events(&self) -> Result<Vec<UsageEvent>, UsageLedgerError> {
        let pages = self
            .bucket
            .list(self.prefix.clone(), None)
            .await
            .map_err(|e| UsageLedgerError::Store(format!("S3 LIST {}: {e}", self.prefix)))?;
        let mut events = Vec::new();
        for page in pages {
            for obj in page.contents {
                let resp = self
                    .bucket
                    .get_object(&obj.key)
                    .await
                    .map_err(|e| UsageLedgerError::Store(format!("S3 GET {}: {e}", obj.key)))?;
                let body = resp.bytes();
                // Each object is one NDJSON line, but parse line-by-line to be robust
                // to an appended/multi-line object. A malformed line is skipped (with
                // a WARN) rather than failing the whole pass on one bad object.
                for line in body.split(|&b| b == b'\n') {
                    if line.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    match serde_json::from_slice::<UsageEvent>(line) {
                        Ok(event) => events.push(event),
                        Err(e) => tracing::warn!(
                            key = %obj.key,
                            error = %e,
                            "skipping unparseable usage event line"
                        ),
                    }
                }
            }
        }
        Ok(events)
    }
}

/// Production [`UsageLedgerSink`] backed by `rust-s3` (Phase 3 writer).
#[derive(Clone)]
pub struct S3UsageLedger {
    bucket: Bucket,
    /// Key prefix rolled records are written under (e.g. `ledger`).
    prefix: String,
}

impl S3UsageLedger {
    /// Build a ledger writer over the record `prefix`.
    pub fn new(
        endpoint: impl Into<String>,
        bucket_name: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, UsageLedgerError> {
        let bucket = build_bucket(
            endpoint.into(),
            bucket_name.into(),
            region.into(),
            access_key.into(),
            secret_key.into(),
        )
        .map_err(UsageLedgerError::Store)?;
        Ok(Self {
            bucket,
            prefix: prefix.into(),
        })
    }
}

#[async_trait]
impl UsageLedgerSink for S3UsageLedger {
    async fn write_record(&self, record: &UsageRecord) -> Result<(), UsageLedgerError> {
        let key = record.object_key(&self.prefix);
        let body = record.to_ndjson_line()?;
        match self.bucket.put_object(&key, body.as_bytes()).await {
            Ok(r) if (200..300).contains(&r.status_code()) => Ok(()),
            Ok(r) => Err(UsageLedgerError::Store(format!(
                "S3 PUT {key}: status {}",
                r.status_code()
            ))),
            Err(e) => Err(UsageLedgerError::Store(format!("S3 PUT {key}: {e}"))),
        }
    }
}
