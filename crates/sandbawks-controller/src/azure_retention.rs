//! Azure Blob Storage-backed [`SnapshotRetentionStore`].
//!
//! Parallel to [`crate::s3_retention::S3RetentionStore`]; selected when
//! `SANDBAWKS_SNAPSHOT_PROVIDER=azure`. LIST + DELETE only — the sweeper
//! never reads object bodies.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobServiceClient, ContainerClient};
use futures::StreamExt;

use crate::retention::{RetentionError, RetentionObject, SnapshotRetentionStore};

/// Convert Azure's `time::OffsetDateTime` last-modified into chrono UTC.
///
/// The retention reconciler does all its age math in `chrono`; Azure's SDK
/// reports timestamps in the `time` crate's type. Nanosecond precision is
/// irrelevant to day-scale retention, so a whole-second `unix_timestamp()`
/// round-trip is exact enough.
fn to_chrono(ts: time::OffsetDateTime) -> DateTime<Utc> {
    // Out-of-range timestamp -> now -> age ~= 0 -> KEEP. A bad timestamp must
    // never cause a delete; this fallback fails toward retaining the object.
    DateTime::from_timestamp(ts.unix_timestamp(), 0).unwrap_or_else(Utc::now)
}

/// Classify an Azure error as "blob absent" (HTTP 404) vs. a real failure.
/// Copied (not shared) from the node's `azure_blob` to avoid a
/// controller->node crate dependency.
fn is_not_found(err: &azure_core::Error) -> bool {
    err.as_http_error()
        .map(|http| http.status() == azure_core::StatusCode::NotFound)
        .unwrap_or(false)
}

/// Production [`SnapshotRetentionStore`] backed by Azure Blob Storage.
///
/// Constructed once at controller startup; the retention ticker loop
/// borrows it across every sweep. Holds a single [`ContainerClient`] —
/// the configured snapshot container — and issues LIST + DELETE against
/// it, mirroring [`crate::s3_retention::S3RetentionStore`].
#[derive(Clone)]
pub struct AzureRetentionStore {
    /// Client scoped to the snapshot container; every LIST/DELETE is
    /// relative to it.
    container: ContainerClient,
}

impl AzureRetentionStore {
    /// Build a store from account credentials and a container name.
    ///
    /// `endpoint`: `None` for public Azure; `Some(uri)` for Azurite /
    /// sovereign cloud.
    ///
    /// # Arguments
    ///
    /// * `account` - Storage account name (also the credential subject).
    /// * `access_key` - Shared-key secret for the account.
    /// * `container` - Container holding the snapshot objects to sweep.
    /// * `endpoint` - `None` → public Azure; `Some(uri)` → custom
    ///   endpoint such as an Azurite emulator or a sovereign cloud.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError::Store`] if the SDK rejects construction.
    pub fn new(
        account: impl Into<String>,
        access_key: impl Into<String>,
        container: impl Into<String>,
        endpoint: Option<String>,
    ) -> Result<Self, RetentionError> {
        let account = account.into();
        let creds = StorageCredentials::access_key(account.clone(), access_key.into());
        // Public Azure uses the default cloud location; a custom endpoint
        // (Azurite / sovereign cloud) is wired through the builder's
        // `CloudLocation::Custom`. `account` is moved into the `Custom`
        // variant, so it's cloned for the credentials above.
        let service = match endpoint {
            Some(uri) => BlobServiceClient::builder(account.clone(), creds)
                .cloud_location(azure_storage::CloudLocation::Custom { account, uri })
                .blob_service_client(),
            None => BlobServiceClient::new(account, creds),
        };
        Ok(Self {
            container: service.container_client(container.into()),
        })
    }
}

#[async_trait]
impl SnapshotRetentionStore for AzureRetentionStore {
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<RetentionObject>, RetentionError> {
        // `into_stream` yields one page per network round-trip; the SDK
        // follows continuation markers internally as we pull pages.
        let mut stream = self
            .container
            .list_blobs()
            .prefix(prefix.to_owned())
            .into_stream();
        let mut out = Vec::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| RetentionError::Store(format!("Azure LIST {prefix}: {e}")))?;
            // `page.blobs.blobs()` is an iterator of `&Blob` (not a Vec),
            // so we clone each name and convert the `time` timestamp.
            for blob in page.blobs.blobs() {
                out.push(RetentionObject {
                    key: blob.name.clone(),
                    last_modified: to_chrono(blob.properties.last_modified),
                });
            }
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), RetentionError> {
        // Idempotency contract: deleting an already-gone blob succeeds.
        // Azure surfaces that as a 404 `Err`, which `is_not_found`
        // classifies as success-equivalent.
        match self.container.blob_client(key).delete().await {
            Ok(_) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(RetentionError::Store(format!("Azure DELETE {key}: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use azure_core::error::{Error, ErrorKind};
    use azure_core::StatusCode;

    #[test]
    fn to_chrono_preserves_unix_seconds() {
        let secs = 1_781_481_600_i64; // 2026-06-15T00:00:00Z
        let azure_ts =
            time::OffsetDateTime::from_unix_timestamp(secs).expect("fixed timestamp is in range");
        assert_eq!(to_chrono(azure_ts).timestamp(), secs);
    }

    // `azure_core` 0.21.0's `HttpError` is not externally constructible
    // (its only constructor is `async fn new(Response)` over a
    // crate-private `PinnedStream`), so a real 404 only reaches
    // `is_not_found` from the live SDK. The "404 => true" leg is covered
    // end-to-end by the Azurite round-trip (Task 8). What is unit-testable
    // here is the NEGATIVE contract: any error without a downcastable
    // `HttpError` must classify as "not absent" so genuine failures
    // propagate instead of being swallowed as a missing object.

    /// A `NotFound`-*kind* error that carries no downcastable `HttpError`
    /// must NOT be classified as absent — the classifier keys off the
    /// real HTTP error in the chain, never the textual `StatusCode` kind.
    #[test]
    fn not_found_kind_without_http_error_is_not_classified_as_not_found() {
        let err = Error::new(
            ErrorKind::http_response(StatusCode::NotFound, Some("BlobNotFound".into())),
            "blob missing",
        );
        assert!(!is_not_found(&err));
    }

    /// A non-HTTP error (here `ErrorKind::Other`) carries no HTTP status
    /// at all and must classify as "not absent".
    #[test]
    fn non_http_error_is_not_classified_as_not_found() {
        let err = Error::new(ErrorKind::Other, "socket reset");
        assert!(!is_not_found(&err));
    }
}
