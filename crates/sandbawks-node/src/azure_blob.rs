//! Azure Blob Storage backend for snapshot objects.
//!
//! Native [`BlobStore`](crate::s3::BlobStore) implementation selected when
//! `SANDBAWKS_SNAPSHOT_PROVIDER=azure`. Parallel to the `rust-s3` impl in
//! [`crate::s3`]; the chunked sink/source ([`crate::s3::S3SnapshotSink`] /
//! [`crate::s3::S3SnapshotSource`]) are backend-agnostic and are reused
//! verbatim over this store.
//!
//! ## Object model
//!
//! An S3 "bucket" maps to an Azure *container*; an S3 object key maps to a
//! *blob name* (Azure treats `/` as a virtual directory, so the
//! `<prefix>/chunks/<sha>` keyspace works unchanged). Auth is a storage
//! account name + account key (mirrors the static S3 key model).

use async_trait::async_trait;

use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobServiceClient, ContainerClient};

use sandbawks_types::RuntimeError;

use crate::s3::BlobStore;

/// Classify an Azure error as "object absent" (HTTP 404) vs. a real failure.
///
/// Azure's `get_properties`/`get_content` on a missing blob return an HTTP
/// 404; the SDK surfaces it as an [`azure_core::Error`] whose
/// `as_http_error().status()` is [`azure_core::StatusCode::NotFound`].
/// Returns `true` iff the error means "not found", so HEAD-style callers map
/// it to absent rather than propagating it.
pub(crate) fn is_not_found(err: &azure_core::Error) -> bool {
    err.as_http_error()
        .map(|http| http.status() == azure_core::StatusCode::NotFound)
        .unwrap_or(false)
}

/// Production [`BlobStore`] backed by Azure Blob Storage.
///
/// Holds one [`ContainerClient`]; cheap to `clone` (the underlying HTTP
/// pipeline is `Arc`-shared), so the sink and source share one
/// authenticated client just like the `rust-s3` path shares one `Bucket`.
#[derive(Clone)]
pub struct AzureBlobStore {
    /// The container the snapshot keyspace lives in; blob clients are minted
    /// per-key off this shared, authenticated client.
    container: ContainerClient,
}

impl AzureBlobStore {
    /// Build a store for `container` in storage account `account` using an
    /// account access key.
    ///
    /// # Arguments
    ///
    /// * `account` - Storage account name (e.g. `devstoreaccount1`).
    /// * `access_key` - Shared-key secret for `account`.
    /// * `container` - Container name the snapshot keyspace lives in.
    /// * `endpoint` - Leave `None` for the public Azure cloud; pass
    ///   `Some("http://azurite:10000/devstoreaccount1")` to target Azurite or
    ///   a sovereign cloud (used by the round-trip test).
    ///
    /// # Returns
    ///
    /// An [`AzureBlobStore`] holding one authenticated [`ContainerClient`].
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::SnapshotUpload`] if the SDK rejects the
    /// account/credentials while constructing the client. (Construction is
    /// infallible in `azure_storage_blobs` 0.21.0 — the `Result` is kept so
    /// the call site and a future fallible SDK stay compatible.)
    pub fn new(
        account: impl Into<String>,
        access_key: impl Into<String>,
        container: impl Into<String>,
        endpoint: Option<String>,
    ) -> Result<Self, RuntimeError> {
        let account = account.into();
        let creds = StorageCredentials::access_key(account.clone(), access_key.into());
        // The public-cloud constructor takes no location; the custom-endpoint
        // (Azurite / sovereign cloud) path must go through the builder because
        // `.cloud_location(..)` is a builder method, not on `new`.
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
impl BlobStore for AzureBlobStore {
    async fn head(&self, key: &str) -> Result<bool, RuntimeError> {
        match self.container.blob_client(key).get_properties().await {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(RuntimeError::SnapshotUpload(format!(
                "Azure HEAD {key}: {e}"
            ))),
        }
    }

    async fn head_size(&self, key: &str) -> Result<Option<u64>, RuntimeError> {
        match self.container.blob_client(key).get_properties().await {
            // `content_length` is a `u64` on `Blob.properties` — no clamp
            // needed, unlike the `Option<i64>` rust-s3 surfaces.
            Ok(props) => Ok(Some(props.blob.properties.content_length)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(RuntimeError::SnapshotUpload(format!(
                "Azure HEAD {key}: {e}"
            ))),
        }
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), RuntimeError> {
        self.container
            .blob_client(key)
            .put_block_blob(body)
            .await
            .map(|_| ())
            .map_err(|e| RuntimeError::SnapshotUpload(format!("Azure PUT {key}: {e}")))
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, RuntimeError> {
        self.container
            .blob_client(key)
            .get_content()
            .await
            .map_err(|e| RuntimeError::SnapshotDownload(format!("Azure GET {key}: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use azure_core::error::{Error, ErrorKind};
    use azure_core::StatusCode;

    // A real 404 `azure_core::Error` only ever reaches `is_not_found`
    // from the live SDK, where the 404 arrives wrapped in an internal
    // `HttpError` that `azure_core::Error::as_http_error` can downcast to.
    // That `HttpError` is not externally constructible in
    // `azure_core` 0.21.0 (its only constructor is `async fn new(Response)`
    // and `Response::new` takes a crate-private `PinnedStream`), so the
    // "404 => `true`" leg is exercised end-to-end by the Azurite missing-blob
    // round-trip in Task 8 rather than here.
    //
    // What is unit-testable — and what these tests pin — is the *negative*
    // contract: any error that does NOT carry a downcastable `HttpError`
    // (i.e. `as_http_error()` is `None`) must classify as "not absent" so a
    // genuine failure propagates instead of being swallowed as a missing
    // object.

    /// A 403-shaped (`Forbidden`) `HttpResponse`-kind error has no
    /// downcastable `HttpError` in its source chain, so it must NOT be
    /// treated as "not found": a permission error must propagate, never be
    /// masked as a missing blob.
    #[test]
    fn forbidden_is_not_classified_as_not_found() {
        let err = Error::new(
            ErrorKind::http_response(StatusCode::Forbidden, Some("AuthorizationFailure".into())),
            "access denied",
        );
        assert!(!is_not_found(&err));
    }

    /// Even an error whose `ErrorKind` *names* `NotFound` is not classified as
    /// absent unless a real `HttpError` is in the chain — confirming the
    /// classifier keys off the downcastable HTTP error (the SDK's actual 404
    /// shape) and never off the textual/`StatusCode` kind alone.
    #[test]
    fn not_found_kind_without_http_error_is_not_classified_as_not_found() {
        let err = Error::new(
            ErrorKind::http_response(StatusCode::NotFound, Some("BlobNotFound".into())),
            "blob missing",
        );
        // No downcastable `HttpError` => `as_http_error()` is `None` => false.
        assert!(!is_not_found(&err));
    }

    /// A non-HTTP error (here `ErrorKind::Other`) carries no HTTP status at
    /// all and must classify as "not absent".
    #[test]
    fn non_http_error_is_not_classified_as_not_found() {
        let err = Error::new(ErrorKind::Other, "socket reset");
        assert!(!is_not_found(&err));
    }
}
