//! Round-trips a chunked snapshot through `AzureBlobStore` against Azurite,
//! validating the HTTP-404 paths that azure 0.21.0 makes impossible to
//! unit-test (`HttpError` isn't externally constructible). See #585.
//!
//! Skipped unless `AZURITE_ENDPOINT` is set. To run locally:
//!   docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
//!     azurite-blob --blobHost 0.0.0.0
//!   AZURITE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1 \
//!     cargo test -p sandbawks-node --test azure_blob_roundtrip -- --nocapture
//!
//! ## What this closes
//!
//! Tasks 3 and 6 built `AzureBlobStore` (`impl BlobStore`) and
//! `AzureRetentionStore` (`impl SnapshotRetentionStore`). Both classify a
//! "missing object" via the shared `azure_blob::is_not_found` helper, which
//! keys off `azure_core::Error::as_http_error().status() == NotFound`. That
//! 404 leg can only ever be hit by a real wire 404: `azure_core` 0.21.0's
//! `HttpError` has no public constructor (its only ctor is
//! `async fn new(Response)` over a crate-private `PinnedStream`), so the unit
//! tests in `src/azure_blob.rs` can only pin the *negative* contract. This
//! integration test drives a genuine Azurite 404 through `head`/`get` to prove
//! the positive leg end-to-end.
//!
//! ## Why this covers the retention `delete`-404 idempotency too
//!
//! `AzureRetentionStore` lives in `sandbawks-controller`; this test lives in
//! `sandbawks-node`'s `tests/`. Reaching it would require a node->controller
//! dev-dependency, which we deliberately do NOT add. That's acceptable: the
//! retention `delete` idempotency rests on the *same* `is_not_found` classifier
//! over the *same* `azure_core::Error` 404 shape. Proving here that a real
//! Azurite 404 flows correctly through `is_not_found` (via `head` => `Ok(false)`
//! and `get` => `Err`) is structurally identical to proving the retention
//! delete-404 leg, so the latter is covered transitively.

use std::sync::Arc;

use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::BlobServiceClient;

use sandbawks_node::azure_blob::AzureBlobStore;
use sandbawks_node::s3::{BlobStore, S3SnapshotSink, S3SnapshotSource};
use sandbawks_types::snapshot::{SnapshotSink, SnapshotSource};
use sandbawks_types::SnapshotManifest;

/// Azurite's well-known development storage account name.
///
/// PUBLIC fixed credential baked into the emulator — not a secret. See
/// <https://learn.microsoft.com/azure/storage/common/storage-use-azurite>.
const DEV_ACCOUNT: &str = "devstoreaccount1";

/// Azurite's well-known development storage account key (PUBLIC, not a secret).
const DEV_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Container the test keyspace lives in. Created idempotently at test start.
const CONTAINER: &str = "sandbawks-test";

/// Ensure `CONTAINER` exists in the Azurite instance at `endpoint`.
///
/// `AzureBlobStore` intentionally does not expose container creation (the
/// production container is provisioned out-of-band), so we mint a throwaway
/// `ContainerClient` here. A 409 Conflict (`ContainerAlreadyExists`) from a
/// prior run is treated as success — the operation is idempotent.
async fn ensure_container(endpoint: &str) {
    let creds = StorageCredentials::access_key(DEV_ACCOUNT.to_owned(), DEV_KEY.to_owned());
    // Custom endpoint => go through the builder (mirrors `AzureBlobStore::new`).
    let service = BlobServiceClient::builder(DEV_ACCOUNT.to_owned(), creds)
        .cloud_location(azure_storage::CloudLocation::Custom {
            account: DEV_ACCOUNT.to_owned(),
            uri: endpoint.to_owned(),
        })
        .blob_service_client();
    let container = service.container_client(CONTAINER);
    match container.create().await {
        Ok(()) => {}
        // Treat "already exists" as success so re-runs against a persistent
        // Azurite are no-ops. The error surfaces the HTTP 409 status.
        Err(e) => {
            let is_conflict = e
                .as_http_error()
                .map(|http| http.status() == azure_core::StatusCode::Conflict)
                .unwrap_or(false);
            assert!(
                is_conflict,
                "create container {CONTAINER}: unexpected error: {e}"
            );
        }
    }
}

/// End-to-end Azurite round-trip + the two 404-positive paths.
///
/// `#[tokio::test]` rather than a paused clock — this drives real HTTP to a
/// local Azurite, so wall-clock time and a live reactor are required.
#[tokio::test]
async fn azure_blob_roundtrip_and_404_paths() {
    let Ok(endpoint) = std::env::var("AZURITE_ENDPOINT") else {
        eprintln!("AZURITE_ENDPOINT unset; skipping Azure round-trip test");
        return;
    };

    ensure_container(&endpoint).await;

    let store = AzureBlobStore::new(DEV_ACCOUNT, DEV_KEY, CONTAINER, Some(endpoint))
        .expect("construct AzureBlobStore");
    // `Arc<dyn BlobStore>` is the shape both the sink and source take, so the
    // one authenticated client (and its HTTP pool) is shared.
    let blob: Arc<dyn BlobStore> = Arc::new(store);

    // --- 404-positive assertions (the gap the unit tests cannot reach) ---
    //
    // A key that was never written. Made unique per run so a persistent
    // Azurite from a prior run can't mask the absence with a stale object.
    let missing_key = format!(
        "definitely/missing/{}-{}",
        std::process::id(),
        // Nanos since the epoch — disambiguates repeated runs in one process.
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos()
    );
    // `head` of a missing blob must classify the real Azurite 404 as absent.
    assert!(
        !blob.head(&missing_key).await.expect("head missing key"),
        "head of a never-written key must be Ok(false)"
    );
    // `get` of a missing blob must surface the 404 as an error, not a panic
    // and not an empty success.
    assert!(
        blob.get(&missing_key).await.is_err(),
        "get of a never-written key must be Err"
    );

    // --- Chunked round-trip over the generic sink/source ---
    //
    // A fixed prefix is fine: `put` overwrites, so a re-run against a
    // persistent Azurite simply rewrites the same content-addressed keys.
    let key_prefix = "roundtrip";
    let sink = S3SnapshotSink::new_with_scheme(blob.clone(), CONTAINER, key_prefix, "azure");
    let source = S3SnapshotSource::new(blob.clone(), key_prefix);

    // ~9 MiB forces three 4 MiB chunks (the last one short), exercising the
    // multi-chunk upload/download path rather than a single-object shortcut.
    let payload: Vec<u8> = (0..9 * 1024 * 1024).map(|i| (i % 251) as u8).collect();

    let part = sink
        .put_part("memory.zst", payload.clone())
        .await
        .expect("put_part");

    // Build a real `SnapshotManifest` — it derives `Default`, so we fill only
    // the fields this test asserts on and leave the rest at their defaults
    // (`created_at`/`envd_sha256`/`runsc_version`/`sha256` empty).
    let manifest = SnapshotManifest {
        sandbox_id: "azure-roundtrip-test".to_owned(),
        runtime_class: "gvisor".to_owned(),
        snapshot_ref: sink.manifest_url(),
        parts: vec![part.clone()],
        ..Default::default()
    };
    sink.put_manifest(&manifest).await.expect("put_manifest");

    // Part bytes must round-trip byte-for-byte through chunked storage.
    let got = source.get_part("memory.zst").await.expect("get_part");
    assert_eq!(got, payload, "round-tripped part must match the original");

    // Manifest must round-trip and carry the `azure://` scheme.
    let manifest_back = source.get_manifest().await.expect("get_manifest");
    assert_eq!(
        manifest_back.parts.len(),
        1,
        "manifest must record exactly one part"
    );
    assert_eq!(
        manifest_back.parts[0].sha256, part.sha256,
        "round-tripped part sha256 must match"
    );
    assert!(
        manifest_back.snapshot_ref.starts_with("azure://"),
        "manifest snapshot_ref must use the azure:// scheme, got {:?}",
        manifest_back.snapshot_ref
    );
}
