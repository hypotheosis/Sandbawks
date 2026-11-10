//! `rust-s3`-backed [`SnapshotRetentionStore`].
//!
//! Thin wrapper over `s3::Bucket::list` + `s3::Bucket::delete_object`
//! so the same client crate that powers `sandbawks-node`'s
//! `S3SnapshotSink` drives the controller's retention sweeper too. No
//! HEAD/GET surface — the trait is intentionally LIST/DELETE only, so
//! this impl is also the minimum surface that production code needs.
//!
//! ## Credentials & endpoint
//!
//! Construction takes the env-driven `(endpoint, bucket, region,
//! access_key, secret_key)` quintuple already used by the node
//! DaemonSet (see `deploy/helm/sandbawks/templates/node-daemonset.yaml`
//! lines 33-42). The controller Deployment surfaces the same five
//! env vars when `snapshotStore.retention.enabled` is `true` — see
//! `controller-deployment.yaml`.
//!
//! ## NotFound is success
//!
//! [`SnapshotRetentionStore::delete`]'s contract requires deleting an
//! already-gone object to succeed. `s3::Bucket::delete_object` returns
//! `Ok(_)` with `status_code == 204` regardless of whether the key
//! existed, so the contract is satisfied by treating any 2xx as
//! success and any 404 as success-equivalent.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::{Bucket, Region};

use crate::retention::{RetentionError, RetentionObject, SnapshotRetentionStore};

/// Production [`SnapshotRetentionStore`] backed by `rust-s3`.
///
/// Constructed once at controller startup; the retention ticker loop
/// borrows it across every sweep.
#[derive(Clone)]
pub struct S3RetentionStore {
    bucket: Bucket,
}

impl S3RetentionStore {
    /// Build a store from raw connection parameters.
    ///
    /// `endpoint`: `https://s3.amazonaws.com` (AWS) or
    /// `http://garage.default.svc.cluster.local:3900` (in-cluster
    /// MinIO/garage). Must be a full URL; the empty string is rejected.
    ///
    /// `region`: AWS region name for S3 proper, or any non-empty string
    /// for S3-compatible backends — they ignore it.
    pub fn new(
        endpoint: impl Into<String>,
        bucket_name: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Result<Self, RetentionError> {
        let endpoint = endpoint.into();
        if endpoint.is_empty() {
            return Err(RetentionError::Store("empty S3 endpoint".into()));
        }
        let region_obj = Region::Custom {
            region: region.into(),
            endpoint,
        };
        let credentials = Credentials::new(
            Some(&access_key.into()),
            Some(&secret_key.into()),
            None,
            None,
            None,
        )
        .map_err(|e| RetentionError::Store(format!("S3 credentials: {e}")))?;
        // `rust-s3` returns a `Box<Bucket>` from `new` *and* from
        // `with_path_style`. Unbox once after the builder chain so the
        // trait impl holds a plain `Bucket` by value.
        //
        // path-style addressing matches what `sandbawks-node`'s sink
        // uses (default for MinIO/garage; AWS S3 also accepts it).
        let bucket_boxed = Bucket::new(&bucket_name.into(), region_obj, credentials)
            .map_err(|e| RetentionError::Store(format!("S3 bucket: {e}")))?
            .with_path_style();
        Ok(Self {
            bucket: *bucket_boxed,
        })
    }
}

#[async_trait]
impl SnapshotRetentionStore for S3RetentionStore {
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<RetentionObject>, RetentionError> {
        // `list` follows continuation tokens internally and returns a
        // `Vec<ListBucketResult>`. Each result's `contents` contains
        // the per-object metadata. We concatenate across pages.
        let pages = self
            .bucket
            .list(prefix.to_owned(), None)
            .await
            .map_err(|e| RetentionError::Store(format!("S3 LIST {prefix}: {e}")))?;
        let mut out = Vec::new();
        for page in pages {
            for obj in page.contents {
                // `obj.last_modified` is an RFC 3339 string per S3's
                // LIST response shape. Parse it; skip the object (with
                // a log) if it's unparseable rather than failing the
                // whole sweep on one malformed entry.
                let last_modified = match DateTime::parse_from_rfc3339(&obj.last_modified) {
                    Ok(dt) => dt.with_timezone(&Utc),
                    Err(e) => {
                        tracing::warn!(
                            key = %obj.key,
                            raw = %obj.last_modified,
                            error = %e,
                            "skipping object: unparseable LastModified",
                        );
                        continue;
                    }
                };
                out.push(RetentionObject {
                    key: obj.key,
                    last_modified,
                });
            }
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), RetentionError> {
        // S3 returns 204 No Content on a successful DELETE and 404 if
        // the object is already gone — both are success under the
        // idempotency contract on [`SnapshotRetentionStore::delete`].
        // 404 can land via either path: as `Ok(r)` with `status==404`
        // when rust-s3 surfaces the response, or as
        // `Err(S3Error::HttpFailWithBody(404, _))` when it converts
        // the HTTP failure to an `Err`. Match the concrete variant
        // rather than `e.to_string().contains("404")` so a future
        // localized error message can't silently regress us.
        match self.bucket.delete_object(key).await {
            Ok(r) if (200..300).contains(&r.status_code()) || r.status_code() == 404 => Ok(()),
            Ok(r) => Err(RetentionError::Store(format!(
                "S3 DELETE {key}: status {}",
                r.status_code()
            ))),
            Err(S3Error::HttpFailWithBody(404, _)) => Ok(()),
            Err(e) => Err(RetentionError::Store(format!("S3 DELETE {key}: {e}"))),
        }
    }
}
