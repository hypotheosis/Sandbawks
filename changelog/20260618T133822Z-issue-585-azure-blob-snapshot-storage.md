# Issue #585 — Configurable snapshot storage: Azure Blob Storage backend

## Summary

Snapshot storage is now backend-selectable cluster-wide via
`SANDBAWKS_SNAPSHOT_PROVIDER` (`s3` default | `azure`). Azure Blob Storage
is supported natively (account-key auth), with no s3-to-blob proxy. S3
deployments are unaffected — the provider defaults to `s3`.

## What changed

- `sandbawks-types`: new `SnapshotProvider` enum (`FromStr`, default `S3`).
- `sandbawks-node`: `AzureBlobStore` implements the existing `BlobStore`
  trait; the chunked sink/source are reused unchanged. The snapshot
  manifest URL scheme is now parameterized (`azure://`). `build_snapshot_pair`
  dispatches on the provider env var. New `SANDBAWKS_AZURE_*` config.
- `sandbawks-controller`: `AzureRetentionStore` implements
  `SnapshotRetentionStore` (LIST + DELETE); `build_retention_runner`
  dispatches on the provider. The S3 staged-rollout behavior is preserved.
- Dependencies: added `azure_core` / `azure_storage` / `azure_storage_blobs`
  0.21 (rustls, no OpenSSL). NOTE: `azure_core` requires the `hmac_rust`
  feature — without it the SDK panics on the first signed request. This was
  caught by the Azurite integration test and is REQUIRED for the backend to
  function in production.
- Helm: `snapshotStore.provider` (s3|azure) + `snapshotStore.azure.*`
  (accountName / accountKey / container / endpoint / existingSecret); an
  account-key Secret branch (operator-supplied, `required`, never generated);
  node + controller env wiring; chart `0.1.18` -> `0.1.20` (0.1.19 was
  taken by #586 landing on main first).

## Auth (v1)

Storage account name + account key via a Kubernetes Secret, mirroring the
static S3 key model. SAS tokens and Workload Identity are out of scope.

## Testing

- Unit: provider parse, manifest scheme, Azure 404-classifier (negative
  contract), timestamp conversion.
- Integration: Azurite-gated chunked round-trip + 404 paths
  (`AZURITE_ENDPOINT=... cargo test -p sandbawks-node --test azure_blob_roundtrip`).
  Self-skips in CI without Azurite.

## Operator notes

- No CRD change (backend is cluster-global config). No `kubectl apply -f deploy/crds`.
- A snapshot taken under one provider is only resumable under the same
  configured provider; the `azure://` / `s3://` manifest scheme is
  informational, not a routing key.
