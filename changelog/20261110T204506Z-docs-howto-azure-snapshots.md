# How-to: store snapshots in Azure Blob Storage

Date: 2026-11-10T20:45:06Z

Added `site/src/content/docs/how-to/store-snapshots-in-azure-blob-storage.md` and its
sidebar entry. It covers:

- account-key auth via a Secret or `--set-string`;
- the `snapshotStore.provider: azure` values and the endpoint override;
- the render guard when no key is given;
- checking the `azure://` snapshot reference, and retention.

It also notes that a provider switch strands existing snapshots and that
usage metering needs an S3-compatible store.
