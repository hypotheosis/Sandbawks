---
title: Store snapshots in Azure Blob Storage
description: Configure Sandbawks to write sandbox snapshots to an Azure Blob Storage container instead of an S3-compatible bucket, using a storage account key.
slug: how-to/store-snapshots-in-azure-blob-storage
head:
  - tag: title
    content: Store snapshots in Azure Blob Storage · Sandbawks docs
tableOfContents: true
---

This guide shows you how to store sandbox snapshots in an Azure Blob Storage
container instead of an S3-compatible bucket. The node DaemonSet writes and reads
snapshots there, and the controller's snapshot retention sweep deletes old ones.

Sandbawks authenticates to Azure Blob Storage with the storage account name and
an account key. It doesn't support SAS tokens or workload identity.

:::caution
Changing the snapshot provider on an existing installation strands snapshots
taken with the old provider: a sandbox paused under one provider can resume only
under the same provider. Resume or delete paused `Snapshot`-mode sandboxes before
you switch.
:::

## Before you begin

- An Azure storage account and a blob container for snapshots, for example
  `sandbawks-snapshots`.
- The storage account's access key.
- Usage metering supports only S3-compatible stores. If you need
  `usage.metering.enabled`, keep an S3-compatible snapshot store.

## Store the account key in a Secret

Create a Secret in the Sandbawks namespace with the key under
`SANDBAWKS_AZURE_ACCOUNT_KEY`:

```sh
kubectl create secret generic sandbawks-azure-snapshots \
  --namespace sandbawks \
  --from-literal=SANDBAWKS_AZURE_ACCOUNT_KEY='<account-key>'
```

You can instead pass the key to Helm with
`--set-string snapshotStore.azure.accountKey=<account-key>`, and the chart creates
the Secret. Don't put the key in a values file that you commit.

## Configure the chart

Set the provider and the Azure values:

```yaml
snapshotStore:
  provider: azure
  azure:
    accountName: <storage-account-name>
    container: sandbawks-snapshots
    existingSecret: sandbawks-azure-snapshots
```

To use Azurite or a sovereign cloud, also set `snapshotStore.azure.endpoint`, for
example `http://azurite:10000/devstoreaccount1`. Leave it empty for public Azure.

Apply the values:

```sh
helm upgrade sandbawks oci://ghcr.io/hypotheosis/charts/sandbawks \
  --version 0.1.29 --namespace sandbawks --reuse-values -f azure-values.yaml
```

If you set neither `azure.existingSecret` nor `azure.accountKey`, the chart
refuses to render with the error
`snapshotStore.azure.accountKey is required when provider=azure and no existingSecret is set`.

## Check that snapshots reach the container

Pause a `Snapshot`-mode sandbox, for example with `sbx.pause()` in the Python SDK,
then check its snapshot reference:

```sh
kubectl get sandbox <sandbox-id> -n team-acme -o jsonpath='{.status.snapshotRef}'
```

The reference starts with `azure://`. List the container to see the snapshot's
objects:

```sh
az storage blob list --account-name <storage-account-name> \
  --container-name sandbawks-snapshots --output table
```

If the pause fails, check the node DaemonSet's logs on the sandbox's node for
storage errors:

```sh
kubectl logs -n sandbawks daemonset/sandbawks-node --since=10m
```

## Delete old snapshots

The controller's retention sweep works with Azure Blob Storage the same way it
does with S3. Enable it with `snapshotStore.retention.enabled`, and set the
window with `snapshotStore.retention.defaultDuration`, a namespace annotation, or
a sandbox's `spec.snapshotRetention`.

## See also

- [Helm chart reference](/reference/helm-chart/) — the `snapshotStore` values.
- [Sandbox reference](/reference/custom-resources/sandbox/) — `snapshotRetention`.
