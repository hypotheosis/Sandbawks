---
title: Usage records
description: Reference for the usage events and hourly usage records the controller writes to the snapshot store — object keys and fields.
slug: reference/usage-records
head:
  - tag: title
    content: Usage records · Sandbawks docs
tableOfContents: true
---

The controller writes two kinds of usage objects to the `snapshotStore` bucket
when usage metering is enabled. Each object holds one JSON object followed by a
newline (NDJSON). Field names are `snake_case` and stable within a schema
version. For how they're produced, see
[Usage metering](/explanation/usage-metering/).

## Usage events

Written when `usage.metering.enabled` is `true`, one per interval a sandbox spent
in `Running`. An event is written when the interval ends: on pause, termination,
or failure.

### Object key

```text
<usage.eventPrefix>/<team>/<sandbox_uid>/<interval_start>.json
```

`usage.eventPrefix` defaults to `usage`. In `<interval_start>`, the colons of the
timestamp are replaced with `-`. Writing the same interval again overwrites the
same object.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `schema` | integer | Event format version. Currently `1`. |
| `team` | string | The sandbox's namespace. |
| `key_name` | string | Name of the `TeamAPIKey` that created the sandbox. Empty when the sandbox has no `sandbawks.dev/api-key-name` annotation, for example one applied with `kubectl`. |
| `sandbox_id` | string | The sandbox's name. |
| `sandbox_uid` | string | The sandbox's `metadata.uid`. Together with `interval_start`, it identifies the event. |
| `interval_start` | string (RFC 3339) | When the sandbox entered `Running`. |
| `interval_end` | string (RFC 3339) | When the sandbox left `Running`. |
| `running_seconds` | number | Seconds between `interval_start` and `interval_end`. |
| `cpu_request` | string | The sandbox's CPU request as a Kubernetes quantity, for example `500m`. |
| `mem_request` | string | The sandbox's memory request as a Kubernetes quantity, for example `1Gi`. |
| `weighted_seconds` | number | `running_seconds` × `max(CPU cores, memory GiB / 4)`. |

## Usage records

Written when `usage.rollup.enabled` is `true`, one per team, API key, and hour.
The rollup runs every `usage.rollup.interval` and rewrites the records it derives.

### Object key

```text
<usage.ledgerPrefix>/<team>/<key_name>/<period_start>.json
```

`usage.ledgerPrefix` defaults to `ledger`. An empty `key_name` is written as
`_nokey`. In `<period_start>`, the colons of the timestamp are replaced with `-`.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `schema` | integer | Record format version. Currently `1`. |
| `team` | string | The sandboxes' namespace. |
| `key_name` | string | Name of the `TeamAPIKey`. Empty for usage without a key. |
| `period_start` | string (RFC 3339) | Start of the hour, inclusive. |
| `period_end` | string (RFC 3339) | End of the hour, exclusive: `period_start` plus one hour. |
| `weighted_running_seconds` | number | Weighted seconds that fell in this hour. The billable figure. |
| `running_seconds` | number | Unweighted seconds that fell in this hour. |
| `distinct_sandboxes` | integer | Number of different sandboxes, by UID, that ran in this hour for this key. |
| `source_interval_count` | integer | Number of `Running` intervals that contributed to this record. |
| `provisional` | boolean | `true` for the current, still-accumulating hour. `false` once the hour has ended; only final records are safe to invoice. Absent in older records, which means `false`. |

Intervals that span several hours are split across them in proportion to time.
For hours that have ended, the sum of `weighted_running_seconds` across records
equals the sum of `weighted_seconds` across the events.

## Related metrics

| Metric | Source | Description |
|--------|--------|-------------|
| `usage.api.requests.count` | API gateway | Authenticated requests, with `team` and `key_name` attributes. For abuse detection, not billing. |
| `controller.usage.events_total` | Controller | Usage event writes, by `outcome`. `dropped` counts events lost after their retries. |

## See also

- [Helm chart reference](/reference/helm-chart/) — the `usage` values.
