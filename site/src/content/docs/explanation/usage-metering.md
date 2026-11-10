---
title: Usage metering
description: How Sandbawks measures per-team, per-API-key sandbox usage for billing — resource-weighted seconds recorded by the controller, rolled up into hourly records, and why telemetry isn't the source.
slug: explanation/usage-metering
head:
  - tag: title
    content: Usage metering · Sandbawks docs
tableOfContents: true
---

Sandbawks can record how much sandbox time each team and each API key uses, so
you can bill for it or enforce quotas. This page explains where those numbers
come from, how they're weighted, and why they can be trusted for invoicing.
The record formats are in the
[usage records reference](/reference/usage-records/), and the settings are in the
[Helm chart reference](/reference/helm-chart/).

## Two signals, two purposes

Sandbawks produces two kinds of usage data, and only one is for billing.

- **Request counts, for abuse detection.** The API gateway counts every
  authenticated request in the OpenTelemetry metric `usage.api.requests.count`,
  with `team` and `key_name` attributes. It's useful for spotting a runaway client
  or setting rate limits.
- **Sandbox time, for billing.** The controller writes usage events and hourly
  usage records to the snapshot store's bucket. These are the system of record.

Telemetry isn't used for billing on purpose. Metrics backends downsample, expire
old data, and reset counters, and a dropped export silently loses data. Numbers
you invoice from need to be complete, durable, and reproducible, so the billing
data is written as objects that you can re-read and re-derive.

## The controller sees every billable second

Only the controller observes every transition of every sandbox, so the controller
measures usage. Each time a sandbox leaves `Running`, because it was paused,
terminated, or failed, the controller writes one usage event for the interval
that just ended. The event records the team (the sandbox's namespace), the API key
that created the sandbox, the interval's start and end, and the sandbox's CPU and
memory requests.

A sandbox doesn't accrue billable time while it's paused or still starting; only
time in `Running` counts.

## Weighting by size

A large sandbox costs more to run than a small one, so each interval's seconds are
weighted by the sandbox's size:

```text
weighted_seconds = running_seconds × max(cpu_cores, memory_gib / 4)
```

The weight is the sandbox's CPU request in cores, or its memory request in units
of 4 GiB if that's larger. So a 4-core sandbox weighs about four times as much as a
1-core one, and a sandbox with a lot of memory and little CPU is weighted by its
memory. The weight is a normalization, not a price; each event also carries the
raw CPU and memory requests, so you can apply your own pricing model.

## Hourly records

Raw events are one per `Running` interval, which is the wrong shape for invoices.
When the rollup is enabled, the controller periodically reads the events and
writes one usage record per team, per key, per hour. An interval that spans
several hours is split across them in proportion to the time it ran in each.

The rollup also counts sandboxes that are still running, up to the current time,
so a sandbox that runs for days accrues usage every hour instead of only when it
finally stops. The record for the current hour is marked `provisional`, because it
is still growing; records for hours that have ended are final and safe to invoice.

## Why the numbers don't double-count

Every write goes to a key derived from what it describes. An event's key comes from
the sandbox's UID and the interval's start time, and a record's key from the team,
key name, and hour. So writing the same event or record again overwrites it
rather than adding a duplicate. That's what makes it safe for the controller to
retry after a crash, for two rollups to overlap, and for a still-running interval
to be counted first as open and later as closed.

## Surviving storage outages

If the object store is briefly unavailable, the controller retries a usage event a
few times and then carries on with the sandbox's transition rather than blocking
it. During a long outage, the events that close while the store is unavailable
would be lost and counted in the `controller.usage.events_total{outcome="dropped"}`
metric. To avoid that, enable the usage spill: the controller then saves events
that it can't write to a PersistentVolumeClaim and replays them when the store is
back.

## Current gaps

- Usage data supports only S3-compatible stores, not Azure Blob Storage.
- Records count running time and distinct sandboxes, not the number of sandboxes
  created or peak concurrency.
- A sandbox that wasn't created through the API, for example one applied with
  `kubectl`, has no API key, so its usage is recorded with an empty key name.

## See also

- [Usage records reference](/reference/usage-records/) — event and record fields
  and object keys.
- [Helm chart reference](/reference/helm-chart/) — the `usage` values.
- [Multi-namespace tenancy](/explanation/multi-namespace-tenancy/) — how teams map
  to namespaces.
