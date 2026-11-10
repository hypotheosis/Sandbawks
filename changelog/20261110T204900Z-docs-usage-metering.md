# Usage metering explanation and usage records reference

Date: 2026-11-10T20:49:00Z

Added `site/src/content/docs/explanation/usage-metering.md` and
`site/src/content/docs/reference/usage-records.md`, with sidebar entries.

- The explanation covers why billing is not taken from OTel, the
  controller-side interval events, size weighting, hourly rollup with
  provisional records, idempotent object keys, and the retry/spill path.
- The reference documents the event and record object keys and fields
  (schema 1) and the two related metrics.
