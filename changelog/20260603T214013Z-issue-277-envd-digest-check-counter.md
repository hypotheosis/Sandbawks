# envd digest determinism-check counter (issue #277)

**Date:** 2026-06-03T21:40:13Z
**Issue:** #277 — *node: counter for skipped envd digest determinism checks (#270 follow-up)*
**Branch:** `feat-issue-277-envd-digest-check-counter`

## Context

`GvisorAdapter::verify_envd_digest` (issue #270) guards a memory `runsc
restore` against a drifted node-staged envd binary: if the digest recorded at
pause differs from this node's staged binary, the restore would reconnect the
workload's mmap'd code pages to a different binary and silently corrupt the
sandbox. The check has two deliberate warn-then-**proceed** fall-throughs (no
recorded digest; local binary unreadable) and one fail-**closed** abort
(digests known and differ). Until now the only signal for any of these was a
`tracing::warn!`, so "did the determinism check actually run on this restore?"
was unanswerable, and a cluster silently skipping it on every restore was
invisible.

The issue was blocked on the absence of a metrics pipeline. That blocker is
gone: OTel Phase 6 (PR #317) wired an OTLP metrics path into the workspace and
established the convention in `crates/sandbawks-node/src/snapshotjob_metrics.rs`
(cached instruments off the global meter, no-op-safe when the SDK is disabled).

## Change

New module `crates/sandbawks-node/src/envd_digest_metrics.rs`, modelled on
`snapshotjob_metrics.rs`:

- One instrument `envd_digest.checks_total` (`Counter<u64>`) under the meter
  `sandbawks-node.envd_digest`, with two bounded attributes:

  | `outcome`  | `reason`           | Exit       |
  |------------|--------------------|------------|
  | `verified` | `match`            | `Ok(())`   |
  | `skipped`  | `empty_manifest`   | `Ok(())`   |
  | `skipped`  | `local_unreadable` | `Ok(())`   |
  | `failed`   | `mismatch`         | `Err(..)`  |

- A third `path` attribute (`resume` | `stage` | `pause_validate`) records
  *which* caller ran the check (see "Review revision" below).
- A `DigestCheckOutcome` enum and a `CheckPath` enum close the label
  vocabulary and make the `(outcome, reason)` / `path` mappings exhaustively
  compiler-checked.
- `record_check(path, outcome)` increments the counter exactly once per call.
  The instrument is cached in a `OnceLock`; with `OTEL_SDK_DISABLED` or no
  installed provider the global meter is a no-op, so the call stays on the
  restore path unconditionally.

`verify_envd_digest` now calls `record_check` at all four of its exits, and the
two `skipped`-branch `tracing::warn!`s also carry structured `outcome` / `reason`
fields so logs are greppable against the same vocabulary the metric exposes.

### Why one counter instead of the two the issue named

The issue proposed `envd_digest_check_skipped_total{reason=...}` plus an
optional `envd_digest_check_failed_total`. Folding **every** exit — including
`verified` — into a single counter with an `outcome` dimension turns each exit
into an alertable series. The `reason` label preserves the issue's exact
`empty_manifest` / `local_unreadable` vocabulary.

### Review revision: the `path` label (PR #319 review #128)

The first cut claimed the SLO was a direct `sum(outcome="verified") / sum()`
ratio. Review caught that `verify_envd_digest` also runs on the pause node via
`validate_restorable`, where the check is a tautology that always records
`verified` / `match` — inflating that denominator on any realistic
pause:restore ratio. The fix threads a `CheckPath` caller through
`fetch_verify_and_lay_out` so the counter carries a `path` label, and the SLO
filters the pause-side tautology:

```
sum(outcome="verified", path=~"resume|stage") / sum(path=~"resume|stage")
```

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all groups pass, including the three new unit tests
  (`labels_cover_every_outcome`, `path_label_covers_every_caller`,
  `recording_against_the_noop_meter_does_not_panic`).

The counter's *emission* over a live OTLP pipeline requires a full restore
against the e2e collector fixture (PR #318) and was not exercised locally; the
recording logic is covered by the no-op-meter unit test, and the four call
sites are covered by the existing `verify_envd_digest` tests.

## Follow-ups

The sibling fail-closed gates flagged in the issue's audit — `enforce_headroom`
(#238) and `assert_writable_mounts_overlay_backed` (#237) — share this gap and
can now adopt the same `*_metrics.rs` template.
