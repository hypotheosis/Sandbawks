# Issue #575 — warm-pool reconcile log spams at info level

## Summary

The per-reconcile `"warm-pool reconciled"` structured log line in
`crates/sandbawks-controller/src/warm_pool.rs` was emitting at `info` level,
producing roughly one line every `WARM_POOL_REQUEUE` (10s) per warm pool — about
six lines/minute — which surfaced as constant noise in Traceway.

## Root cause

Commit `9194d21` ("make warm pool reconciliation a debug log") intentionally
moved this line from `tracing::info!` to `tracing::debug!`. A later merge that
rewrote the surrounding region (the #540 configurable-egress work, `fc175fe`)
resurrected the old `tracing::info!` macro **while keeping the updated comment**
that reads *"visible at the default `debug` level"*. The result was a
half-reverted state: the comment claimed `debug`, the macro was `info`.

## Change

### `crates/sandbawks-controller/src/warm_pool.rs`

- Restored the `"warm-pool reconciled"` emitter from `tracing::info!` to
  `tracing::debug!`, re-aligning the macro with its already-`debug` comment.
  The bin's `for_each` object-ref handler was already at `debug!`, so this line
  was the only per-reconcile `info`-level emitter; the reconcile loop is now
  silent at the default `info` level and observable at `debug`.

No behavior change beyond log verbosity. No Helm chart or CRD impact.
