# Issue #194 — Node reconciler idempotency floor covers in-flight phases

## Why this surfaced now

PR #195's fix for issue #193 (the `*.img` glob) turned the pause path
from a millisecond-scale no-op into a real ~5 second operation on a
16 MiB workload. That widened a latent race in the node-side
SnapshotJob reconciler enough to hit iteration 1 of the bench, every
time.

Bench failure dump from the first CI run after merging #193:

- `Sandbox.status.phase = Failed`
- `SnapshotJob.status.phase = Failed`
- `SnapshotJob.status.message = "checkpoint failed: envd /__state timed out after 10s"`
- BUT `SnapshotJob.status.manifest.parts[].sizeBytes` was **fully
  populated** with real bytes (memory.zst = 17,567,373 bytes — the
  #193 fix working as intended).
- envd container `State: Terminated / Reason: Completed / Exit Code: 0`
  with only two log lines, both from boot.

## Root cause

The node reconciler's `pause()` was firing twice for the same
SnapshotJob:

1. First call: succeeded end-to-end, populated the manifest in the
   SnapshotJob's `status.manifest`, and patched `status.phase =
   Complete`. The `runsc checkpoint` step terminated the envd
   container (its standard, documented behavior).
2. Second call: `resolve_pod` still succeeded (the kube API hadn't
   GC'd the now-Terminated Pod). `fetch_state` (step 3 of `pause()`)
   timed out after 10 s because envd was gone. Returned
   `RuntimeError::CheckpointFailed`. Reconciler patched
   `status.phase = Failed` with the error message. Due to
   `SnapshotJobStatus.manifest`'s `#[serde(skip_serializing_if =
   "Option::is_none")]`, the `manifest: None` field elided from the
   Merge-patch body, so the apiserver kept the manifest from call 1.

The second call fires because the kube-rs Controller can re-fire
`reconcile` with a stale cached object before the watch event for
the terminal-phase patch has landed in the reflector cache. The old
idempotency floor only short-circuited on terminal phases:

```rust
if matches!(prior_phase, SnapshotJobPhase::Complete | SnapshotJobPhase::Failed) {
    return Ok(());
}
```

So a re-fire that read the cached `phase = Checkpointing` (from the
first call's pre-flight patch) fell through and re-ran `pause()`.

## Fix

Add a second idempotency floor in
`crates/sandbawks-node/src/reconcile.rs`:

```rust
if matches!(prior_phase, Pending | Checkpointing | Uploading | Restoring) {
    // started_at is the canonical "we've already begun" signal —
    // we stamp it on every pre-flight patch and never clear it.
    if let Some(started_at_str) = prior_started_at {
        let age = Utc::now().signed_duration_since(parse(started_at_str));
        if age >= 0 && age < STUCK_JOB_RECOVERY_AGE {
            return Ok(());  // no-op: stale re-fire
        }
        // else: presumed crashed, fall through and re-dispatch
    }
}
```

The `STUCK_JOB_RECOVERY_AGE` constant (5 minutes) gates the crash-
recovery path. Sized to be ~10× the worst-case legitimate pause time
on a slow CI runner for a 1 GiB workload, so a real in-flight job
won't ever hit it, but a wedged job (worker pod OOMed, kubelet
killed it) gets re-dispatched within a single bench iteration's 60 s
phase budget.

A negative age (worker clock behind apiserver) is treated as "fresh,
still in flight" rather than re-dispatching — avoids an infinite
recovery loop on clock skew.

## Tests

Four new tests in `crates/sandbawks-node/tests/reconcile.rs`:

- `in_flight_phase_with_recent_started_at_is_idempotent` — the
  exact #194 reproducer. Phase=Checkpointing + started_at=now →
  reconcile must be a no-op.
- `in_flight_phase_with_stale_started_at_recovers` — 6 minutes ago
  (past the 5-minute threshold) → reconcile must re-dispatch
  pause() and emit the normal two-phase patch sequence.
- `in_flight_resume_phase_is_idempotent_too` — same idempotency
  for the resume side (`Restoring` phase).
- `in_flight_phase_without_started_at_falls_through` — defensive:
  malformed status (phase=Checkpointing, started_at=None) is
  treated as fresh rather than wedging.

## Bench / CI doc updates

`tests/bench/src/main.rs` and `.forgejo/workflows/kind-smoke.yaml`
"current state" sections updated to reflect both #193 and #194 are
fixed. CI iteration / payload defaults unchanged (still 3 × 16 MiB
to keep the per-PR job under ~10 minutes); operators can now raise
them freely.

## Out of scope (still open follow-ups)

- Trend-data-driven raising of `BENCH_ITERATIONS` / `BENCH_PAYLOAD_MIB`
  in CI. Wait for a few green runs to establish baseline timings.
- `BENCH_INCLUDE_RESUME=1` — still blocked by forgejo #176 (runsc
  cannot restore over a kubelet-running container).
