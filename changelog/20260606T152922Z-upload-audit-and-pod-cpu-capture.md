# Upload-path audit + per-pod CPU capture (client-vs-server discriminator)

**Date:** 2026-06-06T15:29:22Z

## Why

We had *not* actually confirmed the earlier claim that the upload slowness is
"garage's per-write cost" — the saturation signature (avg_put scales with
concurrency, throughput flat) is identical whether the bottleneck is our own
client CPU or the backend. Per the "assume it's our fault first" principle, this
(a) audits our own upload code/deps and (b) adds the measurement that cleanly
separates our process from garage.

## Code audit findings (rust-s3 0.35, our usage)

- **Double SHA-256 per uploaded chunk.** `command.rs` computes a full SHA-256
  over every PUT body for the SigV4 `x-amz-content-sha256` header — on top of
  our own content-addressing SHA-256 on the reader. That second hash runs in
  `bucket.put()` on the tokio runtime, under the node's 2-CPU limit, ×N
  concurrent. Real our-side waste, though bounded (~1–2 CPU-s for 1 GiB).
- **Connection pooling is fine** — `http_client()` returns a shared
  `Arc<hyper::Client>`; we share one Bucket, so PUTs reuse the pool (not a
  new-connection-per-request problem).
- (Already known: the cold-path HEAD-before-PUT, and the #235 validate
  re-download.)

These are bounded and don't obviously total the ~20 s in PUTs, which *leans*
toward I/O wait — but leaning isn't confirming, so we measure.

## Added: per-pod CPU over the bench window

- `ci/pod-cpu-delta.mjs` — diffs two kubelet `/stats/summary` snapshots (no
  metrics-server needed) and reports CPU-seconds + average cores per pod.
- The `bench` job snapshots node + co-located pod CPU before and after the
  bench (`Snapshot pod CPU (pre-bench)` / `Pod CPU during bench`).

Interpretation:
- `sandbawks-node` near its 2-CPU limit during upload ⇒ **our client** is the
  cap (then chase the double-hash / per-request CPU).
- `sandbawks-node` near-idle while the run is slow ⇒ we're **waiting on
  garage/disk I/O**, not CPU (then chase fsync / per-object cost — which loops
  back to chunk size).

Combined with the in-flight 16 MiB chunk-size run, the next bench answers both
"fixed-vs-byte per-PUT cost" and "client-vs-server CPU" in one pass.

## Verification

`node --check` on both CI scripts; `pod-cpu-delta.mjs` verified on synthetic
snapshots; workflow YAML parses.
