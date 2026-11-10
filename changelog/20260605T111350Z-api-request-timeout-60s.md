# Bump api global per-request timeout 30s → 60s

**Date:** 2026-06-05T11:13:50Z
**Branch:** `fix-api-request-timeout-60s`

## Problem

The api gateway wraps its unary routes in a global `TimeoutLayer` that returns
`408 Request Timeout` after a fixed budget (`middleware::timeout_layer`, wired
in `bin/sandbawks-api.rs`). That budget was **30s**.

The first unary request issued right after a **resume** (`POST
/sandboxes/{id}/connect`) races the resumed envd becoming serve-ready on a
fresh Pod: new Pod IP, restored gRPC filesystem server on `:49983` coming back
up, `status.envdAddress` being re-stamped. For small sandboxes this window is
sub-second, but a large (GiB-scale) restore can push it toward 30s — at which
point a file download issued immediately after `connect` hits the ceiling and
returns 408 even though the sandbox is healthy. This surfaced as an
intermittent pause/resume smoke failure on exactly the post-resume sentinel
download (`handler:files::download`, `idle_ns ≈ 30.0s`, → 408).

## Change

`crates/sandbawks-api/src/bin/sandbawks-api.rs`: `timeout_layer(30)` →
`timeout_layer(60)`, with a comment explaining the resume serve-readiness race
and the GiB-restore headroom rationale.

60s gives restore + serve-readiness headroom while still bounding genuinely
stuck unary calls. Streaming routes (exec WS, file upload/download bodies) are
unaffected — they are exempt via the existing router/layer split and set their
own longer bounds.

## Notes

- The only production cap on these requests is this axum layer; the other
  `Duration::from_secs(30)` occurrences in `envd_cache.rs` are `#[tokio::test]`
  deadlines, not runtime limits.
- This does **not** fix the underlying resume race (readiness gates on the
  `:49984` `/__state` HTTP probe while the download uses the `:49983` gRPC
  filesystem server; `status.envdAddress` can briefly lag a fresh Pod). It
  widens the margin so a slow-but-healthy restore no longer trips 408. A
  follow-up that gates readiness on the gRPC port (or has the smoke wait for a
  fresh `envdAddress` before downloading) would close the race itself.

## Validation

- `cargo fmt --all -- --check` ✅
- `cargo clippy --workspace --all-targets -- -D warnings` ✅
- `cargo test --workspace --all-targets` ✅
