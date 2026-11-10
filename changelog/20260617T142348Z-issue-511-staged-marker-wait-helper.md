# Issue #511 — Shared Rust staging-gate so restore adapters honor the `.staged` marker

**Date:** 2026-06-17T14:23:48Z
**Branch:** `feat-issue-511-staged-marker-wait`
**Area:** `area/node` · **Kind:** feat · **Prio:** p2

## Problem

Scheduler-placed restore (epic #484) is *create-then-stage*: the controller
creates the restore Pod **before** the snapshot exists on any node, the
scheduler places it, and only then does the node stage the bundle onto that node
— dropping the `sandbawks_types::STAGED_MARKER_FILENAME` (`.staged`) marker
*last*, after every part is on disk. A restore that begins before that marker
appears restores from an empty/partial directory.

Where that wait lives is **runtime-specific**; the marker is the
runtime-agnostic contract. For gVisor the wait lives in the Go
`containerd-shim-sandbawks-v1` (`waitForStagedImage` in
`shim/internal/wrapper/marker.go`). The planned Rust `RuntimeAdapter` impls —
`KataFcAdapter` (Plans 08/09) and `RuncCriuAdapter` (Plans 09/10) — do **not**
use that Go shim, so they would not inherit the wait for free. Without a shared
helper, each new adapter would have to re-mirror the Go implementation by hand
and could silently race staging (the exact trap rediscovered the hard way in
PR #509).

## Fix — one tested Rust counterpart to the Go shim's `waitForStagedImage`

New module `crates/sandbawks-node/src/stage_gate.rs` provides the shared,
public, tested wait that future Rust restore adapters **call** rather than
re-derive. It is the read-side companion to the existing
`gvisor::write_staged_marker` writer and a faithful Rust mirror of the Go shim's
gate.

- **`wait_for_staged_image(image_path, timeout, poll)`** — async; polls for
  `<image_path>/.staged`. Checks the marker *before* the first sleep (an
  already-staged image returns immediately), clamps each sleep to the remaining
  time so it never overshoots `timeout`, and fails closed on any non-`NotFound`
  stat error (won't self-heal). A timed-out or failed wait surfaces as
  `RuntimeError::RestoreFailed`, so the controller's bounded `Resuming` recovery
  loop recreates the restore Pod (`RestorePodLost`) instead of restoring from an
  incomplete bundle.
- **`stage_gate_timeout()` + `parse_stage_gate_timeout()`** — resolve the
  timeout from `SANDBAWKS_STAGE_GATE_TIMEOUT_SECS` (the same env var the Go shim
  reads, so one knob tunes both restore paths), falling back to the
  300 s default on any unset/blank/zero/unparseable value — never to a
  zero-length, instant-fail wait. The pure parser is split out so the fallback
  logic is unit-tested without mutating the process-global, test-racy
  environment.
- **`STAGE_GATE_TIMEOUT_ENV`, `DEFAULT_STAGE_GATE_TIMEOUT`,
  `MARKER_POLL_INTERVAL`** — public constants mirroring the Go shim's tunables.

Uses `tokio::time::Instant` (not `std::time::Instant`) so a `start_paused` test
runtime can drive the deadline, matching the crate's existing time-based tests.

### Adapter guidance (encoded in the module docs)

- `KataFcAdapter` (whole-VM snapshot) needs a shim/path-level wait like gVisor —
  call `wait_for_staged_image` at its restore-trigger point.
- `RuncCriuAdapter` (per-container CRIU restore) *may* instead gate via a Pod
  init container — the option gVisor can't take, because a gVisor memory
  checkpoint restores as a whole sandbox and runsc rejects any container absent
  from the checkpoint. If it drives restore from the adapter itself, it calls
  the same helper.

## Why a helper now, not "wait for the second adapter"

The issue is a contract tracker: no adapter wiring is needed until a second
adapter lands. But the durable thing that can ship today — and the thing that
stops the requirement being rediscovered the hard way — is the executable,
tested contract itself. As public library API of `sandbawks-node` (the crate
where every runtime adapter's restore logic runs), the helper is not dead code;
it is the single call site a future adapter reaches for.

## Tests

`stage_gate::tests` (run under `cargo test -p sandbawks-node`):

- `parse_accepts_positive_seconds` — valid integer, whitespace-tolerant.
- `parse_falls_back_to_default` — unset / blank / zero / negative / non-numeric
  all degrade to the default.
- `returns_immediately_when_marker_present` — already-staged image returns `Ok`
  without waiting.
- `returns_after_marker_appears` — wait resolves once the marker is written
  partway through.
- `times_out_when_never_staged` — an unstaged bundle times out as
  `RuntimeError::RestoreFailed`.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace suite green.
