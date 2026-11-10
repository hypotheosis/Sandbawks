# Fix flaky `quiesce_drains_grpc_connections_and_rebinds` (issue #567)

**Date:** 2026-06-21T23:56:21Z
**Issue:** #567 — Flaky test: `quiesce_drains_grpc_connections_and_rebinds` races on its rebind port under full-suite parallelism
**Crate:** `envd` (`crates/envd/src/bin/envd.rs`)

## Summary

The test `tests::quiesce_drains_grpc_connections_and_rebinds` intermittently failed
under full-workspace parallelism (`cargo test --workspace --all-targets`) while
passing reliably in isolation. Investigation found **three** distinct defects; the
issue diagnosed one (the port TOCTOU), but that was not the dominant cause. All
three are fixed.

## Root causes and fixes

### 1. Drain ack could outrun the drain it claimed to satisfy (the real flake)

`run_grpc_loop` acked the drain epoch by re-borrowing `drain_req` at *bind time*:

```rust
let _ = drain_done_tx.send(*drain_req_rx.borrow_and_update());
```

On the **boot** iteration this borrow reads whatever epoch is current. If a drain
request raced in during the microsecond window between the first `bind` and that
borrow, the boot bind acked epoch 1 **without ever tearing a runtime down** — so
the held connection was never force-closed. `DrainHandle::drain` then returned
`true` while the connection stayed open, and the test's read for EOF blocked until
its 2 s cap. Under full-suite CPU contention the supervisor and test race tightly,
so this hit ~10% of runs; in isolation the supervisor always wins the race, so it
never fired.

**Fix:** decouple the ack epoch from the bind-time borrow. The boot iteration acks
epoch `0`; a drain captures its requested epoch when `drain_req.changed()` fires,
and only the *next* iteration — which runs strictly after `shutdown_timeout`
force-closes every connection — acks it. The ack can now never precede the drain
it claims to satisfy. This is a small but real correctness improvement to the #560
drain protocol, not only a test fix.

### 2. Bind-after-release port TOCTOU (issue #567's diagnosis; Option 1)

Production bound the gRPC listener lazily inside the supervisor's first loop
iteration, and the test pre-picked a port with `bind("127.0.0.1:0")` then `drop`ped
it for the supervisor to re-bind — leaving a window where the port was unowned and
another listener could grab it.

**Fix (issue's preferred Option 1):** bind the gRPC listener up front in `run`
(symmetric with the existing HTTP `/__state` listener) and hand the bound
`std::net::TcpListener` to `run_grpc_loop`. The first iteration adopts it via
`TcpListener::from_std`; only later drain rebinds re-bind by address (safe — a
listening socket frees its port immediately on close, no `TIME_WAIT`). This removes
the unowned-port window in both production and the test, and surfaces a gRPC bind
failure synchronously at boot instead of deep in the supervisor thread.

### 3. Over-strict "still readable" assertion

Even with correct ordering, a tonic/HTTP-2 server writes an unprompted `SETTINGS`
frame on accept. Those bytes are buffered on the held connection and delivered
before the close, so asserting the *first* `read` is EOF/reset races that frame.

**Fix:** the test now drains any in-flight server bytes and requires the stream to
*reach* EOF or a reset within the cap — which is what "the connection was closed"
actually means.

## Validation

- `cargo fmt --all --check` — clean.
- `cargo clippy -p envd --all-targets -- -D warnings` — clean.
- `cargo test --workspace --all-targets` run **40×** back-to-back: **0 failures**
  (previously ~10% per run). The isolated test remains green.

## Out of scope

The production drain/rebind *mechanism* (validated by this test and the #560 A/B
harness) is otherwise unchanged; defect #1 is a narrow ack-ordering correction
within it.
