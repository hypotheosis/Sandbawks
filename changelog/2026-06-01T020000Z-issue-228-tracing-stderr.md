# Route sandbawks-node/controller/api tracing to stderr (align with envd)

**Issue:** #228 (follow-up to #227 / #214)
**Type:** chore (`area/node`, `area/controller`, `area/api`)

## Problem

PR #227 (Plan 07 T10) moved `envd`'s `tracing_subscriber::fmt` layer onto
**stderr** via `.with_writer(std::io::stderr)`, because the subscriber's
default writer is stdout and diagnostics belong on stderr (Unix
convention: data on stdout, diagnostics on stderr). That change was
deliberately scoped to `envd`, leaving the three sibling binaries
(`sandbawks-node`, `sandbawks-controller`, `sandbawks-api`) on the `fmt`
default — stdout — so `envd` was the lone outlier.

## Change

Align the three sibling binaries onto stderr so every sandbawks binary
logs to the same stream.

- **`sandbawks-node`**, **`sandbawks-controller`**, **`sandbawks-api`** —
  each binary's `init_tracing` now calls
  `tracing_subscriber::fmt::layer().with_writer(std::io::stderr)`, matching
  `envd`.
- Each `init_tracing` gains a doc comment mirroring the "diagnostics on
  stderr / `with_writer` override is load-bearing" note `envd` already
  carries.
- No in-tree consumer parses these binaries' diagnostic **stdout** — the
  only stdout references are sandbox process exec output
  (`ServerFrame::Stdout`, `data_event::Output::Stdout`), which is
  unrelated to the binaries' own log stream.

## Tests

No new behavior to assert beyond the stream move; covered by the existing
workspace gate. `just fmt`, `cargo clippy --workspace --all-targets --
-D warnings`, and `just test` all pass.

## Operator-visible side-effect

This moves **every** log line for these three binaries from stdout to
stderr. Any stream-aware log scraper (a `kubectl logs`-adjacent pipeline
that filters by stdout vs. stderr) will see a one-release reshuffle —
identical to the `envd` change in #227. No functional behavior changes
beyond the log stream.
