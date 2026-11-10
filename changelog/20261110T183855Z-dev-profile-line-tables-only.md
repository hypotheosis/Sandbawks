# Dev profile: line-tables-only debuginfo

Date: 2026-11-10T18:38:55Z

## Problem

The first GitHub Actions run of `ci.yml` (run 42875197355) failed in the
`rust` job's `test (with coverage)` step: `rustc-LLVM ERROR: IO failure on
output stream: No space left on device` while compiling the `sandbawks-api`
test binary. The workspace had no `[profile.dev]`, so every dev/test artifact
— and every dependency — carried full debuginfo. A local full-debuginfo build
directory for the workspace (clippy + tests) measured 20 GB, more than a
GitHub-hosted runner has free once the cargo cache and llvm-cov's separate
instrumented target are added.

## Change

`Cargo.toml`:

```toml
[profile.dev]
debug = "line-tables-only"

[profile.dev.package."*"]
debug = false
```

Workspace crates keep file:line information for backtraces and panics;
dependencies carry no debuginfo. Coverage is unaffected — `cargo llvm-cov`
uses LLVM's coverage instrumentation, not debuginfo. Release builds are
unchanged (`strip = "symbols"` already).

Trade-off: stepping through workspace code in a debugger no longer shows
local variables. Override locally with
`CARGO_PROFILE_DEV_DEBUG=true cargo build` when that is needed.

## Measured

`cargo test --workspace --no-run` into a fresh build directory: **4.4 GB**
with this profile, against 20 GB for the previous full-debuginfo build
directory (that one also held clippy's check artifacts, so the comparison is
approximate). The same setting cut the autobots workspace's test executables
from 27.9 GB to 6.3 GB.
