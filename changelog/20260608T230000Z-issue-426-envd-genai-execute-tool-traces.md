# feat(envd): emit OTel gen_ai execute_tool traces for agentic commands (issue #426)

**Date:** 2026-06-08

## Summary

`envd` now emits OpenTelemetry **GenAI** `execute_tool` spans for the agentic
commands clients run through the E2B-compatible gRPC protocol, so Traceway
shows *what* a sandbox ran (not just *that* it was busy), correlated to the
owning sandbox via the resource attributes the controller already injects.

A new `crates/envd/src/gen_ai.rs` module owns the convention; the `process` and
`filesystem` services open one span per command.

## Problem

The transport spans installed by `crate::trace` (`handler:process::start`,
`handler:filesystem::*`) carried no command content. envd telemetry already
reaches the per-namespace OTel Collector -> Traceway (#422) and the sandbox
identity is already on the resource (`service.instance.id`), so the gap was
purely that the spans did not describe the agentic action.

## Changes

### New: `crates/envd/src/gen_ai.rs`

- `tool_span(tool_name, call_id)` builds an `execute_tool` span with the GenAI
  attributes (`gen_ai.operation.name`, `gen_ai.tool.name`, `gen_ai.tool.type`,
  `gen_ai.tool.call.id`) and a dynamic OTel span name via `otel.name`.
- `instrument_tool(...)` runs a filesystem handler inside that span and maps a
  returned `tonic::Status` to a low-cardinality `error.type`.
- `record_process_exit(...)` records `process.exit_code` / `error.type` at child
  exit.
- `capture_content()` gates content behind
  `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT` (default off).
- `redact_command` / `redact_path` mask `--token=...` / `KEY=VALUE` secret
  pairs, the value after a known secret flag, and JWT-shaped tokens before any
  argument reaches a span. Environment-variable values are never recorded.

### Modified

- `crates/envd/src/process.rs` — `spawn_pipe_child` / `spawn_pty_child` open the
  span at spawn and close it (recording the exit outcome) in the wait task.
- `crates/envd/src/filesystem.rs` — `make_dir`, `move`, `remove`, `write_all`,
  `read_all` wrap their bodies in `gen_ai::instrument_tool`.
- `crates/envd/src/lib.rs` — registers `pub mod gen_ai` and the module map.
- `TRACING.md` — new "GenAI tool-execution spans (envd)" section.

## Validation

- `cargo test -p envd` — new `gen_ai` unit tests cover redaction, the
  capture-flag parser, tool-name derivation, and the span attributes (asserted
  via a minimal capturing `tracing::Subscriber`); the existing `process` /
  `filesystem` integration suites confirm the wrapped handlers still behave.
- `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`.
- **CI end-to-end**: `deploy/kind/fixtures/otel-arrival-smoke.sh` gained an
  `OTEL_SMOKE_EXPECTED_ATTRS` knob (assert arbitrary `key=value` span/resource
  attributes reached the in-cluster collector). The `kind-smoke` workflow now
  asserts `gen_ai.operation.name=execute_tool` arrives (satisfied by the
  pause/resume smoke's `fs.write`/`fs.read` spans) and, after the Python SDK
  suite, that a process-spawn span tagged `error.type=non_zero_exit` arrives
  (from `test_commands_run_nonzero_exit`'s `sh -c 'exit 17'`).

## Enabling content capture (operators)

Content capture is wired end-to-end through the chart:

- `crates/sandbawks-controller/src/pod_adapter.rs` adds
  `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT` to `FORWARDED_OTEL_VARS`,
  so the controller propagates it into each sandbox's envd (the same two-hop
  path the other `OTEL_*` vars take). It is value-only, never a credential, so
  forwarding into the customer-controlled sandbox Pod is safe.
- The Helm chart gains `observability.captureGenaiContent` (default `false`);
  when true, `sandbawks.observabilityEnv` sets the var on the workloads. The
  kind-smoke workflow installs with it on and the OTel arrival smoke asserts a
  `gen_ai.tool.call.arguments` attribute reaches the collector.

PRIVACY: enabling it puts best-effort-redacted command lines into traces; the
redactor is a denylist of shapes, not a guarantee. Defaults off.

## Out of scope

- PTY keystroke reconstruction, a GenAI metrics instrument, and command-output
  capture (follow-up).
