//! `envd` — the in-sandbox agent for sandbawks.
//!
//! `envd` runs inside the per-sandbox template image as PID 1 (or close
//! to it) and exposes the filesystem, process-execution, and PTY surface
//! area that the controller and end-users drive over gRPC. The full
//! agent is built up across Plan 03's tasks; this crate's job at T1 is
//! to land the workspace scaffolding so subsequent tasks (proto
//! vendoring, codegen wiring, per-RPC implementations) can plug into a
//! stable module layout.
//!
//! # Module map
//!
//! The modules below are declared here so the public layout of the
//! crate is visible from a single file. They are filled in across the
//! Plan 03 tasks:
//!
//! - [`mod@proto`] — `tonic-build`-generated gRPC stubs from the
//!   vendored E2B-compatible `.proto` files. Wired up at T2 (#55).
//! - [`mod@filesystem`] — `FilesystemService` impl: read/write/list,
//!   streaming uploads and downloads, watches (T3+).
//! - [`mod@process`] — `ProcessService` impl: spawn, stream stdio,
//!   signal, wait (T4+).
//! - [`mod@pty`] — `PtyService` impl: allocate, resize, attach (T5+).
//! - [`mod@state`] — shared in-process state (handle tables, cancel
//!   tokens, config) wired into each service (T6+).
//! - [`mod@fingerprint`] — deterministic content fingerprint of the
//!   writable rootfs, the resume divergence oracle (Plan 07 hardening,
//!   #236).
//! - [`mod@gen_ai`] — OpenTelemetry GenAI `execute_tool` spans for the
//!   agentic commands clients run via the E2B protocol (#426).
//!
//! Service implementations under `filesystem`, `process`, `pty`, and
//! `state` remain stubs after T2 — only the generated `proto` surface
//! is real today.

/// Stable crate-name string used by smoke tests to confirm linking.
///
/// This exists so downstream crates and the integration test under
/// `tests/smoke.rs` have something concrete to depend on while the
/// real surface area is still empty.
pub const CRATE_NAME: &str = "envd";

pub mod filesystem;
pub mod fingerprint;
pub mod gen_ai;
pub mod grpc_metrics;
pub mod process;
pub mod proto;
pub mod pty;
pub mod state;
pub mod trace;
