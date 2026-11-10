//! Generated gRPC stubs from the vendored E2B-compatible `.proto` files.
//!
//! `build.rs` runs `tonic-build` against every schema under
//! `crates/envd/proto/`, producing one `.rs` file per proto `package`
//! in `OUT_DIR`. Each submodule below pulls one of those files in via
//! [`tonic::include_proto!`], so downstream code spells the generated
//! types as `envd::proto::<service>::<Type>` rather than referencing
//! `OUT_DIR` paths directly.
//!
//! The proto sources are pinned (see `proto/SOURCE`) to keep the wire
//! format byte-identical to the upstream E2B SDKs.
//!
//! # Why the `pty` module re-exports from [`process`]
//!
//! Upstream E2B does not define a separate `pty.proto`; the PTY message
//! and its nested `Size` live inside `package process;` and `prost`
//! lowers them to `process::Pty` and `process::pty::Size`. Duplicating
//! those types in a second proto package would silently diverge the
//! next time upstream changed the PTY shape, so we expose `pty` as a
//! Rust-side re-export of the types `prost` already generates from
//! `process.proto`. Downstream code that wants "PTY types" can still
//! write `envd::proto::pty::*` without caring where they live on the
//! wire.

/// Generated client + server stubs for the `filesystem` service
/// (`Stat`, `MakeDir`, `Move`, `ListDir`, `Remove`, `WatchDir`, the
/// non-streaming `CreateWatcher` / `GetWatcherEvents` / `RemoveWatcher`
/// trio, and the sandbawks-private `WriteAll` / `ReadAll` content RPCs
/// — see `proto/SOURCE` for the upstream-divergence notice). Backed by
/// `proto/filesystem/filesystem.proto`.
// Generated code: its `Result<_, tonic::Status>` signatures trip
// `clippy::result_large_err` (Status is ~176 B) and can't be changed here.
#[allow(clippy::result_large_err)]
pub mod filesystem {
    tonic::include_proto!("filesystem");
}

/// Generated client + server stubs for the `process` service
/// (`List`, `Connect`, `Start`, `Update`, `StreamInput`, `SendInput`,
/// `SendSignal`, `CloseStdin`) and every message reachable from it,
/// including the `PTY` and nested `PTY.Size` types. Backed by
/// `proto/process/process.proto`.
// Generated code: its `Result<_, tonic::Status>` signatures trip
// `clippy::result_large_err` (Status is ~176 B) and can't be changed here.
#[allow(clippy::result_large_err)]
pub mod process {
    tonic::include_proto!("process");
}

/// PTY message types re-exported from [`process`] for ergonomic
/// access. See the module-level docs for why this is not a separate
/// proto package today.
pub mod pty {
    pub use super::process::pty::Size;
    pub use super::process::Pty;
}
