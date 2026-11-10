//! Per-endpoint route modules. Tasks 6–12 fill these in.
//!
//! `envd_proxy` was added in issue #191: a per-method axum surface that
//! terminates the E2B SDK's Connect-RPC calls at the gateway and
//! re-issues them as native gRPC to the matching sandbox's envd.

pub mod envd_proxy;
pub mod events;
pub mod exec;
pub mod files;
pub mod sandboxes;
