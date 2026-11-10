//! E2B-compatible HTTP/WS API gateway for sandbawks.
//!
//! Layout mirrors the public-surface boundary: every externally
//! visible endpoint lives under `routes::`, while the cross-cutting
//! concerns (errors, auth, signing, envd-client pool) sit at the
//! crate root.
//!
//! The crate is intentionally library-first so integration tests can
//! drive an in-process axum router without binding a TCP port.

/// Smoke-test that downstream tests link against this crate.
pub const CRATE_NAME: &str = "sandbawks-api";

pub mod activity;
pub mod auth;
pub mod envd_auth;
pub mod envd_cache;
pub mod envd_client;
pub mod error;
pub mod http_metrics;
pub mod kube_op_metrics;
pub mod middleware;
pub mod routes;
pub mod signing;
pub mod state;
pub mod tenant_client;
pub mod usage_metrics;
pub mod warm_claim;
pub mod warm_claim_metrics;

pub use error::{AppError, AppResult};
pub use state::AppState;
