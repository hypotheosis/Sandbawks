//! Smoke coverage for the `EnvdPool` channel cache (Plan 04 T9).
//!
//! A real round-trip against a live envd lives in T15's integration
//! suite — here we just verify that asking for a channel against a
//! port nothing is listening on bubbles up an [`AppError`] instead of
//! panicking or hanging past the connect timeout.

use sandbawks_api::envd_client::EnvdPool;
use sandbawks_api::error::AppError;

/// `127.0.0.1:1` is the loopback `tcpmux` port. Nothing routine binds
/// it on a dev box or CI runner, so a `connect` against it should
/// fail fast (connection refused) rather than hang on a SYN that
/// nobody answers.
const UNREACHABLE: &str = "127.0.0.1:1";

#[tokio::test]
async fn channel_against_dead_port_yields_upstream_error() {
    let pool = EnvdPool::new();

    let err = pool
        .channel(UNREACHABLE)
        .await
        .expect_err("connect to :1 must fail — nothing listens there");

    // The pool maps every transport failure to `UpstreamUnavailable`
    // with the stable `envd_unavailable` code. Asserting on the
    // variant + code (rather than the message, which embeds the raw
    // OS error string) keeps this stable across platforms.
    match err {
        AppError::UpstreamUnavailable { code, .. } => {
            assert_eq!(code, "envd_unavailable");
        }
        other => panic!("expected UpstreamUnavailable, got {other:?}"),
    }
}
