//! Crate-link smoke test. Mirrors the same shape as
//! `crates/sandbawks-api/tests/smoke.rs`,
//! `crates/sandbawks-controller/tests/smoke.rs`, and
//! `crates/envd/tests/smoke.rs`.
//!
//! The binary's `--help` invocation itself is exercised by Plan 05
//! T10's `just node-image-run` recipe (which `docker run`s the
//! distroless image with `--help` to confirm the entrypoint loads).
//! Keeping that out of `cargo test` here avoids spawning a child
//! process from a unit test target.

#[test]
fn crate_name_is_correct() {
    assert_eq!(sandbawks_node::CRATE_NAME, "sandbawks-node");
}
