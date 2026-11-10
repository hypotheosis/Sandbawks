//! Crate-link smoke test. Mirrors the same shape as
//! `crates/sandbawks-controller/tests/smoke.rs` and
//! `crates/envd/tests/smoke.rs`.

#[test]
fn crate_name_is_correct() {
    assert_eq!(sandbawks_api::CRATE_NAME, "sandbawks-api");
}
