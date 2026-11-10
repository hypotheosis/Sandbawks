//! Smoke test: confirms the `envd` library links and exposes the
//! stable `CRATE_NAME` constant that downstream tasks key off.
//!
//! This is intentionally tiny — its job at T1 is to fail the build if
//! the workspace member wiring or the `[lib]` target ever regresses,
//! not to test any real behaviour.

#[test]
fn crate_name_is_envd() {
    assert_eq!(envd::CRATE_NAME, "envd");
}
