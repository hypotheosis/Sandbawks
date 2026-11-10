#[test]
fn crate_links() {
    // Smoke test: if this file compiles and links against
    // sandbawks_types, the crate exists and is on the workspace.
    let _ = sandbawks_types::CRATE_NAME;
}

#[test]
fn error_display_renders() {
    let e = sandbawks_types::Error::InvalidQuantity("not a number".into());
    let rendered = format!("{e}");
    assert!(rendered.contains("not a number"), "got: {rendered}");
}
