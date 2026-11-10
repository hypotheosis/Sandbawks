# 2026-05-22T00:59:01Z — Convert root to Cargo workspace, drop template binary

Implements Plan 01 Task 1 (Forgejo issue
#2).

## What changed

- `Cargo.toml` is now a Cargo workspace manifest. It declares one
  member (`crates/sandbawks-types`, landing in Task 2 / #3), pins
  `resolver = "2"`, and centralises shared crate versions under
  `[workspace.dependencies]` so future member crates can write
  `kube = { workspace = true }` etc.
- `[workspace.package]` carries the shared
  `version`/`edition`/`authors`/`license`/`repository`/`rust-version`
  so each member crate inherits them with `.workspace = true`.
- `[profile.release]` applies project-wide release tuning (`lto = "thin"`,
  `codegen-units = 1`, `strip = "symbols"`).
- The template binary at `src/main.rs` is deleted, and the now-empty
  `src/` directory with it. The actual controller binary will live in
  `crates/sandbawks-controller` in a later plan; keeping a stub binary
  at the root would only confuse `cargo run` once the real binaries
  exist.

## Acceptance check

`cargo metadata --format-version 1 --no-deps` exits 101 with:

```
error: failed to load manifest for workspace member
  `…/crates/sandbawks-types`
  …
  No such file or directory (os error 2)
```

This matches the acceptance criterion verbatim: the only failure is the
declared-but-not-yet-created `sandbawks-types` member. Task 2 (#3)
creates that crate skeleton and the metadata call will then exit clean.

## Notes for the next task

Workspace dependency entries already cover everything `sandbawks-types`
will need (`kube`, `k8s-openapi`, `schemars`, the serde stack, and
`thiserror`), so Task 2 only has to add member files — no `Cargo.toml`
edits at the root.
