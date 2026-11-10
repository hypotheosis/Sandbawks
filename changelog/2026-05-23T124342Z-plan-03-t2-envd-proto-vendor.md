# Plan 03 T2: Vendor E2B `.proto` schemas + `tonic-build` codegen

**Issue:** #55 — Plan 03 T2.
**Parent:** #53 — Plan 03 epic.
**Predecessor:** #54 (PR
#85) — landed the workspace
scaffold this task plugs codegen into.

## What changed

- **Vendored E2B protocol buffer schemas** into `crates/envd/proto/`:
  - `filesystem/filesystem.proto` — the `filesystem.Filesystem` service.
  - `process/process.proto` — the `process.Process` service and the
    `process.PTY` message (E2B does not ship a separate PTY service).
- **Pinned upstream provenance** in `crates/envd/proto/SOURCE`
  (`e2b-dev/infra@eceb7419f9d06de2923bae4c4ff7ca694dd9e81d`, the most-
  recent upstream commit touching `packages/envd/spec/` at vendor time)
  and recorded Apache-2.0 attribution in `crates/envd/proto/NOTICE`.
- **Added `crates/envd/build.rs`** driving `tonic_build::configure()
  .compile_protos(&[filesystem, process], &[proto_root])`. Two
  non-default knobs documented inline:
  - `build_transport(false)` — `process.proto` defines an RPC named
    `Connect`, which prost lowers to `ProcessClient::connect(...)` and
    collides head-on with the transport `connect(dst)` constructor that
    tonic-build would otherwise generate. Suppressing the transport
    ctor at codegen time is cheaper than renaming on the proto side
    (which we cannot do without breaking SDK wire compat). Callers
    construct clients from a pre-built `tonic::transport::Channel` via
    `ProcessClient::new(channel)` instead.
  - `cargo:rerun-if-changed` on every proto file + the proto root, so
    codegen is invalidated only by schema edits.
- **Updated `crates/envd/Cargo.toml`** to add `tonic`, `prost`,
  `prost-types` as runtime deps and `tonic-build` as a build dep — all
  pinned through `workspace.dependencies` so version bumps happen in
  one place.
- **Updated `crates/envd/src/proto.rs`** to `tonic::include_proto!` the
  generated `filesystem` and `process` modules. The `pty` module is a
  Rust-side re-export of `process::Pty` and `process::pty::Size`
  rather than a third `include_proto!`, because (a) upstream defines
  PTY inside `process.proto` and (b) duplicating the message in a
  second proto package would silently diverge the next time upstream
  changed the PTY shape. The module-level docs spell this out.
- **Updated `crates/envd/src/lib.rs`** doc comment so the
  `mod@proto` line reflects that codegen is wired now; the other four
  service modules remain T1-era stubs.
- **CI workflows** (`.forgejo/workflows/ci.yml`) now install
  `protobuf-compiler` via apt in both the `rust` and
  `crd-yaml-up-to-date` jobs. Chose apt over the `protobuf-src` crate
  to avoid the ~5-minute hit of compiling `protoc` from source on
  every cold cache. The `kind-smoke` workflow already builds the
  controller image via Docker (whose Dockerfile installed
  `protobuf-compiler` from day one), so no kind-smoke change is
  needed; the controller does not depend on `envd`, but having protoc
  available in the builder stage is forward-compatible with a future
  controller→envd gRPC client.

## Validation

- `just fmt-check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean
  (one clippy fix: `std::slice::from_ref(&proto_root)` in `build.rs`
  in place of `&[proto_root.clone()]`).
- `just test` (`cargo test --workspace --all-targets`) — all suites
  green, including the `envd` smoke test.
- `cargo build -p envd` — succeeds; `target/debug/build/envd-*/out/`
  contains `filesystem.rs` and `process.rs` (the issue listed three —
  see "Departures" below).
- `cargo doc -p envd --no-deps` — succeeds and renders
  `envd::proto::{filesystem, process, pty}` with `filesystem_server`
  and `process_server` submodules visible in the generated HTML.

## Departures from the issue body

The issue body and this PR's reality differ in two places that are
intentional:

1. **`proto/` layout vs. upstream's `spec/`.** Upstream lives at
   `packages/envd/spec/`, not `packages/envd/proto/`. We use `proto/`
   inside `crates/envd/` (matching the issue's prescribed layout) and
   record the upstream → vendored path mapping in `SOURCE`. This keeps
   the issue's spec accurate locally while documenting the rename for
   the next person re-syncing.
2. **No `proto/pty/pty.proto` and no `pty.rs` in `OUT_DIR`.** The
   issue anticipated this case ("if E2B's repo defines a separate PTY
   service; otherwise extract PTY messages from `process.proto`") and
   left the choice to T2. We took the re-export path documented above
   rather than fabricating a second proto package, since duplication
   is the failure mode most likely to bite us on the next upstream
   refresh.
3. **Server module name.** The issue speculated
   `envd::proto::filesystem::filesystem_service_server`; prost's
   actual convention is `<service_snake>_server`, so the real symbol
   is `envd::proto::filesystem::filesystem_server`. The intent of the
   acceptance criterion (server stubs reachable from the docs) is
   satisfied.

## Out of scope (deferred to later Plan 03 tasks)

- `FilesystemService` impl (T3) — the trait will be implemented
  against `envd::proto::filesystem::filesystem_server::Filesystem`.
- `ProcessService` impl (T4) — same pattern against `process_server`.
- `PtyService` impl (T5) — will operate against the `pty` re-exports
  + `nix::pty`; no protobuf surface changes needed.
