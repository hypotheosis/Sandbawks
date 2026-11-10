//! `tonic-build` driver for the vendored E2B protocol buffer schemas.
//!
//! Compiles every `.proto` under `proto/` at build time so that
//! `src/proto.rs` can pull the generated client and server stubs in via
//! `tonic::include_proto!`. Keeping the codegen here — rather than
//! committing the generated `.rs` files into the tree — means our
//! schemas stay in sync with the upstream `.proto` files automatically
//! the next time the vendored sources are refreshed.
//!
//! See `proto/SOURCE` for the pinned upstream commit and `proto/NOTICE`
//! for the Apache-2.0 attribution required by section 4 of the license.

use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    // Resolve the proto include root relative to this `build.rs` so the
    // build is correct regardless of which directory cargo invokes it
    // from (`cargo build` from the workspace root vs. inside the crate).
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto_root = manifest_dir.join("proto");

    // Each entry maps to one vendored file. Adding a new schema means
    // dropping it under `proto/<package>/<package>.proto`, listing it
    // here, and surfacing it from `src/proto.rs` with another
    // `tonic::include_proto!` invocation.
    let protos = [
        proto_root.join("filesystem").join("filesystem.proto"),
        proto_root.join("process").join("process.proto"),
    ];

    // `compile_well_known_types(false)` keeps prost's
    // `google.protobuf.Timestamp` mapping (used by `filesystem.proto`)
    // so we get the prost-types `Timestamp` shim instead of having to
    // vendor a copy of `google/protobuf/timestamp.proto`.
    //
    // `build_transport(false)` suppresses the generated
    // `Client::connect(dst)` transport constructor on every service
    // client. We need this because `process.proto` defines an RPC
    // named `Connect`, which prost lowers to a `connect(...)` method
    // on `ProcessClient` and collides head-on with the transport's
    // `connect`. Disabling the transport ctor at codegen time is the
    // cheapest fix — downstream code constructs clients from a
    // pre-built `tonic::transport::Channel` via
    // `ProcessClient::new(channel)` instead.
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .compile_well_known_types(false)
        .compile_protos(&protos, std::slice::from_ref(&proto_root))?;

    // Tell cargo to re-run codegen only when the schemas themselves
    // change. Without this, every touch of an unrelated source file
    // would re-invoke protoc.
    println!("cargo:rerun-if-changed={}", proto_root.display());
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    // `src/bin/envd.rs` bakes `SANDBAWKS_ENVD_BUILD_ID` into `BUILD_ID` via
    // `option_env!`. `option_env!` is resolved at compile time but cargo does
    // *not* track it as an input on its own, so toggling the variable between
    // two `cargo build` invocations would otherwise reuse the cached binary and
    // silently bake the stale id. Declaring it here makes cargo recompile when
    // the value changes — load-bearing for the capture-in-checkpoint acceptance
    // test (#470), which builds two envd binaries with distinct ids from one
    // target directory.
    println!("cargo:rerun-if-env-changed=SANDBAWKS_ENVD_BUILD_ID");

    Ok(())
}
